import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from html import escape
from pathlib import Path
from time import monotonic
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from normalization import normalize_feed_listing
from sources.biddit_source import BIDDIT_BASE_URL, BidditFetchError, parse_biddit_search_results
from sources.common import DEFAULT_SOURCE_HEADERS, detect_property_type

BIDDIT_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
BIDDIT_API_HEADERS = dict(DEFAULT_SOURCE_HEADERS)
BIDDIT_API_HEADERS["User-Agent"] = BIDDIT_BROWSER_USER_AGENT
BIDDIT_API_HEADERS["Accept"] = "application/json, text/plain, */*"

BIDDIT_RESULT_WAIT_SELECTORS = [
    "a[href^='/fr/search/']",
    "a[href^='/nl/search/']",
    "a[href^='/de/search/']",
    "script[type='application/json']",
    "script[type='application/ld+json']",
]
BIDDIT_NETWORK_JSON_HINTS = [
    "api",
    "graphql",
    "search",
    "listing",
    "auction",
    "property",
    "result",
    "biddit",
]
DEFAULT_DEBUG_DIR = Path("debug") / "biddit"
DEFAULT_MAX_BIDDIT_PAGES = 4
DEFAULT_BIDDIT_EXPANSION_ROUNDS = 4
DEFAULT_BIDDIT_DETAIL_TIMEOUT = 30
LOAD_MORE_TEXT_TOKENS = [
    "voir plus",
    "plus de resultats",
    "plus d'annonces",
    "show more",
    "load more",
    "meer resultaten",
    "toon meer",
    "meer",
]
NEXT_PAGE_TEXT_TOKENS = [
    "suivant",
    "page suivante",
    "next",
    "volgende",
    "volgende pagina",
    "weiter",
]


@dataclass
class BidditBrowserRenderResult:
    html: str
    final_url: str
    page_title: str | None = None
    network_payloads: list[object] = field(default_factory=list)
    response_htmls: list[str] = field(default_factory=list)
    screenshot_bytes: bytes | None = None
    body_text_excerpt: str | None = None
    navigation_timed_out: bool = False
    detected_content_selector: str | None = None
    page_htmls: list[str] = field(default_factory=list)
    visited_page_urls: list[str] = field(default_factory=list)
    pagination_urls_detected: list[str] = field(default_factory=list)
    followed_pagination_urls: list[str] = field(default_factory=list)
    followed_api_page_urls: list[str] = field(default_factory=list)
    detail_urls_followed: list[str] = field(default_factory=list)
    detail_enriched_count: int = 0
    reported_total_results: int | None = None
    coverage_notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class BidditBrowserCollectionResult:
    items: list[dict[str, object]]
    final_url: str
    visited_page_urls: list[str] = field(default_factory=list)
    pagination_urls_detected: list[str] = field(default_factory=list)
    followed_pagination_urls: list[str] = field(default_factory=list)
    followed_api_page_urls: list[str] = field(default_factory=list)
    detail_urls_followed: list[str] = field(default_factory=list)
    detail_enriched_count: int = 0
    reported_total_results: int | None = None
    coverage_notes: list[str] = field(default_factory=list)



def collect_biddit_browser_listing_result(
    search_url: str,
    *,
    timeout_ms: int = 45000,
    headless: bool = True,
    debug_save_html: bool = False,
    debug_screenshot: bool = False,
    debug_dir: str | Path | None = None,
    max_pages: int = DEFAULT_MAX_BIDDIT_PAGES,
) -> BidditBrowserCollectionResult:
    result = render_biddit_search_page_with_playwright(
        search_url,
        timeout_ms=timeout_ms,
        headless=headless,
        capture_screenshot=debug_screenshot,
        debug_save_html=debug_save_html,
        debug_dir=debug_dir,
        max_pages=max_pages,
    )

    network_items = extract_biddit_network_listings(result.network_payloads)
    embedded_items = extract_biddit_embedded_listings(result.html)
    html_items = parse_biddit_search_results(result.html)
    response_html_items = _extract_items_from_html_sources(result.response_htmls)
    page_html_items = _extract_items_from_html_sources(result.page_htmls)
    items = _merge_items_by_listing_id(
        html_items + page_html_items + response_html_items + embedded_items + network_items
    )

    if not items:
        artifact_paths = _persist_debug_artifacts(
            result=result,
            debug_dir=debug_dir,
            debug_save_html=debug_save_html,
            debug_screenshot=debug_screenshot,
        )
        diagnostic = diagnose_biddit_browser_failure(result)
        raise BidditFetchError(
            f"Aucune annonce extraite depuis {result.final_url}. {diagnostic}{_format_artifact_suffix(artifact_paths)}"
        )

    (
        items,
        detail_urls_followed,
        detail_enriched_count,
        detail_notes,
    ) = _enrich_biddit_items_from_detail_api(
        items,
        timeout=DEFAULT_BIDDIT_DETAIL_TIMEOUT,
    )
    coverage_notes = list(result.coverage_notes)
    coverage_notes.extend(detail_notes)

    return BidditBrowserCollectionResult(
        items=[{**item, "data_origin": "live"} for item in items],
        final_url=result.final_url,
        visited_page_urls=result.visited_page_urls,
        pagination_urls_detected=result.pagination_urls_detected,
        followed_pagination_urls=result.followed_pagination_urls,
        followed_api_page_urls=result.followed_api_page_urls,
        detail_urls_followed=detail_urls_followed,
        detail_enriched_count=detail_enriched_count,
        reported_total_results=result.reported_total_results,
        coverage_notes=coverage_notes,
    )



def collect_biddit_browser_listings(
    search_url: str,
    *,
    timeout_ms: int = 45000,
    headless: bool = True,
    debug_save_html: bool = False,
    debug_screenshot: bool = False,
    debug_dir: str | Path | None = None,
    max_pages: int = DEFAULT_MAX_BIDDIT_PAGES,
) -> list[dict[str, object]]:
    return collect_biddit_browser_listing_result(
        search_url,
        timeout_ms=timeout_ms,
        headless=headless,
        debug_save_html=debug_save_html,
        debug_screenshot=debug_screenshot,
        debug_dir=debug_dir,
        max_pages=max_pages,
    ).items



def render_biddit_search_page_with_playwright(
    search_url: str,
    *,
    timeout_ms: int = 45000,
    headless: bool = True,
    capture_screenshot: bool = False,
    debug_save_html: bool = False,
    debug_dir: str | Path | None = None,
    max_pages: int = DEFAULT_MAX_BIDDIT_PAGES,
) -> BidditBrowserRenderResult:
    sync_playwright, playwright_timeout_error = _load_playwright_sync_api()
    page = None
    browser = None
    network_payloads: list[object] = []
    response_htmls: list[str] = []
    page_htmls: list[str] = []
    visited_page_urls: list[str] = []
    pagination_urls_detected: list[str] = []
    followed_pagination_urls: list[str] = []
    followed_api_page_urls: list[str] = []
    coverage_notes: list[str] = []
    navigation_timed_out = False
    detected_content_selector: str | None = None
    reported_total_results: int | None = None

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=headless)
            context = browser.new_context(
                user_agent=BIDDIT_BROWSER_USER_AGENT,
                locale="fr-BE",
                viewport={"width": 1440, "height": 2200},
                ignore_https_errors=True,
                extra_http_headers={
                    "Accept-Language": "fr-BE,fr;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Upgrade-Insecure-Requests": "1",
                },
            )
            page = context.new_page()
            page.on(
                "response",
                lambda response: _capture_network_artifacts(
                    response,
                    network_payloads,
                    response_htmls,
                ),
            )

            try:
                page.goto(
                    search_url,
                    wait_until="domcontentloaded",
                    timeout=min(timeout_ms, 20000),
                )
            except Exception as exc:
                navigation_timed_out = _is_timeout_exception(exc, playwright_timeout_error)

            page.wait_for_timeout(900)
            detected_content_selector = _stabilize_biddit_page(
                page,
                timeout_ms=max(1000, timeout_ms - 2000),
            )
            _expand_biddit_page_in_place(
                page,
                timeout_ms=min(8000, max(2500, timeout_ms // 3)),
                coverage_notes=coverage_notes,
            )
            current_html = _record_biddit_page_snapshot(page, page_htmls, visited_page_urls)
            reported_total_results = _coalesce_max_int(
                reported_total_results,
                _extract_reported_total_from_html(current_html),
                _extract_reported_total_from_network_payloads(network_payloads),
            )
            api_followed_urls, api_reported_total = _expand_biddit_api_pages(
                network_payloads,
                max_pages=max_pages,
                coverage_notes=coverage_notes,
            )
            followed_api_page_urls = _merge_unique_strings(followed_api_page_urls, api_followed_urls)
            reported_total_results = _coalesce_max_int(
                reported_total_results,
                api_reported_total,
                _extract_reported_total_from_network_payloads(network_payloads),
            )
            pagination_urls_detected = _merge_unique_strings(
                pagination_urls_detected,
                _extract_pagination_urls_from_html(
                    current_html,
                    base_url=page.url,
                    current_url=page.url,
                ),
            )

            remaining_pages = max(1, max_pages) - 1
            while remaining_pages > 0:
                next_url = _pick_next_pagination_url(
                    current_html,
                    base_url=page.url,
                    current_url=page.url,
                    already_visited=visited_page_urls,
                )
                if not next_url:
                    break

                followed_pagination_urls.append(next_url)
                try:
                    page.goto(
                        next_url,
                        wait_until="domcontentloaded",
                        timeout=min(timeout_ms, 20000),
                    )
                except Exception as exc:
                    coverage_notes.append(f"navigation pagination echouee: {next_url}")
                    if _is_timeout_exception(exc, playwright_timeout_error):
                        coverage_notes.append("timeout sur page supplementaire Biddit")
                    break

                page.wait_for_timeout(900)
                selector = _stabilize_biddit_page(
                    page,
                    timeout_ms=max(1500, min(7000, timeout_ms // 3)),
                )
                if detected_content_selector is None and selector is not None:
                    detected_content_selector = selector
                _expand_biddit_page_in_place(
                    page,
                    timeout_ms=min(7000, max(2000, timeout_ms // 4)),
                    coverage_notes=coverage_notes,
                )
                current_html = _record_biddit_page_snapshot(page, page_htmls, visited_page_urls)
                reported_total_results = _coalesce_max_int(
                    reported_total_results,
                    _extract_reported_total_from_html(current_html),
                    _extract_reported_total_from_network_payloads(network_payloads),
                )
                pagination_urls_detected = _merge_unique_strings(
                    pagination_urls_detected,
                    _extract_pagination_urls_from_html(
                        current_html,
                        base_url=page.url,
                        current_url=page.url,
                    ),
                )
                remaining_pages -= 1

            if max_pages > 1 and len(visited_page_urls) <= 1 and not followed_pagination_urls and not followed_api_page_urls:
                if pagination_urls_detected:
                    coverage_notes.append(
                        "pagination candidate detectee, mais aucune nouvelle page n'a ete suivie"
                    )
                else:
                    coverage_notes.append(
                        "aucune pagination DOM detectee; la couverture etendue ne peut pas depasser la page courante"
                    )

            html = _best_available_html(page, response_htmls)
            page_title = _safe_page_title(page)
            body_text_excerpt = _safe_page_text_excerpt(page)
            screenshot_bytes = _safe_page_screenshot(page) if capture_screenshot else None
            final_url = page.url if page is not None else search_url
            browser.close()
            return BidditBrowserRenderResult(
                html=html,
                final_url=final_url,
                page_title=page_title,
                network_payloads=network_payloads,
                response_htmls=response_htmls,
                screenshot_bytes=screenshot_bytes,
                body_text_excerpt=body_text_excerpt,
                navigation_timed_out=navigation_timed_out,
                detected_content_selector=detected_content_selector,
                page_htmls=page_htmls,
                visited_page_urls=visited_page_urls,
                pagination_urls_detected=pagination_urls_detected,
                followed_pagination_urls=followed_pagination_urls,
                followed_api_page_urls=followed_api_page_urls,
                reported_total_results=reported_total_results,
                coverage_notes=coverage_notes,
            )
    except BidditFetchError:
        raise
    except Exception as exc:
        result = BidditBrowserRenderResult(
            html=_best_available_html(page, response_htmls),
            final_url=page.url if page is not None else search_url,
            page_title=_safe_page_title(page),
            network_payloads=network_payloads,
            response_htmls=response_htmls,
            screenshot_bytes=_safe_page_screenshot(page) if capture_screenshot else None,
            body_text_excerpt=_safe_page_text_excerpt(page),
            navigation_timed_out=navigation_timed_out,
            detected_content_selector=detected_content_selector,
            page_htmls=page_htmls,
            visited_page_urls=visited_page_urls,
            pagination_urls_detected=pagination_urls_detected,
            followed_pagination_urls=followed_pagination_urls,
            followed_api_page_urls=followed_api_page_urls,
            reported_total_results=reported_total_results,
            coverage_notes=coverage_notes,
        )
        artifact_paths = _persist_debug_artifacts(
            result=result,
            debug_dir=debug_dir,
            debug_save_html=debug_save_html,
            debug_screenshot=capture_screenshot,
        )
        diagnostic = diagnose_biddit_browser_failure(result)
        message = str(exc)
        lowered = message.lower()
        if "executable doesn't exist" in lowered or "browser executable" in lowered:
            raise BidditFetchError(
                "Chromium Playwright n'est pas installe. Lance `python -m playwright install chromium`."
            ) from exc
        if _is_timeout_exception(exc, playwright_timeout_error):
            raise BidditFetchError(
                f"Timeout Playwright pendant la navigation de {search_url}. {diagnostic}{_format_artifact_suffix(artifact_paths)}"
            ) from exc
        raise BidditFetchError(
            f"Echec Playwright pendant la collecte Biddit pour {search_url}: {message}. {diagnostic}{_format_artifact_suffix(artifact_paths)}"
        ) from exc
    finally:
        try:
            if browser is not None:
                browser.close()
        except Exception:
            pass


def extract_biddit_embedded_listings(html: str) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, object]] = []

    for script in soup.find_all("script"):
        payload = _extract_json_payload(script)
        if payload is None:
            continue
        for candidate in _walk_json_objects(payload):
            item = _build_listing_from_json_candidate(candidate)
            if item is not None:
                items.append(item)

    return _merge_items_by_listing_id(items)



def extract_biddit_network_listings(network_payloads: list[object]) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for payload in _iter_network_payload_bodies(network_payloads):
        for candidate in _walk_json_objects(payload):
            item = _build_listing_from_json_candidate(candidate)
            if item is not None:
                items.append(item)
    return _merge_items_by_listing_id(items)



def diagnose_biddit_browser_failure(result: BidditBrowserRenderResult) -> str:
    html_sources = [*result.page_htmls, result.html, *result.response_htmls]
    combined_html = "\n".join(source for source in html_sources if source)
    normalized_html = combined_html.lower()
    title = (result.page_title or "").strip()
    excerpt = (result.body_text_excerpt or "").strip()
    lowered_excerpt = excerpt.lower()
    parts: list[str] = []

    if not combined_html.strip() and result.navigation_timed_out:
        parts.append("Timeout avant obtention d'un premier document HTML exploitable.")
    elif not combined_html.strip():
        parts.append("Page rendue vide apres navigation Playwright.")
    elif _looks_like_antibot(normalized_html, title, result.final_url, lowered_excerpt):
        parts.append("La page rendue ressemble a un challenge anti-bot ou a un acces refuse.")
    elif result.network_payloads:
        parts.append(
            f"{len(result.network_payloads)} reponses JSON utiles ont ete capturees, mais aucune annonce Biddit n'a pu etre mappee proprement."
        )
    elif result.detected_content_selector is None and result.response_htmls:
        parts.append("Des reponses HTML ont ete chargees, mais aucun selecteur attendu n'a ete detecte dans le DOM rendu.")
    elif result.detected_content_selector is None:
        parts.append("Aucun selecteur attendu n'a ete detecte apres chargement; le markup live Biddit est probablement different ou incomplet.")
    else:
        parts.append("Le DOM Biddit a ete charge, mais le parsing actuel n'a pas encore permis une extraction fiable.")

    if title:
        parts.append(f"Titre page: {title}.")
    if excerpt:
        parts.append(f"Extrait page: {excerpt[:220]}.")
    if result.navigation_timed_out:
        parts.append("Navigation cible timeoutee.")
    if result.detected_content_selector:
        parts.append(f"Selecteur detecte: {result.detected_content_selector}.")
    if result.visited_page_urls:
        parts.append(f"Pages visitees: {len(result.visited_page_urls)}.")
    if result.followed_pagination_urls:
        parts.append(f"Pagination suivie: {len(result.followed_pagination_urls)} page(s) supplementaire(s).")
    elif result.pagination_urls_detected:
        parts.append(f"Pagination detectee: {len(result.pagination_urls_detected)} URL(s) candidates.")
    if result.followed_api_page_urls:
        parts.append(f"Pages API suivies: {len(result.followed_api_page_urls)}.")
    if result.reported_total_results is not None:
        parts.append(f"Total resultats detecte: {result.reported_total_results}.")
    if result.coverage_notes:
        parts.append("Notes couverture: " + "; ".join(result.coverage_notes[:4]) + ".")

    return " ".join(parts).strip()



def format_biddit_browser_coverage_summary(result: BidditBrowserCollectionResult) -> str:
    parts = [f"annonces uniques: {len(result.items)}"]
    if result.visited_page_urls:
        parts.append(f"pages visitees: {len(result.visited_page_urls)}")
    if result.pagination_urls_detected:
        parts.append(f"pagination candidates: {len(result.pagination_urls_detected)}")
    if result.followed_pagination_urls:
        parts.append(f"pages supplementaires suivies: {len(result.followed_pagination_urls)}")
    if result.followed_api_page_urls:
        parts.append(f"pages API suivies: {len(result.followed_api_page_urls)}")
    if result.detail_urls_followed:
        parts.append(f"details suivis: {len(result.detail_urls_followed)}")
    if result.detail_enriched_count:
        parts.append(f"details enrichis: {result.detail_enriched_count}")
    if result.reported_total_results is not None:
        parts.append(f"total detecte: {result.reported_total_results}")
        parts.append(f"couverture: {len(result.items)}/{result.reported_total_results}")
    if result.coverage_notes:
        parts.append("notes: " + "; ".join(result.coverage_notes[:3]))
    return " | ".join(parts)




def _enrich_biddit_items_from_detail_api(
    items: list[dict[str, object]],
    *,
    timeout: int,
) -> tuple[list[dict[str, object]], list[str], int, list[str]]:
    if not items:
        return [], [], 0, []

    detail_urls_followed: list[str] = []
    detail_enriched_count = 0
    coverage_notes: list[str] = []
    enriched_items: list[dict[str, object]] = []

    with requests.Session() as session:
        for item in items:
            source_url = str(item.get("source_url") or "").strip()
            listing_id = str(item.get("source_listing_id") or "").strip()
            if not listing_id or not source_url:
                enriched_items.append(dict(item))
                continue

            detail_payload = _fetch_biddit_detail_payload(
                session,
                listing_id,
                referer=source_url,
                timeout=timeout,
            )
            if detail_payload is None:
                enriched_items.append(dict(item))
                continue

            detail_urls_followed.append(source_url)
            detail_item = _build_listing_from_biddit_detail_payload(detail_payload, source_url=source_url)
            if detail_item is None:
                enriched_items.append(dict(item))
                continue

            merged_item = _merge_biddit_listing_with_detail(dict(item), detail_item)
            if merged_item != item:
                detail_enriched_count += 1
            enriched_items.append(merged_item)

    if detail_urls_followed:
        coverage_notes.append(f"details Biddit suivis: {len(detail_urls_followed)}")
    if detail_enriched_count:
        coverage_notes.append(f"details Biddit enrichis: {detail_enriched_count}")

    return enriched_items, detail_urls_followed, detail_enriched_count, coverage_notes


def _fetch_biddit_detail_payload(
    session: requests.Session,
    listing_id: str,
    *,
    referer: str,
    timeout: int,
) -> dict[str, object] | None:
    detail_api_url = f"{BIDDIT_BASE_URL}/api/eco/biddit-bff/lot/{listing_id}"
    headers = dict(BIDDIT_API_HEADERS)
    headers["Referer"] = referer
    try:
        response = session.get(
            detail_api_url,
            headers=headers,
            timeout=(10, timeout),
            allow_redirects=True,
        )
        response.raise_for_status()
    except requests.RequestException:
        return None

    try:
        payload = response.json()
    except ValueError:
        return None
    return payload if isinstance(payload, dict) else None


def _build_listing_from_biddit_detail_payload(
    payload: dict[str, object],
    *,
    source_url: str,
) -> dict[str, object] | None:
    listing_id = _coerce_first_string(payload.get("reference"))
    properties = payload.get("properties")
    if not listing_id or not isinstance(properties, list) or not properties or not isinstance(properties[0], dict):
        return None

    property_item = properties[0]
    rooms = property_item.get("rooms") if isinstance(property_item.get("rooms"), dict) else {}
    features = property_item.get("features") if isinstance(property_item.get("features"), dict) else {}
    construction = property_item.get("construction") if isinstance(property_item.get("construction"), dict) else {}
    address = property_item.get("address") if isinstance(property_item.get("address"), dict) else {}
    municipality = address.get("municipality") if isinstance(address.get("municipality"), dict) else {}

    title = _pick_biddit_localized_text(property_item.get("title"))
    description = _pick_biddit_localized_text(property_item.get("description")) or title
    postal_code = _coerce_first_string(address.get("postalCode"))
    commune = _pick_biddit_localized_text(municipality)
    price = _coerce_float(
        payload.get("sellingPrice")
        or payload.get("currentPrice")
        or payload.get("startingPrice")
        or payload.get("initialStartingPrice")
        or payload.get("amountBouquet")
    )
    property_type_value = _coerce_first_string(property_item.get("propertySubtype") or property_item.get("propertyType"))
    text_seed = " ".join(part for part in [property_type_value, title, description] if part).lower()
    property_type = detect_property_type(text_seed) or _map_property_type(property_type_value)
    existing_units = _coerce_int(construction.get("numberOfHousingUnits")) or extract_biddit_units_from_text(description)
    surface = _coerce_float(
        rooms.get("livingSurfaceArea")
        or property_item.get("livingSurfaceArea")
        or features.get("terrainSurface")
        or property_item.get("terrainSurface")
        or property_item.get("businessSurface")
        or rooms.get("businessSurface")
        or features.get("garageSurface")
        or construction.get("constructionSurface")
    )
    copro_status = _detect_biddit_copro_status(description)
    is_active = _is_biddit_detail_payload_active(payload)

    handling_method = _coerce_first_string(payload.get("handlingMethod")) or ""
    public_sale_status = _coerce_first_string(payload.get("publicSaleStatus")) or ""
    notes = ["Collecte Biddit browser api", "detail: lot api"]
    if handling_method:
        notes.append(f"mode: {handling_method.lower()}")
    if public_sale_status:
        notes.append(f"statut: {public_sale_status.lower()}")

    raw_item = {
        "source_name": "Biddit",
        "source_listing_id": listing_id,
        "source_url": source_url,
        "title": title,
        "description": description,
        "price": price,
        "postal_code": postal_code,
        "commune": commune,
        "property_type": property_type,
        "transaction_type": "sale",
        "existing_units": existing_units,
        "surface": surface,
        "copro_status": copro_status,
        "is_active": is_active,
        "notes": " | ".join(notes),
    }
    return normalize_feed_listing(raw_item, default_source_name="Biddit")


def _merge_biddit_listing_with_detail(
    base_item: dict[str, object],
    detail_item: dict[str, object],
) -> dict[str, object]:
    merged = dict(base_item)
    preferred_fields = {
        "title",
        "description",
        "price",
        "postal_code",
        "commune",
        "property_type",
        "existing_units",
        "surface",
        "copro_status",
        "is_active",
        "notes",
    }
    for field_name in preferred_fields:
        detail_value = detail_item.get(field_name)
        if field_name == "is_active":
            if detail_value is not None:
                merged[field_name] = detail_value
            continue
        if _has_meaningful_biddit_value(detail_value):
            merged[field_name] = detail_value

    merged["is_copro"] = merged.get("copro_status") == "true"
    return merged


def _is_biddit_detail_payload_active(payload: dict[str, object]) -> bool:
    if payload.get("soldTimestamp") or payload.get("withdrawnTimestamp"):
        return False

    status = _normalize_ui_text(_coerce_first_string(payload.get("publicSaleStatus")) or "")
    if status in {"withdrawn", "closed", "sold", "expired", "cancelled", "ended"}:
        return False

    bidding_end = _parse_biddit_datetime(payload.get("biddingEndDateTime"))
    if bidding_end is not None and bidding_end <= datetime.utcnow():
        return False
    return True


def _parse_biddit_datetime(value: object) -> datetime | None:
    rendered = _coerce_first_string(value)
    if not rendered:
        return None
    try:
        return datetime.fromisoformat(rendered.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _detect_biddit_copro_status(text: str | None) -> str:
    normalized = _normalize_ui_text(text or "")
    if any(token in normalized for token in ("copropr", "co-propr", "mede-eigendom")):
        return "true"
    if any(token in normalized for token in ("sans copro", "hors copro", "pas de copro")):
        return "false"
    return "unknown"


def extract_biddit_units_from_text(text: str | None) -> int | None:
    if not text:
        return None
    match = re.search(
        r"(\d+)\s*(?:unites?|unites d'habitation|logements?|appartements?)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None

def _load_playwright_sync_api():
    try:
        from playwright.sync_api import TimeoutError, sync_playwright
    except ImportError as exc:
        raise BidditFetchError(
            "Playwright n'est pas installe. Installe `playwright` puis lance `python -m playwright install chromium`."
        ) from exc
    return sync_playwright, TimeoutError



def _capture_network_artifacts(response, payloads: list[object], response_htmls: list[str]) -> None:
    try:
        if response.status >= 400:
            return
        headers = {key.lower(): value for key, value in response.headers.items()}
        content_type = headers.get("content-type", "").lower()
        response_url = response.url
        lowered_url = response_url.lower()
        text = response.text()
        if not text:
            return

        if "html" in content_type and len(response_htmls) < 8:
            response_htmls.append(text)

        if len(payloads) >= 40:
            return
        if "json" not in content_type and not any(token in lowered_url for token in BIDDIT_NETWORK_JSON_HINTS):
            return
        payload = _load_json_text(text)
        if payload is None:
            return
        payloads.append(
            {
                "_url": response_url,
                "_content_type": content_type,
                "_payload": payload,
            }
        )
    except Exception:
        return



def _expand_biddit_api_pages(
    network_payloads: list[object],
    *,
    max_pages: int,
    coverage_notes: list[str],
) -> tuple[list[str], int | None]:
    seed_entry = _find_biddit_search_payload_entry(network_payloads)
    total_results = _extract_reported_total_from_network_payloads(network_payloads)
    if seed_entry is None:
        return [], total_results

    payload = seed_entry.get("_payload") if isinstance(seed_entry, dict) else None
    if not isinstance(payload, dict):
        return [], total_results

    total_pages = _coerce_int(payload.get("totalPages"))
    total_results = _coalesce_max_int(
        total_results,
        _coerce_int(payload.get("totalElements")),
        _coerce_int(payload.get("count")),
    )
    if total_pages is None or total_pages <= 1:
        return [], total_results

    seed_url = str(seed_entry.get("_url") or "")
    summary_total = str(total_results) if total_results is not None else "?"
    coverage_notes.append(f"pagination API Biddit detectee: {total_pages} pages / {summary_total} lots")

    seed_result_page_number = _coerce_int(payload.get("number"))
    seen_result_page_numbers: set[int] = set()
    if seed_result_page_number is not None:
        seen_result_page_numbers.add(seed_result_page_number)

    target_unique_pages = max(0, max_pages - 1)
    max_request_page = total_pages + 1
    request_page = (_extract_page_number(seed_url) or 0) + 1

    followed_urls: list[str] = []
    known_urls = {
        str(entry.get("_url") or "")
        for entry in network_payloads
        if isinstance(entry, dict) and entry.get("_url")
    }

    with requests.Session() as session:
        while len(followed_urls) < target_unique_pages and request_page <= max_request_page:
            page_url = _replace_query_param(seed_url, "page", str(request_page))
            request_page += 1
            if not page_url or page_url in known_urls:
                continue
            try:
                response = session.get(
                    page_url,
                    headers=BIDDIT_API_HEADERS,
                    timeout=(10, 20),
                    allow_redirects=True,
                )
                response.raise_for_status()
            except requests.RequestException:
                coverage_notes.append(f"page API Biddit non chargee: {page_url}")
                break

            payload_text = response.text
            payload_value = _load_json_text(payload_text)
            if not isinstance(payload_value, dict):
                coverage_notes.append(f"payload API Biddit invalide: {page_url}")
                break

            result_page_number = _coerce_int(payload_value.get("number"))
            if result_page_number is not None and result_page_number in seen_result_page_numbers:
                coverage_notes.append(f"page API Biddit dupliquee ignoree: {page_url}")
                known_urls.add(page_url)
                continue

            network_payloads.append(
                {
                    "_url": page_url,
                    "_content_type": response.headers.get("content-type", "application/json"),
                    "_payload": payload_value,
                }
            )
            followed_urls.append(page_url)
            known_urls.add(page_url)
            if result_page_number is not None:
                seen_result_page_numbers.add(result_page_number)

    if followed_urls:
        coverage_notes.append(f"pages API Biddit suivies: {len(followed_urls)}")

    return followed_urls, total_results


def _find_biddit_search_payload_entry(network_payloads: list[object]) -> dict[str, object] | None:
    for entry in network_payloads:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("_url") or "")
        payload = entry.get("_payload")
        if "/api/eco/search-service/lot/_search" not in url:
            continue
        if isinstance(payload, dict) and isinstance(payload.get("content"), list):
            return entry
    return None


def _replace_query_param(url: str, key: str, value: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query[key] = [value]
    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))


def _stabilize_biddit_page(page, timeout_ms: int) -> str | None:
    deadline = monotonic() + (timeout_ms / 1000)

    selector = _wait_for_any_selector(page, 2500)
    if selector:
        return selector

    _try_wait_for_state(page, "networkidle", 2500)
    selector = _wait_for_any_selector(page, 1800)
    if selector:
        return selector

    _scroll_page(page)
    remaining_ms = max(800, int((deadline - monotonic()) * 1000))
    selector = _wait_for_any_selector(page, min(remaining_ms, 3500))
    page.wait_for_timeout(800)
    return selector



def _expand_biddit_page_in_place(
    page,
    *,
    timeout_ms: int,
    coverage_notes: list[str],
) -> None:
    deadline = monotonic() + (timeout_ms / 1000)
    previous_count = _count_biddit_items_in_page(page)
    initial_count = previous_count
    stable_rounds = 0
    clicked_any = False

    for _ in range(DEFAULT_BIDDIT_EXPANSION_ROUNDS):
        if monotonic() >= deadline:
            break

        clicked = _click_biddit_load_more(page)
        clicked_any = clicked_any or clicked
        _scroll_page(page)
        _try_wait_for_state(page, "networkidle", 1800)
        page.wait_for_timeout(700 if clicked else 500)

        current_count = _count_biddit_items_in_page(page)
        if current_count > previous_count:
            if clicked:
                coverage_notes.append(f"chargement additionnel detecte: {previous_count} -> {current_count}")
            previous_count = current_count
            stable_rounds = 0
            continue

        stable_rounds += 1
        if stable_rounds >= 2 and not clicked:
            break

    if previous_count > initial_count:
        return
    if clicked_any:
        coverage_notes.append(
            "tentatives de chargement additionnel detectees, sans nouveaux resultats visibles"
        )
        return
    coverage_notes.append("aucun chargement additionnel detecte sur la page courante")



def _wait_for_any_selector(page, timeout_ms: int) -> str | None:
    for selector in BIDDIT_RESULT_WAIT_SELECTORS:
        try:
            page.locator(selector).first.wait_for(state="attached", timeout=timeout_ms)
            return selector
        except Exception:
            continue
    return None



def _try_wait_for_state(page, state: str, timeout_ms: int) -> None:
    try:
        page.wait_for_load_state(state, timeout=timeout_ms)
    except Exception:
        return



def _scroll_page(page) -> None:
    try:
        page.mouse.wheel(0, 1800)
        page.wait_for_timeout(400)
        page.mouse.wheel(0, 2200)
        page.wait_for_timeout(500)
    except Exception:
        return



def _click_biddit_load_more(page) -> bool:
    try:
        candidates = page.locator("button, a, [role='button']")
        candidate_count = min(candidates.count(), 80)
    except Exception:
        return False

    for index in range(candidate_count):
        try:
            candidate = candidates.nth(index)
            text = _normalize_ui_text(candidate.inner_text(timeout=500))
            label = _normalize_ui_text(candidate.get_attribute("aria-label") or "")
            combined = f"{text} {label}".strip()
            if not combined or not any(token in combined for token in LOAD_MORE_TEXT_TOKENS):
                continue
            candidate.scroll_into_view_if_needed(timeout=800)
            candidate.click(timeout=1500)
            return True
        except Exception:
            continue
    return False



def _count_biddit_items_in_page(page) -> int:
    return _count_biddit_items_in_html(_safe_page_content(page))



def _extract_json_payload(script) -> object | None:
    script_type = (script.get("type") or "").strip().lower()
    if script_type not in {"application/ld+json", "application/json", ""}:
        return None

    raw_text = script.string or script.get_text(strip=False)
    if not raw_text:
        return None

    return _load_json_text(raw_text.strip())



def _load_json_text(text: str) -> object | None:
    stripped = text.strip() if text else ""
    if not stripped:
        return None
    if stripped[:1] in "[{":
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return None
    return None



def _walk_json_objects(value: object):
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from _walk_json_objects(nested)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_json_objects(item)



def _build_listing_from_json_candidate(candidate: dict[object, object]) -> dict[str, object] | None:
    search_hit_item = _build_listing_from_biddit_search_hit(candidate)
    if search_hit_item is not None:
        return search_hit_item

    absolute_url = _coerce_listing_url(candidate)
    if not absolute_url:
        return None

    listing_id = _extract_listing_id(absolute_url)
    if not listing_id:
        return None

    title = _coerce_first_string(candidate.get("name") or candidate.get("title"))
    description = _coerce_first_string(candidate.get("description") or candidate.get("summary"))
    price = _coerce_float(
        candidate.get("price")
        or candidate.get("startingPrice")
        or candidate.get("currentBid")
        or candidate.get("amount")
    )
    address = candidate.get("address") if isinstance(candidate.get("address"), dict) else {}
    postal_code = _coerce_first_string(
        address.get("postalCode") or candidate.get("postalCode") or candidate.get("zipCode")
    )
    commune = _coerce_first_string(
        address.get("addressLocality") or candidate.get("city") or candidate.get("commune")
    )
    surface = _coerce_float(
        candidate.get("surface")
        or candidate.get("area")
        or candidate.get("livingArea")
        or candidate.get("floorArea")
    )
    existing_units = _coerce_int(
        candidate.get("existing_units") or candidate.get("numberOfUnits") or candidate.get("units")
    )
    property_type_value = _coerce_first_string(
        candidate.get("propertyType") or candidate.get("category") or candidate.get("type")
    )
    text_seed = " ".join(part for part in [property_type_value, title, description] if part).lower()
    property_type = _map_property_type(property_type_value) or detect_property_type(text_seed)

    raw_item = {
        "source_name": "Biddit",
        "source_listing_id": listing_id,
        "source_url": absolute_url,
        "title": title,
        "description": description,
        "price": price,
        "postal_code": postal_code,
        "commune": commune,
        "property_type": property_type,
        "transaction_type": "sale",
        "existing_units": existing_units,
        "surface": surface,
        "is_active": True,
        "notes": "Collecte Biddit browser page",
    }
    return normalize_feed_listing(raw_item, default_source_name="Biddit")


def _build_listing_from_biddit_search_hit(candidate: dict[object, object]) -> dict[str, object] | None:
    inner = candidate.get("content") if isinstance(candidate.get("content"), dict) else None
    if not isinstance(inner, dict):
        return None

    properties = inner.get("properties")
    if not isinstance(properties, list) or not properties or not isinstance(properties[0], dict):
        return None

    property_item = properties[0]
    listing_id = _coerce_first_string(property_item.get("reference") or inner.get("reference"))
    if not listing_id or not listing_id.isdigit():
        return None

    absolute_url = f"{BIDDIT_BASE_URL}/fr/catalog/detail/{listing_id}"
    title = _pick_biddit_localized_text(property_item.get("title"))
    description = _pick_biddit_localized_text(inner.get("description")) or title
    address = property_item.get("address") if isinstance(property_item.get("address"), dict) else {}
    municipality = address.get("municipality") if isinstance(address.get("municipality"), dict) else {}
    postal_code = _coerce_first_string(address.get("postalCode"))
    commune = _pick_biddit_localized_text(municipality)
    price = _coerce_float(
        inner.get("currentPrice")
        or inner.get("sellingPrice")
        or inner.get("startingPrice")
        or inner.get("initialStartingPrice")
        or inner.get("amountBouquet")
    )
    surface = _coerce_float(
        property_item.get("livingSurfaceArea")
        or property_item.get("terrainSurface")
        or property_item.get("businessSurface")
        or property_item.get("garageSurface")
    )
    property_type_value = _coerce_first_string(
        property_item.get("propertySubtype") or property_item.get("propertyType")
    )
    text_seed = " ".join(part for part in [property_type_value, title, description] if part).lower()
    property_type = _map_property_type(property_type_value) or detect_property_type(text_seed)
    status_value = _normalize_ui_text(_coerce_first_string(inner.get("publicSaleStatus")) or "")
    is_active = not bool(inner.get("withdrawn")) and status_value not in {"withdrawn", "closed", "sold", "expired", "cancelled"}

    raw_item = {
        "source_name": "Biddit",
        "source_listing_id": listing_id,
        "source_url": absolute_url,
        "title": title,
        "description": description,
        "price": price,
        "postal_code": postal_code,
        "commune": commune,
        "property_type": property_type,
        "transaction_type": "sale",
        "existing_units": None,
        "surface": surface,
        "is_active": is_active,
        "notes": "Collecte Biddit browser api",
    }
    return normalize_feed_listing(raw_item, default_source_name="Biddit")


def _pick_biddit_localized_text(value: object) -> str | None:
    if isinstance(value, dict):
        for key in ("fr", "nl", "en", "de"):
            rendered = _coerce_first_string(value.get(key))
            if rendered:
                return rendered
        return None
    return _coerce_first_string(value)


def _coerce_listing_url(candidate: dict[object, object]) -> str | None:
    for key in ("url", "href", "link", "permalink", "@id"):
        value = _coerce_first_string(candidate.get(key))
        if not value:
            continue
        absolute_url = urljoin(BIDDIT_BASE_URL, value)
        if _extract_listing_id(absolute_url):
            return absolute_url
    return None



def _map_property_type(value: str | None) -> str | None:
    normalized = (value or "").lower()
    if not normalized:
        return None
    if any(
        token in normalized
        for token in (
            "maison de commerce",
            "commerce house",
            "handelshuis",
            "woning met handelszaak",
            "immeuble mixte",
            "mixed use building",
        )
    ):
        return "commercial_house"
    if any(token in normalized for token in ("immeuble", "rapport", "building")):
        return "apartment_block"
    if any(
        token in normalized
        for token in (
            "commerce",
            "commercial",
            "shop",
            "retail",
            "horeca",
            "winkel",
            "handelszaak",
        )
    ):
        return "commercial"
    if any(token in normalized for token in ("maison", "house", "woning", "huis")):
        return "house"
    if any(token in normalized for token in ("appartement", "apartment", "flat")):
        return "apartment"
    if any(token in normalized for token in ("terrain", "land", "grond")):
        return "land"
    if any(token in normalized for token in ("garage", "parking")):
        return "garage"
    return None



def _extract_items_from_html_sources(html_sources: list[str]) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for html in html_sources:
        if not html:
            continue
        items.extend(parse_biddit_search_results(html))
        items.extend(extract_biddit_embedded_listings(html))
    return _merge_items_by_listing_id(items)



def _iter_network_payload_bodies(network_payloads: list[object]):
    for entry in network_payloads:
        if isinstance(entry, dict) and "_payload" in entry:
            yield entry["_payload"]
        else:
            yield entry



def _extract_reported_total_from_network_payloads(network_payloads: list[object]) -> int | None:
    totals: list[int] = []
    for payload in _iter_network_payload_bodies(network_payloads):
        for candidate in _walk_json_objects(payload):
            if not isinstance(candidate, dict):
                continue
            list_like = None
            for key in ("results", "items", "listings", "auctions", "properties", "content"):
                if isinstance(candidate.get(key), list):
                    list_like = candidate.get(key)
                    break
            if list_like is None:
                continue
            for key in ("total", "totalCount", "totalResults", "totalElements", "count"):
                value = candidate.get(key)
                if isinstance(value, int) and value >= len(list_like):
                    totals.append(value)
    return max(totals) if totals else None



def _extract_reported_total_from_html(html: str) -> int | None:
    if not html:
        return None
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    patterns = [
        r"(\d{1,4})\s+(?:resultats|results|resultaten)",
        r"(?:total|totaal)\s*[:\-]?\s*(\d{1,4})",
    ]
    totals: list[int] = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            try:
                totals.append(int(match.group(1)))
            except (TypeError, ValueError):
                continue
    return max(totals) if totals else None



def _record_biddit_page_snapshot(page, page_htmls: list[str], visited_page_urls: list[str]) -> str:
    html = _safe_page_content(page)
    if html and html not in page_htmls:
        page_htmls.append(html)
    current_url = page.url if page is not None else None
    if current_url and current_url not in visited_page_urls:
        visited_page_urls.append(current_url)
    return html



def _extract_pagination_urls_from_html(
    html: str,
    *,
    base_url: str,
    current_url: str,
) -> list[str]:
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    candidates: list[tuple[int, str]] = []
    seen_urls: set[str] = set()
    current_page_number = _extract_page_number(current_url) or 1
    current_path = urlparse(current_url).path

    for anchor in soup.find_all("a", href=True):
        resolved = urljoin(base_url, anchor.get("href", "").strip())
        if not resolved or resolved == current_url or resolved in seen_urls:
            continue
        seen_urls.add(resolved)
        if _extract_listing_id(resolved):
            continue

        parsed = urlparse(resolved)
        if parsed.netloc and "biddit.be" not in parsed.netloc.lower():
            continue

        link_text = _normalize_ui_text(anchor.get_text(" ", strip=True))
        rel_values = [str(value).lower() for value in (anchor.get("rel") or [])]
        attrs_blob = _normalize_ui_text(
            " ".join(
                str(anchor.get(name) or "")
                for name in ("class", "id", "aria-label", "data-testid")
            )
        )
        combined = f"{link_text} {' '.join(rel_values)} {attrs_blob}".strip()
        page_number = _extract_page_number(resolved)
        score = 0

        if any(value == "next" for value in rel_values):
            score += 100
        if any(token in combined for token in NEXT_PAGE_TEXT_TOKENS):
            score += 80
        if "pagination" in combined or "pager" in combined:
            score += 25
        if page_number is not None:
            if page_number <= current_page_number:
                continue
            score += max(15, 50 - (page_number - current_page_number))
        if "/search" in parsed.path.lower() or parsed.path.lower() == current_path.lower():
            score += 20
        if "page=" in parsed.query.lower() or "offset=" in parsed.query.lower():
            score += 20

        if score > 0:
            candidates.append((score, resolved))

    candidates.sort(key=lambda item: (-item[0], item[1]))
    return [url for _, url in candidates]



def _pick_next_pagination_url(
    html: str,
    *,
    base_url: str,
    current_url: str,
    already_visited: list[str],
) -> str | None:
    for candidate in _extract_pagination_urls_from_html(
        html,
        base_url=base_url,
        current_url=current_url,
    ):
        if candidate not in already_visited:
            return candidate
    return None



def _extract_page_number(url: str) -> int | None:
    parsed = urlparse(url)
    query = parsed.query or ""
    for key in ("page", "p"):
        match = re.search(rf"(?:^|[?&]){key}=(\d+)(?:$|&)", query)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
    return None



def _count_biddit_items_in_html(html: str) -> int:
    if not html:
        return 0
    return len(parse_biddit_search_results(html))



def _merge_unique_strings(current_values: list[str], new_values: list[str]) -> list[str]:
    merged = list(current_values)
    for value in new_values:
        if value not in merged:
            merged.append(value)
    return merged



def _coalesce_max_int(*values: int | None) -> int | None:
    valid_values = [value for value in values if value is not None]
    return max(valid_values) if valid_values else None



def _normalize_ui_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())



def _extract_listing_id(url: str) -> str | None:
    parsed = urlparse(url)
    if parsed.netloc and "biddit.be" not in parsed.netloc:
        return None
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2:
        return None
    candidate = parts[-1]
    return candidate if candidate.isdigit() else None



def _merge_items_by_listing_id(items: list[dict[str, object]]) -> list[dict[str, object]]:
    merged: dict[str, dict[str, object]] = {}
    for item in items:
        listing_id = str(item.get("source_listing_id") or "")
        if not listing_id:
            continue
        if listing_id in merged:
            merged[listing_id] = _prefer_richer_item(merged[listing_id], item)
        else:
            merged[listing_id] = dict(item)
    return list(merged.values())



def _prefer_richer_item(current: dict[str, object], candidate: dict[str, object]) -> dict[str, object]:
    merged = dict(current)
    for field_name, candidate_value in candidate.items():
        if _should_replace_biddit_field(field_name, merged, candidate, candidate_value):
            merged[field_name] = candidate_value

    current_notes = str(current.get("notes") or "")
    candidate_notes = str(candidate.get("notes") or "")
    if candidate_notes == "Collecte Biddit browser api" and current_notes != candidate_notes:
        for field_name in ("existing_units", "surface"):
            if not _has_meaningful_biddit_value(candidate.get(field_name)):
                merged[field_name] = candidate.get(field_name)
    return merged


def _should_replace_biddit_field(
    field_name: str,
    current_item: dict[str, object],
    candidate_item: dict[str, object],
    candidate_value: object,
) -> bool:
    current_value = current_item.get(field_name)
    if not _has_meaningful_biddit_value(candidate_value):
        return False
    if not _has_meaningful_biddit_value(current_value):
        return True

    current_notes = str(current_item.get("notes") or "")
    candidate_notes = str(candidate_item.get("notes") or "")
    api_preferred_fields = {
        "title",
        "description",
        "postal_code",
        "commune",
        "price",
        "property_type",
        "surface",
        "is_active",
        "notes",
    }
    locked_fields = {
        "source_name",
        "source_listing_id",
        "source_url",
        "title",
        "description",
        "postal_code",
        "commune",
        "property_type",
        "price",
        "existing_units",
        "surface",
        "notes",
        "is_active",
    }
    if (
        field_name in api_preferred_fields
        and candidate_notes == "Collecte Biddit browser api"
        and current_notes != "Collecte Biddit browser api"
    ):
        return True
    if field_name in locked_fields:
        return False
    return False


def _has_meaningful_biddit_value(value: object) -> bool:
    if value is None:
        return False
    if value is False:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True



def _coerce_first_string(value: object) -> str | None:
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, list):
        for item in value:
            result = _coerce_first_string(item)
            if result:
                return result
    return None



def _coerce_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        digits = "".join(ch for ch in value if ch.isdigit() or ch in ".,")
        if not digits:
            return None
        try:
            return float(digits.replace(",", "."))
        except ValueError:
            return None
    return None



def _coerce_int(value: object) -> int | None:
    number = _coerce_float(value)
    return int(number) if number is not None else None



def _best_available_html(page, response_htmls: list[str]) -> str:
    page_html = _safe_page_content(page)
    if page_html.strip():
        return page_html
    for html in response_htmls:
        if html.strip():
            return html
    return ""



def _safe_page_content(page) -> str:
    if page is None:
        return ""
    try:
        return page.content()
    except Exception:
        return ""



def _safe_page_title(page) -> str | None:
    if page is None:
        return None
    try:
        return page.title()
    except Exception:
        return None



def _safe_page_text_excerpt(page) -> str | None:
    if page is None:
        return None
    try:
        text = page.locator("body").inner_text(timeout=2000)
    except Exception:
        return None
    normalized = re.sub(r"\s+", " ", text).strip()
    return normalized[:300] if normalized else None



def _safe_page_screenshot(page) -> bytes | None:
    if page is None:
        return None
    try:
        return page.screenshot(full_page=True, timeout=5000)
    except Exception:
        return None



def _looks_like_antibot(normalized_html: str, title: str, final_url: str, body_excerpt: str) -> bool:
    lowered_title = title.lower()
    lowered_url = final_url.lower()
    return any(
        token in normalized_html or token in lowered_title or token in lowered_url or token in body_excerpt
        for token in [
            "captcha",
            "access denied",
            "forbidden",
            "verify you are human",
            "robot",
            "bot detection",
            "blocked",
            "challenge",
        ]
    )



def _persist_debug_artifacts(
    *,
    result: BidditBrowserRenderResult,
    debug_dir: str | Path | None,
    debug_save_html: bool,
    debug_screenshot: bool,
) -> list[Path]:
    if not debug_save_html and not debug_screenshot:
        return []

    target_dir = Path(debug_dir) if debug_dir is not None else DEFAULT_DEBUG_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    paths: list[Path] = []

    if debug_save_html:
        html_path = target_dir / f"biddit_failure_{stamp}.html"
        html_path.write_text(_build_debug_html_artifact(result), encoding="utf-8")
        paths.append(html_path)

    if debug_screenshot and result.screenshot_bytes:
        screenshot_path = target_dir / f"biddit_failure_{stamp}.png"
        screenshot_path.write_bytes(result.screenshot_bytes)
        paths.append(screenshot_path)

    return paths



def _build_debug_html_artifact(result: BidditBrowserRenderResult) -> str:
    html = result.html.strip()
    if html:
        return html
    for response_html in result.response_htmls:
        if response_html.strip():
            return response_html
    return (
        "<!doctype html><html><head><meta charset='utf-8'><title>Biddit debug</title></head>"
        "<body>"
        f"<h1>Biddit debug capture</h1><p>URL: {escape(result.final_url)}</p>"
        f"<p>Title: {escape(result.page_title or '')}</p>"
        f"<p>Excerpt: {escape(result.body_text_excerpt or '')}</p>"
        f"<p>Selector detected: {escape(result.detected_content_selector or '')}</p>"
        f"<p>Navigation timed out: {escape(str(result.navigation_timed_out))}</p>"
        f"<p>Network payloads captured: {escape(str(len(result.network_payloads)))}</p>"
        f"<p>HTML responses captured: {escape(str(len(result.response_htmls)))}</p>"
        "</body></html>"
    )



def _format_artifact_suffix(paths: list[Path]) -> str:
    if not paths:
        return ""
    rendered = ", ".join(str(path) for path in paths)
    return f" Artefacts enregistres: {rendered}"



def _is_timeout_exception(exc: Exception, playwright_timeout_error) -> bool:
    return exc.__class__.__name__ == getattr(playwright_timeout_error, "__name__", "") or "timeout" in str(exc).lower()



















