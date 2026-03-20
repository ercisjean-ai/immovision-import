import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from html import escape
from pathlib import Path
from time import monotonic
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from normalization import normalize_feed_listing
from sources.immoweb_source import (
    IMMOWEB_BASE_URL,
    LISTING_ID_RE,
    LISTING_PATH_RE,
    ImmowebFetchError,
    diagnose_immoweb_empty_results,
    parse_immoweb_search_results,
)

IMMOWEB_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
IMMOWEB_RESULT_WAIT_SELECTORS = [
    "a[href^='/fr/annonce/']",
    "a[href^='/nl/annonce/']",
    "script#__NEXT_DATA__",
    "script[type='application/ld+json']",
]
IMMOWEB_COOKIE_SELECTORS = [
    "button:has-text('Tout accepter')",
    "button:has-text('Accepter')",
    "button:has-text('Accept all')",
    "#uc-btn-accept-banner",
]
NETWORK_JSON_HINTS = [
    "graphql",
    "api",
    "search",
    "classified",
    "listing",
    "result",
    "property",
]
DEFAULT_DEBUG_DIR = Path("debug") / "immoweb"
DEFAULT_SESSION_STATE_PATH = Path("sessions") / "immoweb_state.json"


@dataclass
class BrowserRenderResult:
    html: str
    final_url: str
    page_title: str | None = None
    network_payloads: list[object] = field(default_factory=list)
    response_htmls: list[str] = field(default_factory=list)
    cookie_banner_seen: bool = False
    screenshot_bytes: bytes | None = None
    body_text_excerpt: str | None = None
    home_navigation_timed_out: bool = False
    search_navigation_timed_out: bool = False
    detected_content_selector: str | None = None
    used_session_state_path: str | None = None
    saved_session_state_path: str | None = None
    manual_validation_prompted: bool = False



def collect_immoweb_browser_listings(
    search_url: str,
    *,
    timeout_ms: int = 45000,
    headless: bool = True,
    debug_save_html: bool = False,
    debug_screenshot: bool = False,
    debug_dir: str | Path | None = None,
    session_state_path: str | Path | None = None,
    save_session: bool = False,
    reuse_session: bool = False,
) -> list[dict[str, object]]:
    result = render_immoweb_search_page_with_playwright(
        search_url,
        timeout_ms=timeout_ms,
        headless=headless,
        capture_screenshot=debug_screenshot,
        debug_save_html=debug_save_html,
        debug_dir=debug_dir,
        session_state_path=session_state_path,
        save_session=save_session,
        reuse_session=reuse_session,
    )

    network_items = extract_immoweb_network_listings(result.network_payloads)
    response_html_items = _extract_items_from_html_sources(result.response_htmls)
    embedded_items = extract_immoweb_embedded_listings(result.html)
    html_items = parse_immoweb_search_results(result.html)
    items = _merge_items_by_listing_id(
        network_items + response_html_items + embedded_items + html_items
    )

    if not items:
        artifact_paths = _persist_debug_artifacts(
            result=result,
            debug_dir=debug_dir,
            debug_save_html=debug_save_html,
            debug_screenshot=debug_screenshot,
        )
        diagnostic = diagnose_immoweb_browser_failure(result)
        raise ImmowebFetchError(
            f"Aucune annonce extraite depuis {result.final_url}. {diagnostic}{_format_artifact_suffix(artifact_paths)}"
        )

    return [{**item, "data_origin": "live"} for item in items]



def render_immoweb_search_page_with_playwright(
    search_url: str,
    *,
    timeout_ms: int = 45000,
    headless: bool = True,
    capture_screenshot: bool = False,
    debug_save_html: bool = False,
    debug_dir: str | Path | None = None,
    session_state_path: str | Path | None = None,
    save_session: bool = False,
    reuse_session: bool = False,
) -> BrowserRenderResult:
    if save_session and headless:
        raise ImmowebFetchError(
            "Le mode de sauvegarde de session Playwright requiert --headed pour permettre une validation humaine."
        )

    sync_playwright, playwright_timeout_error = _load_playwright_sync_api()
    page = None
    browser = None
    network_payloads: list[object] = []
    response_htmls: list[str] = []
    cookie_banner_seen = False
    home_navigation_timed_out = False
    search_navigation_timed_out = False
    detected_content_selector: str | None = None
    resolved_session_state = _resolve_session_state_path(
        session_state_path=session_state_path,
        save_session=save_session,
        reuse_session=reuse_session,
    )
    used_session_state_path: str | None = None
    saved_session_state_path: str | None = None
    manual_validation_prompted = False

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=headless)
            context_kwargs = {
                "user_agent": IMMOWEB_BROWSER_USER_AGENT,
                "locale": "fr-BE",
                "viewport": {"width": 1440, "height": 2200},
                "ignore_https_errors": True,
                "extra_http_headers": {
                    "Accept-Language": "fr-BE,fr;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Upgrade-Insecure-Requests": "1",
                },
            }
            if reuse_session:
                if resolved_session_state is None or not resolved_session_state.exists():
                    raise ImmowebFetchError(
                        _build_missing_session_message(
                            resolved_session_state or DEFAULT_SESSION_STATE_PATH
                        )
                    )
                context_kwargs["storage_state"] = str(resolved_session_state)
                used_session_state_path = str(resolved_session_state)

            context = browser.new_context(**context_kwargs)
            page = context.new_page()
            page.on(
                "response",
                lambda response: _capture_network_artifacts(
                    response,
                    network_payloads,
                    response_htmls,
                ),
            )

            home_timeout_ms = min(12000, max(4000, timeout_ms // 4))
            try:
                page.goto(
                    IMMOWEB_BASE_URL,
                    wait_until="domcontentloaded",
                    timeout=home_timeout_ms,
                )
            except Exception as exc:
                home_navigation_timed_out = _is_timeout_exception(
                    exc,
                    playwright_timeout_error,
                )

            page.wait_for_timeout(800)
            cookie_banner_seen = _maybe_accept_cookie_banner(page) or cookie_banner_seen
            page.wait_for_timeout(600)

            search_timeout_ms = min(20000, timeout_ms)
            try:
                page.goto(
                    search_url,
                    wait_until="domcontentloaded",
                    timeout=search_timeout_ms,
                )
            except Exception as exc:
                search_navigation_timed_out = _is_timeout_exception(
                    exc,
                    playwright_timeout_error,
                )

            page.wait_for_timeout(900)
            cookie_banner_seen = _maybe_accept_cookie_banner(page) or cookie_banner_seen
            page.wait_for_timeout(600)

            detected_content_selector = _stabilize_immoweb_page(
                page,
                timeout_ms=max(1000, timeout_ms - home_timeout_ms),
            )

            if save_session:
                manual_validation_prompted = True
                _prompt_for_manual_session_validation(search_url)
                page.wait_for_timeout(1200)
                cookie_banner_seen = _maybe_accept_cookie_banner(page) or cookie_banner_seen
                page.wait_for_timeout(600)
                detected_content_selector = _stabilize_immoweb_page(
                    page,
                    timeout_ms=max(2000, min(10000, timeout_ms // 2)),
                ) or detected_content_selector

            html = _best_available_html(page, response_htmls)
            page_title = _safe_page_title(page)
            body_text_excerpt = _safe_page_text_excerpt(page)
            screenshot_bytes = _safe_page_screenshot(page) if capture_screenshot else None
            final_url = page.url if page is not None else search_url

            if save_session and resolved_session_state is not None and _should_persist_session_state(
                html=html,
                page_title=page_title,
                final_url=final_url,
                body_text_excerpt=body_text_excerpt,
            ):
                resolved_session_state.parent.mkdir(parents=True, exist_ok=True)
                context.storage_state(path=str(resolved_session_state))
                saved_session_state_path = str(resolved_session_state)

            browser.close()
            return BrowserRenderResult(
                html=html,
                final_url=final_url,
                page_title=page_title,
                network_payloads=network_payloads,
                response_htmls=response_htmls,
                cookie_banner_seen=cookie_banner_seen,
                screenshot_bytes=screenshot_bytes,
                body_text_excerpt=body_text_excerpt,
                home_navigation_timed_out=home_navigation_timed_out,
                search_navigation_timed_out=search_navigation_timed_out,
                detected_content_selector=detected_content_selector,
                used_session_state_path=used_session_state_path,
                saved_session_state_path=saved_session_state_path,
                manual_validation_prompted=manual_validation_prompted,
            )
    except ImmowebFetchError:
        raise
    except Exception as exc:
        result = BrowserRenderResult(
            html=_best_available_html(page, response_htmls),
            final_url=page.url if page is not None else search_url,
            page_title=_safe_page_title(page),
            network_payloads=network_payloads,
            response_htmls=response_htmls,
            cookie_banner_seen=cookie_banner_seen,
            screenshot_bytes=_safe_page_screenshot(page) if capture_screenshot else None,
            body_text_excerpt=_safe_page_text_excerpt(page),
            home_navigation_timed_out=home_navigation_timed_out,
            search_navigation_timed_out=search_navigation_timed_out,
            detected_content_selector=detected_content_selector,
            used_session_state_path=used_session_state_path,
            saved_session_state_path=saved_session_state_path,
            manual_validation_prompted=manual_validation_prompted,
        )
        artifact_paths = _persist_debug_artifacts(
            result=result,
            debug_dir=debug_dir,
            debug_save_html=debug_save_html,
            debug_screenshot=capture_screenshot,
        )
        diagnostic = diagnose_immoweb_browser_failure(result)
        message = str(exc)
        lowered = message.lower()
        if "executable doesn't exist" in lowered or "browser executable" in lowered:
            raise ImmowebFetchError(
                "Chromium Playwright n'est pas installe. Lance `python -m playwright install chromium`."
            ) from exc
        if _is_timeout_exception(exc, playwright_timeout_error):
            raise ImmowebFetchError(
                f"Timeout Playwright pendant la navigation de {search_url}. {diagnostic}{_format_artifact_suffix(artifact_paths)}"
            ) from exc
        raise ImmowebFetchError(
            f"Echec Playwright pendant la collecte Immoweb pour {search_url}: {message}. {diagnostic}{_format_artifact_suffix(artifact_paths)}"
        ) from exc
    finally:
        try:
            if browser is not None:
                browser.close()
        except Exception:
            pass



def extract_immoweb_embedded_listings(html: str) -> list[dict[str, object]]:
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



def extract_immoweb_network_listings(network_payloads: list[object]) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for payload in network_payloads:
        for candidate in _walk_json_objects(payload):
            item = _build_listing_from_json_candidate(candidate)
            if item is not None:
                items.append(item)
    return _merge_items_by_listing_id(items)



def diagnose_immoweb_browser_failure(result: BrowserRenderResult) -> str:
    html_sources = [result.html, *result.response_htmls]
    combined_html = "\n".join(source for source in html_sources if source)
    normalized_html = combined_html.lower()
    title = (result.page_title or "").strip()
    excerpt = (result.body_text_excerpt or "").strip()
    lowered_excerpt = excerpt.lower()
    parts: list[str] = []

    if result.final_url.lower() in {"about:blank", ""} and result.search_navigation_timed_out:
        parts.append("La navigation n'a pas atteint de page HTML exploitable avant timeout.")
    elif not combined_html.strip() and result.search_navigation_timed_out:
        parts.append("Timeout avant obtention d'un premier document HTML exploitable.")
    elif not combined_html.strip():
        parts.append("Page rendue vide apres navigation Playwright.")
    elif _looks_like_antibot(normalized_html, title, result.final_url, lowered_excerpt):
        parts.append("La page rendue ressemble a un challenge anti-bot ou a un acces refuse.")
        if result.used_session_state_path:
            parts.append(
                f"La session Playwright reutilisee ({result.used_session_state_path}) semble expiree ou invalidee; DataDome est reapparu."
            )
    elif result.cookie_banner_seen or _looks_like_consent_gate(normalized_html, title, lowered_excerpt):
        parts.append("Une banniere cookie/consentement ou un interstitiel semble encore bloquer le contenu utile.")
    elif result.network_payloads:
        parts.append(
            f"{len(result.network_payloads)} reponses JSON utiles ont ete capturees, mais aucune annonce n'a pu etre mappee proprement."
        )
    elif result.detected_content_selector is None and result.response_htmls:
        parts.append("Des reponses HTML ont ete chargees, mais aucun selecteur attendu n'a ete detecte dans le DOM rendu.")
    elif result.detected_content_selector is None:
        parts.append("Aucun selecteur attendu n'a ete detecte apres chargement; le markup live est probablement different ou incomplet.")
    elif "/fr/annonce/" in normalized_html or "/nl/annonce/" in normalized_html:
        parts.append("Des traces d'annonces existent dans le HTML, mais le parsing actuel n'a pas permis une extraction fiable.")
    else:
        parts.append(diagnose_immoweb_empty_results(combined_html))

    if title:
        parts.append(f"Titre page: {title}.")
    if excerpt:
        parts.append(f"Extrait page: {excerpt[:220]}.")
    if result.home_navigation_timed_out:
        parts.append("Navigation homepage initiale timeoutee.")
    if result.search_navigation_timed_out:
        parts.append("Navigation search cible timeoutee.")
    if result.detected_content_selector:
        parts.append(f"Selecteur detecte: {result.detected_content_selector}.")
    if result.saved_session_state_path:
        parts.append(f"Session Playwright sauvegardee: {result.saved_session_state_path}.")
    elif result.manual_validation_prompted and not result.saved_session_state_path:
        parts.append("La session Playwright n'a pas ete sauvegardee; relance en mode headed et valide completement la page avant d'appuyer sur Entree.")

    return " ".join(parts).strip()



def _load_playwright_sync_api():
    try:
        from playwright.sync_api import TimeoutError, sync_playwright
    except ImportError as exc:
        raise ImmowebFetchError(
            "Playwright n'est pas installe. Installe `playwright` puis lance `python -m playwright install chromium`."
        ) from exc
    return sync_playwright, TimeoutError



def _capture_network_artifacts(
    response,
    payloads: list[object],
    response_htmls: list[str],
) -> None:
    try:
        if response.status >= 400:
            return
        headers = {key.lower(): value for key, value in response.headers.items()}
        content_type = headers.get("content-type", "").lower()
        url = response.url.lower()
        text = response.text()
        if not text:
            return

        if "html" in content_type and len(response_htmls) < 5:
            response_htmls.append(text)

        if len(payloads) >= 20:
            return
        if "json" not in content_type and not any(token in url for token in NETWORK_JSON_HINTS):
            return
        payload = _load_json_text(text)
        if payload is None:
            return
        payloads.append(payload)
    except Exception:
        return



def _stabilize_immoweb_page(page, timeout_ms: int) -> str | None:
    deadline = monotonic() + (timeout_ms / 1000)

    selector = _wait_for_any_selector(page, 3000)
    if selector:
        return selector

    _try_wait_for_state(page, "networkidle", 2500)
    selector = _wait_for_any_selector(page, 1800)
    if selector:
        return selector

    _maybe_accept_cookie_banner(page)
    page.wait_for_timeout(600)
    selector = _wait_for_any_selector(page, 1800)
    if selector:
        return selector

    _scroll_page(page)
    remaining_ms = max(800, int((deadline - monotonic()) * 1000))
    selector = _wait_for_any_selector(page, min(remaining_ms, 3500))
    page.wait_for_timeout(800)
    return selector



def _wait_for_any_selector(page, timeout_ms: int) -> str | None:
    for selector in IMMOWEB_RESULT_WAIT_SELECTORS:
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



def _maybe_accept_cookie_banner(page) -> bool:
    for selector in IMMOWEB_COOKIE_SELECTORS:
        try:
            locator = page.locator(selector).first
            locator.wait_for(state="visible", timeout=1500)
            locator.click(timeout=1500)
            return True
        except Exception:
            continue
    return False



def _scroll_page(page) -> None:
    try:
        page.mouse.wheel(0, 1800)
        page.wait_for_timeout(400)
        page.mouse.wheel(0, 1800)
        page.wait_for_timeout(400)
    except Exception:
        return



def _resolve_session_state_path(
    *,
    session_state_path: str | Path | None,
    save_session: bool,
    reuse_session: bool,
) -> Path | None:
    if not save_session and not reuse_session and session_state_path is None:
        return None
    candidate = Path(session_state_path) if session_state_path is not None else DEFAULT_SESSION_STATE_PATH
    return candidate



def _build_missing_session_message(session_state_path: Path) -> str:
    return (
        f"Session Playwright introuvable: {session_state_path}. "
        "Initialise-la d'abord avec `python fetch_immoweb.py --search-url \"...\" --headed --save-session` "
        f"et, si besoin, `--session-state {session_state_path}`."
    )



def _prompt_for_manual_session_validation(search_url: str) -> None:
    print("Session Playwright ouverte pour Immoweb.")
    print(f"Navigue si necessaire et valide humainement la page: {search_url}")
    print("Si DataDome ou une banniere cookie apparait, valide-la dans Chromium.")
    print("Quand la page de resultats Immoweb est visible, reviens ici et appuie sur Entree pour enregistrer la session.")
    try:
        input("Appuie sur Entree quand la session Immoweb est prete... ")
    except EOFError as exc:
        raise ImmowebFetchError(
            "Le mode --save-session requiert un terminal interactif pour confirmer la validation humaine."
        ) from exc



def _should_persist_session_state(
    *,
    html: str,
    page_title: str | None,
    final_url: str,
    body_text_excerpt: str | None,
) -> bool:
    return not _looks_like_antibot(
        html.lower(),
        page_title or "",
        final_url,
        (body_text_excerpt or "").lower(),
    )



def _extract_json_payload(script) -> object | None:
    script_type = (script.get("type") or "").strip().lower()
    if script_type not in {"application/ld+json", "application/json", ""}:
        return None

    raw_text = script.string or script.get_text(strip=False)
    if not raw_text:
        return None

    return _load_json_text(raw_text.strip())



def _load_json_text(text: str) -> object | None:
    if not text:
        return None

    stripped = text.strip()
    if not stripped:
        return None

    if stripped[:1] in "[{":
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return None

    prefix = "window.__NEXT_DATA__ ="
    if stripped.startswith(prefix):
        payload_text = stripped[len(prefix):].strip().rstrip(";")
        try:
            return json.loads(payload_text)
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
    url_value = _coerce_first_string(candidate.get("url") or candidate.get("@id"))
    if not url_value:
        return None

    absolute_url = urljoin(IMMOWEB_BASE_URL, url_value)
    parsed_path = urlparse(absolute_url).path
    if not LISTING_PATH_RE.match(parsed_path):
        return None

    listing_id_match = LISTING_ID_RE.search(absolute_url)
    if not listing_id_match:
        return None

    offers = candidate.get("offers")
    if isinstance(offers, list) and offers:
        offers = offers[0]
    offers = offers if isinstance(offers, dict) else {}

    address = candidate.get("address")
    address = address if isinstance(address, dict) else {}

    floor_size = candidate.get("floorSize")
    floor_size = floor_size if isinstance(floor_size, dict) else {}

    title = _coerce_first_string(candidate.get("name") or candidate.get("title"))
    description = _coerce_first_string(candidate.get("description"))
    price = _coerce_float(candidate.get("price"))
    if price is None:
        price = _coerce_float(offers.get("price"))

    postal_code = _coerce_first_string(address.get("postalCode") or candidate.get("postalCode"))
    commune = _coerce_first_string(
        address.get("addressLocality")
        or candidate.get("city")
        or candidate.get("commune")
    )
    surface = _coerce_float(floor_size.get("value"))
    if surface is None:
        surface = _coerce_float(candidate.get("surface"))
    existing_units = _coerce_int(candidate.get("existing_units") or candidate.get("numberOfUnits"))

    text_seed = " ".join(
        part
        for part in [
            _coerce_first_string(candidate.get("@type")),
            title,
            description,
            _coerce_first_string(candidate.get("category")),
        ]
        if part
    ).lower()

    property_type = None
    if any(
        token in text_seed
        for token in (
            "maison de commerce",
            "commerce house",
            "handelshuis",
            "woning met handelszaak",
            "immeuble mixte",
            "mixed use building",
        )
    ):
        property_type = "commercial_house"
    elif "immeuble de rapport" in text_seed or "building" in text_seed:
        property_type = "apartment_block"
    elif any(
        token in text_seed
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
        property_type = "commercial"
    elif "maison" in text_seed or "house" in text_seed:
        property_type = "house"
    elif "appartement" in text_seed or "apartment" in text_seed or "flat" in text_seed:
        property_type = "apartment"

    transaction_type = "rent" if any(token in absolute_url.lower() for token in ("a-louer", "for-rent")) else "sale"

    raw_item = {
        "source_name": "Immoweb",
        "source_listing_id": listing_id_match.group(1),
        "source_url": absolute_url,
        "title": title,
        "description": description,
        "price": price,
        "postal_code": postal_code,
        "commune": commune,
        "property_type": property_type,
        "transaction_type": transaction_type,
        "existing_units": existing_units,
        "surface": surface,
        "is_active": True,
        "notes": "Collecte Immoweb browser page",
    }
    return normalize_feed_listing(raw_item, default_source_name="Immoweb")



def _extract_items_from_html_sources(html_sources: list[str]) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for html in html_sources:
        if not html:
            continue
        items.extend(extract_immoweb_embedded_listings(html))
        items.extend(parse_immoweb_search_results(html))
    return _merge_items_by_listing_id(items)



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
    current_score = sum(1 for value in current.values() if value not in (None, "", False))
    candidate_score = sum(1 for value in candidate.values() if value not in (None, "", False))
    return dict(candidate if candidate_score >= current_score else current)



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



def _looks_like_antibot(
    normalized_html: str,
    title: str,
    final_url: str,
    body_excerpt: str,
) -> bool:
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
            "cf-chl",
            "incident id",
            "datadome",
        ]
    )



def _looks_like_consent_gate(normalized_html: str, title: str, body_excerpt: str) -> bool:
    lowered_title = title.lower()
    return any(
        token in normalized_html or token in lowered_title or token in body_excerpt
        for token in [
            "cookie",
            "consent",
            "didomi",
            "onetrust",
            "tout accepter",
            "accept all",
            "privacy",
        ]
    )



def _persist_debug_artifacts(
    *,
    result: BrowserRenderResult,
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
        html_path = target_dir / f"immoweb_failure_{stamp}.html"
        html_path.write_text(_build_debug_html_artifact(result), encoding="utf-8")
        paths.append(html_path)

    if debug_screenshot and result.screenshot_bytes:
        screenshot_path = target_dir / f"immoweb_failure_{stamp}.png"
        screenshot_path.write_bytes(result.screenshot_bytes)
        paths.append(screenshot_path)

    return paths



def _build_debug_html_artifact(result: BrowserRenderResult) -> str:
    html = result.html.strip()
    if html:
        return html
    for response_html in result.response_htmls:
        if response_html.strip():
            return response_html
    return (
        "<!doctype html><html><head><meta charset='utf-8'><title>Immoweb debug</title></head>"
        "<body>"
        f"<h1>Immoweb debug capture</h1><p>URL: {escape(result.final_url)}</p>"
        f"<p>Title: {escape(result.page_title or '')}</p>"
        f"<p>Excerpt: {escape(result.body_text_excerpt or '')}</p>"
        f"<p>Cookie banner seen: {escape(str(result.cookie_banner_seen))}</p>"
        f"<p>Selector detected: {escape(result.detected_content_selector or '')}</p>"
        f"<p>Used session state: {escape(result.used_session_state_path or '')}</p>"
        f"<p>Saved session state: {escape(result.saved_session_state_path or '')}</p>"
        f"<p>Manual validation prompted: {escape(str(result.manual_validation_prompted))}</p>"
        f"<p>Home navigation timed out: {escape(str(result.home_navigation_timed_out))}</p>"
        f"<p>Search navigation timed out: {escape(str(result.search_navigation_timed_out))}</p>"
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

