import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from normalization import normalize_feed_listing
from sources.common import (
    DEFAULT_SOURCE_HEADERS,
    collect_text_parts,
    detect_property_type,
    extract_absolute_url,
    extract_commune,
    extract_description,
    extract_existing_units,
    extract_postal_code,
    extract_price,
    extract_surface,
    extract_title,
    fetch_html_with_requests,
    find_listing_card,
    load_source_html,
    write_listings_jsonl,
)

NOTAIRE_BASE_URL = "https://immo.notaire.be"
NOTAIRE_HEADERS = dict(DEFAULT_SOURCE_HEADERS)
NOTAIRE_HEADERS["Referer"] = NOTAIRE_BASE_URL + "/fr/biens-a-vendre"
DEFAULT_MAX_NOTAIRE_PAGES = 4


class NotaireFetchError(RuntimeError):
    pass


@dataclass(frozen=True)
class NotaireCollectionResult:
    items: list[dict[str, Any]]
    final_url: str
    visited_page_urls: list[str] = field(default_factory=list)
    pagination_urls_detected: list[str] = field(default_factory=list)
    followed_pagination_urls: list[str] = field(default_factory=list)
    detail_urls_followed: list[str] = field(default_factory=list)
    detail_enriched_count: int = 0
    reported_total_pages: int | None = None
    estimated_total_results: int | None = None
    coverage_notes: list[str] = field(default_factory=list)


def fetch_notaire_search_page(search_url: str, timeout: int = 30) -> str:
    return fetch_html_with_requests(
        search_url,
        headers=NOTAIRE_HEADERS,
        timeout=timeout,
        error_cls=NotaireFetchError,
        source_label="Notaire.be/immo",
    )


def collect_notaire_listings(
    *,
    search_url: str | None = None,
    html: str | None = None,
    html_file: str | Path | None = None,
    timeout: int = 30,
    max_pages: int = DEFAULT_MAX_NOTAIRE_PAGES,
) -> list[dict[str, Any]]:
    return collect_notaire_listing_result(
        search_url=search_url,
        html=html,
        html_file=html_file,
        timeout=timeout,
        max_pages=max_pages,
    ).items


def collect_notaire_listing_result(
    *,
    search_url: str | None = None,
    html: str | None = None,
    html_file: str | Path | None = None,
    timeout: int = 30,
    max_pages: int = DEFAULT_MAX_NOTAIRE_PAGES,
) -> NotaireCollectionResult:
    if html is not None or html_file is not None:
        loaded_html = load_source_html(
            search_url=None,
            html=html,
            html_file=html_file,
            fetch_func=lambda url: fetch_notaire_search_page(url, timeout=timeout),
        )
        items = parse_notaire_search_results(loaded_html)
        return NotaireCollectionResult(
            items=[{**item, "data_origin": "fixture"} for item in items],
            final_url=str(search_url or html_file or (NOTAIRE_BASE_URL + "/fr/biens-a-vendre")),
        )

    if not search_url:
        raise ValueError("Fournis search_url, html ou html_file.")

    return _collect_notaire_live_listing_result(
        search_url=search_url,
        timeout=timeout,
        max_pages=max_pages,
    )


def parse_notaire_search_results(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    embedded_items = _parse_notaire_embedded_json(soup)
    if embedded_items:
        return embedded_items

    items: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        absolute_url = extract_absolute_url(NOTAIRE_BASE_URL, href)
        listing_id = _extract_listing_id(absolute_url)
        if not listing_id or listing_id in seen_ids:
            continue

        card = find_listing_card(anchor)
        text_parts = collect_text_parts(card)
        text_blob = " ".join(text_parts)
        sale_mode = _extract_sale_mode(text_blob)
        raw_item = {
            "source_name": "Notaire.be",
            "source_listing_id": listing_id,
            "source_url": absolute_url,
            "title": extract_title(anchor, card),
            "description": extract_description(card),
            "price": extract_price(text_blob),
            "postal_code": extract_postal_code(text_blob, text_parts),
            "commune": extract_commune(text_blob, text_parts),
            "property_type": detect_property_type(text_blob),
            "transaction_type": "sale",
            "existing_units": extract_existing_units(text_blob),
            "surface": extract_surface(text_blob),
            "is_active": True,
            "notes": _build_notes(sale_mode),
        }
        items.append(normalize_feed_listing(raw_item, default_source_name="Notaire.be"))
        seen_ids.add(listing_id)

    return items


def diagnose_notaire_empty_results(html: str) -> str:
    normalized = html.lower()
    if "maintenance" in normalized or "temporairement indisponible" in normalized:
        return "Le site Notaire.be/immo semble en maintenance ou indisponible."
    if "estates_json" in normalized:
        return (
            "Le HTML contient un JSON embarque estates_json, mais aucune annonce exploitable "
            "n'a pu en etre extraite."
        )
    if "/property_details/" not in normalized and "/a-vendre/" not in normalized and "/biens-a-vendre" not in normalized:
        return "Le HTML recupere ne contient aucune URL d'annonce notariale attendue."
    return "Le HTML a ete charge mais aucun bloc d'annonce notariale exploitable n'a ete extrait."


def format_notaire_coverage_summary(result: NotaireCollectionResult) -> str:
    parts = [f"annonces uniques: {len(result.items)}"]
    if result.visited_page_urls:
        parts.append(f"pages visitees: {len(result.visited_page_urls)}")
    if result.pagination_urls_detected:
        parts.append(f"pagination detectee: {len(result.pagination_urls_detected)} URL(s)")
    else:
        parts.append("pagination detectee: non")
    if result.followed_pagination_urls:
        parts.append(f"pages supplementaires suivies: {len(result.followed_pagination_urls)}")
    if result.detail_urls_followed:
        parts.append(f"details suivis: {len(result.detail_urls_followed)}")
    if result.detail_enriched_count:
        parts.append(f"details enrichis: {result.detail_enriched_count}")
    if result.reported_total_pages is not None:
        parts.append(f"pages totales detectees: {result.reported_total_pages}")
    if result.estimated_total_results is not None:
        parts.append(f"couverture estimee: {len(result.items)}/{result.estimated_total_results}")
    if result.coverage_notes:
        parts.append("notes: " + "; ".join(result.coverage_notes[:3]))
    return " | ".join(parts)


def _extract_listing_id(url: str) -> str | None:
    path = urlparse(url).path
    parts = [part for part in path.split("/") if part]
    if "property_details" in parts:
        marker_index = parts.index("property_details")
        if marker_index + 1 >= len(parts):
            return None
        candidate = parts[marker_index + 1]
        return candidate if candidate.isdigit() else None
    if parts and parts[-1].isdigit():
        return parts[-1]
    return None


def _collect_notaire_live_listing_result(
    *,
    search_url: str,
    timeout: int,
    max_pages: int,
) -> NotaireCollectionResult:
    items_by_id: dict[str, dict[str, Any]] = {}
    visited_page_urls: list[str] = []
    pagination_urls_detected: list[str] = []
    followed_pagination_urls: list[str] = []
    coverage_notes: list[str] = []
    reported_total_pages: int | None = None
    estimated_total_results: int | None = None

    next_url: str | None = search_url
    pages_remaining = max(1, max_pages)

    while next_url and pages_remaining > 0 and next_url not in visited_page_urls:
        current_url = next_url
        current_html = fetch_notaire_search_page(current_url, timeout=timeout)
        current_items = parse_notaire_search_results(current_html)
        if not current_items and not visited_page_urls:
            raise NotaireFetchError(
                f"Aucune annonce extraite depuis {current_url}. {diagnose_notaire_empty_results(current_html)}"
            )

        visited_page_urls.append(current_url)
        for item in current_items:
            listing_id = str(item.get("source_listing_id") or "").strip()
            if not listing_id or listing_id in items_by_id:
                continue
            items_by_id[listing_id] = {**item, "data_origin": "live"}

        reported_total_pages = _coalesce_max_int(
            reported_total_pages,
            _extract_notaire_total_pages(current_html),
        )
        estimated_total_results = _coalesce_max_int(
            estimated_total_results,
            _estimate_notaire_total_results(
                total_pages=reported_total_pages,
                page_item_count=len(current_items),
            ),
        )
        pagination_urls_detected = _merge_unique_strings(
            pagination_urls_detected,
            _extract_notaire_pagination_urls(
                current_html,
                base_url=current_url,
                current_url=current_url,
            ),
        )

        next_candidate = _pick_next_notaire_pagination_url(
            current_html,
            base_url=current_url,
            current_url=current_url,
            already_visited=visited_page_urls,
        )
        if next_candidate and pages_remaining > 1:
            followed_pagination_urls.append(next_candidate)
        next_url = next_candidate
        pages_remaining -= 1

    if len(visited_page_urls) == 1 and not pagination_urls_detected:
        coverage_notes.append("aucune pagination HTML detectee sur la page courante")
    elif len(visited_page_urls) == 1 and pagination_urls_detected:
        coverage_notes.append("pagination detectee mais aucune page supplementaire n'a pu etre suivie")
    elif followed_pagination_urls:
        coverage_notes.append("pagination HTML suivie depuis la page de recherche Notaire")

    if next_url and next_url not in visited_page_urls and pages_remaining == 0:
        coverage_notes.append(f"plafond max-pages atteint ({max(1, max_pages)})")

    (
        enriched_items,
        detail_urls_followed,
        detail_enriched_count,
        detail_notes,
    ) = _enrich_notaire_items_from_detail(
        list(items_by_id.values()),
        timeout=timeout,
    )
    coverage_notes.extend(detail_notes)

    return NotaireCollectionResult(
        items=enriched_items,
        final_url=visited_page_urls[-1] if visited_page_urls else search_url,
        visited_page_urls=visited_page_urls,
        pagination_urls_detected=pagination_urls_detected,
        followed_pagination_urls=followed_pagination_urls,
        detail_urls_followed=detail_urls_followed,
        detail_enriched_count=detail_enriched_count,
        reported_total_pages=reported_total_pages,
        estimated_total_results=estimated_total_results,
        coverage_notes=coverage_notes,
    )



def _enrich_notaire_items_from_detail(
    items: list[dict[str, Any]],
    *,
    timeout: int,
) -> tuple[list[dict[str, Any]], list[str], int, list[str]]:
    if not items:
        return [], [], 0, []

    detail_urls_followed: list[str] = []
    detail_enriched_count = 0
    coverage_notes: list[str] = []
    enriched_by_index: dict[int, dict[str, Any]] = {}
    max_workers = min(8, len(items)) or 1

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {}
        for index, item in enumerate(items):
            source_url = str(item.get("source_url") or "").strip()
            if not source_url:
                enriched_by_index[index] = dict(item)
                continue
            future = executor.submit(_fetch_notaire_detail_html, None, source_url, timeout=timeout)
            future_map[future] = (index, dict(item), source_url)

        for future in as_completed(future_map):
            index, base_item, source_url = future_map[future]
            detail_html = future.result()
            if not detail_html:
                enriched_by_index[index] = base_item
                continue

            detail_urls_followed.append(source_url)
            detail_item = _parse_notaire_detail_item(detail_html, source_url=source_url)
            if detail_item is None:
                enriched_by_index[index] = base_item
                continue

            merged_item = _merge_notaire_listing_with_detail(base_item, detail_item)
            if merged_item != base_item:
                detail_enriched_count += 1
            enriched_by_index[index] = merged_item

    enriched_items = [enriched_by_index[index] for index in range(len(items))]

    if detail_urls_followed:
        coverage_notes.append(f"details Notaire suivis: {len(detail_urls_followed)}")
    if detail_enriched_count:
        coverage_notes.append(f"details Notaire enrichis: {detail_enriched_count}")

    return enriched_items, detail_urls_followed, detail_enriched_count, coverage_notes


def _fetch_notaire_detail_html(
    session_or_url: requests.Session | str | None,
    source_url: str | None = None,
    *,
    timeout: int,
) -> str | None:
    if source_url is None:
        source_url = str(session_or_url or "").strip()
    else:
        source_url = str(source_url or "").strip()
    if not source_url:
        return None
    try:
        return fetch_html_with_requests(
            source_url,
            headers=NOTAIRE_HEADERS,
            timeout=timeout,
            error_cls=NotaireFetchError,
            source_label="Notaire.be/immo",
        )
    except NotaireFetchError:
        return None

def _parse_notaire_detail_item(html: str, *, source_url: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="estate_json", attrs={"type": "application/json"})
    if script is not None:
        raw_payload = script.string or script.get_text(strip=True)
        if raw_payload:
            try:
                estate = json.loads(raw_payload)
            except json.JSONDecodeError:
                estate = None
            if isinstance(estate, dict):
                return _build_notaire_item_from_estate(
                    estate,
                    source_url=source_url,
                    note_prefix="Collecte Notaire detail",
                )

    title = None
    heading = soup.find("h1")
    if heading is not None:
        title = " ".join(heading.stripped_strings) or None
    description = None
    about_heading = soup.find(string=re.compile(r"A propos de ce bien|Over dit pand", re.IGNORECASE))
    if about_heading is not None and hasattr(about_heading, 'parent'):
        container = about_heading.parent
        description = " ".join(container.parent.stripped_strings) if getattr(container, 'parent', None) is not None else None

    text_seed = " ".join(part for part in [title, description, soup.get_text(" ", strip=True)] if part)
    listing_id = _extract_listing_id(source_url)
    if not listing_id:
        return None

    raw_item = {
        "source_name": "Notaire.be",
        "source_listing_id": listing_id,
        "source_url": source_url,
        "title": title,
        "description": description,
        "price": extract_price(text_seed),
        "postal_code": extract_postal_code(text_seed, [text_seed]),
        "commune": extract_commune(text_seed, [text_seed]),
        "property_type": detect_property_type(text_seed),
        "transaction_type": "sale",
        "existing_units": extract_existing_units(text_seed),
        "surface": extract_surface(text_seed),
        "copro_status": _detect_notaire_copro_status(text_seed),
        "is_active": True,
        "notes": "Collecte Notaire detail html",
    }
    return normalize_feed_listing(raw_item, default_source_name="Notaire.be")


def _merge_notaire_listing_with_detail(
    base_item: dict[str, Any],
    detail_item: dict[str, Any],
) -> dict[str, Any]:
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
        if detail_value not in (None, ""):
            merged[field_name] = detail_value

    merged["is_copro"] = merged.get("copro_status") == "true"
    return merged


def _extract_sale_mode(text: str) -> str | None:
    normalized = text.lower()
    if "vente online" in normalized or "vente en ligne" in normalized or "verkoop online" in normalized:
        return "vente online"
    if "gre a gre" in normalized or "de gre a gre" in normalized or "onderhandse" in normalized:
        return "vente de gre a gre"
    return None


def _build_notes(sale_mode: str | None, *, prefix: str = "Collecte Notaire immo page") -> str:
    if sale_mode:
        return f"{prefix} | mode: {sale_mode}"
    return prefix


def _parse_notaire_embedded_json(soup: BeautifulSoup) -> list[dict[str, Any]]:
    script = soup.find("script", id="estates_json", attrs={"type": "application/json"})
    if script is None:
        return []

    raw_payload = script.string or script.get_text(strip=True)
    if not raw_payload:
        return []

    try:
        estates = json.loads(raw_payload)
    except json.JSONDecodeError:
        return []

    if not isinstance(estates, list):
        return []

    href_map = _build_notaire_href_map(soup)
    items: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for estate in estates:
        if not isinstance(estate, dict):
            continue
        listing_id = str(estate.get("id") or "").strip()
        if not listing_id or listing_id in seen_ids:
            continue

        raw_item = _build_notaire_item_from_estate(estate, href_map)
        if raw_item is None:
            continue

        items.append(normalize_feed_listing(raw_item, default_source_name="Notaire.be"))
        seen_ids.add(listing_id)

    return items


def _build_notaire_href_map(soup: BeautifulSoup) -> dict[str, str]:
    href_map: dict[str, str] = {}
    for anchor in soup.find_all("a", href=True):
        absolute_url = extract_absolute_url(NOTAIRE_BASE_URL, anchor.get("href", "").strip())
        listing_id = _extract_listing_id(absolute_url)
        if not listing_id:
            continue

        current_best = href_map.get(listing_id)
        if current_best is None or _score_notaire_url(absolute_url) > _score_notaire_url(current_best):
            href_map[listing_id] = absolute_url
    return href_map


def _score_notaire_url(url: str) -> int:
    path = urlparse(url).path.lower()
    score = 0
    if "/a-vendre/" in path or "/te-koop/" in path:
        score += 20
    if "/property_details/" in path:
        score += 10
    score += len(path)
    return score


def _build_notaire_item_from_estate(
    estate: dict[str, Any],
    href_map: dict[str, str] | None = None,
    *,
    source_url: str | None = None,
    note_prefix: str = "Collecte Notaire immo page",
) -> dict[str, Any] | None:
    listing_id = str(estate.get("id") or "").strip()
    if not listing_id:
        return None

    resolved_href_map = href_map or {}
    source_url = source_url or resolved_href_map.get(listing_id)
    if not source_url:
        return None

    title = _pick_translated_value(estate.get("title")) or _pick_first_text(
        estate.get("title_Fr"),
        estate.get("title_Nl"),
        estate.get("title_En"),
        estate.get("title_De"),
    )
    description = _pick_translated_value(estate.get("desc")) or _pick_first_text(
        estate.get("desc_Fr"),
        estate.get("desc_Nl"),
        estate.get("desc_En"),
        estate.get("desc_De"),
    )

    municipality = estate.get("municipality")
    commune = _pick_translated_value(municipality) if isinstance(municipality, dict) else None
    zip_code = estate.get("zip")
    price = _pick_first_number(
        estate.get("price"),
        estate.get("insReserveBid"),
        estate.get("bundledPrice"),
        estate.get("lotPrice"),
    )

    text_seed = " ".join(
        str(part).strip()
        for part in (
            title,
            description,
            estate.get("propertyTypeTranslated"),
            estate.get("saleType"),
            estate.get("propertySubtype"),
            estate.get("propertyType"),
        )
        if part
    )
    sale_mode = _extract_sale_mode(text_seed)
    property_type = _detect_notaire_property_type(estate, text_seed)
    existing_units = _pick_first_int(
        estate.get("numberOfHousingUnits"),
        extract_existing_units(text_seed),
    )
    surface = _pick_first_number(
        estate.get("searchSurface"),
        estate.get("livingArea"),
        estate.get("terrainSurface"),
        extract_surface(text_seed),
    )

    return {
        "source_name": "Notaire.be",
        "source_listing_id": listing_id,
        "source_url": source_url,
        "title": title,
        "description": description,
        "price": price,
        "postal_code": str(zip_code) if zip_code not in (None, "") else None,
        "commune": commune,
        "property_type": property_type,
        "transaction_type": "sale",
        "existing_units": existing_units,
        "surface": surface,
        "copro_status": _detect_notaire_copro_status(text_seed),
        "is_active": _is_notaire_estate_active(estate),
        "notes": _build_notes(sale_mode, prefix=note_prefix),
    }


def _pick_translated_value(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    return _pick_first_text(
        value.get("title_Fr"),
        value.get("freeText_Fr"),
        value.get("municipality_Fr"),
        value.get("street_Fr"),
        value.get("title_Nl"),
        value.get("freeText_Nl"),
        value.get("municipality_Nl"),
        value.get("street_Nl"),
        value.get("title_En"),
        value.get("freeText_En"),
        value.get("municipality_En"),
        value.get("street_En"),
        value.get("title_De"),
        value.get("freeText_De"),
        value.get("municipality_De"),
        value.get("street_De"),
    )


def _pick_first_text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        rendered = str(value).strip()
        if rendered:
            return rendered
    return None


def _pick_first_number(*values: Any) -> float | None:
    for value in values:
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _pick_first_int(*values: Any) -> int | None:
    for value in values:
        if value in (None, ""):
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _detect_notaire_property_type(estate: dict[str, Any], text_seed: str) -> str | None:
    property_type = str(estate.get("propertyType") or "").strip().upper()
    mapped_type = None
    if property_type == "APARTMENT":
        mapped_type = "apartment"
    elif property_type == "HOUSE":
        mapped_type = "house"
    elif property_type == "LAND":
        mapped_type = "land"
    elif property_type in {"COMMERCIAL", "OFFICE"}:
        mapped_type = "commercial"
    elif property_type in {"GARAGE", "PARKING"}:
        mapped_type = "garage"

    detected = detect_property_type(text_seed)
    if detected in {"commercial_house", "apartment_block", "commercial"}:
        return detected
    if mapped_type:
        return mapped_type
    return detected


def _detect_notaire_copro_status(text_seed: str) -> str:
    normalized = text_seed.lower()
    if "copropr" in normalized or "co-propr" in normalized or "mede-eigendom" in normalized:
        return "true"
    return "unknown"


def _is_notaire_estate_active(estate: dict[str, Any]) -> bool:
    ad_status = str(estate.get("adStatus") or "").strip().upper()
    publication_status = estate.get("publicationStatus")
    return ad_status in {"ACTIVE", ""} and publication_status in (None, "", 1, "1")


def _extract_notaire_total_pages(html: str) -> int | None:
    if not html:
        return None
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    patterns = [
        r"page\s+\d+\s+sur\s+(\d+)",
        r"pagina\s+\d+\s+van\s+(\d+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
    return None


def _estimate_notaire_total_results(
    *,
    total_pages: int | None,
    page_item_count: int,
) -> int | None:
    if total_pages is None or total_pages <= 1 or page_item_count <= 0:
        return None
    return total_pages * page_item_count


def _extract_notaire_pagination_urls(
    html: str,
    *,
    base_url: str,
    current_url: str,
) -> list[str]:
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    current_page_number = _extract_page_number(current_url) or 1
    candidates: list[tuple[int, str]] = []
    seen_urls: set[str] = set()
    current_path = urlparse(current_url).path.lower().rstrip("/")

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        resolved = urljoin(base_url, href)
        if not resolved or resolved == current_url or resolved in seen_urls:
            continue
        seen_urls.add(resolved)
        if _extract_listing_id(resolved):
            continue

        parsed = urlparse(resolved)
        if parsed.netloc and "notaire.be" not in parsed.netloc.lower():
            continue

        text = " ".join(anchor.stripped_strings).strip().lower()
        page_number = _extract_page_number(resolved)
        score = 0

        if page_number is not None:
            if page_number <= current_page_number:
                continue
            score += max(20, 60 - (page_number - current_page_number))
        if "page suivante" in text or "suivante" in text or text == ">>":
            score += 80
        if "volgende" in text or "next" in text:
            score += 60
        if "page=" in parsed.query.lower():
            score += 25
        if parsed.path.lower().rstrip("/") == current_path or "/biens-a-vendre" in parsed.path.lower():
            score += 15

        if score > 0:
            candidates.append((score, resolved))

    candidates.sort(key=lambda item: (-item[0], item[1]))
    return [url for _, url in candidates]


def _pick_next_notaire_pagination_url(
    html: str,
    *,
    base_url: str,
    current_url: str,
    already_visited: list[str],
) -> str | None:
    for candidate in _extract_notaire_pagination_urls(
        html,
        base_url=base_url,
        current_url=current_url,
    ):
        if candidate not in already_visited:
            return candidate
    return None


def _extract_page_number(url: str) -> int | None:
    parsed = urlparse(url)
    match = re.search(r"(?:^|[?&])page=(\d+)(?:$|&)", parsed.query or "", flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _merge_unique_strings(current_values: list[str], new_values: list[str]) -> list[str]:
    merged = list(current_values)
    for value in new_values:
        if value not in merged:
            merged.append(value)
    return merged


def _coalesce_max_int(*values: int | None) -> int | None:
    valid_values = [value for value in values if value is not None]
    return max(valid_values) if valid_values else None



