import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from normalization import normalize_feed_listing
from sources.common import (
    DEFAULT_SOURCE_HEADERS,
    detect_property_type,
    extract_absolute_url,
    extract_existing_units,
    fetch_html_with_requests,
    load_source_html,
    write_listings_jsonl,
)

IMMOVLAN_BASE_URL = "https://immovlan.be"
IMMOVLAN_HEADERS = dict(DEFAULT_SOURCE_HEADERS)
IMMOVLAN_HEADERS["Referer"] = IMMOVLAN_BASE_URL + "/fr/immobilier?transactiontypes=a-vendre"
DEFAULT_MAX_IMMOVLAN_PAGES = 4
IMMOVLAN_TOTAL_RESULTS_RE = re.compile(r"([\d\s\u00a0]+)\s*r[ée]sultats", re.IGNORECASE)
IMMOVLAN_PRICE_RE = re.compile(r"([\d\s\u00a0.]+)\s*€")
IMMOVLAN_SURFACE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*m²", re.IGNORECASE)


class ImmovlanFetchError(RuntimeError):
    pass


@dataclass(frozen=True)
class ImmovlanCollectionResult:
    items: list[dict[str, Any]]
    final_url: str
    visited_page_urls: list[str] = field(default_factory=list)
    pagination_urls_detected: list[str] = field(default_factory=list)
    followed_pagination_urls: list[str] = field(default_factory=list)
    detail_urls_followed: list[str] = field(default_factory=list)
    detail_enriched_count: int = 0
    ignored_project_urls: list[str] = field(default_factory=list)
    reported_total_results: int | None = None
    coverage_notes: list[str] = field(default_factory=list)


def fetch_immovlan_search_page(search_url: str, timeout: int = 30) -> str:
    return fetch_html_with_requests(
        search_url,
        headers=IMMOVLAN_HEADERS,
        timeout=timeout,
        error_cls=ImmovlanFetchError,
        source_label="Immovlan",
    )


def collect_immovlan_listings(
    *,
    search_url: str | None = None,
    html: str | None = None,
    html_file: str | Path | None = None,
    timeout: int = 30,
    max_pages: int = DEFAULT_MAX_IMMOVLAN_PAGES,
) -> list[dict[str, Any]]:
    return collect_immovlan_listing_result(
        search_url=search_url,
        html=html,
        html_file=html_file,
        timeout=timeout,
        max_pages=max_pages,
    ).items


def collect_immovlan_listing_result(
    *,
    search_url: str | None = None,
    html: str | None = None,
    html_file: str | Path | None = None,
    timeout: int = 30,
    max_pages: int = DEFAULT_MAX_IMMOVLAN_PAGES,
) -> ImmovlanCollectionResult:
    if html is not None or html_file is not None:
        loaded_html = load_source_html(
            search_url=None,
            html=html,
            html_file=html_file,
            fetch_func=lambda url: fetch_immovlan_search_page(url, timeout=timeout),
        )
        items = parse_immovlan_search_results(loaded_html)
        return ImmovlanCollectionResult(
            items=[{**item, "data_origin": "fixture"} for item in items],
            final_url=str(search_url or html_file or (IMMOVLAN_BASE_URL + "/fr/immobilier?transactiontypes=a-vendre")),
            ignored_project_urls=_extract_immovlan_project_urls(loaded_html),
            reported_total_results=_extract_immovlan_total_results(loaded_html),
        )

    if not search_url:
        raise ValueError("Fournis search_url, html ou html_file.")

    return _collect_immovlan_live_listing_result(
        search_url=search_url,
        timeout=timeout,
        max_pages=max_pages,
    )


def parse_immovlan_search_results(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for card in soup.select(".list-view-item.card.card-border"):
        anchor = _find_immovlan_detail_anchor(card)
        if anchor is None:
            continue

        absolute_url = extract_absolute_url(IMMOVLAN_BASE_URL, anchor.get("href", "").strip())
        listing_id = _extract_immovlan_listing_id(absolute_url)
        if not listing_id or listing_id in seen_ids:
            continue

        price = _extract_immovlan_card_price(card)
        postal_code, commune = _extract_immovlan_card_location(card)
        description = _extract_immovlan_card_description(card)
        title = _extract_immovlan_card_title(card, anchor, postal_code, commune)
        property_type = _extract_immovlan_property_type(absolute_url, title, description)
        existing_units = _extract_immovlan_card_units(card, description)
        surface = _extract_immovlan_card_surface(card, description)
        text_seed = " ".join(part for part in [title, description, commune, property_type] if part)

        raw_item = {
            "source_name": "Immovlan",
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
            "copro_status": _detect_immovlan_copro_status(text_seed),
            "is_active": True,
            "notes": "Collecte Immovlan page",
        }
        items.append(normalize_feed_listing(raw_item, default_source_name="Immovlan"))
        seen_ids.add(listing_id)

    return items


def diagnose_immovlan_empty_results(html: str) -> str:
    normalized = html.lower()
    if "captcha" in normalized or "access denied" in normalized or "forbidden" in normalized:
        return "Le HTML recupere ressemble a une page anti-bot ou d'acces refuse."
    project_urls = _extract_immovlan_project_urls(html)
    if project_urls and not parse_immovlan_search_results(html):
        return "Le HTML recupere contient surtout des pages projet Immovlan, sans annonces detail exploitables cote connecteur."
    if "/fr/detail/" not in normalized and "/nl/detail/" not in normalized:
        return "Le HTML recupere ne contient aucune URL de detail Immovlan attendue."
    return "Le HTML a ete charge mais aucun bloc d'annonce Immovlan exploitable n'a ete extrait."


def format_immovlan_coverage_summary(result: ImmovlanCollectionResult) -> str:
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
    if result.ignored_project_urls:
        parts.append(f"projets ignores: {len(result.ignored_project_urls)}")
    if result.reported_total_results is not None:
        parts.append(f"total detecte: {result.reported_total_results}")
        parts.append(f"couverture: {len(result.items)}/{result.reported_total_results}")
    if result.coverage_notes:
        parts.append("notes: " + "; ".join(result.coverage_notes[:4]))
    return " | ".join(parts)


def _collect_immovlan_live_listing_result(
    *,
    search_url: str,
    timeout: int,
    max_pages: int,
) -> ImmovlanCollectionResult:
    items_by_id: dict[str, dict[str, Any]] = {}
    visited_page_urls: list[str] = []
    pagination_urls_detected: list[str] = []
    followed_pagination_urls: list[str] = []
    ignored_project_urls: list[str] = []
    coverage_notes: list[str] = []
    reported_total_results: int | None = None

    next_url: str | None = search_url
    pages_remaining = max(1, max_pages)

    while next_url and pages_remaining > 0 and next_url not in visited_page_urls:
        current_url = next_url
        current_html = fetch_immovlan_search_page(current_url, timeout=timeout)
        current_items = parse_immovlan_search_results(current_html)
        if not current_items and not visited_page_urls:
            raise ImmovlanFetchError(
                f"Aucune annonce extraite depuis {current_url}. {diagnose_immovlan_empty_results(current_html)}"
            )

        visited_page_urls.append(current_url)
        for item in current_items:
            listing_id = str(item.get("source_listing_id") or "").strip()
            if not listing_id or listing_id in items_by_id:
                continue
            items_by_id[listing_id] = {**item, "data_origin": "live"}

        reported_total_results = _coalesce_max_int(
            reported_total_results,
            _extract_immovlan_total_results(current_html),
        )
        pagination_urls_detected = _merge_unique_strings(
            pagination_urls_detected,
            _extract_immovlan_pagination_urls(
                current_html,
                base_url=current_url,
                current_url=current_url,
            ),
        )
        ignored_project_urls = _merge_unique_strings(
            ignored_project_urls,
            _extract_immovlan_project_urls(current_html),
        )

        next_candidate = _pick_next_immovlan_pagination_url(
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
        coverage_notes.append("pagination HTML suivie depuis la page de recherche Immovlan")

    if ignored_project_urls:
        coverage_notes.append(f"pages projet ignorees: {len(ignored_project_urls)}")
    if next_url and next_url not in visited_page_urls and pages_remaining == 0:
        coverage_notes.append(f"plafond max-pages atteint ({max(1, max_pages)})")

    (
        enriched_items,
        detail_urls_followed,
        detail_enriched_count,
        detail_notes,
    ) = _enrich_immovlan_items_from_detail(
        list(items_by_id.values()),
        timeout=timeout,
    )
    coverage_notes.extend(detail_notes)

    return ImmovlanCollectionResult(
        items=enriched_items,
        final_url=visited_page_urls[-1] if visited_page_urls else search_url,
        visited_page_urls=visited_page_urls,
        pagination_urls_detected=pagination_urls_detected,
        followed_pagination_urls=followed_pagination_urls,
        detail_urls_followed=detail_urls_followed,
        detail_enriched_count=detail_enriched_count,
        ignored_project_urls=ignored_project_urls,
        reported_total_results=reported_total_results,
        coverage_notes=coverage_notes,
    )


def _find_immovlan_detail_anchor(card: Any) -> Any | None:
    for anchor in card.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        if "/detail/" in href and "/projectdetail/" not in href:
            return anchor
    return None


def _extract_immovlan_listing_id(url: str) -> str | None:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if "detail" not in parts:
        return None
    candidate = parts[-1]
    return candidate.upper() if candidate else None


def _extract_immovlan_card_title(card: Any, anchor: Any, postal_code: str | None, commune: str | None) -> str | None:
    heading = card.find(["h1", "h2", "h3"])
    if heading is not None:
        title = " ".join(heading.stripped_strings)
        if title and not title.lower().startswith("best of"):
            return title

    title = (anchor.get("title") or "").strip()
    if title:
        return title

    base_label = " ".join(part for part in [postal_code, commune] if part)
    text = card.get_text(" ", strip=True)
    match = re.search(r"([A-Za-zÀ-ſ' -]+\s+à\s+vendre)", text, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1).strip()} {base_label}".strip()
    return base_label or None


def _extract_immovlan_card_description(card: Any) -> str | None:
    node = card.select_one(".list-item-description [itemprop='description']") or card.select_one(".list-item-description")
    if node is None:
        return None
    description = " ".join(node.stripped_strings)
    description = description.replace("En savoir plus ?", "").strip()
    return description or None


def _extract_immovlan_card_price(card: Any) -> float | None:
    node = card.select_one(".list-item-price")
    text = node.get_text(" ", strip=True) if node is not None else card.get_text(" ", strip=True)
    match = IMMOVLAN_PRICE_RE.search(text)
    if not match:
        return None
    digits = re.sub(r"[^\d]", "", match.group(1))
    return float(digits) if digits else None


def _extract_immovlan_card_location(card: Any) -> tuple[str | None, str | None]:
    address = card.select_one("[itemprop='address']")
    if address is not None:
        postal_code_node = address.select_one("[itemprop='postalCode']")
        commune_node = address.select_one("[itemprop='addressLocality']")
        postal_code = postal_code_node.get_text(" ", strip=True) if postal_code_node is not None else None
        commune = commune_node.get_text(" ", strip=True) if commune_node is not None else None
        return postal_code or None, commune or None

    text = card.get_text(" ", strip=True)
    match = re.search(r"\b(\d{4})\s+([A-Za-zÀ-ſ' -]{2,})", text)
    if not match:
        return None, None
    return match.group(1), match.group(2).strip()


def _extract_immovlan_property_type(url: str, title: str | None, description: str | None) -> str | None:
    path_parts = [part for part in urlparse(url).path.split("/") if part]
    property_hint = path_parts[2] if len(path_parts) >= 3 else ""
    text_seed = " ".join(part for part in [property_hint, title, description] if part)
    return detect_property_type(text_seed)


def _extract_immovlan_card_units(card: Any, description: str | None) -> int | None:
    text_seed = " ".join(part for part in [card.get_text(" ", strip=True), description] if part)
    return extract_existing_units(text_seed)


def _extract_immovlan_card_surface(card: Any, description: str | None) -> float | None:
    text_seed = " ".join(part for part in [card.get_text(" ", strip=True), description] if part)
    match = IMMOVLAN_SURFACE_RE.search(text_seed)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", "."))
    except ValueError:
        return None


def _extract_immovlan_total_results(html: str) -> int | None:
    match = IMMOVLAN_TOTAL_RESULTS_RE.search(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))
    if not match:
        return None
    digits = re.sub(r"[^\d]", "", match.group(1))
    return int(digits) if digits else None


def _extract_immovlan_project_urls(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        if "/projectdetail/" not in href:
            continue
        absolute_url = extract_absolute_url(IMMOVLAN_BASE_URL, href)
        if absolute_url not in urls:
            urls.append(absolute_url)
    return urls


def _extract_immovlan_pagination_urls(
    html: str,
    *,
    base_url: str,
    current_url: str,
) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    current_page_number = _extract_page_number(current_url) or 1
    candidates: list[tuple[int, str]] = []
    seen_urls: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        resolved = urljoin(base_url, anchor.get("href", "").strip())
        if not resolved or resolved == current_url or resolved in seen_urls:
            continue
        seen_urls.add(resolved)
        if "/detail/" in resolved or "/projectdetail/" in resolved:
            continue

        parsed = urlparse(resolved)
        if parsed.netloc and "immovlan.be" not in parsed.netloc.lower():
            continue

        page_number = _extract_page_number(resolved)
        if page_number is None or page_number <= current_page_number:
            continue

        text = " ".join(anchor.stripped_strings).strip().lower()
        score = max(20, 60 - (page_number - current_page_number))
        if "suivant" in text or "next" in text or "volgende" in text:
            score += 40
        if "page=" in parsed.query.lower():
            score += 20
        candidates.append((score, resolved))

    candidates.sort(key=lambda item: (-item[0], item[1]))
    return [url for _, url in candidates]


def _pick_next_immovlan_pagination_url(
    html: str,
    *,
    base_url: str,
    current_url: str,
    already_visited: list[str],
) -> str | None:
    for candidate in _extract_immovlan_pagination_urls(
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


def _enrich_immovlan_items_from_detail(
    items: list[dict[str, Any]],
    *,
    timeout: int,
) -> tuple[list[dict[str, Any]], list[str], int, list[str]]:
    if not items:
        return [], [], 0, []

    enriched_items: list[dict[str, Any]] = []
    detail_urls_followed: list[str] = []
    detail_enriched_count = 0
    coverage_notes: list[str] = []

    for item in items:
        source_url = str(item.get("source_url") or "").strip()
        if not source_url:
            enriched_items.append(dict(item))
            continue

        detail_html = _fetch_immovlan_detail_html(source_url, timeout=timeout)
        if not detail_html:
            enriched_items.append(dict(item))
            continue

        detail_urls_followed.append(source_url)
        detail_item = _parse_immovlan_detail_item(detail_html, source_url=source_url)
        if detail_item is None:
            enriched_items.append(dict(item))
            continue

        merged_item = _merge_immovlan_listing_with_detail(dict(item), detail_item)
        if merged_item != item:
            detail_enriched_count += 1
        enriched_items.append(merged_item)

    if detail_urls_followed:
        coverage_notes.append(f"details Immovlan suivis: {len(detail_urls_followed)}")
    if detail_enriched_count:
        coverage_notes.append(f"details Immovlan enrichis: {detail_enriched_count}")

    return enriched_items, detail_urls_followed, detail_enriched_count, coverage_notes


def _fetch_immovlan_detail_html(source_url: str, *, timeout: int) -> str | None:
    try:
        return fetch_html_with_requests(
            source_url,
            headers=IMMOVLAN_HEADERS,
            timeout=timeout,
            error_cls=ImmovlanFetchError,
            source_label="Immovlan",
        )
    except ImmovlanFetchError:
        return None


def _parse_immovlan_detail_item(html: str, *, source_url: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(html, "html.parser")
    structured = _extract_immovlan_structured_data(soup)
    listing_id = _extract_immovlan_listing_id(source_url)
    if not listing_id:
        return None

    title = None
    heading = soup.find("h1")
    if heading is not None:
        title = " ".join(heading.stripped_strings) or None
    webpage = structured.get("WebPage")
    property_payload = structured.get("property")
    sell_action = structured.get("SellAction")

    title = title or _coerce_text(webpage.get("name") if isinstance(webpage, dict) else None)
    description = _coerce_text(property_payload.get("description") if isinstance(property_payload, dict) else None)
    if not description and isinstance(webpage, dict):
        description = _coerce_text(webpage.get("description"))
    price = _coerce_float(sell_action.get("price") if isinstance(sell_action, dict) else None)
    postal_code, commune = _extract_immovlan_detail_location(property_payload, sell_action)
    surface = _extract_immovlan_detail_surface(property_payload)
    text_seed = " ".join(part for part in [title, description] if part)
    property_type = _extract_immovlan_detail_property_type(source_url, property_payload, text_seed)
    existing_units = extract_existing_units(text_seed)

    raw_item = {
        "source_name": "Immovlan",
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
        "copro_status": _detect_immovlan_copro_status(text_seed),
        "is_active": True,
        "notes": "Collecte Immovlan detail jsonld",
    }
    return normalize_feed_listing(raw_item, default_source_name="Immovlan")


def _extract_immovlan_structured_data(soup: BeautifulSoup) -> dict[str, Any]:
    structured: dict[str, Any] = {}
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = (script.get_text() or "").strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            payload_type = str(payload.get("@type") or "").strip()
            if payload_type in {"House", "Apartment", "Residence", "SingleFamilyResidence"}:
                structured["property"] = payload
            else:
                structured[payload_type] = payload
    return structured


def _extract_immovlan_detail_location(
    property_payload: Any,
    sell_action: Any,
) -> tuple[str | None, str | None]:
    address = None
    if isinstance(property_payload, dict):
        address = property_payload.get("address")
    if not isinstance(address, dict) and isinstance(sell_action, dict):
        address = sell_action.get("location")
    if not isinstance(address, dict):
        return None, None
    postal_code = _coerce_text(address.get("postalCode"))
    commune = _coerce_text(address.get("addressLocality"))
    return postal_code, commune


def _extract_immovlan_detail_surface(property_payload: Any) -> float | None:
    if not isinstance(property_payload, dict):
        return None
    floor_size = property_payload.get("floorSize")
    if isinstance(floor_size, dict):
        return _coerce_float(floor_size.get("value"))
    return None


def _extract_immovlan_detail_property_type(
    source_url: str,
    property_payload: Any,
    text_seed: str,
) -> str | None:
    if isinstance(property_payload, dict):
        payload_type = _coerce_text(property_payload.get("@type"))
        if payload_type:
            mapped = _map_immovlan_schema_property_type(payload_type)
            detected = detect_property_type(text_seed)
            if detected in {"commercial_house", "apartment_block", "commercial"}:
                return detected
            if mapped:
                return mapped
    return detect_property_type(" ".join([source_url, text_seed]))


def _map_immovlan_schema_property_type(value: str | None) -> str | None:
    normalized = (value or "").strip().lower()
    if normalized in {"house", "singlefamilyresidence", "residence"}:
        return "house"
    if normalized == "apartment":
        return "apartment"
    if normalized == "landform":
        return "land"
    return None


def _merge_immovlan_listing_with_detail(
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
        "notes",
    }
    for field_name in preferred_fields:
        detail_value = detail_item.get(field_name)
        if detail_value not in (None, ""):
            merged[field_name] = detail_value

    merged["is_active"] = bool(detail_item.get("is_active", merged.get("is_active", True)))
    merged["is_copro"] = merged.get("copro_status") == "true"
    return merged


def _detect_immovlan_copro_status(text: str | None) -> str:
    normalized = re.sub(r"\s+", " ", (text or "")).strip().lower()
    if any(token in normalized for token in ("copropr", "co-propr", "mede-eigendom")):
        return "true"
    if any(token in normalized for token in ("sans copro", "hors copro", "pas de copro")):
        return "false"
    return "unknown"


def _coerce_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    rendered = re.sub(r"[^\d.,]", "", str(value))
    if not rendered:
        return None
    try:
        return float(rendered.replace(",", "."))
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
