import json
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



def collect_immoweb_browser_listings(
    search_url: str,
    *,
    timeout_ms: int = 45000,
    headless: bool = True,
) -> list[dict[str, object]]:
    html, final_url = render_immoweb_search_page_with_playwright(
        search_url,
        timeout_ms=timeout_ms,
        headless=headless,
    )

    embedded_items = extract_immoweb_embedded_listings(html)
    html_items = parse_immoweb_search_results(html)
    items = _merge_items_by_listing_id(embedded_items + html_items)

    if not items:
        raise ImmowebFetchError(
            f"Aucune annonce extraite depuis {final_url}. "
            f"{diagnose_immoweb_empty_results(html)}"
        )

    return items



def render_immoweb_search_page_with_playwright(
    search_url: str,
    *,
    timeout_ms: int = 45000,
    headless: bool = True,
) -> tuple[str, str]:
    sync_playwright, playwright_timeout_error = _load_playwright_sync_api()

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=headless)
            context = browser.new_context(
                user_agent=IMMOWEB_BROWSER_USER_AGENT,
                locale="fr-BE",
                viewport={"width": 1440, "height": 2200},
            )
            page = context.new_page()
            page.goto(search_url, wait_until="domcontentloaded", timeout=timeout_ms)
            _maybe_accept_cookie_banner(page)
            page.wait_for_load_state("load", timeout=timeout_ms)
            _wait_for_immoweb_content(page, timeout_ms=min(timeout_ms, 10000))
            page.wait_for_timeout(1200)
            html = page.content()
            final_url = page.url
            browser.close()
            return html, final_url
    except ImmowebFetchError:
        raise
    except Exception as exc:
        message = str(exc)
        lowered = message.lower()
        if "executable doesn't exist" in lowered or "browser executable" in lowered:
            raise ImmowebFetchError(
                "Chromium Playwright n'est pas installe. Lance `python -m playwright install chromium`."
            ) from exc
        if "timeout" in lowered or exc.__class__.__name__ == getattr(playwright_timeout_error, "__name__", ""):
            raise ImmowebFetchError(
                f"Timeout Playwright pendant le chargement de {search_url}."
            ) from exc
        raise ImmowebFetchError(
            f"Echec Playwright pendant la collecte Immoweb pour {search_url}: {message}"
        ) from exc



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



def _load_playwright_sync_api():
    try:
        from playwright.sync_api import TimeoutError, sync_playwright
    except ImportError as exc:
        raise ImmowebFetchError(
            "Playwright n'est pas installe. Installe `playwright` puis lance `python -m playwright install chromium`."
        ) from exc
    return sync_playwright, TimeoutError



def _maybe_accept_cookie_banner(page) -> None:
    for selector in IMMOWEB_COOKIE_SELECTORS:
        try:
            locator = page.locator(selector).first
            locator.wait_for(state="visible", timeout=1500)
            locator.click(timeout=1500)
            return
        except Exception:
            continue



def _wait_for_immoweb_content(page, timeout_ms: int) -> None:
    for selector in IMMOWEB_RESULT_WAIT_SELECTORS:
        try:
            page.locator(selector).first.wait_for(state="attached", timeout=timeout_ms)
            return
        except Exception:
            continue



def _extract_json_payload(script) -> object | None:
    script_type = (script.get("type") or "").strip().lower()
    if script_type not in {"application/ld+json", "application/json", ""}:
        return None

    raw_text = script.string or script.get_text(strip=False)
    if not raw_text:
        return None

    stripped = raw_text.strip()
    if not stripped:
        return None

    if script_type in {"application/ld+json", "application/json"} and stripped[:1] in "[{":
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
    if "immeuble de rapport" in text_seed or "building" in text_seed:
        property_type = "apartment_block"
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
