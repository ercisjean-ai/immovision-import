import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from normalization import normalize_feed_listing

IMMOWEB_BASE_URL = "https://www.immoweb.be"
IMMOWEB_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-BE,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": IMMOWEB_BASE_URL + "/",
    "Upgrade-Insecure-Requests": "1",
}
LISTING_PATH_RE = re.compile(r"^/(fr|nl)/annonce/")
LISTING_ID_RE = re.compile(r"/(\d{8,})(?:[/?#]|$)")
PRICE_RE = re.compile(r"(\d[\d\s.]*)\s*(?:EUR|€)", re.IGNORECASE)
POSTAL_COMMUNE_RE = re.compile(r"\b(\d{4})\s+([A-Za-zÀ-ÿ' -]{2,})")
SURFACE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*m(?:2|²)", re.IGNORECASE)
UNITS_RE = re.compile(
    r"(\d+)\s*(?:unites?|units?|logements?|appartements?)",
    re.IGNORECASE,
)
COMMUNE_STOPWORDS_RE = re.compile(
    r"\b(?:prix|price|eur|€|m2|m²|unites?|units?|logements?|appartements?)\b.*$",
    re.IGNORECASE,
)


class ImmowebFetchError(RuntimeError):
    pass



def fetch_immoweb_search_page(search_url: str, timeout: int = 30) -> str:
    session = requests.Session()
    try:
        response = session.get(
            search_url,
            headers=IMMOWEB_HEADERS,
            timeout=(10, timeout),
            allow_redirects=True,
        )
    except requests.RequestException as exc:
        raise ImmowebFetchError(
            f"Echec de collecte Immoweb pour {search_url}: {exc}"
        ) from exc

    if response.status_code == 403:
        raise ImmowebFetchError(
            "Immoweb a refuse la requete (403 Forbidden). "
            "Le connecteur live V1 reste limite. "
            "Utilise la fixture locale pour un test stable ou essaye une URL de recherche plus simple plus tard."
        )

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise ImmowebFetchError(
            f"Immoweb a retourne une erreur HTTP {response.status_code} pour {search_url}."
        ) from exc

    if not response.text.strip():
        raise ImmowebFetchError(
            f"Immoweb a retourne une reponse vide pour {search_url}."
        )

    return response.text



def collect_immoweb_listings(
    *,
    search_url: str | None = None,
    html: str | None = None,
    html_file: str | Path | None = None,
) -> list[dict[str, Any]]:
    data_origin = "fixture"
    if html is None and html_file is not None:
        html = Path(html_file).read_text(encoding="utf-8")
    if html is None and search_url:
        html = fetch_immoweb_search_page(search_url)
        data_origin = "live"
    if html is None:
        raise ValueError("Fournis search_url, html ou html_file.")

    items = parse_immoweb_search_results(html)
    if search_url and not items:
        raise ImmowebFetchError(
            f"Aucune annonce extraite depuis {search_url}. {diagnose_immoweb_empty_results(html)}"
        )
    return [{**item, "data_origin": data_origin} for item in items]



def parse_immoweb_search_results(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        if not LISTING_PATH_RE.match(href):
            continue

        absolute_url = urljoin(IMMOWEB_BASE_URL, href)
        listing_id = _extract_listing_id(absolute_url)
        if not listing_id or listing_id in seen_ids:
            continue

        card = _find_listing_card(anchor)
        text_parts = _collect_text_parts(card)
        text_blob = " ".join(text_parts)
        raw_item = {
            "source_name": "Immoweb",
            "source_listing_id": listing_id,
            "source_url": absolute_url,
            "title": _extract_title(anchor, card),
            "description": _extract_description(card),
            "price": _extract_price(text_blob),
            "postal_code": _extract_postal_code(text_blob, text_parts),
            "commune": _extract_commune(text_blob, text_parts),
            "property_type": _extract_property_type(text_blob),
            "transaction_type": _extract_transaction_type(absolute_url, text_blob),
            "existing_units": _extract_existing_units(text_blob),
            "surface": _extract_surface(text_blob),
            "is_active": True,
            "notes": "Collecte Immoweb search page",
        }
        items.append(normalize_feed_listing(raw_item, default_source_name="Immoweb"))
        seen_ids.add(listing_id)

    return items



def write_listings_jsonl(path: str | Path, items: list[dict[str, Any]]) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(item, ensure_ascii=False) for item in items]
    output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return output_path



def diagnose_immoweb_empty_results(html: str) -> str:
    normalized = html.lower()
    if any(token in normalized for token in ("captcha", "access denied", "forbidden", "robot", "bot")):
        return "Le HTML recupere ressemble a une page anti-bot ou d'acces refuse."
    if any(token in normalized for token in ("enable javascript", "javascript required", "please enable javascript")):
        return "Le HTML recupere semble dependre du JavaScript et ne contient pas les cartes attendues."
    if "/fr/annonce/" not in normalized and "/nl/annonce/" not in normalized:
        return "Le HTML recupere ne contient aucune URL d'annonce attendue; la page live est probablement differente du markup teste."
    return "Le HTML a ete charge mais aucun bloc d'annonce exploitable n'a ete extrait; le selecteur actuel est probablement trop fragile."



def _extract_listing_id(url: str) -> str | None:
    match = LISTING_ID_RE.search(url)
    return match.group(1) if match else None



def _find_listing_card(anchor: Any) -> Any:
    for parent in anchor.parents:
        if not hasattr(parent, "find_all"):
            continue
        if parent.name in {"article", "section"}:
            return parent
        css_class = " ".join(parent.get("class", []))
        if any(token in css_class.lower() for token in ("card", "listing", "result")):
            return parent
    return anchor.parent or anchor



def _collect_text_parts(card: Any) -> list[str]:
    return [text.strip() for text in card.stripped_strings if text.strip()]



def _extract_title(anchor: Any, card: Any) -> str | None:
    for tag_name in ("h1", "h2", "h3", "h4"):
        heading = card.find(tag_name)
        if heading:
            return " ".join(heading.stripped_strings)
    anchor_text = " ".join(anchor.stripped_strings)
    return anchor_text or None



def _extract_description(card: Any) -> str | None:
    paragraphs = []
    for paragraph in card.find_all(["p", "div", "span"]):
        text = " ".join(paragraph.stripped_strings)
        if text:
            paragraphs.append(text)
        if len(paragraphs) >= 3:
            break
    description = " | ".join(paragraphs)
    return description or None



def _extract_price(text: str) -> float | None:
    match = PRICE_RE.search(text)
    if not match:
        return None
    digits = re.sub(r"[^\d]", "", match.group(1))
    return float(digits) if digits else None



def _extract_postal_code(text: str, text_parts: list[str]) -> str | None:
    for part in text_parts:
        match = POSTAL_COMMUNE_RE.search(part)
        if match:
            return match.group(1)
    match = POSTAL_COMMUNE_RE.search(text)
    return match.group(1) if match else None



def _extract_commune(text: str, text_parts: list[str]) -> str | None:
    for part in text_parts:
        match = POSTAL_COMMUNE_RE.search(part)
        if match:
            return _clean_commune(match.group(2))
    match = POSTAL_COMMUNE_RE.search(text)
    return _clean_commune(match.group(2)) if match else None



def _clean_commune(value: str) -> str | None:
    cleaned = COMMUNE_STOPWORDS_RE.sub("", value).strip(" -|,.;")
    return cleaned or None



def _extract_surface(text: str) -> float | None:
    match = SURFACE_RE.search(text)
    if not match:
        return None
    return float(match.group(1).replace(",", "."))



def _extract_existing_units(text: str) -> int | None:
    match = UNITS_RE.search(text)
    return int(match.group(1)) if match else None



def _extract_property_type(text: str) -> str | None:
    normalized = text.lower()
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
    if "immeuble de rapport" in normalized or "building with" in normalized:
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
    if "maison" in normalized or "house" in normalized:
        return "house"
    if "appartement" in normalized or "apartment" in normalized or "flat" in normalized:
        return "apartment"
    return None



def _extract_transaction_type(url: str, text: str) -> str:
    normalized = f"{url} {text}".lower()
    if any(token in normalized for token in ("a-louer", "for-rent", "rent")):
        return "rent"
    return "sale"

