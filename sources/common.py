import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests

DEFAULT_SOURCE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-BE,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}
PRICE_RE = re.compile(r"(\d[\d\s.\u00a0]*)\s*(?:EUR\.?|\u20ac)", re.IGNORECASE)
POSTAL_COMMUNE_RE = re.compile(r"\b(\d{4})\s+([A-Za-z\u00c0-\u017f' -]{2,})")
SURFACE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*m(?:2|\u00b2)", re.IGNORECASE)
UNITS_RE = re.compile(
    r"(\d+)\s*(?:unites?|units?|logements?|appartements?|apartments?)",
    re.IGNORECASE,
)
COMMUNE_STOPWORDS_RE = re.compile(
    r"\b(?:prix|price|eur|m2|m\u00b2|unites?|units?|logements?|appartements?|apartments?)\b.*$",
    re.IGNORECASE,
)



def fetch_html_with_requests(
    search_url: str,
    *,
    headers: dict[str, str] | None,
    timeout: int,
    error_cls: type[RuntimeError],
    source_label: str,
) -> str:
    session = requests.Session()
    try:
        response = session.get(
            search_url,
            headers=headers or DEFAULT_SOURCE_HEADERS,
            timeout=(10, timeout),
            allow_redirects=True,
        )
    except requests.RequestException as exc:
        raise error_cls(f"Echec de collecte {source_label} pour {search_url}: {exc}") from exc

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise error_cls(
            f"{source_label} a retourne une erreur HTTP {response.status_code} pour {search_url}."
        ) from exc

    if not response.text.strip():
        raise error_cls(f"{source_label} a retourne une reponse vide pour {search_url}.")

    return response.text



def load_source_html(
    *,
    search_url: str | None,
    html: str | None,
    html_file: str | Path | None,
    fetch_func,
) -> str:
    if html is None and html_file is not None:
        html = Path(html_file).read_text(encoding="utf-8")
    if html is None and search_url:
        html = fetch_func(search_url)
    if html is None:
        raise ValueError("Fournis search_url, html ou html_file.")
    return html



def write_listings_jsonl(path: str | Path, items: list[dict[str, Any]]) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(item, ensure_ascii=False) for item in items]
    output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return output_path



def extract_absolute_url(base_url: str, href: str) -> str:
    return urljoin(base_url, href)



def find_listing_card(anchor: Any) -> Any:
    for parent in anchor.parents:
        if not hasattr(parent, "find_all"):
            continue
        if parent.name in {"article", "section", "li"}:
            return parent
        css_class = " ".join(parent.get("class", []))
        if any(token in css_class.lower() for token in ("card", "listing", "property", "result", "auction")):
            return parent
    return anchor.parent or anchor



def collect_text_parts(card: Any) -> list[str]:
    return [text.strip() for text in card.stripped_strings if text.strip()]



def extract_title(anchor: Any, card: Any) -> str | None:
    for tag_name in ("h1", "h2", "h3", "h4"):
        heading = card.find(tag_name)
        if heading:
            return " ".join(heading.stripped_strings)
    anchor_text = " ".join(anchor.stripped_strings)
    return anchor_text or None



def extract_description(card: Any, limit: int = 3) -> str | None:
    parts: list[str] = []
    for node in card.find_all(["p", "div", "span"]):
        text = " ".join(node.stripped_strings)
        if not text:
            continue
        parts.append(text)
        if len(parts) >= limit:
            break
    description = " | ".join(parts)
    return description or None



def extract_price(text: str) -> float | None:
    match = PRICE_RE.search(text)
    if not match:
        return None
    digits = re.sub(r"[^\d]", "", match.group(1))
    return float(digits) if digits else None



def extract_postal_code(text: str, text_parts: list[str]) -> str | None:
    for part in text_parts:
        match = POSTAL_COMMUNE_RE.search(part)
        if match:
            return match.group(1)
    match = POSTAL_COMMUNE_RE.search(text)
    return match.group(1) if match else None



def extract_commune(text: str, text_parts: list[str]) -> str | None:
    for part in text_parts:
        match = POSTAL_COMMUNE_RE.search(part)
        if match:
            return clean_commune(match.group(2))
    match = POSTAL_COMMUNE_RE.search(text)
    return clean_commune(match.group(2)) if match else None



def clean_commune(value: str) -> str | None:
    cleaned = COMMUNE_STOPWORDS_RE.sub("", value).strip(" -|,.;")
    return cleaned or None



def extract_surface(text: str) -> float | None:
    match = SURFACE_RE.search(text)
    if not match:
        return None
    return float(match.group(1).replace(",", "."))



def extract_existing_units(text: str) -> int | None:
    match = UNITS_RE.search(text)
    return int(match.group(1)) if match else None



def detect_property_type(text: str) -> str | None:
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
    if any(token in normalized for token in ("immeuble de rapport", "meergezinswoning", "rapportgebouw", "building")):
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
    if any(token in normalized for token in ("maison", "huis", "woning", "house")):
        return "house"
    if any(token in normalized for token in ("appartement", "apartment", "flat")):
        return "apartment"
    if any(token in normalized for token in ("terrain", "grond", "land")):
        return "land"
    if any(token in normalized for token in ("garage", "parking")):
        return "garage"
    return None

