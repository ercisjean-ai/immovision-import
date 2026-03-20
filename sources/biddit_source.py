from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
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
    find_listing_card,
    load_source_html,
    write_listings_jsonl,
)

BIDDIT_BASE_URL = "https://www.biddit.be"
BIDDIT_HEADERS = dict(DEFAULT_SOURCE_HEADERS)
BIDDIT_HEADERS["Referer"] = BIDDIT_BASE_URL + "/fr/"
MAX_BIDDIT_EMBED_DEPTH = 2
IGNORED_EMBED_HOST_TOKENS = [
    "googletagmanager.com",
    "google-analytics.com",
    "analytics.google.com",
    "doubleclick.net",
    "googlesyndication.com",
    "facebook.com",
    "consensu.org",
    "didomi",
    "onetrust",
    "cookiebot",
    "hotjar.com",
    "clarity.ms",
]
IGNORED_EMBED_URL_TOKENS = [
    "ns.html",
    "analytics",
    "tracking",
    "consent",
    "cookie",
    "pixel",
    "tagmanager",
    "googletagmanager",
    "gtm-",
]
PREFERRED_BIDDIT_URL_TOKENS = [
    "biddit",
    "search",
    "result",
    "results",
    "listing",
    "auction",
    "property",
    "vente",
    "verkoop",
    "sale",
    "fr/",
    "nl/",
    "de/",
    "embed",
]


class BidditFetchError(RuntimeError):
    pass


@dataclass(frozen=True)
class BidditEmbedCandidate:
    url: str
    tag_name: str
    score: int
    selection_reason: str | None = None
    ignore_reason: str | None = None


@dataclass(frozen=True)
class BidditFetchResult:
    html: str
    fetched_url: str
    embed_urls: list[str] = field(default_factory=list)
    ignored_embed_details: list[str] = field(default_factory=list)
    followed_embed_urls: list[str] = field(default_factory=list)
    followed_embed_details: list[str] = field(default_factory=list)



def fetch_biddit_search_page(search_url: str, timeout: int = 30) -> str:
    return fetch_biddit_search_result(search_url, timeout=timeout).html



def fetch_biddit_search_result(search_url: str, timeout: int = 30) -> BidditFetchResult:
    session = requests.Session()
    root_html = _fetch_biddit_html(
        session,
        search_url,
        referer=BIDDIT_HEADERS["Referer"],
        timeout=timeout,
    )
    html = root_html
    fetched_url = search_url
    followed_embed_urls: list[str] = []
    followed_embed_details: list[str] = []
    detected_embed_urls: list[str] = []
    ignored_embed_details: list[str] = []
    current_url = search_url
    current_html = root_html

    candidates = _extract_embed_candidates(current_html, base_url=current_url)
    _collect_embed_diagnostics(
        candidates,
        detected_embed_urls=detected_embed_urls,
        ignored_embed_details=ignored_embed_details,
    )

    if candidates and not parse_biddit_search_results(current_html):
        for _ in range(MAX_BIDDIT_EMBED_DEPTH):
            next_candidate = _pick_followable_embed_candidate(candidates, followed_embed_urls)
            if next_candidate is None:
                break

            followed_embed_urls.append(next_candidate.url)
            followed_embed_details.append(
                f"{next_candidate.url} ({next_candidate.selection_reason or 'source embed retenue'})"
            )
            current_html = _fetch_biddit_html(
                session,
                next_candidate.url,
                referer=current_url,
                timeout=timeout,
            )
            current_url = next_candidate.url
            html = current_html
            fetched_url = current_url

            if parse_biddit_search_results(current_html):
                break

            candidates = _extract_embed_candidates(current_html, base_url=current_url)
            _collect_embed_diagnostics(
                candidates,
                detected_embed_urls=detected_embed_urls,
                ignored_embed_details=ignored_embed_details,
            )
            if not candidates:
                break

    return BidditFetchResult(
        html=html,
        fetched_url=fetched_url,
        embed_urls=detected_embed_urls,
        ignored_embed_details=ignored_embed_details,
        followed_embed_urls=followed_embed_urls,
        followed_embed_details=followed_embed_details,
    )



def collect_biddit_listings(
    *,
    search_url: str | None = None,
    html: str | None = None,
    html_file: str | Path | None = None,
    timeout: int = 30,
) -> list[dict[str, Any]]:
    fetch_result: BidditFetchResult | None = None
    data_origin = "fixture"
    if html is None and html_file is None and search_url:
        fetch_result = fetch_biddit_search_result(search_url, timeout=timeout)
        html = fetch_result.html
        data_origin = "live"
    else:
        html = load_source_html(
            search_url=search_url,
            html=html,
            html_file=html_file,
            fetch_func=lambda url: fetch_biddit_search_page(url, timeout=timeout),
        )

    items = parse_biddit_search_results(html)
    if search_url and not items:
        diagnostic = diagnose_biddit_empty_results(
            html,
            embed_urls=fetch_result.embed_urls if fetch_result else None,
            ignored_embed_details=fetch_result.ignored_embed_details if fetch_result else None,
            followed_embed_urls=fetch_result.followed_embed_urls if fetch_result else None,
            followed_embed_details=fetch_result.followed_embed_details if fetch_result else None,
            fetched_url=fetch_result.fetched_url if fetch_result else None,
        )
        raise BidditFetchError(
            f"Aucune annonce extraite depuis {search_url}. {diagnostic}"
        )
    return [{**item, "data_origin": data_origin} for item in items]



def parse_biddit_search_results(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        absolute_url = extract_absolute_url(BIDDIT_BASE_URL, href)
        listing_id = _extract_listing_id(absolute_url)
        if not listing_id or listing_id in seen_ids:
            continue

        card = _find_biddit_listing_card(anchor, listing_id) or find_listing_card(anchor)
        text_parts = collect_text_parts(card)
        text_blob = " ".join(text_parts)
        raw_item = {
            "source_name": "Biddit",
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
            "notes": _build_notes(text_blob),
        }
        items.append(normalize_feed_listing(raw_item, default_source_name="Biddit"))
        seen_ids.add(listing_id)

    return items


def _find_biddit_listing_card(anchor: Any, listing_id: str) -> Any:
    best_parent = None
    best_score = -1

    for parent in anchor.parents:
        if not hasattr(parent, "find_all"):
            continue
        if getattr(parent, "name", None) not in {"article", "section", "li", "div"}:
            continue

        listing_ids: set[str] = set()
        for link in parent.find_all("a", href=True):
            candidate_url = extract_absolute_url(BIDDIT_BASE_URL, link.get("href", "").strip())
            candidate_id = _extract_listing_id(candidate_url)
            if candidate_id:
                listing_ids.add(candidate_id)
            if len(listing_ids) > 1:
                break

        if len(listing_ids) > 1:
            break
        if listing_ids != {listing_id}:
            continue

        text_score = len(" ".join(collect_text_parts(parent)))
        if text_score >= best_score:
            best_parent = parent
            best_score = text_score

    return best_parent




def diagnose_biddit_empty_results(
    html: str,
    *,
    embed_urls: list[str] | None = None,
    ignored_embed_details: list[str] | None = None,
    followed_embed_urls: list[str] | None = None,
    followed_embed_details: list[str] | None = None,
    fetched_url: str | None = None,
) -> str:
    normalized = html.lower()
    embed_urls = embed_urls or []
    ignored_embed_details = ignored_embed_details or []
    followed_embed_urls = followed_embed_urls or []
    followed_embed_details = followed_embed_details or []
    parts: list[str] = []

    if followed_embed_urls:
        followed_label = followed_embed_urls[-1]
        if "iframe" in normalized or "embed" in normalized:
            parts.append(
                f"La source embarquee suivie ({followed_label}) contient encore un conteneur iframe/embed, sans cartes exploitables cote HTML."
            )
        elif any(token in normalized for token in ("captcha", "access denied", "forbidden", "robot", "bot")):
            parts.append(
                f"La source embarquee suivie ({followed_label}) ressemble a une page anti-bot ou d'acces refuse."
            )
        elif "biddit" not in normalized:
            parts.append(
                f"La source embarquee suivie ({followed_label}) a ete chargee, mais ne ressemble pas a une page Biddit exploitable."
            )
        else:
            parts.append(
                f"La source embarquee suivie ({followed_label}) a ete chargee, mais aucun bloc d'annonce Biddit exploitable n'a ete extrait."
            )
    elif embed_urls:
        parts.append("Le HTML recupere contient des sources embarquees, mais aucune source metier plausible n'a pu etre suivie.")
    elif "iframe" in normalized or "embed" in normalized:
        parts.append("Le HTML recupere ressemble a un conteneur iframe ou un embed, sans cartes exploitables cote HTML.")
    elif any(token in normalized for token in ("captcha", "access denied", "forbidden", "robot", "bot")):
        parts.append("Le HTML recupere ressemble a une page anti-bot ou d'acces refuse.")
    elif fetched_url and fetched_url != BIDDIT_BASE_URL and "biddit" not in normalized:
        parts.append(f"La source chargee ({fetched_url}) ne ressemble pas a une page Biddit exploitable.")
    elif "biddit" not in normalized:
        parts.append("Le HTML recupere ne ressemble pas a une page Biddit attendue.")
    else:
        parts.append("Le HTML a ete charge mais aucun bloc d'annonce Biddit exploitable n'a ete extrait.")

    if embed_urls:
        parts.append("Sources embarquees detectees: " + "; ".join(embed_urls[:6]) + ".")
    if ignored_embed_details:
        parts.append("Sources ignorees: " + "; ".join(ignored_embed_details[:6]) + ".")
    if followed_embed_details:
        parts.append("Sources suivies: " + "; ".join(followed_embed_details[:4]) + ".")

    return " ".join(parts).strip()



def _fetch_biddit_html(
    session: requests.Session,
    url: str,
    *,
    referer: str,
    timeout: int,
) -> str:
    headers = dict(BIDDIT_HEADERS)
    headers["Referer"] = referer
    try:
        response = session.get(
            url,
            headers=headers,
            timeout=(10, timeout),
            allow_redirects=True,
        )
    except requests.RequestException as exc:
        raise BidditFetchError(f"Echec de collecte Biddit pour {url}: {exc}") from exc

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise BidditFetchError(
            f"Biddit a retourne une erreur HTTP {response.status_code} pour {url}."
        ) from exc

    if not response.text.strip():
        raise BidditFetchError(f"Biddit a retourne une reponse vide pour {url}.")

    return response.text



def _extract_embed_candidates(html: str, *, base_url: str) -> list[BidditEmbedCandidate]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[BidditEmbedCandidate] = []
    seen_urls: set[str] = set()

    for tag_name, attr_name in (("iframe", "src"), ("embed", "src"), ("object", "data")):
        for node in soup.find_all(tag_name):
            raw_url = (node.get(attr_name) or "").strip()
            if not raw_url or raw_url.startswith("javascript:"):
                continue
            resolved = extract_absolute_url(base_url, raw_url)
            if resolved in seen_urls:
                continue
            seen_urls.add(resolved)
            score, selection_reason, ignore_reason = _classify_embed_candidate(resolved)
            candidates.append(
                BidditEmbedCandidate(
                    url=resolved,
                    tag_name=tag_name,
                    score=score,
                    selection_reason=selection_reason,
                    ignore_reason=ignore_reason,
                )
            )

    return candidates



def _collect_embed_diagnostics(
    candidates: list[BidditEmbedCandidate],
    *,
    detected_embed_urls: list[str],
    ignored_embed_details: list[str],
) -> None:
    for candidate in candidates:
        if candidate.url not in detected_embed_urls:
            detected_embed_urls.append(candidate.url)
        if candidate.ignore_reason:
            detail = f"{candidate.url} ({candidate.ignore_reason})"
            if detail not in ignored_embed_details:
                ignored_embed_details.append(detail)



def _classify_embed_candidate(url: str) -> tuple[int, str | None, str | None]:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    query = parsed.query.lower()
    combined = f"{host} {path} {query}"

    if parsed.scheme not in {"http", "https"}:
        return 0, None, "schema non supporte"
    for token in IGNORED_EMBED_HOST_TOKENS:
        if token in host:
            return 0, None, f"source technique ignoree ({token})"
    for token in IGNORED_EMBED_URL_TOKENS:
        if token in combined:
            return 0, None, f"source technique ignoree ({token})"

    score = 0
    reasons: list[str] = []
    if "biddit.be" in host:
        score += 100
        reasons.append("meme domaine Biddit")
    elif "biddit" in combined:
        score += 60
        reasons.append("URL contenant biddit")
    else:
        return 0, None, "source embarquee sans signal metier Biddit"

    matched_tokens = [token for token in PREFERRED_BIDDIT_URL_TOKENS if token in combined]
    if matched_tokens:
        score += min(40, len(matched_tokens) * 10)
        reasons.append("tokens metier: " + ", ".join(sorted(set(matched_tokens))[:4]))

    selection_reason = "; ".join(reasons) if reasons else "source Biddit plausible"
    return score, selection_reason, None



def _pick_followable_embed_candidate(
    candidates: list[BidditEmbedCandidate],
    already_followed: list[str],
) -> BidditEmbedCandidate | None:
    eligible = [
        candidate
        for candidate in candidates
        if candidate.ignore_reason is None and candidate.url not in already_followed and candidate.score > 0
    ]
    if not eligible:
        return None
    return max(eligible, key=lambda candidate: candidate.score)



def _extract_listing_id(url: str) -> str | None:
    parsed = urlparse(url)
    if parsed.netloc and "biddit.be" not in parsed.netloc:
        return None
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2:
        return None
    candidate = parts[-1]
    return candidate if candidate.isdigit() else None



def _build_notes(text: str) -> str:
    normalized = text.lower()
    notes = ["Collecte Biddit page"]
    if "vente online" in normalized or "biddit" in normalized:
        notes.append("mode: vente online")
    if "mise a prix" in normalized:
        notes.append("prix: mise a prix")
    return " | ".join(notes)





