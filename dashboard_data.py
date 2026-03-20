import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from analysis_logic import PRIORITY_PROPERTY_TYPES


NEW_WINDOW_DAYS = 7


def _load_brussels_timezone():
    try:
        return ZoneInfo("Europe/Brussels")
    except ZoneInfoNotFoundError:
        # Fallback de securite pour Windows si la base IANA n'est pas disponible.
        # `tzdata` dans requirements.txt doit couvrir le cas normal.
        return timezone(timedelta(hours=1), name="Europe/Brussels")


BRUSSELS_TZ = _load_brussels_timezone()
STRICT_ZONE_LABELS = {"Bruxelles cible", "Peripherie cible"}
NOTARIAL_SOURCES = {"Biddit", "Notaire.be"}
CLOSED_STATUS_TOKENS = (
    "cloture",
    "cloturee",
    "terminee",
    "expiree",
    "afgelopen",
    "gesloten",
    "closed",
    "ended",
    "sold",
    "verkocht",
)
ACTIVE_STATUS_TOKENS = (
    "se termine le",
    "vente publique",
    "vente online",
    "vente en ligne",
    "gre a gre",
    "de gre a gre",
    "verkoop online",
    "onderhandse",
)
END_DATE_PATTERNS = (
    re.compile(r"se termine le\s+(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{2})", re.IGNORECASE),
    re.compile(r"finit le\s+(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{2})", re.IGNORECASE),
)
SOURCE_HOST_RULES = {
    "biddit": {"biddit.be"},
    "immoweb": {"immoweb.be"},
    "notaire.be": {"notaire.be", "immo.notaire.be"},
    "immovlan": {"immovlan.be"},
    "zimmo": {"zimmo.be"},
    "immoscoop": {"immoscoop.be"},
}
STRICT_DASHBOARD_PROPERTY_TYPES = PRIORITY_PROPERTY_TYPES | {"commercial", "mixed_use"}


def fetch_dashboard_payload(
    db_path: str | Path,
    *,
    now: datetime | None = None,
    live_only: bool = False,
) -> dict[str, Any]:
    resolved_db_path = Path(db_path).resolve()
    if not resolved_db_path.exists():
        raise FileNotFoundError(f"Base SQLite introuvable: {resolved_db_path}")

    current_time = now or datetime.now(timezone.utc)
    all_listings = _fetch_dashboard_rows(resolved_db_path, current_time)
    secondary_review = [
        item for item in all_listings if item.get("is_secondary_review_eligible")
    ]
    listings = [
        item
        for item in all_listings
        if not live_only or item.get("is_dashboard_eligible")
    ]
    sections = _build_sections(listings)
    summary = {
        "total": len(listings),
        "new": len(sections["new"]),
        "compatible": len(sections["compatible"]),
        "a_analyser": len(sections["a_analyser"]),
        "hors_criteres": len(sections["hors_criteres"]),
        "modified": len(sections["modified"]),
        "live": sum(1 for item in all_listings if item.get("is_live")),
        "non_live": sum(1 for item in all_listings if not item.get("is_live")),
        "live_with_valid_link": sum(
            1
            for item in all_listings
            if item.get("is_live") and item.get("source_url_valid")
        ),
        "live_with_invalid_link": sum(
            1
            for item in all_listings
            if item.get("is_live") and not item.get("source_url_valid")
        ),
        "live_strict_eligible": sum(
            1 for item in all_listings if item.get("is_dashboard_eligible")
        ),
        "live_inactive_or_closed": sum(
            1
            for item in all_listings
            if item.get("is_live")
            and item.get("source_url_valid")
            and not item.get("is_market_active")
        ),
        "live_out_of_criteria": sum(
            1
            for item in all_listings
            if item.get("is_live")
            and item.get("source_url_valid")
            and item.get("is_market_active")
            and not item.get("matches_investor_criteria")
        ),
        "secondary_review_total": len(secondary_review),
    }
    return {
        "generated_at": current_time.isoformat(),
        "database_path": str(resolved_db_path),
        "live_only": live_only,
        "summary": summary,
        "sections": sections,
        "listings": listings,
        "secondary_review": secondary_review,
    }


def _fetch_dashboard_rows(
    db_path: Path,
    current_time: datetime,
) -> list[dict[str, Any]]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT
                nl.id AS listing_id,
                nl.source_name,
                nl.source_listing_id,
                nl.source_url,
                nl.title,
                nl.description,
                nl.price,
                nl.existing_units,
                nl.property_type,
                nl.copro_status,
                nl.postal_code,
                nl.commune,
                nl.first_seen_at,
                nl.last_seen_at,
                nl.last_changed_at,
                nl.last_observation_status,
                nl.is_live_data,
                nl.data_origin,
                la.price_per_unit,
                la.investment_score,
                la.investment_score_label,
                la.confidence_score,
                la.confidence_label,
                la.strategy_label,
                la.zone_label,
                la.compatibility_reason
            FROM normalized_listings AS nl
            LEFT JOIN listing_analysis AS la
                ON la.listing_id = nl.id
            ORDER BY
                COALESCE(nl.last_seen_at, nl.first_seen_at) DESC,
                COALESCE(la.investment_score, 0) DESC,
                nl.id DESC
            """
        ).fetchall()
    finally:
        connection.close()

    listings = [_serialize_dashboard_row(dict(row), current_time) for row in rows]
    return sorted(
        listings,
        key=lambda item: (
            not item["is_new"],
            item["strategy_label"] != "Compatible",
            item["strategy_label"] != "A analyser",
            not item["is_modified"],
            -(item.get("investment_score") or 0),
            item.get("title") or "",
        ),
    )


def _serialize_dashboard_row(
    row: dict[str, Any],
    current_time: datetime,
) -> dict[str, Any]:
    first_seen_at = _parse_iso(row.get("first_seen_at"))
    last_seen_at = _parse_iso(row.get("last_seen_at"))
    last_changed_at = _parse_iso(row.get("last_changed_at"))
    is_new = False
    if first_seen_at is not None:
        is_new = first_seen_at >= current_time - timedelta(days=NEW_WINDOW_DAYS)

    observation_status = row.get("last_observation_status") or "seen"
    is_modified = observation_status == "modified"
    title = row.get("title") or f"{row.get('source_name')} #{row.get('source_listing_id')}"
    data_origin = _resolve_dashboard_data_origin(row)
    is_live = data_origin == "live"
    source_url_valid, source_url_issue = _validate_source_url(
        row.get("source_name"),
        row.get("source_url"),
    )
    sale_status, sale_status_reason = _resolve_sale_status(row, current_time)
    is_market_active = sale_status == "active"
    matches_investor_criteria, investor_view_issue = _matches_investor_main_criteria(
        row,
        sale_status=sale_status,
    )
    is_dashboard_eligible = (
        is_live and source_url_valid and is_market_active and matches_investor_criteria
    )
    (
        is_secondary_review_eligible,
        secondary_review_reason,
    ) = _classify_secondary_review(
        row,
        sale_status=sale_status,
        is_dashboard_eligible=is_dashboard_eligible,
    )

    return {
        "listing_id": row["listing_id"],
        "source_name": row.get("source_name"),
        "source_listing_id": row.get("source_listing_id"),
        "source_url": row.get("source_url"),
        "source_url_valid": source_url_valid,
        "source_url_issue": source_url_issue,
        "title": title,
        "description": row.get("description"),
        "price": row.get("price"),
        "existing_units": row.get("existing_units"),
        "property_type": row.get("property_type"),
        "copro_status": row.get("copro_status"),
        "price_per_unit": row.get("price_per_unit"),
        "investment_score": row.get("investment_score"),
        "investment_score_label": row.get("investment_score_label"),
        "confidence_score": row.get("confidence_score"),
        "confidence_label": row.get("confidence_label"),
        "strategy_label": row.get("strategy_label") or "A analyser",
        "observation_status": observation_status,
        "is_live": is_live,
        "is_live_eligible": is_live and source_url_valid,
        "sale_status": sale_status,
        "sale_status_reason": sale_status_reason,
        "is_market_active": is_market_active,
        "matches_investor_criteria": matches_investor_criteria,
        "investor_view_issue": investor_view_issue,
        "is_dashboard_eligible": is_dashboard_eligible,
        "is_secondary_review_eligible": is_secondary_review_eligible,
        "secondary_review_reason": secondary_review_reason,
        "data_origin": data_origin,
        "data_origin_label": _display_data_origin(data_origin),
        "zone_label": row.get("zone_label"),
        "commune": row.get("commune"),
        "postal_code": row.get("postal_code"),
        "compatibility_reason": row.get("compatibility_reason"),
        "first_seen_at": row.get("first_seen_at"),
        "last_seen_at": row.get("last_seen_at"),
        "last_changed_at": row.get("last_changed_at"),
        "is_new": is_new,
        "is_modified": is_modified,
        "new_window_days": NEW_WINDOW_DAYS,
        "display_location": _build_location_label(
            row.get("commune"),
            row.get("postal_code"),
        ),
        "display_source": row.get("source_name") or "Source",
        "display_first_seen": _format_short_date(first_seen_at),
        "display_last_seen": _format_short_date(last_seen_at),
        "display_last_changed": _format_short_date(last_changed_at),
    }


def _build_sections(listings: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    return {
        "new": [item for item in listings if item["is_new"]],
        "compatible": [item for item in listings if item["strategy_label"] == "Compatible"],
        "a_analyser": [item for item in listings if item["strategy_label"] == "A analyser"],
        "hors_criteres": [item for item in listings if item["strategy_label"] == "Hors criteres"],
        "modified": [item for item in listings if item["is_modified"]],
    }


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _format_short_date(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).strftime("%d/%m/%Y")


def _build_location_label(commune: Any, postal_code: Any) -> str | None:
    if commune and postal_code:
        return f"{postal_code} {commune}"
    if commune:
        return str(commune)
    if postal_code:
        return str(postal_code)
    return None


def _resolve_dashboard_data_origin(row: dict[str, Any]) -> str:
    explicit = str(row.get("data_origin") or "").strip().lower()
    if explicit in {"live", "fixture", "seed", "test", "file_feed"}:
        return explicit

    source_name = str(row.get("source_name") or "").strip().lower()
    source_url = str(row.get("source_url") or "").strip().lower()
    title = str(row.get("title") or "").strip().lower()

    if source_name in {"filefeed", "manualfeed"}:
        return "file_feed"
    if "example.test" in source_url:
        return "test"
    if source_name == "immoweb" and row.get("source_listing_id") == "12345678" and "test local" in title:
        return "seed"
    return "unknown"


def _resolve_sale_status(
    row: dict[str, Any],
    current_time: datetime,
) -> tuple[str, str]:
    source_name = str(row.get("source_name") or "").strip()
    if source_name not in NOTARIAL_SOURCES:
        return "active", "source standard consideree exploitable"

    text_seed = " ".join(
        part for part in [row.get("title"), row.get("description")] if part
    )
    normalized = _normalize_text_seed(text_seed)

    closing_at = _extract_sale_end_at(text_seed, current_time)
    if closing_at is not None:
        local_now = current_time.astimezone(BRUSSELS_TZ)
        if closing_at <= local_now:
            return "closed", f"vente terminee le {closing_at.strftime('%d/%m %H:%M')}"
        return "active", f"vente encore active jusqu'au {closing_at.strftime('%d/%m %H:%M')}"

    if any(token in normalized for token in CLOSED_STATUS_TOKENS):
        return "closed", "vente marquee comme cloturee ou terminee"

    if any(token in normalized for token in ACTIVE_STATUS_TOKENS):
        return "active", "vente signalee comme encore en cours"

    return "active", "aucun signal de cloture detecte sur une annonce live"


def _matches_investor_main_criteria(
    row: dict[str, Any],
    *,
    sale_status: str,
) -> tuple[bool, str | None]:
    if sale_status != "active":
        return False, "vente cloturee ou non exploitable"

    units = _to_int(row.get("existing_units"))
    if units is None or units < 2:
        return False, "moins de 2 unites"

    price_per_unit = _to_float(row.get("price_per_unit"))
    if price_per_unit is None or price_per_unit > 170000:
        return False, "prix par unite hors cible"

    zone_label = str(row.get("zone_label") or "")
    if zone_label not in STRICT_ZONE_LABELS:
        return False, "zone hors cible investisseur"

    property_type = str(row.get("property_type") or "").strip()
    if not property_type:
        return False, "type de bien absent"
    if property_type in {"house", "apartment"}:
        return False, "type simple hors cible investisseur"
    if property_type not in STRICT_DASHBOARD_PROPERTY_TYPES:
        return False, "type de bien hors cible"

    if row.get("strategy_label") == "Hors criteres":
        return False, "bien hors criteres"

    return True, None


def _classify_secondary_review(
    row: dict[str, Any],
    *,
    sale_status: str,
    is_dashboard_eligible: bool,
) -> tuple[bool, str | None]:
    if sale_status != "active":
        return False, "vente cloturee ou non exploitable"

    data_origin = _resolve_dashboard_data_origin(row)
    source_url_valid, _ = _validate_source_url(
        row.get("source_name"),
        row.get("source_url"),
    )
    if data_origin != "live" or not source_url_valid:
        return False, "annonce non exploitable dans la vue investisseur"
    if is_dashboard_eligible:
        return False, None

    units = _to_int(row.get("existing_units"))
    if units is None or units < 2:
        return False, "moins de 2 unites"

    price_per_unit = _to_float(row.get("price_per_unit"))
    if price_per_unit is None:
        return False, "prix par unite non calculable"
    if price_per_unit > 220000:
        return False, "prix par unite trop eleve"

    copro_status = str(row.get("copro_status") or "").strip().lower() or "unknown"
    if copro_status == "true":
        return False, "copropriete"

    zone_label = str(row.get("zone_label") or "")
    if zone_label == "Zone inconnue":
        return False, "zone inconnue"

    property_type = str(row.get("property_type") or "").strip()
    if not property_type:
        return False, "type de bien absent"
    if property_type in {"house", "apartment"}:
        return False, "type simple hors cible investisseur"
    if property_type not in STRICT_DASHBOARD_PROPERTY_TYPES:
        return False, "type de bien hors cible"

    soft_reasons: list[str] = []
    if zone_label == "Zone a analyser":
        soft_reasons.append("zone a analyser")
    if price_per_unit > 170000:
        soft_reasons.append("prix par unite > 170000 EUR")
    if copro_status == "unknown":
        soft_reasons.append("copropriete a confirmer")
    if property_type in {"commercial", "mixed_use"}:
        soft_reasons.append("type acceptable mais non prioritaire")

    if not soft_reasons:
        return False, None
    if len(soft_reasons) > 2:
        return False, "; ".join(soft_reasons)
    return True, "; ".join(soft_reasons)


def _display_data_origin(value: str) -> str:
    return {
        "live": "Live",
        "fixture": "Fixture",
        "seed": "Seed",
        "test": "Test",
        "file_feed": "File feed",
        "unknown": "Origine inconnue",
    }.get(value, "Origine inconnue")


def _normalize_text_seed(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip().lower()


def _extract_sale_end_at(
    text: str | None,
    current_time: datetime,
) -> datetime | None:
    if not text:
        return None

    local_now = current_time.astimezone(BRUSSELS_TZ)
    for pattern in END_DATE_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        day, month, hour, minute = [int(part) for part in match.groups()]
        year = local_now.year
        candidate = datetime(year, month, day, hour, minute, tzinfo=BRUSSELS_TZ)
        if candidate - local_now > timedelta(days=180):
            candidate = candidate.replace(year=year - 1)
        elif local_now - candidate > timedelta(days=180):
            candidate = candidate.replace(year=year + 1)
        return candidate
    return None


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _validate_source_url(source_name: Any, source_url: Any) -> tuple[bool, str | None]:
    raw_url = str(source_url or "").strip()
    if not raw_url:
        return False, "Lien source absent"
    if raw_url in {"#", "/"}:
        return False, "Lien source placeholder"

    lowered_url = raw_url.lower()
    if lowered_url in {"javascript:void(0)", "javascript:;", "about:blank"}:
        return False, "Lien source placeholder"

    parsed = urlparse(raw_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False, "Lien source mal forme"

    hostname = (parsed.hostname or "").strip().lower()
    if not hostname:
        return False, "Lien source mal forme"

    normalized_source = str(source_name or "").strip().lower()
    expected_hosts = SOURCE_HOST_RULES.get(normalized_source)
    if expected_hosts is None:
        return False, "Source live non reconnue pour validation stricte"

    if any(hostname == host or hostname.endswith(f".{host}") for host in expected_hosts):
        return True, None
    return False, "Domaine incoherent avec la source"
