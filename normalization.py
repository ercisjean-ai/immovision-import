from typing import Any

from config import utcnow_iso


VALID_DATA_ORIGINS = {
    "live",
    "fixture",
    "seed",
    "test",
    "file_feed",
    "unknown",
}


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _normalize_copro_status(raw_item: dict[str, Any]) -> str:
    if "copro_status" in raw_item:
        explicit_status = raw_item.get("copro_status")
        if explicit_status in (None, ""):
            return "unknown"
        lowered = str(explicit_status).strip().lower()
        if lowered in {"true", "1", "yes", "y", "on"}:
            return "true"
        if lowered in {"false", "0", "no", "n", "off"}:
            return "false"
        return "unknown"

    if "is_copro" not in raw_item or raw_item.get("is_copro") in (None, ""):
        return "unknown"
    return "true" if _to_bool(raw_item.get("is_copro"), False) else "false"


def normalize_data_origin(
    raw_value: Any,
    *,
    fallback: str = "unknown",
) -> str:
    value = str(raw_value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if value in {"demo", "demo_feed", "sample", "sample_feed", "manual_feed"}:
        value = "file_feed"
    if value in VALID_DATA_ORIGINS:
        return value
    return fallback


def resolve_data_origin(
    raw_item: dict[str, Any],
    *,
    default_source_name: str = "",
) -> str:
    explicit_origin = normalize_data_origin(raw_item.get("data_origin"))
    if explicit_origin != "unknown":
        return explicit_origin

    source_name = str(raw_item.get("source_name") or default_source_name or "").strip().lower()
    source_url = str(raw_item.get("source_url") or raw_item.get("url") or "").strip().lower()
    notes = str(raw_item.get("notes") or "").strip().lower()

    if "seed local" in notes or notes.startswith("seed"):
        return "seed"
    if source_name in {"filefeed", "manualfeed"}:
        return "file_feed"
    if "example.test" in source_url or notes.startswith("cas test") or notes.startswith("test"):
        return "test"
    return "unknown"


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def normalize_feed_listing(
    raw_item: dict[str, Any],
    default_source_name: str = "FileFeed",
) -> dict[str, Any]:
    source_listing_id = raw_item.get("source_listing_id") or raw_item.get("listing_id")
    source_url = raw_item.get("source_url") or raw_item.get("url")

    if not source_listing_id:
        raise ValueError("Champ requis manquant: source_listing_id")
    if not source_url:
        raise ValueError("Champ requis manquant: source_url")

    copro_status = _normalize_copro_status(raw_item)
    data_origin = resolve_data_origin(raw_item, default_source_name=default_source_name)

    return {
        "source_name": raw_item.get("source_name") or default_source_name,
        "source_listing_id": str(source_listing_id),
        "source_url": str(source_url),
        "title": raw_item.get("title"),
        "description": raw_item.get("description"),
        "price": _to_float(raw_item.get("price")),
        "postal_code": raw_item.get("postal_code"),
        "commune": raw_item.get("commune"),
        "property_type": raw_item.get("property_type"),
        "transaction_type": raw_item.get("transaction_type") or "sale",
        "existing_units": _to_int(raw_item.get("existing_units")),
        "surface": _to_float(raw_item.get("surface")),
        "is_copro": copro_status == "true",
        "copro_status": copro_status,
        "is_new_build": _to_bool(raw_item.get("is_new_build"), False),
        "is_live_data": _to_bool(raw_item.get("is_live_data"), True),
        "data_origin": data_origin,
        "is_active": _to_bool(raw_item.get("is_active"), True),
        "notes": raw_item.get("notes"),
        "updated_at": raw_item.get("updated_at") or utcnow_iso(),
    }


def build_listing_payload(item: dict[str, Any], source_id: str) -> dict[str, Any]:
    copro_status = _normalize_copro_status(item)
    data_origin = resolve_data_origin(item, default_source_name=item.get("source_name") or "")
    return {
        "source_id": source_id,
        "source_name": item["source_name"],
        "source_listing_id": item["source_listing_id"],
        "source_url": item["source_url"],
        "title": item.get("title"),
        "description": item.get("description"),
        "price": item.get("price"),
        "postal_code": item.get("postal_code"),
        "commune": item.get("commune"),
        "property_type": item.get("property_type"),
        "transaction_type": item.get("transaction_type") or "sale",
        "existing_units": item.get("existing_units"),
        "surface": item.get("surface"),
        "is_copro": copro_status == "true",
        "copro_status": copro_status,
        "is_new_build": item.get("is_new_build", False),
        "is_live_data": item.get("is_live_data", True),
        "data_origin": data_origin,
        "last_seen_at": utcnow_iso(),
    }
