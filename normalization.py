from typing import Any

from config import utcnow_iso


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
        "is_copro": _to_bool(raw_item.get("is_copro"), False),
        "is_new_build": _to_bool(raw_item.get("is_new_build"), False),
        "is_live_data": _to_bool(raw_item.get("is_live_data"), True),
        "is_active": _to_bool(raw_item.get("is_active"), True),
        "notes": raw_item.get("notes"),
        "updated_at": raw_item.get("updated_at") or utcnow_iso(),
    }


def build_listing_payload(item: dict[str, Any], source_id: str) -> dict[str, Any]:
    return {
        "source_id": source_id,
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
        "is_copro": item.get("is_copro", False),
        "is_new_build": item.get("is_new_build", False),
        "is_live_data": item.get("is_live_data", True),
        "last_seen_at": utcnow_iso(),
    }
