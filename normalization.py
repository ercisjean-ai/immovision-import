from typing import Any

from config import utcnow_iso


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
