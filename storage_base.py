import unicodedata
from dataclasses import dataclass
from typing import Any

from normalization import normalize_data_origin


SIGNIFICANT_CHANGE_FIELDS = (
    "title",
    "price",
    "postal_code",
    "commune",
    "property_type",
    "transaction_type",
    "existing_units",
    "surface",
    "copro_status",
)
SPARSE_SAFE_FIELDS = (
    "title",
    "description",
    "price",
    "postal_code",
    "commune",
    "property_type",
    "transaction_type",
    "existing_units",
    "surface",
)
EFFECTIVE_ITEM_FIELDS = (
    "source_id",
    "source_name",
    "source_listing_id",
    "source_url",
    "title",
    "description",
    "price",
    "postal_code",
    "commune",
    "property_type",
    "transaction_type",
    "existing_units",
    "surface",
    "is_copro",
    "copro_status",
    "is_new_build",
    "is_live_data",
    "data_origin",
    "last_seen_at",
    "first_seen_at",
)


@dataclass
class ListingUpsertResult:
    listing_id: str
    observation_status: str
    changed_fields: list[str]
    is_price_changed: bool
    effective_item: dict[str, Any]


class StorageBackend:
    def get_source_id(self, source_name: str) -> str:
        raise NotImplementedError

    def fetch_search_targets(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def fetch_import_queue(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def upsert_discovered_url(self, item: dict[str, Any]) -> None:
        raise NotImplementedError

    def queue_new_discoveries(self) -> int:
        raise NotImplementedError

    def upsert_listing(self, item: dict[str, Any]) -> ListingUpsertResult:
        raise NotImplementedError

    def upsert_analysis(self, analysis_payload: dict[str, Any]) -> None:
        raise NotImplementedError

    def insert_observation_history(
        self,
        upsert_result: ListingUpsertResult,
        item: dict[str, Any],
    ) -> None:
        raise NotImplementedError

    def insert_price_history(self, listing_id: str, price: Any) -> None:
        raise NotImplementedError

    def update_source_counts(self) -> None:
        raise NotImplementedError

    def insert_sync_log(
        self,
        status: str,
        listings_found: int,
        listings_imported: int,
        error_message: str | None = None,
    ) -> None:
        raise NotImplementedError

    def seed_import_queue_item(self, item: dict[str, Any]) -> None:
        raise NotImplementedError


def merge_listing_payload(
    existing: dict[str, Any] | None,
    payload: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(payload)
    if existing is None:
        merged["copro_status"] = _normalize_copro_status(merged.get("copro_status"))
        merged["is_copro"] = merged["copro_status"] == "true"
        merged["data_origin"] = normalize_data_origin(merged.get("data_origin"))
        return merged

    for field in SPARSE_SAFE_FIELDS:
        if _is_missing_value(merged.get(field)):
            merged[field] = existing.get(field)

    incoming_copro_status = _normalize_copro_status(merged.get("copro_status"))
    existing_copro_status = _normalize_copro_status(existing.get("copro_status"))
    if existing_copro_status == "unknown" and bool(existing.get("is_copro")):
        existing_copro_status = "true"
    if incoming_copro_status == "unknown" and existing_copro_status in {"true", "false"}:
        merged["copro_status"] = existing_copro_status
    else:
        merged["copro_status"] = incoming_copro_status

    if merged["copro_status"] in {"true", "false"}:
        merged["is_copro"] = merged["copro_status"] == "true"
    else:
        merged["is_copro"] = False

    incoming_data_origin = normalize_data_origin(merged.get("data_origin"))
    existing_data_origin = normalize_data_origin(existing.get("data_origin"))
    if incoming_data_origin == "unknown" and existing_data_origin != "unknown":
        merged["data_origin"] = existing_data_origin
    else:
        merged["data_origin"] = incoming_data_origin

    return merged


def compute_observation_change(
    existing: dict[str, Any] | None,
    payload: dict[str, Any],
) -> tuple[str, list[str], bool]:
    if existing is None:
        return "new", [], payload.get("price") is not None

    changed_fields = [
        field
        for field in SIGNIFICANT_CHANGE_FIELDS
        if not _values_equal(field, existing.get(field), payload.get(field))
    ]
    observation_status = "modified" if changed_fields else "seen"
    is_price_changed = "price" in changed_fields
    return observation_status, changed_fields, is_price_changed


def build_effective_item(
    item: dict[str, Any],
    merged_payload: dict[str, Any],
) -> dict[str, Any]:
    effective_item = dict(item)
    for field in EFFECTIVE_ITEM_FIELDS:
        if field in merged_payload:
            effective_item[field] = merged_payload[field]
    return effective_item


def serialize_changed_fields(changed_fields: list[str]) -> str | None:
    return ",".join(changed_fields) if changed_fields else None


def _values_equal(field: str, left: Any, right: Any) -> bool:
    if field == "price":
        return _to_float(left) == _to_float(right)
    if field == "surface":
        left_value = _to_float(left)
        right_value = _to_float(right)
        if left_value is None or right_value is None:
            return left_value == right_value
        return abs(left_value - right_value) < 1.0
    if field == "existing_units":
        return _to_int(left) == _to_int(right)
    if field == "copro_status":
        return _normalize_copro_status(left) == _normalize_copro_status(right)
    if field in {"title", "commune", "postal_code", "property_type", "transaction_type"}:
        return _normalize_text(left) == _normalize_text(right)
    return left == right


def _normalize_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    normalized = unicodedata.normalize("NFKD", str(value))
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    lowered = ascii_value.lower()
    cleaned = "".join(ch if ch.isalnum() else " " for ch in lowered)
    return " ".join(cleaned.split())


def _normalize_copro_status(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return "true" if bool(value) else "false"
    lowered = str(value).strip().lower()
    if lowered in {"true", "1", "yes", "y", "on"}:
        return "true"
    if lowered in {"false", "0", "no", "n", "off"}:
        return "false"
    if lowered in {"unknown", "unk", "na", "n/a", "?", "none", "null"}:
        return "unknown"
    return "unknown"


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return round(float(value), 2)


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _is_missing_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False
