from typing import Any

from config import utcnow_iso
from normalization import build_listing_payload, normalize_feed_listing
from storage_base import (
    ListingUpsertResult,
    StorageBackend,
    build_effective_item,
    compute_observation_change,
    merge_listing_payload,
    serialize_changed_fields,
)

try:
    from supabase import Client, create_client
except ImportError:  # pragma: no cover - exercised only when dependency is absent
    Client = Any  # type: ignore[assignment]
    create_client = None


class SupabaseStorage(StorageBackend):
    def __init__(self, url: str, key: str):
        if create_client is None:
            raise RuntimeError(
                "La dependance supabase n'est pas installee, impossible d'utiliser "
                "le backend Supabase."
            )
        self.client: Client = create_client(url, key)

    def _select_rows_by_identity(
        self,
        table_name: str,
        *,
        source_name: str,
        source_listing_id: str,
        columns: str = "*",
    ) -> list[dict[str, Any]]:
        try:
            return (
                self.client.table(table_name)
                .select(columns)
                .eq("source_name", source_name)
                .eq("source_listing_id", source_listing_id)
                .limit(1)
                .execute()
                .data
            ) or []
        except Exception:
            return (
                self.client.table(table_name)
                .select(columns)
                .eq("source_listing_id", source_listing_id)
                .limit(1)
                .execute()
                .data
            ) or []

    def _upsert_by_identity(
        self,
        table_name: str,
        payload: dict[str, Any],
    ):
        try:
            return (
                self.client.table(table_name)
                .upsert(payload, on_conflict="source_name,source_listing_id")
                .execute()
            )
        except Exception:
            legacy_payload = dict(payload)
            if table_name == "normalized_listings":
                legacy_payload.pop("source_name", None)
            legacy_payload.pop("data_origin", None)
            legacy_payload.pop("copro_status", None)
            return (
                self.client.table(table_name)
                .upsert(legacy_payload, on_conflict="source_listing_id")
                .execute()
            )

    def get_source_id(self, source_name: str) -> str:
        result = (
            self.client.table("sources")
            .select("id")
            .eq("name", source_name)
            .limit(1)
            .execute()
        )
        if not result.data:
            raise ValueError(f"Source introuvable: {source_name}")
        return result.data[0]["id"]

    def fetch_search_targets(self) -> list[dict[str, Any]]:
        result = (
            self.client.table("search_targets")
            .select("*")
            .eq("is_active", True)
            .execute()
        )
        return result.data or []

    def fetch_import_queue(self) -> list[dict[str, Any]]:
        result = (
            self.client.table("import_queue")
            .select("*")
            .eq("is_active", True)
            .execute()
        )
        return result.data or []

    def upsert_discovered_url(self, item: dict[str, Any]) -> None:
        payload = {
            "source_name": item["source_name"],
            "search_target_id": item["search_target_id"],
            "source_url": item["source_url"],
            "source_listing_id": item["source_listing_id"],
            "last_seen_at": utcnow_iso(),
            "is_active": True,
        }
        (
            self.client.table("discovered_urls")
            .upsert(payload, on_conflict="source_url")
            .execute()
        )

    def queue_new_discoveries(self) -> int:
        discovered = (
            self.client.table("discovered_urls")
            .select("*")
            .eq("is_active", True)
            .eq("is_queued", False)
            .execute()
            .data
        ) or []

        queued = 0
        for item in discovered:
            source_listing_id = item.get("source_listing_id")
            if not source_listing_id:
                continue

            existing = self._select_rows_by_identity(
                "import_queue",
                source_name=item["source_name"],
                source_listing_id=source_listing_id,
                columns="id",
            )

            if existing:
                (
                    self.client.table("discovered_urls")
                    .update({"is_queued": True})
                    .eq("id", item["id"])
                    .execute()
                )
                continue

            payload = {
                "source_name": item["source_name"],
                "source_listing_id": source_listing_id,
                "source_url": item["source_url"],
                "is_active": True,
                "is_live_data": True,
                "data_origin": "live",
                "notes": "URL decouverte automatiquement depuis search_targets",
                "updated_at": utcnow_iso(),
            }

            self._upsert_by_identity("import_queue", payload)

            (
                self.client.table("discovered_urls")
                .update({"is_queued": True})
                .eq("id", item["id"])
                .execute()
            )
            queued += 1

        return queued

    def upsert_listing(self, item: dict[str, Any]) -> ListingUpsertResult:
        source_id = self.get_source_id(item["source_name"])
        existing_rows = self._select_rows_by_identity(
            "normalized_listings",
            source_name=item["source_name"],
            source_listing_id=item["source_listing_id"],
        )
        existing = existing_rows[0] if existing_rows else None
        payload = build_listing_payload(item, source_id)
        merged_payload = merge_listing_payload(existing, payload)
        observation_status, changed_fields, is_price_changed = (
            compute_observation_change(existing, merged_payload)
        )
        supabase_payload = {
            key: merged_payload[key]
            for key in [
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
            ]
            if key in merged_payload
        }
        result = self._upsert_by_identity("normalized_listings", supabase_payload)
        return ListingUpsertResult(
            listing_id=str(result.data[0]["id"]),
            observation_status=observation_status,
            changed_fields=changed_fields,
            is_price_changed=is_price_changed,
            effective_item=build_effective_item(item, merged_payload),
        )

    def upsert_analysis(self, analysis_payload: dict[str, Any]) -> None:
        supabase_payload = {
            key: analysis_payload[key]
            for key in [
                "listing_id",
                "zone_label",
                "strategy_compatible",
                "compatibility_reason",
                "price_per_unit",
                "estimated_rent_per_unit",
                "estimated_total_rent_monthly",
                "estimated_total_rent_annual",
                "estimated_monthly_loan_payment",
                "estimated_gross_yield",
                "estimated_monthly_spread",
                "rental_score_label",
                "investment_score",
                "confidence_score",
                "confidence_label",
                "confidence_reason",
                "updated_at",
            ]
            if key in analysis_payload
        }
        (
            self.client.table("listing_analysis")
            .upsert(supabase_payload, on_conflict="listing_id")
            .execute()
        )

    def insert_observation_history(
        self,
        upsert_result: ListingUpsertResult,
        item: dict[str, Any],
    ) -> None:
        payload = {
            "listing_id": upsert_result.listing_id,
            "source_name": item["source_name"],
            "source_listing_id": item.get("source_listing_id"),
            "source_url": item["source_url"],
            "title": item.get("title"),
            "price": item.get("price"),
            "commune": item.get("commune"),
            "postal_code": item.get("postal_code"),
            "is_active": bool(item.get("is_active", True)),
            "observation_status": upsert_result.observation_status,
            "changed_fields": serialize_changed_fields(upsert_result.changed_fields),
            "is_price_changed": bool(upsert_result.is_price_changed),
            "observed_at": utcnow_iso(),
        }
        try:
            self.client.table("listing_observation_history").insert(payload).execute()
        except Exception:
            try:
                legacy_payload = {
                    key: payload[key]
                    for key in [
                        "listing_id",
                        "source_name",
                        "source_listing_id",
                        "source_url",
                        "title",
                        "price",
                        "commune",
                        "postal_code",
                        "is_active",
                        "observed_at",
                    ]
                }
                self.client.table("listing_observation_history").insert(
                    legacy_payload
                ).execute()
            except Exception:
                return

    def insert_price_history(self, listing_id: str, price: Any) -> None:
        if price is None:
            return

        latest = (
            self.client.table("listing_price_history")
            .select("price")
            .eq("listing_id", listing_id)
            .order("observed_at", desc=True)
            .limit(1)
            .execute()
            .data
        ) or []

        if latest and float(latest[0]["price"]) == float(price):
            return

        self.client.table("listing_price_history").insert(
            {
                "listing_id": listing_id,
                "price": price,
                "observed_at": utcnow_iso(),
            }
        ).execute()

    def update_source_counts(self) -> None:
        sources = self.client.table("sources").select("id,name").execute().data or []
        for source in sources:
            count = (
                self.client.table("normalized_listings")
                .select("id", count="exact")
                .eq("source_id", source["id"])
                .eq("is_live_data", True)
                .execute()
                .count
            )

            (
                self.client.table("sources")
                .update({"live_count": count or 0, "last_sync": utcnow_iso()})
                .eq("id", source["id"])
                .execute()
            )

    def insert_sync_log(
        self,
        status: str,
        listings_found: int,
        listings_imported: int,
        error_message: str | None = None,
    ) -> None:
        immoweb = (
            self.client.table("sources")
            .select("id")
            .eq("name", "Immoweb")
            .limit(1)
            .execute()
            .data
        )
        source_id = immoweb[0]["id"] if immoweb else None

        self.client.table("source_syncs").insert(
            {
                "source_id": source_id,
                "status": status,
                "listings_found": listings_found,
                "listings_imported": listings_imported,
                "error_message": error_message,
                "started_at": utcnow_iso(),
                "finished_at": utcnow_iso(),
            }
        ).execute()

    def seed_import_queue_item(self, item: dict[str, Any]) -> None:
        normalized_item = normalize_feed_listing(
            item,
            default_source_name=item.get("source_name") or "FileFeed",
        )
        payload = {
            "source_name": normalized_item["source_name"],
            "source_listing_id": normalized_item["source_listing_id"],
            "source_url": normalized_item["source_url"],
            "title": normalized_item.get("title"),
            "description": normalized_item.get("description"),
            "price": normalized_item.get("price"),
            "postal_code": normalized_item.get("postal_code"),
            "commune": normalized_item.get("commune"),
            "property_type": normalized_item.get("property_type"),
            "transaction_type": normalized_item.get("transaction_type"),
            "existing_units": normalized_item.get("existing_units"),
            "surface": normalized_item.get("surface"),
            "is_copro": normalized_item.get("is_copro", False),
            "copro_status": normalized_item.get("copro_status"),
            "is_new_build": normalized_item.get("is_new_build", False),
            "is_live_data": normalized_item.get("is_live_data", True),
            "data_origin": normalized_item.get("data_origin"),
            "is_active": normalized_item.get("is_active", True),
            "notes": normalized_item.get("notes"),
            "updated_at": normalized_item.get("updated_at") or utcnow_iso(),
        }
        try:
            self._upsert_by_identity("import_queue", payload)
        except Exception:
            legacy_payload = dict(payload)
            legacy_payload.pop("copro_status", None)
            self._upsert_by_identity("import_queue", legacy_payload)
