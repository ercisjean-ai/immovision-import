from typing import Any

from config import utcnow_iso
from normalization import build_listing_payload
from storage_base import StorageBackend

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

            existing = (
                self.client.table("import_queue")
                .select("id")
                .eq("source_listing_id", source_listing_id)
                .limit(1)
                .execute()
                .data
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
                "notes": "URL decouverte automatiquement depuis search_targets",
                "updated_at": utcnow_iso(),
            }

            self.client.table("import_queue").insert(payload).execute()

            (
                self.client.table("discovered_urls")
                .update({"is_queued": True})
                .eq("id", item["id"])
                .execute()
            )
            queued += 1

        return queued

    def upsert_listing(self, item: dict[str, Any]) -> str:
        source_id = self.get_source_id(item["source_name"])
        payload = build_listing_payload(item, source_id)
        result = (
            self.client.table("normalized_listings")
            .upsert(payload, on_conflict="source_listing_id")
            .execute()
        )
        return result.data[0]["id"]

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

    def insert_observation_history(self, listing_id: str, item: dict[str, Any]) -> None:
        try:
            self.client.table("listing_observation_history").insert(
                {
                    "listing_id": listing_id,
                    "source_name": item["source_name"],
                    "source_listing_id": item.get("source_listing_id"),
                    "source_url": item["source_url"],
                    "title": item.get("title"),
                    "price": item.get("price"),
                    "commune": item.get("commune"),
                    "postal_code": item.get("postal_code"),
                    "is_active": bool(item.get("is_active", True)),
                    "observed_at": utcnow_iso(),
                }
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
        payload = {
            "source_name": item["source_name"],
            "source_listing_id": item["source_listing_id"],
            "source_url": item["source_url"],
            "title": item.get("title"),
            "description": item.get("description"),
            "price": item.get("price"),
            "postal_code": item.get("postal_code"),
            "commune": item.get("commune"),
            "property_type": item.get("property_type"),
            "transaction_type": item.get("transaction_type"),
            "existing_units": item.get("existing_units"),
            "surface": item.get("surface"),
            "is_copro": item.get("is_copro", False),
            "is_new_build": item.get("is_new_build", False),
            "is_live_data": item.get("is_live_data", True),
            "is_active": item.get("is_active", True),
            "notes": item.get("notes"),
            "updated_at": item.get("updated_at") or utcnow_iso(),
        }
        self.client.table("import_queue").upsert(
            payload,
            on_conflict="source_listing_id",
        ).execute()
