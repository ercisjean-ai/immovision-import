from typing import Any


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

    def upsert_listing(self, item: dict[str, Any]) -> str:
        raise NotImplementedError

    def upsert_analysis(self, analysis_payload: dict[str, Any]) -> None:
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
