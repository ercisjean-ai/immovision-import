from analysis_logic import build_analysis
from parsing import discover_immoweb_urls
from storage import StorageBackend


def run_pipeline(storage: StorageBackend) -> dict[str, int]:
    imported = 0
    discovered_count = 0

    targets = storage.fetch_search_targets()
    for target in targets:
        discovered = discover_immoweb_urls(target)
        for item in discovered:
            storage.upsert_discovered_url(item)
        discovered_count += len(discovered)

    queued = storage.queue_new_discoveries()

    queue = storage.fetch_import_queue()
    for item in queue:
        listing_id = storage.upsert_listing(item)
        item["listing_id"] = listing_id
        storage.upsert_analysis(build_analysis(item))
        storage.insert_price_history(listing_id, item.get("price"))
        imported += 1

    storage.update_source_counts()
    return {"discovered_count": discovered_count, "queued": queued, "imported": imported}
