from analysis_logic import build_analysis
from parsing import discover_immoweb_urls
from storage import StorageBackend



def run_pipeline(storage: StorageBackend) -> dict[str, int]:
    imported = 0
    discovered_count = 0
    new_count = 0
    seen_count = 0
    modified_count = 0

    targets = storage.fetch_search_targets()
    for target in targets:
        discovered = discover_immoweb_urls(target)
        for item in discovered:
            storage.upsert_discovered_url(item)
        discovered_count += len(discovered)

    queued = storage.queue_new_discoveries()

    queue = storage.fetch_import_queue()
    for item in queue:
        upsert_result = storage.upsert_listing(item)
        effective_item = dict(upsert_result.effective_item)
        effective_item["listing_id"] = upsert_result.listing_id
        storage.upsert_analysis(build_analysis(effective_item))
        storage.insert_observation_history(upsert_result, effective_item)
        storage.insert_price_history(upsert_result.listing_id, effective_item.get("price"))
        if upsert_result.observation_status == "new":
            new_count += 1
        elif upsert_result.observation_status == "modified":
            modified_count += 1
        else:
            seen_count += 1
        imported += 1

    storage.update_source_counts()
    return {
        "discovered_count": discovered_count,
        "queued": queued,
        "imported": imported,
        "new": new_count,
        "seen": seen_count,
        "modified": modified_count,
    }
