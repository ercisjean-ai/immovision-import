from config import load_config
from pipeline import run_pipeline
from storage import build_storage


def main() -> None:
    config = load_config()
    storage = build_storage(config)
    stats = {"discovered_count": 0, "queued": 0, "imported": 0}

    print(f"Backend actif: {config.backend_name}")
    if config.sqlite_path:
        print(f"Base SQLite: {config.sqlite_path}")

    try:
        stats = run_pipeline(storage)
        storage.insert_sync_log(
            "success",
            stats["discovered_count"],
            stats["imported"],
        )
        print(
            f"Decouverte: {stats['discovered_count']} URLs, "
            f"import: {stats['imported']} annonces, "
            f"nouvelles URLs envoyees en file: {stats['queued']}."
        )
    except Exception as exc:
        storage.insert_sync_log(
            "error",
            stats["discovered_count"],
            stats["imported"],
            str(exc),
        )
        raise
