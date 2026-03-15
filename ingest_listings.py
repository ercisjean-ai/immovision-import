import argparse
from pathlib import Path

from config import load_config
from listing_feed import load_listing_feed
from storage import build_storage


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingere un feed local d'annonces immobilieres vers import_queue."
    )
    parser.add_argument(
        "feed_path",
        nargs="?",
        default=str(Path("sample_data") / "sample_listings.jsonl"),
        help="Chemin vers un fichier .jsonl ou .json",
    )
    parser.add_argument(
        "--source-name",
        default="FileFeed",
        help="Nom de source par defaut si absent du feed",
    )
    args = parser.parse_args()

    config = load_config()
    storage = build_storage(config)
    items = load_listing_feed(args.feed_path, default_source_name=args.source_name)

    for item in items:
        storage.seed_import_queue_item(item)

    print(f"Feed ingere: {len(items)} annonces depuis {args.feed_path}")
    print(f"Backend actif: {config.backend_name}")


if __name__ == "__main__":
    main()
