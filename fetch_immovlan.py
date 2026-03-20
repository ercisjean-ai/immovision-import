import argparse
from pathlib import Path

from config import load_config
from sources.common import write_listings_jsonl
from sources.immovlan_source import (
    ImmovlanFetchError,
    collect_immovlan_listing_result,
    collect_immovlan_listings,
    format_immovlan_coverage_summary,
)
from storage import build_storage


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collecte des annonces Immovlan et les convertit vers le format interne."
    )
    parser.add_argument(
        "--search-url",
        help="URL de recherche Immovlan a collecter",
    )
    parser.add_argument(
        "--html-file",
        help="Fichier HTML local pour un run reproductible sans reseau",
    )
    parser.add_argument(
        "--output",
        default=str(Path("sample_data") / "immovlan_latest.jsonl"),
        help="Fichier de sortie JSONL",
    )
    parser.add_argument(
        "--ingest",
        action="store_true",
        help="Injecte directement les annonces dans import_queue",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout HTTP en secondes pour la collecte live",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=4,
        help="Nombre maximum de pages Immovlan a parcourir en mode live",
    )
    args = parser.parse_args()

    if not args.html_file and not args.search_url:
        raise SystemExit("Fournis --html-file ou --search-url.")

    collection_result = None
    try:
        if args.html_file:
            items = collect_immovlan_listings(html_file=args.html_file, timeout=args.timeout)
            collection_mode = "fixture"
        else:
            collection_result = collect_immovlan_listing_result(
                search_url=args.search_url,
                timeout=args.timeout,
                max_pages=max(1, args.max_pages),
            )
            items = collection_result.items
            collection_mode = "live"
    except ImmovlanFetchError as exc:
        raise SystemExit(f"Collecte Immovlan impossible: {exc}") from exc

    output_path = write_listings_jsonl(args.output, items)

    print(f"Collecte Immovlan ({collection_mode}): {len(items)} annonces")
    if collection_result is not None:
        print(format_immovlan_coverage_summary(collection_result))
    print(f"Fichier genere: {output_path}")

    if args.ingest:
        config = load_config()
        storage = build_storage(config)
        for item in items:
            storage.seed_import_queue_item(item)
        print(f"Annonces injectees dans import_queue via {config.backend_name}")


if __name__ == "__main__":
    main()
