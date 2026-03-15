import argparse
from pathlib import Path

from config import load_config
from sources.immoweb_browser_source import collect_immoweb_browser_listings
from sources.immoweb_source import (
    ImmowebFetchError,
    collect_immoweb_listings,
    write_listings_jsonl,
)
from storage import build_storage



def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collecte des annonces Immoweb et les convertit vers le format interne."
    )
    parser.add_argument(
        "--search-url",
        help="URL de recherche Immoweb a collecter",
    )
    parser.add_argument(
        "--html-file",
        help="Fichier HTML local pour un run reproductible sans reseau",
    )
    parser.add_argument(
        "--output",
        default=str(Path("sample_data") / "immoweb_latest.jsonl"),
        help="Fichier de sortie JSONL",
    )
    parser.add_argument(
        "--ingest",
        action="store_true",
        help="Injecte directement les annonces dans import_queue",
    )
    parser.add_argument(
        "--http",
        action="store_true",
        help="Force l'ancien mode HTTP requests au lieu du mode navigateur Playwright",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Lance Chromium en mode visible pour le diagnostic live",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=45000,
        help="Timeout Playwright pour la collecte live en millisecondes",
    )
    args = parser.parse_args()

    if not args.html_file and not args.search_url:
        raise SystemExit("Fournis --html-file ou --search-url.")

    try:
        if args.html_file:
            items = collect_immoweb_listings(html_file=args.html_file)
            collection_mode = "fixture"
        elif args.http:
            items = collect_immoweb_listings(search_url=args.search_url)
            collection_mode = "http"
        else:
            items = collect_immoweb_browser_listings(
                args.search_url,
                timeout_ms=args.timeout_ms,
                headless=not args.headed,
            )
            collection_mode = "browser"
    except ImmowebFetchError as exc:
        raise SystemExit(f"Collecte Immoweb impossible: {exc}") from exc

    output_path = write_listings_jsonl(args.output, items)

    print(f"Collecte Immoweb ({collection_mode}): {len(items)} annonces")
    print(f"Fichier genere: {output_path}")

    if args.ingest:
        config = load_config()
        storage = build_storage(config)
        for item in items:
            storage.seed_import_queue_item(item)
        print(f"Annonces injectees dans import_queue via {config.backend_name}")


if __name__ == "__main__":
    main()
