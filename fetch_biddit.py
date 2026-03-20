import argparse
from pathlib import Path

from config import load_config
from sources.common import write_listings_jsonl
from sources.biddit_browser_source import (
    collect_biddit_browser_listing_result,
    format_biddit_browser_coverage_summary,
)
from sources.biddit_source import BidditFetchError, collect_biddit_listings
from storage import build_storage



def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collecte des annonces Biddit et les convertit vers le format interne."
    )
    parser.add_argument(
        "--search-url",
        help="URL de recherche Biddit a collecter",
    )
    parser.add_argument(
        "--html-file",
        help="Fichier HTML local pour un run reproductible sans reseau",
    )
    parser.add_argument(
        "--output",
        default=str(Path("sample_data") / "biddit_latest.jsonl"),
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
        "--timeout",
        dest="timeout_ms",
        type=int,
        default=45000,
        help="Timeout navigateur pour la collecte live en millisecondes",
    )
    parser.add_argument(
        "--debug-save-html",
        action="store_true",
        help="Sauvegarde le HTML rendu en cas d'echec live",
    )
    parser.add_argument(
        "--debug-screenshot",
        action="store_true",
        help="Sauvegarde un screenshot en cas d'echec live",
    )
    parser.add_argument(
        "--debug-dir",
        default=str(Path("debug") / "biddit"),
        help="Dossier de sortie des artefacts de debug Playwright",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=4,
        help="Nombre maximum de pages de resultats Biddit a parcourir en mode navigateur",
    )
    args = parser.parse_args()

    if not args.html_file and not args.search_url:
        raise SystemExit("Fournis --html-file ou --search-url.")

    browser_collection_result = None
    try:
        if args.html_file:
            items = collect_biddit_listings(html_file=args.html_file)
            collection_mode = "fixture"
        elif args.http:
            items = collect_biddit_listings(
                search_url=args.search_url,
                timeout=max(1, args.timeout_ms // 1000),
            )
            collection_mode = "http"
            browser_collection_result = None
        else:
            browser_collection_result = collect_biddit_browser_listing_result(
                args.search_url,
                timeout_ms=args.timeout_ms,
                headless=not args.headed,
                debug_save_html=args.debug_save_html,
                debug_screenshot=args.debug_screenshot,
                debug_dir=args.debug_dir,
                max_pages=max(1, args.max_pages),
            )
            items = browser_collection_result.items
            collection_mode = "browser"
    except BidditFetchError as exc:
        raise SystemExit(f"Collecte Biddit impossible: {exc}") from exc

    output_path = write_listings_jsonl(args.output, items)

    print(f"Collecte Biddit ({collection_mode}): {len(items)} annonces")
    if collection_mode == "browser" and browser_collection_result is not None:
        print(format_biddit_browser_coverage_summary(browser_collection_result))
    print(f"Fichier genere: {output_path}")

    if args.ingest:
        config = load_config()
        storage = build_storage(config)
        for item in items:
            storage.seed_import_queue_item(item)
        print(f"Annonces injectees dans import_queue via {config.backend_name}")


if __name__ == "__main__":
    main()
