import argparse

from config import load_config
from sources.base import SourceConnector
from sources.common import write_listings_jsonl
from storage import build_storage



def run_source_cli(connector: SourceConnector) -> None:
    parser = argparse.ArgumentParser(
        description=f"Collecte les annonces {connector.source_name} vers le format interne."
    )
    parser.add_argument(
        "--search-url",
        help=f"URL {connector.source_name} a collecter",
    )
    parser.add_argument(
        "--html-file",
        help="Fichier HTML local pour un run reproductible sans reseau",
    )
    parser.add_argument(
        "--output",
        default=str(connector.default_output_path),
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
        help="Timeout HTTP en secondes pour une collecte live simple",
    )
    args = parser.parse_args()

    if not args.html_file and not args.search_url:
        raise SystemExit("Fournis --html-file ou --search-url.")

    items = connector.collect(
        search_url=args.search_url,
        html_file=args.html_file,
        timeout=args.timeout,
    )
    output_path = write_listings_jsonl(args.output, items)

    print(f"Collecte {connector.source_name}: {len(items)} annonces")
    print(f"Fichier genere: {output_path}")

    if args.ingest:
        config = load_config()
        storage = build_storage(config)
        for item in items:
            storage.seed_import_queue_item(item)
        print(f"Annonces injectees dans import_queue via {config.backend_name}")
