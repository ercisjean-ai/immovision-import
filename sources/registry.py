from pathlib import Path
from typing import Any

from sources.base import SourceConnector
from sources.biddit_source import collect_biddit_listings
from sources.immoweb_source import collect_immoweb_listings
from sources.immovlan_source import collect_immovlan_listings
from sources.notaire_source import collect_notaire_listings



def _collect_immoweb_registry(*, search_url: str | None = None, html: str | None = None, html_file: str | Path | None = None, timeout: int = 30) -> list[dict[str, Any]]:
    del timeout
    return collect_immoweb_listings(search_url=search_url, html=html, html_file=html_file)


SOURCE_CONNECTORS = {
    "immoweb": SourceConnector(
        slug="immoweb",
        source_name="Immoweb",
        description="Connecteur immobilier portal HTML/browser existant",
        default_output_path=Path("sample_data") / "immoweb_latest.jsonl",
        fixture_path=Path("sample_data") / "immoweb_search_fixture.html",
        collect=_collect_immoweb_registry,
    ),
    "biddit": SourceConnector(
        slug="biddit",
        source_name="Biddit",
        description="Connecteur Biddit fixture-first avec live browser Playwright et fallback HTTP",
        default_output_path=Path("sample_data") / "biddit_latest.jsonl",
        fixture_path=Path("sample_data") / "biddit_search_fixture.html",
        collect=collect_biddit_listings,
    ),
    "immovlan": SourceConnector(
        slug="immovlan",
        source_name="Immovlan",
        description="Connecteur Immovlan HTML avec pagination et enrichissement detail JSON-LD",
        default_output_path=Path("sample_data") / "immovlan_latest.jsonl",
        fixture_path=Path("sample_data") / "immovlan_search_fixture.html",
        collect=collect_immovlan_listings,
    ),
    "notaire": SourceConnector(
        slug="notaire",
        source_name="Notaire.be",
        description="Connecteur V1 des annonces notariales immo.notaire.be",
        default_output_path=Path("sample_data") / "notaire_latest.jsonl",
        fixture_path=Path("sample_data") / "notaire_search_fixture.html",
        collect=collect_notaire_listings,
    ),
}



def get_source_connector(slug: str) -> SourceConnector:
    try:
        return SOURCE_CONNECTORS[slug]
    except KeyError as exc:
        known = ", ".join(sorted(SOURCE_CONNECTORS))
        raise KeyError(f"Source inconnue: {slug}. Sources disponibles: {known}") from exc



def list_source_connectors() -> list[SourceConnector]:
    return [SOURCE_CONNECTORS[slug] for slug in sorted(SOURCE_CONNECTORS)]

