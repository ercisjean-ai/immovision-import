import json
from pathlib import Path

import pytest
import requests

import sources.notaire_source as notaire_source
from listing_feed import load_listing_feed
from sources.notaire_source import (
    NotaireFetchError,
    collect_notaire_listing_result,
    collect_notaire_listings,
    fetch_notaire_search_page,
    format_notaire_coverage_summary,
    write_listings_jsonl,
)


FIXTURE_PATH = Path("sample_data") / "notaire_search_fixture.html"
ORIGINAL_ENRICH_NOTAIRE_DETAILS = notaire_source._enrich_notaire_items_from_detail


@pytest.fixture(autouse=True)
def _disable_notaire_detail_enrichment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        notaire_source,
        "_enrich_notaire_items_from_detail",
        lambda items, timeout: (items, [], 0, []),
    )


def test_collect_notaire_listings_from_fixture() -> None:
    items = collect_notaire_listings(html_file=FIXTURE_PATH)

    assert len(items) == 2

    first = items[0]
    assert first["source_name"] == "Notaire.be"
    assert first["source_listing_id"] == "167609"
    assert first["price"] == 557500.0
    assert first["postal_code"] == "1030"
    assert first["commune"] == "Schaerbeek"
    assert first["property_type"] == "apartment_block"
    assert first["existing_units"] == 4
    assert first["surface"] == 214.0
    assert first["transaction_type"] == "sale"
    assert first["notes"] == "Collecte Notaire immo page | mode: vente de gre a gre"


def test_collect_notaire_roundtrip_to_internal_feed(tmp_path: Path) -> None:
    items = collect_notaire_listings(html_file=FIXTURE_PATH)
    output_path = tmp_path / "notaire_test_output.jsonl"
    write_listings_jsonl(output_path, items)

    loaded = load_listing_feed(output_path, default_source_name="Notaire.be")

    assert [item["source_listing_id"] for item in loaded] == [
        "167609",
        "167610",
    ]
    assert loaded[1]["commune"] == "Mouscron"
    assert loaded[1]["property_type"] == "apartment"
    assert loaded[1]["surface"] == 63.0


def test_collect_notaire_live_prefers_embedded_json_without_mixing_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    estates = [
        {
            "id": 180988,
            "propertyType": "HOUSE",
            "propertyTypeTranslated": "Maisons",
            "saleType": "Vente online",
            "publicationStatus": 1,
            "adStatus": "ACTIVE",
            "municipality": {"municipality_Fr": "Grimbergen"},
            "zip": 1850,
            "title": {"title_Fr": "Immeuble de rapport a Grimbergen"},
            "desc": {"freeText_Fr": "Immeuble de rapport avec 3 appartements et rendement locatif."},
            "price": 330000,
            "searchSurface": 240,
            "numberOfHousingUnits": 3,
        },
        {
            "id": 180989,
            "propertyType": "APARTMENT",
            "propertyTypeTranslated": "Appartements",
            "saleType": "Vente de gre a gre",
            "publicationStatus": 1,
            "adStatus": "ACTIVE",
            "municipality": {"municipality_Fr": "Mouscron"},
            "zip": 7700,
            "title": {"title_Fr": "Appartement 2 chambres a Mouscron"},
            "desc": {"freeText_Fr": "Appartement lumineux de 63 m2 proche des commodites."},
            "price": 140000,
            "searchSurface": 63,
            "numberOfHousingUnits": None,
        },
    ]
    html = f"""
    <html><body>
      <section class=\"results\">
        <article class=\"property-card\">
          <a href=\"/fr/immeuble-de-rapport/a-vendre/1850-grimbergen/180988\">
            <h2>Titre HTML faux 1</h2>
            <div>999 999 EUR</div>
            <div>1082 Berchem-Sainte-Agathe</div>
          </a>
        </article>
        <article class=\"property-card\">
          <a href=\"/fr/appartement/a-vendre/7700-mouscron/180989\">
            <h2>Titre HTML faux 2</h2>
            <div>1 EUR</div>
            <div>1030 Schaerbeek</div>
          </a>
        </article>
      </section>
      <script id=\"estates_json\" type=\"application/json\">{json.dumps(estates)}</script>
    </body></html>
    """

    monkeypatch.setattr(notaire_source, "fetch_notaire_search_page", lambda *_args, **_kwargs: html)

    items = collect_notaire_listings(search_url="https://immo.notaire.be/fr/biens-a-vendre")
    by_id = {item["source_listing_id"]: item for item in items}

    assert list(by_id) == ["180988", "180989"]
    assert by_id["180988"]["source_url"] == (
        "https://immo.notaire.be/fr/immeuble-de-rapport/a-vendre/1850-grimbergen/180988"
    )
    assert by_id["180988"]["title"] == "Immeuble de rapport a Grimbergen"
    assert by_id["180988"]["commune"] == "Grimbergen"
    assert by_id["180988"]["postal_code"] == "1850"
    assert by_id["180988"]["price"] == 330000.0
    assert by_id["180988"]["existing_units"] == 3
    assert by_id["180988"]["surface"] == 240.0
    assert by_id["180988"]["property_type"] == "apartment_block"
    assert by_id["180988"]["data_origin"] == "live"

    assert by_id["180989"]["source_url"] == (
        "https://immo.notaire.be/fr/appartement/a-vendre/7700-mouscron/180989"
    )
    assert by_id["180989"]["title"] == "Appartement 2 chambres a Mouscron"
    assert by_id["180989"]["commune"] == "Mouscron"
    assert by_id["180989"]["postal_code"] == "7700"
    assert by_id["180989"]["price"] == 140000.0
    assert by_id["180989"]["surface"] == 63.0
    assert by_id["180989"]["property_type"] == "apartment"
    assert by_id["180989"]["data_origin"] == "live"


def test_collect_notaire_live_follows_pagination_and_reports_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page1_estates = [
        {
            "id": 200001,
            "propertyType": "HOUSE",
            "publicationStatus": 1,
            "adStatus": "ACTIVE",
            "municipality": {"municipality_Fr": "Grimbergen"},
            "zip": 1850,
            "title": {"title_Fr": "Immeuble de rapport 1"},
            "desc": {"freeText_Fr": "Immeuble de rapport avec 3 logements."},
            "price": 300000,
            "searchSurface": 210,
            "numberOfHousingUnits": 3,
        },
        {
            "id": 200002,
            "propertyType": "COMMERCIAL",
            "publicationStatus": 1,
            "adStatus": "ACTIVE",
            "municipality": {"municipality_Fr": "Vilvoorde"},
            "zip": 1800,
            "title": {"title_Fr": "Maison de commerce 2"},
            "desc": {"freeText_Fr": "Maison de commerce avec 2 unites."},
            "price": 320000,
            "searchSurface": 190,
            "numberOfHousingUnits": 2,
        },
    ]
    page2_estates = [
        {
            "id": 200003,
            "propertyType": "COMMERCIAL",
            "publicationStatus": 1,
            "adStatus": "ACTIVE",
            "municipality": {"municipality_Fr": "Zellik"},
            "zip": 1731,
            "title": {"title_Fr": "Maison de commerce 3"},
            "desc": {"freeText_Fr": "Commerce avec 2 logements."},
            "price": 340000,
            "searchSurface": 220,
            "numberOfHousingUnits": 2,
        },
    ]
    first_url = "https://immo.notaire.be/fr/biens-a-vendre"
    second_url = "https://immo.notaire.be/fr/biens-a-vendre?page=2"
    html_by_url = {
        first_url: f"""
        <html><body>
          <section class=\"results\">
            <a href=\"/fr/immeuble-de-rapport/a-vendre/1850-grimbergen/200001\">Bien 1</a>
            <a href=\"/fr/maison-de-commerce/a-vendre/1800-vilvoorde/200002\">Bien 2</a>
          </section>
          <div class=\"pagination\">
            <span class=\"current\">Page 1 sur 2.</span>
            <a href=\"?page=2\" title=\"Aller a la page suivante\">>></a>
          </div>
          <script id=\"estates_json\" type=\"application/json\">{json.dumps(page1_estates)}</script>
        </body></html>
        """,
        second_url: f"""
        <html><body>
          <section class=\"results\">
            <a href=\"/fr/maison-de-commerce/a-vendre/1731-zellik/200003\">Bien 3</a>
          </section>
          <div class=\"pagination\">
            <span class=\"current\">Page 2 sur 2.</span>
          </div>
          <script id=\"estates_json\" type=\"application/json\">{json.dumps(page2_estates)}</script>
        </body></html>
        """,
    }

    def fake_fetch(url: str, timeout: int = 30) -> str:
        del timeout
        return html_by_url[url]

    monkeypatch.setattr(notaire_source, "fetch_notaire_search_page", fake_fetch)

    result = collect_notaire_listing_result(search_url=first_url, max_pages=4)
    summary = format_notaire_coverage_summary(result)

    assert [item["source_listing_id"] for item in result.items] == ["200001", "200002", "200003"]
    assert result.visited_page_urls == [first_url, second_url]
    assert result.followed_pagination_urls == [second_url]
    assert second_url in result.pagination_urls_detected
    assert result.reported_total_pages == 2
    assert result.estimated_total_results == 4
    assert "pages visitees: 2" in summary
    assert "couverture estimee: 3/4" in summary
    assert all(item["data_origin"] == "live" for item in result.items)


def test_collect_notaire_listings_raises_clear_error_when_live_html_has_no_listings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeResponse:
        status_code = 200
        text = "<html><body>This website is temporarily unavailable due to maintenance.</body></html>"

        def raise_for_status(self) -> None:
            return None

    class FakeSession:
        def get(self, *args, **kwargs):
            return FakeResponse()

    monkeypatch.setattr(requests, "Session", lambda: FakeSession())

    with pytest.raises(NotaireFetchError) as exc_info:
        collect_notaire_listings(search_url="https://immo.notaire.be/fr/biens-a-vendre")

    assert "maintenance" in str(exc_info.value).lower() or "indisponible" in str(exc_info.value).lower()


def test_fetch_notaire_search_page_raises_clear_error_on_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeResponse:
        status_code = 503
        text = "maintenance"

        def raise_for_status(self) -> None:
            raise requests.HTTPError("503 Server Error: Service Unavailable")

    class FakeSession:
        def get(self, *args, **kwargs):
            return FakeResponse()

    monkeypatch.setattr(requests, "Session", lambda: FakeSession())

    with pytest.raises(NotaireFetchError) as exc_info:
        fetch_notaire_search_page("https://immo.notaire.be/fr/biens-a-vendre")

    assert "HTTP 503" in str(exc_info.value)


def test_collect_notaire_live_enriches_from_detail_estate_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_url = "https://immo.notaire.be/fr/biens-a-vendre"
    detail_url = "https://immo.notaire.be/fr/maison-de-commerce/a-vendre/1800-vilvoorde/200010"
    estates = [
        {
            "id": 200010,
            "propertyType": "HOUSE",
            "propertyTypeTranslated": "Maisons",
            "saleType": "Vente online",
            "publicationStatus": 1,
            "adStatus": "ACTIVE",
            "municipality": {"municipality_Fr": "Vilvoorde"},
            "zip": 1800,
            "title": {"title_Fr": "Maison de commerce a Vilvoorde"},
            "desc": {"freeText_Fr": "Annonce listing courte."},
            "price": 320000,
            "searchSurface": 180,
            "numberOfHousingUnits": None,
        },
    ]
    detail_estate = {
        "id": 200010,
        "propertyType": "COMMERCIAL",
        "propertyTypeTranslated": "Commerce",
        "saleType": "Vente online",
        "publicationStatus": 1,
        "adStatus": "ACTIVE",
        "municipality": {"municipality_Fr": "Vilvoorde"},
        "zip": 1800,
        "title": {"title_Fr": "Maison de commerce a Vilvoorde"},
        "desc": {"freeText_Fr": "Maison de commerce avec 3 logements et 215 m2 exploitables."},
        "price": 320000,
        "searchSurface": 215,
        "numberOfHousingUnits": 3,
    }
    html = f"""
    <html><body>
      <section class="results">
        <a href="/fr/maison-de-commerce/a-vendre/1800-vilvoorde/200010">Bien 1</a>
      </section>
      <script id="estates_json" type="application/json">{json.dumps(estates)}</script>
    </body></html>
    """
    detail_html = f"""
    <html><body>
      <script id="estate_json" type="application/json">{json.dumps(detail_estate)}</script>
    </body></html>
    """

    monkeypatch.setattr(
        notaire_source,
        "_enrich_notaire_items_from_detail",
        ORIGINAL_ENRICH_NOTAIRE_DETAILS,
    )
    monkeypatch.setattr(notaire_source, "fetch_notaire_search_page", lambda *_args, **_kwargs: html)
    monkeypatch.setattr(
        notaire_source,
        "_fetch_notaire_detail_html",
        lambda _session, source_url, timeout=30: detail_html if source_url == detail_url else None,
    )

    result = collect_notaire_listing_result(search_url=first_url, max_pages=1)
    item = result.items[0]

    assert len(result.detail_urls_followed) == 1
    assert result.detail_enriched_count == 1
    assert item["source_url"] == detail_url
    assert item["property_type"] == "commercial_house"
    assert item["existing_units"] == 3
    assert item["surface"] == 215.0
    assert item["notes"] == "Collecte Notaire detail | mode: vente online"
