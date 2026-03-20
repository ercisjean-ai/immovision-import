import json
from pathlib import Path

import pytest

from listing_feed import load_listing_feed
import sources.immovlan_source as immovlan_source
from sources.immovlan_source import (
    ImmovlanFetchError,
    collect_immovlan_listing_result,
    collect_immovlan_listings,
    format_immovlan_coverage_summary,
    write_listings_jsonl,
)


FIXTURE_PATH = Path("sample_data") / "immovlan_search_fixture.html"


def test_collect_immovlan_listings_from_fixture() -> None:
    items = collect_immovlan_listings(html_file=FIXTURE_PATH)

    assert len(items) == 2
    first = items[0]
    assert first["source_name"] == "Immovlan"
    assert first["source_listing_id"] == "VWD00001"
    assert first["source_url"] == "https://immovlan.be/fr/detail/maison/a-vendre/1850/grimbergen/vwd00001"
    assert first["price"] == 330000.0
    assert first["postal_code"] == "1850"
    assert first["commune"] == "Grimbergen"
    assert first["property_type"] == "apartment_block"
    assert first["existing_units"] == 3
    assert first["surface"] == 240.0
    assert first["data_origin"] == "fixture"


def test_collect_immovlan_roundtrip_to_internal_feed(tmp_path: Path) -> None:
    items = collect_immovlan_listings(html_file=FIXTURE_PATH)
    output_path = tmp_path / "immovlan_output.jsonl"
    write_listings_jsonl(output_path, items)

    loaded = load_listing_feed(output_path, default_source_name="Immovlan")

    assert [item["source_listing_id"] for item in loaded] == ["VWD00001", "VWD00002"]
    assert loaded[1]["commune"] == "Mouscron"
    assert loaded[1]["property_type"] == "apartment"


def test_collect_immovlan_live_follows_pagination_and_enriches_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_url = "https://immovlan.be/fr/immobilier?transactiontypes=a-vendre"
    second_url = first_url + "&page=2"
    page1 = """
    <html><body>
      <div>60 resultats (1 - 20)</div>
      <article class="list-view-item mb-3 card card-border">
        <div class="card-body position-relative">
          <h2><a href="https://immovlan.be/fr/detail/maison/a-vendre/1850/grimbergen/vwd10001" title="Maison a vendre - Grimbergen VWD10001">Maison a vendre - Grimbergen VWD10001</a></h2>
          <p itemprop="address"><span itemprop="postalCode">1850</span><span itemprop="addressLocality">Grimbergen</span></p>
          <strong class="list-item-price">330 000 €</strong>
          <p class="list-item-description"><span itemprop="description">Immeuble de rapport avec 3 unites.</span></p>
          <div class="list-item-details"><span class="property-highlight"><strong>3</strong> unite(s)</span><span class="property-highlight"><strong>240</strong> m²</span></div>
        </div>
      </article>
      <article class="list-view-item mb-3 card card-border">
        <div class="card-body position-relative">
          <h2><a href="https://immovlan.be/fr/detail/commerce/a-vendre/1800/vilvoorde/vwd10002" title="Commerce a vendre - Vilvoorde VWD10002">Commerce a vendre - Vilvoorde VWD10002</a></h2>
          <p itemprop="address"><span itemprop="postalCode">1800</span><span itemprop="addressLocality">Vilvoorde</span></p>
          <strong class="list-item-price">340 000 €</strong>
          <p class="list-item-description"><span itemprop="description">Maison de commerce avec 2 logements.</span></p>
          <div class="list-item-details"><span class="property-highlight"><strong>2</strong> unite(s)</span><span class="property-highlight"><strong>220</strong> m²</span></div>
        </div>
      </article>
      <article class="list-view-item mb-3 card card-border">
        <div class="card-body position-relative">
          <h2><a href="https://immovlan.be/fr/projectdetail/99999-000001">Projet</a></h2>
        </div>
      </article>
      <nav class="pagination"><a href="?transactiontypes=a-vendre&page=2">2</a></nav>
    </body></html>
    """
    page2 = """
    <html><body>
      <div>60 resultats (21 - 40)</div>
      <article class="list-view-item mb-3 card card-border">
        <div class="card-body position-relative">
          <h2><a href="https://immovlan.be/fr/detail/appartement/a-vendre/7700/mouscron/vwd10003" title="Appartement a vendre - Mouscron VWD10003">Appartement a vendre - Mouscron VWD10003</a></h2>
          <p itemprop="address"><span itemprop="postalCode">7700</span><span itemprop="addressLocality">Mouscron</span></p>
          <strong class="list-item-price">140 000 €</strong>
          <p class="list-item-description"><span itemprop="description">Appartement lumineux de 63 m².</span></p>
        </div>
      </article>
    </body></html>
    """
    detail_html_by_url = {
        "https://immovlan.be/fr/detail/maison/a-vendre/1850/grimbergen/vwd10001": '''
        <html><body><h1>Maison a vendre - Grimbergen VWD10001</h1>
        <script type="application/ld+json">{"@context":"https://schema.org","@type":"WebPage","name":"Maison a vendre a Grimbergen (VWD10001)","description":"Maison a vendre | 330000 EUR | 3 unites"}</script>
        <script type="application/ld+json">{"@context":"https://schema.org","@type":"House","description":"Immeuble de rapport avec 3 unites et commerce en rez.","floorSize":{"@type":"QuantitativeValue","value":240},"address":{"@type":"PostalAddress","addressLocality":"Grimbergen","postalCode":"1850"}}</script>
        <script type="application/ld+json">{"@context":"https://schema.org","@type":"SellAction","price":330000,"location":{"@type":"PostalAddress","addressLocality":"Grimbergen","postalCode":"1850"}}</script>
        </body></html>
        ''',
        "https://immovlan.be/fr/detail/commerce/a-vendre/1800/vilvoorde/vwd10002": '''
        <html><body><h1>Commerce a vendre - Vilvoorde VWD10002</h1>
        <script type="application/ld+json">{"@context":"https://schema.org","@type":"WebPage","name":"Commerce a vendre a Vilvoorde (VWD10002)"}</script>
        <script type="application/ld+json">{"@context":"https://schema.org","@type":"House","description":"Maison de commerce avec 2 logements.","floorSize":{"@type":"QuantitativeValue","value":220},"address":{"@type":"PostalAddress","addressLocality":"Vilvoorde","postalCode":"1800"}}</script>
        <script type="application/ld+json">{"@context":"https://schema.org","@type":"SellAction","price":340000,"location":{"@type":"PostalAddress","addressLocality":"Vilvoorde","postalCode":"1800"}}</script>
        </body></html>
        ''',
        "https://immovlan.be/fr/detail/appartement/a-vendre/7700/mouscron/vwd10003": '''
        <html><body><h1>Appartement a vendre - Mouscron VWD10003</h1>
        <script type="application/ld+json">{"@context":"https://schema.org","@type":"WebPage","name":"Appartement a vendre a Mouscron (VWD10003)"}</script>
        <script type="application/ld+json">{"@context":"https://schema.org","@type":"Apartment","description":"Appartement lumineux de 63 m² proche des commodites.","floorSize":{"@type":"QuantitativeValue","value":63},"address":{"@type":"PostalAddress","addressLocality":"Mouscron","postalCode":"7700"}}</script>
        <script type="application/ld+json">{"@context":"https://schema.org","@type":"SellAction","price":140000,"location":{"@type":"PostalAddress","addressLocality":"Mouscron","postalCode":"7700"}}</script>
        </body></html>
        ''',
    }

    def fake_fetch(url: str, timeout: int = 30) -> str:
        del timeout
        return {first_url: page1, second_url: page2}[url]

    monkeypatch.setattr(immovlan_source, "fetch_immovlan_search_page", fake_fetch)
    monkeypatch.setattr(
        immovlan_source,
        "_fetch_immovlan_detail_html",
        lambda source_url, timeout=30: detail_html_by_url[source_url],
    )

    result = collect_immovlan_listing_result(search_url=first_url, max_pages=4)
    summary = format_immovlan_coverage_summary(result)
    by_id = {item["source_listing_id"]: item for item in result.items}

    assert [item["source_listing_id"] for item in result.items] == ["VWD10001", "VWD10002", "VWD10003"]
    assert result.visited_page_urls == [first_url, second_url]
    assert result.followed_pagination_urls == [second_url]
    assert len(result.detail_urls_followed) == 3
    assert result.detail_enriched_count == 3
    assert result.ignored_project_urls == ["https://immovlan.be/fr/projectdetail/99999-000001"]
    assert result.reported_total_results == 60
    assert by_id["VWD10001"]["property_type"] == "apartment_block"
    assert by_id["VWD10001"]["existing_units"] == 3
    assert by_id["VWD10002"]["property_type"] == "commercial_house"
    assert by_id["VWD10003"]["property_type"] == "apartment"
    assert "projets ignores: 1" in summary
    assert "details enrichis: 3" in summary


def test_collect_immovlan_live_raises_clear_error_when_no_listings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        immovlan_source,
        "fetch_immovlan_search_page",
        lambda *_args, **_kwargs: "<html><body><div>Access denied</div></body></html>",
    )

    with pytest.raises(ImmovlanFetchError) as exc_info:
        collect_immovlan_listings(search_url="https://immovlan.be/fr/immobilier?transactiontypes=a-vendre")

    assert "acces refuse" in str(exc_info.value).lower() or "anti-bot" in str(exc_info.value).lower()
