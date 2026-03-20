from pathlib import Path

import pytest

import sources.biddit_browser_source as browser_source
from sources.biddit_browser_source import BidditBrowserRenderResult
from sources.biddit_source import BidditFetchError


FIXTURE_HTML = (Path("sample_data") / "biddit_search_fixture.html").read_text(encoding="utf-8")
ORIGINAL_ENRICH_BIDDIT_DETAILS = browser_source._enrich_biddit_items_from_detail_api


@pytest.fixture(autouse=True)
def _disable_biddit_detail_enrichment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        browser_source,
        "_enrich_biddit_items_from_detail_api",
        lambda items, timeout: (items, [], 0, []),
    )


def test_collect_biddit_browser_listings_uses_rendered_html(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        browser_source,
        "render_biddit_search_page_with_playwright",
        lambda *args, **kwargs: BidditBrowserRenderResult(
            html=FIXTURE_HTML,
            final_url="https://www.biddit.be/fr/search",
        ),
    )

    items = browser_source.collect_biddit_browser_listings(
        "https://www.biddit.be/fr/search"
    )

    assert len(items) == 2
    assert items[0]["source_name"] == "Biddit"
    assert items[0]["source_listing_id"] == "271234"
    assert items[1]["commune"] == "Jambes"



def test_collect_biddit_browser_listings_can_fallback_to_response_html(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        browser_source,
        "render_biddit_search_page_with_playwright",
        lambda *args, **kwargs: BidditBrowserRenderResult(
            html="",
            response_htmls=[FIXTURE_HTML],
            final_url="https://www.biddit.be/fr/search",
            navigation_timed_out=True,
        ),
    )

    items = browser_source.collect_biddit_browser_listings(
        "https://www.biddit.be/fr/search"
    )

    assert len(items) == 2
    assert items[0]["source_listing_id"] == "271234"


def test_collect_biddit_browser_listings_preserves_html_location_over_polluted_network_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        browser_source,
        "render_biddit_search_page_with_playwright",
        lambda *args, **kwargs: BidditBrowserRenderResult(
            html=FIXTURE_HTML,
            final_url="https://www.biddit.be/fr/search",
            network_payloads=[
                {
                    "results": [
                        {
                            "url": "https://www.biddit.be/fr/catalog/detail/271234",
                            "title": "Immeuble de rapport a Schaerbeek",
                            "description": "Bloc parasite Hachy",
                            "postalCode": "6720",
                            "city": "Hachy",
                            "price": 999999,
                            "propertyType": "building",
                            "units": 9,
                        }
                    ]
                }
            ],
        ),
    )

    items = browser_source.collect_biddit_browser_listings(
        "https://www.biddit.be/fr/search"
    )
    first = next(item for item in items if item["source_listing_id"] == "271234")
    second = next(item for item in items if item["source_listing_id"] == "271235")

    assert first["postal_code"] == "1030"
    assert first["commune"] == "Schaerbeek"
    assert second["postal_code"] == "5100"
    assert second["commune"] == "Jambes"


def test_collect_biddit_browser_listings_merges_multiple_page_htmls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    second_page_html = FIXTURE_HTML.replace("271234", "271236").replace(
        "Maison 3 facades a Jambes",
        "Maison supplementaire a Jambes",
    )
    monkeypatch.setattr(
        browser_source,
        "render_biddit_search_page_with_playwright",
        lambda *args, **kwargs: BidditBrowserRenderResult(
            html=FIXTURE_HTML,
            page_htmls=[FIXTURE_HTML, second_page_html],
            visited_page_urls=[
                "https://www.biddit.be/fr/search?page=1",
                "https://www.biddit.be/fr/search?page=2",
            ],
            followed_pagination_urls=["https://www.biddit.be/fr/search?page=2"],
            final_url="https://www.biddit.be/fr/search?page=2",
            reported_total_results=3,
        ),
    )

    result = browser_source.collect_biddit_browser_listing_result(
        "https://www.biddit.be/fr/search"
    )

    assert len(result.items) == 3
    assert len(result.visited_page_urls) == 2
    assert result.reported_total_results == 3



def test_extract_biddit_network_listings_from_payloads() -> None:
    payloads = [
        {
            "results": [
                {
                    "url": "https://www.biddit.be/fr/search/maison/jambes/271235",
                    "title": "Maison 3 facades a Jambes",
                    "description": "Maison avec jardin et garage",
                    "price": 240000,
                    "postalCode": "5100",
                    "city": "Jambes",
                    "surface": 198,
                    "propertyType": "house",
                }
            ]
        }
    ]

    items = browser_source.extract_biddit_network_listings(payloads)

    assert len(items) == 1
    assert items[0]["source_listing_id"] == "271235"
    assert items[0]["commune"] == "Jambes"
    assert items[0]["price"] == 240000.0
    assert items[0]["property_type"] == "house"


def test_extract_biddit_pagination_urls_prefers_next_search_pages() -> None:
    html = """
    <html>
      <body>
        <nav class="pagination">
          <a href="/fr/catalog/detail/271234">detail</a>
          <a href="/fr/search?page=2" rel="next">Suivant</a>
          <a href="/fr/search?page=3">3</a>
        </nav>
      </body>
    </html>
    """

    urls = browser_source._extract_pagination_urls_from_html(  # noqa: SLF001
        html,
        base_url="https://www.biddit.be/fr/search?page=1",
        current_url="https://www.biddit.be/fr/search?page=1",
    )

    assert urls[:2] == [
        "https://www.biddit.be/fr/search?page=2",
        "https://www.biddit.be/fr/search?page=3",
    ]



def test_collect_biddit_browser_listings_raises_clear_error_on_rendered_empty_html(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        browser_source,
        "render_biddit_search_page_with_playwright",
        lambda *args, **kwargs: BidditBrowserRenderResult(
            html="<html><body>Please enable JavaScript to continue.</body></html>",
            final_url="https://www.biddit.be/fr/search",
            page_title="JavaScript required",
        ),
    )

    with pytest.raises(BidditFetchError) as exc_info:
        browser_source.collect_biddit_browser_listings(
            "https://www.biddit.be/fr/search"
        )

    message = str(exc_info.value)
    assert "Aucune annonce extraite" in message
    assert "JavaScript" in message or "markup" in message or "aucun selecteur" in message.lower()



def test_collect_biddit_browser_listings_saves_debug_artifacts_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        browser_source,
        "render_biddit_search_page_with_playwright",
        lambda *args, **kwargs: BidditBrowserRenderResult(
            html="",
            final_url="about:blank",
            page_title="",
            screenshot_bytes=b"fakepng",
            navigation_timed_out=True,
        ),
    )

    with pytest.raises(BidditFetchError) as exc_info:
        browser_source.collect_biddit_browser_listings(
            "https://www.biddit.be/fr/search",
            debug_save_html=True,
            debug_screenshot=True,
            debug_dir=tmp_path,
        )

    message = str(exc_info.value)
    assert "Artefacts enregistres" in message
    html_files = list(tmp_path.glob("*.html"))
    png_files = list(tmp_path.glob("*.png"))
    assert len(html_files) == 1
    assert len(png_files) == 1
    assert "Biddit debug capture" in html_files[0].read_text(encoding="utf-8")



def test_diagnose_biddit_browser_failure_mentions_network_payloads() -> None:
    diagnostic = browser_source.diagnose_biddit_browser_failure(
        BidditBrowserRenderResult(
            html="<html><body>Biddit search shell</body></html>",
            final_url="https://www.biddit.be/fr/search",
            page_title="Biddit",
            network_payloads=[{"results": []}],
        )
    )

    assert "reponses json" in diagnostic.lower()


def test_format_biddit_browser_coverage_summary_mentions_pages_and_total() -> None:
    result = browser_source.BidditBrowserCollectionResult(
        items=[{"source_listing_id": "1"}, {"source_listing_id": "2"}],
        final_url="https://www.biddit.be/fr/search?page=2",
        visited_page_urls=[
            "https://www.biddit.be/fr/search?page=1",
            "https://www.biddit.be/fr/search?page=2",
        ],
        pagination_urls_detected=["https://www.biddit.be/fr/search?page=2"],
        followed_pagination_urls=["https://www.biddit.be/fr/search?page=2"],
        reported_total_results=4,
        coverage_notes=["chargement additionnel detecte: 30 -> 45"],
    )

    summary = browser_source.format_biddit_browser_coverage_summary(result)

    assert "annonces uniques: 2" in summary
    assert "pages visitees: 2" in summary
    assert "pagination candidates: 1" in summary
    assert "couverture: 2/4" in summary


def test_format_biddit_browser_coverage_summary_mentions_no_pagination_note() -> None:
    result = browser_source.BidditBrowserCollectionResult(
        items=[{"source_listing_id": "1"}],
        final_url="https://www.biddit.be/fr/search",
        visited_page_urls=["https://www.biddit.be/fr/search"],
        coverage_notes=[
            "aucune pagination DOM detectee; la couverture etendue ne peut pas depasser la page courante"
        ],
    )

    summary = browser_source.format_biddit_browser_coverage_summary(result)

    assert "pages visitees: 1" in summary
    assert "aucune pagination DOM detectee" in summary


def test_extract_biddit_reported_total_from_content_payloads() -> None:
    payloads = [
        {
            "_url": "https://www.biddit.be/api/eco/search-service/lot/_search?page=0&pageSize=30&isNonActive=false&sort=",
            "_payload": {
                "content": [
                    {"url": "https://www.biddit.be/fr/catalog/detail/271234"},
                    {"url": "https://www.biddit.be/fr/catalog/detail/271235"},
                ],
                "totalPages": 39,
                "totalElements": 1168,
            },
        }
    ]

    total = browser_source._extract_reported_total_from_network_payloads(payloads)  # noqa: SLF001

    assert total == 1168



def test_expand_biddit_api_pages_follows_search_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    network_payloads = [
        {
            "_url": "https://www.biddit.be/api/eco/search-service/lot/_search?page=0&pageSize=30&isNonActive=false&sort=",
            "_content_type": "application/json",
            "_payload": {
                "content": [
                    {
                        "url": "https://www.biddit.be/fr/catalog/detail/271234",
                        "title": "Immeuble 1",
                        "description": "Bloc 1",
                        "price": 300000,
                        "postalCode": "1030",
                        "city": "Schaerbeek",
                        "units": 3,
                        "propertyType": "building",
                    }
                ],
                "totalPages": 3,
                "totalElements": 90,
            },
        }
    ]

    payloads_by_url = {
        "https://www.biddit.be/api/eco/search-service/lot/_search?page=1&pageSize=30&isNonActive=false&sort=": {
            "content": [
                {
                    "url": "https://www.biddit.be/fr/catalog/detail/271235",
                    "title": "Immeuble 2",
                    "description": "Bloc 2",
                    "price": 310000,
                    "postalCode": "1850",
                    "city": "Grimbergen",
                    "units": 3,
                    "propertyType": "building",
                }
            ],
            "totalPages": 3,
            "totalElements": 90,
        },
        "https://www.biddit.be/api/eco/search-service/lot/_search?page=2&pageSize=30&isNonActive=false&sort=": {
            "content": [
                {
                    "url": "https://www.biddit.be/fr/catalog/detail/271236",
                    "title": "Immeuble 3",
                    "description": "Bloc 3",
                    "price": 320000,
                    "postalCode": "1800",
                    "city": "Vilvoorde",
                    "units": 2,
                    "propertyType": "commerce house",
                }
            ],
            "totalPages": 3,
            "totalElements": 90,
        },
    }

    class FakeResponse:
        def __init__(self, url: str) -> None:
            self.url = url
            self.text = browser_source.json.dumps(payloads_by_url[url])
            self.headers = {"content-type": "application/json"}

        def raise_for_status(self) -> None:
            return None

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None, timeout=None, allow_redirects=True):
            del headers, timeout, allow_redirects
            return FakeResponse(url)

    monkeypatch.setattr(browser_source.requests, "Session", lambda: FakeSession())

    coverage_notes: list[str] = []
    followed_urls, total = browser_source._expand_biddit_api_pages(  # noqa: SLF001
        network_payloads,
        max_pages=3,
        coverage_notes=coverage_notes,
    )
    items = browser_source.extract_biddit_network_listings(network_payloads)

    assert len(followed_urls) == 2
    assert total == 90
    assert len(network_payloads) == 3
    assert len(items) == 3
    assert any("pages API Biddit suivies: 2" in note for note in coverage_notes)



def test_format_biddit_browser_coverage_summary_mentions_api_pages() -> None:
    result = browser_source.BidditBrowserCollectionResult(
        items=[{"source_listing_id": "1"}, {"source_listing_id": "2"}],
        final_url="https://www.biddit.be/fr/search",
        visited_page_urls=["https://www.biddit.be/fr/search"],
        followed_api_page_urls=[
            "https://www.biddit.be/api/eco/search-service/lot/_search?page=1&pageSize=30&isNonActive=false&sort=",
            "https://www.biddit.be/api/eco/search-service/lot/_search?page=2&pageSize=30&isNonActive=false&sort=",
        ],
        reported_total_results=90,
    )

    summary = browser_source.format_biddit_browser_coverage_summary(result)

    assert "pages API suivies: 2" in summary
    assert "couverture: 2/90" in summary


def test_collect_biddit_browser_listings_prefers_verified_api_fields_over_polluted_dom_page_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    polluted_html = FIXTURE_HTML.replace("1030 Schaerbeek", "6720 Hachy").replace("360000", "150000")
    monkeypatch.setattr(
        browser_source,
        "render_biddit_search_page_with_playwright",
        lambda *args, **kwargs: BidditBrowserRenderResult(
            html=polluted_html,
            final_url="https://www.biddit.be/fr/search",
            network_payloads=[
                {
                    "_url": "https://www.biddit.be/api/eco/search-service/lot/_search?page=0&pageSize=30&isNonActive=false&sort=",
                    "_payload": {
                        "content": [
                            {
                                "content": {
                                    "sellingPrice": 360000,
                                    "withdrawn": False,
                                    "properties": [
                                        {
                                            "reference": "271234",
                                            "propertyType": "HOUSE",
                                            "livingSurfaceArea": 214.0,
                                            "title": {"fr": "Immeuble de rapport a Schaerbeek"},
                                            "address": {
                                                "postalCode": "1030",
                                                "municipality": {"fr": "Schaerbeek"},
                                            },
                                        }
                                    ],
                                }
                            }
                        ],
                        "totalPages": 1,
                        "totalElements": 1,
                    },
                }
            ],
        ),
    )

    items = browser_source.collect_biddit_browser_listings("https://www.biddit.be/fr/search")
    first = next(item for item in items if item["source_listing_id"] == "271234")

    assert first["postal_code"] == "1030"
    assert first["commune"] == "Schaerbeek"
    assert first["price"] == 360000.0
    assert first["notes"] == "Collecte Biddit browser api"



def test_collect_biddit_browser_listings_enriches_from_detail_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        browser_source,
        "_enrich_biddit_items_from_detail_api",
        ORIGINAL_ENRICH_BIDDIT_DETAILS,
    )
    monkeypatch.setattr(
        browser_source,
        "render_biddit_search_page_with_playwright",
        lambda *args, **kwargs: BidditBrowserRenderResult(
            html=FIXTURE_HTML,
            final_url="https://www.biddit.be/fr/search",
        ),
    )

    payloads = {
        "271234": {
            "reference": "271234",
            "handlingMethod": "ONLINE_PRIVATE_SALE",
            "sellingPrice": 360000,
            "properties": [
                {
                    "reference": "271234",
                    "propertyType": "HOUSE",
                    "propertySubtype": "BUILDING",
                    "title": {"fr": "Immeuble de rapport a Schaerbeek"},
                    "description": {"fr": "Immeuble de rapport avec 4 unites a Schaerbeek."},
                    "address": {
                        "postalCode": "1030",
                        "municipality": {"fr": "Schaerbeek"},
                    },
                    "construction": {"numberOfHousingUnits": 4},
                    "features": {"terrainSurface": 232.0},
                    "rooms": {},
                }
            ],
        },
        "271235": {
            "reference": "271235",
            "handlingMethod": "ONLINE_PRIVATE_SALE",
            "sellingPrice": 240000,
            "properties": [
                {
                    "reference": "271235",
                    "propertyType": "HOUSE",
                    "propertySubtype": "HOUSE",
                    "title": {"fr": "Maison 3 facades a Jambes"},
                    "description": {"fr": "Maison 3 facades avec jardin a Jambes."},
                    "address": {
                        "postalCode": "5100",
                        "municipality": {"fr": "Jambes"},
                    },
                    "construction": {"numberOfHousingUnits": 1},
                    "features": {"terrainSurface": 198.0},
                    "rooms": {},
                }
            ],
        },
    }

    monkeypatch.setattr(
        browser_source,
        "_fetch_biddit_detail_payload",
        lambda session, listing_id, referer, timeout: payloads.get(listing_id),
    )

    result = browser_source.collect_biddit_browser_listing_result("https://www.biddit.be/fr/search")
    first = next(item for item in result.items if item["source_listing_id"] == "271234")

    assert len(result.detail_urls_followed) == 2
    assert result.detail_enriched_count == 2
    assert first["property_type"] == "apartment_block"
    assert first["existing_units"] == 4
    assert first["surface"] == 232.0
    assert first["notes"] == "Collecte Biddit browser api | detail: lot api | mode: online_private_sale"
