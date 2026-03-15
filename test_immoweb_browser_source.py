from pathlib import Path

import pytest

import sources.immoweb_browser_source as browser_source
from sources.immoweb_source import ImmowebFetchError


FIXTURE_HTML = (Path("sample_data") / "immoweb_search_fixture.html").read_text(encoding="utf-8")



def test_collect_immoweb_browser_listings_uses_rendered_html(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        browser_source,
        "render_immoweb_search_page_with_playwright",
        lambda *args, **kwargs: (FIXTURE_HTML, "https://www.immoweb.be/fr/recherche/test"),
    )

    items = browser_source.collect_immoweb_browser_listings(
        "https://www.immoweb.be/fr/recherche/test"
    )

    assert len(items) == 2
    assert items[0]["source_name"] == "Immoweb"
    assert items[0]["source_listing_id"] == "20434567"
    assert items[1]["commune"] == "Etterbeek"



def test_extract_immoweb_embedded_listings_from_json_ld() -> None:
    html = """
    <html>
      <body>
        <script type="application/ld+json">
          {
            "@context": "https://schema.org",
            "@type": "Product",
            "name": "Immeuble de rapport a Ixelles",
            "description": "3 appartements avec revenu locatif",
            "url": "https://www.immoweb.be/fr/annonce/immeuble-de-rapport/a-vendre/ixelles/12345678",
            "offers": {"price": 420000},
            "address": {"postalCode": "1050", "addressLocality": "Ixelles"},
            "floorSize": {"value": 220},
            "numberOfUnits": 3
          }
        </script>
      </body>
    </html>
    """

    items = browser_source.extract_immoweb_embedded_listings(html)

    assert len(items) == 1
    assert items[0]["source_listing_id"] == "12345678"
    assert items[0]["price"] == 420000.0
    assert items[0]["postal_code"] == "1050"
    assert items[0]["commune"] == "Ixelles"
    assert items[0]["property_type"] == "apartment_block"
    assert items[0]["existing_units"] == 3
    assert items[0]["surface"] == 220.0



def test_collect_immoweb_browser_listings_raises_clear_error_on_rendered_empty_html(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        browser_source,
        "render_immoweb_search_page_with_playwright",
        lambda *args, **kwargs: (
            "<html><body>Please enable JavaScript to continue.</body></html>",
            "https://www.immoweb.be/fr/recherche/test",
        ),
    )

    with pytest.raises(ImmowebFetchError) as exc_info:
        browser_source.collect_immoweb_browser_listings(
            "https://www.immoweb.be/fr/recherche/test"
        )

    message = str(exc_info.value)
    assert "Aucune annonce extraite" in message
    assert "JavaScript" in message
