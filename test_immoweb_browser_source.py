from pathlib import Path

import pytest

import sources.immoweb_browser_source as browser_source
from sources.immoweb_browser_source import BrowserRenderResult
from sources.immoweb_source import ImmowebFetchError


FIXTURE_HTML = (Path("sample_data") / "immoweb_search_fixture.html").read_text(encoding="utf-8")



def test_collect_immoweb_browser_listings_uses_rendered_html(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        browser_source,
        "render_immoweb_search_page_with_playwright",
        lambda *args, **kwargs: BrowserRenderResult(
            html=FIXTURE_HTML,
            final_url="https://www.immoweb.be/fr/recherche/test",
        ),
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



def test_extract_immoweb_network_listings_from_payloads() -> None:
    payloads = [
        {
            "results": [
                {
                    "url": "https://www.immoweb.be/fr/annonce/maison/a-vendre/etterbeek/87654321",
                    "name": "Maison a Etterbeek",
                    "description": "Bien de rapport",
                    "price": 510000,
                    "postalCode": "1040",
                    "city": "Etterbeek",
                    "surface": 190,
                    "numberOfUnits": 2,
                }
            ]
        }
    ]

    items = browser_source.extract_immoweb_network_listings(payloads)

    assert len(items) == 1
    assert items[0]["source_listing_id"] == "87654321"
    assert items[0]["commune"] == "Etterbeek"
    assert items[0]["price"] == 510000.0



def test_collect_immoweb_browser_listings_raises_clear_error_on_rendered_empty_html(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        browser_source,
        "render_immoweb_search_page_with_playwright",
        lambda *args, **kwargs: BrowserRenderResult(
            html="<html><body>Please enable JavaScript to continue.</body></html>",
            final_url="https://www.immoweb.be/fr/recherche/test",
            page_title="JavaScript required",
        ),
    )

    with pytest.raises(ImmowebFetchError) as exc_info:
        browser_source.collect_immoweb_browser_listings(
            "https://www.immoweb.be/fr/recherche/test"
        )

    message = str(exc_info.value)
    assert "Aucune annonce extraite" in message
    assert "JavaScript" in message



def test_collect_immoweb_browser_listings_saves_debug_artifacts_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        browser_source,
        "render_immoweb_search_page_with_playwright",
        lambda *args, **kwargs: BrowserRenderResult(
            html="<html><body>Access denied</body></html>",
            final_url="https://www.immoweb.be/fr/recherche/test",
            page_title="Access denied",
            screenshot_bytes=b"fakepng",
        ),
    )

    with pytest.raises(ImmowebFetchError) as exc_info:
        browser_source.collect_immoweb_browser_listings(
            "https://www.immoweb.be/fr/recherche/test",
            debug_save_html=True,
            debug_screenshot=True,
            debug_dir=tmp_path,
        )

    message = str(exc_info.value)
    assert "Artefacts enregistres" in message
    assert len(list(tmp_path.glob("*.html"))) == 1
    assert len(list(tmp_path.glob("*.png"))) == 1



def test_diagnose_immoweb_browser_failure_detects_cookie_gate() -> None:
    diagnostic = browser_source.diagnose_immoweb_browser_failure(
        BrowserRenderResult(
            html="<html><body>Cookies - Tout accepter</body></html>",
            final_url="https://www.immoweb.be/fr/recherche/test",
            page_title="Consentement",
            cookie_banner_seen=True,
        )
    )

    assert "consentement" in diagnostic.lower() or "cookie" in diagnostic.lower()
