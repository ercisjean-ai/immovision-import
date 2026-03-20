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



def test_collect_immoweb_browser_listings_can_fallback_to_response_html(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        browser_source,
        "render_immoweb_search_page_with_playwright",
        lambda *args, **kwargs: BrowserRenderResult(
            html="",
            response_htmls=[FIXTURE_HTML],
            final_url="https://www.immoweb.be/fr/recherche/test",
            search_navigation_timed_out=True,
        ),
    )

    items = browser_source.collect_immoweb_browser_listings(
        "https://www.immoweb.be/fr/recherche/test"
    )

    assert len(items) == 2
    assert items[0]["source_listing_id"] == "20434567"



def test_collect_immoweb_browser_listings_forwards_session_options(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}
    session_state_path = tmp_path / "immoweb_state.json"

    def fake_render(*args, **kwargs):
        captured.update(kwargs)
        return BrowserRenderResult(
            html=FIXTURE_HTML,
            final_url="https://www.immoweb.be/fr/recherche/test",
            saved_session_state_path=str(session_state_path),
        )

    monkeypatch.setattr(
        browser_source,
        "render_immoweb_search_page_with_playwright",
        fake_render,
    )

    items = browser_source.collect_immoweb_browser_listings(
        "https://www.immoweb.be/fr/recherche/test",
        session_state_path=session_state_path,
        save_session=True,
        reuse_session=True,
    )

    assert len(items) == 2
    assert captured["session_state_path"] == session_state_path
    assert captured["save_session"] is True
    assert captured["reuse_session"] is True



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
            html="",
            final_url="about:blank",
            page_title="",
            screenshot_bytes=b"fakepng",
            search_navigation_timed_out=True,
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
    html_files = list(tmp_path.glob("*.html"))
    png_files = list(tmp_path.glob("*.png"))
    assert len(html_files) == 1
    assert len(png_files) == 1
    assert "Immoweb debug capture" in html_files[0].read_text(encoding="utf-8")



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



def test_diagnose_immoweb_browser_failure_detects_navigation_timeout_before_html() -> None:
    diagnostic = browser_source.diagnose_immoweb_browser_failure(
        BrowserRenderResult(
            html="",
            final_url="about:blank",
            search_navigation_timed_out=True,
        )
    )

    assert "avant timeout" in diagnostic.lower() or "page html exploitable" in diagnostic.lower()



def test_diagnose_immoweb_browser_failure_mentions_expired_reused_session() -> None:
    diagnostic = browser_source.diagnose_immoweb_browser_failure(
        BrowserRenderResult(
            html="<html><body>DataDome CAPTCHA</body></html>",
            final_url="https://www.immoweb.be/fr/recherche/test",
            page_title="immoweb.be",
            used_session_state_path="sessions/immoweb_state.json",
        )
    )

    assert "expiree" in diagnostic.lower() or "invalidee" in diagnostic.lower()
    assert "datadome" in diagnostic.lower() or "anti-bot" in diagnostic.lower()



def test_resolve_session_state_path_defaults_when_persistent_mode_is_enabled() -> None:
    resolved = browser_source._resolve_session_state_path(
        session_state_path=None,
        save_session=True,
        reuse_session=False,
    )

    assert resolved == browser_source.DEFAULT_SESSION_STATE_PATH
