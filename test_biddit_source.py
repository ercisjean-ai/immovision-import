from pathlib import Path

import pytest
import requests

from listing_feed import load_listing_feed
from sources.biddit_source import (
    BidditFetchError,
    collect_biddit_listings,
    fetch_biddit_search_page,
    fetch_biddit_search_result,
    parse_biddit_search_results,
    write_listings_jsonl,
)


FIXTURE_PATH = Path("sample_data") / "biddit_search_fixture.html"
FIXTURE_HTML = FIXTURE_PATH.read_text(encoding="utf-8")



def test_collect_biddit_listings_from_fixture() -> None:
    items = collect_biddit_listings(html_file=FIXTURE_PATH)

    assert len(items) == 2

    first = items[0]
    assert first["source_name"] == "Biddit"
    assert first["source_listing_id"] == "271234"
    assert first["price"] == 285000.0
    assert first["postal_code"] == "1030"
    assert first["commune"] == "Schaerbeek"
    assert first["property_type"] == "apartment_block"
    assert first["existing_units"] == 4
    assert first["surface"] == 232.0
    assert first["transaction_type"] == "sale"
    assert first["is_active"] is True



def test_collect_biddit_roundtrip_to_internal_feed(tmp_path: Path) -> None:
    items = collect_biddit_listings(html_file=FIXTURE_PATH)
    output_path = tmp_path / "biddit_test_output.jsonl"
    write_listings_jsonl(output_path, items)

    loaded = load_listing_feed(output_path, default_source_name="Biddit")

    assert [item["source_listing_id"] for item in loaded] == [
        "271234",
        "271235",
    ]
    assert loaded[1]["commune"] == "Jambes"
    assert loaded[1]["surface"] == 198.0


def test_parse_biddit_search_results_isolates_each_listing_card() -> None:
    html = """
    <html>
      <body>
        <section class="results-shell">
          <article class="listing-card">
            <a href="/fr/catalog/detail/292808">
              <h3>Appartement a Deurne</h3>
            </a>
            <p>2100 Deurne</p>
            <p>120 m2</p>
            <p>1 unite</p>
          </article>
          <article class="listing-card">
            <a href="/fr/catalog/detail/292705">
              <h3>Maison a Hoeilaart</h3>
            </a>
            <p>1560 Hoeilaart</p>
            <p>180 m2</p>
            <p>1 unite</p>
          </article>
        </section>
      </body>
    </html>
    """

    items = parse_biddit_search_results(html)
    by_id = {item["source_listing_id"]: item for item in items}

    assert by_id["292808"]["postal_code"] == "2100"
    assert by_id["292808"]["commune"] == "Deurne"
    assert by_id["292705"]["postal_code"] == "1560"
    assert by_id["292705"]["commune"] == "Hoeilaart"


def test_parse_biddit_search_results_prefers_richest_single_listing_card() -> None:
    html = """
    <html>
      <body>
        <section class="results-shell">
          <article class="listing-card">
            <div class="listing-head">
              <a href="/fr/catalog/detail/289584">
                <span>Maison a Sclayn</span>
              </a>
            </div>
            <div class="listing-body">
              <p>5300 Sclayn</p>
              <p>3 unites</p>
              <p>315 m2</p>
            </div>
          </article>
          <article class="listing-card">
            <div class="listing-head">
              <a href="/fr/catalog/detail/292808">
                <span>Appartement a Deurne</span>
              </a>
            </div>
            <div class="listing-body">
              <p>2100 Deurne</p>
              <p>1 unite</p>
              <p>120 m2</p>
            </div>
          </article>
        </section>
      </body>
    </html>
    """

    items = parse_biddit_search_results(html)
    by_id = {item["source_listing_id"]: item for item in items}

    assert by_id["289584"]["postal_code"] == "5300"
    assert by_id["289584"]["commune"] == "Sclayn"
    assert by_id["289584"]["existing_units"] == 3
    assert by_id["292808"]["postal_code"] == "2100"
    assert by_id["292808"]["commune"] == "Deurne"



def test_fetch_biddit_search_result_ignores_tracking_iframe_and_prefers_biddit_embed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iframe_html = """
    <html><body>
      <iframe src="https://www.googletagmanager.com/ns.html?id=GTM-5C2GPMG"></iframe>
      <iframe src="/fr/embed/search/results"></iframe>
    </body></html>
    """

    class FakeResponse:
        def __init__(self, status_code: int, text: str) -> None:
            self.status_code = status_code
            self.text = text

        def raise_for_status(self) -> None:
            return None

    class FakeSession:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def get(self, url, *args, **kwargs):
            self.calls.append(url)
            if url == "https://www.biddit.be/fr/search":
                return FakeResponse(200, iframe_html)
            if url == "https://www.biddit.be/fr/embed/search/results":
                return FakeResponse(200, FIXTURE_HTML)
            raise AssertionError(f"unexpected url: {url}")

    fake_session = FakeSession()
    monkeypatch.setattr(requests, "Session", lambda: fake_session)

    result = fetch_biddit_search_result("https://www.biddit.be/fr/search")

    assert result.fetched_url == "https://www.biddit.be/fr/embed/search/results"
    assert result.embed_urls == [
        "https://www.googletagmanager.com/ns.html?id=GTM-5C2GPMG",
        "https://www.biddit.be/fr/embed/search/results",
    ]
    assert result.ignored_embed_details == [
        "https://www.googletagmanager.com/ns.html?id=GTM-5C2GPMG (source technique ignoree (googletagmanager.com))"
    ]
    assert result.followed_embed_urls == ["https://www.biddit.be/fr/embed/search/results"]
    assert "meme domaine Biddit" in result.followed_embed_details[0]
    assert all("googletagmanager" not in url for url in fake_session.calls[1:])



def test_collect_biddit_listings_uses_followed_biddit_embed(monkeypatch: pytest.MonkeyPatch) -> None:
    iframe_html = """
    <html><body>
      <iframe src="https://www.googletagmanager.com/ns.html?id=GTM-5C2GPMG"></iframe>
      <iframe src="/fr/embed/search/results"></iframe>
    </body></html>
    """

    class FakeResponse:
        def __init__(self, status_code: int, text: str) -> None:
            self.status_code = status_code
            self.text = text

        def raise_for_status(self) -> None:
            return None

    class FakeSession:
        def get(self, url, *args, **kwargs):
            if url == "https://www.biddit.be/fr/search":
                return FakeResponse(200, iframe_html)
            if url == "https://www.biddit.be/fr/embed/search/results":
                return FakeResponse(200, FIXTURE_HTML)
            raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(requests, "Session", lambda: FakeSession())

    items = collect_biddit_listings(search_url="https://www.biddit.be/fr/search")

    assert len(items) == 2
    assert items[0]["source_listing_id"] == "271234"



def test_collect_biddit_listings_raises_clear_error_with_embed_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iframe_html = """
    <html><body>
      <iframe src="https://www.googletagmanager.com/ns.html?id=GTM-5C2GPMG"></iframe>
      <iframe src="/fr/embed/search/results"></iframe>
    </body></html>
    """
    empty_embed_html = "<html><body><div>Biddit search shell without cards</div></body></html>"

    class FakeResponse:
        def __init__(self, status_code: int, text: str) -> None:
            self.status_code = status_code
            self.text = text

        def raise_for_status(self) -> None:
            return None

    class FakeSession:
        def get(self, url, *args, **kwargs):
            if url == "https://www.biddit.be/fr/search":
                return FakeResponse(200, iframe_html)
            if url == "https://www.biddit.be/fr/embed/search/results":
                return FakeResponse(200, empty_embed_html)
            raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(requests, "Session", lambda: FakeSession())

    with pytest.raises(BidditFetchError) as exc_info:
        collect_biddit_listings(search_url="https://www.biddit.be/fr/search")

    message = str(exc_info.value)
    assert "sources embarquees detectees" in message.lower()
    assert "sources ignorees" in message.lower()
    assert "sources suivies" in message.lower()
    assert "googletagmanager.com" in message
    assert "fr/embed/search/results" in message
    assert "source technique ignoree" in message.lower()



def test_fetch_biddit_search_page_raises_clear_error_on_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeResponse:
        status_code = 403
        text = "forbidden"

        def raise_for_status(self) -> None:
            raise requests.HTTPError("403 Client Error: HTTP Forbidden")

    class FakeSession:
        def get(self, *args, **kwargs):
            return FakeResponse()

    monkeypatch.setattr(requests, "Session", lambda: FakeSession())

    with pytest.raises(BidditFetchError) as exc_info:
        fetch_biddit_search_page("https://www.biddit.be/fr/search")

    assert "HTTP 403" in str(exc_info.value)
