from pathlib import Path

import pytest
import requests

from listing_feed import load_listing_feed
from sources.immoweb_source import (
    ImmowebFetchError,
    collect_immoweb_listings,
    fetch_immoweb_search_page,
    write_listings_jsonl,
)


FIXTURE_PATH = Path("sample_data") / "immoweb_search_fixture.html"



def test_collect_immoweb_listings_from_fixture() -> None:
    items = collect_immoweb_listings(html_file=FIXTURE_PATH)

    assert len(items) == 2

    first = items[0]
    assert first["source_name"] == "Immoweb"
    assert first["source_listing_id"] == "20434567"
    assert first["price"] == 425000.0
    assert first["postal_code"] == "1070"
    assert first["commune"] == "Anderlecht"
    assert first["property_type"] == "apartment_block"
    assert first["existing_units"] == 4
    assert first["surface"] == 240.0
    assert first["transaction_type"] == "sale"
    assert first["is_active"] is True



def test_collect_immoweb_roundtrip_to_internal_feed(tmp_path: Path) -> None:
    items = collect_immoweb_listings(html_file=FIXTURE_PATH)
    output_path = tmp_path / "immoweb_test_output.jsonl"
    write_listings_jsonl(output_path, items)

    loaded = load_listing_feed(output_path, default_source_name="Immoweb")

    assert [item["source_listing_id"] for item in loaded] == [
        "20434567",
        "20439999",
    ]
    assert loaded[1]["commune"] == "Etterbeek"
    assert loaded[1]["existing_units"] == 3
    assert loaded[1]["surface"] == 185.0



def test_fetch_immoweb_search_page_raises_clear_error_on_403(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        status_code = 403
        text = "forbidden"

        def raise_for_status(self) -> None:
            raise requests.HTTPError("403 Client Error: HTTP Forbidden")

    class FakeSession:
        def get(self, *args, **kwargs):
            return FakeResponse()

    monkeypatch.setattr(requests, "Session", lambda: FakeSession())

    with pytest.raises(ImmowebFetchError) as exc_info:
        fetch_immoweb_search_page("https://www.immoweb.be/fr/recherche/test")

    assert "403 Forbidden" in str(exc_info.value)



def test_collect_immoweb_listings_raises_clear_error_when_live_html_has_no_listings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeResponse:
        status_code = 200
        text = "<html><body>Please enable JavaScript to continue.</body></html>"

        def raise_for_status(self) -> None:
            return None

    class FakeSession:
        def get(self, *args, **kwargs):
            return FakeResponse()

    monkeypatch.setattr(requests, "Session", lambda: FakeSession())

    with pytest.raises(ImmowebFetchError) as exc_info:
        collect_immoweb_listings(search_url="https://www.immoweb.be/fr/recherche/test")

    message = str(exc_info.value)
    assert "Aucune annonce extraite" in message
    assert "JavaScript" in message
