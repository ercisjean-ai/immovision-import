import dashboard_data
from datetime import datetime, timezone

from config import utcnow_iso
from dashboard_data import fetch_dashboard_payload
from pipeline import run_pipeline
from sqlite_storage import SQLiteStorage


def test_dashboard_timezone_loader_falls_back_without_tzdata(monkeypatch):
    def _raise_zoneinfo_not_found(_name):
        raise dashboard_data.ZoneInfoNotFoundError("missing tzdata")

    monkeypatch.setattr(dashboard_data, "ZoneInfo", _raise_zoneinfo_not_found)

    fallback_tz = dashboard_data._load_brussels_timezone()

    assert fallback_tz is not None
    assert datetime(2026, 1, 1, tzinfo=fallback_tz).utcoffset().total_seconds() == 3600


def test_dashboard_payload_uses_real_engine_data(tmp_path):
    storage = SQLiteStorage(tmp_path / "immovision.db")

    storage.seed_import_queue_item(
        {
            "source_name": "Immoweb",
            "source_listing_id": "dash-1",
            "source_url": "https://www.immoweb.be/fr/annonce/dash-1",
            "title": "Immeuble de rapport a Ixelles",
            "price": 360000,
            "postal_code": "1050",
            "commune": "Ixelles",
            "property_type": "apartment_block",
            "transaction_type": "sale",
            "existing_units": 3,
            "surface": 255,
            "copro_status": "false",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )
    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "dash-2",
            "source_url": "https://www.biddit.be/fr/catalog/detail/dash-2",
            "title": "Maison de commerce a Grimbergen",
            "price": 350000,
            "postal_code": "1850",
            "commune": "Grimbergen",
            "property_type": "commercial_house",
            "transaction_type": "sale",
            "existing_units": 3,
            "surface": 240,
            "copro_status": "false",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )
    first_run = run_pipeline(storage)
    assert first_run["new"] == 2

    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "dash-2",
            "source_url": "https://www.biddit.be/fr/catalog/detail/dash-2",
            "title": "Maison de commerce a Grimbergen",
            "price": 365000,
            "postal_code": "1850",
            "commune": "Grimbergen",
            "property_type": "commercial_house",
            "transaction_type": "sale",
            "existing_units": 3,
            "surface": 240,
            "copro_status": "false",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )
    second_run = run_pipeline(storage)
    assert second_run["modified"] == 1

    payload = fetch_dashboard_payload(tmp_path / "immovision.db")

    assert payload["summary"] == {
        "total": 2,
        "new": 2,
        "compatible": 1,
        "a_analyser": 1,
        "hors_criteres": 0,
        "modified": 1,
        "live": 2,
        "non_live": 0,
        "live_with_valid_link": 2,
        "live_with_invalid_link": 0,
        "live_strict_eligible": 2,
        "live_inactive_or_closed": 0,
        "live_out_of_criteria": 0,
        "secondary_review_total": 0,
    }
    assert {item["source_name"] for item in payload["listings"]} == {"Immoweb", "Biddit"}

    compatible_titles = {item["title"] for item in payload["sections"]["compatible"]}
    analyser_titles = {item["title"] for item in payload["sections"]["a_analyser"]}
    modified_ids = {item["source_listing_id"] for item in payload["sections"]["modified"]}

    assert "Immeuble de rapport a Ixelles" in compatible_titles
    assert "Maison de commerce a Grimbergen" in analyser_titles
    assert modified_ids == {"dash-2"}

    first_item = payload["listings"][0]
    assert "source_url" in first_item
    assert "price_per_unit" in first_item
    assert "investment_score" in first_item
    assert "confidence_score" in first_item
    assert "strategy_label" in first_item
    assert "observation_status" in first_item
    assert first_item["data_origin"] == "live"
    assert first_item["is_live"] is True
    assert first_item["source_url_valid"] is True
    assert first_item["source_url_issue"] is None


def test_dashboard_payload_can_filter_live_only(tmp_path):
    storage = SQLiteStorage(tmp_path / "immovision.db")

    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "live-1",
            "source_url": "https://www.biddit.be/fr/catalog/detail/live-1",
            "title": "Immeuble live a Grimbergen",
            "price": 330000,
            "postal_code": "1850",
            "commune": "Grimbergen",
            "property_type": "apartment_block",
            "transaction_type": "sale",
            "existing_units": 3,
            "surface": 230,
            "copro_status": "false",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )
    storage.seed_import_queue_item(
        {
            "source_name": "FileFeed",
            "source_listing_id": "demo-1",
            "source_url": "https://example.test/demo-1",
            "title": "Annonce demo locale",
            "price": 280000,
            "postal_code": "1030",
            "commune": "Schaerbeek",
            "property_type": "apartment_block",
            "transaction_type": "sale",
            "existing_units": 2,
            "surface": 160,
            "copro_status": "false",
            "is_live_data": 1,
            "data_origin": "file_feed",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )

    run_pipeline(storage)

    all_payload = fetch_dashboard_payload(tmp_path / "immovision.db", live_only=False)
    live_payload = fetch_dashboard_payload(tmp_path / "immovision.db", live_only=True)

    assert all_payload["summary"]["total"] == 2
    assert all_payload["summary"]["live"] == 1
    assert all_payload["summary"]["non_live"] == 1
    assert all_payload["summary"]["live_with_valid_link"] == 1
    assert all_payload["summary"]["live_with_invalid_link"] == 0
    assert all_payload["summary"]["live_strict_eligible"] == 1
    assert all_payload["summary"]["live_inactive_or_closed"] == 0
    assert all_payload["summary"]["live_out_of_criteria"] == 0
    assert all_payload["summary"]["secondary_review_total"] == 0
    assert {item["data_origin"] for item in all_payload["listings"]} == {"live", "file_feed"}

    assert live_payload["live_only"] is True
    assert live_payload["summary"]["total"] == 1
    assert live_payload["summary"]["live"] == 1
    assert live_payload["summary"]["non_live"] == 1
    assert live_payload["summary"]["live_with_valid_link"] == 1
    assert live_payload["summary"]["live_with_invalid_link"] == 0
    assert live_payload["summary"]["live_strict_eligible"] == 1
    assert live_payload["summary"]["live_inactive_or_closed"] == 0
    assert live_payload["summary"]["live_out_of_criteria"] == 0
    assert live_payload["summary"]["secondary_review_total"] == 0
    assert [item["source_listing_id"] for item in live_payload["listings"]] == ["live-1"]


def test_dashboard_live_only_excludes_live_items_with_invalid_source_url(tmp_path):
    storage = SQLiteStorage(tmp_path / "immovision.db")

    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "live-ok",
            "source_url": "https://www.biddit.be/fr/catalog/detail/live-ok",
            "title": "Bien Biddit valide",
            "price": 330000,
            "postal_code": "1850",
            "commune": "Grimbergen",
            "property_type": "apartment_block",
            "transaction_type": "sale",
            "existing_units": 3,
            "surface": 230,
            "copro_status": "false",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )
    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "live-bad-placeholder",
            "source_url": "#",
            "title": "Bien live au lien placeholder",
            "price": 310000,
            "postal_code": "1800",
            "commune": "Vilvoorde",
            "property_type": "commercial_house",
            "transaction_type": "sale",
            "existing_units": 2,
            "surface": 200,
            "copro_status": "false",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )
    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "live-bad-domain",
            "source_url": "https://www.immoweb.be/fr/annonce/live-bad-domain",
            "title": "Bien live au domaine incoherent",
            "price": 315000,
            "postal_code": "1090",
            "commune": "Jette",
            "property_type": "apartment_block",
            "transaction_type": "sale",
            "existing_units": 2,
            "surface": 190,
            "copro_status": "false",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )

    run_pipeline(storage)

    all_payload = fetch_dashboard_payload(tmp_path / "immovision.db", live_only=False)
    live_payload = fetch_dashboard_payload(tmp_path / "immovision.db", live_only=True)

    assert all_payload["summary"]["live"] == 3
    assert all_payload["summary"]["live_with_valid_link"] == 1
    assert all_payload["summary"]["live_with_invalid_link"] == 2
    assert all_payload["summary"]["live_strict_eligible"] == 1
    assert all_payload["summary"]["live_inactive_or_closed"] == 0
    assert all_payload["summary"]["live_out_of_criteria"] == 0
    assert all_payload["summary"]["secondary_review_total"] == 0
    assert {item["source_listing_id"] for item in all_payload["listings"]} == {
        "live-ok",
        "live-bad-placeholder",
        "live-bad-domain",
    }

    invalid_items = {
        item["source_listing_id"]: item
        for item in all_payload["listings"]
        if not item["source_url_valid"]
    }
    assert invalid_items["live-bad-placeholder"]["source_url_issue"] == "Lien source placeholder"
    assert invalid_items["live-bad-domain"]["source_url_issue"] == "Domaine incoherent avec la source"

    assert live_payload["live_only"] is True
    assert live_payload["summary"]["total"] == 1
    assert live_payload["summary"]["live"] == 3
    assert live_payload["summary"]["live_with_valid_link"] == 1
    assert live_payload["summary"]["live_with_invalid_link"] == 2
    assert live_payload["summary"]["live_strict_eligible"] == 1
    assert live_payload["summary"]["live_inactive_or_closed"] == 0
    assert live_payload["summary"]["live_out_of_criteria"] == 0
    assert live_payload["summary"]["secondary_review_total"] == 0
    assert [item["source_listing_id"] for item in live_payload["listings"]] == ["live-ok"]


def test_dashboard_live_only_excludes_closed_biddit_sales(tmp_path):
    storage = SQLiteStorage(tmp_path / "immovision.db")

    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "biddit-active",
            "source_url": "https://www.biddit.be/fr/catalog/detail/biddit-active",
            "title": "Immeuble de rapport a Grimbergen Se termine le 20/03 10:00",
            "description": "Vente publique avec 3 unites",
            "price": 330000,
            "postal_code": "1850",
            "commune": "Grimbergen",
            "property_type": "apartment_block",
            "transaction_type": "sale",
            "existing_units": 3,
            "surface": 220,
            "copro_status": "unknown",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )
    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "biddit-closed",
            "source_url": "https://www.biddit.be/fr/catalog/detail/biddit-closed",
            "title": "Immeuble de rapport a Grimbergen Se termine le 18/03 10:00",
            "description": "Vente publique avec 3 unites",
            "price": 330000,
            "postal_code": "1850",
            "commune": "Grimbergen",
            "property_type": "apartment_block",
            "transaction_type": "sale",
            "existing_units": 3,
            "surface": 220,
            "copro_status": "unknown",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )

    run_pipeline(storage)

    now = datetime(2026, 3, 19, 12, 0, tzinfo=timezone.utc)
    all_payload = fetch_dashboard_payload(tmp_path / "immovision.db", now=now, live_only=False)
    live_payload = fetch_dashboard_payload(tmp_path / "immovision.db", now=now, live_only=True)

    statuses = {
        item["source_listing_id"]: (item["sale_status"], item["is_dashboard_eligible"])
        for item in all_payload["listings"]
    }
    assert statuses["biddit-active"] == ("active", True)
    assert statuses["biddit-closed"] == ("closed", False)
    assert all_payload["summary"]["live_strict_eligible"] == 1
    assert all_payload["summary"]["live_inactive_or_closed"] == 1
    assert all_payload["summary"]["secondary_review_total"] == 0
    assert [item["source_listing_id"] for item in live_payload["listings"]] == ["biddit-active"]


def test_dashboard_live_only_excludes_live_items_that_are_out_of_criteria(tmp_path):
    storage = SQLiteStorage(tmp_path / "immovision.db")

    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "biddit-fit",
            "source_url": "https://www.biddit.be/fr/catalog/detail/biddit-fit",
            "title": "Maison de commerce a Grimbergen Se termine le 20/03 10:00",
            "description": "Vente publique avec 3 unites",
            "price": 330000,
            "postal_code": "1850",
            "commune": "Grimbergen",
            "property_type": "commercial_house",
            "transaction_type": "sale",
            "existing_units": 3,
            "surface": 240,
            "copro_status": "unknown",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )
    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "biddit-out",
            "source_url": "https://www.biddit.be/fr/catalog/detail/biddit-out",
            "title": "Maison a Hoeilaart Se termine le 20/03 10:00",
            "description": "Vente publique avec 1 unite",
            "price": 390000,
            "postal_code": "1560",
            "commune": "Hoeilaart",
            "property_type": "house",
            "transaction_type": "sale",
            "existing_units": 1,
            "surface": 170,
            "copro_status": "unknown",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )

    run_pipeline(storage)

    now = datetime(2026, 3, 19, 12, 0, tzinfo=timezone.utc)
    all_payload = fetch_dashboard_payload(tmp_path / "immovision.db", now=now, live_only=False)
    live_payload = fetch_dashboard_payload(tmp_path / "immovision.db", now=now, live_only=True)

    items = {item["source_listing_id"]: item for item in all_payload["listings"]}
    assert items["biddit-fit"]["matches_investor_criteria"] is True
    assert items["biddit-out"]["matches_investor_criteria"] is False
    assert items["biddit-out"]["investor_view_issue"] == "moins de 2 unites"
    assert all_payload["summary"]["live_out_of_criteria"] == 1
    assert all_payload["summary"]["secondary_review_total"] == 0
    assert [item["source_listing_id"] for item in live_payload["listings"]] == ["biddit-fit"]


def test_dashboard_live_only_excludes_simple_houses_and_apartments_even_when_live(tmp_path):
    storage = SQLiteStorage(tmp_path / "immovision.db")

    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "biddit-priority",
            "source_url": "https://www.biddit.be/fr/catalog/detail/biddit-priority",
            "title": "Immeuble de rapport a Grimbergen Se termine le 20/03 10:00",
            "description": "Vente publique avec 3 unites",
            "price": 330000,
            "postal_code": "1850",
            "commune": "Grimbergen",
            "property_type": "apartment_block",
            "transaction_type": "sale",
            "existing_units": 3,
            "surface": 240,
            "copro_status": "unknown",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )
    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "biddit-house",
            "source_url": "https://www.biddit.be/fr/catalog/detail/biddit-house",
            "title": "Maison a Grimbergen Se termine le 20/03 10:00",
            "description": "Vente publique avec 3 unites",
            "price": 390000,
            "postal_code": "1850",
            "commune": "Grimbergen",
            "property_type": "house",
            "transaction_type": "sale",
            "existing_units": 3,
            "surface": 170,
            "copro_status": "unknown",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )
    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "biddit-apartment",
            "source_url": "https://www.biddit.be/fr/catalog/detail/biddit-apartment",
            "title": "Appartement a Deurne Se termine le 20/03 10:00",
            "description": "Vente publique avec 2 unites",
            "price": 240000,
            "postal_code": "2100",
            "commune": "Deurne",
            "property_type": "apartment",
            "transaction_type": "sale",
            "existing_units": 2,
            "surface": 120,
            "copro_status": "unknown",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )

    run_pipeline(storage)

    now = datetime(2026, 3, 19, 12, 0, tzinfo=timezone.utc)
    all_payload = fetch_dashboard_payload(tmp_path / "immovision.db", now=now, live_only=False)
    live_payload = fetch_dashboard_payload(tmp_path / "immovision.db", now=now, live_only=True)

    items = {item["source_listing_id"]: item for item in all_payload["listings"]}
    assert items["biddit-priority"]["matches_investor_criteria"] is True
    assert items["biddit-house"]["matches_investor_criteria"] is False
    assert items["biddit-house"]["investor_view_issue"] == "type simple hors cible investisseur"
    assert items["biddit-apartment"]["matches_investor_criteria"] is False
    assert items["biddit-apartment"]["investor_view_issue"] == "zone hors cible investisseur"
    assert all_payload["summary"]["live_out_of_criteria"] == 2
    assert all_payload["summary"]["secondary_review_total"] == 0
    assert [item["source_listing_id"] for item in live_payload["listings"]] == ["biddit-priority"]


def test_dashboard_exposes_secondary_review_for_soft_misses_only(tmp_path):
    storage = SQLiteStorage(tmp_path / "immovision.db")

    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "strict-fit",
            "source_url": "https://www.biddit.be/fr/catalog/detail/strict-fit",
            "title": "Immeuble de rapport a Grimbergen Se termine le 20/03 10:00",
            "description": "Vente publique avec 3 unites",
            "price": 330000,
            "postal_code": "1850",
            "commune": "Grimbergen",
            "property_type": "apartment_block",
            "transaction_type": "sale",
            "existing_units": 3,
            "surface": 220,
            "copro_status": "false",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )
    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "soft-miss",
            "source_url": "https://www.biddit.be/fr/catalog/detail/soft-miss",
            "title": "Immeuble de rapport a Sclayn Se termine le 20/03 10:00",
            "description": "Vente publique avec 3 unites",
            "price": 360000,
            "postal_code": "5300",
            "commune": "Sclayn",
            "property_type": "apartment_block",
            "transaction_type": "sale",
            "existing_units": 3,
            "surface": 240,
            "copro_status": "false",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )
    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "hard-block",
            "source_url": "https://www.biddit.be/fr/catalog/detail/hard-block",
            "title": "Maison a Deurne Se termine le 20/03 10:00",
            "description": "Vente publique avec 1 unite",
            "price": 260000,
            "postal_code": "2100",
            "commune": "Deurne",
            "property_type": "house",
            "transaction_type": "sale",
            "existing_units": 1,
            "surface": 160,
            "copro_status": "unknown",
            "is_live_data": 1,
            "data_origin": "live",
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )

    run_pipeline(storage)

    now = datetime(2026, 3, 19, 12, 0, tzinfo=timezone.utc)
    payload = fetch_dashboard_payload(tmp_path / "immovision.db", now=now, live_only=True)

    assert [item["source_listing_id"] for item in payload["listings"]] == ["strict-fit"]
    assert [item["source_listing_id"] for item in payload["secondary_review"]] == ["soft-miss"]
    assert payload["secondary_review"][0]["secondary_review_reason"] == "zone a analyser"
    assert payload["summary"]["secondary_review_total"] == 1
