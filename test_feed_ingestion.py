import json
import sqlite3

from config import utcnow_iso
from listing_feed import load_listing_feed
from pipeline import run_pipeline
from sqlite_storage import SQLiteStorage



def test_load_listing_feed_normalizes_jsonl(tmp_path):
    feed_path = tmp_path / "feed.jsonl"
    feed_path.write_text(
        json.dumps(
            {
                "listing_id": "raw-1",
                "url": "https://example.test/raw-1",
                "price": "315000",
                "existing_units": "3",
                "surface": "210",
                "is_new_build": "false",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    items = load_listing_feed(feed_path, default_source_name="ManualFeed")

    assert items == [
        {
            "source_name": "ManualFeed",
            "source_listing_id": "raw-1",
            "source_url": "https://example.test/raw-1",
            "title": None,
            "description": None,
            "price": 315000.0,
            "postal_code": None,
            "commune": None,
            "property_type": None,
            "transaction_type": "sale",
            "existing_units": 3,
            "surface": 210.0,
            "is_copro": False,
            "copro_status": "unknown",
            "is_new_build": False,
            "is_live_data": True,
            "data_origin": "file_feed",
            "is_active": True,
            "notes": None,
            "updated_at": items[0]["updated_at"],
        }
    ]



def test_load_listing_feed_marks_local_file_without_explicit_origin_as_file_feed(tmp_path):
    feed_path = tmp_path / "immoweb_like_feed.jsonl"
    feed_path.write_text(
        json.dumps(
            {
                "source_name": "Immoweb",
                "source_listing_id": "raw-immoweb-1",
                "source_url": "https://www.immoweb.be/fr/annonce/raw-immoweb-1",
                "title": "Annonce exportee localement",
                "price": 325000,
                "existing_units": 3,
                "surface": 205,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    items = load_listing_feed(feed_path)

    assert items[0]["source_name"] == "Immoweb"
    assert items[0]["data_origin"] == "file_feed"


def test_feed_ingestion_to_pipeline_end_to_end(tmp_path):
    feed_path = tmp_path / "feed.jsonl"
    feed_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "source_name": "FileFeed",
                        "source_listing_id": "feed-2001",
                        "source_url": "https://example.test/feed-2001",
                        "title": "Immeuble a Ixelles",
                        "price": 330000,
                        "postal_code": "1050",
                        "commune": "Ixelles",
                        "property_type": "apartment_block",
                        "existing_units": 3,
                        "surface": 240,
                        "notes": "Import feed test",
                    }
                ),
                json.dumps(
                    {
                        "source_name": "FileFeed",
                        "source_listing_id": "feed-2002",
                        "source_url": "https://example.test/feed-2002",
                        "title": "Maison a Anderlecht",
                        "price": 250000,
                        "postal_code": "1070",
                        "commune": "Anderlecht",
                        "property_type": "house",
                        "existing_units": 2,
                        "surface": 170,
                        "notes": "Import feed test",
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    storage = SQLiteStorage(tmp_path / "immovision.db")
    items = load_listing_feed(feed_path)
    for item in items:
        storage.seed_import_queue_item(item)

    stats = run_pipeline(storage)

    assert stats == {
        "discovered_count": 0,
        "queued": 0,
        "imported": 2,
        "new": 2,
        "seen": 0,
        "modified": 0,
    }

    connection = sqlite3.connect(tmp_path / "immovision.db")
    listings = connection.execute(
        """
        SELECT source_listing_id, commune, price
        FROM normalized_listings
        ORDER BY source_listing_id
        """
    ).fetchall()
    analyses = connection.execute(
        """
        SELECT estimated_rent_per_unit, estimated_gross_yield, investment_score_label, strategy_label, confidence_label
        FROM listing_analysis
        ORDER BY listing_id
        """
    ).fetchall()

    assert listings == [
        ("feed-2001", "Ixelles", 330000.0),
        ("feed-2002", "Anderlecht", 250000.0),
    ]
    assert analyses == [
        (950.0, 10.36, "Interessant", "A analyser", "Elevee"),
        (850.0, 8.16, "Moyen", "A analyser", "Elevee"),
    ]



def test_history_tracks_all_observations_but_only_real_price_changes(tmp_path):
    storage = SQLiteStorage(tmp_path / "immovision.db")
    base_item = {
        "source_name": "Immoweb",
        "source_listing_id": "history-1",
        "source_url": "https://example.test/history-1",
        "title": "Immeuble suivi",
        "price": 300000,
        "postal_code": "1050",
        "commune": "Ixelles",
        "property_type": "apartment_block",
        "transaction_type": "sale",
        "existing_units": 3,
        "surface": 240,
        "is_active": 1,
        "is_live_data": 1,
        "updated_at": utcnow_iso(),
    }

    storage.seed_import_queue_item(base_item)
    assert run_pipeline(storage)["new"] == 1

    storage.seed_import_queue_item({**base_item, "updated_at": utcnow_iso()})
    assert run_pipeline(storage)["seen"] == 1

    storage.seed_import_queue_item({**base_item, "title": "Immeuble suivi revu"})
    assert run_pipeline(storage)["modified"] == 1

    storage.seed_import_queue_item(
        {
            **base_item,
            "price": 315000,
            "title": "Immeuble suivi maj prix",
            "updated_at": utcnow_iso(),
        }
    )
    assert run_pipeline(storage)["modified"] == 1

    connection = sqlite3.connect(tmp_path / "immovision.db")
    observation_rows = connection.execute(
        """
        SELECT observation_status, changed_fields, is_price_changed, title, price
        FROM listing_observation_history
        ORDER BY id
        """
    ).fetchall()
    price_history_count = connection.execute(
        "SELECT COUNT(*) FROM listing_price_history"
    ).fetchone()
    latest_price = connection.execute(
        "SELECT price FROM normalized_listings WHERE source_listing_id = ?",
        ("history-1",),
    ).fetchone()
    listing_state = connection.execute(
        """
        SELECT observation_count, last_observation_status, last_changed_fields
        FROM normalized_listings
        WHERE source_listing_id = ?
        """,
        ("history-1",),
    ).fetchone()

    assert observation_rows == [
        ("new", None, 1, "Immeuble suivi", 300000.0),
        ("seen", None, 0, "Immeuble suivi", 300000.0),
        ("modified", "title", 0, "Immeuble suivi revu", 300000.0),
        ("modified", "title,price", 1, "Immeuble suivi maj prix", 315000.0),
    ]
    assert price_history_count == (2,)
    assert latest_price == (315000.0,)
    assert listing_state == (4, "modified", "title,price")


def test_sparse_refresh_keeps_last_known_values_and_stays_seen(tmp_path):
    storage = SQLiteStorage(tmp_path / "immovision.db")
    complete_item = {
        "source_name": "Biddit",
        "source_listing_id": "biddit-1",
        "source_url": "https://www.biddit.be/fr/search/maison/jambes/271235",
        "title": "Maison 3 facades a Jambes",
        "description": "Maison avec jardin et garage",
        "price": 240000,
        "postal_code": "5100",
        "commune": "Jambes",
        "property_type": "house",
        "transaction_type": "sale",
        "existing_units": 1,
        "surface": 198,
        "is_active": 1,
        "is_live_data": 1,
        "updated_at": utcnow_iso(),
    }

    storage.seed_import_queue_item(complete_item)
    assert run_pipeline(storage)["new"] == 1

    storage.seed_import_queue_item(
        {
            **complete_item,
            "title": None,
            "description": None,
            "price": None,
            "surface": None,
            "updated_at": utcnow_iso(),
        }
    )
    stats = run_pipeline(storage)

    connection = sqlite3.connect(tmp_path / "immovision.db")
    listing = connection.execute(
        """
        SELECT title, description, price, surface, observation_count, last_observation_status
        FROM normalized_listings
        WHERE source_listing_id = ?
        """,
        ("biddit-1",),
    ).fetchone()
    latest_observation = connection.execute(
        """
        SELECT observation_status, changed_fields, price, title
        FROM listing_observation_history
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()

    assert stats["seen"] == 1
    assert listing == (
        "Maison 3 facades a Jambes",
        "Maison avec jardin et garage",
        240000.0,
        198.0,
        2,
        "seen",
    )
    assert latest_observation == (
        "seen",
        None,
        240000.0,
        "Maison 3 facades a Jambes",
    )


def test_observation_change_ignores_scraping_noise_on_non_business_fields(tmp_path):
    storage = SQLiteStorage(tmp_path / "immovision.db")
    base_item = {
        "source_name": "Biddit",
        "source_listing_id": "biddit-noise-1",
        "source_url": "https://www.biddit.be/fr/catalog/detail/999999",
        "title": "Maison de commerce a Vilvoorde",
        "description": "Grande maison de commerce avec jardin",
        "price": 340000,
        "postal_code": "1800",
        "commune": "Vilvoorde",
        "property_type": "commercial_house",
        "transaction_type": "sale",
        "existing_units": 3,
        "surface": 245,
        "copro_status": "unknown",
        "is_live_data": 1,
        "is_active": 1,
        "updated_at": utcnow_iso(),
    }

    storage.seed_import_queue_item(base_item)
    assert run_pipeline(storage)["new"] == 1

    storage.seed_import_queue_item(
        {
            **base_item,
            "source_url": "https://www.biddit.be/fr/catalog/detail/999999?refresh=1",
            "title": "  MAISON  DE COMMERCE a Vilvoorde  ",
            "description": "Grande maison de commerce avec jardin | lot 1",
            "updated_at": utcnow_iso(),
        }
    )
    stats = run_pipeline(storage)

    connection = sqlite3.connect(tmp_path / "immovision.db")
    latest_observation = connection.execute(
        """
        SELECT observation_status, changed_fields
        FROM listing_observation_history
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    listing_state = connection.execute(
        """
        SELECT observation_count, last_observation_status, last_changed_fields
        FROM normalized_listings
        WHERE source_listing_id = ?
        """,
        ("biddit-noise-1",),
    ).fetchone()

    assert stats["seen"] == 1
    assert latest_observation == ("seen", None)
    assert listing_state == (2, "seen", None)


def test_biddit_source_is_classed_a_analyser_even_when_economics_fit(tmp_path):
    storage = SQLiteStorage(tmp_path / "immovision.db")
    storage.seed_import_queue_item(
        {
            "source_name": "Biddit",
            "source_listing_id": "biddit-fit-1",
            "source_url": "https://www.biddit.be/fr/search/immeuble/grimbergen/271236",
            "title": "Immeuble de rapport a Grimbergen",
            "description": "Immeuble de rapport avec 3 logements",
            "price": 360000,
            "postal_code": "1850",
            "commune": "Grimbergen",
            "property_type": "apartment_block",
            "transaction_type": "sale",
            "existing_units": 3,
            "surface": 260,
            "is_copro": 0,
            "is_live_data": 1,
            "is_active": 1,
            "updated_at": utcnow_iso(),
        }
    )

    run_pipeline(storage)

    connection = sqlite3.connect(tmp_path / "immovision.db")
    listing = connection.execute(
        """
        SELECT is_copro, copro_status
        FROM normalized_listings
        """
    ).fetchone()
    analysis = connection.execute(
        """
        SELECT strategy_label, strategy_compatible, investment_score, price_per_unit, compatibility_reason
        FROM listing_analysis
        """
    ).fetchone()

    assert listing == (0, "false")
    assert analysis[0] == "A analyser"
    assert analysis[1] == 0
    assert analysis[2] == 92.0
    assert analysis[3] == 120000.0
    assert "vente notariale ou enchere" in analysis[4]


def test_composite_identity_keeps_same_listing_id_separate_across_sources(tmp_path):
    storage = SQLiteStorage(tmp_path / "immovision.db")
    shared_listing_id = "shared-42"

    immoweb_item = {
        "source_name": "Immoweb",
        "source_listing_id": shared_listing_id,
        "source_url": "https://www.immoweb.be/fr/annonce/test/shared-42",
        "title": "Immeuble de rapport a Ixelles",
        "price": 360000,
        "postal_code": "1050",
        "commune": "Ixelles",
        "property_type": "apartment_block",
        "transaction_type": "sale",
        "existing_units": 3,
        "surface": 255,
        "is_live_data": 1,
        "is_active": 1,
        "updated_at": utcnow_iso(),
    }
    biddit_item = {
        "source_name": "Biddit",
        "source_listing_id": shared_listing_id,
        "source_url": "https://www.biddit.be/fr/catalog/detail/shared-42",
        "title": "Maison de commerce a Grimbergen",
        "price": 350000,
        "postal_code": "1850",
        "commune": "Grimbergen",
        "property_type": "commercial_house",
        "transaction_type": "sale",
        "existing_units": 3,
        "surface": 240,
        "is_live_data": 1,
        "is_active": 1,
        "updated_at": utcnow_iso(),
    }

    storage.seed_import_queue_item(immoweb_item)
    storage.seed_import_queue_item(biddit_item)
    assert run_pipeline(storage) == {
        "discovered_count": 0,
        "queued": 0,
        "imported": 2,
        "new": 2,
        "seen": 0,
        "modified": 0,
    }

    storage.seed_import_queue_item({**immoweb_item, "updated_at": utcnow_iso()})
    storage.seed_import_queue_item({**biddit_item, "updated_at": utcnow_iso()})
    second_run_stats = run_pipeline(storage)

    connection = sqlite3.connect(tmp_path / "immovision.db")
    listing_rows = connection.execute(
        """
        SELECT source_name, source_listing_id, observation_count, last_observation_status
        FROM normalized_listings
        WHERE source_listing_id = ?
        ORDER BY source_name
        """,
        (shared_listing_id,),
    ).fetchall()
    observation_rows = connection.execute(
        """
        SELECT source_name, source_listing_id, observation_status
        FROM listing_observation_history
        WHERE source_listing_id = ?
        ORDER BY id
        """,
        (shared_listing_id,),
    ).fetchall()
    price_history_count = connection.execute(
        "SELECT COUNT(*) FROM listing_price_history"
    ).fetchone()

    assert second_run_stats["new"] == 0
    assert second_run_stats["seen"] == 2
    assert second_run_stats["modified"] == 0
    assert listing_rows == [
        ("Biddit", shared_listing_id, 2, "seen"),
        ("Immoweb", shared_listing_id, 2, "seen"),
    ]
    assert observation_rows == [
        ("Immoweb", shared_listing_id, "new"),
        ("Biddit", shared_listing_id, "new"),
        ("Immoweb", shared_listing_id, "seen"),
        ("Biddit", shared_listing_id, "seen"),
    ]
    assert price_history_count == (2,)
