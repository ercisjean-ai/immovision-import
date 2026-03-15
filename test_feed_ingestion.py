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
            "is_new_build": False,
            "is_live_data": True,
            "is_active": True,
            "notes": None,
            "updated_at": items[0]["updated_at"],
        }
    ]



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

    assert stats == {"discovered_count": 0, "queued": 0, "imported": 2}

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
        SELECT estimated_rent_per_unit, estimated_gross_yield, investment_score_label, confidence_label
        FROM listing_analysis
        ORDER BY listing_id
        """
    ).fetchall()

    assert listings == [
        ("feed-2001", "Ixelles", 330000.0),
        ("feed-2002", "Anderlecht", 250000.0),
    ]
    assert analyses == [
        (950.0, 10.36, "Interessant", "Elevee"),
        (850.0, 8.16, "Interessant", "Elevee"),
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
    run_pipeline(storage)

    storage.seed_import_queue_item({**base_item, "title": "Immeuble suivi revu"})
    run_pipeline(storage)

    storage.seed_import_queue_item(
        {
            **base_item,
            "price": 315000,
            "title": "Immeuble suivi maj prix",
            "updated_at": utcnow_iso(),
        }
    )
    run_pipeline(storage)

    connection = sqlite3.connect(tmp_path / "immovision.db")
    observation_count = connection.execute(
        "SELECT COUNT(*) FROM listing_observation_history"
    ).fetchone()
    price_history_count = connection.execute(
        "SELECT COUNT(*) FROM listing_price_history"
    ).fetchone()
    latest_price = connection.execute(
        "SELECT price FROM normalized_listings WHERE source_listing_id = ?",
        ("history-1",),
    ).fetchone()

    assert observation_count == (3,)
    assert price_history_count == (2,)
    assert latest_price == (315000.0,)
