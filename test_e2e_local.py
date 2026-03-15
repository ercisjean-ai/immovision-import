import sqlite3

from config import utcnow_iso
from pipeline import run_pipeline
from sqlite_storage import SQLiteStorage


def test_local_sqlite_pipeline_end_to_end(tmp_path):
    storage = SQLiteStorage(tmp_path / "immovision.db")

    storage.seed_import_queue_item(
        {
            "source_name": "Immoweb",
            "source_listing_id": "12345678",
            "source_url": "https://www.immoweb.be/fr/annonce/test/12345678",
            "title": "Immeuble de rapport",
            "price": 300000,
            "commune": "Ixelles",
            "existing_units": 3,
            "is_active": 1,
            "is_live_data": 1,
            "notes": "Seed local minimal",
            "updated_at": utcnow_iso(),
        }
    )

    stats = run_pipeline(storage)

    assert stats == {"discovered_count": 0, "queued": 0, "imported": 1}

    connection = sqlite3.connect(tmp_path / "immovision.db")
    listing = connection.execute(
        "SELECT source_listing_id, commune, price FROM normalized_listings"
    ).fetchone()
    analysis = connection.execute(
        """
        SELECT estimated_rent_per_unit, estimated_gross_yield, compatibility_reason
        FROM listing_analysis
        """
    ).fetchone()
    history = connection.execute(
        "SELECT COUNT(*) FROM listing_price_history"
    ).fetchone()
    sync = connection.execute(
        "SELECT COUNT(*) FROM source_syncs"
    ).fetchone()

    assert listing == ("12345678", "Ixelles", 300000.0)
    assert analysis == (950.0, 11.4, "Seed local minimal")
    assert history == (1,)
    assert sync == (0,)
