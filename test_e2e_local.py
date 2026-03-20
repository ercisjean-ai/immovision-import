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

    assert stats == {
        "discovered_count": 0,
        "queued": 0,
        "imported": 1,
        "new": 1,
        "seen": 0,
        "modified": 0,
    }

    connection = sqlite3.connect(tmp_path / "immovision.db")
    listing = connection.execute(
        "SELECT source_listing_id, commune, price FROM normalized_listings"
    ).fetchone()
    analysis = connection.execute(
        """
        SELECT estimated_rent_per_unit, estimated_gross_yield, investment_score, investment_score_label, strategy_label, confidence_score, confidence_label, compatibility_reason, confidence_reason
        FROM listing_analysis
        """
    ).fetchone()
    history = connection.execute(
        "SELECT COUNT(*) FROM listing_price_history"
    ).fetchone()
    observations = connection.execute(
        "SELECT COUNT(*) FROM listing_observation_history"
    ).fetchone()
    latest_observation = connection.execute(
        """
        SELECT observation_status, changed_fields, is_price_changed
        FROM listing_observation_history
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    listing_state = connection.execute(
        """
        SELECT observation_count, last_observation_status
        FROM normalized_listings
        WHERE source_listing_id = ?
        """,
        ("12345678",),
    ).fetchone()
    sync = connection.execute(
        "SELECT COUNT(*) FROM source_syncs"
    ).fetchone()

    assert listing == ("12345678", "Ixelles", 300000.0)
    assert analysis[:7] == (950.0, 11.4, 89.0, "Interessant", "A analyser", 68.0, "Correcte")
    assert analysis[7].startswith("Seed local minimal | score 89/100 [Interessant] |")
    assert "prix_unite:28/28" in analysis[7]
    assert "copro:6/15" in analysis[7]
    assert "type:15/15" in analysis[7]
    assert "strategie:A analyser" in analysis[7]
    assert "copropriete inconnue" in analysis[7]
    assert analysis[8].startswith("confiance 68/100 [Correcte] |")
    assert "prix:20/20" in analysis[8]
    assert "transaction:5/5" in analysis[8]
    assert "surface:0/10" in analysis[8]
    assert history == (1,)
    assert observations == (1,)
    assert latest_observation == ("new", None, 1)
    assert listing_state == (1, "new")
    assert sync == (0,)
