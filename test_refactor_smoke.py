from analysis_logic import build_analysis
from config import RuntimeConfig, load_config
from normalization import build_listing_payload
from parsing import extract_immoweb_listing_candidates
from sqlite_storage import SQLiteStorage


def test_build_analysis_keeps_current_formula():
    analysis = build_analysis(
        {
            "listing_id": "42",
            "commune": "Anderlecht",
            "price": 250000,
            "existing_units": 2,
            "notes": "Cas test",
        }
    )

    assert analysis["estimated_rent_per_unit"] == 850
    assert analysis["estimated_total_rent_monthly"] == 1700
    assert analysis["estimated_monthly_loan_payment"] == 1386.25
    assert analysis["estimated_gross_yield"] == 8.16
    assert analysis["compatibility_reason"] == "Cas test"


def test_extract_immoweb_listing_candidates_deduplicates_urls():
    html = """
    <html>
      <body>
        <a href="/fr/annonce/test/ixelles/12345678">One</a>
        <a href="https://www.immoweb.be/fr/annonce/test/ixelles/12345678">Two</a>
        <a href="/fr/autre/chemin">Skip</a>
      </body>
    </html>
    """

    items = extract_immoweb_listing_candidates(
        html,
        source_name="Immoweb",
        search_target_id=7,
    )

    assert items == [
        {
            "source_name": "Immoweb",
            "search_target_id": 7,
            "source_url": "https://www.immoweb.be/fr/annonce/test/ixelles/12345678",
            "source_listing_id": "12345678",
        }
    ]


def test_build_listing_payload_keeps_default_transaction_type():
    payload = build_listing_payload(
        {
            "source_listing_id": "12345678",
            "source_url": "https://example.test/12345678",
        },
        source_id="1",
    )

    assert payload["source_id"] == "1"
    assert payload["source_listing_id"] == "12345678"
    assert payload["transaction_type"] == "sale"


def test_load_config_defaults_to_sqlite(monkeypatch, tmp_path):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_KEY", raising=False)
    monkeypatch.delenv("SUPABASE_ANON_KEY", raising=False)
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "local.db"))

    config = load_config()

    assert isinstance(config, RuntimeConfig)
    assert config.backend_name == "sqlite"
    assert config.sqlite_path == (tmp_path / "local.db").resolve()


def test_sqlite_storage_bootstraps_sources(tmp_path):
    storage = SQLiteStorage(tmp_path / "immovision.db")
    source = storage._fetchone("SELECT name FROM sources WHERE name = ?", ("Immoweb",))

    assert source == {"name": "Immoweb"}


def test_sqlite_storage_seed_api_inserts_queue_item(tmp_path):
    storage = SQLiteStorage(tmp_path / "immovision.db")
    storage.seed_import_queue_item(
        {
            "source_name": "Immoweb",
            "source_listing_id": "seed-1",
            "source_url": "https://example.test/seed-1",
            "title": "Seed test",
            "is_active": 1,
            "is_live_data": 1,
        }
    )

    queue_item = storage._fetchone(
        "SELECT source_listing_id, title FROM import_queue WHERE source_listing_id = ?",
        ("seed-1",),
    )

    assert queue_item == {"source_listing_id": "seed-1", "title": "Seed test"}
