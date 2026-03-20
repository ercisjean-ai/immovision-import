from analysis_logic import (
    build_analysis,
    calculate_confidence_score,
    calculate_investment_score,
)
from config import (
    DEFAULT_SQLITE_FILENAME,
    PROJECT_ROOT,
    RuntimeConfig,
    load_config,
)
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
    assert analysis["investment_score"] == 65
    assert analysis["investment_score_label"] == "Moyen"
    assert analysis["strategy_label"] == "A analyser"
    assert analysis["confidence_score"] == 60
    assert analysis["confidence_label"] == "Correcte"
    assert analysis["compatibility_reason"].startswith("Cas test | score 65/100 [Moyen] |")
    assert "rendement:6/8" in analysis["compatibility_reason"]
    assert "prix_unite:24/28" in analysis["compatibility_reason"]
    assert "copro:6/15" in analysis["compatibility_reason"]
    assert "strategie:A analyser" in analysis["compatibility_reason"]
    assert "copropriete inconnue" in analysis["compatibility_reason"]



def test_calculate_investment_score_is_transparent():
    score, label, explanation, compatible = calculate_investment_score(
        {
            "source_name": "Immoweb",
            "commune": "Ixelles",
            "postal_code": "1050",
            "existing_units": 3,
            "surface": 240,
            "property_type": "apartment_block",
            "is_copro": False,
            "transaction_type": "sale",
        },
        gross_yield=10.36,
        price_per_unit=110000,
    )

    assert score == 98
    assert label == "Interessant"
    assert compatible is True
    assert "score 98/100 [Interessant]" in explanation
    assert "rendement:8/8" in explanation
    assert "copro:15/15" in explanation
    assert "localisation:10/10" in explanation



def test_build_analysis_marks_hors_criteres_when_strategy_blockers_exist():
    analysis = build_analysis(
        {
            "listing_id": "99",
            "source_name": "Immoweb",
            "title": "Maison de commerce a revoir",
            "price": 410000,
            "commune": "Vilvoorde",
            "postal_code": "1800",
            "existing_units": 1,
            "is_copro": True,
            "transaction_type": "sale",
        }
    )

    assert analysis["price_per_unit"] == 410000.0
    assert analysis["strategy_label"] == "Hors criteres"
    assert analysis["strategy_compatible"] is False
    assert "moins de 2 unites" in analysis["compatibility_reason"]
    assert "copropriete" in analysis["compatibility_reason"]



def test_build_analysis_prioritizes_commercial_house_in_target_zone():
    analysis = build_analysis(
        {
            "listing_id": "100",
            "source_name": "Immoweb",
            "title": "Maison de commerce avec logements",
            "description": "Maison de commerce avec 3 unites a Vilvoorde",
            "price": 420000,
            "commune": "Vilvoorde",
            "postal_code": "1800",
            "existing_units": 3,
            "surface": 260,
            "is_copro": False,
            "transaction_type": "sale",
        }
    )

    assert analysis["price_per_unit"] == 140000.0
    assert analysis["strategy_label"] == "Compatible"
    assert analysis["strategy_compatible"] is True
    assert analysis["zone_label"] == "Peripherie cible"
    assert "type prioritaire" in analysis["compatibility_reason"]


def test_unknown_copro_never_reaches_full_compatible_strategy():
    analysis = build_analysis(
        {
            "listing_id": "101",
            "source_name": "Immoweb",
            "title": "Immeuble de rapport",
            "price": 360000,
            "commune": "Ixelles",
            "postal_code": "1050",
            "property_type": "apartment_block",
            "existing_units": 3,
            "surface": 255,
            "transaction_type": "sale",
        }
    )

    assert analysis["investment_score"] == 87
    assert analysis["strategy_label"] == "A analyser"
    assert analysis["strategy_compatible"] is False
    assert "copropriete inconnue" in analysis["compatibility_reason"]



def test_calculate_confidence_score_flags_missing_data():
    score, label, explanation = calculate_confidence_score(
        {
            "title": "Annonce minimale",
        },
        gross_yield=None,
        rent_per_unit=None,
    )

    assert score == 7
    assert label == "Faible"
    assert "confiance 7/100 [Faible]" in explanation
    assert "prix:0/20" in explanation
    assert "rendement:0/20" in explanation



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
            "source_name": "Immoweb",
            "source_listing_id": "12345678",
            "source_url": "https://example.test/12345678",
        },
        source_id="1",
    )

    assert payload["source_id"] == "1"
    assert payload["source_name"] == "Immoweb"
    assert payload["source_listing_id"] == "12345678"
    assert payload["transaction_type"] == "sale"
    assert payload["copro_status"] == "unknown"



def test_load_config_defaults_to_sqlite(monkeypatch, tmp_path):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_KEY", raising=False)
    monkeypatch.delenv("SUPABASE_ANON_KEY", raising=False)
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "local.db"))

    config = load_config()

    assert isinstance(config, RuntimeConfig)
    assert config.backend_name == "sqlite"
    assert config.sqlite_path == (tmp_path / "local.db").resolve()


def test_load_config_defaults_to_repo_sqlite_path(monkeypatch):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_KEY", raising=False)
    monkeypatch.delenv("SUPABASE_ANON_KEY", raising=False)
    monkeypatch.delenv("SQLITE_PATH", raising=False)

    config = load_config()

    assert isinstance(config, RuntimeConfig)
    assert config.backend_name == "sqlite"
    assert config.sqlite_path == (PROJECT_ROOT / DEFAULT_SQLITE_FILENAME).resolve()



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
