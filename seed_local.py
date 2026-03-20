from config import load_config, utcnow_iso
from storage import build_storage


def main() -> None:
    config = load_config()
    if config.backend_name != "sqlite":
        raise RuntimeError(
            "Le seed local fonctionne seulement avec SQLite. "
            "Retire les variables Supabase puis relance."
        )

    storage = build_storage(config)
    storage.seed_import_queue_item(
        {
            "source_name": "Immoweb",
            "source_listing_id": "12345678",
            "source_url": "https://www.immoweb.be/fr/annonce/immeuble-de-rapport/a-vendre/ixelles/12345678",
            "title": "Immeuble de rapport test local",
            "description": "Annonce locale minimale pour valider le pipeline de bout en bout.",
            "price": 300000,
            "postal_code": "1050",
            "commune": "Ixelles",
            "property_type": "apartment_block",
            "transaction_type": "sale",
            "existing_units": 3,
            "surface": 240,
            "is_copro": 0,
            "is_new_build": 0,
            "is_live_data": 1,
            "data_origin": "seed",
            "is_active": 1,
            "notes": "Seed local minimal",
            "updated_at": utcnow_iso(),
        }
    )

    print(f"Seed SQLite pret dans: {config.sqlite_path}")
    print("Listing local insere ou mis a jour: source_listing_id=12345678")


if __name__ == "__main__":
    main()
