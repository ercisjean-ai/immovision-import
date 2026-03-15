import os
from datetime import datetime, timezone

from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_ANON_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

REAL_LISTINGS = [
    {
        "source_name": "Immoweb",
        "source_listing_id": "21408502",
        "source_url": "https://www.immoweb.be/fr/annonce/immeuble-a-appartements/a-vendre/etterbeek/1040/21408502?s=s_XL",
        "title": "Immeuble à appartements – Etterbeek",
        "description": "Maison bourgeoise de 272 m² avec possibilités multiples. Composition annoncée : souplex/rez, appartement 1 chambre au 1er, duplex 1 chambre au 2e.",
        "price": 795000,
        "postal_code": "1040",
        "commune": "Etterbeek",
        "property_type": "immeuble à appartements",
        "transaction_type": "sale",
        "existing_units": 3,
        "surface": 272,
        "is_copro": False,
        "is_new_build": False,
        "is_live_data": True,
    },
    {
        "source_name": "Immoweb",
        "source_listing_id": "21423814",
        "source_url": "https://www.immoweb.be/fr/annonce/immeuble-mixte/a-vendre/anderlecht/1070/21423814?s=s_XL",
        "title": "Immeuble mixte – Anderlecht",
        "description": "Immeuble mixte à régulariser et à rénover. Mise en demeure communale : retour vers maison unifamiliale et rez commercial.",
        "price": 255000,
        "postal_code": "1070",
        "commune": "Anderlecht",
        "property_type": "mixte",
        "transaction_type": "sale",
        "existing_units": 4,
        "surface": 157,
        "is_copro": False,
        "is_new_build": False,
        "is_live_data": True,
    },
    {
        "source_name": "Immoweb",
        "source_listing_id": "21422401",
        "source_url": "https://www.immoweb.be/fr/annonce/immeuble-a-appartements/a-vendre/ixelles/1050/21422401?s=s_XL",
        "title": "Immeuble à appartements – Ixelles",
        "description": "Immeuble de rapport 177 m² avec potentiel d’aménagement ou de division sous réserve d’urbanisme.",
        "price": 499000,
        "postal_code": "1050",
        "commune": "Ixelles",
        "property_type": "immeuble à appartements",
        "transaction_type": "sale",
        "existing_units": None,
        "surface": 177,
        "is_copro": False,
        "is_new_build": False,
        "is_live_data": True,
    },
]


def get_source_id(source_name: str) -> str:
    result = supabase.table("sources").select("id").eq("name", source_name).limit(1).execute()
    if not result.data:
        raise ValueError(f"Source introuvable: {source_name}")
    return result.data[0]["id"]


def upsert_listing(listing: dict) -> str:
    source_id = get_source_id(listing["source_name"])
    payload = {
        "source_id": source_id,
        "source_listing_id": listing["source_listing_id"],
        "source_url": listing["source_url"],
        "title": listing["title"],
        "description": listing["description"],
        "price": listing["price"],
        "postal_code": listing["postal_code"],
        "commune": listing["commune"],
        "property_type": listing["property_type"],
        "transaction_type": listing["transaction_type"],
        "existing_units": listing["existing_units"],
        "surface": listing["surface"],
        "is_copro": listing["is_copro"],
        "is_new_build": listing["is_new_build"],
        "is_live_data": listing["is_live_data"],
        "last_seen_at": datetime.now(timezone.utc).isoformat(),
    }
    result = (
        supabase.table("normalized_listings")
        .upsert(payload, on_conflict="source_listing_id")
        .execute()
    )
    row = result.data[0]
    return row["id"]


def upsert_analysis(listing_id: str, listing: dict) -> None:
    commune = listing["commune"]
    price = listing["price"]
    units = listing["existing_units"]

    if commune in ["Ixelles", "Etterbeek"]:
        rent_per_unit = 950
        rental_score = "Zone locative forte"
    elif commune == "Anderlecht":
        rent_per_unit = 850
        rental_score = "Bonne zone locative"
    else:
        rent_per_unit = None
        rental_score = "À valider"

    if units and price:
        total_monthly_rent = rent_per_unit * units if rent_per_unit else None
        total_annual_rent = total_monthly_rent * 12 if total_monthly_rent else None
        monthly_loan = round(price * 0.005545, 2)
        gross_yield = round((total_annual_rent / price) * 100, 2) if total_annual_rent else None
        monthly_spread = round(total_monthly_rent - monthly_loan, 2) if total_monthly_rent else None
        price_per_unit = round(price / units, 2)
    else:
        total_monthly_rent = None
        total_annual_rent = None
        monthly_loan = round(price * 0.005545, 2) if price else None
        gross_yield = None
        monthly_spread = None
        price_per_unit = None

    if listing["source_listing_id"] == "21408502":
        compatible = False
        reason = "3 unités annoncées. Prix/unité au-dessus du seuil de 170k."
        score = 42
    elif listing["source_listing_id"] == "21423814":
        compatible = False
        reason = "Non compatible : situation urbanistique contraire à la stratégie déjà divisée."
        score = 18
    elif listing["source_listing_id"] == "21422401":
        compatible = False
        reason = "À vérifier : potentiel de division mentionné, unités existantes non confirmées."
        score = 30
    else:
        compatible = False
        reason = "À valider"
        score = None

    analysis_payload = {
        "listing_id": listing_id,
        "zone_label": "Zone prioritaire",
        "strategy_compatible": compatible,
        "compatibility_reason": reason,
        "price_per_unit": price_per_unit,
        "estimated_rent_per_unit": rent_per_unit,
        "estimated_total_rent_monthly": total_monthly_rent,
        "estimated_total_rent_annual": total_annual_rent,
        "estimated_monthly_loan_payment": monthly_loan,
        "estimated_gross_yield": gross_yield,
        "estimated_monthly_spread": monthly_spread,
        "rental_score_label": rental_score,
        "investment_score": score,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    supabase.table("listing_analysis").upsert(analysis_payload, on_conflict="listing_id").execute()


def insert_price_history(listing_id: str, price: float) -> None:
    if price is None:
        return
    supabase.table("listing_price_history").insert({
        "listing_id": listing_id,
        "price": price,
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }).execute()


def update_source_counts() -> None:
    sources = supabase.table("sources").select("id,name").execute().data
    for source in sources:
        count = (
            supabase.table("normalized_listings")
            .select("id", count="exact")
            .eq("source_id", source["id"])
            .eq("is_live_data", True)
            .execute()
            .count
        )
        supabase.table("sources").update({
            "live_count": count or 0,
            "last_sync": datetime.now(timezone.utc).isoformat(),
        }).eq("id", source["id"]).execute()


def insert_sync_log(status: str, listings_found: int, listings_imported: int, error_message: str | None = None) -> None:
    immoweb = supabase.table("sources").select("id").eq("name", "Immoweb").limit(1).execute().data
    source_id = immoweb[0]["id"] if immoweb else None
    supabase.table("source_syncs").insert({
        "source_id": source_id,
        "status": status,
        "listings_found": listings_found,
        "listings_imported": listings_imported,
        "error_message": error_message,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }).execute()


def main() -> None:
    imported = 0
    try:
        for listing in REAL_LISTINGS:
            listing_id = upsert_listing(listing)
            upsert_analysis(listing_id, listing)
            insert_price_history(listing_id, listing["price"])
            imported += 1

        update_source_counts()
        insert_sync_log("success", len(REAL_LISTINGS), imported)
        print(f"Import terminé: {imported} annonces traitées.")
    except Exception as e:
        insert_sync_log("error", len(REAL_LISTINGS), imported, str(e))
        raise


if __name__ == "__main__":
    main()
