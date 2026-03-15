import os
from datetime import datetime, timezone

from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_ANON_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_source_id(source_name: str) -> str:
    result = supabase.table("sources").select("id").eq("name", source_name).limit(1).execute()
    if not result.data:
        raise ValueError(f"Source introuvable: {source_name}")
    return result.data[0]["id"]


def fetch_import_queue() -> list[dict]:
    result = (
        supabase.table("import_queue")
        .select("*")
        .eq("is_active", True)
        .execute()
    )
    return result.data or []


def upsert_listing(item: dict) -> str:
    source_id = get_source_id(item["source_name"])
    payload = {
        "source_id": source_id,
        "source_listing_id": item["source_listing_id"],
        "source_url": item["source_url"],
        "title": item.get("title"),
        "description": item.get("description"),
        "price": item.get("price"),
        "postal_code": item.get("postal_code"),
        "commune": item.get("commune"),
        "property_type": item.get("property_type"),
        "transaction_type": item.get("transaction_type") or "sale",
        "existing_units": item.get("existing_units"),
        "surface": item.get("surface"),
        "is_copro": item.get("is_copro", False),
        "is_new_build": item.get("is_new_build", False),
        "is_live_data": item.get("is_live_data", True),
        "last_seen_at": datetime.now(timezone.utc).isoformat(),
    }
    result = (
        supabase.table("normalized_listings")
        .upsert(payload, on_conflict="source_listing_id")
        .execute()
    )
    return result.data[0]["id"]


def build_analysis(item: dict) -> dict:
    commune = item.get("commune")
    price = item.get("price")
    units = item.get("existing_units")
    listing_id = item["listing_id"]
    source_listing_id = item["source_listing_id"]

    if commune in ["Ixelles", "Etterbeek"]:
        rent_per_unit = 950
        rental_score = "Zone locative forte"
    elif commune == "Anderlecht":
        rent_per_unit = 850
        rental_score = "Bonne zone locative"
    else:
        rent_per_unit = None
        rental_score = "À valider"

    if units and price and rent_per_unit:
        total_monthly_rent = rent_per_unit * units
        total_annual_rent = total_monthly_rent * 12
        monthly_loan = round(price * 0.005545, 2)
        gross_yield = round((total_annual_rent / price) * 100, 2)
        monthly_spread = round(total_monthly_rent - monthly_loan, 2)
        price_per_unit = round(price / units, 2)
    else:
        total_monthly_rent = None
        total_annual_rent = None
        monthly_loan = round(price * 0.005545, 2) if price else None
        gross_yield = None
        monthly_spread = None
        price_per_unit = round(price / units, 2) if price and units else None

    if source_listing_id == "21408502":
        compatible = False
        reason = "3 unités annoncées. Prix/unité au-dessus du seuil de 170k."
        score = 42
    elif source_listing_id == "21423814":
        compatible = False
        reason = "Non compatible : situation urbanistique contraire à la stratégie déjà divisée."
        score = 18
    elif source_listing_id == "21422401":
        compatible = False
        reason = "À vérifier : potentiel de division mentionné, unités existantes non confirmées."
        score = 30
    else:
        compatible = False
        reason = item.get("notes") or "À valider"
        score = None

    return {
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


def upsert_analysis(analysis_payload: dict) -> None:
    supabase.table("listing_analysis").upsert(
        analysis_payload,
        on_conflict="listing_id"
    ).execute()


def insert_price_history(listing_id: str, price: float | None) -> None:
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
    queue = fetch_import_queue()

    try:
        for item in queue:
            listing_id = upsert_listing(item)
            item["listing_id"] = listing_id
            analysis_payload = build_analysis(item)
            upsert_analysis(analysis_payload)
            insert_price_history(listing_id, item.get("price"))
            imported += 1

        update_source_counts()
        insert_sync_log("success", len(queue), imported)
        print(f"Import terminé: {imported} annonces traitées.")
    except Exception as e:
        insert_sync_log("error", len(queue), imported, str(e))
        raise


if __name__ == "__main__":
    main()
