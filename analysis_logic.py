from typing import Any

from config import utcnow_iso


def build_analysis(item: dict[str, Any]) -> dict[str, Any]:
    commune = item.get("commune")
    price = item.get("price")
    units = item.get("existing_units")
    listing_id = item["listing_id"]

    if commune in ["Ixelles", "Etterbeek"]:
        rent_per_unit = 950
        rental_score = "Zone locative forte"
    elif commune == "Anderlecht":
        rent_per_unit = 850
        rental_score = "Bonne zone locative"
    else:
        rent_per_unit = None
        rental_score = "A valider"

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

    return {
        "listing_id": listing_id,
        "zone_label": "Zone prioritaire",
        "strategy_compatible": False,
        "compatibility_reason": item.get("notes") or "A valider",
        "price_per_unit": price_per_unit,
        "estimated_rent_per_unit": rent_per_unit,
        "estimated_total_rent_monthly": total_monthly_rent,
        "estimated_total_rent_annual": total_annual_rent,
        "estimated_monthly_loan_payment": monthly_loan,
        "estimated_gross_yield": gross_yield,
        "estimated_monthly_spread": monthly_spread,
        "rental_score_label": rental_score,
        "investment_score": None,
        "updated_at": utcnow_iso(),
    }
