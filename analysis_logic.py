from typing import Any

from config import utcnow_iso


def _estimate_rent_profile(commune: str | None) -> tuple[int | None, str]:
    if commune in ["Ixelles", "Etterbeek"]:
        return 950, "Zone locative forte"
    if commune == "Anderlecht":
        return 850, "Bonne zone locative"
    return None, "A valider"


def _score_location(commune: str | None, postal_code: str | None) -> tuple[int, int, str]:
    max_points = 5
    if commune == "Ixelles":
        return 5, max_points, "Ixelles soutient legerement le score"
    if commune == "Etterbeek":
        return 5, max_points, "Etterbeek soutient legerement le score"
    if commune == "Anderlecht":
        return 3, max_points, "Anderlecht apporte un soutien locatif secondaire"
    if postal_code in {"1000", "1040", "1050"}:
        return 2, max_points, "code postal plutot porteur"
    if commune:
        return 1, max_points, "localisation connue mais non prioritaire"
    return 0, max_points, "localisation incomplete"


def _score_yield(gross_yield: float | None) -> tuple[int, int, str]:
    max_points = 35
    if gross_yield is None:
        return 0, max_points, "rendement brut non calculable"
    if gross_yield >= 10:
        return 35, max_points, f"rendement brut excellent ({gross_yield}%)"
    if gross_yield >= 8:
        return 28, max_points, f"rendement brut solide ({gross_yield}%)"
    if gross_yield >= 6:
        return 18, max_points, f"rendement brut correct ({gross_yield}%)"
    if gross_yield > 0:
        return 8, max_points, f"rendement brut limite ({gross_yield}%)"
    return 0, max_points, "rendement brut faible"


def _score_price_per_unit(price_per_unit: float | None) -> tuple[int, int, str]:
    max_points = 20
    if price_per_unit is None:
        return 0, max_points, "prix par unite non calculable"
    if price_per_unit <= 120000:
        return 20, max_points, f"prix par unite attractif ({price_per_unit})"
    if price_per_unit <= 150000:
        return 16, max_points, f"prix par unite raisonnable ({price_per_unit})"
    if price_per_unit <= 190000:
        return 10, max_points, f"prix par unite acceptable ({price_per_unit})"
    if price_per_unit <= 240000:
        return 4, max_points, f"prix par unite tendu ({price_per_unit})"
    return 0, max_points, f"prix par unite eleve ({price_per_unit})"


def _score_units(existing_units: int | None) -> tuple[int, int, str]:
    max_points = 15
    if existing_units is None:
        return 0, max_points, "nombre d'unites inconnu"
    if existing_units >= 4:
        return 15, max_points, f"{existing_units} unites en place"
    if existing_units == 3:
        return 13, max_points, "3 unites en place"
    if existing_units == 2:
        return 10, max_points, "2 unites existantes"
    if existing_units == 1:
        return 4, max_points, "1 unite seulement"
    return 0, max_points, "nombre d'unites atypique"


def _score_surface(surface: float | None) -> tuple[int, int, str]:
    max_points = 10
    if surface is None:
        return 0, max_points, "surface inconnue"
    if surface >= 220:
        return 10, max_points, f"surface genereuse ({surface} m2)"
    if surface >= 160:
        return 8, max_points, f"surface confortable ({surface} m2)"
    if surface >= 100:
        return 5, max_points, f"surface correcte ({surface} m2)"
    return 2, max_points, f"surface limitee ({surface} m2)"


def _score_property_type(property_type: str | None) -> tuple[int, int, str]:
    max_points = 10
    if property_type == "apartment_block":
        return 10, max_points, "type de bien bien adapte a l'investissement"
    if property_type == "house":
        return 6, max_points, "maison exploitable mais moins optimisee"
    if property_type == "apartment":
        return 5, max_points, "appartement interessant mais moins scalable"
    if property_type:
        return 3, max_points, "type de bien a confirmer"
    return 0, max_points, "type de bien manquant"


def _score_transaction_type(transaction_type: str | None) -> tuple[int, int, str]:
    max_points = 5
    if (transaction_type or "sale") == "sale":
        return 5, max_points, "vente compatible avec ce pipeline d'investissement"
    return 0, max_points, "transaction atypique"


def _label_from_score(score: int) -> str:
    if score >= 70:
        return "Interessant"
    if score >= 50:
        return "Moyen"
    if score >= 30:
        return "A revoir"
    return "Faible"


def _confidence_label(score: int) -> str:
    if score >= 80:
        return "Elevee"
    if score >= 60:
        return "Correcte"
    if score >= 40:
        return "Fragile"
    return "Faible"


def _build_score_explanation(
    criteria: list[tuple[str, int, int, str]],
    total_score: int,
    label: str,
) -> str:
    factors = " | ".join(
        f"{name}:{points}/{max_points} ({detail})"
        for name, points, max_points, detail in criteria
    )
    return f"score {total_score}/100 [{label}] | {factors}"


def _build_confidence_explanation(
    details: list[tuple[str, int, int, str]],
    score: int,
    label: str,
) -> str:
    factors = " | ".join(
        f"{name}:{points}/{max_points} ({detail})"
        for name, points, max_points, detail in details
    )
    return f"confiance {score}/100 [{label}] | {factors}"


def calculate_investment_score(
    item: dict[str, Any],
    *,
    gross_yield: float | None,
    price_per_unit: float | None,
) -> tuple[int, str, str, bool]:
    criteria = [
        ("rendement", *_score_yield(gross_yield)),
        ("prix_unite", *_score_price_per_unit(price_per_unit)),
        ("unites", *_score_units(item.get("existing_units"))),
        ("surface", *_score_surface(item.get("surface"))),
        ("type", *_score_property_type(item.get("property_type"))),
        ("transaction", *_score_transaction_type(item.get("transaction_type"))),
        ("localisation", *_score_location(item.get("commune"), item.get("postal_code"))),
    ]

    score = min(100, sum(points for _, points, _, _ in criteria))
    label = _label_from_score(score)
    explanation = _build_score_explanation(criteria, score, label)
    compatible = score >= 50
    return score, label, explanation, compatible


def calculate_confidence_score(
    item: dict[str, Any],
    *,
    gross_yield: float | None,
    rent_per_unit: int | None,
) -> tuple[int, str, str]:
    details: list[tuple[str, int, int, str]] = []

    price = item.get("price")
    if price and price > 0:
        details.append(("prix", 20, 20, "prix present"))
    else:
        details.append(("prix", 0, 20, "prix manquant ou incoherent"))

    if item.get("commune") and item.get("postal_code"):
        details.append(("localisation", 15, 15, "commune et code postal presents"))
    elif item.get("commune") or item.get("postal_code"):
        details.append(("localisation", 8, 15, "localisation partielle"))
    else:
        details.append(("localisation", 0, 15, "localisation absente"))

    property_type = item.get("property_type")
    if property_type in {"apartment_block", "house", "apartment"}:
        details.append(("type", 10, 10, "type de bien exploitable"))
    elif property_type:
        details.append(("type", 5, 10, "type de bien ambigu"))
    else:
        details.append(("type", 0, 10, "type de bien absent"))

    surface = item.get("surface")
    if surface and surface > 0:
        details.append(("surface", 10, 10, "surface presente"))
    else:
        details.append(("surface", 0, 10, "surface absente"))

    units = item.get("existing_units")
    if units and units > 0:
        details.append(("unites", 10, 10, "nombre d'unites present"))
    else:
        details.append(("unites", 0, 10, "nombre d'unites absent"))

    transaction_type = item.get("transaction_type")
    if transaction_type:
        details.append(("transaction", 5, 5, "transaction renseignee"))
    else:
        details.append(("transaction", 2, 5, "transaction supposee par defaut"))

    if gross_yield is not None and rent_per_unit is not None:
        details.append(("rendement", 20, 20, "rendement calcule sur hypothese locative connue"))
    elif gross_yield is None and rent_per_unit is not None:
        details.append(("rendement", 10, 20, "hypothese locative connue mais calcul incomplet"))
    elif gross_yield is not None:
        details.append(("rendement", 8, 20, "rendement calcule avec hypothese fragile"))
    else:
        details.append(("rendement", 0, 20, "rendement non calculable"))

    description = item.get("description")
    title = item.get("title")
    if title and description:
        details.append(("contenu", 10, 10, "titre et description presents"))
    elif title or description:
        details.append(("contenu", 5, 10, "contenu partiel"))
    else:
        details.append(("contenu", 0, 10, "contenu tres pauvre"))

    score = sum(points for _, points, _, _ in details)
    label = _confidence_label(score)
    explanation = _build_confidence_explanation(details, score, label)
    return score, label, explanation


def build_analysis(item: dict[str, Any]) -> dict[str, Any]:
    commune = item.get("commune")
    price = item.get("price")
    units = item.get("existing_units")
    listing_id = item["listing_id"]

    rent_per_unit, rental_score = _estimate_rent_profile(commune)

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

    score, score_label, explanation, compatible = calculate_investment_score(
        item,
        gross_yield=gross_yield,
        price_per_unit=price_per_unit,
    )
    confidence_score, confidence_label, confidence_reason = calculate_confidence_score(
        item,
        gross_yield=gross_yield,
        rent_per_unit=rent_per_unit,
    )

    note = item.get("notes")
    compatibility_reason = explanation if not note else f"{note} | {explanation}"

    return {
        "listing_id": listing_id,
        "zone_label": "Zone prioritaire",
        "strategy_compatible": compatible,
        "compatibility_reason": compatibility_reason,
        "price_per_unit": price_per_unit,
        "estimated_rent_per_unit": rent_per_unit,
        "estimated_total_rent_monthly": total_monthly_rent,
        "estimated_total_rent_annual": total_annual_rent,
        "estimated_monthly_loan_payment": monthly_loan,
        "estimated_gross_yield": gross_yield,
        "estimated_monthly_spread": monthly_spread,
        "rental_score_label": rental_score,
        "investment_score": score,
        "investment_score_label": score_label,
        "confidence_score": confidence_score,
        "confidence_label": confidence_label,
        "confidence_reason": confidence_reason,
        "updated_at": utcnow_iso(),
    }
