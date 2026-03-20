import unicodedata
from typing import Any

from config import utcnow_iso


BRUSSELS_TARGET_COMMUNES = {
    "anderlecht",
    "auderghem",
    "berchem-sainte-agathe",
    "berchem saint agathe",
    "bruxelles",
    "bruxelles-ville",
    "brussels",
    "etterbeek",
    "evere",
    "forest",
    "ganshoren",
    "haren",
    "ixelles",
    "jette",
    "koekelberg",
    "laeken",
    "molenbeek-saint-jean",
    "molenbeek saint jean",
    "neder-over-heembeek",
    "saint-gilles",
    "saint-josse-ten-noode",
    "saint josse ten noode",
    "schaerbeek",
    "uccle",
    "watermael-boitsfort",
    "woluwe-saint-lambert",
    "woluwe-saint-pierre",
}
PERIPHERY_TARGET_COMMUNES = {
    "asse",
    "beersel",
    "dilbeek",
    "drogenbos",
    "grimbergen",
    "kraainem",
    "linkebeek",
    "machelen",
    "rhode-saint-genese",
    "sint-genesius-rode",
    "strombeek-bever",
    "vilvoorde",
    "wemmel",
    "wezembeek-oppem",
    "zaventem",
    "zellik",
}
BRUSSELS_POSTAL_CODES = {
    "1000",
    "1020",
    "1030",
    "1040",
    "1050",
    "1060",
    "1070",
    "1080",
    "1081",
    "1082",
    "1083",
    "1090",
    "1140",
    "1150",
    "1160",
    "1170",
    "1180",
    "1190",
    "1200",
}
PERIPHERY_POSTAL_CODES = {
    "1600",
    "1620",
    "1630",
    "1650",
    "1700",
    "1730",
    "1731",
    "1780",
    "1800",
    "1830",
    "1850",
    "1860",
    "1930",
}
NOTARIAL_SOURCES = {"Biddit", "Notaire.be"}
PRIORITY_PROPERTY_TYPES = {"apartment_block", "commercial_house"}
ACCEPTED_PROPERTY_TYPES = PRIORITY_PROPERTY_TYPES | {
    "commercial",
    "mixed_use",
    "house",
    "apartment",
}


def _to_optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    return bool(value)


def _resolve_copro_status(item: dict[str, Any]) -> str:
    if "copro_status" in item:
        explicit_status = item.get("copro_status")
        if explicit_status in (None, ""):
            return "unknown"
        lowered = str(explicit_status).strip().lower()
        if lowered in {"true", "1", "yes", "y", "on"}:
            return "true"
        if lowered in {"false", "0", "no", "n", "off"}:
            return "false"
        return "unknown"

    normalized_copro = _to_optional_bool(item.get("is_copro"))
    if normalized_copro is True:
        return "true"
    if normalized_copro is False and "is_copro" in item:
        return "false"
    return "unknown"


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_value.lower().replace("/", " ").split())


def _infer_property_type_from_text(text: str) -> str | None:
    normalized = _normalize_text(text)
    if not normalized:
        return None
    if any(
        token in normalized
        for token in (
            "maison de commerce",
            "commerce house",
            "handelshuis",
            "woning met handelszaak",
            "immeuble mixte",
            "mixed use building",
        )
    ):
        return "commercial_house"
    if any(
        token in normalized
        for token in (
            "immeuble de rapport",
            "meergezinswoning",
            "rapportgebouw",
            "building",
        )
    ):
        return "apartment_block"
    if any(
        token in normalized
        for token in (
            "commerce",
            "commercial",
            "shop",
            "retail",
            "horeca",
            "winkel",
            "handelszaak",
        )
    ):
        return "commercial"
    if any(token in normalized for token in ("maison", "huis", "woning", "house")):
        return "house"
    if any(
        token in normalized
        for token in ("appartement", "apartment", "flat")
    ):
        return "apartment"
    if any(token in normalized for token in ("terrain", "grond", "land")):
        return "land"
    if any(token in normalized for token in ("garage", "parking")):
        return "garage"
    return None


def _resolve_property_type(item: dict[str, Any]) -> str | None:
    explicit_type = item.get("property_type")
    if explicit_type:
        return explicit_type
    text_seed = " ".join(
        part for part in [item.get("title"), item.get("description")] if part
    )
    return _infer_property_type_from_text(text_seed)


def _classify_zone(
    commune: str | None,
    postal_code: str | None,
) -> tuple[str, str]:
    normalized_commune = _normalize_text(commune)
    postal_code = (postal_code or "").strip()
    if normalized_commune in BRUSSELS_TARGET_COMMUNES or postal_code in BRUSSELS_POSTAL_CODES:
        return "Bruxelles cible", "commune bruxelloise cible"
    if normalized_commune in PERIPHERY_TARGET_COMMUNES or postal_code in PERIPHERY_POSTAL_CODES:
        return "Peripherie cible", "commune proche de Bruxelles compatible avec la strategie"
    if commune or postal_code:
        return "Zone a analyser", "localisation hors zone coeur mais encore analysable"
    return "Zone inconnue", "localisation insuffisante pour juger la zone"


def _estimate_rent_profile(
    commune: str | None,
    zone_label: str,
) -> tuple[int | None, str]:
    normalized_commune = _normalize_text(commune)
    if normalized_commune in {"ixelles", "etterbeek"}:
        return 950, "Zone locative prioritaire"
    if zone_label == "Bruxelles cible":
        return 850, "Zone locative cible Bruxelles"
    if zone_label == "Peripherie cible":
        return 800, "Zone locative cible proche Bruxelles"
    return None, "A valider"


def _score_location(zone_label: str) -> tuple[int, int, str]:
    max_points = 10
    if zone_label == "Bruxelles cible":
        return 10, max_points, "Bruxelles cible pour cette strategie"
    if zone_label == "Peripherie cible":
        return 8, max_points, "peripherie proche compatible"
    if zone_label == "Zone a analyser":
        return 3, max_points, "zone possible mais hors coeur de cible"
    return 0, max_points, "zone insuffisamment renseignee"


def _score_yield(gross_yield: float | None) -> tuple[int, int, str]:
    max_points = 8
    if gross_yield is None:
        return 0, max_points, "rendement brut non calculable"
    if gross_yield >= 10:
        return 8, max_points, f"rendement brut excellent ({gross_yield}%)"
    if gross_yield >= 8:
        return 6, max_points, f"rendement brut solide ({gross_yield}%)"
    if gross_yield >= 6:
        return 4, max_points, f"rendement brut correct ({gross_yield}%)"
    if gross_yield > 0:
        return 2, max_points, f"rendement brut limite ({gross_yield}%)"
    return 0, max_points, "rendement brut faible"


def _score_price_per_unit(price_per_unit: float | None) -> tuple[int, int, str]:
    max_points = 28
    if price_per_unit is None:
        return 0, max_points, "prix par unite non calculable"
    if price_per_unit <= 120000:
        return 28, max_points, f"prix par unite tres attractif ({price_per_unit})"
    if price_per_unit <= 150000:
        return 24, max_points, f"prix par unite attractif ({price_per_unit})"
    if price_per_unit <= 170000:
        return 20, max_points, f"prix par unite encore dans le critere cible ({price_per_unit})"
    if price_per_unit <= 190000:
        return 8, max_points, f"prix par unite au-dessus du critere cible ({price_per_unit})"
    if price_per_unit <= 220000:
        return 3, max_points, f"prix par unite tendu ({price_per_unit})"
    return 0, max_points, f"prix par unite hors cible ({price_per_unit})"


def _score_units(existing_units: int | None) -> tuple[int, int, str]:
    max_points = 20
    if existing_units is None:
        return 0, max_points, "nombre d'unites inconnu"
    if existing_units >= 4:
        return 20, max_points, f"{existing_units} unites en place"
    if existing_units == 3:
        return 18, max_points, "3 unites en place"
    if existing_units == 2:
        return 15, max_points, "2 unites existantes"
    if existing_units == 1:
        return 0, max_points, "moins de 2 unites"
    return 0, max_points, "nombre d'unites atypique"


def _score_copro(item: dict[str, Any]) -> tuple[int, int, str]:
    max_points = 15
    copro_status = _resolve_copro_status(item)
    if copro_status == "true":
        return 0, max_points, "copropriete penalisee par la strategie"
    if copro_status == "false":
        return 15, max_points, "hors copropriete"
    return 6, max_points, "copropriete inconnue, analyse complementaire requise"


def _score_property_type(property_type: str | None) -> tuple[int, int, str]:
    max_points = 15
    if property_type in {"apartment_block", "commercial_house"}:
        return 15, max_points, "type prioritaire pour cette strategie"
    if property_type in {"commercial", "mixed_use"}:
        return 12, max_points, "commerce accepte mais moins prioritaire"
    if property_type == "house":
        return 7, max_points, "maison acceptable mais non prioritaire"
    if property_type == "apartment":
        return 5, max_points, "appartement moins adapte a la strategie"
    if property_type:
        return 2, max_points, "type de bien marginal pour la strategie"
    return 0, max_points, "type de bien manquant"


def _score_transaction_context(
    source_name: str | None,
    transaction_type: str | None,
) -> tuple[int, int, str]:
    max_points = 4
    if (transaction_type or "sale") != "sale":
        return 0, max_points, "transaction hors cadre achat classique"
    if source_name in NOTARIAL_SOURCES:
        return 2, max_points, "vente notariale ou enchere, prudence sur le prix final"
    return 4, max_points, "vente classique"


def _label_from_score(score: int) -> str:
    if score >= 75:
        return "Interessant"
    if score >= 55:
        return "Moyen"
    if score >= 35:
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


def _classify_strategy_fit(
    item: dict[str, Any],
    *,
    price_per_unit: float | None,
    zone_label: str,
    property_type: str | None,
) -> tuple[str, bool, str]:
    blockers: list[str] = []
    cautions: list[str] = []
    positives: list[str] = []

    units = item.get("existing_units")
    if units is None:
        cautions.append("nombre d'unites a confirmer")
    elif units < 2:
        blockers.append("moins de 2 unites")
    else:
        positives.append("2 unites ou plus")

    if price_per_unit is None:
        cautions.append("prix par unite non calculable")
    elif price_per_unit > 170000:
        blockers.append("prix par unite > 170000 EUR")
    else:
        positives.append("prix par unite <= 170000 EUR")

    copro_status = _resolve_copro_status(item)
    if copro_status == "true":
        blockers.append("copropriete")
    elif copro_status == "false":
        positives.append("hors copropriete")
    else:
        cautions.append("copropriete inconnue")

    if zone_label in {"Bruxelles cible", "Peripherie cible"}:
        positives.append(zone_label.lower())
    elif zone_label == "Zone a analyser":
        cautions.append("zone hors coeur de cible")
    else:
        cautions.append("zone inconnue")

    if property_type in PRIORITY_PROPERTY_TYPES:
        positives.append("type prioritaire")
    elif property_type in ACCEPTED_PROPERTY_TYPES:
        cautions.append("type acceptable mais non prioritaire")
    elif property_type:
        cautions.append("type marginal pour la strategie")
    else:
        cautions.append("type de bien absent")

    if item.get("source_name") in NOTARIAL_SOURCES:
        cautions.append("vente notariale ou enchere a analyser avec prudence")

    if blockers:
        detail = "; ".join(blockers)
        return "Hors criteres", False, f"bloquants: {detail}"
    if cautions:
        detail = "; ".join(cautions)
        return "A analyser", False, f"points a verifier: {detail}"
    detail = "; ".join(positives) if positives else "criteres principaux alignes"
    return "Compatible", True, f"criteres alignes: {detail}"


def calculate_investment_score(
    item: dict[str, Any],
    *,
    gross_yield: float | None,
    price_per_unit: float | None,
) -> tuple[int, str, str, bool]:
    property_type = _resolve_property_type(item)
    zone_label, _ = _classify_zone(item.get("commune"), item.get("postal_code"))
    criteria = [
        ("rendement", *_score_yield(gross_yield)),
        ("prix_unite", *_score_price_per_unit(price_per_unit)),
        ("unites", *_score_units(item.get("existing_units"))),
        ("copro", *_score_copro(item)),
        ("type", *_score_property_type(property_type)),
        ("localisation", *_score_location(zone_label)),
        (
            "contexte_vente",
            *_score_transaction_context(
                item.get("source_name"),
                item.get("transaction_type"),
            ),
        ),
    ]

    score = min(100, sum(points for _, points, _, _ in criteria))
    label = _label_from_score(score)
    explanation = _build_score_explanation(criteria, score, label)
    compatible = score >= 60
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
    if property_type in ACCEPTED_PROPERTY_TYPES:
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
        details.append(
            ("rendement", 20, 20, "rendement calcule sur hypothese locative connue")
        )
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
    property_type = _resolve_property_type(item)
    zone_label, zone_reason = _classify_zone(commune, item.get("postal_code"))

    rent_per_unit, rental_score = _estimate_rent_profile(commune, zone_label)

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

    score, score_label, explanation, _ = calculate_investment_score(
        {**item, "property_type": property_type},
        gross_yield=gross_yield,
        price_per_unit=price_per_unit,
    )
    confidence_score, confidence_label, confidence_reason = calculate_confidence_score(
        item,
        gross_yield=gross_yield,
        rent_per_unit=rent_per_unit,
    )
    strategy_label, strategy_compatible, strategy_reason = _classify_strategy_fit(
        item,
        price_per_unit=price_per_unit,
        zone_label=zone_label,
        property_type=property_type,
    )

    note = item.get("notes")
    strategy_fragment = (
        f"strategie:{strategy_label} ({strategy_reason}) | zone:{zone_label} ({zone_reason})"
    )
    compatibility_reason = f"{explanation} | {strategy_fragment}"
    if note:
        compatibility_reason = f"{note} | {compatibility_reason}"

    return {
        "listing_id": listing_id,
        "zone_label": zone_label,
        "strategy_compatible": strategy_compatible,
        "strategy_label": strategy_label,
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
