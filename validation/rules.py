"""Contrat de données : validation, normalisation et empreinte d'un message de prix.

Fonctions pures (sans Kafka), donc testables seules : voir validation/test_rules.py.
"""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

from jsonschema import Draft202012Validator

SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "market_price.schema.json"
SCHEMA = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
_validator = Draft202012Validator(SCHEMA)
CONTRACT_VERSION = SCHEMA["x-contract-version"]  # écrit dans la DLQ (validator_version)

KEY_FIELDS = ("item_id", "city", "quality")
PRICE_FIELDS = ("sell_price_min", "sell_price_max", "buy_price_min", "buy_price_max")


def schema_errors(message) -> List[str]:
    """Erreurs de structure et de type (JSON Schema), sous la forme 'chemin: message'."""
    errors = []
    for err in sorted(_validator.iter_errors(message), key=lambda e: list(e.absolute_path)):
        path = ".".join(str(p) for p in err.absolute_path) or "(racine)"
        errors.append(f"{path}: {err.message}")
    return errors


def _parse_utc(value: str) -> datetime:
    """Date ISO -> datetime UTC. Sans fuseau, l'heure est considérée comme UTC (cas de l'API Albion)."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def business_errors(data: dict, extract_date: Optional[str] = None) -> List[str]:
    """Règles de cohérence que JSON Schema ne sait pas exprimer simplement."""
    errors = []
    # Dates bien formées mais impossibles (mois 13, 30 février...) : le motif du schéma ne les voit pas
    for name, value in [("extract_date", extract_date)] + [(f"data.{f}_date", data[f"{f}_date"]) for f in PRICE_FIELDS]:
        if value is not None:
            try:
                _parse_utc(value)
            except ValueError:
                errors.append(f"{name}: date impossible {value}")
    if errors:
        return errors
    for field in PRICE_FIELDS:
        price, date = data[field], data[f"{field}_date"]
        if price > 0 and date is None:
            errors.append(f"data.{field}: prix {price:g} sans date")
        if price == 0 and date is not None:
            errors.append(f"data.{field}: date {date} sans prix")
        # Cohérence temporelle : un prix ne peut pas avoir été observé après son extraction
        if extract_date is not None and date is not None and _parse_utc(date) > _parse_utc(extract_date):
            errors.append(f"data.{field}_date: observation {date} postérieure à l'extraction {extract_date}")
    for side in ("sell", "buy"):
        low, high = data[f"{side}_price_min"], data[f"{side}_price_max"]
        if low > 0 and high > 0 and low > high:
            errors.append(f"data.{side}_price_min ({low:g}) > data.{side}_price_max ({high:g})")
    return errors


def validate(message) -> List[str]:
    """Toutes les erreurs du message ; liste vide = message valide."""
    errors = schema_errors(message)
    if errors:  # structure fausse : inutile d'appliquer les règles métier
        return errors
    return business_errors(message["data"], message["extract_date"])


def _to_utc_iso(value: Optional[str]) -> Optional[str]:
    """'2026-11-04T06:45:00' -> '2026-11-04T06:45:00Z' (l'API Albion donne des heures UTC)."""
    if value is None:
        return None
    return datetime.fromisoformat(value[:19]).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize(message: dict) -> dict:
    """Message valide -> enregistrement à plat, aux formats uniformes.

    - prix à 0 (aucun ordre) -> null : un prix nul n'est pas un objet gratuit ;
    - prix décimaux -> entiers (la monnaie du jeu n'a pas de centimes) ;
    - dates -> ISO 8601 UTC avec suffixe Z.
    """
    data = message["data"]
    record = {
        "item_id": data["item_id"].strip(),
        "city": data["city"].strip(),
        "quality": int(data["quality"]),
    }
    for field in PRICE_FIELDS:
        price = data[field]
        record[field] = int(price) if price > 0 else None
        record[f"{field}_date"] = _to_utc_iso(data[f"{field}_date"]) if price > 0 else None
    record["dataset_sha"] = message.get("dataset_sha")
    record["extract_date"] = message["extract_date"]
    return record


def business_key(record: dict) -> str:
    return "|".join(str(record[f]) for f in KEY_FIELDS)


def fingerprint(record: dict) -> str:
    """Empreinte du contenu métier : deux observations identiques ont la même empreinte.

    Les métadonnées d'extraction (sha, date d'extraction) sont exclues : republier le même
    fichier donne donc les mêmes empreintes, et les doublons sont détectés.
    """
    content = {k: v for k, v in record.items() if k not in ("dataset_sha", "extract_date")}
    canonical = json.dumps(content, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def check(message) -> Tuple[Optional[dict], List[str]]:
    """Point d'entrée : (enregistrement normalisé, []) ou (None, erreurs)."""
    errors = validate(message)
    if errors:
        return None, errors
    return normalize(message), []
