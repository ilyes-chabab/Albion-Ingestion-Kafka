"""Tests du contrat de données (sans Kafka) :  python validation/test_rules.py"""
import copy

import rules

VALID = {
    "source": "HF_albion_online_market_prices",
    "dataset_sha": "7bec455e16acef0b4646d33a0ef646a53759ebc5",
    "extract_date": "2026-11-27T10:19:38.287640+00:00",
    "data": {
        "item_id": "T3_MAIN_ARCANESTAFF", "city": "Black Market", "quality": 2,
        "sell_price_min": 0.0, "sell_price_min_date": None,
        "sell_price_max": 0.0, "sell_price_max_date": None,
        "buy_price_min": 708.0, "buy_price_min_date": "2026-11-04T06:45:00",
        "buy_price_max": 3658.0, "buy_price_max_date": "2026-11-04T06:45:00",
    },
}


def variant(**changes):
    msg = copy.deepcopy(VALID)
    for field, value in changes.items():
        if value is ...:
            del msg["data"][field]
        else:
            msg["data"][field] = value
    return msg


def test_valid_message():
    record, errors = rules.check(VALID)
    assert errors == [], errors
    assert record["sell_price_min"] is None, "prix à 0 -> null"
    assert record["buy_price_min"] == 708 and isinstance(record["buy_price_min"], int)
    assert record["buy_price_min_date"] == "2026-11-04T06:45:00Z"


def test_invalid_messages():
    cases = {
        "qualité hors plage": variant(quality=7),
        "ville inconnue": variant(city="Paris"),
        "champ manquant": variant(city=...),
        "prix négatif": variant(buy_price_min=-5.0),
        "prix décimal": variant(buy_price_min=708.5),
        "prix sans date": variant(sell_price_min=120.0),
        "date sans prix": variant(sell_price_max_date="2026-11-04T06:45:00"),
        "min > max": variant(buy_price_min=5000.0),
        "item_id invalide": variant(item_id="t4 bag"),
        "pas un objet": [1, 2, 3],
        "pas de data": {"source": "x", "extract_date": "2026-11-27T10:00:00"},
        "champ inattendu": variant(currency="EUR"),
        "date impossible": variant(buy_price_min_date="2026-13-04T06:45:00", buy_price_max_date="2026-13-04T06:45:00"),
        "observation après l'extraction": variant(buy_price_min_date="2026-12-01T00:00:00"),
        "date mal formée": variant(buy_price_min_date="04/11/2026"),
    }
    for name, msg in cases.items():
        record, errors = rules.check(msg)
        assert record is None and errors, f"{name} aurait dû être rejeté"
        print(f"  rejeté ({name}) : {errors[0]}")


def test_contract_version():
    assert rules.CONTRACT_VERSION == "1.0.0"
    extra = copy.deepcopy(VALID)
    extra["unknown"] = 1
    assert rules.validate(extra), "additionalProperties: false aussi sur l'enveloppe"


def test_fingerprint_ignores_extraction_metadata():
    a, _ = rules.check(VALID)
    other = copy.deepcopy(VALID)
    other["extract_date"], other["dataset_sha"] = "2026-12-01T00:00:00+00:00", "0" * 40
    b, _ = rules.check(other)
    assert rules.fingerprint(a) == rules.fingerprint(b), "même contenu -> même empreinte"
    c, _ = rules.check(variant(buy_price_max=3659.0))
    assert rules.fingerprint(a) != rules.fingerprint(c), "contenu différent -> empreinte différente"


if __name__ == "__main__":
    for test in (test_valid_message, test_invalid_messages, test_contract_version,
                 test_fingerprint_ignores_extraction_metadata):
        test()
        print(f"OK {test.__name__}")
