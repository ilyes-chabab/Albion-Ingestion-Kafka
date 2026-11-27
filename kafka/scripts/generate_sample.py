"""Génère un petit jeu de données fictif au format du dataset Albion (JSON Lines).

Sert à tester Kafka sans dépendre du connecteur Hugging Face (étape 2).
Résultat déterministe (graine fixe).

    python kafka/scripts/generate_sample.py --rows 300 --output data/input/sample_market_prices.jsonl
"""
import argparse
import json
import os
import random
from datetime import datetime, timedelta

ITEMS = ["T4_BAG", "T5_BAG", "T6_BAG", "T4_MAIN_SWORD", "T5_MAIN_SWORD", "T4_HEAD_PLATE_SET1",
         "T5_ARMOR_LEATHER_SET2", "T6_SHOES_CLOTH_SET3", "T4_PLANKS", "T5_METALBAR", "T6_CLOTH",
         "T4_POTION_HEAL", "T7_2H_BOW", "T8_CAPE"]
CITIES = ["Bridgewatch", "Caerleon", "Fort Sterling", "Lymhurst", "Martlock", "Thetford", "Brecilien"]
DEFAULT_DATE = "0001-01-01T00:00:00"  # valeur renvoyée par l'API quand aucun ordre n'existe


def _price_and_date(base: int, now: datetime, rng: random.Random):
    if rng.random() < 0.08:
        return 0, DEFAULT_DATE
    price = int(base * rng.uniform(0.8, 1.3))
    date = (now - timedelta(minutes=rng.randint(0, 72 * 60))).strftime("%Y-%m-%dT%H:%M:%S")
    return price, date


def generate(rows: int, seed: int = 42) -> list:
    rng = random.Random(seed)
    now = datetime(2026, 11, 6, 12, 0, 0)
    combos = [(i, c, q) for i in ITEMS for c in CITIES for q in range(1, 6)]
    rng.shuffle(combos)
    records = []
    for item_id, city, quality in combos[:rows]:
        tier = int(item_id[1])
        base = int(300 * 2.2 ** (tier - 4) * (1 + 0.25 * (quality - 1)))
        sell_min, sell_min_date = _price_and_date(base, now, rng)
        sell_max, sell_max_date = _price_and_date(int(base * 1.6), now, rng)
        buy_min, buy_min_date = _price_and_date(int(base * 0.5), now, rng)
        buy_max, buy_max_date = _price_and_date(int(base * 0.85), now, rng)
        records.append({
            "item_id": item_id, "city": city, "quality": quality,
            "sell_price_min": sell_min, "sell_price_min_date": sell_min_date,
            "sell_price_max": sell_max, "sell_price_max_date": sell_max_date,
            "buy_price_min": buy_min, "buy_price_min_date": buy_min_date,
            "buy_price_max": buy_max, "buy_price_max_date": buy_max_date,
        })
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=300)
    parser.add_argument("--output", default="data/input/sample_market_prices.jsonl")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as fh:
        for record in generate(args.rows):
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"{args.rows} lignes écrites dans {args.output}")


if __name__ == "__main__":
    main()
