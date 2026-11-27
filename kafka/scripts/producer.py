"""Producer Kafka : publie des enregistrements de prix (dictionnaires) dans le topic des prix.

Utilisable comme module (le connecteur appelle `publish(records)`) ou en ligne de commande :
    python kafka/scripts/producer.py --input data/input/sample_market_prices.jsonl
"""
import argparse
import json
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Iterable, Optional

from confluent_kafka import Producer

import config

KEY_FIELDS = ("item_id", "city", "quality")


def build_key(record: dict) -> str:
    """Clé métier : un même objet, dans une même ville, à une même qualité."""
    missing = [f for f in KEY_FIELDS if record.get(f) in (None, "")]
    if missing:
        raise ValueError(f"champs de clé manquants : {missing}")
    return "|".join(str(record[f]) for f in KEY_FIELDS)


def make_producer(bootstrap_servers: str = config.BOOTSTRAP_SERVERS) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            # Idempotence : un message renvoyé après un échec réseau n'est pas écrit deux fois
            # (implique acks=all et un ordre conservé par partition)
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "retry.backoff.ms": 500,
            "delivery.timeout.ms": 120000,
            "linger.ms": 20,
            "compression.type": "lz4",
        }
    )


def publish(
    records: Iterable[dict],
    run_id: Optional[str] = None,
    topic: str = config.TOPIC_PRICES,
    producer: Optional[Producer] = None,
) -> dict:
    """Publie les enregistrements et renvoie des statistiques de livraison."""
    producer = producer or make_producer()
    run_id = run_id or str(uuid.uuid4())
    stats = {"run_id": run_id, "sent": 0, "delivered": 0, "failed": 0, "skipped": 0}
    per_partition: Counter = Counter()
    errors = []

    def on_delivery(err, msg):
        if err is not None:
            stats["failed"] += 1
            errors.append(str(err))
        else:
            stats["delivered"] += 1
            per_partition[msg.partition()] += 1

    for record in records:
        try:
            key = build_key(record)
        except ValueError as exc:
            # Sans clé, on ne peut pas partitionner correctement : l'enregistrement est écarté
            stats["skipped"] += 1
            errors.append(f"enregistrement ignoré ({exc}) : {record}")
            continue

        headers = [
            ("message_id", str(uuid.uuid4())),  # identifiant de corrélation du message
            ("run_id", run_id),  # identifiant du lot (ex : run Airflow)
            ("produced_at", datetime.now(timezone.utc).isoformat()),
            ("source", "huggingface:Sayli84/albion-online-market-prices"),
        ]
        while True:
            try:
                producer.produce(
                    topic,
                    key=key.encode("utf-8"),
                    value=json.dumps(record, ensure_ascii=False, default=str).encode("utf-8"),
                    headers=headers,
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                # File d'attente locale pleine : on laisse partir des messages puis on réessaie
                producer.poll(1)
        stats["sent"] += 1
        producer.poll(0)

    remaining = producer.flush(60)
    if remaining:
        stats["failed"] += remaining
        errors.append(f"{remaining} message(s) non livrés après flush")

    stats["per_partition"] = dict(sorted(per_partition.items()))
    stats["errors"] = errors[:20]
    return stats


def read_jsonl(path: str) -> Iterable[dict]:
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="fichier JSON Lines à publier")
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    stats = publish(read_jsonl(args.input), run_id=args.run_id)
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0 if stats["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
