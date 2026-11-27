"""Consumer Kafka : lit le topic des prix et écrit les messages dans data/output/.

Fonctionne en mode lot : il s'arrête après `--idle-timeout` secondes sans nouveau message,
ce qui permet de l'appeler depuis une tâche Airflow.

Garantie « au moins une fois » : les offsets sont commités seulement APRÈS l'écriture
sur disque. En cas de crash entre l'écriture et le commit, les messages sont relus
au redémarrage (doublons possibles, traités à l'étape de déduplication).

    python kafka/scripts/consumer.py --output data/output/market_prices.jsonl
"""
import argparse
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer

import config


def _headers_dict(msg) -> dict:
    return {k: (v.decode("utf-8") if v else None) for k, v in (msg.headers() or [])}


def send_to_dlq(dlq_producer: Producer, msg, error: str) -> None:
    """Envoie le message brut dans la DLQ avec ses métadonnées d'erreur."""
    headers = list(msg.headers() or []) + [
        ("error", error.encode("utf-8")),
        ("error_stage", b"consumer.deserialization"),
        ("original_topic", msg.topic().encode("utf-8")),
        ("original_partition", str(msg.partition()).encode("utf-8")),
        ("original_offset", str(msg.offset()).encode("utf-8")),
        ("failed_at", datetime.now(timezone.utc).isoformat().encode("utf-8")),
    ]
    dlq_producer.produce(config.TOPIC_DLQ, key=msg.key(), value=msg.value(), headers=headers)


def consume(
    output_path: str,
    group_id: str = config.CONSUMER_GROUP,
    idle_timeout: float = 10.0,
    max_messages: int = 0,
    bootstrap_servers: str = config.BOOTSTRAP_SERVERS,
) -> dict:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",  # premier lancement : on lit depuis le début
            "enable.auto.commit": False,  # commit manuel après écriture
        }
    )
    dlq_producer = Producer({"bootstrap.servers": bootstrap_servers, "enable.idempotence": True})
    consumer.subscribe([config.TOPIC_PRICES])

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    stats = {"consumed": 0, "written": 0, "dlq": 0}
    per_partition: Counter = Counter()
    last_message_at = time.monotonic()

    try:
        with open(output_path, "a", encoding="utf-8") as out:
            while True:
                if max_messages and stats["consumed"] >= max_messages:
                    break
                batch = consumer.consume(num_messages=500, timeout=1.0)
                if not batch:
                    if time.monotonic() - last_message_at > idle_timeout:
                        break
                    continue
                last_message_at = time.monotonic()

                for msg in batch:
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            print(f"[consumer] erreur Kafka : {msg.error()}", file=sys.stderr)
                        continue
                    stats["consumed"] += 1
                    per_partition[msg.partition()] += 1
                    try:
                        record = json.loads(msg.value())
                        if not isinstance(record, dict):
                            raise ValueError("le message n'est pas un objet JSON")
                    except ValueError as exc:  # JSONDecodeError hérite de ValueError
                        send_to_dlq(dlq_producer, msg, f"JSON invalide : {exc}")
                        stats["dlq"] += 1
                        continue

                    headers = _headers_dict(msg)
                    record["_kafka"] = {
                        "message_id": headers.get("message_id"),
                        "run_id": headers.get("run_id"),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "key": msg.key().decode("utf-8") if msg.key() else None,
                    }
                    out.write(json.dumps(record, ensure_ascii=False) + "\n")
                    stats["written"] += 1

                # Ordre important : 1) écrire sur disque et la DLQ, 2) commiter les offsets
                out.flush()
                os.fsync(out.fileno())
                dlq_producer.flush(30)
                consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    stats["per_partition"] = dict(sorted(per_partition.items()))
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", default="data/output/market_prices.jsonl")
    parser.add_argument("--group-id", default=config.CONSUMER_GROUP)
    parser.add_argument("--idle-timeout", type=float, default=10.0)
    parser.add_argument("--max-messages", type=int, default=0, help="0 = pas de limite")
    args = parser.parse_args()

    stats = consume(args.output, args.group_id, args.idle_timeout, args.max_messages)
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
