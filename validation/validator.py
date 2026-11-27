"""Consumer de validation : topic brut -> validés / DLQ.

    albion.market.prices  --(désérialiser, valider, normaliser, dédoublonner)-->
        albion.market.prices.validated   messages valides, normalisés, sans doublon
        albion.market.prices.dlq         messages rejetés + métadonnées d'erreur

Idempotent : chaque message traité est mémorisé dans data/state/validator.sqlite
(empreinte du contenu pour les valides, position topic/partition/offset pour les rejets).
Rejouer le topic (--replay) ne produit donc aucun doublon en sortie.

    python validation/validator.py               # traite les nouveaux messages
    python validation/validator.py --replay      # relit tout le topic brut depuis le début
"""
import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "kafka" / "scripts"))
import config  # noqa: E402  (bootstrap, noms des topics brut et DLQ)
import rules  # noqa: E402

TOPIC_VALIDATED = os.getenv("KAFKA_TOPIC_VALIDATED", "albion.market.prices.validated")
GROUP_ID = os.getenv("KAFKA_VALIDATOR_GROUP", "albion-market-validator")
STATE_DB = PROJECT_ROOT / "data" / "state" / "validator.sqlite"
ALERT_THRESHOLD = float(os.getenv("VALIDATION_ALERT_THRESHOLD", "0.05"))  # 5 % de rejets


# ---------- logs structurés (une ligne JSON par événement) ----------
class JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {"ts": datetime.now(timezone.utc).isoformat(), "level": record.levelname,
                 "logger": record.name, "event": record.getMessage()}
        entry.update(getattr(record, "fields", {}))
        return json.dumps(entry, ensure_ascii=False)


log = logging.getLogger("validator")
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(JsonFormatter())
log.addHandler(_handler)
log.setLevel(logging.INFO)


def _log(level, event, **fields):
    log.log(level, event, extra={"fields": fields})


# ---------- mémoire des messages déjà traités ----------
class SeenStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path))
        self.db.execute("CREATE TABLE IF NOT EXISTS seen (id TEXT PRIMARY KEY, kind TEXT, seen_at TEXT)")

    def has(self, ident: str) -> bool:
        return self.db.execute("SELECT 1 FROM seen WHERE id = ?", (ident,)).fetchone() is not None

    def add_all(self, items):
        now = datetime.now(timezone.utc).isoformat()
        self.db.executemany("INSERT OR IGNORE INTO seen VALUES (?, ?, ?)", [(i, k, now) for i, k in items])
        self.db.commit()


def ensure_validated_topic():
    admin = AdminClient({"bootstrap.servers": config.BOOTSTRAP_SERVERS})
    topic = NewTopic(TOPIC_VALIDATED, num_partitions=3, replication_factor=config.REPLICATION_FACTOR,
                     config={"retention.ms": str(7 * 24 * 3600 * 1000), "cleanup.policy": "delete"})
    try:
        admin.create_topics([topic])[TOPIC_VALIDATED].result()
        _log(logging.INFO, "topic créé", topic=TOPIC_VALIDATED)
    except Exception as exc:  # noqa: BLE001
        err = exc.args[0] if exc.args else None
        if not (isinstance(err, KafkaError) and err.code() == KafkaError.TOPIC_ALREADY_EXISTS):
            raise


def _headers(msg) -> dict:
    return {k: (v.decode("utf-8", "replace") if v else None) for k, v in (msg.headers() or [])}


def run(replay: bool = False, idle_timeout: float = 10.0) -> dict:
    ensure_validated_topic()
    store = SeenStore(STATE_DB)
    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": config.BOOTSTRAP_SERVERS, "enable.idempotence": True,
                         "acks": "all", "linger.ms": 20, "compression.type": "lz4"})

    def on_assign(c, partitions):
        if replay:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
        c.assign(partitions)

    consumer.subscribe([config.TOPIC_PRICES], on_assign=on_assign)

    stats = Counter()
    reasons = Counter()
    latest_observation = None  # date d'observation la plus récente (fraîcheur des données)
    delivery_errors = []
    started = time.monotonic()
    last_message_at = time.monotonic()

    def on_delivery(err, _msg):
        if err is not None:
            delivery_errors.append(str(err))

    try:
        while True:
            batch = consumer.consume(num_messages=500, timeout=1.0)
            if not batch:
                if time.monotonic() - last_message_at > idle_timeout:
                    break
                continue
            last_message_at = time.monotonic()
            pending = []  # identifiants à mémoriser une fois le lot livré
            batch_seen = set()

            for msg in batch:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        _log(logging.ERROR, "erreur kafka", error=str(msg.error()))
                    continue
                stats["read"] += 1
                headers = _headers(msg)
                position = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"

                # 1. Désérialiser, puis 2. valider
                message, parsed = None, False
                try:
                    message, parsed = json.loads(msg.value()), True
                    stage, errors = "validation", rules.validate(message)
                except (ValueError, TypeError) as exc:
                    stage = "validation" if parsed else "deserialization"
                    errors = [f"{'erreur de validation' if parsed else 'JSON invalide'} : {exc}"]

                # 2. Rejeté -> DLQ (une seule fois par position, même en cas de rejeu)
                if errors:
                    if store.has("pos:" + position) or ("pos:" + position) in batch_seen:
                        stats["already_processed"] += 1
                        continue
                    stats["invalid"] += 1
                    field = errors[0].split(":")[0].strip()
                    reasons[field] += 1
                    correlation_id = headers.get("message_id")
                    # Format du cours : le message d'origine + les métadonnées du rejet, dans le corps
                    dlq_record = {
                        "original_message": message if parsed else msg.value().decode("utf-8", "replace"),
                        "rejection_reason": errors[0],
                        "errors": errors[:10],
                        "rejection_stage": stage,
                        "rejection_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "source_topic": msg.topic(),
                        "source_partition": msg.partition(),
                        "source_offset": msg.offset(),
                        "source_key": msg.key().decode("utf-8", "replace") if msg.key() else None,
                        "validator_version": rules.CONTRACT_VERSION,
                        "correlation_id": correlation_id,
                        "run_id": headers.get("run_id"),
                    }
                    dlq_headers = list(msg.headers() or []) + [
                        ("error_stage", stage.encode()),
                        ("validator_version", rules.CONTRACT_VERSION.encode()),
                    ]
                    producer.produce(config.TOPIC_DLQ, key=msg.key(),
                                     value=json.dumps(dlq_record, ensure_ascii=False, default=str).encode(),
                                     headers=dlq_headers, on_delivery=on_delivery)
                    _log(logging.WARNING, "validation échouée", error=errors[0], field=field,
                         correlation_id=correlation_id, stage=stage, position=position)
                    pending.append(("pos:" + position, "dlq"))
                    batch_seen.add("pos:" + position)
                    continue

                # 3. Normaliser puis dédoublonner sur l'empreinte du contenu
                record = rules.normalize(message)
                fp = rules.fingerprint(record)
                if store.has("fp:" + fp) or ("fp:" + fp) in batch_seen:
                    stats["duplicates"] += 1
                    continue

                # 4. Valide -> topic validated
                record["fingerprint"] = fp
                record["validated_at"] = datetime.now(timezone.utc).isoformat()
                out_headers = [(k, v.encode()) for k, v in headers.items() if v is not None and k in
                               ("message_id", "run_id", "dataset_sha", "source")]
                out_headers.append(("source_position", position.encode()))
                producer.produce(TOPIC_VALIDATED, key=rules.business_key(record).encode(),
                                 value=json.dumps(record, ensure_ascii=False).encode(),
                                 headers=out_headers, on_delivery=on_delivery)
                stats["valid"] += 1
                dates = [record[f"{f}_date"] for f in rules.PRICE_FIELDS if record[f"{f}_date"]]
                if dates:
                    latest_observation = max([latest_observation] + dates) if latest_observation else max(dates)
                pending.append(("fp:" + fp, "valid"))
                batch_seen.add("fp:" + fp)

            # Ordre : 1) livrer les sorties, 2) mémoriser, 3) commiter l'offset
            producer.flush(60)
            if delivery_errors:
                raise RuntimeError(f"{len(delivery_errors)} envoi(s) en échec : {delivery_errors[0]}")
            store.add_all(pending)
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    checked = stats["valid"] + stats["invalid"] + stats["duplicates"]
    invalid_rate = stats["invalid"] / checked if checked else 0.0
    summary = {
        "read": stats["read"], "valid": stats["valid"], "invalid": stats["invalid"],
        "duplicates": stats["duplicates"], "already_processed": stats["already_processed"],
        "invalid_rate": round(invalid_rate, 4),
        "validation_rate": round(1 - invalid_rate, 4) if checked else None,
        "top_errors": dict(reasons.most_common(5)),
        "latest_observation": latest_observation,
        "contract_version": rules.CONTRACT_VERSION,
        "duration_s": round(time.monotonic() - started, 1),
        "replay": replay,
    }
    _log(logging.INFO, "validation terminée", **summary)
    if invalid_rate > ALERT_THRESHOLD:
        _log(logging.WARNING, "ALERTE taux de rejet élevé", invalid_rate=summary["invalid_rate"],
             threshold=ALERT_THRESHOLD, invalid=stats["invalid"], checked=checked)
    summary["alert"] = invalid_rate > ALERT_THRESHOLD
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--replay", action="store_true", help="relire le topic brut depuis le début")
    parser.add_argument("--idle-timeout", type=float, default=10.0)
    parser.add_argument("--fail-on-alert", action="store_true",
                        help="code de sortie 2 si le taux de rejet dépasse le seuil (utile pour Airflow)")
    args = parser.parse_args()
    summary = run(replay=args.replay, idle_timeout=args.idle_timeout)
    return 2 if (args.fail_on_alert and summary["alert"]) else 0


if __name__ == "__main__":
    sys.exit(main())
