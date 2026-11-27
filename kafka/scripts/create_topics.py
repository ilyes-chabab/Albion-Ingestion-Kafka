"""Crée les topics du pipeline. Idempotent : un topic déjà existant n'est pas recréé."""
import sys

from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

import config


def create_topics(bootstrap_servers: str = config.BOOTSTRAP_SERVERS) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    new_topics = [
        NewTopic(
            name,
            num_partitions=spec["partitions"],
            replication_factor=config.REPLICATION_FACTOR,
            config=spec["config"],
        )
        for name, spec in config.TOPICS.items()
    ]

    futures = admin.create_topics(new_topics, request_timeout=15)
    for name, future in futures.items():
        try:
            future.result()
            print(f"[create_topics] topic créé : {name}")
        except Exception as exc:  # noqa: BLE001
            err = exc.args[0] if exc.args else None
            if isinstance(err, KafkaError) and err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"[create_topics] topic déjà existant : {name}")
            else:
                raise


if __name__ == "__main__":
    try:
        create_topics()
    except Exception as exc:  # noqa: BLE001
        print(f"[create_topics] échec : {exc}", file=sys.stderr)
        sys.exit(1)
