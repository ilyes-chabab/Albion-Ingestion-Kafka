"""Vérifie la publication, la consommation et la répartition des messages.

Pour chaque partition : nombre de messages présents, offset commité par le groupe
de consommateurs et retard (lag) restant à consommer.

    python kafka/scripts/check_topics.py
"""
import argparse
import sys

from confluent_kafka import Consumer, TopicPartition

import config


def describe(topic: str, group_id: str, bootstrap_servers: str = config.BOOTSTRAP_SERVERS) -> list:
    consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": group_id, "enable.auto.commit": False})
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        if metadata.topics[topic].error is not None:
            raise RuntimeError(f"topic {topic} : {metadata.topics[topic].error}")
        partitions = sorted(metadata.topics[topic].partitions)
        committed = consumer.committed([TopicPartition(topic, p) for p in partitions], timeout=10)
        rows = []
        for tp in committed:
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            done = tp.offset if tp.offset >= 0 else low
            rows.append({"partition": tp.partition, "messages": high - low, "end_offset": high,
                         "committed": tp.offset if tp.offset >= 0 else None, "lag": high - done})
        return rows
    finally:
        consumer.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--group-id", default=config.CONSUMER_GROUP)
    args = parser.parse_args()

    for topic in config.TOPICS:
        rows = describe(topic, args.group_id)
        total = sum(r["messages"] for r in rows)
        print(f"\n{topic}  ({len(rows)} partition(s), {total} message(s))")
        print(f"  {'partition':>9} {'messages':>9} {'part':>6} {'commité':>9} {'lag':>6}")
        for r in rows:
            share = f"{100 * r['messages'] / total:.0f}%" if total else "-"
            committed = "-" if r["committed"] is None else r["committed"]
            print(f"  {r['partition']:>9} {r['messages']:>9} {share:>6} {committed:>9} {r['lag']:>6}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
