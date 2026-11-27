"""Configuration Kafka partagée par les scripts (surchargeable par variables d'environnement)."""
import os

# localhost:9092 depuis la machine hôte, kafka:29092 depuis un conteneur (ex : Airflow)
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_PRICES = os.getenv("KAFKA_TOPIC_PRICES", "albion.market.prices")
TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "albion.market.prices.dlq")

CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "albion-market-consumer")

# Définition des topics : (nom, partitions, configuration)
TOPICS = {
    TOPIC_PRICES: {
        "partitions": 3,
        "config": {
            "retention.ms": str(7 * 24 * 3600 * 1000),  # 7 jours : permet de rejouer un lot
            "cleanup.policy": "delete",
        },
    },
    TOPIC_DLQ: {
        "partitions": 1,
        "config": {
            "retention.ms": str(30 * 24 * 3600 * 1000),  # 30 jours : temps d'analyser les rejets
            "cleanup.policy": "delete",
        },
    },
}

# Un seul broker dans le docker-compose : facteur de réplication 1
REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
