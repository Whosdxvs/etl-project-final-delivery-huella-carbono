"""Configuración compartida por el productor, el consumidor y el inyector de eventos inválidos."""
from __future__ import annotations

import os
from pathlib import Path

import pymysql
from dotenv import load_dotenv

# Lee el .env de la raíz del proyecto, sin importar desde dónde se ejecute el script
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "3"))


def mysql_connection(autocommit: bool = False) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", os.getenv("MYSQL_HOST_PORT", "3307"))),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.getenv("MYSQL_DATABASE", "huella_carbono_dw_v2"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=autocommit,
    )


def ensure_topics(*topics: str) -> None:
    """Crea los topics si no existen (3 particiones, factor de replicación 1)."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER], client_id="huella-admin")
    try:
        existing = set(admin.list_topics())
        new = [NewTopic(t, num_partitions=TOPIC_PARTITIONS, replication_factor=1)
               for t in topics if t not in existing]
        if new:
            try:
                admin.create_topics(new)
                print(f"Topics creados: {', '.join(t.name for t in new)} ({TOPIC_PARTITIONS} particiones)")
            except TopicAlreadyExistsError:
                pass
    finally:
        admin.close()
