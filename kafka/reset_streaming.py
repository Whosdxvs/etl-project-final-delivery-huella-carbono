"""Deja la capa de streaming en cero para repetir la demostración.

Borra los topics `huella-events` y `huella-events-dlq`, el grupo de consumidores
y las tablas `stream_*` del DW. NO toca el esquema estrella ni los datos batch.
Detener antes el productor y el consumidor.

Uso:
    python kafka/reset_streaming.py
"""
from __future__ import annotations

import time

from kafka.admin import KafkaAdminClient

from common import KAFKA_BROKER, mysql_connection
from consumer_events import GROUP_ID
from event_contract import DLQ_TOPIC, TOPIC


def main() -> None:
    admin = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER], client_id="huella-reset")
    try:
        try:
            admin.delete_groups([GROUP_ID])
            print(f"Grupo de consumidores borrado: {GROUP_ID}")
        except Exception as exc:  # el grupo no existe todavía
            print(f"Grupo {GROUP_ID} no borrado ({type(exc).__name__}); se ignora")
        topics = [t for t in (TOPIC, DLQ_TOPIC) if t in set(admin.list_topics())]
        if topics:
            admin.delete_topics(topics)
            print(f"Topics borrados: {', '.join(topics)}")
    finally:
        admin.close()

    conn = mysql_connection(autocommit=True)
    try:
        with conn.cursor() as cur:
            for table in ("stream_events", "stream_invalid_events", "stream_metrics_snapshot"):
                cur.execute(f"DROP TABLE IF EXISTS `{table}`")
                print(f"Tabla borrada: {table}")
    finally:
        conn.close()
    time.sleep(2)  # Kafka borra los topics de forma asíncrona
    print("Listo. El consumidor recrea las tablas y el productor recrea los topics al arrancar.")


if __name__ == "__main__":
    main()
