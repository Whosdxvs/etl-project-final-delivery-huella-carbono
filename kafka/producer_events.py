"""Productor de Kafka: re-emite hogares del Data Warehouse como eventos (SIMULACIÓN).

Qué simula: la llegada, uno a uno, de reportes de consumo de hogares (como los
que recibiría una empresa de energía o un programa de monitoreo de emisiones).
Cada evento es una fila de `fact_huella_carbono` con sus dimensiones; se marca
`simulated: true` porque los datos son históricos (ECV 2025), no lecturas nuevas.

Por qué no se publican KPI repetidos: cada mensaje trae un hogar distinto, así
que cada evento cambia las métricas del consumidor (media acumulada, alertas,
conteos por piso térmico). El orden es aleatorio pero reproducible (semilla),
para que los primeros eventos ya mezclen departamentos y estratos.

Uso:
    python kafka/producer_events.py                 # 2 eventos/s, todos los hogares
    python kafka/producer_events.py --rate 10 --limit 500
"""
from __future__ import annotations

import argparse
import json
import time

from kafka import KafkaProducer

from common import KAFKA_BROKER, ensure_topics, mysql_connection
from event_contract import DLQ_TOPIC, TOPIC, build_event, validate_event

QUERY = """
    SELECT f.sk_fact,
           u.codigo_departamento, u.nombre_departamento, u.estrato,
           c.piso_termico,
           h.num_personas,
           f.kwh_consumidos_estimados AS kwh_mes,
           f.emisiones_energia_kg, f.emisiones_coccion_kg, f.huella_total_anual_kg
    FROM fact_huella_carbono f
    JOIN dim_ubicacion u ON u.sk_ubicacion = f.fk_ubicacion
    JOIN dim_hogar h     ON h.sk_hogar = f.fk_hogar
    LEFT JOIN dim_clima c ON c.sk_clima = f.fk_clima
    ORDER BY RAND(%s)
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rate", type=float, default=2.0, help="eventos por segundo (por defecto 2)")
    parser.add_argument("--limit", type=int, default=0, help="máximo de eventos; 0 = todos los hogares")
    parser.add_argument("--seed", type=int, default=2025, help="semilla del orden aleatorio")
    parser.add_argument("--skip", type=int, default=0, help="saltar los primeros N hogares (continuar una corrida)")
    args = parser.parse_args()

    ensure_topics(TOPIC, DLQ_TOPIC)
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        acks="all",          # el broker confirma cada mensaje antes de seguir
        retries=5,
        linger_ms=0,
    )

    conn = mysql_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(QUERY, (args.seed,))
            rows = cur.fetchall()
    finally:
        conn.close()
    rows = rows[args.skip:]
    if args.limit:
        rows = rows[: args.limit]

    print(f"Productor listo: {len(rows)} hogares para re-emitir a '{TOPIC}' "
          f"a {args.rate:g} eventos/s (broker {KAFKA_BROKER}). Ctrl+C para detener.")
    pause = 1.0 / args.rate if args.rate > 0 else 0.0
    sent = 0
    try:
        for row in rows:
            event = build_event(row)
            key = str(event["codigo_departamento"])
            validate_event(event, key)  # el productor nunca publica un evento que rompa el contrato
            meta = producer.send(
                TOPIC, key=key.encode("utf-8"), value=json.dumps(event, ensure_ascii=False).encode("utf-8")).get(timeout=30)
            sent += 1
            print(f"[{sent:>6}] p{meta.partition} o{meta.offset:<7} key={key:<3} "
                  f"{event['nombre_departamento'][:18]:<18} estrato {event['estrato']} "
                  f"{event['piso_termico']:<9} huella={event['huella_total_anual_kg']:>8.1f} kg")
            time.sleep(pause)
    except KeyboardInterrupt:
        print("\nProductor detenido por el usuario.")
    finally:
        producer.flush()
        producer.close()
        print(f"Eventos publicados: {sent}. Para continuar donde quedó: --skip {args.skip + sent}")


if __name__ == "__main__":
    main()
