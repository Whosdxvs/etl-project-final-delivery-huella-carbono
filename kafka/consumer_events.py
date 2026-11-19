"""Consumidor de Kafka: valida eventos, actualiza métricas de streaming y las guarda en MySQL.

Flujo por cada lote leído del topic `huella-events`:
  1. Valida cada mensaje con el contrato (kafka/event_contract.py).
  2. Válido   -> INSERT en `stream_events` y actualiza las métricas en memoria.
     Inválido -> INSERT en `stream_invalid_events` y copia al topic `huella-events-dlq`.
  3. Escribe una foto de las métricas en `stream_metrics_snapshot` (la lee Grafana).
  4. Confirma la transacción en MySQL y DESPUÉS el offset en Kafka.

Garantías: entrega "al menos una vez". Si el consumidor se cae entre el paso 4
de MySQL y el commit del offset, Kafka le vuelve a entregar esos mensajes; como
`event_id` es llave primaria, el INSERT IGNORE los descarta y no se cuentan dos
veces. Al reiniciar, las métricas se reconstruyen desde `stream_events`, así que
detener el consumidor no pierde estado: retoma desde el último offset confirmado.

Métricas de streaming (se actualizan con cada evento válido):
  - eventos válidos, inválidos y % de inválidos
  - eventos procesados en el último minuto (rendimiento)
  - huella media acumulada (kg CO2/año) y kWh/mes medio
  - kWh/mes medio en piso cálido contra templado/frío (relación R6 en vivo)
  - alertas de alta huella: hogares por encima del p95 histórico del DW
  - latencia media productor -> consumidor (ms)

Uso:
    python kafka/consumer_events.py
"""
from __future__ import annotations

import json
import os
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer

from common import KAFKA_BROKER, ensure_topics, mysql_connection
from event_contract import DLQ_TOPIC, TOPIC, ContractError, parse_ts, validate_event

GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "huella-stream-consumer")
HEARTBEAT_S = 5  # foto de métricas aunque no lleguen eventos (para que "último minuto" baje a 0)
SCHEMA_SQL = Path(__file__).resolve().parent.parent / "sql" / "streaming_schema.sql"


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Metrics:
    """Estado acumulado de las métricas. Se reconstruye desde MySQL al arrancar."""

    def __init__(self, threshold: float):
        self.threshold = threshold
        self.valid = 0
        self.invalid = 0
        self.alerts = 0
        self.sum_huella = 0.0
        self.sum_kwh = 0.0
        self.calido = [0, 0.0]          # [n, suma kWh]
        self.frio_templado = [0, 0.0]
        self.recent: deque[datetime] = deque()  # processed_at del último minuto

    def restore(self, cur) -> None:
        cur.execute("""
            SELECT COUNT(*) n, COALESCE(SUM(huella_total_anual_kg),0) sh, COALESCE(SUM(kwh_mes),0) sk,
                   COALESCE(SUM(alta_huella),0) al,
                   SUM(piso_termico='Cálido') nc, COALESCE(SUM(IF(piso_termico='Cálido', kwh_mes, 0)),0) kc,
                   SUM(piso_termico IN ('Templado','Frío')) nf,
                   COALESCE(SUM(IF(piso_termico IN ('Templado','Frío'), kwh_mes, 0)),0) kf
            FROM stream_events""")
        r = cur.fetchone()
        self.valid, self.alerts = int(r["n"]), int(r["al"])
        self.sum_huella, self.sum_kwh = float(r["sh"]), float(r["sk"])
        self.calido = [int(r["nc"] or 0), float(r["kc"])]
        self.frio_templado = [int(r["nf"] or 0), float(r["kf"])]
        cur.execute("SELECT COUNT(*) n FROM stream_invalid_events")
        self.invalid = int(cur.fetchone()["n"])
        cur.execute("SELECT processed_at FROM stream_events WHERE processed_at >= %s ORDER BY processed_at",
                    (utcnow() - timedelta(minutes=1),))
        self.recent.extend(r["processed_at"] for r in cur.fetchall())

    def add(self, e: dict, processed_at: datetime) -> bool:
        self.valid += 1
        self.sum_huella += e["huella_total_anual_kg"]
        self.sum_kwh += e["kwh_mes"]
        if e["piso_termico"] == "Cálido":
            self.calido[0] += 1; self.calido[1] += e["kwh_mes"]
        elif e["piso_termico"] in ("Templado", "Frío"):
            self.frio_templado[0] += 1; self.frio_templado[1] += e["kwh_mes"]
        self.recent.append(processed_at)
        alert = e["huella_total_anual_kg"] > self.threshold
        self.alerts += alert
        return alert

    def last_minute(self, now: datetime) -> int:
        while self.recent and self.recent[0] < now - timedelta(minutes=1):
            self.recent.popleft()
        return len(self.recent)

    @staticmethod
    def _avg(n: float, s: float) -> float | None:
        return round(s / n, 2) if n else None

    def snapshot(self, now: datetime, batch_latency: float | None) -> dict:
        total = self.valid + self.invalid
        return {
            "snapshot_ts": now,
            "eventos_validos": self.valid,
            "eventos_invalidos": self.invalid,
            "pct_invalidos": round(100 * self.invalid / total, 2) if total else 0,
            "eventos_ultimo_minuto": self.last_minute(now),
            "huella_media_kg": self._avg(self.valid, self.sum_huella),
            "kwh_medio_mes": self._avg(self.valid, self.sum_kwh),
            "kwh_medio_calido": self._avg(*self.calido),
            "kwh_medio_frio_templado": self._avg(*self.frio_templado),
            "alertas_alta_huella": self.alerts,
            "latencia_media_ms": batch_latency,
            "umbral_alerta_kg": self.threshold,
        }


def create_tables(cur) -> None:
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    sql = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))
    for stmt in (s.strip() for s in sql.split(";")):
        if stmt:
            cur.execute(stmt)


def alert_threshold(cur) -> float:
    """Umbral de alerta: percentil 95 histórico de la huella en el DW (o STREAM_ALERT_KG)."""
    if os.getenv("STREAM_ALERT_KG"):
        return float(os.environ["STREAM_ALERT_KG"])
    cur.execute("SELECT COUNT(*) n FROM fact_huella_carbono")
    n = int(cur.fetchone()["n"])
    if not n:
        return 2000.0
    cur.execute("SELECT huella_total_anual_kg h FROM fact_huella_carbono ORDER BY h LIMIT %s, 1",
                (int(n * 0.95),))
    return round(float(cur.fetchone()["h"]), 2)


INSERT_EVENT = """
    INSERT IGNORE INTO stream_events (event_id, event_ts, processed_at, latency_ms, kafka_partition,
        kafka_offset, event_key, sk_fact, codigo_departamento, nombre_departamento, estrato, piso_termico,
        num_personas, kwh_mes, emisiones_energia_kg, emisiones_coccion_kg, huella_total_anual_kg, alta_huella)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
INSERT_INVALID = """
    INSERT IGNORE INTO stream_invalid_events (received_at, kafka_partition, kafka_offset, event_key, raw_value, error)
    VALUES (%s,%s,%s,%s,%s,%s)"""


def write_snapshot(cur, snap: dict) -> None:
    cols = ", ".join(snap)
    marks = ", ".join(["%s"] * len(snap))
    cur.execute(f"REPLACE INTO stream_metrics_snapshot ({cols}) VALUES ({marks})", tuple(snap.values()))


def main() -> None:
    ensure_topics(TOPIC, DLQ_TOPIC)
    conn = mysql_connection()
    with conn.cursor() as cur:
        create_tables(cur)
        threshold = alert_threshold(cur)
        metrics = Metrics(threshold)
        metrics.restore(cur)
    conn.commit()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=GROUP_ID,
        enable_auto_commit=False,      # el offset se confirma después de guardar en MySQL
        auto_offset_reset="earliest",  # un grupo nuevo lee el topic desde el inicio
    )
    dlq = KafkaProducer(bootstrap_servers=[KAFKA_BROKER])

    print(f"Consumidor '{GROUP_ID}' suscrito a '{TOPIC}' (broker {KAFKA_BROKER}).")
    print(f"Estado restaurado: {metrics.valid} válidos, {metrics.invalid} inválidos, "
          f"{metrics.alerts} alertas. Umbral de alerta (p95 histórico): {threshold:,.1f} kg CO2/año.")
    print("Esperando eventos... (Ctrl+C para detener)\n")

    last_snapshot = 0.0
    try:
        while True:
            batch = consumer.poll(timeout_ms=1000, max_records=200)
            records = [r for recs in batch.values() for r in recs]
            if not records and time.monotonic() - last_snapshot < HEARTBEAT_S:
                continue

            latencies = []
            with conn.cursor() as cur:
                for rec in records:
                    now = utcnow()
                    key = rec.key.decode("utf-8", "replace") if rec.key is not None else None
                    raw = rec.value.decode("utf-8", "replace") if rec.value is not None else ""
                    try:
                        try:
                            event = json.loads(raw)
                        except json.JSONDecodeError as exc:
                            raise ContractError(f"JSON mal formado: {exc.msg}") from exc
                        event = validate_event(event, key)
                    except ContractError as err:
                        if cur.execute(INSERT_INVALID, (now, rec.partition, rec.offset, key, raw[:60000],
                                                        str(err)[:500])):
                            dlq.send(DLQ_TOPIC, key=rec.key, value=rec.value,
                                     headers=[("error", str(err).encode("utf-8"))])
                            metrics.invalid += 1
                        print(f"[INVÁLIDO] p{rec.partition} o{rec.offset}: {err}")
                        continue

                    ts = parse_ts(event["event_ts"]).replace(tzinfo=None)
                    latency = max(0, int((now - ts).total_seconds() * 1000))
                    alert = event["huella_total_anual_kg"] > threshold
                    inserted = cur.execute(INSERT_EVENT, (
                        event["event_id"], ts, now, latency, rec.partition, rec.offset, key, event["sk_fact"],
                        event["codigo_departamento"], event["nombre_departamento"], event["estrato"],
                        event["piso_termico"], event["num_personas"], event["kwh_mes"],
                        event["emisiones_energia_kg"], event["emisiones_coccion_kg"],
                        event["huella_total_anual_kg"], int(alert)))
                    if not inserted:  # duplicado (re-entrega de Kafka): ya está contado
                        print(f"[DUPLICADO] {event['event_id']} ignorado")
                        continue
                    metrics.add(event, now)
                    latencies.append(latency)
                    if alert:
                        print(f"[ALERTA] {event['nombre_departamento']} estrato {event['estrato']}: "
                              f"{event['huella_total_anual_kg']:,.1f} kg CO2/año > umbral {threshold:,.1f}")

                snap = metrics.snapshot(utcnow(), round(sum(latencies) / len(latencies), 1) if latencies else None)
                write_snapshot(cur, snap)
            conn.commit()          # 1) primero MySQL
            if records:
                dlq.flush()
                consumer.commit()  # 2) después el offset en Kafka
            last_snapshot = time.monotonic()

            if records:
                print(f"Lote de {len(records):>3} | válidos {snap['eventos_validos']:>6} | inválidos "
                      f"{snap['eventos_invalidos']:>3} ({snap['pct_invalidos']}%) | último min "
                      f"{snap['eventos_ultimo_minuto']:>4} | huella media {snap['huella_media_kg']} kg | "
                      f"kWh cálido {snap['kwh_medio_calido']} vs frío/templado {snap['kwh_medio_frio_templado']} | "
                      f"alertas {snap['alertas_alta_huella']} | latencia {snap['latencia_media_ms']} ms")
    except KeyboardInterrupt:
        print("\nConsumidor detenido por el usuario. Los offsets confirmados quedan en Kafka.")
    finally:
        consumer.close()   # sin autocommit: lo no confirmado se vuelve a entregar al reiniciar
        dlq.close()
        conn.close()


if __name__ == "__main__":
    main()
