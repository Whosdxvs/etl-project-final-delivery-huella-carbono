"""Experimento: publica eventos que rompen el contrato para ver cómo reacciona el consumidor.

Envía un evento por cada defecto (JSON mal formado, campo faltante, tipo
incorrecto, valor fuera de rango, llave que no coincide y huella menor que sus
componentes). El consumidor debe rechazarlos todos: no los suma a las métricas,
los guarda en `stream_invalid_events`, los reenvía a `huella-events-dlq` y
sigue procesando los eventos válidos.

Uso:
    python kafka/send_invalid_events.py
"""
from __future__ import annotations

import json

from kafka import KafkaProducer

from common import KAFKA_BROKER, ensure_topics
from event_contract import DLQ_TOPIC, TOPIC, build_event


def base_event() -> dict:
    return build_event({
        "sk_fact": 1, "codigo_departamento": 76, "nombre_departamento": "Valle del Cauca",
        "estrato": 3, "piso_termico": "Cálido", "num_personas": 3, "kwh_mes": 150.0,
        "emisiones_energia_kg": 396.0, "emisiones_coccion_kg": 300.0, "huella_total_anual_kg": 696.0,
    })


def cases() -> list[tuple[str, str, bytes]]:
    """(descripción, llave, valor en bytes) de cada evento defectuoso."""
    out = []

    def add(desc: str, event: dict, key: str = "76") -> None:
        out.append((desc, key, json.dumps(event, ensure_ascii=False).encode("utf-8")))

    out.append(("JSON mal formado", "76", b'{"event_id": "roto", "huella_total_anual_kg": '))

    e = base_event(); del e["huella_total_anual_kg"]
    add("falta huella_total_anual_kg", e)

    e = base_event(); e["kwh_mes"] = "ciento cincuenta"
    add("kwh_mes es texto", e)

    e = base_event(); e["estrato"] = 9
    add("estrato 9 fuera de rango", e)

    e = base_event(); e["huella_total_anual_kg"] = -50.0
    add("huella negativa", e)

    e = base_event()
    add("llave 05 no coincide con departamento 76", e, key="5")

    e = base_event(); e["huella_total_anual_kg"] = 100.0
    add("huella menor que energía + cocción", e)

    e = base_event(); e["event_ts"] = "18/11/2026 10:00"
    add("event_ts no es ISO 8601 UTC", e)
    return out


def main() -> None:
    ensure_topics(TOPIC, DLQ_TOPIC)
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER])
    try:
        for desc, key, value in cases():
            meta = producer.send(TOPIC, key=key.encode("utf-8"), value=value).get(timeout=30)
            print(f"Enviado evento inválido -> p{meta.partition} o{meta.offset}: {desc}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
