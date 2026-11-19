"""Contrato del evento de streaming `household_footprint` (versión 1).

Lo usan el productor (para armar eventos) y el consumidor (para validarlos).
Cada evento es un JSON con un hogar del Data Warehouse y su huella de carbono.

Llave del mensaje de Kafka: el código DANE del departamento (texto). Así todos
los eventos de un departamento caen en la misma partición y llegan en orden.
"""
from __future__ import annotations

import math
import uuid
from datetime import datetime, timezone

TOPIC = "huella-events"
DLQ_TOPIC = "huella-events-dlq"
EVENT_TYPE = "household_footprint"
SCHEMA_VERSION = 1
PISOS_TERMICOS = {"Cálido", "Templado", "Frío", "Sin clima"}

# campo -> (tipos aceptados, descripción). Todos son obligatorios.
NUMBER = (int, float)
FIELDS: dict[str, tuple[tuple[type, ...], str]] = {
    "event_id": ((str,), "UUID v4 único por evento"),
    "event_type": ((str,), f"siempre '{EVENT_TYPE}'"),
    "schema_version": ((int,), f"siempre {SCHEMA_VERSION}"),
    "event_ts": ((str,), "ISO 8601 en UTC, p. ej. 2026-11-18T21:30:00.123Z"),
    "simulated": ((bool,), "true: el evento re-emite una fila histórica del DW"),
    "sk_fact": ((int,), "llave de fact_huella_carbono que se re-emite"),
    "codigo_departamento": ((int,), "código DANE (5 a 99); es la llave del mensaje"),
    "nombre_departamento": ((str,), "nombre del departamento"),
    "estrato": ((int,), "0 (sin estrato) a 6"),
    "piso_termico": ((str,), "Cálido, Templado, Frío o Sin clima"),
    "num_personas": ((int,), ">= 1"),
    "kwh_mes": (NUMBER, "kWh/mes estimados, 0 a 5000"),
    "emisiones_energia_kg": (NUMBER, "kg CO2/año por electricidad, >= 0"),
    "emisiones_coccion_kg": (NUMBER, "kg CO2/año por cocción, >= 0"),
    "huella_total_anual_kg": (NUMBER, "kg CO2/año, >= energía + cocción"),
}


class ContractError(ValueError):
    """El evento no cumple el contrato."""


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_ts(value: str) -> datetime:
    if not value.endswith("Z"):
        raise ContractError("event_ts debe estar en UTC y terminar en 'Z'")
    try:
        return datetime.fromisoformat(value[:-1]).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ContractError(f"event_ts no es una fecha ISO 8601: {value!r}") from exc


def build_event(row: dict) -> dict:
    """Arma un evento a partir de una fila de la consulta del productor."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": EVENT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "event_ts": now_iso(),
        "simulated": True,
        "sk_fact": int(row["sk_fact"]),
        "codigo_departamento": int(row["codigo_departamento"]),
        "nombre_departamento": row["nombre_departamento"],
        "estrato": int(row["estrato"]),
        "piso_termico": row["piso_termico"] or "Sin clima",
        "num_personas": int(row["num_personas"]),
        "kwh_mes": float(row["kwh_mes"]),
        "emisiones_energia_kg": float(row["emisiones_energia_kg"]),
        "emisiones_coccion_kg": float(row["emisiones_coccion_kg"]),
        "huella_total_anual_kg": float(row["huella_total_anual_kg"]),
    }


def validate_event(event: object, key: str | None) -> dict:
    """Devuelve el evento si es válido; si no, lanza ContractError con el motivo."""
    if not isinstance(event, dict):
        raise ContractError("el mensaje no es un objeto JSON")

    missing = [f for f in FIELDS if f not in event]
    if missing:
        raise ContractError(f"faltan campos obligatorios: {', '.join(missing)}")
    extra = [f for f in event if f not in FIELDS]
    if extra:
        raise ContractError(f"campos no definidos en el contrato: {', '.join(extra)}")

    for name, (types, _) in FIELDS.items():
        value = event[name]
        # bool es subclase de int en Python: no se acepta donde se pide un número
        if isinstance(value, bool) and bool not in types:
            raise ContractError(f"{name} debe ser {types[0].__name__}, llegó bool")
        if not isinstance(value, types):
            expected = "número" if types == NUMBER else types[0].__name__
            raise ContractError(f"{name} debe ser {expected}, llegó {type(value).__name__}")
        if isinstance(value, float) and not math.isfinite(value):
            raise ContractError(f"{name} no es un número finito")

    try:
        uuid.UUID(event["event_id"])
    except ValueError as exc:
        raise ContractError("event_id no es un UUID") from exc
    if event["event_type"] != EVENT_TYPE:
        raise ContractError(f"event_type debe ser '{EVENT_TYPE}'")
    if event["schema_version"] != SCHEMA_VERSION:
        raise ContractError(f"schema_version {event['schema_version']} no soportada")
    parse_ts(event["event_ts"])

    if not 5 <= event["codigo_departamento"] <= 99:
        raise ContractError("codigo_departamento fuera del rango DANE (5 a 99)")
    if key != str(event["codigo_departamento"]):
        raise ContractError(f"la llave del mensaje ({key!r}) no coincide con codigo_departamento")
    if not 0 <= event["estrato"] <= 6:
        raise ContractError("estrato fuera de 0 a 6")
    if event["piso_termico"] not in PISOS_TERMICOS:
        raise ContractError(f"piso_termico inválido: {event['piso_termico']!r}")
    if event["num_personas"] < 1:
        raise ContractError("num_personas debe ser >= 1")
    if not 0 <= event["kwh_mes"] <= 5000:
        raise ContractError("kwh_mes fuera de 0 a 5000")
    for f in ("emisiones_energia_kg", "emisiones_coccion_kg", "huella_total_anual_kg"):
        if event[f] < 0:
            raise ContractError(f"{f} es negativo")
    # Misma idea que la regla DQ23 del batch: la huella no puede ser menor que sus partes
    if event["huella_total_anual_kg"] + 0.01 < event["emisiones_energia_kg"] + event["emisiones_coccion_kg"]:
        raise ContractError("huella_total_anual_kg es menor que energía + cocción")
    return event
