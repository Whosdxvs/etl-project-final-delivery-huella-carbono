"""Genera grafana/dashboards/huella_streaming.json (dashboard en tiempo real).

El JSON se genera con este script para que las consultas SQL queden legibles y
versionadas aquí. Ejecutar después de cambiar un panel:
    python scripts/build_grafana_dashboard.py
"""
from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "grafana" / "dashboards" / "huella_streaming.json"
DS = {"type": "mysql", "uid": "dw-mysql"}
LAST = "SELECT {col} AS valor FROM stream_metrics_snapshot ORDER BY snapshot_ts DESC LIMIT 1"

_id = 0


def target(sql: str, fmt: str = "table", ref: str = "A") -> dict:
    return {"refId": ref, "datasource": DS, "editorMode": "code", "rawQuery": True, "format": fmt, "rawSql": sql}


def panel(kind: str, title: str, x: int, y: int, w: int, h: int, targets: list, desc: str = "", **extra) -> dict:
    global _id
    _id += 1
    p = {"id": _id, "type": kind, "title": title, "description": desc, "datasource": DS,
         "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets}
    p.update(extra)
    return p


def stat(title, col, x, y, unit="none", decimals=0, desc="", color="blue", thresholds=None, sql=None):
    steps = thresholds or [{"color": color, "value": None}]
    return panel("stat", title, x, y, 4, 4, [target(sql or LAST.format(col=col))], desc,
                 options={"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                          "colorMode": "background", "graphMode": "none", "textMode": "value",
                          "justifyMode": "center"},
                 fieldConfig={"defaults": {"unit": unit, "decimals": decimals,
                                           "thresholds": {"mode": "absolute", "steps": steps}},
                              "overrides": []})


def timeseries(title, sql, x, y, w, h, unit="none", desc=""):
    return panel("timeseries", title, x, y, w, h, [target(sql, "time_series")], desc,
                 options={"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                          "tooltip": {"mode": "multi"}},
                 fieldConfig={"defaults": {"unit": unit, "custom": {"lineWidth": 2, "fillOpacity": 10,
                                                                   "showPoints": "never", "spanNulls": True}},
                              "overrides": []})


SERIES = """SELECT snapshot_ts AS time, {cols}
FROM stream_metrics_snapshot
WHERE $__timeFilter(snapshot_ts)
ORDER BY snapshot_ts"""

panels = [
    stat("Eventos válidos procesados", "eventos_validos", 0, 0,
         desc="Hogares re-emitidos por el productor que pasaron el contrato y se guardaron en stream_events."),
    stat("Eventos en el último minuto", "eventos_ultimo_minuto", 4, 0, color="purple",
         desc="Rendimiento del consumidor: eventos válidos procesados en los últimos 60 segundos. Baja a 0 si el productor se detiene."),
    stat("Huella media acumulada", "huella_media_kg", 8, 0, unit="kg CO₂/año", decimals=1, color="green",
         desc="Media de huella_total_anual_kg de todos los eventos válidos recibidos hasta ahora."),
    stat("Alertas de alta huella", "alertas_alta_huella", 12, 0, color="orange",
         desc="Hogares con huella mayor que el umbral (p95 histórico del DW)."),
    stat("% eventos inválidos", "pct_invalidos", 16, 0, unit="percent", decimals=2,
         thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 0.5}, {"color": "red", "value": 5}],
         desc="Eventos rechazados por el contrato sobre el total recibido. Van a stream_invalid_events y al topic huella-events-dlq."),
    stat("Latencia media del último lote", "latencia_media_ms", 20, 0, unit="ms", decimals=0, color="text",
         sql="""SELECT latencia_media_ms AS valor FROM stream_metrics_snapshot
WHERE latencia_media_ms IS NOT NULL ORDER BY snapshot_ts DESC LIMIT 1""",
         desc="Tiempo entre la publicación (event_ts) y el procesamiento, en el último lote con eventos. Sube mucho si el consumidor estuvo detenido y procesa eventos atrasados."),

    timeseries("Eventos procesados en el último minuto", SERIES.format(cols="eventos_ultimo_minuto AS `eventos/min`"),
               0, 4, 12, 8, desc="Cada punto es una foto de métricas del consumidor (cada lote o cada 5 s)."),
    timeseries("Huella media acumulada y umbral de alerta",
               SERIES.format(cols="huella_media_kg AS `huella media`, umbral_alerta_kg AS `umbral de alerta (p95)`"),
               12, 4, 12, 8, unit="kg CO₂/año",
               desc="La media se estabiliza a medida que llegan más hogares; el umbral es fijo (p95 del DW)."),
    timeseries("kWh/mes medio: piso cálido contra templado/frío (R6 en vivo)",
               SERIES.format(cols="kwh_medio_calido AS `cálido`, kwh_medio_frio_templado AS `templado o frío`"),
               0, 12, 12, 8, unit="kWh/mes",
               desc="Misma pregunta de R6, actualizada con cada evento: los hogares de departamentos cálidos consumen más."),
    panel("barchart", "kWh/mes medio por piso térmico (eventos recibidos)", 12, 12, 12, 8,
          [target("""SELECT CONCAT(piso_termico, ' (', COUNT(*), ' hogares)') AS `piso térmico`,
       ROUND(AVG(kwh_mes),1) AS `kWh/mes medio`
FROM stream_events GROUP BY piso_termico
ORDER BY FIELD(piso_termico, 'Cálido', 'Templado', 'Frío', 'Sin clima')""")],
          "Consumo medio de los eventos recibidos en cada piso térmico; entre paréntesis, cuántos hogares lleva cada grupo.",
          options={"orientation": "horizontal", "showValue": "always", "legend": {"showLegend": False,
                                                                                 "placement": "bottom",
                                                                                 "displayMode": "list"},
                   "xField": "piso térmico", "groupWidth": 0.7, "barWidth": 0.9},
          fieldConfig={"defaults": {"unit": "kWh/mes", "min": 0, "color": {"mode": "fixed", "fixedColor": "yellow"}},
                       "overrides": []}),
    panel("table", "Últimas alertas de alta huella", 0, 20, 12, 9,
          [target("""SELECT processed_at AS procesado, nombre_departamento AS departamento, estrato,
       piso_termico AS `piso térmico`, ROUND(huella_total_anual_kg,1) AS `huella kg/año`
FROM stream_events WHERE alta_huella = 1
ORDER BY processed_at DESC LIMIT 15""")],
          "Hogares que superaron el umbral, del más reciente al más antiguo."),
    panel("table", "Últimos eventos inválidos (rechazados por el contrato)", 12, 20, 12, 9,
          [target("""SELECT received_at AS recibido, CONCAT('p', kafka_partition, ' o', kafka_offset) AS `posición`,
       event_key AS llave, error AS motivo
FROM stream_invalid_events ORDER BY received_at DESC LIMIT 15""")],
          "Motivo de rechazo de cada evento que no cumplió el contrato (partición y offset en Kafka).",
          fieldConfig={"defaults": {}, "overrides": [
              {"matcher": {"id": "byName", "options": "recibido"}, "properties": [{"id": "custom.width", "value": 175}]},
              {"matcher": {"id": "byName", "options": "posición"}, "properties": [{"id": "custom.width", "value": 80}]},
              {"matcher": {"id": "byName", "options": "llave"}, "properties": [{"id": "custom.width", "value": 55}]},
          ]}),
    panel("bargauge", "Huella media por departamento (eventos recibidos, top 10)", 0, 29, 24, 9,
          [target("""SELECT nombre_departamento AS departamento, ROUND(AVG(huella_total_anual_kg),1) AS huella
FROM stream_events GROUP BY nombre_departamento HAVING COUNT(*) >= 5
ORDER BY huella DESC LIMIT 10""")],
          "Solo departamentos con al menos 5 eventos, para no destacar medias de 1 o 2 hogares.",
          options={"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
                   "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": True}},
          fieldConfig={"defaults": {"unit": "kg CO₂/año", "min": 0}, "overrides": []}),
]

dashboard = {
    "uid": "huella-streaming",
    "title": "Huella de carbono — streaming en tiempo real",
    "description": "Métricas del consumidor de Kafka (tablas stream_* del DW). Se refresca cada 5 s.",
    "tags": ["kafka", "streaming", "huella"],
    "timezone": "browser",
    "refresh": "5s",
    "time": {"from": "now-15m", "to": "now"},
    "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m"]},
    "schemaVersion": 39,
    "version": 1,
    "editable": False,
    "panels": panels,
}

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"Dashboard escrito en {OUT}")
