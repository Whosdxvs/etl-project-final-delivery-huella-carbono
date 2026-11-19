-- =========================================================================
-- PROYECTO: ETL Entrega Final — Huella de Carbono en Hogares Colombianos (ODS 12)
-- ARCHIVO : sql/streaming_schema.sql
-- MOTOR   : MySQL 8.0 (misma base del Data Warehouse: huella_carbono_dw_v2)
-- DESCRIPCIÓN: Tablas de la capa de streaming. Las escribe el consumidor de
--   Kafka (kafka/consumer_events.py) y las lee el dashboard en tiempo real
--   (Grafana). El consumidor las crea solo si no existen, así que este script
--   es documentación y una forma de crearlas a mano.
--   La carga batch del DAG (src/load.py) borra SOLO las tablas del esquema
--   estrella; estas tablas no se tocan al recargar el DW.
-- =========================================================================

-- Un registro por evento válido procesado. event_id es la llave primaria:
-- si Kafka entrega un evento dos veces (entrega "al menos una vez"), el
-- segundo INSERT se ignora y las métricas no se duplican.
CREATE TABLE IF NOT EXISTS stream_events (
    event_id               CHAR(36)      NOT NULL PRIMARY KEY,
    event_ts               DATETIME(3)   NOT NULL COMMENT 'Momento en que el productor publicó el evento (UTC)',
    processed_at           DATETIME(3)   NOT NULL COMMENT 'Momento en que el consumidor lo procesó (UTC)',
    latency_ms             INT           NOT NULL COMMENT 'processed_at - event_ts',
    kafka_partition        INT           NOT NULL,
    kafka_offset           BIGINT        NOT NULL,
    event_key              VARCHAR(10)   NOT NULL COMMENT 'Llave del mensaje: código DANE del departamento',
    sk_fact                INT           NOT NULL COMMENT 'Fila de fact_huella_carbono que se re-emitió (simulación)',
    codigo_departamento    INT           NOT NULL,
    nombre_departamento    VARCHAR(100)  NOT NULL,
    estrato                TINYINT       NOT NULL,
    piso_termico           VARCHAR(20)   NOT NULL,
    num_personas           SMALLINT      NOT NULL,
    kwh_mes                DECIMAL(12,2) NOT NULL,
    emisiones_energia_kg   DECIMAL(12,4) NOT NULL,
    emisiones_coccion_kg   DECIMAL(12,4) NOT NULL,
    huella_total_anual_kg  DECIMAL(12,4) NOT NULL,
    alta_huella            TINYINT       NOT NULL COMMENT '1 si supera el umbral de alerta (p95 histórico del DW)',
    INDEX idx_processed_at (processed_at),
    INDEX idx_depto (codigo_departamento),
    INDEX idx_piso (piso_termico)
);

-- Eventos que no cumplen el contrato. Se guardan con el texto original y el
-- motivo para poder revisarlos; también se reenvían al topic huella-events-dlq.
CREATE TABLE IF NOT EXISTS stream_invalid_events (
    id               BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY,
    received_at      DATETIME(3)   NOT NULL,
    kafka_partition  INT           NOT NULL,
    kafka_offset     BIGINT        NOT NULL,
    event_key        VARCHAR(50)   NULL,
    raw_value        TEXT          NOT NULL,
    error            VARCHAR(500)  NOT NULL,
    UNIQUE KEY uq_posicion (kafka_partition, kafka_offset) COMMENT 'Una re-entrega de Kafka no se cuenta dos veces',
    INDEX idx_received_at (received_at)
);

-- Foto de las métricas de streaming, una fila cada vez que el consumidor
-- confirma un lote. Es la serie de tiempo que dibuja el dashboard.
CREATE TABLE IF NOT EXISTS stream_metrics_snapshot (
    snapshot_ts              DATETIME(3)   NOT NULL PRIMARY KEY,
    eventos_validos          BIGINT        NOT NULL,
    eventos_invalidos        BIGINT        NOT NULL,
    pct_invalidos            DECIMAL(6,2)  NOT NULL,
    eventos_ultimo_minuto    INT           NOT NULL,
    huella_media_kg          DECIMAL(12,2) NULL COMMENT 'Media acumulada de huella_total_anual_kg',
    kwh_medio_mes            DECIMAL(12,2) NULL,
    kwh_medio_calido         DECIMAL(12,2) NULL,
    kwh_medio_frio_templado  DECIMAL(12,2) NULL,
    alertas_alta_huella      BIGINT        NOT NULL,
    latencia_media_ms        DECIMAL(12,1) NULL COMMENT 'Latencia media de los eventos del último lote',
    umbral_alerta_kg         DECIMAL(12,2) NOT NULL
);
