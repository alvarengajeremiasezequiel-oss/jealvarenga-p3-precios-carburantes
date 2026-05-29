-------------------------------------------
-- Create database, schema and stage
-------------------------------------------
CREATE DATABASE IF NOT EXISTS {DB_NAME}
    COMMENT = 'DB asociado al reto 3: Observatorio de precios de carburante';
USE DATABASE {DB_NAME};
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;
USE SCHEMA RAW;
CREATE STAGE IF NOT EXISTS raw_data;

-------------------------------------------
-- RAW table
-------------------------------------------  
CREATE TABLE IF NOT EXISTS RAW.fuel_prices_json (
    ingestion_ts   TIMESTAMP,
    payload        VARIANT,
    source_url     VARCHAR(512)
);

-------------------------------------------
-- SILVER tables
-------------------------------------------
CREATE TABLE IF NOT EXISTS SILVER.dim_station (
    ideess     VARCHAR(20),
    rotulo     VARCHAR(200),
    direccion  VARCHAR(500),
    cp         VARCHAR(10),
    municipio  VARCHAR(100),
    provincia  VARCHAR(100),
    idccaa     VARCHAR(10),
    latitud    FLOAT,
    longitud   FLOAT
)
COMMENT = 'Dimensión de estaciones de servicio';

CREATE TABLE IF NOT EXISTS SILVER.fct_price_snapshot (
    ingestion_ts  TIMESTAMP_NTZ,
    ideess        VARCHAR(20),
    product       VARCHAR(50),
    price_eur_l   FLOAT
)
COMMENT = 'Histórico de precios de Gasoleo A, Gasolina 95 E5 y AdBlue por estación';

-------------------------------------------
-- Stream para capturar inserts en RAW
-------------------------------------------
CREATE STREAM IF NOT EXISTS RAW.fuel_prices_json_stream
    ON TABLE RAW.fuel_prices_json
    APPEND_ONLY = TRUE
    COMMENT = 'Stream para detectar nuevas cargas RAW y propagarlas a SILVER';

-------------------------------------------
-- Task SILVER: procesa el stream cada 3 horas
-------------------------------------------
CREATE OR REPLACE TASK SILVER.raw_to_silver_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '3 HOUR'
WHEN
    SYSTEM$STREAM_HAS_DATA('RAW.fuel_prices_json_stream')
AS
BEGIN
    -- Capturar el stream en temp table para poder usarlo en varios DMLs
    CREATE OR REPLACE TEMP TABLE SILVER._raw_staging AS
    SELECT * FROM RAW.fuel_prices_json_stream;

    -- Merge de estaciones en dim_station
    MERGE INTO SILVER.dim_station AS t
    USING (
        SELECT DISTINCT
            CAST(r.value:IDEESS        AS VARCHAR) AS ideess,
            CAST(r.value:"Rótulo"      AS VARCHAR) AS rotulo,
            CAST(r.value:"Dirección"   AS VARCHAR) AS direccion,
            CAST(r.value:"C.P."        AS VARCHAR) AS cp,
            CAST(r.value:Municipio     AS VARCHAR) AS municipio,
            CAST(r.value:Provincia     AS VARCHAR) AS provincia,
            CAST(r.value:IDCCAA        AS VARCHAR) AS idccaa,
            CAST(REPLACE(r.value:Latitud::VARCHAR, ',', '.')          AS FLOAT) AS latitud,
            CAST(REPLACE(r.value:"Longitud (WGS84)"::VARCHAR, ',', '.') AS FLOAT) AS longitud
        FROM SILVER._raw_staging s,
             LATERAL FLATTEN(input => s.payload:ListaEESSPrecio) r
    ) AS src
    ON t.ideess = src.ideess
    WHEN MATCHED THEN UPDATE SET
        rotulo      = src.rotulo,       direccion   = src.direccion, cp = src.cp,
        municipio   = src.municipio,    provincia   = src.provincia, idccaa = src.idccaa,
        latitud     = src.latitud,      longitud    = src.longitud
    WHEN NOT MATCHED THEN INSERT
        (ideess, rotulo, direccion, cp, municipio, provincia, idccaa, latitud, longitud)
        VALUES (src.ideess, src.rotulo, src.direccion, src.cp,
                src.municipio, src.provincia, src.idccaa, src.latitud, src.longitud);


    -- Insertar precios solo de los 3 productos (AdBlue solo si tiene valor)
    INSERT INTO SILVER.fct_price_snapshot (ingestion_ts, ideess, product, price_eur_l)
    SELECT
        sub.ingestion_ts::TIMESTAMP_NTZ,
        sub.ideess,
        sub.product,
        REPLACE(sub.price_str, ',', '.')::FLOAT AS price_eur_l
    FROM (
        SELECT
            CAST(s.ingestion_ts              AS TIMESTAMP_NTZ) AS ingestion_ts,
            CAST(r.value:IDEESS                    AS VARCHAR) AS ideess,
            CAST(r.value:"Precio Gasoleo A"        AS VARCHAR) AS "Gasoleo A",
            CAST(r.value:"Precio Gasolina 95 E5"   AS VARCHAR) AS "Gasolina 95 E5",
            CAST(r.value:"Precio Adblue"           AS VARCHAR) AS "AdBlue"
        FROM SILVER._raw_staging s,
             LATERAL FLATTEN(input => s.payload:ListaEESSPrecio) r
    ) sub
    UNPIVOT (
        price_str FOR product IN (
            "Gasoleo A",
            "Gasolina 95 E5",
            "AdBlue"
        )
    )
    WHERE price_str != '';
END;

-- Habilitar el task
ALTER TASK SILVER.raw_to_silver_task RESUME;
