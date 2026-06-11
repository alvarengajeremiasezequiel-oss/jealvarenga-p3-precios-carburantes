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
-- GOLD tables
-------------------------------------------
CREATE TABLE IF NOT EXISTS GOLD.fact_fuel_price (
    date_id       DATE,
    ingestion_ts  TIMESTAMP_NTZ,
    ideess        VARCHAR(20),
    product       VARCHAR(50),
    price         NUMBER(12,3),
    daily_avg_price     NUMBER(12,3),
    daily_min_price     NUMBER(12,3),
    daily_max_price     NUMBER(12,3)
)
COMMENT = 'Histórico de precios de Gasoleo A, Gasolina 95 E5 y AdBlue por estación con agregación diaria';

CREATE TABLE IF NOT EXISTS GOLD.dim_station (
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

CREATE TABLE IF NOT EXISTS GOLD.dim_product (
    product_name    VARCHAR(50)
)
COMMENT = 'Dimensión de productos vendidos';

CREATE TABLE IF NOT EXISTS GOLD.dim_date (
    date_id         DATE PRIMARY KEY,
    year            INT,
    month           INT,
    day_of_the_week VARCHAR(20)
)
COMMENT = 'Dimensión de fechas';


----------------------------------------------------------------------
-- Task for processing RAW layer and updating SILVER and GOLD layers: 
----------------------------------------------------------------------
CREATE TASK IF NOT EXISTS SILVER.raw_processing_task
    WAREHOUSE = COMPUTE_WH
WHEN
    SYSTEM$STREAM_HAS_DATA('RAW.fuel_prices_json_stream')
AS
BEGIN
    -- Recovery path: rebuild from all RAW if SILVER or GOLD fact tables are empty
    IF ((SELECT COUNT(1) FROM SILVER.fct_price_snapshot) = 0) THEN
        TRUNCATE TABLE SILVER.fct_price_snapshot;
        TRUNCATE TABLE GOLD.fact_fuel_price;

        CREATE OR REPLACE TEMP TABLE SILVER._raw_staging AS
        SELECT ingestion_ts, payload, source_url
        FROM RAW.fuel_prices_json_stream
        UNION
        SELECT ingestion_ts, payload, source_url
        FROM RAW.fuel_prices_json;
    ELSE
        CREATE OR REPLACE TEMP TABLE SILVER._raw_staging AS
        SELECT ingestion_ts, payload, source_url
        FROM RAW.fuel_prices_json_stream;
    END IF;

    -- Flatten and unpivot prices into a shared temp table
    CREATE OR REPLACE TEMP TABLE SILVER._raw_prices AS
    WITH flattened AS (
        SELECT
            s.ingestion_ts::TIMESTAMP_NTZ AS ingestion_ts,
            r.value:IDEESS::VARCHAR AS ideess,
            r.value:"Precio Gasoleo A"::VARCHAR      AS "Gasoleo A",
            r.value:"Precio Gasolina 95 E5"::VARCHAR  AS "Gasolina 95 E5",
            r.value:"Precio Adblue"::VARCHAR          AS "AdBlue"
        FROM SILVER._raw_staging s,
             LATERAL FLATTEN(input => s.payload:ListaEESSPrecio) r
    )
    SELECT
        ingestion_ts,
        ideess,
        product,
        REPLACE(price_str, ',', '.')::FLOAT AS price_eur_l
    FROM flattened
    UNPIVOT (
        price_str FOR product IN ("Gasoleo A", "Gasolina 95 E5", "AdBlue")
    ) unp
    WHERE price_str != '';


    -- Merge of new stations into dim_station in SILVER
    MERGE INTO SILVER.dim_station AS t
    USING (
        SELECT
            ideess,
            rotulo,
            direccion,
            cp,
            municipio,
            provincia,
            idccaa,
            latitud,
            longitud
        FROM (
            SELECT
                s.ingestion_ts,
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
        )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ideess ORDER BY ingestion_ts DESC) = 1
    ) AS src
    ON t.ideess = src.ideess
    WHEN MATCHED THEN UPDATE SET
        rotulo    = src.rotulo,     direccion = src.direccion,  cp = src.cp,
        municipio = src.municipio,  provincia = src.provincia,  idccaa = src.idccaa,
        latitud   = src.latitud,    longitud  = src.longitud
    WHEN NOT MATCHED THEN INSERT
        (ideess, rotulo, direccion, cp, municipio, provincia, idccaa, latitud, longitud)
        VALUES (src.ideess,     src.rotulo,  src.direccion,  src.cp,      src.municipio,
                src.provincia,  src.idccaa,  src.latitud,    src.longitud);


    -- Insert of the 3 products' new prices in SILVER
    INSERT INTO SILVER.fct_price_snapshot (ingestion_ts, ideess, product, price_eur_l)
    SELECT ingestion_ts, ideess, product, price_eur_l
    FROM SILVER._raw_prices;


    -- Insert new dates into GOLD.dim_date
    INSERT INTO GOLD.dim_date (date_id, year, month, day_of_the_week)
    SELECT DISTINCT
        s.ingestion_ts::DATE,
        YEAR(s.ingestion_ts::DATE),
        MONTH(s.ingestion_ts::DATE),
        DAYNAME(s.ingestion_ts::DATE)
    FROM SILVER._raw_staging s
    WHERE NOT EXISTS (
        SELECT 1 FROM GOLD.dim_date d WHERE d.date_id = s.ingestion_ts::DATE
    );

    -- Insert new prices into GOLD.fact_fuel_price
    INSERT INTO GOLD.fact_fuel_price (date_id, ingestion_ts, ideess, product, price)
    SELECT
        ingestion_ts::DATE,
        ingestion_ts,
        ideess,
        product,
        price_eur_l::NUMBER(12,3) AS price
    FROM SILVER._raw_prices;

    -- Update daily aggregation columns for current date
    MERGE INTO GOLD.fact_fuel_price AS t
    USING (
        SELECT
            date_id,
            product,
            ROUND(AVG(price), 2) AS daily_avg_price,
            ROUND(MIN(price), 2) AS daily_min_price,
            ROUND(MAX(price), 2) AS daily_max_price
        FROM GOLD.fact_fuel_price
        WHERE date_id IN (SELECT DISTINCT ingestion_ts::DATE FROM SILVER._raw_prices)
        GROUP BY date_id, product
    ) AS s
    ON t.date_id = s.date_id AND t.product = s.product
    WHEN MATCHED THEN UPDATE SET
        t.daily_avg_price = s.daily_avg_price,
        t.daily_min_price = s.daily_min_price,
        t.daily_max_price = s.daily_max_price;

    -- Merge new/updated stations into GOLD.dim_station
    MERGE INTO GOLD.dim_station AS t
    USING (
        SELECT
            ideess,
            rotulo,
            direccion,
            cp,
            municipio,
            provincia,
            idccaa,
            latitud,
            longitud
        FROM (
            SELECT
                s.ingestion_ts,
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
        )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ideess ORDER BY ingestion_ts DESC) = 1
    ) AS src
    ON t.ideess = src.ideess
    WHEN MATCHED THEN UPDATE SET
        rotulo    = src.rotulo,     direccion = src.direccion,  cp = src.cp,
        municipio = src.municipio,  provincia = src.provincia,  idccaa = src.idccaa,
        latitud   = src.latitud,    longitud  = src.longitud
    WHEN NOT MATCHED THEN INSERT
        (ideess, rotulo, direccion, cp, municipio, provincia, idccaa, latitud, longitud)
        VALUES (src.ideess,     src.rotulo,  src.direccion,  src.cp,      src.municipio,
                src.provincia,  src.idccaa,  src.latitud,    src.longitud);

END;

-- Enable task
ALTER TASK SILVER.raw_processing_task RESUME;