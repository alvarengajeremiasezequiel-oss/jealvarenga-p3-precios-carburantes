# Observatorio de precios de carburantes

Pipeline que captura cada 3 horas los precios de carburantes de las estaciones de servicio españolas desde la API pública del Ministerio.

## Arquitectura

```
API Pública (Ministerio)
        │
        ▼
extractData.py ───► data/raw/<ingestion_ts>.json   (JSON completo sin transformar)
        │
        ▼
loadData.py ───► Snowflake (RAW.fuel_prices_json)
```
GitHub Actions ejecuta el pipeline cada 3 horas mediante el workflow `.github/workflows/pipeline.yml` con cron `0 */3 * * *`.

### Scripts

#### extractData.py
- Consulta la API REST y almacena el **JSON completo sin transformar** en `data/raw/` con un timestamp de ingesta.
- El filtrado por producto (**Gasoleo A**, **Gasolina 95 E5**, **AdBlue**) se realiza en la capa SILVER de Snowflake vía SQL.

#### loadData.py
- Consulta Snowflake para evitar insertar timestamps ya cargados en ejecuciones posteriores.
- Lee los archivos de `data/raw/` y los inserta en la tabla `RAW.fuel_prices_json` de Snowflake (`ingestion_ts`, `payload` VARIANT, `source_url`).
- Crea el task `raw_to_silver_task` que actualiza las capas SILVER y GOLD si no existe.
- Reanuda el task si se acaba de crear.

## Tecnologías utilizadas
El proyecto emplea el conector de snowflake para python, requiere una cuenta de snowflake con un warehouse existente, github actions y finalmente un cliente de powerBI para la visualización del informe.

## Pasos para ejecutar el proyecto
Se deben tener un par de claves de snowflake para ejecutar el pipeline. Se pueden crear con `src/generateSnowflakeKeypair.py`. El script `register_public_key.sql` generado en `snowflake_keys/` debe editarse con el nombre del usuario de snowflake en **YOUR USER** y ejecutarse en snowflake. Las credenciales de Snowflake se pasan como secretos de GitHub para la ejecución en github actions del pipeline:

| Variable | Descripción |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Cuenta de Snowflake |
| `SNOWFLAKE_USER` | Usuario |
| `SNOWFLAKE_PRIVATE_KEY` | Clave privada |
| `SNOWFLAKE_WAREHOUSE` | Warehouse (opcional) |
| `SNOWFLAKE_DATABASE` | Base de datos (opcional) |

