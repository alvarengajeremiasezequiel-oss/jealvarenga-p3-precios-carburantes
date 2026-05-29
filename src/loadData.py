import json
import os
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()


RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
SOURCE_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "private_key": os.getenv("SNOWFLAKE_PRIVATE_KEY"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "PRECIOS_CARBURANTE_DB"),
}

# Helper function to load and transform SNOWFLAKE_PRIVATE_KEY from .env
def _load_private_key(pem_str: str) -> bytes:
    pem_str = pem_str.replace("\\n", "\n")
    key = serialization.load_pem_private_key(pem_str.encode(), password=None, backend=default_backend())
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _get_snowflake_config():
    """Return a config dict with non-empty attrs, private_key converted to DER bytes."""
    cfg = {k: v for k, v in SNOWFLAKE_CONFIG.items() if v}
    cfg.setdefault("schema", "RAW")
    required = ["account", "user", "warehouse", "database"]
    missing = [k for k in required if k not in cfg]
    if "private_key" not in cfg:
        missing.append("private_key")
    if missing:
        raise EnvironmentError(f"Missing Snowflake env vars: {', '.join(missing)}")
    cfg["private_key"] = _load_private_key(cfg["private_key"])
    return cfg


def is_already_loaded(conn, ingestion_ts: str) -> bool:
    cfg = _get_snowflake_config()
    db, schema = cfg["database"], "RAW"
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(1) FROM {db}.{schema}.fuel_prices_json WHERE ingestion_ts = CAST(%s AS TIMESTAMP_NTZ)",
            (ingestion_ts,),
        )
        return cur.fetchone()[0] > 0


def get_snowflake_connection():
    cfg = _get_snowflake_config()
    return snowflake.connector.connect(**cfg)


# Helper function for multi statement sql files
def exec_sql_file(conn, path: str, db_name: str = None):
    sql = Path(path).read_text(encoding="utf-8").lstrip("\ufeff")
    if db_name:
        sql = sql.replace("{DB_NAME}", db_name)

    statements = []
    current = []
    depth = 0

    for line in sql.splitlines():
        stripped = line.strip().upper()

        # BEGIN END blocks treated as single statement
        if stripped == "BEGIN":
            depth += 1
        if stripped.startswith("END"):
            depth -= 1
        current.append(line)
        if depth == 0 and line.rstrip().endswith(";"):
            statements.append("\n".join(current))
            current = []

    if current:
        remaining = "\n".join(current).strip()
        if remaining:
            # If all remaining lines are comments or blank they are not appended
            if not all(l.strip().startswith("--") or not l.strip()
                       for l in remaining.splitlines()):
                statements.append(remaining)

    # Filter comment-only statements
    filtered = []
    for stmt in statements:
        stmt = stmt.strip()
        if not stmt:
            continue
        if all(line.strip().startswith("--") or not line.strip()
               for line in stmt.splitlines()):
            continue
        filtered.append(stmt)

    cur = conn.cursor()
    try:
        for stmt in filtered:
            cur.execute(stmt)
    finally:
        cur.close()


# Inserts the latest extracted json
def insert_raw_json(conn, ingestion_ts: str, payload_json: str, source_url: str):
    cfg = _get_snowflake_config()
    db, schema = cfg["database"], cfg.get("schema", "RAW")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {db}.{schema}.fuel_prices_json
                (ingestion_ts, payload, source_url)
            SELECT CAST(%s AS TIMESTAMP_NTZ), PARSE_JSON(%s), %s
            """,
            (ingestion_ts, payload_json, source_url),
        )
        return cur.rowcount

def update_stream(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            EXECUTE TASK SILVER.raw_to_silver_task;
            """
        )


def main():

    json_files = sorted(RAW_DIR.glob("*.json"))
    if not json_files:
        print("No raw JSON files found.")
        return

    conn = get_snowflake_connection()

    # Inicialización de base de datos
    exec_sql_file(conn, "sql/initDatabase.sql", SNOWFLAKE_CONFIG["database"])

    total_loaded = 0
    source_url = SOURCE_URL

    for filepath in json_files:

        # Normalization of timestamp for TIMESTAMP_NTZ format
        stem = filepath.stem.replace("T", " ")
        parts = stem.split(" ")
        ingestion_ts = f"{parts[0]} {parts[1].replace('-', ':')}"

        if is_already_loaded(conn, ingestion_ts):
            print(f"  Skipping {filepath.name} (already loaded)")
            continue

        try:
            payload = filepath.read_text(encoding="utf-8")
            json.loads(payload)
        except (json.JSONDecodeError, OSError) as e:
            print(f"  Skipping {filepath.name}: {e}")
            continue

        # Insertion of data into raw schema
        rows = insert_raw_json(conn, ingestion_ts, payload, source_url)
        if rows:
            total_loaded += rows
            update_stream(conn)
        print(f"  Loaded {filepath.name} ({rows} row(s))")

    conn.close()
    cfg = _get_snowflake_config()
    print(f"Done. {total_loaded} row(s) inserted into {cfg['database']}.{cfg.get('schema', 'RAW')}.fuel_prices_json")


if __name__ == "__main__":
    main()
