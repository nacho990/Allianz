import os
import argparse
import logging
import json
import psycopg2
import numpy as np
from datetime import datetime, timezone
import pandas as pd
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError, OperationalError
from concurrent.futures import ThreadPoolExecutor, as_completed
from cryptography.fernet import Fernet

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

os.environ["LC_MESSAGES"] = "C"
os.environ["PYTHONIOENCODING"] = "utf-8"
os.environ["PGCLIENTENCODING"] = "utf-8"


def load_config(path='config.yaml'):
    with open(path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    # Allow environment variables to override YAML defaults
    cfg['csv_path'] = os.environ.get('CSV_PATH', cfg.get('csv_path'))
    cfg['etl']['chunk_size'] = int(os.environ.get('CHUNK_SIZE', cfg['etl'].get('chunk_size', 5000)))
    cfg['etl']['max_workers'] = int(os.environ.get('MAX_WORKERS', cfg['etl'].get('max_workers', 4)))

    return cfg


def setup_logging(path, level='INFO'):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    logging.basicConfig(
        filename=path,
        level=getattr(logging, level),
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    # Stream logs to console as well
    console = logging.StreamHandler()
    console.setLevel(getattr(logging, level))
    console.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logging.getLogger().addHandler(console)

#Database Connection and Schema Management
def build_connection_string(cfg_db):
    dialect = cfg_db.get('dialect', 'sqlite')

    if dialect == 'sqlite':
        dbfile = cfg_db.get('database') or cfg_db.get('path') or 'etl_sales.db'
        return f"sqlite:///{dbfile}"

    user = cfg_db.get('username') or os.environ.get('DB_USERNAME', '')
    pwd = cfg_db.get('password') or os.environ.get('DB_PASSWORD', '')
    host = cfg_db.get('host') or os.environ.get('DB_HOST', 'localhost')
    port = cfg_db.get('port') or os.environ.get('DB_PORT', '5432')
    db = cfg_db.get('database') or os.environ.get('DB_NAME', '')

    return f"{dialect}://{user}:{pwd}@{host}:{port}/{db}"


def ensure_schema(engine, schema_file='db_schema.sql'):
    if not os.path.exists(schema_file):
        logging.warning("Schema file %s not found; skipping schema creation", schema_file)
        return

    sql = open(schema_file, 'r', encoding='utf-8').read()
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)
    logging.info("Ensured DB schema from %s", schema_file)

def get_fernet(key=None):
    if key:
        return Fernet(key.encode() if isinstance(key, str) else key)
    else:
        k = Fernet.generate_key()
        logging.warning("No fernet_key provided. Generated key: %s — save it to decrypt later.", k.decode())
        return Fernet(k)

def data_quality_checks(df):
    issues = []

    # Check for missing values
    missing = df.isnull().sum().to_dict()
    if sum(missing.values()) > 0:
        issues.append({'type': 'missing', 'detail': missing})

    # Check for duplicate transaction IDs
    if df.duplicated(subset=['transaction_id']).any():
        issues.append({'type': 'duplicates', 'count': int(df.duplicated(subset=['transaction_id']).sum())})

    # Check for invalid quantities
    if (pd.to_numeric(df['quantity'], errors='coerce') <= 0).any():
        issues.append({'type': 'quantity_non_positive'})

    # Simple outlier detection
    if 'quantity' in df.columns:
        try:
            q99 = df['quantity'].quantile(0.99)
            if q99 > 1000:
                issues.append({'type': 'quantity_outliers', 'q99': int(q99)})
        except Exception:
            pass

    return issues


def harmonize_schema(df, expected_cols):

    # Add missing expected columns
    for col in expected_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # Collect unexpected columns
    extras = [c for c in df.columns if c not in expected_cols]
    if extras:
        df['raw_payload'] = df[extras].apply(
            lambda row: {k: v for k, v in row.to_dict().items() if pd.notna(v)}, axis=1)
        df = df.drop(columns=extras)

    # Ensure correct column order
    cols = expected_cols + (['raw_payload'] if 'raw_payload' in df.columns else [])
    return df[cols]

def transform_chunk(df, fernet=None):

    df = df.copy()
    df['sale_date'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)

    if fernet:
        def _enc(x):
            if pd.isna(x): return None
            return fernet.encrypt(str(x).encode()).decode()
        df['customer_id'] = df['customer_id'].astype(str).apply(_enc)
        df['product_id'] = df['product_id'].astype(str).apply(_enc)

    return df

def get_last_load(engine, metadata_table):
    with engine.begin() as conn:
        res = conn.execute(text(f"SELECT value FROM {metadata_table} WHERE key='last_load'")).fetchone()
        return res[0] if res else None


def set_last_load(engine, metadata_table, value):
    with engine.begin() as conn:
        dialect = engine.dialect.name
        if dialect == 'sqlite':
            conn.execute(
                text(f"INSERT OR REPLACE INTO {metadata_table} (key, value) VALUES ('last_load', :v)"),
                {'v': value})
        else:
            conn.execute(
                text("""
                    INSERT INTO {metadata_table} (key, value)
                    VALUES ('last_load', :v)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """.format(metadata_table=metadata_table)),
                {'v': value})


def load_subchunk(engine, table_name, df_sub):
    try:
        df_sub.to_sql(table_name, engine, if_exists='append', index=False)
        return len(df_sub)
    except IntegrityError:
        logging.exception("Integrity error in bulk insert; falling back to row-by-row")
        count = 0
        with engine.begin() as conn:
            for _, row in df_sub.iterrows():
                cols = ','.join(row.index)
                params = ','.join([f":{c}" for c in row.index])
                try:
                    conn.execute(text(f"INSERT INTO {table_name} ({cols}) VALUES ({params})"), row.to_dict())
                    count += 1
                except Exception:
                    logging.warning("Skipping row (likely duplicate or constraint): %s", row.get('transaction_id'))
        return count
    except OperationalError:
        logging.exception("Operational error during insertion")
        raise

def run_etl(config_path):
    cfg = load_config(os.path.join(BASE_DIR, config_path))
    setup_logging(cfg['logging']['path'], cfg['logging'].get('level', 'INFO'))
    logging.info("Starting ETL run")

    # Test DB connection
    try:
        conn = psycopg2.connect(
            dbname=cfg["db"]["database"],
            user=cfg["db"]["username"],
            password=cfg["db"]["password"],
            host=cfg["db"]["host"],
            port=cfg["db"]["port"],
            options="-c client_encoding=UTF8"
        )
        logging.info("Successful psycopg2 connection")
        conn.close()
    except Exception as e:
        logging.error("Database connection failed: %s", e)
        raise

    # SQLAlchemy engine setup
    db = cfg["db"]
    conn_str = f"postgresql+psycopg2://{db['username']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
    engine = create_engine(conn_str, connect_args={"options": "-c client_encoding=UTF8"}, future=True)
    logging.info("SQLAlchemy engine created")

    ensure_schema(engine)

    # Encryption setup
    fernet = None
    if cfg.get('encryption', {}).get('enabled'):
        key = cfg.get('encryption', {}).get('fernet_key') or os.environ.get('FERNET_KEY')
        fernet = get_fernet(key) if key else get_fernet(None)

    # ETL parameters
    csv_path = cfg['csv_path']
    chunk_size = int(cfg['etl'].get('chunk_size', 5000))
    insert_batch = int(cfg['etl'].get('insert_batch', 1000))
    max_workers = int(cfg['etl'].get('max_workers', 4))
    incremental = bool(cfg['etl'].get('incremental', True))
    metadata_table = cfg['etl'].get('metadata_table', 'etl_metadata')
    table_name = cfg['db'].get('table', 'sales')
    expected_cols = ['transaction_id', 'customer_id', 'product_id', 'quantity', 'timestamp']

    # Retrieve last successful load timestamp
    last_load = None
    if incremental:
        try:
            last_load = get_last_load(engine, metadata_table)
            logging.info("Last load timestamp: %s", last_load)
        except Exception:
            logging.exception("Failed to retrieve last load; full load mode")

    # Helper: safe JSON serialization
    def safe_json_dumps(obj):
        def default(o):
            if isinstance(o, (pd.Timestamp, datetime)): return o.isoformat()
            elif isinstance(o, (np.integer, int)): return int(o)
            elif isinstance(o, (np.floating, float)): return float(o)
            elif pd.isna(o): return None
            return str(o)
        try:
            return json.dumps(obj, default=default, ensure_ascii=False)
        except Exception as e:
            logging.warning("JSON serialization failed: %s", e)
            return None

    # Process CSV in chunks
    total_inserted = 0
    for raw_chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        logging.info("Read chunk with %d rows", len(raw_chunk))

        # Incremental filtering
        if incremental and last_load:
            raw_chunk['_parsed_ts'] = pd.to_datetime(raw_chunk['timestamp'], errors='coerce', utc=True)
            last_load_ts = pd.to_datetime(last_load, utc=True)
            raw_chunk = raw_chunk[raw_chunk['_parsed_ts'] > last_load_ts]
            if raw_chunk.empty:
                logging.info("No new rows since last_load")
                continue

        # Transform, validate, and insert
        harmonized = harmonize_schema(raw_chunk, expected_cols)
        transformed = transform_chunk(harmonized, fernet=fernet)

        issues = data_quality_checks(transformed)
        if issues:
            logging.warning("Data quality issues found: %s", issues)

        insert_df = transformed.drop(columns=['timestamp'], errors='ignore')
        cols = ['transaction_id', 'customer_id', 'product_id', 'quantity', 'sale_date']
        if 'raw_payload' in insert_df.columns:
            cols.append('raw_payload')
        insert_df = insert_df[cols]

        # Parallel loading
        sub_batch = max(1, int(insert_batch))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = []
            for i in range(0, len(insert_df), sub_batch):
                sub = insert_df.iloc[i:i + sub_batch]
                if 'raw_payload' in sub.columns:
                    sub = sub.copy()
                    sub['raw_payload'] = sub['raw_payload'].apply(
                        lambda x: safe_json_dumps(x) if pd.notna(x) else None)
                futures.append(ex.submit(load_subchunk, engine, table_name, sub))
            for fut in as_completed(futures):
                inserted = fut.result()
                total_inserted += inserted
                logging.info("Inserted %d rows (subchunk)", inserted)

    # Update metadata
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        set_last_load(engine, metadata_table, now_iso)
        logging.info("Updated last_load to %s", now_iso)
    except Exception:
        logging.exception("Failed to update last_load metadata")

    logging.info("ETL Sucess. Total rows inserted: %d", total_inserted)
    return total_inserted

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run with config file.")
    parser.add_argument('--config', '-c', default='config.yaml', help='YAML Path')
    args = parser.parse_args()

    try:
        run_etl(args.config)
    except Exception as e:
        logging.exception("ETL failed: %s", e)
        raise
