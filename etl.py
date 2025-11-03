#!/usr/bin/env python3
"""
etl.py
------------------------------------------------------------
ETL (Extract, Transform, Load) Pipeline

Core features:
- Reads CSV files in chunks for memory efficiency
- Transforms and validates incoming data
- Optionally encrypts sensitive fields (e.g., customer_id)
- Inserts data into the 'sales' table in PostgreSQL
- Supports incremental loads and concurrent inserts
- Handles schema drift by storing unexpected fields
  as JSON inside a 'raw_payload' column
------------------------------------------------------------
Author: Your Name
Version: 1.0
Date: 2025-11-03
"""

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

# ======================================================
# Environment setup
# ======================================================

# Base directory for relative paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Set encoding variables to ensure UTF-8 compatibility
os.environ["LC_MESSAGES"] = "C"
os.environ["PYTHONIOENCODING"] = "utf-8"
os.environ["PGCLIENTENCODING"] = "utf-8"


# ======================================================
# Helpers: Configuration and Logging
# ======================================================

def load_config(path='config.yaml'):
    """
    Loads the YAML configuration file and applies environment variable overrides.

    Args:
        path (str): Path to the YAML configuration file.

    Returns:
        dict: Parsed configuration dictionary.
    """
    with open(path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    # Allow environment variables to override YAML defaults
    cfg['csv_path'] = os.environ.get('CSV_PATH', cfg.get('csv_path'))
    cfg['etl']['chunk_size'] = int(os.environ.get('CHUNK_SIZE', cfg['etl'].get('chunk_size', 5000)))
    cfg['etl']['max_workers'] = int(os.environ.get('MAX_WORKERS', cfg['etl'].get('max_workers', 4)))

    return cfg


def setup_logging(path, level='INFO'):
    """
    Configures logging to both file and console outputs.

    Args:
        path (str): Path to the log file.
        level (str): Logging level (e.g., DEBUG, INFO, WARNING, ERROR).
    """
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


# ======================================================
# Database Connection and Schema Management
# ======================================================

def build_connection_string(cfg_db):
    """
    Constructs an SQLAlchemy-compatible database connection string.

    Supports both PostgreSQL and SQLite.

    Args:
        cfg_db (dict): Database configuration parameters.

    Returns:
        str: SQLAlchemy connection string.
    """
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
    """
    Ensures that the database schema exists.

    Reads and executes the SQL DDL statements from a schema file.

    Args:
        engine (sqlalchemy.Engine): SQLAlchemy engine object.
        schema_file (str): Path to SQL file containing schema definition.
    """
    if not os.path.exists(schema_file):
        logging.warning("Schema file %s not found; skipping schema creation", schema_file)
        return

    sql = open(schema_file, 'r', encoding='utf-8').read()
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)
    logging.info("Ensured DB schema from %s", schema_file)


# ======================================================
# Encryption Helpers
# ======================================================

def get_fernet(key=None):
    """
    Initializes a Fernet encryption object.

    If no key is provided, a new one is generated and logged.

    Args:
        key (str|bytes|None): Encryption key.

    Returns:
        Fernet: Encryption object.
    """
    if key:
        return Fernet(key.encode() if isinstance(key, str) else key)
    else:
        k = Fernet.generate_key()
        logging.warning("No fernet_key provided. Generated key: %s — save it to decrypt later.", k.decode())
        return Fernet(k)


# ======================================================
# Data Quality and Schema Harmonization
# ======================================================

def data_quality_checks(df):
    """
    Performs basic data quality checks on a DataFrame.

    Checks for:
      - Missing values
      - Duplicate transaction IDs
      - Non-positive quantities
      - Quantity outliers (above 99th percentile)

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        list[dict]: List of identified data quality issues.
    """
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
    """
    Aligns incoming data with the expected schema.

    - Adds missing columns with nulls.
    - Moves unexpected columns into a 'raw_payload' JSON column.

    Args:
        df (pd.DataFrame): Input data.
        expected_cols (list): Expected column names.

    Returns:
        pd.DataFrame: Harmonized DataFrame.
    """
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


# ======================================================
# Transformations
# ======================================================

def transform_chunk(df, fernet=None):
    """
    Applies necessary transformations to a data chunk.

    - Converts timestamp → sale_date
    - Normalizes quantity to integer
    - Encrypts IDs if encryption is enabled

    Args:
        df (pd.DataFrame): Input data chunk.
        fernet (Fernet|None): Optional encryption object.

    Returns:
        pd.DataFrame: Transformed data.
    """
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


# ======================================================
# Load Functions
# ======================================================

def get_last_load(engine, metadata_table):
    """Retrieve the timestamp of the last successful ETL execution."""
    with engine.begin() as conn:
        res = conn.execute(text(f"SELECT value FROM {metadata_table} WHERE key='last_load'")).fetchone()
        return res[0] if res else None


def set_last_load(engine, metadata_table, value):
    """Update or insert the 'last_load' timestamp in the ETL metadata table."""
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
    """
    Inserts a small batch of rows (sub-chunk) into the target table.

    Uses bulk insertion for performance.
    Falls back to row-by-row insertion if IntegrityError occurs.

    Args:
        engine (sqlalchemy.Engine): Active SQLAlchemy engine.
        table_name (str): Destination table name.
        df_sub (pd.DataFrame): Data subset to insert.

    Returns:
        int: Number of successfully inserted rows.
    """
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


# ======================================================
# Main ETL Pipeline
# ======================================================

def run_etl(config_path):
    """
    Executes the complete ETL process:
    - Loads configuration
    - Connects to the database and ensures schema
    - Reads CSV data in chunks
    - Transforms, validates, and loads data
    - Updates ETL metadata table
    """
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
        """Safely converts pandas/numpy objects to JSON serializable types."""
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

        # Parallelized loading
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

    # Update ETL metadata
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        set_last_load(engine, metadata_table, now_iso)
        logging.info("Updated last_load to %s", now_iso)
    except Exception:
        logging.exception("Failed to update last_load metadata")

    logging.info("ETL completed successfully. Total rows inserted: %d", total_inserted)
    return total_inserted


# ======================================================
# Script Entry Point
# ======================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the ETL pipeline with a given config file.")
    parser.add_argument('--config', '-c', default='config.yaml', help='Path to YAML configuration file')
    args = parser.parse_args()

    try:
        run_etl(args.config)
    except Exception as e:
        logging.exception("ETL failed: %s", e)
        raise
