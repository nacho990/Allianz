-- Schema inicial para la tabla sales (SQLite/Postgres compatible)
CREATE TABLE IF NOT EXISTS sales (
transaction_id TEXT PRIMARY KEY,
customer_id TEXT NOT NULL,
product_id TEXT NOT NULL,
quantity INTEGER NOT NULL,
sale_date TIMESTAMP NOT NULL,
raw_payload JSON DEFAULT NULL
);


-- metadata table to hold last load timestamp
CREATE TABLE IF NOT EXISTS etl_metadata (
key TEXT PRIMARY KEY,
value TEXT
);