import psycopg2
from sqlalchemy import create_engine

# Datos de conexión (puedes copiarlos del YAML)
db_config = {
    "dialect": "postgresql",
    "username": "postgres",
    "password": "secret",
    "host": "localhost",
    "port": 5432,
    "database": "mydb"
}

print("=== 🔍 PRUEBA DE CONEXIÓN DIRECTA (psycopg2) ===")
try:
    conn = psycopg2.connect(
        dbname=db_config["database"],
        user=db_config["username"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"],
        options="-c client_encoding=UTF8"
    )
    print("✅ Conexión directa exitosa (psycopg2)")
    cur = conn.cursor()
    cur.execute("SELECT version();")
    print("PostgreSQL versión:", cur.fetchone())
    cur.close()
    conn.close()
except Exception as e:
    print("❌ Error en conexión directa:", e)

print("\n=== 🔍 PRUEBA DE CONEXIÓN SQLALCHEMY ===")
try:
    conn_str = (
        f"postgresql+psycopg2://{db_config['username']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    print("Cadena de conexión:", conn_str)

    engine = create_engine(
        conn_str,
        connect_args={"options": "-c client_encoding=UTF8"},
        future=True
    )

    with engine.connect() as conn:
        result = conn.execute("SELECT current_database();")
        print("✅ SQLAlchemy conectado a la base:", result.scalar())
except Exception as e:
    print("❌ Error en conexión SQLAlchemy:", e)
