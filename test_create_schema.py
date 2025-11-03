from sqlalchemy import create_engine
import os

db = {
    'username': 'postgres',
    'password': 'secret',
    'host': 'localhost',
    'port': 5432,
    'database': 'mydb',
    'dialect': 'postgresql'
}
conn_str = f"postgresql+psycopg2://{db['username']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
engine = create_engine(conn_str, connect_args={"options":"-c client_encoding=UTF8"}, future=True)

with open("db_schema.sql","r",encoding="utf-8") as f:
    sql = f.read()

with engine.begin() as conn:
    conn.exec_driver_sql(sql)

print(" OK db_schema.sql with SQLAlchemy.")
