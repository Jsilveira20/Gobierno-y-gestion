import pandas as pd
import psycopg2
import sqlalchemy as sa

# --- credenciales Aiven ---
USER = "students"
PASSWORD = "AVNS_X6ug7DQ9I0TvJB8KGKC"
HOST = "pg-179f1912-festevesmunoz-0f62.b.aivencloud.com"
PORT = 11065
DB = "curated"

# --- string de conexión (el mismo formato que cuando viste las tablas) ---
url = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"

engine = sa.create_engine(url)

# --- consultas a las tablas ---
df_clients = pd.read_sql("SELECT * FROM clients;", engine)
df_orders = pd.read_sql("SELECT * FROM orders;", engine)
df_products = pd.read_sql("SELECT * FROM products;", engine)

# --- chequeo rápido ---
print("clients:", df_clients.shape)
print("orders :", df_orders.shape)
print("products:", df_products.shape)


#from sqlalchemy import inspect; print(inspect(engine).get_table_names())

#print(pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';", engine))
