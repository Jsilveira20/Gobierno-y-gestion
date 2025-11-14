import pandas as pd
import psycopg2
import sqlalchemy as sa
from config import USER, PASSWORD, HOST, PORT, DB

# --- string de conexión ---
url = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"
engine = sa.create_engine(url)

# --- consultas ---
df_clients = pd.read_sql("SELECT * FROM clients;", engine)
df_orders = pd.read_sql("SELECT * FROM orders;", engine)
df_products = pd.read_sql("SELECT * FROM products;", engine)

# --- chequeo rápido ---
print("clients:", df_clients.shape)
print("orders:", df_orders.shape)
print("products:", df_products.shape)

# --- guardar CSV ---
df_clients.to_csv("data/clients.csv", index=False)
df_orders.to_csv("data/orders.csv", index=False)
df_products.to_csv("data/products.csv", index=False)

print("\n✔ CSV GENERADOS en carpeta /data\n")


#from sqlalchemy import inspect; print(inspect(engine).get_table_names())

#print(pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';", engine))
