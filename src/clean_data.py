import pandas as pd

# 1) Cargar datasets
clients = pd.read_csv("data/clients.csv")
orders  = pd.read_csv("data/orders.csv")
products = pd.read_csv("data/products.csv")

# 2) Normalizar nombres de columnas
clients.columns  = clients.columns.str.lower().str.strip()
orders.columns   = orders.columns.str.lower().str.strip()
products.columns = products.columns.str.lower().str.strip()

# 3) Diagnóstico inicial
def missing_table(df, name):
    print(f"\n===== MISSING VALUES: {name} =====")
    print((df.isnull().mean() * 100).rename("missing_%"))

print("\n===== DESCRIBE CLIENTS =====")
print(clients.describe(include="all"))

print("\n===== DESCRIBE ORDERS =====")
print(orders.describe(include="all"))

print("\n===== DESCRIBE PRODUCTS =====")
print(products.describe(include="all"))

missing_table(clients, "CLIENTS")
missing_table(orders, "ORDERS")
missing_table(products, "PRODUCTS")

# 4) Eliminar duplicados
clients = clients.drop_duplicates()
orders = orders.drop_duplicates()
products = products.drop_duplicates()


# 5) Procesar fechas
orders["order_timestamp"] = pd.to_datetime(orders["order_timestamp"], errors="coerce")
orders["order_date"]      = orders["order_timestamp"].dt.date
orders["order_month"]     = orders["order_timestamp"].dt.month
orders["order_year"]      = orders["order_timestamp"].dt.year

# 6) Missing values

for col in ["order_price", "shipping_cost"]:
    if col in orders.columns:
        orders[col] = orders[col].fillna(orders[col].mean())

clients["country"] = clients["country"].fillna("Unknown")

# 7) Filtrar datos por semestres (2024)
orders_first_sem_2024 = orders[
    (orders["order_year"] == 2024) &
    (orders["order_month"].between(1, 6))
]

orders_second_sem_2024 = orders[
    (orders["order_year"] == 2024) &
    (orders["order_month"].between(7, 12))
]

print("\n--- FILTRO POR SEMESTRES (2024) ---")
print("Primer semestre:", orders_first_sem_2024.shape)
print("Segundo semestre:", orders_second_sem_2024.shape)

# 8) Cálculo de RFM usando primer semestre 2024
reference_date = pd.to_datetime("2024-06-30")

# --- RECENCY ---
recency_df = (
    orders_first_sem_2024.groupby("client_id")["order_timestamp"]
    .max()
    .reset_index()
)
recency_df["recency"] = (reference_date - recency_df["order_timestamp"]).dt.days

# --- FREQUENCY ---
frequency_df = (
    orders_first_sem_2024.groupby("client_id")["order_id"]
    .count()
    .reset_index()
)
frequency_df.columns = ["client_id", "frequency"]

# --- MONETARY ---
monetary_df = (
    orders_first_sem_2024.groupby("client_id")["order_price"]
    .sum()
    .reset_index()
)
monetary_df.columns = ["client_id", "monetary"]

# --- MERGE ---
rfm = recency_df.merge(frequency_df, on="client_id", how="left")
rfm = rfm.merge(monetary_df, on="client_id", how="left")

clients = clients.merge(rfm, on="client_id", how="left")
clients[["recency", "frequency", "monetary"]] = clients[["recency", "frequency", "monetary"]].fillna(0)

print("\n===== RFM PREVIEW =====")
print(clients[["client_id", "recency", "frequency", "monetary"]].head())

# 9) CHURN (cliente que NO compró en el segundo semestre)

freq_s1 = (
    orders_first_sem_2024.groupby("client_id")["order_id"]
    .count()
    .rename("freq_s1")
)

freq_s2 = (
    orders_second_sem_2024.groupby("client_id")["order_id"]
    .count()
    .rename("freq_s2")
)

clients = clients.merge(freq_s1, left_on="client_id", right_index=True, how="left")
clients = clients.merge(freq_s2, left_on="client_id", right_index=True, how="left")

clients["freq_s1"] = clients["freq_s1"].fillna(0)
clients["freq_s2"] = clients["freq_s2"].fillna(0)

# CHURN = 1 si NO compró en segundo semestre
clients["churn"] = (clients["freq_s2"] == 0).astype(int)

print("\nDistribución de CHURN:")
print(clients["churn"].value_counts())

# 10) Segmentación RFM (etiquetas y ranks)

# --- Etiquetas por cuartiles ---
clients["recency_segment"] = pd.qcut(
    clients["recency"], 4,
    labels=["1 - Muy reciente", "2 - Reciente", "3 - Tardío", "4 - Muy tardío"]
)

clients["frequency_segment"] = pd.qcut(
    clients["frequency"].rank(method="first"), 4,
    labels=["1 - Muy frecuente", "2 - Frecuente", "3 - Ocasional", "4 - Rara vez"]
)

clients["spender_segment"] = pd.qcut(
    clients["monetary"].rank(method="first"), 4,
    labels=["1 - Alto gasto", "2 - Medio-alto", "3 - Medio", "4 - Bajo gasto"]
)

# --- Rank numérico ---
clients["R_rank"] = pd.qcut(clients["recency"], 4, labels=[4,3,2,1]).astype(int)
clients["F_rank"] = pd.qcut(clients["frequency"], 4, labels=[1,2,3,4]).astype(int)
clients["M_rank"] = pd.qcut(clients["monetary"], 4, labels=[1,2,3,4]).astype(int)

clients["RFM_score"] = clients["R_rank"] + clients["F_rank"] + clients["M_rank"]

# Categoría final de riesgo
def churn_risk(score):
    if score <= 5:  return "Alto riesgo"
    if score <= 7:  return "Riesgo medio"
    return "Bajo riesgo"

clients["churn_risk_segment"] = clients["RFM_score"].apply(churn_risk)

print("\n=== SEGMENTACIONES CREADAS ===")
print(clients[
    ["client_id", "recency", "frequency", "monetary",
     "recency_segment", "frequency_segment", "spender_segment",
     "RFM_score", "churn_risk_segment"]
].head())

# 11) Exportar datos finales
clients.to_csv("data/clients_clean.csv", index=False)
orders.to_csv("data/orders_clean.csv", index=False)
products.to_csv("data/products_clean.csv", index=False)

print("\nARCHIVOS GENERADOS CORRECTAMENTE.")
print("Clientes:", clients.shape)
print("Orders:", orders.shape)
print("Products:", products.shape)

# Verificación de fechas
print("\nFechas primer semestre:",
      orders_first_sem_2024["order_date"].min(),
      orders_first_sem_2024["order_date"].max())

print("Fechas segundo semestre:",
      orders_second_sem_2024["order_date"].min(),
      orders_second_sem_2024["order_date"].max())

print("Columnas finales:", clients.columns.tolist())
