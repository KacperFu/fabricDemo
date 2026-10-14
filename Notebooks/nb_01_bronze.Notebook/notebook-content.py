# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d7e6b174-b973-4526-b2b3-bfd5d1c5b29b",
# META       "default_lakehouse_name": "lh_sales",
# META       "default_lakehouse_workspace_id": "b1104717-e924-476e-b65a-02af81596bcf",
# META       "known_lakehouses": [
# META         {
# META           "id": "d7e6b174-b973-4526-b2b3-bfd5d1c5b29b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# nb_01_bronze – notatnik wzorcowy (M2/M4, wdrażany na Prod w M9)
# Domyślny lakehouse notatnika: lh_sales. Wymaga plików z nb_00_pobierz_dane (Files/contoso).
# Źródło: plan szkolenia, sekcja M7 „Notatnik wzorcowy nb_01_bronze”.
# Wynik: sześć tabel bronze – surowe kopie plików, bez zmian nazw i typów.

# %% komórka 1
TABLES = {"orders": "bronze_orders", "orderrows": "bronze_orderrows", "customer": "bronze_customers",
          "product": "bronze_products", "store": "bronze_stores", "date": "bronze_dates"}

for name, table in TABLES.items():
    df = spark.read.parquet(f"Files/contoso/{name}.parquet")  # Parquet niesie schemat – bez inferSchema
    (df.write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(table))

# %% komórka 2 – kontrola (dopisana do testów)
for table in TABLES.values():
    print(f"{table:<17} {spark.table(table).count()}")
# oczekiwane: 98924485, 237245485, 2099808, 2517, 74, 4018

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
