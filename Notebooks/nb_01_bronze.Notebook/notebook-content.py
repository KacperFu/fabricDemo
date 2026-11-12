# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "54c4e96c-590e-48c4-adad-42ee7f249f24",
# META       "default_lakehouse_name": "lh_sales",
# META       "default_lakehouse_workspace_id": "aa37d6f8-5522-46d1-af05-8ced52a78257",
# META       "known_lakehouses": [
# META         {
# META           "id": "54c4e96c-590e-48c4-adad-42ee7f249f24"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# nb_01_bronze – notatnik wzorcowy (M2/M4, wdrażany na Prod w M9)
# Domyślny lakehouse notatnika: lh_sales. Wymaga plików z nb_00_pobierz_dane (Files/contoso).
# Źródło: plan szkolenia, sekcja M7 „Notatnik wzorcowy nb_01_bronze”.
# Wynik: sześć tabel bronze – surowe kopie plików, bez zmian nazw i typów.

# CELL ********************

# komórka 1
TABLES = {"orders": "bronze_orders", "orderrows": "bronze_orderrows", "customer": "bronze_customers",
          "product": "bronze_products", "store": "bronze_stores", "date": "bronze_dates"}

for name, table in TABLES.items():
    df = spark.read.parquet(f"Files/contoso/{name}.parquet")  # Parquet niesie schemat – bez inferSchema
    (df.write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(table))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# komórka 2 – kontrola (dopisana do testów)
for table in TABLES.values():
    print(f"{table:<17} {spark.table(table).count()}")
# oczekiwane: 98924485, 237245485, 2099808, 2517, 74, 4018

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
