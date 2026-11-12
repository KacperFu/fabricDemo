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

# nb_02_silver – warstwa silver Contoso w lakehousie (Spark). Szkolenie 1-dniowe F4, blok Medalion.
# Domyślny lakehouse notatnika: lh_sales. Wymaga tabel z nb_01_bronze.
# Gold NIE powstaje tutaj: liczy go T-SQL w Warehouse wh_gold, plik 03_gold_warehouse.sql (bronze i silver w lakehousie,
# gold w Warehouse – tak jak na slajdzie med-decyzje). Wersja z gold w lakehousie: kod/dzien1/nb_02_silver_gold.py (Cognity).
# Liczby policzone lokalnie na plikach Contoso 100M (12.10.2026); przebieg w Fabric do próby.

# CELL ********************

# komórka 1 – silver (z planu)
from pyspark.sql import functions as F

orders = spark.table("bronze_orders")
rows = spark.table("bronze_orderrows")

silver = (rows.join(orders, "OrderKey")                    # pozycja + nagłówek zamówienia
    .filter(F.col("Quantity") > 0)
    .withColumnRenamed("RowNumber", "LineNumber")
    .withColumn("OrderDate", F.to_date("DT"))              # w źródle znacznik czasu o północy
    .withColumn("DeliveryDate", F.to_date("DeliveryDate"))
    .drop("DT"))
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_sales")
print(rows.count(), spark.table("silver_sales").count())  # ile zgubił silver? tu: 237245485 237245485

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
