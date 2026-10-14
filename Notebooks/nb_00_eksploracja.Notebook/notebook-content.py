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

# MARKDOWN ********************

# Ćwiczenie 3.1 – odczyt i eksploracja zamówień (M3)
# Domyślny lakehouse: lh_sales; pliki Files/contoso/*.parquet z ćw. 2.1 (nb_00_pobierz_dane).
# Komórka 1 – z planu i slajdu m3-cw31. Komórka 2 – rozwiązania zadań (dopisane).
# Liczby policzone lokalnie na plikach Contoso 100M (12.10.2026).

# CELL ********************

# komórka 1
from pyspark.sql import functions as F

df = spark.read.parquet("Files/contoso/orders.parquet")
display(df.limit(20))
df.printSchema()
print(df.count())  # 98924485
df.write.mode("overwrite").saveAsTable("bronze_orders")  # praktyczna część M4

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# komórka 2 – rozwiązania
from functools import reduce

print(df.filter(F.year("DT") == 2020).count())  # zamówienia w 2020 r.: 7104425

rows = spark.read.parquet("Files/contoso/orderrows.parquet")
print(rows.count())  # pozycje zamówień: 237245485
top5 = rows.groupBy("ProductKey").count().orderBy(F.desc("count")).limit(5)
display(top5)  # w wyniku przełącz widok na Chart; ProductKey: 453, 458, 416, 449, 419

missing = df.filter(reduce(lambda a, b: a | b, [F.col(c).isNull() for c in df.columns]))
print(missing.count())  # zamówienia z brakami: 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# demo – CSV nie ma schematu, Parquet ma (skrypt trenera, M3; dopisane)
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DecimalType)

products = spark.read.parquet("Files/contoso/product.parquet")
(products.select("ProductKey", "ProductCode", "ProductName", "CategoryName", "Price")
    .coalesce(1).write.mode("overwrite").option("header", "true").csv("Files/demo/products_csv"))

csv = spark.read.option("header", "true").csv("Files/demo/products_csv")
csv.printSchema()        # wszystko string
csv_infer = spark.read.option("header", "true").option("inferSchema", "true").csv("Files/demo/products_csv")
csv_infer.printSchema()  # ProductCode zgadnięty jako liczba – '0101001' traci zera wiodące

schema = StructType([
    StructField("ProductKey", IntegerType()),
    StructField("ProductCode", StringType()),
    StructField("ProductName", StringType()),
    StructField("CategoryName", StringType()),
    StructField("Price", DecimalType(20, 5)),
])
csv_schema = spark.read.option("header", "true").schema(schema).csv("Files/demo/products_csv")
csv_schema.printSchema()
display(csv_schema.limit(3))  # ProductCode znowu '0101001'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("bronze_products")
df.write.mode("overwrite").saveAsTable("demo_managed")        # tabela zarządzana

EXT = "abfss://WS-KF-DEV@onelake.dfs.fabric.microsoft.com/lh_sales.Lakehouse/Files/demo/external_products"
df.write.mode("overwrite").format("delta").option("path", EXT).saveAsTable("demo_external")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS demo_sql (ProductKey INT, ProductName STRING, Price DECIMAL(20,5)) USING DELTA;
# MAGIC INSERT INTO demo_sql SELECT ProductKey, ProductName, Price FROM bronze_products LIMIT 5;
# MAGIC SELECT * FROM demo_sql;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for f in notebookutils.fs.ls("Tables/dbo/demo_sql/_delta_log"):
    print(f.size, f.name)      # ...000.json = CREATE, ...001.json = INSERT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t in ["demo_managed", "demo_external", "demo_sql"]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
print(notebookutils.fs.exists(EXT))     # True – pliki tabeli zewnętrznej zostały w Files
notebookutils.fs.rm(EXT, True)          # sprzątanie po demo

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
