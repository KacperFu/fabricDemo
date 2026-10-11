# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3dea15d8-bff8-426a-b1e6-9c8c1349e34c",
# META       "default_lakehouse_name": "lh_demo",
# META       "default_lakehouse_workspace_id": "5b27cda4-905d-45b2-aecb-c0634805362f",
# META       "known_lakehouses": [
# META         {
# META           "id": "3dea15d8-bff8-426a-b1e6-9c8c1349e34c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


# Domyślny lakehouse notatnika: lh_sales.
# Źródło: plan szkolenia, sekcja M7 „Notatnik wzorcowy nb_01_bronze”.
# Wynik: tabele bronze_sales i bronze_products.

# %% komórka 1
import os, urllib.request

BASE = "https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/"
for name in ["sales", "products"]:
    folder = f"/lakehouse/default/Files/bronze/{name}"
    os.makedirs(folder, exist_ok=True)
    path = f"{folder}/{name}.csv"
    if not os.path.exists(path):  # w Dev pliki wgrano w ćw. 2.1, w Prod pobieramy je z GitHub
        urllib.request.urlretrieve(BASE + name + ".csv", path)
    df = (spark.read.option("header", "true").option("inferSchema", "true")
          .csv(f"Files/bronze/{name}/{name}.csv"))
    (df.write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"bronze_{name}"))

# %% komórka 2 – kontrola (dopisana do testów)
print(spark.table("bronze_sales").count(), spark.table("bronze_products").count())  # oczekiwane: 32718 295


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# nb_02_silver_gold – ćwiczenie 7.1 (M7), wdrażany na Prod w M9
# Domyślny lakehouse notatnika: lh_sales. Wymaga tabel z nb_01_bronze.
# Komórka 1 (silver) pochodzi z planu szkolenia i slajdu m7-cw71.
# Komórki 2–6 (gold) są DOPISANE: plan i slajd m7-cw71b opisują gold tylko słownie.
# Porównaj je ze swoim repo wzorcowym przed szkoleniem.

# %% komórka 1 – silver (z planu)
from pyspark.sql import functions as F, Window

silver = (spark.table("bronze_sales")
    .dropDuplicates(["SalesOrderNumber", "SalesOrderLineNumber"])
    .filter(F.col("Quantity") > 0)
    .withColumn("CustomerName", F.trim("CustomerName"))  # bez initcap: zepsułby McDonald
    .withColumn("EmailAddress", F.lower(F.trim("EmailAddress")))
    .withColumn("OrderDate", F.to_date("OrderDate"))
    .fillna({"TaxAmount": 0}))
silver.write.format("delta").mode("overwrite").saveAsTable("silver_sales")
print(spark.table("bronze_sales").count(), silver.count())  # ile usunął silver? tu: 32718 32718

dim_customer = (silver.select("EmailAddress", "CustomerName").distinct()  # SCD typ 1
    .withColumn("CustomerKey", F.row_number().over(Window.orderBy("EmailAddress"))))

# %% komórka 2 – gold_dim_customer (dopisane)
dim_customer.write.format("delta").mode("overwrite").saveAsTable("gold_dim_customer")

# %% komórka 3 – gold_dim_product (dopisane)
products = spark.table("bronze_products")
dup = products.groupBy("ProductName").count().filter("count > 1").count()
assert dup == 0, f"{dup} nazw produktów się powtarza – join Item = ProductName zdublowałby wiersze"
dim_product = products.select("ProductID", "ProductName", "Category", "ListPrice")
dim_product.write.format("delta").mode("overwrite").saveAsTable("gold_dim_product")

# %% komórka 4 – gold_dim_date (dopisane; podpowiedź ze slajdu m7-cw71b)
dim_date = (spark.sql("""
        SELECT explode(sequence(min(OrderDate), max(OrderDate))) AS Date
        FROM silver_sales""")
    .withColumn("Year", F.year("Date"))
    .withColumn("Quarter", F.quarter("Date"))
    .withColumn("Month", F.month("Date"))
    .withColumn("DayName", F.date_format("Date", "EEEE")))
dim_date.write.format("delta").mode("overwrite").saveAsTable("gold_dim_date")

# %% komórka 5 – gold_fact_sales (dopisane)
# Ziarno: jeden wiersz na pozycję zamówienia (SalesOrderNumber + SalesOrderLineNumber).
# Sprzedaż nie ma ProductID, więc produkt łączymy po nazwie: Item = ProductName.
fact = (silver
    .join(dim_customer.select("EmailAddress", "CustomerKey"), "EmailAddress", "left")
    .join(dim_product.select("ProductID", F.col("ProductName").alias("Item")), "Item", "left")
    .withColumn("Revenue", F.col("Quantity") * F.col("UnitPrice"))
    .select("SalesOrderNumber", "SalesOrderLineNumber", "CustomerKey", "ProductID",
            "OrderDate", "Quantity", "UnitPrice", "TaxAmount", "Revenue"))
fact.write.format("delta").mode("overwrite").saveAsTable("gold_fact_sales")

# %% komórka 6 – kontrola (dopisane)
n_silver = spark.table("silver_sales").count()
fact = spark.table("gold_fact_sales")
n_null = fact.filter(F.col("CustomerKey").isNull() | F.col("ProductID").isNull()).count()
print(n_silver, fact.count(), n_null)  # oczekiwane: 32718 32718 0
print(spark.table("gold_dim_customer").count(), spark.table("gold_dim_product").count(),
      spark.table("gold_dim_date").count())  # oczekiwane: 12427 295 915


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Testowa komorka dla gita

# CELL ********************

from pyspark.sql import functions as F

df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("Files/bronze/sales/sales.csv"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


display(df.limit(20))
df.printSchema()
print(df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType, DateType)

schema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerName", StringType()),
    StructField("EmailAddress", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", DoubleType()),
    StructField("TaxAmount", DoubleType()),
])
df_schema = (spark.read.format("csv").option("header", "true")
             .schema(schema).load("Files/bronze/sales/sales.csv"))
df_schema.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
