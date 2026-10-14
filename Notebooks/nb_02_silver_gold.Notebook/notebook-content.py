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

# nb_02_silver_gold – ćwiczenie 7.1 (M7), wdrażany na Prod w M9
# Domyślny lakehouse notatnika: lh_sales. Wymaga tabel z nb_01_bronze.
# Komórka 1 (silver) pochodzi z planu szkolenia i slajdu m7-cw71.
# Komórki 2–7 (gold) są DOPISANE: plan i slajd m7-cw71b opisują gold tylko słownie.
# Każda komórka gold czyta z tabel, więc da się ją uruchomić osobno.
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

# CELL ********************

# komórka 2 – gold_dim_customer (dopisane)
from pyspark.sql import functions as F

dim_customer = (spark.table("bronze_customers")            # SCD typ 1: jeden wiersz na klienta
    .select("CustomerKey",
            F.concat_ws(" ", "GivenName", "Surname").alias("Name"),
            "Gender", "City",
            F.col("StateFull").alias("State"),
            F.col("CountryFull").alias("Country"),
            "Continent",
            F.to_date("Birthday").alias("Birthday")))
dim_customer.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_customer")
print(spark.table("gold_dim_customer").count())  # oczekiwane: 2099808

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# komórka 3 – gold_dim_product (dopisane)
from pyspark.sql import functions as F

dim_product = (spark.table("bronze_products")
    .select("ProductKey", "ProductCode", "ProductName", "Brand", "Color",
            F.col("CategoryName").alias("Category"),
            F.col("SubCategoryName").alias("Subcategory"),
            F.col("Price").cast("decimal(18,2)").alias("Price")))
dim_product.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_product")
print(spark.table("gold_dim_product").count())  # oczekiwane: 2517

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# komórka 4 – gold_dim_store (dopisane)
from pyspark.sql import functions as F

dim_store = (spark.table("bronze_stores")
    .select("StoreKey",
            F.col("Description").alias("Name"),
            F.col("CountryName").alias("Country"),
            "State",
            F.to_date("OpenDate").alias("OpenDate"),
            F.to_date("CloseDate").alias("CloseDate"),
            F.when(F.trim("Status") == "", None).otherwise(F.col("Status")).alias("Status")))  # '' → NULL
dim_store.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_store")
print(spark.table("gold_dim_store").count())  # oczekiwane: 74 (StoreKey 999999 = sklep internetowy)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# komórka 5 – gold_dim_date (dopisane; kalendarz jest w źródle, nie trzeba go generować)
from pyspark.sql import functions as F

dim_date = (spark.table("bronze_dates")
    .select(F.to_date("Date").alias("Date"),
            "Year",
            F.col("Quarter"),
            F.col("MonthNumber").alias("Month"),
            F.col("Month").alias("MonthName"),
            F.col("DayofWeek").alias("DayName")))
dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_date")
print(spark.table("gold_dim_date").count())  # oczekiwane: 4018 (2016-01-01 – 2026-12-31)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# komórka 6 – gold_fact_sales (dopisane)
# Ziarno: jeden wiersz na pozycję zamówienia (OrderKey + LineNumber).
# Klucze wymiarów są już w źródle, więc fakt to silver z wybranymi kolumnami i przychodem.
# Ceny są w USD niezależnie od CurrencyCode, więc przychodu nie przeliczamy.
from pyspark.sql import functions as F

fact = (spark.table("silver_sales")
    .withColumn("Revenue", F.round(F.col("Quantity") * F.col("NetPrice"), 2).cast("decimal(18,2)"))
    .select("OrderKey", "LineNumber", "OrderDate", "DeliveryDate", "CustomerKey", "StoreKey",
            "ProductKey", "Quantity", "NetPrice", "UnitCost", "Revenue"))
fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_fact_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# komórka 7 – kontrola (dopisane)
from pyspark.sql import functions as F

fact = spark.table("gold_fact_sales")
print(spark.table("silver_sales").count(), fact.count())  # oczekiwane: 237245485 237245485
print(fact.agg(F.sum("Revenue")).first()[0])              # oczekiwane: 232153677248.67
for table, key in [("gold_dim_customer", "CustomerKey"), ("gold_dim_product", "ProductKey"),
                   ("gold_dim_store", "StoreKey"), ("gold_dim_date", "Date")]:
    d = spark.table(table)
    orphans = fact.join(d, fact[key if key != "Date" else "OrderDate"] == d[key], "left_anti").count()
    print(table, d.count(), d.count() - d.select(key).distinct().count(), orphans)
# oczekiwane: liczba wierszy 2099808 / 2517 / 74 / 4018, potem 0 powtórzonych kluczy i 0 wierszy faktu bez wymiaru

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
