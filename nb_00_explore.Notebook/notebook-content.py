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

df = spark.table("bronze_products")
df.write.mode("overwrite").saveAsTable("demo_managed")        # tabela zarządzana

EXT = "abfss://Tutoring@onelake.dfs.fabric.microsoft.com/lh_demo.Lakehouse/Files/demo/external_products"
df.write.mode("overwrite").format("delta").option("path", EXT).saveAsTable("demo_external")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS demo_sql (ProductID INT, ProductName STRING, ListPrice DOUBLE) USING DELTA;
# MAGIC INSERT INTO demo_sql SELECT ProductID, ProductName, ListPrice FROM bronze_products LIMIT 5;
# MAGIC SELECT * FROM demo_sql;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for f in notebookutils.fs.ls("Tables/dbo/demo_sql/_delta_log"):
    print(f.size, f.name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t in ["demo_managed", "demo_external", "demo_sql"]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
print(notebookutils.fs.exists(EXT))     # True – pliki tabeli zewnętrznej zostały w Files
notebookutils.fs.rm(EXT, True) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
df = spark.table("bronze_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Testowy MD

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
