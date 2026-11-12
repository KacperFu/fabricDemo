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

# nb_python_zamiast_spark – demo: ta sama transformacja w PySpark i w czystym Pythonie (DuckDB + delta-rs)
# Język notatnika: Python. Domyślny lakehouse: ten z tabelą silver_sales w schemacie dbo (nazwa dowolna).
# Na szkoleniu F4 gold jest w Warehouse (03_gold_warehouse.sql), więc notatnik czyta silver z lakehouse'u i sam liczy przychód.
# Przekaz: na F2 jedna sesja Sparka (starter pool: sterownik + węzeł 8 vCore = 16 vCore) zajmuje prawie całe
# 20 vCore z burstem; sesja Python ma 2 vCore (1 CU), więc kilka zadań idzie obok siebie.
# Learn (wybór kernela): Python dla danych do ok. 1 GB po kompresji; powyżej Spark (choćby jednowęzłowy).
# Czego Python nie umie przy zapisie Delta: deletion vectors (domyślne w Runtime 2.0), V-Order, pełny VACUUM.
# W projekcie ciężka praca (237 mln wierszy) jest w Warehouse; Python robi lekkie zadania i kontrolę (decyzja D17).
# Gotowy fragment zapisu do Delta w notatniku: Edit → Browse code snippet → Write data to Delta table.

# CELL ********************

# komórka 0 – instalacja DuckDB 
# W notatniku Python w Fabric DuckDB nie zawsze jest w środowisku, a bez niego komórka 2 kończy się ModuleNotFoundError.
%pip install duckdb --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#komórka 1 – dla porównania: wersja PySpark
# from pyspark.sql import functions as F
# df = (spark.table("silver_sales")
#       .filter(F.col("OrderDate") >= "2025-12-01")
#       .withColumn("Revenue", F.round(F.col("Quantity") * F.col("NetPrice"), 2))
#       .groupBy("OrderDate", "StoreKey")
#       .agg(F.sum("Revenue").alias("Revenue"), F.count("*").alias("Lines")))
# df.write.mode("overwrite").saveAsTable("spark_sales_daily_store")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# komórka 2 – to samo w DuckDB: aktualne pliki tabeli z logu Delta, DuckDB czyta je jako parquet
# Dwie prostsze drogi zawodzą w notatniku Python w Fabric i obie są lekcją:
#   * delta_scan() w DuckDB: rozszerzenie delta pobiera się z internetu przy INSTALL, a INSTALL wisiał bez końca.
#   * deltalake (delta-rs): „reader features {'deletionVectors'} not yet supported”. Tabele ze Sparka w Fabric mają
#     wektory usunięć włączone domyślnie, a czytnik delta-rs w tym środowisku ich nie obsługuje.
# Dlatego czytamy log Delta sami: plik jest aktualny, jeśli został dodany (add) i nie usunięty (remove). Jeśli któryś
# aktualny plik ma wektor usunięć, przerywamy, bo sam parquet policzyłby też usunięte wiersze. Wtedy zostaje Spark
# albo SQL analytics endpoint. Tabela silver_sales powstała przez overwrite, więc wektorów nie powinna mieć.
import json
import os
import re
import time
import urllib.parse
import duckdb
import notebookutils
import pyarrow.parquet as pq

# Adres zapisu bierzemy z kontekstu notatnika (ID workspace'u i domyślnego lakehouse'u), a nie z nazwy wpisanej na
# sztywno: przy innej nazwie lakehouse'u zapis w komórce 3 kończył się błędem ArtifactNotFound.
ctx = notebookutils.runtime.context
base = f"abfss://{ctx['currentWorkspaceId']}@onelake.dfs.fabric.microsoft.com/{ctx['defaultLakehouseId']}/Tables/dbo"
print("lakehouse:", ctx.get("defaultLakehouseName"), "→", base)
src = "/lakehouse/default/Tables/dbo/silver_sales"      # domyślny lakehouse jest podmontowany lokalnie


def pliki_delta(tabela):
    """Aktualne pliki parquet tabeli Delta: ostatni checkpoint + późniejsze commity (add minus remove)."""
    log = os.path.join(tabela, "_delta_log")
    pliki, od = {}, 0
    if os.path.exists(os.path.join(log, "_last_checkpoint")):
        info = json.load(open(os.path.join(log, "_last_checkpoint")))
        v, n = info["version"], info.get("parts")
        nazwy = [f"{v:020d}.checkpoint.parquet"] if not n else \
                [f"{v:020d}.checkpoint.{i:010d}.{n:010d}.parquet" for i in range(1, n + 1)]
        for nazwa in nazwy:
            for a in pq.read_table(os.path.join(log, nazwa), columns=["add"]).column("add").to_pylist():
                if a:
                    pliki[a["path"]] = a.get("deletionVector")
        od = v + 1
    for f in sorted(f for f in os.listdir(log) if re.fullmatch(r"\d{20}\.json", f) and int(f[:20]) >= od):
        for linia in open(os.path.join(log, f)):
            akcja = json.loads(linia)
            if "add" in akcja:
                pliki[akcja["add"]["path"]] = akcja["add"].get("deletionVector")
            if "remove" in akcja:
                pliki.pop(akcja["remove"]["path"], None)
    z_wektorem = [k for k, dv in pliki.items() if dv]
    if z_wektorem:
        raise RuntimeError(f"{len(z_wektorem)} z {len(pliki)} plików ma wektory usunięć: czytaj tę tabelę Sparkiem albo przez SQL endpoint")
    return [os.path.join(tabela, urllib.parse.unquote(k)) for k in pliki]


t = time.time()
pliki = pliki_delta(src)
fakt = duckdb.read_parquet(pliki)                       # parquet jest wbudowany w DuckDB, bez INSTALL
wynik = duckdb.sql("""
    SELECT OrderDate, StoreKey, SUM(ROUND(Quantity * NetPrice, 2)) AS Revenue, COUNT(*) AS Lines   -- przychód jak w gold
    FROM fakt
    WHERE OrderDate >= DATE '2025-12-01'
    GROUP BY ALL
    ORDER BY ALL
""").arrow()
daily = wynik.read_all() if hasattr(wynik, "read_all") else wynik   # nowsza DuckDB zwraca strumień, starsza tabelę
print(len(pliki), "plików,", daily.num_rows, "wierszy,", round(time.time() - t, 1), "s")
# oczekiwane (liczone wcześniej na gold_fact_sales, ta sama logika): 1 798 wierszy (dzień × sklep w grudniu 2025), suma Revenue 3 228 753 092,41, Lines 4 339 953

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# komórka 3 – zapis tabeli Delta przez delta-rs (bez Sparka)
from deltalake import write_deltalake, DeltaTable

opts = {"bearer_token": notebookutils.credentials.getToken("storage"), "use_fabric_endpoint": "true"}
target = f"{base}/py_sales_daily_store"
write_deltalake(target, daily, mode="overwrite", storage_options=opts)
print(DeltaTable(target, storage_options=opts).version())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# komórka 4 – zanim przepiszesz notatnik Sparka na Pythona: sprawdź protokół tabeli docelowej
# Jeśli writer_features zawiera deletionVectors (albo inne nieobsługiwane funkcje), delta-rs nie zapisze do tej tabeli.
for name in ["silver_sales", "py_sales_daily_store"]:
    try:
        dt = DeltaTable(f"{base}/{name}", storage_options=opts)
        print(name, dt.protocol(), dt.metadata().configuration)
    except Exception as e:                              
        print(name, "→", type(e).__name__, str(e)[:200])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# komórka 5 – utrzymanie tabel pisanych Pythonem (nikt nie zrobi tego za was)
dt = DeltaTable(target, storage_options=opts)
print(dt.optimize.compact())                              # łączy małe pliki
print(dt.vacuum(retention_hours=168, dry_run=True))       # lista plików do usunięcia po 7 dniach; dry_run = nic nie kasuje

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
