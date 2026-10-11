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

# komórka 1 – przygotowanie: schemat, sprzątanie po poprzednim pokazie, dwie funkcje pomocnicze
SCHEMA = "demo_delta"
T = f"{SCHEMA}.zamowienia"
TABELE = ["zamowienia", "zamowienia_part", "zamowienia_part2"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
for t in TABELE:
    spark.sql(f"DROP TABLE IF EXISTS {SCHEMA}.{t}")

def pliki(tabela):
    """Pokaż, co leży w folderze tabeli: pliki danych i wpisy dziennika."""
    def walk(path, prefix=""):
        for f in notebookutils.fs.ls(path):
            name = f.name.rstrip("/")
            is_dir = f.isDir() if callable(f.isDir) else f.isDir
            if is_dir or (not f.size and "." not in name):       # folder: _delta_log albo Category=…
                yield from walk(f"{path}/{name}", f"{prefix}{name}/")
            else:
                yield prefix + name, f.size
    wszystkie = list(walk(f"Tables/{SCHEMA}/{tabela}"))
    dane = sorted((p, s) for p, s in wszystkie if not p.startswith("_delta_log/"))
    log = [p for p, s in wszystkie if p.startswith("_delta_log/") and p.endswith(".json") and p.count("/") == 1]
    for p, s in dane:
        folder, _, name = p.rpartition("/")
        print(f"{s:>7} B  {folder + '/' if folder else ''}{name[:22]}…{name[-8:]}")
    print(f"→ pliki danych: {len(dane)} · wpisy dziennika: {len(log)}")

def historia(tabela):
    display(spark.sql(f"DESCRIBE HISTORY {SCHEMA}.{tabela}")
            .select("version", "timestamp", "operation", "operationMetrics"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# komórka 2 – pierwsza tabela Delta: sześć zamówień
dane = [(1, "Mountain-100", "Rowery",    "Europa",  2, "nowe"),
        (2, "Road-250",     "Rowery",    "Ameryka", 1, "nowe"),
        (3, "Kask Sport",   "Kaski",     "Europa",  4, "nowe"),
        (4, "Bidon",        "Akcesoria", "Europa",  6, "nowe"),
        (5, "Touring-1000", "Rowery",    "Europa",  1, "nowe"),
        (6, "Pompka",       "Akcesoria", "Ameryka", 3, "nowe")]
kolumny = ["OrderID", "Item", "Category", "Region", "Quantity", "Status"]

spark.createDataFrame(dane, kolumny).write.format("delta").mode("overwrite").saveAsTable(T)
display(spark.table(T).orderBy("OrderID"))
pliki("zamowienia")          # pliki Parquet w folderze tabeli i pierwszy wpis w _delta_log
historia("zamowienia")       # wersja 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# komórka 3 – INSERT: nowe pliki i nowy wpis w dzienniku, stare pliki bez zmian
spark.sql(f"""INSERT INTO {T} VALUES
    (7, 'Road-650',   'Rowery', 'Ameryka', 1, 'nowe'),
    (8, 'Kask Junior', 'Kaski', 'Europa',  2, 'nowe')""")
pliki("zamowienia")
historia("zamowienia")       # wersja 1: WRITE

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# komórka 4 – UPDATE: Delta niczego nie poprawia w miejscu
spark.sql(f"UPDATE {T} SET Status = 'wysłane' WHERE OrderID = 1")
pliki("zamowienia")          # plików przybyło: stary plik zostaje, dziennik oznacza go jako nieaktualny

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
