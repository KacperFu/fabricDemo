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

# nb_00_pobierz_dane – ćwiczenie 2.1 (M2), wdrażany na Prod w M9
# Domyślny lakehouse notatnika: lh_sales.
# Pobiera zestaw Contoso 100M (SQLBI, licencja MIT) z GitHub i rozpakowuje go do Files/contoso.
# 12 części po 500 MB, razem 6,1 GB; po rozpakowaniu sześć plików Parquet, razem 3,8 GB.
# Drugie uruchomienie nic nie pobiera – notatnik sprawdza, czy pliki już są.
# Kod dopisany. Rozpakowanie sprawdzone lokalnie, przebieg w Fabric do próby.

# %% komórka 1 – biblioteka do archiwów 7z (instaluje się w sesji notatnika)
%pip install py7zr

# %% komórka 2 – pobranie i rozpakowanie
import os, shutil, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
import py7zr

BASE = "https://github.com/sql-bi/Contoso-Data-Generator-V2-Data/releases/download/ready-to-use-data/"
PARTS = [f"parquet-100m.7z.{i:03d}" for i in range(1, 13)]
FILES = ["orders", "orderrows", "customer", "product", "store", "date"]  # bez sales i currencyexchange
WORK = "/tmp/contoso"                            # dysk lokalny sterownika Sparka
TARGET = "/lakehouse/default/Files/contoso"      # ten sam folder co Files/contoso w lakehousie

if all(os.path.exists(f"{TARGET}/{n}.parquet") for n in FILES):
    print("Pliki już są w Files/contoso – nic nie pobieram.")
else:
    start = time.time()
    os.makedirs(WORK, exist_ok=True)
    free = shutil.disk_usage(WORK).free / 1e9
    assert free > 12, f"Na dysku lokalnym jest {free:.0f} GB, potrzeba 12 GB – zmień WORK"

    def pobierz(part):
        urllib.request.urlretrieve(BASE + part, f"{WORK}/{part}")

    with ThreadPoolExecutor(4) as pool:              # 4 części naraz
        list(pool.map(pobierz, PARTS))
    print(f"pobrane: {time.time() - start:.0f} s")

    archive = f"{WORK}/all.7z"                       # części to pocięty jeden plik – sklejamy je po kolei
    with open(archive, "wb") as out:
        for part in PARTS:
            with open(f"{WORK}/{part}", "rb") as src:
                shutil.copyfileobj(src, out, 16 * 1024 * 1024)
            os.remove(f"{WORK}/{part}")

    with py7zr.SevenZipFile(archive) as z:
        z.extract(path=f"{WORK}/x", targets=[f"{n}.parquet" for n in FILES])
    os.remove(archive)
    print(f"rozpakowane: {time.time() - start:.0f} s")

    os.makedirs(TARGET, exist_ok=True)
    for n in FILES:
        shutil.copyfile(f"{WORK}/x/{n}.parquet", f"{TARGET}/{n}.parquet")
    shutil.rmtree(WORK)
    print(f"gotowe: {time.time() - start:.0f} s")

# %% komórka 3 – kontrola
for n in FILES:
    print(f"{n:<10} {os.path.getsize(f'{TARGET}/{n}.parquet') / 1e6:>8.1f} MB")
# oczekiwane: orders 724.3, orderrows 2906.4, customer 142.6, product 0.1, store 0.0, date 0.0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
