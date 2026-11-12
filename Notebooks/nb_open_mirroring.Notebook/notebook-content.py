# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
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

# nb_open_mirroring – pliki do strefy lądowania open mirroringu (demo Mirroring, slajd mir-open)
# Język notatnika: Python (nie PySpark). Lakehouse niepotrzebny.
# Kiedy: na próbie T−7 oba kroki; T−1 na świeżym mirrorze tylko krok 1. Na szkoleniu krok 2 na żywo (ok. 10:10).
# Tabela: zwroty – zwroty pozycji zamówień z demo.orders w Azure SQL. Udaje źródło spoza listy mirroringu
# (np. PostgreSQL w AWS albo system zwrotów u partnera), które samo zapisuje zmiany do OneLake.
# #
# Przed uruchomieniem, w Fabric:
#   1. Workspace demo → New item → Mirrored database (open mirroring) → nazwa mir_open.
#   2. Na stronie mirrora skopiuj adres strefy lądowania (Landing zone URL) i wklej do p_strefa w komórce parametrów.
# Krok 1 zapisuje _metadata.json i pierwszy plik (pierwsza kopia: 5 wierszy, bez __rowMarker__ = same wstawienia).
# Krok 2 zapisuje drugi plik ze zmianami: 1 zmień, 2 usuń, 0 wstaw. Po kilku sekundach tabela w mirrorze ma 5 wierszy.
# Zasady (Learn, open mirroring landing zone): folder na tabelę, _metadata.json z keyColumns, pliki Parquet
# numerowane kolejno 20 cyframi, __rowMarker__ ostatnią kolumną, wiersz zmieniany niesie wszystkie kolumny.
# Nieprzetestowane w Fabric – sprawdź na próbie.


# PARAMETERS CELL ********************

# parametry (oznacz tę komórkę: Toggle parameter cell)
p_strefa = "https://onelake.dfs.fabric.microsoft.com/aa37d6f8-5522-46d1-af05-8ced52a78257/4966b5ea-1da8-47e1-aa6d-31fbd75ba7f4/Files/LandingZone"  # Landing zone URL ze strony mir_open (https albo abfss)
p_tabela = "zwroty"
p_krok = 2                           # 1 = metadane + pierwsza kopia, 2 = plik ze zmianami

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 1. dane: pierwsza kopia i zmiany
import datetime as dt
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import notebookutils

schemat = pa.schema([
    ("return_id", pa.int32()),
    ("order_id", pa.int32()),
    ("line_id", pa.int32()),
    ("return_date", pa.date32()),
    ("quantity", pa.int32()),
    ("reason", pa.string()),
])
d = dt.date
pierwsza_kopia = [
    (1, 90002, 1, d(2026, 1, 10), 1, "uszkodzony w transporcie"),
    (2, 90002, 1, d(2026, 1, 12), 1, "zły rozmiar"),
    (3, 90000, 1, d(2026, 1, 15), 1, "rezygnacja"),
    (4, 90002, 1, d(2026, 1, 20), 1, "zły kolor"),
    (5, 90002, 1, d(2026, 1, 22), 1, "rezygnacja"),
]
zmiany = [  # (wiersz, __rowMarker__): 0 wstaw, 1 zmień, 2 usuń, 4 wstaw albo zmień
    ((2, 90002, 1, d(2026, 1, 12), 1, "zły rozmiar – wymiana"), 1),
    ((3, 90000, 1, d(2026, 1, 15), 1, "rezygnacja"), 2),
    ((6, 90001, 1, d(2026, 2, 2), 1, "uszkodzony w transporcie"), 0),
]

def tabela(wiersze, znaczniki=None):
    kolumny = {pole.name: [w[i] for w in wiersze] for i, pole in enumerate(schemat)}
    t = pa.table(kolumny, schema=schemat)
    if znaczniki is not None:                       # __rowMarker__ zawsze ostatnią kolumną
        t = t.append_column("__rowMarker__", pa.array(znaczniki, pa.int32()))
    return t

# Zapis wprost przez API OneLake (to samo co ADLS Gen2: utwórz plik, dopisz bajty, zatwierdź).
# Zmiana 12.11.2026: notebookutils.fs.cp z /tmp do strefy lądowania kończył się błędem „azcopy not exit 0”.
def https(sciezka):
    """abfss://<ws>@onelake.dfs.fabric.microsoft.com/<reszta> albo adres https z portalu → adres https."""
    if sciezka.startswith("abfss://"):
        ws, _, reszta = sciezka[len("abfss://"):].partition("@onelake.dfs.fabric.microsoft.com/")
        return f"https://onelake.dfs.fabric.microsoft.com/{ws}/{reszta}"
    return sciezka

def wyslij(bajty, nazwa):
    url = https(f"{p_strefa.rstrip('/')}/{p_tabela}/{nazwa}")
    h = {"Authorization": f"Bearer {notebookutils.credentials.getToken('storage')}"}
    for metoda, parametry, dane in [("put", "resource=file", None),
                                    ("patch", "action=append&position=0", bajty),
                                    ("patch", f"action=flush&position={len(bajty)}", None)]:
        r = requests.request(metoda, f"{url}?{parametry}", headers=h, data=dane)
        if r.status_code >= 300:
            raise RuntimeError(f"{nazwa}: {metoda.upper()} {parametry} → HTTP {r.status_code} {r.text[:300]}")
    print("zapisano", url, len(bajty), "B")

def zapisz(t, nazwa):
    bufor = pa.BufferOutputStream()
    pq.write_table(t, bufor)
    wyslij(bufor.getvalue().to_pybytes(), nazwa)
    print("  ", t.num_rows, "wierszy")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 2. zapis do strefy lądowania
folder = f"{p_strefa}/{p_tabela}"
if p_krok == 1:
    # _metadata.json: bez keyColumns nie ma UPDATE ani DELETE, a raz ustawionych kluczy nie zmienisz
    wyslij(json.dumps({"keyColumns": ["return_id"]}).encode("utf-8"), "_metadata.json")
    zapisz(tabela(pierwsza_kopia), f"{1:020d}.parquet")
elif p_krok == 2:
    zapisz(tabela([w for w, _ in zmiany], [z for _, z in zmiany]), f"{2:020d}.parquet")
else:
    raise ValueError("p_krok: 1 albo 2")

try:
    for plik in notebookutils.fs.ls(folder):
        print(plik.name, plik.size)
except Exception as e:                              # sam podgląd; pliki są już zapisane
    print("podgląd folderu niedostępny:", str(e)[:200], "– sprawdź pliki na stronie mir_open")
# oczekiwane po kroku 1: _metadata.json, 00000000000000000001.parquet
# oczekiwane po kroku 2: dodatkowo 00000000000000000002.parquet (przetworzone pliki znikają po 7 dniach)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 3. kontrola w SQL analytics endpoint mirrora mir_open (T-SQL, uruchom tam, nie tutaj)
# SELECT return_id, order_id, return_date, reason FROM dbo.zwroty ORDER BY return_id;
# oczekiwane po kroku 1: 5 wierszy (1–5)
# oczekiwane po kroku 2: 5 wierszy – 1, 2 (powód „zły rozmiar – wymiana”), 4, 5, 6; nie ma 3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

for f in notebookutils.fs.ls(p_strefa + "/zwroty"): print(f.name, f.size)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
