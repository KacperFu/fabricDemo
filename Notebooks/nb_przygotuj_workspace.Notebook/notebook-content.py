# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   }
# META }

# PARAMETERS CELL ********************

# nb_przygotuj_workspace – notatnik PYTHON (nie PySpark): pusty workspace → gotowe środowisko demo szkolenia F4.
# Język: Python. Domyślny lakehouse: żaden (notatnik sam zakłada lakehouse'y). Import: Workspace → Import → Notebook.
# Kolejność i to, czego notatnik nie zrobi (SQL, modele, mirrory, Git): 00_przygotowanie/README.md.
#
# Kroki (parametr p_krok):
#   1 "elementy"  – lakehouse'y lh_sales i lh_ml (ze schematami), Warehouse wh_gold i wh_plan, SQL database sqldb_plan,
#                   Eventhouse eh_api z bazą KQL eh_api. Drugie uruchomienie niczego nie dubluje: istniejące pomija.
#   2 "notatniki" – wymaga folderu „przygotowanie” (00_przygotowanie/przygotowanie) wgranego do lh_sales → Files
#                   (Lakehouse → Files → … → Upload → Upload folder); powstaje Files/przygotowanie/notatniki i /pliki.
#                   Zakłada notatniki szkolenia z przypiętym domyślnym lakehouse'em i kopiuje yt_sample.json do lh_sales.
#   3 "dane"      – uruchamia notatniki z danymi po kolei i czeka na każdy: Contoso (nb_00 → nb_01 → nb_02_silver)
#                   i WWI (nb_ml_dane → nb_ml_trening → nb_ml_prognoza_python). Na F4 całość to ok. 1–2 h, głównie Contoso.
#   "wszystko"    – 1, 2 i 3 po kolei (folder z kroku 2 musi już być wgrany, więc zwykle: 1, upload, potem "wszystko").
#
# API: Fabric REST (Items, Lakehouses, Warehouses, Eventhouses, KQL Databases, SQL Databases, Notebooks, Job Scheduler)
# z tokenem osoby, która uruchamia notatnik – potrzebna rola Admin albo Member w workspace'ie.
# Nieprzetestowane w Fabric – sprawdź na próbie T−10 (zwłaszcza zakładanie SQL database i bazy KQL przez API).

# %% parametry (oznacz tę komórkę: Toggle parameter cell)
p_krok = "elementy"              # "elementy" | "notatniki" | "dane" | "wszystko"
p_folder = "Files/przygotowanie" # gdzie w lh_sales leży wgrany folder przygotowanie (notatniki/, pliki/)
p_predict = False                # True: w kroku 3 także nb_ml_predict (PySpark, PREDICT) – na F4 sesja Sparka, kilka minut
p_limit_min = 180                # ile minut najdłużej czekać na jeden notatnik w kroku 3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************



# %% 1. klient Fabric REST: token, stronicowanie, operacje długotrwałe (202 + Location)
import base64, json, os, time
import requests
import notebookutils

API = "https://api.fabric.microsoft.com/v1"
WS = notebookutils.runtime.context["currentWorkspaceId"]
print("workspace:", notebookutils.runtime.context.get("currentWorkspaceName"), WS)


def naglowki():
    return {"Authorization": f"Bearer {notebookutils.credentials.getToken('pbi')}", "Content-Type": "application/json"}


def czekaj(r, opis):
    """202 = operacja długotrwała: pytamy Location, aż skończy, potem bierzemy wynik."""
    if r.status_code != 202:
        r.raise_for_status()
        return r.json() if r.content else {}
    url = r.headers["Location"]
    while True:
        time.sleep(int(r.headers.get("Retry-After", 5)))
        r = requests.get(url, headers=naglowki())
        r.raise_for_status()
        stan = r.json().get("status")
        if stan in ("Succeeded", "Failed", "Undefined"):
            break
    if stan != "Succeeded":
        raise RuntimeError(f"{opis}: {stan} {r.text[:300]}")
    wynik = requests.get(url + "/result", headers=naglowki())
    return wynik.json() if wynik.ok and wynik.content else {}


def wywolaj(metoda, sciezka, opis="", **kw):
    r = requests.request(metoda, f"{API}/{sciezka}", headers=naglowki(), **kw)
    if r.status_code == 429:                         # limit wywołań: czekamy i ponawiamy raz
        time.sleep(int(r.headers.get("Retry-After", 30)))
        r = requests.request(metoda, f"{API}/{sciezka}", headers=naglowki(), **kw)
    if r.status_code >= 400:
        raise RuntimeError(f"{opis or sciezka}: HTTP {r.status_code} {r.text[:300]}")
    return czekaj(r, opis or sciezka)


def elementy():
    """Wszystkie elementy workspace'u: {(typ, nazwa): id}."""
    wynik, url = {}, f"{API}/workspaces/{WS}/items"
    while url:
        r = requests.get(url, headers=naglowki())
        r.raise_for_status()
        d = r.json()
        for it in d["value"]:
            wynik[(it["type"], it["displayName"])] = it["id"]
        url = d.get("continuationUri")
    return wynik


def zaloz(typ, sciezka, nazwa, cialo=None):
    istnieje = elementy().get((typ, nazwa))
    if istnieje:
        print(f"jest      {typ:<14} {nazwa}")
        return istnieje
    wywolaj("post", f"workspaces/{WS}/{sciezka}", f"{typ} {nazwa}", json={"displayName": nazwa, **(cialo or {})})
    nowy = elementy()[(typ, nazwa)]                  # id bierzemy z listy – wynik operacji 202 bywa pusty
    print(f"założony  {typ:<14} {nazwa}")
    return nowy


def krok(nazwa):
    return p_krok in (nazwa, "wszystko")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************



# %% 2. krok 1 – elementy
if krok("elementy"):
    zaloz("Lakehouse", "lakehouses", "lh_sales", {"creationPayload": {"enableSchemas": True}})
    zaloz("Lakehouse", "lakehouses", "lh_ml", {"creationPayload": {"enableSchemas": True}})
    zaloz("Warehouse", "warehouses", "wh_gold")
    zaloz("Warehouse", "warehouses", "wh_plan")
    zaloz("SQLDatabase", "sqlDatabases", "sqldb_plan")
    eh = zaloz("Eventhouse", "eventhouses", "eh_api")
    # Eventhouse z portalu dostaje bazę KQL o tej samej nazwie; przez API nie zawsze – wtedy zakładamy ją sami.
    if ("KQLDatabase", "eh_api") not in elementy():
        zaloz("KQLDatabase", "kqlDatabases", "eh_api",
              {"creationPayload": {"databaseType": "ReadWrite", "parentEventhouseItemId": eh}})
    else:
        print(f"jest      {'KQLDatabase':<14} eh_api")
    p = wywolaj("get", f"workspaces/{WS}/eventhouses/{eh}").get("properties", {})
    print("\nDo parametrów nb_api_eventhouse:")
    print("  p_query_uri  =", p.get("queryServiceUri"))
    print("  p_ingest_uri =", p.get("ingestionServiceUri"))
    print("\nTeraz: lh_sales → Files → Upload → Upload folder → folder 00_przygotowanie/przygotowanie, potem p_krok = \"notatniki\".")

# %% 3. krok 2 – notatniki szkolenia z przypiętym domyślnym lakehouse'em
# Który notatnik dostaje który lakehouse. None = bez domyślnego lakehouse'u.
LAKEHOUSE = {
    "nb_00_pobierz_dane": "lh_sales", "nb_01_bronze": "lh_sales", "nb_02_silver": "lh_sales",
    "nb_python_zamiast_spark": "lh_sales", "nb_api_eventhouse": "lh_sales", "nb_odchudzanie_modelu": "lh_sales",
    "nb_ml_dane": "lh_ml", "nb_ml_trening": "lh_ml", "nb_ml_predict": "lh_ml", "nb_ml_prognoza_python": "lh_ml",
    "nb_semantic_link": None, "nb_open_mirroring": None,
}

if krok("notatniki"):
    ids = elementy()
    sales = ids[("Lakehouse", "lh_sales")]
    zrodlo = f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{sales}/{p_folder}"
    lokalnie = "/tmp/przygotowanie"
    notebookutils.fs.cp(zrodlo, f"file:{lokalnie}", recurse=True)
    pliki = sorted(f for f in os.listdir(f"{lokalnie}/notatniki") if f.endswith(".ipynb"))
    print(len(pliki), "notatników w", p_folder)

    for plik in pliki:
        nazwa = plik[:-6]
        if ("Notebook", nazwa) in ids:
            print(f"jest      Notebook       {nazwa}")
            continue
        nb = json.load(open(f"{lokalnie}/notatniki/{plik}", encoding="utf-8"))
        lh = LAKEHOUSE.get(nazwa)
        if lh:
            nb["metadata"]["dependencies"] = {"lakehouse": {
                "default_lakehouse": ids[("Lakehouse", lh)], "default_lakehouse_name": lh,
                "default_lakehouse_workspace_id": WS}}
        tresc = base64.b64encode(json.dumps(nb, ensure_ascii=False).encode("utf-8")).decode()
        wywolaj("post", f"workspaces/{WS}/notebooks", f"notatnik {nazwa}", json={
            "displayName": nazwa,
            "definition": {"format": "ipynb", "parts": [
                {"path": "notebook-content.ipynb", "payload": tresc, "payloadType": "InlineBase64"}]}})
        print(f"założony  Notebook       {nazwa:<26} lakehouse: {lh or '–'}")

    # próbka API do Eventhouse: tam, gdzie szuka jej nb_api_eventhouse (p_tryb = "plik")
    cel = f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{sales}/Files/api/youtube/yt_sample.json"
    notebookutils.fs.cp(f"{zrodlo}/pliki/yt_sample.json", cel)
    print("skopiowany yt_sample.json → lh_sales Files/api/youtube")

# %% 4. krok 3 – dane: notatniki po kolei, każdy jako zadanie RunNotebook, z czekaniem
KOLEJNOSC = ["nb_00_pobierz_dane", "nb_01_bronze", "nb_02_silver",
             "nb_ml_dane", "nb_ml_trening", "nb_ml_prognoza_python"] + (["nb_ml_predict"] if p_predict else [])


def uruchom(nazwa):
    nb = elementy()[("Notebook", nazwa)]
    r = requests.post(f"{API}/workspaces/{WS}/items/{nb}/jobs/instances?jobType=RunNotebook", headers=naglowki())
    if r.status_code != 202:
        raise RuntimeError(f"{nazwa}: HTTP {r.status_code} {r.text[:300]}")
    url, start = r.headers["Location"], time.time()
    while True:
        time.sleep(30)
        s = requests.get(url, headers=naglowki()).json()
        stan = s.get("status")
        if stan in ("Completed", "Failed", "Cancelled", "Deduped"):
            break
        if time.time() - start > p_limit_min * 60:
            raise TimeoutError(f"{nazwa}: dłużej niż {p_limit_min} min – sprawdź w Monitor")
    minuty = (time.time() - start) / 60
    print(f"{nazwa:<26} {stan:<10} {minuty:5.1f} min")
    if stan != "Completed":
        raise RuntimeError(f"{nazwa}: {stan} – {s.get('failureReason')}. Otwórz notatnik, popraw, uruchom krok 3 ponownie.")


if krok("dane"):
    # nb_00 nic nie pobiera, gdy pliki Contoso już są, a reszta nadpisuje tabele – powtórka kroku jest bezpieczna.
    for nazwa in KOLEJNOSC:
        uruchom(nazwa)
    print("\nDane gotowe. Dalej ręcznie: 00_przygotowanie/README.md, kroki 5–9 (SQL, modele, Eventhouse, mirrory, Git).")

# %% 5. kontrola: co jest w workspace'ie
for (typ, nazwa) in sorted(elementy()):
    print(f"{typ:<16} {nazwa}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
