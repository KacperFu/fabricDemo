# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# MARKDOWN ********************

# nb_api_eventhouse – notatnik PYTHON (nie PySpark): dane z API Google do Eventhouse. Zapas fazy 8 (Z1), decyzja D24.
# Język notatnika: Python (Home → język → Python). Domyślny lakehouse: dowolny, np. lh_sales (ślad surowych odpowiedzi w Files/api).
# Co robi: pobiera statystyki ostatnich filmów z kanałów YouTube (YouTube Data API v3), zapisuje surową odpowiedź
# w lakehouse'ie i wysyła wiersze do tabeli yt_raw w Eventhouse. Tabele, polityki i zapytania: 72_eventhouse_api.kql.
# #
# Dlaczego YouTube Data API: wystarczy klucz API (bez OAuth), a każde wywołanie list kosztuje 1 jednostkę z 10 000
# dziennie na projekt (reset o północy czasu pacyficznego). search.list ma osobny limit 100 wywołań dziennie, więc go nie
# używamy: lista filmów kanału idzie przez channels.list → playlistItems.list → videos.list (do 50 filmów na wywołanie).
# Każde wywołanie, nawet błędne, kosztuje co najmniej 1 jednostkę – dlatego błędów 400/401/403 nie ponawiamy.
# Bez klucza Google odrzuca zapytania: 429 RESOURCE_EXHAUSTED, limit 0 na wspólnym projekcie.
# #
# Dlaczego notatnik, a nie Copy albo Eventstream (decyzja D24):
#   * Copy (konektor REST) uwierzytelnia się tylko: Anonymous, Basic, Organizational account, Service principal.
#     Klucz Google trzeba by wpisać w adres albo nagłówek konfiguracji, a OAuth Google (GA4, Search Console) nie zadziała.
#   * Eventstream ze źródłem HTTP (preview) odpytuje API co 1–3600 s z kluczem API, ale bez OAuth, tylko JSON, a każdy
#     cykl zaczyna od pierwszej strony, więc te same wiersze przychodzą wiele razy. Płacisz też za godzinę działania strumienia.
#   * Notatnik obsłuży każdy rodzaj logowania (klucz, konto usługi Google), paginację, limity i ponowienia,
#     a sesja Python to 2 vCore = 1 CU tylko na czas przebiegu. Cena: kod do utrzymania.
# Klucz API: Azure Key Vault (notebookutils.credentials.getSecret), nigdy w kodzie ani w parametrze pipeline'u.
# Na szkoleniu bez Key Vault: p_tryb = "plik" (próbka 70_zapas/pliki/yt_sample.json) albo klucz w p_api_key tylko na czas
# pokazu – nie zapisuj notatnika z kluczem. Klucz idzie w nagłówku x-goog-api-key, nie w adresie (adresy trafiają do logów).

# CELL ********************

# biblioteki (raz na sesję, około minuty)
%pip install azure-kusto-data azure-kusto-ingest --quiet
# Instalacja restartuje interpreter i kasuje zmienne, dlatego stoi PRZED komórką parametrów.

# PARAMETERS CELL ********************

# parametry (oznacz tę komórkę: Toggle parameter cell)
p_tryb = "plik"                  # "api" – YouTube Data API z kluczem; "plik" – próbka z Files/api/yt_sample.json (plan B)
p_keyvault = ""                  # np. "https://kv-szkolenie.vault.azure.net/"; puste = bez Key Vault
p_secret = "youtube-api-key"     # nazwa sekretu w Key Vault
p_api_key = ""                   # tylko na pokaz bez Key Vault; po pokazie wyczyść i nie zapisuj
p_kanaly = "@GoogleDevelopers"   # uchwyty kanałów rozdzielone przecinkami
p_max_filmow = 50                # ostatnie filmy na kanał: 50 = jedna strona playlistItems = 1 jednostka
p_query_uri = ""                 # Eventhouse → KQL database → Overview → Query URI
p_ingest_uri = ""                # ta sama strona → Ingestion URI
p_baza = "eh_api"
p_tabela = "yt_raw"
p_folder = "/lakehouse/default/Files/api/youtube"

# CELL ********************

# 1. klucz i klient HTTP z ponowieniami
import datetime as dt, io, json, os, random, time
import requests
import notebookutils          # w notatniku Python nie zawsze jest załadowany sam

API = "https://www.googleapis.com/youtube/v3"
licznik = {"wywolania": 0, "ponowienia": 0}


def klucz():
    if p_tryb == "plik":
        return None
    if p_keyvault:
        return notebookutils.credentials.getSecret(p_keyvault, p_secret)
    if p_api_key:
        return p_api_key
    raise ValueError("Brak klucza: ustaw p_keyvault albo p_api_key, albo p_tryb = 'plik'.")


KLUCZ = klucz()


def get(sciezka, params, proby=5):
    """GET z ponowieniami. Ponawiamy tylko to, co może się udać za chwilę: 429 i błędy 5xx."""
    for proba in range(proby):
        r = requests.get(f"{API}/{sciezka}", params=params, headers={"x-goog-api-key": KLUCZ}, timeout=30)
        licznik["wywolania"] += 1                       # każde wywołanie, także błędne, kosztuje co najmniej 1 jednostkę
        if r.status_code == 200:
            return r.json()
        try:
            powod = r.json()["error"]["errors"][0]["reason"]
        except Exception:
            powod = ""
        if powod in ("quotaExceeded", "dailyLimitExceeded"):
            # dzienny limit: ponawianie nic nie da, kolejny przebieg po północy czasu pacyficznego
            raise RuntimeError(f"Dzienny limit YouTube Data API wyczerpany ({powod}).")
        if r.status_code in (429, 500, 502, 503, 504):
            czekaj = float(r.headers.get("Retry-After", 0)) or min(60, 2 ** proba + random.random())
            licznik["ponowienia"] += 1
            time.sleep(czekaj)
            continue
        r.raise_for_status()                            # 400/401/403: zły klucz, brak uprawnień albo złe parametry
    raise RuntimeError(f"{sciezka}: {proby} nieudanych prób, ostatni status {r.status_code}")

# CELL ********************

# 2. pobranie: kanał → lista filmów → statystyki
pobrano = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def filmy_kanalu(uchwyt):
    ch = get("channels", {"part": "contentDetails", "forHandle": uchwyt})
    if not ch.get("items"):
        print("nie znaleziono kanału", uchwyt)
        return []
    uploads = ch["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    ids, token = [], None
    while len(ids) < p_max_filmow:                      # paginacja: nextPageToken aż do końca albo do limitu filmów
        p = {"part": "contentDetails", "playlistId": uploads, "maxResults": 50}
        if token:
            p["pageToken"] = token
        strona = get("playlistItems", p)
        ids += [i["contentDetails"]["videoId"] for i in strona.get("items", [])]
        token = strona.get("nextPageToken")
        if not token:
            break
    return ids[:p_max_filmow]


def statystyki(ids):
    out = []
    for i in range(0, len(ids), 50):                    # do 50 identyfikatorów w jednym wywołaniu: 1 jednostka
        out += get("videos", {"part": "snippet,statistics", "id": ",".join(ids[i:i + 50])}).get("items", [])
    return out


if p_tryb == "api":
    filmy = []
    for uchwyt in [k.strip() for k in p_kanaly.split(",") if k.strip()]:
        filmy += statystyki(filmy_kanalu(uchwyt))
    wiersze = [{"pobrano_utc": pobrano, "zrodlo": "youtube", "klucz": v["id"], "payload": v} for v in filmy]
else:
    sciezka = f"{p_folder}/yt_sample.json"
    if os.path.exists(sciezka):
        tekst = open(sciezka, encoding="utf-8").read()
    else:
        # Montaż /lakehouse/default powstaje przy starcie sesji: lakehouse przypięty później go nie ma.
        # Wtedy czytamy ten sam plik przez OneLake, bez montażu.
        # Adres z ID workspace'u i domyślnego lakehouse'u, nie z nazwy: działa przy każdej nazwie lakehouse'u.
        ctx = notebookutils.runtime.context
        sciezka = (f"abfss://{ctx['currentWorkspaceId']}@onelake.dfs.fabric.microsoft.com/{ctx['defaultLakehouseId']}/"
                   + p_folder.split("/lakehouse/default/", 1)[-1] + "/yt_sample.json")
        tekst = notebookutils.fs.head(sciezka, 10_000_000)
    print("plik:", sciezka)
    wiersze = [json.loads(line) for line in tekst.splitlines() if line.strip()]
print(f"wiersze: {len(wiersze)} · wywołania API: {licznik['wywolania']} (tyle jednostek) · ponowienia: {licznik['ponowienia']}")
# oczekiwane w trybie "plik": wiersze: 61 · wywołania API: 0

# CELL ********************

# 3. ślad: surowa odpowiedź w lakehouse'ie (do ponownego załadowania i do audytu)
if p_tryb == "api":
    folder = f"{p_folder}/{pobrano[:10].replace('-', '/')}"
    os.makedirs(folder, exist_ok=True)
    plik = f"{folder}/{pobrano[11:19].replace(':', '')}.json"
    with open(plik, "w", encoding="utf-8") as fh:
        fh.write("\n".join(json.dumps(w, ensure_ascii=False) for w in wiersze))
    print("zapisano", plik)

# CELL ********************

# 4. do Eventhouse: kolejkowane ładowanie z mapowaniem JSON
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import IngestionProperties, QueuedIngestClient


def kcsb(uri):
    return KustoConnectionStringBuilder.with_aad_application_token_authentication(uri, notebookutils.credentials.getToken("kusto"))


dane = "\n".join(json.dumps(w, ensure_ascii=False) for w in wiersze).encode("utf-8")
props = IngestionProperties(database=p_baza, table=p_tabela, data_format=DataFormat.JSON,
                            ingestion_mapping_reference="yt_raw_map",
                            flush_immediately=True)  # tylko na pokaz: omija batching; w produkcji zostaw batching (decyzja D24)
QueuedIngestClient(kcsb(p_ingest_uri)).ingest_from_stream(io.BytesIO(dane), ingestion_properties=props)
print("wysłano", len(wiersze), "wierszy do", p_tabela)

# CELL ********************

# 5. kontrola: czy wiersze doszły (ładowanie kolejkowane jest asynchroniczne, więc czekamy)
klient = KustoClient(kcsb(p_query_uri))
for proba in range(20):
    t = klient.execute(p_baza, f"{p_tabela} | summarize wiersze = count(), ostatnie = max(pobrano_utc)").primary_results[0]
    n = t[0]["wiersze"]
    print(f"{proba * 15:>4} s: {n} wierszy, ostatnie pobranie {t[0]['ostatnie']}")
    if n >= len(wiersze):
        break
    time.sleep(15)
# oczekiwane w trybie "plik" za pierwszym razem: 61 wierszy, ostatnie pobranie 2026-10-15 09:00:00
# Plan B, gdy komórka 4 kończy się błędem uwierzytelnienia: Eventhouse → baza eh_api → Get data → Local file →
# yt_sample.json → tabela yt_raw → mapowanie yt_raw_map. Dalej 72_eventhouse_api.kql bez zmian.

# CELL ********************

# GA4 zamiast YouTube (do pokazania, nie do uruchomienia tutaj): konto usługi Google z Key Vault
# from google.analytics.data_v1beta import BetaAnalyticsDataClient
# from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
# from google.oauth2 import service_account
# info = json.loads(notebookutils.credentials.getSecret(p_keyvault, "ga4-service-account"))   # JSON konta usługi
# ga = BetaAnalyticsDataClient(credentials=service_account.Credentials.from_service_account_info(info))
# odp = ga.run_report(RunReportRequest(property="properties/<ID>", date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
#                                      dimensions=[Dimension(name="date"), Dimension(name="sessionDefaultChannelGroup")],
#                                      metrics=[Metric(name="sessions"), Metric(name="totalUsers")], return_property_quota=True))
# print(odp.property_quota)   # ile tokenów zużyto i ile zostało: 200 000 dziennie i 40 000 na godzinę na usługę standardową,
#                             # 14 000 na godzinę na projekt, 10 zapytań naraz; po wyczerpaniu 429 RESOURCE_EXHAUSTED
