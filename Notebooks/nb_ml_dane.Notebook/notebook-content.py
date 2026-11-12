# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
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

# nb_ml_dane – notatnik PYTHON (nie PySpark): dane WWI do lakehouse'u i tabela cech do prognozy popytu. Blok 7 (ML).
# Język: Python. Domyślny lakehouse: lh_ml (przypnij przed uruchomieniem). Uruchom raz, przed szkoleniem.
# #
# Dane: oryginalna hurtownia Wide World Importers (WideWorldImportersDW, sprzedaż 01.2013–05.2016, 227 produktów)
# z publicznego kontenera Microsoftu, z którego korzystają tutoriale Fabric (Lakehouse i Warehouse).
# Dlaczego nie folder full/fact_sale z tutoriali: to 50 mln wierszy sztucznie powielonych z 11 miesięcy 2000 r.,
# każdy klient kupuje w każdym miesiącu, więc nie ma w nich historii, z której model mógłby się czegoś nauczyć.
# Dlaczego prognoza popytu, a nie odejście klienta: w WWI wszyscy 402 klienci kupują przez cały okres,
# a przerwy w zakupach są losowe (AUC prostych wskaźników ok. 0,5 – sprawdzone na tych danych).
# #
# Problem: ile sztuk każdego produktu sprzedamy w przyszłym miesiącu. Jeden wiersz tabeli cech = produkt × miesiąc,
# cechy liczone na koniec miesiąca, cel = sztuki w miesiącu następnym. Wiersze z maja 2016 nie mają celu:
# to na nich notatnik 73 (PREDICT) liczy prognozę na czerwiec 2016.
# Dlaczego notatnik Python: 3,5 MB danych, DuckDB liczy cechy w kilka sekund na 2 vCore (1 CU). Sesja Sparka
# na F4 zajęłaby węzeł 8 vCore, czyli całą bazową moc pojemności.


# PARAMETERS CELL ********************

# parametry (oznacz tę komórkę: Toggle parameter cell)
p_korzen = "/lakehouse/default"      # punkt montowania domyślnego lakehouse'u
p_schemat = "dbo"                    # lakehouse ze schematami (domyślny dla nowych); bez schematów: ""
p_zrodlo = "https://fabrictutorialdata.blob.core.windows.net/sampledata/WideWorldImportersDW/parquet/tables"
p_pliki = ["FactSale", "DimStockItem", "DimDate"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 1. pliki WWI do Files/wwi (ok. 3,5 MB, kilka sekund)
import os
import urllib.request

folder = f"{p_korzen}/Files/wwi"
os.makedirs(folder, exist_ok=True)
for nazwa in p_pliki:
    cel = f"{folder}/{nazwa}.parquet"
    if not os.path.exists(cel):
        urllib.request.urlretrieve(f"{p_zrodlo}/{nazwa}.parquet", cel)
    print(f"{nazwa:14s} {os.path.getsize(cel) / 1e6:6.2f} MB")
# oczekiwane: FactSale 3,46 MB · DimStockItem 0,02 MB · DimDate 0,01 MB

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 2. tabele Delta (bronze): trzy tabele WWI, tylko potrzebne kolumny
import duckdb
from deltalake import write_deltalake

tabele = f"{p_korzen}/Tables/{p_schemat}" if p_schemat else f"{p_korzen}/Tables"
# Zapis przez OneLake (abfss + token), nie przez /lakehouse/default: montowanie nie obsługuje zmiany nazwy pliku,
# której delta-rs potrzebuje przy zatwierdzaniu zapisu („Unable to rename file”). Odczyt z montowania działa.
try:
    import notebookutils
    ctx = notebookutils.runtime.context
    zapis = (f"abfss://{ctx['currentWorkspaceId']}@onelake.dfs.fabric.microsoft.com/{ctx['defaultLakehouseId']}/Tables"
             + (f"/{p_schemat}" if p_schemat else ""))
    opcje = {"bearer_token": notebookutils.credentials.getToken("storage"), "use_fabric_endpoint": "true"}
except ImportError:                                    # test lokalny poza Fabric: zwykła ścieżka
    zapis, opcje = tabele, None
con = duckdb.connect()
zapytania = {
    "wwi_fact_sale": f"""
        SELECT SaleKey, CustomerKey, StockItemKey, InvoiceDateKey, WWIInvoiceID, Quantity,
               UnitPrice, TotalExcludingTax, Profit
        FROM '{folder}/FactSale.parquet'""",
    # produkt ma kilka wersji (historia zmian, SCD 2): klucz StockItemKey wskazuje wersję, WWIStockItemID produkt
    "wwi_dim_stock_item": f"""
        SELECT StockItemKey, WWIStockItemID, StockItem, Color, Brand, Size, LeadTimeDays, QuantityPerOuter,
               IsChillerStock, UnitPrice, RecommendedRetailPrice, TypicalWeightPerUnit,
               CAST(ValidFrom AS DATE) AS ValidFrom, CAST(ValidTo AS DATE) AS ValidTo
        FROM '{folder}/DimStockItem.parquet'""",
    "wwi_dim_date": f"""
        SELECT Date, CalendarYear, CalendarMonthNumber, CalendarMonthLabel, ISOWeekNumber
        FROM '{folder}/DimDate.parquet'""",
}
for nazwa, sql in zapytania.items():
    tabela = con.sql(sql).to_arrow_table()
    write_deltalake(f"{zapis}/{nazwa}", tabela, mode="overwrite", schema_mode="overwrite", storage_options=opcje)
    print(f"{nazwa:20s} {tabela.num_rows:8,d} wierszy")
# oczekiwane: wwi_fact_sale 228 265 · wwi_dim_stock_item 672 · wwi_dim_date 1 461

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 3. tabela cech ml_popyt_cechy: produkt × miesiąc, cechy na koniec miesiąca, cel = sztuki w miesiącu następnym
# Wszystkie cechy jako DOUBLE: sygnatura modelu i PREDICT nie muszą wtedy dopasowywać int/long/double.
# dni_sprzedazy_cel: WWI sprzedaje od poniedziałku do soboty, w sobotę ok. połowę zwykłego dnia (sprawdzone na danych).
cechy_sql = f"""
WITH produkty AS (
    SELECT WWIStockItemID, StockItem AS produkt, CAST(UnitPrice AS DOUBLE) AS cena,
           CAST(IsChillerStock AS DOUBLE) AS chlodnia, CAST(LeadTimeDays AS DOUBLE) AS dni_dostawy,
           CAST(QuantityPerOuter AS DOUBLE) AS sztuk_w_opakowaniu, CAST(TypicalWeightPerUnit AS DOUBLE) AS waga
    FROM '{folder}/DimStockItem.parquet'
    WHERE WWIStockItemID <> 0 AND ValidTo > TIMESTAMP '9000-01-01'          -- aktualna wersja produktu
),
miesiace AS (
    SELECT CAST(date_trunc('month', Date) AS DATE) AS miesiac,
           CAST(SUM(CASE WHEN dayofweek(Date) BETWEEN 1 AND 5 THEN 1.0 WHEN dayofweek(Date) = 6 THEN 0.5 ELSE 0 END)
                AS DOUBLE) AS dni_sprzedazy
    FROM '{folder}/DimDate.parquet'
    GROUP BY 1
),
sprzedaz AS (
    SELECT s.WWIStockItemID, CAST(date_trunc('month', f.InvoiceDateKey) AS DATE) AS miesiac,
           SUM(f.Quantity) AS sztuki, COUNT(DISTINCT f.CustomerKey) AS klienci, COUNT(DISTINCT f.WWIInvoiceID) AS faktury
    FROM '{folder}/FactSale.parquet' f
    JOIN '{folder}/DimStockItem.parquet' s USING (StockItemKey)
    GROUP BY ALL
),
siatka AS (          -- każdy produkt w każdym miesiącu z danymi, także w miesiącach bez sprzedaży (wtedy 0)
    SELECT p.*, m.miesiac
    FROM produkty p
    CROSS JOIN (SELECT DISTINCT miesiac FROM sprzedaz) m
),
baza AS (
    SELECT g.*, CAST(COALESCE(s.sztuki, 0) AS DOUBLE) AS sztuki, CAST(COALESCE(s.klienci, 0) AS DOUBLE) AS klienci,
           CAST(COALESCE(s.faktury, 0) AS DOUBLE) AS faktury
    FROM siatka g
    LEFT JOIN sprzedaz s USING (WWIStockItemID, miesiac)
),
okna AS (
    SELECT *,
        LAG(sztuki, 1)  OVER w AS sztuki_m1,
        LAG(sztuki, 2)  OVER w AS sztuki_m2,
        LAG(sztuki, 11) OVER w AS sztuki_m11,          -- ten sam miesiąc rok wcześniej co miesiąc prognozy
        AVG(sztuki) OVER (w ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)  AS srednia_3m,
        AVG(sztuki) OVER (w ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)  AS srednia_6m,
        AVG(sztuki) OVER (w ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS srednia_12m,
        COUNT(*)    OVER (w ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS historia_mies,
        LEAD(sztuki, 1) OVER w AS cel_sztuki
    FROM baza
    WINDOW w AS (PARTITION BY WWIStockItemID ORDER BY miesiac)
)
SELECT o.WWIStockItemID, o.produkt, o.miesiac,
       CAST(o.miesiac + INTERVAL 1 MONTH AS DATE) AS miesiac_prognozy,
       o.sztuki AS sztuki_m0, o.sztuki_m1, o.sztuki_m2, o.sztuki_m11,
       o.srednia_3m, o.srednia_6m, o.srednia_12m,
       o.klienci AS klienci_m0, o.faktury AS faktury_m0,
       mc.dni_sprzedazy AS dni_sprzedazy_cel,
       CAST(month(o.miesiac + INTERVAL 1 MONTH) AS DOUBLE) AS miesiac_cel,
       o.cena, o.chlodnia, o.dni_dostawy, o.sztuk_w_opakowaniu, o.waga,
       o.cel_sztuki
FROM okna o
JOIN miesiace mc ON mc.miesiac = CAST(o.miesiac + INTERVAL 1 MONTH AS DATE)
WHERE o.historia_mies = 12                             -- pełne 12 miesięcy historii
ORDER BY o.WWIStockItemID, o.miesiac
"""
cechy = con.sql(cechy_sql).to_arrow_table()
write_deltalake(f"{zapis}/ml_popyt_cechy", cechy, mode="overwrite", schema_mode="overwrite", storage_options=opcje)

kontrola = con.sql("""
    SELECT COUNT(*) AS wiersze, COUNT(DISTINCT WWIStockItemID) AS produkty,
           MIN(miesiac) AS od_miesiaca, MAX(miesiac) AS do_miesiaca,
           COUNT(*) FILTER (WHERE cel_sztuki IS NULL) AS do_prognozy,
           ROUND(AVG(cel_sztuki), 1) AS sredni_cel
    FROM cechy""").df()
print(kontrola.to_string(index=False))
# oczekiwane: 6 810 wierszy · 227 produktów · od 2013-12-01 do 2016-05-01 · 227 do prognozy · średni cel 994,8

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 4. podgląd: jeden produkt w ostatnich miesiącach
print(con.sql("""
    SELECT miesiac, produkt, sztuki_m0, srednia_3m, srednia_12m, dni_sprzedazy_cel, cel_sztuki
    FROM cechy WHERE WWIStockItemID = 1 ORDER BY miesiac DESC LIMIT 6""").df().to_string(index=False))
# oczekiwane dla USB missile launcher (Green), maj 2016: sztuki 220 · średnia 3 mies. 176,0 · średnia 12 mies. 147,75 · cel pusty
# Ostatni wiersz (maj 2016) nie ma celu: tę liczbę policzy model. Tabelę zobaczycie też w SQL analytics endpoint.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
