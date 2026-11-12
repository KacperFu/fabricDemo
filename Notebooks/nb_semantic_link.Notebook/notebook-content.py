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
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# nb_semantic_link – notatnik PYTHON (nie PySpark): semantic link na modelu gold z bloku Medalion. Szkolenie F4, blok 5.
# Język: Python. Domyślny lakehouse niepotrzebny. Wymaga: model Direct Lake sm_gold na wh_gold z miarą Revenue
# (00_przygotowanie/README.md, krok 6) i tabelami gold_fact_sales, gold_dim_customer, gold_dim_product, gold_dim_store, gold_dim_date.
# Zmiana 12.11.2026: notatnik pochodził z kursu Cognity (modele sm_sprzedaz_import i sm_sprzedaz_dl, tabele dim_*);
# teraz czyta sm_gold. Porównanie „przed i po” zostało jako opcja (p_model_porownanie), domyślnie wyłączone.
# #
# Co to jest: biblioteka sempy (wbudowana w notatniki Fabric) czyta modele semantyczne jak ramki danych pandas,
# wykonuje DAX i woła API Fabric i Power BI z tożsamością użytkownika. semantic-link-labs (open source, Microsoft)
# dokłada narzędzia administratora: Best Practice Analyzer, VertiPaq Analyzer, kontrole Direct Lake, migrację modeli.
# Dlaczego notatnik Python: sempy działa w obu rodzajach notatników, a te zadania nie potrzebują Sparka (2 vCore = 1 CU).
# Gdy funkcja semantic-link-labs ma w nowszej wersji inną sygnaturę: help(labs.<funkcja>) w komórce.
# Nieprzetestowane w Fabric na sm_gold – liczby „do uzupełnienia” wpisz po próbie.


# CELL ********************

# biblioteka (raz na sesję, około minuty)
%pip install semantic-link-labs lxml --quiet     # lxml: get_direct_lake_guardrails czyta tabelę ze strony Learn
# Instalacja restartuje interpreter i kasuje zmienne, dlatego stoi PRZED komórką parametrów.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# parametry
p_model = "sm_gold"            # model Direct Lake na wh_gold
p_ws = ""                      # workspace z modelem; puste = workspace notatnika
p_kraj = "Germany"             # kraj sklepu do filtra w komórce 2 (wartość z gold_dim_store[Country])
p_revenue_wh = 232153677248.67 # SUM(Revenue) z kontroli w 03_gold_warehouse.sql – wzorzec do kontroli zgodności
p_model_porownanie = ""        # opcjonalnie drugi model do BPA obok sm_gold, np. "sm_plan"; puste = tylko sm_gold
p_odswiez = False              # True: komórka 4 zleca odświeżenie (w Direct Lake: ponowne wskazanie aktualnych plików Delta)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 1. model i jego zawartość jako ramki danych
import sempy.fabric as fabric

ws = p_ws or None
print(fabric.list_datasets(workspace=ws)[["Dataset Name"]].to_string())
miary = fabric.list_measures(p_model, workspace=ws)
print(len(miary), "miar w", p_model)
print(fabric.list_relationships(p_model, workspace=ws).to_string())
# oczekiwane: cztery relacje z gold_fact_sales do wymiarów (CustomerKey, ProductKey, StoreKey, OrderDate → Date),
# wszystkie wiele do jednego, jeden kierunek

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 2. miara bez pisania DAX: evaluate_measure buduje zapytanie sam (grupowanie i filtry jako parametry)
rok = fabric.evaluate_measure(p_model, "Revenue_", groupby_columns=["gold_dim_date[Year]"], workspace=ws)
print(rok.tail(3).to_string())
kraj = fabric.evaluate_measure(p_model, "Revenue_", groupby_columns=["gold_dim_date[Year]"],
                               filters={"gold_dim_store[Country]": [p_kraj]}, workspace=ws)
print(kraj.tail(3).to_string())
# oczekiwane: suma wszystkich lat = 232 153 677 248,67 (jak w wh_gold); wartości lat i kraju: do uzupełnienia po próbie.
# Pusty wynik dla kraju = inna pisownia w danych: sprawdź unikalne wartości w komórce 3 i popraw p_kraj.
# Kiedy evaluate_dax: gdy potrzebujesz pełnego DAX (SUMMARIZECOLUMNS z kilkoma miarami, TREATAS, rola) – przykład w komórce 3.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 3. kontrola zgodności model ↔ Warehouse i mały wymiar jako pandas
suma = fabric.evaluate_dax(p_model, 'EVALUATE ROW("Revenue", [Revenue_])', workspace=ws).iloc[0, 0]
roznica = float(suma) - p_revenue_wh
print(f"model: {float(suma):,.2f} · wh_gold: {p_revenue_wh:,.2f} · różnica: {roznica:,.2f}")
if abs(roznica) > 0.01:
    raise ValueError("Model i Warehouse się różnią – w procesie wydawania tu zatrzymujemy wdrożenie.")
# W prawdziwym procesie wzorzec liczy zapytanie do Warehouse'u, a nie stała w parametrze; zasada jest ta sama.

sklepy = fabric.read_table(p_model, "gold_dim_store", workspace=ws)
print(len(sklepy), "sklepów")
print(sklepy.groupby("Country").size().sort_values(ascending=False).to_string())
# oczekiwane: 74 sklepy (jak w kontroli 03_gold_warehouse.sql); podział na kraje: do uzupełnienia po próbie.
# Nie czytaj tak faktu: read_table ściąga całą tabelę, a gold_fact_sales ma 237 mln wierszy.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import sempy_labs as labs
labs.vertipaq_analyzer(dataset="sm_gold", workspace=ws)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 4. automatyzacja: odświeżenie i jego szczegóły (to robi pipeline aktywnością Semantic model refresh)
# W Direct Lake odświeżenie nie kopiuje danych, tylko wskazuje modelowi aktualną wersję tabel Delta (reframing).
if p_odswiez:
    zadanie = fabric.refresh_dataset(p_model, workspace=ws, refresh_type="full")
    print("zlecone odświeżenie:", zadanie)
    # fabric.get_refresh_execution_details(p_model, zadanie, workspace=ws)  – status, czas, błędy po zakończeniu

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 5. Best Practice Analyzer: model gold (i opcjonalnie drugi model obok)
import sempy_labs as labs

labs.run_model_bpa(dataset=p_model, workspace=ws)
if p_model_porownanie:
    labs.run_model_bpa(dataset=p_model_porownanie, workspace=ws)
# Co pokazać w sm_gold: klucze (CustomerKey, ProductKey…) widoczne dla użytkownika, kolumny liczbowe z domyślnym
# sumowaniem, miara bez formatu, brak opisów. Model zbudowany kreatorem z Warehouse'u zwykle ma te uwagi –
# to dobra lista kontrolna na wasze modele. Wynik zapisany do lakehouse'u: notatnik nb_odchudzanie_modelu, p_zapis = True.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 6. Direct Lake: limity SKU, rozmiar pojemności i powód przejścia na DirectQuery
from sempy_labs import directlake

# Każde wywołanie osobno: gdy jedno padnie (np. brak lxml, inna sygnatura), dwa pozostałe i tak się wykonają.
for opis, wywolanie in [
    ("limity Direct Lake dla SKU (tabela ze strony Learn)", lambda: directlake.get_direct_lake_guardrails().to_string()),
    ("rozmiar pojemności", lambda: directlake.get_sku_size(workspace=ws)),
    ("powody przejścia na DirectQuery", lambda: directlake.check_fallback_reason(dataset=p_model, workspace=ws).to_string()),
]:
    try:
        print(f"--- {opis}\n{wywolanie()}")
    except Exception as e:
        print(f"--- {opis}: {type(e).__name__}: {str(e)[:300]}")
# Na co patrzeć: wiersz F4 w limitach (300 mln wierszy na tabelę) wobec 237 mln w gold_fact_sales.
# Model kreatora z Warehouse'u to Direct Lake na SQL endpoint: przy widoku albo przekroczonym limicie przechodzi
# na DirectQuery – check_fallback_reason pokazuje, która tabela i dlaczego. Oczekiwane: brak przyczyn.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 7. Migracja Import → Direct Lake w semantic-link-labs (do pokazania, nie uruchamiamy)
# Moduł sempy_labs.migration przenosi model 1:1: tabele obliczane trafiają do lakehouse'u jako tabele
# (migrate_calc_tables_to_lakehouse), kolumny, relacje i miary do nowego modelu Direct Lake
# (migrate_tables_columns_to_semantic_model, migrate_model_objects_to_semantic_model), a migration_validation sprawdza,
# czy wszystko przeszło. Zapytania Power Query trzeba odtworzyć osobno (create_pqt_file robi szablon dla Dataflow Gen2).
# Pułapka: narzędzie kopiuje logikę tak, jak jest – kolumny obliczane, UNION w DAX i kilka definicji tej samej miary
# przechodzą bez zmian. Kiedy go użyć: model jest już uporządkowany i chodzi tylko o zmianę trybu (np. Import za duży
# na 3 GB F4), albo jako szybki pierwszy krok, po którym sprzątacie logikę.
# from sempy_labs import migration
# help(migration.migrate_calc_tables_to_lakehouse)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
