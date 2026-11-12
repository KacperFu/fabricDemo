# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# MARKDOWN ********************

# nb_odchudzanie_modelu – notatnik PYTHON: ile waży model i co poprawić (semantic link labs)
# Język notatnika: Python. Faza 1 (ocena stanu): rozmiar modeli „przed”; faza 7: ten sam pomiar dla modelu centralnego.
# Notatnik sam wyszukuje modele w workspace'ie; p_wzorzec zawęża listę (faza 1: "_import$", faza 7: "_dl$").
# Biblioteka semantic-link-labs (open source, Microsoft) instaluje się przez %pip; nie jest częścią produktu.
# Wymagania jak w nb_inwentaryzacja: XMLA co najmniej Read, rola Contributor albo wyższa w workspace'ie z modelami.
# Model, którego nie da się odczytać, trafia do listy „pominięte” z przyczyną; reszta liczy się dalej.

# CELL ********************

# komórka 1 – instalacja
%pip install semantic-link-labs

# PARAMETERS CELL ********************

# parametry (Toggle parameter cell)
p_workspace = ""                   # workspace z modelami; puste = workspace notatnika
p_wzorzec = ""                     # wyrażenie regularne na nazwę modelu, np. "_import$"; puste = wszystkie modele
p_bpa = False                      # True: dodatkowo Best Practice Analyzer dla każdego modelu (komórka 3)
p_zapis = False                    # True: komórka 4 zapisuje wyniki BPA i VertiPaq Analyzer do lh_sales (historia przeglądów)

# CELL ********************

# komórka 2 – Memory Analyzer: rozmiar tabel i kolumn każdego znalezionego modelu
import re
import pandas as pd
import sempy.fabric as fabric
import sempy_labs as labs

ws = p_workspace or None
modele_ws = fabric.list_datasets(workspace=ws)
kol_nazwy = next(c for c in modele_ws.columns if "Name" in c)
modele = sorted(n for n in modele_ws[kol_nazwy].astype(str) if re.search(p_wzorzec, n))
print(len(modele), "modeli:", ", ".join(modele))

pominiete = []
for model in modele:
    print(f"\n=== {model}")
    try:
        labs.vertipaq_analyzer(dataset=model, workspace=ws)
    except Exception as e:                       # brak uprawnień, XMLA wyłączone, workspace Pro
        pominiete.append({"Model": model, "Przyczyna": str(e).splitlines()[0][:200]})
if pominiete:
    display(pd.DataFrame(pominiete))
# Na warsztacie: w modelu A porównaj rozmiar fact_sales (tabela obliczana) z fact_sales_history + fact_sales_current:
# to ta sama treść. W modelu B ten sam fakt jest trzeci raz. Model centralny (faza 7, p_wzorzec = "_dl$") po kilku
# zapytaniach z 31_model_centralny.dax: fakt raz, bez kolumn obliczanych, w pamięci tylko używane kolumny.
# Czego szukać: kolumny tekstowe o dużej liczbie unikatów (Name, adresy, identyfikatory), kolumny daty z czasem,
# kolumny obliczane, kolumny, których nie używa żadna miara ani raport. W Direct Lake kolumny ładują się do
# pamięci dopiero przy zapytaniu, ale każda zbędna kolumna to i tak koszt przy pierwszym użyciu i w limitach F2.

# CELL ********************

# komórka 3 – Best Practice Analyzer: reguły modelowania (gdy p_bpa = True)
if p_bpa:
    for model in modele:
        if model in {p["Model"] for p in pominiete}:
            continue
        print(f"\n=== BPA: {model}")
        try:
            labs.run_model_bpa(dataset=model, workspace=ws)
        except Exception as e:
            print("pominięty:", str(e).splitlines()[0][:200])
# Typowe znaleziska: relacje dwukierunkowe, klucze widoczne w raporcie, brak tabeli dat, miary bez formatu,
# kolumny liczbowe z domyślnym sumowaniem.

# CELL ********************

# komórka 4 – zapis wyników do lakehouse'u (gdy p_zapis = True): historia przeglądów modeli
# Wymaga domyślnego lakehouse'u lh_sales, przypiętego PRZED startem sesji. BPA dopisuje wiersze do tabeli
# modelbparesults (każdy przegląd modelu dostaje nowy RunId), VertiPaq Analyzer zapisuje tabele vertipaqanalyzer_*.
# Zapis robi biblioteka: w notatniku Python przez delta-rs. Gdy zgłosi brak modułu deltalake, dopisz go do %pip w komórce 1.
# Podsumowanie niżej liczy się z wyniku zwróconego przez run_model_bpa, więc działa także wtedy, gdy zapis się nie uda.
# Po co: uruchomione przed migracją i po niej (albo co miesiąc z harmonogramu) pokazuje, czy błędów i pamięci ubywa.
if p_zapis:
    POZIOM = {"\u274c": "Error", "\u26a0\ufe0f": "Warning", "\u2139\ufe0f": "Info"}
    wyniki = []
    for model in modele:
        if model in {p["Model"] for p in pominiete}:
            continue
        print(f"\n=== zapis: {model}")
        try:
            df = labs.run_model_bpa(dataset=model, workspace=ws, export=True, return_dataframe=True)
        except Exception as e:
            print("zapis BPA nieudany, liczę bez zapisu:", str(e).splitlines()[0][:200])
            df = labs.run_model_bpa(dataset=model, workspace=ws, return_dataframe=True)
        wyniki.append(df.assign(Model=model))
        try:
            labs.vertipaq_analyzer(dataset=model, workspace=ws, export="table", visualize=False)
        except Exception as e:
            print("zapis VertiPaq nieudany:", str(e).splitlines()[0][:200])

    bpa = pd.concat(wyniki, ignore_index=True)
    bpa["Poziom"] = bpa["Severity"].map(POZIOM).fillna(bpa["Severity"])
    display(bpa.pivot_table(index="Model", columns="Poziom", values="Rule Name", aggfunc="count", fill_value=0))
    display(bpa.groupby(["Model", "Category", "Rule Name"]).size().rename("obiekty")
            .reset_index().sort_values(["Model", "obiekty"], ascending=[True, False]))
# Pierwsza tabela: liczba naruszeń wg poziomu (Error, Warning, Info) w tym przeglądzie każdego modelu.
# Druga: które reguły i ile obiektów. Historia wszystkich przeglądów: tabela modelbparesults (kolumny RunId, Timestamp).
