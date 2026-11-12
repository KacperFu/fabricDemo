# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# MARKDOWN ********************

# nb_ml_prognoza_python – notatnik PYTHON (nie PySpark): ta sama prognoza co PREDICT, bez Sparka. Blok 7 (ML).
# Język: Python. Domyślny lakehouse: lh_ml. Model: ml_wwi_popyt z notatnika 72.
# #
# mlflow.pyfunc wczytuje model z rejestru Fabric i liczy predykcje w zwykłym pandas. Dla setek albo tysięcy wierszy
# to wystarcza i kosztuje 1 CU (2 vCore) zamiast węzła Sparka. PREDICT (notatnik 73) ma sens, gdy wierszy są miliony.
# Zapis do osobnej tabeli gold_prognoza_popytu_python: tabela ze Sparka może mieć wektory usunięć (deletion vectors),
# których delta-rs nie nadpisze. Obie tabele powinny mieć te same prognozy.
# Komórka 3 drukuje wiersz cech do okna „Preview predictions” endpointu modelu.

# CELL ********************

# biblioteki (raz na sesję): deltalake do odczytu cech i zapisu prognozy – nie zawsze jest w środowisku
%pip install deltalake --quiet
# Instalacja restartuje interpreter i kasuje zmienne, dlatego stoi PRZED komórką parametrów.

# PARAMETERS CELL ********************

# parametry (oznacz tę komórkę: Toggle parameter cell)
p_korzen = "/lakehouse/default"
p_schemat = "dbo"                    # lakehouse bez schematów: ""
p_model = "ml_wwi_popyt"
p_wersja = 1

# CELL ********************

# 1. model z rejestru i prognoza dla 227 produktów
import mlflow
import numpy as np
import pandas as pd
from deltalake import DeltaTable, write_deltalake

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
model = mlflow.pyfunc.load_model(f"models:/{p_model}/{p_wersja}")
CECHY = model.metadata.get_input_schema().input_names()      # kolumny i kolejność z sygnatury modelu

cechy = DeltaTable(f"{tabele}/ml_popyt_cechy").to_pandas()
do_prognozy = cechy[cechy["cel_sztuki"].isna()]
prognoza = np.asarray(model.predict(do_prognozy[CECHY])).ravel()

wynik = do_prognozy[["WWIStockItemID", "produkt", "miesiac_prognozy", "srednia_3m", "srednia_12m"]].assign(
    prognoza_sztuk=np.round(prognoza, 0),
    model=f"{p_model}/{p_wersja}",
    utworzono=pd.Timestamp.now(tz="UTC"))
print(len(wynik), "produktów · suma prognoz", f"{wynik.prognoza_sztuk.sum():,.0f}",
      "· suma średnich 12 mies.", f"{wynik.srednia_12m.sum():,.0f}")
print(wynik.sort_values("prognoza_sztuk", ascending=False).head(5)[["produkt", "srednia_12m", "prognoza_sztuk"]]
      .to_string(index=False))
# oczekiwane (model z przebiegu lokalnego, extra_tree): 227 produktów · suma prognoz 248 740 · suma średnich 12 mies. 237 207;
# na czele „Black and orange fragile despatch tape 48mmx100m”: średnia 4 854, prognoza 5 704

# CELL ********************

# 2. zapis do tabeli gold
write_deltalake(f"{zapis}/gold_prognoza_popytu_python", wynik, mode="overwrite", schema_mode="overwrite",
                storage_options=opcje)
print("zapisano gold_prognoza_popytu_python")

# CELL ********************

# 3. wiersz do „Preview predictions” w endpoincie modelu (zamiast losowych wartości z Autofill)
import json

przyklad = do_prognozy[do_prognozy["WWIStockItemID"] == 1]
print(przyklad["produkt"].iloc[0], "→ prognoza", wynik.loc[przyklad.index[0], "prognoza_sztuk"])
print(json.dumps(przyklad[CECHY].iloc[0].to_dict(), indent=1))
# oczekiwane: USB missile launcher (Green) → prognoza 143 (model z przebiegu lokalnego)
# Wartości wpiszcie w formularz albo wklejcie w widoku JSON okna Preview predictions (format JSON sprawdźcie w tym widoku).
# Endpoint powinien zwrócić tę samą liczbę co tabela gold – to dowód, że w obu miejscach działa ta sama wersja modelu.
