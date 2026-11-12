# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# MARKDOWN ********************

# nb_ml_trening – notatnik PYTHON (nie PySpark): eksperyment MLflow i rejestracja modelu prognozy popytu. Blok 7 (ML).
# Język: Python. Domyślny lakehouse: lh_ml (z tabelą ml_popyt_cechy z notatnika 71). Uruchom przed szkoleniem.
# #
# Eksperyment exp_wwi_popyt ma trzy przebiegi:
#   1. baseline_srednia_12m – bez modelu: prognoza = średnia sprzedaż z ostatnich 12 miesięcy. Punkt odniesienia.
#   2. lightgbm             – LightGBM z ręcznie dobranymi parametrami.
#   3. automl               – FLAML sam porównuje algorytmy i parametry w budżecie czasu (domyślnie 2 minuty).
# Najlepszy z przebiegów 2–3 według walidacji trafia do rejestru jako model ml_wwi_popyt (element workspace'u z wersjami).
# Dane dzielimy w czasie, nie losowo: uczenie do czerwca 2015, walidacja 07–12.2015, test 01–04.2016.
# Losowy podział wpuściłby przyszłość do uczenia i dał zbyt dobre wyniki.
# #
# Czego się spodziewać (przebieg lokalny na tych danych): na walidacji AutoML wychodzi tyle co średnia z 12 miesięcy
# (MAE 203,1 wobec 203,4), na teście jest o 15% lepszy (240,6 wobec 283,5), ale prawie tyle co średnia z 3 miesięcy
# (246,8). WWI to symulacja: popyt na produkt waha się losowo wokół poziomu z łagodnym trendem, więc model ma niewiele
# do nauczenia. Pokazujemy to celowo: przebieg bazowy w eksperymencie to pierwsza rzecz, którą robicie na własnych danych.
# Sygnatura modelu (kolumny i typy wejścia) jest wymagana przez PREDICT i przez endpoint, dlatego logujemy ją jawnie.

# CELL ********************

# biblioteki (raz na sesję, około minuty): FLAML do AutoML i deltalake do odczytu tabeli cech
# deltalake nie zawsze jest w środowisku notatnika Python (próba 11.11.2026: „No module named 'deltalake'”).
%pip install "flaml[automl]" deltalake --quiet
# Instalacja restartuje interpreter i kasuje zmienne, dlatego stoi PRZED komórką parametrów.

# PARAMETERS CELL ********************

# parametry (oznacz tę komórkę: Toggle parameter cell)
p_korzen = "/lakehouse/default"
p_schemat = "dbo"                    # lakehouse bez schematów: ""
p_eksperyment = "exp_wwi_popyt"
p_model = "ml_wwi_popyt"
p_budzet_automl = 120                # sekundy: górny limit czasu AutoML
p_proby_automl = 40                  # limit prób; każda próba to przebieg podrzędny w eksperymencie

# CELL ********************

# 1. dane: tabela cech i podział w czasie
import pandas as pd
from deltalake import DeltaTable

tabele = f"{p_korzen}/Tables/{p_schemat}" if p_schemat else f"{p_korzen}/Tables"
df = DeltaTable(f"{tabele}/ml_popyt_cechy").to_pandas()
df["miesiac"] = pd.to_datetime(df["miesiac"])

CECHY = ["sztuki_m0", "sztuki_m1", "sztuki_m2", "sztuki_m11", "srednia_3m", "srednia_6m", "srednia_12m",
         "klienci_m0", "faktury_m0", "dni_sprzedazy_cel", "miesiac_cel",
         "cena", "chlodnia", "dni_dostawy", "sztuk_w_opakowaniu", "waga"]
CEL = "cel_sztuki"

uczenie = df[df.miesiac < "2015-07-01"]
walidacja = df[(df.miesiac >= "2015-07-01") & (df.miesiac < "2016-01-01")]
test = df[(df.miesiac >= "2016-01-01") & df[CEL].notna()]
print(f"uczenie {len(uczenie):,} · walidacja {len(walidacja):,} · test {len(test):,} · "
      f"do prognozy {df[CEL].isna().sum()}")
# oczekiwane: uczenie 4 313 · walidacja 1 362 · test 908 · do prognozy 227

# CELL ********************

# 2. eksperyment i przebieg bazowy: średnia z 12 miesięcy
import mlflow
from sklearn.metrics import mean_absolute_error

mlflow.autolog(disable=True)       # logujemy sami: jeden model z sygnaturą i metryki na naszym podziale w czasie
mlflow.set_experiment(p_eksperyment)


def mae(y, p):
    return round(float(mean_absolute_error(y, p)), 1)


wyniki = {}                         # przebieg → (MAE walidacja, MAE test, run_id)
with mlflow.start_run(run_name="baseline_srednia_12m") as run:
    m = {"mae_walidacja": mae(walidacja[CEL], walidacja.srednia_12m),
         "mae_test": mae(test[CEL], test.srednia_12m),
         "mae_walidacja_srednia_3m": mae(walidacja[CEL], walidacja.srednia_3m),
         "mae_test_srednia_3m": mae(test[CEL], test.srednia_3m)}
    mlflow.log_metrics(m)
    mlflow.set_tag("rodzaj", "punkt odniesienia, bez modelu")
    wyniki["baseline_srednia_12m"] = (m["mae_walidacja"], m["mae_test"], run.info.run_id)
print(m)
# oczekiwane: MAE walidacja 203,4 · test 283,5 (średnia z 3 miesięcy: 223,4 · 246,8). Średni popyt to ok. 1 000 sztuk.

# CELL ********************

# 3. LightGBM z ręcznie dobranymi parametrami
import lightgbm as lgb
from mlflow.models import infer_signature

params = dict(objective="regression_l1", n_estimators=400, learning_rate=0.03, num_leaves=15,
              min_child_samples=30, subsample=0.8, subsample_freq=1, colsample_bytree=0.8,
              random_state=42, verbose=-1)
with mlflow.start_run(run_name="lightgbm") as run:
    model = lgb.LGBMRegressor(**params).fit(uczenie[CECHY], uczenie[CEL])
    m = {"mae_walidacja": mae(walidacja[CEL], model.predict(walidacja[CECHY])),
         "mae_test": mae(test[CEL], model.predict(test[CECHY]))}
    mlflow.log_params(params)
    mlflow.log_metrics(m)
    # wyjście jako nazwana kolumna: endpoint nie obsługuje schematów tensorowych
    sygnatura = infer_signature(uczenie[CECHY], pd.DataFrame({"prognoza_sztuk": model.predict(uczenie[CECHY])}))
    mlflow.lightgbm.log_model(model, artifact_path="model", signature=sygnatura)
    wyniki["lightgbm"] = (m["mae_walidacja"], m["mae_test"], run.info.run_id)
print(m)
waznosc = pd.Series(model.feature_importances_, CECHY).sort_values(ascending=False)
print(waznosc.head(5).to_string())
# oczekiwane: MAE walidacja 209,5 · test 251,5; najczęściej używane cechy: srednia_6m, sztuki_m0, miesiac_cel, dni_sprzedazy_cel

# CELL ********************

# 4. AutoML (FLAML): algorytm i parametry w budżecie czasu, każdy próbowany wariant jako przebieg podrzędny
# retrain_full=False: model zostaje nauczony tylko na zbiorze uczącym, więc walidacja i test są uczciwe.
# max_iter: bez limitu FLAML robi setki prób w dwie minuty i każdą zapisuje jako przebieg – eksperyment staje się nieczytelny.
# Na F4: notatnik Python ma 2 vCore (1 CU). Ten sam AutoML w Sparku z use_spark=True zająłby węzeł 8 vCore.
from flaml import AutoML

automl = AutoML()
with mlflow.start_run(run_name="automl") as run:
    automl.fit(X_train=uczenie[CECHY], y_train=uczenie[CEL], X_val=walidacja[CECHY], y_val=walidacja[CEL],
               task="regression", metric="mae", time_budget=p_budzet_automl, max_iter=p_proby_automl,
               estimator_list=["lgbm", "rf", "extra_tree"], retrain_full=False, seed=42, verbose=0)
    najlepszy = automl.model.estimator           # obiekt scikit-learn albo LightGBM – te odmiany obsługuje endpoint
    m = {"mae_walidacja": mae(walidacja[CEL], najlepszy.predict(walidacja[CECHY])),
         "mae_test": mae(test[CEL], najlepszy.predict(test[CECHY]))}
    mlflow.log_param("najlepszy_algorytm", automl.best_estimator)
    mlflow.log_params({f"hp_{k}": v for k, v in automl.best_config.items()})
    mlflow.log_metrics(m)
    sygnatura = infer_signature(uczenie[CECHY], pd.DataFrame({"prognoza_sztuk": najlepszy.predict(uczenie[CECHY])}))
    mlflow.sklearn.log_model(najlepszy, artifact_path="model", signature=sygnatura)
    wyniki["automl"] = (m["mae_walidacja"], m["mae_test"], run.info.run_id)
print(automl.best_estimator, m)
# oczekiwane (przebieg lokalny, 40 prób): extra_tree · MAE walidacja 203,1 · test 240,6. Inne wersje bibliotek w Fabric
# mogą wybrać inny algorytm; ważne jest porównanie z przebiegiem bazowym, nie dokładna liczba.

# CELL ********************

# 5. porównanie przebiegów i rejestracja najlepszego modelu (wybór po walidacji, test tylko do raportu)
porownanie = pd.DataFrame(wyniki, index=["mae_walidacja", "mae_test", "run_id"]).T
print(porownanie[["mae_walidacja", "mae_test"]].to_string())

modele = porownanie.drop(index="baseline_srednia_12m")
zwyciezca = modele["mae_walidacja"].astype(float).idxmin()
wersja = mlflow.register_model(f"runs:/{modele.loc[zwyciezca, 'run_id']}/model", p_model)
print(f"zarejestrowano {wersja.name}, wersja {wersja.version}, z przebiegu {zwyciezca}")
# Zobaczcie w workspace: eksperyment exp_wwi_popyt (przebiegi, metryki, porównanie na wykresie) i model ml_wwi_popyt.
# oczekiwane: baseline 203,4 / 283,5 · lightgbm 209,5 / 251,5 · automl 203,1 / 240,6 · zarejestrowano wersję 1 z przebiegu automl
# Uczciwy wniosek dla WWI: model nie bije wyraźnie prostych średnich. Na waszych danych to pierwsze pytanie
# przed wdrożeniem: czy model jest lepszy od prostej reguły, którą da się policzyć jedną miarą DAX.
