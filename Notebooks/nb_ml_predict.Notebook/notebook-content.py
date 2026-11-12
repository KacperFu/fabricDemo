# Fabric notebook source


# MARKDOWN ********************

# nb_ml_predict – notatnik PYSPARK: PREDICT, czyli prognoza popytu na czerwiec 2016 do tabeli gold. Blok 7 (ML).
# Język: PySpark. Domyślny lakehouse: lh_ml. Model: ml_wwi_popyt z notatnika 72 (wersja z parametru).
# #
# PREDICT to funkcja Fabric (SynapseML) do predykcji wsadowych: wczytuje model z rejestru po nazwie i wersji
# i liczy predykcje w Sparku. Wymaga modelu w formacie MLflow z sygnaturą (notatnik 72 ją zapisuje).
# Kod jak w Learn: Transformer API (komórka 2) i Spark SQL przez SQLTransformer (komórka 4).
# Na F4: sesja Sparka zajmuje węzeł 8 vCore, czyli całą bazową moc F4. Uruchamiajcie PREDICT z harmonogramu,
# krótko, i zamykajcie sesję (notebookutils.session.stop()). Dla 227 wierszy to samo robi notatnik 74 w Pythonie za 1 CU.
# Tego notatnika nie dało się uruchomić poza Fabric (SynapseML); logikę sprawdza notatnik 74 na tym samym modelu.

# PARAMETERS CELL ********************

# parametry (oznacz tę komórkę: Toggle parameter cell)
p_schemat = "dbo"                    # lakehouse bez schematów: ""
p_model = "ml_wwi_popyt"
p_wersja = 1

# CELL ********************

# 1. wiersze do prognozy: maj 2016 (cechy na koniec miesiąca), cel pusty
from pyspark.sql import functions as F

sciezka = f"Tables/{p_schemat}" if p_schemat else "Tables"
cechy = spark.read.format("delta").load(f"{sciezka}/ml_popyt_cechy")
CECHY = ["sztuki_m0", "sztuki_m1", "sztuki_m2", "sztuki_m11", "srednia_3m", "srednia_6m", "srednia_12m",
         "klienci_m0", "faktury_m0", "dni_sprzedazy_cel", "miesiac_cel",
         "cena", "chlodnia", "dni_dostawy", "sztuk_w_opakowaniu", "waga"]
do_prognozy = cechy.filter(F.col("cel_sztuki").isNull())
print(do_prognozy.count(), "produktów do prognozy")
# oczekiwane: 227

# CELL ********************

# 2. PREDICT przez Transformer API
from synapse.ml.predict import MLFlowTransformer

model = MLFlowTransformer(inputCols=CECHY, outputCol="prognoza_sztuk", modelName=p_model, modelVersion=p_wersja)
wynik = (model.transform(do_prognozy)
         .select("WWIStockItemID", "produkt", "miesiac_prognozy", "srednia_3m", "srednia_12m",
                 F.round("prognoza_sztuk", 0).alias("prognoza_sztuk"))
         .withColumn("model", F.lit(f"{p_model}/{p_wersja}"))
         .withColumn("utworzono", F.current_timestamp()))
wynik.orderBy(F.desc("prognoza_sztuk")).show(5, truncate=False)
# oczekiwane (model z przebiegu lokalnego, extra_tree): suma prognoz na czerwiec 2016 ok. 248,7 tys. sztuk; najwięcej
# „Black and orange fragile despatch tape 48mmx100m” – 5 704. Inna wersja modelu da trochę inne liczby, te same co notatnik 74.

# CELL ********************

# 3. zapis do tabeli gold – trafia do modelu semantycznego jak każda inna tabela
wynik.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(f"{sciezka}/gold_prognoza_popytu")
spark.read.format("delta").load(f"{sciezka}/gold_prognoza_popytu").agg(
    F.count("*").alias("produkty"), F.sum("prognoza_sztuk").alias("suma_prognoz")).show()

# CELL ********************

# 4. ten sam PREDICT w Spark SQL (wymaga obiektu MLFlowTransformer z komórki 2)
from pyspark.ml.feature import SQLTransformer

sqlt = SQLTransformer().setStatement(
    f"SELECT produkt, PREDICT('{p_model}/{p_wersja}', {', '.join(CECHY)}) AS prognoza_sztuk FROM __THIS__")
sqlt.transform(do_prognozy).orderBy(F.desc("prognoza_sztuk")).show(5, truncate=False)

# CELL ********************

# 5. koniec sesji: na F4 nie zostawiajcie Sparka w tle
notebookutils.session.stop()
