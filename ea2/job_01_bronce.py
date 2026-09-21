# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea 1 del Job · `ingesta_bronce`
# MAGIC
# MAGIC **Pipeline Wanderbricks · Grupo 56 · EA2**
# MAGIC
# MAGIC Copia las nueve tablas de `samples.wanderbricks` a `bronce` y detiene el Job si una carga llega vacía.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "bigdata_grupo56"
ORIGEN   = "samples.wanderbricks"
TABLAS   = ["users", "hosts", "properties", "destinations", "bookings",
            "payments", "booking_updates", "reviews", "clickstream"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.bronce")

for t in TABLAS:
    df = (spark.table(f"{ORIGEN}.{t}")
            .withColumn("_ingesta_ts", F.current_timestamp())      # cuándo entró
            .withColumn("_fuente", F.lit(f"{ORIGEN}.{t}")))        # de dónde entró
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOGO}.bronce.{t}"))

    filas = spark.table(f"{CATALOGO}.bronce.{t}").count()
    if filas == 0:
        # Falla a propósito: el Job se detiene y la tarea 2 no corre sobre datos vacíos
        raise ValueError(f"Ingesta vacía en bronce.{t}: se detiene el pipeline")
    print(f"bronce.{t:<16} {filas:>10,} filas")

print("✅ Capa bronce actualizada")
