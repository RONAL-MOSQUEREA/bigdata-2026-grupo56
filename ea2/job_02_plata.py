# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea 2 del Job · `transformar_plata`
# MAGIC
# MAGIC **Pipeline Wanderbricks · Grupo 56 · EA2**
# MAGIC
# MAGIC Limpia y tipa bronce y escribe `plata`. Depende de `ingesta_bronce`.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "bigdata_grupo56"
B = f"{CATALOGO}.bronce"
P = f"{CATALOGO}.plata"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {P}")

def guardar(df, nombre):
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{P}.{nombre}"))
    print(f"plata.{nombre:<12} {spark.table(f'{P}.{nombre}').count():>10,} filas")

# 1) Reservas: fechas y montos tipados, sin duplicados, sin nulos críticos
bookings = (spark.table(f"{B}.bookings")
    .withColumn("check_in", F.to_date("check_in"))
    .withColumn("check_out", F.to_date("check_out"))
    .withColumn("total_amount", F.col("total_amount").cast("decimal(10,2)"))
    .dropDuplicates(["booking_id"])
    .filter(F.col("user_id").isNotNull() & F.col("property_id").isNotNull())
    .withColumn("noches", F.datediff("check_out", "check_in"))
    .withColumn("es_cancelada", F.col("status") == "cancelled")
    .drop("_fuente"))
guardar(bookings, "bookings")

# 2) Reseñas: fuera los borrados lógicos, rating como entero
reviews = (spark.table(f"{B}.reviews")
    .filter(F.col("is_deleted") == False)
    .withColumn("rating", F.col("rating").cast("int"))
    .drop("_fuente"))
guardar(reviews, "reviews")

# 3) Clickstream: se aplana el struct anidado metadata -> device, referrer
clickstream = (spark.table(f"{B}.clickstream")
    .select("user_id", "event", "property_id", "timestamp",
            F.col("metadata.device").alias("device"),
            F.col("metadata.referrer").alias("referrer"),
            "_ingesta_ts"))
guardar(clickstream, "clickstream")

# 4) Propiedades: dimensión desnormalizada (propiedad + destino)
propiedades = (spark.table(f"{B}.properties").alias("p")
    .join(spark.table(f"{B}.destinations").alias("d"),
          F.col("p.destination_id") == F.col("d.destination_id"), "left")
    .select(F.col("p.property_id"), F.col("p.host_id"), F.col("p.destination_id"),
            F.col("p.title").alias("titulo"),
            F.col("p.property_type").alias("tipo_propiedad"),
            F.col("p.base_price"), F.col("p.max_guests"), F.col("p.bedrooms"),
            F.col("d.destination").alias("destino"),
            F.col("d.country").alias("pais")))
guardar(propiedades, "propiedades")

print("✅ Capa plata actualizada")
