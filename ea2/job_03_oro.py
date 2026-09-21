# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea 3 del Job · `agregar_oro`
# MAGIC
# MAGIC **Pipeline Wanderbricks · Grupo 56 · EA2**
# MAGIC
# MAGIC Publica las cinco tablas de métricas en `oro`. Depende de `transformar_plata`.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "bigdata_grupo56"
P = f"{CATALOGO}.plata"
O = f"{CATALOGO}.oro"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {O}")

def guardar(df, nombre, comentario):
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{O}.{nombre}"))
    spark.sql(f"COMMENT ON TABLE {O}.{nombre} IS '{comentario}'")
    print(f"oro.{nombre:<28} {spark.table(f'{O}.{nombre}').count():>6,} filas")

bk  = spark.table(f"{P}.bookings")
pr  = spark.table(f"{P}.propiedades")
rv  = spark.table(f"{P}.reviews")
clk = spark.table(f"{P}.clickstream")

# Pregunta 1: ¿qué destinos generan más ingresos? (se excluyen las canceladas)
ingresos_por_destino = (bk.filter(F.col("status") != "cancelled")
    .join(pr, "property_id")
    .groupBy("destino", "pais")
    .agg(F.count("*").alias("reservas"),
         F.round(F.sum("total_amount"), 2).alias("ingresos_no_cancelados"),
         F.round(F.sum(F.when(F.col("status").isin("confirmed", "completed"),
                              F.col("total_amount"))), 2).alias("ingresos_confirmados"),
         F.round(F.avg("total_amount"), 2).alias("ticket_promedio")))
guardar(ingresos_por_destino, "ingresos_por_destino",
        "Ingresos por destino. no_cancelados incluye pendientes; confirmados solo confirmed y completed")

# Pregunta 2: ¿cuál es la distribución de estados de reserva (tasa de cancelación)?
total_reservas = bk.count()
estado_reservas = (bk.groupBy("status")
    .agg(F.count("*").alias("total_reservas"))
    .withColumn("porcentaje", F.round(100 * F.col("total_reservas") / F.lit(total_reservas), 2)))
guardar(estado_reservas, "estado_reservas",
        "Distribución de reservas por estado; cancelled es la tasa de cancelación")

# Pregunta 3: ¿cómo evoluciona el valor promedio de reserva?
valor_mensual = (bk.filter(F.col("check_in").isNotNull())
    .groupBy(F.date_format("check_in", "yyyy-MM").alias("mes"))
    .agg(F.count("*").alias("reservas"),
         F.round(F.avg("total_amount"), 2).alias("valor_promedio_reserva")))
guardar(valor_mensual, "valor_mensual_reservas",
        "Reservas y valor promedio por mes de check-in")

# Pregunta 4: ¿qué propiedades están mejor calificadas? (mínimo 5 reseñas válidas)
calificaciones = (rv.join(pr, "property_id")
    .groupBy("titulo", "tipo_propiedad")
    .agg(F.round(F.avg("rating"), 2).alias("calificacion_promedio"),
         F.count("rating").alias("numero_resenas"))
    .filter(F.col("numero_resenas") >= 5))
guardar(calificaciones, "calificacion_propiedades",
        "Calificación promedio por propiedad con al menos 5 reseñas no borradas")

# Pregunta 5: ¿desde qué dispositivo se navega más?
eventos_dispositivo = (clk.groupBy("device", "event")
    .agg(F.count("*").alias("numero_eventos")))
guardar(eventos_dispositivo, "eventos_por_dispositivo",
        "Eventos de navegación por dispositivo y tipo de evento")

print("✅ Capa oro actualizada")
