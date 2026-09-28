# Databricks notebook source
# MAGIC %md
# MAGIC # EA3 — Procesamiento distribuido de datos con Apache Spark
# MAGIC
# MAGIC **Big Data (ISD-25)** · Ingeniería de Software y Datos · IU Digital de Antioquia
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Grupo** | 56 |
# MAGIC | **Integrantes** | Ronal Mosquera · Federico López Torres |
# MAGIC | **Caso de estudio** | Wanderbricks (marketplace de alquiler vacacional) |
# MAGIC | **Fecha de entrega** | domingo 27 de septiembre, 11:59 p. m. |
# MAGIC | **🎥 Enlace al video** | ⚠️ **COMPLETAR:** *(https://docs.google.com/document/d/1U5BjYmbXjgt4WCx_lO5IT8O6TbCUOfof/edit?usp=sharing&ouid=106910370328930537306&rtpof=true&sd=true)* |
# MAGIC | **Repositorio** | https://github.com/RONAL-MOSQUEREA/bigdata-2026-grupo56 (carpeta `/ea3`) |
# MAGIC
# MAGIC > ⚠️ **Antes de entregar:** verificar que el enlace del video abra desde una cuenta distinta a la propia.
# MAGIC > Un enlace inaccesible se califica como no entregado.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Contexto y problema
# MAGIC
# MAGIC Este trabajo cierra el caso que venimos construyendo desde agosto. En la **EA1** diseñamos el modelo de
# MAGIC datos de Wanderbricks y respondimos cinco preguntas de negocio; en la **EA2** desplegamos y gobernamos el
# MAGIC entorno (catálogo `bigdata_grupo56` con las capas `bronce`, `plata` y `oro`, permisos, linaje y un Job).
# MAGIC Aquí llevamos ese pipeline hasta su capa de consumo y **medimos y optimizamos una operación distribuida**.
# MAGIC
# MAGIC **La pregunta de negocio que responde la capa oro:**
# MAGIC
# MAGIC > **¿Qué destinos convierten mejor el interés de navegación en reservas, y cuánto ingreso dejan?**
# MAGIC
# MAGIC *Quién la usa:* la gerencia comercial y de marketing, para decidir en qué destinos invertir promoción:
# MAGIC un destino con mucha navegación y pocas reservas es una oportunidad perdida; uno con pocas visitas pero
# MAGIC muchas reservas se está quedando corto de visibilidad.
# MAGIC
# MAGIC **Por qué es un problema de procesamiento distribuido.** Responderla obliga a **cruzar dos tablas de
# MAGIC hechos** (los eventos de navegación de `clickstream` y las reservas de `bookings`) con la dimensión de
# MAGIC propiedades para llegar al destino. Cada cruce mueve datos entre máquinas, y esa operación (el *shuffle*)
# MAGIC es lo que cuesta. Con el volumen de hoy (cientos de miles de filas) un solo equipo alcanzaría; lo que
# MAGIC evaluamos aquí es **el método**: saber por qué tarda lo que tarda, mejorarlo y explicar qué cambió. El
# MAGIC mismo diseño es el que tendría que resistir el crecimiento que vimos en la EA1, donde las reservas por mes
# MAGIC pasaron de 1 en diciembre de 2022 a más de 25.000 en julio de 2025.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Descripción de los datos
# MAGIC
# MAGIC Trabajamos con los datos del caso, **sin amplificarlos**: el enunciado de esta evidencia no exige un
# MAGIC volumen mínimo y dice expresamente que se use lo que ya se tiene. Las tablas vienen de las capas que
# MAGIC construimos en la EA2 a partir de `samples.wanderbricks`.
# MAGIC
# MAGIC | Tabla | Capa | Filas (aprox.) | Papel en esta evidencia |
# MAGIC |---|---|---:|---|
# MAGIC | `clickstream` | bronce y plata | 100.000 | **Hechos de navegación**, la tabla más grande junto con reseñas. En plata se aplanó el struct `metadata` (dispositivo, referrer) |
# MAGIC | `bookings` | bronce y plata | 72.247 | **Hechos de reservas**: estado, fechas y monto |
# MAGIC | `propiedades` | plata | 18.163 | **Dimensión**: propiedad ya unida con su destino y país |
# MAGIC | `reviews` | bronce y plata | 99.793 | Se limpió en plata (borrado lógico); no entra a la capa oro de esta evidencia |
# MAGIC
# MAGIC Las cifras exactas se calculan en la celda siguiente, para que queden visibles en el HTML exportado. La
# MAGIC celda también verifica que las capas de la EA2 existan: si falta alguna, hay que ejecutar antes el
# MAGIC notebook `EA2_grupo56` (o su Job).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window
import time, statistics, io, re
from contextlib import redirect_stdout

CATALOGO = "bigdata_grupo56"
B, P, O = f"{CATALOGO}.bronce", f"{CATALOGO}.plata", f"{CATALOGO}.oro"

# 1) Las capas de la EA2 deben existir
requeridas = [f"{B}.bookings", f"{B}.clickstream", f"{B}.reviews",
              f"{P}.bookings", f"{P}.clickstream", f"{P}.reviews", f"{P}.propiedades"]
faltan = []
for t in requeridas:
    try:
        spark.table(t).limit(1).collect()
    except Exception:
        faltan.append(t)
if faltan:
    raise RuntimeError("Faltan tablas de la EA2 (ejecutar primero EA2_grupo56 o su Job): " + ", ".join(faltan))

# 2) Volumen con el que se trabaja
conteos = {}
for nombre, ruta in [("bronce.clickstream", f"{B}.clickstream"), ("plata.clickstream", f"{P}.clickstream"),
                     ("bronce.bookings",    f"{B}.bookings"),    ("plata.bookings",    f"{P}.bookings"),
                     ("bronce.reviews",     f"{B}.reviews"),     ("plata.reviews",     f"{P}.reviews"),
                     ("plata.propiedades",  f"{P}.propiedades")]:
    conteos[nombre] = spark.table(ruta).count()
    print(f"{nombre:<22}{conteos[nombre]:>12,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Decisiones de diseño y justificación
# MAGIC
# MAGIC **Arquitectura de capas: reutilizamos la de la EA2.** Las capas `bronce`, `plata` y `oro` ya existen, con
# MAGIC sus permisos y su linaje. Crear otras para esta evidencia habría duplicado datos y roto la trazabilidad
# MAGIC que ya construimos. La capa oro nueva vive en el mismo esquema `oro`, así que **hereda el `SELECT` que ya
# MAGIC otorgamos a nivel de esquema** en la EA2.
# MAGIC
# MAGIC **Reglas de negocio que se aplican** (las de plata vienen de la EA2; las de oro son nuevas):
# MAGIC
# MAGIC | Capa | Regla | Por qué |
# MAGIC |---|---|---|
# MAGIC | Plata | Reservas deduplicadas por `booking_id` y con `user_id` y `property_id` presentes | Una reserva repetida infla ingresos |
# MAGIC | Plata | Reseñas con `is_deleted = true` fuera | Son borrados lógicos, no deben promediarse |
# MAGIC | Plata | Struct `metadata` del clickstream aplanado en `device` y `referrer` | Las columnas anidadas no se agrupan ni se leen fácil |
# MAGIC | Oro | Las reservas **canceladas no cuentan** como reservas | Una cancelada no es una conversión |
# MAGIC | Oro | `ingresos_confirmados` suma solo `confirmed` y `completed` | El 43,7 % de las reservas está en `pending`; contarlas sobrestima el ingreso (hallazgo de la EA2) |
# MAGIC
# MAGIC **Qué esperamos que cueste.** La consulta cruza dos tablas de cerca de 100.000 filas
# MAGIC (`clickstream` y `bookings`) con la dimensión `propiedades` (18.163 filas) por `property_id`. Por defecto
# MAGIC Spark cruza con `SortMergeJoin`: reparte y ordena **las dos tablas** por la clave de cruce, y ese movimiento
# MAGIC entre máquinas (`Exchange`) es el costo. Además hay un `countDistinct` de visitantes y una función de
# MAGIC ventana para el ranking, que agregan movimientos propios.
# MAGIC
# MAGIC **La técnica elegida: *broadcast join* de la dimensión `propiedades`.** Una sola técnica, elegida porque
# MAGIC ataca directamente ese costo: la dimensión es mucho más pequeña que los hechos, así que en lugar de mover
# MAGIC los hechos, se le entrega una copia de la dimensión a cada máquina. Descartamos las otras porque no encajan
# MAGIC con este caso:
# MAGIC
# MAGIC | Técnica | Por qué no |
# MAGIC |---|---|
# MAGIC | Particionado o *liquid clustering* | Con ~100.000 filas cada tabla cabe en muy pocos archivos; no hay nada que podar |
# MAGIC | Caché | La consulta se ejecuta pocas veces y el motor ya guarda los archivos leídos; no ataca el *shuffle* |
# MAGIC | Filtros anticipados | La pregunta usa todos los eventos y todas las reservas no canceladas: no hay filtro que adelantar |
# MAGIC | Reescritura (agregar antes de cruzar) | Cambia la forma de la consulta y complica la comparación; el *broadcast* deja la consulta idéntica |
# MAGIC
# MAGIC **Hipótesis antes de medir.** Como los datos son pequeños, esperamos una **mejora modesta, posiblemente
# MAGIC cercana al ruido de medición**. Si la diferencia no supera la variación entre corridas, lo reportaremos
# MAGIC así (el enunciado lo pide) y lo explicaremos con el plan de ejecución.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Implementación
# MAGIC ### 4.1 Herramienta de medición
# MAGIC
# MAGIC Una sola corrida es ruido: la primera ejecución siempre es más lenta (Spark planea, lee metadatos y a
# MAGIC veces enciende cómputo) y la misma consulta tarda distinto sin que nada cambie. Por eso el método es:
# MAGIC **una ejecución de calentamiento que se descarta, tres mediciones y se reporta la mediana.** Las tres
# MAGIC mediciones quedan visibles en la salida de cada medición.

# COMMAND ----------

import time, statistics

MEDICIONES = {}   # aquí queda el registro de cada medición, para usarlo después en el análisis

def medir(descripcion, funcion, repeticiones=3):
    """Calienta (descartado), repite y reporta la mediana."""
    funcion()                                   # calentamiento: no se cuenta
    tiempos = []
    for _ in range(repeticiones):
        inicio = time.time()
        funcion()
        tiempos.append(time.time() - inicio)
    mediana = statistics.median(tiempos)
    variacion = max(tiempos) - min(tiempos)
    MEDICIONES[descripcion] = {"tiempos": tiempos, "mediana": mediana, "variacion": variacion}
    print(descripcion)
    print(f"   mediciones : {', '.join(f'{t:.2f}s' for t in tiempos)}")
    print(f"   MEDIANA    : {mediana:.2f}s")
    print(f"   variación  : {variacion:.2f}s\n")
    return mediana

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Capa bronce
# MAGIC
# MAGIC Bronce **ya existe**: la construyó la tarea `ingesta_bronce` del Job de la EA2, que copia las tablas de
# MAGIC `samples.wanderbricks` sin transformarlas y agrega dos columnas de trazabilidad, `_ingesta_ts` (cuándo
# MAGIC entró) y `_fuente` (de dónde vino). Aquí solo se verifica lo que esta evidencia usa.

# COMMAND ----------

print("bronce.clickstream — el struct metadata llega anidado, tal como viene de la fuente:")
spark.table(f"{B}.clickstream").printSchema()
display(spark.table(f"{B}.clickstream").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Capa plata — calidad de datos
# MAGIC
# MAGIC La construyó la tarea `transformar_plata` del mismo Job. La siguiente tabla documenta **cuántas filas
# MAGIC entraron, cuántas salieron y por qué**; después se comprueban nulos y duplicados sobre lo que quedó.

# COMMAND ----------

reglas = [
    ("bookings",    "Duplicados por booking_id y nulos en user_id o property_id",
     conteos["bronce.bookings"],    conteos["plata.bookings"]),
    ("reviews",     "Borrado lógico (is_deleted = true)",
     conteos["bronce.reviews"],     conteos["plata.reviews"]),
    ("clickstream", "Sin descarte: solo se aplanó el struct metadata en device y referrer",
     conteos["bronce.clickstream"], conteos["plata.clickstream"]),
]
resumen = [(t, r, a, d, a - d) for t, r, a, d in reglas]
display(spark.createDataFrame(resumen, "tabla string, regla string, filas_bronce long, filas_plata long, filas_descartadas long"))

# COMMAND ----------

def nulos(df, columnas):
    fila = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columnas]).first()
    return {c: int(fila[c] or 0) for c in columnas}

clic = spark.table(f"{P}.clickstream")
bk   = spark.table(f"{P}.bookings")
prop = spark.table(f"{P}.propiedades")

print("Nulos en plata.clickstream :", nulos(clic, ["user_id", "property_id", "event", "timestamp", "device"]))
print("Nulos en plata.bookings    :", nulos(bk, ["booking_id", "user_id", "property_id", "check_in", "total_amount", "status"]))
print("Duplicados por booking_id  :", bk.count() - bk.select("booking_id").distinct().count())
print("Duplicados por property_id en propiedades:", prop.count() - prop.select("property_id").distinct().count())
print()
print("Tipos en plata.bookings (fechas y montos ya tipados):")
bk.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Transformaciones distribuidas
# MAGIC
# MAGIC La consulta que responde la pregunta de negocio tiene tres partes:
# MAGIC
# MAGIC 1. **Navegación por destino:** cruza `clickstream` con `propiedades` por `property_id` y agrega eventos y
# MAGIC    visitantes únicos (`countDistinct`).
# MAGIC 2. **Reservas por destino:** cruza `bookings` (sin canceladas) con `propiedades` y agrega reservas,
# MAGIC    ticket promedio e ingresos confirmados.
# MAGIC 3. **Unión y ranking:** une las dos agregaciones por destino, calcula las reservas por cada 100 eventos y
# MAGIC    ordena con una **función de ventana** (`row_number`).
# MAGIC
# MAGIC Se escribe **una sola función** con un parámetro `usar_broadcast`. Así la consulta base y la optimizada
# MAGIC son **exactamente la misma lógica**, y lo único que cambia entre ellas es la técnica que se mide.

# COMMAND ----------

def construir_conversion(usar_broadcast=False):
    """Conversión navegación → reservas por destino. Con usar_broadcast=True difunde la dimensión."""
    dim = prop.select("property_id", "destino", "pais").filter(F.col("destino").isNotNull())
    if usar_broadcast:
        dim = broadcast(dim)                      # ← la ÚNICA diferencia entre base y optimizada

    eventos = (clic.join(dim, "property_id")
        .groupBy("destino", "pais")
        .agg(F.count("*").alias("eventos_navegacion"),
             F.countDistinct("user_id").alias("visitantes_unicos")))

    reservas = (bk.filter(F.col("status") != "cancelled")
        .join(dim, "property_id")
        .groupBy("destino", "pais")
        .agg(F.count("*").alias("reservas"),
             F.round(F.avg("total_amount"), 2).alias("ticket_promedio"),
             F.round(F.sum(F.when(F.col("status").isin("confirmed", "completed"),
                                  F.col("total_amount"))), 2).alias("ingresos_confirmados")))

    ranking = Window.orderBy(F.desc("reservas_por_100_eventos"))
    return (eventos.join(reservas, ["destino", "pais"], "left")
        .withColumn("reservas", F.coalesce("reservas", F.lit(0)))
        .withColumn("reservas_por_100_eventos",
                    F.round(F.col("reservas") / F.col("eventos_navegacion") * 100, 2))
        .withColumn("posicion", F.row_number().over(ranking))
        .select("posicion", "destino", "pais", "eventos_navegacion", "visitantes_unicos",
                "reservas", "reservas_por_100_eventos", "ingresos_confirmados", "ticket_promedio")
        .orderBy("posicion"))

construir_conversion(False).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Medición base
# MAGIC
# MAGIC **Caso base.** Spark tiene una optimización automática: si una tabla es pequeña (por defecto, menos de
# MAGIC 10 MB), la difunde solo. Para poder medir el efecto de la técnica, en el caso base **desactivamos esa
# MAGIC decisión automática** (`spark.sql.autoBroadcastJoinThreshold = -1`), igual que en el laboratorio guiado.
# MAGIC Esta configuración se mantiene en las dos mediciones, de modo que **la única diferencia es el `broadcast`
# MAGIC explícito**. Más adelante, en los límites, se discute qué significa esto para un entorno real.
# MAGIC
# MAGIC Primero el **plan de ejecución inicial** y luego la medición, sin optimizar nada.

# COMMAND ----------

try:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    print("spark.sql.autoBroadcastJoinThreshold = -1  (Spark no difunde nada por su cuenta)\n")
except Exception as e:
    print("⚠️ No se pudo fijar el umbral:", e)

def plan_texto(df):
    """Captura la salida de .explain() como texto, para mostrarla y contar operadores."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain()
    return buf.getvalue()

def resumen_plan(texto):
    return {
        "SortMergeJoin":                         len(re.findall(r"SortMergeJoin", texto)),
        "BroadcastHashJoin":                     len(re.findall(r"BroadcastHashJoin", texto)),
        "Exchange (mueve datos entre máquinas)": len(re.findall(r"(?<![A-Za-z])Exchange ", texto)),
        "BroadcastExchange (copia a cada máquina)": len(re.findall(r"BroadcastExchange", texto)),
        "Sort (ordenamiento previo al cruce)":   len(re.findall(r"^[\s+:\-]*Sort \[", texto, flags=re.M)),
    }

df_base   = construir_conversion(usar_broadcast=False)
plan_base = plan_texto(df_base)
print("════════ PLAN DE EJECUCIÓN · ANTES DE OPTIMIZAR ════════\n")
print(plan_base)

# COMMAND ----------

# MAGIC %md
# MAGIC **Dónde está el costo en este plan.** El plan se lee de abajo hacia arriba. Buscamos `Exchange`, que es
# MAGIC mover datos entre máquinas y la operación cara. Cada cruce por `property_id` aparece como `SortMergeJoin`
# MAGIC con un `Exchange hashpartitioning(property_id…)` **sobre sus dos entradas**: tanto la tabla de hechos
# MAGIC (`clickstream` o `bookings`) como la dimensión `propiedades` se reparten y se ordenan por la clave. La
# MAGIC tabla de hechos es la que más pesa en ese movimiento. Las agregaciones (`HashAggregate`), el
# MAGIC `countDistinct`, la unión final y la ventana suman sus propios `Exchange`, que **esta optimización no
# MAGIC toca**.

# COMMAND ----------

DESC_BASE = "BASE · SortMergeJoin, sin broadcast"
t_base = medir(DESC_BASE, lambda: construir_conversion(False).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 Optimización
# MAGIC
# MAGIC **Una técnica: `broadcast` explícito de `propiedades`.** La dimensión tiene 18.163 filas, muy pocas frente
# MAGIC a los hechos. En vez de repartir las dos tablas por `property_id`, Spark le entrega **una copia de la
# MAGIC dimensión a cada máquina**, y los hechos se cruzan donde ya están, sin moverse. La analogía del estadio:
# MAGIC si la lista de socios es corta, es más barato darle una copia a cada contador que mover a 40.000 asistentes.
# MAGIC
# MAGIC La consulta es la misma función de 4.4, con `usar_broadcast=True`. El umbral automático sigue en `-1`,
# MAGIC así que el único cambio es el `broadcast`.

# COMMAND ----------

df_opt   = construir_conversion(usar_broadcast=True)
plan_opt = plan_texto(df_opt)
print("════════ PLAN DE EJECUCIÓN · DESPUÉS DE OPTIMIZAR ════════\n")
print(plan_opt)

# COMMAND ----------

DESC_OPT = "OPTIMIZADA · BroadcastHashJoin sobre propiedades"
t_opt = medir(DESC_OPT, lambda: construir_conversion(True).collect())
print(f"Mejora sobre la mediana: {(1 - t_opt / t_base) * 100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Qué cambió en el plan físico
# MAGIC
# MAGIC La tabla cuenta los operadores de los dos planes que se imprimieron arriba; se calcula sobre los planes
# MAGIC reales, no se escribe a mano.

# COMMAND ----------

rb, ro = resumen_plan(plan_base), resumen_plan(plan_opt)
display(spark.createDataFrame([(k, rb[k], ro[k]) for k in rb], "operador string, antes long, despues long"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Lectura del cambio.** La diferencia de fondo es la **estrategia de cruce**. Los **dos cruces por
# MAGIC `property_id`** (navegación con propiedades y reservas con propiedades) pasaron de `SortMergeJoin`
# MAGIC (reparte y ordena las dos tablas) a `BroadcastHashJoin` (copia la dimensión a cada máquina y cruza donde
# MAGIC están los hechos). Por eso, en la tabla de arriba, **desaparecen los `Exchange` y los `Sort` que existían
# MAGIC solo para preparar esos dos cruces**, y aparecen dos `BroadcastExchange` sobre `propiedades`, que son la
# MAGIC copia pequeña. Lo que **no** cambió: la unión final entre destinos sigue siendo un `SortMergeJoin` (no la
# MAGIC tocamos a propósito, para que la única diferencia sea el `broadcast` de la dimensión), y siguen los
# MAGIC `Exchange` de las agregaciones, del `countDistinct` y de la ventana. Por eso la mejora tiene un techo.
# MAGIC
# MAGIC **Por qué importa:** optimizar en Spark es, casi siempre, quitar o reducir un `Exchange`, porque es la
# MAGIC única operación que mueve datos por la red. Se quitó el movimiento de las tablas de hechos, que son las
# MAGIC grandes; el costo restante es el de agrupar, que la técnica no ataca.

# COMMAND ----------

# MAGIC %md
# MAGIC #### El resultado medido, sin adornos

# COMMAND ----------

m_b, m_o = MEDICIONES[DESC_BASE], MEDICIONES[DESC_OPT]
diferencia = m_b["mediana"] - m_o["mediana"]
ruido      = max(m_b["variacion"], m_o["variacion"])
mejora     = (1 - m_o["mediana"] / m_b["mediana"]) * 100

print(f"Mediana base       : {m_b['mediana']:.2f}s   (mediciones {[round(t, 2) for t in m_b['tiempos']]})")
print(f"Mediana optimizada : {m_o['mediana']:.2f}s   (mediciones {[round(t, 2) for t in m_o['tiempos']]})")
print(f"Diferencia         : {diferencia:+.2f}s  ({mejora:+.1f}%)")
print(f"Variación entre corridas (ruido): hasta {ruido:.2f}s\n")

if diferencia > ruido:
    veredicto = "La mejora es MAYOR que la variación entre corridas: es distinguible del ruido."
elif diferencia > 0:
    veredicto = ("La mejora es MENOR que la variación entre corridas: con estos datos no se puede afirmar "
                 "que el broadcast ayudó; el plan cambió, pero el tiempo no lo distingue del ruido.")
else:
    veredicto = "No hubo mejora: el tiempo no bajó. Se reporta igual y se explica abajo."
print("Veredicto:", veredicto)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cuándo esta optimización sería contraproducente
# MAGIC
# MAGIC Difundir significa **copiar la tabla completa a cada máquina**. Sirve mientras esa tabla sea pequeña.
# MAGIC En nuestro caso deja de servir o empeora si:
# MAGIC
# MAGIC 1. **La dimensión crece.** Hoy `propiedades` tiene 18.163 filas. Si el marketplace tuviera decenas de
# MAGIC    millones de propiedades, copiar la tabla a cada máquina costaría más que moverla una vez, y si no cabe
# MAGIC    en la memoria de cada ejecutor **la consulta no se pone lenta: falla**. La celda siguiente calcula
# MAGIC    cuántas veces podría crecer antes de acercarse al umbral automático de Spark.
# MAGIC 2. **Las dos tablas son grandes.** Si cruzáramos `clickstream` con `bookings` directamente (ambas de
# MAGIC    cientos de miles de filas y creciendo), ninguna es lo bastante pequeña para difundirla; ahí el
# MAGIC    `SortMergeJoin` es la estrategia correcta.
# MAGIC 3. **Hay poco volumen total.** Con ~100.000 filas por tabla, el tiempo lo dominan los costos fijos
# MAGIC    (planificar, arrancar tareas), no el movimiento de datos; la mejora se confunde con el ruido. Es lo que
# MAGIC    el veredicto de arriba permite comprobar.
# MAGIC 4. **Spark ya lo estaba haciendo solo.** Con la configuración por defecto, cualquier tabla de menos de
# MAGIC    10 MB se difunde automáticamente. Nuestra "mejora" existe **porque desactivamos esa decisión para
# MAGIC    poder medirla**; en un entorno real con la configuración por defecto, esta técnica sería redundante
# MAGIC    para `propiedades`.

# COMMAND ----------

try:
    detalle = spark.sql(f"DESCRIBE DETAIL {P}.propiedades").first()
    bytes_prop = int(detalle["sizeInBytes"])
    umbral = 10 * 1024 * 1024        # 10 MB, umbral por defecto de Spark para difundir sola una tabla
    filas_prop = conteos["plata.propiedades"]
    print(f"plata.propiedades   : {filas_prop:,} filas · {bytes_prop / 1024 / 1024:.2f} MB en disco")
    if bytes_prop > 0:
        print(f"Umbral automático   : 10 MB → la tabla podría crecer ~{umbral / bytes_prop:,.0f} veces "
              f"(~{filas_prop * umbral / bytes_prop:,.0f} filas) antes de superarlo")
    print("(Tamaño comprimido en disco; en memoria ocupa más, así que el límite real llega antes.)")
except Exception as e:
    print("No se pudo leer el tamaño de la tabla:", e)

# Restaurar la configuración original de Spark
try:
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    print("\nConfiguración restaurada.")
except Exception as e:
    print("No se pudo restaurar:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.7 Capa oro y consumo
# MAGIC
# MAGIC La capa oro es **una tabla que responde una pregunta concreta para alguien concreto** y que esa persona
# MAGIC entiende sin que estemos al lado:
# MAGIC
# MAGIC | Una buena capa oro se lee… | Cómo lo logramos |
# MAGIC |---|---|
# MAGIC | **Sin identificadores** | Destino y país por nombre, nunca `destination_id` ni `property_id` |
# MAGIC | **Sin calculadora** | `reservas_por_100_eventos`, `ticket_promedio` e `ingresos_confirmados` ya vienen calculados |
# MAGIC | **Sin buscar** | Ordenada por `posicion`: arriba los destinos que mejor convierten |
# MAGIC
# MAGIC Se guarda como tabla Delta (no solo se muestra), con comentarios de negocio en cada columna. Como vive en
# MAGIC el esquema `oro`, **hereda el `SELECT` que otorgamos en la EA2**.

# COMMAND ----------

TABLA_ORO = f"{O}.conversion_por_destino"

(construir_conversion(usar_broadcast=True)
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(TABLA_ORO))

spark.sql(f"COMMENT ON TABLE {TABLA_ORO} IS 'Conversion de navegacion a reservas por destino: cuanto interes genera cada destino, cuantas reservas se convierten y cuanto ingreso confirmado dejan'")
comentarios = {
    "posicion":                 "Lugar del destino segun sus reservas por cada 100 eventos de navegacion (1 = mejor)",
    "destino":                  "Nombre del destino turistico",
    "pais":                     "Pais del destino",
    "eventos_navegacion":       "Eventos de navegacion (vistas, clics, busquedas, filtros) sobre propiedades del destino",
    "visitantes_unicos":        "Usuarios distintos que navegaron propiedades del destino",
    "reservas":                 "Reservas no canceladas de propiedades del destino",
    "reservas_por_100_eventos": "Reservas por cada 100 eventos de navegacion; indicador aproximado de conversion",
    "ingresos_confirmados":     "Suma de los montos de reservas confirmadas o completadas",
    "ticket_promedio":          "Monto promedio por reserva no cancelada",
}
for col, txt in comentarios.items():
    spark.sql(f"ALTER TABLE {TABLA_ORO} ALTER COLUMN {col} COMMENT '{txt}'")

display(spark.table(TABLA_ORO))

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE {TABLA_ORO}"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Recorrido del dato: bronce → plata → oro
# MAGIC
# MAGIC Cuántos registros hay en cada capa y **a qué corresponde cada diferencia**. Se calcula sobre las tablas
# MAGIC reales para que cuadre.

# COMMAND ----------

oro   = spark.table(TABLA_ORO)
tot   = oro.agg(F.sum("eventos_navegacion").alias("ev"), F.sum("reservas").alias("rs"),
                F.count("*").alias("destinos")).first()
ev_oro, rs_oro, n_destinos = int(tot["ev"]), int(tot["rs"]), int(tot["destinos"])

n_click_p = conteos["plata.clickstream"]
n_bk_p    = conteos["plata.bookings"]
n_bk_nc   = bk.filter(F.col("status") != "cancelled").count()

filas = [
    ("Eventos de navegación", conteos["bronce.clickstream"], n_click_p, ev_oro,
     f"Plata conserva todo (solo se aplanó metadata). Oro pierde {n_click_p - ev_oro:,} eventos que no "
     f"se asocian a ninguna propiedad (property_id nulo, inexistente en propiedades o propiedad sin destino)."),
    ("Reservas", conteos["bronce.bookings"], n_bk_p, rs_oro,
     f"Plata no descartó nada (sin duplicados ni nulos críticos). Oro excluye {n_bk_p - n_bk_nc:,} "
     f"reservas canceladas (status = cancelled)"
     + (f" y {n_bk_nc - rs_oro:,} sin propiedad asociada." if n_bk_nc != rs_oro else ".")),
    ("Destinos (filas de la tabla oro)", None, None, n_destinos,
     "Una fila por destino con eventos de navegación; es el resultado de agrupar los cruces."),
]
display(spark.createDataFrame(filas, "dato string, bronce long, plata long, oro long, explicacion string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.8 Consumo de la capa oro (opcional, hasta +5 puntos)
# MAGIC
# MAGIC > ✍️ **COMPLETAR solo si lo hicieron; si no, borrar esta celda entera** (no hacerlo no resta nada).
# MAGIC >
# MAGIC > - **Opción A · Tablero:** al menos tres visualizaciones sobre `conversion_por_destino` que respondan la
# MAGIC >   pregunta de negocio. *(Sugeridas: barras de `reservas_por_100_eventos` por destino; dispersión de
# MAGIC >   `eventos_navegacion` contra `reservas`; barras de `ingresos_confirmados`.)* Insertar la captura.
# MAGIC > - **Opción B · Genie:** espacio de Genie sobre la tabla, con tres preguntas de negocio en lenguaje natural
# MAGIC >   y las capturas de las respuestas, indicando **qué tuvieron que ajustar** (nombres, descripciones,
# MAGIC >   instrucciones). Los comentarios de columna de 4.7 ya están escritos en lenguaje de negocio para esto.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Resultados
# MAGIC
# MAGIC **Pregunta del negocio:** ¿qué destinos convierten mejor el interés de navegación en reservas, y cuánto
# MAGIC ingreso dejan?
# MAGIC
# MAGIC La celda siguiente lee la tabla oro y redacta la respuesta con las cifras reales, para que el HTML
# MAGIC exportado siempre coincida con lo ejecutado. Recordatorio de lectura: `reservas_por_100_eventos` es un
# MAGIC **indicador aproximado**: divide reservas entre eventos por destino, pero los eventos y las reservas son
# MAGIC muestras independientes del dataset, no el embudo de una misma sesión.

# COMMAND ----------

oro = spark.table(TABLA_ORO)
top3   = oro.orderBy("posicion").limit(3).collect()
ultimo = oro.orderBy(F.desc("posicion")).first()
mas_ingreso = oro.orderBy(F.desc("ingresos_confirmados")).first()
mas_trafico = oro.orderBy(F.desc("eventos_navegacion")).first()

print("Mejor conversión (reservas por 100 eventos de navegación):")
for r in top3:
    print(f"   {r['posicion']}. {r['destino']} ({r['pais']}): {r['reservas_por_100_eventos']} reservas por 100 eventos "
          f"· {r['reservas']:,} reservas · {r['eventos_navegacion']:,} eventos")
print(f"\nPeor conversión: {ultimo['destino']} ({ultimo['pais']}) con {ultimo['reservas_por_100_eventos']} por 100 eventos.")
print(f"Más ingreso confirmado: {mas_ingreso['destino']} con {mas_ingreso['ingresos_confirmados']:,.0f}.")
print(f"Más navegación: {mas_trafico['destino']} con {mas_trafico['eventos_navegacion']:,} eventos "
      f"(posición {mas_trafico['posicion']} en conversión).")

# COMMAND ----------

# MAGIC %md
# MAGIC | Pregunta del negocio | Respuesta obtenida |
# MAGIC |---|---|
# MAGIC | ¿Qué destinos convierten mejor la navegación en reservas y cuánto ingreso dejan? | La tabla `oro.conversion_por_destino`, ordenada por `posicion`; las cifras exactas están en la salida de la celda anterior |
# MAGIC | ¿Qué tan rápido se calcula? | Tiempos medidos con calentamiento, tres corridas y mediana: ver 4.5 y 4.6 |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Conclusiones
# MAGIC ### 6.1 Cierre del caso: la pregunta orientadora
# MAGIC
# MAGIC > *Una empresa recibe datos de su portal web, de Facebook, de Instagram, de TikTok y de WhatsApp Business.
# MAGIC > ¿Bajo qué paradigma, con qué herramientas y con qué arquitectura debería analizarlos?*
# MAGIC
# MAGIC **Paradigma: un *lakehouse* con procesamiento distribuido.** Ni una base relacional pura ni un almacén
# MAGIC NoSQL solo. Las cinco fuentes no llegan igual: el portal entrega **eventos** (como nuestro `clickstream`),
# MAGIC las redes sociales entregan **respuestas anidadas cuyo formato pueden cambiar sin avisar**, y WhatsApp
# MAGIC trae **texto libre**. En nuestro caso, el struct `metadata` del clickstream nos obligó a guardar el dato
# MAGIC crudo en bronce y aplanarlo después en plata; un modelo relacional puro habría exigido fijar el esquema
# MAGIC antes de recibir el dato. Delta permite lo contrario: guardar primero y tipar después, con transacciones
# MAGIC ACID (la EA1 lo demostró con un `UPDATE`) y evolución de esquema cuando una red cambie su formato.
# MAGIC
# MAGIC **Herramientas.** Spark para procesar, porque el cruce de navegación con negocio que medimos en 4.5 y 4.6
# MAGIC es el patrón que se repetiría con cada red; Delta Lake para almacenar; Unity Catalog para permisos y
# MAGIC linaje, como en la EA2; y Jobs para orquestar. Para la ingesta continua propondríamos Auto Loader (es una
# MAGIC propuesta: no lo implementamos, el enunciado no lo pide). El texto libre de WhatsApp se guardaría crudo en
# MAGIC bronce y solo se procesaría con técnicas de lenguaje natural en una capa posterior.
# MAGIC
# MAGIC **Arquitectura.** Medallón `bronce → plata → oro` con gobierno por capa, como en la EA2: **bronce cerrado**
# MAGIC (números de teléfono, cuentas y mensajes son datos personales), plata para ciencia de datos y oro abierto
# MAGIC al negocio. Las fuentes se unifican en plata, con un identificador común de usuario o campaña, y oro
# MAGIC responde preguntas como la nuestra.
# MAGIC
# MAGIC **El costo que asumimos.** Ninguna arquitectura es gratis. Cada capa duplica almacenamiento y agrega
# MAGIC complejidad de operación. El pipeline de la EA2 reescribe todo cada noche, y eso no escalaría a redes
# MAGIC sociales sin cargas incrementales. Y lo medido en 4.6 lo confirma a escala pequeña: optimizar solo se
# MAGIC nota cuando el volumen lo justifica; con ~100.000 filas la diferencia se acerca al ruido, y medirla ya
# MAGIC consume cuota de cómputo.

# COMMAND ----------

print("CIFRAS QUE SUSTENTAN LAS CONCLUSIONES")
print("─" * 60)
print(f"Volumen: clickstream {conteos['plata.clickstream']:,} filas · bookings {conteos['plata.bookings']:,} · propiedades {conteos['plata.propiedades']:,}")
print(f"Tiempo base       : {t_base:.2f}s (mediana de 3 mediciones, tras calentamiento)")
print(f"Tiempo optimizado : {t_opt:.2f}s")
print(f"Cambio            : {mejora:+.1f}%  ·  ruido entre corridas: hasta {ruido:.2f}s")
print(f"Operadores        : SortMergeJoin {rb['SortMergeJoin']} → {ro['SortMergeJoin']} · "
      f"BroadcastHashJoin {rb['BroadcastHashJoin']} → {ro['BroadcastHashJoin']} · "
      f"Exchange {rb['Exchange (mueve datos entre máquinas)']} → {ro['Exchange (mueve datos entre máquinas)']}")
print(f"Lectura           : {veredicto}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Conclusiones del proyecto completo
# MAGIC
# MAGIC **Qué funcionó.**
# MAGIC
# MAGIC - **Reutilizar las capas de la EA2 fue lo correcto.** La capa oro nueva quedó dentro del mismo catálogo,
# MAGIC   hereda los permisos ya otorgados y su linaje se captura solo. No duplicamos datos ni gobierno.
# MAGIC - **Escribir una sola función para la consulta base y la optimizada** garantizó que la comparación fuera
# MAGIC   justa: lo único que cambia es el `broadcast`. Y mantener el umbral automático en `-1` durante las dos
# MAGIC   mediciones aisló esa única variable.
# MAGIC - **Medir con calentamiento, tres corridas y mediana** evitó juzgar por una sola ejecución. El veredicto
# MAGIC   compara la diferencia contra la variación entre corridas, no contra cero.
# MAGIC - **El plan de ejecución explica el resultado.** En los dos cruces por `property_id`, el cambio de
# MAGIC   `SortMergeJoin` a `BroadcastHashJoin` quitó el movimiento de las tablas de hechos; los `Exchange` de las
# MAGIC   agregaciones y de la unión final siguen, y eso fija el techo de la mejora.
# MAGIC
# MAGIC **Qué no funcionó o quedó limitado.**
# MAGIC
# MAGIC - **El volumen es pequeño.** Con ~100.000 filas por tabla, el tiempo lo dominan los costos fijos y la
# MAGIC   mejora puede quedar cerca del ruido (ver la lectura de 6.1). Esto no es un fallo de la técnica sino una
# MAGIC   propiedad de los datos, y así se reporta.
# MAGIC - **La ganancia depende de una decisión que tomamos nosotros:** desactivar el difundido automático para el
# MAGIC   caso base. Con la configuración por defecto de Spark, `propiedades` ya se habría difundido sola.
# MAGIC - **`reservas_por_100_eventos` es un indicador aproximado**, no una conversión real por sesión, porque
# MAGIC   navegación y reservas son muestras independientes del dataset.
# MAGIC - **Medir consumió cuota** (ocho ejecuciones de la misma consulta), lo que obliga a no dejarlo para el
# MAGIC   final.
# MAGIC
# MAGIC **Qué haríamos distinto si empezáramos de nuevo.**
# MAGIC
# MAGIC 1. **Elegir una operación más pesada desde el inicio**, por ejemplo cruzando `clickstream` con reseñas y
# MAGIC    reservas a la vez, o con más datos, para que la mejora se distinga del ruido con más claridad.
# MAGIC 2. **Medir también con el umbral automático activo**, para reportar cuánto de la mejora la habría dado
# MAGIC    Spark gratis.
# MAGIC 3. **Pasar de reescribir todo cada noche a cargas incrementales**, como concluimos en la EA2, porque el
# MAGIC    costo de recalcular crece con el volumen.
# MAGIC
# MAGIC > ✍️ **COMPLETAR (una frase, opcional pero recomendable):** qué les sorprendió del número que midieron,
# MAGIC > o cualquier fallo real que tuvieron al ejecutar (por ejemplo, un error de cuota o de configuración).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Reparto del trabajo y uso de IA
# MAGIC
# MAGIC | Integrante | De qué se encargó | Qué sustenta en el video |
# MAGIC |---|---|---|
# MAGIC | Ronal Mosquera | Datos y calidad (secciones 2, 4.2 y 4.3), capa oro y recorrido del dato (4.4 y 4.7), y la pregunta orientadora (6.1) | La tabla oro y por qué sus columnas se leen solas, el recorrido bronce → plata → oro con sus conteos, y la respuesta a la pregunta orientadora |
# MAGIC | Federico López Torres | Medición y optimización (4.1, 4.5 y 4.6), planes de ejecución, límites de la técnica y conclusiones (6.2) | Los dos planes de ejecución en pantalla, las tres mediciones con su mediana, por qué mejoró (o no) y cuándo sería contraproducente |
# MAGIC
# MAGIC > ⚠️ **Ajustar este reparto al real antes de entregar.** Debe coincidir con lo que cada persona demuestra
# MAGIC > en el video y con el historial de commits. **Aun así, cada integrante debe poder mostrar los planes y
# MAGIC > responder las tres preguntas obligatorias**, porque las dos primeras se responden por persona.
# MAGIC
# MAGIC **Uso de asistentes de IA:** se usó Claude (asistente de IA de Anthropic) para estructurar el notebook,
# MAGIC redactar los textos de las secciones, escribir el código de las celdas (función de construcción de la capa
# MAGIC oro, captura y conteo de operadores de los planes, y análisis de las mediciones) y proponer la lectura de
# MAGIC los planes. Los datos provienen de `samples.wanderbricks` a través de las capas de la EA2. Cada integrante
# MAGIC debe poder explicar cualquier línea del código en el video.
# MAGIC
# MAGIC > Este reparto debe coincidir con lo que cada persona demuestra en el video y con el
# MAGIC > historial de commits del repositorio. Las tres fuentes se contrastan al calificar.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📹 Preguntas obligatorias de sustentación
# MAGIC
# MAGIC Cada integrante responde estas tres preguntas en su intervención del video. Ideas de referencia (con sus
# MAGIC palabras, mostrando los planes y las cifras en pantalla, no capturas):
# MAGIC
# MAGIC **1. Muestre el plan de ejecución antes y después de la optimización, y explique qué cambió.**
# MAGIC Mostrar `plan_base` y `plan_opt` de la sección 4.5 y 4.6 ejecutándose, y la tabla de operadores. Lo que
# MAGIC cambió: los dos cruces por `property_id`, que eran `SortMergeJoin` con `Exchange` sobre las dos tablas,
# MAGIC pasaron a `BroadcastHashJoin`, con un `BroadcastExchange` solo sobre `propiedades`. La tabla de hechos
# MAGIC dejó de moverse; siguen los `Exchange` de las agregaciones y la unión final sigue siendo `SortMergeJoin`.
# MAGIC
# MAGIC **2. ¿En qué caso esa misma optimización sería contraproducente?**
# MAGIC Si la dimensión creciera: hoy `propiedades` tiene 18.163 filas (la celda de 4.6 calcula su tamaño en MB y
# MAGIC cuánto podría crecer), y difundir una tabla grande copia mucho a cada máquina y puede no caber en memoria,
# MAGIC con lo que la consulta falla. También si las dos tablas fueran grandes, o si el volumen total fuera tan
# MAGIC pequeño que la diferencia no se distingue del ruido, como en nuestro caso.
# MAGIC
# MAGIC **3. Responda la pregunta orientadora en menos de un minuto.**
# MAGIC *Paradigma:* lakehouse con procesamiento distribuido, porque las fuentes mezclan eventos, datos anidados
# MAGIC que cambian de formato y texto libre. *Herramientas:* Spark, Delta Lake, Unity Catalog y Jobs, con Auto
# MAGIC Loader como propuesta de ingesta. *Arquitectura:* medallón bronce, plata y oro con gobierno por capa
# MAGIC (bronce cerrado por datos personales). *Costo:* complejidad y cómputo; lo medido muestra que optimizar
# MAGIC solo se nota cuando el volumen lo justifica.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Antes de entregar
# MAGIC
# MAGIC - [ ] El notebook corre completo de arriba abajo sin errores
# MAGIC - [ ] La capa oro existe y sus columnas se entienden sin explicación
# MAGIC - [ ] Las tres mediciones están visibles, antes y después, con la mediana reportada
# MAGIC - [ ] Los dos planes de ejecución están en el notebook, con la salida visible
# MAGIC - [ ] Está escrito por qué mejoró (o por qué no) y cuándo la técnica sería contraproducente
# MAGIC - [ ] La pregunta orientadora está respondida, apoyada en lo que construyeron
# MAGIC - [ ] La sección 7 declara qué hizo cada integrante, y coincide con lo que cada uno sustenta en el video
# MAGIC - [ ] El enlace del video está en la portada y abre desde otra cuenta
# MAGIC - [ ] No quedan marcas ⚠️ / ✍️ sin completar
# MAGIC - [ ] El notebook `.ipynb` está confirmado en el repositorio, carpeta `/ea3`
# MAGIC - [ ] El HTML con salidas visibles está subido a Canvas
# MAGIC - [ ] Opcional: capturas del tablero o del espacio de Genie (sección 4.8)