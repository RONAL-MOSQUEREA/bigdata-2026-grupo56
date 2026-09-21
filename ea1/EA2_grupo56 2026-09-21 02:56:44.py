# Databricks notebook source
# MAGIC %md
# MAGIC # EA2 — Despliegue y gobierno de una infraestructura de datos en la nube
# MAGIC
# MAGIC **Big Data (ISD-25)** · Ingeniería de Software y Datos · IU Digital de Antioquia
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Grupo** | 56 |
# MAGIC | **Integrantes** | Ronal Mosquera · Federico López Torres · *(tercer integrante, si aplica)* |
# MAGIC | **Caso de estudio** | Wanderbricks (marketplace de alquiler vacacional) |
# MAGIC | **Fecha de entrega** | domingo 6 de septiembre |
# MAGIC | **🎥 Enlace al video** | *(pegar aquí — 6 a 9 minutos, mínimo 3 minutos por integrante)* |
# MAGIC | **Repositorio** | https://github.com/RONAL-MOSQUEREA/bigdata-2026-grupo56 (carpeta `/ea2`) |
# MAGIC
# MAGIC > ⚠️ **Antes de entregar:** verificar que el enlace del video abra desde una cuenta distinta a la propia.
# MAGIC > Un enlace inaccesible se califica como no entregado.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Contexto y problema
# MAGIC
# MAGIC En la EA1 diseñamos una base analítica para **Wanderbricks**, un marketplace de alquiler vacacional que
# MAGIC conecta viajeros (`users`) con anfitriones (`hosts`) que publican propiedades (`properties`) en distintos
# MAGIC destinos (`destinations`). Esa base quedó en un único esquema, con las capas bronce y plata mezcladas
# MAGIC mediante prefijos (`bronze_`, `silver_`) y ejecutada a mano, celda por celda. Sirve para explorar, pero no
# MAGIC para operar: **no se puede dar acceso a las métricas sin exponer también los datos crudos, nadie sabe qué
# MAGIC tabla depende de cuál, y todo depende de que alguien ejecute el notebook.**
# MAGIC
# MAGIC La EA2 plantea la necesidad de infraestructura que esa base todavía no cubre. El entorno debe soportar:
# MAGIC (a) una **separación por capas con permisos diferenciados**, porque los datos crudos contienen información
# MAGIC personal (`users.email`, `users.name`, `hosts.email`, `hosts.phone`) que un analista de negocio no necesita;
# MAGIC (b) **trazabilidad** de cada tabla hasta su origen; (c) una **ejecución automática y programada** con
# MAGIC dependencia entre pasos, para no transformar datos que no se ingirieron bien; y (d) un modelo de gobierno
# MAGIC único para datos tabulares (`bookings`, `payments`) y semiestructurados (`clickstream.metadata`). Además, el
# MAGIC equipo son tres estudiantes sin personal de infraestructura, así que el costo en reposo y el esfuerzo de
# MAGIC operación pesan tanto como la funcionalidad.
# MAGIC
# MAGIC La capa oro debe seguir respondiendo las preguntas de negocio de la EA1: ¿qué destinos generan más
# MAGIC ingresos?, ¿qué propiedades están mejor calificadas?, ¿cuál es la tasa de cancelación?, ¿desde qué
# MAGIC dispositivo se navega más? y ¿cómo evoluciona el valor promedio de reserva? La pregunta de infraestructura
# MAGIC que resolvemos en este trabajo es **qué modelo de servicio (IaaS, PaaS o SaaS) conviene para desplegar y
# MAGIC gobernar todo esto, y qué habría que montar si no usáramos una plataforma gestionada.**

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Descripción de los datos
# MAGIC
# MAGIC Lo que va a vivir en esta infraestructura son **nueve tablas de `samples.wanderbricks`**, con
# MAGIC **566.844 filas en total** (conteos obtenidos en la EA1 y verificados de nuevo en la celda siguiente):
# MAGIC
# MAGIC | Tabla | Filas | Naturaleza | Sensibilidad |
# MAGIC |---|---:|---|---|
# MAGIC | `users` | 124.509 | Tabular · dimensión | **Personal** (correo, nombre) |
# MAGIC | `hosts` | 19.384 | Tabular · dimensión | **Personal** (correo, teléfono) |
# MAGIC | `properties` | 18.163 | Tabular · dimensión | Baja |
# MAGIC | `destinations` | 42 | Tabular · dimensión | Baja |
# MAGIC | `bookings` | 72.247 | Tabular · **hechos** | Media (montos, fechas) |
# MAGIC | `payments` | 49.638 | Tabular · hechos | Media (montos, método de pago) |
# MAGIC | `booking_updates` | 83.068 | **CDC**: un cambio de estado por fila | Media |
# MAGIC | `reviews` | 99.793 | Tabular con borrado lógico (`is_deleted`) | Baja |
# MAGIC | `clickstream` | 100.000 | **Semiestructurada**: struct `metadata` (dispositivo, referrer) | Baja |
# MAGIC
# MAGIC Quedan fuera de este trabajo `page_views` (500.000 filas) y `customer_support_logs` (1.900), que existen en
# MAGIC la fuente pero no alimentan ninguna de las cinco preguntas de negocio.
# MAGIC
# MAGIC **Volumen y crecimiento.** Hoy el conjunto es pequeño (del orden de cientos de miles de filas), pero crece
# MAGIC rápido: las reservas por mes de check-in pasan de 1 en diciembre de 2022 a 25.708 en julio de 2025, según la
# MAGIC consulta 5 de la EA1. Ese ritmo es lo que justifica diseñar la infraestructura para escalar y no solo para
# MAGIC el volumen actual.
# MAGIC
# MAGIC **Frecuencia de actualización (propuesta para producción).** La fuente de este curso es una muestra fija,
# MAGIC pero en un caso real `bookings`, `payments` y `booking_updates` cambian a diario, `clickstream` llega de forma
# MAGIC continua y se procesa en lote diario, y las dimensiones (`users`, `hosts`, `properties`, `destinations`)
# MAGIC cambian poco. Por eso el Job se programa **una vez al día, de madrugada** (2:00 a. m., hora de Bogotá).
# MAGIC
# MAGIC **Quién la consume.**
# MAGIC
# MAGIC | Consumidor | Qué necesita | Capa |
# MAGIC |---|---|---|
# MAGIC | Analista de negocio | Métricas ya calculadas, sin datos personales | Oro |
# MAGIC | Científico de datos | Detalle limpio para explorar y modelar | Plata y oro |
# MAGIC | Ingeniero de datos | Construir y corregir el pipeline completo | Bronce, plata y oro |
# MAGIC | Administrador | Operar el entorno y los permisos | Todas |

# COMMAND ----------

# Verificación de volumen en la fuente
ORIGEN = "samples.wanderbricks"
TABLAS = ["users", "hosts", "properties", "destinations", "bookings",
          "payments", "booking_updates", "reviews", "clickstream"]

total = 0
for t in TABLAS:
    n = spark.table(f"{ORIGEN}.{t}").count()
    total += n
    print(f"{t:<18}{n:>12,}")
print("-" * 30)
print(f"{'TOTAL':<18}{total:>12,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Decisiones de diseño y justificación
# MAGIC ### 3.1 Diagrama de la arquitectura
# MAGIC
# MAGIC El flujo completo, de la fuente al consumo. Cada caja indica **qué administra el proveedor y qué
# MAGIC administramos nosotros** en esa capa:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │ 1 · FUENTES  ·  samples.wanderbricks                                                     │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │ 9 tablas: users, hosts, properties, destinations, bookings, payments,                    │
# MAGIC │ booking_updates, reviews y clickstream (con struct anidado metadata)                     │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │ PROVEEDOR: el dataset de muestra y su almacenamiento físico                              │
# MAGIC │ EQUIPO   : elegir qué tablas entran y con qué criterio                                   │
# MAGIC └─────────────────────────────────────────────┬────────────────────────────────────────────┘
# MAGIC                                               │
# MAGIC                                               ▼
# MAGIC ┌──────────────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │ 2 · INGESTA  ·  Job, tarea 1: ingesta_bronce                                             │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │ Notebook PySpark: lee cada tabla fuente, agrega _ingesta_ts y _fuente                    │
# MAGIC │ y la escribe como Delta. Si una carga llega vacía, el Job se detiene.                    │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │ PROVEEDOR: cómputo serverless y el orquestador de Jobs                                   │
# MAGIC │ EQUIPO   : el código del notebook y la dependencia con la tarea 2                        │
# MAGIC └─────────────────────────────────────────────┬────────────────────────────────────────────┘
# MAGIC                                               │
# MAGIC                                               ▼
# MAGIC ┌──────────────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │ 3 · ALMACENAMIENTO  ·  Unity Catalog, catálogo bigdata_grupo56                           │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │ bronce (crudo, con datos personales) → plata (limpio) → oro (métricas)                   │
# MAGIC │ volumen bronce.datos_crudos: zona de aterrizaje de archivos                              │
# MAGIC │ formato Delta: transacciones ACID, historial y time travel                               │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │ PROVEEDOR: object storage en la nube, Delta/Parquet, metastore, linaje, auditoría        │
# MAGIC │ EQUIPO   : esquemas, volumen, comentarios y permisos (GRANT)                             │
# MAGIC └─────────────────────────────────────────────┬────────────────────────────────────────────┘
# MAGIC                                               │
# MAGIC                                               ▼
# MAGIC ┌──────────────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │ 4 · PROCESAMIENTO  ·  Job, tareas 2 y 3: transformar_plata, agregar_oro                  │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │ Limpieza, tipado, deduplicación, aplanado del struct, joins y agregaciones.              │
# MAGIC │ Cada tarea corre solo si la anterior terminó bien.                                       │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │ PROVEEDOR: motor Spark, sistema operativo, versiones, escalado y apagado automático      │
# MAGIC │ EQUIPO   : reglas de negocio (dedupe, is_deleted, cancelaciones) y programación diaria   │
# MAGIC └─────────────────────────────────────────────┬────────────────────────────────────────────┘
# MAGIC                                               │
# MAGIC                                               ▼
# MAGIC ┌──────────────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │ 5 · CONSUMO  ·  capa oro y Catalog Explorer                                              │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │ Analistas: SELECT sobre oro (SQL o notebooks). Ingeniería y ciencia: plata.              │
# MAGIC │ Linaje visible en el Catalog Explorer, sin documentarlo a mano.                          │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │ PROVEEDOR: interfaz web, editor SQL y Catalog Explorer                                   │
# MAGIC │ EQUIPO   : qué se publica en oro y quién puede leerlo                                    │
# MAGIC └──────────────────────────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC Y la misma división de responsabilidades, vista de forma global:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │ LO QUE ADMINISTRA EL EQUIPO (nosotros)                                                   │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │ · Estructura: catálogo, esquemas bronce/plata/oro, volumen, comentarios                  │
# MAGIC │ · Gobierno: quién puede ver o modificar qué (GRANT)                                      │
# MAGIC │ · Lógica: notebooks PySpark de ingesta, limpieza y agregación                            │
# MAGIC │ · Programación: el Job, sus dependencias y su horario                                    │
# MAGIC │ · Definición de las métricas de la capa oro                                              │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │ LO QUE ADMINISTRA EL PROVEEDOR (Databricks y la nube)                                    │
# MAGIC ├──────────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │ · Cómputo serverless: máquinas, sistema operativo, arranque y apagado                    │
# MAGIC │ · Motor Spark y sus versiones, escalado automático                                       │
# MAGIC │ · Almacenamiento físico (object storage) y formato Delta/Parquet                         │
# MAGIC │ · Unity Catalog: metastore, captura de linaje y auditoría                                │
# MAGIC │ · Orquestador de Jobs, interfaz web, disponibilidad y parches de seguridad               │
# MAGIC └──────────────────────────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Cómo se lee.** El cómputo, el sistema operativo, el motor Spark y el almacenamiento los administra el
# MAGIC proveedor: nosotros nunca elegimos máquinas ni instalamos software. Lo que sí es nuestro es todo lo que
# MAGIC decide *qué datos hay, quién los ve y cuándo se procesan*: la estructura del catálogo, los permisos, la
# MAGIC lógica de transformación y la programación. Esa frontera es la que hace de esto un servicio **PaaS**:
# MAGIC recibimos la plataforma lista y ponemos la lógica y el gobierno.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Matriz de roles
# MAGIC
# MAGIC Matriz diseñada para un entorno real de Wanderbricks. **`—` significa sin acceso.**
# MAGIC
# MAGIC | Rol | Bronce | Plata | Oro |
# MAGIC |---|---|---|---|
# MAGIC | **Analista de negocio** | — | — | `SELECT` |
# MAGIC | **Científico de datos** | — | `SELECT` | `SELECT` |
# MAGIC | **Ingeniero de datos** | `SELECT`, `MODIFY` | `SELECT`, `MODIFY` | `SELECT`, `MODIFY` |
# MAGIC | **Administrador** | `ALL PRIVILEGES` | `ALL PRIVILEGES` | `ALL PRIVILEGES` |
# MAGIC
# MAGIC Todos los roles necesitan además `USE CATALOG` sobre `bigdata_grupo56` y `USE SCHEMA` sobre cada esquema al que
# MAGIC accedan: sin esas dos llaves el `SELECT` no sirve de nada.
# MAGIC
# MAGIC **Las razones, que es lo que realmente sustenta la matriz:**
# MAGIC
# MAGIC 1. **El analista solo entra a oro.** Bronce guarda los datos tal como llegan, incluidos correos, nombres y
# MAGIC    teléfonos de `users` y `hosts`. Menos personas con acceso a datos personales es menos riesgo, y un
# MAGIC    analista de negocio no necesita el detalle crudo para responder ingresos por destino.
# MAGIC 2. **El científico lee plata y oro, pero no bronce ni escribe nada.** Necesita el detalle limpio para
# MAGIC    explorar y modelar, pero no los datos personales sin depurar; y un experimento no debería poder alterar
# MAGIC    las tablas de las que otros dependen.
# MAGIC 3. **Solo ingeniería tiene `MODIFY`.** Las reglas de negocio viven en plata (deduplicar por `booking_id`,
# MAGIC    excluir reseñas con `is_deleted`, marcar cancelaciones). Si dos personas las cambian sin coordinarse, los
# MAGIC    ingresos dejan de cuadrar entre reportes.
# MAGIC 4. **Oro se escribe únicamente desde el Job.** Así una métrica como «ingresos» tiene una sola definición.
# MAGIC    Esto importa de verdad en este caso: el 43,7 % de las reservas está en estado `pending`, y contarlas o
# MAGIC    no como ingreso cambia el resultado (ver sección 5).
# MAGIC 5. **El administrador es un rol, no una persona de uso diario.** Tiene todo, pero no ejecuta el pipeline;
# MAGIC    eso lo hace el Job, y el administrador queda para cambiar permisos y resolver incidentes.
# MAGIC
# MAGIC **Qué se ejecutó y qué solo se diseñó.** La edición gratuita de Databricks solo tiene dos grupos, `admins`
# MAGIC y `users`, así que no se pueden crear los cuatro roles como grupos reales. En la sección 4.2 ejecutamos los
# MAGIC `GRANT` contra `account users` como sustituto. En un entorno real las sentencias serían así:
# MAGIC
# MAGIC ```sql
# MAGIC GRANT SELECT          ON SCHEMA bigdata_grupo56.oro    TO `analistas_negocio`;
# MAGIC GRANT SELECT          ON SCHEMA bigdata_grupo56.plata  TO `ciencia_datos`;
# MAGIC GRANT SELECT          ON SCHEMA bigdata_grupo56.oro    TO `ciencia_datos`;
# MAGIC GRANT SELECT, MODIFY  ON SCHEMA bigdata_grupo56.bronce TO `ingenieria_datos`;
# MAGIC GRANT SELECT, MODIFY  ON SCHEMA bigdata_grupo56.plata  TO `ingenieria_datos`;
# MAGIC GRANT SELECT, MODIFY  ON SCHEMA bigdata_grupo56.oro    TO `ingenieria_datos`;
# MAGIC GRANT ALL PRIVILEGES  ON CATALOG bigdata_grupo56       TO `admin_datos`;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Especificación del equivalente IaaS
# MAGIC
# MAGIC *Se diseña, no se implementa.* Es lo que tendríamos que montar y mantener nosotros si en lugar de una
# MAGIC plataforma gestionada alquiláramos máquinas virtuales. Todo lo de esta lista **existe hoy en Databricks,
# MAGIC solo que no lo vemos**.
# MAGIC
# MAGIC **Topología propuesta**
# MAGIC
# MAGIC ```
# MAGIC Internet / VPN ──► [Bastión: única entrada, SSH con llaves]
# MAGIC                           │
# MAGIC                           ▼
# MAGIC                   VPC privada 10.0.0.0/16 · subred privada 10.0.1.0/24
# MAGIC                           ├── spark-master    (4 vCPU · 16 GB RAM · 100 GB SSD)
# MAGIC                           ├── spark-worker-1  (8 vCPU · 32 GB RAM · 200 GB SSD)
# MAGIC                           ├── spark-worker-2  (8 vCPU · 32 GB RAM · 200 GB SSD)
# MAGIC                           ├── servicios       (4 vCPU · 16 GB RAM · 100 GB SSD)
# MAGIC                           │                   PostgreSQL (metastore) + Airflow (orquestador)
# MAGIC                           └── almacenamiento de objetos (bucket, 500 GB, con versionado)
# MAGIC ```
# MAGIC
# MAGIC **Especificación**
# MAGIC
# MAGIC | Componente | Especificación | Justificación |
# MAGIC |---|---|---|
# MAGIC | Máquinas | 4 VM: 1 coordinadora Spark, 2 trabajadoras, 1 de servicios | El motor necesita un coordinador y al menos dos ejecutores para repartir el trabajo; los servicios auxiliares van aparte para no competir por memoria |
# MAGIC | Dimensionamiento | Trabajadoras de 8 vCPU / 32 GB | Hoy los datos son de cientos de miles de filas (sobrado); el tamaño deja margen para crecer unas 10 veces sin rediseñar |
# MAGIC | Sistema operativo | Ubuntu Server 22.04 LTS en todas | Soporte de largo plazo y paquetes estables |
# MAGIC | Software | Java 17, Python 3.11, Apache Spark 3.5 y Delta Lake, con **versiones compatibles entre sí en cada máquina** | Sin esto Spark no arranca o falla en tiempo de ejecución |
# MAGIC | Metastore | PostgreSQL en la VM de servicios | Sustituye a Unity Catalog: guarda qué tablas existen y dónde |
# MAGIC | Orquestación | Apache Airflow en la VM de servicios | Sustituye al Job: dependencias, horario y reintentos |
# MAGIC | Almacenamiento | Bucket de objetos con versionado, 500 GB | Sustituye al almacenamiento gestionado; requiere credenciales y políticas de acceso |
# MAGIC | Red | VPC privada, subred privada, bastión, grupos de seguridad y TLS | Las VM no deben quedar expuestas a internet |
# MAGIC | Gobierno | Permisos por rol con una herramienta aparte (p. ej. Apache Ranger) o ACL manuales | Unity Catalog ya lo trae; aquí se construye |
# MAGIC | Linaje | **No existe de forma automática.** Habría que integrar OpenLineage o documentarlo a mano | En Databricks se captura solo |
# MAGIC | Apagado automático | **No existe.** Las máquinas cobran encendidas aunque nadie las use | El serverless se apaga solo |
# MAGIC
# MAGIC **Estimación del esfuerzo de puesta en marcha** *(criterio del grupo, orden de magnitud; no es una medición)*
# MAGIC
# MAGIC | Tarea | Días-persona |
# MAGIC |---|---:|
# MAGIC | Red, VPC, bastión y creación de las VM | 2 |
# MAGIC | Instalar y compatibilizar Java, Python, Spark y Delta en cada VM | 2 |
# MAGIC | Almacenamiento de objetos, credenciales y políticas | 2 |
# MAGIC | Metastore y conexión con Spark | 2 |
# MAGIC | Modelo de permisos por rol | 3 a 5 |
# MAGIC | Orquestador (instalación, DAGs, alertas) | 2 a 3 |
# MAGIC | Linaje, monitoreo, logs y respaldos | 3 a 5 |
# MAGIC | Pruebas y endurecimiento de seguridad | 2 |
# MAGIC | **Total** | **18 a 24 (4 a 5 semanas de una persona)** |
# MAGIC
# MAGIC **Esfuerzo de operación mensual.** Entre un cuarto y media persona dedicada a parches del sistema,
# MAGIC actualizaciones de Spark, rotación de credenciales, respaldos y atención de incidentes. Con las VM
# MAGIC encendidas 24/7, el costo de cómputo ronda **entre USD 800 y 1.000 al mes** (orden de magnitud con precios
# MAGIC de lista de una nube pública; debe verificarse en el calculador del proveedor). En la plataforma gestionada
# MAGIC la puesta en marcha tomó minutos y el costo en reposo es casi cero.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Comparación IaaS / PaaS / SaaS
# MAGIC
# MAGIC | Criterio | IaaS (VM propias) | PaaS (Databricks, lo que usamos) | SaaS (herramienta de BI ya hecha) |
# MAGIC |---|---|---|---|
# MAGIC | **Control** | Total: sistema operativo, versiones, red, cada configuración | Medio: controlamos datos, permisos, lógica y programación; no el motor ni el SO | Mínimo: solo se consume lo que la herramienta ofrece |
# MAGIC | **Tiempo hasta el primer resultado** | Semanas (18 a 24 días-persona de montaje antes de correr una consulta) | Minutos: el cómputo aparece al ejecutar la primera celda | Horas o días, pero solo si los datos ya están limpios en algún lado |
# MAGIC | **Esfuerzo operativo** | Alto: parches, actualizaciones, respaldos y vigilancia permanentes | Casi nulo: el proveedor administra la infraestructura | Nulo, pero también sin capacidad de construir pipelines |
# MAGIC | **Costo** | Fijo y alto: se paga por máquina encendida, se use o no | Variable: se paga por trabajo real (DBU); en la edición gratuita hay una cuota diaria | Por licencia o por usuario, predecible pero recurrente |
# MAGIC | **Escalabilidad** | Manual: hay que agregar y configurar máquinas | Automática: el serverless ajusta el cómputo al volumen | Limitada por el plan contratado |
# MAGIC | **Gobierno** | Se construye desde cero (permisos, auditoría, linaje) | Integrado: Unity Catalog da permisos, linaje y auditoría desde el primer día | El que traiga la herramienta; no cubre datos crudos ni capas |
# MAGIC
# MAGIC **Conclusión.** Para Wanderbricks conviene **PaaS**. Necesitamos construir un pipeline de tres capas con
# MAGIC permisos diferenciados, linaje y ejecución programada, y eso descarta SaaS, que solo consume datos ya
# MAGIC preparados. Descartamos IaaS por tres razones ligadas al caso: somos un equipo de tres personas sin
# MAGIC infraestructura, con lo que 18 a 24 días de montaje competirían directamente con el trabajo de datos;
# MAGIC el volumen actual es pequeño y con crecimiento irregular, y pagar máquinas encendidas 24/7 para procesarlo
# MAGIC no se justifica frente a un cómputo que se apaga solo; y el gobierno y el linaje, que son el núcleo de esta
# MAGIC evidencia, tendríamos que construirlos a mano. IaaS ganaría si hubiera exigencias regulatorias de control o
# MAGIC residencia de datos que una plataforma gestionada no pueda cumplir, o un volumen tan grande y constante que
# MAGIC el costo por DBU superara al de máquinas propias bien aprovechadas. **Nada de esto impide agregar SaaS
# MAGIC encima**: una herramienta de BI podría leer la capa oro ya gobernada.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Implementación
# MAGIC ### 4.1 Organización del entorno
# MAGIC
# MAGIC Se organiza un único catálogo del grupo con **un esquema por capa**. La razón es de gobierno: cada esquema
# MAGIC tiene una audiencia distinta y los permisos se otorgan por esquema. Con un solo esquema, como en la EA1,
# MAGIC no se puede dejar `oro` a la vista del negocio y `bronce` cerrado.
# MAGIC
# MAGIC | Objeto | Nombre | Para qué |
# MAGIC |---|---|---|
# MAGIC | Catálogo | `bigdata_grupo56` | Contenedor único del grupo (el esquema `wanderbricks` de la EA1 queda como histórico y no se usa) |
# MAGIC | Esquema | `bronce` | Datos crudos, con datos personales. Solo ingeniería |
# MAGIC | Esquema | `plata` | Datos limpios y tipados con las reglas de negocio |
# MAGIC | Esquema | `oro` | Métricas listas para el negocio |
# MAGIC | Volumen | `bronce.datos_crudos` | Zona de aterrizaje para archivos (CSV o JSON) que lleguen de sistemas externos; queda junto a bronce porque es su punto de entrada. En esta entrega la fuente es un catálogo de muestra, así que el volumen se crea pero no recibe archivos |

# COMMAND ----------

CATALOGO = "bigdata_grupo56"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO}")
spark.sql(f"USE CATALOG {CATALOGO}")

esquemas = {
    "bronce": "Datos crudos tal como llegan de la fuente. Contiene datos personales. Acceso: solo ingenieria de datos",
    "plata":  "Datos limpios, tipados y con reglas de negocio. Acceso: ingenieria y ciencia de datos",
    "oro":    "Metricas listas para el negocio, sin datos personales. Acceso: todo el negocio en lectura",
}
for esquema, comentario in esquemas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{esquema} COMMENT '{comentario}'")

spark.sql(f"""CREATE VOLUME IF NOT EXISTS {CATALOGO}.bronce.datos_crudos
              COMMENT 'Zona de aterrizaje de archivos externos'""")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOGO}"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Carga inicial de las tres capas
# MAGIC
# MAGIC Las tres celdas siguientes son **exactamente el código de los tres notebooks que ejecuta el Job**
# MAGIC (`job_01_bronce`, `job_02_plata` y `job_03_oro`, ver sección 4.4). Se ejecutan aquí una vez para poblar el
# MAGIC entorno antes de aplicar permisos y consultar el linaje. Cada capa se escribe con `overwrite`, así que
# MAGIC ejecutarlas de nuevo, o dejar que las ejecute el Job, no duplica datos.
# MAGIC
# MAGIC **Bronce — tarea 1.** Copia las nueve tablas fuente sin transformarlas y agrega dos columnas de
# MAGIC trazabilidad: `_ingesta_ts` (cuándo entró) y `_fuente` (de dónde vino). Si alguna carga llega vacía, lanza un
# MAGIC error para que el Job se detenga.

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

# COMMAND ----------

# MAGIC %md
# MAGIC **Plata — tarea 2.** Aplica las reglas de negocio: tipado de fechas y montos, deduplicación de reservas por
# MAGIC `booking_id`, exclusión de reseñas con borrado lógico, aplanado del struct `metadata` del clickstream y
# MAGIC una dimensión de propiedades ya unida con su destino.

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

# COMMAND ----------

# MAGIC %md
# MAGIC **Oro — tarea 3.** Publica una tabla por cada pregunta de negocio de la EA1. Cada tabla lleva un comentario
# MAGIC con la definición de la métrica, para que quien la consulte sepa qué está leyendo.

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Permisos
# MAGIC
# MAGIC Se ejecutan tres sentencias `GRANT` **con niveles distintos sobre objetos distintos**: catálogo, esquema y
# MAGIC tabla.
# MAGIC
# MAGIC | # | Nivel | Sentencia | A quién y por qué |
# MAGIC |---|---|---|---|
# MAGIC | 0 | Catálogo | `USE CATALOG` sobre `bigdata_grupo56` | Es la llave del edificio: sin ella ningún otro permiso sirve |
# MAGIC | 1 | **Esquema** | `SELECT` sobre `oro` | Lectura de **todas** las tablas de oro para el negocio (papel del analista). Oro no tiene datos personales, y al otorgarlo por esquema cualquier tabla nueva queda cubierta |
# MAGIC | 2 | **Tabla** | `SELECT, MODIFY` sobre `plata.bookings` | Escritura sobre **una sola tabla** de plata (papel del ingeniero). Es más estrecho a propósito: `MODIFY` es un permiso peligroso y se da al objeto más pequeño posible |
# MAGIC
# MAGIC Bronce **no recibe ningún permiso**. Como en la edición gratuita solo existe el grupo `account users`, se
# MAGIC usa como sustituto de cada rol (ver 3.2).

# COMMAND ----------

TABLA = f"{CATALOGO}.plata.bookings"

# GRANT 0 — prerrequisito · nivel CATALOGO
# A quién: account users. Qué: poder entrar al catálogo. Por qué: sin USE CATALOG
# ningún permiso sobre esquemas o tablas dentro del catálogo tiene efecto.
spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOGO} TO `account users`")

# GRANT 1 — nivel ESQUEMA
# A quién: account users (sustituto del analista de negocio).
# Qué: leer todas las tablas del esquema oro. Por qué: oro contiene métricas sin datos
# personales y es lo que el negocio consume; por esquema, las tablas futuras quedan cubiertas.
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOGO}.oro TO `account users`")
spark.sql(f"GRANT SELECT ON SCHEMA {CATALOGO}.oro TO `account users`")

# GRANT 2 — nivel TABLA
# A quién: account users (sustituto del ingeniero de datos).
# Qué: leer y modificar SOLO plata.bookings. Por qué: es la tabla donde viven las reglas de
# negocio de reservas; MODIFY se concede sobre el objeto más pequeño posible, no sobre todo plata.
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOGO}.plata TO `account users`")
spark.sql(f"GRANT SELECT, MODIFY ON TABLE {TABLA} TO `account users`")

print("✅ Permisos otorgados")

# COMMAND ----------

# MAGIC %md
# MAGIC **Ejecutar el `GRANT` no basta: hay que mostrar que quedó.** Estas son las cuatro verificaciones:

# COMMAND ----------

print("── CATÁLOGO ──")
display(spark.sql(f"SHOW GRANTS ON CATALOG {CATALOGO}"))

# COMMAND ----------

print("── ESQUEMA oro ──")
display(spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOGO}.oro"))

# COMMAND ----------

print("── TABLA plata.bookings ──")
display(spark.sql(f"SHOW GRANTS ON TABLE {TABLA}"))

# COMMAND ----------

print("── ESQUEMA bronce (no se otorgó nada a account users) ──")
display(spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOGO}.bronce"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Cómo leer el resultado.** Sobre `oro` aparece `SELECT` a nivel de esquema; sobre `plata` solo aparece la
# MAGIC tabla `bookings` con `SELECT` y `MODIFY`; y sobre `bronce` no aparece ningún permiso para `account users`.
# MAGIC Esa asimetría es el gobierno: no la hizo la herramienta, la decidimos nosotros. (Como propietarios del
# MAGIC catálogo conservamos todos los privilegios; por eso no aparecen como una fila más.)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Linaje
# MAGIC
# MAGIC El linaje es el registro automático de **de dónde salió cada tabla y qué se construyó a partir de ella**.
# MAGIC Unity Catalog lo captura solo, a partir de las consultas que ejecutamos en las secciones anteriores; nadie lo
# MAGIC documentó a mano. Sirve para dos preguntas cotidianas: *«este número está mal, ¿de dónde salió?»* y
# MAGIC *«voy a cambiar esta tabla, ¿qué se rompe?»*.
# MAGIC
# MAGIC **Cómo se obtuvo la captura:** menú lateral → **Catalog** → `bigdata_grupo56` → esquema `oro` → tabla
# MAGIC `ingresos_por_destino` → pestaña **Lineage** → **See lineage graph**.
# MAGIC
# MAGIC **Recorrido esperado en el grafo:**
# MAGIC
# MAGIC ```
# MAGIC samples.wanderbricks.bookings     ──► bronce.bookings     ────────────► plata.bookings    ──┐
# MAGIC samples.wanderbricks.properties   ──► bronce.properties   ──┬─────────► plata.propiedades ──┼──► oro.ingresos_por_destino
# MAGIC samples.wanderbricks.destinations ──► bronce.destinations ──┘
# MAGIC ```
# MAGIC
# MAGIC > 📸 **Insertar aquí la captura del grafo de linaje de `oro.ingresos_por_destino`**
# MAGIC > (arrastrar la imagen a esta celda de texto).
# MAGIC
# MAGIC Como complemento a la captura, la misma información consultada por SQL desde las tablas del sistema
# MAGIC (puede tardar unos minutos en reflejar las ejecuciones más recientes):

# COMMAND ----------

try:
    display(spark.sql(f"""
        SELECT DISTINCT
               source_table_full_name AS origen,
               target_table_full_name AS destino
        FROM   system.access.table_lineage
        WHERE  target_table_catalog = '{CATALOGO}'
          AND  source_table_full_name IS NOT NULL
        ORDER  BY destino, origen
    """))
except Exception as e:
    print("Las tablas de sistema no están disponibles en este entorno; se usa la captura del Catalog Explorer.")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Automatización
# MAGIC
# MAGIC Hasta aquí todo se ejecutó a mano. Eso es un notebook, no una plataforma de datos. Un **Job** ejecuta el
# MAGIC pipeline solo, en un horario, y se detiene si algo falla.
# MAGIC
# MAGIC ```
# MAGIC [ ingesta_bronce ] ──éxito──▶ [ transformar_plata ] ──éxito──▶ [ agregar_oro ]
# MAGIC         │
# MAGIC         └── falla o carga vacía ──▶ el Job se detiene y las tareas siguientes no corren
# MAGIC ```
# MAGIC
# MAGIC Se encadenan porque **si la ingesta falla no tiene sentido transformar datos incompletos**. Cada notebook
# MAGIC del Job es una copia del código de la sección 4.1 (archivos `job_01_bronce.py`, `job_02_plata.py` y
# MAGIC `job_03_oro.py` en el repositorio, carpeta `/ea2`).
# MAGIC
# MAGIC | Elemento | Configuración |
# MAGIC |---|---|
# MAGIC | Nombre del Job | `pipeline_wanderbricks_grupo56` |
# MAGIC | Tarea 1 | `ingesta_bronce` · Notebook `job_01_bronce` · cómputo serverless |
# MAGIC | Tarea 2 | `transformar_plata` · Notebook `job_02_plata` · **depende de** `ingesta_bronce` |
# MAGIC | Tarea 3 | `agregar_oro` · Notebook `job_03_oro` · **depende de** `transformar_plata` |
# MAGIC | Programación | Diaria, 2:00 a. m., zona horaria `America/Bogota` |
# MAGIC | Identificador del Job | ⚠️ **COMPLETAR:** `(pegar el ID; aparece en la URL del Job, ...#job/<ID>)` |
# MAGIC
# MAGIC > 📸 **Insertar aquí la captura de una ejecución exitosa** (pestaña *Runs*: las tres tareas en verde
# MAGIC > con la flecha entre ellas).
# MAGIC >
# MAGIC > 📸 **Insertar aquí la captura de la programación definida** (panel *Schedules & Triggers*).
# MAGIC
# MAGIC > ⚠️ **Apenas tengan las capturas, pausen la programación.** Un Job diario consume cuota; la evidencia
# MAGIC > ya queda con el horario definido y la ejecución probada.
# MAGIC
# MAGIC **Código de las tareas.** Es el de las tres celdas de la sección 4.1. La siguiente celda comprueba que el
# MAGIC Job corrió: la marca de tiempo de la última operación de cada capa debe coincidir con el momento de la
# MAGIC ejecución.

# COMMAND ----------

# Verificación: la última escritura de cada capa debe coincidir con la ejecución del Job
for capa, tabla in [("bronce", "bookings"), ("plata", "bookings"), ("oro", "ingresos_por_destino")]:
    ruta = f"{CATALOGO}.{capa}.{tabla}"
    h = spark.sql(f"DESCRIBE HISTORY {ruta}").first()
    print(f"{capa}.{tabla:<22} {spark.table(ruta).count():>8,} filas · {h['operation']:<26} · {h['timestamp']}")

# COMMAND ----------

# Historial de ejecuciones del Job (opcional: requiere pegar el ID del Job)
JOB_ID = None   # ⚠️ COMPLETAR con el identificador del Job, por ejemplo 123456789012345

if JOB_ID:
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        for r in w.jobs.list_runs(job_id=JOB_ID, limit=5):
            print(f"run {r.run_id} · {r.state.result_state} · inicio {r.start_time}")
    except Exception as e:
        print("No se pudo consultar el historial por código; usar la captura de la pestaña Runs.")
        print(e)
else:
    print("Pegar el JOB_ID para listar las ejecuciones.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Resultados
# MAGIC
# MAGIC Salidas del entorno desplegado. Primero, cuántas filas entran y cuántas salen en cada transformación de
# MAGIC plata; después, las cinco tablas de oro.

# COMMAND ----------

from pyspark.sql import functions as F

resumen = []
for t in ["bookings", "reviews", "clickstream"]:
    b = spark.table(f"{CATALOGO}.bronce.{t}").count()
    p = spark.table(f"{CATALOGO}.plata.{t}").count()
    resumen.append((t, b, p, b - p))

display(spark.createDataFrame(resumen, ["tabla", "filas_bronce", "filas_plata", "filas_descartadas"]))

# COMMAND ----------

# Oro 1 — ¿qué destinos generan más ingresos?
display(spark.table(f"{CATALOGO}.oro.ingresos_por_destino")
        .orderBy(F.col("ingresos_no_cancelados").desc()).limit(10))

# COMMAND ----------

# Oro 2 — distribución de estados de reserva (tasa de cancelación)
display(spark.table(f"{CATALOGO}.oro.estado_reservas").orderBy(F.col("total_reservas").desc()))

# COMMAND ----------

# Oro 3 — evolución del valor promedio de reserva por mes de check-in
display(spark.table(f"{CATALOGO}.oro.valor_mensual_reservas").orderBy("mes"))

# COMMAND ----------

# Oro 4 — propiedades mejor calificadas (mínimo 5 reseñas)
display(spark.table(f"{CATALOGO}.oro.calificacion_propiedades")
        .orderBy(F.col("calificacion_promedio").desc()).limit(10))

# COMMAND ----------

# Oro 5 — eventos de navegación por dispositivo
display(spark.table(f"{CATALOGO}.oro.eventos_por_dispositivo").orderBy(F.col("numero_eventos").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC **Lectura de los resultados.** La capa oro reproduce con la misma lógica los resultados de la EA1, ahora
# MAGIC desde tablas gobernadas por capas:
# MAGIC
# MAGIC - **Destinos.** Phuket lidera con 5.771 reservas y unos 3,23 millones en ingresos no cancelados, seguido de
# MAGIC   Gold Coast (5.140 reservas, 2,86 millones) y Mallorca (5.146 reservas, 2,83 millones).
# MAGIC - **Estados.** El 43,70 % de las reservas está en `pending`, el 24,84 % en `confirmed`, el 21,16 % en
# MAGIC   `cancelled` y el 10,30 % en `completed`. Como casi la mitad sigue pendiente, la tabla de ingresos separa
# MAGIC   `ingresos_no_cancelados` (incluye pendientes) de `ingresos_confirmados` (solo `confirmed` y `completed`).
# MAGIC - **Calificaciones.** La mejor calificación promedio entre propiedades con al menos 5 reseñas es 3,17
# MAGIC   (*Chalet in Innsbruck*, 6 reseñas), y 9 de las 10 primeras son de tipo *Ski Resort*.
# MAGIC - **Dispositivos.** Los 100.000 eventos se reparten casi por igual: tablet 33.753, desktop 33.303 y mobile
# MAGIC   32.944.
# MAGIC - **Valor promedio.** Desde 2024 el valor promedio por reserva se mantiene alrededor de 550 mientras el
# MAGIC   número de reservas por mes sube de cientos a más de 25.000.
# MAGIC
# MAGIC *Las cifras de este comentario provienen de la EA1 con la misma lógica; si al ejecutar el notebook alguna
# MAGIC difiere de la tabla mostrada arriba, debe actualizarse aquí.*

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Conclusiones
# MAGIC
# MAGIC **Qué funcionó.**
# MAGIC
# MAGIC - **Separar por esquemas habilitó el gobierno.** El `SHOW GRANTS` de la sección 4.2 muestra `SELECT` sobre
# MAGIC   `oro`, `MODIFY` sobre una sola tabla de `plata` y nada sobre `bronce`. Esa distinción era imposible con el
# MAGIC   esquema único de la EA1: es la estructura la que hace posible el permiso, no la herramienta.
# MAGIC - **El linaje se obtuvo sin documentar nada.** Unity Catalog lo dedujo de las consultas que ya ejecutábamos;
# MAGIC   en una máquina virtual habría que construirlo a mano (sección 3.3).
# MAGIC - **El Job encadenado evita procesar datos incompletos.** La tarea de bronce lanza un error si una carga llega
# MAGIC   vacía, y las tareas siguientes dependen de ella.
# MAGIC - **La lógica se pudo reutilizar.** La capa oro reproduce los resultados de la EA1 (Phuket, Gold Coast y
# MAGIC   Mallorca al frente; 21,16 % de cancelación), lo que confirma que reorganizar en capas no alteró las métricas.
# MAGIC
# MAGIC **Qué no funcionó o quedó limitado.**
# MAGIC
# MAGIC - **La matriz de roles quedó diseñada, no ejecutada.** La edición gratuita solo ofrece los grupos `admins` y
# MAGIC   `users`. Los `GRANT` se aplicaron a `account users`, que representa a un rol pero no lo distingue de los
# MAGIC   demás; la separación real entre analista, científico e ingeniero solo existe en la matriz de la sección 3.2.
# MAGIC - **La programación tuvo que pausarse tras la evidencia** para no gastar la cuota diaria. El horario queda
# MAGIC   definido y probado, pero no corre solo en este momento.
# MAGIC - **Encontramos un problema de definición en nuestra propia EA1.** La consulta 1 se presentaba como ingresos
# MAGIC   de reservas «confirmadas», pero solo excluía las canceladas; con el 43,70 % de reservas en `pending`, eso
# MAGIC   incluía ingresos aún no confirmados. La capa oro corrige esto separando las dos columnas.
# MAGIC - **El resultado por dispositivo no respalda la conclusión de UX de la EA1.** La diferencia entre el
# MAGIC   dispositivo líder (tablet, 33,75 %) y el último (mobile, 32,94 %) es menor a un punto porcentual, así que
# MAGIC   no hay una señal clara para priorizar uno.
# MAGIC
# MAGIC **Qué haríamos distinto si empezáramos de nuevo.**
# MAGIC
# MAGIC 1. **Cargas incrementales en lugar de `overwrite`.** Cada noche reescribimos las 566.844 filas. Con el
# MAGIC    volumen multiplicado por cien, el Job sería a la vez el cuello de botella y el mayor costo; convendría
# MAGIC    procesar solo lo nuevo con `MERGE` o carga incremental.
# MAGIC 2. **Enmascarar los datos personales desde plata** (correo, teléfono) en vez de depender solo de que nadie
# MAGIC    entre a bronce.
# MAGIC 3. **Parametrizar el catálogo** y separar entornos de desarrollo y producción, en lugar de dejar el nombre
# MAGIC    escrito dentro de cada notebook.
# MAGIC 4. **Agregar validaciones de calidad y alertas de fallo** al Job, para enterarnos de un error sin abrir la
# MAGIC    pestaña de ejecuciones.
# MAGIC
# MAGIC > ✍️ **COMPLETAR (opcional):** cualquier error o hallazgo real que hayan tenido al ejecutar el entorno
# MAGIC > (por ejemplo, la duración del Job o algún fallo que resolvieron). Es lo que más valor aporta aquí.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Reparto del trabajo y uso de IA
# MAGIC
# MAGIC | Integrante | De qué se encargó | Qué sustenta en el video |
# MAGIC |---|---|---|
# MAGIC | Ronal Mosquera | Entorno y permisos: catálogo, esquemas, volumen, matriz de roles, los `GRANT` y el linaje (secciones 3.2, 4.1, 4.2 y 4.3) | Recorrido del entorno en pantalla, el `GRANT` ejecutado con su `SHOW GRANTS` y por qué esquemas separados |
# MAGIC | Federico López Torres | Automatización: los tres notebooks, el Job encadenado, su programación y los resultados (secciones 4.4 y 5) | Ejecución del Job, explicación línea por línea del código de las tareas y cierre |
# MAGIC | *(integrante 3, si aplica)* | Diagrama de arquitectura y comparación IaaS / PaaS / SaaS (secciones 3.1, 3.3 y 3.4) | Qué administra el proveedor y qué el equipo, y qué se complicaría sobre máquinas virtuales |
# MAGIC
# MAGIC > ⚠️ **Ajustar este reparto al real antes de entregar.** Si el grupo es de dos personas, las secciones del
# MAGIC > tercer integrante pasan a repartirse entre Ronal y Federico.
# MAGIC
# MAGIC **Uso de asistentes de IA:** se usó Claude (asistente de IA de Anthropic) para estructurar el notebook,
# MAGIC redactar los textos de las secciones, proponer el diagrama, la matriz de roles y la especificación IaaS, y
# MAGIC escribir el código PySpark de las tres tareas del Job, partiendo de la EA1 y de las prácticas de clase. Los
# MAGIC datos y resultados provienen del dataset `samples.wanderbricks` y de la EA1 del grupo. Cada integrante debe
# MAGIC poder explicar cualquier línea del código en el video.
# MAGIC
# MAGIC > Este reparto debe coincidir con lo que cada persona demuestra en el video y con el
# MAGIC > historial de commits del repositorio. Las tres fuentes se contrastan al calificar.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📹 Preguntas obligatorias de sustentación
# MAGIC
# MAGIC Cada integrante responde estas tres preguntas en su intervención del video. Respuestas de referencia del
# MAGIC grupo, para que todas las intervenciones sean coherentes con lo entregado:
# MAGIC
# MAGIC **1. ¿Qué parte de esta arquitectura administra el proveedor y cuál administran ustedes?**
# MAGIC El proveedor administra el cómputo serverless, el sistema operativo, el motor Spark, el almacenamiento
# MAGIC físico, Unity Catalog (metastore, linaje y auditoría) y el orquestador de Jobs. Nosotros administramos la
# MAGIC estructura (catálogo, esquemas y volumen), los permisos, la lógica de los notebooks, la programación del Job
# MAGIC y la definición de las métricas de oro.
# MAGIC
# MAGIC **2. Muestre un GRANT que ejecutó y explique a quién le está dando qué, y por qué.**
# MAGIC `GRANT SELECT ON SCHEMA bigdata_grupo56.oro TO account users`: da lectura sobre todas las tablas de oro al
# MAGIC grupo de usuarios (sustituto del analista de negocio) porque oro no contiene datos personales y es lo que el
# MAGIC negocio consume; va acompañado de `USE CATALOG` y `USE SCHEMA`. El segundo,
# MAGIC `GRANT SELECT, MODIFY ON TABLE bigdata_grupo56.plata.bookings`, da escritura sobre una sola tabla de plata
# MAGIC (papel del ingeniero) y se limita a una tabla porque `MODIFY` es el permiso más riesgoso.
# MAGIC
# MAGIC **3. Si tuvieran que montar esto sobre máquinas virtuales, ¿qué sería lo primero que se les complicaría?**
# MAGIC El gobierno y el linaje. En Databricks, `GRANT`, `SHOW GRANTS` y el grafo de linaje funcionan sin instalar
# MAGIC nada; sobre máquinas virtuales habría que escoger e instalar una herramienta de permisos, conectarla al
# MAGIC metastore y al almacenamiento, y el linaje no existe de forma automática. Es la parte con menos equivalente
# MAGIC directo y una de las que más días-persona consume en la estimación de la sección 3.3.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Antes de entregar
# MAGIC
# MAGIC - [ ] El diagrama de arquitectura está incluido y descrito
# MAGIC - [ ] Los dos GRANT están ejecutados y el SHOW GRANTS muestra el resultado
# MAGIC - [ ] La captura del linaje está insertada en la sección 4.3
# MAGIC - [ ] El Job tiene dos o más tareas, está programado y hay evidencia de ejecución (captura + ID del Job)
# MAGIC - [ ] La programación del Job fue pausada después de la captura
# MAGIC - [ ] La comparación IaaS/PaaS/SaaS termina en una conclusión, no en una tabla suelta
# MAGIC - [ ] No quedan textos de plantilla ni marcas ⚠️ / ✍️ sin completar
# MAGIC - [ ] El enlace del video está en la portada y abre desde otra cuenta
# MAGIC - [ ] Todo confirmado en /ea2 (notebook y los tres `job_0X.py`) y el HTML subido a Canvas