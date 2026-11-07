# Databricks notebook source
# MAGIC %md
# MAGIC # Curso de Big Data e IA · Módulo 1
# MAGIC ## Bloque 3 (ampliación): Streaming con Spark en Databricks
# MAGIC
# MAGIC **Duración estimada:** 45-60 minutos
# MAGIC
# MAGIC **Qué haremos:**
# MAGIC 1. Leer un **flujo real y gratuito** (cambios en Wikipedia, de Wikimedia EventStreams) y guardarlo en ficheros.
# MAGIC 2. Procesarlo con **Structured Streaming** y ver cómo Spark solo procesa lo nuevo.
# MAGIC 3. Repetir la idea con el **Sporting**: reproducir un partido minuto a minuto y acumular el xG.
# MAGIC
# MAGIC **Importante:** en Databricks Free Edition solo hay cómputo *serverless*, que solo admite el disparador
# MAGIC `availableNow` (procesa todo lo pendiente y termina). No hay flujos que se queden escuchando para siempre.
# MAGIC Para eso se necesitaría un clúster propio.
# MAGIC
# MAGIC **Datos del Sporting:** StatsBomb Open Data (https://github.com/statsbomb/open-data). Si compartes análisis, cita StatsBomb.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración: dónde guardamos el flujo

# COMMAND ----------

# AJUSTA estos nombres si tu catálogo o esquema son otros (Catalog en el menú lateral)
CATALOGO = "workspace"
ESQUEMA  = "default"
VOLUMEN  = "streaming"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{ESQUEMA}.{VOLUMEN}")
BASE = f"/Volumes/{CATALOGO}/{ESQUEMA}/{VOLUMEN}"
print("Carpeta de trabajo:", BASE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ¿Llega el flujo de Wikimedia a nuestro espacio de trabajo?
# MAGIC
# MAGIC Wikimedia publica los cambios de todos sus proyectos como un flujo continuo (formato *Server-Sent Events*, sobre HTTP).
# MAGIC No hace falta cuenta ni clave. Si esta celda da `200`, el dominio está permitido desde tu espacio de trabajo.

# COMMAND ----------

import requests

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
CABECERAS = {"User-Agent": "CursoBigData/1.0 (formacion; contacto: tu-correo)"}   # pon tu correo

r = requests.get(URL, headers=CABECERAS, stream=True, timeout=15)
print(r.status_code, r.headers.get("content-type"))
r.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Capturar el flujo en ficheros
# MAGIC
# MAGIC Una función lee el flujo durante unos segundos y guarda los eventos en **un fichero JSON por captura**
# MAGIC dentro del volumen. Cada fichero es un "trozo" nuevo que Spark verá llegar.

# COMMAND ----------

import json, time, os

def capturar(segundos=20, max_eventos=2000):
    """Lee el flujo de Wikimedia y guarda un fichero JSON (una línea por evento)."""
    destino = f"{BASE}/wiki_eventos"
    os.makedirs(destino, exist_ok=True)
    ruta = f"{destino}/captura_{int(time.time())}.json"
    n, inicio = 0, time.time()
    with requests.get(URL, headers=CABECERAS, stream=True, timeout=(10, 30)) as r, open(ruta, "w") as f:
        for linea in r.iter_lines():
            if linea.startswith(b"data:"):
                f.write(linea[5:].decode("utf-8").strip() + "\n")
                n += 1
            if n >= max_eventos or time.time() - inicio > segundos:
                break
    print(f"{n} eventos guardados en {ruta}")

capturar(segundos=20)

# COMMAND ----------

# Mira qué hay en la carpeta
display(dbutils.fs.ls(f"{BASE}/wiki_eventos"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Structured Streaming: leer la carpeta como un flujo
# MAGIC
# MAGIC Spark trata el flujo como **una tabla que no deja de crecer**. Tú escribes la consulta igual que con un DataFrame
# MAGIC normal; Spark la ejecuta en micro-lotes sobre lo que va llegando.
# MAGIC
# MAGIC - `readStream` en lugar de `read`.
# MAGIC - Un **esquema** explícito (un flujo no puede inferirlo bien).
# MAGIC - Un **checkpoint**: Spark apunta qué ficheros ya procesó. Así, al relanzar, solo lee lo nuevo.
# MAGIC - `trigger(availableNow=True)`: procesa todo lo pendiente y termina (el único modo que admite serverless).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

esquema_wiki = StructType([
    StructField("wiki",        StringType()),
    StructField("type",        StringType()),
    StructField("title",       StringType()),
    StructField("user",        StringType()),
    StructField("bot",         BooleanType()),
    StructField("server_name", StringType()),
    StructField("timestamp",   LongType()),
])

TABLA_WIKI = f"{CATALOGO}.{ESQUEMA}.wiki_cambios"

def procesar_wiki():
    flujo = (spark.readStream
             .schema(esquema_wiki)
             .json(f"{BASE}/wiki_eventos"))

    consulta = (flujo
                .withColumn("instante", F.to_timestamp(F.from_unixtime("timestamp")))
                .writeStream
                .option("checkpointLocation", f"{BASE}/checkpoint_wiki")
                .trigger(availableNow=True)
                .toTable(TABLA_WIKI))
    consulta.awaitTermination()
    print("Filas en la tabla:", spark.table(TABLA_WIKI).count())

procesar_wiki()

# COMMAND ----------

# Consulta normal sobre la tabla que ha ido llenando el flujo
display(spark.table(TABLA_WIKI)
        .groupBy("wiki").count()
        .orderBy(F.desc("count")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba del checkpoint
# MAGIC 1. Vuelve a ejecutar la celda de **captura** (sección 3): se crea un fichero nuevo.
# MAGIC 2. Ejecuta otra vez `procesar_wiki()`.
# MAGIC 3. Mira cuántas filas había antes y cuántas hay ahora.
# MAGIC
# MAGIC **Pregunta:** ¿Spark ha vuelto a leer los eventos de la primera captura?
# MAGIC **Respuesta:** no. El checkpoint recuerda qué ficheros ya procesó y solo lee los nuevos.
# MAGIC **Conclusión:** el flujo es incremental: cada ejecución cuesta lo que llega de nuevo, no todo el histórico.

# COMMAND ----------

capturar(segundos=20)
procesar_wiki()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Lo mismo con el Sporting: un partido "en directo" simulado
# MAGIC
# MAGIC No hay un flujo gratuito de fútbol, así que **reproducimos un partido** de los datos de StatsBomb:
# MAGIC guardamos los tiros de cada tramo de 10 minutos en un fichero distinto, como si fueran llegando.
# MAGIC Spark los lee de uno en uno (`maxFilesPerTrigger=1`) y va **acumulando el xG** de cada equipo.
# MAGIC
# MAGIC Necesitas los CSV del Notebook 1, subidos como en el Notebook 2.

# COMMAND ----------

# AJUSTA esta ruta a donde hayas subido el fichero (la misma que en el Notebook 2)
ruta_eventos = "/FileStore/tables/sporting_eventos.csv"

eventos = (spark.read.option("header", "true").option("inferSchema", "true").csv(ruta_eventos))

partido = eventos.select("match_id").first()[0]
tiros = (eventos
         .filter((F.col("match_id") == partido) & (F.col("type") == "Shot"))
         .select("minute", "team", "player", "shot_xg", "shot_outcome")
         .toPandas())
print(f"Partido {partido}: {len(tiros)} tiros")
tiros.head()

# COMMAND ----------

import shutil
carpeta_sim = f"{BASE}/partido_sim"
shutil.rmtree(carpeta_sim, ignore_errors=True)
os.makedirs(carpeta_sim)

# Un fichero por cada tramo de 10 minutos
for tramo in range(0, 100, 10):
    parte = tiros[(tiros["minute"] >= tramo) & (tiros["minute"] < tramo + 10)]
    if len(parte):
        parte.to_json(f"{carpeta_sim}/tramo_{tramo:03d}.json", orient="records", lines=True)

display(dbutils.fs.ls(carpeta_sim))

# COMMAND ----------

from pyspark.sql.types import DoubleType

esquema_tiros = StructType([
    StructField("minute",       LongType()),
    StructField("team",         StringType()),
    StructField("player",       StringType()),
    StructField("shot_xg",      DoubleType()),
    StructField("shot_outcome", StringType()),
])

TABLA_XG = f"{CATALOGO}.{ESQUEMA}.sim_xg_equipo"
shutil.rmtree(f"{BASE}/checkpoint_xg", ignore_errors=True)
spark.sql(f"DROP TABLE IF EXISTS {TABLA_XG}")

flujo = (spark.readStream
         .schema(esquema_tiros)
         .option("maxFilesPerTrigger", 1)          # un tramo por micro-lote
         .json(carpeta_sim))

acumulado = (flujo.groupBy("team")
             .agg(F.count("*").alias("tiros"), F.round(F.sum("shot_xg"), 2).alias("xg")))

consulta = (acumulado.writeStream
            .outputMode("complete")                 # reescribe el acumulado completo en cada lote
            .option("checkpointLocation", f"{BASE}/checkpoint_xg")
            .trigger(availableNow=True)
            .toTable(TABLA_XG))
consulta.awaitTermination()

for p in consulta.recentProgress:
    print("lote", p["batchId"], "· filas leídas:", p["numInputRows"])
display(spark.table(TABLA_XG).orderBy(F.desc("xg")))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** ¿por qué hay varios lotes si lanzamos la consulta una sola vez?
# MAGIC **Respuesta:** `maxFilesPerTrigger=1` obliga a leer un fichero por lote; cada lote actualiza el acumulado.
# MAGIC **Conclusión:** un flujo es una secuencia de micro-lotes sobre una tabla que crece, y el estado (aquí, el xG acumulado) se mantiene entre lotes.
# MAGIC
# MAGIC ---
# MAGIC ## 6. Limpieza (opcional)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TABLA_WIKI}")
spark.sql(f"DROP TABLE IF EXISTS {TABLA_XG}")
for c in ("checkpoint_wiki", "checkpoint_xg", "wiki_eventos", "partido_sim"):
    shutil.rmtree(f"{BASE}/{c}", ignore_errors=True)
