# Databricks notebook source
# MAGIC %md
# MAGIC # Curso de Big Data e IA · Módulo 1
# MAGIC ## Bloque 3 (ampliación): Streaming con Spark en Databricks
# MAGIC
# MAGIC **Duración estimada:** 60 minutos
# MAGIC
# MAGIC **Qué haremos:**
# MAGIC 1. Leer un **flujo real y gratuito** (cambios en Wikipedia, de Wikimedia EventStreams) y guardarlo en ficheros.
# MAGIC 2. Procesarlo con **Structured Streaming** y ver cómo Spark solo procesa lo nuevo.
# MAGIC 3. Resolver **varios casos** sobre el mismo flujo: copia limpia, filtro, conteos y ventanas de tiempo.
# MAGIC
# MAGIC **Importante:** en Databricks Free Edition solo hay cómputo *serverless*, que solo admite el disparador
# MAGIC `availableNow` (procesa todo lo pendiente y termina). No hay flujos que se queden escuchando para siempre.
# MAGIC Para eso se necesitaría un clúster propio.
# MAGIC
# MAGIC **Si tu espacio de trabajo no llega a Wikimedia:** el docente puede compartir un fichero ya capturado.
# MAGIC Súbelo a la carpeta `wiki_eventos` del volumen (sección 1) y salta la sección 3.

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
# MAGIC
# MAGIC Para el caso de las ventanas de tiempo (sección 9) conviene capturar **más de un minuto**.

# COMMAND ----------

import json, time, os

def capturar(segundos=20, max_eventos=5000):
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

capturar(segundos=75)

# COMMAND ----------

# Mira qué hay en la carpeta
display(dbutils.fs.ls(f"{BASE}/wiki_eventos"))

# COMMAND ----------

# Un vistazo a los eventos en crudo (lectura normal, no streaming)
display(spark.read.json(f"{BASE}/wiki_eventos").select("wiki", "type", "title", "user", "bot", "timestamp").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Structured Streaming: leer la carpeta como un flujo
# MAGIC
# MAGIC Spark trata el flujo como **una tabla que no deja de crecer**. Tú escribes la consulta igual que con un DataFrame
# MAGIC normal; Spark la ejecuta en micro-lotes sobre lo que va llegando.
# MAGIC
# MAGIC - `readStream` en lugar de `read`.
# MAGIC - Un **esquema** explícito (un flujo no puede inferirlo bien).
# MAGIC - Un **checkpoint** por consulta: Spark apunta qué ficheros ya procesó. Así, al relanzar, solo lee lo nuevo.
# MAGIC - `trigger(availableNow=True)`: procesa todo lo pendiente y termina (el único modo que admite serverless).
# MAGIC
# MAGIC Definimos dos funciones que reutilizaremos en todos los casos: una que **lee** el flujo y otra que lo **lanza**.

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

def leer_wiki():
    """Flujo de la carpeta de capturas, con una columna de fecha legible."""
    return (spark.readStream
            .schema(esquema_wiki)
            .json(f"{BASE}/wiki_eventos")
            .withColumn("instante", F.to_timestamp(F.from_unixtime("timestamp"))))

def lanzar(flujo, tabla, nombre, modo="append"):
    """Escribe el flujo en una tabla Delta. Cada consulta tiene su propio checkpoint."""
    consulta = (flujo.writeStream
                .outputMode(modo)
                .option("checkpointLocation", f"{BASE}/ck_{nombre}")
                .trigger(availableNow=True)
                .toTable(f"{CATALOGO}.{ESQUEMA}.{tabla}"))
    consulta.awaitTermination()
    print(f"{tabla}: {spark.table(f'{CATALOGO}.{ESQUEMA}.{tabla}').count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Caso 1: copia limpia del flujo
# MAGIC
# MAGIC El caso más básico y el más usado en la práctica: guardar lo que llega, con sus tipos, en una tabla.
# MAGIC Es el equivalente a la capa **bronce** de la arquitectura medallón.

# COMMAND ----------

lanzar(leer_wiki(), "wiki_cambios", "copia")
display(spark.table(f"{CATALOGO}.{ESQUEMA}.wiki_cambios").groupBy("wiki").count().orderBy(F.desc("count")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba del checkpoint
# MAGIC 1. Vuelve a ejecutar la celda de **captura** (sección 3): se crea un fichero nuevo.
# MAGIC 2. Ejecuta otra vez la celda del caso 1.
# MAGIC 3. Mira cuántas filas había antes y cuántas hay ahora.
# MAGIC
# MAGIC **Pregunta:** ¿Spark ha vuelto a leer los eventos de la primera captura?
# MAGIC **Respuesta:** no. El checkpoint recuerda qué ficheros ya procesó y solo lee los nuevos.
# MAGIC **Conclusión:** el flujo es incremental: cada ejecución cuesta lo que llega de nuevo, no todo el histórico.

# COMMAND ----------

capturar(segundos=20)
lanzar(leer_wiki(), "wiki_cambios", "copia")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verlo crecer: datos nuevos ronda a ronda
# MAGIC
# MAGIC Con cómputo serverless el flujo no se queda escuchando, pero podemos **simular el tiempo real** repitiendo un ciclo:
# MAGIC llegan datos nuevos (captura de unos segundos) → Spark procesa **solo lo nuevo** → miramos cómo han cambiado las tablas.
# MAGIC
# MAGIC En cada ronda verás:
# MAGIC - cuántas **filas nuevas** han entrado y el **total** acumulado (la tabla crece),
# MAGIC - el **% de cambios hechos por bots** (un resultado con estado que se va actualizando),
# MAGIC - las **3 Wikipedias** con más actividad.
# MAGIC
# MAGIC Dura unos 2-3 minutos (6 rondas). Mientras corre, la celda va escribiendo una línea por ronda.

# COMMAND ----------

def demo_en_vivo(rondas=6, segundos=10):
    tabla_copia = f"{CATALOGO}.{ESQUEMA}.wiki_cambios"
    por_tipo = (leer_wiki().groupBy("type", "bot").agg(F.count("*").alias("cambios")))
    antes = spark.table(tabla_copia).count() if spark.catalog.tableExists(tabla_copia) else 0
    historial = []
    for ronda in range(1, rondas + 1):
        capturar(segundos=segundos)                          # 1) llegan datos nuevos
        lanzar(leer_wiki(), "wiki_cambios", "copia")         # 2) Spark lee solo los ficheros nuevos
        lanzar(por_tipo, "wiki_por_tipo", "por_tipo", modo="complete")

        copia = spark.table(tabla_copia)
        total = copia.count()
        nuevos = total - antes
        antes = total
        bots = copia.filter(F.col("bot") == True).count()
        pct_bots = round(100 * bots / total, 1) if total else 0.0
        top3 = [f"{r['wiki']} ({r['count']})" for r in copia.groupBy("wiki").count().orderBy(F.desc("count")).limit(3).collect()]
        historial.append((ronda, total, nuevos, pct_bots))

        print(f"--- Ronda {ronda} · {time.strftime('%H:%M:%S')} ---")
        print(f"  filas nuevas: {nuevos:>5} {'█' * min(60, nuevos // 20)}")
        print(f"  total tabla : {total:>5}")
        print(f"  % bots      : {pct_bots}%")
        print(f"  top 3 wikis : {', '.join(top3)}")
    return historial

historial = demo_en_vivo(rondas=6, segundos=10)

# COMMAND ----------

# La evolución en una tabla (pulsa "+" en el resultado para crear un gráfico de líneas con total por ronda)
display(spark.createDataFrame(historial, "ronda int, total long, nuevos long, pct_bots double"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** ¿por qué cada ronda tarda lo mismo aunque la tabla ya sea más grande?
# MAGIC **Respuesta:** porque Spark solo procesa los ficheros nuevos; el checkpoint ya recuerda los anteriores.
# MAGIC **Conclusión:** así se ve un flujo: la tabla crece con cada llegada y los resultados con estado (% de bots) se actualizan sin recalcular el histórico.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Caso 2: filtrar el flujo
# MAGIC
# MAGIC Una consulta **sin estado**: cada evento se mira por separado. Nos quedamos con las **ediciones hechas por personas**
# MAGIC (no bots) en las Wikipedias en inglés, español y francés.

# COMMAND ----------

humanas = (leer_wiki()
           .filter((F.col("type") == "edit") & (F.col("bot") == False)
                   & F.col("wiki").isin("enwiki", "eswiki", "frwiki"))
           .select("instante", "wiki", "title", "user"))

lanzar(humanas, "wiki_ediciones_humanas", "humanas")
display(spark.table(f"{CATALOGO}.{ESQUEMA}.wiki_ediciones_humanas").orderBy(F.desc("instante")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Caso 3: contar por tipo de cambio y por bot
# MAGIC
# MAGIC Ahora una consulta **con estado**: Spark tiene que recordar los conteos entre lotes.
# MAGIC Usamos `outputMode("complete")`: en cada lote se reescribe el resultado completo.
# MAGIC
# MAGIC **Pregunta:** ¿qué proporción de los cambios los hacen bots?

# COMMAND ----------

por_tipo = (leer_wiki()
            .groupBy("type", "bot")
            .agg(F.count("*").alias("cambios")))

lanzar(por_tipo, "wiki_por_tipo", "por_tipo", modo="complete")
display(spark.table(f"{CATALOGO}.{ESQUEMA}.wiki_por_tipo").orderBy(F.desc("cambios")))

# COMMAND ----------

# MAGIC %md
# MAGIC **Respuesta:** la que salga en tu tabla (depende del momento de la captura).
# MAGIC **Conclusión:** un conteo en streaming es un `groupBy` normal; Spark guarda el estado y lo actualiza con cada lote.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Caso 4: ventanas de tiempo
# MAGIC
# MAGIC En streaming casi siempre se pregunta *"¿cuántos por minuto?"*. Se hace con una **ventana** sobre la hora del evento
# MAGIC (`window`). Aquí contamos cambios por ventana de 1 minuto.

# COMMAND ----------

por_minuto = (leer_wiki()
              .groupBy(F.window("instante", "1 minute"))
              .agg(F.count("*").alias("cambios")))

lanzar(por_minuto, "wiki_por_minuto", "por_minuto", modo="complete")
display(spark.table(f"{CATALOGO}.{ESQUEMA}.wiki_por_minuto")
        .select(F.col("window.start").alias("minuto"), "cambios")
        .orderBy("minuto"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** ¿hay minutos con muchos más cambios que otros?
# MAGIC **Respuesta:** míralo en la tabla: la actividad de Wikipedia varía durante el día y entre minutos.
# MAGIC **Conclusión:** las ventanas convierten un flujo de eventos sueltos en una serie temporal, lista para un gráfico o una alerta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Varias consultas, una sola fuente
# MAGIC
# MAGIC Hemos lanzado **cuatro consultas** sobre la misma carpeta. Cada una tiene su **propio checkpoint** (`ck_copia`,
# MAGIC `ck_humanas`, `ck_por_tipo`, `ck_por_minuto`), así que cada una avanza a su ritmo.
# MAGIC
# MAGIC Prueba: ejecuta otra **captura** y relanza **solo una** de las consultas. Las demás tablas no cambian hasta que las relances.
# MAGIC
# MAGIC **Conclusión:** una misma fuente puede alimentar muchas consultas independientes; el checkpoint es lo que las separa.
# MAGIC
# MAGIC ---
# MAGIC ## 11. Limpieza (opcional)

# COMMAND ----------

import shutil
for t in ("wiki_cambios", "wiki_ediciones_humanas", "wiki_por_tipo", "wiki_por_minuto"):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOGO}.{ESQUEMA}.{t}")
for c in ("ck_copia", "ck_humanas", "ck_por_tipo", "ck_por_minuto", "wiki_eventos"):
    shutil.rmtree(f"{BASE}/{c}", ignore_errors=True)
