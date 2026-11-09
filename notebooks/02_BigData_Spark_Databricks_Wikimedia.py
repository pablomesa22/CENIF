# Databricks notebook source
# MAGIC %md
# MAGIC # Curso de Big Data e IA · Módulo 1
# MAGIC ## Bloque 3: Sistemas y arquitecturas Big Data (Spark en Databricks)
# MAGIC
# MAGIC **Duración estimada:** 3 horas
# MAGIC
# MAGIC **Caso práctico:** los **cambios de Wikipedia en tiempo real** (Wikimedia EventStreams). Es un flujo público y gratuito:
# MAGIC **no hay que descargar ni subir ningún fichero**; el propio notebook captura los datos.
# MAGIC
# MAGIC **Qué haremos:**
# MAGIC 1. Capturar unos minutos del flujo y cargarlo en un DataFrame de Spark.
# MAGIC 2. Filtros, agregaciones, joins, funciones de ventana y SQL.
# MAGIC 3. Cache, guardado y errores típicos de los jobs de Spark.
# MAGIC 4. **Structured Streaming**: leer lo mismo como flujo y ver cómo la tabla crece con datos nuevos.
# MAGIC
# MAGIC **Importante:** Databricks Free Edition usa cómputo *serverless*: no hay Spark UI ni API de RDD, y los flujos
# MAGIC solo admiten el disparador `availableNow` (procesa todo lo pendiente y termina).
# MAGIC Si tu espacio de trabajo no llega a Wikimedia, el docente puede compartir un fichero ya capturado
# MAGIC (súbelo a la carpeta `wiki_eventos` del volumen, sección 1, y salta la captura de la sección 3).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración: dónde guardamos los datos

# COMMAND ----------

# AJUSTA estos nombres si tu catálogo o esquema son otros (Catalog en el menú lateral)
CATALOGO = "workspace"
ESQUEMA  = "default"
VOLUMEN  = "wikimedia"

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
# MAGIC dentro del volumen. Capturamos **75 segundos** para tener datos suficientes y varios minutos de actividad.

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

# MAGIC %md
# MAGIC ## 4. Carga: del fichero JSON a un DataFrame
# MAGIC
# MAGIC Ahora lo leemos como un **DataFrame normal** (lectura por lotes, `read`). Spark lee el fichero repartido en particiones.
# MAGIC Con un esquema explícito evitamos que Spark tenga que adivinar los tipos leyendo todos los datos.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
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

cambios = (spark.read
           .schema(esquema_wiki)
           .json(f"{BASE}/wiki_eventos")
           .withColumn("instante", F.to_timestamp(F.from_unixtime("timestamp"))))

print("Cambios capturados:", cambios.count())   # acción: lanza el trabajo
cambios.printSchema()

# COMMAND ----------

display(cambios.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sintaxis básica de Spark DataFrames
# MAGIC
# MAGIC Es muy parecida a pandas y a SQL, pero cada operación se ejecuta de forma distribuida.
# MAGIC Spark es **perezoso**: las transformaciones no se ejecutan hasta que pedimos un resultado (`show`, `count`, `display`).

# COMMAND ----------

# Seleccionar columnas y filtrar: ediciones de la Wikipedia en español
(cambios
 .filter((F.col("wiki") == "eswiki") & (F.col("type") == "edit"))
 .select("instante", "title", "user", "bot")
 .show(5, truncate=False))

# COMMAND ----------

# Tipos de cambio más frecuentes
(cambios.groupBy("type")
 .count()
 .orderBy(F.desc("count"))
 .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ### El plan de ejecución
# MAGIC `explain()` muestra el plan físico que Spark ha elegido. Busca `Filter`, `HashAggregate` y `Exchange`
# MAGIC (un `Exchange` es un *shuffle*: datos que viajan entre máquinas). En Free Edition no hay Spark UI; usa también el **perfil de consulta**.

# COMMAND ----------

(cambios.filter(F.col("type") == "edit")
 .groupBy("wiki").count()
 .explain())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Agregaciones: ¿cuánta actividad es de bots?
# MAGIC
# MAGIC Contamos los cambios de cada Wikipedia y qué porcentaje ha hecho un bot. Nos quedamos con las que tienen actividad suficiente.
# MAGIC
# MAGIC **Pregunta:** ¿qué proyecto tiene el mayor porcentaje de cambios hechos por bots?

# COMMAND ----------

(cambios.groupBy("wiki")
 .agg(F.count("*").alias("cambios"),
      F.sum(F.when(F.col("bot") == True, 1).otherwise(0)).alias("de_bots"))
 .withColumn("bots_%", F.round(F.col("de_bots") / F.col("cambios") * 100, 1))
 .filter(F.col("cambios") >= 20)
 .orderBy(F.desc("cambios"))
 .show(10, truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC **Respuesta:** la que salga en tu tabla (depende del momento de la captura).
# MAGIC **Conclusión:** un `groupBy` con `agg` es el equivalente distribuido de una tabla dinámica; Spark calcula conteos parciales en cada partición y los combina.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Join: unir con una tabla pequeña
# MAGIC
# MAGIC Tenemos el código de cada proyecto (`enwiki`, `eswiki`...), pero queremos el **nombre del idioma**.
# MAGIC Creamos una tabla pequeña a mano y la unimos con `join`, igual que en SQL. Como es pequeña, la marcamos con `broadcast`:
# MAGIC Spark la envía entera a cada máquina y evita mover los datos grandes.

# COMMAND ----------

idiomas = spark.createDataFrame([
    ("enwiki", "inglés"), ("eswiki", "español"), ("frwiki", "francés"), ("dewiki", "alemán"),
    ("ruwiki", "ruso"), ("itwiki", "italiano"), ("jawiki", "japonés"), ("zhwiki", "chino"),
    ("commonswiki", "Commons (imágenes)"), ("wikidatawiki", "Wikidata (datos)"),
], "wiki string, idioma string")

(cambios
 .join(F.broadcast(idiomas), on="wiki", how="left")
 .withColumn("idioma", F.coalesce("idioma", F.lit("otros")))
 .groupBy("idioma")
 .agg(F.count("*").alias("cambios"))
 .orderBy(F.desc("cambios"))
 .show(12, truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** ¿por qué usamos `left` y no `inner`?
# MAGIC **Respuesta:** con `inner` perderíamos los cambios de proyectos que no están en la tabla; con `left` se conservan y los marcamos como `otros`.
# MAGIC **Conclusión:** el join de Spark es el de SQL; la tabla pequeña se difunde con `broadcast` para no mover la grande.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Funciones de ventana: los tres usuarios más activos de cada proyecto
# MAGIC
# MAGIC Una ventana permite calcular algo "dentro de cada grupo" sin perder las filas, por ejemplo un ranking por proyecto.
# MAGIC `partitionBy` define el grupo y `orderBy` el orden dentro de él. Excluimos a los bots.

# COMMAND ----------

por_usuario = (cambios.filter(F.col("bot") == False)
               .groupBy("wiki", "user")
               .agg(F.count("*").alias("cambios")))

ventana = Window.partitionBy("wiki").orderBy(F.desc("cambios"))

(por_usuario
 .withColumn("puesto", F.row_number().over(ventana))
 .filter(F.col("puesto") <= 3)
 .filter(F.col("wiki").isin("enwiki", "eswiki", "frwiki"))
 .orderBy("wiki", "puesto")
 .show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Lo mismo con SQL
# MAGIC
# MAGIC Spark permite consultar un DataFrame con SQL. Es el puente con las bases de datos relacionales que vimos en el Bloque 1.

# COMMAND ----------

cambios.createOrReplaceTempView("cambios")
idiomas.createOrReplaceTempView("idiomas")

# COMMAND ----------

%sql
SELECT COALESCE(i.idioma, 'otros') AS idioma,
       COUNT(*) AS cambios,
       SUM(CASE WHEN c.bot THEN 1 ELSE 0 END) AS de_bots
FROM cambios c
LEFT JOIN idiomas i ON c.wiki = i.wiki
GROUP BY COALESCE(i.idioma, 'otros')
ORDER BY cambios DESC
LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Cache y guardado
# MAGIC
# MAGIC Si vamos a reutilizar un DataFrame varias veces, `.cache()` lo guarda en memoria (en serverless solo funciona `df.cache()`;
# MAGIC `CACHE TABLE` y `cacheTable()` no). Para guardar un resultado en un formato propio de Big Data usamos **Delta** (o Parquet).

# COMMAND ----------

ediciones = cambios.filter(F.col("type") == "edit").cache()
ediciones.count()   # materializa la cache

# COMMAND ----------

resumen = (ediciones.groupBy("wiki")
           .agg(F.count("*").alias("ediciones"),
                F.countDistinct("user").alias("usuarios")))

resumen.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOGO}.{ESQUEMA}.resumen_ediciones_por_wiki")
display(spark.table(f"{CATALOGO}.{ESQUEMA}.resumen_ediciones_por_wiki").orderBy(F.desc("ediciones")).limit(10))

# COMMAND ----------

# Comprobación: la tabla debe aparecer en el catálogo (Catalog > workspace > default > Tables, o SQL Editor)
tablas = [t.tableName for t in spark.sql(f"SHOW TABLES IN {CATALOGO}.{ESQUEMA}").collect()]
print("Tablas en", f"{CATALOGO}.{ESQUEMA}:", tablas)
assert "resumen_ediciones_por_wiki" in tablas, "La tabla no se ha creado: revisa CATALOGO y ESQUEMA en la sección 1"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1 La caché tiene un coste
# MAGIC `cache()` ocupa memoria que luego falta para ejecutar. Mira dónde la guarda Spark y libérala cuando ya no la uses.

# COMMAND ----------

print("Nivel de almacenamiento:", ediciones.storageLevel)   # memoria y disco
ediciones.unpersist()                                       # libera la memoria

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.2 Formatos de guardado: Parquet, Avro y Delta
# MAGIC - **Parquet**: por columnas, con esquema y compresión (snappy por defecto). Es el estándar para analítica.
# MAGIC - **Avro**: por filas, con el esquema en el fichero. Se usa para mover registros entre sistemas (por ejemplo con Kafka).
# MAGIC - **Delta**: ficheros Parquet más un registro de transacciones (versiones, escrituras atómicas). Es el formato por defecto de las tablas en Databricks.
# MAGIC
# MAGIC **Cuidado con los ficheros:** Spark escribe una *carpeta* con un fichero por partición. Mira qué sale.

# COMMAND ----------

ruta = f"{BASE}/salida_parquet"
resumen.write.mode("overwrite").parquet(ruta)
display(dbutils.fs.ls(ruta))                     # _SUCCESS y uno o varios ficheros part-...

ruta_uno = f"{BASE}/salida_un_fichero"
resumen.coalesce(1).write.mode("overwrite").parquet(ruta_uno)   # un único fichero part
display(dbutils.fs.ls(ruta_uno))

ruta_wiki = f"{BASE}/salida_por_wiki"
cambios.write.mode("overwrite").partitionBy("wiki").parquet(ruta_wiki)   # una carpeta por wiki
display(dbutils.fs.ls(ruta_wiki))

# COMMAND ----------

# Avro: es un módulo externo; si tu entorno no lo trae, lo verás en el mensaje
try:
    resumen.write.mode("overwrite").format("avro").save(f"{BASE}/salida_avro")
    display(spark.read.format("avro").load(f"{BASE}/salida_avro"))
except Exception as e:
    print("Avro no disponible en este entorno:", str(e)[:200])

# COMMAND ----------

# Delta: cada escritura crea una versión de la tabla
display(spark.sql(f"DESCRIBE HISTORY {CATALOGO}.{ESQUEMA}.resumen_ediciones_por_wiki"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** si guardas un resultado con Spark y salen varios ficheros, ¿qué ha pasado?
# MAGIC **Respuesta:** había varias particiones y cada una escribió el suyo.
# MAGIC **Conclusión:** para analítica, Parquet o Delta; piensa en cuántos ficheros salen antes de escribir.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Structured Streaming: leer la carpeta como un flujo
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
# MAGIC ### 11.1 Caso 1: copia limpia del flujo
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
# MAGIC ### 11.2 Verlo crecer: datos nuevos ronda a ronda
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
# MAGIC ### 11.3 Caso 2: filtrar el flujo
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
# MAGIC ### 11.4 Caso 3: contar por tipo de cambio y por bot
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
# MAGIC ### 11.5 Caso 4: ventanas de tiempo
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
# MAGIC ### 11.6 Varias consultas, una sola fuente
# MAGIC
# MAGIC Hemos lanzado **cuatro consultas** sobre la misma carpeta. Cada una tiene su **propio checkpoint** (`ck_copia`,
# MAGIC `ck_humanas`, `ck_por_tipo`, `ck_por_minuto`), así que cada una avanza a su ritmo.
# MAGIC
# MAGIC Prueba: ejecuta otra **captura** y relanza **solo una** de las consultas. Las demás tablas no cambian hasta que las relances.
# MAGIC
# MAGIC **Conclusión:** una misma fuente puede alimentar muchas consultas independientes; el checkpoint es lo que las separa.

# COMMAND ----------

# Comprobación: las consultas de streaming escriben tablas Delta en el catálogo
tablas = [t.tableName for t in spark.sql(f"SHOW TABLES IN {CATALOGO}.{ESQUEMA}").collect()]
esperadas = ["wiki_cambios", "wiki_ediciones_humanas", "wiki_por_tipo", "wiki_por_minuto"]
print("Faltan:", [t for t in esperadas if t not in tablas] or "ninguna")
# Desde el SQL Editor se consultan con el nombre completo: SELECT * FROM workspace.default.wiki_cambios

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Errores típicos de los jobs de Spark
# MAGIC
# MAGIC Con unos miles de filas nada de esto falla, y justo por eso conviene verlo: **el código es el mismo con cientos de millones de filas**.
# MAGIC Usamos el DataFrame `cambios` de la sección 4.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.1 `collect()` trae todo al driver
# MAGIC `collect()` y `toPandas()` mueven *todos* los datos a una sola máquina. Aquí cabe; con datos grandes el driver se queda sin memoria.
# MAGIC
# MAGIC **Pregunta:** ¿quieres ver 20 filas, qué usas? **Respuesta:** `limit(20).show()`.

# COMMAND ----------

# PELIGROSO con datos grandes (aquí son pocos miles de filas, así que funciona):
filas = cambios.collect()
print("Filas traídas al driver:", len(filas))
del filas

# SEGURO: calcula en el clúster y trae solo el resultado
cambios.limit(20).show(5)
por_wiki = cambios.groupBy("wiki").count().collect()   # pocas filas: esto sí es razonable
print("Proyectos distintos:", len(por_wiki))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.2 Variables que no se comparten
# MAGIC Una variable de Python modificada dentro de una función distribuida no se actualiza en el driver: cada executor trabaja con su copia.
# MAGIC Para sumar o contar, **agrega**.

# COMMAND ----------

# Forma correcta: que cuente el clúster
n_bots = cambios.agg(F.sum(F.when(F.col("bot") == True, 1).otherwise(0))).first()[0]
print("Cambios hechos por bots (calculado en el clúster):", n_bots)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.3 Desbalanceo (skew): una clave domina
# MAGIC Mira cómo se reparten los cambios por proyecto. Si un proyecto concentra una parte enorme, un `groupBy("wiki")` envía todas
# MAGIC esas filas a una sola partición, y una tarea tarda mucho más que las demás.

# COMMAND ----------

por_wiki = cambios.groupBy("wiki").count().orderBy(F.desc("count"))
por_wiki.show(5)

total  = cambios.count()
maximo = por_wiki.first()["count"]
print(f"El proyecto con más cambios concentra el {maximo/total*100:.1f} % del total")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.4 Particiones: ni pocas ni demasiadas
# MAGIC El número de particiones decide cuántas tareas hay. Con pocos datos y 200 particiones de shuffle, casi todas quedan vacías
# MAGIC (AQE las une automáticamente). En serverless este parámetro se gestiona solo.

# COMMAND ----------

# Muestra el valor configurado (puede ser "auto" en Databricks)
print("shuffle.partitions =", spark.conf.get("spark.sql.shuffle.partitions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.5 Trampas en el código
# MAGIC - **UDF de Python** frente a funciones nativas: usa `F.lower`, `F.when`... siempre que puedas.
# MAGIC - **Ventanas sin `partitionBy`**: la ventana abarca todo el conjunto (compara con la sección 8, que sí lleva `partitionBy("wiki")`).
# MAGIC - **Broadcast de tablas grandes**: solo difunde las pequeñas (umbral por defecto: 10 MB).

# COMMAND ----------

# Función nativa en lugar de una UDF: título en minúsculas
cambios.select("title", F.lower("title").alias("titulo_min")).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.6 Recalcular sin cache
# MAGIC Cada acción vuelve a leer y procesar los ficheros desde el principio. Compara el tiempo de la misma consulta
# MAGIC sobre el DataFrame sin cache y sobre el cacheado de la sección 10.

# COMMAND ----------

import time
def tiempo(f):
    t = time.time(); f(); return round(time.time() - t, 2)

sin_cache = lambda: cambios.filter(F.col("type") == "edit").groupBy("wiki").count().collect()
con_cache = lambda: ediciones.groupBy("wiki").count().collect()
print("Sin cache:", tiempo(sin_cache), "s")
print("Con cache:", tiempo(con_cache), "s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.7 Muchos ficheros pequeños
# MAGIC Cada captura crea un fichero nuevo. Con miles de ficheros pequeños, leer es lento: Spark paga un coste por abrir cada uno.

# COMMAND ----------

ficheros = dbutils.fs.ls(f"{BASE}/wiki_eventos")
print("Ficheros en la carpeta:", len(ficheros))
print("Tamaño medio (bytes):", round(sum(f.size for f in ficheros) / max(len(ficheros), 1)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Verlo en vivo, evento a evento
# MAGIC
# MAGIC Hasta ahora hemos trabajado con capturas. Aquí ves el flujo **tal como llega**. En **Databricks Free (serverless) no se puede dejar
# MAGIC una consulta de Structured Streaming ejecutándose de forma continua** (solo `availableNow`), así que lo hacemos de dos maneras:
# MAGIC 1. Leer el flujo directamente y escribir cada evento según llega (sin Spark).
# MAGIC 2. Repetir *captura + lectura con Spark* en rondas cortas y ver crecer los contadores (microlotes hechos a mano).

# COMMAND ----------

# 13.1 Cada evento, línea a línea, durante 30 segundos
inicio, n = time.time(), 0
with requests.get(URL, headers=CABECERAS, stream=True, timeout=(10, 30)) as r:
    for linea in r.iter_lines():
        if not linea.startswith(b"data:"):
            continue
        try:
            e = json.loads(linea[5:].decode("utf-8"))
        except ValueError:
            continue
        n += 1
        print(f"{time.strftime('%H:%M:%S')}  {e.get('wiki','?'):<12} {e.get('type','?'):<10} {str(e.get('user','?'))[:18]:<18} {str(e.get('title',''))[:50]}", flush=True)
        if time.time() - inicio > 30:
            break
print(f"--- {n} eventos en 30 s ---")

# COMMAND ----------

# 13.2 Contadores que crecen: cada ronda captura 10 s y vuelve a leer todo con Spark
from IPython.display import clear_output
carpeta = f"{BASE}/wiki_vivo"
os.makedirs(carpeta, exist_ok=True)
for ronda in range(1, 7):
    ruta = f"{carpeta}/captura_{int(time.time())}.json"
    inicio = time.time()
    with requests.get(URL, headers=CABECERAS, stream=True, timeout=(10, 30)) as r, open(ruta, "w") as f:
        for linea in r.iter_lines():
            if linea.startswith(b"data:"):
                f.write(linea[5:].decode("utf-8").strip() + "\n")
            if time.time() - inicio > 10:
                break
    vivo = spark.read.schema(esquema_wiki).json(carpeta)
    clear_output(wait=True)
    print(f"Ronda {ronda}/6 · {time.strftime('%H:%M:%S')} · eventos acumulados: {vivo.count()}")
    vivo.groupBy("wiki").count().orderBy(F.desc("count")).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** ¿esto es streaming de verdad?
# MAGIC **Respuesta:** la 13.1 sí lee el flujo en directo, pero sin Spark. La 13.2 son microlotes que lanzamos nosotros; Structured Streaming hace lo mismo solo, con un checkpoint, cuando el cluster lo permite.
# MAGIC **Conclusión:** en Free usamos `availableNow` y bucles; en un cluster normal, `trigger(processingTime=...)` mantiene la consulta viva.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio propuesto
# MAGIC 1. Calcula el porcentaje de cambios hechos por bots en cada proyecto y muestra los 5 con mayor porcentaje (con al menos 20 cambios).
# MAGIC 2. ¿Cuántos cambios hay de cada tipo (`edit`, `new`, `log`, `categorize`...) en cada ventana de 1 minuto? Usa la sección 11.5 como modelo.
# MAGIC 3. Compara el tiempo de una agregación con y sin `.cache()`.
# MAGIC 4. Haz una **nueva captura** y relanza el caso 1 de streaming: ¿cuántas filas nuevas entran? ¿Por qué no se vuelven a leer las anteriores?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpieza (opcional)

# COMMAND ----------

import shutil
for t in ("wiki_cambios", "wiki_ediciones_humanas", "wiki_por_tipo", "wiki_por_minuto", "resumen_ediciones_por_wiki"):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOGO}.{ESQUEMA}.{t}")
for c in ("ck_copia", "ck_humanas", "ck_por_tipo", "ck_por_minuto", "wiki_eventos", "salida_parquet", "salida_un_fichero", "salida_por_wiki", "salida_avro", "wiki_vivo"):
    shutil.rmtree(f"{BASE}/{c}", ignore_errors=True)
