# Databricks notebook source
# MAGIC %md
# MAGIC # Curso de Big Data e IA - Modulo 1
# MAGIC ## Bloque 3: Sistemas y arquitecturas Big Data (Spark en Databricks)
# MAGIC
# MAGIC **Duracion estimada:** 3 horas
# MAGIC
# MAGIC **Caso practico:** eventos de la temporada 2015/16 de La Liga del Real Sporting de Gijon (38 partidos).
# MAGIC
# MAGIC **Datos:** StatsBomb Open Data (https://github.com/statsbomb/open-data). Si publicas o compartes analisis
# MAGIC basados en estos datos, indica StatsBomb como fuente.
# MAGIC
# MAGIC **Objetivos:**
# MAGIC - Entender la diferencia entre procesar en local (pandas) y en un cluster (Spark).
# MAGIC - Cargar los CSV generados en el Notebook 1 en DataFrames de Spark.
# MAGIC - Hacer filtros, agregaciones, joins y funciones de ventana.
# MAGIC - Usar SQL sobre los mismos datos.
# MAGIC - Leer un plan de ejecucion (`explain`) y reconocer errores tipicos de los jobs de Spark.
# MAGIC
# MAGIC ---
# MAGIC ### Preparacion en Databricks (version gratuita)
# MAGIC 1. Crea una cuenta gratuita en Databricks y abre un notebook (Workspace > Import, y selecciona este `.py`).
# MAGIC 2. Ejecuta la celda de configuracion de la seccion 2: crea un **volumen** (una carpeta gobernada por Unity Catalog).
# MAGIC 3. Sube `sporting_partidos.csv` y `sporting_eventos.csv` (generados en el Notebook 1) a ese volumen desde
# MAGIC    **Catalog > tu catalogo > tu esquema > el volumen > Upload to this volume**.
# MAGIC 4. Comprueba con la celda de la seccion 2 que Spark ve los dos ficheros.
# MAGIC
# MAGIC > En Databricks Free Edition el calculo es *serverless*: no hay Spark UI ni API de RDD, y la cache solo funciona con `df.cache()`.
# MAGIC > La ruta antigua `/FileStore/...` puede no estar disponible; por eso usamos volumenes.
# MAGIC
# MAGIC Despues de este notebook, en casa: `02b_BigData_Streaming_Databricks` (streaming con datos reales de Wikimedia).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Computacion distribuida: la idea clave
# MAGIC
# MAGIC Cuando los datos no caben en una maquina o tardan demasiado, se reparten entre las maquinas de un **cluster**.
# MAGIC Cada una procesa una parte en paralelo y despues se combinan los resultados.
# MAGIC
# MAGIC **Apache Spark** es el motor de procesamiento distribuido mas usado. Aqui trabajamos con unos 129.000 eventos,
# MAGIC una cantidad pequena para Spark, pero el codigo es el mismo que usarias con cientos de millones de filas.
# MAGIC
# MAGIC > El repositorio completo de StatsBomb tiene miles de partidos. Con todos ellos si tendria sentido un cluster.

# COMMAND ----------

# MAGIC %md ## 2. Carga de los datos

# COMMAND ----------

# AJUSTA estos nombres si tu catalogo o esquema son otros (Catalog en el menu lateral)
CATALOGO = "workspace"
ESQUEMA  = "default"
VOLUMEN  = "sporting"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{ESQUEMA}.{VOLUMEN}")
BASE = f"/Volumes/{CATALOGO}/{ESQUEMA}/{VOLUMEN}"
print("Sube los CSV a:", BASE)
display(dbutils.fs.ls(BASE))   # deben aparecer sporting_eventos.csv y sporting_partidos.csv

# COMMAND ----------

ruta_eventos  = f"{BASE}/sporting_eventos.csv"
ruta_partidos = f"{BASE}/sporting_partidos.csv"

eventos = (spark.read
           .option("header", "true")
           .option("inferSchema", "true")
           .csv(ruta_eventos))

partidos = (spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(ruta_partidos))

print(f"Eventos: {eventos.count()}  |  Partidos: {partidos.count()}")
eventos.printSchema()

# COMMAND ----------

display(eventos.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sintaxis basica de Spark DataFrames
# MAGIC
# MAGIC Es muy parecida a pandas y a SQL, pero cada operacion se ejecuta de forma distribuida.
# MAGIC Spark es **perezoso**: las transformaciones no se ejecutan hasta que pedimos un resultado (`show`, `count`, `display`).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Seleccionar columnas y filtrar
(eventos
 .filter(F.col("team") == "Sporting Gijón")
 .select("match_id", "minute", "type", "player")
 .show(5))

# COMMAND ----------

# Tipos de evento mas frecuentes
(eventos.groupBy("type")
 .count()
 .orderBy(F.desc("count"))
 .show(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### El plan de ejecucion
# MAGIC `explain()` muestra el plan fisico que Spark ha elegido. Busca `Filter`, `HashAggregate` y `Exchange`
# MAGIC (un `Exchange` es un *shuffle*: datos que viajan entre maquinas). En Free Edition no hay Spark UI; usa tambien el **perfil de consulta**.

# COMMAND ----------

(eventos.filter(F.col("team") == "Sporting Gijón")
 .groupBy("type").count()
 .explain())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Agregaciones: precision de pase por equipo
# MAGIC
# MAGIC Recuerda la convencion del Notebook 1: un pase con `pass_outcome = Completo` es un pase acertado.

# COMMAND ----------

pases = eventos.filter(F.col("type") == "Pass")

(pases.groupBy("team")
 .agg(F.count("*").alias("pases"),
      F.sum(F.when(F.col("pass_outcome") == "Completo", 1).otherwise(0)).alias("completos"))
 .withColumn("acierto_%", F.round(F.col("completos") / F.col("pases") * 100, 1))
 .orderBy(F.desc("pases"))
 .show(10, truncate=False))

# COMMAND ----------

# MAGIC %md ## 5. Tiros, goles y xG

# COMMAND ----------

tiros = eventos.filter(F.col("type") == "Shot")

(tiros.groupBy("team")
 .agg(F.count("*").alias("tiros"),
      F.sum(F.when(F.col("shot_outcome") == "Goal", 1).otherwise(0)).alias("goles"),
      F.round(F.sum("shot_xg"), 1).alias("xg_total"))
 .withColumn("goles_menos_xg", F.round(F.col("goles") - F.col("xg_total"), 1))
 .orderBy(F.desc("tiros"))
 .show(10, truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Join: unir eventos con partidos
# MAGIC
# MAGIC `partidos` tiene el resultado de cada partido; `eventos` tiene lo que paso. Los unimos por `match_id`,
# MAGIC igual que haciamos con un JOIN en SQL. Asi sabemos cuantos tiros hizo el Sporting segun el resultado.

# COMMAND ----------

tiros_sporting = tiros.filter(F.col("team") == "Sporting Gijón")

(tiros_sporting
 .join(partidos.select("match_id", "resultado", "local"), on="match_id", how="inner")
 .groupBy("resultado")
 .agg(F.countDistinct("match_id").alias("partidos"),
      F.count("*").alias("tiros"),
      F.round(F.sum("shot_xg"), 1).alias("xg"))
 .withColumn("tiros_por_partido", F.round(F.col("tiros") / F.col("partidos"), 1))
 .orderBy("resultado")
 .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Funciones de ventana: top 3 tiradores de cada equipo
# MAGIC
# MAGIC Una ventana permite calcular algo "dentro de cada grupo" sin perder las filas, por ejemplo un ranking por equipo.

# COMMAND ----------

por_jugador = (tiros.groupBy("team", "player")
               .agg(F.count("*").alias("tiros"),
                    F.sum(F.when(F.col("shot_outcome") == "Goal", 1).otherwise(0)).alias("goles")))

ventana = Window.partitionBy("team").orderBy(F.desc("tiros"))

(por_jugador
 .withColumn("puesto", F.row_number().over(ventana))
 .filter(F.col("puesto") <= 3)
 .filter(F.col("team").isin("Sporting Gijón", "Barcelona", "Real Madrid"))
 .orderBy("team", "puesto")
 .show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Lo mismo con SQL
# MAGIC
# MAGIC Spark permite consultar un DataFrame con SQL. Es el puente con las bases de datos relacionales que vimos en el Bloque 1.

# COMMAND ----------

eventos.createOrReplaceTempView("eventos")
partidos.createOrReplaceTempView("partidos")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.rival,
# MAGIC        p.resultado,
# MAGIC        COUNT(*) AS tiros_sporting,
# MAGIC        ROUND(SUM(e.shot_xg), 2) AS xg_sporting
# MAGIC FROM eventos e
# MAGIC JOIN partidos p ON e.match_id = p.match_id
# MAGIC WHERE e.type = 'Shot' AND e.team = 'Sporting Gijón'
# MAGIC GROUP BY p.rival, p.resultado
# MAGIC ORDER BY xg_sporting DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cache y guardado
# MAGIC
# MAGIC Si vamos a reutilizar un DataFrame varias veces, `.cache()` lo guarda en memoria. Para guardar un resultado en un formato
# MAGIC propio de Big Data usamos **Delta** (o Parquet).

# COMMAND ----------

tiros.cache()
tiros.count()  # materializa la cache

# COMMAND ----------

resumen_equipos = (tiros.groupBy("team")
                   .agg(F.count("*").alias("tiros"),
                        F.round(F.sum("shot_xg"), 1).alias("xg_total")))

resumen_equipos.write.mode("overwrite").format("delta").saveAsTable("resumen_tiros_por_equipo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Errores tipicos de los jobs de Spark
# MAGIC Con 129.000 filas nada de esto falla, y justo por eso conviene verlo: **el codigo es el mismo con cientos de millones de filas**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1 `collect()` trae todo al driver
# MAGIC `collect()` y `toPandas()` mueven *todos* los datos a una sola maquina. Aqui cabe; con datos grandes el driver se queda sin memoria.
# MAGIC Pregunta: quieres ver 20 filas, que usas? Respuesta: `limit(20).show()`.

# COMMAND ----------

# PELIGROSO con datos grandes (aqui son 129.000 filas, asi que funciona):
filas = eventos.collect()
print("Filas traidas al driver:", len(filas))
del filas

# SEGURO: calcula en el cluster y trae solo el resultado
eventos.limit(20).show(5)
resumen = eventos.groupBy("team").count().collect()   # 20 filas: esto si es razonable
print("Equipos:", len(resumen))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.2 Variables que no se comparten
# MAGIC Una variable de Python modificada dentro de una funcion distribuida no se actualiza en el driver.
# MAGIC En `foreach` cada executor trabaja con su copia. Para sumar, **agrega**.
# MAGIC (En serverless `foreach` puede no estar disponible; si da error, ese es el aviso de la diapositiva.)

# COMMAND ----------

# Forma correcta: que sume el cluster
xg_total = tiros.agg(F.sum("shot_xg")).first()[0]
print("xG total (suma en el cluster):", round(xg_total, 1))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.3 Desbalanceo (skew): una clave domina
# MAGIC El Sporting aparece en los 38 partidos y el resto de equipos en uno o dos. Mira como se reparten los eventos por equipo:

# COMMAND ----------

por_equipo = eventos.groupBy("team").count().orderBy(F.desc("count"))
por_equipo.show(5)

total = eventos.count()
maximo = por_equipo.first()["count"]
print(f"El equipo con mas eventos concentra el {maximo/total*100:.1f} % del total")
# Un groupBy("team") envia todas las filas de cada equipo a una misma particion: la del Sporting es mucho mayor.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.4 Particiones: ni pocas ni demasiadas
# MAGIC El numero de particiones decide cuantas tareas hay. Con pocos datos y 200 particiones de shuffle, casi todas quedan vacias
# MAGIC (AQE las une automaticamente). En serverless `spark.sql.shuffle.partitions` esta en modo automatico.

# COMMAND ----------

# Muestra el valor configurado (puede ser "auto" en Databricks)
print("shuffle.partitions =", spark.conf.get("spark.sql.shuffle.partitions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.5 Trampas en el codigo
# MAGIC - **UDF de Python** frente a funciones nativas: usa `F.round`, `F.when`... siempre que puedas.
# MAGIC - **Ventanas sin `partitionBy`**: la ventana abarca todo el conjunto (compara con la seccion 7, que si lleva `partitionBy("team")`).
# MAGIC - **Broadcast de tablas grandes**: solo difunde las pequenas (umbral por defecto: 10 MB).

# COMMAND ----------

# Ejemplo de funcion nativa en lugar de UDF: redondear xG a 2 decimales
tiros.select("player", F.round("shot_xg", 2).alias("xg_2dec")).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.6 Recalcular sin cache y ficheros pequenos
# MAGIC Cada accion vuelve a leer y procesar el CSV desde el principio. Por eso en la seccion 9 usamos `tiros.cache()`.
# MAGIC Con `df.cache()` basta: `CACHE TABLE`, `cacheTable()` y `checkpoint()` no funcionan en serverless.
# MAGIC
# MAGIC Y cuidado con **muchos ficheros pequenos**: cada captura del notebook 02b crea un fichero nuevo; miles de ficheros pequenos hacen lenta la lectura.

# COMMAND ----------

import time
def tiempo(f):
    t = time.time(); f(); return round(time.time() - t, 2)

consulta = lambda: eventos.filter(F.col("type") == "Shot").groupBy("team").count().collect()
print("Sin cache:", tiempo(consulta), "s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio propuesto
# MAGIC 1. Calcula el porcentaje de pases completos **solo del Sporting** en cada partido y ordena los partidos de mejor a peor.
# MAGIC 2. Cuantos goles marco el Sporting en la segunda parte (`period = 2`) frente a la primera?
# MAGIC 3. Compara el tiempo de una agregacion con y sin `.cache()`.
# MAGIC 4. Reflexiona: que ventajas tendria Spark si tuvieras los eventos de todas las competiciones del repositorio?
# MAGIC
# MAGIC **Extra:** busca en el perfil de consulta el paso mas lento de la seccion 5 y di si hay desbalanceo o spill.
