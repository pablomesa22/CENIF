# Databricks notebook source
# MAGIC %md
# MAGIC # Bloque 4 (parte Databricks): el mismo KPI con SQL
# MAGIC
# MAGIC Requisito: haber ejecutado el notebook del Bloque 3, que crea las vistas temporales `eventos` y `partidos`.
# MAGIC Si no existen, vuelve a crearlas con `eventos.createOrReplaceTempView("eventos")` y `partidos.createOrReplaceTempView("partidos")`.
# MAGIC
# MAGIC Datos: StatsBomb Open Data. Si compartes análisis basados en ellos, cita a StatsBomb como fuente.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tabla por partido con xG a favor y en contra

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.jornada, p.rival, p.local, p.puntos,
# MAGIC   ROUND(SUM(CASE WHEN e.team = 'Sporting Gijón' THEN e.shot_xg ELSE 0 END), 2) AS xg_favor,
# MAGIC   ROUND(SUM(CASE WHEN e.team <> 'Sporting Gijón' THEN e.shot_xg ELSE 0 END), 2) AS xg_contra
# MAGIC FROM eventos e JOIN partidos p ON p.match_id = e.match_id
# MAGIC WHERE e.type = 'Shot'
# MAGIC GROUP BY p.jornada, p.rival, p.local, p.puntos
# MAGIC ORDER BY p.jornada

# COMMAND ----------

# MAGIC %md
# MAGIC En la tabla de resultados, usa el botón **+** para añadir una visualización (por ejemplo, barras de `xg_favor` y `xg_contra` por jornada).
# MAGIC Los menús pueden variar según la versión de Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Alerta de racha: 2 puntos o menos en los últimos 5 partidos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT jornada, rival, puntos_5
# MAGIC FROM (
# MAGIC   SELECT jornada, rival,
# MAGIC     SUM(puntos) OVER (ORDER BY jornada ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS puntos_5,
# MAGIC     COUNT(*) OVER (ORDER BY jornada ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS n
# MAGIC   FROM partidos
# MAGIC )
# MAGIC WHERE n = 5 AND puntos_5 <= 2
# MAGIC ORDER BY jornada

# COMMAND ----------

# MAGIC %md
# MAGIC Con los datos del curso deberían salir las jornadas 19, 20, 28 y 29. La ventana (5) y el umbral (2) son decisiones tuyas.
