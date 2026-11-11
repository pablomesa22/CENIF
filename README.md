# Curso Big Data e IA · CENIF

Material del curso de 20 horas (BOE-A-2021-7686). Este repositorio recoge los **Bloques 1 a 3** (fundamentos de Big Data, tratamiento de datos con Python y Spark en Databricks) y el **bloque de IA** (slides y prácticas de aprendizaje por refuerzo y MCP).

Casos prácticos: **Real Sporting de Gijón, temporada 2015/16** (La Liga, datos abiertos de StatsBomb) en los bloques 1 y 2, y **cambios de Wikipedia en tiempo real** (Wikimedia EventStreams) en el bloque 3.

## Contenido

| Carpeta | Qué hay |
|---|---|
| `slides/` | Diapositivas del curso: PDF (`Big-Data-e-IA_Bloques-1-y-2.pdf`) y versión web (`diapositivas.html`, ábrela en el navegador). Bloque de IA: `Bloque-IA_Update.pdf` (129 slides) y `Bloque-IA_Update.html` (versión web, funciona sin conexión). |
| `notebooks/` | Cuadernos de práctica `01`, `03`–`05` (para Google Colab) el notebook de Databricks del Bloque 3 (`02_BigData_Spark_Databricks_Wikimedia`, en `.py` e `.ipynb`) la demo de aprendizaje por refuerzo del bloque de IA (`06_IA_Aprendizaje_Refuerzo`, en versión Databricks y local) y el servidor MCP de práctica (`07_IA_Servidor_MCP_Sporting.py`). |
| `data/` | `sporting_partidos.csv` (38 partidos) y `sporting_eventos.csv` (≈129.000 eventos). |

## Cómo usar los cuadernos

1. Entra en [Google Colab](https://colab.research.google.com) con tu cuenta de Google.
2. **Archivo → Subir cuaderno** y elige un fichero de `notebooks/` (o **Archivo → Abrir cuaderno → GitHub** si el repositorio está publicado).
3. Ejecuta las celdas en orden. El cuaderno 01 descarga los datos de StatsBomb y genera los CSV; si prefieres no esperar, sube los de `data/` al panel de ficheros de Colab (icono de carpeta).
4. `02_BigData_Spark_Databricks_Wikimedia` (Bloque 3) es para Databricks Free Edition: **Workspace → Import** y elige el `.py` (o el `.ipynb`). No hay que subir ficheros: el propio notebook captura unos minutos del flujo de Wikimedia EventStreams y los guarda en un volumen. Cubre carga, filtros, agregaciones, joins, ventanas, SQL, cache, Structured Streaming (copia, filtro, conteo y ventanas) y errores típicos de Spark. Requiere acceso saliente a `stream.wikimedia.org` desde tu espacio de trabajo; en Free Edition los flujos solo funcionan con `trigger(availableNow=True)`.

5. `06_IA_Aprendizaje_Refuerzo` (bloque de IA) muestra cómo aprende una IA por prueba y error, como las de Trackmania: un coche recorre un circuito de texto sin ningún dataset previo, solo con premios y castigos (Q-learning). Hay dos versiones:
   - `06_IA_Aprendizaje_Refuerzo_Databricks.py`: **Workspace → Import** y **Run all**. Funciona en cualquier clúster sin instalar nada; al final guarda el historial de intentos como DataFrame de Spark.
   - `06_IA_Aprendizaje_Refuerzo_local.py`: `pip install numpy matplotlib` y `python 06_IA_Aprendizaje_Refuerzo_local.py`.

6. `07_IA_Servidor_MCP_Sporting.py` (bloque de IA, agentes) es un servidor MCP (Model Context Protocol) que da a un agente acceso de solo lectura a `data/sporting_partidos.csv`. Ofrece tres herramientas (`resumen_temporada`, `buscar_partidos`, `partido`), un recurso (`sporting://partidos`) y un prompt (`cronica_partido`).
   - Instalación: `pip install "mcp[cli]"` (SDK oficial de Python, versión 2).
   - Prueba sin IA: `python notebooks/07_IA_Servidor_MCP_Sporting.py --probar` lista las herramientas y las llama.
   - Inspector visual: `mcp dev notebooks/07_IA_Servidor_MCP_Sporting.py` (necesita Node.js).
   - Con un asistente como Claude Desktop: añadir un servidor MCP con el comando `python` y la ruta completa del script.

## Datos y licencia

Los datos de fútbol proceden de [StatsBomb Open Data](https://github.com/statsbomb/open-data). Si los usas o compartes, debes **atribuir a StatsBomb** y seguir [sus condiciones de uso](https://github.com/statsbomb/open-data/blob/master/LICENSE.pdf).

## Autor

Pablo Mesa Canal
