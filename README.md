# Curso Big Data e IA · CENIF

Material del curso de 20 horas (BOE-A-2021-7686). Este repositorio recoge los **Bloques 1 a 3** (fundamentos de Big Data, tratamiento de datos con Python y Spark en Databricks), el **bloque de IA** (slides y prácticas de aprendizaje por refuerzo y MCP) y un bloque sobre **la ingeniería informática en la era de la IA**. En las carpetas `proposals/` hay material complementario de los bloques 4 a 7, para quien quiera seguir por su cuenta.

Casos prácticos: **Real Sporting de Gijón, temporada 2015/16** (La Liga, datos abiertos de StatsBomb) en los bloques 1 y 2, y **cambios de Wikipedia en tiempo real** (Wikimedia EventStreams) en el bloque 3.

## Contenido

| Carpeta | Qué hay |
|---|---|
| `slides/` | Diapositivas trabajadas en clase, cada una en PDF y en versión web (`.html`, se abre en el navegador y funciona sin conexión): `Big-Data-e-IA_Bloques-1-y-2` (bloques 1 y 2), `Bloque-3_Spark-Databricks` (99 slides), `IA-Sesion-1` (90 slides: historia, juegos y refuerzo, tokens y RAG, agentes y coches autónomos), `IA-Sesion-2` (83 slides: reglas de conocimiento, aprendizaje automático, redes neuronales, PLN, Stable Diffusion y riesgos) e `Ingenieria-informatica-era-IA` (57 slides, retos de la profesión con IA, cómo cambian las entrevistas a perfiles junior con el caso de Canva y problemas nuevos que siguen necesitando a una persona; cada slide cita su fuente y las referencias completas están al final). |
| `slides/proposals/` | Material complementario que no se ha trabajado en clase: `Bloque-4_Visualizacion-BI`, `Bloque-5_Fundamentos-IA`, `Bloque-6_Machine-Learning`, `Bloque-7_Proyecto-integrador` y `Big-Data_Fundamentos-borrador` (versión inicial, incompleta, de los bloques 1 y 2). |
| `notebooks/` | Prácticas trabajadas: `01` (Google Colab), el notebook de Databricks del Bloque 3 (`02_BigData_Spark_Databricks_Wikimedia`, en `.py` e `.ipynb`), la demo de aprendizaje por refuerzo (`06_IA_Aprendizaje_Refuerzo`, versión Databricks y local) el servidor MCP de práctica (`07_IA_Servidor_MCP_Sporting.py`) y el proyecto supervisado en ocho pasos (`08_IA_Supervisado_8_pasos.ipynb`). |
| `notebooks/proposals/` | Prácticas complementarias de los bloques no trabajados: `02_BigData_Databricks_Spark_Sporting.py` y `02b_BigData_Streaming_Databricks` (versión anterior del Bloque 3 con datos del Sporting y streaming), `03_BigData_Visualizacion_BI_Sporting.ipynb` y `03b_BI_en_Databricks.py` (Bloque 4), `04_IA_MachineLearning_Sporting.ipynb` (Bloques 5 y 6) y `05_Proyecto_Integrador_Sporting.ipynb` (Bloque 7). |
| `data/` | `sporting_partidos.csv` (38 partidos) y `sporting_eventos.csv` (≈129.000 eventos). |

## Cómo usar los cuadernos

1. Entra en [Google Colab](https://colab.research.google.com) con tu cuenta de Google.
2. **Archivo → Subir cuaderno** y elige un fichero de `notebooks/` o `notebooks/proposals/` (o **Archivo → Abrir cuaderno → GitHub** si el repositorio está publicado).
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

7. `08_IA_Supervisado_8_pasos.ipynb` (Sesión 2 de IA) recorre los ocho pasos de un proyecto supervisado con datos reales: el valor de la vivienda en California (censo de 1990, incluido en scikit-learn). Se abre en Colab y no hay que subir nada.

8. `09_IA_Red_Neuronal_Juguete.py` (Sesión 2 de IA) genera las figuras de las slides de redes neuronales: una neurona que aprende a separar aprobados y suspensos y una red con capa oculta frente a un círculo. `pip install scikit-learn matplotlib` y `python 09_IA_Red_Neuronal_Juguete.py`.

   - `10_IA_Refuerzo_Laberinto.py`: Q-learning en un laberinto, con los caminos por episodio, la curva de recompensa y la tabla Q dibujada.
   - `11_IA_KMeans_Clientes.py`: k-means paso a paso con clientes y cómo se asignan clientes nuevos.

9. Análisis forense de imágenes (Sesión 2, Stable Diffusion): `herramientas/forense/analizar_fotos.sh CARPETA_FOTOS` descarga [Sherloq](https://github.com/GuidoBartoli/sherloq), le pasa cada foto de la carpeta y abre un informe con todas las salidas. Funciona en local (macOS o Linux); los detalles están en `herramientas/forense/README.md`.

10. Los cuadernos de `notebooks/proposals/` son complementarios: los `.ipynb` se abren en Colab como el 01 (necesitan los CSV de `data/`) y los `.py` se importan en Databricks como el 02.

## Datos y licencia

Los datos de fútbol proceden de [StatsBomb Open Data](https://github.com/statsbomb/open-data). Si los usas o compartes, debes **atribuir a StatsBomb** y seguir [sus condiciones de uso](https://github.com/statsbomb/open-data/blob/master/LICENSE.pdf).

## Autor

Pablo Mesa Canal
