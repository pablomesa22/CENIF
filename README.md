# Curso Big Data e IA · CENIF

Material del curso de 20 horas (BOE-A-2021-7686). Este repositorio recoge los **Bloques 1 a 3**: fundamentos de Big Data, tratamiento de datos con Python y Spark en Databricks.

Casos prácticos: **Real Sporting de Gijón, temporada 2015/16** (La Liga, datos abiertos de StatsBomb) en los bloques 1 y 2, y **cambios de Wikipedia en tiempo real** (Wikimedia EventStreams) en el bloque 3.

## Contenido

| Carpeta | Qué hay |
|---|---|
| `slides/` | Diapositivas del curso: PDF (`Big-Data-e-IA_Bloques-1-y-2.pdf`) y versión web (`diapositivas.html`, ábrela en el navegador). |
| `notebooks/` | Cuadernos de práctica `01`, `03`–`05` (para Google Colab) y el notebook de Databricks del Bloque 3 (`02_BigData_Spark_Databricks_Wikimedia`, en `.py` e `.ipynb`). |
| `data/` | `sporting_partidos.csv` (38 partidos) y `sporting_eventos.csv` (≈129.000 eventos). |

## Cómo usar los cuadernos

1. Entra en [Google Colab](https://colab.research.google.com) con tu cuenta de Google.
2. **Archivo → Subir cuaderno** y elige un fichero de `notebooks/` (o **Archivo → Abrir cuaderno → GitHub** si el repositorio está publicado).
3. Ejecuta las celdas en orden. El cuaderno 01 descarga los datos de StatsBomb y genera los CSV; si prefieres no esperar, sube los de `data/` al panel de ficheros de Colab (icono de carpeta).
4. `02_BigData_Spark_Databricks_Wikimedia` (Bloque 3) es para Databricks Free Edition: **Workspace → Import** y elige el `.py` (o el `.ipynb`). No hay que subir ficheros: el propio notebook captura unos minutos del flujo de Wikimedia EventStreams y los guarda en un volumen. Cubre carga, filtros, agregaciones, joins, ventanas, SQL, cache, Structured Streaming (copia, filtro, conteo y ventanas) y errores típicos de Spark. Requiere acceso saliente a `stream.wikimedia.org` desde tu espacio de trabajo; en Free Edition los flujos solo funcionan con `trigger(availableNow=True)`.

## Datos y licencia

Los datos de fútbol proceden de [StatsBomb Open Data](https://github.com/statsbomb/open-data). Si los usas o compartes, debes **atribuir a StatsBomb** y seguir [sus condiciones de uso](https://github.com/statsbomb/open-data/blob/master/LICENSE.pdf).

## Autor

Pablo Mesa Canal
