# Curso Big Data e IA · CENIF

Material del curso de 20 horas (BOE-A-2021-7686). Este repositorio recoge los **Bloques 1 y 2**: fundamentos de Big Data y tratamiento de datos con Python.

Caso práctico: **Real Sporting de Gijón, temporada 2015/16** (La Liga), con datos abiertos de StatsBomb.

## Contenido

| Carpeta | Qué hay |
|---|---|
| `slides/` | Diapositivas del curso: PDF (`Big-Data-e-IA_Bloques-1-y-2.pdf`) y versión web (`diapositivas.html`, ábrela en el navegador). |
| `notebooks/` | Cuadernos de práctica (`01`–`05`, para Google Colab) y scripts de Databricks (`02_…py` y `02b_…` de streaming, en `.py` y `.ipynb`). |
| `data/` | `sporting_partidos.csv` (38 partidos) y `sporting_eventos.csv` (≈129.000 eventos). |

## Cómo usar los cuadernos

1. Entra en [Google Colab](https://colab.research.google.com) con tu cuenta de Google.
2. **Archivo → Subir cuaderno** y elige un fichero de `notebooks/` (o **Archivo → Abrir cuaderno → GitHub** si el repositorio está publicado).
3. Ejecuta las celdas en orden. El cuaderno 01 descarga los datos de StatsBomb y genera los CSV; si prefieres no esperar, sube los de `data/` al panel de ficheros de Colab (icono de carpeta).
4. El script `02_BigData_Databricks_Spark_Sporting.py` es para Databricks Community/Free Edition: importa el fichero y sube los CSV a `/FileStore/tables/`.
5. `02b_BigData_Streaming_Databricks` (ampliación del Bloque 3) lee un flujo real de Wikimedia EventStreams y simula un partido del Sporting. Requiere acceso saliente a `stream.wikimedia.org` desde tu espacio de trabajo; en Free Edition solo funciona con `trigger(availableNow=True)`.

## Datos y licencia

Los datos proceden de [StatsBomb Open Data](https://github.com/statsbomb/open-data). Si los usas o compartes, debes **atribuir a StatsBomb** y seguir [sus condiciones de uso](https://github.com/statsbomb/open-data/blob/master/LICENSE.pdf).

## Autor

Pablo Mesa Canal
