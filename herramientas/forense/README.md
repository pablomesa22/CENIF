# Análisis forense de imágenes con Sherloq

Práctica de la Sesión 2 del bloque de IA (parte 3, Stable Diffusion y análisis forense).

[Sherloq](https://github.com/GuidoBartoli/sherloq) es una herramienta de código abierto de análisis forense de imágenes. Es una aplicación de escritorio, así que no funciona en Colab ni en Databricks: se usa en local.

## Uso

```bash
cd herramientas/forense
./analizar_fotos.sh ~/Desktop/fotos          # analiza todas las fotos de la carpeta
./analizar_fotos.sh ~/Desktop/fotos salida   # igual, guardando el resultado en ./salida
./analizar_fotos.sh --gui                    # instala todo y abre la aplicación de Sherloq
```

La primera vez el script:

1. Instala [uv](https://docs.astral.sh/uv/) si no está (gestor de Python).
2. Descarga Sherloq en `herramientas/forense/sherloq` (versión fijada en el script).
3. Crea un entorno de Python 3.11 aislado e instala las librerías del análisis (unos 300 MB).

Después pasa a cada foto nueve análisis de Sherloq sin abrir ventanas y lee sus metadatos:

| Análisis | Qué mirar |
|---|---|
| Error Level Analysis (ELA) | Zonas que responden distinto al recomprimir en JPEG |
| Separación de ruido | Zonas con un ruido distinto al del resto de la foto |
| Ruido por bloques | Bloques con un nivel de ruido claramente diferente |
| Gradiente de luminancia | Luz que viene de una dirección incoherente |
| Filtro de bordes (echo) | Zonas suavizadas o desenfocadas a propósito |
| Umbral de wavelets | Lo añadido o borrado al cambiar el detalle fino |
| Alta frecuencia | Detalle fino ausente o diferente |
| Espectro de Fourier | Picos o rejillas repetidas (remuestreo, imágenes generadas) |
| Proyección PCA del color | Objetos que destacan en una componente de color |

El resultado queda en `analisis_sherloq/` dentro de la carpeta de fotos: una subcarpeta con las imágenes de cada foto, `informe.json` y `index.html`, que se abre solo en el navegador.

Ninguna prueba demuestra por sí sola que una imagen sea falsa o auténtica: se comparan zonas de la misma foto y se contrastan varias pruebas con el contexto.

## Requisitos

macOS o Linux con `git` y conexión a internet (en Windows, con WSL). El modo `--gui` instala además todas las dependencias de Sherloq, entre ellas TensorFlow (unos 2 GB); en macOS conviene tener `libmagic` (`brew install libmagic`).
