"""Analiza una carpeta de fotos con las herramientas de Sherloq, sin abrir la interfaz.

Uso (lo llama analizar_fotos.sh):
    python sherloq_lote.py CARPETA_FOTOS CARPETA_SALIDA

Para cada foto guarda una imagen por análisis y genera CARPETA_SALIDA/index.html,
una página con todas las salidas para compararlas de un vistazo.
Hay que ejecutarlo con la carpeta de Sherloq en el PYTHONPATH.
"""
import html
import json
import os
import sys
import traceback
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")  # sin ventanas

import cv2 as cv
import numpy as np
from PIL import Image, ExifTags
from PySide6.QtWidgets import QApplication

from gui.sherloq_app.tools.colors.pca import PcaWidget
from gui.sherloq_app.tools.detail.echo import EchoWidget
from gui.sherloq_app.tools.detail.frequency import FrequencyWidget
from gui.sherloq_app.tools.detail.gradient import GradientWidget
from gui.sherloq_app.tools.detail.wavelets import WaveletWidget
from gui.sherloq_app.tools.jpeg.ela import ElaWidget
from gui.sherloq_app.tools.noise.noise import NoiseWidget
from gui.sherloq_app.tools.noise.noise_estimmation import NoiseWaveletBlockingWidget

EXTENSIONES = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".webp"}
LADO_MAX = 2000  # las fotos más grandes se reducen para que el análisis no tarde minutos

# (clave, título, qué mirar, cómo crear la herramienta, de dónde sacar el resultado:
#  "visor" lee visor.processed; "visor.atributo" lee ese atributo)
ANALISIS = [
    ("ela", "Error Level Analysis (ELA)",
     "Vuelve a comprimir la foto en JPEG y muestra la diferencia. Una zona pegada o retocada suele brillar distinto que su entorno.",
     lambda ruta, img: ElaWidget(img), "viewer"),
    ("ruido", "Separación de ruido",
     "Aísla el ruido de la imagen. El sensor de una cámara deja un ruido parecido en toda la foto; una zona con ruido distinto es sospechosa.",
     lambda ruta, img: NoiseWidget(img), "viewer"),
    ("bloques_ruido", "Ruido por bloques (wavelets)",
     "Mapa del nivel de ruido por bloques. Busca zonas con un nivel claramente diferente al resto.",
     lambda ruta, img: NoiseWaveletBlockingWidget(str(ruta), img), "viewer"),
    ("gradiente", "Gradiente de luminancia",
     "Muestra hacia dónde cambia el brillo. La luz de un objeto pegado puede venir de otra dirección.",
     lambda ruta, img: GradientWidget(img), "viewer"),
    ("eco", "Filtro de bordes (echo)",
     "Resalta los bordes. Las zonas desenfocadas a propósito o suavizadas quedan muy planas.",
     lambda ruta, img: EchoWidget(img), "viewer"),
    ("wavelets", "Umbral de wavelets",
     "Reconstruye la imagen quitando detalle fino. Lo que se ha añadido o borrado puede notarse al cambiar el detalle.",
     lambda ruta, img: WaveletWidget(img), "viewer"),
    ("alta_frecuencia", "Alta frecuencia",
     "Solo el detalle fino. Las imágenes generadas o muy procesadas suelen tener un detalle fino distinto al de una foto.",
     lambda ruta, img: FrequencyWidget(img), "high_viewer"),
    ("espectro", "Espectro de Fourier (magnitud)",
     "Frecuencias de la imagen. Picos o rejillas repetidas pueden delatar remuestreo o imágenes generadas por IA.",
     lambda ruta, img: FrequencyWidget(img), "mag_viewer.original"),
    ("pca", "Proyección PCA del color",
     "Proyecta el color en sus componentes principales. Un objeto añadido puede destacar en una componente.",
     lambda ruta, img: PcaWidget(img), "viewer"),
]


def leer_metadatos(ruta):
    """EXIF de cámara y textos PNG (algunos generadores guardan ahí el prompt)."""
    datos = {}
    try:
        with Image.open(ruta) as im:
            datos["Formato"] = f"{im.format} {im.width}×{im.height}"
            exif = im.getexif()
            for etiqueta, valor in exif.items():
                nombre = ExifTags.TAGS.get(etiqueta, str(etiqueta))
                if nombre in ("Make", "Model", "Software", "DateTime", "Artist"):
                    datos[nombre] = str(valor)
            for clave, valor in (getattr(im, "text", None) or {}).items():
                datos[f"PNG · {clave}"] = str(valor)[:400]
    except Exception as error:  # noqa: BLE001
        datos["Error"] = str(error)
    if not any(k in datos for k in ("Make", "Model")):
        datos["Cámara"] = "sin datos de cámara (puede ser una captura, una imagen editada o generada)"
    return datos


def cargar(ruta):
    img = cv.imread(str(ruta), cv.IMREAD_COLOR)
    if img is None:
        return None
    alto, ancho = img.shape[:2]
    escala = LADO_MAX / max(alto, ancho)
    if escala < 1:
        img = cv.resize(img, (int(ancho * escala), int(alto * escala)), interpolation=cv.INTER_AREA)
    return img


def main():
    if len(sys.argv) != 3:
        sys.exit("Uso: python sherloq_lote.py CARPETA_FOTOS CARPETA_SALIDA")
    entrada, salida = Path(sys.argv[1]).expanduser(), Path(sys.argv[2]).expanduser()
    fotos = sorted(p for p in entrada.iterdir() if p.suffix.lower() in EXTENSIONES)
    if not fotos:
        sys.exit(f"No hay fotos en {entrada} (extensiones: {', '.join(sorted(EXTENSIONES))})")
    salida.mkdir(parents=True, exist_ok=True)
    app = QApplication.instance() or QApplication([])  # noqa: F841

    informe = []
    for n, ruta in enumerate(fotos, 1):
        print(f"[{n}/{len(fotos)}] {ruta.name}", flush=True)
        img = cargar(ruta)
        if img is None:
            print("   no se puede leer, se omite")
            continue
        carpeta = salida / ruta.stem
        carpeta.mkdir(exist_ok=True)
        cv.imwrite(str(carpeta / "00_original.png"), img)
        fila = {"foto": ruta.name, "carpeta": carpeta.name, "metadatos": leer_metadatos(ruta), "salidas": {}}
        for i, (clave, titulo, _, crear, visor) in enumerate(ANALISIS, 1):
            try:
                herramienta = crear(ruta, img)
                objeto = herramienta
                for parte in (visor if "." in visor else visor + ".processed").split("."):
                    objeto = getattr(objeto, parte)
                resultado = objeto
                if resultado is None:
                    raise RuntimeError("la herramienta no devolvió imagen")
                nombre = f"{i:02d}_{clave}.png"
                cv.imwrite(str(carpeta / nombre), resultado)
                fila["salidas"][clave] = nombre
                herramienta.deleteLater()
                print(f"   ok  {titulo}")
            except Exception as error:  # noqa: BLE001
                fila["salidas"][clave] = None
                print(f"   --  {titulo}: {error}")
                traceback.print_exc(limit=1, file=sys.stdout)
        informe.append(fila)

    (salida / "informe.json").write_text(json.dumps(informe, ensure_ascii=False, indent=2), encoding="utf-8")
    escribir_html(salida, informe)
    print(f"\nListo: {salida / 'index.html'}")


def escribir_html(salida, informe):
    e = html.escape
    leyenda = "".join(f"<dt>{e(t)}</dt><dd>{e(d)}</dd>" for _, t, d, _, _ in ANALISIS)
    bloques = []
    for fila in informe:
        meta = "".join(f"<li><b>{e(k)}:</b> {e(v)}</li>" for k, v in fila["metadatos"].items())
        celdas = [f'<figure><a href="{e(fila["carpeta"])}/00_original.png"><img src="{e(fila["carpeta"])}/00_original.png" loading="lazy"></a><figcaption>Original</figcaption></figure>']
        for clave, titulo, _, _, _ in ANALISIS:
            nombre = fila["salidas"].get(clave)
            if nombre:
                src = f'{fila["carpeta"]}/{nombre}'
                celdas.append(f'<figure><a href="{e(src)}"><img src="{e(src)}" loading="lazy"></a><figcaption>{e(titulo)}</figcaption></figure>')
            else:
                celdas.append(f'<figure class="falla"><div>no disponible</div><figcaption>{e(titulo)}</figcaption></figure>')
        bloques.append(f'<section><h2>{e(fila["foto"])}</h2><ul class="meta">{meta}</ul><div class="rejilla">{"".join(celdas)}</div></section>')
    pagina = f"""<!doctype html><html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Análisis forense con Sherloq</title>
<style>
body{{font-family:system-ui,sans-serif;margin:0;padding:24px;background:#F6F4EE;color:#0F1F2E}}
h1{{margin:0 0 8px}} h2{{margin:32px 0 8px;font-size:22px}}
.aviso{{background:#FBE6C8;padding:12px 16px;border-radius:10px;max-width:1100px}}
dl{{display:grid;grid-template-columns:max-content 1fr;gap:4px 16px;max-width:1100px}} dt{{font-weight:600}} dd{{margin:0}}
.meta{{list-style:none;padding:0;margin:0 0 12px;font-size:14px;color:#3F4B57}}
.rejilla{{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px}}
figure{{margin:0;background:#FFFDF8;border:1px solid #DDD8CB;border-radius:10px;padding:8px}}
figure img{{width:100%;height:180px;object-fit:contain;background:#0F1F2E;border-radius:6px}}
figcaption{{font-size:13px;margin-top:6px}} .falla div{{height:180px;display:grid;place-items:center;color:#8A6D3B}}
</style></head><body>
<h1>Análisis forense con Sherloq</h1>
<p class="aviso">Ninguna de estas pruebas demuestra por sí sola que una imagen sea falsa o auténtica. Se buscan zonas que se comporten distinto al resto y se contrastan varias pruebas y el contexto (origen, metadatos, otras fotos de la escena).</p>
<h2>Qué mirar en cada análisis</h2><dl>{leyenda}</dl>
{"".join(bloques)}
</body></html>"""
    (salida / "index.html").write_text(pagina, encoding="utf-8")


if __name__ == "__main__":
    main()
