"""Abre la aplicación de Sherloq (lo llama analizar_fotos.sh --gui).

Carga TensorFlow antes que el resto: en macOS, TensorFlow 2.16 puede quedarse
bloqueado al importarse después de pandas o de Qt, y Sherloq lo carga al arrancar.
Si algo se queda colgado más de 3 minutos, se imprime dónde está para poder diagnosticarlo.
"""
import faulthandler
import sys
import time

faulthandler.dump_traceback_later(180, exit=False)

inicio = time.time()
print("Cargando TensorFlow (la primera vez puede tardar uno o dos minutos)...", flush=True)
import tensorflow  # noqa: E402,F401  (primero, a propósito)

print(f"TensorFlow {tensorflow.__version__} listo en {time.time() - inicio:.0f} s", flush=True)

print("Cargando las herramientas de Sherloq...", flush=True)
from PySide6.QtCore import QTimer  # noqa: E402
from PySide6.QtWidgets import QApplication  # noqa: E402

from gui.sherloq_app.main import MainWindow  # noqa: E402

aplicacion = QApplication(sys.argv)

ventana = MainWindow()
ventana.raise_()
ventana.activateWindow()
faulthandler.cancel_dump_traceback_later()
print(f"Sherloq abierto en {time.time() - inicio:.0f} s. Para salir, cierra su ventana.", flush=True)

if "--prueba" in sys.argv:  # comprobación sin interfaz: abre y cierra
    QTimer.singleShot(500, aplicacion.quit)
sys.exit(aplicacion.exec())
