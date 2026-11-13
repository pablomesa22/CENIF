#!/usr/bin/env bash
# Análisis forense de una carpeta de fotos con Sherloq (https://github.com/GuidoBartoli/sherloq).
#
# Uso:
#   ./analizar_fotos.sh CARPETA_FOTOS [CARPETA_SALIDA]   analiza las fotos y abre el informe
#   ./analizar_fotos.sh --gui                            instala todo y abre la aplicación de Sherloq
#
# La primera vez descarga Sherloq e instala Python 3.11 y las librerías en un entorno aislado
# (dentro de esta carpeta, no toca el Python del sistema). Probado en macOS y Linux.
set -euo pipefail

INICIO="$(pwd)"
AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHERLOQ="$AQUI/sherloq"
VERSION="3fe95fcb56037e47e2eefbc2d3785804a31a74f5"   # commit de Sherloq con el que se probó el script (16/07/2026)

info() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
error() { printf '\n\033[1;31mError: %s\033[0m\n' "$*" >&2; exit 1; }

# 1. Herramientas necesarias: git y uv (gestor de Python)
command -v git >/dev/null || error "falta git. En macOS: xcode-select --install"
if ! command -v uv >/dev/null; then
  export PATH="$HOME/.local/bin:$PATH"
  if ! command -v uv >/dev/null; then
    info "Instalando uv (gestor de entornos de Python de Astral)"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
  fi
fi

# 2. Descargar Sherloq (solo la primera vez)
if [ ! -d "$SHERLOQ/.git" ]; then
  info "Descargando Sherloq"
  git clone https://github.com/GuidoBartoli/sherloq.git "$SHERLOQ"
  git -C "$SHERLOQ" checkout -q "$VERSION"
fi

# 3. Entorno de Python 3.11 con las librerías del análisis por lotes
cd "$SHERLOQ"
PYTHON_PEDIDO="3.11"
ARQ_ESPERADA=""
if [ "$(uname)" = "Darwin" ] && [ "$(sysctl -n hw.optional.arm64 2>/dev/null)" = "1" ]; then
  # Mac con Apple Silicon: Python nativo arm64. Un Python x86_64 iría por Rosetta,
  # mucho más lento, y TensorFlow puede no llegar a arrancar.
  PYTHON_PEDIDO="cpython-3.11-macos-aarch64-none"
  ARQ_ESPERADA="arm64"
fi
if [ -x .venv/bin/python ] && [ -n "$ARQ_ESPERADA" ] && \
   [ "$(.venv/bin/python -c 'import platform; print(platform.machine())')" != "$ARQ_ESPERADA" ]; then
  info "El entorno usa un Python de Intel en un Mac con Apple Silicon: se vuelve a crear"
  rm -rf .venv
fi
if [ ! -x .venv/bin/python ]; then
  info "Creando el entorno de Python 3.11"
  uv venv --python "$PYTHON_PEDIDO" .venv
fi
if [ ! -f .venv/.lote_ok ]; then
  info "Instalando las librerías del análisis (unos 300 MB)"
  uv pip install --python .venv/bin/python "numpy<2" "opencv-contrib-python-headless==4.*" \
    "pyside6==6.7.*" "matplotlib==3.8.*" "PyWavelets==1.5.*" "rawpy==0.19.*" "scikit-learn==1.5.*" pillow
  touch .venv/.lote_ok
fi

# Modo aplicación: instala todas las dependencias de Sherloq y abre la interfaz
if [ "${1:-}" = "--gui" ]; then
  if [ ! -f .venv/.gui_ok ]; then
    info "Instalando todas las dependencias de Sherloq (incluye TensorFlow, unos 2 GB)"
    if [ "$(uname)" = "Darwin" ] && ! brew list libmagic >/dev/null 2>&1; then
      command -v brew >/dev/null && brew install libmagic || echo "Aviso: instala libmagic (brew install libmagic) para la herramienta File Digest"
    fi
    uv pip install --python .venv/bin/python -r gui/requirements.txt
    touch .venv/.gui_ok
  fi
  info "Abriendo Sherloq (la ventana puede tardar un poco; mira también el Dock)"
  PYTHONPATH="$SHERLOQ" exec .venv/bin/python "$AQUI/sherloq_gui.py"
fi

# 4. Análisis por lotes
[ $# -ge 1 ] || error "indica la carpeta de fotos. Ejemplo: ./analizar_fotos.sh ~/Desktop/fotos"
FOTOS="$(cd "$INICIO" && cd "$1" 2>/dev/null && pwd)" || error "no existe la carpeta $1"
SALIDA="${2:-$FOTOS/analisis_sherloq}"
case "$SALIDA" in /*) ;; *) SALIDA="$INICIO/$SALIDA" ;; esac
info "Analizando $FOTOS"
PYTHONPATH="$SHERLOQ" .venv/bin/python "$AQUI/sherloq_lote.py" "$FOTOS" "$SALIDA"

# 5. Abrir el informe
INFORME="$SALIDA/index.html"
echo "Informe: $INFORME"
if [ "$(uname)" = "Darwin" ]; then open "$INFORME"
elif command -v xdg-open >/dev/null; then xdg-open "$INFORME" >/dev/null 2>&1 || true
fi
