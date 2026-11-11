"""
Práctica: un servidor MCP con los datos del Sporting
====================================================

MCP (Model Context Protocol) es un estándar abierto para conectar aplicaciones
de IA con herramientas y datos. Este script es un servidor MCP pequeño que da a
cualquier agente o asistente compatible (Claude, VS Code, Cursor…) acceso de
SOLO LECTURA a los 38 partidos del Real Sporting de Gijón en la temporada 2015/16
(data/sporting_partidos.csv, datos de StatsBomb Open Data).

Ofrece las tres primitivas de un servidor MCP:
  - Herramientas (tools): funciones que el modelo puede llamar.
      resumen_temporada()          → victorias, empates, derrotas, goles y puntos
      buscar_partidos(...)         → filtrar por rival, campo o resultado
      partido(jornada)             → el detalle de una jornada
  - Recurso (resource): datos que la aplicación puede leer.
      sporting://partidos          → el CSV completo
  - Prompt: una plantilla de petición reutilizable.
      cronica_partido(jornada)     → pide una crónica usando las herramientas

Instalación (una vez):
    pip install "mcp[cli]"

Tres formas de usarlo:
  1) Prueba rápida, sin IA: llama a las herramientas desde un cliente MCP de prueba.
         python 07_IA_Servidor_MCP_Sporting.py --probar
  2) Inspector visual de MCP (abre una web para probar cada herramienta a mano):
         mcp dev 07_IA_Servidor_MCP_Sporting.py
  3) Conectarlo a un asistente como Claude Desktop: añadir en su configuración
     un servidor con el comando
         python /ruta/completa/07_IA_Servidor_MCP_Sporting.py
     El asistente lo arranca y le habla por stdin/stdout (transporte stdio).
"""

import csv
import os
import sys
from pathlib import Path

from mcp.server import MCPServer

# --- Datos ---------------------------------------------------------------------
# Por defecto busca ../data/sporting_partidos.csv (estructura del repositorio).
# Se puede cambiar con la variable de entorno SPORTING_CSV.
RUTA_CSV = Path(
    os.environ.get(
        "SPORTING_CSV",
        Path(__file__).resolve().parent.parent / "data" / "sporting_partidos.csv",
    )
)
NOMBRES_RESULTADO = {"V": "victoria", "E": "empate", "D": "derrota"}


def cargar_partidos():
    """Lee el CSV una vez y convierte los tipos. Solo lectura: nunca se modifica."""
    if not RUTA_CSV.exists():
        sys.exit(f"No encuentro {RUTA_CSV}. Indica la ruta con la variable SPORTING_CSV.")
    with open(RUTA_CSV, encoding="utf-8") as f:
        filas = list(csv.DictReader(f))
    for p in filas:
        p["jornada"] = int(p["jornada"])
        p["goles_favor"] = int(p["goles_favor"])
        p["goles_contra"] = int(p["goles_contra"])
        p["puntos"] = int(p["puntos"])
        p["puntos_acum"] = int(p["puntos_acum"])
        p["local"] = p["local"] == "True"
    return filas


PARTIDOS = cargar_partidos()


def resumen(p):
    """Una línea legible por partido."""
    campo = "en casa" if p["local"] else "fuera"
    return (f"J{p['jornada']} ({p['fecha']}) · Sporting {p['goles_favor']}-{p['goles_contra']} "
            f"{p['rival']} · {campo} · {NOMBRES_RESULTADO[p['resultado']]}")


# --- El servidor MCP ----------------------------------------------------------------
mcp = MCPServer("sporting-2015-16")


@mcp.tool()
def resumen_temporada() -> dict:
    """Balance de la temporada 2015/16 del Sporting: partidos, victorias, empates,
    derrotas, goles a favor y en contra, y puntos totales."""
    return {
        "partidos": len(PARTIDOS),
        "victorias": sum(p["resultado"] == "V" for p in PARTIDOS),
        "empates": sum(p["resultado"] == "E" for p in PARTIDOS),
        "derrotas": sum(p["resultado"] == "D" for p in PARTIDOS),
        "goles_favor": sum(p["goles_favor"] for p in PARTIDOS),
        "goles_contra": sum(p["goles_contra"] for p in PARTIDOS),
        "puntos": PARTIDOS[-1]["puntos_acum"],
    }


@mcp.tool()
def buscar_partidos(rival: str = "", en_casa: bool | None = None, resultado: str = "") -> list[str]:
    """Busca partidos de la temporada. Todos los filtros son opcionales.
    - rival: parte del nombre del equipo rival, por ejemplo "Madrid".
    - en_casa: True para partidos en El Molinón, False para partidos fuera.
    - resultado: "V" (victoria), "E" (empate) o "D" (derrota)."""
    encontrados = [
        p for p in PARTIDOS
        if rival.lower() in p["rival"].lower()
        and (en_casa is None or p["local"] == en_casa)
        and (not resultado or p["resultado"] == resultado.upper())
    ]
    return [resumen(p) for p in encontrados] or ["Ningún partido cumple esos filtros."]


@mcp.tool()
def partido(jornada: int) -> dict:
    """Detalle de un partido por número de jornada (de 1 a 38): fecha, rival, campo,
    estadio, árbitro, marcador, resultado y puntos acumulados tras esa jornada."""
    for p in PARTIDOS:
        if p["jornada"] == jornada:
            return {
                "jornada": p["jornada"],
                "fecha": p["fecha"],
                "rival": p["rival"],
                "en_casa": p["local"],
                "estadio": p["estadio"],
                "arbitro": p["arbitro"],
                "marcador": f"Sporting {p['goles_favor']}-{p['goles_contra']} {p['rival']}",
                "resultado": NOMBRES_RESULTADO[p["resultado"]],
                "puntos_acumulados": p["puntos_acum"],
            }
    return {"error": f"No hay jornada {jornada}. La temporada tiene {len(PARTIDOS)} jornadas."}


@mcp.resource("sporting://partidos")
def todos_los_partidos() -> str:
    """El CSV completo de los 38 partidos, tal cual."""
    return RUTA_CSV.read_text(encoding="utf-8")


@mcp.prompt()
def cronica_partido(jornada: int) -> str:
    """Plantilla para pedir una crónica breve de un partido."""
    return (
        f"Escribe una crónica breve (unas 120 palabras) del partido del Sporting de la "
        f"jornada {jornada} de la temporada 2015/16. Usa la herramienta 'partido' para "
        f"obtener los datos y no inventes nada que no esté en ellos: ni goleadores ni "
        f"minutos. Termina indicando cuántos puntos llevaba el equipo."
    )


# --- Prueba sin IA: un cliente MCP que habla con este mismo servidor -----------------
async def probar():
    from mcp import Client

    async with Client(mcp) as cliente:
        herramientas = await cliente.list_tools()
        print("Herramientas que anuncia el servidor:")
        for h in herramientas.tools:
            print(f"  - {h.name}: {h.description.splitlines()[0]}")

        for nombre, argumentos in [
            ("resumen_temporada", {}),
            ("buscar_partidos", {"en_casa": True, "resultado": "V"}),
            ("partido", {"jornada": 4}),
        ]:
            resultado = await cliente.call_tool(nombre, argumentos)
            print(f"\nLlamada: {nombre}({argumentos})")
            for bloque in resultado.content:  # una lista llega como varios bloques
                print(bloque.text)

        prompt = await cliente.get_prompt("cronica_partido", {"jornada": "4"})
        print("\nPrompt 'cronica_partido' para la jornada 4:")
        print(prompt.messages[0].content.text)


if __name__ == "__main__":
    if "--probar" in sys.argv:
        import asyncio
        asyncio.run(probar())
    else:
        mcp.run()  # transporte stdio: lo arranca el asistente o el inspector
