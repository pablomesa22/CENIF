"""
Aprendizaje por refuerzo: un coche aprende un circuito por prueba y error
=========================================================================

La misma idea que las IA de Trackmania, en pequeño:
  - Nadie le enseña el camino ni le da partidas de ejemplo (sin dataset).
  - Empieza moviéndose al azar.
  - Recibe premios y castigos: -1 por cada paso (cuanto más tarde, peor),
    -5 si choca contra un muro y +100 al llegar a la meta.
  - Repite el circuito cientos de veces y va recordando qué movimiento
    le fue mejor en cada casilla (una tabla Q).

Cómo ejecutarlo:
  - En local:      pip install numpy matplotlib   y   python entrenamiento_prueba_error.py
  - En Databricks: pega el código en una celda de un notebook y ejecútala
                   (numpy y matplotlib ya vienen instalados).

Prueba a cambiar los parámetros de abajo (EPISODIOS, EXPLORACION_MINIMA,
las recompensas o el propio circuito) y vuelve a ejecutarlo.
"""

import random

import matplotlib.pyplot as plt
import numpy as np

# --- El circuito ------------------------------------------------------------
# S = salida, F = meta, # = muro, . = pista. Tiene caminos sin salida a propósito.
CIRCUITO = [
    "############",
    "#S...#.....#",
    "###.##.###.#",
    "#...#..#...#",
    "#.###.##.###",
    "#.....#....#",
    "#####.####F#",
    "############",
]

# --- Parámetros que se pueden tocar en clase -------------------------------------
EPISODIOS = 500           # cuántas vueltas de entrenamiento
MAX_PASOS = 300           # si no llega en estos pasos, se corta el intento
ALFA = 0.5                # velocidad de aprendizaje: cuánto pesa cada experiencia nueva
GAMMA = 0.95              # cuánto le importan las recompensas futuras frente a las inmediatas
EXPLORACION_MINIMA = 0.05 # probabilidad mínima de probar un movimiento al azar
RECOMPENSA_PASO = -1
RECOMPENSA_CHOQUE = -5
RECOMPENSA_META = 100
SEMILLA = 42              # para que el resultado se repita igual en cada ejecución

ACCIONES = [(-1, 0), (1, 0), (0, -1), (0, 1)]  # arriba, abajo, izquierda, derecha
NOMBRES = ["↑", "↓", "←", "→"]

random.seed(SEMILLA)
np.random.seed(SEMILLA)

FILAS, COLUMNAS = len(CIRCUITO), len(CIRCUITO[0])


def buscar(letra):
    for f, fila in enumerate(CIRCUITO):
        if letra in fila:
            return f, fila.index(letra)


SALIDA, META = buscar("S"), buscar("F")


def mover(pos, accion):
    """Aplica un movimiento y devuelve (nueva posición, recompensa, ¿ha terminado?)."""
    df, dc = ACCIONES[accion]
    nueva = (pos[0] + df, pos[1] + dc)
    if CIRCUITO[nueva[0]][nueva[1]] == "#":
        return pos, RECOMPENSA_CHOQUE, False  # choca y se queda donde estaba
    if nueva == META:
        return nueva, RECOMPENSA_META, True
    return nueva, RECOMPENSA_PASO, False


def elegir_accion(q, pos, exploracion):
    """Con probabilidad 'exploracion' prueba algo al azar; si no, usa lo aprendido."""
    if random.random() < exploracion:
        return random.randrange(4)
    valores = q[pos]
    mejores = np.flatnonzero(valores == valores.max())
    return int(random.choice(mejores))


def jugar_episodio(q, exploracion, aprender=True):
    pos, camino, total, choques = SALIDA, [SALIDA], 0, 0
    for _ in range(MAX_PASOS):
        accion = elegir_accion(q, pos, exploracion)
        nueva, recompensa, fin = mover(pos, accion)
        if aprender:
            # La regla de Q-learning: acercar el valor de (casilla, movimiento)
            # a "lo que he ganado ahora + lo mejor que espero ganar después".
            futuro = 0 if fin else q[nueva].max()
            q[pos][accion] += ALFA * (recompensa + GAMMA * futuro - q[pos][accion])
        choques += recompensa == RECOMPENSA_CHOQUE
        total += recompensa
        pos = nueva
        camino.append(pos)
        if fin:
            return camino, total, True, choques
    return camino, total, False, choques


def dibujar(camino, titulo):
    """Pinta el circuito marcando con * las casillas por las que ha pasado."""
    mapa = [list(fila) for fila in CIRCUITO]
    for f, c in camino:
        if mapa[f][c] == ".":
            mapa[f][c] = "*"
    print(f"\n{titulo}")
    for fila in mapa:
        print("   " + " ".join(fila))


def dibujar_politica(q):
    """Muestra la flecha que el coche ha aprendido a elegir en cada casilla."""
    print("\nLo que ha aprendido: el mejor movimiento en cada casilla")
    for f in range(FILAS):
        linea = []
        for c in range(COLUMNAS):
            letra = CIRCUITO[f][c]
            linea.append(letra if letra in "#SF" else NOMBRES[int(q[f, c].argmax())])
        print("   " + " ".join(linea))


# --- Entrenamiento ------------------------------------------------------------
q = np.zeros((FILAS, COLUMNAS, 4))  # al principio no sabe nada: todo vale 0
historial_pasos, historial_recompensa = [], []
mostrar = {1, 25, 100}

print(f"Entrenando {EPISODIOS} episodios...\n")
print(f"{'Episodio':>9} {'Pasos':>7} {'Choques':>8} {'¿Llega?':>8}")

for episodio in range(1, EPISODIOS + 1):
    # Exploración: empieza al 100 % (todo al azar) y baja poco a poco
    exploracion = max(EXPLORACION_MINIMA, 1 - episodio / (0.6 * EPISODIOS))
    camino, total, llega, choques = jugar_episodio(q, exploracion)
    historial_pasos.append(len(camino) - 1)
    historial_recompensa.append(total)

    if episodio in mostrar or episodio % 50 == 0:
        print(f"{episodio:>9} {len(camino) - 1:>7} {choques:>8} {'sí' if llega else 'no':>8}")
    if episodio in mostrar:
        dibujar(camino, f"Episodio {episodio}: {len(camino) - 1} pasos, {choques} choques")
        print()

# --- Resultado final: conduce solo con lo aprendido, sin azar --------------------
camino, total, llega, choques = jugar_episodio(q, exploracion=0, aprender=False)
dibujar(camino, f"Tras el entrenamiento: {len(camino) - 1} pasos, {choques} choques")
dibujar_politica(q)

# --- Curva de aprendizaje -------------------------------------------------------
ventana = 20
media = np.convolve(historial_pasos, np.ones(ventana) / ventana, mode="valid")

fig, ax = plt.subplots(figsize=(10, 4.5))
ax.plot(historial_pasos, color="#9FB8B5", linewidth=1, label="Pasos en cada episodio")
ax.plot(range(ventana - 1, EPISODIOS), media, color="#17706A", linewidth=2.5,
        label=f"Media de {ventana} episodios")
ax.axhline(len(camino) - 1, color="#F2A541", linestyle="--", label="Ruta final aprendida")
ax.set_xlabel("Episodio (intento)")
ax.set_ylabel("Pasos hasta la meta (menos es mejor)")
ax.set_title("Prueba y error: el coche tarda cada vez menos en llegar")
ax.legend()
plt.tight_layout()
plt.show()
