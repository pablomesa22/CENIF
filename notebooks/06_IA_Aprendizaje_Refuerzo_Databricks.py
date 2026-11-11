# Databricks notebook source
# MAGIC %md
# MAGIC # Aprendizaje por refuerzo: un coche aprende un circuito por prueba y error
# MAGIC
# MAGIC La misma idea que las IA de Trackmania, en pequeño:
# MAGIC
# MAGIC - **Sin dataset:** nadie le enseña el camino ni le da partidas de ejemplo.
# MAGIC - **Empieza al azar** y recibe premios y castigos: **−1** por cada paso, **−5** si choca contra un muro y **+100** al llegar a la meta.
# MAGIC - **Repite** el circuito cientos de veces y va apuntando en una tabla qué movimiento le fue mejor en cada casilla (la *tabla Q*).
# MAGIC
# MAGIC Ejecuta las celdas en orden (o **Run all**). Después, cambia los parámetros de la celda 1 y vuelve a ejecutar.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros (prueba a cambiarlos)

# COMMAND ----------

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

EPISODIOS = 500           # cuántas vueltas de entrenamiento (prueba con 20)
MAX_PASOS = 300           # si no llega en estos pasos, se corta el intento
ALFA = 0.5                # velocidad de aprendizaje: cuánto pesa cada experiencia nueva
GAMMA = 0.95              # cuánto le importan las recompensas futuras
EXPLORACION_MINIMA = 0.05 # probabilidad mínima de probar un movimiento al azar
RECOMPENSA_PASO = -1
RECOMPENSA_CHOQUE = -5    # prueba con 0: aprende a chocar sin parar
RECOMPENSA_META = 100
SEMILLA = 42              # mismo resultado en cada ejecución; cámbiala para otro entrenamiento
EPISODIOS_A_MOSTRAR = [1, 25, 100]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. El entorno y el agente

# COMMAND ----------

import random

import matplotlib.pyplot as plt
import numpy as np

random.seed(SEMILLA)
np.random.seed(SEMILLA)

ACCIONES = [(-1, 0), (1, 0), (0, -1), (0, 1)]  # arriba, abajo, izquierda, derecha
FLECHAS = ["↑", "↓", "←", "→"]
FILAS, COLUMNAS = len(CIRCUITO), len(CIRCUITO[0])


def buscar(letra):
    for f, fila in enumerate(CIRCUITO):
        if letra in fila:
            return f, fila.index(letra)


SALIDA, META = buscar("S"), buscar("F")


def mover(pos, accion):
    """El entorno: aplica un movimiento y devuelve (nueva posición, recompensa, ¿ha terminado?)."""
    df, dc = ACCIONES[accion]
    nueva = (pos[0] + df, pos[1] + dc)
    if CIRCUITO[nueva[0]][nueva[1]] == "#":
        return pos, RECOMPENSA_CHOQUE, False  # choca y se queda donde estaba
    if nueva == META:
        return nueva, RECOMPENSA_META, True
    return nueva, RECOMPENSA_PASO, False


def elegir_accion(q, pos, exploracion):
    """El agente: con probabilidad 'exploracion' prueba algo al azar; si no, usa lo aprendido."""
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
            # Regla de Q-learning: acercar el valor de (casilla, movimiento)
            # a "lo que he ganado ahora + lo mejor que espero ganar después".
            futuro = 0 if fin else q[nueva].max()
            q[pos][accion] += ALFA * (recompensa + GAMMA * futuro - q[pos][accion])
        choques += int(recompensa == RECOMPENSA_CHOQUE)
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
    print(titulo)
    for fila in mapa:
        print("   " + " ".join(fila))
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Entrenamiento: cientos de intentos de prueba y error

# COMMAND ----------

q = np.zeros((FILAS, COLUMNAS, 4))  # al principio no sabe nada: todo vale 0
historial = []
caminos_guardados = {}

for episodio in range(1, EPISODIOS + 1):
    # Exploración: empieza al 100 % (todo al azar) y baja poco a poco
    exploracion = max(EXPLORACION_MINIMA, 1 - episodio / (0.6 * EPISODIOS))
    camino, total, llega, choques = jugar_episodio(q, exploracion)
    historial.append({
        "episodio": episodio,
        "pasos": len(camino) - 1,
        "choques": choques,
        "recompensa": total,
        "llega": llega,
        "exploracion": round(exploracion, 2),
    })
    if episodio in EPISODIOS_A_MOSTRAR:
        caminos_guardados[episodio] = (camino, choques)

print(f"Entrenamiento terminado: {EPISODIOS} episodios")
for h in historial:
    if h["episodio"] in EPISODIOS_A_MOSTRAR or h["episodio"] % 50 == 0:
        print(f"Episodio {h['episodio']:>4}: {h['pasos']:>3} pasos, {h['choques']:>3} choques, "
              f"{'llega' if h['llega'] else 'NO llega'} (azar {h['exploracion']:.0%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cómo conducía en distintos momentos del entrenamiento

# COMMAND ----------

for episodio, (camino, choques) in caminos_guardados.items():
    dibujar(camino, f"Episodio {episodio}: {len(camino) - 1} pasos, {choques} choques")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resultado final: conduce solo con lo aprendido, sin azar

# COMMAND ----------

camino, total, llega, choques = jugar_episodio(q, exploracion=0, aprender=False)
dibujar(camino, f"Tras el entrenamiento: {len(camino) - 1} pasos, {choques} choques")

print("Lo que ha aprendido: el mejor movimiento en cada casilla")
for f in range(FILAS):
    linea = []
    for c in range(COLUMNAS):
        letra = CIRCUITO[f][c]
        linea.append(letra if letra in "#SF" else FLECHAS[int(q[f, c].argmax())])
    print("   " + " ".join(linea))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Curva de aprendizaje

# COMMAND ----------

pasos = [h["pasos"] for h in historial]
ventana = min(20, EPISODIOS)
media = np.convolve(pasos, np.ones(ventana) / ventana, mode="valid")

fig, ax = plt.subplots(figsize=(10, 4.5))
ax.plot(pasos, color="#9FB8B5", linewidth=1, label="Pasos en cada episodio")
ax.plot(range(ventana - 1, EPISODIOS), media, color="#17706A", linewidth=2.5,
        label=f"Media de {ventana} episodios")
ax.axhline(len(camino) - 1, color="#F2A541", linestyle="--", label="Ruta final aprendida")
ax.set_xlabel("Episodio (intento)")
ax.set_ylabel("Pasos hasta la meta (menos es mejor)")
ax.set_title("Prueba y error: el coche tarda cada vez menos en llegar")
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Extra: el historial como tabla de Spark
# MAGIC Cada intento es una fila: se puede filtrar, agrupar o graficar con las herramientas de Databricks (botón **+** sobre la tabla → *Visualization*).

# COMMAND ----------

df_historial = spark.createDataFrame(historial)
display(df_historial)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preguntas para el grupo
# MAGIC - ¿Cuántos intentos ha necesitado hasta llegar a la meta por primera vez?
# MAGIC - ¿Qué pasa con `EPISODIOS = 20`? ¿Y con `RECOMPENSA_CHOQUE = 0`?
# MAGIC - ¿Se parece esto a cómo aprende una persona a conducir? ¿En qué no se parece?
