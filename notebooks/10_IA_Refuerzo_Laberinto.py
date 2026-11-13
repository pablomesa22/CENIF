"""Q-learning en un circuito de cuadrícula: figuras de la Sesión 2 de IA. Entorno inventado."""
import numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
BG="#F6F4EE"; INK="#0F1F2E"; TEAL="#17706A"; AMB="#E08A1E"
plt.rcParams.update({"font.family":"DejaVu Sans","font.size":15,"axes.labelcolor":INK,"xtick.color":"#5C646B","ytick.color":"#5C646B","axes.edgecolor":"#8A949C"})
MAPA=["##########",
      "#.......M#",
      "#.####.###",
      "#.#....#.#",
      "#.#.##.#.#",
      "#...#....#",
      "###.#.##.#",
      "#S..#....#",
      "##########"]
G=np.array([list(r) for r in MAPA]); H,W=G.shape
start=tuple(np.argwhere(G=="S")[0]); goal=tuple(np.argwhere(G=="M")[0])
ACC=[(-1,0),(1,0),(0,-1),(0,1)]  # arriba abajo izquierda derecha
ALFA,GAMMA=0.5,0.95; RC,RP,RM=-10,-1,100
rng=np.random.default_rng(1)
Q=np.zeros((H,W,4))
def paso(s,a):
    n=(s[0]+ACC[a][0],s[1]+ACC[a][1])
    if G[n]=="#": return s,RC,True
    if n==goal: return n,RM,True
    return n,RP,False
N=600; rec=[]; caminos={}; eps_hist=[]
for ep in range(1,N+1):
    eps=max(0.05,1.0*0.99**ep); eps_hist.append(eps)
    s=start; tot=0; cam=[s]
    for t in range(200):
        a=rng.integers(4) if rng.random()<eps else int(np.argmax(Q[s]))
        n,r,fin=paso(s,a); fut=0 if fin else Q[n].max()
        Q[s][a]+=ALFA*(r+GAMMA*fut-Q[s][a]); tot+=r; s=n; cam.append(s)
        if fin: break
    rec.append(tot)
    caminos[ep]=(cam,r==RM,tot)
# camino final sin explorar
s=start; cam=[s]; tot=0
for t in range(100):
    a=int(np.argmax(Q[s])); n,r,fin=paso(s,a); tot+=r; s=n; cam.append(s)
    if fin: break
caminos["final"]=(cam,r==RM,tot)

PRIMERO=next(i+1 for i,v in enumerate(rec) if v>0)
LARGO=max(range(80,PRIMERO),key=lambda e:len(caminos[e][0]))
CORTO=1
print("primer exito",PRIMERO,"pasos",len(caminos[PRIMERO][0])-1,"episodio largo fallido",LARGO,len(caminos[LARGO][0])-1, "ep1 pasos",len(caminos[1][0])-1)
SEL=[CORTO,LARGO,PRIMERO,"final"]
cm=ListedColormap(["#FFFDF8","#2B3A47"])
def dibuja(ax,cam,meta):
    ax.imshow(G=="#",cmap=cm,interpolation="nearest")
    ys,xs=zip(*cam)
    ax.plot(xs,ys,color=AMB,lw=3,alpha=0.85,marker="o",ms=4)
    if not meta:
        ax.scatter([xs[-1]],[ys[-1]],marker="X",s=420,c="#D64545",edgecolors="white",linewidths=2,zorder=5)
    ax.text(start[1],start[0],"S",ha="center",va="center",fontsize=20,fontweight="bold",color=TEAL)
    ax.text(goal[1],goal[0],"M",ha="center",va="center",fontsize=20,fontweight="bold",color=TEAL)
    ax.set_xticks([]); ax.set_yticks([])
    for sp in ax.spines.values(): sp.set_visible(False)
fig,axs=plt.subplots(1,4,figsize=(20,4.6),facecolor=BG)
for ax,k in zip(axs,SEL): dibuja(ax,caminos[k][0],caminos[k][1])
print("recompensas",[caminos[k][2] for k in SEL]); print("eps",[round(eps_hist[k-1],2) for k in SEL[:3]])
plt.tight_layout(pad=1.0); plt.savefig("rl_episodios.png",dpi=110,facecolor=BG)
# curva de recompensa
fig,ax=plt.subplots(figsize=(8,4.6),facecolor=BG); ax.set_facecolor(BG)
rr=np.array(rec); m=np.convolve(rr,np.ones(20)/20,mode="valid")
ax.plot(rr,color="#AEB6BD",lw=1,label="Cada episodio")
ax.plot(np.arange(19,N),m,color=TEAL,lw=3,label="Media de 20")
ax.set_xlabel("Episodio"); ax.set_ylabel("Recompensa total"); ax.legend(frameon=False,loc="upper left")
for s_ in ("top","right"): ax.spines[s_].set_visible(False)
plt.tight_layout(); plt.savefig("rl_curva.png",dpi=110,facecolor=BG)
# mapa de lo aprendido: mejor acción y valor
fig,ax=plt.subplots(figsize=(8,7.2),facecolor=BG)
V=np.where(G=="#",np.nan,Q.max(2))
ax.imshow(G=="#",cmap=cm,interpolation="nearest")
im=ax.imshow(np.ma.masked_invalid(V),cmap="YlGn",interpolation="nearest",alpha=0.9)
for y in range(H):
    for x in range(W):
        if G[y,x] in ".S" and Q[y,x].max()>0:
            a=int(np.argmax(Q[y,x])); dy,dx=ACC[a]
            ax.arrow(x-dx*0.25,y-dy*0.25,dx*0.3,dy*0.3,head_width=0.22,head_length=0.18,color=INK,length_includes_head=True)
ax.text(goal[1],goal[0],"M",ha="center",va="center",fontsize=20,fontweight="bold",color=INK)
ax.set_xticks([]); ax.set_yticks([])
for sp in ax.spines.values(): sp.set_visible(False)
cb=plt.colorbar(im,ax=ax,fraction=0.035,pad=0.02); cb.set_label("Valor aprendido (mejor Q)")
plt.tight_layout(); plt.savefig("rl_mapa.png",dpi=110,facecolor=BG)
