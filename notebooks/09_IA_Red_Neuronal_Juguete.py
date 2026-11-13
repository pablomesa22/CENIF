"""Red neuronal de juguete para la Sesión 2 de IA (slides de redes neuronales).

Genera las tres figuras de las slides:
  evolucion.png  una neurona aprendiendo a separar aprobados y suspensos (vueltas 0, 10, 50 y 1000)
  perdida.png    la curva de error del mismo entrenamiento
  circulo.png    una neurona frente a una red con 8 neuronas ocultas
También imprime el cálculo de una vuelta de aprendizaje con Ana (6 h de estudio, 7 h de sueño).

Datos inventados. En Colab: !pip install scikit-learn matplotlib  y  !python 09_IA_Red_Neuronal_Juguete.py
"""
import numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.neural_network import MLPClassifier
from sklearn.linear_model import LogisticRegression
BG="#F6F4EE"; TEAL="#17706A"; AMB="#E08A1E"; INK="#0F1F2E"
plt.rcParams.update({"font.family":"DejaVu Sans","font.size":15,"axes.edgecolor":"#8A949C","axes.labelcolor":INK,"xtick.color":"#5C646B","ytick.color":"#5C646B"})
sig=lambda z:1/(1+np.exp(-z))
# --- datos inventados: 40 alumnos
rng=np.random.default_rng(7)
est=rng.uniform(0,10,40); sue=rng.uniform(4,9,40)
y=((0.9*est+0.6*sue+rng.normal(0,1.2,40))>8.5).astype(int)
X=np.c_[est,sue]
print("aprueban",y.sum(),"de",len(y))
# --- un paso con un alumno
w=np.array([0.2,-0.1]); b=0.0; lr=0.05
x1=np.array([6.0,7.0]); t=1
z=w@x1+b; p=sig(z); print("paso1 z",z,"p",p)
g=p-t; w2=w-lr*g*x1; b2=b-lr*g; p2=sig(w2@x1+b2)
print("error",g,"nuevos",w2,b2,"p2",p2)
# --- entrenamiento completo (descenso de gradiente, log-loss)
w=np.array([-0.6,0.4]); b=0.0; lr=0.1
M=np.array([5.0,6.5]); Xc=X-M
hist={}; loss=[]
for ep in range(1001):
    p=sig(Xc@w+b)
    L=-np.mean(y*np.log(p+1e-9)+(1-y)*np.log(1-p+1e-9)); loss.append(L)
    if ep in (0,10,50,1000): hist[ep]=(w.copy(),b,((p>0.5)==y).mean(),L)
    gw=Xc.T@(p-y)/len(y); gb=np.mean(p-y)
    w-=lr*gw; b-=lr*gb
for k,v in hist.items(): print(k, v[0].round(3), round(v[1],3), "acierto",round(v[2],3),"perdida",round(v[3],3))
gx,gy=np.meshgrid(np.linspace(0,10,300),np.linspace(3.5,9.5,300))
fig,axs=plt.subplots(1,4,figsize=(20,5.2),facecolor=BG)
for ax,(ep,(ww,bb,acc,L)) in zip(axs,hist.items()):
    P=sig(ww[0]*(gx-M[0])+ww[1]*(gy-M[1])+bb)
    ax.set_facecolor(BG)
    ax.contourf(gx,gy,P,levels=[0,0.5,1],colors=["#FBE6C8","#CFEBE8"],alpha=0.9)
    ax.contour(gx,gy,P,levels=[0.5],colors=[INK],linewidths=2.5)
    ax.scatter(est[y==1],sue[y==1],marker="o",s=70,c=TEAL,edgecolors="white",label="Aprueba")
    ax.scatter(est[y==0],sue[y==0],marker="X",s=80,c=AMB,edgecolors="white",label="Suspende")
    ax.set_xlim(0,10); ax.set_ylim(3.5,9.5); ax.set_xlabel("Horas de estudio")
    if ep==0: ax.set_ylabel("Horas de sueño")
    for s in ("top","right"): ax.spines[s].set_visible(False)
plt.tight_layout(pad=1.2); plt.savefig("evolucion.png",dpi=110,facecolor=BG)
# curva de pérdida
fig,ax=plt.subplots(figsize=(7,4.2),facecolor=BG); ax.set_facecolor(BG)
ax.plot(loss,color=TEAL,lw=3); ax.set_xlabel("Vueltas por los datos (épocas)"); ax.set_ylabel("Error (pérdida)")
for s in ("top","right"): ax.spines[s].set_visible(False)
for ep in (0,10,50,1000): ax.plot(ep,loss[ep],"o",color=AMB,ms=9)
plt.tight_layout(); plt.savefig("perdida.png",dpi=110,facecolor=BG)
# --- círculo: una neurona vs red con capa oculta
r=np.sqrt(rng.uniform(0,1,300))*2; a=rng.uniform(0,2*np.pi,300)
C=np.c_[r*np.cos(a),r*np.sin(a)]; yc=(r<1.2).astype(int)
lin=LogisticRegression().fit(C,yc); mlp=MLPClassifier(hidden_layer_sizes=(8,),activation="tanh",max_iter=5000,random_state=0).fit(C,yc)
print("circulo una neurona",lin.score(C,yc),"red 8",mlp.score(C,yc))
gx,gy=np.meshgrid(np.linspace(-2.1,2.1,300),np.linspace(-2.1,2.1,300)); G=np.c_[gx.ravel(),gy.ravel()]
fig,axs=plt.subplots(1,2,figsize=(12,5.6),facecolor=BG)
for ax,m in zip(axs,[lin,mlp]):
    P=m.predict_proba(G)[:,1].reshape(gx.shape); ax.set_facecolor(BG)
    ax.contourf(gx,gy,P,levels=[0,0.5,1],colors=["#FBE6C8","#CFEBE8"],alpha=0.9)
    ax.contour(gx,gy,P,levels=[0.5],colors=[INK],linewidths=2.5)
    ax.scatter(*C[yc==1].T,marker="o",s=40,c=TEAL,edgecolors="white"); ax.scatter(*C[yc==0].T,marker="X",s=45,c=AMB,edgecolors="white")
    ax.set_aspect("equal"); ax.set_xticks([]); ax.set_yticks([])
    for s in ax.spines.values(): s.set_visible(False)
plt.tight_layout(pad=1.0); plt.savefig("circulo.png",dpi=110,facecolor=BG)
