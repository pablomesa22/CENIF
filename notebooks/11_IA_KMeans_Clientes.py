"""k-means paso a paso y asignación de clientes nuevos (figuras de la Sesión 2 de IA). Datos inventados."""
import numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans, MiniBatchKMeans
BG="#F6F4EE"; INK="#0F1F2E"
COL=["#17706A","#E08A1E","#4A6FA5"]; MK=["o","s","^"]
plt.rcParams.update({"font.family":"DejaVu Sans","font.size":15,"axes.edgecolor":"#8A949C","axes.labelcolor":INK,"xtick.color":"#5C646B","ytick.color":"#5C646B"})
rng=np.random.default_rng(4)
# clientes: gasto anual (€) y compras al año
g=[(300,4,60,1.5,40),(1200,24,180,4,40),(2600,10,300,3,30)]
X=np.vstack([np.c_[rng.normal(m1,s1,n),rng.normal(m2,s2,n)] for m1,m2,s1,s2,n in g])
X[:,1]=np.clip(X[:,1],1,None); X[:,0]=np.clip(X[:,0],50,None)
mu,sd=X.mean(0),X.std(0); Z=(X-mu)/sd
C=Z[[3,5,8]].copy()            # centros iniciales mal colocados (tres clientes del mismo grupo)
hist=[C.copy()]; labs=[]
for it in range(20):
    d=((Z[:,None,:]-C[None])**2).sum(-1); l=d.argmin(1); labs.append(l)
    Cn=np.array([Z[l==k].mean(0) for k in range(3)])
    hist.append(Cn.copy())
    if np.allclose(Cn,C): break
    C=Cn
print("iteraciones hasta converger:",len(labs))
un=lambda c: c*sd+mu
show=[0,1,2,len(labs)-1]
fig,axs=plt.subplots(1,4,figsize=(20,5.2),facecolor=BG)
for ax,i in zip(axs,show):
    ax.set_facecolor(BG)
    if i==0:
        ax.scatter(*X.T,c="#AEB6BD",s=28,marker="o",edgecolors="white",linewidths=0.5)
        cc=un(hist[0])
    else:
        l=labs[i-1]
        for k in range(3): ax.scatter(*X[l==k].T,c=COL[k],s=30,marker=MK[k],edgecolors="white",linewidths=0.5)
        cc=un(hist[i])
        for j in range(1,i+1):
            a,b=un(hist[j-1]),un(hist[j])
            for k in range(3): ax.plot([a[k,0],b[k,0]],[a[k,1],b[k,1]],color=INK,lw=1.5,ls="--")
    for k in range(3): ax.scatter(*cc[k],marker="X",s=420,c=COL[k] if i else "#5C646B",edgecolors=INK,linewidths=2,zorder=5)
    ax.set_xlabel("Gasto anual (€)"); ax.set_xlim(0,3600); ax.set_ylim(0,35)
    if i==0: ax.set_ylabel("Compras al año")
    for s in ("top","right"): ax.spines[s].set_visible(False)
plt.tight_layout(pad=1.2); plt.savefig("kmeans_pasos.png",dpi=110,facecolor=BG)
# clientes nuevos
km=KMeans(3,n_init=10,random_state=0).fit(Z)
order=np.argsort(un(km.cluster_centers_)[:,0])  # ordenar grupos por gasto
remap={o:i for i,o in enumerate(order)}
L=np.array([remap[v] for v in km.labels_]); cen=un(km.cluster_centers_)[order]
new=np.array([[700,14],[2300,18],[3400,33]])
Zn=(new-mu)/sd; dist=np.sqrt((( Zn[:,None,:]-((cen-mu)/sd)[None])**2).sum(-1)); ln=dist.argmin(1)
dmax=[np.sqrt((((Z[L==k]-((cen[k]-mu)/sd))**2).sum(1))).max() for k in range(3)]
print("nuevos ->",ln,"dist",dist.min(1).round(2),"radio max grupo",np.round(dmax,2))
print("centros",cen.round(0))
fig,ax=plt.subplots(figsize=(10,6),facecolor=BG); ax.set_facecolor(BG)
COLR=[COL[1],COL[2],COL[0]]; MKR=[MK[1],MK[2],MK[0]]
for k in range(3):
    ax.scatter(*X[L==k].T,c=COLR[k],s=28,marker=MKR[k],edgecolors="white",linewidths=0.5,alpha=0.55)
    ax.scatter(*cen[k],marker="X",s=380,c=COLR[k],edgecolors=INK,linewidths=2,zorder=5)
names=["A","B","C"]
for i,(p,k) in enumerate(zip(new,ln)):
    ax.plot([p[0],cen[k][0]],[p[1],cen[k][1]],color=INK,lw=2,ls="--",zorder=4)
    ax.scatter(*p,marker="*",s=650,c="#F2C14E",edgecolors=INK,linewidths=1.8,zorder=6)
    ax.annotate(names[i],p,xytext=(12,8),textcoords="offset points",fontsize=20,fontweight="bold",color=INK)
ax.set_xlabel("Gasto anual (€)"); ax.set_ylabel("Compras al año"); ax.set_xlim(0,3600); ax.set_ylim(0,35)
for s in ("top","right"): ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig("kmeans_nuevos.png",dpi=110,facecolor=BG)
# actualización incremental
mb=MiniBatchKMeans(3,random_state=0,n_init=3).fit(Z); before=un(mb.cluster_centers_).round(0)
lote=np.c_[rng.normal(1500,150,50),rng.normal(28,3,50)]; mb.partial_fit((lote-mu)/sd)
print("partial_fit antes",before,"despues",un(mb.cluster_centers_).round(0))
