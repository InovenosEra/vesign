import pandas as pd, numpy as np
px=pd.read_parquet('research_universe/universe_prices.parquet')
ms=pd.read_csv('research_universe/universe_master.csv')
px['date']=pd.to_datetime(px['date']); px=px.sort_values(['symbol','date']).reset_index(drop=True)
px['p']=px['adjclose'].where(px['adjclose'].notna()&(px['adjclose']>0), px['close'])
st=dict(zip(ms.symbol, ms.status))
def rsi14(c):
    d=c.diff(); up=d.clip(lower=0); dn=-d.clip(upper=0)
    return 100-100/(1+up.ewm(alpha=1/14,adjust=False).mean()/dn.ewm(alpha=1/14,adjust=False).mean().replace(0,np.nan))
g=px.groupby('symbol',sort=False)
px['rsi']=g['p'].transform(rsi14)
m=g['p'].transform(lambda c:c.rolling(20).mean()); s=g['p'].transform(lambda c:c.rolling(20).std())
px['bb']=(px.p-(m-2*s))/(4*s)
px['vr']=px.volume/g['volume'].transform(lambda v:v.rolling(20).mean())
hi=g['p'].transform(lambda c:c.rolling(252,min_periods=60).max()); px['fromhi']=px.p/hi-1
px['dv20']=g.apply(lambda d:(d.p*d.volume).rolling(20).mean()).reset_index(level=0,drop=True)
pc=g['close'].shift(1)
tr=np.maximum(px.high-px.low,np.maximum((px.high-pc).abs(),(px.low-pc).abs()))
px['atrp']=tr.groupby(px.symbol).transform(lambda x:x.ewm(alpha=1/14,adjust=False).mean())/px.close
px['yr']=px.date.dt.year
A={sym:(d.date.values,d.p.values,d.rsi.values,d.atrp.values) for sym,d in px.groupby('symbol',sort=False)}

def sim(sym,i0,stop=0.25,katr=None):
    dates,p,rsi,atrp=A[sym]; e=p[i0]
    if not np.isfinite(e) or e<=0: return None
    sd = (katr*atrp[i0]) if katr is not None else stop
    if katr is not None:
        if not np.isfinite(sd): sd=0.25
        sd=min(0.6,max(0.10,sd))
    end=dates[i0]+np.timedelta64(365,'D')
    fc=p[i0+1:]; fr=rsi[i0+1:]; fd=dates[i0+1:]
    if len(fc)==0: return None
    hs=(fc<e*(1-sd)) if sd is not None else np.zeros(len(fc),bool)
    hit=hs|(np.nan_to_num(fr)>=70)|(fd>=end)
    j=np.argmax(hit) if hit.any() else len(fc)-1
    return fc[j]/e-1

# entry proxy + optional liquidity filter
px['entry']=(px.rsi<35)&(px.bb<=0.10)&(px.vr>=1.2)&(px.fromhi<=-0.10)
def entries(liq):
    e=px[px.entry].copy()
    if liq: e=e[(e.p>=5)&(e.dv20>=5e6)]
    return e
def runset(e, stop=0.25, katr=None):
    e=e.sort_values(['symbol','date']); pos=g.cumcount();
    idx={s:dict(zip(d.date.values,range(len(d)))) for s,d in px.groupby('symbol',sort=False)}
    o={}; rets=[]
    for r in e.itertuples():
        i0=idx[r.symbol][np.datetime64(r.date)]
        if r.symbol in o and i0<=o[r.symbol]: continue
        out=sim(r.symbol,i0,stop=stop,katr=katr)
        if out is None: continue
        # advance open-until by holding (recompute exit index)
        rets.append(out); o[r.symbol]=i0+1
    r=pd.Series(rets)
    return dict(n=len(r),win=round((r>0).mean()*100,1),avg=round(r.mean()*100,1),
        med=round(r.median()*100,1),geo=round((np.exp(np.log1p(r.clip(-0.99)).mean())-1)*100,1),
        worst5=round(r.quantile(0.05)*100,1),maxloss=round(r.min()*100,1),cat=round((r<-0.5).mean()*100,1))

print("=== TABLE 1: SURVIVORSHIP DRAG (entry proxy, stop=25%) ===")
for liq,lbl in [(False,'raw (all common)'),(True,'liquidity-filtered px>=$5 & $5M/day')]:
    e=entries(liq)
    allset=runset(e)
    act=runset(e[e.symbol.map(st)=='active'])
    print(f"\n[{lbl}]")
    print(f"  ACTIVE-only (survivors): n={act['n']} win={act['win']}% avg={act['avg']}%")
    print(f"  ALL (incl delisted):     n={allset['n']} win={allset['win']}% avg={allset['avg']}%")
    print(f"  >>> survivorship drag on win rate: {round(act['win']-allset['win'],1)} pp")

print("\n=== TABLE 2: STOP SWEEP on ALL universe (liquidity-filtered, disaster-inclusive) ===")
e=entries(True)
print(f"{'stop':>7}{'n':>7}{'win%':>7}{'avg%':>7}{'geo%':>7}{'worst5%':>9}{'maxloss%':>9}{'<-50%':>7}")
for stp in [0.15,0.20,0.25,0.30,0.35,0.40,None]:
    r=runset(e,stop=stp); lbl='NONE' if stp is None else f"{int(stp*100)}%"
    print(f"{lbl:>7}{r['n']:>7}{r['win']:>7}{r['avg']:>7}{r['geo']:>7}{r['worst5']:>9}{r['maxloss']:>9}{r['cat']:>7}")

print("\n=== TABLE 3: same sweep on ACTIVE-only (to show how survivorship flipped it) ===")
ea=entries(True); ea=ea[ea.symbol.map(st)=='active']
for stp in [0.25,0.40,None]:
    r=runset(ea,stop=stp); lbl='NONE' if stp is None else f"{int(stp*100)}%"
    print(f"{lbl:>7}{r['n']:>7}{r['win']:>7}{r['avg']:>7}{r['geo']:>7}{r['worst5']:>9}{r['maxloss']:>9}{r['cat']:>7}")

print("\n=== TABLE 4: ATR-adaptive stop on ALL universe (liquidity-filtered) ===")
for k in [3,5,7]:
    r=runset(e,katr=k); print(f"  k={k}: n={r['n']} win={r['win']}% avg={r['avg']}% geo={r['geo']}% maxloss={r['maxloss']}%")
