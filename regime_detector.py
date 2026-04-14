"""
RAPTOR Regime Detector
Genera data/regime_report.html con dati embedded
"""
import yfinance as yf
import pandas as pd
import numpy as np
import json, os, warnings
from datetime import datetime
warnings.filterwarnings('ignore')

ER_PERIOD     = 10
ATR_PERIOD    = 14
MA_PERIOD     = 200
FWD_DAYS      = 20
START_DATE    = "2018-01-01"
ER_TREND_TH   = 0.40
ATR_CRISIS_TH = 2.0

TICKERS = [
    ("SWDA","SWDA.MI"),("XUCS","XUCS.L"),("IWMO","IWMO.MI"),("IWDP","IWDP.MI"),
    ("HLTH","HLTH.L"),("VFEM","VFEM.MI"),("IESE","IESE.MI"),("VWCE","VWCE.MI"),
    ("EMDV","EMDV.MI"),("BTEC","BTEC.L"),("ISEU","ISEU.L"),("VECP","VECP.MI"),
    ("LQDE","LQDE.MI"),("IHYG","IHYG.MI"),("JAPN","JAPN.L"),("IBGL","IBGL.MI"),
    ("VGEA","VGEA.MI"),("BNKS","BNKS.L"),("IDTL","IDTL.L"),("SMART","SMART.MI"),
    ("XEON","XEON.MI"),("SE15","SE15.MI"),("VHYL","VHYL.MI"),("ISUS","ISUS.L"),
    ("IBGM","IBGM.MI"),("IEGE","IEGE.MI"),("EXCS","EXCS.MI"),("IEAC","IEAC.MI"),
    ("IGLN","IGLN.L"),("IBCI","IBCI.MI"),
]

def calc_er(close, period):
    direction  = (close - close.shift(period)).abs()
    volatility = close.diff().abs().rolling(period).sum()
    return (direction / volatility.replace(0, np.nan)).clip(0, 1)

def calc_atr_pct(high, low, close, period):
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs()
    ], axis=1).max(axis=1)
    atr = tr.ewm(span=period, adjust=False).mean()
    return (atr / close * 100).clip(0, 20)

def classify_regime(er, atr_pct, above_ma200):
    if er > ER_TREND_TH:
        return "TREND_UP" if above_ma200 else "TREND_DOWN"
    return "CRISIS" if atr_pct > ATR_CRISIS_TH else "MEAN_REVERSION"

def regime_strength(regime, er, atr_pct):
    if regime in ("TREND_UP","TREND_DOWN"): return round(float(er), 3)
    if regime == "MEAN_REVERSION":          return round(1 - float(er), 3)
    return round(min(float(atr_pct)/5, 1.0), 3)

def process_ticker(name, yf_ticker):
    try:
        df = yf.download(yf_ticker, start=START_DATE, progress=False, auto_adjust=True)
        if df.empty or len(df) < MA_PERIOD:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        close = df["Close"].squeeze()
        high  = df["High"].squeeze()
        low   = df["Low"].squeeze()

        er      = calc_er(close, ER_PERIOD)
        atr_pct = calc_atr_pct(high, low, close, ATR_PERIOD)
        ma200   = close.rolling(MA_PERIOD, min_periods=MA_PERIOD//2).mean()

        regimes, strengths = [], []
        for i in range(len(close)):
            if any(pd.isna(v) for v in [er.iloc[i], atr_pct.iloc[i], ma200.iloc[i]]):
                regimes.append("UNDEFINED"); strengths.append(None)
            else:
                r = classify_regime(er.iloc[i], atr_pct.iloc[i], close.iloc[i] > ma200.iloc[i])
                regimes.append(r)
                strengths.append(regime_strength(r, er.iloc[i], atr_pct.iloc[i]))

        return {
            "name":      name,
            "dates":     [d.strftime("%Y-%m-%d") for d in close.index],
            "closes":    [round(v,4) if not pd.isna(v) else None for v in close],
            "er":        [round(v,4) if not pd.isna(v) else None for v in er],
            "atr_pct":   [round(v,4) if not pd.isna(v) else None for v in atr_pct],
            "regimes":   regimes,
            "strengths": strengths,
        }
    except Exception as e:
        print(f"  Errore {name}: {e}")
        return None

def run_backtest(all_data):
    obs = []
    for d in all_data:
        n = len(d["closes"])
        for i in range(n - FWD_DAYS):
            if d["regimes"][i] == "UNDEFINED": continue
            c0, cf = d["closes"][i], d["closes"][i+FWD_DAYS]
            if c0 is None or cf is None: continue
            obs.append({"regime": d["regimes"][i], "strength": d["strengths"][i],
                         "fwd": round((cf/c0-1)*100, 4)})

    groups = {}
    for o in obs:
        groups.setdefault(o["regime"], []).append(o["fwd"])

    stats = {}
    for regime, rets in groups.items():
        a = np.array(rets)
        avg = float(np.mean(a)); med = float(np.median(a))
        win = float(np.mean(a>0)*100); std = float(np.std(a))
        stats[regime] = {"n":len(rets),"avg":round(avg,3),"med":round(med,3),
                         "win":round(win,1),"sharpe":round(avg/std if std>0 else 0,3)}

    tu = sorted([o for o in obs if o["regime"]=="TREND_UP"], key=lambda x: x["strength"] or 0)
    sq = []
    if len(tu) > 30:
        t = len(tu)//3
        for label, grp in [("Bassa",tu[:t]),("Media",tu[t:2*t]),("Alta",tu[2*t:])]:
            a = np.array([o["fwd"] for o in grp])
            sq.append({"label":label,"n":len(a),"avg":round(float(np.mean(a)),3),
                       "win":round(float(np.mean(a>0)*100),1)})
    return stats, sq

def get_today(all_data):
    rows = []
    for d in all_data:
        i = len(d["regimes"])-1
        rows.append({"name":d["name"],"regime":d["regimes"][i],
                     "strength":d["strengths"][i],"er":d["er"][i],"atr_pct":d["atr_pct"][i]})
    order = ["TREND_UP","TREND_DOWN","MEAN_REVERSION","CRISIS","UNDEFINED"]
    rows.sort(key=lambda x: order.index(x["regime"]) if x["regime"] in order else 9)
    return rows

def build_thermo(today):
    c = {"TREND_UP":0,"TREND_DOWN":0,"MEAN_REVERSION":0,"CRISIS":0}
    for t in today:
        if t["regime"] in c: c[t["regime"]] += 1
    total = sum(c.values())
    pcts  = {r: round(c[r]/total*100,1) if total>0 else 0 for r in c}
    macro = max(c, key=c.get)
    return {"counts":c,"pcts":pcts,"macro":macro,"risk_off":pcts.get("CRISIS",0)>25,"total":total}

def build_thermo_history(all_data):
    all_dates = sorted(set(d for etf in all_data for d in etf["dates"]))[-500:]
    history = []
    for date_str in all_dates:
        counts = {"TREND_UP":0,"TREND_DOWN":0,"MEAN_REVERSION":0,"CRISIS":0}
        total = 0
        for etf in all_data:
            if date_str in etf["dates"]:
                idx = etf["dates"].index(date_str)
                r   = etf["regimes"][idx]
                if r in counts:
                    counts[r] += 1; total += 1
        if total > 0:
            history.append({
                "date": date_str,
                "TU": round(counts["TREND_UP"]/total*100,1),
                "TD": round(counts["TREND_DOWN"]/total*100,1),
                "MR": round(counts["MEAN_REVERSION"]/total*100,1),
                "CR": round(counts["CRISIS"]/total*100,1),
            })
    return history

# ── MAIN ──────────────────────────────────────────────────────────────────────
print("="*60)
print("  RAPTOR Regime Detector")
print("="*60)

all_data = []
for name, yf_ticker in TICKERS:
    result = process_ticker(name, yf_ticker)
    if result:
        all_data.append(result)
        print(f"  ✓ {name:6s} — {len(result['dates'])} barre")
    else:
        print(f"  ✗ {name:6s} — skip")

print(f"\n  Caricati: {len(all_data)}/{len(TICKERS)}")

bt_stats, strength_stats = run_backtest(all_data)
today   = get_today(all_data)
thermo  = build_thermo(today)
history = build_thermo_history(all_data)

payload = {
    "generated":      datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC"),
    "config":         {"er_period":ER_PERIOD,"atr_period":ATR_PERIOD,
                       "ma_period":MA_PERIOD,"fwd_days":FWD_DAYS,
                       "er_thresh":ER_TREND_TH,"atr_thresh":ATR_CRISIS_TH},
    "thermo":         thermo,
    "today":          today,
    "bt_stats":       bt_stats,
    "strength_stats": strength_stats,
    "thermo_history": history,
}

payload_json = json.dumps(payload, ensure_ascii=False)

html = f"""<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>RAPTOR Regime Detector</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
  :root{{--bg:#0a0c0f;--bg2:#111318;--bg3:#181c23;--border:#1f2430;--text:#c8d0dc;--muted:#4a5568;--accent:#00d4ff;--up:#00e676;--down:#ff1744;--mean:#ffab00;--crisis:#d500f9;--font-mono:'IBM Plex Mono',monospace;--font-sans:'IBM Plex Sans',sans-serif;}}
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:var(--bg);color:var(--text);font-family:var(--font-sans);font-size:13px;line-height:1.5}}
  header{{display:flex;align-items:center;justify-content:space-between;padding:16px 24px;border-bottom:1px solid var(--border);background:var(--bg2);position:sticky;top:0;z-index:100}}
  .logo{{font-family:var(--font-mono);font-size:15px;font-weight:600;color:var(--accent);letter-spacing:.1em}}
  .logo span{{color:var(--text);font-weight:300}}
  .hmeta{{font-family:var(--font-mono);font-size:11px;color:var(--muted)}}
  .main{{padding:20px 24px;display:flex;flex-direction:column;gap:20px}}
  .card{{background:var(--bg2);border:1px solid var(--border);border-radius:4px;padding:16px 20px}}
  .card-title{{font-family:var(--font-mono);font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:14px}}
  .grid-2{{display:grid;grid-template-columns:1fr 1fr;gap:20px}}
  @media(max-width:900px){{.grid-2{{grid-template-columns:1fr}}}}
  .thermo-segments{{display:flex;height:28px;border-radius:3px;overflow:hidden;gap:2px}}
  .thermo-seg{{display:flex;align-items:center;justify-content:center;font-family:var(--font-mono);font-size:10px;font-weight:600;color:#000;overflow:hidden;white-space:nowrap}}
  .thermo-labels{{display:flex;gap:20px;margin-top:10px;flex-wrap:wrap}}
  .thermo-lbl{{display:flex;align-items:center;gap:6px;font-family:var(--font-mono);font-size:11px}}
  .thermo-dot{{width:8px;height:8px;border-radius:50%}}
  .macro-status{{display:flex;gap:16px;margin-top:12px;padding-top:12px;border-top:1px solid var(--border);flex-wrap:wrap}}
  .kpi{{display:flex;flex-direction:column;gap:2px}}
  .kpi-l{{font-family:var(--font-mono);font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em}}
  .kpi-v{{font-family:var(--font-mono);font-size:14px;font-weight:600}}
  table{{width:100%;border-collapse:collapse;font-family:var(--font-mono);font-size:11px}}
  thead th{{text-align:left;padding:5px 8px;color:var(--muted);font-weight:400;border-bottom:1px solid var(--border);white-space:nowrap}}
  tbody tr:hover{{background:var(--bg3)}}
  tbody td{{padding:5px 8px;border-bottom:1px solid #151921;white-space:nowrap}}
  .badge{{display:inline-block;padding:2px 7px;border-radius:2px;font-size:10px;font-weight:600;letter-spacing:.05em}}
  .bTU{{background:#00e67622;color:var(--up);border:1px solid #00e67644}}
  .bTD{{background:#ff174422;color:var(--down);border:1px solid #ff174444}}
  .bMR{{background:#ffab0022;color:var(--mean);border:1px solid #ffab0044}}
  .bCR{{background:#d500f922;color:var(--crisis);border:1px solid #d500f944}}
  .bUN{{background:#4a556822;color:var(--muted);border:1px solid #4a556844}}
  .sbar{{display:flex;align-items:center;gap:6px}}
  .strack{{width:60px;height:4px;background:var(--border);border-radius:2px;overflow:hidden}}
  .sfill{{height:100%;border-radius:2px}}
  .pos{{color:var(--up)}}.neg{{color:var(--down)}}
  canvas{{max-height:240px}}
  ::-webkit-scrollbar{{width:5px;height:5px}}
  ::-webkit-scrollbar-track{{background:var(--bg)}}
  ::-webkit-scrollbar-thumb{{background:var(--border);border-radius:3px}}
</style>
</head>
<body>
<header>
  <div class="logo">RAPTOR <span>/ Regime Detector</span></div>
  <div class="hmeta" id="hmeta"></div>
</header>
<div class="main">
  <div class="card" id="thermo-card"></div>
  <div class="grid-2">
    <div class="card">
      <div class="card-title">Stato Attuale — ogni ETF</div>
      <div style="overflow-x:auto"><table>
        <thead><tr><th>ETF</th><th>Regime</th><th>Strength</th><th>ER</th><th>ATR%</th></tr></thead>
        <tbody id="today-tbody"></tbody>
      </table></div>
    </div>
    <div class="card">
      <div class="card-title" id="bt-title"></div>
      <div style="overflow-x:auto"><table>
        <thead><tr><th>Regime</th><th style="text-align:right">N</th><th style="text-align:right">Avg%</th><th style="text-align:right">Med%</th><th style="text-align:right">Win%</th><th style="text-align:right">Sharpe</th></tr></thead>
        <tbody id="bt-tbody"></tbody>
      </table></div>
      <div style="margin-top:18px">
        <div class="card-title">TREND UP — per forza segnale</div>
        <table>
          <thead><tr><th>Strength</th><th style="text-align:right">N</th><th style="text-align:right">Avg%</th><th style="text-align:right">Win%</th></tr></thead>
          <tbody id="str-tbody"></tbody>
        </table>
      </div>
    </div>
  </div>
  <div class="grid-2">
    <div class="card"><div class="card-title">Rendimento Medio FWD per Regime</div><canvas id="cBar"></canvas></div>
    <div class="card"><div class="card-title">Termometro Storico</div><canvas id="cArea"></canvas></div>
  </div>
</div>
<script>
const D = {payload_json};
const RC = {{TREND_UP:"#00e676",TREND_DOWN:"#ff1744",MEAN_REVERSION:"#ffab00",CRISIS:"#d500f9"}};
const BC = {{TREND_UP:"bTU",TREND_DOWN:"bTD",MEAN_REVERSION:"bMR",CRISIS:"bCR",UNDEFINED:"bUN"}};
const RN = {{TREND_UP:"Trend Up",TREND_DOWN:"Trend Down",MEAN_REVERSION:"Mean Rev",CRISIS:"Crisis"}};

document.getElementById('hmeta').textContent = 'Aggiornato: ' + D.generated + ' | ER=' + D.config.er_period + ' ATR=' + D.config.atr_period + ' MA' + D.config.ma_period + ' FWD' + D.config.fwd_days + 'gg';
document.getElementById('bt-title').textContent = 'Backtest FWD ' + D.config.fwd_days + 'gg — per Regime';

// TERMOMETRO
(function(){{
  const t = D.thermo;
  const segs = ["TREND_UP","TREND_DOWN","MEAN_REVERSION","CRISIS"].map(r=>
    '<div class="thermo-seg" style="flex:'+t.pcts[r]+';background:'+RC[r]+'">'+(t.pcts[r]>8?t.pcts[r]+'%':'')+'</div>'
  ).join('');
  const lbls = ["TREND_UP","TREND_DOWN","MEAN_REVERSION","CRISIS"].map(r=>
    '<div class="thermo-lbl"><div class="thermo-dot" style="background:'+RC[r]+'"></div>'+(RN[r]||r)+': <strong>'+t.counts[r]+'</strong>&nbsp;('+t.pcts[r]+'%)</div>'
  ).join('');
  const roff = t.risk_off;
  document.getElementById('thermo-card').innerHTML =
    '<div class="card-title">Termometro Macro — Oggi</div>'+
    '<div class="thermo-segments">'+segs+'</div>'+
    '<div class="thermo-labels">'+lbls+'</div>'+
    '<div class="macro-status">'+
      '<div class="kpi"><div class="kpi-l">Macro Regime</div><div class="kpi-v" style="color:'+RC[t.macro]+'">'+t.macro.replace('_',' ')+'</div></div>'+
      '<div class="kpi"><div class="kpi-l">ETF analizzati</div><div class="kpi-v" style="color:var(--accent)">'+t.total+'</div></div>'+
      '<div class="kpi"><div class="kpi-l">Risk Signal</div><div class="kpi-v" style="color:'+(roff?'var(--crisis)':'var(--up)')+'>'+(roff?'⚠ RISK OFF':'✓ RISK ON')+'</div></div>'+
    '</div>';
}})();

// TODAY
document.getElementById('today-tbody').innerHTML = D.today.map(r=>{{
  const str=r.strength!=null?r.strength.toFixed(2):'-';
  const er=r.er!=null?r.er.toFixed(3):'-';
  const atr=r.atr_pct!=null?r.atr_pct.toFixed(2):'-';
  const col=RC[r.regime]||'#4a5568';
  const fw=r.strength!=null?Math.round(r.strength*100):0;
  return '<tr><td style="font-weight:600;color:white">'+r.name+'</td>'+
    '<td><span class="badge '+(BC[r.regime]||'bUN')+'">'+(RN[r.regime]||r.regime)+'</span></td>'+
    '<td><div class="sbar"><div class="strack"><div class="sfill" style="width:'+fw+'%;background:'+col+'"></div></div><span>'+str+'</span></div></td>'+
    '<td style="color:var(--muted)">'+er+'</td>'+
    '<td style="color:var(--muted)">'+atr+'</td></tr>';
}}).join('');

// BACKTEST
const order = ["TREND_UP","TREND_DOWN","MEAN_REVERSION","CRISIS"];
document.getElementById('bt-tbody').innerHTML = order.filter(r=>D.bt_stats[r]).map(r=>{{
  const s=D.bt_stats[r];
  return '<tr><td><span class="badge '+BC[r]+'">'+(RN[r]||r)+'</span></td>'+
    '<td style="text-align:right">'+s.n.toLocaleString()+'</td>'+
    '<td style="text-align:right" class="'+(s.avg>=0?'pos':'neg')+'">'+(s.avg>=0?'+':'')+s.avg.toFixed(2)+'%</td>'+
    '<td style="text-align:right" class="'+(s.med>=0?'pos':'neg')+'">'+(s.med>=0?'+':'')+s.med.toFixed(2)+'%</td>'+
    '<td style="text-align:right">'+s.win.toFixed(1)+'%</td>'+
    '<td style="text-align:right;color:var(--muted)">'+s.sharpe.toFixed(3)+'</td></tr>';
}}).join('');

document.getElementById('str-tbody').innerHTML = D.strength_stats.map(s=>
  '<tr><td>'+s.label+'</td><td style="text-align:right">'+s.n+'</td>'+
  '<td style="text-align:right" class="'+(s.avg>=0?'pos':'neg')+'">'+(s.avg>=0?'+':'')+s.avg.toFixed(2)+'%</td>'+
  '<td style="text-align:right">'+s.win.toFixed(1)+'%</td></tr>'
).join('');

// CHARTS
const gridC='#1f2430',tickC='#4a5568',monoF={{family:"'IBM Plex Mono'",size:10}};
const labels=order.filter(r=>D.bt_stats[r]).map(r=>RN[r]||r);
const avgs=order.filter(r=>D.bt_stats[r]).map(r=>D.bt_stats[r].avg);
const colors=order.filter(r=>D.bt_stats[r]).map(r=>RC[r]+'99');
const borders=order.filter(r=>D.bt_stats[r]).map(r=>RC[r]);

new Chart(document.getElementById('cBar'),{{
  type:'bar',
  data:{{labels,datasets:[{{data:avgs,backgroundColor:colors,borderColor:borders,borderWidth:1,borderRadius:3}}]}},
  options:{{responsive:true,plugins:{{legend:{{display:false}}}},scales:{{
    x:{{ticks:{{color:tickC,font:monoF}},grid:{{color:gridC}}}},
    y:{{ticks:{{color:tickC,font:monoF}},grid:{{color:gridC}},title:{{display:true,text:'Avg FWD %',color:tickC}}}}
  }}}}
}});

if(D.thermo_history.length>0){{
  new Chart(document.getElementById('cArea'),{{
    type:'line',
    data:{{
      labels:D.thermo_history.map(h=>h.date),
      datasets:[
        {{label:'Trend Up',  data:D.thermo_history.map(h=>h.TU),fill:true,backgroundColor:'#00e67633',borderColor:'#00e676',borderWidth:1,pointRadius:0,tension:0.3}},
        {{label:'Trend Down',data:D.thermo_history.map(h=>h.TD),fill:true,backgroundColor:'#ff174433',borderColor:'#ff1744',borderWidth:1,pointRadius:0,tension:0.3}},
        {{label:'Mean Rev',  data:D.thermo_history.map(h=>h.MR),fill:true,backgroundColor:'#ffab0033',borderColor:'#ffab00',borderWidth:1,pointRadius:0,tension:0.3}},
        {{label:'Crisis',    data:D.thermo_history.map(h=>h.CR),fill:true,backgroundColor:'#d500f933',borderColor:'#d500f9',borderWidth:1,pointRadius:0,tension:0.3}},
      ]
    }},
    options:{{
      responsive:true,
      interaction:{{mode:'index',intersect:false}},
      plugins:{{legend:{{labels:{{color:'#c8d0dc',font:monoF}}}}}},
      scales:{{
        x:{{ticks:{{color:tickC,font:monoF,maxTicksLimit:10}},grid:{{color:gridC}}}},
        y:{{ticks:{{color:tickC,font:monoF}},grid:{{color:gridC}},min:0}}
      }}
    }}
  }});
}}
</script>
</body>
</html>"""

os.makedirs("data", exist_ok=True)
out = "data/regime_report.html"
with open(out, "w", encoding="utf-8") as f:
    f.write(html)

print(f"\n✅ Report salvato: {out}")
