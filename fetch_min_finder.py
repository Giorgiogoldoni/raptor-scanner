#!/usr/bin/env python3
"""
RAPTOR MIN FINDER — fetch_min_finder.py
════════════════════════════════════════
Script notturno (02:00 IT) — scansiona tutti i 2.253 ETF dell'universo
e identifica 4 categorie di opportunità sui minimi.

Output: data/min_finder.json
"""

import json, os, time, sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

def install(pkg):
    os.system(f"pip install {pkg} --break-system-packages -q")

try:
    import yfinance as yf
except ImportError:
    install("yfinance"); import yfinance as yf

try:
    import numpy as np
except ImportError:
    install("numpy"); import numpy as np

# ── CONFIGURAZIONE ────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
DATA_DIR   = BASE_DIR / "data"
OUT_FILE   = DATA_DIR / "min_finder.json"
ETF_FILE   = BASE_DIR / "data" / "etf_universe.json"

# Soglie
SOGLIA_MIN_STORICO   = 0.05   # entro 5% dal 52w low
SOGLIA_PULLBACK_MIN  = 0.05   # calo minimo 5% da massimo 4w
SOGLIA_PULLBACK_MAX  = 0.15   # calo massimo 15% da massimo 4w
SOGLIA_ATR_COMPRES   = 0.30   # ATR sceso >30% vs media 3 mesi
SOGLIA_MIN_RELATIVO  = 0.08   # sottoperformance >8% vs media settore 3 mesi

BATCH_SIZE  = 50    # ticker per batch yfinance
BATCH_DELAY = 1.5   # secondi tra batch

# ── UNIVERSO ETF ──────────────────────────────────────────────────
ETF_UNIVERSE = [
    {"t":"18MN","n":"Amundi MSCI Switzerland","b":"USA/Altro","c":"Lazy"},
    {"t":"18MN.DE","n":"Amundi MSCI Switzerland DE","b":"Xetra","c":"Lazy"},
]  # verrà sovrascritto dal file JSON

def load_universe():
    """Carica universo ETF da file JSON (generato da RAPTOR_ETF_Universe)."""
    if ETF_FILE.exists():
        with open(ETF_FILE) as f:
            data = json.load(f)
        universe = []
        for d in data:
            t = str(d.get('TICKER','')).strip()
            if not t or len(t) < 2 or t == 'nan': continue
            universe.append({
                "t": t,
                "n": str(d.get('NOME','')).strip().replace('nan','') or t,
                "b": str(d.get('BORSA','')).strip().replace('nan','') or '',
                "c": str(d.get('CATEGORIA','')).strip().replace('nan','') or 'Altro',
            })
        print(f"  Universo caricato: {len(universe)} ETF da {ETF_FILE}")
        return universe
    else:
        print(f"  ⚠ File universo non trovato: {ETF_FILE} — uso lista embedded")
        return ETF_UNIVERSE

# ── FETCH PREZZI STORICI ──────────────────────────────────────────
def fetch_batch(tickers: list, period="1y") -> dict:
    """
    Scarica prezzi storici per un batch di ticker.
    Ritorna {ticker: pd.Series di close prices}
    """
    result = {}
    if not tickers: return result
    try:
        if len(tickers) == 1:
            hist = yf.Ticker(tickers[0]).history(period=period, auto_adjust=True)
            if not hist.empty and 'Close' in hist.columns:
                closes = hist['Close'].dropna()
                if len(closes) > 20:
                    result[tickers[0]] = closes
        else:
            data = yf.download(
                tickers, period=period, interval='1d',
                group_by='ticker', auto_adjust=True, progress=False,
                threads=True
            )
            for t in tickers:
                try:
                    if len(tickers) > 1:
                        closes = data[t]['Close'].dropna() if t in data.columns.get_level_values(0) else None
                    else:
                        closes = data['Close'].dropna()
                    if closes is not None and len(closes) > 20:
                        result[t] = closes
                except Exception:
                    pass
    except Exception as e:
        print(f"  err batch: {e}")
    return result

# ── CALCOLO ATR ───────────────────────────────────────────────────
def calc_atr(closes, period=14):
    """ATR semplificato su sole close (True Range = |close_t - close_t-1|)."""
    if len(closes) < period + 1: return None
    diffs = np.abs(np.diff(closes.values))
    atr_current = np.mean(diffs[-period:])
    atr_3m      = np.mean(diffs[-63:]) if len(diffs) >= 63 else np.mean(diffs)
    return atr_current, atr_3m

# ── CALCOLO MA ────────────────────────────────────────────────────
def calc_ma(closes, period):
    if len(closes) < period: return None
    return float(closes.values[-period:].mean())

# ── ANALISI SINGOLO ETF ───────────────────────────────────────────
def analyze_etf(ticker: str, closes, etf_info: dict) -> dict:
    """
    Calcola tutti gli indicatori per un ETF.
    Ritorna dict con flags per le 4 categorie.
    """
    try:
        arr     = closes.values.astype(float)
        price   = float(arr[-1])
        n       = len(arr)

        # Protezione
        if price <= 0 or n < 50: return None

        # ── Minimi e massimi storici ──────────────────────────────
        low_52w  = float(np.min(arr[-252:])) if n >= 252 else float(np.min(arr))
        high_52w = float(np.max(arr[-252:])) if n >= 252 else float(np.max(arr))
        low_all  = float(np.min(arr))

        dist_52w_low  = (price - low_52w)  / low_52w  if low_52w  > 0 else 1
        dist_52w_high = (high_52w - price) / high_52w if high_52w > 0 else 1
        dist_all_low  = (price - low_all)  / low_all  if low_all  > 0 else 1

        # ── Performance ───────────────────────────────────────────
        ret_1w  = (price / arr[-6]  - 1) if n > 6  else None
        ret_4w  = (price / arr[-21] - 1) if n > 21 else None
        ret_3m  = (price / arr[-63] - 1) if n > 63 else None
        ret_ytd = None
        try:
            # Approssimazione YTD: inizio anno = ~252 * frazione anno
            days_since_jan = (datetime.now() - datetime(datetime.now().year,1,1)).days
            idx_ytd = max(1, n - days_since_jan)
            ret_ytd = (price / arr[idx_ytd] - 1) if idx_ytd < n else None
        except: pass

        # ── MA ─────────────────────────────────────────────────────
        ma50  = calc_ma(closes, 50)
        ma200 = calc_ma(closes, 200)

        # ── ATR ────────────────────────────────────────────────────
        atr_data    = calc_atr(closes, 14)
        atr_current = atr_data[0] if atr_data else None
        atr_3m_avg  = atr_data[1] if atr_data else None
        atr_ratio   = (atr_current / atr_3m_avg) if atr_current and atr_3m_avg and atr_3m_avg > 0 else None

        # ── Massimo 4 settimane ────────────────────────────────────
        high_4w = float(np.max(arr[-21:])) if n > 21 else price
        drawdown_4w = (high_4w - price) / high_4w if high_4w > 0 else 0

        # ── CATEGORIA 1: Minimo Storico ───────────────────────────
        is_min_storico = (dist_52w_low <= SOGLIA_MIN_STORICO)
        score_min_storico = 0
        if is_min_storico:
            score_min_storico = int(100 - dist_52w_low / SOGLIA_MIN_STORICO * 50)

        # ── CATEGORIA 2: Pullback in Trend ────────────────────────
        ma200_rising = False
        if ma200 and n >= 210:
            ma200_prev = calc_ma(closes.iloc[:-5] if hasattr(closes,'iloc') else closes[:-5], 200)
            ma200_rising = ma200 > (ma200_prev or 0)

        is_pullback = (
            SOGLIA_PULLBACK_MIN <= drawdown_4w <= SOGLIA_PULLBACK_MAX and
            ma200 is not None and price > ma200 * 0.95 and
            ma200_rising
        )
        score_pullback = 0
        if is_pullback:
            # Score più alto se vicino a MA200 e trend forte
            ma200_dist = abs(price - ma200) / ma200 if ma200 else 1
            score_pullback = int(min(100, 70 + (1 - ma200_dist * 10) * 30))

        # ── CATEGORIA 3: Compressione Volatilità ──────────────────
        is_compressione = (
            atr_ratio is not None and
            atr_ratio < (1 - SOGLIA_ATR_COMPRES)
        )
        score_compressione = 0
        if is_compressione:
            score_compressione = int(min(100, (1 - atr_ratio) * 150))

        # ── CATEGORIA 4: Minimo Relativo (calcolato dopo aggregazione) ──
        # Qui salviamo ret_3m per calcolo post-aggregazione
        is_min_relativo   = False  # impostato dopo
        score_min_relativo = 0

        # ── Score totale ──────────────────────────────────────────
        categorie = sum([is_min_storico, is_pullback, is_compressione])
        score_master = max(score_min_storico, score_pullback, score_compressione)

        return {
            "ticker":         ticker,
            "name":           etf_info.get("n", ticker),
            "borsa":          etf_info.get("b", ""),
            "categoria":      etf_info.get("c", ""),
            "price":          round(price, 4),
            "low_52w":        round(low_52w, 4),
            "high_52w":       round(high_52w, 4),
            "dist_52w_low":   round(dist_52w_low * 100, 2),
            "dist_52w_high":  round(dist_52w_high * 100, 2),
            "ret_1w":         round(ret_1w * 100, 2)  if ret_1w  is not None else None,
            "ret_4w":         round(ret_4w * 100, 2)  if ret_4w  is not None else None,
            "ret_3m":         round(ret_3m * 100, 2)  if ret_3m  is not None else None,
            "ret_ytd":        round(ret_ytd * 100, 2) if ret_ytd is not None else None,
            "ma50":           round(ma50, 4)  if ma50  else None,
            "ma200":          round(ma200, 4) if ma200 else None,
            "ma200_rising":   ma200_rising,
            "drawdown_4w":    round(drawdown_4w * 100, 2),
            "atr_ratio":      round(atr_ratio, 3) if atr_ratio else None,
            # FLAGS
            "is_min_storico":   is_min_storico,
            "is_pullback":      is_pullback,
            "is_compressione":  is_compressione,
            "is_min_relativo":  False,  # calcolato dopo
            # SCORES
            "score_min_storico":   score_min_storico,
            "score_pullback":      score_pullback,
            "score_compressione":  score_compressione,
            "score_min_relativo":  0,
            "score_master":        score_master,
            "n_categorie":         categorie,
        }
    except Exception as e:
        return None

# ── CALCOLO MINIMI RELATIVI ───────────────────────────────────────
def compute_min_relativi(results: list) -> list:
    """
    Calcola minimi relativi per settore.
    ETF con ret_3m < media_settore - SOGLIA_MIN_RELATIVO
    """
    # Raggruppa per categoria
    from collections import defaultdict
    by_cat = defaultdict(list)
    for r in results:
        if r and r.get('ret_3m') is not None:
            by_cat[r['categoria']].append(r['ret_3m'])

    # Media per categoria
    cat_means = {cat: np.mean(vals) for cat, vals in by_cat.items() if len(vals) >= 5}

    # Applica soglia
    for r in results:
        if not r: continue
        cat  = r['categoria']
        ret3 = r.get('ret_3m')
        if ret3 is not None and cat in cat_means:
            mean_cat = cat_means[cat]
            diff     = mean_cat - ret3  # positivo = sottoperformance
            if diff >= SOGLIA_MIN_RELATIVO * 100:
                r['is_min_relativo']   = True
                r['score_min_relativo'] = int(min(100, diff * 5))
                r['n_categorie']       += 1
                r['score_master']      = max(r['score_master'], r['score_min_relativo'])
        r['cat_mean_ret3m'] = round(cat_means.get(cat, 0), 2)

    return results

# ── MAIN ──────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"RAPTOR MIN FINDER — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Carica universo
    universe = load_universe()
    ticker_map = {e["t"]: e for e in universe}
    tickers    = [e["t"] for e in universe]
    print(f"\n  Universo: {len(tickers)} ETF")

    # 2. Scarica prezzi in batch
    print(f"\n  Download prezzi (batch {BATCH_SIZE})...")
    all_closes = {}
    batches    = [tickers[i:i+BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    total      = len(batches)

    for i, batch in enumerate(batches):
        print(f"  Batch {i+1}/{total} ({len(batch)} ticker)...", end=' ', flush=True)
        closes = fetch_batch(batch, period="1y")
        all_closes.update(closes)
        print(f"{len(closes)} ok")
        if i < total - 1:
            time.sleep(BATCH_DELAY)

    print(f"\n  Prezzi scaricati: {len(all_closes)}/{len(tickers)}")

    # 3. Analizza ogni ETF
    print("\n  Analisi ETF...")
    results = []
    for ticker, closes in all_closes.items():
        info = ticker_map.get(ticker, {"n": ticker, "b": "", "c": "Altro"})
        r = analyze_etf(ticker, closes, info)
        if r: results.append(r)

    print(f"  Analizzati: {len(results)} ETF")

    # 4. Calcola minimi relativi
    results = compute_min_relativi(results)

    # 5. Costruisce le 4 liste
    lista1 = sorted([r for r in results if r['is_min_storico']],   key=lambda x: x['score_min_storico'],   reverse=True)
    lista2 = sorted([r for r in results if r['is_pullback']],      key=lambda x: x['score_pullback'],      reverse=True)
    lista3 = sorted([r for r in results if r['is_compressione']],  key=lambda x: x['score_compressione'],  reverse=True)
    lista4 = sorted([r for r in results if r['is_min_relativo']],  key=lambda x: x['score_min_relativo'],  reverse=True)
    master  = sorted([r for r in results if r['n_categorie'] >= 2], key=lambda x: x['score_master'],       reverse=True)

    print(f"\n  📉 Minimi Storici:        {len(lista1)}")
    print(f"  🔄 Pullback in Trend:     {len(lista2)}")
    print(f"  🔥 Compressione Vol:      {len(lista3)}")
    print(f"  📊 Minimi Relativi:       {len(lista4)}")
    print(f"  ⭐ Master (2+ categorie): {len(master)}")

    # 6. Salva candidati per il live update
    all_candidates = list({r['ticker'] for r in lista1 + lista2 + lista3 + lista4 + master})
    print(f"\n  Candidati per live update: {len(all_candidates)}")

    # 7. Output JSON
    output = {
        "generated":    datetime.now(timezone.utc).isoformat(),
        "version":      "1.0",
        "universe_tot": len(tickers),
        "analyzed":     len(results),
        "candidates":   all_candidates,
        "stats": {
            "min_storico":  len(lista1),
            "pullback":     len(lista2),
            "compressione": len(lista3),
            "min_relativo": len(lista4),
            "master":       len(master),
        },
        "soglie": {
            "min_storico_pct":  SOGLIA_MIN_STORICO * 100,
            "pullback_min_pct": SOGLIA_PULLBACK_MIN * 100,
            "pullback_max_pct": SOGLIA_PULLBACK_MAX * 100,
            "atr_compres_pct":  SOGLIA_ATR_COMPRES * 100,
            "min_relativo_pct": SOGLIA_MIN_RELATIVO * 100,
        },
        "lista1_min_storico":  lista1[:200],
        "lista2_pullback":     lista2[:200],
        "lista3_compressione": lista3[:200],
        "lista4_min_relativo": lista4[:200],
        "lista_master":        master[:100],
    }

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(',', ':'))

    print(f"\n✅ Salvato: {OUT_FILE}")
    print(f"   Dimensione: {OUT_FILE.stat().st_size / 1024:.1f} KB")

if __name__ == "__main__":
    main()
