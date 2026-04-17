"""
RAPTOR SCANNER — Portfolio Tracker
Aggiunto alla fine di fetch_and_compute.py

Legge signals.json, aggiorna portfolio.json:
- Apre posizioni BUY/SELL con score >= 65
- Stop iniziale ATR×2
- Trailing stop -5% dal massimo quando gain > +3%
- Chiude su segnale opposto o stop violato
- Max 5 posizioni per categoria
"""

import json, os
from datetime import datetime, timezone

PORTFOLIO_FILE = "data/portfolio.json"
SCORE_MIN      = 65
MAX_PER_CAT    = 5
ATR_MULT       = 2.0
ATR_MULT_LEVA  = 1.5
TRAILING_TRIGGER = 3.0   # % gain per attivare trailing
TRAILING_PCT     = 5.0   # % dal massimo per trailing stop


def load_portfolio() -> dict:
    if os.path.exists(PORTFOLIO_FILE):
        try:
            with open(PORTFOLIO_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"positions": [], "updated": ""}


def save_portfolio(data: dict):
    os.makedirs("data", exist_ok=True)
    data["updated"] = datetime.now(timezone.utc).isoformat()
    with open(PORTFOLIO_FILE, "w") as f:
        json.dump(data, f, indent=2)


def days_between(date_str: str) -> int:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return (datetime.utcnow() - d).days
    except Exception:
        return 0


def update_portfolio(signals: list):
    """
    Aggiorna il portafoglio basandosi sui segnali correnti.
    signals = lista di dict da signals.json
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    portfolio = load_portfolio()
    positions = portfolio.get("positions", [])

    # Indice per lookup rapido
    sig_by_ticker = {s["ticker"]: s for s in signals}

    # ── 1. Aggiorna posizioni aperte ─────────────────────────
    for pos in positions:
        if pos["status"] != "open":
            continue

        ticker = pos["ticker"]
        sig    = sig_by_ticker.get(ticker)
        if not sig:
            continue

        current_price = sig.get("price")
        if not current_price:
            continue

        pos["current_price"] = current_price
        pos["score"]         = sig.get("score", pos.get("score", 0))
        pos["days_held"]     = days_between(pos.get("entry_date", today))

        is_buy = pos["direction"] == "BUY"

        # Calcola gain
        if pos["entry_price"] and pos["entry_price"] > 0:
            if is_buy:
                gain_pct = (current_price - pos["entry_price"]) / pos["entry_price"] * 100
            else:
                gain_pct = (pos["entry_price"] - current_price) / pos["entry_price"] * 100
        else:
            gain_pct = 0
        pos["gain_pct"] = round(gain_pct, 3)

        # Aggiorna massimo prezzo (per trailing)
        if is_buy:
            if current_price > pos.get("max_price", 0):
                pos["max_price"] = current_price
        else:
            if current_price < pos.get("min_price", pos["entry_price"] * 2):
                pos["min_price"] = current_price

        # Trailing stop — attiva se gain > 3%
        atr = sig.get("atr") or pos.get("atr_entry", 0)
        if gain_pct >= TRAILING_TRIGGER:
            pos["trailing_active"] = True
            if is_buy:
                trailing_stop = pos["max_price"] * (1 - TRAILING_PCT / 100)
                # Trailing non può scendere sotto il costo di entrata
                pos["current_stop"] = max(trailing_stop, pos["entry_price"])
            else:
                trailing_stop = pos["min_price"] * (1 + TRAILING_PCT / 100)
                pos["current_stop"] = min(trailing_stop, pos["entry_price"])
        else:
            pos["trailing_active"] = False

        # ── Verifica chiusura ──
        close_reason = None
        current_stop = pos.get("current_stop", pos.get("initial_stop"))

        # Stop violato
        if current_stop:
            if is_buy and current_price <= current_stop:
                close_reason = "stop_loss"
            elif not is_buy and current_price >= current_stop:
                close_reason = "stop_loss"

        # Segnale opposto
        current_signal = sig.get("signal", "RANGING")
        if is_buy and current_signal in ("SELL", "RANGING"):
            close_reason = "signal_reversal"
        elif not is_buy and current_signal in ("BUY", "RANGING"):
            close_reason = "signal_reversal"

        if close_reason:
            pos["status"]       = "closed"
            pos["close_date"]   = today
            pos["close_price"]  = current_price
            pos["close_reason"] = close_reason
            print(f"  CHIUSA {ticker}: {pos['direction']} gain={gain_pct:.2f}% motivo={close_reason}")

    # ── 2. Apri nuove posizioni ──────────────────────────────
    # Conta posizioni aperte per categoria
    open_by_cat = {}
    for pos in positions:
        if pos["status"] == "open":
            cat = pos.get("categoria", "")
            open_by_cat[cat] = open_by_cat.get(cat, 0) + 1

    # Ticker già in portafoglio aperto
    open_tickers = {pos["ticker"] for pos in positions if pos["status"] == "open"}

    for sig in signals:
        ticker    = sig.get("ticker")
        signal    = sig.get("signal")
        score     = sig.get("score", 0)
        categoria = sig.get("categoria", "")
        price     = sig.get("price")
        atr       = sig.get("atr")

        # Solo BUY e SELL con score >= 65
        if signal not in ("BUY", "SELL"):
            continue
        if score < SCORE_MIN:
            continue
        if not price or not atr:
            continue
        if ticker in open_tickers:
            continue

        # Max posizioni per categoria
        if open_by_cat.get(categoria, 0) >= MAX_PER_CAT:
            continue

        # Calcola stop iniziale
        is_buy     = signal == "BUY"
        is_lev     = sig.get("is_leveraged", False)
        mult       = ATR_MULT_LEVA if is_lev else ATR_MULT

        if is_buy:
            initial_stop = round(price - atr * mult, 4)
        else:
            initial_stop = round(price + atr * mult, 4)

        new_pos = {
            "ticker":        ticker,
            "nome":          sig.get("nome", ticker),
            "categoria":     categoria,
            "direction":     signal,
            "status":        "open",
            "entry_date":    today,
            "entry_price":   price,
            "current_price": price,
            "close_date":    None,
            "close_price":   None,
            "close_reason":  None,
            "initial_stop":  initial_stop,
            "current_stop":  initial_stop,
            "max_price":     price,
            "min_price":     price,
            "atr_entry":     atr,
            "score":         score,
            "gain_pct":      0.0,
            "days_held":     0,
            "trailing_active": False,
        }
        positions.append(new_pos)
        open_tickers.add(ticker)
        open_by_cat[categoria] = open_by_cat.get(categoria, 0) + 1
        print(f"  APERTA {ticker}: {signal} @ {price} stop={initial_stop} score={score}")

    # Mantieni max 200 posizioni totali (aperte + ultime chiuse)
    open_pos   = [p for p in positions if p["status"] == "open"]
    closed_pos = sorted(
        [p for p in positions if p["status"] == "closed"],
        key=lambda p: p.get("close_date", ""), reverse=True
    )[:150]
    portfolio["positions"] = open_pos + closed_pos

    save_portfolio(portfolio)
    print(f"  Portfolio salvato: {len(open_pos)} aperte, {len(closed_pos)} chiuse")
    return portfolio


# ── INTEGRAZIONE IN fetch_and_compute.py ───────────────────────────────────────
# Aggiungere alla fine della funzione main(), dopo aver salvato signals.json:
#
#   from portfolio_tracker import update_portfolio
#   log.info("Aggiornamento portafoglio...")
#   update_portfolio(results)
#   log.info("Portafoglio aggiornato")


if __name__ == "__main__":
    # Test standalone — carica signals.json e aggiorna portfolio
    import sys
    signals_path = sys.argv[1] if len(sys.argv) > 1 else "data/signals.json"
    if os.path.exists(signals_path):
        with open(signals_path) as f:
            data = json.load(f)
        signals = data.get("signals", [])
        print(f"Caricati {len(signals)} segnali da {signals_path}")
        update_portfolio(signals)
    else:
        print(f"File non trovato: {signals_path}")
