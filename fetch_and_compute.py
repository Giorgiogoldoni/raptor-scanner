"""
RAPTOR WEALTH MERIDIAN — ETF Scanner Engine
fetch_and_compute.py

Scarica dati OHLCV da Yahoo Finance per tutti gli ETF dell'universe,
calcola 19 indicatori tecnici, genera signals.json per il frontend.

Run: python fetch_and_compute.py
Output: data/signals.json, data/cache/*.json, logs/run.log
"""

import os, json, time, logging, traceback, math
from datetime import datetime, timedelta
from pathlib import Path
import requests
import numpy as np

# ── CONFIGURAZIONE ─────────────────────────────────────────────────────────────
CONFIG = {
    "history_days":           120,
    "min_candles":            60,
    "request_delay":          0.35,
    "max_retries":            3,
    "retry_delay":            2.0,
    "cache_dir":              "data/cache",
    "output_file":            "data/signals.json",
    "log_file":               "logs/run.log",
    "cache_max_age_hours":    6,
    # Soglie ER
    "er_threshold_leva":      0.25,
    "er_trending_leva":       0.30,
    "er_threshold_normal":    0.20,
    "er_trending_normal":     0.25,
    "er_watch_rising_bars":   3,
    # Supertrend
    "supertrend_atr_period":  10,
    "supertrend_multiplier":  3.0,
    # Bollinger
    "bb_period":              20,
    "bb_std":                 2.0,
    "bb_compression_pct":     0.15,
    # Volume
    "volume_ratio_period":    20,
    "volume_ratio_watch":     1.3,
    # Score weights
    "score_weights": {
        "er":         0.25,
        "supertrend": 0.20,
        "kama":       0.20,
        "momentum3m": 0.15,
        "adx":        0.10,
        "rsi":        0.05,
        "sar":        0.05,
    },
}

# ── ETF UNIVERSE ───────────────────────────────────────────────────────────────
# (ticker_yahoo, nome, categoria, is_leveraged, is_short, currency)
ETF_UNIVERSE = [
    # LEVA LONG
    ("QQQ3.MI","WisdomTree Nasdaq 3x Daily","Leva Long",True,False,"USD"),
    ("3USL.MI","WisdomTree S&P500 3x Daily","Leva Long",True,False,"USD"),
    ("LQQ.MI","Leverage Shares 3x Nasdaq","Leva Long",True,False,"USD"),
    ("3DAL.MI","WisdomTree DAX 3x Daily","Leva Long",True,False,"EUR"),
    ("DX3L.MI","WisdomTree DAX 3x Long","Leva Long",True,False,"EUR"),
    ("2BUL.MI","WisdomTree S&P500 2x Daily","Leva Long",True,False,"USD"),
    ("2QQL.MI","WisdomTree Nasdaq 2x Daily","Leva Long",True,False,"USD"),
    ("3LTS.MI","GraniteShares 3x Tesla","Leva Long",True,False,"USD"),
    ("3LAP.MI","GraniteShares 3x Apple","Leva Long",True,False,"USD"),
    ("3LNV.MI","GraniteShares 3x Nvidia","Leva Long",True,False,"USD"),
    ("3LAM.MI","GraniteShares 3x Amazon","Leva Long",True,False,"USD"),
    ("3LMS.MI","GraniteShares 3x Microsoft","Leva Long",True,False,"USD"),
    ("3LGO.MI","GraniteShares 3x Alphabet","Leva Long",True,False,"USD"),
    ("3LMT.MI","GraniteShares 3x Meta","Leva Long",True,False,"USD"),
    ("3LNF.MI","GraniteShares 3x Netflix","Leva Long",True,False,"USD"),
    ("3LCO.MI","GraniteShares 3x Coinbase","Leva Long",True,False,"USD"),
    ("3LBA.MI","GraniteShares 3x BAC","Leva Long",True,False,"USD"),
    ("3LJP.MI","GraniteShares 3x JPMorgan","Leva Long",True,False,"USD"),
    ("3LAD.MI","GraniteShares 3x AMD","Leva Long",True,False,"USD"),
    ("TSL2.MI","Leverage Shares 2x Tesla","Leva Long",True,False,"USD"),
    ("NVD2.MI","Leverage Shares 2x Nvidia","Leva Long",True,False,"USD"),
    ("AMZ2.MI","Leverage Shares 2x Amazon","Leva Long",True,False,"USD"),
    ("AAP2.MI","Leverage Shares 2x Apple","Leva Long",True,False,"USD"),
    ("GOG2.MI","Leverage Shares 2x Alphabet","Leva Long",True,False,"USD"),
    ("MET2.MI","Leverage Shares 2x Meta","Leva Long",True,False,"USD"),
    ("MSF2.MI","Leverage Shares 2x Microsoft","Leva Long",True,False,"USD"),
    ("MSTR2.MI","Leverage Shares 2x MicroStrategy","Leva Long",True,False,"USD"),
    ("PLTR2.MI","Leverage Shares 2x Palantir","Leva Long",True,False,"USD"),
    ("COIN2.MI","Leverage Shares 2x Coinbase","Leva Long",True,False,"USD"),
    ("3GOL.MI","WisdomTree Gold 3x","Leva Long",True,False,"USD"),
    ("3SIL.MI","WisdomTree Silver 3x","Leva Long",True,False,"USD"),
    ("3OIL.MI","WisdomTree Crude Oil 3x","Leva Long",True,False,"USD"),
    ("2GOL.MI","WisdomTree Gold 2x","Leva Long",True,False,"USD"),
    ("TECL.MI","Leverage Shares 3x Technology","Leva Long",True,False,"USD"),
    # SHORT
    ("XSPS.MI","Xtrackers S&P500 Short","Short",False,True,"USD"),
    ("XSND.MI","Xtrackers Nasdaq Short","Short",False,True,"USD"),
    ("XSDX.MI","Xtrackers DAX Short","Short",False,True,"EUR"),
    ("SDS.MI","WisdomTree S&P500 2x Short","Short",True,True,"USD"),
    ("QQQ3S.MI","WisdomTree Nasdaq 3x Short","Short",True,True,"USD"),
    ("DX3S.MI","WisdomTree DAX 3x Short","Short",True,True,"EUR"),
    ("3USS.MI","WisdomTree S&P500 3x Short","Short",True,True,"USD"),
    ("ESXS.MI","WisdomTree Euro Stoxx 50 2x Short","Short",True,True,"EUR"),
    ("3STS.MI","GraniteShares 3x Short Tesla","Short",True,True,"USD"),
    ("3SAP.MI","GraniteShares 3x Short Apple","Short",True,True,"USD"),
    ("3SNV.MI","GraniteShares 3x Short Nvidia","Short",True,True,"USD"),
    ("3SAM.MI","GraniteShares 3x Short Amazon","Short",True,True,"USD"),
    ("3SMS.MI","GraniteShares 3x Short Microsoft","Short",True,True,"USD"),
    ("3SGO.MI","GraniteShares 3x Short Alphabet","Short",True,True,"USD"),
    ("3SMT.MI","GraniteShares 3x Short Meta","Short",True,True,"USD"),
    ("STSL.MI","Leverage Shares 2x Short Tesla","Short",True,True,"USD"),
    ("SNVD.MI","Leverage Shares 2x Short Nvidia","Short",True,True,"USD"),
    ("SAMZ.MI","Leverage Shares 2x Short Amazon","Short",True,True,"USD"),
    ("SCRU.MI","WisdomTree Crude Oil Short","Short",False,True,"USD"),
    ("SGOL.MI","WisdomTree Gold Short","Short",False,True,"USD"),
    ("SSIL.MI","WisdomTree Silver Short","Short",False,True,"USD"),
    # MATERIE PRIME
    ("PHAU.MI","WisdomTree Physical Gold","Materie Prime",False,False,"USD"),
    ("PHAG.MI","WisdomTree Physical Silver","Materie Prime",False,False,"USD"),
    ("PHPT.MI","WisdomTree Physical Platinum","Materie Prime",False,False,"USD"),
    ("PHPD.MI","WisdomTree Physical Palladium","Materie Prime",False,False,"USD"),
    ("SGLD.MI","iShares Physical Gold","Materie Prime",False,False,"USD"),
    ("COPA.MI","WisdomTree Copper","Materie Prime",False,False,"USD"),
    ("ALUM.MI","WisdomTree Aluminium","Materie Prime",False,False,"USD"),
    ("NICK.MI","WisdomTree Nickel","Materie Prime",False,False,"USD"),
    ("ZINC.MI","WisdomTree Zinc","Materie Prime",False,False,"USD"),
    ("CRUD.MI","WisdomTree Crude Oil","Materie Prime",False,False,"USD"),
    ("BRNT.MI","WisdomTree Brent Crude","Materie Prime",False,False,"USD"),
    ("NGAS.MI","WisdomTree Natural Gas","Materie Prime",False,False,"USD"),
    ("WEAT.MI","WisdomTree Wheat","Materie Prime",False,False,"USD"),
    ("CORN.MI","WisdomTree Corn","Materie Prime",False,False,"USD"),
    ("SOYB.MI","WisdomTree Soybeans","Materie Prime",False,False,"USD"),
    ("CMOD.MI","WisdomTree Commodity","Materie Prime",False,False,"USD"),
    ("AIGP.MI","iShares Diversified Commodity","Materie Prime",False,False,"USD"),
    ("BCOM.DE","iShares Bloomberg Commodity","Materie Prime",False,False,"USD"),
    # TEMATICI
    ("WTAI.MI","WisdomTree AI","Tematico",False,False,"USD"),
    ("AIAI.DE","L&G AI & Big Data","Tematico",False,False,"USD"),
    ("XAIX.DE","Xtrackers AI & Big Data","Tematico",False,False,"USD"),
    ("IRBO.DE","iShares Robotics & AI","Tematico",False,False,"USD"),
    ("SEMI.DE","iShares Semiconductor","Tematico",False,False,"USD"),
    ("CHIP.DE","VanEck Semiconductor","Tematico",False,False,"USD"),
    ("UCYB.MI","Invesco Cybersecurity","Tematico",False,False,"USD"),
    ("CYBE.DE","L&G Cybersecurity","Tematico",False,False,"USD"),
    ("HACK.DE","WisdomTree Cybersecurity","Tematico",False,False,"USD"),
    ("ROBO.MI","iShares Automation & Robotics","Tematico",False,False,"USD"),
    ("RBOT.DE","Xtrackers Robotics & AI","Tematico",False,False,"USD"),
    ("BFIV.MI","Amundi 5G","Tematico",False,False,"USD"),
    ("YODA.MI","HANetf Future of Space","Tematico",False,False,"USD"),
    ("GAME.DE","VanEck Video Gaming & Esports","Tematico",False,False,"USD"),
    ("FINT.MI","Invesco Financial Innovation","Tematico",False,False,"USD"),
    ("KOIN.MI","L&G Blockchain","Tematico",False,False,"USD"),
    ("DAPP.DE","VanEck Blockchain","Tematico",False,False,"USD"),
    ("BTCE.DE","ETC Group Physical Bitcoin","Tematico",False,False,"USD"),
    ("ETHE.DE","ETC Group Physical Ethereum","Tematico",False,False,"USD"),
    ("ECAR.MI","iShares Electric Vehicles","Tematico",False,False,"USD"),
    ("DRIV.DE","Global X Electric Vehicles","Tematico",False,False,"USD"),
    ("BATT.MI","Amundi Battery Technology","Tematico",False,False,"USD"),
    ("CHRG.DE","WisdomTree Battery Solutions","Tematico",False,False,"USD"),
    ("HTWO.MI","L&G Hydrogen Economy","Tematico",False,False,"USD"),
    ("HGEN.DE","VanEck Hydrogen Economy","Tematico",False,False,"USD"),
    ("ISUN.MI","Invesco Solar Energy","Tematico",False,False,"USD"),
    ("INRG.MI","iShares Global Clean Energy","Tematico",False,False,"USD"),
    ("IGEN.MI","L&G Clean Energy","Tematico",False,False,"USD"),
    ("WTEW.DE","WisdomTree Energy Transition","Tematico",False,False,"EUR"),
    ("NUKL.DE","VanEck Uranium & Nuclear","Tematico",False,False,"USD"),
    ("URAN.DE","Global X Uranium","Tematico",False,False,"USD"),
    ("AURA.MI","HANetf Uranium Miners","Tematico",False,False,"USD"),
    ("IQQW.MI","iShares Global Water","Tematico",False,False,"USD"),
    ("WATL.MI","Invesco Water Resources","Tematico",False,False,"USD"),
    ("GLUG.MI","L&G Clean Water","Tematico",False,False,"USD"),
    ("FOOD.MI","Rize Sustainable Food","Tematico",False,False,"USD"),
    ("AGRI.DE","iShares Agribusiness","Tematico",False,False,"USD"),
    ("WOOD.DE","iShares Global Timber","Tematico",False,False,"USD"),
    ("IQQH.MI","iShares Healthcare Innovation","Tematico",False,False,"USD"),
    ("SBIO.MI","SPDR Biotech","Tematico",False,False,"USD"),
    ("DNA.MI","L&G Pharma Breakthrough","Tematico",False,False,"USD"),
    ("GNOM.DE","iShares Genomics Immunology","Tematico",False,False,"USD"),
    ("HEAL.DE","iShares MSCI World Health Care","Tematico",False,False,"USD"),
    ("AGES.MI","iShares Ageing Population","Tematico",False,False,"USD"),
    ("BOLD.DE","L&G Global Ageing","Tematico",False,False,"USD"),
    ("DFEN.MI","HANetf Future of Defence","Tematico",False,False,"USD"),
    ("NATO.DE","VanEck Defence","Tematico",False,False,"USD"),
    ("SHLD.DE","WisdomTree Defence","Tematico",False,False,"USD"),
    ("AERO.DE","Invesco Aerospace & Defence","Tematico",False,False,"USD"),
    ("INFR.MI","iShares Global Infrastructure","Tematico",False,False,"USD"),
    ("IQQP.MI","iShares Global REIT","Tematico",False,False,"USD"),
    ("XREA.DE","Xtrackers FTSE EPRA REIT","Tematico",False,False,"EUR"),
    ("SUSW.MI","iShares MSCI World ESG Screened","Tematico",False,False,"USD"),
    # SETTORIALI MONDO
    ("XDWT.DE","Xtrackers MSCI World IT","Settoriale Mondo",False,False,"USD"),
    ("XDWH.DE","Xtrackers MSCI World Healthcare","Settoriale Mondo",False,False,"USD"),
    ("XDWF.DE","Xtrackers MSCI World Financials","Settoriale Mondo",False,False,"USD"),
    ("XDWE.DE","Xtrackers MSCI World Energy","Settoriale Mondo",False,False,"USD"),
    ("XDWU.DE","Xtrackers MSCI World Utilities","Settoriale Mondo",False,False,"USD"),
    ("XDWM.DE","Xtrackers MSCI World Materials","Settoriale Mondo",False,False,"USD"),
    ("XDWI.DE","Xtrackers MSCI World Industrials","Settoriale Mondo",False,False,"USD"),
    ("XDWC.DE","Xtrackers MSCI World Cons Discret","Settoriale Mondo",False,False,"USD"),
    ("XDWS.DE","Xtrackers MSCI World Cons Staples","Settoriale Mondo",False,False,"USD"),
    ("XDWR.DE","Xtrackers MSCI World Real Estate","Settoriale Mondo",False,False,"USD"),
    ("XDWK.DE","Xtrackers MSCI World Comm Services","Settoriale Mondo",False,False,"USD"),
    ("STWT.DE","SPDR MSCI World Technology","Settoriale Mondo",False,False,"USD"),
    ("STWH.DE","SPDR MSCI World Healthcare","Settoriale Mondo",False,False,"USD"),
    ("STWF.DE","SPDR MSCI World Financials","Settoriale Mondo",False,False,"USD"),
    ("STWE.DE","SPDR MSCI World Energy","Settoriale Mondo",False,False,"USD"),
    ("STWU.DE","SPDR MSCI World Utilities","Settoriale Mondo",False,False,"USD"),
    ("STWI.DE","SPDR MSCI World Industrials","Settoriale Mondo",False,False,"USD"),
    ("STWD.DE","SPDR MSCI World Cons Discret","Settoriale Mondo",False,False,"USD"),
    ("STWS.DE","SPDR MSCI World Cons Staples","Settoriale Mondo",False,False,"USD"),
    ("STWR.DE","SPDR MSCI World Real Estate","Settoriale Mondo",False,False,"USD"),
    # SETTORIALI USA
    ("IUIT.MI","iShares S&P500 IT Sector","Settoriale USA",False,False,"USD"),
    ("IUHC.MI","iShares S&P500 Healthcare","Settoriale USA",False,False,"USD"),
    ("IUFS.MI","iShares S&P500 Financials","Settoriale USA",False,False,"USD"),
    ("IUES.MI","iShares S&P500 Energy","Settoriale USA",False,False,"USD"),
    ("IUUS.MI","iShares S&P500 Utilities","Settoriale USA",False,False,"USD"),
    ("IUMS.MI","iShares S&P500 Materials","Settoriale USA",False,False,"USD"),
    ("IUIS.MI","iShares S&P500 Industrials","Settoriale USA",False,False,"USD"),
    ("IUCD.MI","iShares S&P500 Cons Discret","Settoriale USA",False,False,"USD"),
    ("IUCS.MI","iShares S&P500 Cons Staples","Settoriale USA",False,False,"USD"),
    ("IURE.MI","iShares S&P500 Real Estate","Settoriale USA",False,False,"USD"),
    ("SXLK.MI","SPDR S&P500 Technology","Settoriale USA",False,False,"USD"),
    ("SXLV.MI","SPDR S&P500 Health Care","Settoriale USA",False,False,"USD"),
    ("SXLF.MI","SPDR S&P500 Financials","Settoriale USA",False,False,"USD"),
    ("SXLE.MI","SPDR S&P500 Energy","Settoriale USA",False,False,"USD"),
    ("SXLB.MI","SPDR S&P500 Materials","Settoriale USA",False,False,"USD"),
    ("SXLI.MI","SPDR S&P500 Industrials","Settoriale USA",False,False,"USD"),
    ("SXLY.MI","SPDR S&P500 Cons Discret","Settoriale USA",False,False,"USD"),
    ("SXLC.MI","SPDR S&P500 Cons Staples","Settoriale USA",False,False,"USD"),
    # SETTORIALI EUROPA
    ("EXV1.DE","iShares STOXX Europe 600 Banks","Settoriale Europa",False,False,"EUR"),
    ("EXV2.DE","iShares STOXX Europe 600 Basic Resources","Settoriale Europa",False,False,"EUR"),
    ("EXV3.DE","iShares STOXX Europe 600 Technology","Settoriale Europa",False,False,"EUR"),
    ("EXV4.DE","iShares STOXX Europe 600 Healthcare","Settoriale Europa",False,False,"EUR"),
    ("EXV5.DE","iShares STOXX Europe 600 Utilities","Settoriale Europa",False,False,"EUR"),
    ("EXV6.DE","iShares STOXX Europe 600 Industrials","Settoriale Europa",False,False,"EUR"),
    ("EXV7.DE","iShares STOXX Europe 600 Oil & Gas","Settoriale Europa",False,False,"EUR"),
    ("EXV8.DE","iShares STOXX Europe 600 Financial Svcs","Settoriale Europa",False,False,"EUR"),
    ("EXV9.DE","iShares STOXX Europe 600 Cons Staples","Settoriale Europa",False,False,"EUR"),
    ("EXVA.DE","iShares STOXX Europe 600 Telecom","Settoriale Europa",False,False,"EUR"),
    ("EXVB.DE","iShares STOXX Europe 600 Cons Discret","Settoriale Europa",False,False,"EUR"),
    ("EXVC.DE","iShares STOXX Europe 600 Real Estate","Settoriale Europa",False,False,"EUR"),
    ("EXVD.DE","iShares STOXX Europe 600 Insurance","Settoriale Europa",False,False,"EUR"),
    # AZIONARI BROAD
    ("SWRD.MI","SPDR MSCI World","Azionario",False,False,"USD"),
    ("IWDA.MI","iShares Core MSCI World","Azionario",False,False,"USD"),
    ("VWCE.DE","Vanguard FTSE All-World Acc","Azionario",False,False,"USD"),
    ("CSPX.MI","iShares Core S&P500","Azionario",False,False,"USD"),
    ("VUAA.MI","Vanguard S&P500 Acc","Azionario",False,False,"USD"),
    ("EQQQ.MI","Invesco EQQQ Nasdaq-100","Azionario",False,False,"USD"),
    ("XDAX.DE","Xtrackers DAX","Azionario",False,False,"EUR"),
    ("EXS1.DE","iShares Core DAX","Azionario",False,False,"EUR"),
    ("MEUD.MI","Amundi MSCI Europe","Azionario",False,False,"EUR"),
    ("IMEU.MI","iShares Core MSCI Europe","Azionario",False,False,"EUR"),
    ("ISP5.MI","iShares Core Euro Stoxx 50","Azionario",False,False,"EUR"),
    ("XJPN.DE","Xtrackers MSCI Japan","Azionario",False,False,"JPY"),
    ("VFEM.MI","Vanguard FTSE Emerging Markets","Azionario",False,False,"USD"),
    ("IEMA.MI","iShares Core MSCI EM IMI","Azionario",False,False,"USD"),
    # AZIONARI FACTOR
    ("IWVL.MI","iShares MSCI World Value Factor","Azionario",False,False,"USD"),
    ("IWMO.MI","iShares MSCI World Momentum Factor","Azionario",False,False,"USD"),
    ("IWQU.MI","iShares MSCI World Quality Factor","Azionario",False,False,"USD"),
    ("MVOL.MI","iShares MSCI World Min Vol","Azionario",False,False,"USD"),
    ("XDWM2.DE","Xtrackers MSCI World Multifactor","Azionario",False,False,"USD"),
    ("XDEQ.DE","Xtrackers MSCI World Equal Weight","Azionario",False,False,"USD"),
    ("XDEM.DE","Xtrackers MSCI World Momentum","Azionario",False,False,"USD"),
    ("XDEV.DE","Xtrackers MSCI World Value","Azionario",False,False,"USD"),
    # AZIONARI SMALL CAP
    ("WLDS.MI","iShares MSCI World Small Cap","Azionario",False,False,"USD"),
    ("WSML.MI","iShares MSCI World Small Cap","Azionario",False,False,"USD"),
    ("XSCU.DE","Xtrackers MSCI World Small Cap","Azionario",False,False,"USD"),
    ("XESC.DE","Xtrackers MSCI Europe Small Cap","Azionario",False,False,"EUR"),
    # AZIONARI DIVIDENDI ACC
    ("VHYD.MI","Vanguard FTSE All-World High Div","Azionario",False,False,"USD"),
    ("IQDV.MI","iShares MSCI World Quality Dividend","Azionario",False,False,"USD"),
    ("XDWD.DE","Xtrackers MSCI World High Dividend","Azionario",False,False,"USD"),
    ("TDIV.MI","VanEck Morningstar Dev Markets Div","Azionario",False,False,"EUR"),
    # COUNTRY SVILUPPATI
    ("XGSD.DE","Xtrackers MSCI Germany","Azionario",False,False,"EUR"),
    ("EWQ.DE","iShares MSCI France","Azionario",False,False,"EUR"),
    ("XESI.DE","Xtrackers MSCI Spain","Azionario",False,False,"EUR"),
    ("IITL.MI","iShares FTSE MIB","Azionario",False,False,"EUR"),
    ("EWU.DE","iShares MSCI United Kingdom","Azionario",False,False,"GBP"),
    ("XDJP.DE","Xtrackers Nikkei 225","Azionario",False,False,"JPY"),
    ("EAUS.DE","iShares MSCI Australia","Azionario",False,False,"AUD"),
    ("XCAN.DE","Xtrackers MSCI Canada","Azionario",False,False,"CAD"),
    ("XCHF.DE","Xtrackers MSCI Switzerland","Azionario",False,False,"CHF"),
    ("XSWE.DE","Xtrackers MSCI Sweden","Azionario",False,False,"SEK"),
    ("XNOR.DE","Xtrackers MSCI Norway","Azionario",False,False,"NOK"),
    ("XDEN.DE","Xtrackers MSCI Denmark","Azionario",False,False,"DKK"),
    ("XNLB.DE","Xtrackers MSCI Netherlands","Azionario",False,False,"EUR"),
    ("XSGP.DE","Xtrackers MSCI Singapore","Azionario",False,False,"SGD"),
    ("XHKG.DE","Xtrackers MSCI Hong Kong","Azionario",False,False,"HKD"),
    ("XKOR.DE","Xtrackers MSCI South Korea","Azionario",False,False,"KRW"),
    ("XTAI.DE","Xtrackers MSCI Taiwan","Azionario",False,False,"TWD"),
    # COUNTRY EMERGENTI
    ("XINC.DE","Xtrackers MSCI India","Azionario",False,False,"USD"),
    ("NDIA.MI","iShares MSCI India","Azionario",False,False,"USD"),
    ("XCHI.DE","Xtrackers MSCI China","Azionario",False,False,"USD"),
    ("MCHI.MI","iShares MSCI China","Azionario",False,False,"USD"),
    ("XBRA.DE","Xtrackers MSCI Brazil","Azionario",False,False,"USD"),
    ("XMEX.DE","Xtrackers MSCI Mexico","Azionario",False,False,"USD"),
    ("XTUR.DE","Xtrackers MSCI Turkey","Azionario",False,False,"USD"),
    ("XPOL.DE","Xtrackers MSCI Poland","Azionario",False,False,"EUR"),
    ("XSAF.DE","Xtrackers MSCI South Africa","Azionario",False,False,"USD"),
    ("XVIE.DE","Xtrackers MSCI Vietnam","Azionario",False,False,"USD"),
    ("XIND.DE","Xtrackers MSCI Indonesia","Azionario",False,False,"USD"),
    ("XSAU.DE","Xtrackers MSCI Saudi Arabia","Azionario",False,False,"USD"),
    ("XUAE.DE","Xtrackers MSCI UAE","Azionario",False,False,"USD"),
    # AREE GEOGRAFICHE
    ("XDEX.DE","Xtrackers MSCI Europe ex-UK","Azionario",False,False,"EUR"),
    ("XEMC.DE","Xtrackers MSCI EM Asia","Azionario",False,False,"USD"),
    ("XEML.DE","Xtrackers MSCI EM Latin America","Azionario",False,False,"USD"),
    ("XNAS.DE","Xtrackers MSCI Nordic","Azionario",False,False,"EUR"),
    ("XPAC.DE","Xtrackers MSCI Pacific ex-Japan","Azionario",False,False,"USD"),
    ("XEME.DE","Xtrackers MSCI Eastern Europe","Azionario",False,False,"EUR"),
    # OBBLIGAZIONARI GOV EUR
    ("IGLA.MI","iShares Core EUR Govt Bond","Obbligazionario",False,False,"EUR"),
    ("IS3N.DE","iShares EUR Govt Bond 1-3yr","Obbligazionario",False,False,"EUR"),
    ("IS3O.DE","iShares EUR Govt Bond 3-7yr","Obbligazionario",False,False,"EUR"),
    ("IS3P.DE","iShares EUR Govt Bond 7-10yr","Obbligazionario",False,False,"EUR"),
    ("IDTL.MI","iShares EUR Govt Bond 20yr+","Obbligazionario",False,False,"EUR"),
    ("DBXG.DE","Xtrackers Germany Govt Bond","Obbligazionario",False,False,"EUR"),
    ("BTPS.MI","iShares BTP","Obbligazionario",False,False,"EUR"),
    ("IBCI.MI","iShares EUR Inflation Linked Govt","Obbligazionario",False,False,"EUR"),
    # OBBLIGAZIONARI GOV USD
    ("DTLA.MI","iShares USD Treasury 20+yr","Obbligazionario",False,False,"USD"),
    ("IBTM.MI","iShares USD Treasury 7-10yr","Obbligazionario",False,False,"USD"),
    ("IBTS.MI","iShares USD Treasury 1-3yr","Obbligazionario",False,False,"USD"),
    ("DTLAH.MI","iShares USD Treasury 20+yr EUR Hdg","Obbligazionario",False,False,"EUR-hedged"),
    ("IBTMH.MI","iShares USD Treasury 7-10yr EUR Hdg","Obbligazionario",False,False,"EUR-hedged"),
    ("IBTSH.MI","iShares USD Treasury 1-3yr EUR Hdg","Obbligazionario",False,False,"EUR-hedged"),
    ("ITPS.MI","iShares USD TIPS","Obbligazionario",False,False,"USD"),
    ("ITPSH.MI","iShares USD TIPS EUR Hedged","Obbligazionario",False,False,"EUR-hedged"),
    # CORPORATE
    ("IEAC.MI","iShares Core EUR Corp Bond","Obbligazionario",False,False,"EUR"),
    ("XDEB.DE","Xtrackers EUR Corporate Bond","Obbligazionario",False,False,"EUR"),
    ("LQDA.MI","iShares USD Corp Bond","Obbligazionario",False,False,"USD"),
    ("LQDAH.MI","iShares USD Corp Bond EUR Hedged","Obbligazionario",False,False,"EUR-hedged"),
    ("IHYG.MI","iShares EUR High Yield Corp Bond","Obbligazionario",False,False,"EUR"),
    ("IHYU.MI","iShares USD High Yield Corp Bond","Obbligazionario",False,False,"USD"),
    ("IHYUH.MI","iShares USD HY Corp Bond EUR Hdg","Obbligazionario",False,False,"EUR-hedged"),
    ("BGRN.MI","iShares Green Bond","Obbligazionario",False,False,"EUR"),
    # EM BOND
    ("IEMB.MI","iShares JP Morgan USD EM Bond","Obbligazionario",False,False,"USD"),
    ("SEML.MI","iShares JP Morgan EM Local Govt Bond","Obbligazionario",False,False,"USD"),
    # TARGET MATURITY
    ("IB26.MI","iShares iBonds Dec 2026 EUR Corp","Obbligazionario",False,False,"EUR"),
    ("IB27.MI","iShares iBonds Dec 2027 EUR Corp","Obbligazionario",False,False,"EUR"),
    ("IB28.MI","iShares iBonds Dec 2028 EUR Corp","Obbligazionario",False,False,"EUR"),
    ("IB29.MI","iShares iBonds Dec 2029 EUR Corp","Obbligazionario",False,False,"EUR"),
    ("IB30.MI","iShares iBonds Dec 2030 EUR Corp","Obbligazionario",False,False,"EUR"),
    ("IBG6.MI","iShares iBonds Dec 2026 EUR Govt","Obbligazionario",False,False,"EUR"),
    ("IBG7.MI","iShares iBonds Dec 2027 EUR Govt","Obbligazionario",False,False,"EUR"),
    ("IBG8.MI","iShares iBonds Dec 2028 EUR Govt","Obbligazionario",False,False,"EUR"),
    # MONETARI
    ("XEON.MI","Xtrackers EUR Overnight Rate Swap","Monetario",False,False,"EUR"),
    ("CSH2.MI","iShares EUR Ultrashort Bond","Monetario",False,False,"EUR"),
    ("SMART.MI","Amundi EUR Floating Rate Bond","Monetario",False,False,"EUR"),
    ("ERNE.DE","iShares EUR Money Market","Monetario",False,False,"EUR"),
    ("PRAT.MI","Amundi Prime Euro Govies 0-1Y","Monetario",False,False,"EUR"),
]

# ── LOGGING ────────────────────────────────────────────────────────────────────
def setup_logging():
    Path("logs").mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(CONFIG["log_file"], encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

log = logging.getLogger(__name__)

# ── YAHOO FINANCE FETCH ────────────────────────────────────────────────────────
def fetch_yahoo(ticker: str) -> dict | None:
    """
    Scarica OHLCV giornaliero da Yahoo Finance.
    Ritorna dict con liste: dates, opens, highs, lows, closes, volumes
    oppure None se fallisce.
    """
    end   = datetime.utcnow()
    start = end - timedelta(days=CONFIG["history_days"])
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        f"?interval=1d&period1={int(start.timestamp())}&period2={int(end.timestamp())}"
        f"&events=history&includeAdjustedClose=true"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }
    for attempt in range(1, CONFIG["max_retries"] + 1):
        try:
            r = requests.get(url, headers=headers, timeout=15)
            r.raise_for_status()
            data = r.json()
            result = data.get("chart", {}).get("result")
            if not result:
                log.warning(f"{ticker}: nessun dato Yahoo (attempt {attempt})")
                time.sleep(CONFIG["retry_delay"])
                continue
            ts        = result[0]["timestamp"]
            indicators = result[0]["indicators"]
            q         = indicators["quote"][0]
            adj_close  = indicators.get("adjclose", [{}])[0].get("adjclose", q["close"])
            dates  = [datetime.utcfromtimestamp(t).strftime("%Y-%m-%d") for t in ts]
            closes = [float(v) if v is not None else None for v in adj_close]
            opens  = [float(v) if v is not None else None for v in q["open"]]
            highs  = [float(v) if v is not None else None for v in q["high"]]
            lows   = [float(v) if v is not None else None for v in q["low"]]
            vols   = [int(v)   if v is not None else 0    for v in q["volume"]]
            # Rimuovi barre con close None
            valid = [(d,o,h,l,c,v) for d,o,h,l,c,v in
                     zip(dates,opens,highs,lows,closes,vols) if c is not None]
            if len(valid) < CONFIG["min_candles"]:
                log.warning(f"{ticker}: solo {len(valid)} candele valide")
                return None
            d,o,h,l,c,v = zip(*valid)
            return {
                "dates":  list(d), "opens": list(o), "highs": list(h),
                "lows":   list(l), "closes": list(c), "volumes": list(v)
            }
        except Exception as e:
            log.warning(f"{ticker}: attempt {attempt} fallito — {e}")
            time.sleep(CONFIG["retry_delay"] * attempt)
    return None

# ── CACHE ──────────────────────────────────────────────────────────────────────
def cache_path(ticker: str) -> Path:
    safe = ticker.replace(".", "_")
    return Path(CONFIG["cache_dir"]) / f"{safe}.json"

def load_cache(ticker: str) -> dict | None:
    p = cache_path(ticker)
    if not p.exists():
        return None
    try:
        with open(p) as f:
            cached = json.load(f)
        ts = datetime.fromisoformat(cached.get("cached_at","2000-01-01"))
        age_h = (datetime.utcnow() - ts).total_seconds() / 3600
        if age_h < CONFIG["cache_max_age_hours"]:
            return cached.get("ohlcv")
    except Exception:
        pass
    return None

def save_cache(ticker: str, ohlcv: dict):
    p = cache_path(ticker)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w") as f:
        json.dump({"cached_at": datetime.utcnow().isoformat(), "ohlcv": ohlcv}, f)

def get_ohlcv(ticker: str) -> dict | None:
    cached = load_cache(ticker)
    if cached:
        log.info(f"{ticker}: da cache")
        return cached
    ohlcv = fetch_yahoo(ticker)
    if ohlcv:
        save_cache(ticker, ohlcv)
    return ohlcv

# ── INDICATORI ─────────────────────────────────────────────────────────────────
def safe_arr(lst):
    return np.array(lst, dtype=float)

def calc_er(closes: np.ndarray, period: int = 10) -> np.ndarray:
    """Efficiency Ratio di Kaufman."""
    er = np.full(len(closes), np.nan)
    for i in range(period, len(closes)):
        direction = abs(closes[i] - closes[i - period])
        volatility = np.sum(np.abs(np.diff(closes[i - period:i + 1])))
        er[i] = direction / volatility if volatility != 0 else 0.0
    return er

def calc_kama(closes: np.ndarray, n=10, fast=2, slow=30) -> np.ndarray:
    """Kaufman Adaptive Moving Average."""
    er     = calc_er(closes, n)
    fast_sc = 2.0 / (fast + 1)
    slow_sc = 2.0 / (slow + 1)
    kama = np.full(len(closes), np.nan)
    kama[n] = closes[n]
    for i in range(n + 1, len(closes)):
        if not np.isnan(er[i]):
            sc = (er[i] * (fast_sc - slow_sc) + slow_sc) ** 2
            kama[i] = kama[i-1] + sc * (closes[i] - kama[i-1])
        else:
            kama[i] = kama[i-1]
    return kama

def calc_atr(highs, lows, closes, period=14) -> np.ndarray:
    tr = np.maximum(highs[1:] - lows[1:],
         np.maximum(np.abs(highs[1:] - closes[:-1]),
                    np.abs(lows[1:]  - closes[:-1])))
    tr = np.concatenate([[highs[0]-lows[0]], tr])
    atr = np.full(len(closes), np.nan)
    atr[period-1] = np.mean(tr[:period])
    for i in range(period, len(closes)):
        atr[i] = (atr[i-1] * (period-1) + tr[i]) / period
    return atr

def calc_supertrend(highs, lows, closes, period=10, multiplier=3.0):
    """Supertrend. Ritorna (supertrend_line, direction) dove direction=1 bullish -1 bearish."""
    atr = calc_atr(highs, lows, closes, period)
    hl2 = (highs + lows) / 2
    upper = hl2 + multiplier * atr
    lower = hl2 - multiplier * atr
    st    = np.full(len(closes), np.nan)
    direction = np.zeros(len(closes))
    for i in range(1, len(closes)):
        if np.isnan(atr[i]):
            continue
        prev_upper = upper[i-1] if not np.isnan(st[i-1]) else upper[i]
        prev_lower = lower[i-1] if not np.isnan(st[i-1]) else lower[i]
        upper[i] = min(upper[i], prev_upper) if closes[i-1] <= prev_upper else upper[i]
        lower[i] = max(lower[i], prev_lower) if closes[i-1] >= prev_lower else lower[i]
        if np.isnan(st[i-1]):
            direction[i] = 1
        elif st[i-1] == upper[i-1]:
            direction[i] = -1 if closes[i] > upper[i] else -1
        else:
            direction[i] = 1 if closes[i] < lower[i] else 1
        if direction[i] == -1:
            direction[i] = 1 if closes[i] > upper[i] else -1
        else:
            direction[i] = -1 if closes[i] < lower[i] else 1
        st[i] = lower[i] if direction[i] == 1 else upper[i]
    return st, direction

def calc_rsi(closes: np.ndarray, period=14) -> np.ndarray:
    delta = np.diff(closes)
    gain  = np.where(delta > 0, delta, 0.0)
    loss  = np.where(delta < 0, -delta, 0.0)
    rsi   = np.full(len(closes), np.nan)
    if len(gain) < period:
        return rsi
    avg_gain = np.mean(gain[:period])
    avg_loss = np.mean(loss[:period])
    for i in range(period, len(closes)-1):
        avg_gain = (avg_gain * (period-1) + gain[i]) / period
        avg_loss = (avg_loss * (period-1) + loss[i]) / period
        rs = avg_gain / avg_loss if avg_loss != 0 else 100.0
        rsi[i+1] = 100 - (100 / (1 + rs))
    return rsi

def calc_adx(highs, lows, closes, period=14) -> np.ndarray:
    atr  = calc_atr(highs, lows, closes, period)
    up   = highs[1:] - highs[:-1]
    down = lows[:-1] - lows[1:]
    pdm  = np.where((up > down) & (up > 0), up, 0.0)
    ndm  = np.where((down > up) & (down > 0), down, 0.0)
    adx  = np.full(len(closes), np.nan)
    if len(closes) < period * 2:
        return adx
    pdm14 = np.mean(pdm[:period])
    ndm14 = np.mean(ndm[:period])
    atr14 = atr[period]
    pdi_arr, ndi_arr, dx_arr = [], [], []
    for i in range(period, len(closes)-1):
        pdm14 = pdm14 - pdm14/period + pdm[i]
        ndm14 = ndm14 - ndm14/period + ndm[i]
        atr14 = atr[i+1]
        pdi = 100 * pdm14 / atr14 if atr14 != 0 else 0
        ndi = 100 * ndm14 / atr14 if atr14 != 0 else 0
        dx  = 100 * abs(pdi - ndi) / (pdi + ndi) if (pdi + ndi) != 0 else 0
        pdi_arr.append(pdi); ndi_arr.append(ndi); dx_arr.append(dx)
    if len(dx_arr) >= period:
        adx_val = np.mean(dx_arr[:period])
        for i in range(period, len(dx_arr)):
            adx_val = (adx_val * (period-1) + dx_arr[i]) / period
        adx[period*2] = adx_val
        idx = period*2 + 1
        for i in range(period, len(dx_arr)):
            adx_val = (adx_val * (period-1) + dx_arr[i]) / period
            if idx < len(adx):
                adx[idx] = adx_val
            idx += 1
    return adx

def calc_bollinger(closes: np.ndarray, period=20, n_std=2.0):
    mid   = np.full(len(closes), np.nan)
    upper = np.full(len(closes), np.nan)
    lower = np.full(len(closes), np.nan)
    width = np.full(len(closes), np.nan)
    for i in range(period-1, len(closes)):
        window = closes[i-period+1:i+1]
        m = np.mean(window)
        s = np.std(window, ddof=1)
        mid[i]   = m
        upper[i] = m + n_std * s
        lower[i] = m - n_std * s
        width[i] = (upper[i] - lower[i]) / m if m != 0 else 0
    return mid, upper, lower, width

def calc_sar(highs, lows, af_step=0.02, af_max=0.2):
    """Parabolic SAR."""
    sar = np.full(len(highs), np.nan)
    if len(highs) < 2:
        return sar
    bull   = True
    af     = af_step
    ep     = highs[0]
    sar[0] = lows[0]
    for i in range(1, len(highs)):
        prev_sar = sar[i-1]
        if bull:
            sar[i] = prev_sar + af * (ep - prev_sar)
            sar[i] = min(sar[i], lows[i-1], lows[i-2] if i > 1 else lows[i-1])
            if lows[i] < sar[i]:
                bull = False; af = af_step; ep = lows[i]; sar[i] = ep
            else:
                if highs[i] > ep:
                    ep = highs[i]; af = min(af + af_step, af_max)
        else:
            sar[i] = prev_sar + af * (ep - prev_sar)
            sar[i] = max(sar[i], highs[i-1], highs[i-2] if i > 1 else highs[i-1])
            if highs[i] > sar[i]:
                bull = True; af = af_step; ep = highs[i]; sar[i] = ep
            else:
                if lows[i] < ep:
                    ep = lows[i]; af = min(af + af_step, af_max)
    return sar

def calc_obv(closes, volumes) -> np.ndarray:
    obv = np.zeros(len(closes))
    for i in range(1, len(closes)):
        if closes[i] > closes[i-1]:
            obv[i] = obv[i-1] + volumes[i]
        elif closes[i] < closes[i-1]:
            obv[i] = obv[i-1] - volumes[i]
        else:
            obv[i] = obv[i-1]
    return obv

def calc_ema(closes: np.ndarray, period: int) -> np.ndarray:
    ema = np.full(len(closes), np.nan)
    k   = 2.0 / (period + 1)
    start = period - 1
    if start >= len(closes):
        return ema
    ema[start] = np.mean(closes[:period])
    for i in range(start+1, len(closes)):
        ema[i] = closes[i] * k + ema[i-1] * (1-k)
    return ema

def calc_sma(closes: np.ndarray, period: int) -> np.ndarray:
    sma = np.full(len(closes), np.nan)
    for i in range(period-1, len(closes)):
        sma[i] = np.mean(closes[i-period+1:i+1])
    return sma

def calc_volume_ratio(volumes: np.ndarray, period=20) -> np.ndarray:
    vr = np.full(len(volumes), np.nan)
    for i in range(period, len(volumes)):
        avg = np.mean(volumes[i-period:i])
        vr[i] = volumes[i] / avg if avg != 0 else 1.0
    return vr

def momentum_return(closes: np.ndarray, bars: int) -> float:
    """Rendimento percentuale rolling su N barre."""
    if len(closes) <= bars or closes[-bars-1] == 0:
        return 0.0
    return (closes[-1] / closes[-bars-1] - 1) * 100

def safe_last(arr: np.ndarray, default=None):
    """Ultimo valore non-nan di un array."""
    valid = arr[~np.isnan(arr)]
    return float(valid[-1]) if len(valid) > 0 else default

def find_signal_date(kama_fast, kama_slow, dates) -> str | None:
    """Trova la data dell'ultimo KAMA cross."""
    for i in range(len(kama_fast)-1, 0, -1):
        if np.isnan(kama_fast[i]) or np.isnan(kama_slow[i]):
            continue
        if np.isnan(kama_fast[i-1]) or np.isnan(kama_slow[i-1]):
            continue
        curr_diff = kama_fast[i] - kama_slow[i]
        prev_diff = kama_fast[i-1] - kama_slow[i-1]
        if curr_diff * prev_diff < 0:  # cambio segno = cross
            return dates[i]
    return None

def days_since(date_str: str) -> int | None:
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return (datetime.utcnow() - d).days
    except Exception:
        return None

# ── CLASSIFICAZIONE SEGNALE ────────────────────────────────────────────────────
def classify_signal(
    er_val, er_series, kama_fast_val, kama_slow_val,
    bb_width_series, vol_ratio_val, is_leveraged,
    supertrend_dir
) -> str:
    """
    Ritorna: BUY | SELL | WATCH | RANGING
    """
    er_thresh   = CONFIG["er_threshold_leva"]   if is_leveraged else CONFIG["er_threshold_normal"]
    er_trending = CONFIG["er_trending_leva"]    if is_leveraged else CONFIG["er_trending_normal"]

    # ER in salita (ultime N barre)
    n = CONFIG["er_watch_rising_bars"]
    er_valid = er_series[~np.isnan(er_series)]
    er_rising = len(er_valid) >= n and all(
        er_valid[-n+i] < er_valid[-n+i+1] for i in range(n-1)
    )

    # BB compressione
    bw_valid = bb_width_series[~np.isnan(bb_width_series)]
    if len(bw_valid) >= 20:
        bw_mean = np.mean(bw_valid[-20:])
        bb_compressed = bw_valid[-1] < bw_mean * (1 - CONFIG["bb_compression_pct"])
    else:
        bb_compressed = False

    kama_bull = kama_fast_val > kama_slow_val if (kama_fast_val and kama_slow_val) else False
    st_bull   = supertrend_dir == 1

    if er_val is None or er_val < er_thresh:
        # RANGING — ma controlla WATCH
        if er_rising and (bb_compressed or vol_ratio_val and vol_ratio_val > CONFIG["volume_ratio_watch"]):
            return "WATCH"
        return "RANGING"

    if er_val >= er_trending:
        return "BUY" if (kama_bull and st_bull) else "SELL"

    # ER nella zona grigia (thresh < er < trending)
    if kama_bull and st_bull:
        return "BUY"
    elif not kama_bull and not st_bull:
        return "SELL"
    else:
        return "WATCH"

# ── SCORE COMPOSITO ────────────────────────────────────────────────────────────
def compute_score(er_val, kama_bull, st_bull, mom3m, adx_val, rsi_val, sar_bull) -> int:
    w = CONFIG["score_weights"]
    score = 0.0

    # ER (0-1) → 0-100
    er_score = min(1.0, (er_val or 0) / 0.5) * 100
    score += w["er"] * er_score

    # Supertrend
    score += w["supertrend"] * (100 if st_bull else 0)

    # KAMA
    score += w["kama"] * (100 if kama_bull else 0)

    # Momentum 3m (normalizzato -30%/+30% → 0/100)
    mom_score = max(0, min(100, (mom3m + 30) / 60 * 100))
    score += w["momentum3m"] * mom_score

    # ADX (0-60 → 0-100)
    adx_score = min(100, (adx_val or 0) / 60 * 100)
    score += w["adx"] * adx_score

    # RSI (distanza da 50, cap a 30 punti)
    rsi_dist = abs((rsi_val or 50) - 50)
    rsi_score = min(100, rsi_dist / 30 * 100)
    score += w["rsi"] * rsi_score

    # SAR
    score += w["sar"] * (100 if sar_bull else 0)

    return round(score)

# ── PROCESSO SINGOLO ETF ───────────────────────────────────────────────────────
def process_etf(etf_tuple: tuple) -> dict | None:
    ticker, nome, categoria, is_lev, is_short, currency = etf_tuple
    log.info(f"Processing {ticker} ...")

    ohlcv = get_ohlcv(ticker)
    if not ohlcv:
        log.error(f"{ticker}: SKIP — dati non disponibili")
        return None

    closes  = safe_arr(ohlcv["closes"])
    highs   = safe_arr(ohlcv["highs"])
    lows    = safe_arr(ohlcv["lows"])
    volumes = safe_arr(ohlcv["volumes"])
    dates   = ohlcv["dates"]

    try:
        # ── Indicatori ──────────────────────────────────────────────────────
        kama_fast = calc_kama(closes, n=10, fast=5,  slow=20)
        kama_slow = calc_kama(closes, n=10, fast=2,  slow=30)
        er_series = calc_er(closes, 10)
        ema20     = calc_ema(closes, 20)
        ema50     = calc_ema(closes, 50)
        sma200    = calc_sma(closes, 200)
        rsi       = calc_rsi(closes, 14)
        adx       = calc_adx(highs, lows, closes, 14)
        atr       = calc_atr(highs, lows, closes, 14)
        st_line, st_dir = calc_supertrend(
            highs, lows, closes,
            CONFIG["supertrend_atr_period"],
            CONFIG["supertrend_multiplier"]
        )
        bb_mid, bb_upper, bb_lower, bb_width = calc_bollinger(
            closes, CONFIG["bb_period"], CONFIG["bb_std"]
        )
        sar    = calc_sar(highs, lows)
        obv    = calc_obv(closes, volumes)
        vol_ratio = calc_volume_ratio(volumes, CONFIG["volume_ratio_period"])

        mom1m  = momentum_return(closes, 21)
        mom3m  = momentum_return(closes, 63)
        mom6m  = momentum_return(closes, 126)

        # ── Valori correnti ─────────────────────────────────────────────────
        last_close    = float(closes[-1])
        last_er       = safe_last(er_series)
        last_kf       = safe_last(kama_fast)
        last_ks       = safe_last(kama_slow)
        last_rsi      = safe_last(rsi)
        last_adx      = safe_last(adx)
        last_atr      = safe_last(atr)
        last_vr       = safe_last(vol_ratio)
        last_st_dir   = int(st_dir[-1])
        last_sar      = safe_last(sar)
        last_ema20    = safe_last(ema20)
        last_ema50    = safe_last(ema50)
        last_sma200   = safe_last(sma200)
        last_bb_width = safe_last(bb_width)
        last_bb_upper = safe_last(bb_upper)
        last_bb_lower = safe_last(bb_lower)
        last_bb_mid   = safe_last(bb_mid)
        last_obv      = safe_last(obv)

        kama_bull = last_kf > last_ks if (last_kf and last_ks) else False
        sar_bull  = last_close > last_sar if last_sar else False
        above_sma200 = last_close > last_sma200 if last_sma200 else None

        # ── Variazione % giornaliera ─────────────────────────────────────────
        prev_close = float(closes[-2]) if len(closes) >= 2 else last_close
        chg_pct    = round((last_close / prev_close - 1) * 100, 2) if prev_close else 0

        # ── Classificazione segnale ─────────────────────────────────────────
        signal = classify_signal(
            last_er, er_series, last_kf, last_ks,
            bb_width, last_vr, is_lev, last_st_dir
        )

        # ── Data segnale ────────────────────────────────────────────────────
        signal_date  = find_signal_date(kama_fast, kama_slow, dates)
        signal_days  = days_since(signal_date)

        # ── Score ────────────────────────────────────────────────────────────
        score = compute_score(
            last_er, kama_bull, last_st_dir == 1,
            mom3m, last_adx, last_rsi, sar_bull
        )

        # ── KAMA state label (compatibile RAPTOR) ────────────────────────────
        er_thresh = CONFIG["er_threshold_leva"] if is_lev else CONFIG["er_threshold_normal"]
        er_trend  = CONFIG["er_trending_leva"]  if is_lev else CONFIG["er_trending_normal"]
        if last_er is None:
            kama_state = "UNKNOWN"
        elif last_er < er_thresh:
            kama_state = "RANGING"
        elif last_er < er_trend:
            kama_state = "BOTTOMING" if kama_bull else "TIRED"
        else:
            kama_state = "FRESH" if kama_bull else "EXTENDED"

        # ── Candlestick data per grafico (ultimi 90gg) ────────────────────────
        n90 = min(90, len(dates))
        chart = {
            "dates":      dates[-n90:],
            "opens":      [round(v,4) for v in ohlcv["opens"][-n90:]],
            "highs":      [round(v,4) for v in ohlcv["highs"][-n90:]],
            "lows":       [round(v,4) for v in ohlcv["lows"][-n90:]],
            "closes":     [round(v,4) for v in closes[-n90:].tolist()],
            "volumes":    ohlcv["volumes"][-n90:],
            "kama_fast":  [round(v,4) if not math.isnan(v) else None for v in kama_fast[-n90:].tolist()],
            "kama_slow":  [round(v,4) if not math.isnan(v) else None for v in kama_slow[-n90:].tolist()],
            "bb_upper":   [round(v,4) if not math.isnan(v) else None for v in bb_upper[-n90:].tolist()],
            "bb_mid":     [round(v,4) if not math.isnan(v) else None for v in bb_mid[-n90:].tolist()],
            "bb_lower":   [round(v,4) if not math.isnan(v) else None for v in bb_lower[-n90:].tolist()],
            "sar":        [round(v,4) if not math.isnan(v) else None for v in sar[-n90:].tolist()],
            "supertrend": [round(v,4) if not math.isnan(v) else None for v in st_line[-n90:].tolist()],
            "st_dir":     [int(d) for d in st_dir[-n90:].tolist()],
            "er":         [round(v,4) if not math.isnan(v) else None for v in er_series[-n90:].tolist()],
            "rsi":        [round(v,2) if not math.isnan(v) else None for v in rsi[-n90:].tolist()],
            "obv":        [round(v,0) if not math.isnan(v) else None for v in obv[-n90:].tolist()],
            "vol_ratio":  [round(v,2) if not math.isnan(v) else None for v in vol_ratio[-n90:].tolist()],
        }

        # Segnali BUY/SELL per frecce sul grafico
        buy_signals  = []
        sell_signals = []
        for i in range(1, n90):
            idx = len(dates) - n90 + i
            if idx < 1 or np.isnan(kama_fast[idx]) or np.isnan(kama_slow[idx]):
                continue
            if np.isnan(kama_fast[idx-1]) or np.isnan(kama_slow[idx-1]):
                continue
            curr = kama_fast[idx] - kama_slow[idx]
            prev = kama_fast[idx-1] - kama_slow[idx-1]
            if prev <= 0 and curr > 0:
                buy_signals.append({"date": dates[idx], "price": round(float(closes[idx]),4)})
            elif prev >= 0 and curr < 0:
                sell_signals.append({"date": dates[idx], "price": round(float(closes[idx]),4)})

        chart["buy_signals"]  = buy_signals
        chart["sell_signals"] = sell_signals

        return {
            # Identificativo
            "ticker":        ticker,
            "nome":          nome,
            "categoria":     categoria,
            "is_leveraged":  is_lev,
            "is_short":      is_short,
            "currency":      currency,
            # Prezzi
            "price":         round(last_close, 4),
            "chg_pct":       chg_pct,
            "atr":           round(last_atr, 4) if last_atr else None,
            # Segnale
            "signal":        signal,
            "signal_date":   signal_date,
            "signal_days":   signal_days,
            "score":         score,
            "kama_state":    kama_state,
            # Indicatori chiave (tabella)
            "er":            round(last_er, 4) if last_er else None,
            "rsi":           round(last_rsi, 1) if last_rsi else None,
            "adx":           round(last_adx, 1) if last_adx else None,
            "mom1m":         round(mom1m, 2),
            "mom3m":         round(mom3m, 2),
            "mom6m":         round(mom6m, 2),
            "vol_ratio":     round(last_vr, 2) if last_vr else None,
            "bb_width":      round(last_bb_width, 4) if last_bb_width else None,
            "kama_fast":     round(last_kf, 4) if last_kf else None,
            "kama_slow":     round(last_ks, 4) if last_ks else None,
            "kama_bull":     kama_bull,
            "supertrend_dir":last_st_dir,
            "sar_bull":      sar_bull,
            "ema20":         round(last_ema20, 4) if last_ema20 else None,
            "ema50":         round(last_ema50, 4) if last_ema50 else None,
            "sma200":        round(last_sma200, 4) if last_sma200 else None,
            "above_sma200":  above_sma200,
            "bb_upper":      round(last_bb_upper, 4) if last_bb_upper else None,
            "bb_lower":      round(last_bb_lower, 4) if last_bb_lower else None,
            # Dati grafico
            "chart":         chart,
        }

    except Exception as e:
        log.error(f"{ticker}: errore calcolo — {e}\n{traceback.format_exc()}")
        return None

# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    setup_logging()
    Path("data").mkdir(exist_ok=True)
    Path(CONFIG["cache_dir"]).mkdir(parents=True, exist_ok=True)
    Path("logs").mkdir(exist_ok=True)

    run_start = datetime.utcnow()
    log.info(f"=== RAPTOR WEALTH MERIDIAN — Scanner Engine START {run_start.isoformat()} ===")
    log.info(f"Universe: {len(ETF_UNIVERSE)} ETF")

    results   = []
    errors    = []
    skipped   = []

    for i, etf in enumerate(ETF_UNIVERSE):
        ticker = etf[0]
        try:
            result = process_etf(etf)
            if result:
                results.append(result)
            else:
                skipped.append(ticker)
        except Exception as e:
            log.error(f"{ticker}: eccezione non gestita — {e}")
            errors.append(ticker)
        # Throttling tra requests
        if i < len(ETF_UNIVERSE) - 1:
            time.sleep(CONFIG["request_delay"])

    # ── Output JSON ────────────────────────────────────────────────────────────
    output = {
        "meta": {
            "generated_at":  datetime.utcnow().isoformat() + "Z",
            "total":         len(results),
            "errors":        errors,
            "skipped":       skipped,
            "universe_size": len(ETF_UNIVERSE),
            "run_duration_s": round((datetime.utcnow() - run_start).total_seconds(), 1),
            "config": {
                "er_threshold_leva":   CONFIG["er_threshold_leva"],
                "er_trending_leva":    CONFIG["er_trending_leva"],
                "er_threshold_normal": CONFIG["er_threshold_normal"],
                "er_trending_normal":  CONFIG["er_trending_normal"],
                "score_weights":       CONFIG["score_weights"],
            }
        },
        "signals": results
    }

    output_path = Path(CONFIG["output_file"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",",":"))

    # ── Summary ────────────────────────────────────────────────────────────────
    counts = {"BUY":0,"SELL":0,"WATCH":0,"RANGING":0}
    for r in results:
        counts[r["signal"]] = counts.get(r["signal"], 0) + 1

    log.info("=== RUN COMPLETATO ===")
    log.info(f"Successo: {len(results)} / {len(ETF_UNIVERSE)}")
    log.info(f"Errori: {len(errors)} {errors}")
    log.info(f"Saltati: {len(skipped)} {skipped}")
    log.info(f"Segnali — BUY:{counts['BUY']} SELL:{counts['SELL']} WATCH:{counts['WATCH']} RANGING:{counts['RANGING']}")
    log.info(f"Output: {output_path} ({output_path.stat().st_size/1024:.1f} KB)")
    log.info(f"Durata: {(datetime.utcnow()-run_start).total_seconds():.1f}s")

if __name__ == "__main__":
    main()
