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
    "history_days": 120,
    "min_candles": 60,
    "request_delay": 0.08,
    "max_retries": 2,
    "retry_delay": 1.0,
    "cache_dir": "data/cache",
    "output_file": "data/signals.json",
    "log_file": "logs/run.log",
    "cache_max_age_hours": 5,
    "er_threshold_leva": 0.25,
    "er_trending_leva": 0.30,
    "er_threshold_normal": 0.20,
    "er_trending_normal": 0.25,
    "er_watch_rising_bars": 3,
    "supertrend_atr_period": 10,
    "supertrend_multiplier": 3.0,
    "bb_period": 20,
    "bb_std": 2.0,
    "bb_compression_pct": 0.15,
    "volume_ratio_period": 20,
    "volume_ratio_watch": 1.3,
    "score_weights": {
        "er": 0.25,
        "supertrend": 0.20,
        "kama": 0.20,
        "momentum3m": 0.15,
        "adx": 0.10,
        "rsi": 0.05,
        "sar": 0.05,
    },
}

# ── ETF UNIVERSE (370 ETF da RWM_ETF_Universe.xlsx) ───────────────────────────
ETF_UNIVERSE = [
    # LEVA LONG
    ("QQQ3.MI","WisdomTree Nasdaq 100 3x Daily","Leva Long",True,False,"USD"),
    ("3USL.MI","WisdomTree S&P500 3x Daily","Leva Long",True,False,"USD"),
    ("LQQ.MI","Leverage Shares 3x Nasdaq","Leva Long",True,False,"USD"),
    ("3DAL.MI","WisdomTree DAX 3x Daily","Leva Long",True,False,"EUR"),
    ("DX3L.MI","WisdomTree DAX 3x Long","Leva Long",True,False,"EUR"),
    ("EUL.MI","Leverage Shares 3x Euro Stoxx 50","Leva Long",True,False,"EUR"),
    ("3SML.MI","WisdomTree S&P MidCap 3x","Leva Long",True,False,"USD"),
    ("3JPL.MI","WisdomTree Nikkei 3x","Leva Long",True,False,"JPY"),
    ("2BUL.MI","WisdomTree S&P500 2x Daily","Leva Long",True,False,"USD"),
    ("2QQL.MI","WisdomTree Nasdaq 2x Daily","Leva Long",True,False,"USD"),
    ("UCO2.MI","Leverage Shares 2x Crude Oil","Leva Long",True,False,"USD"),
    ("3LTS.MI","GraniteShares 3x Tesla","Leva Long",True,False,"USD"),
    ("3LAP.MI","GraniteShares 3x Apple","Leva Long",True,False,"USD"),
    ("3LNV.MI","GraniteShares 3x Nvidia","Leva Long",True,False,"USD"),
    ("3LAM.MI","GraniteShares 3x Amazon","Leva Long",True,False,"USD"),
    ("3LMS.MI","GraniteShares 3x Microsoft","Leva Long",True,False,"USD"),
    ("3LGO.MI","GraniteShares 3x Alphabet","Leva Long",True,False,"USD"),
    ("3LMT.MI","GraniteShares 3x Meta","Leva Long",True,False,"USD"),
    ("3LNF.MI","GraniteShares 3x Netflix","Leva Long",True,False,"USD"),
    ("3LCO.MI","GraniteShares 3x Coinbase","Leva Long",True,False,"USD"),
    ("3LBA.L","GraniteShares 3x BAC","Leva Long",True,False,"USD"),
    ("3LJP.L","GraniteShares 3x JPMorgan","Leva Long",True,False,"USD"),
    ("3LAD.L","GraniteShares 3x AMD","Leva Long",True,False,"USD"),
    ("TSL2.L","Leverage Shares 2x Tesla","Leva Long",True,False,"USD"),
    ("NVD2.L","Leverage Shares 2x Nvidia","Leva Long",True,False,"USD"),
    ("AMZ2.L","Leverage Shares 2x Amazon","Leva Long",True,False,"USD"),
    ("AAP2.L","Leverage Shares 2x Apple","Leva Long",True,False,"USD"),
    ("GOG2.L","Leverage Shares 2x Alphabet","Leva Long",True,False,"USD"),
    ("MET2.L","Leverage Shares 2x Meta","Leva Long",True,False,"USD"),
    ("MSF2.L","Leverage Shares 2x Microsoft","Leva Long",True,False,"USD"),
    ("INTC2.L","Leverage Shares 2x Intel","Leva Long",True,False,"USD"),
    ("BABA2.L","Leverage Shares 2x Alibaba","Leva Long",True,False,"USD"),
    ("PFE2.L","Leverage Shares 2x Pfizer","Leva Long",True,False,"USD"),
    ("COIN2.L","Leverage Shares 2x Coinbase","Leva Long",True,False,"USD"),
    ("MSTR2.L","Leverage Shares 2x MicroStrategy","Leva Long",True,False,"USD"),
    ("PLTR2.L","Leverage Shares 2x Palantir","Leva Long",True,False,"USD"),
    ("RIVN2.L","Leverage Shares 2x Rivian","Leva Long",True,False,"USD"),
    ("SNOW2.L","Leverage Shares 2x Snowflake","Leva Long",True,False,"USD"),
    ("3GOL.MI","WisdomTree Gold 3x","Leva Long",True,False,"USD"),
    ("3SIL.MI","WisdomTree Silver 3x","Leva Long",True,False,"USD"),
    ("3OIL.MI","WisdomTree Crude Oil 3x","Leva Long",True,False,"USD"),
    ("2GOL.MI","WisdomTree Gold 2x","Leva Long",True,False,"USD"),
    ("LNGE.MI","WisdomTree Natural Gas 2x","Leva Long",True,False,"USD"),
    ("3LFI.L","GraniteShares 3x Financials","Leva Long",True,False,"USD"),
    ("3LEN.L","GraniteShares 3x Energy","Leva Long",True,False,"USD"),
    ("3LHC.L","GraniteShares 3x Healthcare","Leva Long",True,False,"USD"),
    ("TECL.MI","Leverage Shares 3x Technology","Leva Long",True,False,"USD"),
    # SHORT
    ("XSPS.MI","Xtrackers S&P500 Short","Short",False,True,"USD"),
    ("XSND.MI","Xtrackers Nasdaq Short","Short",False,True,"USD"),
    ("XSDX.MI","Xtrackers DAX Short","Short",False,True,"EUR"),
    ("SDS.MI","WisdomTree S&P500 2x Short","Short",True,True,"USD"),
    ("QQQ3S.MI","WisdomTree Nasdaq 3x Short","Short",True,True,"USD"),
    ("DX3S.MI","WisdomTree DAX 3x Short","Short",True,True,"EUR"),
    ("SUK2.MI","WisdomTree FTSE100 2x Short","Short",True,True,"GBP"),
    ("ESXS.MI","WisdomTree Euro Stoxx 50 2x Short","Short",True,True,"EUR"),
    ("3USS.MI","WisdomTree S&P500 3x Short","Short",True,True,"USD"),
    ("3STS.MI","GraniteShares 3x Short Tesla","Short",True,True,"USD"),
    ("3SAP.MI","GraniteShares 3x Short Apple","Short",True,True,"USD"),
    ("3SNV.MI","GraniteShares 3x Short Nvidia","Short",True,True,"USD"),
    ("3SAM.MI","GraniteShares 3x Short Amazon","Short",True,True,"USD"),
    ("3SMS.MI","GraniteShares 3x Short Microsoft","Short",True,True,"USD"),
    ("3SGO.MI","GraniteShares 3x Short Alphabet","Short",True,True,"USD"),
    ("3SMT.MI","GraniteShares 3x Short Meta","Short",True,True,"USD"),
    ("STSL.L","Leverage Shares 2x Short Tesla","Short",True,True,"USD"),
    ("SNVD.L","Leverage Shares 2x Short Nvidia","Short",True,True,"USD"),
    ("SAMZ.L","Leverage Shares 2x Short Amazon","Short",True,True,"USD"),
    ("SAAP.L","Leverage Shares 2x Short Apple","Short",True,True,"USD"),
    ("SMTA.L","Leverage Shares 2x Short Meta","Short",True,True,"USD"),
    ("SCRU.MI","WisdomTree Crude Oil 1x Short","Short",False,True,"USD"),
    ("SNGA.MI","WisdomTree Natural Gas 1x Short","Short",False,True,"USD"),
    ("SGOL.MI","WisdomTree Gold 1x Short","Short",False,True,"USD"),
    ("SSIL.MI","WisdomTree Silver 1x Short","Short",False,True,"USD"),
    ("3GOL_S.MI","WisdomTree Gold 3x Short","Short",True,True,"USD"),
    # MATERIE PRIME
    ("PHAU.MI","WisdomTree Physical Gold","Materie Prime",False,False,"USD"),
    ("PHAG.MI","WisdomTree Physical Silver","Materie Prime",False,False,"USD"),
    ("PHPT.MI","WisdomTree Physical Platinum","Materie Prime",False,False,"USD"),
    ("PHPD.MI","WisdomTree Physical Palladium","Materie Prime",False,False,"USD"),
    ("SGLD.MI","iShares Physical Gold","Materie Prime",False,False,"USD"),
    ("IGLN.MI","iShares Physical Gold ETC","Materie Prime",False,False,"USD"),
    ("COPA.MI","WisdomTree Copper","Materie Prime",False,False,"USD"),
    ("ALUM.MI","WisdomTree Aluminium","Materie Prime",False,False,"USD"),
    ("NICK.MI","WisdomTree Nickel","Materie Prime",False,False,"USD"),
    ("ZINC.MI","WisdomTree Zinc","Materie Prime",False,False,"USD"),
    ("LITH.MI","WisdomTree Lithium","Materie Prime",False,False,"USD"),
    ("CRUD.MI","WisdomTree Crude Oil","Materie Prime",False,False,"USD"),
    ("BRNT.MI","WisdomTree Brent Crude","Materie Prime",False,False,"USD"),
    ("NGAS.MI","WisdomTree Natural Gas","Materie Prime",False,False,"USD"),
    ("WEAT.MI","WisdomTree Wheat","Materie Prime",False,False,"USD"),
    ("CORN.MI","WisdomTree Corn","Materie Prime",False,False,"USD"),
    ("SOYB.MI","WisdomTree Soybeans","Materie Prime",False,False,"USD"),
    ("COFF.MI","WisdomTree Coffee","Materie Prime",False,False,"USD"),
    ("COTN.MI","WisdomTree Cotton","Materie Prime",False,False,"USD"),
    ("SUGA.MI","WisdomTree Sugar","Materie Prime",False,False,"USD"),
    ("CMOD.MI","WisdomTree Commodity","Materie Prime",False,False,"USD"),
    ("AIGP.MI","iShares Diversified Commodity Swap","Materie Prime",False,False,"USD"),
    ("BCOM.DE","iShares Bloomberg Enhanced Roll Commodity","Materie Prime",False,False,"USD"),
    # TEMATICO
    ("WTAI.MI","WisdomTree AI UCITS ETF","Tematico",False,False,"USD"),
    ("AIAI.DE","L&G AI & Big Data UCITS ETF","Tematico",False,False,"USD"),
    ("XAIX.DE","Xtrackers AI & Big Data UCITS ETF","Tematico",False,False,"USD"),
    ("BOTZ.DE","Global X Robotics & AI UCITS ETF","Tematico",False,False,"USD"),
    ("IRBO.DE","iShares Robotics & AI Multisector","Tematico",False,False,"USD"),
    ("SEMI.DE","iShares Semiconductor UCITS ETF","Tematico",False,False,"USD"),
    ("CHIP.DE","VanEck Semiconductor UCITS ETF","Tematico",False,False,"USD"),
    ("SXRP.DE","SPDR MSCI World Technology","Tematico",False,False,"USD"),
    ("ASML.DE","HANetf ETC Group MSCI Semis","Tematico",False,False,"USD"),
    ("CLOD.DE","WisdomTree Cloud Computing UCITS ETF","Tematico",False,False,"USD"),
    ("WCLD.MI","WisdomTree Cloud Computing","Tematico",False,False,"USD"),
    ("UCYB.MI","Invesco Cybersecurity UCITS ETF","Tematico",False,False,"USD"),
    ("CYBE.DE","L&G Cybersecurity UCITS ETF","Tematico",False,False,"USD"),
    ("HACK.DE","WisdomTree Cybersecurity UCITS ETF","Tematico",False,False,"USD"),
    ("ROBO.MI","iShares Automation & Robotics UCITS ETF","Tematico",False,False,"USD"),
    ("RBOT.DE","Xtrackers Robotics & AI UCITS ETF","Tematico",False,False,"USD"),
    ("AUTM.DE","Amundi Robotics & AI UCITS ETF","Tematico",False,False,"USD"),
    ("BFIV.MI","Amundi 5G UCITS ETF","Tematico",False,False,"USD"),
    ("TPAY.DE","WisdomTree 5G UCITS ETF","Tematico",False,False,"USD"),
    ("YODA.MI","HANetf Future of Space UCITS ETF","Tematico",False,False,"USD"),
    ("ARKX.DE","ARK Space Exploration UCITS ETF","Tematico",False,False,"USD"),
    ("MTVR.DE","Fidelity Metaverse UCITS ETF","Tematico",False,False,"USD"),
    ("GAME.DE","VanEck Video Gaming & Esports","Tematico",False,False,"USD"),
    ("ESPO.MI","VanEck Video Gaming & Esports","Tematico",False,False,"USD"),
    ("FINT.MI","Invesco Financial Innovation UCITS ETF","Tematico",False,False,"USD"),
    ("KOIN.MI","L&G Blockchain UCITS ETF","Tematico",False,False,"USD"),
    ("DAPP.DE","VanEck Blockchain UCITS ETF","Tematico",False,False,"USD"),
    ("BTCE.DE","ETC Group Physical Bitcoin","Tematico",False,False,"USD"),
    ("ETHE.DE","ETC Group Physical Ethereum","Tematico",False,False,"USD"),
    ("VBTC.DE","VanEck Bitcoin ETN","Tematico",False,False,"USD"),
    ("ECAR.MI","iShares Electric Vehicles & Driving Tech","Tematico",False,False,"USD"),
    ("DRIV.DE","Global X Electric Vehicles & Tech","Tematico",False,False,"USD"),
    ("EVPE.DE","Invesco Electric Vehicles UCITS ETF","Tematico",False,False,"USD"),
    ("BATT.MI","Amundi Battery Technology UCITS ETF","Tematico",False,False,"USD"),
    ("CHRG.DE","WisdomTree Battery Solutions UCITS ETF","Tematico",False,False,"USD"),
    ("HTWO.MI","L&G Hydrogen Economy UCITS ETF","Tematico",False,False,"USD"),
    ("HGEN.DE","VanEck Hydrogen Economy UCITS ETF","Tematico",False,False,"USD"),
    ("HYGN.DE","Amundi Hydrogen UCITS ETF","Tematico",False,False,"USD"),
    ("ISUN.MI","Invesco Solar Energy UCITS ETF","Tematico",False,False,"USD"),
    ("RENW.MI","HANetf Solar Energy UCITS ETF","Tematico",False,False,"USD"),
    ("WNDS.DE","WisdomTree Wind UCITS ETF","Tematico",False,False,"EUR"),
    ("INRG.MI","iShares Global Clean Energy UCITS ETF","Tematico",False,False,"USD"),
    ("IGEN.MI","L&G Clean Energy UCITS ETF","Tematico",False,False,"USD"),
    ("WTEW.DE","WisdomTree Energy Transition UCITS ETF","Tematico",False,False,"EUR"),
    ("ENRG.DE","Amundi MSCI New Energy ESG Screened","Tematico",False,False,"USD"),
    ("NUKL.DE","VanEck Uranium and Nuclear Technologies","Tematico",False,False,"USD"),
    ("URAN.DE","Global X Uranium UCITS ETF","Tematico",False,False,"USD"),
    ("AURA.MI","HANetf Sprott Uranium Miners","Tematico",False,False,"USD"),
    ("IQQW.MI","iShares Global Water UCITS ETF","Tematico",False,False,"USD"),
    ("WATL.MI","Invesco Water Resources UCITS ETF","Tematico",False,False,"USD"),
    ("GLUG.MI","L&G Clean Water UCITS ETF","Tematico",False,False,"USD"),
    ("FOOD.MI","Rize Sustainable Future of Food","Tematico",False,False,"USD"),
    ("AGRI.DE","iShares MSCI Agriculture Producers","Tematico",False,False,"USD"),
    ("WOOD.DE","iShares Global Timber & Forestry","Tematico",False,False,"USD"),
    ("IQQH.MI","iShares Healthcare Innovation UCITS ETF","Tematico",False,False,"USD"),
    ("SBIO.MI","SPDR S&P Biotech UCITS ETF","Tematico",False,False,"USD"),
    ("DNA.MI","L&G Pharma Breakthrough UCITS ETF","Tematico",False,False,"USD"),
    ("GNOM.DE","iShares Genomics Immunology Healthcare","Tematico",False,False,"USD"),
    ("HEAL.DE","iShares MSCI World Health Care Sector","Tematico",False,False,"USD"),
    ("AGES.MI","iShares Ageing Population UCITS ETF","Tematico",False,False,"USD"),
    ("BOLD.DE","L&G Global Ageing UCITS ETF","Tematico",False,False,"USD"),
    ("DFEN.MI","HANetf Future of Defence UCITS ETF","Tematico",False,False,"USD"),
    ("NATO.DE","VanEck Defence UCITS ETF","Tematico",False,False,"USD"),
    ("SHLD.DE","WisdomTree Defence UCITS ETF","Tematico",False,False,"USD"),
    ("AERO.DE","Invesco Aerospace & Defence UCITS ETF","Tematico",False,False,"USD"),
    ("INFR.MI","iShares Global Infrastructure UCITS ETF","Tematico",False,False,"USD"),
    ("XDGI.DE","Xtrackers Global Infrastructure UCITS ETF","Tematico",False,False,"USD"),
    ("IQQP.MI","iShares Global REIT UCITS ETF","Tematico",False,False,"USD"),
    ("XREA.DE","Xtrackers FTSE EPRA REIT Developed","Tematico",False,False,"EUR"),
    ("EPRE.DE","Amundi FTSE EPRA Europe Real Estate","Tematico",False,False,"EUR"),
    ("SUSW.MI","iShares MSCI World ESG Screened","Tematico",False,False,"USD"),
    ("SUEW.DE","Xtrackers MSCI World ESG","Tematico",False,False,"USD"),
    ("SPRT.DE","Amundi Sports & Entertainment","Tematico",False,False,"USD"),
    # SETTORIALE MONDO
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
    ("STWM.DE","SPDR MSCI World Materials","Settoriale Mondo",False,False,"USD"),
    ("STWI.DE","SPDR MSCI World Industrials","Settoriale Mondo",False,False,"USD"),
    ("STWD.DE","SPDR MSCI World Cons Discret","Settoriale Mondo",False,False,"USD"),
    ("STWS.DE","SPDR MSCI World Cons Staples","Settoriale Mondo",False,False,"USD"),
    ("STWR.DE","SPDR MSCI World Real Estate","Settoriale Mondo",False,False,"USD"),
    # SETTORIALE USA
    ("IUIT.MI","iShares S&P500 IT Sector UCITS ETF","Settoriale USA",False,False,"USD"),
    ("IUHC.MI","iShares S&P500 Healthcare Sector","Settoriale USA",False,False,"USD"),
    ("IUFS.MI","iShares S&P500 Financials Sector","Settoriale USA",False,False,"USD"),
    ("IUES.MI","iShares S&P500 Energy Sector","Settoriale USA",False,False,"USD"),
    ("IUUS.MI","iShares S&P500 Utilities Sector","Settoriale USA",False,False,"USD"),
    ("IUMS.MI","iShares S&P500 Materials Sector","Settoriale USA",False,False,"USD"),
    ("IUIS.MI","iShares S&P500 Industrials Sector","Settoriale USA",False,False,"USD"),
    ("IUCD.MI","iShares S&P500 Cons Discret Sector","Settoriale USA",False,False,"USD"),
    ("IUCS.MI","iShares S&P500 Cons Staples Sector","Settoriale USA",False,False,"USD"),
    ("IURE.MI","iShares S&P500 Real Estate Sector","Settoriale USA",False,False,"USD"),
    ("IUCM.MI","iShares S&P500 Comm Services Sector","Settoriale USA",False,False,"USD"),
    ("SXLK.MI","SPDR S&P500 Technology Select Sector","Settoriale USA",False,False,"USD"),
    ("SXLV.MI","SPDR S&P500 Health Care Select Sector","Settoriale USA",False,False,"USD"),
    ("SXLF.MI","SPDR S&P500 Financial Select Sector","Settoriale USA",False,False,"USD"),
    ("SXLE.MI","SPDR S&P500 Energy Select Sector","Settoriale USA",False,False,"USD"),
    ("SXLP.MI","SPDR S&P500 Utilities Select Sector","Settoriale USA",False,False,"USD"),
    ("SXLB.MI","SPDR S&P500 Materials Select Sector","Settoriale USA",False,False,"USD"),
    ("SXLI.MI","SPDR S&P500 Industrial Select Sector","Settoriale USA",False,False,"USD"),
    ("SXLY.MI","SPDR S&P500 Cons Discret Select Sector","Settoriale USA",False,False,"USD"),
    ("SXLC.MI","SPDR S&P500 Cons Staples Select Sector","Settoriale USA",False,False,"USD"),
    # SETTORIALE EUROPA
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
    ("XESK.DE","Xtrackers EURO STOXX 50 Banks","Settoriale Europa",False,False,"EUR"),
    ("XESF.DE","Xtrackers EURO STOXX 50 Financials","Settoriale Europa",False,False,"EUR"),
    # AZIONARIO
    ("SWRD.MI","SPDR MSCI World UCITS ETF","Azionario",False,False,"USD"),
    ("IWDA.MI","iShares Core MSCI World UCITS ETF","Azionario",False,False,"USD"),
    ("VWCE.DE","Vanguard FTSE All-World UCITS ETF Acc","Azionario",False,False,"USD"),
    ("ACWI.MI","iShares MSCI ACWI UCITS ETF","Azionario",False,False,"USD"),
    ("CSPX.MI","iShares Core S&P500 UCITS ETF","Azionario",False,False,"USD"),
    ("VUAA.MI","Vanguard S&P500 UCITS ETF Acc","Azionario",False,False,"USD"),
    ("XDEW.DE","Xtrackers MSCI World Equal Weight","Azionario",False,False,"USD"),
    ("EQQQ.MI","Invesco EQQQ Nasdaq-100 UCITS ETF","Azionario",False,False,"USD"),
    ("EXXT.DE","iShares TecDAX UCITS ETF","Azionario",False,False,"EUR"),
    ("XDAX.DE","Xtrackers DAX UCITS ETF","Azionario",False,False,"EUR"),
    ("EXS1.DE","iShares Core DAX UCITS ETF","Azionario",False,False,"EUR"),
    ("MEUD.MI","Amundi MSCI Europe UCITS ETF","Azionario",False,False,"EUR"),
    ("IMEU.MI","iShares Core MSCI Europe UCITS ETF","Azionario",False,False,"EUR"),
    ("VEUR.MI","Vanguard FTSE Developed Europe UCITS ETF","Azionario",False,False,"EUR"),
    ("ISP5.MI","iShares Core Euro Stoxx 50 UCITS ETF","Azionario",False,False,"EUR"),
    ("XJPN.DE","Xtrackers MSCI Japan UCITS ETF","Azionario",False,False,"JPY"),
    ("IJPA.MI","iShares Core MSCI Japan IMI UCITS ETF","Azionario",False,False,"JPY"),
    ("VFEM.MI","Vanguard FTSE Emerging Markets UCITS ETF","Azionario",False,False,"USD"),
    ("IEMA.MI","iShares Core MSCI EM IMI UCITS ETF","Azionario",False,False,"USD"),
    ("EMIM.MI","iShares Core MSCI EM IMI UCITS ETF","Azionario",False,False,"USD"),
    ("IWVL.MI","iShares Edge MSCI World Value Factor","Azionario",False,False,"USD"),
    ("IWMO.MI","iShares Edge MSCI World Momentum Factor","Azionario",False,False,"USD"),
    ("IWQU.MI","iShares Edge MSCI World Quality Factor","Azionario",False,False,"USD"),
    ("MVOL.MI","iShares Edge MSCI World Min Vol","Azionario",False,False,"USD"),
    ("XDWM2.DE","Xtrackers MSCI World Multifactor","Azionario",False,False,"USD"),
    ("XDEQ.DE","Xtrackers MSCI World Equal Weight","Azionario",False,False,"USD"),
    ("WLDS.MI","iShares MSCI World Small Cap UCITS ETF","Azionario",False,False,"USD"),
    ("XDEM.DE","Xtrackers MSCI World Momentum Factor","Azionario",False,False,"USD"),
    ("XDEV.DE","Xtrackers MSCI World Value Factor","Azionario",False,False,"USD"),
    ("XDEQ2.DE","Xtrackers MSCI World Quality Factor","Azionario",False,False,"USD"),
    ("VHYD.MI","Vanguard FTSE All-World High Div Yield","Azionario",False,False,"USD"),
    ("IQDV.MI","iShares MSCI World Quality Dividend","Azionario",False,False,"USD"),
    ("XDWD.DE","Xtrackers MSCI World High Dividend Yield","Azionario",False,False,"USD"),
    ("TDIV.MI","VanEck Morningstar Dev Markets Dividend","Azionario",False,False,"EUR"),
    ("XSCU.DE","Xtrackers MSCI World Small Cap","Azionario",False,False,"USD"),
    ("WSML.MI","iShares MSCI World Small Cap","Azionario",False,False,"USD"),
    ("IUS3.DE","iShares MSCI USA Small Cap","Azionario",False,False,"USD"),
    ("XESC.DE","Xtrackers MSCI Europe Small Cap","Azionario",False,False,"EUR"),
    ("CUSA.MI","iShares Core S&P500 UCITS ETF USD Acc","Azionario",False,False,"USD"),
    ("XGSD.DE","Xtrackers MSCI Germany","Azionario",False,False,"EUR"),
    ("EWG.DE","iShares MSCI Germany UCITS ETF","Azionario",False,False,"EUR"),
    ("EWQ.DE","iShares MSCI France UCITS ETF","Azionario",False,False,"EUR"),
    ("XESI.DE","Xtrackers MSCI Spain","Azionario",False,False,"EUR"),
    ("IITL.MI","iShares FTSE MIB UCITS ETF","Azionario",False,False,"EUR"),
    ("EWU.DE","iShares MSCI United Kingdom","Azionario",False,False,"GBP"),
    ("XDJP.DE","Xtrackers Nikkei 225","Azionario",False,False,"JPY"),
    ("EAUS.DE","iShares MSCI Australia UCITS ETF","Azionario",False,False,"AUD"),
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
    ("XINC.DE","Xtrackers MSCI India Swap","Azionario",False,False,"USD"),
    ("NDIA.MI","iShares MSCI India UCITS ETF","Azionario",False,False,"USD"),
    ("XCHI.DE","Xtrackers MSCI China","Azionario",False,False,"USD"),
    ("MCHI.MI","iShares MSCI China UCITS ETF","Azionario",False,False,"USD"),
    ("XBRA.DE","Xtrackers MSCI Brazil","Azionario",False,False,"USD"),
    ("XMEX.DE","Xtrackers MSCI Mexico","Azionario",False,False,"USD"),
    ("XTUR.DE","Xtrackers MSCI Turkey","Azionario",False,False,"USD"),
    ("XPOL.DE","Xtrackers MSCI Poland","Azionario",False,False,"EUR"),
    ("XGRC.DE","Xtrackers MSCI Greece","Azionario",False,False,"EUR"),
    ("XSAF.DE","Xtrackers MSCI South Africa","Azionario",False,False,"USD"),
    ("XVIE.DE","Xtrackers MSCI Vietnam","Azionario",False,False,"USD"),
    ("XIND.DE","Xtrackers MSCI Indonesia","Azionario",False,False,"USD"),
    ("XTHA.DE","Xtrackers MSCI Thailand","Azionario",False,False,"USD"),
    ("XPHL.DE","Xtrackers MSCI Philippines","Azionario",False,False,"USD"),
    ("XMYS.DE","Xtrackers MSCI Malaysia","Azionario",False,False,"USD"),
    ("XSAU.DE","Xtrackers MSCI Saudi Arabia","Azionario",False,False,"USD"),
    ("XUAE.DE","Xtrackers MSCI UAE","Azionario",False,False,"USD"),
    ("XEGY.DE","Xtrackers MSCI Egypt","Azionario",False,False,"USD"),
    ("XQAT.DE","Xtrackers MSCI Qatar","Azionario",False,False,"USD"),
    ("XDEX.DE","Xtrackers MSCI Europe ex-UK","Azionario",False,False,"EUR"),
    ("XEMC.DE","Xtrackers MSCI EM Asia","Azionario",False,False,"USD"),
    ("XEML.DE","Xtrackers MSCI EM Latin America","Azionario",False,False,"USD"),
    ("XNAS.DE","Xtrackers MSCI Nordic","Azionario",False,False,"EUR"),
    ("XGCC.DE","Xtrackers MSCI Gulf Cooperation","Azionario",False,False,"USD"),
    ("XAFR.DE","Xtrackers MSCI Africa","Azionario",False,False,"USD"),
    ("XSEA.DE","Xtrackers MSCI South East Asia","Azionario",False,False,"USD"),
    ("XPAC.DE","Xtrackers MSCI Pacific ex-Japan","Azionario",False,False,"USD"),
    ("XFEX.DE","Xtrackers MSCI Far East","Azionario",False,False,"USD"),
    ("XEME.DE","Xtrackers MSCI Eastern Europe","Azionario",False,False,"EUR"),
    # OBBLIGAZIONARIO
    ("XGLE.DE","Xtrackers Eurozone Gov Bond UCITS ETF","Obbligazionario",False,False,"EUR"),
    ("IGLA.MI","iShares Core EUR Govt Bond UCITS ETF","Obbligazionario",False,False,"EUR"),
    ("IS3N.DE","iShares EUR Govt Bond 1-3yr UCITS ETF","Obbligazionario",False,False,"EUR"),
    ("IS3O.DE","iShares EUR Govt Bond 3-7yr UCITS ETF","Obbligazionario",False,False,"EUR"),
    ("IS3P.DE","iShares EUR Govt Bond 7-10yr UCITS ETF","Obbligazionario",False,False,"EUR"),
    ("IDTL.MI","iShares EUR Govt Bond 20yr+ UCITS ETF","Obbligazionario",False,False,"EUR"),
    ("DBXG.DE","Xtrackers Germany Govt Bond UCITS ETF","Obbligazionario",False,False,"EUR"),
    ("BTPS.MI","iShares BTP UCITS ETF","Obbligazionario",False,False,"EUR"),
    ("IBCI.MI","iShares EUR Inflation Linked Govt Bond","Obbligazionario",False,False,"EUR"),
    ("XGIN.DE","Xtrackers EUR Inflation-Linked Bond","Obbligazionario",False,False,"EUR"),
    ("DTLA.MI","iShares USD Treasury Bond 20+yr UCITS","Obbligazionario",False,False,"USD"),
    ("IBTM.MI","iShares USD Treasury Bond 7-10yr","Obbligazionario",False,False,"USD"),
    ("IBTS.MI","iShares USD Treasury Bond 1-3yr","Obbligazionario",False,False,"USD"),
    ("XUTB.DE","Xtrackers USD Treasury Bond UCITS ETF","Obbligazionario",False,False,"USD"),
    ("DTLAH.MI","iShares USD Treasury Bond 20+yr EUR Hdg","Obbligazionario",False,False,"EUR-hedged"),
    ("IBTMH.MI","iShares USD Treasury Bond 7-10yr EUR Hdg","Obbligazionario",False,False,"EUR-hedged"),
    ("IBTSH.MI","iShares USD Treasury Bond 1-3yr EUR Hdg","Obbligazionario",False,False,"EUR-hedged"),
    ("ITPS.MI","iShares USD TIPS UCITS ETF","Obbligazionario",False,False,"USD"),
    ("XUIT.DE","Xtrackers USD TIPS UCITS ETF","Obbligazionario",False,False,"USD"),
    ("ITPSH.MI","iShares USD TIPS EUR Hedged","Obbligazionario",False,False,"EUR-hedged"),
    ("IEAC.MI","iShares Core EUR Corp Bond UCITS ETF","Obbligazionario",False,False,"EUR"),
    ("XDEB.DE","Xtrackers EUR Corporate Bond UCITS ETF","Obbligazionario",False,False,"EUR"),
    ("XBLC.DE","Xtrackers EUR Corp Bond Short Duration","Obbligazionario",False,False,"EUR"),
    ("LQDA.MI","iShares USD Corp Bond UCITS ETF","Obbligazionario",False,False,"USD"),
    ("IUAA.MI","iShares USD Corp Bond UCITS ETF Acc","Obbligazionario",False,False,"USD"),
    ("LQDAH.MI","iShares USD Corp Bond EUR Hedged","Obbligazionario",False,False,"EUR-hedged"),
    ("IHYG.MI","iShares EUR High Yield Corp Bond","Obbligazionario",False,False,"EUR"),
    ("XHYG.DE","Xtrackers EUR High Yield Corp Bond","Obbligazionario",False,False,"EUR"),
    ("IHYU.MI","iShares USD High Yield Corp Bond","Obbligazionario",False,False,"USD"),
    ("IHYUH.MI","iShares USD HY Corp Bond EUR Hedged","Obbligazionario",False,False,"EUR-hedged"),
    ("BGRN.MI","iShares Green Bond UCITS ETF","Obbligazionario",False,False,"EUR"),
    ("XGRE.DE","Xtrackers EUR Green Bond UCITS ETF","Obbligazionario",False,False,"EUR"),
    ("IEMB.MI","iShares JP Morgan USD EM Bond","Obbligazionario",False,False,"USD"),
    ("XEMB.DE","Xtrackers USD EM Bond UCITS ETF","Obbligazionario",False,False,"USD"),
    ("SEML.MI","iShares JP Morgan EM Local Govt Bond","Obbligazionario",False,False,"USD"),
    ("XEML2.DE","Xtrackers EM Local Currency Bond","Obbligazionario",False,False,"USD"),
    ("EMCB.DE","iShares EM Corporate Bond UCITS ETF","Obbligazionario",False,False,"USD"),
    ("IB25.MI","iShares iBonds Dec 2025 EUR Corp","Obbligazionario",False,False,"EUR"),
    ("IB26.MI","iShares iBonds Dec 2026 EUR Corp","Obbligazionario",False,False,"EUR"),
    ("IB27.MI","iShares iBonds Dec 2027 EUR Corp","Obbligazionario",False,False,"EUR"),
    ("IB28.MI","iShares iBonds Dec 2028 EUR Corp","Obbligazionario",False,False,"EUR"),
    ("IB29.MI","iShares iBonds Dec 2029 EUR Corp","Obbligazionario",False,False,"EUR"),
    ("IB30.MI","iShares iBonds Dec 2030 EUR Corp","Obbligazionario",False,False,"EUR"),
    ("IBG5.MI","iShares iBonds Dec 2025 EUR Govt","Obbligazionario",False,False,"EUR"),
    ("IBG6.MI","iShares iBonds Dec 2026 EUR Govt","Obbligazionario",False,False,"EUR"),
    ("IBG7.MI","iShares iBonds Dec 2027 EUR Govt","Obbligazionario",False,False,"EUR"),
    ("IBG8.MI","iShares iBonds Dec 2028 EUR Govt","Obbligazionario",False,False,"EUR"),
    ("AM25.MI","Amundi Fixed Maturity 2025 EUR Corp","Obbligazionario",False,False,"EUR"),
    ("AM26.MI","Amundi Fixed Maturity 2026 EUR Corp","Obbligazionario",False,False,"EUR"),
    ("AM27.MI","Amundi Fixed Maturity 2027 EUR Corp","Obbligazionario",False,False,"EUR"),
    ("AM28.MI","Amundi Fixed Maturity 2028 EUR Corp","Obbligazionario",False,False,"EUR"),
    # MONETARIO
    ("XEON.MI","Xtrackers EUR Overnight Rate Swap","Monetario",False,False,"EUR"),
    ("CSH2.MI","iShares EUR Ultrashort Bond UCITS ETF","Monetario",False,False,"EUR"),
    ("SMART.MI","Amundi EUR Floating Rate Bond UCITS ETF","Monetario",False,False,"EUR"),
    ("ERNE.DE","iShares EUR Money Market UCITS ETF","Monetario",False,False,"EUR"),
    ("XSTR.DE","Xtrackers EUR Short Duration Corp Bond","Monetario",False,False,"EUR"),
    ("PRAT.MI","Amundi Prime Euro Govies 0-1Y UCITS ETF","Monetario",False,False,"EUR"),
    ("XGLD.DE","Xtrackers II EUR Corporate Bond 0-1yr","Monetario",False,False,"EUR"),
    ("LUSB.MI","L&G EUR Ultrashort Bond UCITS ETF","Monetario",False,False,"EUR"),
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
            data   = r.json()
            result = data.get("chart", {}).get("result")
            if not result:
                log.warning(f"{ticker}: nessun dato Yahoo (attempt {attempt})")
                time.sleep(CONFIG["retry_delay"])
                continue
            ts         = result[0]["timestamp"]
            indicators = result[0]["indicators"]
            q          = indicators["quote"][0]
            adj_close  = indicators.get("adjclose", [{}])[0].get("adjclose", q["close"])
            dates  = [datetime.utcfromtimestamp(t).strftime("%Y-%m-%d") for t in ts]
            closes = [float(v) if v is not None else None for v in adj_close]
            opens  = [float(v) if v is not None else None for v in q["open"]]
            highs  = [float(v) if v is not None else None for v in q["high"]]
            lows   = [float(v) if v is not None else None for v in q["low"]]
            vols   = [int(v)   if v is not None else 0    for v in q["volume"]]
            valid  = [(d,o,h,l,c,v) for d,o,h,l,c,v in zip(dates,opens,highs,lows,closes,vols) if c is not None]
            if len(valid) < CONFIG["min_candles"]:
                log.warning(f"{ticker}: solo {len(valid)} candele valide")
                return None
            d,o,h,l,c,v = zip(*valid)
            return {"dates":list(d),"opens":list(o),"highs":list(h),"lows":list(l),"closes":list(c),"volumes":list(v)}
        except Exception as e:
            log.warning(f"{ticker}: attempt {attempt} fallito — {e}")
            time.sleep(CONFIG["retry_delay"] * attempt)
    return None

# ── CACHE ──────────────────────────────────────────────────────────────────────
def cache_path(ticker: str) -> Path:
    return Path(CONFIG["cache_dir"]) / f"{ticker.replace('.','_')}.json"

def load_cache(ticker: str) -> dict | None:
    p = cache_path(ticker)
    if not p.exists(): return None
    try:
        with open(p) as f: cached = json.load(f)
        age_h = (datetime.utcnow() - datetime.fromisoformat(cached.get("cached_at","2000-01-01"))).total_seconds() / 3600
        if age_h < CONFIG["cache_max_age_hours"]: return cached.get("ohlcv")
    except Exception: pass
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
    if ohlcv: save_cache(ticker, ohlcv)
    return ohlcv

# ── INDICATORI ─────────────────────────────────────────────────────────────────
def safe_arr(lst): return np.array(lst, dtype=float)

def calc_er(closes: np.ndarray, period: int = 10) -> np.ndarray:
    er = np.full(len(closes), np.nan)
    for i in range(period, len(closes)):
        direction  = abs(closes[i] - closes[i - period])
        volatility = np.sum(np.abs(np.diff(closes[i - period:i + 1])))
        er[i] = direction / volatility if volatility != 0 else 0.0
    return er

def calc_kama(closes: np.ndarray, n=10, fast=2, slow=30) -> np.ndarray:
    er       = calc_er(closes, n)
    fast_sc  = 2.0 / (fast + 1)
    slow_sc  = 2.0 / (slow + 1)
    kama     = np.full(len(closes), np.nan)
    kama[n]  = closes[n]
    for i in range(n + 1, len(closes)):
        if not np.isnan(er[i]):
            sc      = (er[i] * (fast_sc - slow_sc) + slow_sc) ** 2
            kama[i] = kama[i-1] + sc * (closes[i] - kama[i-1])
        else:
            kama[i] = kama[i-1]
    return kama

def calc_atr(highs, lows, closes, period=14) -> np.ndarray:
    tr  = np.maximum(highs[1:]-lows[1:], np.maximum(np.abs(highs[1:]-closes[:-1]), np.abs(lows[1:]-closes[:-1])))
    tr  = np.concatenate([[highs[0]-lows[0]], tr])
    atr = np.full(len(closes), np.nan)
    atr[period-1] = np.mean(tr[:period])
    for i in range(period, len(closes)):
        atr[i] = (atr[i-1] * (period-1) + tr[i]) / period
    return atr

def calc_supertrend(highs, lows, closes, period=10, multiplier=3.0):
    atr       = calc_atr(highs, lows, closes, period)
    hl2       = (highs + lows) / 2
    upper     = hl2 + multiplier * atr
    lower     = hl2 - multiplier * atr
    st        = np.full(len(closes), np.nan)
    direction = np.zeros(len(closes))
    for i in range(1, len(closes)):
        if np.isnan(atr[i]): continue
        prev_upper = upper[i-1] if not np.isnan(st[i-1]) else upper[i]
        prev_lower = lower[i-1] if not np.isnan(st[i-1]) else lower[i]
        upper[i]   = min(upper[i], prev_upper) if closes[i-1] <= prev_upper else upper[i]
        lower[i]   = max(lower[i], prev_lower) if closes[i-1] >= prev_lower else lower[i]
        if np.isnan(st[i-1]):
            direction[i] = 1
        elif st[i-1] == upper[i-1]:
            direction[i] = 1 if closes[i] > upper[i] else -1
        else:
            direction[i] = -1 if closes[i] < lower[i] else 1
        st[i] = lower[i] if direction[i] == 1 else upper[i]
    return st, direction

def calc_rsi(closes: np.ndarray, period=14) -> np.ndarray:
    delta    = np.diff(closes)
    gain     = np.where(delta > 0, delta, 0.0)
    loss     = np.where(delta < 0, -delta, 0.0)
    rsi      = np.full(len(closes), np.nan)
    if len(gain) < period: return rsi
    avg_gain = np.mean(gain[:period])
    avg_loss = np.mean(loss[:period])
    for i in range(period, len(closes)-1):
        avg_gain = (avg_gain * (period-1) + gain[i]) / period
        avg_loss = (avg_loss * (period-1) + loss[i]) / period
        rs       = avg_gain / avg_loss if avg_loss != 0 else 100.0
        rsi[i+1] = 100 - (100 / (1 + rs))
    return rsi

def calc_adx(highs, lows, closes, period=14) -> np.ndarray:
    atr  = calc_atr(highs, lows, closes, period)
    up   = highs[1:] - highs[:-1]
    down = lows[:-1] - lows[1:]
    pdm  = np.where((up > down) & (up > 0), up, 0.0)
    ndm  = np.where((down > up) & (down > 0), down, 0.0)
    adx  = np.full(len(closes), np.nan)
    if len(closes) < period * 2: return adx
    pdm14 = np.mean(pdm[:period])
    ndm14 = np.mean(ndm[:period])
    dx_arr = []
    for i in range(period, len(closes)-1):
        pdm14  = pdm14 - pdm14/period + pdm[i]
        ndm14  = ndm14 - ndm14/period + ndm[i]
        atr14  = atr[i+1]
        pdi    = 100 * pdm14 / atr14 if atr14 != 0 else 0
        ndi    = 100 * ndm14 / atr14 if atr14 != 0 else 0
        dx     = 100 * abs(pdi - ndi) / (pdi + ndi) if (pdi + ndi) != 0 else 0
        dx_arr.append(dx)
    if len(dx_arr) >= period:
        adx_val = np.mean(dx_arr[:period])
        idx = period * 2
        for i in range(period, len(dx_arr)):
            adx_val = (adx_val * (period-1) + dx_arr[i]) / period
            if idx < len(adx): adx[idx] = adx_val
            idx += 1
    return adx

def calc_bollinger(closes: np.ndarray, period=20, n_std=2.0):
    mid   = np.full(len(closes), np.nan)
    upper = np.full(len(closes), np.nan)
    lower = np.full(len(closes), np.nan)
    width = np.full(len(closes), np.nan)
    for i in range(period-1, len(closes)):
        w       = closes[i-period+1:i+1]
        m, s    = np.mean(w), np.std(w, ddof=1)
        mid[i]   = m
        upper[i] = m + n_std * s
        lower[i] = m - n_std * s
        width[i] = (upper[i] - lower[i]) / m if m != 0 else 0
    return mid, upper, lower, width

def calc_sar(highs, lows, af_step=0.02, af_max=0.2):
    sar  = np.full(len(highs), np.nan)
    if len(highs) < 2: return sar
    bull = True; af = af_step; ep = highs[0]; sar[0] = lows[0]
    for i in range(1, len(highs)):
        prev_sar = sar[i-1]
        if bull:
            sar[i] = prev_sar + af * (ep - prev_sar)
            sar[i] = min(sar[i], lows[i-1], lows[i-2] if i > 1 else lows[i-1])
            if lows[i] < sar[i]:
                bull = False; af = af_step; ep = lows[i]; sar[i] = ep
            else:
                if highs[i] > ep: ep = highs[i]; af = min(af + af_step, af_max)
        else:
            sar[i] = prev_sar + af * (ep - prev_sar)
            sar[i] = max(sar[i], highs[i-1], highs[i-2] if i > 1 else highs[i-1])
            if highs[i] > sar[i]:
                bull = True; af = af_step; ep = highs[i]; sar[i] = ep
            else:
                if lows[i] < ep: ep = lows[i]; af = min(af + af_step, af_max)
    return sar

def calc_obv(closes, volumes) -> np.ndarray:
    obv = np.zeros(len(closes))
    for i in range(1, len(closes)):
        obv[i] = obv[i-1] + (volumes[i] if closes[i] > closes[i-1] else -volumes[i] if closes[i] < closes[i-1] else 0)
    return obv

def calc_ema(closes: np.ndarray, period: int) -> np.ndarray:
    ema = np.full(len(closes), np.nan)
    k   = 2.0 / (period + 1)
    s   = period - 1
    if s >= len(closes): return ema
    ema[s] = np.mean(closes[:period])
    for i in range(s+1, len(closes)):
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
        avg  = np.mean(volumes[i-period:i])
        vr[i] = volumes[i] / avg if avg != 0 else 1.0
    return vr

def momentum_return(closes: np.ndarray, bars: int) -> float:
    if len(closes) <= bars or closes[-bars-1] == 0: return 0.0
    return (closes[-1] / closes[-bars-1] - 1) * 100

def safe_last(arr: np.ndarray, default=None):
    valid = arr[~np.isnan(arr)]
    return float(valid[-1]) if len(valid) > 0 else default

def find_signal_date(kama_fast, kama_slow, dates) -> str | None:
    for i in range(len(kama_fast)-1, 0, -1):
        if np.isnan(kama_fast[i]) or np.isnan(kama_slow[i]): continue
        if np.isnan(kama_fast[i-1]) or np.isnan(kama_slow[i-1]): continue
        if (kama_fast[i] - kama_slow[i]) * (kama_fast[i-1] - kama_slow[i-1]) < 0:
            return dates[i]
    return None

def days_since(date_str: str) -> int | None:
    if not date_str: return None
    try: return (datetime.utcnow() - datetime.strptime(date_str, "%Y-%m-%d")).days
    except Exception: return None

# ── CLASSIFICAZIONE SEGNALE ────────────────────────────────────────────────────
def classify_signal(er_val, er_series, kama_fast_val, kama_slow_val, bb_width_series, vol_ratio_val, is_leveraged, supertrend_dir) -> str:
    er_thresh   = CONFIG["er_threshold_leva"]  if is_leveraged else CONFIG["er_threshold_normal"]
    er_trending = CONFIG["er_trending_leva"]   if is_leveraged else CONFIG["er_trending_normal"]
    n           = CONFIG["er_watch_rising_bars"]
    er_valid    = er_series[~np.isnan(er_series)]
    er_rising   = len(er_valid) >= n and all(er_valid[-n+i] < er_valid[-n+i+1] for i in range(n-1))
    bw_valid    = bb_width_series[~np.isnan(bb_width_series)]
    bb_compressed = len(bw_valid) >= 20 and bw_valid[-1] < np.mean(bw_valid[-20:]) * (1 - CONFIG["bb_compression_pct"])
    kama_bull   = kama_fast_val > kama_slow_val if (kama_fast_val and kama_slow_val) else False
    st_bull     = supertrend_dir == 1
    if er_val is None or er_val < er_thresh:
        if er_rising and (bb_compressed or vol_ratio_val and vol_ratio_val > CONFIG["volume_ratio_watch"]):
            return "WATCH"
        return "RANGING"
    if er_val >= er_trending:
        return "BUY" if (kama_bull and st_bull) else "SELL"
    if kama_bull and st_bull:   return "BUY"
    if not kama_bull and not st_bull: return "SELL"
    return "WATCH"

# ── SCORE ──────────────────────────────────────────────────────────────────────
def compute_score(er_val, kama_bull, st_bull, mom3m, adx_val, rsi_val, sar_bull) -> int:
    w     = CONFIG["score_weights"]
    score = 0.0
    score += w["er"]         * min(1.0, (er_val or 0) / 0.5) * 100
    score += w["supertrend"] * (100 if st_bull else 0)
    score += w["kama"]       * (100 if kama_bull else 0)
    score += w["momentum3m"] * max(0, min(100, (mom3m + 30) / 60 * 100))
    score += w["adx"]        * min(100, (adx_val or 0) / 60 * 100)
    score += w["rsi"]        * min(100, abs((rsi_val or 50) - 50) / 30 * 100)
    score += w["sar"]        * (100 if sar_bull else 0)
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
        kama_fast  = calc_kama(closes, n=10, fast=5, slow=20)
        kama_slow  = calc_kama(closes, n=10, fast=2, slow=30)
        er_series  = calc_er(closes, 10)
        ema20      = calc_ema(closes, 20)
        ema50      = calc_ema(closes, 50)
        sma200     = calc_sma(closes, 200)
        rsi        = calc_rsi(closes, 14)
        adx        = calc_adx(highs, lows, closes, 14)
        atr        = calc_atr(highs, lows, closes, 14)
        st_line, st_dir = calc_supertrend(highs, lows, closes, CONFIG["supertrend_atr_period"], CONFIG["supertrend_multiplier"])
        bb_mid, bb_upper, bb_lower, bb_width = calc_bollinger(closes, CONFIG["bb_period"], CONFIG["bb_std"])
        sar        = calc_sar(highs, lows)
        obv        = calc_obv(closes, volumes)
        vol_ratio  = calc_volume_ratio(volumes, CONFIG["volume_ratio_period"])
        mom1m      = momentum_return(closes, 21)
        mom3m      = momentum_return(closes, 63)
        mom6m      = momentum_return(closes, 126)

        last_close  = float(closes[-1])
        last_er     = safe_last(er_series)
        last_kf     = safe_last(kama_fast)
        last_ks     = safe_last(kama_slow)
        last_rsi    = safe_last(rsi)
        last_adx    = safe_last(adx)
        last_atr    = safe_last(atr)
        last_vr     = safe_last(vol_ratio)
        last_st_dir = int(st_dir[-1])
        last_sar    = safe_last(sar)
        last_ema20  = safe_last(ema20)
        last_ema50  = safe_last(ema50)
        last_sma200 = safe_last(sma200)
        last_bb_width = safe_last(bb_width)
        last_bb_upper = safe_last(bb_upper)
        last_bb_lower = safe_last(bb_lower)

        kama_bull   = last_kf > last_ks if (last_kf and last_ks) else False
        sar_bull    = last_close > last_sar if last_sar else False
        above_sma200= last_close > last_sma200 if last_sma200 else None
        prev_close  = float(closes[-2]) if len(closes) >= 2 else last_close
        chg_pct     = round((last_close / prev_close - 1) * 100, 2) if prev_close else 0

        signal       = classify_signal(last_er, er_series, last_kf, last_ks, bb_width, last_vr, is_lev, last_st_dir)
        signal_date  = find_signal_date(kama_fast, kama_slow, dates)
        signal_days  = days_since(signal_date)
        score        = compute_score(last_er, kama_bull, last_st_dir == 1, mom3m, last_adx, last_rsi, sar_bull)

        er_thresh = CONFIG["er_threshold_leva"] if is_lev else CONFIG["er_threshold_normal"]
        er_trend  = CONFIG["er_trending_leva"]  if is_lev else CONFIG["er_trending_normal"]
        if last_er is None:               kama_state = "UNKNOWN"
        elif last_er < er_thresh:         kama_state = "RANGING"
        elif last_er < er_trend:          kama_state = "BOTTOMING" if kama_bull else "TIRED"
        else:                             kama_state = "FRESH"     if kama_bull else "EXTENDED"

        n90  = min(90, len(dates))
        def clean(arr): return [round(v,4) if not math.isnan(v) else None for v in arr[-n90:].tolist()]

        chart = {
            "dates":     dates[-n90:],
            "opens":     [round(v,4) for v in ohlcv["opens"][-n90:]],
            "highs":     [round(v,4) for v in ohlcv["highs"][-n90:]],
            "lows":      [round(v,4) for v in ohlcv["lows"][-n90:]],
            "closes":    [round(v,4) for v in closes[-n90:].tolist()],
            "volumes":   ohlcv["volumes"][-n90:],
            "kama_fast": clean(kama_fast),
            "kama_slow": clean(kama_slow),
            "bb_upper":  clean(bb_upper),
            "bb_mid":    clean(bb_mid),
            "bb_lower":  clean(bb_lower),
            "sar":       clean(sar),
            "supertrend":clean(st_line),
            "st_dir":    [int(d) for d in st_dir[-n90:].tolist()],
            "er":        clean(er_series),
            "rsi":       [round(v,2) if not math.isnan(v) else None for v in rsi[-n90:].tolist()],
            "obv":       clean(obv),
            "vol_ratio": [round(v,2) if not math.isnan(v) else None for v in vol_ratio[-n90:].tolist()],
            "buy_signals":  [],
            "sell_signals": [],
        }
        for i in range(1, n90):
            idx = len(dates) - n90 + i
            if idx < 1 or np.isnan(kama_fast[idx]) or np.isnan(kama_slow[idx]): continue
            if np.isnan(kama_fast[idx-1]) or np.isnan(kama_slow[idx-1]): continue
            curr = kama_fast[idx] - kama_slow[idx]
            prev = kama_fast[idx-1] - kama_slow[idx-1]
            if prev <= 0 and curr > 0: chart["buy_signals"].append({"date":dates[idx],"price":round(float(closes[idx]),4)})
            elif prev >= 0 and curr < 0: chart["sell_signals"].append({"date":dates[idx],"price":round(float(closes[idx]),4)})

        return {
            "ticker": ticker, "nome": nome, "categoria": categoria,
            "is_leveraged": is_lev, "is_short": is_short, "currency": currency,
            "price": round(last_close, 4), "chg_pct": chg_pct,
            "atr": round(last_atr, 4) if last_atr else None,
            "signal": signal, "signal_date": signal_date, "signal_days": signal_days,
            "score": score, "kama_state": kama_state,
            "er": round(last_er, 4) if last_er else None,
            "rsi": round(last_rsi, 1) if last_rsi else None,
            "adx": round(last_adx, 1) if last_adx else None,
            "mom1m": round(mom1m, 2), "mom3m": round(mom3m, 2), "mom6m": round(mom6m, 2),
            "vol_ratio": round(last_vr, 2) if last_vr else None,
            "bb_width": round(last_bb_width, 4) if last_bb_width else None,
            "kama_fast": round(last_kf, 4) if last_kf else None,
            "kama_slow": round(last_ks, 4) if last_ks else None,
            "kama_bull": kama_bull, "supertrend_dir": last_st_dir,
            "sar_bull": sar_bull,
            "ema20": round(last_ema20, 4) if last_ema20 else None,
            "ema50": round(last_ema50, 4) if last_ema50 else None,
            "sma200": round(last_sma200, 4) if last_sma200 else None,
            "above_sma200": above_sma200,
            "bb_upper": round(last_bb_upper, 4) if last_bb_upper else None,
            "bb_lower": round(last_bb_lower, 4) if last_bb_lower else None,
            "chart": chart,
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

    results, errors, skipped = [], [], []

    for i, etf in enumerate(ETF_UNIVERSE):
        ticker = etf[0]
        try:
            result = process_etf(etf)
            if result: results.append(result)
            else:      skipped.append(ticker)
        except Exception as e:
            log.error(f"{ticker}: eccezione non gestita — {e}")
            errors.append(ticker)
        if i < len(ETF_UNIVERSE) - 1:
            time.sleep(CONFIG["request_delay"])

    output = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "total": len(results),
            "errors": errors,
            "skipped": skipped,
            "version": "2.0.0",
        },
        "signals": results,
    }

    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(',', ':'))

    elapsed = (datetime.utcnow() - run_start).total_seconds()
    log.info(f"=== COMPLETATO in {elapsed:.0f}s — {len(results)} ETF processati, {len(skipped)} skippati, {len(errors)} errori ===")

if __name__ == "__main__":
    main()
