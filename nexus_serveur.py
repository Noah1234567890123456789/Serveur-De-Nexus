# -*- coding: utf-8 -*-
"""
================================================================================
  NEXUS SERVER (EN LIGNE)  —  durci + journal des connexions + synchro + NXC
================================================================================
"""

import os
import json
import time
import hashlib
import secrets
import threading
import datetime
from collections import defaultdict

from flask import Flask, request, jsonify, send_file, Response

MASTER_KEY = os.environ.get("NEXUS_MASTER_KEY", "change-moi-cle-maitre-nexus-2026")
PORT = int(os.environ.get("PORT", "8000"))

BASE = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE, "nexus_db.json")
_lock = threading.Lock()
app = Flask(__name__)

# ══ ÉTAT MARCHÉ NXC (en mémoire, partagé entre tous les clients) ══
NXC_FAILS = []   # tentatives de vente echouees (insolvabilite)

NXC_SOLVABILITY = {
    "enabled": False,       # Activer/désactiver le contrôle de solvabilité
    "gesture": 50           # Rewards offerts en geste commercial si banque insolvable
}

NXC_MARKET = {
    "price": 5213,
    "history": [],
    "volume24": 0,
    "trades24": 0,
    "ts": 0
}

def _load_nxc_from_db():
    """Restaure le dernier prix NXC depuis la DB au démarrage du serveur."""
    try:
        db = load_db()
        # Chercher dans le compte noah
        noah = db.get("users", {}).get("noah", {})
        mkt = noah.get("data", {}).get("nxcoin_market", {})
        if mkt and mkt.get("price", 0) > 0:
            NXC_MARKET["price"] = float(mkt["price"])
            NXC_MARKET["history"] = mkt.get("history", [])[-288:]
            NXC_MARKET["volume24"] = mkt.get("volume24", 0)
            NXC_MARKET["trades24"] = mkt.get("trades24", 0)
            # Mettre ts = maintenant pour eviter le rattrapage au redemarrage
            NXC_MARKET["ts"] = int(time.time() * 1000)
    except Exception as e:
        pass  # Garder le prix par défaut

# Charger au démarrage (appelé après la définition des fonctions)

import random as _rnd

def _nxc_autotick():
    """Le serveur fait evoluer le prix NXC tout seul, toutes les 15s."""
    while True:
        try:
            time.sleep(15)
            p = NXC_MARKET["price"]
            sigma = 0.008 + _rnd.random() * 0.015
            adj = (_rnd.random() - 0.48) * sigma
            if p > 80000: adj -= 0.012
            if p < 200: adj += 0.018
            p = max(50.0, min(100000.0, p * (1 + adj)))
            p = round(p * 100) / 100 if _rnd.random() > 0.03 else float(round(p))
            NXC_MARKET["price"] = p
            NXC_MARKET["ts"] = int(time.time() * 1000)
            NXC_MARKET["history"].append({"price": p, "ts": NXC_MARKET["ts"],
                                          "vol": int(_rnd.random() * 800 + 30)})
            if len(NXC_MARKET["history"]) > 576:
                NXC_MARKET["history"] = NXC_MARKET["history"][-576:]
            # Persister dans la DB toutes les ~2 min (8 ticks) pour survivre aux redemarrages
            if len(NXC_MARKET["history"]) % 8 == 0:
                with _lock:
                    db = load_db()
                    noah = db.get("users", {}).get("noah")
                    if noah is not None:
                        noah.setdefault("data", {})["nxcoin_market"] = {
                            "price": p, "history": NXC_MARKET["history"][-144:],
                            "volume24": NXC_MARKET["volume24"],
                            "trades24": NXC_MARKET["trades24"],
                            "ts": NXC_MARKET["ts"]}
                        save_db(db)
        except Exception:
            pass

_tick_started = False
_tick_lock = threading.Lock()

def _ensure_tick():
    """Demarre le thread de tick une seule fois (marche avec Gunicorn)."""
    global _tick_started
    if _tick_started:
        return
    with _tick_lock:
        if not _tick_started:
            threading.Thread(target=_nxc_autotick, daemon=True).start()
            _tick_started = True

_ensure_tick()


# Restaurer le prix NXC au démarrage (Gunicorn + local)
try:
    _load_nxc_from_db()
except Exception:
    pass

@app.after_request
def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    resp.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
    return resp

_hits = defaultdict(list)
_RATE_MAX = 30
_RATE_WINDOW = 60

def client_ip():
    fwd = request.headers.get("X-Forwarded-For", "")
    return (fwd.split(",")[0].strip() if fwd else request.remote_addr) or "?"

def rate_limited():
    ip = client_ip()
    now = time.time()
    _hits[ip] = [t for t in _hits[ip] if now - t < _RATE_WINDOW]
    _hits[ip].append(now)
    return len(_hits[ip]) > _RATE_MAX

def now_iso():
    return datetime.datetime.now().isoformat(timespec="seconds")

def load_db():
    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"users": {}}

def save_db(db):
    tmp = DB_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DB_FILE)

def hash_pw(pw, salt):
    return hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"), bytes.fromhex(salt), 200_000).hex()

def make_user(pw, role):
    salt = secrets.token_hex(16)
    return {"role": role, "salt": salt, "pass_hash": hash_pw(pw, salt),
            "nickname": "", "hidden": False, "data": {}, "logins": [],
            "created": now_iso(), "updated": now_iso()}

def check(db, u, p):
    x = db["users"].get(u)
    return bool(x) and secrets.compare_digest(x["pass_hash"], hash_pw(p, x["salt"]))

def is_admin(db, u, p):
    x = db["users"].get(u)
    return bool(x) and x.get("role") == "admin" and check(db, u, p)

def admin_ok(d, db):
    mk = d.get("master_key") or ""
    if mk and secrets.compare_digest(mk, MASTER_KEY):
        return True
    return is_admin(db, (d.get("admin_user") or "").strip(), d.get("admin_password") or "")

# ══════════════════════════════════════════════════════════
# PANNEAU NXC COIN
# ══════════════════════════════════════════════════════════
NXC_PANEL_HTML = '<!DOCTYPE html>\n<html lang="fr">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">\n<title>◈ Nexus</title>\n<style>\n*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent;touch-action:manipulation}\n:root{--bg:#02040a;--bg2:#080d1a;--bg3:#0d1428;--cyan:#00e5ff;--green:#00ff9d;--red:#ff3d5e;--gold:#ffb020;--purple:#a06bff;--muted:#5c6b8c;--text:#d4e8ff;--border:rgba(0,229,255,.12)}\nhtml,body{background:var(--bg);color:var(--text);font-family:\'Segoe UI\',system-ui,sans-serif;min-height:100dvh;overflow-x:hidden;-webkit-text-size-adjust:100%}\n\n/* LOGIN */\n#ls{position:fixed;inset:0;background:var(--bg);z-index:9999;display:flex;align-items:center;justify-content:center;padding:20px}\n.lb{background:var(--bg2);border:1px solid var(--border);border-radius:22px;padding:32px 24px;width:100%;max-width:340px;text-align:center;box-shadow:0 24px 80px rgba(0,0,0,.6)}\n.lb-logo{font-family:monospace;font-size:30px;font-weight:900;color:var(--cyan);letter-spacing:4px;margin-bottom:4px;text-shadow:0 0 20px rgba(0,229,255,.4)}\n.lb-sub{font-size:10px;color:var(--muted);margin-bottom:24px;letter-spacing:3px;text-transform:uppercase}\n.fi{width:100%;padding:13px 16px;background:var(--bg3);border:1px solid var(--border);border-radius:12px;color:var(--text);font-size:16px;margin-bottom:10px;outline:none}\n.fi:focus{border-color:var(--cyan)}\n.btn-login{width:100%;padding:14px;border-radius:12px;font-size:15px;font-weight:800;cursor:pointer;border:none;background:linear-gradient(135deg,var(--cyan),#0097b2);color:#000;letter-spacing:.5px}\n#lm{font-size:12px;color:var(--red);margin-top:8px;min-height:16px}\n\n/* HUD */\n.hud{position:fixed;top:0;left:0;right:0;height:52px;background:rgba(2,4,10,.97);border-bottom:1px solid var(--border);display:flex;align-items:center;padding:0 14px;gap:10px;z-index:100;backdrop-filter:blur(20px)}\n.hud-logo{font-family:monospace;font-size:15px;font-weight:900;color:var(--cyan);letter-spacing:2px;flex-shrink:0}\n.hud-price{font-family:monospace;font-size:12px;font-weight:800;color:var(--cyan)}\n.hud-chg{font-size:10px;font-weight:700;padding:2px 7px;border-radius:20px}\n.hud-chg.up{background:rgba(0,255,157,.12);color:var(--green);border:1px solid rgba(0,255,157,.2)}\n.hud-chg.dn{background:rgba(255,61,94,.12);color:var(--red);border:1px solid rgba(255,61,94,.2)}\n.hud-right{margin-left:auto;display:flex;align-items:center;gap:8px}\n.dot{width:7px;height:7px;border-radius:50%;background:var(--muted);flex-shrink:0}\n.dot.on{background:var(--green);box-shadow:0 0 8px var(--green);animation:dp 2s infinite}\n@keyframes dp{0%,100%{opacity:1}50%{opacity:.3}}\n.hud-time{font-family:monospace;font-size:10px;color:var(--muted)}\n\n/* TABS */\n.tabs{position:fixed;top:52px;left:0;right:0;background:rgba(2,4,10,.97);border-bottom:1px solid var(--border);display:flex;z-index:99;backdrop-filter:blur(20px);overflow-x:auto;scrollbar-width:none}\n.tabs::-webkit-scrollbar{display:none}\n.tab{flex:0 0 auto;padding:12px 18px;font-size:12px;font-weight:700;color:var(--muted);cursor:pointer;border:none;background:none;border-bottom:2px solid transparent;white-space:nowrap;transition:.15s}\n.tab.on{color:var(--cyan);border-bottom-color:var(--cyan)}\n.tab-more{flex:0 0 auto;padding:12px 16px;font-size:16px;color:var(--muted);cursor:pointer;border:none;background:none;border-bottom:2px solid transparent;margin-left:auto}\n.tab-more.on{color:var(--cyan)}\n\n/* DROPDOWN MENU */\n.dropdown{position:fixed;top:52px;right:0;background:var(--bg2);border:1px solid var(--border);border-radius:0 0 0 14px;z-index:200;min-width:180px;display:none;box-shadow:0 8px 32px rgba(0,0,0,.5)}\n.dropdown.show{display:block}\n.dd-item{padding:12px 18px;font-size:13px;font-weight:600;color:var(--muted);cursor:pointer;border-bottom:1px solid rgba(0,229,255,.06);display:flex;align-items:center;gap:10px}\n.dd-item:hover{background:rgba(0,229,255,.05);color:var(--text)}\n.dd-item:last-child{border:none}\n\n/* CONTENT */\n.content{padding-top:100px;padding-bottom:20px}\n.view{display:none;padding:14px;max-width:960px;margin:0 auto}\n.view.on{display:block}\n#view-nexus{display:none;flex-direction:column;padding:0;max-width:none}\n#view-nexus.on{display:flex}\n\n/* CARDS */\n.card{background:var(--bg2);border:1px solid var(--border);border-radius:16px;padding:16px;margin-bottom:12px}\n.card.cyan{border-color:rgba(0,229,255,.22)}.card.green{border-color:rgba(0,255,157,.22)}.card.red{border-color:rgba(255,61,94,.22)}.card.gold{border-color:rgba(255,176,32,.22)}.card.purple{border-color:rgba(160,107,255,.22)}\n.ct{font-size:9px;letter-spacing:2px;color:var(--muted);margin-bottom:12px;font-weight:700;text-transform:uppercase;display:flex;align-items:center;justify-content:space-between}\n.g4{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:12px}\n.g3{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px}\n.g2{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}\n.st{background:var(--bg3);border:1px solid rgba(0,229,255,.07);border-radius:12px;padding:12px 8px;text-align:center}\n.sv{font-family:monospace;font-size:16px;font-weight:800;color:var(--cyan);margin-bottom:2px}\n.sl{font-size:8px;color:var(--muted);letter-spacing:.8px;text-transform:uppercase}\n.sv.gold{color:var(--gold)}.sv.green{color:var(--green)}.sv.red{color:var(--red)}.sv.purple{color:var(--purple)}\n.sec{font-size:10px;color:var(--cyan);font-weight:700;letter-spacing:1px;text-transform:uppercase;margin:12px 0 6px;border-left:2px solid var(--cyan);padding-left:8px}\ninput,select,textarea{width:100%;padding:12px 13px;background:var(--bg3);border:1px solid var(--border);border-radius:11px;color:var(--text);font-size:14px;margin-bottom:8px;outline:none;font-family:inherit}\ninput:focus,select:focus{border-color:var(--cyan)}\n.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}\n.grow{flex:1;min-width:0;margin-bottom:0!important}\n.btn{padding:10px 14px;border-radius:10px;font-size:12px;font-weight:700;cursor:pointer;border:1px solid var(--border);background:var(--bg3);color:var(--text);white-space:nowrap;flex-shrink:0;transition:.15s}\n.btn:active{transform:scale(.96)}\n.btn.cyan{background:rgba(0,229,255,.1);border-color:rgba(0,229,255,.3);color:var(--cyan)}\n.btn.green{background:rgba(0,255,157,.1);border-color:rgba(0,255,157,.3);color:var(--green)}\n.btn.red{background:rgba(255,61,94,.1);border-color:rgba(255,61,94,.3);color:var(--red)}\n.btn.gold{background:rgba(255,176,32,.1);border-color:rgba(255,176,32,.3);color:var(--gold)}\n.btn.purple{background:rgba(160,107,255,.1);border-color:rgba(160,107,255,.3);color:var(--purple)}\n.btn.primary{background:linear-gradient(135deg,var(--cyan),#0097b2);color:#000;border:none}\n.btn.full{width:100%;padding:12px;font-size:13px;margin-bottom:8px;display:block}\n.ab{padding:10px 13px;border-radius:10px;font-size:12px;margin-bottom:6px}\n.ao{background:rgba(0,255,157,.07);border:1px solid rgba(0,255,157,.15);color:var(--green)}\n.aw{background:rgba(255,176,32,.07);border:1px solid rgba(255,176,32,.15);color:var(--gold)}\n.ae{background:rgba(255,61,94,.07);border:1px solid rgba(255,61,94,.15);color:var(--red)}\n.ai{background:rgba(0,229,255,.07);border:1px solid rgba(0,229,255,.15);color:var(--cyan)}\n.chart-wrap{position:relative;margin-bottom:10px}\n.ch200{height:200px}.ch150{height:150px}\n.fl-item{padding:10px 12px;border-bottom:1px solid rgba(0,229,255,.05);display:flex;align-items:center;gap:8px;font-size:12px}\n.fl-item:last-child{border:none}\n.tg{width:46px;height:25px;background:rgba(255,255,255,.07);border:1px solid var(--border);border-radius:13px;cursor:pointer;position:relative;flex-shrink:0;transition:.3s}\n.tg.on{background:rgba(0,229,255,.2);border-color:var(--cyan)}\n.tg-k{position:absolute;top:3px;left:3px;width:17px;height:17px;background:#8899aa;border-radius:50%;transition:.3s}\n.tg.on .tg-k{left:24px;background:var(--cyan)}\n.pbar{height:6px;background:rgba(0,0,0,.4);border-radius:3px;overflow:hidden;margin-top:4px}\n.pbar-fill{height:100%;border-radius:3px;background:linear-gradient(90deg,var(--cyan),var(--purple));transition:width .5s}\n.log-item{padding:7px 12px;border-bottom:1px solid rgba(0,229,255,.04);font-size:11px;display:flex;gap:8px}\n.log-time{color:var(--muted);font-family:monospace;flex-shrink:0;font-size:10px}\n.tbl-wrap{overflow-x:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)}\ntable{width:100%;border-collapse:collapse;font-size:11px}\nth,td{padding:9px 8px;text-align:left;border-bottom:1px solid rgba(0,229,255,.05)}\nth{color:var(--muted);font-size:9px;text-transform:uppercase;letter-spacing:.5px;font-weight:700}\n.ibar{background:var(--bg2);border-bottom:1px solid var(--border);padding:10px 14px;display:flex;align-items:center;gap:10px}\n.iurl{flex:1;font-size:10px;color:var(--muted);font-family:monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#nf{flex:1;border:none;width:100%;background:var(--bg)}\n.sw{position:relative}\n.sw input{padding-left:34px;margin:0}\n.sw::before{content:\'🔍\';position:absolute;left:10px;top:50%;transform:translateY(-50%);font-size:13px;pointer-events:none;z-index:1}\n.notif{position:absolute;top:6px;right:calc(50% - 12px);width:8px;height:8px;background:var(--red);border-radius:50%;display:none;border:2px solid var(--bg);animation:blink .8s ease infinite}\n@keyframes blink{0%,100%{transform:scale(1)}50%{transform:scale(1.3)}}\n@media(max-width:480px){.g4{grid-template-columns:repeat(2,1fr)}.sv{font-size:14px}.content{padding-top:96px}}\n@media(min-width:768px){.sv{font-size:20px}.ch200{height:240px}}\n</style>\n</head>\n<body>\n\n\n\n<!-- HUD -->\n<div class="hud">\n<div class="hud-logo">◈ NXC</div>\n<div class="hud-price" id="hp">—</div>\n<div class="hud-chg" id="hc" style="display:none"></div>\n<div class="hud-right">\n<div class="dot" id="hd"></div>\n<span class="hud-time" id="htm">—</span>\n</div>\n</div>\n\n<!-- TABS -->\n<div class="tabs" id="main-tabs">\n<button class="tab on" onclick="go(\'marche\',this)">📈 Marché</button>\n<button class="tab" onclick="go(\'banque\',this)">🏦 Banque<span class="notif" id="nd-b"></span></button>\n<button class="tab" onclick="go(\'nexus\',this)">🌐 App</button>\n<button class="tab" onclick="go(\'admin\',this)">👑 Admin</button>\n<button class="tab-more" id="btn-more" onclick="toggleMore()">•••</button>\n</div>\n\n<!-- DROPDOWN MENU -->\n<div class="dropdown" id="dropdown">\n<div class="dd-item" onclick="go(\'trading\',null);toggleMore()">⚙️ Contrôle</div>\n<div class="dd-item" onclick="go(\'users\',null);toggleMore()">👥 Comptes</div>\n<div class="dd-item" onclick="go(\'stats\',null);toggleMore()">📊 Stats</div>\n<div class="dd-item" onclick="go(\'solv\',null);toggleMore()">🛡️ Solvabilité</div>\n<div class="dd-item" onclick="go(\'tools\',null);toggleMore()">🛠️ Outils</div>\n<div class="dd-item" onclick="go(\'log\',null);toggleMore()">📋 Journal</div>\n<div class="dd-item" onclick="go(\'config\',null);toggleMore()">⚙️ Config</div>\n<div class="dd-item" onclick="go(\'notifs\',null);toggleMore()">🔔 Alertes</div>\n<div class="dd-item" onclick="go(\'cycles\',null);toggleMore()">📅 Cycles de marché</div>\n<div class="dd-item" onclick="go(\'prevision\',null);toggleMore()">🔮 Prévisions</div>\n</div>\n\n<div class="content">\n\n<!-- MARCHÉ -->\n<div class="view on" id="view-marche">\n<div class="g4">\n<div class="st"><div class="sv" id="s-p">—</div><div class="sl">Prix R/NXC</div></div>\n<div class="st"><div class="sv gold" id="s-v">—</div><div class="sl">Vol. 24h</div></div>\n<div class="st"><div class="sv green" id="s-t">—</div><div class="sl">Trades 24h</div></div>\n<div class="st"><div class="sv purple" id="s-h">—</div><div class="sl">Hist. pts</div></div>\n<div class="st"><div class="sv green" id="s-hi">—</div><div class="sl">Haut 24h</div></div>\n<div class="st"><div class="sv red" id="s-lo">—</div><div class="sl">Bas 24h</div></div>\n<div class="st"><div class="sv" id="s-var">—</div><div class="sl">Variation</div></div>\n<div class="st"><div class="sv" style="color:#ff6eb4" id="s-cap">—</div><div class="sl">Cap. marché</div></div>\n</div>\n<div class="card cyan">\n<div class="ct">◈ HISTORIQUE DU COURS\n<div style="display:flex;gap:5px">\n<button class="btn" onclick="setRange(25)" style="padding:3px 8px;font-size:9px">25</button>\n<button class="btn cyan" onclick="setRange(50)" style="padding:3px 8px;font-size:9px">50</button>\n<button class="btn" onclick="setRange(100)" style="padding:3px 8px;font-size:9px">100</button>\n</div>\n</div>\n<div class="chart-wrap ch200"><canvas id="ch"></canvas></div>\n<div style="display:flex;gap:6px;margin-top:8px;flex-wrap:wrap">\n<button class="btn gold" onclick="chObj&&chObj.zoom(1.5)">🔍+</button>\n<button class="btn gold" onclick="chObj&&chObj.zoom(0.7)">🔍−</button>\n<button class="btn" onclick="chObj&&chObj.resetZoom()">Reset</button>\n<button class="btn cyan" onclick="toggleChartType()">📊 Type</button>\n<button class="btn purple" onclick="dlChart()">⬇️ PNG</button>\n</div>\n</div>\n<div class="card"><div class="ct">◈ ALERTES MARCHÉ</div><div id="al"></div></div>\n<div class="card gold"><div class="ct">◈ RSI (14 ticks)</div><div class="chart-wrap ch150"><canvas id="ch-rsi"></canvas></div><div style="font-size:10px;color:var(--muted);margin-top:4px">RSI >70 = surachat · RSI <30 = survente</div></div>\n</div>\n\n<!-- CONTRÔLE -->\n<div class="view" id="view-trading">\n<div class="card cyan">\n<div class="ct">◈ MODIFIER LE COURS</div>\n<div class="sec">Raccourcis ±%</div>\n<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:10px">\n<button class="btn green" onclick="adjP(.05)">+5%</button>\n<button class="btn green" onclick="adjP(.02)">+2%</button>\n<button class="btn green" onclick="adjP(.01)">+1%</button>\n<button class="btn green" onclick="adjP(.005)">+0.5%</button>\n<button class="btn red" onclick="adjP(-.005)">-0.5%</button>\n<button class="btn red" onclick="adjP(-.01)">-1%</button>\n<button class="btn red" onclick="adjP(-.02)">-2%</button>\n<button class="btn red" onclick="adjP(-.05)">-5%</button>\n</div>\n<div class="sec">Prix exact</div>\n<div class="row"><input id="np" type="number" min="50" max="100000" placeholder="Prix (50–100 000)" class="grow"><button class="btn primary" onclick="setP()">✓</button></div>\n<div class="sec">Variation %</div>\n<div class="row"><input id="np-pct" type="number" placeholder="Ex: +10 ou -5" class="grow"><button class="btn cyan" onclick="setPct()">Appliquer</button></div>\n<div id="pm" style="font-size:11px;font-weight:600;min-height:14px;margin-top:4px"></div>\n</div>\n<div class="card">\n<div class="ct">◈ TENDANCE AUTO <span id="tt-timer" style="font-family:monospace;font-size:10px;color:var(--muted)"></span></div>\n<select id="ts" style="margin-bottom:8px">\n<option value="0.001">Ultra lent 0.1%</option>\n<option value="0.002">Très lent 0.2%</option>\n<option value="0.005" selected>Lent 0.5%</option>\n<option value="0.01">Moyen 1%</option>\n<option value="0.02">Rapide 2%</option>\n<option value="0.05">Très rapide 5%</option>\n<option value="0.1">Extrême 10%</option>\n</select>\n<select id="ti" style="margin-bottom:8px">\n<option value="5000">5s</option>\n<option value="12000" selected>12s</option>\n<option value="30000">30s</option>\n<option value="60000">1min</option>\n</select>\n<div class="sec">Amplitude de variation par tick</div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="noise-slider" type="range" min="1" max="10" value="4" oninput="updateNoise(this.value)" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--cyan)">\n<span id="noise-val" style="color:var(--cyan);font-weight:700;font-size:13px;width:48px;text-align:right;flex-shrink:0">0.4%</span>\n</div>\n<div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:8px">\n<button class="btn green" onclick="setT(\'up\')">📈 Hausse</button>\n<button class="btn red" onclick="setT(\'down\')">📉 Baisse</button>\n<button class="btn purple" onclick="setT(\'random\')">🎲 Aléatoire</button>\n<button class="btn" onclick="setT(\'stop\')" style="color:var(--muted)">⏸ Stop</button>\n</div>\n<div id="tst" style="font-size:12px;color:var(--muted);font-weight:600;padding:8px;background:var(--bg3);border-radius:8px;text-align:center">⏸ Arrêté</div>\n</div>\n<div class="card gold">\n<div class="ct">◈ SCÉNARIOS</div>\n<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">\n<button class="btn gold" onclick="scenario(\'crash\')">💥 Crash −30%</button>\n<button class="btn gold" onclick="scenario(\'moon\')">🚀 Moon +30%</button>\n<button class="btn gold" onclick="scenario(\'volatile\')">⚡ Volatil</button>\n<button class="btn gold" onclick="scenario(\'stable\')">😴 Stabiliser</button>\n<button class="btn gold" onclick="scenario(\'ath\')">🏆 ATH</button>\n<button class="btn gold" onclick="scenario(\'floor\')">🛑 Plancher 200R</button>\n</div>\n</div>\n<div class="card green">\n<div class="ct">◈ COURS NORMAL (PLANCHER + PLAFOND)</div>\n<div class="row" style="margin-bottom:8px">\n<input id="t-floor" type="number" placeholder="Plancher min (R)" class="grow">\n<button class="btn green" onclick="setFloor()">✓ Plancher</button>\n<button class="btn red" onclick="_cfgFloor=null;updFloorDisplay()" style="padding:10px 12px">✕</button>\n</div>\n<div class="row" style="margin-bottom:8px">\n<input id="t-ceil" type="number" placeholder="Plafond max (R)" class="grow">\n<button class="btn green" onclick="setCeil()">✓ Plafond</button>\n<button class="btn red" onclick="_cfgCeil=null;updFloorDisplay()" style="padding:10px 12px">✕</button>\n</div>\n<div id="floor-display" style="font-size:11px;padding:8px;background:var(--bg3);border-radius:8px;color:var(--muted)">Plancher: non défini · Plafond: non défini</div>\n<button class="btn green full" style="margin-top:8px" onclick="setNormalMode()">📊 Activer cours normal</button>\n<div style="font-size:10px;color:var(--muted);margin-top:4px">Le prix fluctue librement mais reste entre le plancher et le plafond</div>\n</div>\n<div class="card"><div class="ct">◈ RESET</div>\n<button class="btn full" style="color:var(--gold);border-color:rgba(255,176,32,.3);background:rgba(255,176,32,.06)" onclick="resetH()">🔄 Reset historique</button>\n<button class="btn full red" onclick="if(confirm(\'Reset complet ?\'))resetH()">⚠️ Reset complet</button>\n</div>\n</div>\n\n<!-- BANQUE -->\n<div class="view" id="view-banque">\n<div class="g4">\n<div class="st"><div class="sv" style="color:#00b4d8;font-size:14px" id="bk-r">—</div><div class="sl">Réserves</div></div>\n<div class="st"><div class="sv gold" style="font-size:14px" id="bk-i">—</div><div class="sl">Total entré</div></div>\n<div class="st"><div class="sv red" style="font-size:14px" id="bk-o">—</div><div class="sl">Total sorti</div></div>\n<div class="st"><div class="sv green" style="font-size:14px" id="bk-rt">—</div><div class="sl">Ratio</div></div>\n<div class="st"><div class="sv purple" style="font-size:14px" id="bk-nx">—</div><div class="sl">NXC émis</div></div>\n<div class="st"><div class="sv" style="font-size:14px;color:#4ea8de" id="bk-vx">—</div><div class="sl">Val. stock</div></div>\n<div class="st"><div class="sv" style="font-size:14px" id="bk-bn">—</div><div class="sl">Bénéfice</div></div>\n<div class="st"><div class="sv" style="font-size:14px;color:#ff6eb4" id="bk-fl">—</div><div class="sl">Nb flux</div></div>\n</div>\n<div class="card cyan">\n<div class="ct">◈ OPÉRATIONS</div>\n<div class="row" style="margin-bottom:8px">\n<input id="bk-amt" type="number" placeholder="Montant (R)" class="grow">\n<button class="btn green" onclick="bankOp(\'in\')">+ Injecter</button>\n<button class="btn red" onclick="bankOp(\'out\')">− Retirer</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px">\n<button class="btn cyan" onclick="setAmt(100)" style="font-size:11px;padding:6px 10px">100</button>\n<button class="btn cyan" onclick="setAmt(500)" style="font-size:11px;padding:6px 10px">500</button>\n<button class="btn cyan" onclick="setAmt(1000)" style="font-size:11px;padding:6px 10px">1 000</button>\n<button class="btn cyan" onclick="setAmt(5000)" style="font-size:11px;padding:6px 10px">5 000</button>\n<button class="btn cyan" onclick="setAmt(10000)" style="font-size:11px;padding:6px 10px">10 000</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap">\n<button class="btn gold" onclick="bankResetHist()" style="font-size:11px">🗑️ Reset hist.</button>\n<button class="btn red" onclick="bankResetAll()" style="font-size:11px">💥 Reset complet</button>\n<button class="btn purple" onclick="loadBank()" style="font-size:11px">🔄 Actualiser</button>\n<button class="btn" onclick="exportFlux()" style="font-size:11px">📊 CSV</button>\n</div>\n<div id="bk-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:8px"></div>\n</div>\n<div class="card">\n<div class="ct">◈ FLUX\n<div style="display:flex;gap:4px">\n<button class="btn cyan" id="fl-all" onclick="filterFlux(\'all\')" style="padding:3px 7px;font-size:9px">Tous</button>\n<button class="btn" id="fl-in" onclick="filterFlux(\'IN\')" style="padding:3px 7px;font-size:9px">Entrées</button>\n<button class="btn" id="fl-out" onclick="filterFlux(\'OUT\')" style="padding:3px 7px;font-size:9px">Sorties</button>\n</div>\n</div>\n<div id="bk-flux" style="max-height:220px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"></div>\n</div>\n<div class="card red">\n<div class="ct" style="color:var(--red)">⚠️ TENTATIVES ÉCHOUÉES <span id="fails-ct" style="display:none;background:var(--red);color:#000;border-radius:20px;padding:1px 7px;font-size:9px"></span></div>\n<div id="bk-fails" style="max-height:220px;overflow-y:auto"></div>\n</div>\n</div>\n\n<!-- APP -->\n<div class="view" id="view-nexus">\n<div style="padding:12px;background:var(--bg2);border-bottom:1px solid var(--border)">\n<div id="pinned-bar" style="display:none;gap:6px;flex-wrap:wrap;margin-bottom:8px;padding:6px;background:rgba(255,176,32,.05);border:1px solid rgba(255,176,32,.15);border-radius:10px"></div>\n<div class="row" style="margin-bottom:8px">\n<input id="iframe-in" type="url" placeholder="https://..." class="grow" onkeydown="if(event.key===\'Enter\')goUrl()">\n<button class="btn primary" onclick="goUrl()">▶</button>\n</div>\n<div id="saved-sites" style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px"></div>\n<div class="row">\n<input id="site-lbl" placeholder="Nom" style="flex:1;margin:0;font-size:12px;padding:8px 10px">\n<button class="btn gold" onclick="saveSite()" style="font-size:11px">💾 Sauver</button>\n<button class="btn cyan" onclick="reloadF()" style="font-size:11px">🔄</button>\n<button class="btn" onclick="openNewTab()" style="font-size:11px">↗</button>\n</div>\n</div>\n<div class="ibar">\n<span style="color:var(--cyan);font-size:12px;font-weight:800" id="if-title">◈ App</span>\n<span class="iurl" id="if-url">—</span>\n</div>\n<iframe id="nf" src="about:blank" allow="clipboard-write" style="flex:1;border:none;width:100%;min-height:calc(100dvh - 200px)"></iframe>\n</div>\n\n<!-- ADMIN -->\n<div class="view" id="view-admin">\n<div class="card cyan">\n<div class="ct">◈ STATISTIQUES SERVEUR EN TEMPS RÉEL</div>\n<div class="g4" id="adm-stats">\n<div class="st"><div class="sv" id="adm-price">—</div><div class="sl">Prix actuel</div></div>\n<div class="st"><div class="sv gold" id="adm-vol">—</div><div class="sl">Vol. 24h</div></div>\n<div class="st"><div class="sv green" id="adm-trades">—</div><div class="sl">Trades</div></div>\n<div class="st"><div class="sv purple" id="adm-users">—</div><div class="sl">Utilisateurs</div></div>\n<div class="st"><div class="sv" style="color:#00b4d8" id="adm-res">—</div><div class="sl">Réserves</div></div>\n<div class="st"><div class="sv gold" id="adm-nxc">—</div><div class="sl">NXC émis</div></div>\n<div class="st"><div class="sv green" id="adm-fails">—</div><div class="sl">Tentatives échouées</div></div>\n<div class="st"><div class="sv" id="adm-hist">—</div><div class="sl">Points hist.</div></div>\n</div>\n<button class="btn cyan" onclick="refreshAdminStats()" style="width:100%;margin-top:4px;padding:10px">🔄 Actualiser tout</button>\n</div>\n<div class="card green"><div class="ct">◈ SAUVEGARDE ET IMPORT DES DONNÉES</div><button class="btn green full" onclick="saveAllData()">💾 Sauvegarder toutes les données (JSON)</button><button class="btn cyan full" onclick="importData()">📥 Importer depuis un fichier JSON</button><button class="btn purple full" onclick="printDashboard()">🖨️ Imprimer le tableau de bord</button><div id="data-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:4px"></div></div>\n<div class="card gold">\n<div class="ct">◈ DONNER DES REWARDS À UN UTILISATEUR</div>\n<div class="row" style="margin-bottom:8px">\n<select id="rw-u" class="grow" style="margin:0"><option value="">Utilisateur...</option></select>\n<input id="rw-amt" type="number" placeholder="Montant" style="width:100px;margin:0;flex-shrink:0">\n<button class="btn gold" onclick="giveRewards()">💰 Donner</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:4px">\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=50" style="font-size:11px;padding:6px 10px">50</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=100" style="font-size:11px;padding:6px 10px">100</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=500" style="font-size:11px;padding:6px 10px">500</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=1000" style="font-size:11px;padding:6px 10px">1 000</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=5000" style="font-size:11px;padding:6px 10px">5 000</button>\n</div>\n<div id="rw-msg" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n<div class="card purple">\n<div class="ct">◈ CHANGER LE RÔLE D\'UN UTILISATEUR</div>\n<div class="row">\n<select id="role-u" class="grow" style="margin:0"><option value="">Utilisateur...</option></select>\n<select id="role-v" style="width:auto;margin:0;flex-shrink:0;padding:12px 8px">\n<option value="user">user</option>\n<option value="admin">admin</option>\n<option value="moderator">moderator</option>\n<option value="vip">vip</option>\n</select>\n<button class="btn purple" onclick="changeRole()">✓</button>\n</div>\n<div id="role-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:6px"></div>\n</div>\n<div class="card">\n<div class="ct">◈ LISTE COMPLÈTE DES UTILISATEURS</div>\n<div class="sw" style="margin-bottom:8px"><input id="adm-q" placeholder="Rechercher..." oninput="filterAdmUsers()"></div>\n<div class="tbl-wrap">\n<table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur</th></tr></thead>\n<tbody id="adm-ut"></tbody></table>\n</div>\n</div>\n<div class="card red">\n<div class="ct">◈ ACTIONS DE MAINTENANCE</div>\n<button class="btn full" style="color:var(--gold);border-color:rgba(255,176,32,.3);background:rgba(255,176,32,.06)" onclick="pruneHistory()">✂️ Réduire historique NXC (100 pts)</button>\n<button class="btn full red" onclick="resetAllTrades()">🗑️ Reset trades 24h</button>\n<button class="btn full" style="color:var(--cyan);border-color:rgba(0,229,255,.3);background:rgba(0,229,255,.06)" onclick="backupDB()">💾 Backup base de données JSON</button>\n<button class="btn full" style="color:var(--purple);border-color:rgba(160,107,255,.3);background:rgba(160,107,255,.06)" onclick="pingServer()">📡 Ping serveur</button>\n<div id="maint-msg" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n<div class="card purple">\n<div class="ct">◈ LOGS SYSTÈME</div>\n<div style="display:flex;gap:6px;margin-bottom:8px">\n<button class="btn purple" onclick="renderLog()" style="font-size:11px">🔄 Actualiser</button>\n<button class="btn red" onclick="_log=[];renderLog()" style="font-size:11px">🗑️ Vider</button>\n</div>\n<div id="log-list" style="max-height:250px;overflow-y:auto;border-radius:10px;border:1px solid rgba(160,107,255,.1)">\n<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun log</p>\n</div>\n</div>\n</div>\n\n<!-- UTILISATEURS -->\n<div class="view" id="view-users">\n<div class="g3">\n<div class="st"><div class="sv" id="u-total">—</div><div class="sl">Comptes</div></div>\n<div class="st"><div class="sv gold" id="u-admins">—</div><div class="sl">Admins</div></div>\n<div class="st"><div class="sv green" id="u-rew">—</div><div class="sl">Total rewards</div></div>\n</div>\n<div class="card">\n<div class="ct">◈ UTILISATEURS\n<div style="display:flex;gap:4px">\n<button class="btn cyan" onclick="sortU(\'rew\')" style="padding:3px 7px;font-size:9px">Rewards</button>\n<button class="btn" onclick="sortU(\'nxc\')" style="padding:3px 7px;font-size:9px">NXC</button>\n<button class="btn" onclick="sortU(\'name\')" style="padding:3px 7px;font-size:9px">A-Z</button>\n</div>\n</div>\n<div class="sw" style="margin-bottom:8px"><input id="us-q" placeholder="Rechercher..." oninput="filterU()"></div>\n<div class="tbl-wrap">\n<table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur R</th></tr></thead>\n<tbody id="ut"></tbody></table>\n</div>\n<div id="us-msg" style="font-size:11px;color:var(--muted);margin-top:8px;text-align:center"></div>\n</div>\n</div>\n\n<!-- STATS -->\n<div class="view" id="view-stats">\n<div class="card purple"><div class="ct">◈ VOLUME 24H</div><div class="chart-wrap ch150"><canvas id="ch-vol"></canvas></div></div>\n<div class="card gold"><div class="ct">◈ REWARDS PAR UTILISATEUR</div><div id="rew-bars"></div></div>\n<div class="card"><div class="ct">◈ SANTÉ DU MARCHÉ</div><div class="g2" id="health-grid"></div></div>\n</div>\n\n<!-- SOLVABILITÉ -->\n<div class="view" id="view-solv">\n<div class="card">\n<div class="ct">◈ SOLVABILITÉ</div>\n<div style="display:flex;align-items:center;gap:14px;padding:14px;background:var(--bg3);border-radius:12px;margin-bottom:12px;cursor:pointer" onclick="toggleSolv()">\n<div class="tg" id="stg"><div class="tg-k"></div></div>\n<div id="sl" style="font-size:14px;font-weight:700;color:var(--muted)">Désactivée</div>\n</div>\n<div class="row" style="margin-bottom:8px">\n<span style="font-size:12px;color:var(--muted);white-space:nowrap;flex-shrink:0">Geste commercial :</span>\n<input id="sg" type="number" value="50" class="grow">\n<button class="btn primary" onclick="saveSolv()">Sauver</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:6px">\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=10" style="font-size:11px">10R</button>\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=50" style="font-size:11px">50R</button>\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=100" style="font-size:11px">100R</button>\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=500" style="font-size:11px">500R</button>\n</div>\n<div id="sm" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n</div>\n\n<!-- OUTILS -->\n<div class="view" id="view-tools">\n<div class="card cyan">\n<div class="ct">◈ CALCULATRICE NXC ↔ REWARDS</div>\n<div class="row" style="margin-bottom:8px">\n<input id="c-nxc" type="number" placeholder="NXC" class="grow" oninput="calcN()">\n<span style="color:var(--muted);font-size:18px">→</span>\n<input id="c-rew" type="number" placeholder="Rewards R" class="grow" readonly style="background:rgba(0,229,255,.05)">\n</div>\n<div class="row">\n<input id="c-rew2" type="number" placeholder="Rewards R" class="grow" oninput="calcR()">\n<span style="color:var(--muted);font-size:18px">→</span>\n<input id="c-nxc2" type="number" placeholder="NXC" class="grow" readonly style="background:rgba(0,229,255,.05)">\n</div>\n</div>\n<div class="card gold">\n<div class="ct">◈ SIMULATEUR DE VENTE</div>\n<div class="row" style="margin-bottom:8px">\n<input id="ss-nxc" type="number" placeholder="NXC à vendre" class="grow" oninput="simS()">\n<input id="ss-fee" type="number" placeholder="Frais %" value="0" style="width:90px;margin:0;flex-shrink:0" oninput="simS()">\n</div>\n<div id="ss-res" style="padding:12px;background:var(--bg3);border-radius:10px;min-height:44px;font-size:13px"></div>\n</div>\n<div class="card purple">\n<div class="ct">◈ MINUTEUR ADMIN</div>\n<div class="row" style="margin-bottom:8px">\n<input id="tm-m" type="number" placeholder="Min" value="5" class="grow">\n<input id="tm-s" type="number" placeholder="Sec" value="0" class="grow">\n<select id="tm-a" style="flex:1;margin:0;font-size:12px">\n<option value="stop">Arrêter tendance</option>\n<option value="up">Lancer hausse</option>\n<option value="down">Lancer baisse</option>\n<option value="crash">Crash -30%</option>\n<option value="moon">Moon +30%</option>\n</select>\n</div>\n<button class="btn cyan full" onclick="startTimer()">⏱️ Démarrer</button>\n<button class="btn full" style="color:var(--muted)" onclick="stopTimer()">✕ Annuler</button>\n<div id="tm-disp" style="font-family:monospace;font-size:36px;font-weight:900;color:var(--cyan);text-align:center;padding:10px;min-height:56px"></div>\n</div>\n<div class="card green">\n<div class="ct">◈ PING SERVEUR</div>\n<button class="btn green full" onclick="pingServer()">📡 Tester</button>\n<div id="ping-res" style="font-size:13px;font-weight:700;text-align:center;padding:10px;min-height:36px"></div>\n</div>\n</div>\n\n<!-- JOURNAL -->\n<div class="view" id="view-log">\n<div class="card">\n<div class="ct">◈ JOURNAL ADMIN <button class="btn red" onclick="_log=[];renderLog()" style="padding:3px 8px;font-size:9px">Vider</button></div>\n<div id="log-list2" style="max-height:500px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)">\n<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun log</p>\n</div>\n</div>\n</div>\n\n<!-- CONFIG -->\n<div class="view" id="view-config">\n<div class="card purple">\n<div class="ct">◈ PLANCHER / PLAFOND AUTOMATIQUES</div>\n<div class="row" style="margin-bottom:8px">\n<input id="cfg-fl" type="number" placeholder="Plancher min (R)" class="grow">\n<button class="btn purple" onclick="_cfgFloor=parseFloat(document.getElementById(\'cfg-fl\').value)||null;updCfg();updFloorDisplay()">✓ Plancher</button>\n<button class="btn red" onclick="_cfgFloor=null;updCfg();updFloorDisplay()" style="padding:10px">✕</button>\n</div>\n<div class="row" style="margin-bottom:8px">\n<input id="cfg-cl" type="number" placeholder="Plafond max (R)" class="grow">\n<button class="btn purple" onclick="_cfgCeil=parseFloat(document.getElementById(\'cfg-cl\').value)||null;updCfg();updFloorDisplay()">✓ Plafond</button>\n<button class="btn red" onclick="_cfgCeil=null;updCfg();updFloorDisplay()" style="padding:10px">✕</button>\n</div>\n<div id="cfg-info" style="font-size:11px;color:var(--muted);padding:8px;background:var(--bg3);border-radius:8px">Plancher: non défini · Plafond: non défini</div>\n</div>\n<div class="card gold">\n<div class="ct">◈ TENDANCE PROGRAMMÉE</div>\n<div class="row" style="margin-bottom:8px">\n<input id="cfg-st" type="time" class="grow">\n<input id="cfg-sp" type="time" class="grow">\n<select id="cfg-sd" style="flex:1;margin:0"><option value="up">Hausse</option><option value="down">Baisse</option><option value="random">Aléatoire</option></select>\n</div>\n<button class="btn gold full" onclick="scheduleT()">⏰ Programmer</button>\n<button class="btn full" style="color:var(--muted)" onclick="if(_schedInt){clearInterval(_schedInt);_schedInt=null;document.getElementById(\'cfg-sch-msg\').textContent=\'Annulé\';}">✕ Annuler</button>\n<div id="cfg-sch-msg" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n<div class="card cyan">\n<div class="ct">◈ EXPORTS</div>\n<button class="btn cyan full" onclick="exportHist()">📥 Historique JSON</button>\n<button class="btn purple full" onclick="exportStats()">📊 Rapport complet JSON</button>\n<button class="btn gold full" onclick="exportFlux()">💰 Flux bancaires CSV</button>\n</div>\n</div>\n\n<!-- ALERTES -->\n<div class="view" id="view-notifs">\n<div class="card gold">\n<div class="ct">◈ ALERTES DE PRIX</div>\n<div class="row" style="margin-bottom:8px">\n<input id="al-p" type="number" placeholder="Prix cible (R)" class="grow">\n<select id="al-d" style="width:auto;flex-shrink:0;margin:0;font-size:12px;padding:10px 8px"><option value="above">Si &gt;</option><option value="below">Si &lt;</option></select>\n<button class="btn gold" onclick="addAlert()">+ Alerte</button>\n</div>\n<div id="al-list" style="max-height:200px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"><p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune alerte</p></div>\n</div>\n<div class="card"><div class="ct">◈ ALERTES INTELLIGENTES</div><div id="smart-al"></div></div>\n<div class="card purple">\n<div class="ct">◈ HISTORIQUE ALERTES <button class="btn red" onclick="_alHist=[];renderAlHist()" style="padding:3px 7px;font-size:9px">Vider</button></div>\n<div id="al-hist" style="max-height:200px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"><p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune</p></div>\n</div>\n</div>\n\n\n<!-- CYCLES DE MARCHÉ -->\n<div class="view" id="view-cycles">\n\n<!-- MODAL INFO -->\n<div id="info-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:500;align-items:center;justify-content:center;padding:20px" onclick="this.style.display=\'none\'">\n<div style="background:var(--bg2);border:1px solid var(--border);border-radius:16px;padding:20px;max-width:340px;width:100%" onclick="event.stopPropagation()">\n<div style="font-weight:700;color:var(--cyan);margin-bottom:10px;font-size:14px" id="info-title">Info</div>\n<div style="font-size:13px;color:var(--muted);line-height:1.7" id="info-body"></div>\n<button onclick="$(\'info-modal\').style.display=\'none\'" style="margin-top:14px;width:100%;padding:10px;background:var(--bg3);border:1px solid var(--border);border-radius:10px;color:var(--text);cursor:pointer;font-weight:700">Fermer</button>\n</div>\n</div>\n\n<div class="card cyan">\n<div class="ct">◈ BORNES DU NXC <button onclick="showInfo(\'bornes\')" style="background:none;border:1px solid rgba(0,229,255,.3);border-radius:50%;width:18px;height:18px;color:var(--cyan);font-size:9px;cursor:pointer;padding:0">i</button></div>\n<div class="g2">\n<div>\n<div class="sec">Prix minimum absolu (R)</div>\n<div class="row"><input id="cy-absmin" type="number" min="1" placeholder="Ex: 100" class="grow"><button class="btn cyan" onclick="setCyVal(\'absmin\')">✓</button></div>\n<div id="cy-absmin-disp" style="font-size:10px;color:var(--green);margin-top:2px">Non défini</div>\n</div>\n<div>\n<div class="sec">Prix maximum absolu (R)</div>\n<div class="row"><input id="cy-absmax" type="number" placeholder="Ex: 50000" class="grow"><button class="btn cyan" onclick="setCyVal(\'absmax\')">✓</button></div>\n<div id="cy-absmax-disp" style="font-size:10px;color:var(--red);margin-top:2px">Non défini</div>\n</div>\n</div>\n</div>\n\n<div class="card gold">\n<div class="ct">◈ FRÉQUENCE DES EXTRÊMES PAR PÉRIODE <button onclick="showInfo(\'freq\')" style="background:none;border:1px solid rgba(255,176,32,.3);border-radius:50%;width:18px;height:18px;color:var(--gold);font-size:9px;cursor:pointer;padding:0">i</button></div>\n<div style="font-size:11px;color:var(--muted);margin-bottom:12px;padding:8px;background:var(--bg3);border-radius:8px">\nDéfinir combien de fois le NXC touchera son <b style="color:var(--green)">minimum</b> ou son <b style="color:var(--red)">maximum</b> dans chaque période. Le moteur calcule automatiquement la probabilité par tick.\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:4px">\n<span style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">Période</span>\n<span style="font-size:9px;color:var(--green);text-transform:uppercase;letter-spacing:1px;text-align:center">× Min</span>\n<span style="font-size:9px;color:var(--red);text-transform:uppercase;letter-spacing:1px;text-align:center">× Max</span>\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">📅 Par minute <button onclick="showInfo(\'freq-min\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-m" type="number" min="0" value="0" placeholder="0" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-m" type="number" min="0" value="0" placeholder="0" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">🕐 Par heure <button onclick="showInfo(\'freq-h\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-h" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-h" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">📆 Par jour <button onclick="showInfo(\'freq-d\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-d" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-d" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">📅 Par semaine <button onclick="showInfo(\'freq-w\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-w" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-w" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">🗓️ Par mois <button onclick="showInfo(\'freq-mo\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-mo" type="number" min="0" value="2" placeholder="2" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-mo" type="number" min="0" value="2" placeholder="2" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">📅 Par an <button onclick="showInfo(\'freq-y\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-y" type="number" min="0" value="4" placeholder="4" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-y" type="number" min="0" value="4" placeholder="4" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="margin-top:10px;padding-top:10px;border-top:1px solid rgba(255,176,32,.15)">\n<div style="font-size:10px;color:var(--muted);margin-bottom:6px;display:flex;align-items:center;gap:6px">Durée personnalisée <button onclick="showInfo(\'freq-custom\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:grid;grid-template-columns:auto auto 1fr 1fr;gap:8px;align-items:center">\n<input id="cy-custom-dur" type="number" placeholder="X" style="width:60px;padding:8px;margin:0;text-align:center">\n<select id="cy-custom-unit" style="width:auto;margin:0;font-size:11px;padding:8px 6px">\n<option value="60000">min</option>\n<option value="3600000" selected>h</option>\n<option value="86400000">j</option>\n<option value="604800000">sem</option>\n</select>\n<input id="cy-min-c" type="number" min="0" value="0" placeholder="Min" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-c" type="number" min="0" value="0" placeholder="Max" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n</div>\n\n<button class="btn gold" onclick="updateCyProb()" style="width:100%;margin-top:12px;padding:10px">🔄 Calculer les probabilités par tick</button>\n<div id="cy-prob-display" style="font-size:11px;color:var(--muted);margin-top:8px;padding:8px;background:var(--bg3);border-radius:8px;line-height:1.8"></div>\n</div>\n\n<div class="card purple">\n<div class="ct">◈ COMPORTEMENT DES CYCLES <button onclick="showInfo(\'comportement\')" style="background:none;border:1px solid rgba(160,107,255,.3);border-radius:50%;width:18px;height:18px;color:var(--purple);font-size:9px;cursor:pointer;padding:0">i</button></div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Transition vers extrême <button onclick="showInfo(\'transition\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<select id="cy-transition" style="margin-bottom:10px">\n<option value="brutal">Brutal (saut immédiat)</option>\n<option value="progressif" selected>Progressif (descente/montée graduelle)</option>\n<option value="sinusoide">Sinusoïde (courbe naturelle)</option>\n</select>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Durée de maintien au min/max <button onclick="showInfo(\'hold\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div class="row" style="margin-bottom:10px">\n<input id="cy-hold-min" type="number" min="0" value="1" placeholder="Min" style="width:70px;flex-shrink:0;margin:0">\n<span style="color:var(--muted);font-size:12px;flex-shrink:0">à</span>\n<input id="cy-hold-max" type="number" min="0" value="3" placeholder="Max" style="width:70px;flex-shrink:0;margin:0">\n<select id="cy-hold-unit" style="flex:1;margin:0;font-size:12px;padding:10px 8px">\n<option value="1">ticks</option>\n<option value="5" selected>minutes</option>\n<option value="300">heures</option>\n</select>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Drift de fond (tendance long terme) <button onclick="showInfo(\'drift\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="cy-drift" type="range" min="-5" max="5" value="0" step="0.5" oninput="$(\'cy-drift-val\').textContent=this.value>0?\'+\'+this.value+\'%/j\':this.value+\'%/j\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--purple)">\n<span id="cy-drift-val" style="color:var(--purple);font-weight:700;font-size:13px;width:60px;text-align:right;flex-shrink:0">0%/j</span>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Volatilité de fond <button onclick="showInfo(\'volbg\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="cy-vol-bg" type="range" min="0" max="10" value="2" oninput="$(\'cy-vol-bg-val\').textContent=(this.value/10).toFixed(1)+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--cyan)">\n<span id="cy-vol-bg-val" style="color:var(--cyan);font-weight:700;font-size:13px;width:40px;text-align:right;flex-shrink:0">0.2%</span>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Probabilité de pic surprise <button onclick="showInfo(\'spike\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="cy-spike" type="range" min="0" max="20" value="2" oninput="$(\'cy-spike-val\').textContent=this.value+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--red)">\n<span id="cy-spike-val" style="color:var(--red);font-weight:700;font-size:13px;width:32px;text-align:right;flex-shrink:0">2%</span>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Amplitude des pics <button onclick="showInfo(\'spikeamp\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="cy-spike-amp" type="range" min="1" max="30" value="10" oninput="$(\'cy-spike-amp-val\').textContent=\'±\'+this.value+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--red)">\n<span id="cy-spike-amp-val" style="color:var(--red);font-weight:700;font-size:13px;width:40px;text-align:right;flex-shrink:0">±10%</span>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Rebond au plancher <button onclick="showInfo(\'bounce\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="cy-bounce" type="range" min="0" max="10" value="3" oninput="$(\'cy-bounce-val\').textContent=this.value+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--green)">\n<span id="cy-bounce-val" style="color:var(--green);font-weight:700;font-size:13px;width:32px;text-align:right;flex-shrink:0">3%</span>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Résistance au plafond <button onclick="showInfo(\'resist\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">\n<input id="cy-resist" type="range" min="0" max="10" value="3" oninput="$(\'cy-resist-val\').textContent=this.value+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--gold)">\n<span id="cy-resist-val" style="color:var(--gold);font-weight:700;font-size:13px;width:32px;text-align:right;flex-shrink:0">3%</span>\n</div>\n</div>\n\n<div class="card green">\n<div class="ct">◈ ACTIVATION <button onclick="showInfo(\'activation\')" style="background:none;border:1px solid rgba(0,255,157,.3);border-radius:50%;width:18px;height:18px;color:var(--green);font-size:9px;cursor:pointer;padding:0">i</button></div>\n<button class="btn green full" onclick="startCycles()" id="cy-start-btn">▶ Activer les cycles de marché</button>\n<button class="btn red full" onclick="stopCycles()" style="display:none" id="cy-stop-btn">⏸ Désactiver les cycles</button>\n<div id="cy-status" style="font-size:12px;padding:10px;background:var(--bg3);border-radius:10px;color:var(--muted);min-height:40px">Cycles désactivés</div>\n<div id="cy-next" style="font-size:11px;color:var(--muted);margin-top:6px"></div>\n</div>\n\n<div class="card">\n<div class="ct">◈ PRÉVISUALISATION <button onclick="showInfo(\'preview\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:18px;height:18px;color:var(--cyan);font-size:9px;cursor:pointer;padding:0">i</button></div>\n<div class="chart-wrap ch150"><canvas id="cy-preview"></canvas></div>\n<button class="btn cyan" onclick="previewCycle()" style="width:100%;margin-top:8px;padding:10px">🔮 Générer prévisualisation (100 ticks simulés)</button>\n</div>\n</div>\n\n<!-- PRÉVISIONS -->\n<div class="view" id="view-prevision">\n\n<div class="card cyan">\n<div class="ct">◈ PARAMÈTRES DE SIMULATION\n<button onclick="showInfo(\'prev-params\')" style="background:none;border:1px solid rgba(0,229,255,.3);border-radius:50%;width:18px;height:18px;color:var(--cyan);font-size:9px;cursor:pointer;padding:0">i</button>\n</div>\n<div class="g2">\n<div>\n<div class="sec">Durée de simulation</div>\n<div class="row">\n<input id="pv-dur" type="number" min="1" value="24" placeholder="Durée" class="grow">\n<select id="pv-unit" style="flex:1;margin:0;font-size:12px;padding:11px 8px">\n<option value="30">secondes</option>\n<option value="60">minutes</option>\n<option value="3600" selected>heures</option>\n<option value="86400">jours</option>\n<option value="604800">semaines</option>\n<option value="2592000">mois</option>\n<option value="31536000">années</option>\n</select>\n</div>\n</div>\n<div>\n<div class="sec">Nombre de points simulés</div>\n<select id="pv-pts" style="margin:0;width:100%">\n<option value="50">50 points (rapide)</option>\n<option value="100" selected>100 points</option>\n<option value="200">200 points</option>\n<option value="500">500 points (précis)</option>\n<option value="1000">1000 points (très précis)</option>\n</select>\n</div>\n</div>\n<div class="sec">Scenario de simulation\n<button onclick="showInfo(\'prev-scenario\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button>\n</div>\n<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-bottom:10px">\n<button class="btn cyan" id="pv-sc-current" onclick="setPvScenario(\'current\')" style="font-size:11px;padding:8px">📊 Paramètres actuels</button>\n<button class="btn" id="pv-sc-bull" onclick="setPvScenario(\'bull\')" style="font-size:11px;padding:8px">📈 Haussier</button>\n<button class="btn" id="pv-sc-bear" onclick="setPvScenario(\'bear\')" style="font-size:11px;padding:8px">📉 Baissier</button>\n<button class="btn" id="pv-sc-volatile" onclick="setPvScenario(\'volatile\')" style="font-size:11px;padding:8px">⚡ Volatile</button>\n<button class="btn" id="pv-sc-stable" onclick="setPvScenario(\'stable\')" style="font-size:11px;padding:8px">😴 Stable</button>\n<button class="btn" id="pv-sc-cycles" onclick="setPvScenario(\'cycles\')" style="font-size:11px;padding:8px">📅 Cycles</button>\n</div>\n<div class="sec">Prix de départ\n<button onclick="showInfo(\'prev-start\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button>\n</div>\n<div class="row" style="margin-bottom:8px">\n<input id="pv-start" type="number" placeholder="Prix actuel par défaut" class="grow">\n<button class="btn cyan" onclick="document.getElementById(\'pv-start\').value=parseFloat(mkt.price||5213).toFixed(2)">← Actuel</button>\n</div>\n<button class="btn primary" onclick="runSimulation()" style="width:100%;padding:12px;font-size:14px;font-weight:800">🔮 Lancer la simulation</button>\n<div id="pv-loading" style="display:none;text-align:center;padding:10px;color:var(--cyan);font-size:12px">⏳ Simulation en cours...</div>\n</div>\n\n<!-- RÉSULTAT PRINCIPAL -->\n<div id="pv-result-card" style="display:none">\n<div class="card gold">\n<div class="ct">◈ RÉSULTAT DE LA SIMULATION\n<button onclick="showInfo(\'prev-result\')" style="background:none;border:1px solid rgba(255,176,32,.3);border-radius:50%;width:18px;height:18px;color:var(--gold);font-size:9px;cursor:pointer;padding:0">i</button>\n</div>\n<div class="g4" id="pv-stats">\n</div>\n</div>\n\n<!-- GRAPHIQUE 1 : Évolution du prix -->\n<div class="card cyan">\n<div class="ct">◈ GRAPHIQUE 1 — ÉVOLUTION DU PRIX\n<button onclick="showInfo(\'prev-g1\')" style="background:none;border:1px solid rgba(0,229,255,.3);border-radius:50%;width:18px;height:18px;color:var(--cyan);font-size:9px;cursor:pointer;padding:0">i</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px;align-items:center">\n<span style="font-size:10px;color:var(--muted)">Axe Y:</span>\n<input id="pv-g1-ymin" type="number" placeholder="Min Y" style="width:80px;padding:6px;margin:0;font-size:11px">\n<input id="pv-g1-ymax" type="number" placeholder="Max Y" style="width:80px;padding:6px;margin:0;font-size:11px">\n<button class="btn cyan" onclick="updateG1()" style="padding:6px 10px;font-size:10px">↻ Appliquer</button>\n<button class="btn" onclick="zoomIn(\'g1\')" style="padding:6px 10px;font-size:10px">🔍+</button>\n<button class="btn" onclick="zoomOut(\'g1\')" style="padding:6px 10px;font-size:10px">🔍−</button>\n<button class="btn" onclick="resetZoom(\'g1\')" style="padding:6px 10px;font-size:10px">↺</button>\n</div>\n<div class="chart-wrap" style="height:240px"><canvas id="pv-g1"></canvas></div>\n</div>\n\n<!-- GRAPHIQUE 2 : Bandes de confiance -->\n<div class="card purple">\n<div class="ct">◈ GRAPHIQUE 2 — BANDES DE CONFIANCE (MIN / MOYEN / MAX)\n<button onclick="showInfo(\'prev-g2\')" style="background:none;border:1px solid rgba(160,107,255,.3);border-radius:50%;width:18px;height:18px;color:var(--purple);font-size:9px;cursor:pointer;padding:0">i</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px;align-items:center">\n<input id="pv-g2-ymin" type="number" placeholder="Min Y" style="width:80px;padding:6px;margin:0;font-size:11px">\n<input id="pv-g2-ymax" type="number" placeholder="Max Y" style="width:80px;padding:6px;margin:0;font-size:11px">\n<button class="btn purple" onclick="updateG2()" style="padding:6px 10px;font-size:10px">↻</button>\n<button class="btn" onclick="zoomIn(\'g2\')" style="padding:6px 10px;font-size:10px">🔍+</button>\n<button class="btn" onclick="zoomOut(\'g2\')" style="padding:6px 10px;font-size:10px">🔍−</button>\n<button class="btn" onclick="resetZoom(\'g2\')" style="padding:6px 10px;font-size:10px">↺</button>\n</div>\n<div style="display:flex;gap:12px;font-size:10px;margin-bottom:6px;flex-wrap:wrap">\n<span style="color:var(--red)">▬ Scenario pessimiste</span>\n<span style="color:var(--cyan)">▬ Scenario realiste</span>\n<span style="color:var(--green)">▬ Scenario optimiste</span>\n<span style="color:rgba(0,229,255,.2)">■ Zone d\'incertitude</span>\n</div>\n<div class="chart-wrap" style="height:220px"><canvas id="pv-g2"></canvas></div>\n</div>\n\n<!-- GRAPHIQUE 3 : Volatilité simulée -->\n<div class="card gold">\n<div class="ct">◈ GRAPHIQUE 3 — VOLATILITÉ SIMULÉE\n<button onclick="showInfo(\'prev-g3\')" style="background:none;border:1px solid rgba(255,176,32,.3);border-radius:50%;width:18px;height:18px;color:var(--gold);font-size:9px;cursor:pointer;padding:0">i</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px;align-items:center">\n<input id="pv-g3-ymax" type="number" placeholder="Max %" style="width:80px;padding:6px;margin:0;font-size:11px">\n<button class="btn gold" onclick="updateG3()" style="padding:6px 10px;font-size:10px">↻</button>\n<button class="btn" onclick="zoomIn(\'g3\')" style="padding:6px 10px;font-size:10px">🔍+</button>\n<button class="btn" onclick="zoomOut(\'g3\')" style="padding:6px 10px;font-size:10px">🔍−</button>\n</div>\n<div class="chart-wrap" style="height:180px"><canvas id="pv-g3"></canvas></div>\n</div>\n\n<!-- GRAPHIQUE 4 : Distribution des prix -->\n<div class="card green">\n<div class="ct">◈ GRAPHIQUE 4 — DISTRIBUTION DES PRIX (HISTOGRAMME)\n<button onclick="showInfo(\'prev-g4\')" style="background:none;border:1px solid rgba(0,255,157,.3);border-radius:50%;width:18px;height:18px;color:var(--green);font-size:9px;cursor:pointer;padding:0">i</button>\n</div>\n<div class="chart-wrap" style="height:180px"><canvas id="pv-g4"></canvas></div>\n</div>\n\n<!-- GRAPHIQUE 5 : Drawdown -->\n<div class="card red">\n<div class="ct">◈ GRAPHIQUE 5 — DRAWDOWN (CHUTE DEPUIS LE SOMMET)\n<button onclick="showInfo(\'prev-g5\')" style="background:none;border:1px solid rgba(255,61,94,.3);border-radius:50%;width:18px;height:18px;color:var(--red);font-size:9px;cursor:pointer;padding:0">i</button>\n</div>\n<div class="chart-wrap" style="height:180px"><canvas id="pv-g5"></canvas></div>\n</div>\n\n<!-- GRAPHIQUE 6 : Retour sur investissement -->\n<div class="card" style="border-color:rgba(255,110,180,.3)">\n<div class="ct" style="color:#ff6eb4">◈ GRAPHIQUE 6 — RETOUR SUR INVESTISSEMENT (ROI)\n<button onclick="showInfo(\'prev-g6\')" style="background:none;border:1px solid rgba(255,110,180,.3);border-radius:50%;width:18px;height:18px;color:#ff6eb4;font-size:9px;cursor:pointer;padding:0">i</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px;align-items:center">\n<span style="font-size:11px;color:var(--muted)">Mise initiale:</span>\n<input id="pv-roi-invest" type="number" placeholder="Ex: 1000" value="1000" style="width:100px;padding:6px;margin:0;font-size:11px">\n<span style="font-size:11px;color:var(--muted)">NXC achetés</span>\n<button class="btn" onclick="updateG6()" style="padding:6px 10px;font-size:10px;color:#ff6eb4;border-color:rgba(255,110,180,.3)">↻</button>\n</div>\n<div class="chart-wrap" style="height:180px"><canvas id="pv-g6"></canvas></div>\n</div>\n\n<div class="card">\n<div class="ct">◈ EXPORTER LA SIMULATION\n</div>\n<div style="display:flex;gap:8px;flex-wrap:wrap">\n<button class="btn cyan" onclick="exportSimJSON()" style="flex:1;padding:10px;font-size:12px">📥 Export JSON</button>\n<button class="btn purple" onclick="exportSimCSV()" style="flex:1;padding:10px;font-size:12px">📊 Export CSV</button>\n<button class="btn gold" onclick="printSim()" style="flex:1;padding:10px;font-size:12px">🖨️ Imprimer</button>\n</div>\n</div>\n</div><!-- end pv-result-card -->\n</div>\n\n</div><!-- end content -->\n\n<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>\n<script>\n\n// ══ ÉPINGLAGE SITES (sync cross-device via serveur) ══\nvar _pinnedSites=[];\n\nasync function loadPinnedSites(){\n  try{\n    var r=await fetch(\'/admin/pinned-sites\');var d=await r.json();\n    if(d.ok){_pinnedSites=d.sites||[];renderSavedSites();}\n  }catch(e){_pinnedSites=JSON.parse(localStorage.getItem(\'nxc_pinned\')||\'[]\');}\n}\n\nasync function togglePin(url,label){\n  var idx=_pinnedSites.findIndex(s=>s.url===url);\n  if(idx>=0)_pinnedSites.splice(idx,1);\n  else _pinnedSites.push({url,label});\n  // Sauvegarder sur le serveur ET en local\n  localStorage.setItem(\'nxc_pinned\',JSON.stringify(_pinnedSites));\n  try{await fetch(\'/admin/pinned-sites\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,sites:_pinnedSites})});}catch(e){}\n  renderSavedSites();\n  addLog(\'📌\',(idx>=0?\'Désépinglé\':\'Épinglé\')+\': \'+label);\n}\n\nfunction renderPinnedBar(){\n  var el=$(\'pinned-bar\');if(!el)return;\n  if(!_pinnedSites.length){el.style.display=\'none\';return;}\n  el.style.display=\'flex\';\n  el.innerHTML=_pinnedSites.map(s=>\'<button onclick="loadSite(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" style="padding:5px 12px;background:rgba(255,176,32,.12);border:1px solid rgba(255,176,32,.3);border-radius:8px;color:var(--gold);font-size:11px;font-weight:700;cursor:pointer;white-space:nowrap">📌 \'+esc(s.label)+\'</button>\').join(\'\');\n}\n\n// ══ SAUVEGARDE / IMPORT DONNÉES GLOBALES ══\nasync function saveAllData(){\n  try{\n    var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'export\'})});\n    var d=await r.json();\n    if(!d.ok){setMsg(\'data-msg\',\'❌ Erreur export\',false);return;}\n    var blob=new Blob([JSON.stringify(d.data,null,2)],{type:\'application/json\'});\n    var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_full_backup_\'+Date.now()+\'.json\';a.click();\n    setMsg(\'data-msg\',\'✅ Backup complet téléchargé\',true);\n    addLog(\'💾\',\'Sauvegarde complète téléchargée\');\n  }catch(e){setMsg(\'data-msg\',\'❌ Erreur: \'+e.message,false);}\n}\n\nfunction importData(){\n  var input=document.createElement(\'input\');input.type=\'file\';input.accept=\'.json\';\n  input.onchange=async function(e){\n    var file=e.target.files[0];if(!file)return;\n    var text=await file.text();\n    try{\n      var data=JSON.parse(text);\n      if(!confirm(\'Importer ces données ? Cela écrasera les données actuelles du serveur.\'))return;\n      var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'import\',data:data})});\n      var res=await r.json();\n      setMsg(\'data-msg\',res.ok?\'✅ Données importées avec succès\':\'❌ \'+(res.error||\'Erreur import\'),res.ok);\n      if(res.ok){addLog(\'📥\',\'Données importées depuis fichier\');setTimeout(function(){ref();loadBank();},1000);}\n    }catch(ex){setMsg(\'data-msg\',\'❌ Fichier JSON invalide\',false);}\n  };\n  input.click();\n}\n\n// ══ IMPRESSION ══\nfunction printDashboard(){\n  var p=parseFloat(mkt.price||0);var h=mkt.history||[];\n  var hi=h.length>1?Math.max(...h.slice(-24).map(x=>x.price)):p;\n  var lo=h.length>1?Math.min(...h.slice(-24).map(x=>x.price)):p;\n  var chg=_prevP>0?((p-_prevP)/_prevP*100):0;\n  // Capturer le graphique en PNG\n  var chartImg=\'\';var cv=$(\'ch\');if(cv)chartImg=cv.toDataURL(\'image/png\');\n  var rsiImg=\'\';var rsiCv=$(\'ch-rsi\');if(rsiCv)rsiImg=rsiCv.toDataURL(\'image/png\');\n  var now=new Date().toLocaleString(\'fr-FR\');\n  var win=window.open(\'\',\'_blank\');\n  win.document.write(\'<!DOCTYPE html><html><head><meta charset="utf-8"><title>◈ Nexus NXC — Rapport \'+now+\'</title><style>*{font-family:Arial,sans-serif;box-sizing:border-box}body{background:#fff;color:#000;padding:20px;max-width:900px;margin:0 auto}.header{text-align:center;border-bottom:3px solid #000;padding-bottom:16px;margin-bottom:20px}.title{font-size:28px;font-weight:900;letter-spacing:3px}.date{font-size:12px;color:#666;margin-top:4px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}.stat{border:1px solid #ddd;border-radius:8px;padding:12px;text-align:center}.stat-val{font-size:20px;font-weight:700;margin-bottom:4px}.stat-lbl{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:#666}img{max-width:100%;border:1px solid #ddd;border-radius:8px;margin-bottom:12px}h3{margin:16px 0 8px;font-size:14px;border-bottom:1px solid #eee;padding-bottom:4px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:8px;text-align:left;border:1px solid #ddd}th{background:#f5f5f5;font-weight:700}@media print{.no-print{display:none}}</style></head><body>\');\n  win.document.write(\'<div class="header"><div class="title">◈ NEXUS NXC</div><div class="date">Rapport généré le \'+now+\'</div></div>\');\n  win.document.write(\'<div class="grid">\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(p,2)+\' R</div><div class="stat-lbl">Prix actuel</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%</div><div class="stat-lbl">Variation</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(hi,0)+\' R</div><div class="stat-lbl">Haut 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(lo,0)+\' R</div><div class="stat-lbl">Bas 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(mkt.volume24||0,0)+\' R</div><div class="stat-lbl">Volume 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(mkt.trades24||0)+\'</div><div class="stat-lbl">Trades 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+h.length+\'</div><div class="stat-lbl">Points hist.</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(_users.length||0)+\'</div><div class="stat-lbl">Utilisateurs</div></div>\');\n  win.document.write(\'</div>\');\n  if(chartImg)win.document.write(\'<h3>Historique du cours (\'+_ctRange+\' derniers points)</h3><img src="\'+chartImg+\'">\');\n  if(rsiImg)win.document.write(\'<h3>RSI (14 ticks)</h3><img src="\'+rsiImg+\'">\');\n  if(_users.length){\n    win.document.write(\'<h3>Utilisateurs</h3><table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur (R)</th></tr></thead><tbody>\');\n    _users.forEach(u=>{win.document.write(\'<tr><td>\'+esc(u.n)+\'</td><td>\'+esc(u.role)+\'</td><td>\'+fmt(u.rew,0)+\'</td><td>\'+u.nxc.toFixed(4)+\'</td><td>\'+fmt(u.val,0)+\'</td></tr>\');});\n    win.document.write(\'</tbody></table>\');\n  }\n  win.document.write(\'<h3>Derniers logs</h3><table><thead><tr><th>Heure</th><th>Action</th></tr></thead><tbody>\');\n  _log.slice(0,20).forEach(l=>{win.document.write(\'<tr><td>\'+fmtT(l.ts)+\'</td><td>\'+l.ico+\' \'+esc(l.txt)+\'</td></tr>\');});\n  win.document.write(\'</tbody></table>\');\n  win.document.write(\'</body></html>\');\n  win.document.close();\n  setTimeout(function(){win.print();},500);\n  addLog(\'🖨️\',\'Impression du tableau de bord\');\n}\n\n\n// ══ CYCLES DE MARCHÉ ══\nvar _cy={absmin:null,absmax:null,active:false,int:null,phase:\'normal\',phaseStart:Date.now(),holdUntil:0};\nvar _cyPreviewObj=null;\n\nfunction setCyVal(key){\n  var v=parseFloat($(\'cy-\'+key).value);\n  if(isNaN(v)||v<=0)return;\n  _cy[key]=v;\n  var el=$(\'cy-\'+key+\'-disp\');if(el)el.textContent=fmt(v,0)+\' R\';\n  // Sync avec _cfgFloor/_cfgCeil\n  if(key===\'absmin\'){_cfgFloor=v;updFloorDisplay();}\n  if(key===\'absmax\'){_cfgCeil=v;updFloorDisplay();}\n  addLog(\'📅\',\'Borne \'+key+\': \'+fmt(v,0)+\' R\');\n}\n\nfunction getCyConfig(){\n  return {\n    absmin: _cy.absmin||parseFloat($(\'cy-absmin\').value)||50,\n    absmax: _cy.absmax||parseFloat($(\'cy-absmax\').value)||100000,\n    transition: $(\'cy-transition\').value,\n    holdMin: parseFloat($(\'cy-hold-min\').value)||1,\n    holdMax: parseFloat($(\'cy-hold-max\').value)||3,\n    holdUnit: parseFloat($(\'cy-hold-unit\').value)||60,\n    drift: parseFloat($(\'cy-drift\').value)/100/1440,\n    volBg: parseFloat($(\'cy-vol-bg\').value)/1000,\n    spikeProb: parseFloat($(\'cy-spike\').value)/100,\n    spikeAmp: parseFloat($(\'cy-spike-amp\').value)/100,\n    bounce: parseFloat($(\'cy-bounce\').value)/100,\n    resist: parseFloat($(\'cy-resist\').value)/100,\n    // Frequences par période → probabilité par tick (tick = 12s)\n    freqMin: {\n      m: parseFloat($(\'cy-min-m\').value)||0,\n      h: parseFloat($(\'cy-min-h\').value)||1,\n      d: parseFloat($(\'cy-min-d\').value)||1,\n      w: parseFloat($(\'cy-min-w\').value)||1,\n      mo: parseFloat($(\'cy-min-mo\').value)||2,\n      y: parseFloat($(\'cy-min-y\').value)||4,\n    },\n    freqMax: {\n      m: parseFloat($(\'cy-max-m\').value)||0,\n      h: parseFloat($(\'cy-max-h\').value)||1,\n      d: parseFloat($(\'cy-max-d\').value)||1,\n      w: parseFloat($(\'cy-max-w\').value)||1,\n      mo: parseFloat($(\'cy-max-mo\').value)||2,\n      y: parseFloat($(\'cy-max-y\').value)||4,\n    },\n  };\n}\n\nfunction calcProbPerTick(freqObj){\n  // Convertir les frequences en probabilité par tick (12s)\n  var ticksPerMin=5,ticksPerH=300,ticksPerD=7200,ticksPerW=50400,ticksPerMo=216000,ticksPerY=2628000;\n  var pMin=freqObj.m/ticksPerMin+freqObj.h/ticksPerH+freqObj.d/ticksPerD+freqObj.w/ticksPerW+freqObj.mo/ticksPerMo+freqObj.y/ticksPerY;\n  return Math.min(pMin,0.5); // max 50% par tick\n}\n\nfunction startCycles(){\n  var cfg=getCyConfig();\n  if(cfg.absmin>=cfg.absmax){alert(\'Le plancher doit être inférieur au plafond\');return;}\n  // Calculer automatiquement les probabilités\n  updateCyProb();\n  var pToMin=window._cyPMin||0;\n  var pToMax=window._cyPMax||0;\n  _cy.active=true;_cy.phase=\'normal\';_cy.holdUntil=0;\n  $(\'cy-start-btn\').style.display=\'none\';$(\'cy-stop-btn\').style.display=\'block\';\n  var iv=parseInt($(\'ti\').value)||12000;\n  if(tInt){clearInterval(tInt);tInt=null;}\n  if(_cy.int){clearInterval(_cy.int);_cy.int=null;}\n  tMode=\'cycles\';\n  var el=$(\'tst\');el.textContent=\'📅 Cycles actifs · \'+fmt(cfg.absmin,0)+\'R – \'+fmt(cfg.absmax,0)+\'R\';el.style.color=\'var(--cyan)\';\n  addLog(\'📅\',\'Cycles activés · P(min)=\'+(pToMin*100).toFixed(3)+\'% P(max)=\'+(pToMax*100).toFixed(3)+\'% /tick\');\n\n  _cy.int=setInterval(async function(){\n    var p=parseFloat(mkt.price||5213);\n    var now=Date.now();\n    var range=cfg.absmax-cfg.absmin;\n    var adj=0;\n\n    // Drift + volatilité de fond\n    adj+=cfg.drift+(Math.random()-0.5)*cfg.volBg*2;\n\n    // Pics surprises\n    if(Math.random()<cfg.spikeProb){\n      var dir=Math.random()>0.5?1:-1;\n      var spikeAdj=dir*cfg.spikeAmp*Math.random();\n      adj+=spikeAdj;\n      addLog(\'⚡\',\'Pic surprise: \'+(spikeAdj>0?\'+\':\'\')+(spikeAdj*100).toFixed(1)+\'%\');\n    }\n\n    if(now<_cy.holdUntil){\n      // Maintien : légère oscillation autour de l\'extrême\n      if(_cy.phase===\'atmin\'){adj=(Math.random()-0.3)*0.002;}\n      if(_cy.phase===\'atmax\'){adj=(Math.random()-0.7)*0.002;}\n    } else {\n      // Phase normale : décider si on part vers un extrême\n      if(_cy.phase===\'normal\'||_cy.phase===\'atmin\'||_cy.phase===\'atmax\'){\n        if(Math.random()<pToMin){_cy.phase=\'tomin\';addLog(\'📅\',\'→ Descente vers minimum (\'+fmt(cfg.absmin,0)+\'R)\');}\n        else if(Math.random()<pToMax){_cy.phase=\'tomax\';addLog(\'📅\',\'→ Montée vers maximum (\'+fmt(cfg.absmax,0)+\'R)\');}\n        else if(_cy.phase!==\'normal\'){_cy.phase=\'normal\';}\n      }\n\n      if(_cy.phase===\'tomin\'){\n        // Force proportionnelle à la distance — arrive en ~10 ticks\n        var dist=(p-cfg.absmin)/range;\n        var force;\n        if(cfg.transition===\'brutal\'){\n          p=cfg.absmin;adj=0;\n        } else if(cfg.transition===\'sinusoide\'){\n          force=-Math.sin(dist*Math.PI)*0.15-0.02;\n          adj+=force;\n        } else {\n          // Progressif : force proportionnelle, min 2% par tick\n          force=-(dist*0.3+0.02);\n          adj+=force;\n        }\n        if(p*(1+adj)<=cfg.absmin*1.005){\n          p=cfg.absmin;adj=0;\n          _cy.phase=\'atmin\';\n          var holdTicks=cfg.holdMin+Math.random()*(cfg.holdMax-cfg.holdMin);\n          _cy.holdUntil=now+holdTicks*cfg.holdUnit*12000;\n          addLog(\'📅\',\'✅ Minimum atteint: \'+fmt(cfg.absmin,0)+\'R · maintien \'+Math.round(holdTicks*cfg.holdUnit)+\'min\');\n        }\n      }\n\n      if(_cy.phase===\'tomax\'){\n        var dist=(cfg.absmax-p)/range;\n        var force;\n        if(cfg.transition===\'brutal\'){\n          p=cfg.absmax;adj=0;\n        } else if(cfg.transition===\'sinusoide\'){\n          force=Math.sin(dist*Math.PI)*0.15+0.02;\n          adj+=force;\n        } else {\n          force=dist*0.3+0.02;\n          adj+=force;\n        }\n        if(p*(1+adj)>=cfg.absmax*0.995){\n          p=cfg.absmax;adj=0;\n          _cy.phase=\'atmax\';\n          var holdTicks=cfg.holdMin+Math.random()*(cfg.holdMax-cfg.holdMin);\n          _cy.holdUntil=now+holdTicks*cfg.holdUnit*12000;\n          addLog(\'📅\',\'✅ Maximum atteint: \'+fmt(cfg.absmax,0)+\'R · maintien \'+Math.round(holdTicks*cfg.holdUnit)+\'min\');\n        }\n      }\n    }\n\n    p=Math.max(cfg.absmin,Math.min(cfg.absmax,p*(1+adj)));\n    p=Math.round(p*100)/100;\n\n    var rem=Math.max(0,Math.round((_cy.holdUntil-now)/1000));\n    var phaseLabel={\'normal\':\'Oscillation libre\',\'tomin\':\'↓ Descente vers min\',\'atmin\':\'🟢 Au minimum\',\'tomax\':\'↑ Montée vers max\',\'atmax\':\'🔴 Au maximum\'}[_cy.phase]||_cy.phase;\n    var st=$(\'cy-status\');\n    if(st)st.innerHTML=\'<b style="color:var(--cyan)">\'+phaseLabel+\'</b>\'+(_cy.holdUntil>now?\' · maintien encore <b>\'+rem+\'s</b>\':\'\')\n      +\'<br><span style="font-size:10px">Prix: \'+fmt(p,2)+\' R · Min: \'+fmt(cfg.absmin,0)+\' R · Max: \'+fmt(cfg.absmax,0)+\' R</span>\';\n\n    await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:Math.floor(Math.random()*200+20),volume24:(mkt.volume24||0)+80,trades24:(mkt.trades24||0)+1})});\n  },iv);\n}\n\nfunction stopCycles(){\n  _cy.active=false;_cy.phase=\'normal\';\n  if(_cy.int){clearInterval(_cy.int);_cy.int=null;}\n  if(tMode===\'cycles\'){tMode=null;tInt=null;}\n  $(\'cy-start-btn\').style.display=\'block\';$(\'cy-stop-btn\').style.display=\'none\';\n  var el=$(\'cy-status\');if(el)el.textContent=\'Cycles désactivés\';\n  var el2=$(\'tst\');if(el2){el2.textContent=\'⏸ Arrêté\';el2.style.color=\'var(--muted)\';}\n  addLog(\'📅\',\'Cycles désactivés\');\n}\n\nfunction previewCycle(){\n  var cfg=getCyConfig();var cv=$(\'cy-preview\');if(!cv||!window.Chart)return;\n  if(_cyPreviewObj){_cyPreviewObj.destroy();_cyPreviewObj=null;}\n  var pts=[];var p=(cfg.absmin+cfg.absmax)/2;\n  var pMin=calcProbPerTick(cfg.freqMin);var pMax=calcProbPerTick(cfg.freqMax);\n  var phase=\'normal\';var holdUntil=0;\n  for(var t=0;t<100;t++){\n    var adj=(Math.random()-0.5)*cfg.volBg*2+cfg.drift;\n    if(Math.random()<cfg.spikeProb)adj+=(Math.random()>0.5?1:-1)*cfg.spikeAmp*Math.random();\n    if(t>holdUntil){\n      if(phase!==\'tomin\'&&phase!==\'tomax\'){\n        if(Math.random()<pMin)phase=\'tomin\';\n        else if(Math.random()<pMax)phase=\'tomax\';\n        else phase=\'normal\';\n      }\n      if(phase===\'tomin\'){adj-=0.01*(1+cfg.bounce);if(p<=cfg.absmin*1.01){phase=\'atmin\';holdUntil=t+3;}}\n      if(phase===\'tomax\'){adj+=0.01*(1+cfg.resist);if(p>=cfg.absmax*0.99){phase=\'atmax\';holdUntil=t+3;}}\n    }\n    p=Math.max(cfg.absmin,Math.min(cfg.absmax,p*(1+adj)));\n    pts.push(Math.round(p*100)/100);\n  }\n  var labs=pts.map((_,i)=>\'T\'+i);\n  var ctx=cv.getContext(\'2d\');\n  var g=ctx.createLinearGradient(0,0,0,150);g.addColorStop(0,\'rgba(0,229,255,.2)\');g.addColorStop(1,\'rgba(0,229,255,0)\');\n  _cyPreviewObj=new Chart(ctx,{type:\'line\',data:{labels:labs,datasets:[\n    {data:pts,borderColor:\'#00e5ff\',backgroundColor:g,borderWidth:2,pointRadius:0,fill:true,tension:0.3},\n    {data:Array(100).fill(cfg.absmin),borderColor:\'rgba(0,255,157,.4)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[4,4]},\n    {data:Array(100).fill(cfg.absmax),borderColor:\'rgba(255,61,94,.4)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[4,4]},\n  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{display:false},y:{ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n}\n\n\n// ══ INFOS BULLES ══\nvar _infos={\n  bornes:"Les bornes sont les limites absolues du prix NXC. Le prix ne pourra jamais descendre en dessous du minimum ni monter au-dessus du maximum, quoi qu\'il arrive.",\n  freq:"Définit combien de fois le prix touchera exactement son minimum ou maximum dans chaque période. Le moteur calcule automatiquement la probabilité par tick (intervalle de 12s par défaut) pour respecter ces frequences.",\n  "freq-min":"Par minute : combien de fois dans la prochaine minute le prix touchera son minimum (colonne verte) ou maximum (colonne rouge). 0 = jamais dans la minute.",\n  "freq-h":"Par heure : combien de fois dans la prochaine heure le prix touchera son minimum ou maximum. Ex: 2 = deux fois dans l\'heure.",\n  "freq-d":"Par jour : combien de fois dans les 24 prochaines heures le prix touchera son minimum ou maximum.",\n  "freq-w":"Par semaine : combien de fois dans les 7 prochains jours le prix touchera son minimum ou maximum.",\n  "freq-mo":"Par mois (30 jours) : combien de fois dans le mois le prix touchera son minimum ou maximum.",\n  "freq-y":"Par an (365 jours) : combien de fois dans l\'année le prix touchera son minimum ou maximum. Ex: 4 = une fois par trimestre.",\n  "freq-custom":"Durée personnalisée : définir une période sur mesure. Ex: 6 heures, 2 jours... et combien de fois le prix touchera les extrêmes dans cette durée.",\n  comportement:"Paramètres qui définissent comment le prix se comporte quand il se déplace vers un extrême.",\n  transition:"Comment le prix atteint le min ou le max. Brutal = saut instantané. Progressif = descente/montée sur plusieurs ticks. Sinusoïde = courbe douce et naturelle.",\n  hold:"Combien de temps le prix reste au minimum ou maximum avant de repartir. Une durée aléatoire entre Min et Max est choisie à chaque fois.",\n  drift:"Tendance de fond sur le long terme. +2%/j = le prix a une légère tendance à monter de 2% par jour en moyenne. 0 = aucune tendance.",\n  volbg:"Quantité de mouvement aléatoire à chaque tick, indépendant des cycles. 0% = prix totalement lisse entre les cycles. Plus élevé = plus de micro-variations.",\n  spike:"Probabilité qu\'un pic inattendu se produise à chaque tick. Ex: 5% = 1 chance sur 20 à chaque tick d\'avoir un mouvement brutal.",\n  spikeamp:"Amplitude maximale d\'un pic surprise. ±10% = le pic peut faire bouger le prix de jusqu\'à 10% instantanément.",\n  bounce:"Force du rebond quand le prix touche le plancher. 0% = s\'arrête exactement au plancher. 5% = rebondit légèrement vers le haut.",\n  resist:"Résistance quand le prix approche du plafond. 0% = monte jusqu\'au plafond facilement. 5% = plus difficile de dépasser le plafond.",\n  activation:"Active le moteur de cycles. Une fois activé, le prix suivra automatiquement les frequences définies pour atteindre les extrêmes.",\n  preview:"Simule 100 ticks avec les paramètres actuels pour voir à quoi ressemblera le comportement du prix avant de l\'activer.",\n  \'prev-params\':"Choisir la duree de simulation, le nombre de points et le scenario. Plus il y a de points, plus la simulation est précise mais plus elle est longue.",\n  \'prev-scenario\':"Paramètres actuels = utilise exactement les réglages de l\'onglet Contrôle et Cycles. Haussier/Baissier = force une tendance. Volatile = forte agitation. Stable = peu de mouvement. Cycles = utilise les paramètres de cycles.",\n  \'prev-start\':"Le prix à partir duquel la simulation démarre. Par défaut c\'est le prix actuel du NXC.",\n  \'prev-result\':"Les statistiques clés extraites de la simulation : prix final prévu, variation, minimum et maximum atteints, volatilité moyenne.",\n  \'prev-g1\':"Graphique de l\'évolution du prix au fil du temps. Tu peux modifier les axes Y pour zoomer sur une zone précise.",\n  \'prev-g2\':"Trois scenarios calcules en parallele : pessimiste (prix bas), realiste (prix moyen), optimiste (prix haut). La zone bleue montre l\'incertitude totale.",\n  \'prev-g3\':"La volatilité à chaque point : mesure à quel point le prix change rapidement. Un pic = le prix bouge beaucoup à cet instant.",\n  \'prev-g4\':"Histogramme montrant à quels prix le NXC passe le plus de temps. Les barres les plus hautes = zones de prix fréquentes.",\n  \'prev-g5\':"Le drawdown = la chute depuis le sommet le plus récent. Permet de voir le pire cas de perte depuis un pic.",\n  \'prev-g6\':"Si tu achètes X rewards de NXC au prix actuel, combien vaudront-ils dans le temps selon la simulation ?"\n\n};\n\nfunction showInfo(key){\n  var modal=$(\'info-modal\');if(!modal)return;\n  $(\'info-title\').textContent=\'ℹ️ \'+key.replace(/-/g,\' \').replace(/\\b\\w/g,c=>c.toUpperCase());\n  $(\'info-body\').textContent=_infos[key]||\'Information non disponible.\';\n  modal.style.display=\'flex\';\n}\n\n// ══ PROBABILITÉS PAR TICK ══\nfunction updateCyProb(){\n  var ticksPerMin=5,ticksPerH=300,ticksPerD=7200,ticksPerW=50400,ticksPerMo=216000,ticksPerY=2628000;\n  var customDur=parseFloat($(\'cy-custom-dur\').value)||0;\n  var customUnit=parseFloat($(\'cy-custom-unit\').value)||3600000;\n  var customMs=customDur*customUnit;\n  var customTicks=customMs/12000;\n\n  var freqMin={m:parseFloat($(\'cy-min-m\').value)||0,h:parseFloat($(\'cy-min-h\').value)||0,d:parseFloat($(\'cy-min-d\').value)||0,w:parseFloat($(\'cy-min-w\').value)||0,mo:parseFloat($(\'cy-min-mo\').value)||0,y:parseFloat($(\'cy-min-y\').value)||0,c:parseFloat($(\'cy-min-c\').value)||0};\n  var freqMax={m:parseFloat($(\'cy-max-m\').value)||0,h:parseFloat($(\'cy-max-h\').value)||0,d:parseFloat($(\'cy-max-d\').value)||0,w:parseFloat($(\'cy-max-w\').value)||0,mo:parseFloat($(\'cy-max-mo\').value)||0,y:parseFloat($(\'cy-max-y\').value)||0,c:parseFloat($(\'cy-max-c\').value)||0};\n\n  var pMin=freqMin.m/ticksPerMin+freqMin.h/ticksPerH+freqMin.d/ticksPerD+freqMin.w/ticksPerW+freqMin.mo/ticksPerMo+freqMin.y/ticksPerY+(customTicks>0?freqMin.c/customTicks:0);\n  var pMax=freqMax.m/ticksPerMin+freqMax.h/ticksPerH+freqMax.d/ticksPerD+freqMax.w/ticksPerW+freqMax.mo/ticksPerMo+freqMax.y/ticksPerY+(customTicks>0?freqMax.c/customTicks:0);\n\n  pMin=Math.min(pMin,0.8);pMax=Math.min(pMax,0.8);\n\n  // Estimation des frequences résultantes\n  var estPerH_min=Math.round(pMin*ticksPerH*10)/10;\n  var estPerH_max=Math.round(pMax*ticksPerH*10)/10;\n  var estPerD_min=Math.round(pMin*ticksPerD);\n  var estPerD_max=Math.round(pMax*ticksPerD);\n\n  var el=$(\'cy-prob-display\');if(!el)return;\n  el.innerHTML=\n    \'<b style="color:var(--green)">MIN</b> — probabilité/tick: <b>\'+(pMin*100).toFixed(3)+\'%</b> · ~\'+estPerH_min+\'/heure · ~\'+estPerD_min+\'/jour<br>\'\n    +\'<b style="color:var(--red)">MAX</b> — probabilité/tick: <b>\'+(pMax*100).toFixed(3)+\'%</b> · ~\'+estPerH_max+\'/heure · ~\'+estPerD_max+\'/jour<br>\'\n    +(pMin+pMax>0.5?\'<span style="color:var(--red)">⚠️ Frequences tres elevees - le prix sera souvent aux extremes</span>\':\'<span style="color:var(--green)">✅ Frequences realistes</span>\');\n\n  window._cyPMin=pMin;window._cyPMax=pMax;\n}\n\n\n// ══ PRÉVISIONS ══\nvar _pvScenario=\'current\';\nvar _pvCharts={g1:null,g2:null,g3:null,g4:null,g5:null,g6:null};\nvar _pvData=null;\n\nfunction setPvScenario(sc){\n  _pvScenario=sc;\n  document.querySelectorAll(\'[id^="pv-sc-"]\').forEach(b=>{b.className=\'btn\';b.style.fontSize=\'11px\';b.style.padding=\'8px\';});\n  var b=$(\'pv-sc-\'+sc);if(b){b.className=\'btn cyan\';b.style.fontSize=\'11px\';b.style.padding=\'8px\';}\n}\n\nfunction simStep(p,scenario,floor,ceil,drift,volBg){\n  var adj=drift+(Math.random()-0.5)*volBg*2;\n  if(scenario===\'bull\')adj+=0.003+(Math.random()-0.3)*volBg;\n  else if(scenario===\'bear\')adj-=0.003+(Math.random()-0.7)*volBg;\n  else if(scenario===\'volatile\')adj+=(Math.random()-0.5)*0.05;\n  else if(scenario===\'stable\')adj*=0.1;\n  else if(scenario===\'cycles\'){\n    if(Math.random()<0.02)adj-=0.08; // descente cycle\n    if(Math.random()<0.02)adj+=0.08; // montée cycle\n  }\n  return Math.max(floor,Math.min(ceil,p*(1+adj)));\n}\n\nfunction runSimulation(){\n  var dur=parseFloat($(\'pv-dur\').value)||24;\n  var unit=parseFloat($(\'pv-unit\').value)||3600;\n  var totalSec=dur*unit;\n  var pts=parseInt($(\'pv-pts\').value)||100;\n  var startP=parseFloat($(\'pv-start\').value)||parseFloat(mkt.price||5213);\n  var floor=_cfgFloor||50;\n  var ceil=_cfgCeil||100000;\n  var drift=parseFloat($(\'cy-drift\')?.value||0)/100/1440;\n  var volBg=parseFloat($(\'cy-vol-bg\')?.value||2)/1000;\n  var sc=_pvScenario;\n\n  $(\'pv-loading\').style.display=\'block\';\n  $(\'pv-result-card\').style.display=\'none\';\n\n  setTimeout(function(){\n    // Simulation principale (realiste)\n    var main=[startP];\n    for(var j=0;j<pts-1;j++)main.push(simStep(main[main.length-1],sc,floor,ceil,drift,volBg));\n\n    // Scenario optimiste (+50% volatilité, drift positif)\n    var opt=[startP];\n    for(var j=0;j<pts-1;j++)opt.push(simStep(opt[opt.length-1],\'bull\',floor,ceil,drift+0.001,volBg*1.5));\n\n    // Scenario pessimiste (-50% volatilité, drift négatif)\n    var pes=[startP];\n    for(var j=0;j<pts-1;j++)pes.push(simStep(pes[pes.length-1],\'bear\',floor,ceil,drift-0.001,volBg*1.5));\n\n    // Labels temporels\n    var secPerPt=totalSec/pts;\n    var labs=[];\n    for(var j=0;j<pts;j++){\n      var s=j*secPerPt;\n      if(totalSec<=3600)labs.push(Math.round(s/60)+\'min\');\n      else if(totalSec<=86400)labs.push(Math.round(s/3600)+\'h\');\n      else if(totalSec<=2592000)labs.push(Math.round(s/86400)+\'j\');\n      else if(totalSec<=31536000)labs.push(Math.round(s/2592000)+\'mois\');\n      else labs.push((s/31536000).toFixed(1)+\'an\');\n    }\n\n    _pvData={main,opt,pes,labs,startP,floor,ceil,pts,totalSec,secPerPt};\n\n    // Stats\n    var finalP=main[main.length-1];\n    var minP=Math.min(...main);var maxP=Math.max(...main);\n    var chg=(finalP-startP)/startP*100;\n    var vols=[];for(var j=1;j<main.length;j++)vols.push(Math.abs(main[j]-main[j-1])/main[j-1]*100);\n    var avgVol=vols.reduce((s,v)=>s+v,0)/vols.length;\n\n    $(\'pv-stats\').innerHTML=[\n      [\'Prix final\',fmt(finalP,2)+\' R\',(chg>=0?\'var(--green)\':\'var(--red)\')],\n      [\'Variation\',(chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%\',(chg>=0?\'var(--green)\':\'var(--red)\')],\n      [\'Minimum atteint\',fmt(minP,2)+\' R\',\'var(--red)\'],\n      [\'Maximum atteint\',fmt(maxP,2)+\' R\',\'var(--green)\'],\n      [\'Volatilité moy.\',avgVol.toFixed(3)+\'%/tick\',\'var(--gold)\'],\n      [\'Amplitude\',fmt(maxP-minP,0)+\' R\',\'var(--purple)\'],\n      [\'Durée simulée\',dur+\' \'+([$(\'pv-unit\').selectedOptions[0].text]||[\'\']),\'var(--cyan)\'],\n      [\'Points simulés\',pts,\'var(--muted)\'],\n    ].map(([k,v,col])=>\'<div class="st"><div class="sv" style="color:\'+col+\';font-size:13px">\'+v+\'</div><div class="sl">\'+k+\'</div></div>\').join(\'\');\n\n    drawAllPvCharts();\n    $(\'pv-loading\').style.display=\'none\';\n    $(\'pv-result-card\').style.display=\'block\';\n  },50);\n}\n\nfunction destroyChart(key){if(_pvCharts[key]){try{_pvCharts[key].destroy();}catch(e){}}_pvCharts[key]=null;}\n\nfunction drawAllPvCharts(){\n  if(!_pvData||!window.Chart)return;\n  var d=_pvData;\n  var g1ymin=parseFloat($(\'pv-g1-ymin\').value)||undefined;\n  var g1ymax=parseFloat($(\'pv-g1-ymax\').value)||undefined;\n  var g2ymin=parseFloat($(\'pv-g2-ymin\').value)||undefined;\n  var g2ymax=parseFloat($(\'pv-g2-ymax\').value)||undefined;\n  var g3ymax=parseFloat($(\'pv-g3-ymax\').value)||undefined;\n\n  // G1 : évolution prix\n  destroyChart(\'g1\');\n  var ctx1=$(\'pv-g1\').getContext(\'2d\');\n  var g=ctx1.createLinearGradient(0,0,0,240);g.addColorStop(0,\'rgba(0,229,255,.25)\');g.addColorStop(1,\'rgba(0,229,255,0)\');\n  _pvCharts.g1=new Chart(ctx1,{type:\'line\',data:{labels:d.labs,datasets:[\n    {label:\'Prix\',data:d.main,borderColor:\'#00e5ff\',backgroundColor:g,borderWidth:2.5,pointRadius:0,fill:true,tension:0.3},\n    {label:\'Plancher\',data:Array(d.pts).fill(d.floor),borderColor:\'rgba(0,255,157,.5)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[5,5]},\n    {label:\'Plafond\',data:Array(d.pts).fill(d.ceil),borderColor:\'rgba(255,61,94,.5)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[5,5]},\n  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:\'var(--muted)\',font:{size:9}}}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:8,font:{size:8}},grid:{color:\'rgba(0,229,255,.04)\'}},y:{min:g1ymin,max:g1ymax,ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n\n  // G2 : bandes de confiance\n  destroyChart(\'g2\');\n  var ctx2=$(\'pv-g2\').getContext(\'2d\');\n  _pvCharts.g2=new Chart(ctx2,{type:\'line\',data:{labels:d.labs,datasets:[\n    {label:\'Optimiste\',data:d.opt,borderColor:\'rgba(0,255,157,.8)\',borderWidth:1.5,pointRadius:0,fill:false,tension:0.3},\n    {label:\'Réaliste\',data:d.main,borderColor:\'#00e5ff\',borderWidth:2.5,pointRadius:0,fill:false,tension:0.3},\n    {label:\'Pessimiste\',data:d.pes,borderColor:\'rgba(255,61,94,.8)\',borderWidth:1.5,pointRadius:0,fill:\'-1\',backgroundColor:\'rgba(0,229,255,.08)\',tension:0.3},\n    {label:\'Plancher\',data:Array(d.pts).fill(d.floor),borderColor:\'rgba(0,255,157,.3)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[5,5]},\n    {label:\'Plafond\',data:Array(d.pts).fill(d.ceil),borderColor:\'rgba(255,61,94,.3)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[5,5]},\n  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:\'var(--muted)\',font:{size:9}}}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:8,font:{size:8}},grid:{color:\'rgba(0,229,255,.04)\'}},y:{min:g2ymin,max:g2ymax,ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n\n  // G3 : volatilité\n  destroyChart(\'g3\');\n  var vols=[];for(var j=1;j<d.main.length;j++)vols.push(Math.abs(d.main[j]-d.main[j-1])/d.main[j-1]*100);\n  var ctx3=$(\'pv-g3\').getContext(\'2d\');\n  _pvCharts.g3=new Chart(ctx3,{type:\'bar\',data:{labels:d.labs.slice(1),datasets:[{label:\'Volatilité %\',data:vols,backgroundColor:vols.map(v=>v>1?\'rgba(255,61,94,.6)\':v>0.5?\'rgba(255,176,32,.6)\':\'rgba(0,229,255,.5)\'),borderWidth:0}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{display:false},y:{max:g3ymax,ticks:{color:\'#5c6b8c\',callback:v=>v.toFixed(2)+\'%\'},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n\n  // G4 : histogramme distribution\n  destroyChart(\'g4\');\n  var bins=20;var minP=Math.min(...d.main);var maxP=Math.max(...d.main);var binW=(maxP-minP)/bins||1;\n  var hist=Array(bins).fill(0);var binLabs=[];\n  for(var j=0;j<bins;j++)binLabs.push(fmt(minP+j*binW,0));\n  d.main.forEach(p=>{var b=Math.min(bins-1,Math.floor((p-minP)/binW));hist[b]++;});\n  var ctx4=$(\'pv-g4\').getContext(\'2d\');\n  _pvCharts.g4=new Chart(ctx4,{type:\'bar\',data:{labels:binLabs,datasets:[{label:\'Fréquence\',data:hist,backgroundColor:\'rgba(0,255,157,.5)\',borderColor:\'var(--green)\',borderWidth:1}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:8,font:{size:8}},grid:{display:false}},y:{ticks:{color:\'#5c6b8c\'},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n\n  // G5 : drawdown\n  destroyChart(\'g5\');\n  var peak=d.main[0];var dd=d.main.map(p=>{if(p>peak)peak=p;return ((p-peak)/peak*100);});\n  var ctx5=$(\'pv-g5\').getContext(\'2d\');\n  var g5=ctx5.createLinearGradient(0,0,0,180);g5.addColorStop(0,\'rgba(255,61,94,.3)\');g5.addColorStop(1,\'rgba(255,61,94,0)\');\n  _pvCharts.g5=new Chart(ctx5,{type:\'line\',data:{labels:d.labs,datasets:[{label:\'Drawdown %\',data:dd,borderColor:\'var(--red)\',backgroundColor:g5,borderWidth:2,pointRadius:0,fill:true,tension:0.3}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:8,font:{size:8}},grid:{display:false}},y:{ticks:{color:\'#5c6b8c\',callback:v=>v.toFixed(1)+\'%\'},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n\n  // G6 : ROI\n  updateG6();\n}\n\nfunction updateG6(){\n  if(!_pvData||!window.Chart)return;\n  var d=_pvData;\n  var invest=parseFloat($(\'pv-roi-invest\').value)||1000;\n  var nxcBought=invest/d.startP;\n  var roiData=d.main.map(p=>nxcBought*p);\n  var roiPct=d.main.map(p=>((p-d.startP)/d.startP*100));\n  destroyChart(\'g6\');\n  var ctx6=$(\'pv-g6\').getContext(\'2d\');\n  var g6=ctx6.createLinearGradient(0,0,0,180);\n  g6.addColorStop(0,\'rgba(255,110,180,.25)\');g6.addColorStop(1,\'rgba(255,110,180,0)\');\n  _pvCharts.g6=new Chart(ctx6,{type:\'line\',data:{labels:d.labs,datasets:[\n    {label:\'Valeur (R)\',data:roiData,borderColor:\'#ff6eb4\',backgroundColor:g6,borderWidth:2.5,pointRadius:0,fill:true,tension:0.3,yAxisID:\'y\'},\n    {label:\'ROI %\',data:roiPct,borderColor:\'rgba(255,176,32,.7)\',borderWidth:1.5,pointRadius:0,fill:false,tension:0.3,yAxisID:\'y2\'},\n    {label:\'Mise initiale\',data:Array(d.pts).fill(invest),borderColor:\'rgba(255,255,255,.2)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[5,5],yAxisID:\'y\'},\n  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:\'var(--muted)\',font:{size:9}}}},scales:{\n    x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:8,font:{size:8}},grid:{display:false}},\n    y:{ticks:{color:\'#ff6eb4\',callback:v=>fmt(v,0)+\' R\'},grid:{color:\'rgba(0,229,255,.04)\'},position:\'left\'},\n    y2:{ticks:{color:\'var(--gold)\',callback:v=>v.toFixed(1)+\'%\'},grid:{display:false},position:\'right\'},\n  },animation:{duration:0}}});\n}\n\nfunction updateG1(){if(_pvData)drawAllPvCharts();}\nfunction updateG2(){if(_pvData)drawAllPvCharts();}\nfunction updateG3(){if(_pvData)drawAllPvCharts();}\n\nfunction zoomIn(key){if(_pvCharts[key])_pvCharts[key].zoom?_pvCharts[key].zoom(1.3):null;}\nfunction zoomOut(key){if(_pvCharts[key])_pvCharts[key].zoom?_pvCharts[key].zoom(0.7):null;}\nfunction resetZoom(key){if(_pvCharts[key])_pvCharts[key].resetZoom?_pvCharts[key].resetZoom():null;}\n\nfunction exportSimJSON(){\n  if(!_pvData)return;\n  var blob=new Blob([JSON.stringify(_pvData,null,2)],{type:\'application/json\'});\n  var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'simulation_\'+Date.now()+\'.json\';a.click();\n  addLog(\'📥\',\'Export simulation JSON\');\n}\nfunction exportSimCSV(){if(!_pvData)return;var d=_pvData;var out=\'Temps,Realiste,Optimiste,Pessimiste\';for(var j2=0;j2<d.pts;j2++){out+=String.fromCharCode(10)+d.labs[j2]+\',\'+d.main[j2]+\',\'+(d.opt[j2]||0)+\',\'+(d.pes[j2]||0);}var blob=new Blob([out],{type:\'text/csv\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'sim_\'+Date.now()+\'.csv\';a.click();}\n// CONTRÔLE\nasync function adjP(pct){var p=Math.max(50,Math.min(100000,parseFloat(mkt.price||5213)*(1+pct)));p=Math.round(p*100)/100;await tick(p);setMsg(\'pm\',\'✅ \'+(pct>0?\'+\':\'\')+((pct*100).toFixed(1))+\'% → \'+fmt(p,2)+\' R\',true);addLog(\'📊\',\'Cours \'+(pct>0?\'+\':\'\')+((pct*100).toFixed(1))+\'%\');setTimeout(ref,500);}\nasync function setP(){var p=parseFloat($(\'np\').value);if(!p||p<50||p>100000){setMsg(\'pm\',\'Prix invalide\',false);return;}await tick(p);setMsg(\'pm\',\'✅ Cours → \'+fmt(p,2)+\' R\',true);$(\'np\').value=\'\';addLog(\'💱\',\'Cours fixé: \'+fmt(p,2)+\' R\');setTimeout(ref,500);}\nasync function setPct(){var pct=parseFloat($(\'np-pct\').value)/100;if(isNaN(pct)){setMsg(\'pm\',\'% invalide\',false);return;}await adjP(pct);$(\'np-pct\').value=\'\';}\nasync function resetH(){if(!confirm(\'Reset historique ?\'))return;await fetch(\'/nxc/reset\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY})});addLog(\'🔄\',\'Reset historique NXC\');ref();}\n\nvar _tStart=null,_tTimerInt=null;\nfunction setT(m){\n  var s=parseFloat($(\'ts\').value)||0.005,iv=parseInt($(\'ti\').value)||12000;\n  if(tInt){clearInterval(tInt);tInt=null;}if(_tTimerInt){clearInterval(_tTimerInt);_tTimerInt=null;}\n  tMode=m===\'stop\'?null:m;tStr=s;tIv=iv;_tStart=tMode?Date.now():null;\n  var el=$(\'tst\'),ht=$(\'hc\');\n  if(!tMode){el.textContent=\'⏸ Arrêté\';el.style.color=\'var(--muted)\';if($(\'tt-timer\'))$(\'tt-timer\').textContent=\'\';addLog(\'⏸\',\'Tendance arrêtée\');return;}\n  var lbl=m===\'up\'?\'📈 Hausse +\':m===\'down\'?\'📉 Baisse -\':\'🎲 Aléatoire\';var spd=m!==\'random\'?(s*100).toFixed(1)+\'%\':\'\';\n  el.textContent=lbl+spd+\' · \'+(iv/1000)+\'s/tick\';el.style.color=m===\'up\'?\'var(--green)\':m===\'down\'?\'var(--red)\':\'var(--purple)\';\n  addLog(m===\'up\'?\'📈\':m===\'down\'?\'📉\':\'🎲\',\'Tendance \'+m+\' · \'+(s*100).toFixed(1)+\'%\');\n  _tTimerInt=setInterval(function(){if(_tStart){var el=elapsed=Math.floor((Date.now()-_tStart)/1000);$(\'tt-timer\').textContent=\'⏱ \'+Math.floor(el/60)+\'m\'+(\'0\'+(el%60)).slice(-2)+\'s\';}},1000);\n  tInt=setInterval(async function(){\n    var p=parseFloat(mkt.price||5213);var adj=(Math.random()-0.5)*_noiseLevel*2;\n    if(m===\'up\')adj+=s;else if(m===\'down\')adj-=s;\n    p=Math.max(parseFloat(_cfgFloor)||50,Math.min(parseFloat(_cfgCeil)||100000,p*(1+adj)));\n    p=Math.random()>.03?Math.round(p*100)/100:Math.round(p);\n    await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:Math.floor(Math.random()*300+50),volume24:(mkt.volume24||0)+100,trades24:(mkt.trades24||0)+1})});\n  },iv);\n}\n\nasync function scenario(sc){\n  var p=parseFloat(mkt.price||5213),t;\n  if(sc===\'crash\')t=p*.7;else if(sc===\'moon\')t=p*1.3;else if(sc===\'ath\')t=Math.min(100000,Math.max(p*1.5,90000));else if(sc===\'floor\')t=200;\n  if(t){t=Math.max(50,Math.min(100000,Math.round(t*100)/100));await tick(t);addLog(\'🎭\',\'Scenario \'+sc+\' → \'+fmt(t,2)+\' R\');setTimeout(ref,500);}\n  else if(sc===\'volatile\'){setT(\'random\');addLog(\'⚡\',\'Scenario volatil\');}\n  else if(sc===\'stable\'){setT(\'stop\');addLog(\'😴\',\'Stabilisation\');}\n}\n\n// BANQUE\nfunction setAmt(v){$(\'bk-amt\').value=v;}\nfunction filterFlux(f){_fluxF=f;[\'fl-all\',\'fl-in\',\'fl-out\'].forEach(id=>{var e=$(id);if(e)e.className=\'btn\';});var e=$(\'fl-\'+f);if(e)e.className=\'btn cyan\';renderFlux();}\nfunction renderFlux(){\n  var flux=(_fluxF===\'all\'?_flux:_flux.filter(f=>f.type===_fluxF)).slice(0,30);\n  var el=$(\'bk-flux\');if(!el)return;\n  el.innerHTML=flux.length?flux.map(f=>\'<div class="fl-item"><div style="width:8px;height:8px;border-radius:50%;flex-shrink:0;background:\'+(f.type===\'IN\'?\'var(--green)\':\'var(--red)\')+\';box-shadow:0 0 6px \'+(f.type===\'IN\'?\'rgba(0,255,157,.4)\':\'rgba(255,61,94,.4)\')+\'"></div><span style="font-weight:700;color:\'+(f.type===\'IN\'?\'var(--green)\':\'var(--red)\')+\';flex-shrink:0">\'+(f.type===\'IN\'?\'+\':\'-\')+fmt(f.amount||0,0)+\' R</span><span style="color:var(--muted);flex:1">\'+esc(f.user||\'?\')+\'</span><span style="color:var(--muted);font-size:10px">\'+new Date(f.ts).toLocaleTimeString(\'fr-FR\')+\'</span></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun flux</p>\';\n}\n\nfunction exportFlux(){var csv=\'Date,Type,User,Montant\\n\';_flux.forEach(f=>csv+=new Date(f.ts).toLocaleString(\'fr-FR\')+\',\'+f.type+\',\'+(f.user||\'\')+\',\'+(f.amount||0)+\'\\n\');var b=new Blob([csv],{type:\'text/csv\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(b);a.download=\'flux_\'+Date.now()+\'.csv\';a.click();addLog(\'📊\',\'Export CSV flux\');}\n\nasync function loadBank(){\n  try{\n    var r=await fetch(\'/nxc/bank\');var d=await r.json();if(!d.ok)return;var b=d.bank||{};\n    _flux=(b.flux||[]).slice().reverse();\n    var p=parseFloat(mkt.price||0);\n    $(\'bk-r\').textContent=fmt(b.reserves||0,0)+\' R\';$(\'bk-i\').textContent=fmt(b.totalIn||0,0);\n    $(\'bk-o\').textContent=fmt(b.totalOut||0,0);\n    $(\'bk-rt\').textContent=(b.totalIn>0?((b.reserves||0)/b.totalIn*100):100).toFixed(1)+\'%\';\n    $(\'bk-nx\').textContent=parseFloat(b.nxcEmis||0).toFixed(4)+\' NXC\';\n    $(\'bk-vx\').textContent=fmt((b.nxcEmis||0)*p,0)+\' R\';\n    var bn=(b.totalIn||0)-(b.totalOut||0);var el=$(\'bk-bn\');el.textContent=(bn>=0?\'+\':\'\')+fmt(bn,0)+\' R\';el.style.color=bn>=0?\'var(--green)\':\'var(--red)\';\n    $(\'bk-fl\').textContent=_flux.length;\n    renderFlux();\n  }catch(e){}\n}\n\nasync function bankOp(type){\n  var amt=parseFloat($(\'bk-amt\').value);if(!amt||amt<=0){setMsg(\'bk-msg\',\'Montant invalide\',false);return;}\n  var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{reserves:0,totalIn:0,totalOut:0,nxcEmis:0,flux:[]};\n  if(type===\'out\'&&amt>(b.reserves||0)){setMsg(\'bk-msg\',\'❌ Réserves insuffisantes\',false);return;}\n  if(type===\'in\'){b.reserves=parseFloat(((b.reserves||0)+amt).toFixed(2));b.totalIn=parseFloat(((b.totalIn||0)+amt).toFixed(2));}\n  else{b.reserves=parseFloat(((b.reserves||0)-amt).toFixed(2));b.totalOut=parseFloat(((b.totalOut||0)+amt).toFixed(2));}\n  b.flux=b.flux||[];b.flux.push({type:type===\'in\'?\'IN\':\'OUT\',user:\'SERVEUR\',amount:amt,nxc:0,ts:Date.now()});\n  var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:b,reset:true})});\n  var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ \'+(type===\'in\'?\'+\':\'-\')+fmt(amt,0)+\' R\':\'❌ Erreur\',res.ok);\n  if(res.ok){$(\'bk-amt\').value=\'\';addLog(type===\'in\'?\'💰\':\'💸\',(type===\'in\'?\'Injection +\':\'Retrait -\')+fmt(amt,0)+\' R\');loadBank();}\n}\n\nasync function bankResetHist(){var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{};if(!confirm(\'Reset historique ? Réserves: \'+fmt(b.reserves||0,0)+\' R conservées\'))return;var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:{reserves:b.reserves||0,nxcEmis:0,totalIn:0,totalOut:0,flux:[]},reset:true})});var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ Historique effacé\':\'❌ Erreur\',res.ok);if(res.ok){addLog(\'🗑️\',\'Reset historique banque\');loadBank();}}\nasync function bankResetAll(){var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{};var g=confirm(\'Garder réserves (\'+fmt(b.reserves||0,0)+\' R) ?\');if(!confirm(\'Confirmer ?\'))return;var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:{reserves:g?(b.reserves||0):0,nxcEmis:0,totalIn:0,totalOut:0,flux:[]},reset:true})});var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ Réinitialisé\':\'❌ Erreur\',res.ok);if(res.ok){addLog(\'💥\',\'Reset complet banque\');loadBank();}}\n\nasync function loadFails(){\n  try{\n    var r=await fetch(\'/nxc/bank/fail\');var d=await r.json();\n    var el=$(\'bk-fails\'),fc=$(\'fails-ct\');if(!el)return;\n    var fails=(d.fails||[]).slice().reverse();\n    if(fails.length&&fc){fc.textContent=fails.length;fc.style.display=\'block\';}$(\'nd-b\').style.display=fails.length?\'block\':\'none\';\n    el.innerHTML=fails.length?fails.map(f=>\'<div style="padding:12px;border-bottom:1px solid rgba(255,61,94,.08);display:flex;flex-direction:column;gap:6px"><div style="display:flex;justify-content:space-between"><span style="color:var(--red);font-weight:700">❌ \'+esc(f.user)+\'</span><span style="color:var(--muted);font-size:10px;font-family:monospace">\'+new Date(f.ts).toLocaleTimeString(\'fr-FR\')+\'</span></div><div style="color:var(--muted);font-size:11px">Voulait vendre <b style="color:var(--text)">\'+f.nxc+\' NXC</b> (\'+fmt(f.amount||0,0)+\' R)</div>\'+(f.gesture>0?\'<button onclick="sendGesture(\\\'\'+esc(f.user)+\'\\\',\'+f.gesture+\',\'+f.ts+\')" style="padding:8px 14px;background:rgba(0,255,157,.1);border:1px solid rgba(0,255,157,.3);border-radius:9px;color:var(--green);font-size:12px;cursor:pointer;font-weight:700;align-self:flex-start">💝 Verser +\'+f.gesture+\' R</button>\':\'\')+\'</div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">✅ Aucune tentative</p>\';\n  }catch(e){}\n}\n\nasync function sendGesture(user,amount,failTs){\n  if(!confirm(\'Verser \'+amount+\' R à \'+user+\' ?\'))return;\n  var r=await fetch(\'/nxc/bank/gesture\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,target:user,amount:amount,fail_ts:failTs})});\n  var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ \'+amount+\' R versés à \'+user:\'❌ \'+(res.error||\'Erreur\'),res.ok);\n  if(res.ok){addLog(\'💝\',\'Geste +\'+amount+\' R → \'+user);loadBank();loadFails();}\n}\n\n// APP (iframe configurable)\nfunction renderSavedSites(){\n  var el=$(\'saved-sites\');if(!el)return;\n  var pinned=_pinnedSites.map(s=>s.url);\n  el.innerHTML=_savedSites.length?_savedSites.map(s=>{\n    var isPinned=pinned.includes(s.url);\n    return \'<div style="display:flex;align-items:center;gap:4px;background:var(--bg3);border:1px solid \'+(isPinned?\'rgba(255,176,32,.4)\':\'var(--border)\')+\';border-radius:8px;padding:4px 8px;white-space:nowrap">\'\n      +\'<button onclick="loadSite(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" style="background:none;border:none;color:\'+(isPinned?\'var(--gold)\':\'var(--cyan)\')+\';font-size:11px;font-weight:700;cursor:pointer;padding:0">\'+(isPinned?\'📌 \':\'\')+esc(s.label)+\'</button>\'\n      +\'<button onclick="togglePin(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" title="\'+(isPinned?\'Désépingler\':\'Épingler\')+\'" style="background:none;border:none;color:\'+(isPinned?\'var(--gold)\':\'var(--muted)\')+\';font-size:11px;cursor:pointer;padding:0;margin-left:2px">\'+(isPinned?\'📌\':\'📍\')+\'</button>\'\n      +\'<button onclick="deleteSite(\\\'\'+esc(s.url)+\'\\\')" style="background:none;border:none;color:var(--red);font-size:12px;cursor:pointer;padding:0;margin-left:2px">✕</button>\'\n      +\'</div>\';\n  }).join(\'\'):\'<span style="color:var(--muted);font-size:11px">Aucun site sauvegardé</span>\';\n  // Afficher les sites épinglés en premier si existants\n  renderPinnedBar();\n}\nfunction goUrl(){var url=$(\'iframe-in\').value.trim();if(!url)return;if(!url.startsWith(\'http\'))url=\'https://\'+url;loadSite(url,null);$(\'iframe-in\').value=\'\';}\nfunction loadSite(url,label){_curUrl=url;var f=$(\'nf\');if(f)f.src=url;var t=$(\'if-title\');if(t)t.textContent=\'◈ \'+(label||url.replace(\'https://\',\'\').split(\'/\')[0]);var u=$(\'if-url\');if(u)u.textContent=url.replace(\'https://\',\'\').replace(\'http://\',\'\');}\nfunction saveSite(){var url=$(\'iframe-in\').value.trim()||_curUrl;var lbl=$(\'site-lbl\').value.trim()||url.replace(\'https://\',\'\').split(\'/\')[0];if(!url)return;if(!url.startsWith(\'http\'))url=\'https://\'+url;_savedSites=_savedSites.filter(s=>s.url!==url);_savedSites.unshift({label:lbl,url});if(_savedSites.length>8)_savedSites.pop();localStorage.setItem(\'nxc_sites\',JSON.stringify(_savedSites));$(\'site-lbl\').value=\'\';$(\'iframe-in\').value=\'\';renderSavedSites();addLog(\'💾\',\'Site sauvegardé: \'+lbl);}\nfunction deleteSite(url){_savedSites=_savedSites.filter(s=>s.url!==url);localStorage.setItem(\'nxc_sites\',JSON.stringify(_savedSites));renderSavedSites();}\nfunction reloadF(){var f=$(\'nf\');if(f)f.src=f.src;}\nfunction openNewTab(){if(_curUrl)window.open(_curUrl,\'_blank\');}\n\n// ADMIN\nasync function refreshAdminStats(){\n  try{\n    var pd=await fetch(\'/nxc/price\').then(r=>r.json());\n    var bd=await fetch(\'/nxc/bank\').then(r=>r.json());\n    var fd=await fetch(\'/nxc/bank/fail\').then(r=>r.json());\n    var b=bd.bank||{};var p=parseFloat(pd.price||0);\n    $(\'adm-price\').textContent=fmt(p,2)+\' R\';\n    $(\'adm-vol\').textContent=fmt(pd.volume24||0,0)+\' R\';\n    $(\'adm-trades\').textContent=pd.trades24||0;\n    $(\'adm-res\').textContent=fmt(b.reserves||0,0)+\' R\';\n    $(\'adm-nxc\').textContent=parseFloat(b.nxcEmis||0).toFixed(4);\n    $(\'adm-fails\').textContent=(fd.fails||[]).length;\n    $(\'adm-hist\').textContent=(pd.history||[]).length;\n    if(_users.length)$(\'adm-users\').textContent=_users.length;\n    addLog(\'📊\',\'Stats admin actualisées\');\n  }catch(e){}\n}\n\nasync function loadAdmUsers(){\n  if(!_users.length)await loadUsers();\n  var sel1=$(\'rw-u\'),sel2=$(\'role-u\');\n  [sel1,sel2].forEach(sel=>{if(sel)sel.innerHTML=\'<option value="">Utilisateur...</option>\'+_users.map(u=>\'<option value="\'+esc(u.n)+\'">\'+esc(u.n)+(u.role===\'admin\'?\' 👑\':u.role===\'moderator\'?\' 🛡️\':u.role===\'vip\'?\' ⭐\':\'\')+\'</option>\').join(\'\');});\n  $(\'adm-users\').textContent=_users.length;\n  renderAdmUsers(_users);\n}\n\nfunction renderAdmUsers(rows){\n  var el=$(\'adm-ut\');if(!el)return;\n  el.innerHTML=rows.map(r=>\'<tr><td style="font-weight:700;color:var(--cyan)">\'+esc(r.n)+(r.role===\'admin\'?\' 👑\':r.role===\'moderator\'?\' 🛡️\':r.role===\'vip\'?\' ⭐\':\'\')+\'</td><td style="color:var(--muted);font-size:10px">\'+esc(r.role)+\'</td><td style="color:var(--gold)">\'+fmt(r.rew,0)+\'</td><td style="color:var(--cyan);font-family:monospace">\'+r.nxc.toFixed(4)+\'</td><td style="color:var(--purple)">\'+fmt(r.val,0)+\'</td></tr>\').join(\'\');\n}\nfunction filterAdmUsers(){var q=($(\'adm-q\').value||\'\').toLowerCase();renderAdmUsers(q?_users.filter(u=>u.n.toLowerCase().includes(q)):_users);}\n\nasync function giveRewards(){\n  var target=$(\'rw-u\').value,amt=parseFloat($(\'rw-amt\').value);\n  if(!target||!amt||amt<=0){setMsg(\'rw-msg\',\'Remplir tous les champs\',false);return;}\n  var r=await fetch(\'/admin/give-rewards\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,target:target,amount:amt})});\n  var res=await r.json();\n  setMsg(\'rw-msg\',res.ok?\'✅ +\'+fmt(amt,0)+\' R donnés à \'+target+\' (total: \'+fmt(res.new_rewards||0,0)+\' R)\':\'❌ \'+(res.error||\'Erreur\'),res.ok);\n  if(res.ok){addLog(\'🏆\',\'Rewards +\'+fmt(amt,0)+\' R → \'+target);}\n}\n\nasync function changeRole(){\n  var u=$(\'role-u\').value,role=$(\'role-v\').value;\n  if(!u){setMsg(\'role-msg\',\'Sélectionner un utilisateur\',false);return;}\n  var r=await fetch(\'/admin/set-role\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,target:u,role:role})});\n  var res=await r.json();\n  setMsg(\'role-msg\',res.ok?\'✅ Rôle de \'+u+\' changé en \'+role:\'❌ \'+(res.error||\'Erreur\'),res.ok);\n  if(res.ok)addLog(\'👑\',\'Rôle \'+u+\' → \'+role);\n}\n\nasync function pruneHistory(){if(!confirm(\'Réduire historique à 100 points ?\'))return;await fetch(\'/nxc/reset\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY})});setMsg(\'maint-msg\',\'✅ Historique réduit\',true);addLog(\'✂️\',\'Historique NXC réduit\');}\nasync function resetAllTrades(){if(!confirm(\'Reset trades 24h ?\'))return;await tick(parseFloat(mkt.price||5213));setMsg(\'maint-msg\',\'✅ Trades remis à zéro\',true);addLog(\'🗑️\',\'Reset trades 24h\');}\n\nasync function backupDB(){\n  try{var p=await(await fetch(\'/nxc/price\')).json();var b=await(await fetch(\'/nxc/bank\')).json();var u=await api(\'/admin/list\');var data={date:new Date().toISOString(),market:p,bank:b.bank||{},users:u.users||[]};var blob=new Blob([JSON.stringify(data,null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_backup_\'+Date.now()+\'.json\';a.click();setMsg(\'maint-msg\',\'✅ Backup téléchargé\',true);addLog(\'💾\',\'Backup DB téléchargé\');}catch(e){setMsg(\'maint-msg\',\'❌ Erreur backup\',false);}\n}\n\nasync function pingServer(){\n  var el=$(\'ping-res\');if(el){el.textContent=\'📡 Test...\';el.style.color=\'var(--muted)\';}\n  var t=Date.now();\n  try{await fetch(\'/nxc/price\');var lat=Date.now()-t;var c=lat<500?\'var(--green)\':lat<1000?\'var(--gold)\':\'var(--red)\';if(el){el.textContent=\'✅ En ligne — \'+lat+\' ms\';el.style.color=c;}}\n  catch(e){if(el){el.textContent=\'❌ Inaccessible\';el.style.color=\'var(--red)\';}}\n}\n\n// USERS\nasync function loadUsers(){\n  $(\'us-msg\').textContent=\'Chargement…\';\n  try{\n    var r=await api(\'/admin/list\');if(!r||!r.ok){$(\'us-msg\').textContent=\'Erreur\';return;}\n    var p=parseFloat(mkt.price||0);\n    var rows=await Promise.all((r.users||[]).map(async u=>{\n      var d=await api(\'/admin/get\',{target:u.username});\n      var rew=Math.max((d.data&&d.data.nx2098&&d.data.nx2098.rewards)||0,(d.data&&d.data.rewards&&d.data.rewards.points)||0);\n      var nxc=parseFloat((d.data&&d.data.nxcoin&&d.data.nxcoin.nxc)||0);\n      return {n:u.username,role:u.role,rew,nxc,val:nxc*p};\n    }));\n    _users=rows;\n    $(\'u-total\').textContent=rows.length;$(\'u-admins\').textContent=rows.filter(r=>r.role===\'admin\').length;\n    $(\'u-rew\').textContent=fmt(rows.reduce((s,r)=>s+r.rew,0),0);\n    sortU(\'rew\');$(\'us-msg\').textContent=\'\';\n    if($(\'adm-ut\'))loadAdmUsers();\n  }catch(e){$(\'us-msg\').textContent=\'Erreur\';}\n}\nfunction sortU(by){_users.sort((a,b)=>by===\'name\'?a.n.localeCompare(b.n):(b[by]-a[by]));renderU(_users);}\nfunction renderU(rows){var el=$(\'ut\');if(!el)return;el.innerHTML=rows.map(r=>\'<tr><td style="font-weight:700;color:var(--cyan)">\'+esc(r.n)+(r.role===\'admin\'?\' 👑\':r.role===\'moderator\'?\' 🛡️\':r.role===\'vip\'?\' ⭐\':\'\')+\'</td><td style="color:var(--muted);font-size:10px">\'+esc(r.role)+\'</td><td style="color:var(--gold)">\'+fmt(r.rew,0)+\'</td><td style="color:var(--cyan);font-family:monospace">\'+r.nxc.toFixed(4)+\'</td><td style="color:var(--purple)">\'+fmt(r.val,0)+\'</td></tr>\').join(\'\');}\nfunction filterU(){var q=($(\'us-q\').value||\'\').toLowerCase();renderU(q?_users.filter(r=>r.n.toLowerCase().includes(q)):_users);}\n\n// STATS\nvar volObj=null;\nasync function loadStats(){\n  if(!_users.length)await loadUsers();\n  var h=mkt.history||[];var p=parseFloat(mkt.price||0);\n  if(h.length>5){var cv=$(\'ch-vol\');if(cv&&window.Chart){var pts=h.slice(-20);var labs=pts.map(x=>new Date(x.ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\'}));var vols=pts.map(x=>x.vol||0);if(volObj){volObj.data.labels=labs;volObj.data.datasets[0].data=vols;volObj.update(\'none\');}else{var ctx=cv.getContext(\'2d\');volObj=new Chart(ctx,{type:\'bar\',data:{labels:labs,datasets:[{data:vols,backgroundColor:\'rgba(160,107,255,.5)\',borderColor:\'#a06bff\',borderWidth:1}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:4,font:{size:8}},grid:{display:false}},y:{ticks:{color:\'#5c6b8c\'},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});}}}\n  var el=$(\'rew-bars\');if(el&&_users.length){var maxR=Math.max(..._users.map(u=>u.rew))||1;el.innerHTML=[..._users].sort((a,b)=>b.rew-a.rew).slice(0,8).map(u=>\'<div style="margin-bottom:8px"><div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:3px"><span style="color:var(--cyan);font-weight:700">\'+esc(u.n)+\'</span><span style="color:var(--gold)">\'+fmt(u.rew,0)+\' R</span></div><div class="pbar"><div class="pbar-fill" style="width:\'+Math.round(u.rew/maxR*100)+\'%"></div></div></div>\').join(\'\');}\n  var hi=h.length>1?Math.max(...h.slice(-24).map(x=>x.price)):p;var lo=h.length>1?Math.min(...h.slice(-24).map(x=>x.price)):p;var vol=lo>0?(hi-lo)/lo*100:0;\n  var hg=$(\'health-grid\');if(hg)hg.innerHTML=[[\'📈 Tendance\',h.length>5?(h.slice(-5).map(x=>x.price).every((v,i,a)=>i===0||v>a[i-1])?\'<span style="color:var(--green)">Haussière</span>\':h.slice(-5).map(x=>x.price).every((v,i,a)=>i===0||v<a[i-1])?\'<span style="color:var(--red)">Baissière</span>\':\'<span style="color:var(--muted)">Neutre</span>\'):\'—\'],[\'⚡ Volatilité\',vol.toFixed(2)+\'%\'],[\'📊 Amplitude\',fmt(hi-lo,0)+\' R\'],[\'🔢 Trades\',mkt.trades24||0]].map(([k,v])=>\'<div class="st"><div class="sv" style="font-size:12px">\'+v+\'</div><div class="sl">\'+k+\'</div></div>\').join(\'\');\n}\n\n// SOLVABILITÉ\nasync function loadSolv(){try{var r=await fetch(\'/nxc/solvability\');var d=await r.json();if(d.ok){solvOn=d.enabled;var inp=$(\'sg\');if(inp)inp.value=d.gesture||50;updSolv();}}catch(e){}}\nfunction updSolv(){var t=$(\'stg\'),l=$(\'sl\');if(solvOn){if(t)t.classList.add(\'on\');if(l){l.textContent=\'✅ Activée\';l.style.color=\'var(--green)\';}}else{if(t)t.classList.remove(\'on\');if(l){l.textContent=\'⏸ Désactivée\';l.style.color=\'var(--muted)\';}}}\nasync function toggleSolv(){solvOn=!solvOn;updSolv();await saveSolv();}\nasync function saveSolv(){var g=parseInt($(\'sg\').value)||50;var r=await fetch(\'/nxc/solvability\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,enabled:solvOn,gesture:g})});var res=await r.json();setMsg(\'sm\',res.ok?(solvOn?\'✅ Activée\':\'⏸ Désactivée\'):\'❌ Erreur\',res.ok);if(res.ok)addLog(\'🛡️\',\'Solvabilité \'+(solvOn?\'activée\':\'désactivée\'));}\n\n// OUTILS\nvar _noiseLevel=0.004;\nfunction updateNoise(v){\n  _noiseLevel=parseFloat(v)/1000;\n  var el=$(\'noise-val\');if(el)el.textContent=(parseFloat(v)/10).toFixed(1)+\'%\';\n}\nfunction calcN(){var n=parseFloat($(\'c-nxc\').value)||0;var p=parseFloat(mkt.price||0);$(\'c-rew\').value=n&&p?Math.round(n*p*100)/100:\'\';}\nfunction calcR(){var r=parseFloat($(\'c-rew2\').value)||0;var p=parseFloat(mkt.price||1);$(\'c-nxc2\').value=r&&p?(r/p).toFixed(6):\'\';}\nfunction simS(){var n=parseFloat($(\'ss-nxc\').value)||0;var fee=parseFloat($(\'ss-fee\').value)||0;var p=parseFloat(mkt.price||0);if(!n||!p){$(\'ss-res\').innerHTML=\'\';return;}var gross=n*p;var fees=gross*fee/100;var net=gross-fees;$(\'ss-res\').innerHTML=\'Brut: <b style="color:var(--text)">\'+fmt(gross,2)+\' R</b> · Frais: <b style="color:var(--red)">-\'+fmt(fees,2)+\' R</b> · <b style="color:var(--green);font-size:16px">Net: \'+fmt(net,2)+\' R</b>\';}\n\nvar _tmEnd=null;\nfunction startTimer(){var m=parseInt($(\'tm-m\').value)||0;var s=parseInt($(\'tm-s\').value)||0;var total=m*60+s;var action=$(\'tm-a\').value;if(!total)return;if(_tmInt)clearInterval(_tmInt);_tmEnd=Date.now()+total*1000;addLog(\'⏱️\',\'Minuteur: \'+action+\' dans \'+total+\'s\');_tmInt=setInterval(async function(){var rem=Math.max(0,Math.round((_tmEnd-Date.now())/1000));var el=$(\'tm-disp\');if(el)el.textContent=(\'0\'+Math.floor(rem/60)).slice(-2)+\':\'+(\'0\'+(rem%60)).slice(-2);if(rem<=0){clearInterval(_tmInt);_tmInt=null;if(el){el.textContent=\'✅\';el.style.color=\'var(--green)\';}if(action===\'stop\')setT(\'stop\');else if(action===\'up\'||action===\'down\')setT(action);else if(action===\'crash\'||action===\'moon\')scenario(action);addLog(\'⏱️\',\'Minuteur déclenché: \'+action);}},500);}\nfunction stopTimer(){if(_tmInt){clearInterval(_tmInt);_tmInt=null;var d=$(\'tm-disp\');if(d)d.textContent=\'\';}}\n\n// CONFIG\nfunction updCfg(){\n  var txt=\'Plancher: \'+(_cfgFloor?fmt(_cfgFloor,0)+\' R\':\'non défini\')+\' · Plafond: \'+(_cfgCeil?fmt(_cfgCeil,0)+\' R\':\'non défini\');\n  var el=$(\'cfg-info\');if(el)el.textContent=txt;\n  updFloorDisplay();\n}\nfunction updFloorDisplay(){\n  var txt=\'Plancher: \'+(_cfgFloor?fmt(_cfgFloor,0)+\' R\':\'non défini\')+\' · Plafond: \'+(_cfgCeil?fmt(_cfgCeil,0)+\' R\':\'non défini\');\n  var el=$(\'floor-display\');if(el)el.textContent=txt;\n  var ec=$(\'cfg-info\');if(ec)ec.textContent=txt;\n}\nfunction setFloor(){\n  var v=parseFloat($(\'t-floor\').value);if(!v||v<50){alert(\'Plancher invalide (min 50R)\');return;}\n  _cfgFloor=v;updFloorDisplay();addLog(\'⚙️\',\'Plancher: \'+fmt(v,0)+\' R\');\n}\nfunction setCeil(){\n  var v=parseFloat($(\'t-ceil\').value);if(!v||v>100000){alert(\'Plafond invalide (max 100 000R)\');return;}\n  _cfgCeil=v;updFloorDisplay();addLog(\'⚙️\',\'Plafond: \'+fmt(v,0)+\' R\');\n}\nfunction setNormalMode(){\n  // Cours normal = tendance aléatoire légère avec plancher/plafond actifs\n  if(!_cfgFloor&&!_cfgCeil){alert(\'Définir au moins un plancher ou un plafond\');return;}\n  setT(\'stop\'); // Arrêter toute tendance\n  // Lancer une légère variation aléatoire neutre\n  var iv=parseInt($(\'ti\').value)||12000;\n  if(tInt){clearInterval(tInt);tInt=null;}\n  tMode=\'normal\';\n  var el=$(\'tst\');el.textContent=\'📊 Cours normal · plancher: \'+(_cfgFloor?fmt(_cfgFloor,0)+\'R\':\'—\')+\' · plafond: \'+(_cfgCeil?fmt(_cfgCeil,0)+\'R\':\'—\');el.style.color=\'var(--cyan)\';\n  addLog(\'📊\',\'Cours normal activé\');\n  tInt=setInterval(async function(){\n    var p=parseFloat(mkt.price||5213);\n    var adj=(Math.random()-0.5)*_noiseLevel*0.5; // très légère variation\n    p=Math.max(_cfgFloor||50,Math.min(_cfgCeil||100000,p*(1+adj)));\n    p=Math.round(p*100)/100;\n    await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:Math.floor(Math.random()*100+10),volume24:(mkt.volume24||0)+50,trades24:(mkt.trades24||0)+1})});\n  },iv);\n}\nfunction scheduleT(){var st=$(\'cfg-st\').value,sp=$(\'cfg-sp\').value,dir=$(\'cfg-sd\').value;if(!st||!sp){setMsg(\'cfg-sch-msg\',\'Renseigner les deux heures\',false);return;}if(_schedInt)clearInterval(_schedInt);_schedInt=setInterval(function(){var now=new Date();var cur=(\'0\'+now.getHours()).slice(-2)+\':\'+(\'0\'+now.getMinutes()).slice(-2);if(cur===st&&!tMode)setT(dir);if(cur===sp&&tMode)setT(\'stop\');},30000);setMsg(\'cfg-sch-msg\',\'✅ Programmé: \'+dir+\' \'+st+\'→\'+sp,true);addLog(\'⏰\',\'Tendance programmée \'+dir+\' \'+st+\'→\'+sp);}\n\nfunction exportHist(){var h=mkt.history||[];var b=new Blob([JSON.stringify({date:new Date().toISOString(),price:mkt.price,history:h},null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(b);a.download=\'nxc_history_\'+Date.now()+\'.json\';a.click();addLog(\'📥\',\'Export historique JSON\');}\nfunction exportStats(){var b=new Blob([JSON.stringify({date:new Date().toISOString(),market:mkt,users:_users},null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(b);a.download=\'nxc_report_\'+Date.now()+\'.json\';a.click();addLog(\'📊\',\'Export rapport JSON\');}\n\n// ALERTES\nfunction addAlert(){var price=parseFloat($(\'al-p\').value),dir=$(\'al-d\').value;if(!price)return;_alerts.push({price,dir,id:Date.now(),triggered:false});$(\'al-p\').value=\'\';renderAlerts();addLog(\'🔔\',\'Alerte: prix \'+(dir===\'above\'?\'>\':\'<\')+\' \'+fmt(price,0)+\' R\');}\nfunction removeAlert(id){_alerts=_alerts.filter(a=>a.id!==id);renderAlerts();}\nfunction renderAlerts(){var el=$(\'al-list\');if(!el)return;el.innerHTML=_alerts.length?_alerts.map(a=>\'<div style="padding:10px 12px;border-bottom:1px solid rgba(0,229,255,.05);display:flex;justify-content:space-between;align-items:center;font-size:12px"><span style="color:\'+(a.triggered?\'var(--muted)\':\'var(--gold)\')+\'">Prix \'+(a.dir===\'above\'?\'>\':\'<\')+\' \'+fmt(a.price,0)+\' R\'+(a.triggered?\' ✅\':\'\')+\'</span><button onclick="removeAlert(\'+a.id+\')" style="padding:4px 8px;border-radius:6px;background:rgba(255,61,94,.1);border:1px solid rgba(255,61,94,.3);color:var(--red);font-size:10px;cursor:pointer">✕</button></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune alerte</p>\';}\nfunction checkAlerts(p){_alerts.forEach(function(a){if(a.triggered)return;if((a.dir===\'above\'&&p>a.price)||(a.dir===\'below\'&&p<a.price)){a.triggered=true;var m=\'🔔 Prix \'+(a.dir===\'above\'?\'>\':\'<\')+\' \'+fmt(a.price,0)+\' R (actuel: \'+fmt(p,0)+\' R)\';_alHist.unshift({ts:Date.now(),msg:m});addLog(\'🔔\',m);renderAlerts();renderAlHist();if(window.Notification&&Notification.permission===\'granted\')new Notification(\'◈ Nexus NXC\',{body:m});}});}\nfunction renderAlHist(){var el=$(\'al-hist\');if(!el)return;el.innerHTML=_alHist.length?_alHist.map(a=>\'<div class="log-item"><span class="log-time">\'+fmtT(a.ts)+\'</span><span style="color:var(--gold)">\'+esc(a.msg)+\'</span></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune</p>\';}\nif(window.Notification&&Notification.permission===\'default\')setTimeout(function(){Notification.requestPermission();},3000);\n\n// ══ ÉPINGLAGE SITES (sync cross-device via serveur) ══\nvar _pinnedSites=[];\n\nasync function loadPinnedSites(){\n  try{\n    var r=await fetch(\'/admin/pinned-sites\');var d=await r.json();\n    if(d.ok){_pinnedSites=d.sites||[];renderSavedSites();}\n  }catch(e){_pinnedSites=JSON.parse(localStorage.getItem(\'nxc_pinned\')||\'[]\');}\n}\n\nasync function togglePin(url,label){\n  var idx=_pinnedSites.findIndex(s=>s.url===url);\n  if(idx>=0)_pinnedSites.splice(idx,1);\n  else _pinnedSites.push({url,label});\n  // Sauvegarder sur le serveur ET en local\n  localStorage.setItem(\'nxc_pinned\',JSON.stringify(_pinnedSites));\n  try{await fetch(\'/admin/pinned-sites\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,sites:_pinnedSites})});}catch(e){}\n  renderSavedSites();\n  addLog(\'📌\',(idx>=0?\'Désépinglé\':\'Épinglé\')+\': \'+label);\n}\n\nfunction renderPinnedBar(){\n  var el=$(\'pinned-bar\');if(!el)return;\n  if(!_pinnedSites.length){el.style.display=\'none\';return;}\n  el.style.display=\'flex\';\n  el.innerHTML=_pinnedSites.map(s=>\'<button onclick="loadSite(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" style="padding:5px 12px;background:rgba(255,176,32,.12);border:1px solid rgba(255,176,32,.3);border-radius:8px;color:var(--gold);font-size:11px;font-weight:700;cursor:pointer;white-space:nowrap">📌 \'+esc(s.label)+\'</button>\').join(\'\');\n}\n\n// ══ SAUVEGARDE / IMPORT DONNÉES GLOBALES ══\nasync function saveAllData(){\n  try{\n    var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'export\'})});\n    var d=await r.json();\n    if(!d.ok){setMsg(\'data-msg\',\'❌ Erreur export\',false);return;}\n    var blob=new Blob([JSON.stringify(d.data,null,2)],{type:\'application/json\'});\n    var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_full_backup_\'+Date.now()+\'.json\';a.click();\n    setMsg(\'data-msg\',\'✅ Backup complet téléchargé\',true);\n    addLog(\'💾\',\'Sauvegarde complète téléchargée\');\n  }catch(e){setMsg(\'data-msg\',\'❌ Erreur: \'+e.message,false);}\n}\n\nfunction importData(){\n  var input=document.createElement(\'input\');input.type=\'file\';input.accept=\'.json\';\n  input.onchange=async function(e){\n    var file=e.target.files[0];if(!file)return;\n    var text=await file.text();\n    try{\n      var data=JSON.parse(text);\n      if(!confirm(\'Importer ces données ? Cela écrasera les données actuelles du serveur.\'))return;\n      var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'import\',data:data})});\n      var res=await r.json();\n      setMsg(\'data-msg\',res.ok?\'✅ Données importées avec succès\':\'❌ \'+(res.error||\'Erreur import\'),res.ok);\n      if(res.ok){addLog(\'📥\',\'Données importées depuis fichier\');setTimeout(function(){ref();loadBank();},1000);}\n    }catch(ex){setMsg(\'data-msg\',\'❌ Fichier JSON invalide\',false);}\n  };\n  input.click();\n}\n\n// ══ IMPRESSION ══\nfunction printDashboard(){\n  var p=parseFloat(mkt.price||0);var h=mkt.history||[];\n  var hi=h.length>1?Math.max(...h.slice(-24).map(x=>x.price)):p;\n  var lo=h.length>1?Math.min(...h.slice(-24).map(x=>x.price)):p;\n  var chg=_prevP>0?((p-_prevP)/_prevP*100):0;\n  // Capturer le graphique en PNG\n  var chartImg=\'\';var cv=$(\'ch\');if(cv)chartImg=cv.toDataURL(\'image/png\');\n  var rsiImg=\'\';var rsiCv=$(\'ch-rsi\');if(rsiCv)rsiImg=rsiCv.toDataURL(\'image/png\');\n  var now=new Date().toLocaleString(\'fr-FR\');\n  var win=window.open(\'\',\'_blank\');\n  win.document.write(\'<!DOCTYPE html><html><head><meta charset="utf-8"><title>◈ Nexus NXC — Rapport \'+now+\'</title><style>*{font-family:Arial,sans-serif;box-sizing:border-box}body{background:#fff;color:#000;padding:20px;max-width:900px;margin:0 auto}.header{text-align:center;border-bottom:3px solid #000;padding-bottom:16px;margin-bottom:20px}.title{font-size:28px;font-weight:900;letter-spacing:3px}.date{font-size:12px;color:#666;margin-top:4px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}.stat{border:1px solid #ddd;border-radius:8px;padding:12px;text-align:center}.stat-val{font-size:20px;font-weight:700;margin-bottom:4px}.stat-lbl{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:#666}img{max-width:100%;border:1px solid #ddd;border-radius:8px;margin-bottom:12px}h3{margin:16px 0 8px;font-size:14px;border-bottom:1px solid #eee;padding-bottom:4px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:8px;text-align:left;border:1px solid #ddd}th{background:#f5f5f5;font-weight:700}@media print{.no-print{display:none}}</style></head><body>\');\n  win.document.write(\'<div class="header"><div class="title">◈ NEXUS NXC</div><div class="date">Rapport généré le \'+now+\'</div></div>\');\n  win.document.write(\'<div class="grid">\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(p,2)+\' R</div><div class="stat-lbl">Prix actuel</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%</div><div class="stat-lbl">Variation</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(hi,0)+\' R</div><div class="stat-lbl">Haut 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(lo,0)+\' R</div><div class="stat-lbl">Bas 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(mkt.volume24||0,0)+\' R</div><div class="stat-lbl">Volume 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(mkt.trades24||0)+\'</div><div class="stat-lbl">Trades 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+h.length+\'</div><div class="stat-lbl">Points hist.</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(_users.length||0)+\'</div><div class="stat-lbl">Utilisateurs</div></div>\');\n  win.document.write(\'</div>\');\n  if(chartImg)win.document.write(\'<h3>Historique du cours (\'+_ctRange+\' derniers points)</h3><img src="\'+chartImg+\'">\');\n  if(rsiImg)win.document.write(\'<h3>RSI (14 ticks)</h3><img src="\'+rsiImg+\'">\');\n  if(_users.length){\n    win.document.write(\'<h3>Utilisateurs</h3><table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur (R)</th></tr></thead><tbody>\');\n    _users.forEach(u=>{win.document.write(\'<tr><td>\'+esc(u.n)+\'</td><td>\'+esc(u.role)+\'</td><td>\'+fmt(u.rew,0)+\'</td><td>\'+u.nxc.toFixed(4)+\'</td><td>\'+fmt(u.val,0)+\'</td></tr>\');});\n    win.document.write(\'</tbody></table>\');\n  }\n  win.document.write(\'<h3>Derniers logs</h3><table><thead><tr><th>Heure</th><th>Action</th></tr></thead><tbody>\');\n  _log.slice(0,20).forEach(l=>{win.document.write(\'<tr><td>\'+fmtT(l.ts)+\'</td><td>\'+l.ico+\' \'+esc(l.txt)+\'</td></tr>\');});\n  win.document.write(\'</tbody></table>\');\n  win.document.write(\'</body></html>\');\n  win.document.close();\n  setTimeout(function(){win.print();},500);\n  addLog(\'🖨️\',\'Impression du tableau de bord\');\n}\n\n\n// ══ CYCLES DE MARCHÉ ══\nvar _cy={absmin:null,absmax:null,active:false,int:null,phase:\'normal\',phaseStart:Date.now(),holdUntil:0};\nvar _cyPreviewObj=null;\n\nfunction setCyVal(key){\n  var v=parseFloat($(\'cy-\'+key).value);\n  if(isNaN(v)||v<=0)return;\n  _cy[key]=v;\n  var el=$(\'cy-\'+key+\'-disp\');if(el)el.textContent=fmt(v,0)+\' R\';\n  // Sync avec _cfgFloor/_cfgCeil\n  if(key===\'absmin\'){_cfgFloor=v;updFloorDisplay();}\n  if(key===\'absmax\'){_cfgCeil=v;updFloorDisplay();}\n  addLog(\'📅\',\'Borne \'+key+\': \'+fmt(v,0)+\' R\');\n}\n\nfunction getCyConfig(){\n  return {\n    absmin: _cy.absmin||parseFloat($(\'cy-absmin\').value)||50,\n    absmax: _cy.absmax||parseFloat($(\'cy-absmax\').value)||100000,\n    transition: $(\'cy-transition\').value,\n    holdMin: parseFloat($(\'cy-hold-min\').value)||1,\n    holdMax: parseFloat($(\'cy-hold-max\').value)||3,\n    holdUnit: parseFloat($(\'cy-hold-unit\').value)||60,\n    drift: parseFloat($(\'cy-drift\').value)/100/1440,\n    volBg: parseFloat($(\'cy-vol-bg\').value)/1000,\n    spikeProb: parseFloat($(\'cy-spike\').value)/100,\n    spikeAmp: parseFloat($(\'cy-spike-amp\').value)/100,\n    bounce: parseFloat($(\'cy-bounce\').value)/100,\n    resist: parseFloat($(\'cy-resist\').value)/100,\n    // Frequences par période → probabilité par tick (tick = 12s)\n    freqMin: {\n      m: parseFloat($(\'cy-min-m\').value)||0,\n      h: parseFloat($(\'cy-min-h\').value)||1,\n      d: parseFloat($(\'cy-min-d\').value)||1,\n      w: parseFloat($(\'cy-min-w\').value)||1,\n      mo: parseFloat($(\'cy-min-mo\').value)||2,\n      y: parseFloat($(\'cy-min-y\').value)||4,\n    },\n    freqMax: {\n      m: parseFloat($(\'cy-max-m\').value)||0,\n      h: parseFloat($(\'cy-max-h\').value)||1,\n      d: parseFloat($(\'cy-max-d\').value)||1,\n      w: parseFloat($(\'cy-max-w\').value)||1,\n      mo: parseFloat($(\'cy-max-mo\').value)||2,\n      y: parseFloat($(\'cy-max-y\').value)||4,\n    },\n  };\n}\n\nfunction calcProbPerTick(freqObj){\n  // Convertir les frequences en probabilité par tick (12s)\n  var ticksPerMin=5,ticksPerH=300,ticksPerD=7200,ticksPerW=50400,ticksPerMo=216000,ticksPerY=2628000;\n  var pMin=freqObj.m/ticksPerMin+freqObj.h/ticksPerH+freqObj.d/ticksPerD+freqObj.w/ticksPerW+freqObj.mo/ticksPerMo+freqObj.y/ticksPerY;\n  return Math.min(pMin,0.5); // max 50% par tick\n}\n\nfunction startCycles(){\n  var cfg=getCyConfig();\n  if(cfg.absmin>=cfg.absmax){alert(\'Le plancher doit être inférieur au plafond\');return;}\n  // Calculer automatiquement les probabilités\n  updateCyProb();\n  var pToMin=window._cyPMin||0;\n  var pToMax=window._cyPMax||0;\n  _cy.active=true;_cy.phase=\'normal\';_cy.holdUntil=0;\n  $(\'cy-start-btn\').style.display=\'none\';$(\'cy-stop-btn\').style.display=\'block\';\n  var iv=parseInt($(\'ti\').value)||12000;\n  if(tInt){clearInterval(tInt);tInt=null;}\n  if(_cy.int){clearInterval(_cy.int);_cy.int=null;}\n  tMode=\'cycles\';\n  var el=$(\'tst\');el.textContent=\'📅 Cycles actifs · \'+fmt(cfg.absmin,0)+\'R – \'+fmt(cfg.absmax,0)+\'R\';el.style.color=\'var(--cyan)\';\n  addLog(\'📅\',\'Cycles activés · P(min)=\'+(pToMin*100).toFixed(3)+\'% P(max)=\'+(pToMax*100).toFixed(3)+\'% /tick\');\n\n  _cy.int=setInterval(async function(){\n    var p=parseFloat(mkt.price||5213);\n    var now=Date.now();\n    var range=cfg.absmax-cfg.absmin;\n    var adj=0;\n\n    // Drift + volatilité de fond\n    adj+=cfg.drift+(Math.random()-0.5)*cfg.volBg*2;\n\n    // Pics surprises\n    if(Math.random()<cfg.spikeProb){\n      var dir=Math.random()>0.5?1:-1;\n      var spikeAdj=dir*cfg.spikeAmp*Math.random();\n      adj+=spikeAdj;\n      addLog(\'⚡\',\'Pic surprise: \'+(spikeAdj>0?\'+\':\'\')+(spikeAdj*100).toFixed(1)+\'%\');\n    }\n\n    if(now<_cy.holdUntil){\n      // Maintien : légère oscillation autour de l\'extrême\n      if(_cy.phase===\'atmin\'){adj=(Math.random()-0.3)*0.002;}\n      if(_cy.phase===\'atmax\'){adj=(Math.random()-0.7)*0.002;}\n    } else {\n      // Phase normale : décider si on part vers un extrême\n      if(_cy.phase===\'normal\'||_cy.phase===\'atmin\'||_cy.phase===\'atmax\'){\n        if(Math.random()<pToMin){_cy.phase=\'tomin\';addLog(\'📅\',\'→ Descente vers minimum (\'+fmt(cfg.absmin,0)+\'R)\');}\n        else if(Math.random()<pToMax){_cy.phase=\'tomax\';addLog(\'📅\',\'→ Montée vers maximum (\'+fmt(cfg.absmax,0)+\'R)\');}\n        else if(_cy.phase!==\'normal\'){_cy.phase=\'normal\';}\n      }\n\n      if(_cy.phase===\'tomin\'){\n        // Force proportionnelle à la distance — arrive en ~10 ticks\n        var dist=(p-cfg.absmin)/range;\n        var force;\n        if(cfg.transition===\'brutal\'){\n          p=cfg.absmin;adj=0;\n        } else if(cfg.transition===\'sinusoide\'){\n          force=-Math.sin(dist*Math.PI)*0.15-0.02;\n          adj+=force;\n        } else {\n          // Progressif : force proportionnelle, min 2% par tick\n          force=-(dist*0.3+0.02);\n          adj+=force;\n        }\n        if(p*(1+adj)<=cfg.absmin*1.005){\n          p=cfg.absmin;adj=0;\n          _cy.phase=\'atmin\';\n          var holdTicks=cfg.holdMin+Math.random()*(cfg.holdMax-cfg.holdMin);\n          _cy.holdUntil=now+holdTicks*cfg.holdUnit*12000;\n          addLog(\'📅\',\'✅ Minimum atteint: \'+fmt(cfg.absmin,0)+\'R · maintien \'+Math.round(holdTicks*cfg.holdUnit)+\'min\');\n        }\n      }\n\n      if(_cy.phase===\'tomax\'){\n        var dist=(cfg.absmax-p)/range;\n        var force;\n        if(cfg.transition===\'brutal\'){\n          p=cfg.absmax;adj=0;\n        } else if(cfg.transition===\'sinusoide\'){\n          force=Math.sin(dist*Math.PI)*0.15+0.02;\n          adj+=force;\n        } else {\n          force=dist*0.3+0.02;\n          adj+=force;\n        }\n        if(p*(1+adj)>=cfg.absmax*0.995){\n          p=cfg.absmax;adj=0;\n          _cy.phase=\'atmax\';\n          var holdTicks=cfg.holdMin+Math.random()*(cfg.holdMax-cfg.holdMin);\n          _cy.holdUntil=now+holdTicks*cfg.holdUnit*12000;\n          addLog(\'📅\',\'✅ Maximum atteint: \'+fmt(cfg.absmax,0)+\'R · maintien \'+Math.round(holdTicks*cfg.holdUnit)+\'min\');\n        }\n      }\n    }\n\n    p=Math.max(cfg.absmin,Math.min(cfg.absmax,p*(1+adj)));\n    p=Math.round(p*100)/100;\n\n    var rem=Math.max(0,Math.round((_cy.holdUntil-now)/1000));\n    var phaseLabel={\'normal\':\'Oscillation libre\',\'tomin\':\'↓ Descente vers min\',\'atmin\':\'🟢 Au minimum\',\'tomax\':\'↑ Montée vers max\',\'atmax\':\'🔴 Au maximum\'}[_cy.phase]||_cy.phase;\n    var st=$(\'cy-status\');\n    if(st)st.innerHTML=\'<b style="color:var(--cyan)">\'+phaseLabel+\'</b>\'+(_cy.holdUntil>now?\' · maintien encore <b>\'+rem+\'s</b>\':\'\')\n      +\'<br><span style="font-size:10px">Prix: \'+fmt(p,2)+\' R · Min: \'+fmt(cfg.absmin,0)+\' R · Max: \'+fmt(cfg.absmax,0)+\' R</span>\';\n\n    await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:Math.floor(Math.random()*200+20),volume24:(mkt.volume24||0)+80,trades24:(mkt.trades24||0)+1})});\n  },iv);\n}\n\nfunction stopCycles(){\n  _cy.active=false;_cy.phase=\'normal\';\n  if(_cy.int){clearInterval(_cy.int);_cy.int=null;}\n  if(tMode===\'cycles\'){tMode=null;tInt=null;}\n  $(\'cy-start-btn\').style.display=\'block\';$(\'cy-stop-btn\').style.display=\'none\';\n  var el=$(\'cy-status\');if(el)el.textContent=\'Cycles désactivés\';\n  var el2=$(\'tst\');if(el2){el2.textContent=\'⏸ Arrêté\';el2.style.color=\'var(--muted)\';}\n  addLog(\'📅\',\'Cycles désactivés\');\n}\n\nfunction previewCycle(){\n  var cfg=getCyConfig();var cv=$(\'cy-preview\');if(!cv||!window.Chart)return;\n  if(_cyPreviewObj){_cyPreviewObj.destroy();_cyPreviewObj=null;}\n  var pts=[];var p=(cfg.absmin+cfg.absmax)/2;\n  var pMin=calcProbPerTick(cfg.freqMin);var pMax=calcProbPerTick(cfg.freqMax);\n  var phase=\'normal\';var holdUntil=0;\n  for(var t=0;t<100;t++){\n    var adj=(Math.random()-0.5)*cfg.volBg*2+cfg.drift;\n    if(Math.random()<cfg.spikeProb)adj+=(Math.random()>0.5?1:-1)*cfg.spikeAmp*Math.random();\n    if(t>holdUntil){\n      if(phase!==\'tomin\'&&phase!==\'tomax\'){\n        if(Math.random()<pMin)phase=\'tomin\';\n        else if(Math.random()<pMax)phase=\'tomax\';\n        else phase=\'normal\';\n      }\n      if(phase===\'tomin\'){adj-=0.01*(1+cfg.bounce);if(p<=cfg.absmin*1.01){phase=\'atmin\';holdUntil=t+3;}}\n      if(phase===\'tomax\'){adj+=0.01*(1+cfg.resist);if(p>=cfg.absmax*0.99){phase=\'atmax\';holdUntil=t+3;}}\n    }\n    p=Math.max(cfg.absmin,Math.min(cfg.absmax,p*(1+adj)));\n    pts.push(Math.round(p*100)/100);\n  }\n  var labs=pts.map((_,i)=>\'T\'+i);\n  var ctx=cv.getContext(\'2d\');\n  var g=ctx.createLinearGradient(0,0,0,150);g.addColorStop(0,\'rgba(0,229,255,.2)\');g.addColorStop(1,\'rgba(0,229,255,0)\');\n  _cyPreviewObj=new Chart(ctx,{type:\'line\',data:{labels:labs,datasets:[\n    {data:pts,borderColor:\'#00e5ff\',backgroundColor:g,borderWidth:2,pointRadius:0,fill:true,tension:0.3},\n    {data:Array(100).fill(cfg.absmin),borderColor:\'rgba(0,255,157,.4)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[4,4]},\n    {data:Array(100).fill(cfg.absmax),borderColor:\'rgba(255,61,94,.4)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[4,4]},\n  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{display:false},y:{ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n}\n\n\n// ══ INFOS BULLES ══\nvar _infos={\n  bornes:"Les bornes sont les limites absolues du prix NXC. Le prix ne pourra jamais descendre en dessous du minimum ni monter au-dessus du maximum, quoi qu\'il arrive.",\n  freq:"Définit combien de fois le prix touchera exactement son minimum ou maximum dans chaque période. Le moteur calcule automatiquement la probabilité par tick (intervalle de 12s par défaut) pour respecter ces frequences.",\n  "freq-min":"Par minute : combien de fois dans la prochaine minute le prix touchera son minimum (colonne verte) ou maximum (colonne rouge). 0 = jamais dans la minute.",\n  "freq-h":"Par heure : combien de fois dans la prochaine heure le prix touchera son minimum ou maximum. Ex: 2 = deux fois dans l\'heure.",\n  "freq-d":"Par jour : combien de fois dans les 24 prochaines heures le prix touchera son minimum ou maximum.",\n  "freq-w":"Par semaine : combien de fois dans les 7 prochains jours le prix touchera son minimum ou maximum.",\n  "freq-mo":"Par mois (30 jours) : combien de fois dans le mois le prix touchera son minimum ou maximum.",\n  "freq-y":"Par an (365 jours) : combien de fois dans l\'année le prix touchera son minimum ou maximum. Ex: 4 = une fois par trimestre.",\n  "freq-custom":"Durée personnalisée : définir une période sur mesure. Ex: 6 heures, 2 jours... et combien de fois le prix touchera les extrêmes dans cette durée.",\n  comportement:"Paramètres qui définissent comment le prix se comporte quand il se déplace vers un extrême.",\n  transition:"Comment le prix atteint le min ou le max. Brutal = saut instantané. Progressif = descente/montée sur plusieurs ticks. Sinusoïde = courbe douce et naturelle.",\n  hold:"Combien de temps le prix reste au minimum ou maximum avant de repartir. Une durée aléatoire entre Min et Max est choisie à chaque fois.",\n  drift:"Tendance de fond sur le long terme. +2%/j = le prix a une légère tendance à monter de 2% par jour en moyenne. 0 = aucune tendance.",\n  volbg:"Quantité de mouvement aléatoire à chaque tick, indépendant des cycles. 0% = prix totalement lisse entre les cycles. Plus élevé = plus de micro-variations.",\n  spike:"Probabilité qu\'un pic inattendu se produise à chaque tick. Ex: 5% = 1 chance sur 20 à chaque tick d\'avoir un mouvement brutal.",\n  spikeamp:"Amplitude maximale d\'un pic surprise. ±10% = le pic peut faire bouger le prix de jusqu\'à 10% instantanément.",\n  bounce:"Force du rebond quand le prix touche le plancher. 0% = s\'arrête exactement au plancher. 5% = rebondit légèrement vers le haut.",\n  resist:"Résistance quand le prix approche du plafond. 0% = monte jusqu\'au plafond facilement. 5% = plus difficile de dépasser le plafond.",\n  activation:"Active le moteur de cycles. Une fois activé, le prix suivra automatiquement les frequences définies pour atteindre les extrêmes.",\n  preview:"Simule 100 ticks avec les paramètres actuels pour voir à quoi ressemblera le comportement du prix avant de l\'activer.",\n  \'prev-params\':"Choisir la duree de simulation, le nombre de points et le scenario. Plus il y a de points, plus la simulation est précise mais plus elle est longue.",\n  \'prev-scenario\':"Paramètres actuels = utilise exactement les réglages de l\'onglet Contrôle et Cycles. Haussier/Baissier = force une tendance. Volatile = forte agitation. Stable = peu de mouvement. Cycles = utilise les paramètres de cycles.",\n  \'prev-start\':"Le prix à partir duquel la simulation démarre. Par défaut c\'est le prix actuel du NXC.",\n  \'prev-result\':"Les statistiques clés extraites de la simulation : prix final prévu, variation, minimum et maximum atteints, volatilité moyenne.",\n  \'prev-g1\':"Graphique de l\'évolution du prix au fil du temps. Tu peux modifier les axes Y pour zoomer sur une zone précise.",\n  \'prev-g2\':"Trois scenarios calcules en parallele : pessimiste (prix bas), realiste (prix moyen), optimiste (prix haut). La zone bleue montre l\'incertitude totale.",\n  \'prev-g3\':"La volatilité à chaque point : mesure à quel point le prix change rapidement. Un pic = le prix bouge beaucoup à cet instant.",\n  \'prev-g4\':"Histogramme montrant à quels prix le NXC passe le plus de temps. Les barres les plus hautes = zones de prix fréquentes.",\n  \'prev-g5\':"Le drawdown = la chute depuis le sommet le plus récent. Permet de voir le pire cas de perte depuis un pic.",\n  \'prev-g6\':"Si tu achètes X rewards de NXC au prix actuel, combien vaudront-ils dans le temps selon la simulation ?"\n\n};\n\nfunction showInfo(key){\n  var modal=$(\'info-modal\');if(!modal)return;\n  $(\'info-title\').textContent=\'ℹ️ \'+key.replace(/-/g,\' \').replace(/\\b\\w/g,c=>c.toUpperCase());\n  $(\'info-body\').textContent=_infos[key]||\'Information non disponible.\';\n  modal.style.display=\'flex\';\n}\n\n// ══ PROBABILITÉS PAR TICK ══\nfunction updateCyProb(){\n  var ticksPerMin=5,ticksPerH=300,ticksPerD=7200,ticksPerW=50400,ticksPerMo=216000,ticksPerY=2628000;\n  var customDur=parseFloat($(\'cy-custom-dur\').value)||0;\n  var customUnit=parseFloat($(\'cy-custom-unit\').value)||3600000;\n  var customMs=customDur*customUnit;\n  var customTicks=customMs/12000;\n\n  var freqMin={m:parseFloat($(\'cy-min-m\').value)||0,h:parseFloat($(\'cy-min-h\').value)||0,d:parseFloat($(\'cy-min-d\').value)||0,w:parseFloat($(\'cy-min-w\').value)||0,mo:parseFloat($(\'cy-min-mo\').value)||0,y:parseFloat($(\'cy-min-y\').value)||0,c:parseFloat($(\'cy-min-c\').value)||0};\n  var freqMax={m:parseFloat($(\'cy-max-m\').value)||0,h:parseFloat($(\'cy-max-h\').value)||0,d:parseFloat($(\'cy-max-d\').value)||0,w:parseFloat($(\'cy-max-w\').value)||0,mo:parseFloat($(\'cy-max-mo\').value)||0,y:parseFloat($(\'cy-max-y\').value)||0,c:parseFloat($(\'cy-max-c\').value)||0};\n\n  var pMin=freqMin.m/ticksPerMin+freqMin.h/ticksPerH+freqMin.d/ticksPerD+freqMin.w/ticksPerW+freqMin.mo/ticksPerMo+freqMin.y/ticksPerY+(customTicks>0?freqMin.c/customTicks:0);\n  var pMax=freqMax.m/ticksPerMin+freqMax.h/ticksPerH+freqMax.d/ticksPerD+freqMax.w/ticksPerW+freqMax.mo/ticksPerMo+freqMax.y/ticksPerY+(customTicks>0?freqMax.c/customTicks:0);\n\n  pMin=Math.min(pMin,0.8);pMax=Math.min(pMax,0.8);\n\n  // Estimation des frequences résultantes\n  var estPerH_min=Math.round(pMin*ticksPerH*10)/10;\n  var estPerH_max=Math.round(pMax*ticksPerH*10)/10;\n  var estPerD_min=Math.round(pMin*ticksPerD);\n  var estPerD_max=Math.round(pMax*ticksPerD);\n\n  var el=$(\'cy-prob-display\');if(!el)return;\n  el.innerHTML=\n    \'<b style="color:var(--green)">MIN</b> — probabilité/tick: <b>\'+(pMin*100).toFixed(3)+\'%</b> · ~\'+estPerH_min+\'/heure · ~\'+estPerD_min+\'/jour<br>\'\n    +\'<b style="color:var(--red)">MAX</b> — probabilité/tick: <b>\'+(pMax*100).toFixed(3)+\'%</b> · ~\'+estPerH_max+\'/heure · ~\'+estPerD_max+\'/jour<br>\'\n    +(pMin+pMax>0.5?\'<span style="color:var(--red)">⚠️ Frequences tres elevees - le prix sera souvent aux extremes</span>\':\'<span style="color:var(--green)">✅ Frequences realistes</span>\');\n\n  window._cyPMin=pMin;window._cyPMax=pMax;\n}\n\n\n// ══ PRÉVISIONS ══\nvar _pvScenario=\'current\';\nvar _pvCharts={g1:null,g2:null,g3:null,g4:null,g5:null,g6:null};\nvar _pvData=null;\n\nfunction setPvScenario(sc){\n  _pvScenario=sc;\n  document.querySelectorAll(\'[id^="pv-sc-"]\').forEach(b=>{b.className=\'btn\';b.style.fontSize=\'11px\';b.style.padding=\'8px\';});\n  var b=$(\'pv-sc-\'+sc);if(b){b.className=\'btn cyan\';b.style.fontSize=\'11px\';b.style.padding=\'8px\';}\n}\n\nfunction simStep(p,scenario,floor,ceil,drift,volBg){\n  var adj=drift+(Math.random()-0.5)*volBg*2;\n  if(scenario===\'bull\')adj+=0.003+(Math.random()-0.3)*volBg;\n  else if(scenario===\'bear\')adj-=0.003+(Math.random()-0.7)*volBg;\n  else if(scenario===\'volatile\')adj+=(Math.random()-0.5)*0.05;\n  else if(scenario===\'stable\')adj*=0.1;\n  else if(scenario===\'cycles\'){\n    if(Math.random()<0.02)adj-=0.08; // descente cycle\n    if(Math.random()<0.02)adj+=0.08; // montée cycle\n  }\n  return Math.max(floor,Math.min(ceil,p*(1+adj)));\n}\n\nfunction runSimulation(){\n  var dur=parseFloat($(\'pv-dur\').value)||24;\n  var unit=parseFloat($(\'pv-unit\').value)||3600;\n  var totalSec=dur*unit;\n  var pts=parseInt($(\'pv-pts\').value)||100;\n  var startP=parseFloat($(\'pv-start\').value)||parseFloat(mkt.price||5213);\n  var floor=_cfgFloor||50;\n  var ceil=_cfgCeil||100000;\n  var drift=parseFloat($(\'cy-drift\')?.value||0)/100/1440;\n  var volBg=parseFloat($(\'cy-vol-bg\')?.value||2)/1000;\n  var sc=_pvScenario;\n\n  $(\'pv-loading\').style.display=\'block\';\n  $(\'pv-result-card\').style.display=\'none\';\n\n  setTimeout(function(){\n    // Simulation principale (realiste)\n    var main=[startP];\n    for(var j=0;j<pts-1;j++)main.push(simStep(main[main.length-1],sc,floor,ceil,drift,volBg));\n\n    // Scenario optimiste (+50% volatilité, drift positif)\n    var opt=[startP];\n    for(var j=0;j<pts-1;j++)opt.push(simStep(opt[opt.length-1],\'bull\',floor,ceil,drift+0.001,volBg*1.5));\n\n    // Scenario pessimiste (-50% volatilité, drift négatif)\n    var pes=[startP];\n    for(var j=0;j<pts-1;j++)pes.push(simStep(pes[pes.length-1],\'bear\',floor,ceil,drift-0.001,volBg*1.5));\n\n    // Labels temporels\n    var secPerPt=totalSec/pts;\n    var labs=[];\n    for(var j=0;j<pts;j++){\n      var s=j*secPerPt;\n      if(totalSec<=3600)labs.push(Math.round(s/60)+\'min\');\n      else if(totalSec<=86400)labs.push(Math.round(s/3600)+\'h\');\n      else if(totalSec<=2592000)labs.push(Math.round(s/86400)+\'j\');\n      else if(totalSec<=31536000)labs.push(Math.round(s/2592000)+\'mois\');\n      else labs.push((s/31536000).toFixed(1)+\'an\');\n    }\n\n    _pvData={main,opt,pes,labs,startP,floor,ceil,pts,totalSec,secPerPt};\n\n    // Stats\n    var finalP=main[main.length-1];\n    var minP=Math.min(...main);var maxP=Math.max(...main);\n    var chg=(finalP-startP)/startP*100;\n    var vols=[];for(var j=1;j<main.length;j++)vols.push(Math.abs(main[j]-main[j-1])/main[j-1]*100);\n    var avgVol=vols.reduce((s,v)=>s+v,0)/vols.length;\n\n    $(\'pv-stats\').innerHTML=[\n      [\'Prix final\',fmt(finalP,2)+\' R\',(chg>=0?\'var(--green)\':\'var(--red)\')],\n      [\'Variation\',(chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%\',(chg>=0?\'var(--green)\':\'var(--red)\')],\n      [\'Minimum atteint\',fmt(minP,2)+\' R\',\'var(--red)\'],\n      [\'Maximum atteint\',fmt(maxP,2)+\' R\',\'var(--green)\'],\n      [\'Volatilité moy.\',avgVol.toFixed(3)+\'%/tick\',\'var(--gold)\'],\n      [\'Amplitude\',fmt(maxP-minP,0)+\' R\',\'var(--purple)\'],\n      [\'Durée simulée\',dur+\' \'+([$(\'pv-unit\').selectedOptions[0].text]||[\'\']),\'var(--cyan)\'],\n      [\'Points simulés\',pts,\'var(--muted)\'],\n    ].map(([k,v,col])=>\'<div class="st"><div class="sv" style="color:\'+col+\';font-size:13px">\'+v+\'</div><div class="sl">\'+k+\'</div></div>\').join(\'\');\n\n    drawAllPvCharts();\n    $(\'pv-loading\').style.display=\'none\';\n    $(\'pv-result-card\').style.display=\'block\';\n  },50);\n}\n\nfunction destroyChart(key){if(_pvCharts[key]){try{_pvCharts[key].destroy();}catch(e){}}_pvCharts[key]=null;}\n\nfunction drawAllPvCharts(){\n  if(!_pvData||!window.Chart)return;\n  var d=_pvData;\n  var g1ymin=parseFloat($(\'pv-g1-ymin\').value)||undefined;\n  var g1ymax=parseFloat($(\'pv-g1-ymax\').value)||undefined;\n  var g2ymin=parseFloat($(\'pv-g2-ymin\').value)||undefined;\n  var g2ymax=parseFloat($(\'pv-g2-ymax\').value)||undefined;\n  var g3ymax=parseFloat($(\'pv-g3-ymax\').value)||undefined;\n\n  // G1 : évolution prix\n  destroyChart(\'g1\');\n  var ctx1=$(\'pv-g1\').getContext(\'2d\');\n  var g=ctx1.createLinearGradient(0,0,0,240);g.addColorStop(0,\'rgba(0,229,255,.25)\');g.addColorStop(1,\'rgba(0,229,255,0)\');\n  _pvCharts.g1=new Chart(ctx1,{type:\'line\',data:{labels:d.labs,datasets:[\n    {label:\'Prix\',data:d.main,borderColor:\'#00e5ff\',backgroundColor:g,borderWidth:2.5,pointRadius:0,fill:true,tension:0.3},\n    {label:\'Plancher\',data:Array(d.pts).fill(d.floor),borderColor:\'rgba(0,255,157,.5)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[5,5]},\n    {label:\'Plafond\',data:Array(d.pts).fill(d.ceil),borderColor:\'rgba(255,61,94,.5)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[5,5]},\n  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:\'var(--muted)\',font:{size:9}}}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:8,font:{size:8}},grid:{color:\'rgba(0,229,255,.04)\'}},y:{min:g1ymin,max:g1ymax,ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n\n  // G2 : bandes de confiance\n  destroyChart(\'g2\');\n  var ctx2=$(\'pv-g2\').getContext(\'2d\');\n  _pvCharts.g2=new Chart(ctx2,{type:\'line\',data:{labels:d.labs,datasets:[\n    {label:\'Optimiste\',data:d.opt,borderColor:\'rgba(0,255,157,.8)\',borderWidth:1.5,pointRadius:0,fill:false,tension:0.3},\n    {label:\'Réaliste\',data:d.main,borderColor:\'#00e5ff\',borderWidth:2.5,pointRadius:0,fill:false,tension:0.3},\n    {label:\'Pessimiste\',data:d.pes,borderColor:\'rgba(255,61,94,.8)\',borderWidth:1.5,pointRadius:0,fill:\'-1\',backgroundColor:\'rgba(0,229,255,.08)\',tension:0.3},\n    {label:\'Plancher\',data:Array(d.pts).fill(d.floor),borderColor:\'rgba(0,255,157,.3)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[5,5]},\n    {label:\'Plafond\',data:Array(d.pts).fill(d.ceil),borderColor:\'rgba(255,61,94,.3)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[5,5]},\n  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:\'var(--muted)\',font:{size:9}}}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:8,font:{size:8}},grid:{color:\'rgba(0,229,255,.04)\'}},y:{min:g2ymin,max:g2ymax,ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n\n  // G3 : volatilité\n  destroyChart(\'g3\');\n  var vols=[];for(var j=1;j<d.main.length;j++)vols.push(Math.abs(d.main[j]-d.main[j-1])/d.main[j-1]*100);\n  var ctx3=$(\'pv-g3\').getContext(\'2d\');\n  _pvCharts.g3=new Chart(ctx3,{type:\'bar\',data:{labels:d.labs.slice(1),datasets:[{label:\'Volatilité %\',data:vols,backgroundColor:vols.map(v=>v>1?\'rgba(255,61,94,.6)\':v>0.5?\'rgba(255,176,32,.6)\':\'rgba(0,229,255,.5)\'),borderWidth:0}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{display:false},y:{max:g3ymax,ticks:{color:\'#5c6b8c\',callback:v=>v.toFixed(2)+\'%\'},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n\n  // G4 : histogramme distribution\n  destroyChart(\'g4\');\n  var bins=20;var minP=Math.min(...d.main);var maxP=Math.max(...d.main);var binW=(maxP-minP)/bins||1;\n  var hist=Array(bins).fill(0);var binLabs=[];\n  for(var j=0;j<bins;j++)binLabs.push(fmt(minP+j*binW,0));\n  d.main.forEach(p=>{var b=Math.min(bins-1,Math.floor((p-minP)/binW));hist[b]++;});\n  var ctx4=$(\'pv-g4\').getContext(\'2d\');\n  _pvCharts.g4=new Chart(ctx4,{type:\'bar\',data:{labels:binLabs,datasets:[{label:\'Fréquence\',data:hist,backgroundColor:\'rgba(0,255,157,.5)\',borderColor:\'var(--green)\',borderWidth:1}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:8,font:{size:8}},grid:{display:false}},y:{ticks:{color:\'#5c6b8c\'},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n\n  // G5 : drawdown\n  destroyChart(\'g5\');\n  var peak=d.main[0];var dd=d.main.map(p=>{if(p>peak)peak=p;return ((p-peak)/peak*100);});\n  var ctx5=$(\'pv-g5\').getContext(\'2d\');\n  var g5=ctx5.createLinearGradient(0,0,0,180);g5.addColorStop(0,\'rgba(255,61,94,.3)\');g5.addColorStop(1,\'rgba(255,61,94,0)\');\n  _pvCharts.g5=new Chart(ctx5,{type:\'line\',data:{labels:d.labs,datasets:[{label:\'Drawdown %\',data:dd,borderColor:\'var(--red)\',backgroundColor:g5,borderWidth:2,pointRadius:0,fill:true,tension:0.3}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:8,font:{size:8}},grid:{display:false}},y:{ticks:{color:\'#5c6b8c\',callback:v=>v.toFixed(1)+\'%\'},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n\n  // G6 : ROI\n  updateG6();\n}\n\nfunction updateG6(){\n  if(!_pvData||!window.Chart)return;\n  var d=_pvData;\n  var invest=parseFloat($(\'pv-roi-invest\').value)||1000;\n  var nxcBought=invest/d.startP;\n  var roiData=d.main.map(p=>nxcBought*p);\n  var roiPct=d.main.map(p=>((p-d.startP)/d.startP*100));\n  destroyChart(\'g6\');\n  var ctx6=$(\'pv-g6\').getContext(\'2d\');\n  var g6=ctx6.createLinearGradient(0,0,0,180);\n  g6.addColorStop(0,\'rgba(255,110,180,.25)\');g6.addColorStop(1,\'rgba(255,110,180,0)\');\n  _pvCharts.g6=new Chart(ctx6,{type:\'line\',data:{labels:d.labs,datasets:[\n    {label:\'Valeur (R)\',data:roiData,borderColor:\'#ff6eb4\',backgroundColor:g6,borderWidth:2.5,pointRadius:0,fill:true,tension:0.3,yAxisID:\'y\'},\n    {label:\'ROI %\',data:roiPct,borderColor:\'rgba(255,176,32,.7)\',borderWidth:1.5,pointRadius:0,fill:false,tension:0.3,yAxisID:\'y2\'},\n    {label:\'Mise initiale\',data:Array(d.pts).fill(invest),borderColor:\'rgba(255,255,255,.2)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[5,5],yAxisID:\'y\'},\n  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:\'var(--muted)\',font:{size:9}}}},scales:{\n    x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:8,font:{size:8}},grid:{display:false}},\n    y:{ticks:{color:\'#ff6eb4\',callback:v=>fmt(v,0)+\' R\'},grid:{color:\'rgba(0,229,255,.04)\'},position:\'left\'},\n    y2:{ticks:{color:\'var(--gold)\',callback:v=>v.toFixed(1)+\'%\'},grid:{display:false},position:\'right\'},\n  },animation:{duration:0}}});\n}\n\nfunction updateG1(){if(_pvData)drawAllPvCharts();}\nfunction updateG2(){if(_pvData)drawAllPvCharts();}\nfunction updateG3(){if(_pvData)drawAllPvCharts();}\n\nfunction zoomIn(key){if(_pvCharts[key])_pvCharts[key].zoom?_pvCharts[key].zoom(1.3):null;}\nfunction zoomOut(key){if(_pvCharts[key])_pvCharts[key].zoom?_pvCharts[key].zoom(0.7):null;}\nfunction resetZoom(key){if(_pvCharts[key])_pvCharts[key].resetZoom?_pvCharts[key].resetZoom():null;}\n\nfunction exportSimJSON(){\n  if(!_pvData)return;\n  var blob=new Blob([JSON.stringify(_pvData,null,2)],{type:\'application/json\'});\n  var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'simulation_\'+Date.now()+\'.json\';a.click();\n  addLog(\'📥\',\'Export simulation JSON\');\n}\nfunction exportSimCSV(){if(!_pvData)return;var d=_pvData;var out=\'Temps,Realiste,Optimiste,Pessimiste\';for(var j2=0;j2<d.pts;j2++){out+=String.fromCharCode(10)+d.labs[j2]+\',\'+d.main[j2]+\',\'+(d.opt[j2]||0)+\',\'+(d.pes[j2]||0);}var blob=new Blob([out],{type:\'text/csv\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'sim_\'+Date.now()+\'.csv\';a.click();}\nfunction printSim(){\n  if(!_pvData)return;\n  var imgs=[];\n  [\'pv-g1\',\'pv-g2\',\'pv-g3\',\'pv-g4\',\'pv-g5\',\'pv-g6\'].forEach(id=>{var cv=$(id);if(cv)imgs.push({id,src:cv.toDataURL()});});\n  var win=window.open(\'\',\'_blank\');\n  var stats=$(\'pv-stats\').innerHTML;\n  win.document.write(\'<!DOCTYPE html><html><head><meta charset="utf-8"><title>Simulation NXC</title><style>*{font-family:Arial,sans-serif;box-sizing:border-box}body{padding:20px;max-width:900px;margin:0 auto}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:20px}.st{border:1px solid #ddd;border-radius:8px;padding:10px;text-align:center}.sv{font-size:16px;font-weight:700;margin-bottom:4px}.sl{font-size:9px;text-transform:uppercase;color:#666}img{max-width:100%;border:1px solid #ddd;border-radius:8px;margin-bottom:16px}h3{margin:16px 0 8px;font-size:14px;border-bottom:1px solid #eee;padding-bottom:4px}</style></head><body>\');\n  win.document.write(\'<h2>◈ Simulation NXC — \'+new Date().toLocaleString(\'fr-FR\')+\'</h2>\');\n  win.document.write(\'<div class="grid">\'+stats+\'</div>\');\n  imgs.forEach((img,i)=>{win.document.write(\'<h3>Graphique \'+(i+1)+\'</h3><img src="\'+img.src+\'">\');});\n  win.document.write(\'</body></html>\');\n  win.document.close();setTimeout(()=>win.print(),500);\n  addLog(\'🖨️\',\'Impression simulation\');\n}\n\n\nfunction startApp(){\n  ref();\n  loadBank();\n  loadSolv();\n  loadFails();\n  loadPinnedSites();\n  setInterval(ref,15000);\n  setInterval(function(){loadBank();loadFails();},25000);\n  setInterval(function(){var e=$(\'htm\');if(e)e.textContent=new Date().toLocaleTimeString(\'fr-FR\');},1000);\n  var hd=$(\'hd\');if(hd)hd.classList.add(\'on\');\n  var htm=$(\'htm\');if(htm)htm.style.display=\'block\';\n  addLog(\'ok\',\'Connecte\');\n}\nKEY=\'change-moi-cle-maitre-nexus-2026\';\nstartApp();\n\n</script>\n</body>\n</html>\n'

ADMIN_HTML = '<!DOCTYPE html><html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Nexus — Administration</title><style>\n*{box-sizing:border-box;font-family:\'Segoe UI\',system-ui,Arial,sans-serif;}body{margin:0;background:#0a0d14;color:#eaf0fb;}a{color:#a06bff;}.wrap{max-width:920px;margin:0 auto;padding:18px;}h1{font-size:22px;margin:0 0 4px;}.muted{color:#8a96ad;font-size:13px;}.card{background:#121724;border:1px solid #283046;border-radius:14px;padding:16px;margin-top:14px;}input,select,button{font-size:15px;border-radius:10px;padding:11px 13px;border:1px solid #283046;background:#1b2233;color:#eaf0fb;outline:none;}input:focus,select:focus{border-color:#5b9dff;}button{cursor:pointer;}button:hover{border-color:#5b9dff;}.accent{border:none;font-weight:700;color:#06080c;background:linear-gradient(90deg,#5b9dff,#a06bff);}.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center;}.grow{flex:1;min-width:120px;}table{width:100%;border-collapse:collapse;margin-top:8px;}th,td{text-align:left;padding:10px 8px;border-bottom:1px solid #1c2333;font-size:14px;}th{color:#8a96ad;font-size:12px;text-transform:uppercase;letter-spacing:.5px;}.badge{font-size:11px;padding:2px 8px;border-radius:20px;}.adm{background:#3b2d5e;color:#c9b6ff;}.usr{background:#1e3346;color:#9ec7ff;}.act{background:transparent;border:1px solid #283046;padding:6px 9px;font-size:13px;border-radius:8px;}.ok{color:#34d399;}.warn{color:#f5b740;}.off{color:#ef5d6b;}.hidden{display:none;}.overlay{position:fixed;inset:0;background:rgba(0,0,0,.6);display:flex;align-items:center;justify-content:center;padding:16px;}.modal{background:#121724;border:1px solid #283046;border-radius:16px;padding:18px;max-width:560px;width:100%;max-height:85vh;overflow:auto;}pre{white-space:pre-wrap;word-break:break-word;background:#0a0d14;border:1px solid #283046;border-radius:10px;padding:10px;font-size:12px;color:#c7d2e6;}</style></head><body><div class="wrap"><h1>🛡️ Nexus — Administration</h1><div class="muted">Tout ce que tu fais ici est enregistré sur le serveur en ligne et récupéré par les serveurs locaux.</div><div id="login" class="card"><div class="row"><input id="mk" class="grow" type="password" placeholder="Clé maître"><button class="accent" onclick="connecter()">Se connecter</button></div><div id="loginmsg" class="muted" style="margin-top:8px"></div></div><div id="dash" class="hidden"><div class="card"><div class="row"><div class="grow"><span id="status" class="ok">Connecté</span></div><button onclick="location.href=\'/nexus\'">🌐 Nexus</button><button onclick="location.href=\'/nxc\'" style="background:#0d1428;border-color:#00e5ff;color:#00e5ff">◈ NXC</button><input id="search" class="grow" placeholder="🔍 Rechercher…" oninput="render()"><label class="muted"><input type="checkbox" id="showHidden" onchange="render()"> voir masqués</label></div></div><div class="card"><b>➕ Créer un compte</b><div class="row" style="margin-top:10px"><input id="nu" class="grow" placeholder="Nom d\'utilisateur"><input id="np" class="grow" type="text" placeholder="Mot de passe"><select id="nr"><option value="user">Utilisateur</option><option value="admin">Administrateur</option></select><button class="accent" onclick="creer()">Créer</button></div><div id="createmsg" class="muted" style="margin-top:8px"></div></div><div class="card"><div class="row"><b class="grow">Comptes (<span id="count">0</span>)</b><span class="muted" id="tick">actualisation auto…</span></div><table><thead><tr><th>Compte</th><th>Rôle</th><th>Pages</th><th>Dernière connexion</th><th></th></tr></thead><tbody id="tbody"></tbody></table></div></div></div><div id="modal"></div><script>\nlet KEY = "";let USERS = [];async function api(path,body){body = body ||{};body.master_key = KEY;const r = await fetch(path,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});return await r.json();}async function connecter(){KEY = document.getElementById("mk").value.trim();const msg = document.getElementById("loginmsg");msg.textContent = "Connexion…";const res = await api("/admin/list");if (res && res.ok){document.getElementById("login").classList.add("hidden");document.getElementById("dash").classList.remove("hidden");USERS = res.users || [];render();if (!window._timer) window._timer = setInterval(rafraichir,3000);}else{msg.innerHTML = "<span class=\'off\'>Clé maître refusée.</span>";}}async function rafraichir(){const res = await api("/admin/list");if (res && res.ok){USERS = res.users || [];document.getElementById("status").innerHTML = "<span class=\'ok\'>● En ligne — synchronisé</span>";render();const t = document.getElementById("tick");t.textContent = "à jour • " + new Date().toLocaleTimeString();}else{document.getElementById("status").innerHTML = "<span class=\'warn\'>● reconnexion…</span>";}}function render(){const q = document.getElementById("search").value.toLowerCase();const showHidden = document.getElementById("showHidden").checked;const tb = document.getElementById("tbody");tb.innerHTML = "";let shown = 0;USERS.forEach(u =>{if (u.hidden && !showHidden) return;if (q && !u.username.toLowerCase().includes(q) && !(u.nickname||"").toLowerCase().includes(q)) return;shown++;const tr = document.createElement("tr");const nick = u.nickname ? " « "+esc(u.nickname)+" »":"";const badge = u.role === "admin" ? "<span class=\'badge adm\'>👑 admin</span>":"<span class=\'badge usr\'>👤 user</span>";const mask = u.hidden ? "🙈 ":"";tr.innerHTML =\n"<td>"+mask+"<b>"+esc(u.username)+"</b>"+nick+"</td>"+\n"<td>"+badge+"</td>"+\n"<td>"+u.history+"</td>"+\n"<td class=\'muted\'>"+(u.last_login? esc(u.last_login)+" · "+esc(u.last_ip):"jamais")+"</td>"+\n"<td class=\'row\'>"+\n"<button class=\'act\' onclick=\\"voir(\'"+jsq(u.username)+"\')\\">Voir</button>"+\n"<button class=\'act\' onclick=\\"renommer(\'"+jsq(u.username)+"\')\\">Renommer</button>"+\n"<button class=\'act\' onclick=\\"surnom(\'"+jsq(u.username)+"\')\\">Surnom</button>"+\n"<button class=\'act\' onclick=\\"masquer(\'"+jsq(u.username)+"\',"+(u.hidden?"false":"true")+")\\">"+(u.hidden?"Afficher":"Masquer")+"</button>"+\n"<button class=\'act off\' onclick=\\"supprimer(\'"+jsq(u.username)+"\')\\">Suppr</button>"+\n"</td>";tb.appendChild(tr);});document.getElementById("count").textContent = shown;}async function creer(){const u = document.getElementById("nu").value.trim();const p = document.getElementById("np").value;const r = document.getElementById("nr").value;const msg = document.getElementById("createmsg");if (!u || !p){msg.innerHTML = "<span class=\'warn\'>Nom et mot de passe requis.</span>";return;}const res = await api("/admin/create",{new_username:u,new_password:p,role:r});if (res.ok){msg.innerHTML = "<span class=\'ok\'>Compte « "+esc(u)+" » créé ✅</span>";document.getElementById("nu").value="";document.getElementById("np").value="";rafraichir();}else{msg.innerHTML = "<span class=\'off\'>"+esc(res.error||"erreur")+"</span>";}}async function voir(name){const res = await api("/admin/get",{target:name});if (!res.ok) return;const logins = (res.logins||[]).slice(0,20).map(l => " "+l.time+" — "+l.ip).join("\\n") || " (aucune)";const nx2098 = ((res.data||{}).nx2098||{});const nxcoin = ((res.data||{}).nxcoin||{});const nxInfo = " Rewards:"+(nx2098.rewards||0)+"\\n NXC:"+(nxcoin.nxc||0);openModal("<h3>"+esc(name)+"</h3>"+\n"<div class=\'muted\'>Rôle:"+esc(res.role)+(res.nickname?" · « "+esc(res.nickname)+" »":"")+"</div>"+\n"<b>◈ NXC Coin</b><pre>"+esc(nxInfo)+"</pre>"+\n"<b>Connexions (IP + heure)</b><pre>"+esc(logins)+"</pre>"+\n"<button class=\'accent\' onclick=\'closeModal()\'>Fermer</button>");}async function renommer(name){const nn = prompt("Nouveau nom pour « "+name+" »:",name);if (!nn || !nn.trim()) return;const res = await api("/admin/rename",{target:name,new_username:nn.trim()});if (!res.ok) alert(res.error||"erreur");rafraichir();}async function surnom(name){const nk = prompt("Surnom pour « "+name+" »:","");if (nk === null) return;await api("/admin/nickname",{target:name,nickname:nk});rafraichir();}async function masquer(name,hide){await api("/admin/hide",{target:name,hidden:hide});rafraichir();}async function supprimer(name){if (!confirm("Supprimer DÉFINITIVEMENT « "+name+" » ?")) return;await api("/admin/purge",{target:name});rafraichir();}function openModal(html){document.getElementById("modal").innerHTML =\n"<div class=\'overlay\' onclick=\'if(event.target===this)closeModal()\'><div class=\'modal\'>"+html+"</div></div>";}function closeModal(){document.getElementById("modal").innerHTML="";}function esc(s){return (s+"").replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",\'"\':"&quot;"}[c]));}function jsq(s){return (s+"").replace(/\\\\/g,"\\\\\\\\").replace(/\'/g,"\\\\\'");}document.getElementById("mk").addEventListener("keydown",e=>{if(e.key==="Enter") connecter();});</script></body></html>'

@app.get("/")
def home():
    db = load_db()
    n = len(db["users"])
    a = sum(1 for u in db["users"].values() if u.get("role") == "admin")
    p = NXC_MARKET["price"]
    return (f"<body style='font-family:sans-serif;background:#0b0f17;color:#eaf0fb;"
            f"text-align:center;padding-top:60px'>"
            f"<h1 style='color:#5b9dff'>Nexus Server &#9989;</h1>"
            f"<p>En ligne — {n} compte(s), {a} admin(s).</p>"
            f"<p style='color:#00e5ff'>◈ NXC : {p:,.2f} R/NXC</p>"
            f"<p><a style='color:#a06bff' href='/panel'>Panneau d'administration &#8594;</a></p>"
            f"<p><a style='color:#00e5ff' href='/nxc'>◈ Panneau NXC &#8594;</a></p>"
            f"<p><a style='color:#5b9dff' href='/nexus'>Ouvrir Nexus Web &#8594;</a></p></body>")


@app.get("/panel")
def panel():
    return Response(ADMIN_HTML, mimetype="text/html")


@app.get("/nxc")
def nxc_panel():
    return Response(NXC_PANEL_HTML, mimetype="text/html")


# ══ ENDPOINTS NXC PRIX ══



@app.route("/admin/set-role", methods=["POST"])
def admin_set_role():
    """Change le role d un utilisateur."""
    body = request.get_json(force=True, silent=True) or {}
    mk = body.get("master_key") or ""
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    target = body.get("target") or ""
    role = body.get("role") or "user"
    if role not in ("user", "admin", "moderator", "vip"):
        return jsonify({"ok": False, "error": "Role invalide"})
    with _lock:
        db = load_db()
        if target not in db.get("users", {}):
            return jsonify({"ok": False, "error": "Utilisateur introuvable"})
        db["users"][target]["role"] = role
        save_db(db)
    return jsonify({"ok": True, "target": target, "role": role})

@app.route("/admin/give-rewards", methods=["POST"])
def admin_give_rewards():
    """Donne des rewards a un utilisateur directement sans passer par la banque."""
    body = request.get_json(force=True, silent=True) or {}
    mk = body.get("master_key") or ""
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    target = body.get("target") or ""
    amount = float(body.get("amount") or 0)
    if not target or amount <= 0:
        return jsonify({"ok": False, "error": "Parametres invalides"})
    with _lock:
        db = load_db()
        users = db.get("users", {})
        if target not in users:
            return jsonify({"ok": False, "error": "Utilisateur introuvable"})
        # Debiter la banque
        noah = db.get("users", {}).get("noah", {})
        bank = noah.get("data", {}).get("nxcoin_bank", {})
        reserves = float(bank.get("reserves") or 0)
        if reserves < amount:
            return jsonify({"ok": False, "error": "Reserves bancaires insuffisantes (" + str(round(reserves,2)) + " R disponibles)"})
        bank["reserves"] = round(reserves - amount, 2)
        bank["totalOut"] = round(float(bank.get("totalOut") or 0) + amount, 2)
        bank.setdefault("flux", []).append({
            "type": "OUT", "user": "ADMIN->"+target,
            "amount": amount, "nxc": 0,
            "ts": int(__import__("time").time()*1000)
        })
        noah.setdefault("data", {})["nxcoin_bank"] = bank
        # Crediter l utilisateur
        if "data" not in users[target]:
            users[target]["data"] = {}
        if "nx2098" not in users[target]["data"]:
            users[target]["data"]["nx2098"] = {}
        if "rewards" not in users[target]["data"]:
            users[target]["data"]["rewards"] = {"points": 0}
        current = float(users[target]["data"]["nx2098"].get("rewards") or 0)
        new_total = round(current + amount, 2)
        users[target]["data"]["nx2098"]["rewards"] = new_total
        users[target]["data"]["rewards"]["points"] = new_total
        db["users"] = users
        save_db(db)
    return jsonify({"ok": True, "new_rewards": new_total, "bank_reserves": bank["reserves"]})


@app.route("/admin/save-data", methods=["POST"])
def admin_save_data():
    """Sauvegarde ou restaure toutes les donnees du serveur."""
    body = request.get_json(force=True, silent=True) or {}
    mk = body.get("master_key") or ""
    action = body.get("action") or "export"
    if action == "import":
        # Import : ne requiert pas forcément de connexion mais on vérifie quand même
        data = body.get("data") or {}
        if data:
            with _lock:
                db = load_db()
                if "market" in data:
                    db["nxc_market"] = data["market"]
                if "bank" in data:
                    noah = db.get("users", {}).get("noah", {})
                    noah.setdefault("data", {})["nxcoin_bank"] = data["bank"]
                if "users" in data:
                    for uname, udata in data.get("users", {}).items():
                        if uname in db.get("users", {}):
                            db["users"][uname]["data"] = udata
                save_db(db)
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": "Donnees invalides"})
    # Export
    with _lock:
        db = load_db()
    return jsonify({"ok": True, "data": db})


@app.route("/admin/pinned-sites", methods=["GET", "POST"])
def admin_pinned_sites():
    """GET: retourne les sites epingles. POST: sauvegarde les sites epingles."""
    if request.method == "GET":
        with _lock:
            db = load_db()
        sites = db.get("pinned_sites", [])
        return jsonify({"ok": True, "sites": sites})
    body = request.get_json(force=True, silent=True) or {}
    mk = body.get("master_key") or ""
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    sites = body.get("sites") or []
    with _lock:
        db = load_db()
        db["pinned_sites"] = sites
        save_db(db)
    return jsonify({"ok": True})

@app.route("/nxc/price", methods=["GET", "POST"])
def nxc_price():
    """Prix NXC en temps réel. Le prix evolue AU MOMENT de la lecture
    selon le temps ecoule — aucun thread necessaire, fiable sur Render."""
    now_ms = int(time.time() * 1000)
    last_ts = NXC_MARKET.get("ts") or 0
    if last_ts <= 0:
        NXC_MARKET["ts"] = now_ms
        last_ts = now_ms
    TICK_MS = 15000
    elapsed = now_ms - last_ts
    if elapsed > 3600000: NXC_MARKET["ts"] = now_ms - TICK_MS; elapsed = TICK_MS
    n = min(int(elapsed // TICK_MS), 10)  # max 10 ticks de rattrapage
    if n > 0:
        p = float(NXC_MARKET["price"])
        for i in range(n):
            sigma = 0.008 + _rnd.random() * 0.015
            adj = (_rnd.random() - 0.48) * sigma
            if p > 80000: adj -= 0.012
            if p < 200: adj += 0.018
            p = max(50.0, min(100000.0, p * (1 + adj)))
            p = round(p * 100) / 100 if _rnd.random() > 0.03 else float(round(p))
            t = last_ts + (i + 1) * TICK_MS
            NXC_MARKET["history"].append(
                {"price": p, "ts": t, "vol": int(_rnd.random() * 800 + 30)})
        NXC_MARKET["price"] = p
        NXC_MARKET["ts"] = last_ts + n * TICK_MS
        if len(NXC_MARKET["history"]) > 576:
            NXC_MARKET["history"] = NXC_MARKET["history"][-576:]
        # Persister a CHAQUE tick pour survivre aux redemarrages
        try:
            with _lock:
                db = load_db()
                noah = db.get("users", {}).get("noah")
                if noah is not None:
                    noah.setdefault("data", {})["nxcoin_market"] = {
                        "price": NXC_MARKET["price"],
                        "history": NXC_MARKET["history"][-144:],
                        "volume24": NXC_MARKET["volume24"],
                        "trades24": NXC_MARKET["trades24"],
                        "ts": NXC_MARKET["ts"]}
                    save_db(db)
        except Exception:
            pass
    return jsonify({
        "ok": True,
        "price": NXC_MARKET["price"],
        "ts": NXC_MARKET["ts"],
        "volume24": NXC_MARKET["volume24"],
        "trades24": NXC_MARKET["trades24"],
        "history": NXC_MARKET["history"][-144:]
    })


@app.route("/nxc/tick", methods=["POST"])
def nxc_tick():
    """Mise à jour du prix NXC — requiert master_key."""
    body = request.get_json(force=True, silent=True) or {}
    mk = body.get("master_key") or ""
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify(ok=False, error="Unauthorized"), 403
    price = float(body.get("price", 0))
    if price < 50 or price > 100000:
        return jsonify(ok=False, error="Prix invalide"), 400
    NXC_MARKET["price"] = price
    NXC_MARKET["ts"] = body.get("ts", int(time.time() * 1000))
    NXC_MARKET["volume24"] = body.get("volume24", NXC_MARKET["volume24"])
    NXC_MARKET["trades24"] = body.get("trades24", NXC_MARKET["trades24"])
    entry = {"price": price, "ts": NXC_MARKET["ts"], "vol": body.get("vol", 100)}
    NXC_MARKET["history"].append(entry)
    if len(NXC_MARKET["history"]) > 576:
        NXC_MARKET["history"] = NXC_MARKET["history"][-576:]
    return jsonify(ok=True)


@app.route("/nxc/bank", methods=["GET", "POST"])
def nxc_bank():
    """Banque NXC partagee entre tous les appareils.
    GET : retourne bankData depuis noah.
    POST {master_key, bank} : met a jour bankData sur noah.
    """
    if request.method == "GET":
        try:
            with _lock:
                db = load_db()
                noah = db.get("users", {}).get("noah", {})
                bank = noah.get("data", {}).get("nxcoin_bank",
                    {"reserves": 0, "nxcEmis": 0, "totalIn": 0, "totalOut": 0, "flux": []})
            return jsonify({"ok": True, "bank": bank})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)})
    # POST : mettre a jour
    body = request.get_json(force=True, silent=True) or {}
    mk = body.get("master_key") or ""
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    incoming = body.get("bank") or {}
    force_reset = bool(body.get("reset", False))
    try:
        with _lock:
            db = load_db()
            noah = db.get("users", {}).get("noah")
            if noah is None:
                return jsonify({"ok": False, "error": "Compte noah introuvable"})
            current = noah.get("data", {}).get("nxcoin_bank",
                {"reserves": 0, "nxcEmis": 0, "totalIn": 0, "totalOut": 0, "flux": []})
            if force_reset:
                # Reset : ecraser completement sans fusion
                new_bank = {
                    "reserves": round(float(incoming.get("reserves", 0)), 2),
                    "nxcEmis": round(float(incoming.get("nxcEmis", 0)), 4),
                    "totalIn": round(float(incoming.get("totalIn", 0)), 2),
                    "totalOut": round(float(incoming.get("totalOut", 0)), 2),
                    "flux": incoming.get("flux", [])
                }
            else:
                # Mode normal : fusion anti-duplication
                all_flux = list(current.get("flux", []))
                existing_ts = {f.get("ts") for f in all_flux}
                for f in incoming.get("flux", []):
                    if f.get("ts") not in existing_ts:
                        all_flux.append(f)
                        existing_ts.add(f.get("ts"))
                all_flux = sorted(all_flux, key=lambda x: x.get("ts", 0))[-200:]
                new_bank = {
                    "reserves": round(float(incoming.get("reserves", current.get("reserves", 0))), 2),
                    "nxcEmis": round(max(float(incoming.get("nxcEmis", 0)), float(current.get("nxcEmis", 0))), 4),
                    "totalIn": round(max(float(incoming.get("totalIn", 0)), float(current.get("totalIn", 0))), 2),
                    "totalOut": round(max(float(incoming.get("totalOut", 0)), float(current.get("totalOut", 0))), 2),
                    "flux": all_flux
                }
            noah.setdefault("data", {})["nxcoin_bank"] = new_bank
            save_db(db)
        return jsonify({"ok": True, "bank": new_bank})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/nxc/solvability", methods=["GET", "POST"])
def nxc_solvability():
    """GET : retourne les parametres de solvabilite.
    POST {master_key, enabled, gesture} : met a jour les parametres."""
    if request.method == "GET":
        return jsonify({"ok": True, "enabled": NXC_SOLVABILITY["enabled"],
                        "gesture": NXC_SOLVABILITY["gesture"]})
    body = request.get_json(force=True, silent=True) or {}
    mk = body.get("master_key") or ""
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    if "enabled" in body:
        NXC_SOLVABILITY["enabled"] = bool(body["enabled"])
    if "gesture" in body:
        NXC_SOLVABILITY["gesture"] = max(0, int(body.get("gesture", 50)))
    return jsonify({"ok": True, "enabled": NXC_SOLVABILITY["enabled"],
                    "gesture": NXC_SOLVABILITY["gesture"]})


@app.route("/nxc/bank/fail", methods=["GET", "POST"])
def nxc_bank_fail():
    """GET : retourne les tentatives echouees.
    POST {master_key, entry} : enregistre une tentative echouee."""
    if request.method == "GET":
        return jsonify({"ok": True, "fails": NXC_FAILS[-50:]})
    body = request.get_json(force=True, silent=True) or {}
    mk = body.get("master_key") or ""
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    entry = body.get("entry") or {}
    if entry:
        NXC_FAILS.append(entry)
        if len(NXC_FAILS) > 200:
            NXC_FAILS.pop(0)
    return jsonify({"ok": True})


@app.route("/nxc/bank/gesture", methods=["POST"])
def nxc_bank_gesture():
    """Verse le geste commercial a un utilisateur depuis les reserves de la banque."""
    body = request.get_json(force=True, silent=True) or {}
    mk = body.get("master_key") or ""
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    target = body.get("target") or ""
    amount = float(body.get("amount") or 0)
    fail_ts = body.get("fail_ts")
    if not target or amount <= 0:
        return jsonify({"ok": False, "error": "Parametres invalides"})
    with _lock:
        db = load_db()
        # Verifier que la banque a les fonds
        noah = db.get("users", {}).get("noah", {})
        bank = noah.get("data", {}).get("nxcoin_bank", {})
        if (bank.get("reserves") or 0) < amount:
            return jsonify({"ok": False, "error": "Reserves insuffisantes"})
        # Debiter la banque
        bank["reserves"] = round(bank.get("reserves", 0) - amount, 2)
        bank["totalOut"] = round(bank.get("totalOut", 0) + amount, 2)
        bank.setdefault("flux", []).append({
            "type": "OUT", "user": "GESTE->"+target,
            "amount": amount, "nxc": 0, "ts": int(__import__("time").time()*1000)})
        noah.setdefault("data", {})["nxcoin_bank"] = bank
        # Crediter l'utilisateur
        user = db.get("users", {}).get(target)
        if not user:
            return jsonify({"ok": False, "error": "Utilisateur introuvable"})
        udata = user.get("data", {})
        udata.setdefault("nx2098", {})
        udata["nx2098"]["rewards"] = round((udata["nx2098"].get("rewards") or 0) + amount, 2)
        udata.setdefault("rewards", {})["points"] = udata["nx2098"]["rewards"]
        user["data"] = udata
        save_db(db)
    # Supprimer la tentative echouee si fail_ts fourni
    if fail_ts:
        global NXC_FAILS
        NXC_FAILS = [f for f in NXC_FAILS if f.get("ts") != fail_ts]
    return jsonify({"ok": True, "new_rewards": udata["nx2098"]["rewards"]})


@app.route("/nxc/reset", methods=["POST"])
def nxc_reset():
    """Remet l'historique NXC à zéro."""
    body = request.get_json(force=True, silent=True) or {}
    mk = body.get("master_key") or ""
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify(ok=False, error="Unauthorized"), 403
    NXC_MARKET["history"] = []
    NXC_MARKET["volume24"] = 0
    NXC_MARKET["trades24"] = 0
    return jsonify(ok=True)



NEXUS_HTML = r"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<meta name="theme-color" content="#0a0d14">
<title>Nexus</title>
<style>
  * { box-sizing:border-box; -webkit-tap-highlight-color:transparent;
      font-family:'Segoe UI',system-ui,Arial,sans-serif; }
  body { margin:0; background:#0a0d14; color:#eaf0fb; min-height:100vh; }
  .wrap { max-width:720px; margin:0 auto; padding:18px; }
  input, button, textarea { font-size:16px; border-radius:12px; padding:13px 15px;
      border:1px solid #283046; background:#1b2233; color:#eaf0fb; outline:none; }
  input:focus, textarea:focus { border-color:#5b9dff; }
  button { cursor:pointer; }
  .accent { border:none; font-weight:700; color:#06080c;
      background:linear-gradient(90deg,#5b9dff,#a06bff); }
  .row { display:flex; gap:10px; flex-wrap:wrap; align-items:center; }
  .grow { flex:1; min-width:120px; }
  .hidden { display:none; }
  .muted { color:#8a96ad; font-size:13px; }
  #login { max-width:380px; margin:12vh auto 0; text-align:center; }
  .logo { font-size:40px; font-weight:800;
      background:linear-gradient(90deg,#5b9dff,#a06bff); -webkit-background-clip:text;
      background-clip:text; color:transparent; letter-spacing:1px; }
  #login input { width:100%; margin-top:10px; text-align:center; }
  #login button { width:100%; margin-top:10px; }
  header { display:flex; align-items:center; justify-content:space-between; padding:6px 0 14px; }
  .search { width:100%; font-size:18px; padding:16px 18px; border-radius:16px; }
  .chips { display:flex; gap:8px; flex-wrap:wrap; margin-top:12px; }
  .chip { padding:9px 14px; border-radius:20px; background:#151b29; border:1px solid #283046;
      font-size:14px; cursor:pointer; }
  .chip:hover { border-color:#5b9dff; }
  .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(96px,1fr)); gap:10px; margin-top:12px; }
  .fav { position:relative; background:#121724; border:1px solid #283046; border-radius:14px;
      padding:14px 8px; text-align:center; cursor:pointer; }
  .fav:hover { border-color:#5b9dff; }
  .fav .ico { font-size:24px; } .fav .nm { font-size:12px; margin-top:6px; word-break:break-word; }
  .fav .x { position:absolute; top:4px; right:6px; color:#ef5d6b; font-size:14px; opacity:.7; }
  .bar { display:flex; gap:10px; margin-top:18px; flex-wrap:wrap; }
  .bar button { flex:1; min-width:130px; }
  .sect { color:#8a96ad; font-size:12px; text-transform:uppercase; letter-spacing:1px; margin:20px 0 2px; }
  .overlay { position:fixed; inset:0; background:rgba(0,0,0,.6); display:flex;
      align-items:flex-end; justify-content:center; }
  .sheet { background:#121724; border:1px solid #283046; border-radius:18px 18px 0 0;
      padding:16px; max-width:720px; width:100%; max-height:80vh; overflow:auto; }
  .msg { background:#0a0d14; border:1px solid #1c2333; border-radius:12px; padding:10px 12px; margin:8px 0; }
  a { color:#a06bff; }
</style>
</head>
<body>
<div class="wrap">
  <div id="login">
    <div class="logo">NEXUS</div>
    <div class="muted">Ton navigateur, en ligne.</div>
    <input id="u" placeholder="Nom d'utilisateur" autocomplete="username">
    <input id="p" type="password" placeholder="Mot de passe" autocomplete="current-password">
    <button class="accent" onclick="login()">Se connecter</button>
    <button onclick="register()">Créer un compte</button>
    <div id="lmsg" class="muted" style="margin-top:10px"></div>
  </div>
  <div id="app" class="hidden">
    <header>
      <div class="logo" style="font-size:26px">NEXUS</div>
      <div class="row">
        <span id="who" class="muted"></span>
        <button onclick="logout()">Quitter</button>
      </div>
    </header>
    <input id="q" class="search" placeholder="🔍 Rechercher sur le web…"
           onkeydown="if(event.key==='Enter')search()">
    <div class="chips">
      <div class="chip" onclick="openUrl('https://www.google.com','Google')">Google</div>
      <div class="chip" onclick="openUrl('https://www.youtube.com','YouTube')">YouTube</div>
      <div class="chip" onclick="openUrl('https://fr.wikipedia.org','Wikipédia')">Wikipédia</div>
      <div class="chip" onclick="openUrl('https://chat.openai.com','ChatGPT')">ChatGPT</div>
      <div class="chip" onclick="addFav()">➕ Favori</div>
    </div>
    <div class="sect">Favoris (synchronisés)</div>
    <div id="favs" class="grid"></div>
    <div class="bar">
      <button onclick="showHistory()">🕘 Historique</button>
      <button onclick="showForum()">💬 Forum</button>
      <button id="adminBtn" class="hidden" onclick="location.href='/panel'">🛡️ Admin</button>
    </div>
    <div id="sync" class="muted" style="margin-top:12px"></div>
  </div>
</div>
<div id="modal"></div>
<script>
let S = { user:"", pass:"", role:"", nick:"", data:{bookmarks:[], history:[]} };
async function api(path, body) {
  try {
    const r = await fetch(path, {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(body||{})});
    return await r.json();
  } catch(e) { return {ok:false, error:"réseau"}; }
}
async function login() {
  const u=val("u"), p=val("p");
  if(!u||!p){ lmsg("Entre ton nom et ton mot de passe."); return; }
  lmsg("Connexion…");
  const r = await api("/login",{username:u,password:p});
  if(r.ok){ start(u,p,r); } else { lmsg("❌ "+(r.error||"échec")); }
}
async function register() {
  const u=val("u"), p=val("p");
  if(!u||p.length<4){ lmsg("Nom requis + mot de passe (4 caractères min)."); return; }
  lmsg("Création…");
  const r = await api("/register",{username:u,password:p});
  if(r.ok){ start(u,p,r); } else { lmsg("❌ "+(r.error||"échec")); }
}
function start(u,p,r) {
  S.user=u; S.pass=p; S.role=r.role||"user"; S.nick=r.nick||r.nickname||"";
  S.data = r.data || {}; S.data.bookmarks = S.data.bookmarks||[]; S.data.history = S.data.history||[];
  try { sessionStorage.setItem("nx", JSON.stringify({u,p})); } catch(e){}
  document.getElementById("login").classList.add("hidden");
  document.getElementById("app").classList.remove("hidden");
  document.getElementById("who").textContent = "👤 " + (S.nick||S.user);
  if(S.role==="admin") document.getElementById("adminBtn").classList.remove("hidden");
  renderFavs();
}
async function doSync() {
  setSync("Synchronisation…");
  const r = await api("/sync",{username:S.user,password:S.pass,data:S.data});
  setSync(r.ok ? "✅ Synchronisé dans le cloud" : "⚠️ synchro échouée");
}
function search() {
  const q=val("q"); if(!q) return;
  const url = "https://www.google.com/search?q="+encodeURIComponent(q);
  openUrl(url, "🔍 "+q);
  document.getElementById("q").value="";
}
function openUrl(url, label) {
  if(!/^https?:\/\//.test(url)) url="https://"+url;
  window.open(url, "_blank");
  S.data.history.unshift({label:label||url, url:url, time:new Date().toLocaleString()});
  S.data.history = S.data.history.slice(0,40);
  doSync();
}
function addFav() {
  const name = prompt("Nom du favori :"); if(!name) return;
  let url = prompt("Adresse (ex: youtube.com) :"); if(!url) return;
  if(!/^https?:\/\//.test(url)) url="https://"+url;
  S.data.bookmarks.push({name:name, url:url}); renderFavs(); doSync();
}
function removeFav(i, ev) { ev.stopPropagation(); S.data.bookmarks.splice(i,1); renderFavs(); doSync(); }
function renderFavs() {
  const g=document.getElementById("favs"); g.innerHTML="";
  if(!S.data.bookmarks.length){ g.innerHTML="<div class='muted'>Aucun favori.</div>"; return; }
  S.data.bookmarks.forEach((b,i)=>{
    const d=document.createElement("div"); d.className="fav";
    d.onclick=()=>openUrl(b.url,b.name);
    const letter=(b.name||"?").trim().charAt(0).toUpperCase();
    d.innerHTML="<div class='x' onclick='removeFav("+i+",event)'>✕</div>"+
      "<div class='ico'>"+letter+"</div><div class='nm'>"+esc(b.name)+"</div>";
    g.appendChild(d);
  });
}
function showHistory() {
  let h = S.data.history.map(x=>"<div class='msg'><a href='"+x.url+"' target='_blank'>"+esc(x.label)+"</a>"+
    "<div class='muted'>"+esc(x.time)+"</div></div>").join("") || "<div class='muted'>Historique vide.</div>";
  sheet("<div class='row'><b class='grow'>🕘 Historique</b>"+
    "<button onclick='clearHist()'>Effacer</button><button onclick='closeSheet()'>Fermer</button></div>"+h);
}
function clearHist(){ S.data.history=[]; doSync(); closeSheet(); }
async function showForum() {
  sheet("<b>💬 Forum</b><div id='fl' class='muted'>Chargement…</div>"+
    "<div class='row' style='margin-top:10px'><input id='ft' class='grow' placeholder='Ton message…'>"+
    "<button class='accent' onclick='postForum()'>Envoyer</button></div>"+
    "<div style='height:6px'></div><button onclick='closeSheet()'>Fermer</button>");
  loadForum();
}
async function loadForum() {
  const r = await api("/forum/list",{});
  const el = document.getElementById("fl"); if(!el) return;
  if(r.ok){ el.innerHTML = (r.messages||[]).slice(-60).reverse().map(m=>
    "<div class='msg'><b>"+esc(m.nick||m.user)+"</b> <span class='muted'>"+esc(m.time||"")+"</span><br>"+esc(m.text)+"</div>").join("")
    || "<div class='muted'>Aucun message.</div>"; }
  else el.textContent="Erreur de chargement.";
}
async function postForum() {
  const t=val("ft"); if(!t) return;
  await api("/forum/post",{username:S.user,password:S.pass,text:t});
  document.getElementById("ft").value=""; loadForum();
}
function logout(){ try{sessionStorage.removeItem("nx");}catch(e){} location.reload(); }
function sheet(html){ document.getElementById("modal").innerHTML=
  "<div class='overlay' onclick='if(event.target===this)closeSheet()'><div class='sheet'>"+html+"</div></div>"; }
function closeSheet(){ document.getElementById("modal").innerHTML=""; }
function val(id){ return (document.getElementById(id).value||"").trim(); }
function lmsg(t){ document.getElementById("lmsg").textContent=t; }
function setSync(t){ document.getElementById("sync").textContent=t; }
function esc(s){ return (s+"").replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"\':"&quot;"}[c])); }
(function(){ try { const s=JSON.parse(sessionStorage.getItem("nx")||"null");
  if(s&&s.u){ api("/login",{username:s.u,password:s.p}).then(r=>{ if(r.ok) start(s.u,s.p,r); }); } } catch(e){} })();
</script>
</body>
</html>"""


@app.get("/nexus")
def nexus_web():
    return Response(NEXUS_HTML, mimetype="text/html")


@app.post("/register")
def register():
    if rate_limited():
        return jsonify(ok=False, error="trop de tentatives, réessaie dans 1 min")
    d = request.get_json(force=True, silent=True) or {}
    u = (d.get("username") or "").strip()
    p = d.get("password") or ""
    if not u or len(p) < 4:
        return jsonify(ok=False, error="nom requis et mot de passe (4 car. min)")
    with _lock:
        db = load_db()
        if u in db["users"]:
            return jsonify(ok=False, error="ce nom est déjà pris")
        db["users"][u] = make_user(p, "user")
        save_db(db)
    return jsonify(ok=True, role="user", nickname="", data={})


@app.post("/login")
def login():
    if rate_limited():
        return jsonify(ok=False, error="trop de tentatives, réessaie dans 1 min")
    d = request.get_json(force=True, silent=True) or {}
    u = (d.get("username") or "").strip()
    p = d.get("password") or ""
    with _lock:
        db = load_db()
        if not check(db, u, p):
            return jsonify(ok=False, error="identifiants incorrects")
        log = db["users"][u].setdefault("logins", [])
        log.insert(0, {"ip": client_ip(), "time": now_iso()})
        del log[50:]
        save_db(db)
        x = db["users"][u]
    return jsonify(ok=True, role=x["role"], nickname=x.get("nickname", ""), data=x.get("data", {}))


@app.post("/sync")
def sync():
    d = request.get_json(force=True, silent=True) or {}
    u = (d.get("username") or "").strip()
    p = d.get("password") or ""
    with _lock:
        db = load_db()
        if not check(db, u, p):
            return jsonify(ok=False, error="identifiants invalides")
        new_data = d.get("data", {})
        old_data = db["users"][u].get("data", {})
        # Fusionner en gardant le MAX des rewards pour eviter ecrasement
        old_rew = float((old_data.get("nx2098") or {}).get("rewards") or 0)
        new_rew = float((new_data.get("nx2098") or {}).get("rewards") or 0)
        old_pts = float((old_data.get("rewards") or {}).get("points") or 0)
        new_pts = float((new_data.get("rewards") or {}).get("points") or 0)
        max_rew = max(old_rew, new_rew, old_pts, new_pts)
        # Appliquer les nouvelles donnees
        db["users"][u]["data"] = new_data
        # Mais forcer le MAX des rewards
        if "nx2098" not in db["users"][u]["data"]:
            db["users"][u]["data"]["nx2098"] = {}
        db["users"][u]["data"]["nx2098"]["rewards"] = max_rew
        if "rewards" not in db["users"][u]["data"]:
            db["users"][u]["data"]["rewards"] = {}
        db["users"][u]["data"]["rewards"]["points"] = max_rew
        db["users"][u]["updated"] = now_iso()
        save_db(db)
    return jsonify(ok=True)


@app.post("/change_password")
def change_password():
    d = request.get_json(force=True, silent=True) or {}
    u = (d.get("username") or "").strip()
    old = d.get("old_password") or ""
    new = d.get("new_password") or ""
    if len(new) < 4:
        return jsonify(ok=False, error="nouveau mot de passe trop court")
    with _lock:
        db = load_db()
        if not check(db, u, old):
            return jsonify(ok=False, error="ancien mot de passe incorrect")
        salt = secrets.token_hex(16)
        db["users"][u]["salt"] = salt
        db["users"][u]["pass_hash"] = hash_pw(new, salt)
        db["users"][u]["updated"] = now_iso()
        save_db(db)
    return jsonify(ok=True)


@app.post("/admin/list")
def admin_list():
    d = request.get_json(force=True, silent=True) or {}
    db = load_db()
    if not admin_ok(d, db):
        return jsonify(ok=False, error="accès refusé")
    out = []
    for name, u in db["users"].items():
        data = u.get("data", {}) or {}
        logins = u.get("logins", [])
        out.append({"username": name, "nickname": u.get("nickname", ""),
                    "role": u.get("role"), "created": u.get("created", ""),
                    "hidden": u.get("hidden", False),
                    "history": len(data.get("history", [])),
                    "last_ip": logins[0]["ip"] if logins else "",
                    "last_login": logins[0]["time"] if logins else ""})
    return jsonify(ok=True, users=out)


@app.post("/admin/get")
def admin_get():
    d = request.get_json(force=True, silent=True) or {}
    db = load_db()
    if not admin_ok(d, db):
        return jsonify(ok=False, error="accès refusé")
    u = db["users"].get(d.get("target"))
    if not u:
        return jsonify(ok=False, error="introuvable")
    return jsonify(ok=True, username=d.get("target"), nickname=u.get("nickname", ""),
                   role=u.get("role"), data=u.get("data", {}),
                   logins=u.get("logins", []), hidden=u.get("hidden", False))


@app.post("/admin/delete")
def admin_delete():
    d = request.get_json(force=True, silent=True) or {}
    with _lock:
        db = load_db()
        if not admin_ok(d, db):
            return jsonify(ok=False, error="accès refusé")
        if d.get("target") in db["users"]:
            del db["users"][d["target"]]
            save_db(db)
            return jsonify(ok=True)
    return jsonify(ok=False, error="introuvable")


@app.post("/admin/purge")
def admin_purge():
    d = request.get_json(force=True, silent=True) or {}
    with _lock:
        db = load_db()
        if not admin_ok(d, db):
            return jsonify(ok=False, error="accès refusé")
        t = d.get("target")
        if not t:
            return jsonify(ok=False, error="cible manquante")
        db.setdefault("deleted", {})[t] = now_iso()
        db["users"].pop(t, None)
        save_db(db)
    return jsonify(ok=True)


@app.post("/admin/purge_all")
def admin_purge_all():
    d = request.get_json(force=True, silent=True) or {}
    with _lock:
        db = load_db()
        if not admin_ok(d, db):
            return jsonify(ok=False, error="accès refusé")
        if (d.get("purge_password") or "") != db.get("purge_password", "nexus"):
            return jsonify(ok=False, error="mot de passe d'effacement incorrect")
        tomb = db.setdefault("deleted", {})
        for name in list(db["users"].keys()):
            tomb[name] = now_iso()
        n = len(db["users"])
        db["users"] = {}
        save_db(db)
    return jsonify(ok=True, count=n)


@app.post("/admin/set_purge_password")
def admin_set_purge_password():
    d = request.get_json(force=True, silent=True) or {}
    with _lock:
        db = load_db()
        if not admin_ok(d, db):
            return jsonify(ok=False, error="accès refusé")
        if db.get("purge_password", "nexus") != (d.get("old_password") or ""):
            return jsonify(ok=False, error="ancien mot de passe incorrect")
        if len((d.get("new_password") or "")) < 3:
            return jsonify(ok=False, error="nouveau mot de passe trop court (3 min)")
        db["purge_password"] = d["new_password"]
        save_db(db)
    return jsonify(ok=True)


@app.post("/admin/rename")
def admin_rename():
    d = request.get_json(force=True, silent=True) or {}
    with _lock:
        db = load_db()
        if not admin_ok(d, db):
            return jsonify(ok=False, error="accès refusé")
        t = d.get("target"); new = (d.get("new_username") or "").strip()
        if t not in db["users"] or not new or new in db["users"]:
            return jsonify(ok=False, error="nom invalide ou déjà pris")
        db["users"][new] = db["users"].pop(t)
        save_db(db)
    return jsonify(ok=True)


@app.post("/admin/nickname")
def admin_nickname():
    d = request.get_json(force=True, silent=True) or {}
    with _lock:
        db = load_db()
        if not admin_ok(d, db):
            return jsonify(ok=False, error="accès refusé")
        if d.get("target") not in db["users"]:
            return jsonify(ok=False, error="introuvable")
        db["users"][d["target"]]["nickname"] = d.get("nickname", "")
        save_db(db)
    return jsonify(ok=True)


@app.post("/admin/hide")
def admin_hide():
    d = request.get_json(force=True, silent=True) or {}
    with _lock:
        db = load_db()
        if not admin_ok(d, db):
            return jsonify(ok=False, error="accès refusé")
        if d.get("target") not in db["users"]:
            return jsonify(ok=False, error="introuvable")
        db["users"][d["target"]]["hidden"] = bool(d.get("hidden", True))
        save_db(db)
    return jsonify(ok=True)


@app.post("/admin/create")
def admin_create():
    d = request.get_json(force=True, silent=True) or {}
    with _lock:
        db = load_db()
        if not admin_ok(d, db):
            return jsonify(ok=False, error="accès refusé")
        u = (d.get("new_username") or "").strip()
        p = d.get("new_password") or ""
        role = d.get("role", "user")
        if role not in ("user", "admin"):
            role = "user"
        if not u or not p:
            return jsonify(ok=False, error="champs manquants")
        if u in db["users"]:
            return jsonify(ok=False, error="nom déjà pris")
        db["users"][u] = make_user(p, role)
        db.get("deleted", {}).pop(u, None)
        save_db(db)
    return jsonify(ok=True, role=role)


@app.post("/forum/post")
def forum_post():
    if rate_limited():
        return jsonify(ok=False, error="trop de messages, attends un peu")
    d = request.get_json(force=True, silent=True) or {}
    u = (d.get("username") or "").strip()
    p = d.get("password") or ""
    text = (d.get("text") or "").strip()[:1000]
    if not text:
        return jsonify(ok=False, error="message vide")
    with _lock:
        db = load_db()
        if not check(db, u, p):
            return jsonify(ok=False, error="identifiants invalides")
        nick = db["users"][u].get("nickname") or u
        msgs = db.setdefault("forum", [])
        msgs.append({"user": u, "nick": nick, "text": text, "time": now_iso()})
        del msgs[:-500]
        save_db(db)
    return jsonify(ok=True)


@app.post("/forum/list")
def forum_list():
    db = load_db()
    return jsonify(ok=True, messages=db.get("forum", [])[-200:])


@app.post("/admin/ext_add")
def ext_add():
    d = request.get_json(force=True, silent=True) or {}
    with _lock:
        db = load_db()
        if not admin_ok(d, db):
            return jsonify(ok=False, error="accès refusé")
        name = (d.get("name") or "").strip()
        code = d.get("code") or ""
        if not name or not code:
            return jsonify(ok=False, error="nom ou code manquant")
        db.setdefault("extensions", {})[name] = {
            "code": code, "enabled": True, "added": now_iso()}
        save_db(db)
    return jsonify(ok=True)


@app.post("/admin/ext_list")
def ext_list_admin():
    d = request.get_json(force=True, silent=True) or {}
    db = load_db()
    if not admin_ok(d, db):
        return jsonify(ok=False, error="accès refusé")
    out = [{"name": n, "enabled": e.get("enabled", True), "added": e.get("added", "")}
           for n, e in db.get("extensions", {}).items()]
    return jsonify(ok=True, extensions=out)


@app.post("/admin/ext_toggle")
def ext_toggle():
    d = request.get_json(force=True, silent=True) or {}
    with _lock:
        db = load_db()
        if not admin_ok(d, db):
            return jsonify(ok=False, error="accès refusé")
        ext = db.get("extensions", {}).get(d.get("name"))
        if not ext:
            return jsonify(ok=False, error="introuvable")
        ext["enabled"] = bool(d.get("enabled", True))
        save_db(db)
    return jsonify(ok=True)


@app.post("/admin/ext_delete")
def ext_delete():
    d = request.get_json(force=True, silent=True) or {}
    with _lock:
        db = load_db()
        if not admin_ok(d, db):
            return jsonify(ok=False, error="accès refusé")
        db.get("extensions", {}).pop(d.get("name"), None)
        save_db(db)
    return jsonify(ok=True)


@app.post("/ext_enabled")
def ext_enabled():
    db = load_db()
    out = {n: e["code"] for n, e in db.get("extensions", {}).items() if e.get("enabled", True)}
    return jsonify(ok=True, extensions=out)


FILES_DIR = os.path.join(BASE, "nexus_files")
MAX_TOTAL = 100 * 1024 ** 3


def _safe_name(name):
    name = (name or "").replace("\\", "/").split("/")[-1]
    name = "".join(c for c in name if c.isalnum() or c in "._- ()[]")
    return name.strip() or "fichier"


def _user_dir(u):
    d = os.path.join(FILES_DIR, "".join(c for c in u if c.isalnum() or c in "._-") or "user")
    os.makedirs(d, exist_ok=True)
    return d


def _files_auth():
    u = request.headers.get("X-User", "")
    p = request.headers.get("X-Pass", "")
    return u if check(load_db(), u, p) else None


def _dir_size(d):
    return sum(os.path.getsize(os.path.join(d, n)) for n in os.listdir(d)
               if os.path.isfile(os.path.join(d, n)))


@app.post("/files/list")
def files_list():
    u = _files_auth()
    if not u:
        return jsonify(ok=False, error="auth")
    d = _user_dir(u)
    files = [{"name": n, "size": os.path.getsize(os.path.join(d, n))}
             for n in sorted(os.listdir(d)) if os.path.isfile(os.path.join(d, n))]
    return jsonify(ok=True, files=files, used=_dir_size(d), maxi=MAX_TOTAL)


@app.post("/files/upload")
def files_upload():
    u = _files_auth()
    if not u:
        return jsonify(ok=False, error="auth")
    import urllib.parse
    name = _safe_name(urllib.parse.unquote(request.headers.get("X-Filename", "fichier")))
    d = _user_dir(u)
    clen = int(request.headers.get("Content-Length", "0") or 0)
    if clen and _dir_size(d) + clen > MAX_TOTAL:
        return jsonify(ok=False, error="espace plein")
    path = os.path.join(d, name)
    with open(path, "wb") as f:
        while True:
            chunk = request.stream.read(262144)
            if not chunk:
                break
            f.write(chunk)
    return jsonify(ok=True, size=os.path.getsize(path))


@app.post("/files/download")
def files_download():
    u = _files_auth()
    if not u:
        return ("auth", 403)
    import urllib.parse
    name = _safe_name(urllib.parse.unquote(request.headers.get("X-Filename", "")))
    path = os.path.join(_user_dir(u), name)
    if not os.path.exists(path):
        return ("introuvable", 404)
    return send_file(path, as_attachment=True, download_name=name)


@app.post("/files/delete")
def files_delete():
    u = _files_auth()
    if not u:
        return jsonify(ok=False, error="auth")
    import urllib.parse
    name = _safe_name(urllib.parse.unquote(request.headers.get("X-Filename", "")))
    try:
        os.remove(os.path.join(_user_dir(u), name))
    except Exception:
        pass
    return jsonify(ok=True)


@app.post("/admin/dump")
def admin_dump():
    d = request.get_json(force=True, silent=True) or {}
    db = load_db()
    if not admin_ok(d, db):
        return jsonify(ok=False, error="accès refusé")
    return jsonify(ok=True, db=db)


@app.post("/admin/merge")
def admin_merge():
    d = request.get_json(force=True, silent=True) or {}
    incoming = d.get("db") or {}
    with _lock:
        db = load_db()
        if not admin_ok(d, db):
            return jsonify(ok=False, error="accès refusé")
        tomb = db.setdefault("deleted", {})
        for name, t in (incoming.get("deleted", {}) or {}).items():
            if t > tomb.get(name, ""):
                tomb[name] = t
        for name in list(db["users"].keys()):
            if name in tomb and tomb[name] >= db["users"][name].get("updated", ""):
                del db["users"][name]
        for name, u in (incoming.get("users", {}) or {}).items():
            if name in tomb and tomb[name] >= u.get("updated", ""):
                continue
            cur = db["users"].get(name)
            if not cur or u.get("updated", "") > cur.get("updated", ""):
                db["users"][name] = u
        seen = {(m["user"], m["time"], m["text"]) for m in db.get("forum", [])}
        for m in incoming.get("forum", []) or []:
            key = (m.get("user"), m.get("time"), m.get("text"))
            if key not in seen:
                db.setdefault("forum", []).append(m); seen.add(key)
        db["forum"] = sorted(db.get("forum", []), key=lambda m: m.get("time", ""))[-500:]
        for n, e in (incoming.get("extensions", {}) or {}).items():
            cur = db.setdefault("extensions", {}).get(n)
            if not cur or e.get("added", "") > cur.get("added", ""):
                db["extensions"][n] = e
        save_db(db)
        merged = db
    return jsonify(ok=True, db=merged)


if __name__ == "__main__":
    _load_nxc_from_db()
    print("=" * 54)
    print("  NEXUS SERVER (en ligne)  —  http://127.0.0.1:%d" % PORT)
    print("  Clé maître :", MASTER_KEY)
    print("  Prix NXC restauré : %.2f R" % NXC_MARKET["price"])
    print("=" * 54)
    app.run(host="0.0.0.0", port=PORT)
