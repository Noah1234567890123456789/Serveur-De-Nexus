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
NXC_PANEL_HTML = '<!DOCTYPE html>\n<html lang="fr">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">\n<title>◈ Nexus</title>\n<style>\n*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent;touch-action:manipulation}\n:root{--bg:#02040a;--bg2:#080d1a;--bg3:#0d1428;--cyan:#00e5ff;--green:#00ff9d;--red:#ff3d5e;--gold:#ffb020;--purple:#a06bff;--muted:#5c6b8c;--text:#d4e8ff;--border:rgba(0,229,255,.12)}\nhtml,body{background:var(--bg);color:var(--text);font-family:\'Segoe UI\',system-ui,sans-serif;min-height:100dvh;overflow-x:hidden;-webkit-text-size-adjust:100%}\n\n/* LOGIN */\n#ls{position:fixed;inset:0;background:var(--bg);z-index:9999;display:flex;align-items:center;justify-content:center;padding:20px}\n.lb{background:var(--bg2);border:1px solid var(--border);border-radius:22px;padding:32px 24px;width:100%;max-width:340px;text-align:center;box-shadow:0 24px 80px rgba(0,0,0,.6)}\n.lb-logo{font-family:monospace;font-size:30px;font-weight:900;color:var(--cyan);letter-spacing:4px;margin-bottom:4px;text-shadow:0 0 20px rgba(0,229,255,.4)}\n.lb-sub{font-size:10px;color:var(--muted);margin-bottom:24px;letter-spacing:3px;text-transform:uppercase}\n.fi{width:100%;padding:13px 16px;background:var(--bg3);border:1px solid var(--border);border-radius:12px;color:var(--text);font-size:16px;margin-bottom:10px;outline:none}\n.fi:focus{border-color:var(--cyan)}\n.btn-login{width:100%;padding:14px;border-radius:12px;font-size:15px;font-weight:800;cursor:pointer;border:none;background:linear-gradient(135deg,var(--cyan),#0097b2);color:#000;letter-spacing:.5px}\n#lm{font-size:12px;color:var(--red);margin-top:8px;min-height:16px}\n\n/* HUD */\n.hud{position:fixed;top:0;left:0;right:0;height:52px;background:rgba(2,4,10,.97);border-bottom:1px solid var(--border);display:flex;align-items:center;padding:0 14px;gap:10px;z-index:100;backdrop-filter:blur(20px)}\n.hud-logo{font-family:monospace;font-size:15px;font-weight:900;color:var(--cyan);letter-spacing:2px;flex-shrink:0}\n.hud-price{font-family:monospace;font-size:12px;font-weight:800;color:var(--cyan)}\n.hud-chg{font-size:10px;font-weight:700;padding:2px 7px;border-radius:20px}\n.hud-chg.up{background:rgba(0,255,157,.12);color:var(--green);border:1px solid rgba(0,255,157,.2)}\n.hud-chg.dn{background:rgba(255,61,94,.12);color:var(--red);border:1px solid rgba(255,61,94,.2)}\n.hud-right{margin-left:auto;display:flex;align-items:center;gap:8px}\n.dot{width:7px;height:7px;border-radius:50%;background:var(--muted);flex-shrink:0}\n.dot.on{background:var(--green);box-shadow:0 0 8px var(--green);animation:dp 2s infinite}\n@keyframes dp{0%,100%{opacity:1}50%{opacity:.3}}\n.hud-time{font-family:monospace;font-size:10px;color:var(--muted)}\n\n/* TABS */\n.tabs{position:fixed;top:52px;left:0;right:0;background:rgba(2,4,10,.97);border-bottom:1px solid var(--border);display:flex;z-index:99;backdrop-filter:blur(20px);overflow-x:auto;scrollbar-width:none}\n.tabs::-webkit-scrollbar{display:none}\n.tab{flex:0 0 auto;padding:12px 18px;font-size:12px;font-weight:700;color:var(--muted);cursor:pointer;border:none;background:none;border-bottom:2px solid transparent;white-space:nowrap;transition:.15s}\n.tab.on{color:var(--cyan);border-bottom-color:var(--cyan)}\n.tab-more{flex:0 0 auto;padding:12px 16px;font-size:16px;color:var(--muted);cursor:pointer;border:none;background:none;border-bottom:2px solid transparent;margin-left:auto}\n.tab-more.on{color:var(--cyan)}\n\n/* DROPDOWN MENU */\n.dropdown{position:fixed;top:52px;right:0;background:var(--bg2);border:1px solid var(--border);border-radius:0 0 0 14px;z-index:200;min-width:180px;display:none;box-shadow:0 8px 32px rgba(0,0,0,.5)}\n.dropdown.show{display:block}\n.dd-item{padding:12px 18px;font-size:13px;font-weight:600;color:var(--muted);cursor:pointer;border-bottom:1px solid rgba(0,229,255,.06);display:flex;align-items:center;gap:10px}\n.dd-item:hover{background:rgba(0,229,255,.05);color:var(--text)}\n.dd-item:last-child{border:none}\n\n/* CONTENT */\n.content{padding-top:100px;padding-bottom:20px}\n.view{display:none;padding:14px;max-width:960px;margin:0 auto}\n.view.on{display:block}\n#view-nexus{display:none;flex-direction:column;padding:0;max-width:none}\n#view-nexus.on{display:flex}\n\n/* CARDS */\n.card{background:var(--bg2);border:1px solid var(--border);border-radius:16px;padding:16px;margin-bottom:12px}\n.card.cyan{border-color:rgba(0,229,255,.22)}.card.green{border-color:rgba(0,255,157,.22)}.card.red{border-color:rgba(255,61,94,.22)}.card.gold{border-color:rgba(255,176,32,.22)}.card.purple{border-color:rgba(160,107,255,.22)}\n.ct{font-size:9px;letter-spacing:2px;color:var(--muted);margin-bottom:12px;font-weight:700;text-transform:uppercase;display:flex;align-items:center;justify-content:space-between}\n.g4{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:12px}\n.g3{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px}\n.g2{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}\n.st{background:var(--bg3);border:1px solid rgba(0,229,255,.07);border-radius:12px;padding:12px 8px;text-align:center}\n.sv{font-family:monospace;font-size:16px;font-weight:800;color:var(--cyan);margin-bottom:2px}\n.sl{font-size:8px;color:var(--muted);letter-spacing:.8px;text-transform:uppercase}\n.sv.gold{color:var(--gold)}.sv.green{color:var(--green)}.sv.red{color:var(--red)}.sv.purple{color:var(--purple)}\n.sec{font-size:10px;color:var(--cyan);font-weight:700;letter-spacing:1px;text-transform:uppercase;margin:12px 0 6px;border-left:2px solid var(--cyan);padding-left:8px}\ninput,select,textarea{width:100%;padding:12px 13px;background:var(--bg3);border:1px solid var(--border);border-radius:11px;color:var(--text);font-size:14px;margin-bottom:8px;outline:none;font-family:inherit}\ninput:focus,select:focus{border-color:var(--cyan)}\n.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}\n.grow{flex:1;min-width:0;margin-bottom:0!important}\n.btn{padding:10px 14px;border-radius:10px;font-size:12px;font-weight:700;cursor:pointer;border:1px solid var(--border);background:var(--bg3);color:var(--text);white-space:nowrap;flex-shrink:0;transition:.15s}\n.btn:active{transform:scale(.96)}\n.btn.cyan{background:rgba(0,229,255,.1);border-color:rgba(0,229,255,.3);color:var(--cyan)}\n.btn.green{background:rgba(0,255,157,.1);border-color:rgba(0,255,157,.3);color:var(--green)}\n.btn.red{background:rgba(255,61,94,.1);border-color:rgba(255,61,94,.3);color:var(--red)}\n.btn.gold{background:rgba(255,176,32,.1);border-color:rgba(255,176,32,.3);color:var(--gold)}\n.btn.purple{background:rgba(160,107,255,.1);border-color:rgba(160,107,255,.3);color:var(--purple)}\n.btn.primary{background:linear-gradient(135deg,var(--cyan),#0097b2);color:#000;border:none}\n.btn.full{width:100%;padding:12px;font-size:13px;margin-bottom:8px;display:block}\n.ab{padding:10px 13px;border-radius:10px;font-size:12px;margin-bottom:6px}\n.ao{background:rgba(0,255,157,.07);border:1px solid rgba(0,255,157,.15);color:var(--green)}\n.aw{background:rgba(255,176,32,.07);border:1px solid rgba(255,176,32,.15);color:var(--gold)}\n.ae{background:rgba(255,61,94,.07);border:1px solid rgba(255,61,94,.15);color:var(--red)}\n.ai{background:rgba(0,229,255,.07);border:1px solid rgba(0,229,255,.15);color:var(--cyan)}\n.chart-wrap{position:relative;margin-bottom:10px}\n.ch200{height:200px}.ch150{height:150px}\n.fl-item{padding:10px 12px;border-bottom:1px solid rgba(0,229,255,.05);display:flex;align-items:center;gap:8px;font-size:12px}\n.fl-item:last-child{border:none}\n.tg{width:46px;height:25px;background:rgba(255,255,255,.07);border:1px solid var(--border);border-radius:13px;cursor:pointer;position:relative;flex-shrink:0;transition:.3s}\n.tg.on{background:rgba(0,229,255,.2);border-color:var(--cyan)}\n.tg-k{position:absolute;top:3px;left:3px;width:17px;height:17px;background:#8899aa;border-radius:50%;transition:.3s}\n.tg.on .tg-k{left:24px;background:var(--cyan)}\n.pbar{height:6px;background:rgba(0,0,0,.4);border-radius:3px;overflow:hidden;margin-top:4px}\n.pbar-fill{height:100%;border-radius:3px;background:linear-gradient(90deg,var(--cyan),var(--purple));transition:width .5s}\n.log-item{padding:7px 12px;border-bottom:1px solid rgba(0,229,255,.04);font-size:11px;display:flex;gap:8px}\n.log-time{color:var(--muted);font-family:monospace;flex-shrink:0;font-size:10px}\n.tbl-wrap{overflow-x:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)}\ntable{width:100%;border-collapse:collapse;font-size:11px}\nth,td{padding:9px 8px;text-align:left;border-bottom:1px solid rgba(0,229,255,.05)}\nth{color:var(--muted);font-size:9px;text-transform:uppercase;letter-spacing:.5px;font-weight:700}\n.ibar{background:var(--bg2);border-bottom:1px solid var(--border);padding:10px 14px;display:flex;align-items:center;gap:10px}\n.iurl{flex:1;font-size:10px;color:var(--muted);font-family:monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#nf{flex:1;border:none;width:100%;background:var(--bg)}\n.sw{position:relative}\n.sw input{padding-left:34px;margin:0}\n.sw::before{content:\'🔍\';position:absolute;left:10px;top:50%;transform:translateY(-50%);font-size:13px;pointer-events:none;z-index:1}\n.notif{position:absolute;top:6px;right:calc(50% - 12px);width:8px;height:8px;background:var(--red);border-radius:50%;display:none;border:2px solid var(--bg);animation:blink .8s ease infinite}\n@keyframes blink{0%,100%{transform:scale(1)}50%{transform:scale(1.3)}}\n@media(max-width:480px){.g4{grid-template-columns:repeat(2,1fr)}.sv{font-size:14px}.content{padding-top:96px}}\n@media(min-width:768px){.sv{font-size:20px}.ch200{height:240px}}\n</style>\n</head>\n<body>\n\n<!-- LOGIN -->\n<div id="ls">\n<div class="lb">\n<div class="lb-logo">◈ NEXUS</div>\n<div class="lb-sub">Panneau Serveur</div>\n<input id="mk" type="password" placeholder="Clé maître" class="fi" onkeydown="if(event.key===\'Enter\')doLogin()">\n<button class="btn-login" onclick="doLogin()">⚡ Connexion</button>\n<div id="lm"></div>\n</div>\n</div>\n\n<!-- HUD -->\n<div class="hud">\n<div class="hud-logo">◈ NXC</div>\n<div class="hud-price" id="hp">—</div>\n<div class="hud-chg" id="hc" style="display:none"></div>\n<div class="hud-right">\n<div class="dot" id="hd"></div>\n<span class="hud-time" id="htm">—</span>\n</div>\n</div>\n\n<!-- TABS -->\n<div class="tabs" id="main-tabs">\n<button class="tab on" onclick="go(\'marche\',this)">📈 Marché</button>\n<button class="tab" onclick="go(\'banque\',this)">🏦 Banque<span class="notif" id="nd-b"></span></button>\n<button class="tab" onclick="go(\'nexus\',this)">🌐 App</button>\n<button class="tab" onclick="go(\'admin\',this)">👑 Admin</button>\n<button class="tab-more" id="btn-more" onclick="toggleMore()">•••</button>\n</div>\n\n<!-- DROPDOWN MENU -->\n<div class="dropdown" id="dropdown">\n<div class="dd-item" onclick="go(\'trading\',null);toggleMore()">⚙️ Contrôle</div>\n<div class="dd-item" onclick="go(\'users\',null);toggleMore()">👥 Comptes</div>\n<div class="dd-item" onclick="go(\'stats\',null);toggleMore()">📊 Stats</div>\n<div class="dd-item" onclick="go(\'solv\',null);toggleMore()">🛡️ Solvabilité</div>\n<div class="dd-item" onclick="go(\'tools\',null);toggleMore()">🛠️ Outils</div>\n<div class="dd-item" onclick="go(\'log\',null);toggleMore()">📋 Journal</div>\n<div class="dd-item" onclick="go(\'config\',null);toggleMore()">⚙️ Config</div>\n<div class="dd-item" onclick="go(\'notifs\',null);toggleMore()">🔔 Alertes</div>\n</div>\n\n<div class="content">\n\n<!-- MARCHÉ -->\n<div class="view on" id="view-marche">\n<div class="g4">\n<div class="st"><div class="sv" id="s-p">—</div><div class="sl">Prix R/NXC</div></div>\n<div class="st"><div class="sv gold" id="s-v">—</div><div class="sl">Vol. 24h</div></div>\n<div class="st"><div class="sv green" id="s-t">—</div><div class="sl">Trades 24h</div></div>\n<div class="st"><div class="sv purple" id="s-h">—</div><div class="sl">Hist. pts</div></div>\n<div class="st"><div class="sv green" id="s-hi">—</div><div class="sl">Haut 24h</div></div>\n<div class="st"><div class="sv red" id="s-lo">—</div><div class="sl">Bas 24h</div></div>\n<div class="st"><div class="sv" id="s-var">—</div><div class="sl">Variation</div></div>\n<div class="st"><div class="sv" style="color:#ff6eb4" id="s-cap">—</div><div class="sl">Cap. marché</div></div>\n</div>\n<div class="card cyan">\n<div class="ct">◈ HISTORIQUE DU COURS\n<div style="display:flex;gap:5px">\n<button class="btn" onclick="setRange(25)" style="padding:3px 8px;font-size:9px">25</button>\n<button class="btn cyan" onclick="setRange(50)" style="padding:3px 8px;font-size:9px">50</button>\n<button class="btn" onclick="setRange(100)" style="padding:3px 8px;font-size:9px">100</button>\n</div>\n</div>\n<div class="chart-wrap ch200"><canvas id="ch"></canvas></div>\n<div style="display:flex;gap:6px;margin-top:8px;flex-wrap:wrap">\n<button class="btn gold" onclick="chObj&&chObj.zoom(1.5)">🔍+</button>\n<button class="btn gold" onclick="chObj&&chObj.zoom(0.7)">🔍−</button>\n<button class="btn" onclick="chObj&&chObj.resetZoom()">Reset</button>\n<button class="btn cyan" onclick="toggleChartType()">📊 Type</button>\n<button class="btn purple" onclick="dlChart()">⬇️ PNG</button>\n</div>\n</div>\n<div class="card"><div class="ct">◈ ALERTES MARCHÉ</div><div id="al"></div></div>\n<div class="card gold"><div class="ct">◈ RSI (14 ticks)</div><div class="chart-wrap ch150"><canvas id="ch-rsi"></canvas></div><div style="font-size:10px;color:var(--muted);margin-top:4px">RSI >70 = surachat · RSI <30 = survente</div></div>\n</div>\n\n<!-- CONTRÔLE -->\n<div class="view" id="view-trading">\n<div class="card cyan">\n<div class="ct">◈ MODIFIER LE COURS</div>\n<div class="sec">Raccourcis ±%</div>\n<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:10px">\n<button class="btn green" onclick="adjP(.05)">+5%</button>\n<button class="btn green" onclick="adjP(.02)">+2%</button>\n<button class="btn green" onclick="adjP(.01)">+1%</button>\n<button class="btn green" onclick="adjP(.005)">+0.5%</button>\n<button class="btn red" onclick="adjP(-.005)">-0.5%</button>\n<button class="btn red" onclick="adjP(-.01)">-1%</button>\n<button class="btn red" onclick="adjP(-.02)">-2%</button>\n<button class="btn red" onclick="adjP(-.05)">-5%</button>\n</div>\n<div class="sec">Prix exact</div>\n<div class="row"><input id="np" type="number" min="50" max="100000" placeholder="Prix (50–100 000)" class="grow"><button class="btn primary" onclick="setP()">✓</button></div>\n<div class="sec">Variation %</div>\n<div class="row"><input id="np-pct" type="number" placeholder="Ex: +10 ou -5" class="grow"><button class="btn cyan" onclick="setPct()">Appliquer</button></div>\n<div id="pm" style="font-size:11px;font-weight:600;min-height:14px;margin-top:4px"></div>\n</div>\n<div class="card">\n<div class="ct">◈ TENDANCE AUTO <span id="tt-timer" style="font-family:monospace;font-size:10px;color:var(--muted)"></span></div>\n<select id="ts" style="margin-bottom:8px">\n<option value="0.001">Ultra lent 0.1%</option>\n<option value="0.002">Très lent 0.2%</option>\n<option value="0.005" selected>Lent 0.5%</option>\n<option value="0.01">Moyen 1%</option>\n<option value="0.02">Rapide 2%</option>\n<option value="0.05">Très rapide 5%</option>\n<option value="0.1">Extrême 10%</option>\n</select>\n<select id="ti" style="margin-bottom:8px">\n<option value="5000">5s</option>\n<option value="12000" selected>12s</option>\n<option value="30000">30s</option>\n<option value="60000">1min</option>\n</select>\n<div class="sec">Amplitude de variation par tick</div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="noise-slider" type="range" min="1" max="10" value="4" oninput="updateNoise(this.value)" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--cyan)">\n<span id="noise-val" style="color:var(--cyan);font-weight:700;font-size:13px;width:48px;text-align:right;flex-shrink:0">0.4%</span>\n</div>\n<div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:8px">\n<button class="btn green" onclick="setT(\'up\')">📈 Hausse</button>\n<button class="btn red" onclick="setT(\'down\')">📉 Baisse</button>\n<button class="btn purple" onclick="setT(\'random\')">🎲 Aléatoire</button>\n<button class="btn" onclick="setT(\'stop\')" style="color:var(--muted)">⏸ Stop</button>\n</div>\n<div id="tst" style="font-size:12px;color:var(--muted);font-weight:600;padding:8px;background:var(--bg3);border-radius:8px;text-align:center">⏸ Arrêté</div>\n</div>\n<div class="card gold">\n<div class="ct">◈ SCÉNARIOS</div>\n<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">\n<button class="btn gold" onclick="scenario(\'crash\')">💥 Crash −30%</button>\n<button class="btn gold" onclick="scenario(\'moon\')">🚀 Moon +30%</button>\n<button class="btn gold" onclick="scenario(\'volatile\')">⚡ Volatil</button>\n<button class="btn gold" onclick="scenario(\'stable\')">😴 Stabiliser</button>\n<button class="btn gold" onclick="scenario(\'ath\')">🏆 ATH</button>\n<button class="btn gold" onclick="scenario(\'floor\')">🛑 Plancher 200R</button>\n</div>\n</div>\n<div class="card"><div class="ct">◈ RESET</div>\n<button class="btn full" style="color:var(--gold);border-color:rgba(255,176,32,.3);background:rgba(255,176,32,.06)" onclick="resetH()">🔄 Reset historique</button>\n<button class="btn full red" onclick="if(confirm(\'Reset complet ?\'))resetH()">⚠️ Reset complet</button>\n</div>\n</div>\n\n<!-- BANQUE -->\n<div class="view" id="view-banque">\n<div class="g4">\n<div class="st"><div class="sv" style="color:#00b4d8;font-size:14px" id="bk-r">—</div><div class="sl">Réserves</div></div>\n<div class="st"><div class="sv gold" style="font-size:14px" id="bk-i">—</div><div class="sl">Total entré</div></div>\n<div class="st"><div class="sv red" style="font-size:14px" id="bk-o">—</div><div class="sl">Total sorti</div></div>\n<div class="st"><div class="sv green" style="font-size:14px" id="bk-rt">—</div><div class="sl">Ratio</div></div>\n<div class="st"><div class="sv purple" style="font-size:14px" id="bk-nx">—</div><div class="sl">NXC émis</div></div>\n<div class="st"><div class="sv" style="font-size:14px;color:#4ea8de" id="bk-vx">—</div><div class="sl">Val. stock</div></div>\n<div class="st"><div class="sv" style="font-size:14px" id="bk-bn">—</div><div class="sl">Bénéfice</div></div>\n<div class="st"><div class="sv" style="font-size:14px;color:#ff6eb4" id="bk-fl">—</div><div class="sl">Nb flux</div></div>\n</div>\n<div class="card cyan">\n<div class="ct">◈ OPÉRATIONS</div>\n<div class="row" style="margin-bottom:8px">\n<input id="bk-amt" type="number" placeholder="Montant (R)" class="grow">\n<button class="btn green" onclick="bankOp(\'in\')">+ Injecter</button>\n<button class="btn red" onclick="bankOp(\'out\')">− Retirer</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px">\n<button class="btn cyan" onclick="setAmt(100)" style="font-size:11px;padding:6px 10px">100</button>\n<button class="btn cyan" onclick="setAmt(500)" style="font-size:11px;padding:6px 10px">500</button>\n<button class="btn cyan" onclick="setAmt(1000)" style="font-size:11px;padding:6px 10px">1 000</button>\n<button class="btn cyan" onclick="setAmt(5000)" style="font-size:11px;padding:6px 10px">5 000</button>\n<button class="btn cyan" onclick="setAmt(10000)" style="font-size:11px;padding:6px 10px">10 000</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap">\n<button class="btn gold" onclick="bankResetHist()" style="font-size:11px">🗑️ Reset hist.</button>\n<button class="btn red" onclick="bankResetAll()" style="font-size:11px">💥 Reset complet</button>\n<button class="btn purple" onclick="loadBank()" style="font-size:11px">🔄 Actualiser</button>\n<button class="btn" onclick="exportFlux()" style="font-size:11px">📊 CSV</button>\n</div>\n<div id="bk-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:8px"></div>\n</div>\n<div class="card">\n<div class="ct">◈ FLUX\n<div style="display:flex;gap:4px">\n<button class="btn cyan" id="fl-all" onclick="filterFlux(\'all\')" style="padding:3px 7px;font-size:9px">Tous</button>\n<button class="btn" id="fl-in" onclick="filterFlux(\'IN\')" style="padding:3px 7px;font-size:9px">Entrées</button>\n<button class="btn" id="fl-out" onclick="filterFlux(\'OUT\')" style="padding:3px 7px;font-size:9px">Sorties</button>\n</div>\n</div>\n<div id="bk-flux" style="max-height:220px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"></div>\n</div>\n<div class="card red">\n<div class="ct" style="color:var(--red)">⚠️ TENTATIVES ÉCHOUÉES <span id="fails-ct" style="display:none;background:var(--red);color:#000;border-radius:20px;padding:1px 7px;font-size:9px"></span></div>\n<div id="bk-fails" style="max-height:220px;overflow-y:auto"></div>\n</div>\n</div>\n\n<!-- APP -->\n<div class="view" id="view-nexus">\n<div style="padding:12px;background:var(--bg2);border-bottom:1px solid var(--border)">\n<div id="pinned-bar" style="display:none;gap:6px;flex-wrap:wrap;margin-bottom:8px;padding:6px;background:rgba(255,176,32,.05);border:1px solid rgba(255,176,32,.15);border-radius:10px"></div>\n<div class="row" style="margin-bottom:8px">\n<input id="iframe-in" type="url" placeholder="https://..." class="grow" onkeydown="if(event.key===\'Enter\')goUrl()">\n<button class="btn primary" onclick="goUrl()">▶</button>\n</div>\n<div id="saved-sites" style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px"></div>\n<div class="row">\n<input id="site-lbl" placeholder="Nom" style="flex:1;margin:0;font-size:12px;padding:8px 10px">\n<button class="btn gold" onclick="saveSite()" style="font-size:11px">💾 Sauver</button>\n<button class="btn cyan" onclick="reloadF()" style="font-size:11px">🔄</button>\n<button class="btn" onclick="openNewTab()" style="font-size:11px">↗</button>\n</div>\n</div>\n<div class="ibar">\n<span style="color:var(--cyan);font-size:12px;font-weight:800" id="if-title">◈ App</span>\n<span class="iurl" id="if-url">—</span>\n</div>\n<iframe id="nf" src="about:blank" allow="clipboard-write" style="flex:1;border:none;width:100%;min-height:calc(100dvh - 200px)"></iframe>\n</div>\n\n<!-- ADMIN -->\n<div class="view" id="view-admin">\n<div class="card cyan">\n<div class="ct">◈ STATISTIQUES SERVEUR EN TEMPS RÉEL</div>\n<div class="g4" id="adm-stats">\n<div class="st"><div class="sv" id="adm-price">—</div><div class="sl">Prix actuel</div></div>\n<div class="st"><div class="sv gold" id="adm-vol">—</div><div class="sl">Vol. 24h</div></div>\n<div class="st"><div class="sv green" id="adm-trades">—</div><div class="sl">Trades</div></div>\n<div class="st"><div class="sv purple" id="adm-users">—</div><div class="sl">Utilisateurs</div></div>\n<div class="st"><div class="sv" style="color:#00b4d8" id="adm-res">—</div><div class="sl">Réserves</div></div>\n<div class="st"><div class="sv gold" id="adm-nxc">—</div><div class="sl">NXC émis</div></div>\n<div class="st"><div class="sv green" id="adm-fails">—</div><div class="sl">Tentatives échouées</div></div>\n<div class="st"><div class="sv" id="adm-hist">—</div><div class="sl">Points hist.</div></div>\n</div>\n<button class="btn cyan" onclick="refreshAdminStats()" style="width:100%;margin-top:4px;padding:10px">🔄 Actualiser tout</button>\n</div>\n<div class="card green"><div class="ct">◈ SAUVEGARDE ET IMPORT DES DONNÉES</div><button class="btn green full" onclick="saveAllData()">💾 Sauvegarder toutes les données (JSON)</button><button class="btn cyan full" onclick="importData()">📥 Importer depuis un fichier JSON</button><button class="btn purple full" onclick="printDashboard()">🖨️ Imprimer le tableau de bord</button><div id="data-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:4px"></div></div>\n<div class="card gold">\n<div class="ct">◈ DONNER DES REWARDS À UN UTILISATEUR</div>\n<div class="row" style="margin-bottom:8px">\n<select id="rw-u" class="grow" style="margin:0"><option value="">Utilisateur...</option></select>\n<input id="rw-amt" type="number" placeholder="Montant" style="width:100px;margin:0;flex-shrink:0">\n<button class="btn gold" onclick="giveRewards()">💰 Donner</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:4px">\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=50" style="font-size:11px;padding:6px 10px">50</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=100" style="font-size:11px;padding:6px 10px">100</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=500" style="font-size:11px;padding:6px 10px">500</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=1000" style="font-size:11px;padding:6px 10px">1 000</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=5000" style="font-size:11px;padding:6px 10px">5 000</button>\n</div>\n<div id="rw-msg" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n<div class="card purple">\n<div class="ct">◈ CHANGER LE RÔLE D\'UN UTILISATEUR</div>\n<div class="row">\n<select id="role-u" class="grow" style="margin:0"><option value="">Utilisateur...</option></select>\n<select id="role-v" style="width:auto;margin:0;flex-shrink:0;padding:12px 8px">\n<option value="user">user</option>\n<option value="admin">admin</option>\n<option value="moderator">moderator</option>\n<option value="vip">vip</option>\n</select>\n<button class="btn purple" onclick="changeRole()">✓</button>\n</div>\n<div id="role-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:6px"></div>\n</div>\n<div class="card">\n<div class="ct">◈ LISTE COMPLÈTE DES UTILISATEURS</div>\n<div class="sw" style="margin-bottom:8px"><input id="adm-q" placeholder="Rechercher..." oninput="filterAdmUsers()"></div>\n<div class="tbl-wrap">\n<table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur</th></tr></thead>\n<tbody id="adm-ut"></tbody></table>\n</div>\n</div>\n<div class="card red">\n<div class="ct">◈ ACTIONS DE MAINTENANCE</div>\n<button class="btn full" style="color:var(--gold);border-color:rgba(255,176,32,.3);background:rgba(255,176,32,.06)" onclick="pruneHistory()">✂️ Réduire historique NXC (100 pts)</button>\n<button class="btn full red" onclick="resetAllTrades()">🗑️ Reset trades 24h</button>\n<button class="btn full" style="color:var(--cyan);border-color:rgba(0,229,255,.3);background:rgba(0,229,255,.06)" onclick="backupDB()">💾 Backup base de données JSON</button>\n<button class="btn full" style="color:var(--purple);border-color:rgba(160,107,255,.3);background:rgba(160,107,255,.06)" onclick="pingServer()">📡 Ping serveur</button>\n<div id="maint-msg" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n<div class="card purple">\n<div class="ct">◈ LOGS SYSTÈME</div>\n<div style="display:flex;gap:6px;margin-bottom:8px">\n<button class="btn purple" onclick="renderLog()" style="font-size:11px">🔄 Actualiser</button>\n<button class="btn red" onclick="_log=[];renderLog()" style="font-size:11px">🗑️ Vider</button>\n</div>\n<div id="log-list" style="max-height:250px;overflow-y:auto;border-radius:10px;border:1px solid rgba(160,107,255,.1)">\n<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun log</p>\n</div>\n</div>\n</div>\n\n<!-- UTILISATEURS -->\n<div class="view" id="view-users">\n<div class="g3">\n<div class="st"><div class="sv" id="u-total">—</div><div class="sl">Comptes</div></div>\n<div class="st"><div class="sv gold" id="u-admins">—</div><div class="sl">Admins</div></div>\n<div class="st"><div class="sv green" id="u-rew">—</div><div class="sl">Total rewards</div></div>\n</div>\n<div class="card">\n<div class="ct">◈ UTILISATEURS\n<div style="display:flex;gap:4px">\n<button class="btn cyan" onclick="sortU(\'rew\')" style="padding:3px 7px;font-size:9px">Rewards</button>\n<button class="btn" onclick="sortU(\'nxc\')" style="padding:3px 7px;font-size:9px">NXC</button>\n<button class="btn" onclick="sortU(\'name\')" style="padding:3px 7px;font-size:9px">A-Z</button>\n</div>\n</div>\n<div class="sw" style="margin-bottom:8px"><input id="us-q" placeholder="Rechercher..." oninput="filterU()"></div>\n<div class="tbl-wrap">\n<table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur R</th></tr></thead>\n<tbody id="ut"></tbody></table>\n</div>\n<div id="us-msg" style="font-size:11px;color:var(--muted);margin-top:8px;text-align:center"></div>\n</div>\n</div>\n\n<!-- STATS -->\n<div class="view" id="view-stats">\n<div class="card purple"><div class="ct">◈ VOLUME 24H</div><div class="chart-wrap ch150"><canvas id="ch-vol"></canvas></div></div>\n<div class="card gold"><div class="ct">◈ REWARDS PAR UTILISATEUR</div><div id="rew-bars"></div></div>\n<div class="card"><div class="ct">◈ SANTÉ DU MARCHÉ</div><div class="g2" id="health-grid"></div></div>\n</div>\n\n<!-- SOLVABILITÉ -->\n<div class="view" id="view-solv">\n<div class="card">\n<div class="ct">◈ SOLVABILITÉ</div>\n<div style="display:flex;align-items:center;gap:14px;padding:14px;background:var(--bg3);border-radius:12px;margin-bottom:12px;cursor:pointer" onclick="toggleSolv()">\n<div class="tg" id="stg"><div class="tg-k"></div></div>\n<div id="sl" style="font-size:14px;font-weight:700;color:var(--muted)">Désactivée</div>\n</div>\n<div class="row" style="margin-bottom:8px">\n<span style="font-size:12px;color:var(--muted);white-space:nowrap;flex-shrink:0">Geste commercial :</span>\n<input id="sg" type="number" value="50" class="grow">\n<button class="btn primary" onclick="saveSolv()">Sauver</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:6px">\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=10" style="font-size:11px">10R</button>\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=50" style="font-size:11px">50R</button>\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=100" style="font-size:11px">100R</button>\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=500" style="font-size:11px">500R</button>\n</div>\n<div id="sm" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n</div>\n\n<!-- OUTILS -->\n<div class="view" id="view-tools">\n<div class="card cyan">\n<div class="ct">◈ CALCULATRICE NXC ↔ REWARDS</div>\n<div class="row" style="margin-bottom:8px">\n<input id="c-nxc" type="number" placeholder="NXC" class="grow" oninput="calcN()">\n<span style="color:var(--muted);font-size:18px">→</span>\n<input id="c-rew" type="number" placeholder="Rewards R" class="grow" readonly style="background:rgba(0,229,255,.05)">\n</div>\n<div class="row">\n<input id="c-rew2" type="number" placeholder="Rewards R" class="grow" oninput="calcR()">\n<span style="color:var(--muted);font-size:18px">→</span>\n<input id="c-nxc2" type="number" placeholder="NXC" class="grow" readonly style="background:rgba(0,229,255,.05)">\n</div>\n</div>\n<div class="card gold">\n<div class="ct">◈ SIMULATEUR DE VENTE</div>\n<div class="row" style="margin-bottom:8px">\n<input id="ss-nxc" type="number" placeholder="NXC à vendre" class="grow" oninput="simS()">\n<input id="ss-fee" type="number" placeholder="Frais %" value="0" style="width:90px;margin:0;flex-shrink:0" oninput="simS()">\n</div>\n<div id="ss-res" style="padding:12px;background:var(--bg3);border-radius:10px;min-height:44px;font-size:13px"></div>\n</div>\n<div class="card purple">\n<div class="ct">◈ MINUTEUR ADMIN</div>\n<div class="row" style="margin-bottom:8px">\n<input id="tm-m" type="number" placeholder="Min" value="5" class="grow">\n<input id="tm-s" type="number" placeholder="Sec" value="0" class="grow">\n<select id="tm-a" style="flex:1;margin:0;font-size:12px">\n<option value="stop">Arrêter tendance</option>\n<option value="up">Lancer hausse</option>\n<option value="down">Lancer baisse</option>\n<option value="crash">Crash -30%</option>\n<option value="moon">Moon +30%</option>\n</select>\n</div>\n<button class="btn cyan full" onclick="startTimer()">⏱️ Démarrer</button>\n<button class="btn full" style="color:var(--muted)" onclick="stopTimer()">✕ Annuler</button>\n<div id="tm-disp" style="font-family:monospace;font-size:36px;font-weight:900;color:var(--cyan);text-align:center;padding:10px;min-height:56px"></div>\n</div>\n<div class="card green">\n<div class="ct">◈ PING SERVEUR</div>\n<button class="btn green full" onclick="pingServer()">📡 Tester</button>\n<div id="ping-res" style="font-size:13px;font-weight:700;text-align:center;padding:10px;min-height:36px"></div>\n</div>\n</div>\n\n<!-- JOURNAL -->\n<div class="view" id="view-log">\n<div class="card">\n<div class="ct">◈ JOURNAL ADMIN <button class="btn red" onclick="_log=[];renderLog()" style="padding:3px 8px;font-size:9px">Vider</button></div>\n<div id="log-list2" style="max-height:500px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)">\n<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun log</p>\n</div>\n</div>\n</div>\n\n<!-- CONFIG -->\n<div class="view" id="view-config">\n<div class="card purple">\n<div class="ct">◈ PLANCHER / PLAFOND AUTOMATIQUES</div>\n<div class="row" style="margin-bottom:8px">\n<input id="cfg-fl" type="number" placeholder="Plancher min (R)" class="grow">\n<button class="btn purple" onclick="_cfgFloor=parseFloat(document.getElementById(\'cfg-fl\').value)||null;updCfg()">✓ Plancher</button>\n<button class="btn red" onclick="_cfgFloor=null;updCfg()" style="padding:10px">✕</button>\n</div>\n<div class="row" style="margin-bottom:8px">\n<input id="cfg-cl" type="number" placeholder="Plafond max (R)" class="grow">\n<button class="btn purple" onclick="_cfgCeil=parseFloat(document.getElementById(\'cfg-cl\').value)||null;updCfg()">✓ Plafond</button>\n<button class="btn red" onclick="_cfgCeil=null;updCfg()" style="padding:10px">✕</button>\n</div>\n<div id="cfg-info" style="font-size:11px;color:var(--muted);padding:8px;background:var(--bg3);border-radius:8px">Plancher: non défini · Plafond: non défini</div>\n</div>\n<div class="card gold">\n<div class="ct">◈ TENDANCE PROGRAMMÉE</div>\n<div class="row" style="margin-bottom:8px">\n<input id="cfg-st" type="time" class="grow">\n<input id="cfg-sp" type="time" class="grow">\n<select id="cfg-sd" style="flex:1;margin:0"><option value="up">Hausse</option><option value="down">Baisse</option><option value="random">Aléatoire</option></select>\n</div>\n<button class="btn gold full" onclick="scheduleT()">⏰ Programmer</button>\n<button class="btn full" style="color:var(--muted)" onclick="if(_schedInt){clearInterval(_schedInt);_schedInt=null;document.getElementById(\'cfg-sch-msg\').textContent=\'Annulé\';}">✕ Annuler</button>\n<div id="cfg-sch-msg" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n<div class="card cyan">\n<div class="ct">◈ EXPORTS</div>\n<button class="btn cyan full" onclick="exportHist()">📥 Historique JSON</button>\n<button class="btn purple full" onclick="exportStats()">📊 Rapport complet JSON</button>\n<button class="btn gold full" onclick="exportFlux()">💰 Flux bancaires CSV</button>\n</div>\n</div>\n\n<!-- ALERTES -->\n<div class="view" id="view-notifs">\n<div class="card gold">\n<div class="ct">◈ ALERTES DE PRIX</div>\n<div class="row" style="margin-bottom:8px">\n<input id="al-p" type="number" placeholder="Prix cible (R)" class="grow">\n<select id="al-d" style="width:auto;flex-shrink:0;margin:0;font-size:12px;padding:10px 8px"><option value="above">Si &gt;</option><option value="below">Si &lt;</option></select>\n<button class="btn gold" onclick="addAlert()">+ Alerte</button>\n</div>\n<div id="al-list" style="max-height:200px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"><p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune alerte</p></div>\n</div>\n<div class="card"><div class="ct">◈ ALERTES INTELLIGENTES</div><div id="smart-al"></div></div>\n<div class="card purple">\n<div class="ct">◈ HISTORIQUE ALERTES <button class="btn red" onclick="_alHist=[];renderAlHist()" style="padding:3px 7px;font-size:9px">Vider</button></div>\n<div id="al-hist" style="max-height:200px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"><p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune</p></div>\n</div>\n</div>\n\n</div><!-- end content -->\n\n<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js">\n// ══ ÉPINGLAGE SITES (sync cross-device via serveur) ══\nvar _pinnedSites=[];\n\nasync function loadPinnedSites(){\n  try{\n    var r=await fetch(\'/admin/pinned-sites\');var d=await r.json();\n    if(d.ok){_pinnedSites=d.sites||[];renderSavedSites();}\n  }catch(e){_pinnedSites=JSON.parse(localStorage.getItem(\'nxc_pinned\')||\'[]\');}\n}\n\nasync function togglePin(url,label){\n  var idx=_pinnedSites.findIndex(s=>s.url===url);\n  if(idx>=0)_pinnedSites.splice(idx,1);\n  else _pinnedSites.push({url,label});\n  // Sauvegarder sur le serveur ET en local\n  localStorage.setItem(\'nxc_pinned\',JSON.stringify(_pinnedSites));\n  try{await fetch(\'/admin/pinned-sites\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,sites:_pinnedSites})});}catch(e){}\n  renderSavedSites();\n  addLog(\'📌\',(idx>=0?\'Désépinglé\':\'Épinglé\')+\': \'+label);\n}\n\nfunction renderPinnedBar(){\n  var el=$(\'pinned-bar\');if(!el)return;\n  if(!_pinnedSites.length){el.style.display=\'none\';return;}\n  el.style.display=\'flex\';\n  el.innerHTML=_pinnedSites.map(s=>\'<button onclick="loadSite(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" style="padding:5px 12px;background:rgba(255,176,32,.12);border:1px solid rgba(255,176,32,.3);border-radius:8px;color:var(--gold);font-size:11px;font-weight:700;cursor:pointer;white-space:nowrap">📌 \'+esc(s.label)+\'</button>\').join(\'\');\n}\n\n// ══ SAUVEGARDE / IMPORT DONNÉES GLOBALES ══\nasync function saveAllData(){\n  try{\n    var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'export\'})});\n    var d=await r.json();\n    if(!d.ok){setMsg(\'data-msg\',\'❌ Erreur export\',false);return;}\n    var blob=new Blob([JSON.stringify(d.data,null,2)],{type:\'application/json\'});\n    var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_full_backup_\'+Date.now()+\'.json\';a.click();\n    setMsg(\'data-msg\',\'✅ Backup complet téléchargé\',true);\n    addLog(\'💾\',\'Sauvegarde complète téléchargée\');\n  }catch(e){setMsg(\'data-msg\',\'❌ Erreur: \'+e.message,false);}\n}\n\nfunction importData(){\n  var input=document.createElement(\'input\');input.type=\'file\';input.accept=\'.json\';\n  input.onchange=async function(e){\n    var file=e.target.files[0];if(!file)return;\n    var text=await file.text();\n    try{\n      var data=JSON.parse(text);\n      if(!confirm(\'Importer ces données ? Cela écrasera les données actuelles du serveur.\'))return;\n      var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'import\',data:data})});\n      var res=await r.json();\n      setMsg(\'data-msg\',res.ok?\'✅ Données importées avec succès\':\'❌ \'+(res.error||\'Erreur import\'),res.ok);\n      if(res.ok){addLog(\'📥\',\'Données importées depuis fichier\');setTimeout(function(){ref();loadBank();},1000);}\n    }catch(ex){setMsg(\'data-msg\',\'❌ Fichier JSON invalide\',false);}\n  };\n  input.click();\n}\n\n// ══ IMPRESSION ══\nfunction printDashboard(){\n  var p=parseFloat(mkt.price||0);var h=mkt.history||[];\n  var hi=h.length>1?Math.max(...h.slice(-24).map(x=>x.price)):p;\n  var lo=h.length>1?Math.min(...h.slice(-24).map(x=>x.price)):p;\n  var chg=_prevP>0?((p-_prevP)/_prevP*100):0;\n  // Capturer le graphique en PNG\n  var chartImg=\'\';var cv=$(\'ch\');if(cv)chartImg=cv.toDataURL(\'image/png\');\n  var rsiImg=\'\';var rsiCv=$(\'ch-rsi\');if(rsiCv)rsiImg=rsiCv.toDataURL(\'image/png\');\n  var now=new Date().toLocaleString(\'fr-FR\');\n  var win=window.open(\'\',\'_blank\');\n  win.document.write(\'<!DOCTYPE html><html><head><meta charset="utf-8"><title>◈ Nexus NXC — Rapport \'+now+\'</title><style>*{font-family:Arial,sans-serif;box-sizing:border-box}body{background:#fff;color:#000;padding:20px;max-width:900px;margin:0 auto}.header{text-align:center;border-bottom:3px solid #000;padding-bottom:16px;margin-bottom:20px}.title{font-size:28px;font-weight:900;letter-spacing:3px}.date{font-size:12px;color:#666;margin-top:4px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}.stat{border:1px solid #ddd;border-radius:8px;padding:12px;text-align:center}.stat-val{font-size:20px;font-weight:700;margin-bottom:4px}.stat-lbl{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:#666}img{max-width:100%;border:1px solid #ddd;border-radius:8px;margin-bottom:12px}h3{margin:16px 0 8px;font-size:14px;border-bottom:1px solid #eee;padding-bottom:4px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:8px;text-align:left;border:1px solid #ddd}th{background:#f5f5f5;font-weight:700}@media print{.no-print{display:none}}</style></head><body>\');\n  win.document.write(\'<div class="header"><div class="title">◈ NEXUS NXC</div><div class="date">Rapport généré le \'+now+\'</div></div>\');\n  win.document.write(\'<div class="grid">\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(p,2)+\' R</div><div class="stat-lbl">Prix actuel</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%</div><div class="stat-lbl">Variation</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(hi,0)+\' R</div><div class="stat-lbl">Haut 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(lo,0)+\' R</div><div class="stat-lbl">Bas 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(mkt.volume24||0,0)+\' R</div><div class="stat-lbl">Volume 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(mkt.trades24||0)+\'</div><div class="stat-lbl">Trades 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+h.length+\'</div><div class="stat-lbl">Points hist.</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(_users.length||0)+\'</div><div class="stat-lbl">Utilisateurs</div></div>\');\n  win.document.write(\'</div>\');\n  if(chartImg)win.document.write(\'<h3>Historique du cours (\'+_ctRange+\' derniers points)</h3><img src="\'+chartImg+\'">\');\n  if(rsiImg)win.document.write(\'<h3>RSI (14 ticks)</h3><img src="\'+rsiImg+\'">\');\n  if(_users.length){\n    win.document.write(\'<h3>Utilisateurs</h3><table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur (R)</th></tr></thead><tbody>\');\n    _users.forEach(u=>{win.document.write(\'<tr><td>\'+esc(u.n)+\'</td><td>\'+esc(u.role)+\'</td><td>\'+fmt(u.rew,0)+\'</td><td>\'+u.nxc.toFixed(4)+\'</td><td>\'+fmt(u.val,0)+\'</td></tr>\');});\n    win.document.write(\'</tbody></table>\');\n  }\n  win.document.write(\'<h3>Derniers logs</h3><table><thead><tr><th>Heure</th><th>Action</th></tr></thead><tbody>\');\n  _log.slice(0,20).forEach(l=>{win.document.write(\'<tr><td>\'+fmtT(l.ts)+\'</td><td>\'+l.ico+\' \'+esc(l.txt)+\'</td></tr>\');});\n  win.document.write(\'</tbody></table>\');\n  win.document.write(\'</body></html>\');\n  win.document.close();\n  setTimeout(function(){win.print();},500);\n  addLog(\'🖨️\',\'Impression du tableau de bord\');\n}\n\n</script>\n<script>\nvar KEY=\'\',mkt={},tInt=null,tMode=null,tStr=0.005,tIv=12000,chObj=null,rsiObj=null,volObj=null;\nvar solvOn=false,_users=[],_flux=[],_fluxF=\'all\',_log=[],_alerts=[],_alHist=[];\nvar _prevP=0,_ctType=\'line\',_ctRange=50,_cfgFloor=null,_cfgCeil=null,_schedInt=null;\nvar _tmInt=null,_randP=null,_savedSites=JSON.parse(localStorage.getItem(\'nxc_sites\')||\'[]\'),_curUrl=\'\';\n\nfunction $(i){return document.getElementById(i);}\nfunction fmt(n,d){return Number(n||0).toLocaleString(\'fr-FR\',{minimumFractionDigits:d||0,maximumFractionDigits:d==null?2:d});}\nfunction esc(s){return (s+\'\').replace(/[&<>"]/g,c=>({\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\'}[c]));}\nfunction fmtT(ts){return new Date(ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\',second:\'2-digit\'});}\nfunction setMsg(id,t,ok){var e=$(id);if(!e)return;e.textContent=t;e.style.color=ok?\'var(--green)\':\'var(--red)\';}\nfunction addLog(ico,txt){_log.unshift({ico,txt,ts:Date.now()});if(_log.length>200)_log.pop();renderLog();}\nfunction renderLog(){\n  var h=_log.length?_log.map(l=>\'<div class="log-item"><span class="log-time">\'+fmtT(l.ts)+\'</span><span>\'+l.ico+\'</span><span style="color:var(--text);flex:1">\'+esc(l.txt)+\'</span></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun log</p>\';\n  var l=$(\'log-list\');if(l)l.innerHTML=h;\n  var l2=$(\'log-list2\');if(l2)l2.innerHTML=h;\n}\nasync function api(p,b){b=b||{};b.master_key=KEY;try{var r=await fetch(p,{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify(b)});return await r.json();}catch(e){return{ok:false};}}\n\n// LOGIN\nfunction doLogin(){\n  var k=$(\'mk\');if(!k)return;\n  KEY=k.value.trim();\n  if(!KEY){$(\'lm\').textContent=\'Entrer la clé\';return;}\n  $(\'lm\').textContent=\'Connexion…\';\n  fetch(\'/nxc/price\').then(function(r){return r.json();}).then(function(d){\n    // Tester avec admin/list\n    return fetch(\'/admin/list\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY})});\n  }).then(function(r){return r.json();}).then(function(d){\n    if(d&&d.ok){\n      $(\'ls\').style.display=\'none\';\n      $(\'hd\').classList.add(\'on\');\n      $(\'htm\').style.display=\'block\';\n      addLog(\'🔑\',\'Connexion admin réussie\');loadPinnedSites();\n      ref();loadBank();loadSolv();loadFails();\n      setInterval(ref,15000);\n      setInterval(function(){loadBank();loadFails();},25000);\n      setInterval(function(){$(\'htm\').textContent=new Date().toLocaleTimeString(\'fr-FR\');},1000);\n      // Charger les sites sauvegardés\n      if(!_savedSites.length){\n        _savedSites=[\n          {label:\'Nexus Coin\',url:\'https://lively-art-86d9.noah-guetta.workers.dev\'},\n          {label:\'Panel Admin\',url:location.origin+\'/panel\'},\n          {label:\'GitHub\',url:\'https://github.com/Noah1234567890123456789\'}\n        ];\n        localStorage.setItem(\'nxc_sites\',JSON.stringify(_savedSites));\n      }\n      renderSavedSites();\n    }else{\n      $(\'lm\').textContent=\'❌ Clé incorrecte\';KEY=\'\';\n    }\n  }).catch(function(){$(\'lm\').textContent=\'❌ Serveur inaccessible\';KEY=\'\';});\n}\n\n// TABS\nfunction toggleMore(){var d=$(\'dropdown\');d.classList.toggle(\'show\');}\ndocument.addEventListener(\'click\',function(e){if(!e.target.closest(\'#dropdown\')&&!e.target.closest(\'#btn-more\'))$(\'dropdown\').classList.remove(\'show\');});\n\nfunction go(tab,btn){\n  document.querySelectorAll(\'.view\').forEach(v=>v.classList.remove(\'on\'));\n  document.querySelectorAll(\'.tab\').forEach(t=>t.classList.remove(\'on\'));\n  var v=$(\'view-\'+tab);if(v)v.classList.add(\'on\');\n  if(btn)btn.classList.add(\'on\');\n  if(tab===\'users\')loadUsers();\n  if(tab===\'stats\')loadStats();\n  if(tab===\'admin\'){refreshAdminStats();loadAdmUsers();}\n  if(tab===\'banque\')$(\'nd-b\').style.display=\'none\';\n}\n\n// MARCHÉ\nasync function ref(){\n  try{\n    var r=await fetch(\'/nxc/price\');var d=await r.json();mkt=d;\n    var p=parseFloat(d.price||0),h=d.history||[];\n    var chg=_prevP>0?((p-_prevP)/_prevP*100):0;\n    var hi=h.length>1?Math.max(...h.slice(-24).map(x=>x.price)):p;\n    var lo=h.length>1?Math.min(...h.slice(-24).map(x=>x.price)):p;\n    $(\'s-p\').textContent=fmt(p,2);$(\'s-v\').textContent=fmt(d.volume24||0,0);\n    $(\'s-t\').textContent=d.trades24||0;$(\'s-h\').textContent=h.length;\n    $(\'s-hi\').textContent=fmt(hi,0);$(\'s-lo\').textContent=fmt(lo,0);\n    $(\'s-var\').textContent=(chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%\';$(\'s-var\').style.color=chg>=0?\'var(--green)\':\'var(--red)\';\n    $(\'s-cap\').textContent=fmt(p*3,0);\n    $(\'hp\').textContent=fmt(p,2)+\' R\';\n    var hc=$(\'hc\');if(_prevP>0){hc.textContent=(chg>=0?\'▲+\':\'▼\')+chg.toFixed(2)+\'%\';hc.className=\'hud-chg \'+(chg>=0?\'up\':\'dn\');hc.style.display=\'block\';}\n    _prevP=p;\n    drawC(h);drawA(p,h);drawRSI(h);\n    checkAlerts(p);\n    if(_cfgFloor&&p<_cfgFloor){await tick(_cfgFloor);return;}\n    if(_cfgCeil&&p>_cfgCeil){await tick(_cfgCeil);return;}\n  }catch(e){}\n}\n\nasync function tick(p){await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:0,volume24:mkt.volume24||0,trades24:mkt.trades24||0})});}\n\nfunction setRange(n){_ctRange=n;if(chObj){chObj.destroy();chObj=null;}ref();}\nfunction toggleChartType(){_ctType=_ctType===\'line\'?\'bar\':\'line\';if(chObj){chObj.destroy();chObj=null;}ref();}\nfunction dlChart(){var cv=$(\'ch\');if(!cv)return;var a=document.createElement(\'a\');a.download=\'nxc_\'+Date.now()+\'.png\';a.href=cv.toDataURL();a.click();}\n\nfunction drawC(h){\n  var cv=$(\'ch\');if(!cv||!window.Chart)return;\n  var pts=h.slice(-_ctRange);\n  var labs=pts.map(x=>new Date(x.ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\'}));\n  var prices=pts.map(x=>parseFloat(x.price));\n  if(prices.length<2)return;\n  var mn=Math.min(...prices)*0.85,mx=Math.max(...prices)*1.15;\n  if(chObj){chObj.data.labels=labs;chObj.data.datasets[0].data=prices;chObj.options.scales.y.min=mn;chObj.options.scales.y.max=mx;chObj.update(\'none\');return;}\n  var ctx=cv.getContext(\'2d\');\n  var g=ctx.createLinearGradient(0,0,0,cv.offsetHeight||200);g.addColorStop(0,\'rgba(0,229,255,.2)\');g.addColorStop(1,\'rgba(0,229,255,0)\');\n  chObj=new Chart(ctx,{type:_ctType===\'bar\'?\'bar\':\'line\',data:{labels:labs,datasets:[{data:prices,borderColor:\'#00e5ff\',backgroundColor:_ctType===\'bar\'?\'rgba(0,229,255,.4)\':g,borderWidth:2.5,pointRadius:0,fill:_ctType!==\'bar\',tension:0.4}]},\n    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},\n      scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:5,font:{size:8}},grid:{color:\'rgba(0,229,255,.04)\'}},\n        y:{min:mn,max:mx,ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n}\n\nfunction drawRSI(h){\n  var cv=$(\'ch-rsi\');if(!cv||!window.Chart||h.length<15)return;\n  var prices=h.slice(-28).map(x=>parseFloat(x.price));\n  var rsi=[];\n  for(var i=14;i<prices.length;i++){\n    var g=0,l=0;for(var j=i-14;j<i;j++){var dv=prices[j+1]-prices[j];if(dv>0)g+=dv;else l-=dv;}\n    rsi.push(Math.round(l===0?100:100-100/(1+(g/l))));\n  }\n  var labs=h.slice(-rsi.length).map(x=>new Date(x.ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\'}));\n  if(rsiObj){rsiObj.data.labels=labs;rsiObj.data.datasets[0].data=rsi;rsiObj.update(\'none\');return;}\n  var ctx=cv.getContext(\'2d\');\n  rsiObj=new Chart(ctx,{type:\'line\',data:{labels:labs,datasets:[{data:rsi,borderColor:\'#a06bff\',borderWidth:2,pointRadius:0,fill:false,tension:0.4}]},\n    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},\n      scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:4,font:{size:8}},grid:{display:false}},y:{min:0,max:100,ticks:{color:\'#5c6b8c\',stepSize:25},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n}\n\nfunction drawA(p,h){\n  var el=$(\'al\'),a=[];\n  if(p>80000)a.push({c:\'ae\',m:\'⚡ Prix critique >80 000 R\'});\n  else if(p<500)a.push({c:\'ae\',m:\'🔴 Prix effondrement <500 R\'});\n  else a.push({c:\'ao\',m:\'✅ Prix normal: \'+fmt(p,0)+\' R\'});\n  if(h.length>10){var rv=h.slice(-10).map(x=>x.price);var vol=(Math.max(...rv)-Math.min(...rv))/Math.min(...rv)*100;a.push(vol>20?{c:\'aw\',m:\'⚡ Volatilité: \'+vol.toFixed(1)+\'%\'}:{c:\'ao\',m:\'📊 Stable — volatilité: \'+vol.toFixed(1)+\'%\'});}\n  a.push(tMode?{c:\'aw\',m:\'📊 Tendance \'+tMode+\' · \'+(tStr*100).toFixed(1)+\'%/tick\'}:{c:\'ai\',m:\'⏸ Aucune tendance\'});\n  if(el)el.innerHTML=a.map(x=>\'<div class="ab \'+x.c+\'">\'+x.m+\'</div>\').join(\'\');\n  // Smart alerts\n  var sa=$(\'smart-al\');if(sa)sa.innerHTML=a.map(x=>\'<div class="ab \'+x.c+\'">\'+x.m+\'</div>\').join(\'\');\n}\n\n// CONTRÔLE\nasync function adjP(pct){var p=Math.max(50,Math.min(100000,parseFloat(mkt.price||5213)*(1+pct)));p=Math.round(p*100)/100;await tick(p);setMsg(\'pm\',\'✅ \'+(pct>0?\'+\':\'\')+((pct*100).toFixed(1))+\'% → \'+fmt(p,2)+\' R\',true);addLog(\'📊\',\'Cours \'+(pct>0?\'+\':\'\')+((pct*100).toFixed(1))+\'%\');setTimeout(ref,500);}\nasync function setP(){var p=parseFloat($(\'np\').value);if(!p||p<50||p>100000){setMsg(\'pm\',\'Prix invalide\',false);return;}await tick(p);setMsg(\'pm\',\'✅ Cours → \'+fmt(p,2)+\' R\',true);$(\'np\').value=\'\';addLog(\'💱\',\'Cours fixé: \'+fmt(p,2)+\' R\');setTimeout(ref,500);}\nasync function setPct(){var pct=parseFloat($(\'np-pct\').value)/100;if(isNaN(pct)){setMsg(\'pm\',\'% invalide\',false);return;}await adjP(pct);$(\'np-pct\').value=\'\';}\nasync function resetH(){if(!confirm(\'Reset historique ?\'))return;await fetch(\'/nxc/reset\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY})});addLog(\'🔄\',\'Reset historique NXC\');ref();}\n\nvar _tStart=null,_tTimerInt=null;\nfunction setT(m){\n  var s=parseFloat($(\'ts\').value)||0.005,iv=parseInt($(\'ti\').value)||12000;\n  if(tInt){clearInterval(tInt);tInt=null;}if(_tTimerInt){clearInterval(_tTimerInt);_tTimerInt=null;}\n  tMode=m===\'stop\'?null:m;tStr=s;tIv=iv;_tStart=tMode?Date.now():null;\n  var el=$(\'tst\'),ht=$(\'hc\');\n  if(!tMode){el.textContent=\'⏸ Arrêté\';el.style.color=\'var(--muted)\';if($(\'tt-timer\'))$(\'tt-timer\').textContent=\'\';addLog(\'⏸\',\'Tendance arrêtée\');return;}\n  var lbl=m===\'up\'?\'📈 Hausse +\':m===\'down\'?\'📉 Baisse -\':\'🎲 Aléatoire\';var spd=m!==\'random\'?(s*100).toFixed(1)+\'%\':\'\';\n  el.textContent=lbl+spd+\' · \'+(iv/1000)+\'s/tick\';el.style.color=m===\'up\'?\'var(--green)\':m===\'down\'?\'var(--red)\':\'var(--purple)\';\n  addLog(m===\'up\'?\'📈\':m===\'down\'?\'📉\':\'🎲\',\'Tendance \'+m+\' · \'+(s*100).toFixed(1)+\'%\');\n  _tTimerInt=setInterval(function(){if(_tStart){var el=elapsed=Math.floor((Date.now()-_tStart)/1000);$(\'tt-timer\').textContent=\'⏱ \'+Math.floor(el/60)+\'m\'+(\'0\'+(el%60)).slice(-2)+\'s\';}},1000);\n  tInt=setInterval(async function(){\n    var p=parseFloat(mkt.price||5213);var adj=(Math.random()-0.5)*_noiseLevel*2;\n    if(m===\'up\')adj+=s;else if(m===\'down\')adj-=s;\n    p=Math.max(_cfgFloor||50,Math.min(_cfgCeil||100000,p*(1+adj)));\n    p=Math.random()>.03?Math.round(p*100)/100:Math.round(p);\n    await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:Math.floor(Math.random()*300+50),volume24:(mkt.volume24||0)+100,trades24:(mkt.trades24||0)+1})});\n  },iv);\n}\n\nasync function scenario(sc){\n  var p=parseFloat(mkt.price||5213),t;\n  if(sc===\'crash\')t=p*.7;else if(sc===\'moon\')t=p*1.3;else if(sc===\'ath\')t=Math.min(100000,Math.max(p*1.5,90000));else if(sc===\'floor\')t=200;\n  if(t){t=Math.max(50,Math.min(100000,Math.round(t*100)/100));await tick(t);addLog(\'🎭\',\'Scénario \'+sc+\' → \'+fmt(t,2)+\' R\');setTimeout(ref,500);}\n  else if(sc===\'volatile\'){setT(\'random\');addLog(\'⚡\',\'Scénario volatil\');}\n  else if(sc===\'stable\'){setT(\'stop\');addLog(\'😴\',\'Stabilisation\');}\n}\n\n// BANQUE\nfunction setAmt(v){$(\'bk-amt\').value=v;}\nfunction filterFlux(f){_fluxF=f;[\'fl-all\',\'fl-in\',\'fl-out\'].forEach(id=>{var e=$(id);if(e)e.className=\'btn\';});var e=$(\'fl-\'+f);if(e)e.className=\'btn cyan\';renderFlux();}\nfunction renderFlux(){\n  var flux=(_fluxF===\'all\'?_flux:_flux.filter(f=>f.type===_fluxF)).slice(0,30);\n  var el=$(\'bk-flux\');if(!el)return;\n  el.innerHTML=flux.length?flux.map(f=>\'<div class="fl-item"><div style="width:8px;height:8px;border-radius:50%;flex-shrink:0;background:\'+(f.type===\'IN\'?\'var(--green)\':\'var(--red)\')+\';box-shadow:0 0 6px \'+(f.type===\'IN\'?\'rgba(0,255,157,.4)\':\'rgba(255,61,94,.4)\')+\'"></div><span style="font-weight:700;color:\'+(f.type===\'IN\'?\'var(--green)\':\'var(--red)\')+\';flex-shrink:0">\'+(f.type===\'IN\'?\'+\':\'-\')+fmt(f.amount||0,0)+\' R</span><span style="color:var(--muted);flex:1">\'+esc(f.user||\'?\')+\'</span><span style="color:var(--muted);font-size:10px">\'+new Date(f.ts).toLocaleTimeString(\'fr-FR\')+\'</span></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun flux</p>\';\n}\n\nfunction exportFlux(){var csv=\'Date,Type,User,Montant\\n\';_flux.forEach(f=>csv+=new Date(f.ts).toLocaleString(\'fr-FR\')+\',\'+f.type+\',\'+(f.user||\'\')+\',\'+(f.amount||0)+\'\\n\');var b=new Blob([csv],{type:\'text/csv\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(b);a.download=\'flux_\'+Date.now()+\'.csv\';a.click();addLog(\'📊\',\'Export CSV flux\');}\n\nasync function loadBank(){\n  try{\n    var r=await fetch(\'/nxc/bank\');var d=await r.json();if(!d.ok)return;var b=d.bank||{};\n    _flux=(b.flux||[]).slice().reverse();\n    var p=parseFloat(mkt.price||0);\n    $(\'bk-r\').textContent=fmt(b.reserves||0,0)+\' R\';$(\'bk-i\').textContent=fmt(b.totalIn||0,0);\n    $(\'bk-o\').textContent=fmt(b.totalOut||0,0);\n    $(\'bk-rt\').textContent=(b.totalIn>0?((b.reserves||0)/b.totalIn*100):100).toFixed(1)+\'%\';\n    $(\'bk-nx\').textContent=parseFloat(b.nxcEmis||0).toFixed(4)+\' NXC\';\n    $(\'bk-vx\').textContent=fmt((b.nxcEmis||0)*p,0)+\' R\';\n    var bn=(b.totalIn||0)-(b.totalOut||0);var el=$(\'bk-bn\');el.textContent=(bn>=0?\'+\':\'\')+fmt(bn,0)+\' R\';el.style.color=bn>=0?\'var(--green)\':\'var(--red)\';\n    $(\'bk-fl\').textContent=_flux.length;\n    renderFlux();\n  }catch(e){}\n}\n\nasync function bankOp(type){\n  var amt=parseFloat($(\'bk-amt\').value);if(!amt||amt<=0){setMsg(\'bk-msg\',\'Montant invalide\',false);return;}\n  var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{reserves:0,totalIn:0,totalOut:0,nxcEmis:0,flux:[]};\n  if(type===\'out\'&&amt>(b.reserves||0)){setMsg(\'bk-msg\',\'❌ Réserves insuffisantes\',false);return;}\n  if(type===\'in\'){b.reserves=parseFloat(((b.reserves||0)+amt).toFixed(2));b.totalIn=parseFloat(((b.totalIn||0)+amt).toFixed(2));}\n  else{b.reserves=parseFloat(((b.reserves||0)-amt).toFixed(2));b.totalOut=parseFloat(((b.totalOut||0)+amt).toFixed(2));}\n  b.flux=b.flux||[];b.flux.push({type:type===\'in\'?\'IN\':\'OUT\',user:\'SERVEUR\',amount:amt,nxc:0,ts:Date.now()});\n  var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:b,reset:true})});\n  var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ \'+(type===\'in\'?\'+\':\'-\')+fmt(amt,0)+\' R\':\'❌ Erreur\',res.ok);\n  if(res.ok){$(\'bk-amt\').value=\'\';addLog(type===\'in\'?\'💰\':\'💸\',(type===\'in\'?\'Injection +\':\'Retrait -\')+fmt(amt,0)+\' R\');loadBank();}\n}\n\nasync function bankResetHist(){var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{};if(!confirm(\'Reset historique ? Réserves: \'+fmt(b.reserves||0,0)+\' R conservées\'))return;var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:{reserves:b.reserves||0,nxcEmis:0,totalIn:0,totalOut:0,flux:[]},reset:true})});var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ Historique effacé\':\'❌ Erreur\',res.ok);if(res.ok){addLog(\'🗑️\',\'Reset historique banque\');loadBank();}}\nasync function bankResetAll(){var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{};var g=confirm(\'Garder réserves (\'+fmt(b.reserves||0,0)+\' R) ?\');if(!confirm(\'Confirmer ?\'))return;var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:{reserves:g?(b.reserves||0):0,nxcEmis:0,totalIn:0,totalOut:0,flux:[]},reset:true})});var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ Réinitialisé\':\'❌ Erreur\',res.ok);if(res.ok){addLog(\'💥\',\'Reset complet banque\');loadBank();}}\n\nasync function loadFails(){\n  try{\n    var r=await fetch(\'/nxc/bank/fail\');var d=await r.json();\n    var el=$(\'bk-fails\'),fc=$(\'fails-ct\');if(!el)return;\n    var fails=(d.fails||[]).slice().reverse();\n    if(fails.length&&fc){fc.textContent=fails.length;fc.style.display=\'block\';}$(\'nd-b\').style.display=fails.length?\'block\':\'none\';\n    el.innerHTML=fails.length?fails.map(f=>\'<div style="padding:12px;border-bottom:1px solid rgba(255,61,94,.08);display:flex;flex-direction:column;gap:6px"><div style="display:flex;justify-content:space-between"><span style="color:var(--red);font-weight:700">❌ \'+esc(f.user)+\'</span><span style="color:var(--muted);font-size:10px;font-family:monospace">\'+new Date(f.ts).toLocaleTimeString(\'fr-FR\')+\'</span></div><div style="color:var(--muted);font-size:11px">Voulait vendre <b style="color:var(--text)">\'+f.nxc+\' NXC</b> (\'+fmt(f.amount||0,0)+\' R)</div>\'+(f.gesture>0?\'<button onclick="sendGesture(\\\'\'+esc(f.user)+\'\\\',\'+f.gesture+\',\'+f.ts+\')" style="padding:8px 14px;background:rgba(0,255,157,.1);border:1px solid rgba(0,255,157,.3);border-radius:9px;color:var(--green);font-size:12px;cursor:pointer;font-weight:700;align-self:flex-start">💝 Verser +\'+f.gesture+\' R</button>\':\'\')+\'</div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">✅ Aucune tentative</p>\';\n  }catch(e){}\n}\n\nasync function sendGesture(user,amount,failTs){\n  if(!confirm(\'Verser \'+amount+\' R à \'+user+\' ?\'))return;\n  var r=await fetch(\'/nxc/bank/gesture\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,target:user,amount:amount,fail_ts:failTs})});\n  var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ \'+amount+\' R versés à \'+user:\'❌ \'+(res.error||\'Erreur\'),res.ok);\n  if(res.ok){addLog(\'💝\',\'Geste +\'+amount+\' R → \'+user);loadBank();loadFails();}\n}\n\n// APP (iframe configurable)\nfunction renderSavedSites(){\n  var el=$(\'saved-sites\');if(!el)return;\n  var pinned=_pinnedSites.map(s=>s.url);\n  el.innerHTML=_savedSites.length?_savedSites.map(s=>{\n    var isPinned=pinned.includes(s.url);\n    return \'<div style="display:flex;align-items:center;gap:4px;background:var(--bg3);border:1px solid \'+(isPinned?\'rgba(255,176,32,.4)\':\'var(--border)\')+\';border-radius:8px;padding:4px 8px;white-space:nowrap">\'\n      +\'<button onclick="loadSite(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" style="background:none;border:none;color:\'+(isPinned?\'var(--gold)\':\'var(--cyan)\')+\';font-size:11px;font-weight:700;cursor:pointer;padding:0">\'+(isPinned?\'📌 \':\'\')+esc(s.label)+\'</button>\'\n      +\'<button onclick="togglePin(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" title="\'+(isPinned?\'Désépingler\':\'Épingler\')+\'" style="background:none;border:none;color:\'+(isPinned?\'var(--gold)\':\'var(--muted)\')+\';font-size:11px;cursor:pointer;padding:0;margin-left:2px">\'+(isPinned?\'📌\':\'📍\')+\'</button>\'\n      +\'<button onclick="deleteSite(\\\'\'+esc(s.url)+\'\\\')" style="background:none;border:none;color:var(--red);font-size:12px;cursor:pointer;padding:0;margin-left:2px">✕</button>\'\n      +\'</div>\';\n  }).join(\'\'):\'<span style="color:var(--muted);font-size:11px">Aucun site sauvegardé</span>\';\n  // Afficher les sites épinglés en premier si existants\n  renderPinnedBar();\n}\nfunction goUrl(){var url=$(\'iframe-in\').value.trim();if(!url)return;if(!url.startsWith(\'http\'))url=\'https://\'+url;loadSite(url,null);$(\'iframe-in\').value=\'\';}\nfunction loadSite(url,label){_curUrl=url;var f=$(\'nf\');if(f)f.src=url;var t=$(\'if-title\');if(t)t.textContent=\'◈ \'+(label||url.replace(\'https://\',\'\').split(\'/\')[0]);var u=$(\'if-url\');if(u)u.textContent=url.replace(\'https://\',\'\').replace(\'http://\',\'\');}\nfunction saveSite(){var url=$(\'iframe-in\').value.trim()||_curUrl;var lbl=$(\'site-lbl\').value.trim()||url.replace(\'https://\',\'\').split(\'/\')[0];if(!url)return;if(!url.startsWith(\'http\'))url=\'https://\'+url;_savedSites=_savedSites.filter(s=>s.url!==url);_savedSites.unshift({label:lbl,url});if(_savedSites.length>8)_savedSites.pop();localStorage.setItem(\'nxc_sites\',JSON.stringify(_savedSites));$(\'site-lbl\').value=\'\';$(\'iframe-in\').value=\'\';renderSavedSites();addLog(\'💾\',\'Site sauvegardé: \'+lbl);}\nfunction deleteSite(url){_savedSites=_savedSites.filter(s=>s.url!==url);localStorage.setItem(\'nxc_sites\',JSON.stringify(_savedSites));renderSavedSites();}\nfunction reloadF(){var f=$(\'nf\');if(f)f.src=f.src;}\nfunction openNewTab(){if(_curUrl)window.open(_curUrl,\'_blank\');}\n\n// ADMIN\nasync function refreshAdminStats(){\n  try{\n    var pd=await fetch(\'/nxc/price\').then(r=>r.json());\n    var bd=await fetch(\'/nxc/bank\').then(r=>r.json());\n    var fd=await fetch(\'/nxc/bank/fail\').then(r=>r.json());\n    var b=bd.bank||{};var p=parseFloat(pd.price||0);\n    $(\'adm-price\').textContent=fmt(p,2)+\' R\';\n    $(\'adm-vol\').textContent=fmt(pd.volume24||0,0)+\' R\';\n    $(\'adm-trades\').textContent=pd.trades24||0;\n    $(\'adm-res\').textContent=fmt(b.reserves||0,0)+\' R\';\n    $(\'adm-nxc\').textContent=parseFloat(b.nxcEmis||0).toFixed(4);\n    $(\'adm-fails\').textContent=(fd.fails||[]).length;\n    $(\'adm-hist\').textContent=(pd.history||[]).length;\n    if(_users.length)$(\'adm-users\').textContent=_users.length;\n    addLog(\'📊\',\'Stats admin actualisées\');\n  }catch(e){}\n}\n\nasync function loadAdmUsers(){\n  if(!_users.length)await loadUsers();\n  var sel1=$(\'rw-u\'),sel2=$(\'role-u\');\n  [sel1,sel2].forEach(sel=>{if(sel)sel.innerHTML=\'<option value="">Utilisateur...</option>\'+_users.map(u=>\'<option value="\'+esc(u.n)+\'">\'+esc(u.n)+(u.role===\'admin\'?\' 👑\':u.role===\'moderator\'?\' 🛡️\':u.role===\'vip\'?\' ⭐\':\'\')+\'</option>\').join(\'\');});\n  $(\'adm-users\').textContent=_users.length;\n  renderAdmUsers(_users);\n}\n\nfunction renderAdmUsers(rows){\n  var el=$(\'adm-ut\');if(!el)return;\n  el.innerHTML=rows.map(r=>\'<tr><td style="font-weight:700;color:var(--cyan)">\'+esc(r.n)+(r.role===\'admin\'?\' 👑\':r.role===\'moderator\'?\' 🛡️\':r.role===\'vip\'?\' ⭐\':\'\')+\'</td><td style="color:var(--muted);font-size:10px">\'+esc(r.role)+\'</td><td style="color:var(--gold)">\'+fmt(r.rew,0)+\'</td><td style="color:var(--cyan);font-family:monospace">\'+r.nxc.toFixed(4)+\'</td><td style="color:var(--purple)">\'+fmt(r.val,0)+\'</td></tr>\').join(\'\');\n}\nfunction filterAdmUsers(){var q=($(\'adm-q\').value||\'\').toLowerCase();renderAdmUsers(q?_users.filter(u=>u.n.toLowerCase().includes(q)):_users);}\n\nasync function giveRewards(){\n  var target=$(\'rw-u\').value,amt=parseFloat($(\'rw-amt\').value);\n  if(!target||!amt||amt<=0){setMsg(\'rw-msg\',\'Remplir tous les champs\',false);return;}\n  var r=await fetch(\'/admin/give-rewards\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,target:target,amount:amt})});\n  var res=await r.json();\n  setMsg(\'rw-msg\',res.ok?\'✅ +\'+fmt(amt,0)+\' R donnés à \'+target+\' (total: \'+fmt(res.new_rewards||0,0)+\' R)\':\'❌ \'+(res.error||\'Erreur\'),res.ok);\n  if(res.ok){addLog(\'🏆\',\'Rewards +\'+fmt(amt,0)+\' R → \'+target);}\n}\n\nasync function changeRole(){\n  var u=$(\'role-u\').value,role=$(\'role-v\').value;\n  if(!u){setMsg(\'role-msg\',\'Sélectionner un utilisateur\',false);return;}\n  var r=await fetch(\'/admin/set-role\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,target:u,role:role})});\n  var res=await r.json();\n  setMsg(\'role-msg\',res.ok?\'✅ Rôle de \'+u+\' changé en \'+role:\'❌ \'+(res.error||\'Erreur\'),res.ok);\n  if(res.ok)addLog(\'👑\',\'Rôle \'+u+\' → \'+role);\n}\n\nasync function pruneHistory(){if(!confirm(\'Réduire historique à 100 points ?\'))return;await fetch(\'/nxc/reset\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY})});setMsg(\'maint-msg\',\'✅ Historique réduit\',true);addLog(\'✂️\',\'Historique NXC réduit\');}\nasync function resetAllTrades(){if(!confirm(\'Reset trades 24h ?\'))return;await tick(parseFloat(mkt.price||5213));setMsg(\'maint-msg\',\'✅ Trades remis à zéro\',true);addLog(\'🗑️\',\'Reset trades 24h\');}\n\nasync function backupDB(){\n  try{var p=await(await fetch(\'/nxc/price\')).json();var b=await(await fetch(\'/nxc/bank\')).json();var u=await api(\'/admin/list\');var data={date:new Date().toISOString(),market:p,bank:b.bank||{},users:u.users||[]};var blob=new Blob([JSON.stringify(data,null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_backup_\'+Date.now()+\'.json\';a.click();setMsg(\'maint-msg\',\'✅ Backup téléchargé\',true);addLog(\'💾\',\'Backup DB téléchargé\');}catch(e){setMsg(\'maint-msg\',\'❌ Erreur backup\',false);}\n}\n\nasync function pingServer(){\n  var el=$(\'ping-res\');if(el){el.textContent=\'📡 Test...\';el.style.color=\'var(--muted)\';}\n  var t=Date.now();\n  try{await fetch(\'/nxc/price\');var lat=Date.now()-t;var c=lat<500?\'var(--green)\':lat<1000?\'var(--gold)\':\'var(--red)\';if(el){el.textContent=\'✅ En ligne — \'+lat+\' ms\';el.style.color=c;}}\n  catch(e){if(el){el.textContent=\'❌ Inaccessible\';el.style.color=\'var(--red)\';}}\n}\n\n// USERS\nasync function loadUsers(){\n  $(\'us-msg\').textContent=\'Chargement…\';\n  try{\n    var r=await api(\'/admin/list\');if(!r||!r.ok){$(\'us-msg\').textContent=\'Erreur\';return;}\n    var p=parseFloat(mkt.price||0);\n    var rows=await Promise.all((r.users||[]).map(async u=>{\n      var d=await api(\'/admin/get\',{target:u.username});\n      var rew=Math.max((d.data&&d.data.nx2098&&d.data.nx2098.rewards)||0,(d.data&&d.data.rewards&&d.data.rewards.points)||0);\n      var nxc=parseFloat((d.data&&d.data.nxcoin&&d.data.nxcoin.nxc)||0);\n      return {n:u.username,role:u.role,rew,nxc,val:nxc*p};\n    }));\n    _users=rows;\n    $(\'u-total\').textContent=rows.length;$(\'u-admins\').textContent=rows.filter(r=>r.role===\'admin\').length;\n    $(\'u-rew\').textContent=fmt(rows.reduce((s,r)=>s+r.rew,0),0);\n    sortU(\'rew\');$(\'us-msg\').textContent=\'\';\n    if($(\'adm-ut\'))loadAdmUsers();\n  }catch(e){$(\'us-msg\').textContent=\'Erreur\';}\n}\nfunction sortU(by){_users.sort((a,b)=>by===\'name\'?a.n.localeCompare(b.n):(b[by]-a[by]));renderU(_users);}\nfunction renderU(rows){var el=$(\'ut\');if(!el)return;el.innerHTML=rows.map(r=>\'<tr><td style="font-weight:700;color:var(--cyan)">\'+esc(r.n)+(r.role===\'admin\'?\' 👑\':r.role===\'moderator\'?\' 🛡️\':r.role===\'vip\'?\' ⭐\':\'\')+\'</td><td style="color:var(--muted);font-size:10px">\'+esc(r.role)+\'</td><td style="color:var(--gold)">\'+fmt(r.rew,0)+\'</td><td style="color:var(--cyan);font-family:monospace">\'+r.nxc.toFixed(4)+\'</td><td style="color:var(--purple)">\'+fmt(r.val,0)+\'</td></tr>\').join(\'\');}\nfunction filterU(){var q=($(\'us-q\').value||\'\').toLowerCase();renderU(q?_users.filter(r=>r.n.toLowerCase().includes(q)):_users);}\n\n// STATS\nvar volObj=null;\nasync function loadStats(){\n  if(!_users.length)await loadUsers();\n  var h=mkt.history||[];var p=parseFloat(mkt.price||0);\n  if(h.length>5){var cv=$(\'ch-vol\');if(cv&&window.Chart){var pts=h.slice(-20);var labs=pts.map(x=>new Date(x.ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\'}));var vols=pts.map(x=>x.vol||0);if(volObj){volObj.data.labels=labs;volObj.data.datasets[0].data=vols;volObj.update(\'none\');}else{var ctx=cv.getContext(\'2d\');volObj=new Chart(ctx,{type:\'bar\',data:{labels:labs,datasets:[{data:vols,backgroundColor:\'rgba(160,107,255,.5)\',borderColor:\'#a06bff\',borderWidth:1}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:4,font:{size:8}},grid:{display:false}},y:{ticks:{color:\'#5c6b8c\'},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});}}}\n  var el=$(\'rew-bars\');if(el&&_users.length){var maxR=Math.max(..._users.map(u=>u.rew))||1;el.innerHTML=[..._users].sort((a,b)=>b.rew-a.rew).slice(0,8).map(u=>\'<div style="margin-bottom:8px"><div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:3px"><span style="color:var(--cyan);font-weight:700">\'+esc(u.n)+\'</span><span style="color:var(--gold)">\'+fmt(u.rew,0)+\' R</span></div><div class="pbar"><div class="pbar-fill" style="width:\'+Math.round(u.rew/maxR*100)+\'%"></div></div></div>\').join(\'\');}\n  var hi=h.length>1?Math.max(...h.slice(-24).map(x=>x.price)):p;var lo=h.length>1?Math.min(...h.slice(-24).map(x=>x.price)):p;var vol=lo>0?(hi-lo)/lo*100:0;\n  var hg=$(\'health-grid\');if(hg)hg.innerHTML=[[\'📈 Tendance\',h.length>5?(h.slice(-5).map(x=>x.price).every((v,i,a)=>i===0||v>a[i-1])?\'<span style="color:var(--green)">Haussière</span>\':h.slice(-5).map(x=>x.price).every((v,i,a)=>i===0||v<a[i-1])?\'<span style="color:var(--red)">Baissière</span>\':\'<span style="color:var(--muted)">Neutre</span>\'):\'—\'],[\'⚡ Volatilité\',vol.toFixed(2)+\'%\'],[\'📊 Amplitude\',fmt(hi-lo,0)+\' R\'],[\'🔢 Trades\',mkt.trades24||0]].map(([k,v])=>\'<div class="st"><div class="sv" style="font-size:12px">\'+v+\'</div><div class="sl">\'+k+\'</div></div>\').join(\'\');\n}\n\n// SOLVABILITÉ\nasync function loadSolv(){try{var r=await fetch(\'/nxc/solvability\');var d=await r.json();if(d.ok){solvOn=d.enabled;var inp=$(\'sg\');if(inp)inp.value=d.gesture||50;updSolv();}}catch(e){}}\nfunction updSolv(){var t=$(\'stg\'),l=$(\'sl\');if(solvOn){if(t)t.classList.add(\'on\');if(l){l.textContent=\'✅ Activée\';l.style.color=\'var(--green)\';}}else{if(t)t.classList.remove(\'on\');if(l){l.textContent=\'⏸ Désactivée\';l.style.color=\'var(--muted)\';}}}\nasync function toggleSolv(){solvOn=!solvOn;updSolv();await saveSolv();}\nasync function saveSolv(){var g=parseInt($(\'sg\').value)||50;var r=await fetch(\'/nxc/solvability\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,enabled:solvOn,gesture:g})});var res=await r.json();setMsg(\'sm\',res.ok?(solvOn?\'✅ Activée\':\'⏸ Désactivée\'):\'❌ Erreur\',res.ok);if(res.ok)addLog(\'🛡️\',\'Solvabilité \'+(solvOn?\'activée\':\'désactivée\'));}\n\n// OUTILS\nvar _noiseLevel=0.004;\nfunction updateNoise(v){\n  _noiseLevel=parseFloat(v)/1000;\n  var el=$(\'noise-val\');if(el)el.textContent=(parseFloat(v)*0.1).toFixed(1)+\'%\';\n}\nfunction calcN(){var n=parseFloat($(\'c-nxc\').value)||0;var p=parseFloat(mkt.price||0);$(\'c-rew\').value=n&&p?Math.round(n*p*100)/100:\'\';}\nfunction calcR(){var r=parseFloat($(\'c-rew2\').value)||0;var p=parseFloat(mkt.price||1);$(\'c-nxc2\').value=r&&p?(r/p).toFixed(6):\'\';}\nfunction simS(){var n=parseFloat($(\'ss-nxc\').value)||0;var fee=parseFloat($(\'ss-fee\').value)||0;var p=parseFloat(mkt.price||0);if(!n||!p){$(\'ss-res\').innerHTML=\'\';return;}var gross=n*p;var fees=gross*fee/100;var net=gross-fees;$(\'ss-res\').innerHTML=\'Brut: <b style="color:var(--text)">\'+fmt(gross,2)+\' R</b> · Frais: <b style="color:var(--red)">-\'+fmt(fees,2)+\' R</b> · <b style="color:var(--green);font-size:16px">Net: \'+fmt(net,2)+\' R</b>\';}\n\nvar _tmEnd=null;\nfunction startTimer(){var m=parseInt($(\'tm-m\').value)||0;var s=parseInt($(\'tm-s\').value)||0;var total=m*60+s;var action=$(\'tm-a\').value;if(!total)return;if(_tmInt)clearInterval(_tmInt);_tmEnd=Date.now()+total*1000;addLog(\'⏱️\',\'Minuteur: \'+action+\' dans \'+total+\'s\');_tmInt=setInterval(async function(){var rem=Math.max(0,Math.round((_tmEnd-Date.now())/1000));var el=$(\'tm-disp\');if(el)el.textContent=(\'0\'+Math.floor(rem/60)).slice(-2)+\':\'+(\'0\'+(rem%60)).slice(-2);if(rem<=0){clearInterval(_tmInt);_tmInt=null;if(el){el.textContent=\'✅\';el.style.color=\'var(--green)\';}if(action===\'stop\')setT(\'stop\');else if(action===\'up\'||action===\'down\')setT(action);else if(action===\'crash\'||action===\'moon\')scenario(action);addLog(\'⏱️\',\'Minuteur déclenché: \'+action);}},500);}\nfunction stopTimer(){if(_tmInt){clearInterval(_tmInt);_tmInt=null;var d=$(\'tm-disp\');if(d)d.textContent=\'\';}}\n\n// CONFIG\nfunction updCfg(){var el=$(\'cfg-info\');if(el)el.textContent=\'Plancher: \'+(_cfgFloor?fmt(_cfgFloor,0)+\' R\':\'non défini\')+\' · Plafond: \'+(_cfgCeil?fmt(_cfgCeil,0)+\' R\':\'non défini\');}\nfunction scheduleT(){var st=$(\'cfg-st\').value,sp=$(\'cfg-sp\').value,dir=$(\'cfg-sd\').value;if(!st||!sp){setMsg(\'cfg-sch-msg\',\'Renseigner les deux heures\',false);return;}if(_schedInt)clearInterval(_schedInt);_schedInt=setInterval(function(){var now=new Date();var cur=(\'0\'+now.getHours()).slice(-2)+\':\'+(\'0\'+now.getMinutes()).slice(-2);if(cur===st&&!tMode)setT(dir);if(cur===sp&&tMode)setT(\'stop\');},30000);setMsg(\'cfg-sch-msg\',\'✅ Programmé: \'+dir+\' \'+st+\'→\'+sp,true);addLog(\'⏰\',\'Tendance programmée \'+dir+\' \'+st+\'→\'+sp);}\n\nfunction exportHist(){var h=mkt.history||[];var b=new Blob([JSON.stringify({date:new Date().toISOString(),price:mkt.price,history:h},null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(b);a.download=\'nxc_history_\'+Date.now()+\'.json\';a.click();addLog(\'📥\',\'Export historique JSON\');}\nfunction exportStats(){var b=new Blob([JSON.stringify({date:new Date().toISOString(),market:mkt,users:_users},null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(b);a.download=\'nxc_report_\'+Date.now()+\'.json\';a.click();addLog(\'📊\',\'Export rapport JSON\');}\n\n// ALERTES\nfunction addAlert(){var price=parseFloat($(\'al-p\').value),dir=$(\'al-d\').value;if(!price)return;_alerts.push({price,dir,id:Date.now(),triggered:false});$(\'al-p\').value=\'\';renderAlerts();addLog(\'🔔\',\'Alerte: prix \'+(dir===\'above\'?\'>\':\'<\')+\' \'+fmt(price,0)+\' R\');}\nfunction removeAlert(id){_alerts=_alerts.filter(a=>a.id!==id);renderAlerts();}\nfunction renderAlerts(){var el=$(\'al-list\');if(!el)return;el.innerHTML=_alerts.length?_alerts.map(a=>\'<div style="padding:10px 12px;border-bottom:1px solid rgba(0,229,255,.05);display:flex;justify-content:space-between;align-items:center;font-size:12px"><span style="color:\'+(a.triggered?\'var(--muted)\':\'var(--gold)\')+\'">Prix \'+(a.dir===\'above\'?\'>\':\'<\')+\' \'+fmt(a.price,0)+\' R\'+(a.triggered?\' ✅\':\'\')+\'</span><button onclick="removeAlert(\'+a.id+\')" style="padding:4px 8px;border-radius:6px;background:rgba(255,61,94,.1);border:1px solid rgba(255,61,94,.3);color:var(--red);font-size:10px;cursor:pointer">✕</button></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune alerte</p>\';}\nfunction checkAlerts(p){_alerts.forEach(function(a){if(a.triggered)return;if((a.dir===\'above\'&&p>a.price)||(a.dir===\'below\'&&p<a.price)){a.triggered=true;var m=\'🔔 Prix \'+(a.dir===\'above\'?\'>\':\'<\')+\' \'+fmt(a.price,0)+\' R (actuel: \'+fmt(p,0)+\' R)\';_alHist.unshift({ts:Date.now(),msg:m});addLog(\'🔔\',m);renderAlerts();renderAlHist();if(window.Notification&&Notification.permission===\'granted\')new Notification(\'◈ Nexus NXC\',{body:m});}});}\nfunction renderAlHist(){var el=$(\'al-hist\');if(!el)return;el.innerHTML=_alHist.length?_alHist.map(a=>\'<div class="log-item"><span class="log-time">\'+fmtT(a.ts)+\'</span><span style="color:var(--gold)">\'+esc(a.msg)+\'</span></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune</p>\';}\nif(window.Notification&&Notification.permission===\'default\')setTimeout(function(){Notification.requestPermission();},3000);\n\n// ══ ÉPINGLAGE SITES (sync cross-device via serveur) ══\nvar _pinnedSites=[];\n\nasync function loadPinnedSites(){\n  try{\n    var r=await fetch(\'/admin/pinned-sites\');var d=await r.json();\n    if(d.ok){_pinnedSites=d.sites||[];renderSavedSites();}\n  }catch(e){_pinnedSites=JSON.parse(localStorage.getItem(\'nxc_pinned\')||\'[]\');}\n}\n\nasync function togglePin(url,label){\n  var idx=_pinnedSites.findIndex(s=>s.url===url);\n  if(idx>=0)_pinnedSites.splice(idx,1);\n  else _pinnedSites.push({url,label});\n  // Sauvegarder sur le serveur ET en local\n  localStorage.setItem(\'nxc_pinned\',JSON.stringify(_pinnedSites));\n  try{await fetch(\'/admin/pinned-sites\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,sites:_pinnedSites})});}catch(e){}\n  renderSavedSites();\n  addLog(\'📌\',(idx>=0?\'Désépinglé\':\'Épinglé\')+\': \'+label);\n}\n\nfunction renderPinnedBar(){\n  var el=$(\'pinned-bar\');if(!el)return;\n  if(!_pinnedSites.length){el.style.display=\'none\';return;}\n  el.style.display=\'flex\';\n  el.innerHTML=_pinnedSites.map(s=>\'<button onclick="loadSite(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" style="padding:5px 12px;background:rgba(255,176,32,.12);border:1px solid rgba(255,176,32,.3);border-radius:8px;color:var(--gold);font-size:11px;font-weight:700;cursor:pointer;white-space:nowrap">📌 \'+esc(s.label)+\'</button>\').join(\'\');\n}\n\n// ══ SAUVEGARDE / IMPORT DONNÉES GLOBALES ══\nasync function saveAllData(){\n  try{\n    var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'export\'})});\n    var d=await r.json();\n    if(!d.ok){setMsg(\'data-msg\',\'❌ Erreur export\',false);return;}\n    var blob=new Blob([JSON.stringify(d.data,null,2)],{type:\'application/json\'});\n    var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_full_backup_\'+Date.now()+\'.json\';a.click();\n    setMsg(\'data-msg\',\'✅ Backup complet téléchargé\',true);\n    addLog(\'💾\',\'Sauvegarde complète téléchargée\');\n  }catch(e){setMsg(\'data-msg\',\'❌ Erreur: \'+e.message,false);}\n}\n\nfunction importData(){\n  var input=document.createElement(\'input\');input.type=\'file\';input.accept=\'.json\';\n  input.onchange=async function(e){\n    var file=e.target.files[0];if(!file)return;\n    var text=await file.text();\n    try{\n      var data=JSON.parse(text);\n      if(!confirm(\'Importer ces données ? Cela écrasera les données actuelles du serveur.\'))return;\n      var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'import\',data:data})});\n      var res=await r.json();\n      setMsg(\'data-msg\',res.ok?\'✅ Données importées avec succès\':\'❌ \'+(res.error||\'Erreur import\'),res.ok);\n      if(res.ok){addLog(\'📥\',\'Données importées depuis fichier\');setTimeout(function(){ref();loadBank();},1000);}\n    }catch(ex){setMsg(\'data-msg\',\'❌ Fichier JSON invalide\',false);}\n  };\n  input.click();\n}\n\n// ══ IMPRESSION ══\nfunction printDashboard(){\n  var p=parseFloat(mkt.price||0);var h=mkt.history||[];\n  var hi=h.length>1?Math.max(...h.slice(-24).map(x=>x.price)):p;\n  var lo=h.length>1?Math.min(...h.slice(-24).map(x=>x.price)):p;\n  var chg=_prevP>0?((p-_prevP)/_prevP*100):0;\n  // Capturer le graphique en PNG\n  var chartImg=\'\';var cv=$(\'ch\');if(cv)chartImg=cv.toDataURL(\'image/png\');\n  var rsiImg=\'\';var rsiCv=$(\'ch-rsi\');if(rsiCv)rsiImg=rsiCv.toDataURL(\'image/png\');\n  var now=new Date().toLocaleString(\'fr-FR\');\n  var win=window.open(\'\',\'_blank\');\n  win.document.write(\'<!DOCTYPE html><html><head><meta charset="utf-8"><title>◈ Nexus NXC — Rapport \'+now+\'</title><style>*{font-family:Arial,sans-serif;box-sizing:border-box}body{background:#fff;color:#000;padding:20px;max-width:900px;margin:0 auto}.header{text-align:center;border-bottom:3px solid #000;padding-bottom:16px;margin-bottom:20px}.title{font-size:28px;font-weight:900;letter-spacing:3px}.date{font-size:12px;color:#666;margin-top:4px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}.stat{border:1px solid #ddd;border-radius:8px;padding:12px;text-align:center}.stat-val{font-size:20px;font-weight:700;margin-bottom:4px}.stat-lbl{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:#666}img{max-width:100%;border:1px solid #ddd;border-radius:8px;margin-bottom:12px}h3{margin:16px 0 8px;font-size:14px;border-bottom:1px solid #eee;padding-bottom:4px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:8px;text-align:left;border:1px solid #ddd}th{background:#f5f5f5;font-weight:700}@media print{.no-print{display:none}}</style></head><body>\');\n  win.document.write(\'<div class="header"><div class="title">◈ NEXUS NXC</div><div class="date">Rapport généré le \'+now+\'</div></div>\');\n  win.document.write(\'<div class="grid">\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(p,2)+\' R</div><div class="stat-lbl">Prix actuel</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%</div><div class="stat-lbl">Variation</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(hi,0)+\' R</div><div class="stat-lbl">Haut 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(lo,0)+\' R</div><div class="stat-lbl">Bas 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(mkt.volume24||0,0)+\' R</div><div class="stat-lbl">Volume 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(mkt.trades24||0)+\'</div><div class="stat-lbl">Trades 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+h.length+\'</div><div class="stat-lbl">Points hist.</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(_users.length||0)+\'</div><div class="stat-lbl">Utilisateurs</div></div>\');\n  win.document.write(\'</div>\');\n  if(chartImg)win.document.write(\'<h3>Historique du cours (\'+_ctRange+\' derniers points)</h3><img src="\'+chartImg+\'">\');\n  if(rsiImg)win.document.write(\'<h3>RSI (14 ticks)</h3><img src="\'+rsiImg+\'">\');\n  if(_users.length){\n    win.document.write(\'<h3>Utilisateurs</h3><table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur (R)</th></tr></thead><tbody>\');\n    _users.forEach(u=>{win.document.write(\'<tr><td>\'+esc(u.n)+\'</td><td>\'+esc(u.role)+\'</td><td>\'+fmt(u.rew,0)+\'</td><td>\'+u.nxc.toFixed(4)+\'</td><td>\'+fmt(u.val,0)+\'</td></tr>\');});\n    win.document.write(\'</tbody></table>\');\n  }\n  win.document.write(\'<h3>Derniers logs</h3><table><thead><tr><th>Heure</th><th>Action</th></tr></thead><tbody>\');\n  _log.slice(0,20).forEach(l=>{win.document.write(\'<tr><td>\'+fmtT(l.ts)+\'</td><td>\'+l.ico+\' \'+esc(l.txt)+\'</td></tr>\');});\n  win.document.write(\'</tbody></table>\');\n  win.document.write(\'</body></html>\');\n  win.document.close();\n  setTimeout(function(){win.print();},500);\n  addLog(\'🖨️\',\'Impression du tableau de bord\');\n}\n\n</script>\n</body>\n</html>\n'

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
