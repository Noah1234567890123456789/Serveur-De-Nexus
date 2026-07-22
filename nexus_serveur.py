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

NXC_MEAN_PRICE = {
    "enabled": False,
    "target": 5000.0
}

NXC_BIAS = {
    "drift": 0.0,
    "speed": 1.0
}

# Frais de transaction NXC par rôle (en %, ex: 2.5 = 2.5 %)
NXC_FEES = {
    "user":      {"buy": 2.5,  "sell": 2.5},
    "vip":       {"buy": 1.0,  "sell": 1.0},
    "moderator": {"buy": 1.5,  "sell": 1.5},
    "admin":     {"buy": 0.0,  "sell": 0.0},
    "default":   {"buy": 3.0,  "sell": 3.0}
}

# Gel d urgence du prix
NXC_FROZEN = {"active": False, "frozen_price": None, "since": None}

# Multiplicateur de volatilite (1.0 = normal, 0 = plat, 2.0 = double)
NXC_VOLATILITY_MULT = {"value": 1.0}

# Alertes prix (cote serveur — validation uniquement)
NXC_PRICE_ALERTS = []

def _freeze_watchdog():
    """Thread : maintient le prix gele si NXC_FROZEN["active"] est vrai."""
    while True:
        try:
            time.sleep(0.5)
            if NXC_FROZEN.get("active") and NXC_FROZEN.get("frozen_price"):
                NXC_MARKET["price"] = float(NXC_FROZEN["frozen_price"])
        except Exception:
            pass

threading.Thread(target=_freeze_watchdog, daemon=True).start()

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
            sigma = (0.008 + _rnd.random() * 0.015) * NXC_VOLATILITY_MULT.get("value", 1.0)
            noise = (_rnd.random() - 0.50) * sigma
            # Légère mean-reversion vers la cible (force 0.3% max) pour éviter la dérive infinie
            if NXC_MEAN_PRICE.get("enabled") and NXC_MEAN_PRICE.get("target", 0) > 0:
                target = float(NXC_MEAN_PRICE["target"])
                mr_pull = (target - p) / max(p, 1) * 0.003  # force douce 0.3%
            else:
                mr_pull = 0.0
            adj = noise + mr_pull
            if p > 80000: adj -= 0.012
            if p < 200:   adj += 0.018
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

# Anti-dérive automatique : la mean reversion est activée dès le démarrage
# pour neutraliser le biais haussier naturel de l'autotick (+0.02*sigma par tick).
# L'admin peut toujours la désactiver / changer la cible depuis le panel Contrôle.
NXC_MEAN_PRICE["enabled"] = True   # mean-reversion douce (0.3%) vers la cible

# ==============================================================================
# BLOC DE CONFIGURATION AVANCÉE — options étendues du serveur NXC
# Ajouté pour la lisibilité et la maintenance du code serveur
# ==============================================================================
#
# Résumé des paramètres globaux actifs :
#
#   NXC_MEAN_PRICE
#     enabled : bool  — activer/désactiver la correction du prix (True par défaut)
#     target  : float — cible de stabilisation en R (5000 par défaut)
#
#   NXC_BIAS
#     drift : float  — biais directionnel [-1 = full bear, +1 = full bull]
#     speed : float  — multiplicateur de vitesse de variation [0.1 – 8.0]
#
#   NXC_SOLVABILITY
#     enabled : bool — si True, bloquer les retraits si ratio < seuil
#     gesture : int  — seuil de ratio de solvabilité (%)
#
#   NXC_AUTO_CORRECT (futur)
#     Correction anti-dérive toujours active, indépendante de la mean reversion.
#     Utilise la médiane glissante sur les 20 derniers ticks.
#
# Comportement du prix NXC :
#   1. _nxc_autotick()       — tick aléatoire toutes les 15 s (biais +0.02×sigma)
#   2. _mean_reversion_tick() — rappel vers la cible toutes les 15 s (force 4 %)
#                               Suspendu quand |NXC_BIAS.drift| > 0.05
#   3. _bias_tick()          — dérive directionnelle configurable (force 5 %)
#                               Cadence : 30 s / NXC_BIAS.speed
#   4. Anti-dérive auto      — NXC_MEAN_PRICE activée dès le démarrage
#
# Persistance :
#   _save_nxc_to_db() → appelé après chaque trade et périodiquement
#   _load_nxc_from_db() → appelé au démarrage (Gunicorn + local)
#
# Sécurité :
#   Toutes les routes admin vérifient MASTER_KEY via secrets.compare_digest()
#   Les routes publiques (prix, forum) ne requièrent pas d'authentification
#
# ==============================================================================

# Constantes de simulation du marché NXC
_NXC_SIGMA_BASE    = 0.015   # Volatilité de base par tick (15 s)
_NXC_AUTOBIAS      = 0.02    # Facteur de biais haussier natif de l'autotick
_NXC_BIAS_FORCE    = 0.05    # Force du biais directionnel (_bias_tick)
_NXC_MR_FORCE      = 0.04    # Force de la mean reversion (_mean_reversion_tick)
_NXC_MR_THRESHOLD  = 0.05    # Seuil drift au-delà duquel la MR est suspendue
_NXC_TICKS_PER_H   = 240     # Nombre de ticks autotick par heure (15 s each)
_NXC_PRICE_MIN     = 50.0    # Prix plancher absolu en R
_NXC_PRICE_MAX     = 999999.0 # Prix plafond absolu en R


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
NXC_PANEL_HTML = '<!DOCTYPE html>\n<html lang="fr">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">\n<title>◈ Nexus</title>\n<style>\n*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent;touch-action:manipulation}\n:root{--bg:#02040a;--bg2:#080d1a;--bg3:#0d1428;--cyan:#00e5ff;--green:#00ff9d;--red:#ff3d5e;--gold:#ffb020;--purple:#a06bff;--muted:#5c6b8c;--text:#d4e8ff;--fg:#d4e8ff;\n.active-btn { background: var(--cyan) !important; color: #000 !important; font-weight: 700; }\n.dd-item:hover { background: var(--bg3); color: var(--cyan); }\n.card { animation: fadeInCard 0.18s ease; }\n@keyframes fadeInCard { from { opacity:0; transform:translateY(6px); } to { opacity:1; transform:none; } }\n--border:rgba(0,229,255,.12)}\nhtml,body{background:var(--bg);color:var(--text);font-family:\'Segoe UI\',system-ui,sans-serif;min-height:100dvh;overflow-x:hidden;-webkit-text-size-adjust:100%}\n\n/* LOGIN */\n#ls{position:fixed;inset:0;background:var(--bg);z-index:9999;display:flex;align-items:center;justify-content:center;padding:20px}\n.lb{background:var(--bg2);border:1px solid var(--border);border-radius:22px;padding:32px 24px;width:100%;max-width:340px;text-align:center;box-shadow:0 24px 80px rgba(0,0,0,.6)}\n.lb-logo{font-family:monospace;font-size:30px;font-weight:900;color:var(--cyan);letter-spacing:4px;margin-bottom:4px;text-shadow:0 0 20px rgba(0,229,255,.4)}\n.lb-sub{font-size:10px;color:var(--muted);margin-bottom:24px;letter-spacing:3px;text-transform:uppercase}\n.fi{width:100%;padding:13px 16px;background:var(--bg3);border:1px solid var(--border);border-radius:12px;color:var(--text);font-size:16px;margin-bottom:10px;outline:none}\n.fi:focus{border-color:var(--cyan)}\n.btn-login{width:100%;padding:14px;border-radius:12px;font-size:15px;font-weight:800;cursor:pointer;border:none;background:linear-gradient(135deg,var(--cyan),#0097b2);color:#000;letter-spacing:.5px}\n#lm{font-size:12px;color:var(--red);margin-top:8px;min-height:16px}\n\n/* HUD */\n.hud{position:fixed;top:0;left:0;right:0;height:52px;background:rgba(2,4,10,.97);border-bottom:1px solid var(--border);display:flex;align-items:center;padding:0 14px;gap:10px;z-index:100;backdrop-filter:blur(20px)}\n.hud-logo{font-family:monospace;font-size:15px;font-weight:900;color:var(--cyan);letter-spacing:2px;flex-shrink:0}\n.hud-price{font-family:monospace;font-size:12px;font-weight:800;color:var(--cyan)}\n.hud-chg{font-size:10px;font-weight:700;padding:2px 7px;border-radius:20px}\n.hud-chg.up{background:rgba(0,255,157,.12);color:var(--green);border:1px solid rgba(0,255,157,.2)}\n.hud-chg.dn{background:rgba(255,61,94,.12);color:var(--red);border:1px solid rgba(255,61,94,.2)}\n.hud-right{margin-left:auto;display:flex;align-items:center;gap:8px}\n.dot{width:7px;height:7px;border-radius:50%;background:var(--muted);flex-shrink:0}\n.dot.on{background:var(--green);box-shadow:0 0 8px var(--green);animation:dp 2s infinite}\n@keyframes dp{0%,100%{opacity:1}50%{opacity:.3}}\n.hud-time{font-family:monospace;font-size:10px;color:var(--muted)}\n\n/* TABS */\n.tabs{position:fixed;top:52px;left:0;right:0;background:rgba(2,4,10,.97);border-bottom:1px solid var(--border);display:flex;z-index:99;backdrop-filter:blur(20px);overflow-x:auto;scrollbar-width:none}\n.tabs::-webkit-scrollbar{display:none}\n.tab{flex:0 0 auto;padding:12px 18px;font-size:12px;font-weight:700;color:var(--muted);cursor:pointer;border:none;background:none;border-bottom:2px solid transparent;white-space:nowrap;transition:.15s}\n.tab.on{color:var(--cyan);border-bottom-color:var(--cyan)}\n.tab-more{flex:0 0 auto;padding:12px 16px;font-size:16px;color:var(--muted);cursor:pointer;border:none;background:none;border-bottom:2px solid transparent;margin-left:auto}\n.tab-more.on{color:var(--cyan)}\n\n/* DROPDOWN MENU */\n.dropdown{position:fixed;top:52px;right:0;background:var(--bg2);border:1px solid var(--border);border-radius:0 0 0 14px;z-index:200;min-width:180px;display:none;box-shadow:0 8px 32px rgba(0,0,0,.5)}\n.dropdown.show{display:block}\n.dd-item{padding:12px 18px;font-size:13px;font-weight:600;color:var(--muted);cursor:pointer;border-bottom:1px solid rgba(0,229,255,.06);display:flex;align-items:center;gap:10px}\n.dd-item:hover{background:rgba(0,229,255,.05);color:var(--text)}\n.dd-item:last-child{border:none}\n\n/* CONTENT */\n.content{padding-top:100px;padding-bottom:20px}\n.view{display:none;padding:14px;max-width:960px;margin:0 auto}\n.view.on{display:block}\n#view-nexus{display:none;flex-direction:column;padding:0;max-width:none}\n#view-nexus.on{display:flex}\n\n/* CARDS */\n.card{background:var(--bg2);border:1px solid var(--border);border-radius:16px;padding:16px;margin-bottom:12px}\n.card.cyan{border-color:rgba(0,229,255,.22)}.card.green{border-color:rgba(0,255,157,.22)}.card.red{border-color:rgba(255,61,94,.22)}.card.gold{border-color:rgba(255,176,32,.22)}.card.purple{border-color:rgba(160,107,255,.22)}\n.ct{font-size:9px;letter-spacing:2px;color:var(--muted);margin-bottom:12px;font-weight:700;text-transform:uppercase;display:flex;align-items:center;justify-content:space-between}\n.g4{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:12px}\n.g3{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px}\n.g2{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}\n.st{background:var(--bg3);border:1px solid rgba(0,229,255,.07);border-radius:12px;padding:12px 8px;text-align:center}\n.sv{font-family:monospace;font-size:16px;font-weight:800;color:var(--cyan);margin-bottom:2px}\n.sl{font-size:8px;color:var(--muted);letter-spacing:.8px;text-transform:uppercase}\n.sv.gold{color:var(--gold)}.sv.green{color:var(--green)}.sv.red{color:var(--red)}.sv.purple{color:var(--purple)}\n.sec{font-size:10px;color:var(--cyan);font-weight:700;letter-spacing:1px;text-transform:uppercase;margin:12px 0 6px;border-left:2px solid var(--cyan);padding-left:8px}\ninput,select,textarea{width:100%;padding:12px 13px;background:var(--bg3);border:1px solid var(--border);border-radius:11px;color:var(--text);font-size:14px;margin-bottom:8px;outline:none;font-family:inherit}\ninput:focus,select:focus{border-color:var(--cyan)}\n.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}\n.grow{flex:1;min-width:0;margin-bottom:0!important}\n.btn{padding:10px 14px;border-radius:10px;font-size:12px;font-weight:700;cursor:pointer;border:1px solid var(--border);background:var(--bg3);color:var(--text);white-space:nowrap;flex-shrink:0;transition:.15s}\n.btn:active{transform:scale(.96)}\n.btn.cyan{background:rgba(0,229,255,.1);border-color:rgba(0,229,255,.3);color:var(--cyan)}\n.btn.green{background:rgba(0,255,157,.1);border-color:rgba(0,255,157,.3);color:var(--green)}\n.btn.red{background:rgba(255,61,94,.1);border-color:rgba(255,61,94,.3);color:var(--red)}\n.btn.gold{background:rgba(255,176,32,.1);border-color:rgba(255,176,32,.3);color:var(--gold)}\n.btn.purple{background:rgba(160,107,255,.1);border-color:rgba(160,107,255,.3);color:var(--purple)}\n.btn.primary{background:linear-gradient(135deg,var(--cyan),#0097b2);color:#000;border:none}\n.btn.full{width:100%;padding:12px;font-size:13px;margin-bottom:8px;display:block}\n.ab{padding:10px 13px;border-radius:10px;font-size:12px;margin-bottom:6px}\n.ao{background:rgba(0,255,157,.07);border:1px solid rgba(0,255,157,.15);color:var(--green)}\n.aw{background:rgba(255,176,32,.07);border:1px solid rgba(255,176,32,.15);color:var(--gold)}\n.ae{background:rgba(255,61,94,.07);border:1px solid rgba(255,61,94,.15);color:var(--red)}\n.ai{background:rgba(0,229,255,.07);border:1px solid rgba(0,229,255,.15);color:var(--cyan)}\n.chart-wrap{position:relative;margin-bottom:10px}\n.ch200{height:200px}.ch150{height:150px}\n.fl-item{padding:10px 12px;border-bottom:1px solid rgba(0,229,255,.05);display:flex;align-items:center;gap:8px;font-size:12px}\n.fl-item:last-child{border:none}\n.tg{width:46px;height:25px;background:rgba(255,255,255,.07);border:1px solid var(--border);border-radius:13px;cursor:pointer;position:relative;flex-shrink:0;transition:.3s}\n.tg.on{background:rgba(0,229,255,.2);border-color:var(--cyan)}\n.tg-k{position:absolute;top:3px;left:3px;width:17px;height:17px;background:#8899aa;border-radius:50%;transition:.3s}\n.tg.on .tg-k{left:24px;background:var(--cyan)}\n.pbar{height:6px;background:rgba(0,0,0,.4);border-radius:3px;overflow:hidden;margin-top:4px}\n.pbar-fill{height:100%;border-radius:3px;background:linear-gradient(90deg,var(--cyan),var(--purple));transition:width .5s}\n.log-item{padding:7px 12px;border-bottom:1px solid rgba(0,229,255,.04);font-size:11px;display:flex;gap:8px}\n.log-time{color:var(--muted);font-family:monospace;flex-shrink:0;font-size:10px}\n.tbl-wrap{overflow-x:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)}\ntable{width:100%;border-collapse:collapse;font-size:11px}\nth,td{padding:9px 8px;text-align:left;border-bottom:1px solid rgba(0,229,255,.05)}\nth{color:var(--muted);font-size:9px;text-transform:uppercase;letter-spacing:.5px;font-weight:700}\n.ibar{background:var(--bg2);border-bottom:1px solid var(--border);padding:10px 14px;display:flex;align-items:center;gap:10px}\n.iurl{flex:1;font-size:10px;color:var(--muted);font-family:monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#nf{flex:1;border:none;width:100%;background:var(--bg)}\n.sw{position:relative}\n.sw input{padding-left:34px;margin:0}\n.sw::before{content:\'🔍\';position:absolute;left:10px;top:50%;transform:translateY(-50%);font-size:13px;pointer-events:none;z-index:1}\n.notif{position:absolute;top:6px;right:calc(50% - 12px);width:8px;height:8px;background:var(--red);border-radius:50%;display:none;border:2px solid var(--bg);animation:blink .8s ease infinite}\n@keyframes blink{0%,100%{transform:scale(1)}50%{transform:scale(1.3)}}\n@media(max-width:480px){.g4{grid-template-columns:repeat(2,1fr)}.sv{font-size:14px}.content{padding-top:96px}}\n@media(min-width:768px){.sv{font-size:20px}.ch200{height:240px}}\n</style>\n</head>\n<body>\n\n<!-- LOGIN -->\n<div id="ls">\n<div class="lb">\n<div class="lb-logo">◈ NEXUS</div>\n<div class="lb-sub">Panneau Serveur</div>\n<input id="mk" type="password" placeholder="Clé maître" class="fi" onkeydown="if(event.key===\'Enter\')doLogin()">\n<button class="btn-login" onclick="doLogin()">⚡ Connexion</button>\n<div id="lm"></div>\n</div>\n</div>\n\n<!-- HUD -->\n<div class="hud">\n<div class="hud-logo">◈ NXC</div>\n<div class="hud-price" id="hp">—</div>\n<div class="hud-chg" id="hc" style="display:none"></div>\n<div class="hud-right">\n<div class="dot" id="hd"></div>\n<span class="hud-time" id="htm">—</span>\n</div>\n</div>\n\n<!-- TABS -->\n<div class="tabs" id="main-tabs">\n<button class="tab on" onclick="go(\'marche\',this)">📈 Marché</button>\n<button class="tab" onclick="go(\'banque\',this)">🏦 Banque<span class="notif" id="nd-b"></span></button>\n<button class="tab" onclick="go(\'nexus\',this)">🌐 App</button>\n<button class="tab" onclick="go(\'admin\',this)">👑 Admin</button>\n<button class="tab-more" id="btn-more" onclick="toggleMore()">•••</button>\n</div>\n\n<!-- DROPDOWN MENU -->\n<div class="dropdown" id="dropdown">\n<div class="dd-item" onclick="go(\'trading\',null);toggleMore()">⚙️ Contrôle</div>\n<div class="dd-item" onclick="go(\'users\',null);toggleMore()">👥 Comptes</div>\n<div class="dd-item" onclick="go(\'stats\',null);toggleMore()">📊 Stats</div>\n<div class="dd-item" onclick="go(\'solv\',null);toggleMore()">🛡️ Solvabilité</div>\n<div class="dd-item" onclick="go(\'tools\',null);toggleMore()">🛠️ Outils</div>\n<div class="dd-item" onclick="go(\'log\',null);toggleMore()">📋 Journal</div>\n<div class="dd-item" onclick="go(\'config\',null);toggleMore()">⚙️ Config</div>\n<div class="dd-item" onclick="go(\'notifs\',null);toggleMore()">🔔 Alertes</div>\n<div class="dd-item" onclick="go(\'cycles\',null);toggleMore()">📅 Cycles de marché</div>\n<div class=\"dd-item\" onclick=\"go(\'prevision\',null);toggleMore()\">🔮 Prévision</div>\n<div class="dd-item" onclick="go(\'urgence\',null);toggleMore()">🚨 Urgence</div>\n<div class="dd-item" onclick="go(\'dashboard\',null);toggleMore()">📊 Dashboard</div>\n<div class="dd-item" onclick="go(\'alertesp\',null);toggleMore()">🎯 Alertes Prix</div>\n<div class="dd-item" onclick="go(\'simulateur\',null);toggleMore()">🔬 Simulateur</div>\n<div class="dd-item" onclick="go(\'avance\',null);toggleMore()">⚙️ Avancé</div>\n<div class="dd-item" onclick="go(\'historique\',null);toggleMore()">📈 Historique</div>\n<div class="dd-item" onclick="go(\'convertisseur\',null);toggleMore()">💱 Convertisseur</div>\n<div class="dd-item" onclick="go(\'evenements\',null);toggleMore()">🎲 Événements</div>\n<div class="dd-item" onclick="go(\'export\',null);toggleMore()">📤 Export</div>\n<div class="dd-item" onclick="go(\'memo\',null);toggleMore()">📌 Mémo Admin</div>\n</div>\n\n<div class="content">\n\n<!-- MARCHÉ -->\n<div class="view on" id="view-marche">\n<div class="g4">\n<div class="st"><div class="sv" id="s-p">—</div><div class="sl">Prix R/NXC</div></div>\n<div class="st"><div class="sv gold" id="s-v">—</div><div class="sl">Vol. 24h</div></div>\n<div class="st"><div class="sv green" id="s-t">—</div><div class="sl">Trades 24h</div></div>\n<div class="st"><div class="sv purple" id="s-h">—</div><div class="sl">Hist. pts</div></div>\n<div class="st"><div class="sv green" id="s-hi">—</div><div class="sl">Haut 24h</div></div>\n<div class="st"><div class="sv red" id="s-lo">—</div><div class="sl">Bas 24h</div></div>\n<div class="st"><div class="sv" id="s-var">—</div><div class="sl">Variation</div></div>\n<div class="st"><div class="sv" style="color:#ff6eb4" id="s-cap">—</div><div class="sl">Cap. marché</div></div>\n</div>\n<div class="card cyan">\n<div class="ct">◈ HISTORIQUE DU COURS\n<div style="display:flex;gap:5px">\n<button class="btn" onclick="setRange(25)" style="padding:3px 8px;font-size:9px">25</button>\n<button class="btn cyan" onclick="setRange(50)" style="padding:3px 8px;font-size:9px">50</button>\n<button class="btn" onclick="setRange(100)" style="padding:3px 8px;font-size:9px">100</button>\n</div>\n</div>\n<div class="chart-wrap ch200"><canvas id="ch"></canvas></div>\n<div style="display:flex;gap:6px;margin-top:8px;flex-wrap:wrap">\n<button class="btn gold" onclick="chObj&&chObj.zoom(1.5)">🔍+</button>\n<button class="btn gold" onclick="chObj&&chObj.zoom(0.7)">🔍−</button>\n<button class="btn" onclick="chObj&&chObj.resetZoom()">Reset</button>\n<button class="btn cyan" onclick="toggleChartType()">📊 Type</button>\n<button class="btn purple" onclick="dlChart()">⬇️ PNG</button>\n</div>\n</div>\n<div class="card"><div class="ct">◈ ALERTES MARCHÉ</div><div id="al"></div></div>\n<div class="card gold"><div class="ct">◈ RSI (14 ticks)</div><div class="chart-wrap ch150"><canvas id="ch-rsi"></canvas></div><div style="font-size:10px;color:var(--muted);margin-top:4px">RSI >70 = surachat · RSI <30 = survente</div></div>\n</div>\n\n<!-- CONTRÔLE -->\n<div class="view" id="view-trading">\n<div class="card cyan">\n<div class="ct">◈ MODIFIER LE COURS</div>\n<div class="sec">Raccourcis ±%</div>\n<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:10px">\n<button class="btn green" onclick="adjP(.05)">+5%</button>\n<button class="btn green" onclick="adjP(.02)">+2%</button>\n<button class="btn green" onclick="adjP(.01)">+1%</button>\n<button class="btn green" onclick="adjP(.005)">+0.5%</button>\n<button class="btn red" onclick="adjP(-.005)">-0.5%</button>\n<button class="btn red" onclick="adjP(-.01)">-1%</button>\n<button class="btn red" onclick="adjP(-.02)">-2%</button>\n<button class="btn red" onclick="adjP(-.05)">-5%</button>\n</div>\n<div class="sec">Prix exact</div>\n<div class="row"><input id="np" type="number" min="50" max="100000" placeholder="Prix (50–100 000)" class="grow"><button class="btn primary" onclick="setP()">✓</button></div>\n<div class="sec">Variation %</div>\n<div class="row"><input id="np-pct" type="number" placeholder="Ex: +10 ou -5" class="grow"><button class="btn cyan" onclick="setPct()">Appliquer</button></div>\n<div id="pm" style="font-size:11px;font-weight:600;min-height:14px;margin-top:4px"></div>\n</div>\n<div class="card">\n<div class="ct">◈ TENDANCE AUTO <span id="tt-timer" style="font-family:monospace;font-size:10px;color:var(--muted)"></span></div>\n<select id="ts" style="margin-bottom:8px">\n<option value="0.001">Ultra lent 0.1%</option>\n<option value="0.002">Très lent 0.2%</option>\n<option value="0.005" selected>Lent 0.5%</option>\n<option value="0.01">Moyen 1%</option>\n<option value="0.02">Rapide 2%</option>\n<option value="0.05">Très rapide 5%</option>\n<option value="0.1">Extrême 10%</option>\n</select>\n<select id="ti" style="margin-bottom:8px">\n<option value="5000">5s</option>\n<option value="12000" selected>12s</option>\n<option value="30000">30s</option>\n<option value="60000">1min</option>\n</select>\n<div class="sec">Amplitude de variation par tick</div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="noise-slider" type="range" min="1" max="10" value="4" oninput="updateNoise(this.value)" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--cyan)">\n<span id="noise-val" style="color:var(--cyan);font-weight:700;font-size:13px;width:48px;text-align:right;flex-shrink:0">0.4%</span>\n</div>\n<div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:8px">\n<button class="btn green" onclick="setT(\'up\')">📈 Hausse</button>\n<button class="btn red" onclick="setT(\'down\')">📉 Baisse</button>\n<button class="btn purple" onclick="setT(\'random\')">🎲 Aléatoire</button>\n<button class="btn" onclick="setT(\'stop\')" style="color:var(--muted)">⏸ Stop</button>\n</div>\n<div id="tst" style="font-size:12px;color:var(--muted);font-weight:600;padding:8px;background:var(--bg3);border-radius:8px;text-align:center">⏸ Arrêté</div>\n</div>\n<div class="card" style="border-color:rgba(0,229,255,.25)">\n<div class="ct" style="color:var(--cyan)">◈ FRAIS DE TRANSACTION NXC</div>\n<div style="font-size:11px;color:var(--muted);margin-bottom:10px">\n  Frais prélevés à l\'achat et à la vente de NXC, selon le rôle de l\'utilisateur.\n</div>\n<!-- Appliquer à tous -->\n<div style="background:var(--bg3);border-radius:10px;padding:10px;margin-bottom:10px">\n  <div style="font-weight:700;font-size:11px;color:var(--gold);margin-bottom:6px">⚡ Appliquer à TOUS les rôles</div>\n  <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">\n    <input id="fee-all-buy"  type="number" min="0" max="50" step="0.1" placeholder="Achat %" style="width:90px;padding:6px 8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:12px">\n    <input id="fee-all-sell" type="number" min="0" max="50" step="0.1" placeholder="Vente %" style="width:90px;padding:6px 8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:12px">\n    <button class="btn gold" onclick="setAllFees()" style="font-size:11px">Tout mettre à jour</button>\n  </div>\n</div>\n<!-- Tableau par rôle -->\n<table style="width:100%;border-collapse:collapse;font-size:12px">\n<thead><tr style="color:var(--muted);font-size:10px;text-transform:uppercase;border-bottom:1px solid var(--border)">\n  <th style="text-align:left;padding:6px 4px">Rôle</th>\n  <th style="text-align:center;padding:6px 4px">Achat (%)</th>\n  <th style="text-align:center;padding:6px 4px">Vente (%)</th>\n  <th style="text-align:center;padding:6px 4px">Action</th>\n</tr></thead>\n<tbody id="fees-tbody">\n  <tr><td colspan="4" style="color:var(--muted);font-size:11px;padding:10px;text-align:center">Chargement…</td></tr>\n</tbody>\n</table>\n<div id="fees-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:8px"></div>\n</div>\n\n<div class="card gold">\n<div class="ct">◈ SCÉNARIOS</div>\n<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">\n<button class="btn gold" onclick="scenario(\'crash\')">💥 Crash −30%</button>\n<button class="btn gold" onclick="scenario(\'moon\')">🚀 Moon +30%</button>\n<button class="btn gold" onclick="scenario(\'volatile\')">⚡ Volatil</button>\n<button class="btn gold" onclick="scenario(\'stable\')">😴 Stabiliser</button>\n<button class="btn gold" onclick="scenario(\'ath\')">🏆 ATH</button>\n<button class="btn gold" onclick="scenario(\'floor\')">🛑 Plancher 200R</button>\n</div>\n</div>\n<div class="card green">\n<div class="ct">◈ COURS NORMAL (PLANCHER + PLAFOND)</div>\n<div class="row" style="margin-bottom:8px">\n<input id="t-floor" type="number" placeholder="Plancher min (R)" class="grow">\n<button class="btn green" onclick="setFloor()">✓ Plancher</button>\n<button class="btn red" onclick="_cfgFloor=null;updFloorDisplay()" style="padding:10px 12px">✕</button>\n</div>\n<div class="row" style="margin-bottom:8px">\n<input id="t-ceil" type="number" placeholder="Plafond max (R)" class="grow">\n<button class="btn green" onclick="setCeil()">✓ Plafond</button>\n<button class="btn red" onclick="_cfgCeil=null;updFloorDisplay()" style="padding:10px 12px">✕</button>\n</div>\n<div id="floor-display" style="font-size:11px;padding:8px;background:var(--bg3);border-radius:8px;color:var(--muted)">Plancher: non défini · Plafond: non défini</div>\n<button class="btn green full" style="margin-top:8px" onclick="setNormalMode()">📊 Activer cours normal</button>\n<div style="font-size:10px;color:var(--muted);margin-top:4px">Le prix fluctue librement mais reste entre le plancher et le plafond</div>\n</div>\n<div class="card" style="border-color:#a855f7;">\n<div class="ct" style="color:#a855f7;">◈ PRIX MOYEN (MEAN REVERSION)</div>\n<div style="display:flex;align-items:center;gap:14px;margin-bottom:10px;">\n<div id="mp-tg" class="tg" onclick="toggleMp()"><div class="tg-k"></div></div>\n<span id="mp-lbl" style="color:var(--muted);font-size:13px;">⏸ Désactivé</span>\n</div>\n<label style="font-size:12px;color:var(--muted);margin-bottom:4px;display:block;">Prix moyen cible (R)</label>\n<input id="mp-target" type="number" min="50" max="100000" step="1" placeholder="Ex: 5000">\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px;">\n<button class="btn" onclick="document.getElementById(\'mp-target\').value=Math.round(mkt.price||5000);saveMeanPrice();">Prix actuel</button>\n<button class="btn" onclick="document.getElementById(\'mp-target\').value=1000;saveMeanPrice();">1 000 R</button>\n<button class="btn" onclick="document.getElementById(\'mp-target\').value=5000;saveMeanPrice();">5 000 R</button>\n<button class="btn" onclick="document.getElementById(\'mp-target\').value=10000;saveMeanPrice();">10 000 R</button>\n</div>\n<button class="btn full" onclick="saveMeanPrice()">💾 Sauvegarder</button>\n<div id="mp-msg" style="margin-top:6px;font-size:12px;"></div>\n</div>\n<style>\n.bc-track{position:relative;height:10px;background:linear-gradient(90deg,#ff3d5e 0%,rgba(255,255,255,0.06) 50%,#00ff9d 100%);border-radius:5px;margin:16px 0 8px;cursor:pointer;box-shadow:inset 0 0 6px rgba(0,0,0,.3)}\n.bc-thumb{position:absolute;top:50%;width:26px;height:26px;background:#fff;border-radius:50%;transform:translate(-50%,-50%);box-shadow:0 0 14px rgba(255,255,255,.7);cursor:grab;z-index:2;transition:background-color .25s,box-shadow .25s;border:2px solid rgba(255,255,255,.3)}\n.bc-thumb:active{cursor:grabbing;transform:translate(-50%,-50%) scale(1.15)}\n.bc-zones{display:flex;justify-content:space-between;font-size:9px;color:var(--muted);margin-top:4px;letter-spacing:.5px;text-transform:uppercase}\n.bc-ind{text-align:center;padding:11px;border-radius:10px;margin-bottom:10px;font-weight:800;font-size:13px;letter-spacing:.5px;transition:all .4s}\n.bc-ind.bear{background:rgba(255,61,94,.14);border:1px solid rgba(255,61,94,.35);color:var(--red);text-shadow:0 0 10px var(--red)}\n.bc-ind.bull{background:rgba(0,255,157,.14);border:1px solid rgba(0,255,157,.35);color:var(--green);text-shadow:0 0 10px var(--green)}\n.bc-ind.neutral{background:rgba(0,229,255,.06);border:1px solid var(--border);color:var(--muted)}\n.spd-track{position:relative;height:6px;background:rgba(255,255,255,.06);border-radius:3px;margin:12px 0 6px;cursor:pointer}\n.spd-fill{height:100%;border-radius:3px;background:linear-gradient(90deg,var(--cyan),var(--purple));transition:width .15s}\n.spd-thumb{position:absolute;top:50%;width:18px;height:18px;background:var(--cyan);border-radius:50%;transform:translate(-50%,-50%);box-shadow:0 0 8px var(--cyan);cursor:grab;z-index:2}\n.spd-thumb:active{cursor:grabbing}\n#bc-info-modal{display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:9999;align-items:center;justify-content:center}\n#bc-info-modal.show{display:flex}\n.bc-info-box{background:#1a1a2e;border:1px solid rgba(160,107,255,.4);border-radius:16px;padding:24px;max-width:320px;color:#e0e0e0;font-size:13px;line-height:1.6;position:relative}\n.bc-info-box h3{margin:0 0 12px;color:#a06bff;font-size:15px}\n.bc-info-close{position:absolute;top:10px;right:14px;cursor:pointer;font-size:18px;color:var(--muted);background:none;border:none}\n</style>\n\n<div id="bc-info-modal" onclick="if(event.target===this)this.classList.remove(\'show\')">\n  <div class="bc-info-box">\n    <button class="bc-info-close" onclick="document.getElementById(\'bc-info-modal\').classList.remove(\'show\')">✕</button>\n    <h3>⚡ Dynamique du Prix</h3>\n    <b>Direction (curseur haut)</b><br>\n    Pousse le prix vers le haut (Bull 🚀) ou vers le bas (Bear 🐻). Neutre = variation équilibrée sans dérive.<br><br>\n    <b>Vitesse (curseur bas)</b><br>\n    Multiplie la fréquence de variation. ×1 = normal (tick toutes les 30s), ×8 = extrême (tick toutes les 4s).<br><br>\n    <b>Note</b> : quand un biais est actif, la correction anti-dérive est suspendue automatiquement.\n  </div>\n</div>\n\n<div class="card" style="background:linear-gradient(135deg,rgba(0,229,255,.03) 0%,rgba(160,107,255,.07) 50%,rgba(255,61,94,.03) 100%);border-color:rgba(160,107,255,.28);position:relative;overflow:hidden">\n<div style="position:absolute;top:-50px;right:-50px;width:140px;height:140px;background:radial-gradient(circle,rgba(160,107,255,.15),transparent 70%);pointer-events:none"></div>\n<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px">\n  <div class="ct" style="color:#a06bff;font-size:10px;letter-spacing:3px;margin:0">⚡ DYNAMIQUE DU PRIX</div>\n  <button onclick="document.getElementById(\'bc-info-modal\').classList.add(\'show\')" style="width:22px;height:22px;border-radius:50%;background:rgba(160,107,255,.15);border:1px solid rgba(160,107,255,.4);color:#a06bff;font-size:11px;font-weight:700;cursor:pointer;display:flex;align-items:center;justify-content:center;line-height:1">i</button>\n</div>\n<div style="font-size:10px;color:var(--muted);margin-bottom:8px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase">↕ Direction du marché</div>\n<div id="bc-ind" class="bc-ind neutral">⚖ NEUTRE — variation équilibrée</div>\n<div class="bc-track" id="bc-track" onmousedown="startBiasDrag(event)" ontouchstart="startBiasDrag(event)">\n  <div class="bc-thumb" id="bc-thumb" style="left:50%"></div>\n</div>\n<div class="bc-zones"><span style="color:var(--red)">🐻 Bear</span><span>⚖ Neutre</span><span style="color:var(--green)">🚀 Bull</span></div>\n<div style="display:flex;gap:6px;margin-top:10px;margin-bottom:4px">\n<button class="btn red" style="flex:1;font-size:11px" onclick="setBias(-1)">🐻 Full Bear</button>\n<button class="btn" style="flex:1;font-size:11px" onclick="setBias(0)">⚖ Neutre</button>\n<button class="btn green" style="flex:1;font-size:11px" onclick="setBias(1)">🚀 Full Bull</button>\n</div>\n<div style="height:1px;background:rgba(255,255,255,.05);margin:14px 0"></div>\n<div style="font-size:10px;color:var(--muted);margin-bottom:8px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase">⏱ Vitesse de variation</div>\n<div id="spd-ind" style="text-align:center;font-family:monospace;font-size:13px;font-weight:800;color:var(--muted);margin-bottom:6px">× 1.00  —  Normal</div>\n<div class="spd-track" id="spd-track" onmousedown="startSpdDrag(event)" ontouchstart="startSpdDrag(event)">\n  <div class="spd-fill" id="spd-fill" style="width:50%"></div>\n  <div class="spd-thumb" id="spd-thumb" style="left:50%"></div>\n</div>\n<div style="display:flex;justify-content:space-between;font-size:9px;color:var(--muted);margin-bottom:10px"><span>🐌</span><span>Lent</span><span>Normal</span><span>⚡</span><span>🔥</span></div>\n<div style="display:flex;gap:4px;flex-wrap:wrap">\n<button class="btn" style="flex:1;min-width:0;font-size:10px;padding:8px 4px" onclick="setSpd(0.25)">×¼</button>\n<button class="btn" style="flex:1;min-width:0;font-size:10px;padding:8px 4px" onclick="setSpd(0.5)">×½</button>\n<button class="btn cyan" style="flex:1;min-width:0;font-size:10px;padding:8px 4px" onclick="setSpd(1)">×1</button>\n<button class="btn gold" style="flex:1;min-width:0;font-size:10px;padding:8px 4px" onclick="setSpd(2)">×2</button>\n<button class="btn red" style="flex:1;min-width:0;font-size:10px;padding:8px 4px" onclick="setSpd(4)">×4</button>\n<button class="btn" style="flex:1;min-width:0;font-size:10px;padding:8px 4px;background:rgba(255,61,94,.2);border-color:rgba(255,61,94,.5);color:var(--red)" onclick="setSpd(8)">×8🔥</button>\n</div>\n<div id="bias-msg" style="margin-top:8px;font-size:11px;text-align:center;min-height:14px"></div>\n</div>\n<div class="card"><div class="ct">◈ RESET</div>\n<button class="btn full" style="color:var(--gold);border-color:rgba(255,176,32,.3);background:rgba(255,176,32,.06)" onclick="resetH()">🔄 Reset historique</button>\n<button class="btn full red" onclick="if(confirm(\'Reset complet ?\'))resetH()">⚠️ Reset complet</button>\n</div>\n</div>\n\n<!-- BANQUE -->\n<div class="view" id="view-banque">\n<div class="g4">\n<div class="st"><div class="sv" style="color:#00b4d8;font-size:14px" id="bk-r">—</div><div class="sl">Réserves</div></div>\n<div class="st"><div class="sv gold" style="font-size:14px" id="bk-i">—</div><div class="sl">Total entré</div></div>\n<div class="st"><div class="sv red" style="font-size:14px" id="bk-o">—</div><div class="sl">Total sorti</div></div>\n<div class="st"><div class="sv green" style="font-size:14px" id="bk-rt">—</div><div class="sl">Ratio</div></div>\n<div class="st"><div class="sv purple" style="font-size:14px" id="bk-nx">—</div><div class="sl">NXC émis</div></div>\n<div class="st"><div class="sv" style="font-size:14px;color:#4ea8de" id="bk-vx">—</div><div class="sl">Val. stock</div></div>\n<div class="st"><div class="sv" style="font-size:14px" id="bk-bn">—</div><div class="sl">Bénéfice</div></div>\n<div class="st"><div class="sv" style="font-size:14px;color:#ff6eb4" id="bk-fl">—</div><div class="sl">Nb flux</div></div>\n</div>\n<div class="card cyan">\n<div class="ct">◈ OPÉRATIONS</div>\n<div class="row" style="margin-bottom:8px">\n<input id="bk-amt" type="number" placeholder="Montant (R)" class="grow">\n<button class="btn green" onclick="bankOp(\'in\')">+ Injecter</button>\n<button class="btn red" onclick="bankOp(\'out\')">− Retirer</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px">\n<button class="btn cyan" onclick="setAmt(100)" style="font-size:11px;padding:6px 10px">100</button>\n<button class="btn cyan" onclick="setAmt(500)" style="font-size:11px;padding:6px 10px">500</button>\n<button class="btn cyan" onclick="setAmt(1000)" style="font-size:11px;padding:6px 10px">1 000</button>\n<button class="btn cyan" onclick="setAmt(5000)" style="font-size:11px;padding:6px 10px">5 000</button>\n<button class="btn cyan" onclick="setAmt(10000)" style="font-size:11px;padding:6px 10px">10 000</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap">\n<button class="btn gold" onclick="bankResetHist()" style="font-size:11px">🗑️ Reset hist.</button>\n<button class="btn red" onclick="bankResetAll()" style="font-size:11px">💥 Reset complet</button>\n<button class="btn purple" onclick="loadBank()" style="font-size:11px">🔄 Actualiser</button>\n<button class="btn" onclick="exportFlux()" style="font-size:11px">📊 CSV</button>\n</div>\n<div id="bk-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:8px"></div>\n</div>\n<div class="card">\n<div class="ct">◈ FLUX\n<div style="display:flex;gap:4px">\n<button class="btn cyan" id="fl-all" onclick="filterFlux(\'all\')" style="padding:3px 7px;font-size:9px">Tous</button>\n<button class="btn" id="fl-in" onclick="filterFlux(\'IN\')" style="padding:3px 7px;font-size:9px">Entrées</button>\n<button class="btn" id="fl-out" onclick="filterFlux(\'OUT\')" style="padding:3px 7px;font-size:9px">Sorties</button>\n</div>\n</div>\n<div id="bk-flux" style="max-height:220px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"></div>\n</div>\n<div class="card red">\n<div class="ct" style="color:var(--red)">⚠️ TENTATIVES ÉCHOUÉES <span id="fails-ct" style="display:none;background:var(--red);color:#000;border-radius:20px;padding:1px 7px;font-size:9px"></span></div>\n<div id="bk-fails" style="max-height:220px;overflow-y:auto"></div>\n</div>\n<div class="card" style="margin-top:12px">\n<div class="ct" style="font-size:10px;letter-spacing:2px;margin-bottom:10px">📈 PRIX NXC — HISTORIQUE</div>\n<svg id="bk-svg" viewBox="0 0 320 90" style="width:100%;height:auto;display:block;background:rgba(0,0,0,.15);border-radius:8px"></svg>\n<div style="display:flex;justify-content:space-between;font-size:9px;color:var(--muted);margin-top:5px">\n<span id="bk-svg-lo">-</span><span id="bk-svg-cur" style="color:var(--cyan);font-weight:700">-</span><span id="bk-svg-hi">-</span>\n</div>\n</div>\n<script>\nfunction drawBkGraph(){\n  var hist=(mkt&&mkt.history)||[];\n  var svg=document.getElementById(\'bk-svg\');\n  if(!svg)return;\n  if(hist.length<2){svg.innerHTML=\'<text x="50%" y="50%" text-anchor="middle" fill="#555" font-size="11">Pas encore de données</text>\';return;}\n  var pts=hist.slice(-80);\n  var prices=pts.map(function(x){return parseFloat(x.price||x);});\n  var mn=Math.min.apply(null,prices);\n  var mx=Math.max.apply(null,prices);\n  var rng=mx-mn||1;\n  var W=320,H=90,P=10;\n  function sx(i){return P+i/(pts.length-1)*(W-2*P);}\n  function sy(p){return P+(1-(p-mn)/rng)*(H-2*P);}\n  var d=\'\';\n  for(var i=0;i<prices.length;i++){d+=(i===0?\'M\':\'L\')+sx(i).toFixed(1)+\' \'+sy(prices[i]).toFixed(1);}\n  var fill=d+\'L\'+(W-P)+\' \'+(H-P)+\'L\'+P+\' \'+(H-P)+\'Z\';\n  var cur=prices[prices.length-1];\n  var col=cur>=prices[0]?\'#00ff9d\':\'#ff3d5e\';\n  svg.innerHTML=\'<defs><linearGradient id="bkG" x1="0" x2="0" y1="0" y2="1">\'\n    +\'<stop offset="0%" stop-color="\'+col+\'" stop-opacity="0.35"/>\'\n    +\'<stop offset="100%" stop-color="\'+col+\'" stop-opacity="0.02"/>\'\n    +\'</linearGradient></defs>\'\n    +\'<path d="\'+fill+\'" fill="url(#bkG)"/>\'\n    +\'<path d="\'+d+\'" stroke="\'+col+\'" stroke-width="1.5" fill="none" stroke-linejoin="round"/>\'\n    +\'<circle cx="\'+(W-P)+\'" cy="\'+sy(cur)+\'" r="3" fill="\'+col+\'"/>\';\n  var lo=document.getElementById(\'bk-svg-lo\');\n  var hi=document.getElementById(\'bk-svg-hi\');\n  var cc=document.getElementById(\'bk-svg-cur\');\n  if(lo)lo.textContent=fmt(mn,0)+\' R\';\n  if(hi)hi.textContent=fmt(mx,0)+\' R\';\n  if(cc)cc.textContent=fmt(cur,0)+\' R\';\n}\n</script>\n</div>\n\n<!-- APP -->\n<div class="view" id="view-nexus">\n<div style="padding:12px;background:var(--bg2);border-bottom:1px solid var(--border)">\n<div id="pinned-bar" style="display:none;gap:6px;flex-wrap:wrap;margin-bottom:8px;padding:6px;background:rgba(255,176,32,.05);border:1px solid rgba(255,176,32,.15);border-radius:10px"></div>\n<div class="row" style="margin-bottom:8px">\n<input id="iframe-in" type="url" placeholder="https://..." class="grow" onkeydown="if(event.key===\'Enter\')goUrl()">\n<button class="btn primary" onclick="goUrl()">▶</button>\n</div>\n<div id="saved-sites" style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px"></div>\n<div class="row">\n<input id="site-lbl" placeholder="Nom" style="flex:1;margin:0;font-size:12px;padding:8px 10px">\n<button class="btn gold" onclick="saveSite()" style="font-size:11px">💾 Sauver</button>\n<button class="btn cyan" onclick="reloadF()" style="font-size:11px">🔄</button>\n<button class="btn" onclick="openNewTab()" style="font-size:11px">↗</button>\n</div>\n</div>\n<div class="ibar">\n<span style="color:var(--cyan);font-size:12px;font-weight:800" id="if-title">◈ App</span>\n<span class="iurl" id="if-url">—</span>\n</div>\n<iframe id="nf" src="about:blank" allow="clipboard-write" style="flex:1;border:none;width:100%;min-height:calc(100dvh - 200px)"></iframe>\n</div>\n\n<!-- ADMIN -->\n<div class="view" id="view-admin">\n<div class="card cyan">\n<div class="ct">◈ STATISTIQUES SERVEUR EN TEMPS RÉEL</div>\n<div class="g4" id="adm-stats">\n<div class="st"><div class="sv" id="adm-price">—</div><div class="sl">Prix actuel</div></div>\n<div class="st"><div class="sv gold" id="adm-vol">—</div><div class="sl">Vol. 24h</div></div>\n<div class="st"><div class="sv green" id="adm-trades">—</div><div class="sl">Trades</div></div>\n<div class="st"><div class="sv purple" id="adm-users">—</div><div class="sl">Utilisateurs</div></div>\n<div class="st"><div class="sv" style="color:#00b4d8" id="adm-res">—</div><div class="sl">Réserves</div></div>\n<div class="st"><div class="sv gold" id="adm-nxc">—</div><div class="sl">NXC émis</div></div>\n<div class="st"><div class="sv green" id="adm-fails">—</div><div class="sl">Tentatives échouées</div></div>\n<div class="st"><div class="sv" id="adm-hist">—</div><div class="sl">Points hist.</div></div>\n</div>\n<button class="btn cyan" onclick="refreshAdminStats()" style="width:100%;margin-top:4px;padding:10px">🔄 Actualiser tout</button>\n</div>\n<div class="card green"><div class="ct">◈ SAUVEGARDE ET IMPORT DES DONNÉES</div><button class="btn green full" onclick="saveAllData()">💾 Sauvegarder toutes les données (JSON)</button><button class="btn cyan full" onclick="importData()">📥 Importer depuis un fichier JSON</button><button class="btn purple full" onclick="printDashboard()">🖨️ Imprimer le tableau de bord</button><div id="data-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:4px"></div></div>\n<div class="card gold">\n<div class="ct">◈ DONNER DES REWARDS À UN UTILISATEUR</div>\n<div class="row" style="margin-bottom:8px">\n<select id="rw-u" class="grow" style="margin:0"><option value="">Utilisateur...</option></select>\n<input id="rw-amt" type="number" placeholder="Montant" style="width:100px;margin:0;flex-shrink:0">\n<button class="btn gold" onclick="giveRewards()">💰 Donner</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:4px">\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=50" style="font-size:11px;padding:6px 10px">50</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=100" style="font-size:11px;padding:6px 10px">100</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=500" style="font-size:11px;padding:6px 10px">500</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=1000" style="font-size:11px;padding:6px 10px">1 000</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=5000" style="font-size:11px;padding:6px 10px">5 000</button>\n</div>\n<div id="rw-msg" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n<div class="card purple">\n<div class="ct">◈ CHANGER LE RÔLE D\'UN UTILISATEUR</div>\n<div class="row">\n<select id="role-u" class="grow" style="margin:0"><option value="">Utilisateur...</option></select>\n<select id="role-v" style="width:auto;margin:0;flex-shrink:0;padding:12px 8px">\n<option value="user">user</option>\n<option value="admin">admin</option>\n<option value="moderator">moderator</option>\n<option value="vip">vip</option>\n</select>\n<button class="btn purple" onclick="changeRole()">✓</button>\n</div>\n<div id="role-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:6px"></div>\n</div>\n<div class="card">\n<div class="ct">◈ LISTE COMPLÈTE DES UTILISATEURS</div>\n<div class="sw" style="margin-bottom:8px"><input id="adm-q" placeholder="Rechercher..." oninput="filterAdmUsers()"></div>\n<div class="tbl-wrap">\n<table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur</th></tr></thead>\n<tbody id="adm-ut"></tbody></table>\n</div>\n</div>\n<div class="card red">\n<div class="ct">◈ ACTIONS DE MAINTENANCE</div>\n<button class="btn full" style="color:var(--gold);border-color:rgba(255,176,32,.3);background:rgba(255,176,32,.06)" onclick="pruneHistory()">✂️ Réduire historique NXC (100 pts)</button>\n<button class="btn full red" onclick="resetAllTrades()">🗑️ Reset trades 24h</button>\n<button class="btn full" style="color:var(--cyan);border-color:rgba(0,229,255,.3);background:rgba(0,229,255,.06)" onclick="backupDB()">💾 Backup base de données JSON</button>\n<button class="btn full" style="color:var(--purple);border-color:rgba(160,107,255,.3);background:rgba(160,107,255,.06)" onclick="pingServer()">📡 Ping serveur</button>\n<div id="maint-msg" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n<div class="card purple">\n<div class="ct">◈ LOGS SYSTÈME</div>\n<div style="display:flex;gap:6px;margin-bottom:8px">\n<button class="btn purple" onclick="renderLog()" style="font-size:11px">🔄 Actualiser</button>\n<button class="btn red" onclick="_log=[];renderLog()" style="font-size:11px">🗑️ Vider</button>\n</div>\n<div id="log-list" style="max-height:250px;overflow-y:auto;border-radius:10px;border:1px solid rgba(160,107,255,.1)">\n<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun log</p>\n</div>\n</div>\n</div>\n\n<!-- UTILISATEURS -->\n<div class="view" id="view-users">\n<div class="g3">\n<div class="st"><div class="sv" id="u-total">—</div><div class="sl">Comptes</div></div>\n<div class="st"><div class="sv gold" id="u-admins">—</div><div class="sl">Admins</div></div>\n<div class="st"><div class="sv green" id="u-rew">—</div><div class="sl">Total rewards</div></div>\n</div>\n<div class="card">\n<div class="ct">◈ UTILISATEURS\n<div style="display:flex;gap:4px">\n<button class="btn cyan" onclick="sortU(\'rew\')" style="padding:3px 7px;font-size:9px">Rewards</button>\n<button class="btn" onclick="sortU(\'nxc\')" style="padding:3px 7px;font-size:9px">NXC</button>\n<button class="btn" onclick="sortU(\'name\')" style="padding:3px 7px;font-size:9px">A-Z</button>\n</div>\n</div>\n<div class="sw" style="margin-bottom:8px"><input id="us-q" placeholder="Rechercher..." oninput="filterU()"></div>\n<div class="tbl-wrap">\n<table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur R</th></tr></thead>\n<tbody id="ut"></tbody></table>\n</div>\n<div id="us-msg" style="font-size:11px;color:var(--muted);margin-top:8px;text-align:center"></div>\n</div>\n</div>\n\n<!-- STATS -->\n<div class="view" id="view-stats">\n<div class="card purple"><div class="ct">◈ VOLUME 24H</div><div class="chart-wrap ch150"><canvas id="ch-vol"></canvas></div></div>\n<div class="card gold"><div class="ct">◈ REWARDS PAR UTILISATEUR</div><div id="rew-bars"></div></div>\n<div class="card"><div class="ct">◈ SANTÉ DU MARCHÉ</div><div class="g2" id="health-grid"></div></div>\n</div>\n\n<!-- SOLVABILITÉ -->\n<div class="view" id="view-solv">\n<div class="card">\n<div class="ct">◈ SOLVABILITÉ</div>\n<div style="display:flex;align-items:center;gap:14px;padding:14px;background:var(--bg3);border-radius:12px;margin-bottom:12px;cursor:pointer" onclick="toggleSolv()">\n<div class="tg" id="stg"><div class="tg-k"></div></div>\n<div id="sl" style="font-size:14px;font-weight:700;color:var(--muted)">Désactivée</div>\n</div>\n<div class="row" style="margin-bottom:8px">\n<span style="font-size:12px;color:var(--muted);white-space:nowrap;flex-shrink:0">Geste commercial :</span>\n<input id="sg" type="number" value="50" class="grow">\n<button class="btn primary" onclick="saveSolv()">Sauver</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:6px">\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=10" style="font-size:11px">10R</button>\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=50" style="font-size:11px">50R</button>\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=100" style="font-size:11px">100R</button>\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=500" style="font-size:11px">500R</button>\n</div>\n<div id="sm" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n</div>\n\n<!-- OUTILS -->\n<div class="view" id="view-tools">\n<div class="card cyan">\n<div class="ct">◈ CALCULATRICE NXC ↔ REWARDS</div>\n<div class="row" style="margin-bottom:8px">\n<input id="c-nxc" type="number" placeholder="NXC" class="grow" oninput="calcN()">\n<span style="color:var(--muted);font-size:18px">→</span>\n<input id="c-rew" type="number" placeholder="Rewards R" class="grow" readonly style="background:rgba(0,229,255,.05)">\n</div>\n<div class="row">\n<input id="c-rew2" type="number" placeholder="Rewards R" class="grow" oninput="calcR()">\n<span style="color:var(--muted);font-size:18px">→</span>\n<input id="c-nxc2" type="number" placeholder="NXC" class="grow" readonly style="background:rgba(0,229,255,.05)">\n</div>\n</div>\n<div class="card gold">\n<div class="ct">◈ SIMULATEUR DE VENTE</div>\n<div class="row" style="margin-bottom:8px">\n<input id="ss-nxc" type="number" placeholder="NXC à vendre" class="grow" oninput="simS()">\n<input id="ss-fee" type="number" placeholder="Frais %" value="0" style="width:90px;margin:0;flex-shrink:0" oninput="simS()">\n</div>\n<div id="ss-res" style="padding:12px;background:var(--bg3);border-radius:10px;min-height:44px;font-size:13px"></div>\n</div>\n<div class="card purple">\n<div class="ct">◈ MINUTEUR ADMIN</div>\n<div class="row" style="margin-bottom:8px">\n<input id="tm-m" type="number" placeholder="Min" value="5" class="grow">\n<input id="tm-s" type="number" placeholder="Sec" value="0" class="grow">\n<select id="tm-a" style="flex:1;margin:0;font-size:12px">\n<option value="stop">Arrêter tendance</option>\n<option value="up">Lancer hausse</option>\n<option value="down">Lancer baisse</option>\n<option value="crash">Crash -30%</option>\n<option value="moon">Moon +30%</option>\n</select>\n</div>\n<button class="btn cyan full" onclick="startTimer()">⏱️ Démarrer</button>\n<button class="btn full" style="color:var(--muted)" onclick="stopTimer()">✕ Annuler</button>\n<div id="tm-disp" style="font-family:monospace;font-size:36px;font-weight:900;color:var(--cyan);text-align:center;padding:10px;min-height:56px"></div>\n</div>\n<div class="card green">\n<div class="ct">◈ PING SERVEUR</div>\n<button class="btn green full" onclick="pingServer()">📡 Tester</button>\n<div id="ping-res" style="font-size:13px;font-weight:700;text-align:center;padding:10px;min-height:36px"></div>\n</div>\n</div>\n\n<!-- JOURNAL -->\n<div class="view" id="view-log">\n<div class="card">\n<div class="ct">◈ JOURNAL ADMIN <button class="btn red" onclick="_log=[];renderLog()" style="padding:3px 8px;font-size:9px">Vider</button></div>\n<div id="log-list2" style="max-height:500px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)">\n<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun log</p>\n</div>\n</div>\n</div>\n\n<!-- CONFIG -->\n<div class="view" id="view-config">\n<div class="card purple">\n<div class="ct">◈ PLANCHER / PLAFOND AUTOMATIQUES</div>\n<div class="row" style="margin-bottom:8px">\n<input id="cfg-fl" type="number" placeholder="Plancher min (R)" class="grow">\n<button class="btn purple" onclick="_cfgFloor=parseFloat(document.getElementById(\'cfg-fl\').value)||null;updCfg();updFloorDisplay()">✓ Plancher</button>\n<button class="btn red" onclick="_cfgFloor=null;updCfg();updFloorDisplay()" style="padding:10px">✕</button>\n</div>\n<div class="row" style="margin-bottom:8px">\n<input id="cfg-cl" type="number" placeholder="Plafond max (R)" class="grow">\n<button class="btn purple" onclick="_cfgCeil=parseFloat(document.getElementById(\'cfg-cl\').value)||null;updCfg();updFloorDisplay()">✓ Plafond</button>\n<button class="btn red" onclick="_cfgCeil=null;updCfg();updFloorDisplay()" style="padding:10px">✕</button>\n</div>\n<div id="cfg-info" style="font-size:11px;color:var(--muted);padding:8px;background:var(--bg3);border-radius:8px">Plancher: non défini · Plafond: non défini</div>\n</div>\n<div class="card gold">\n<div class="ct">◈ TENDANCE PROGRAMMÉE</div>\n<div class="row" style="margin-bottom:8px">\n<input id="cfg-st" type="time" class="grow">\n<input id="cfg-sp" type="time" class="grow">\n<select id="cfg-sd" style="flex:1;margin:0"><option value="up">Hausse</option><option value="down">Baisse</option><option value="random">Aléatoire</option></select>\n</div>\n<button class="btn gold full" onclick="scheduleT()">⏰ Programmer</button>\n<button class="btn full" style="color:var(--muted)" onclick="if(_schedInt){clearInterval(_schedInt);_schedInt=null;document.getElementById(\'cfg-sch-msg\').textContent=\'Annulé\';}">✕ Annuler</button>\n<div id="cfg-sch-msg" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n<div class="card cyan">\n<div class="ct">◈ EXPORTS</div>\n<button class="btn cyan full" onclick="exportHist()">📥 Historique JSON</button>\n<button class="btn purple full" onclick="exportStats()">📊 Rapport complet JSON</button>\n<button class="btn gold full" onclick="exportFlux()">💰 Flux bancaires CSV</button>\n</div>\n</div>\n\n<!-- ALERTES -->\n<div class="view" id="view-notifs">\n<div class="card gold">\n<div class="ct">◈ ALERTES DE PRIX</div>\n<div class="row" style="margin-bottom:8px">\n<input id="al-p" type="number" placeholder="Prix cible (R)" class="grow">\n<select id="al-d" style="width:auto;flex-shrink:0;margin:0;font-size:12px;padding:10px 8px"><option value="above">Si &gt;</option><option value="below">Si &lt;</option></select>\n<button class="btn gold" onclick="addAlert()">+ Alerte</button>\n</div>\n<div id="al-list" style="max-height:200px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"><p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune alerte</p></div>\n</div>\n<div class="card"><div class="ct">◈ ALERTES INTELLIGENTES</div><div id="smart-al"></div></div>\n<div class="card purple">\n<div class="ct">◈ HISTORIQUE ALERTES <button class="btn red" onclick="_alHist=[];renderAlHist()" style="padding:3px 7px;font-size:9px">Vider</button></div>\n<div id="al-hist" style="max-height:200px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"><p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune</p></div>\n</div>\n</div>\n\n\n<!-- CYCLES DE MARCHÉ -->\n<div class="view" id="view-cycles">\n\n<!-- MODAL INFO -->\n<div id="info-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:500;align-items:center;justify-content:center;padding:20px" onclick="this.style.display=\'none\'">\n<div style="background:var(--bg2);border:1px solid var(--border);border-radius:16px;padding:20px;max-width:340px;width:100%" onclick="event.stopPropagation()">\n<div style="font-weight:700;color:var(--cyan);margin-bottom:10px;font-size:14px" id="info-title">Info</div>\n<div style="font-size:13px;color:var(--muted);line-height:1.7" id="info-body"></div>\n<button onclick="$(\'info-modal\').style.display=\'none\'" style="margin-top:14px;width:100%;padding:10px;background:var(--bg3);border:1px solid var(--border);border-radius:10px;color:var(--text);cursor:pointer;font-weight:700">Fermer</button>\n</div>\n</div>\n\n<div class="card cyan">\n<div class="ct">◈ BORNES DU NXC <button onclick="showInfo(\'bornes\')" style="background:none;border:1px solid rgba(0,229,255,.3);border-radius:50%;width:18px;height:18px;color:var(--cyan);font-size:9px;cursor:pointer;padding:0">i</button></div>\n<div class="g2">\n<div>\n<div class="sec">Prix minimum absolu (R)</div>\n<div class="row"><input id="cy-absmin" type="number" min="1" placeholder="Ex: 100" class="grow"><button class="btn cyan" onclick="setCyVal(\'absmin\')">✓</button></div>\n<div id="cy-absmin-disp" style="font-size:10px;color:var(--green);margin-top:2px">Non défini</div>\n</div>\n<div>\n<div class="sec">Prix maximum absolu (R)</div>\n<div class="row"><input id="cy-absmax" type="number" placeholder="Ex: 50000" class="grow"><button class="btn cyan" onclick="setCyVal(\'absmax\')">✓</button></div>\n<div id="cy-absmax-disp" style="font-size:10px;color:var(--red);margin-top:2px">Non défini</div>\n</div>\n</div>\n</div>\n\n<div class="card gold">\n<div class="ct">◈ FRÉQUENCE DES EXTRÊMES PAR PÉRIODE <button onclick="showInfo(\'freq\')" style="background:none;border:1px solid rgba(255,176,32,.3);border-radius:50%;width:18px;height:18px;color:var(--gold);font-size:9px;cursor:pointer;padding:0">i</button></div>\n<div style="font-size:11px;color:var(--muted);margin-bottom:12px;padding:8px;background:var(--bg3);border-radius:8px">\nDéfinir combien de fois le NXC touchera son <b style="color:var(--green)">minimum</b> ou son <b style="color:var(--red)">maximum</b> dans chaque période. Le moteur calcule automatiquement la probabilité par tick.\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:4px">\n<span style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">Période</span>\n<span style="font-size:9px;color:var(--green);text-transform:uppercase;letter-spacing:1px;text-align:center">× Min</span>\n<span style="font-size:9px;color:var(--red);text-transform:uppercase;letter-spacing:1px;text-align:center">× Max</span>\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">📅 Par minute <button onclick="showInfo(\'freq-min\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-m" type="number" min="0" value="0" placeholder="0" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-m" type="number" min="0" value="0" placeholder="0" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">🕐 Par heure <button onclick="showInfo(\'freq-h\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-h" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-h" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">📆 Par jour <button onclick="showInfo(\'freq-d\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-d" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-d" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">📅 Par semaine <button onclick="showInfo(\'freq-w\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-w" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-w" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">🗓️ Par mois <button onclick="showInfo(\'freq-mo\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-mo" type="number" min="0" value="2" placeholder="2" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-mo" type="number" min="0" value="2" placeholder="2" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">📅 Par an <button onclick="showInfo(\'freq-y\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-y" type="number" min="0" value="4" placeholder="4" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-y" type="number" min="0" value="4" placeholder="4" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="margin-top:10px;padding-top:10px;border-top:1px solid rgba(255,176,32,.15)">\n<div style="font-size:10px;color:var(--muted);margin-bottom:6px;display:flex;align-items:center;gap:6px">Durée personnalisée <button onclick="showInfo(\'freq-custom\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:grid;grid-template-columns:auto auto 1fr 1fr;gap:8px;align-items:center">\n<input id="cy-custom-dur" type="number" placeholder="X" style="width:60px;padding:8px;margin:0;text-align:center">\n<select id="cy-custom-unit" style="width:auto;margin:0;font-size:11px;padding:8px 6px">\n<option value="60000">min</option>\n<option value="3600000" selected>h</option>\n<option value="86400000">j</option>\n<option value="604800000">sem</option>\n</select>\n<input id="cy-min-c" type="number" min="0" value="0" placeholder="Min" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-c" type="number" min="0" value="0" placeholder="Max" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n</div>\n\n<button class="btn gold" onclick="updateCyProb()" style="width:100%;margin-top:12px;padding:10px">🔄 Calculer les probabilités par tick</button>\n<div id="cy-prob-display" style="font-size:11px;color:var(--muted);margin-top:8px;padding:8px;background:var(--bg3);border-radius:8px;line-height:1.8"></div>\n</div>\n\n<div class="card purple">\n<div class="ct">◈ COMPORTEMENT DES CYCLES <button onclick="showInfo(\'comportement\')" style="background:none;border:1px solid rgba(160,107,255,.3);border-radius:50%;width:18px;height:18px;color:var(--purple);font-size:9px;cursor:pointer;padding:0">i</button></div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Transition vers extrême <button onclick="showInfo(\'transition\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<select id="cy-transition" style="margin-bottom:10px">\n<option value="brutal">Brutal (saut immédiat)</option>\n<option value="progressif" selected>Progressif (descente/montée graduelle)</option>\n<option value="sinusoide">Sinusoïde (courbe naturelle)</option>\n</select>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Durée de maintien au min/max <button onclick="showInfo(\'hold\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div class="row" style="margin-bottom:10px">\n<input id="cy-hold-min" type="number" min="0" value="1" placeholder="Min" style="width:70px;flex-shrink:0;margin:0">\n<span style="color:var(--muted);font-size:12px;flex-shrink:0">à</span>\n<input id="cy-hold-max" type="number" min="0" value="3" placeholder="Max" style="width:70px;flex-shrink:0;margin:0">\n<select id="cy-hold-unit" style="flex:1;margin:0;font-size:12px;padding:10px 8px">\n<option value="1">ticks</option>\n<option value="5" selected>minutes</option>\n<option value="300">heures</option>\n</select>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Drift de fond (tendance long terme) <button onclick="showInfo(\'drift\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="cy-drift" type="range" min="-5" max="5" value="0" step="0.5" oninput="$(\'cy-drift-val\').textContent=this.value>0?\'+\'+this.value+\'%/j\':this.value+\'%/j\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--purple)">\n<span id="cy-drift-val" style="color:var(--purple);font-weight:700;font-size:13px;width:60px;text-align:right;flex-shrink:0">0%/j</span>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Volatilité de fond <button onclick="showInfo(\'volbg\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="cy-vol-bg" type="range" min="0" max="10" value="2" oninput="$(\'cy-vol-bg-val\').textContent=(this.value/10).toFixed(1)+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--cyan)">\n<span id="cy-vol-bg-val" style="color:var(--cyan);font-weight:700;font-size:13px;width:40px;text-align:right;flex-shrink:0">0.2%</span>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Probabilité de pic surprise <button onclick="showInfo(\'spike\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="cy-spike" type="range" min="0" max="20" value="2" oninput="$(\'cy-spike-val\').textContent=this.value+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--red)">\n<span id="cy-spike-val" style="color:var(--red);font-weight:700;font-size:13px;width:32px;text-align:right;flex-shrink:0">2%</span>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Amplitude des pics <button onclick="showInfo(\'spikeamp\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="cy-spike-amp" type="range" min="1" max="30" value="10" oninput="$(\'cy-spike-amp-val\').textContent=\'±\'+this.value+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--red)">\n<span id="cy-spike-amp-val" style="color:var(--red);font-weight:700;font-size:13px;width:40px;text-align:right;flex-shrink:0">±10%</span>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Rebond au plancher <button onclick="showInfo(\'bounce\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="cy-bounce" type="range" min="0" max="10" value="3" oninput="$(\'cy-bounce-val\').textContent=this.value+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--green)">\n<span id="cy-bounce-val" style="color:var(--green);font-weight:700;font-size:13px;width:32px;text-align:right;flex-shrink:0">3%</span>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Résistance au plafond <button onclick="showInfo(\'resist\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">\n<input id="cy-resist" type="range" min="0" max="10" value="3" oninput="$(\'cy-resist-val\').textContent=this.value+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--gold)">\n<span id="cy-resist-val" style="color:var(--gold);font-weight:700;font-size:13px;width:32px;text-align:right;flex-shrink:0">3%</span>\n</div>\n</div>\n\n<div class="card green">\n<div class="ct">◈ ACTIVATION <button onclick="showInfo(\'activation\')" style="background:none;border:1px solid rgba(0,255,157,.3);border-radius:50%;width:18px;height:18px;color:var(--green);font-size:9px;cursor:pointer;padding:0">i</button></div>\n<button class="btn green full" onclick="startCycles()" id="cy-start-btn">▶ Activer les cycles de marché</button>\n<button class="btn red full" onclick="stopCycles()" style="display:none" id="cy-stop-btn">⏸ Désactiver les cycles</button>\n<div id="cy-status" style="font-size:12px;padding:10px;background:var(--bg3);border-radius:10px;color:var(--muted);min-height:40px">Cycles désactivés</div>\n<div id="cy-next" style="font-size:11px;color:var(--muted);margin-top:6px"></div>\n</div>\n\n<div class="card">\n<div class="ct">◈ PRÉVISUALISATION <button onclick="showInfo(\'preview\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:18px;height:18px;color:var(--cyan);font-size:9px;cursor:pointer;padding:0">i</button></div>\n<div class="chart-wrap ch150"><canvas id="cy-preview"></canvas></div>\n<button class="btn cyan" onclick="previewCycle()" style="width:100%;margin-top:8px;padding:10px">🔮 Générer prévisualisation (100 ticks simulés)</button>\n</div>\n</div>\n</div><!-- end content -->\n\n<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js">\n// ══ ÉPINGLAGE SITES (sync cross-device via serveur) ══\nvar _pinnedSites=[];\n\nasync function loadPinnedSites(){\n  try{\n    var r=await fetch(\'/admin/pinned-sites\');var d=await r.json();\n    if(d.ok){_pinnedSites=d.sites||[];renderSavedSites();}\n  }catch(e){_pinnedSites=JSON.parse(localStorage.getItem(\'nxc_pinned\')||\'[]\');}\n}\n\nasync function togglePin(url,label){\n  var idx=_pinnedSites.findIndex(s=>s.url===url);\n  if(idx>=0)_pinnedSites.splice(idx,1);\n  else _pinnedSites.push({url,label});\n  // Sauvegarder sur le serveur ET en local\n  localStorage.setItem(\'nxc_pinned\',JSON.stringify(_pinnedSites));\n  try{await fetch(\'/admin/pinned-sites\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,sites:_pinnedSites})});}catch(e){}\n  renderSavedSites();\n  addLog(\'📌\',(idx>=0?\'Désépinglé\':\'Épinglé\')+\': \'+label);\n}\n\nfunction renderPinnedBar(){\n  var el=$(\'pinned-bar\');if(!el)return;\n  if(!_pinnedSites.length){el.style.display=\'none\';return;}\n  el.style.display=\'flex\';\n  el.innerHTML=_pinnedSites.map(s=>\'<button onclick="loadSite(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" style="padding:5px 12px;background:rgba(255,176,32,.12);border:1px solid rgba(255,176,32,.3);border-radius:8px;color:var(--gold);font-size:11px;font-weight:700;cursor:pointer;white-space:nowrap">📌 \'+esc(s.label)+\'</button>\').join(\'\');\n}\n\n// ══ SAUVEGARDE / IMPORT DONNÉES GLOBALES ══\nasync function saveAllData(){\n  try{\n    var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'export\'})});\n    var d=await r.json();\n    if(!d.ok){setMsg(\'data-msg\',\'❌ Erreur export\',false);return;}\n    var blob=new Blob([JSON.stringify(d.data,null,2)],{type:\'application/json\'});\n    var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_full_backup_\'+Date.now()+\'.json\';a.click();\n    setMsg(\'data-msg\',\'✅ Backup complet téléchargé\',true);\n    addLog(\'💾\',\'Sauvegarde complète téléchargée\');\n  }catch(e){setMsg(\'data-msg\',\'❌ Erreur: \'+e.message,false);}\n}\n\nfunction importData(){\n  var input=document.createElement(\'input\');input.type=\'file\';input.accept=\'.json\';\n  input.onchange=async function(e){\n    var file=e.target.files[0];if(!file)return;\n    var text=await file.text();\n    try{\n      var data=JSON.parse(text);\n      if(!confirm(\'Importer ces données ? Cela écrasera les données actuelles du serveur.\'))return;\n      var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'import\',data:data})});\n      var res=await r.json();\n      setMsg(\'data-msg\',res.ok?\'✅ Données importées avec succès\':\'❌ \'+(res.error||\'Erreur import\'),res.ok);\n      if(res.ok){addLog(\'📥\',\'Données importées depuis fichier\');setTimeout(function(){ref();loadBank();},1000);}\n    }catch(ex){setMsg(\'data-msg\',\'❌ Fichier JSON invalide\',false);}\n  };\n  input.click();\n}\n\n// ══ IMPRESSION ══\nfunction printDashboard(){\n  var p=parseFloat(mkt.price||0);var h=mkt.history||[];\n  var hi=h.length>1?Math.max(...h.slice(-24).map(x=>x.price)):p;\n  var lo=h.length>1?Math.min(...h.slice(-24).map(x=>x.price)):p;\n  var chg=_prevP>0?((p-_prevP)/_prevP*100):0;\n  // Capturer le graphique en PNG\n  var chartImg=\'\';var cv=$(\'ch\');if(cv)chartImg=cv.toDataURL(\'image/png\');\n  var rsiImg=\'\';var rsiCv=$(\'ch-rsi\');if(rsiCv)rsiImg=rsiCv.toDataURL(\'image/png\');\n  var now=new Date().toLocaleString(\'fr-FR\');\n  var win=window.open(\'\',\'_blank\');\n  win.document.write(\'<!DOCTYPE html><html><head><meta charset="utf-8"><title>◈ Nexus NXC — Rapport \'+now+\'</title><style>*{font-family:Arial,sans-serif;box-sizing:border-box}body{background:#fff;color:#000;padding:20px;max-width:900px;margin:0 auto}.header{text-align:center;border-bottom:3px solid #000;padding-bottom:16px;margin-bottom:20px}.title{font-size:28px;font-weight:900;letter-spacing:3px}.date{font-size:12px;color:#666;margin-top:4px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}.stat{border:1px solid #ddd;border-radius:8px;padding:12px;text-align:center}.stat-val{font-size:20px;font-weight:700;margin-bottom:4px}.stat-lbl{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:#666}img{max-width:100%;border:1px solid #ddd;border-radius:8px;margin-bottom:12px}h3{margin:16px 0 8px;font-size:14px;border-bottom:1px solid #eee;padding-bottom:4px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:8px;text-align:left;border:1px solid #ddd}th{background:#f5f5f5;font-weight:700}@media print{.no-print{display:none}}</style></head><body>\');\n  win.document.write(\'<div class="header"><div class="title">◈ NEXUS NXC</div><div class="date">Rapport généré le \'+now+\'</div></div>\');\n  win.document.write(\'<div class="grid">\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(p,2)+\' R</div><div class="stat-lbl">Prix actuel</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%</div><div class="stat-lbl">Variation</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(hi,0)+\' R</div><div class="stat-lbl">Haut 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(lo,0)+\' R</div><div class="stat-lbl">Bas 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(mkt.volume24||0,0)+\' R</div><div class="stat-lbl">Volume 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(mkt.trades24||0)+\'</div><div class="stat-lbl">Trades 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+h.length+\'</div><div class="stat-lbl">Points hist.</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(_users.length||0)+\'</div><div class="stat-lbl">Utilisateurs</div></div>\');\n  win.document.write(\'</div>\');\n  if(chartImg)win.document.write(\'<h3>Historique du cours (\'+_ctRange+\' derniers points)</h3><img src="\'+chartImg+\'">\');\n  if(rsiImg)win.document.write(\'<h3>RSI (14 ticks)</h3><img src="\'+rsiImg+\'">\');\n  if(_users.length){\n    win.document.write(\'<h3>Utilisateurs</h3><table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur (R)</th></tr></thead><tbody>\');\n    _users.forEach(u=>{win.document.write(\'<tr><td>\'+esc(u.n)+\'</td><td>\'+esc(u.role)+\'</td><td>\'+fmt(u.rew,0)+\'</td><td>\'+u.nxc.toFixed(4)+\'</td><td>\'+fmt(u.val,0)+\'</td></tr>\');});\n    win.document.write(\'</tbody></table>\');\n  }\n  win.document.write(\'<h3>Derniers logs</h3><table><thead><tr><th>Heure</th><th>Action</th></tr></thead><tbody>\');\n  _log.slice(0,20).forEach(l=>{win.document.write(\'<tr><td>\'+fmtT(l.ts)+\'</td><td>\'+l.ico+\' \'+esc(l.txt)+\'</td></tr>\');});\n  win.document.write(\'</tbody></table>\');\n    var bR=document.getElementById(\'bk-r\')?document.getElementById(\'bk-r\').textContent:\'—\';\n  var bI=document.getElementById(\'bk-i\')?document.getElementById(\'bk-i\').textContent:\'—\';\n  var bO=document.getElementById(\'bk-o\')?document.getElementById(\'bk-o\').textContent:\'—\';\n  var bRt=document.getElementById(\'bk-rt\')?document.getElementById(\'bk-rt\').textContent:\'—\';\n  var bNx=document.getElementById(\'bk-nx\')?document.getElementById(\'bk-nx\').textContent:\'—\';\n  var bVx=document.getElementById(\'bk-vx\')?document.getElementById(\'bk-vx\').textContent:\'—\';\n  var bBn=document.getElementById(\'bk-bn\')?document.getElementById(\'bk-bn\').textContent:\'—\';\n  var bFl=document.getElementById(\'bk-fl\')?document.getElementById(\'bk-fl\').textContent:\'—\';\n  win.document.write(\'<hr style="margin:20px 0;border:none;border-top:2px solid #6366f1">\')\n  win.document.write(\'<h2 style="font-family:monospace;color:#6366f1;margin:0 0 12px">◈ BANQUE NXC</h2>\')\n  win.document.write(\'<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px">\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Réserves</div><div style="font-weight:bold">\'+ bR +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Entrées</div><div style="font-weight:bold">\'+ bI +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Sorties</div><div style="font-weight:bold">\'+ bO +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Taux</div><div style="font-weight:bold">\'+ bRt +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">NXC circ.</div><div style="font-weight:bold">\'+ bNx +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Valeur NXC</div><div style="font-weight:bold">\'+ bVx +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Billets</div><div style="font-weight:bold">\'+ bBn +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Flux total</div><div style="font-weight:bold">\'+ bFl +\'</div></div>\')\n  win.document.write(\'</div>\')\n  if(typeof _flux!==\'undefined\'&&_flux&&_flux.length>0){\n    win.document.write(\'<h3 style="font-family:monospace;margin:0 0 8px">Flux récents</h3>\')\n    win.document.write(\'<table style="width:100%;border-collapse:collapse;font-size:11px"><thead><tr style="background:#6366f1;color:#fff"><th style="padding:4px 8px;text-align:left">Date</th><th>Type</th><th>Utilisateur</th><th>Montant</th><th>Solde</th></tr></thead><tbody>\')\n    _flux.slice(0,50).forEach(function(f){\n      var fd=new Date(f.ts).toLocaleString(\'fr-FR\');\n      win.document.write(\'<tr style="border-bottom:1px solid #eee"><td style="padding:3px 8px">\'+fd+\'</td><td>\'+esc(f.type||\'\')+ \'</td><td>\'+esc(f.user||\'\')+ \'</td><td>\'+esc(String(f.amount||\'\'))+ \'</td><td>\'+esc(String(f.balance||\'\'))+ \'</td></tr>\')\n    });\n    win.document.write(\'</tbody></table>\')\n  }\n  win.document.write(\'</body></html>\');\n  win.document.close();\n  setTimeout(function(){win.print();},500);\n  addLog(\'🖨️\',\'Impression du tableau de bord\');\n}\n\n\n// ══ CYCLES DE MARCHÉ ══\nvar _cy={absmin:null,absmax:null,active:false,int:null,phase:\'normal\',phaseStart:Date.now(),holdUntil:0};\nvar _cyPreviewObj=null;\n\nfunction setCyVal(key){\n  var v=parseFloat($(\'cy-\'+key).value);\n  if(isNaN(v)||v<=0)return;\n  _cy[key]=v;\n  var el=$(\'cy-\'+key+\'-disp\');if(el)el.textContent=fmt(v,0)+\' R\';\n  // Sync avec _cfgFloor/_cfgCeil\n  if(key===\'absmin\'){_cfgFloor=v;updFloorDisplay();}\n  if(key===\'absmax\'){_cfgCeil=v;updFloorDisplay();}\n  addLog(\'📅\',\'Borne \'+key+\': \'+fmt(v,0)+\' R\');\n}\n\nfunction getCyConfig(){\n  return {\n    absmin: _cy.absmin||parseFloat($(\'cy-absmin\').value)||50,\n    absmax: _cy.absmax||parseFloat($(\'cy-absmax\').value)||100000,\n    transition: $(\'cy-transition\').value,\n    holdMin: parseFloat($(\'cy-hold-min\').value)||1,\n    holdMax: parseFloat($(\'cy-hold-max\').value)||3,\n    holdUnit: parseFloat($(\'cy-hold-unit\').value)||60,\n    drift: parseFloat($(\'cy-drift\').value)/100/1440,\n    volBg: parseFloat($(\'cy-vol-bg\').value)/1000,\n    spikeProb: parseFloat($(\'cy-spike\').value)/100,\n    spikeAmp: parseFloat($(\'cy-spike-amp\').value)/100,\n    bounce: parseFloat($(\'cy-bounce\').value)/100,\n    resist: parseFloat($(\'cy-resist\').value)/100,\n    // Fréquences par période → probabilité par tick (tick = 12s)\n    freqMin: {\n      m: parseFloat($(\'cy-min-m\').value)||0,\n      h: parseFloat($(\'cy-min-h\').value)||1,\n      d: parseFloat($(\'cy-min-d\').value)||1,\n      w: parseFloat($(\'cy-min-w\').value)||1,\n      mo: parseFloat($(\'cy-min-mo\').value)||2,\n      y: parseFloat($(\'cy-min-y\').value)||4,\n    },\n    freqMax: {\n      m: parseFloat($(\'cy-max-m\').value)||0,\n      h: parseFloat($(\'cy-max-h\').value)||1,\n      d: parseFloat($(\'cy-max-d\').value)||1,\n      w: parseFloat($(\'cy-max-w\').value)||1,\n      mo: parseFloat($(\'cy-max-mo\').value)||2,\n      y: parseFloat($(\'cy-max-y\').value)||4,\n    },\n  };\n}\n\nfunction calcProbPerTick(freqObj){\n  // Convertir les fréquences en probabilité par tick (12s)\n  var ticksPerMin=5,ticksPerH=300,ticksPerD=7200,ticksPerW=50400,ticksPerMo=216000,ticksPerY=2628000;\n  var pMin=freqObj.m/ticksPerMin+freqObj.h/ticksPerH+freqObj.d/ticksPerD+freqObj.w/ticksPerW+freqObj.mo/ticksPerMo+freqObj.y/ticksPerY;\n  return Math.min(pMin,0.5); // max 50% par tick\n}\n\nfunction startCycles(){\n  var cfg=getCyConfig();\n  if(cfg.absmin>=cfg.absmax){alert(\'Le plancher doit être inférieur au plafond\');return;}\n  _cy.active=true;_cy.phase=\'normal\';_cy.holdUntil=0;\n  $(\'cy-start-btn\').style.display=\'none\';$(\'cy-stop-btn\').style.display=\'block\';\n  var iv=parseInt($(\'ti\').value)||12000;\n  if(tInt){clearInterval(tInt);tInt=null;}\n  tMode=\'cycles\';\n  var el=$(\'tst\');el.textContent=\'📅 Cycles actifs · \'+fmt(cfg.absmin,0)+\'R – \'+fmt(cfg.absmax,0)+\'R\';el.style.color=\'var(--cyan)\';\n  addLog(\'📅\',\'Cycles de marché activés\');\n\n  var pToMin=calcProbPerTick(cfg.freqMin);\n  var pToMax=calcProbPerTick(cfg.freqMax);\n\n  _cy.int=setInterval(async function(){\n    var p=parseFloat(mkt.price||5213);\n    var now=Date.now();\n    var adj=0;\n\n    // Drift de fond\n    adj+=cfg.drift;\n    // Volatilité de fond\n    adj+=(Math.random()-0.5)*cfg.volBg*2;\n\n    // Pics surprises\n    if(Math.random()<cfg.spikeProb){\n      var dir=Math.random()>0.5?1:-1;\n      adj+=dir*cfg.spikeAmp*(Math.random()*0.5+0.5);\n      addLog(\'⚡\',\'Pic surprise: \'+(dir>0?\'+\':\'\')+((adj*100).toFixed(1))+\'%\');\n    }\n\n    // Gestion des phases\n    if(now<_cy.holdUntil){\n      // Maintien en position (min ou max)\n      if(_cy.phase===\'atmin\')adj=Math.max(0,(Math.random()-0.3)*0.001);\n      if(_cy.phase===\'atmax\')adj=Math.min(0,(Math.random()-0.7)*0.001);\n    } else {\n      // Décider si on va vers le min ou le max\n      if(_cy.phase!==\'tomin\'&&_cy.phase!==\'tomax\'){\n        var goMin=Math.random()<pToMin;\n        var goMax=Math.random()<pToMax;\n        if(goMin&&!goMax){_cy.phase=\'tomin\';addLog(\'📅\',\'Cycle → minimum\');}\n        else if(goMax&&!goMin){_cy.phase=\'tomax\';addLog(\'📅\',\'Cycle → maximum\');}\n        else _cy.phase=\'normal\';\n      }\n      if(_cy.phase===\'tomin\'){\n        // Descente vers le min\n        var distRatio=(p-cfg.absmin)/(cfg.absmax-cfg.absmin);\n        var force=cfg.transition===\'brutal\'?-0.1:cfg.transition===\'sinusoide\'?-Math.sin(distRatio*Math.PI)*0.02:-0.01;\n        adj+=force*(1+cfg.bounce);\n        if(p<=cfg.absmin*1.01){_cy.phase=\'atmin\';var holdSec=(cfg.holdMin+Math.random()*(cfg.holdMax-cfg.holdMin))*cfg.holdUnit;_cy.holdUntil=now+holdSec*1000;addLog(\'📅\',\'Cycle: minimum atteint · maintien \'+(holdSec/60).toFixed(0)+\'min\');}\n      }\n      if(_cy.phase===\'tomax\'){\n        // Montée vers le max\n        var distRatio=(cfg.absmax-p)/(cfg.absmax-cfg.absmin);\n        var force=cfg.transition===\'brutal\'?0.1:cfg.transition===\'sinusoide\'?Math.sin(distRatio*Math.PI)*0.02:0.01;\n        adj+=force*(1+cfg.resist);\n        if(p>=cfg.absmax*0.99){_cy.phase=\'atmax\';var holdSec=(cfg.holdMin+Math.random()*(cfg.holdMax-cfg.holdMin))*cfg.holdUnit;_cy.holdUntil=now+holdSec*1000;addLog(\'📅\',\'Cycle: maximum atteint · maintien \'+(holdSec/60).toFixed(0)+\'min\');}\n      }\n    }\n\n    // Résistance aux bornes\n    if(p<cfg.absmin*1.05)adj+=cfg.bounce*0.05;\n    if(p>cfg.absmax*0.95)adj-=cfg.resist*0.05;\n\n    p=Math.max(cfg.absmin,Math.min(cfg.absmax,p*(1+adj)));\n    p=Math.round(p*100)/100;\n\n    // Mise à jour du statut\n    var rem=Math.max(0,Math.round((_cy.holdUntil-now)/1000));\n    var statusTxt=\'Phase: \'+_cy.phase+(_cy.holdUntil>now?\' · maintien encore \'+rem+\'s\':\'\')+\' · P(min)/tick: \'+(pToMin*100).toFixed(2)+\'% · P(max)/tick: \'+(pToMax*100).toFixed(2)+\'%\';\n    var st=$(\'cy-status\');if(st)st.textContent=statusTxt;\n\n    await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:Math.floor(Math.random()*200+20),volume24:(mkt.volume24||0)+80,trades24:(mkt.trades24||0)+1})});\n  },iv);\n}\n\nfunction stopCycles(){\n  _cy.active=false;_cy.phase=\'normal\';\n  if(_cy.int){clearInterval(_cy.int);_cy.int=null;}\n  if(tMode===\'cycles\'){tMode=null;tInt=null;}\n  $(\'cy-start-btn\').style.display=\'block\';$(\'cy-stop-btn\').style.display=\'none\';\n  var el=$(\'cy-status\');if(el)el.textContent=\'Cycles désactivés\';\n  var el2=$(\'tst\');if(el2){el2.textContent=\'⏸ Arrêté\';el2.style.color=\'var(--muted)\';}\n  addLog(\'📅\',\'Cycles désactivés\');\n}\n\nfunction previewCycle(){\n  var cfg=getCyConfig();var cv=$(\'cy-preview\');if(!cv||!window.Chart)return;\n  if(_cyPreviewObj){_cyPreviewObj.destroy();_cyPreviewObj=null;}\n  var pts=[];var p=(cfg.absmin+cfg.absmax)/2;\n  var pMin=calcProbPerTick(cfg.freqMin);var pMax=calcProbPerTick(cfg.freqMax);\n  var phase=\'normal\';var holdUntil=0;\n  for(var t=0;t<100;t++){\n    var adj=(Math.random()-0.5)*cfg.volBg*2+cfg.drift;\n    if(Math.random()<cfg.spikeProb)adj+=(Math.random()>0.5?1:-1)*cfg.spikeAmp*Math.random();\n    if(t>holdUntil){\n      if(phase!==\'tomin\'&&phase!==\'tomax\'){\n        if(Math.random()<pMin)phase=\'tomin\';\n        else if(Math.random()<pMax)phase=\'tomax\';\n        else phase=\'normal\';\n      }\n      if(phase===\'tomin\'){adj-=0.01*(1+cfg.bounce);if(p<=cfg.absmin*1.01){phase=\'atmin\';holdUntil=t+3;}}\n      if(phase===\'tomax\'){adj+=0.01*(1+cfg.resist);if(p>=cfg.absmax*0.99){phase=\'atmax\';holdUntil=t+3;}}\n    }\n    p=Math.max(cfg.absmin,Math.min(cfg.absmax,p*(1+adj)));\n    pts.push(Math.round(p*100)/100);\n  }\n  var labs=pts.map((_,i)=>\'T\'+i);\n  var ctx=cv.getContext(\'2d\');\n  var g=ctx.createLinearGradient(0,0,0,150);g.addColorStop(0,\'rgba(0,229,255,.2)\');g.addColorStop(1,\'rgba(0,229,255,0)\');\n  _cyPreviewObj=new Chart(ctx,{type:\'line\',data:{labels:labs,datasets:[\n    {data:pts,borderColor:\'#00e5ff\',backgroundColor:g,borderWidth:2,pointRadius:0,fill:true,tension:0.3},\n    {data:Array(100).fill(cfg.absmin),borderColor:\'rgba(0,255,157,.4)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[4,4]},\n    {data:Array(100).fill(cfg.absmax),borderColor:\'rgba(255,61,94,.4)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[4,4]},\n  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{display:false},y:{ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n}\n\n\n// ══ INFOS BULLES ══\nvar _infos={\n  bornes:"Les bornes sont les limites absolues du prix NXC. Le prix ne pourra jamais descendre en dessous du minimum ni monter au-dessus du maximum, quoi qu\'il arrive.",\n  freq:"Définit combien de fois le prix touchera exactement son minimum ou maximum dans chaque période. Le moteur calcule automatiquement la probabilité par tick (intervalle de 12s par défaut) pour respecter ces fréquences.",\n  "freq-min":"Par minute : combien de fois dans la prochaine minute le prix touchera son minimum (colonne verte) ou maximum (colonne rouge). 0 = jamais dans la minute.",\n  "freq-h":"Par heure : combien de fois dans la prochaine heure le prix touchera son minimum ou maximum. Ex: 2 = deux fois dans l\'heure.",\n  "freq-d":"Par jour : combien de fois dans les 24 prochaines heures le prix touchera son minimum ou maximum.",\n  "freq-w":"Par semaine : combien de fois dans les 7 prochains jours le prix touchera son minimum ou maximum.",\n  "freq-mo":"Par mois (30 jours) : combien de fois dans le mois le prix touchera son minimum ou maximum.",\n  "freq-y":"Par an (365 jours) : combien de fois dans l\'année le prix touchera son minimum ou maximum. Ex: 4 = une fois par trimestre.",\n  "freq-custom":"Durée personnalisée : définir une période sur mesure. Ex: 6 heures, 2 jours... et combien de fois le prix touchera les extrêmes dans cette durée.",\n  comportement:"Paramètres qui définissent comment le prix se comporte quand il se déplace vers un extrême.",\n  transition:"Comment le prix atteint le min ou le max. Brutal = saut instantané. Progressif = descente/montée sur plusieurs ticks. Sinusoïde = courbe douce et naturelle.",\n  hold:"Combien de temps le prix reste au minimum ou maximum avant de repartir. Une durée aléatoire entre Min et Max est choisie à chaque fois.",\n  drift:"Tendance de fond sur le long terme. +2%/j = le prix a une légère tendance à monter de 2% par jour en moyenne. 0 = aucune tendance.",\n  volbg:"Quantité de mouvement aléatoire à chaque tick, indépendant des cycles. 0% = prix totalement lisse entre les cycles. Plus élevé = plus de micro-variations.",\n  spike:"Probabilité qu\'un pic inattendu se produise à chaque tick. Ex: 5% = 1 chance sur 20 à chaque tick d\'avoir un mouvement brutal.",\n  spikeamp:"Amplitude maximale d\'un pic surprise. ±10% = le pic peut faire bouger le prix de jusqu\'à 10% instantanément.",\n  bounce:"Force du rebond quand le prix touche le plancher. 0% = s\'arrête exactement au plancher. 5% = rebondit légèrement vers le haut.",\n  resist:"Résistance quand le prix approche du plafond. 0% = monte jusqu\'au plafond facilement. 5% = plus difficile de dépasser le plafond.",\n  activation:"Active le moteur de cycles. Une fois activé, le prix suivra automatiquement les fréquences définies pour atteindre les extrêmes.",\n  preview:"Simule 100 ticks avec les paramètres actuels pour voir à quoi ressemblera le comportement du prix avant de l\'activer."\n};\n\nfunction showInfo(key){\n  var modal=$(\'info-modal\');if(!modal)return;\n  $(\'info-title\').textContent=\'ℹ️ \'+key.replace(/-/g,\' \').replace(/\\b\\w/g,c=>c.toUpperCase());\n  $(\'info-body\').textContent=_infos[key]||\'Information non disponible.\';\n  modal.style.display=\'flex\';\n}\n\n// ══ PROBABILITÉS PAR TICK ══\nfunction updateCyProb(){\n  var ticksPerMin=5,ticksPerH=300,ticksPerD=7200,ticksPerW=50400,ticksPerMo=216000,ticksPerY=2628000;\n  var customDur=parseFloat($(\'cy-custom-dur\').value)||0;\n  var customUnit=parseFloat($(\'cy-custom-unit\').value)||3600000;\n  var customMs=customDur*customUnit;\n  var customTicks=customMs/12000;\n\n  var freqMin={m:parseFloat($(\'cy-min-m\').value)||0,h:parseFloat($(\'cy-min-h\').value)||0,d:parseFloat($(\'cy-min-d\').value)||0,w:parseFloat($(\'cy-min-w\').value)||0,mo:parseFloat($(\'cy-min-mo\').value)||0,y:parseFloat($(\'cy-min-y\').value)||0,c:parseFloat($(\'cy-min-c\').value)||0};\n  var freqMax={m:parseFloat($(\'cy-max-m\').value)||0,h:parseFloat($(\'cy-max-h\').value)||0,d:parseFloat($(\'cy-max-d\').value)||0,w:parseFloat($(\'cy-max-w\').value)||0,mo:parseFloat($(\'cy-max-mo\').value)||0,y:parseFloat($(\'cy-max-y\').value)||0,c:parseFloat($(\'cy-max-c\').value)||0};\n\n  var pMin=freqMin.m/ticksPerMin+freqMin.h/ticksPerH+freqMin.d/ticksPerD+freqMin.w/ticksPerW+freqMin.mo/ticksPerMo+freqMin.y/ticksPerY+(customTicks>0?freqMin.c/customTicks:0);\n  var pMax=freqMax.m/ticksPerMin+freqMax.h/ticksPerH+freqMax.d/ticksPerD+freqMax.w/ticksPerW+freqMax.mo/ticksPerMo+freqMax.y/ticksPerY+(customTicks>0?freqMax.c/customTicks:0);\n\n  pMin=Math.min(pMin,0.8);pMax=Math.min(pMax,0.8);\n\n  // Estimation des fréquences résultantes\n  var estPerH_min=Math.round(pMin*ticksPerH*10)/10;\n  var estPerH_max=Math.round(pMax*ticksPerH*10)/10;\n  var estPerD_min=Math.round(pMin*ticksPerD);\n  var estPerD_max=Math.round(pMax*ticksPerD);\n\n  var el=$(\'cy-prob-display\');if(!el)return;\n  el.innerHTML=\n    \'<b style="color:var(--green)">MIN</b> — probabilité/tick: <b>\'+(pMin*100).toFixed(3)+\'%</b> · ~\'+estPerH_min+\'/heure · ~\'+estPerD_min+\'/jour<br>\'\n    +\'<b style="color:var(--red)">MAX</b> — probabilité/tick: <b>\'+(pMax*100).toFixed(3)+\'%</b> · ~\'+estPerH_max+\'/heure · ~\'+estPerD_max+\'/jour<br>\'\n    +(pMin+pMax>0.5?\'<span style="color:var(--red)">⚠️ Fréquences très élevées — le prix sera souvent aux extrêmes</span>\':\'<span style="color:var(--green)">✅ Fréquences réalistes</span>\');\n\n  window._cyPMin=pMin;window._cyPMax=pMax;\n}\n\n</script>\n<script>\nvar KEY=\'\',mkt={},tInt=null,tMode=null,tStr=0.005,tIv=12000,chObj=null,rsiObj=null,volObj=null;\nvar solvOn=false,mpOn=false,biasDrift=0,biasSpd=1.0,_users=[],_flux=[],_fluxF=\'all\',_log=[],_alerts=[],_alHist=[];\nvar _prevP=0,_ctType=\'line\',_ctRange=50,_cfgFloor=null,_cfgCeil=null,_schedInt=null;\nvar _tmInt=null,_randP=null,_savedSites=JSON.parse(localStorage.getItem(\'nxc_sites\')||\'[]\'),_curUrl=\'\';\n\nfunction $(i){return document.getElementById(i);}\nfunction fmt(n,d){return Number(n||0).toLocaleString(\'fr-FR\',{minimumFractionDigits:d||0,maximumFractionDigits:d==null?2:d});}\nfunction esc(s){return (s+\'\').replace(/[&<>"]/g,c=>({\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\'}[c]));}\nfunction fmtT(ts){return new Date(ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\',second:\'2-digit\'});}\nfunction setMsg(id,t,ok){var e=$(id);if(!e)return;e.textContent=t;e.style.color=ok?\'var(--green)\':\'var(--red)\';}\nfunction addLog(ico,txt){_log.unshift({ico,txt,ts:Date.now()});if(_log.length>200)_log.pop();renderLog();}\nfunction renderLog(){\n  var h=_log.length?_log.map(l=>\'<div class="log-item"><span class="log-time">\'+fmtT(l.ts)+\'</span><span>\'+l.ico+\'</span><span style="color:var(--text);flex:1">\'+esc(l.txt)+\'</span></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun log</p>\';\n  var l=$(\'log-list\');if(l)l.innerHTML=h;\n  var l2=$(\'log-list2\');if(l2)l2.innerHTML=h;\n}\nasync function api(p,b){b=b||{};b.master_key=KEY;try{var r=await fetch(p,{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify(b)});return await r.json();}catch(e){return{ok:false};}}\n\n// LOGIN\nfunction doLogin(){\n  var k=$(\'mk\');if(!k)return;\n  KEY=k.value.trim();\n  if(!KEY){$(\'lm\').textContent=\'Entrer la clé\';return;}\n  $(\'lm\').textContent=\'Connexion…\';\n  fetch(\'/nxc/price\').then(function(r){return r.json();}).then(function(d){\n    // Tester avec admin/list\n    return fetch(\'/admin/list\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY})});\n  }).then(function(r){return r.json();}).then(function(d){\n    if(d&&d.ok){\n      $(\'ls\').style.display=\'none\';\n      $(\'hd\').classList.add(\'on\');\n      $(\'htm\').style.display=\'block\';\n      addLog(\'🔑\',\'Connexion admin réussie\');loadPinnedSites();\n      ref();loadBank();loadSolv();loadMeanPrice();loadBias();loadFails();\n      setInterval(ref,15000);\n      setInterval(function(){loadBank();loadFails();},25000);\n      setInterval(function(){$(\'htm\').textContent=new Date().toLocaleTimeString(\'fr-FR\');},1000);\n      // Charger les sites sauvegardés\n      if(!_savedSites.length){\n        _savedSites=[\n          {label:\'Nexus Coin\',url:\'https://lively-art-86d9.noah-guetta.workers.dev\'},\n          {label:\'Panel Admin\',url:location.origin+\'/panel\'},\n          {label:\'GitHub\',url:\'https://github.com/Noah1234567890123456789\'}\n        ];\n        localStorage.setItem(\'nxc_sites\',JSON.stringify(_savedSites));\n      }\n      renderSavedSites();\n    }else{\n      $(\'lm\').textContent=\'❌ Clé incorrecte\';KEY=\'\';\n    }\n  }).catch(function(){$(\'lm\').textContent=\'❌ Serveur inaccessible\';KEY=\'\';});\n}\n\n// TABS\nfunction toggleMore(){var d=$(\'dropdown\');d.classList.toggle(\'show\');}\ndocument.addEventListener(\'click\',function(e){if(!e.target.closest(\'#dropdown\')&&!e.target.closest(\'#btn-more\'))$(\'dropdown\').classList.remove(\'show\');});\n\nfunction go(tab,btn){\n  document.querySelectorAll(\'.view\').forEach(v=>v.classList.remove(\'on\'));\n  document.querySelectorAll(\'.tab\').forEach(t=>t.classList.remove(\'on\'));\n  var v=$(\'view-\'+tab);if(v)v.classList.add(\'on\');\n  if(btn)btn.classList.add(\'on\');\n  if(tab===\'users\')loadUsers();\n  if(tab===\'stats\')loadStats();\n  if(tab===\'admin\'){refreshAdminStats();loadAdmUsers();}\n  if(tab===\'banque\')$(\'nd-b\').style.display=\'none\';\n  if(tab===\'banque\')drawBkGraph();\n  if(tab===\'prevision\')calcPrev();\n}\n\n// MARCHÉ\nasync function ref(){\n  try{\n    var r=await fetch(\'/nxc/price\');var d=await r.json();mkt=d;\n    var p=parseFloat(d.price||0),h=d.history||[];\n    var chg=_prevP>0?((p-_prevP)/_prevP*100):0;\n    var hi=h.length>1?Math.max(...h.slice(-24).map(x=>x.price)):p;\n    var lo=h.length>1?Math.min(...h.slice(-24).map(x=>x.price)):p;\n    $(\'s-p\').textContent=fmt(p,2);$(\'s-v\').textContent=fmt(d.volume24||0,0);\n    $(\'s-t\').textContent=d.trades24||0;$(\'s-h\').textContent=h.length;\n    $(\'s-hi\').textContent=fmt(hi,0);$(\'s-lo\').textContent=fmt(lo,0);\n    $(\'s-var\').textContent=(chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%\';$(\'s-var\').style.color=chg>=0?\'var(--green)\':\'var(--red)\';\n    $(\'s-cap\').textContent=fmt(p*3,0);\n    $(\'hp\').textContent=fmt(p,2)+\' R\';\n    var hc=$(\'hc\');if(_prevP>0){hc.textContent=(chg>=0?\'▲+\':\'▼\')+chg.toFixed(2)+\'%\';hc.className=\'hud-chg \'+(chg>=0?\'up\':\'dn\');hc.style.display=\'block\';}\n    _prevP=p;\n    drawC(h);drawA(p,h);drawRSI(h);\n    checkAlerts(p);\n    if(_cfgFloor&&p<_cfgFloor){await tick(_cfgFloor);await ref();return;}\n    if(_cfgCeil&&p>_cfgCeil){await tick(_cfgCeil);await ref();return;}\n  }catch(e){}\n}\n\nasync function tick(p){await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:0,volume24:mkt.volume24||0,trades24:mkt.trades24||0})});}\n\nfunction setRange(n){_ctRange=n;if(chObj){chObj.destroy();chObj=null;}ref();}\nfunction toggleChartType(){_ctType=_ctType===\'line\'?\'bar\':\'line\';if(chObj){chObj.destroy();chObj=null;}ref();}\nfunction dlChart(){var cv=$(\'ch\');if(!cv)return;var a=document.createElement(\'a\');a.download=\'nxc_\'+Date.now()+\'.png\';a.href=cv.toDataURL();a.click();}\n\nfunction drawC(h){\n  var cv=$(\'ch\');if(!cv||!window.Chart)return;\n  var pts=h.slice(-_ctRange);\n  var labs=pts.map(x=>new Date(x.ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\'}));\n  var prices=pts.map(x=>parseFloat(x.price));\n  if(prices.length<2)return;\n  var mn=Math.min(...prices)*0.85,mx=Math.max(...prices)*1.15;\n  if(chObj){chObj.data.labels=labs;chObj.data.datasets[0].data=prices;chObj.options.scales.y.min=mn;chObj.options.scales.y.max=mx;chObj.update(\'none\');return;}\n  var ctx=cv.getContext(\'2d\');\n  var g=ctx.createLinearGradient(0,0,0,cv.offsetHeight||200);g.addColorStop(0,\'rgba(0,229,255,.2)\');g.addColorStop(1,\'rgba(0,229,255,0)\');\n  chObj=new Chart(ctx,{type:_ctType===\'bar\'?\'bar\':\'line\',data:{labels:labs,datasets:[{data:prices,borderColor:\'#00e5ff\',backgroundColor:_ctType===\'bar\'?\'rgba(0,229,255,.4)\':g,borderWidth:2.5,pointRadius:0,fill:_ctType!==\'bar\',tension:0.4}]},\n    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},\n      scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:5,font:{size:8}},grid:{color:\'rgba(0,229,255,.04)\'}},\n        y:{min:mn,max:mx,ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n}\n\nfunction drawRSI(h){\n  var cv=$(\'ch-rsi\');if(!cv||!window.Chart||h.length<15)return;\n  var prices=h.slice(-28).map(x=>parseFloat(x.price));\n  var rsi=[];\n  for(var i=14;i<prices.length;i++){\n    var g=0,l=0;for(var j=i-14;j<i;j++){var dv=prices[j+1]-prices[j];if(dv>0)g+=dv;else l-=dv;}\n    rsi.push(Math.round(l===0?100:100-100/(1+(g/l))));\n  }\n  var labs=h.slice(-rsi.length).map(x=>new Date(x.ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\'}));\n  if(rsiObj){rsiObj.data.labels=labs;rsiObj.data.datasets[0].data=rsi;rsiObj.update(\'none\');return;}\n  var ctx=cv.getContext(\'2d\');\n  rsiObj=new Chart(ctx,{type:\'line\',data:{labels:labs,datasets:[{data:rsi,borderColor:\'#a06bff\',borderWidth:2,pointRadius:0,fill:false,tension:0.4}]},\n    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},\n      scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:4,font:{size:8}},grid:{display:false}},y:{min:0,max:100,ticks:{color:\'#5c6b8c\',stepSize:25},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n}\n\nfunction drawA(p,h){\n  var el=$(\'al\'),a=[];\n  if(p>80000)a.push({c:\'ae\',m:\'⚡ Prix critique >80 000 R\'});\n  else if(p<500)a.push({c:\'ae\',m:\'🔴 Prix effondrement <500 R\'});\n  else a.push({c:\'ao\',m:\'✅ Prix normal: \'+fmt(p,0)+\' R\'});\n  if(h.length>10){var rv=h.slice(-10).map(x=>x.price);var vol=(Math.max(...rv)-Math.min(...rv))/Math.min(...rv)*100;a.push(vol>20?{c:\'aw\',m:\'⚡ Volatilité: \'+vol.toFixed(1)+\'%\'}:{c:\'ao\',m:\'📊 Stable — volatilité: \'+vol.toFixed(1)+\'%\'});}\n  a.push(tMode?{c:\'aw\',m:\'📊 Tendance \'+tMode+\' · \'+(tStr*100).toFixed(1)+\'%/tick\'}:{c:\'ai\',m:\'⏸ Aucune tendance\'});\n  if(el)el.innerHTML=a.map(x=>\'<div class="ab \'+x.c+\'">\'+x.m+\'</div>\').join(\'\');\n  // Smart alerts\n  var sa=$(\'smart-al\');if(sa)sa.innerHTML=a.map(x=>\'<div class="ab \'+x.c+\'">\'+x.m+\'</div>\').join(\'\');\n}\n\n// CONTRÔLE\nasync function adjP(pct){var p=Math.max(50,Math.min(100000,parseFloat(mkt.price||5213)*(1+pct)));p=Math.round(p*100)/100;await tick(p);setMsg(\'pm\',\'✅ \'+(pct>0?\'+\':\'\')+((pct*100).toFixed(1))+\'% → \'+fmt(p,2)+\' R\',true);addLog(\'📊\',\'Cours \'+(pct>0?\'+\':\'\')+((pct*100).toFixed(1))+\'%\');setTimeout(ref,500);}\nasync function setP(){var p=parseFloat($(\'np\').value);if(!p||p<50||p>100000){setMsg(\'pm\',\'Prix invalide\',false);return;}await tick(p);setMsg(\'pm\',\'✅ Cours → \'+fmt(p,2)+\' R\',true);$(\'np\').value=\'\';addLog(\'💱\',\'Cours fixé: \'+fmt(p,2)+\' R\');setTimeout(ref,500);}\nasync function setPct(){var pct=parseFloat($(\'np-pct\').value)/100;if(isNaN(pct)){setMsg(\'pm\',\'% invalide\',false);return;}await adjP(pct);$(\'np-pct\').value=\'\';}\nasync function resetH(){if(!confirm(\'Reset historique ?\'))return;await fetch(\'/nxc/reset\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY})});addLog(\'🔄\',\'Reset historique NXC\');ref();}\n\nvar _tStart=null,_tTimerInt=null;\nfunction setT(m){\n  var s=parseFloat($(\'ts\').value)||0.005,iv=parseInt($(\'ti\').value)||12000;\n  if(tInt){clearInterval(tInt);tInt=null;}if(_tTimerInt){clearInterval(_tTimerInt);_tTimerInt=null;}\n  tMode=m===\'stop\'?null:m;tStr=s;tIv=iv;_tStart=tMode?Date.now():null;\n  var el=$(\'tst\'),ht=$(\'hc\');\n  if(!tMode){el.textContent=\'⏸ Arrêté\';el.style.color=\'var(--muted)\';if($(\'tt-timer\'))$(\'tt-timer\').textContent=\'\';addLog(\'⏸\',\'Tendance arrêtée\');return;}\n  var lbl=m===\'up\'?\'📈 Hausse +\':m===\'down\'?\'📉 Baisse -\':\'🎲 Aléatoire\';var spd=m!==\'random\'?(s*100).toFixed(1)+\'%\':\'\';\n  el.textContent=lbl+spd+\' · \'+(iv/1000)+\'s/tick\';el.style.color=m===\'up\'?\'var(--green)\':m===\'down\'?\'var(--red)\':\'var(--purple)\';\n  addLog(m===\'up\'?\'📈\':m===\'down\'?\'📉\':\'🎲\',\'Tendance \'+m+\' · \'+(s*100).toFixed(1)+\'%\');\n  _tTimerInt=setInterval(function(){if(_tStart){var el=elapsed=Math.floor((Date.now()-_tStart)/1000);$(\'tt-timer\').textContent=\'⏱ \'+Math.floor(el/60)+\'m\'+(\'0\'+(el%60)).slice(-2)+\'s\';}},1000);\n  tInt=setInterval(async function(){\n    var p=parseFloat(mkt.price||5213);var adj=(Math.random()-0.5)*_noiseLevel*2;\n    if(m===\'up\')adj+=s;else if(m===\'down\')adj-=s;\n    p=Math.max(parseFloat(_cfgFloor)||50,Math.min(parseFloat(_cfgCeil)||100000,p*(1+adj)));\n    p=Math.random()>.03?Math.round(p*100)/100:Math.round(p);\n    await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:Math.floor(Math.random()*300+50),volume24:(mkt.volume24||0)+100,trades24:(mkt.trades24||0)+1})});\n  },iv);\n}\n\nasync function scenario(sc){\n  var p=parseFloat(mkt.price||5213),t;\n  if(sc===\'crash\')t=p*.7;else if(sc===\'moon\')t=p*1.3;else if(sc===\'ath\')t=Math.min(100000,Math.max(p*1.5,90000));else if(sc===\'floor\')t=200;\n  if(t){t=Math.max(50,Math.min(100000,Math.round(t*100)/100));await tick(t);addLog(\'🎭\',\'Scénario \'+sc+\' → \'+fmt(t,2)+\' R\');setTimeout(ref,500);}\n  else if(sc===\'volatile\'){setT(\'random\');addLog(\'⚡\',\'Scénario volatil\');}\n  else if(sc===\'stable\'){setT(\'stop\');addLog(\'😴\',\'Stabilisation\');}\n}\n\n// BANQUE\nfunction setAmt(v){$(\'bk-amt\').value=v;}\nfunction filterFlux(f){_fluxF=f;[\'fl-all\',\'fl-in\',\'fl-out\'].forEach(id=>{var e=$(id);if(e)e.className=\'btn\';});var e=$(\'fl-\'+f);if(e)e.className=\'btn cyan\';renderFlux();}\nfunction renderFlux(){\n  var flux=(_fluxF===\'all\'?_flux:_flux.filter(f=>f.type===_fluxF)).slice(0,30);\n  var el=$(\'bk-flux\');if(!el)return;\n  el.innerHTML=flux.length?flux.map(f=>\'<div class="fl-item"><div style="width:8px;height:8px;border-radius:50%;flex-shrink:0;background:\'+(f.type===\'IN\'?\'var(--green)\':\'var(--red)\')+\';box-shadow:0 0 6px \'+(f.type===\'IN\'?\'rgba(0,255,157,.4)\':\'rgba(255,61,94,.4)\')+\'"></div><span style="font-weight:700;color:\'+(f.type===\'IN\'?\'var(--green)\':\'var(--red)\')+\';flex-shrink:0">\'+(f.type===\'IN\'?\'+\':\'-\')+fmt(f.amount||0,0)+\' R</span><span style="color:var(--muted);flex:1">\'+esc(f.user||\'?\')+\'</span><span style="color:var(--muted);font-size:10px">\'+new Date(f.ts).toLocaleTimeString(\'fr-FR\')+\'</span></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun flux</p>\';\n}\n\nfunction exportFlux(){var csv=\'Date,Type,User,Montant\\n\';_flux.forEach(f=>csv+=new Date(f.ts).toLocaleString(\'fr-FR\')+\',\'+f.type+\',\'+(f.user||\'\')+\',\'+(f.amount||0)+\'\\n\');var b=new Blob([csv],{type:\'text/csv\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(b);a.download=\'flux_\'+Date.now()+\'.csv\';a.click();addLog(\'📊\',\'Export CSV flux\');}\n\nasync function loadBank(){\n  try{\n    var r=await fetch(\'/nxc/bank\');var d=await r.json();if(!d.ok)return;var b=d.bank||{};\n    _flux=(b.flux||[]).slice().reverse();\n    var p=parseFloat(mkt.price||0);\n    $(\'bk-r\').textContent=fmt(b.reserves||0,0)+\' R\';$(\'bk-i\').textContent=fmt(b.totalIn||0,0);\n    $(\'bk-o\').textContent=fmt(b.totalOut||0,0);\n    $(\'bk-rt\').textContent=(b.totalIn>0?((b.reserves||0)/b.totalIn*100):100).toFixed(1)+\'%\';\n    $(\'bk-nx\').textContent=parseFloat(b.nxcEmis||0).toFixed(4)+\' NXC\';\n    $(\'bk-vx\').textContent=fmt((b.nxcEmis||0)*p,0)+\' R\';\n    var bn=(b.totalIn||0)-(b.totalOut||0);var el=$(\'bk-bn\');el.textContent=(bn>=0?\'+\':\'\')+fmt(bn,0)+\' R\';el.style.color=bn>=0?\'var(--green)\':\'var(--red)\';\n    $(\'bk-fl\').textContent=_flux.length;\n    renderFlux();\n  }catch(e){}\n}\n\nasync function bankOp(type){\n  var amt=parseFloat($(\'bk-amt\').value);if(!amt||amt<=0){setMsg(\'bk-msg\',\'Montant invalide\',false);return;}\n  var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{reserves:0,totalIn:0,totalOut:0,nxcEmis:0,flux:[]};\n  if(type===\'out\'&&amt>(b.reserves||0)){setMsg(\'bk-msg\',\'❌ Réserves insuffisantes\',false);return;}\n  if(type===\'in\'){b.reserves=parseFloat(((b.reserves||0)+amt).toFixed(2));b.totalIn=parseFloat(((b.totalIn||0)+amt).toFixed(2));}\n  else{b.reserves=parseFloat(((b.reserves||0)-amt).toFixed(2));b.totalOut=parseFloat(((b.totalOut||0)+amt).toFixed(2));}\n  b.flux=b.flux||[];b.flux.push({type:type===\'in\'?\'IN\':\'OUT\',user:\'SERVEUR\',amount:amt,nxc:0,ts:Date.now()});\n  var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:b,reset:true})});\n  var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ \'+(type===\'in\'?\'+\':\'-\')+fmt(amt,0)+\' R\':\'❌ Erreur\',res.ok);\n  if(res.ok){$(\'bk-amt\').value=\'\';addLog(type===\'in\'?\'💰\':\'💸\',(type===\'in\'?\'Injection +\':\'Retrait -\')+fmt(amt,0)+\' R\');loadBank();}\n}\n\nasync function bankResetHist(){var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{};if(!confirm(\'Reset historique ? Réserves: \'+fmt(b.reserves||0,0)+\' R conservées\'))return;var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:{reserves:b.reserves||0,nxcEmis:0,totalIn:0,totalOut:0,flux:[]},reset:true})});var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ Historique effacé\':\'❌ Erreur\',res.ok);if(res.ok){addLog(\'🗑️\',\'Reset historique banque\');loadBank();}}\nasync function bankResetAll(){var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{};var g=confirm(\'Garder réserves (\'+fmt(b.reserves||0,0)+\' R) ?\');if(!confirm(\'Confirmer ?\'))return;var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:{reserves:g?(b.reserves||0):0,nxcEmis:0,totalIn:0,totalOut:0,flux:[]},reset:true})});var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ Réinitialisé\':\'❌ Erreur\',res.ok);if(res.ok){addLog(\'💥\',\'Reset complet banque\');loadBank();}}\n\nasync function loadFails(){\n  try{\n    var r=await fetch(\'/nxc/bank/fail\');var d=await r.json();\n    var el=$(\'bk-fails\'),fc=$(\'fails-ct\');if(!el)return;\n    var fails=(d.fails||[]).slice().reverse();\n    if(fails.length&&fc){fc.textContent=fails.length;fc.style.display=\'block\';}$(\'nd-b\').style.display=fails.length?\'block\':\'none\';\n    el.innerHTML=fails.length?fails.map(f=>\'<div style="padding:12px;border-bottom:1px solid rgba(255,61,94,.08);display:flex;flex-direction:column;gap:6px"><div style="display:flex;justify-content:space-between"><span style="color:var(--red);font-weight:700">❌ \'+esc(f.user)+\'</span><span style="color:var(--muted);font-size:10px;font-family:monospace">\'+new Date(f.ts).toLocaleTimeString(\'fr-FR\')+\'</span></div><div style="color:var(--muted);font-size:11px">Voulait vendre <b style="color:var(--text)">\'+f.nxc+\' NXC</b> (\'+fmt(f.amount||0,0)+\' R)</div>\'+(f.gesture>0?\'<button onclick="sendGesture(\\\'\'+esc(f.user)+\'\\\',\'+f.gesture+\',\'+f.ts+\')" style="padding:8px 14px;background:rgba(0,255,157,.1);border:1px solid rgba(0,255,157,.3);border-radius:9px;color:var(--green);font-size:12px;cursor:pointer;font-weight:700;align-self:flex-start">💝 Verser +\'+f.gesture+\' R</button>\':\'\')+\'</div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">✅ Aucune tentative</p>\';\n  }catch(e){}\n}\n\nasync function sendGesture(user,amount,failTs){\n  if(!confirm(\'Verser \'+amount+\' R à \'+user+\' ?\'))return;\n  var r=await fetch(\'/nxc/bank/gesture\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,target:user,amount:amount,fail_ts:failTs})});\n  var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ \'+amount+\' R versés à \'+user:\'❌ \'+(res.error||\'Erreur\'),res.ok);\n  if(res.ok){addLog(\'💝\',\'Geste +\'+amount+\' R → \'+user);loadBank();loadFails();}\n}\n\n// APP (iframe configurable)\nfunction renderSavedSites(){\n  var el=$(\'saved-sites\');if(!el)return;\n  var pinned=_pinnedSites.map(s=>s.url);\n  el.innerHTML=_savedSites.length?_savedSites.map(s=>{\n    var isPinned=pinned.includes(s.url);\n    return \'<div style="display:flex;align-items:center;gap:4px;background:var(--bg3);border:1px solid \'+(isPinned?\'rgba(255,176,32,.4)\':\'var(--border)\')+\';border-radius:8px;padding:4px 8px;white-space:nowrap">\'\n      +\'<button onclick="loadSite(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" style="background:none;border:none;color:\'+(isPinned?\'var(--gold)\':\'var(--cyan)\')+\';font-size:11px;font-weight:700;cursor:pointer;padding:0">\'+(isPinned?\'📌 \':\'\')+esc(s.label)+\'</button>\'\n      +\'<button onclick="togglePin(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" title="\'+(isPinned?\'Désépingler\':\'Épingler\')+\'" style="background:none;border:none;color:\'+(isPinned?\'var(--gold)\':\'var(--muted)\')+\';font-size:11px;cursor:pointer;padding:0;margin-left:2px">\'+(isPinned?\'📌\':\'📍\')+\'</button>\'\n      +\'<button onclick="deleteSite(\\\'\'+esc(s.url)+\'\\\')" style="background:none;border:none;color:var(--red);font-size:12px;cursor:pointer;padding:0;margin-left:2px">✕</button>\'\n      +\'</div>\';\n  }).join(\'\'):\'<span style="color:var(--muted);font-size:11px">Aucun site sauvegardé</span>\';\n  // Afficher les sites épinglés en premier si existants\n  renderPinnedBar();\n}\nfunction goUrl(){var url=$(\'iframe-in\').value.trim();if(!url)return;if(!url.startsWith(\'http\'))url=\'https://\'+url;loadSite(url,null);$(\'iframe-in\').value=\'\';}\nfunction loadSite(url,label){_curUrl=url;var f=$(\'nf\');if(f)f.src=url;var t=$(\'if-title\');if(t)t.textContent=\'◈ \'+(label||url.replace(\'https://\',\'\').split(\'/\')[0]);var u=$(\'if-url\');if(u)u.textContent=url.replace(\'https://\',\'\').replace(\'http://\',\'\');}\nfunction saveSite(){var url=$(\'iframe-in\').value.trim()||_curUrl;var lbl=$(\'site-lbl\').value.trim()||url.replace(\'https://\',\'\').split(\'/\')[0];if(!url)return;if(!url.startsWith(\'http\'))url=\'https://\'+url;_savedSites=_savedSites.filter(s=>s.url!==url);_savedSites.unshift({label:lbl,url});if(_savedSites.length>8)_savedSites.pop();localStorage.setItem(\'nxc_sites\',JSON.stringify(_savedSites));$(\'site-lbl\').value=\'\';$(\'iframe-in\').value=\'\';renderSavedSites();addLog(\'💾\',\'Site sauvegardé: \'+lbl);}\nfunction deleteSite(url){_savedSites=_savedSites.filter(s=>s.url!==url);localStorage.setItem(\'nxc_sites\',JSON.stringify(_savedSites));renderSavedSites();}\nfunction reloadF(){var f=$(\'nf\');if(f)f.src=f.src;}\nfunction openNewTab(){if(_curUrl)window.open(_curUrl,\'_blank\');}\n\n// ADMIN\nasync function refreshAdminStats(){\n  try{\n    var pd=await fetch(\'/nxc/price\').then(r=>r.json());\n    var bd=await fetch(\'/nxc/bank\').then(r=>r.json());\n    var fd=await fetch(\'/nxc/bank/fail\').then(r=>r.json());\n    var b=bd.bank||{};var p=parseFloat(pd.price||0);\n    $(\'adm-price\').textContent=fmt(p,2)+\' R\';\n    $(\'adm-vol\').textContent=fmt(pd.volume24||0,0)+\' R\';\n    $(\'adm-trades\').textContent=pd.trades24||0;\n    $(\'adm-res\').textContent=fmt(b.reserves||0,0)+\' R\';\n    $(\'adm-nxc\').textContent=parseFloat(b.nxcEmis||0).toFixed(4);\n    $(\'adm-fails\').textContent=(fd.fails||[]).length;\n    $(\'adm-hist\').textContent=(pd.history||[]).length;\n    if(_users.length)$(\'adm-users\').textContent=_users.length;\n    addLog(\'📊\',\'Stats admin actualisées\');\n  }catch(e){}\n}\n\nasync function loadAdmUsers(){\n  if(!_users.length)await loadUsers();\n  var sel1=$(\'rw-u\'),sel2=$(\'role-u\');\n  [sel1,sel2].forEach(sel=>{if(sel)sel.innerHTML=\'<option value="">Utilisateur...</option>\'+_users.map(u=>\'<option value="\'+esc(u.n)+\'">\'+esc(u.n)+(u.role===\'admin\'?\' 👑\':u.role===\'moderator\'?\' 🛡️\':u.role===\'vip\'?\' ⭐\':\'\')+\'</option>\').join(\'\');});\n  $(\'adm-users\').textContent=_users.length;\n  renderAdmUsers(_users);\n}\n\nfunction renderAdmUsers(rows){\n  var el=$(\'adm-ut\');if(!el)return;\n  el.innerHTML=rows.map(r=>\'<tr><td style="font-weight:700;color:var(--cyan)">\'+esc(r.n)+(r.role===\'admin\'?\' 👑\':r.role===\'moderator\'?\' 🛡️\':r.role===\'vip\'?\' ⭐\':\'\')+\'</td><td style="color:var(--muted);font-size:10px">\'+esc(r.role)+\'</td><td style="color:var(--gold)">\'+fmt(r.rew,0)+\'</td><td style="color:var(--cyan);font-family:monospace">\'+r.nxc.toFixed(4)+\'</td><td style="color:var(--purple)">\'+fmt(r.val,0)+\'</td></tr>\').join(\'\');\n}\nfunction filterAdmUsers(){var q=($(\'adm-q\').value||\'\').toLowerCase();renderAdmUsers(q?_users.filter(u=>u.n.toLowerCase().includes(q)):_users);}\n\nasync function giveRewards(){\n  var target=$(\'rw-u\').value,amt=parseFloat($(\'rw-amt\').value);\n  if(!target||!amt||amt<=0){setMsg(\'rw-msg\',\'Remplir tous les champs\',false);return;}\n  var r=await fetch(\'/admin/give-rewards\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,target:target,amount:amt})});\n  var res=await r.json();\n  setMsg(\'rw-msg\',res.ok?\'✅ +\'+fmt(amt,0)+\' R donnés à \'+target+\' (total: \'+fmt(res.new_rewards||0,0)+\' R)\':\'❌ \'+(res.error||\'Erreur\'),res.ok);\n  if(res.ok){addLog(\'🏆\',\'Rewards +\'+fmt(amt,0)+\' R → \'+target);}\n}\n\nasync function changeRole(){\n  var u=$(\'role-u\').value,role=$(\'role-v\').value;\n  if(!u){setMsg(\'role-msg\',\'Sélectionner un utilisateur\',false);return;}\n  var r=await fetch(\'/admin/set-role\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,target:u,role:role})});\n  var res=await r.json();\n  setMsg(\'role-msg\',res.ok?\'✅ Rôle de \'+u+\' changé en \'+role:\'❌ \'+(res.error||\'Erreur\'),res.ok);\n  if(res.ok)addLog(\'👑\',\'Rôle \'+u+\' → \'+role);\n}\n\nasync function pruneHistory(){if(!confirm(\'Réduire historique à 100 points ?\'))return;await fetch(\'/nxc/reset\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY})});setMsg(\'maint-msg\',\'✅ Historique réduit\',true);addLog(\'✂️\',\'Historique NXC réduit\');}\nasync function resetAllTrades(){if(!confirm(\'Reset trades 24h ?\'))return;await tick(parseFloat(mkt.price||5213));setMsg(\'maint-msg\',\'✅ Trades remis à zéro\',true);addLog(\'🗑️\',\'Reset trades 24h\');}\n\nasync function backupDB(){\n  try{var p=await(await fetch(\'/nxc/price\')).json();var b=await(await fetch(\'/nxc/bank\')).json();var u=await api(\'/admin/list\');var data={date:new Date().toISOString(),market:p,bank:b.bank||{},users:u.users||[]};var blob=new Blob([JSON.stringify(data,null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_backup_\'+Date.now()+\'.json\';a.click();setMsg(\'maint-msg\',\'✅ Backup téléchargé\',true);addLog(\'💾\',\'Backup DB téléchargé\');}catch(e){setMsg(\'maint-msg\',\'❌ Erreur backup\',false);}\n}\n\nasync function pingServer(){\n  var el=$(\'ping-res\');if(el){el.textContent=\'📡 Test...\';el.style.color=\'var(--muted)\';}\n  var t=Date.now();\n  try{await fetch(\'/nxc/price\');var lat=Date.now()-t;var c=lat<500?\'var(--green)\':lat<1000?\'var(--gold)\':\'var(--red)\';if(el){el.textContent=\'✅ En ligne — \'+lat+\' ms\';el.style.color=c;}}\n  catch(e){if(el){el.textContent=\'❌ Inaccessible\';el.style.color=\'var(--red)\';}}\n}\n\n// USERS\nasync function loadUsers(){\n  $(\'us-msg\').textContent=\'Chargement…\';\n  try{\n    var r=await api(\'/admin/list\');if(!r||!r.ok){$(\'us-msg\').textContent=\'Erreur\';return;}\n    var p=parseFloat(mkt.price||0);\n    var rows=await Promise.all((r.users||[]).map(async u=>{\n      var d=await api(\'/admin/get\',{target:u.username});\n      var rew=Math.max((d.data&&d.data.nx2098&&d.data.nx2098.rewards)||0,(d.data&&d.data.rewards&&d.data.rewards.points)||0);\n      var nxc=parseFloat((d.data&&d.data.nxcoin&&d.data.nxcoin.nxc)||0);\n      return {n:u.username,role:u.role,rew,nxc,val:nxc*p};\n    }));\n    _users=rows;\n    $(\'u-total\').textContent=rows.length;$(\'u-admins\').textContent=rows.filter(r=>r.role===\'admin\').length;\n    $(\'u-rew\').textContent=fmt(rows.reduce((s,r)=>s+r.rew,0),0);\n    sortU(\'rew\');$(\'us-msg\').textContent=\'\';\n    if($(\'adm-ut\'))loadAdmUsers();\n  }catch(e){$(\'us-msg\').textContent=\'Erreur\';}\n}\nfunction sortU(by){_users.sort((a,b)=>by===\'name\'?a.n.localeCompare(b.n):(b[by]-a[by]));renderU(_users);}\nfunction renderU(rows){var el=$(\'ut\');if(!el)return;el.innerHTML=rows.map(r=>\'<tr><td style="font-weight:700;color:var(--cyan)">\'+esc(r.n)+(r.role===\'admin\'?\' 👑\':r.role===\'moderator\'?\' 🛡️\':r.role===\'vip\'?\' ⭐\':\'\')+\'</td><td style="color:var(--muted);font-size:10px">\'+esc(r.role)+\'</td><td style="color:var(--gold)">\'+fmt(r.rew,0)+\'</td><td style="color:var(--cyan);font-family:monospace">\'+r.nxc.toFixed(4)+\'</td><td style="color:var(--purple)">\'+fmt(r.val,0)+\'</td></tr>\').join(\'\');}\nfunction filterU(){var q=($(\'us-q\').value||\'\').toLowerCase();renderU(q?_users.filter(r=>r.n.toLowerCase().includes(q)):_users);}\n\n// STATS\nvar volObj=null;\nasync function loadStats(){\n  if(!_users.length)await loadUsers();\n  var h=mkt.history||[];var p=parseFloat(mkt.price||0);\n  if(h.length>5){var cv=$(\'ch-vol\');if(cv&&window.Chart){var pts=h.slice(-20);var labs=pts.map(x=>new Date(x.ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\'}));var vols=pts.map(x=>x.vol||0);if(volObj){volObj.data.labels=labs;volObj.data.datasets[0].data=vols;volObj.update(\'none\');}else{var ctx=cv.getContext(\'2d\');volObj=new Chart(ctx,{type:\'bar\',data:{labels:labs,datasets:[{data:vols,backgroundColor:\'rgba(160,107,255,.5)\',borderColor:\'#a06bff\',borderWidth:1}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:4,font:{size:8}},grid:{display:false}},y:{ticks:{color:\'#5c6b8c\'},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});}}}\n  var el=$(\'rew-bars\');if(el&&_users.length){var maxR=Math.max(..._users.map(u=>u.rew))||1;el.innerHTML=[..._users].sort((a,b)=>b.rew-a.rew).slice(0,8).map(u=>\'<div style="margin-bottom:8px"><div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:3px"><span style="color:var(--cyan);font-weight:700">\'+esc(u.n)+\'</span><span style="color:var(--gold)">\'+fmt(u.rew,0)+\' R</span></div><div class="pbar"><div class="pbar-fill" style="width:\'+Math.round(u.rew/maxR*100)+\'%"></div></div></div>\').join(\'\');}\n  var hi=h.length>1?Math.max(...h.slice(-24).map(x=>x.price)):p;var lo=h.length>1?Math.min(...h.slice(-24).map(x=>x.price)):p;var vol=lo>0?(hi-lo)/lo*100:0;\n  var hg=$(\'health-grid\');if(hg)hg.innerHTML=[[\'📈 Tendance\',h.length>5?(h.slice(-5).map(x=>x.price).every((v,i,a)=>i===0||v>a[i-1])?\'<span style="color:var(--green)">Haussière</span>\':h.slice(-5).map(x=>x.price).every((v,i,a)=>i===0||v<a[i-1])?\'<span style="color:var(--red)">Baissière</span>\':\'<span style="color:var(--muted)">Neutre</span>\'):\'—\'],[\'⚡ Volatilité\',vol.toFixed(2)+\'%\'],[\'📊 Amplitude\',fmt(hi-lo,0)+\' R\'],[\'🔢 Trades\',mkt.trades24||0]].map(([k,v])=>\'<div class="st"><div class="sv" style="font-size:12px">\'+v+\'</div><div class="sl">\'+k+\'</div></div>\').join(\'\');\n}\n\n// SOLVABILITÉ\nasync function loadSolv(){try{var r=await fetch(\'/nxc/solvability\');var d=await r.json();if(d.ok){solvOn=d.enabled;var inp=$(\'sg\');if(inp)inp.value=d.gesture||50;updSolv();}}catch(e){}}\nfunction updSolv(){var t=$(\'stg\'),l=$(\'sl\');if(solvOn){if(t)t.classList.add(\'on\');if(l){l.textContent=\'✅ Activée\';l.style.color=\'var(--green)\';}}else{if(t)t.classList.remove(\'on\');if(l){l.textContent=\'⏸ Désactivée\';l.style.color=\'var(--muted)\';}}}\nasync function toggleSolv(){solvOn=!solvOn;updSolv();await saveSolv();}\nasync function saveSolv(){var g=parseInt($(\'sg\').value)||50;var r=await fetch(\'/nxc/solvability\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,enabled:solvOn,gesture:g})});var res=await r.json();setMsg(\'sm\',res.ok?(solvOn?\'✅ Activée\':\'⏸ Désactivée\'):\'❌ Erreur\',res.ok);if(res.ok)addLog(\'🛡️\',\'Solvabilité \'+(solvOn?\'activée\':\'désactivée\'));}\n\n// OUTILS\nvar _noiseLevel=0.004;\nfunction updateNoise(v){\n  _noiseLevel=parseFloat(v)/1000;\n  var el=$(\'noise-val\');if(el)el.textContent=(parseFloat(v)/10).toFixed(1)+\'%\';\n}\nfunction calcN(){var n=parseFloat($(\'c-nxc\').value)||0;var p=parseFloat(mkt.price||0);$(\'c-rew\').value=n&&p?Math.round(n*p*100)/100:\'\';}\nfunction calcR(){var r=parseFloat($(\'c-rew2\').value)||0;var p=parseFloat(mkt.price||1);$(\'c-nxc2\').value=r&&p?(r/p).toFixed(6):\'\';}\nfunction simS(){var n=parseFloat($(\'ss-nxc\').value)||0;var fee=parseFloat($(\'ss-fee\').value)||0;var p=parseFloat(mkt.price||0);if(!n||!p){$(\'ss-res\').innerHTML=\'\';return;}var gross=n*p;var fees=gross*fee/100;var net=gross-fees;$(\'ss-res\').innerHTML=\'Brut: <b style="color:var(--text)">\'+fmt(gross,2)+\' R</b> · Frais: <b style="color:var(--red)">-\'+fmt(fees,2)+\' R</b> · <b style="color:var(--green);font-size:16px">Net: \'+fmt(net,2)+\' R</b>\';}\n\nvar _tmEnd=null;\nfunction startTimer(){var m=parseInt($(\'tm-m\').value)||0;var s=parseInt($(\'tm-s\').value)||0;var total=m*60+s;var action=$(\'tm-a\').value;if(!total)return;if(_tmInt)clearInterval(_tmInt);_tmEnd=Date.now()+total*1000;addLog(\'⏱️\',\'Minuteur: \'+action+\' dans \'+total+\'s\');_tmInt=setInterval(async function(){var rem=Math.max(0,Math.round((_tmEnd-Date.now())/1000));var el=$(\'tm-disp\');if(el)el.textContent=(\'0\'+Math.floor(rem/60)).slice(-2)+\':\'+(\'0\'+(rem%60)).slice(-2);if(rem<=0){clearInterval(_tmInt);_tmInt=null;if(el){el.textContent=\'✅\';el.style.color=\'var(--green)\';}if(action===\'stop\')setT(\'stop\');else if(action===\'up\'||action===\'down\')setT(action);else if(action===\'crash\'||action===\'moon\')scenario(action);addLog(\'⏱️\',\'Minuteur déclenché: \'+action);}},500);}\nfunction stopTimer(){if(_tmInt){clearInterval(_tmInt);_tmInt=null;var d=$(\'tm-disp\');if(d)d.textContent=\'\';}}\n\n// CONFIG\nfunction updCfg(){\n  var txt=\'Plancher: \'+(_cfgFloor?fmt(_cfgFloor,0)+\' R\':\'non défini\')+\' · Plafond: \'+(_cfgCeil?fmt(_cfgCeil,0)+\' R\':\'non défini\');\n  var el=$(\'cfg-info\');if(el)el.textContent=txt;\n  updFloorDisplay();\n}\nfunction updFloorDisplay(){\n  var txt=\'Plancher: \'+(_cfgFloor?fmt(_cfgFloor,0)+\' R\':\'non défini\')+\' · Plafond: \'+(_cfgCeil?fmt(_cfgCeil,0)+\' R\':\'non défini\');\n  var el=$(\'floor-display\');if(el)el.textContent=txt;\n  var ec=$(\'cfg-info\');if(ec)ec.textContent=txt;\n}\nfunction setFloor(){\n  var v=parseFloat($(\'t-floor\').value);if(!v||v<50){alert(\'Plancher invalide (min 50R)\');return;}\n  _cfgFloor=v;updFloorDisplay();addLog(\'⚙️\',\'Plancher: \'+fmt(v,0)+\' R\');\n}\nfunction setCeil(){\n  var v=parseFloat($(\'t-ceil\').value);if(!v||v>100000){alert(\'Plafond invalide (max 100 000R)\');return;}\n  _cfgCeil=v;updFloorDisplay();addLog(\'⚙️\',\'Plafond: \'+fmt(v,0)+\' R\');\n}\nfunction setNormalMode(){\n  // Cours normal = tendance aléatoire légère avec plancher/plafond actifs\n  if(!_cfgFloor&&!_cfgCeil){alert(\'Définir au moins un plancher ou un plafond\');return;}\n  setT(\'stop\'); // Arrêter toute tendance\n  // Lancer une légère variation aléatoire neutre\n  var iv=parseInt($(\'ti\').value)||12000;\n  if(tInt){clearInterval(tInt);tInt=null;}\n  tMode=\'normal\';\n  var el=$(\'tst\');el.textContent=\'📊 Cours normal · plancher: \'+(_cfgFloor?fmt(_cfgFloor,0)+\'R\':\'—\')+\' · plafond: \'+(_cfgCeil?fmt(_cfgCeil,0)+\'R\':\'—\');el.style.color=\'var(--cyan)\';\n  addLog(\'📊\',\'Cours normal activé\');\n  tInt=setInterval(async function(){\n    var p=parseFloat(mkt.price||5213);\n    var adj=(Math.random()-0.5)*_noiseLevel*0.5; // très légère variation\n    p=Math.max(_cfgFloor||50,Math.min(_cfgCeil||100000,p*(1+adj)));\n    p=Math.round(p*100)/100;\n    await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:Math.floor(Math.random()*100+10),volume24:(mkt.volume24||0)+50,trades24:(mkt.trades24||0)+1})});\n  },iv);\n}\nfunction scheduleT(){var st=$(\'cfg-st\').value,sp=$(\'cfg-sp\').value,dir=$(\'cfg-sd\').value;if(!st||!sp){setMsg(\'cfg-sch-msg\',\'Renseigner les deux heures\',false);return;}if(_schedInt)clearInterval(_schedInt);_schedInt=setInterval(function(){var now=new Date();var cur=(\'0\'+now.getHours()).slice(-2)+\':\'+(\'0\'+now.getMinutes()).slice(-2);if(cur===st&&!tMode)setT(dir);if(cur===sp&&tMode)setT(\'stop\');},30000);setMsg(\'cfg-sch-msg\',\'✅ Programmé: \'+dir+\' \'+st+\'→\'+sp,true);addLog(\'⏰\',\'Tendance programmée \'+dir+\' \'+st+\'→\'+sp);}\n\nfunction exportHist(){var h=mkt.history||[];var b=new Blob([JSON.stringify({date:new Date().toISOString(),price:mkt.price,history:h},null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(b);a.download=\'nxc_history_\'+Date.now()+\'.json\';a.click();addLog(\'📥\',\'Export historique JSON\');}\nfunction exportStats(){var b=new Blob([JSON.stringify({date:new Date().toISOString(),market:mkt,users:_users},null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(b);a.download=\'nxc_report_\'+Date.now()+\'.json\';a.click();addLog(\'📊\',\'Export rapport JSON\');}\n\n// ALERTES\nfunction addAlert(){var price=parseFloat($(\'al-p\').value),dir=$(\'al-d\').value;if(!price)return;_alerts.push({price,dir,id:Date.now(),triggered:false});$(\'al-p\').value=\'\';renderAlerts();addLog(\'🔔\',\'Alerte: prix \'+(dir===\'above\'?\'>\':\'<\')+\' \'+fmt(price,0)+\' R\');}\nfunction removeAlert(id){_alerts=_alerts.filter(a=>a.id!==id);renderAlerts();}\nfunction renderAlerts(){var el=$(\'al-list\');if(!el)return;el.innerHTML=_alerts.length?_alerts.map(a=>\'<div style="padding:10px 12px;border-bottom:1px solid rgba(0,229,255,.05);display:flex;justify-content:space-between;align-items:center;font-size:12px"><span style="color:\'+(a.triggered?\'var(--muted)\':\'var(--gold)\')+\'">Prix \'+(a.dir===\'above\'?\'>\':\'<\')+\' \'+fmt(a.price,0)+\' R\'+(a.triggered?\' ✅\':\'\')+\'</span><button onclick="removeAlert(\'+a.id+\')" style="padding:4px 8px;border-radius:6px;background:rgba(255,61,94,.1);border:1px solid rgba(255,61,94,.3);color:var(--red);font-size:10px;cursor:pointer">✕</button></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune alerte</p>\';}\nfunction checkAlerts(p){_alerts.forEach(function(a){if(a.triggered)return;if((a.dir===\'above\'&&p>a.price)||(a.dir===\'below\'&&p<a.price)){a.triggered=true;var m=\'🔔 Prix \'+(a.dir===\'above\'?\'>\':\'<\')+\' \'+fmt(a.price,0)+\' R (actuel: \'+fmt(p,0)+\' R)\';_alHist.unshift({ts:Date.now(),msg:m});addLog(\'🔔\',m);renderAlerts();renderAlHist();if(window.Notification&&Notification.permission===\'granted\')new Notification(\'◈ Nexus NXC\',{body:m});}});}\nfunction renderAlHist(){var el=$(\'al-hist\');if(!el)return;el.innerHTML=_alHist.length?_alHist.map(a=>\'<div class="log-item"><span class="log-time">\'+fmtT(a.ts)+\'</span><span style="color:var(--gold)">\'+esc(a.msg)+\'</span></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune</p>\';}\nif(window.Notification&&Notification.permission===\'default\')setTimeout(function(){Notification.requestPermission();},3000);\n\n// ══ ÉPINGLAGE SITES (sync cross-device via serveur) ══\nvar _pinnedSites=[];\n\nasync function loadPinnedSites(){\n  try{\n    var r=await fetch(\'/admin/pinned-sites\');var d=await r.json();\n    if(d.ok){_pinnedSites=d.sites||[];renderSavedSites();}\n  }catch(e){_pinnedSites=JSON.parse(localStorage.getItem(\'nxc_pinned\')||\'[]\');}\n}\n\nasync function togglePin(url,label){\n  var idx=_pinnedSites.findIndex(s=>s.url===url);\n  if(idx>=0)_pinnedSites.splice(idx,1);\n  else _pinnedSites.push({url,label});\n  // Sauvegarder sur le serveur ET en local\n  localStorage.setItem(\'nxc_pinned\',JSON.stringify(_pinnedSites));\n  try{await fetch(\'/admin/pinned-sites\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,sites:_pinnedSites})});}catch(e){}\n  renderSavedSites();\n  addLog(\'📌\',(idx>=0?\'Désépinglé\':\'Épinglé\')+\': \'+label);\n}\n\nfunction renderPinnedBar(){\n  var el=$(\'pinned-bar\');if(!el)return;\n  if(!_pinnedSites.length){el.style.display=\'none\';return;}\n  el.style.display=\'flex\';\n  el.innerHTML=_pinnedSites.map(s=>\'<button onclick="loadSite(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" style="padding:5px 12px;background:rgba(255,176,32,.12);border:1px solid rgba(255,176,32,.3);border-radius:8px;color:var(--gold);font-size:11px;font-weight:700;cursor:pointer;white-space:nowrap">📌 \'+esc(s.label)+\'</button>\').join(\'\');\n}\n\n// ══ SAUVEGARDE / IMPORT DONNÉES GLOBALES ══\nasync function saveAllData(){\n  try{\n    var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'export\'})});\n    var d=await r.json();\n    if(!d.ok){setMsg(\'data-msg\',\'❌ Erreur export\',false);return;}\n    var blob=new Blob([JSON.stringify(d.data,null,2)],{type:\'application/json\'});\n    var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_full_backup_\'+Date.now()+\'.json\';a.click();\n    setMsg(\'data-msg\',\'✅ Backup complet téléchargé\',true);\n    addLog(\'💾\',\'Sauvegarde complète téléchargée\');\n  }catch(e){setMsg(\'data-msg\',\'❌ Erreur: \'+e.message,false);}\n}\n\nfunction importData(){\n  var input=document.createElement(\'input\');input.type=\'file\';input.accept=\'.json\';\n  input.onchange=async function(e){\n    var file=e.target.files[0];if(!file)return;\n    var text=await file.text();\n    try{\n      var data=JSON.parse(text);\n      if(!confirm(\'Importer ces données ? Cela écrasera les données actuelles du serveur.\'))return;\n      var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'import\',data:data})});\n      var res=await r.json();\n      setMsg(\'data-msg\',res.ok?\'✅ Données importées avec succès\':\'❌ \'+(res.error||\'Erreur import\'),res.ok);\n      if(res.ok){addLog(\'📥\',\'Données importées depuis fichier\');setTimeout(function(){ref();loadBank();},1000);}\n    }catch(ex){setMsg(\'data-msg\',\'❌ Fichier JSON invalide\',false);}\n  };\n  input.click();\n}\n\n// ══ IMPRESSION ══\nfunction printDashboard(){\n  var p=parseFloat(mkt.price||0);var h=mkt.history||[];\n  var hi=h.length>1?Math.max(...h.slice(-24).map(x=>x.price)):p;\n  var lo=h.length>1?Math.min(...h.slice(-24).map(x=>x.price)):p;\n  var chg=_prevP>0?((p-_prevP)/_prevP*100):0;\n  // Capturer le graphique en PNG\n  var chartImg=\'\';var cv=$(\'ch\');if(cv)chartImg=cv.toDataURL(\'image/png\');\n  var rsiImg=\'\';var rsiCv=$(\'ch-rsi\');if(rsiCv)rsiImg=rsiCv.toDataURL(\'image/png\');\n  var now=new Date().toLocaleString(\'fr-FR\');\n  var win=window.open(\'\',\'_blank\');\n  win.document.write(\'<!DOCTYPE html><html><head><meta charset="utf-8"><title>◈ Nexus NXC — Rapport \'+now+\'</title><style>*{font-family:Arial,sans-serif;box-sizing:border-box}body{background:#fff;color:#000;padding:20px;max-width:900px;margin:0 auto}.header{text-align:center;border-bottom:3px solid #000;padding-bottom:16px;margin-bottom:20px}.title{font-size:28px;font-weight:900;letter-spacing:3px}.date{font-size:12px;color:#666;margin-top:4px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}.stat{border:1px solid #ddd;border-radius:8px;padding:12px;text-align:center}.stat-val{font-size:20px;font-weight:700;margin-bottom:4px}.stat-lbl{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:#666}img{max-width:100%;border:1px solid #ddd;border-radius:8px;margin-bottom:12px}h3{margin:16px 0 8px;font-size:14px;border-bottom:1px solid #eee;padding-bottom:4px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:8px;text-align:left;border:1px solid #ddd}th{background:#f5f5f5;font-weight:700}@media print{.no-print{display:none}}</style></head><body>\');\n  win.document.write(\'<div class="header"><div class="title">◈ NEXUS NXC</div><div class="date">Rapport généré le \'+now+\'</div></div>\');\n  win.document.write(\'<div class="grid">\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(p,2)+\' R</div><div class="stat-lbl">Prix actuel</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%</div><div class="stat-lbl">Variation</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(hi,0)+\' R</div><div class="stat-lbl">Haut 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(lo,0)+\' R</div><div class="stat-lbl">Bas 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(mkt.volume24||0,0)+\' R</div><div class="stat-lbl">Volume 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(mkt.trades24||0)+\'</div><div class="stat-lbl">Trades 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+h.length+\'</div><div class="stat-lbl">Points hist.</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(_users.length||0)+\'</div><div class="stat-lbl">Utilisateurs</div></div>\');\n  win.document.write(\'</div>\');\n  if(chartImg)win.document.write(\'<h3>Historique du cours (\'+_ctRange+\' derniers points)</h3><img src="\'+chartImg+\'">\');\n  if(rsiImg)win.document.write(\'<h3>RSI (14 ticks)</h3><img src="\'+rsiImg+\'">\');\n  if(_users.length){\n    win.document.write(\'<h3>Utilisateurs</h3><table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur (R)</th></tr></thead><tbody>\');\n    _users.forEach(u=>{win.document.write(\'<tr><td>\'+esc(u.n)+\'</td><td>\'+esc(u.role)+\'</td><td>\'+fmt(u.rew,0)+\'</td><td>\'+u.nxc.toFixed(4)+\'</td><td>\'+fmt(u.val,0)+\'</td></tr>\');});\n    win.document.write(\'</tbody></table>\');\n  }\n  win.document.write(\'<h3>Derniers logs</h3><table><thead><tr><th>Heure</th><th>Action</th></tr></thead><tbody>\');\n  _log.slice(0,20).forEach(l=>{win.document.write(\'<tr><td>\'+fmtT(l.ts)+\'</td><td>\'+l.ico+\' \'+esc(l.txt)+\'</td></tr>\');});\n  win.document.write(\'</tbody></table>\');\n    var bR=document.getElementById(\'bk-r\')?document.getElementById(\'bk-r\').textContent:\'—\';\n  var bI=document.getElementById(\'bk-i\')?document.getElementById(\'bk-i\').textContent:\'—\';\n  var bO=document.getElementById(\'bk-o\')?document.getElementById(\'bk-o\').textContent:\'—\';\n  var bRt=document.getElementById(\'bk-rt\')?document.getElementById(\'bk-rt\').textContent:\'—\';\n  var bNx=document.getElementById(\'bk-nx\')?document.getElementById(\'bk-nx\').textContent:\'—\';\n  var bVx=document.getElementById(\'bk-vx\')?document.getElementById(\'bk-vx\').textContent:\'—\';\n  var bBn=document.getElementById(\'bk-bn\')?document.getElementById(\'bk-bn\').textContent:\'—\';\n  var bFl=document.getElementById(\'bk-fl\')?document.getElementById(\'bk-fl\').textContent:\'—\';\n  win.document.write(\'<hr style="margin:20px 0;border:none;border-top:2px solid #6366f1">\')\n  win.document.write(\'<h2 style="font-family:monospace;color:#6366f1;margin:0 0 12px">◈ BANQUE NXC</h2>\')\n  win.document.write(\'<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px">\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Réserves</div><div style="font-weight:bold">\'+ bR +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Entrées</div><div style="font-weight:bold">\'+ bI +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Sorties</div><div style="font-weight:bold">\'+ bO +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Taux</div><div style="font-weight:bold">\'+ bRt +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">NXC circ.</div><div style="font-weight:bold">\'+ bNx +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Valeur NXC</div><div style="font-weight:bold">\'+ bVx +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Billets</div><div style="font-weight:bold">\'+ bBn +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Flux total</div><div style="font-weight:bold">\'+ bFl +\'</div></div>\')\n  win.document.write(\'</div>\')\n  if(typeof _flux!==\'undefined\'&&_flux&&_flux.length>0){\n    win.document.write(\'<h3 style="font-family:monospace;margin:0 0 8px">Flux récents</h3>\')\n    win.document.write(\'<table style="width:100%;border-collapse:collapse;font-size:11px"><thead><tr style="background:#6366f1;color:#fff"><th style="padding:4px 8px;text-align:left">Date</th><th>Type</th><th>Utilisateur</th><th>Montant</th><th>Solde</th></tr></thead><tbody>\')\n    _flux.slice(0,50).forEach(function(f){\n      var fd=new Date(f.ts).toLocaleString(\'fr-FR\');\n      win.document.write(\'<tr style="border-bottom:1px solid #eee"><td style="padding:3px 8px">\'+fd+\'</td><td>\'+esc(f.type||\'\')+ \'</td><td>\'+esc(f.user||\'\')+ \'</td><td>\'+esc(String(f.amount||\'\'))+ \'</td><td>\'+esc(String(f.balance||\'\'))+ \'</td></tr>\')\n    });\n    win.document.write(\'</tbody></table>\')\n  }\n  win.document.write(\'</body></html>\');\n  win.document.close();\n  setTimeout(function(){win.print();},500);\n  addLog(\'🖨️\',\'Impression du tableau de bord\');\n}\n\n\n// ══ CYCLES DE MARCHÉ ══\nvar _cy={absmin:null,absmax:null,active:false,int:null,phase:\'normal\',phaseStart:Date.now(),holdUntil:0};\nvar _cyPreviewObj=null;\n\nfunction setCyVal(key){\n  var v=parseFloat($(\'cy-\'+key).value);\n  if(isNaN(v)||v<=0)return;\n  _cy[key]=v;\n  var el=$(\'cy-\'+key+\'-disp\');if(el)el.textContent=fmt(v,0)+\' R\';\n  // Sync avec _cfgFloor/_cfgCeil\n  if(key===\'absmin\'){_cfgFloor=v;updFloorDisplay();}\n  if(key===\'absmax\'){_cfgCeil=v;updFloorDisplay();}\n  addLog(\'📅\',\'Borne \'+key+\': \'+fmt(v,0)+\' R\');\n}\n\nfunction getCyConfig(){\n  return {\n    absmin: _cy.absmin||parseFloat($(\'cy-absmin\').value)||50,\n    absmax: _cy.absmax||parseFloat($(\'cy-absmax\').value)||100000,\n    transition: $(\'cy-transition\').value,\n    holdMin: parseFloat($(\'cy-hold-min\').value)||1,\n    holdMax: parseFloat($(\'cy-hold-max\').value)||3,\n    holdUnit: parseFloat($(\'cy-hold-unit\').value)||60,\n    drift: parseFloat($(\'cy-drift\').value)/100/1440,\n    volBg: parseFloat($(\'cy-vol-bg\').value)/1000,\n    spikeProb: parseFloat($(\'cy-spike\').value)/100,\n    spikeAmp: parseFloat($(\'cy-spike-amp\').value)/100,\n    bounce: parseFloat($(\'cy-bounce\').value)/100,\n    resist: parseFloat($(\'cy-resist\').value)/100,\n    // Fréquences par période → probabilité par tick (tick = 12s)\n    freqMin: {\n      m: parseFloat($(\'cy-min-m\').value)||0,\n      h: parseFloat($(\'cy-min-h\').value)||1,\n      d: parseFloat($(\'cy-min-d\').value)||1,\n      w: parseFloat($(\'cy-min-w\').value)||1,\n      mo: parseFloat($(\'cy-min-mo\').value)||2,\n      y: parseFloat($(\'cy-min-y\').value)||4,\n    },\n    freqMax: {\n      m: parseFloat($(\'cy-max-m\').value)||0,\n      h: parseFloat($(\'cy-max-h\').value)||1,\n      d: parseFloat($(\'cy-max-d\').value)||1,\n      w: parseFloat($(\'cy-max-w\').value)||1,\n      mo: parseFloat($(\'cy-max-mo\').value)||2,\n      y: parseFloat($(\'cy-max-y\').value)||4,\n    },\n  };\n}\n\nfunction calcProbPerTick(freqObj){\n  // Convertir les fréquences en probabilité par tick (12s)\n  var ticksPerMin=5,ticksPerH=300,ticksPerD=7200,ticksPerW=50400,ticksPerMo=216000,ticksPerY=2628000;\n  var pMin=freqObj.m/ticksPerMin+freqObj.h/ticksPerH+freqObj.d/ticksPerD+freqObj.w/ticksPerW+freqObj.mo/ticksPerMo+freqObj.y/ticksPerY;\n  return Math.min(pMin,0.5); // max 50% par tick\n}\n\nfunction startCycles(){\n  var cfg=getCyConfig();\n  if(cfg.absmin>=cfg.absmax){alert(\'Le plancher doit être inférieur au plafond\');return;}\n  _cy.active=true;_cy.phase=\'normal\';_cy.holdUntil=0;\n  $(\'cy-start-btn\').style.display=\'none\';$(\'cy-stop-btn\').style.display=\'block\';\n  var iv=parseInt($(\'ti\').value)||12000;\n  if(tInt){clearInterval(tInt);tInt=null;}\n  tMode=\'cycles\';\n  var el=$(\'tst\');el.textContent=\'📅 Cycles actifs · \'+fmt(cfg.absmin,0)+\'R – \'+fmt(cfg.absmax,0)+\'R\';el.style.color=\'var(--cyan)\';\n  addLog(\'📅\',\'Cycles de marché activés\');\n\n  var pToMin=calcProbPerTick(cfg.freqMin);\n  var pToMax=calcProbPerTick(cfg.freqMax);\n\n  _cy.int=setInterval(async function(){\n    var p=parseFloat(mkt.price||5213);\n    var now=Date.now();\n    var adj=0;\n\n    // Drift de fond\n    adj+=cfg.drift;\n    // Volatilité de fond\n    adj+=(Math.random()-0.5)*cfg.volBg*2;\n\n    // Pics surprises\n    if(Math.random()<cfg.spikeProb){\n      var dir=Math.random()>0.5?1:-1;\n      adj+=dir*cfg.spikeAmp*(Math.random()*0.5+0.5);\n      addLog(\'⚡\',\'Pic surprise: \'+(dir>0?\'+\':\'\')+((adj*100).toFixed(1))+\'%\');\n    }\n\n    // Gestion des phases\n    if(now<_cy.holdUntil){\n      // Maintien en position (min ou max)\n      if(_cy.phase===\'atmin\')adj=Math.max(0,(Math.random()-0.3)*0.001);\n      if(_cy.phase===\'atmax\')adj=Math.min(0,(Math.random()-0.7)*0.001);\n    } else {\n      // Décider si on va vers le min ou le max\n      if(_cy.phase!==\'tomin\'&&_cy.phase!==\'tomax\'){\n        var goMin=Math.random()<pToMin;\n        var goMax=Math.random()<pToMax;\n        if(goMin&&!goMax){_cy.phase=\'tomin\';addLog(\'📅\',\'Cycle → minimum\');}\n        else if(goMax&&!goMin){_cy.phase=\'tomax\';addLog(\'📅\',\'Cycle → maximum\');}\n        else _cy.phase=\'normal\';\n      }\n      if(_cy.phase===\'tomin\'){\n        // Descente vers le min\n        var distRatio=(p-cfg.absmin)/(cfg.absmax-cfg.absmin);\n        var force=cfg.transition===\'brutal\'?-0.1:cfg.transition===\'sinusoide\'?-Math.sin(distRatio*Math.PI)*0.02:-0.01;\n        adj+=force*(1+cfg.bounce);\n        if(p<=cfg.absmin*1.01){_cy.phase=\'atmin\';var holdSec=(cfg.holdMin+Math.random()*(cfg.holdMax-cfg.holdMin))*cfg.holdUnit;_cy.holdUntil=now+holdSec*1000;addLog(\'📅\',\'Cycle: minimum atteint · maintien \'+(holdSec/60).toFixed(0)+\'min\');}\n      }\n      if(_cy.phase===\'tomax\'){\n        // Montée vers le max\n        var distRatio=(cfg.absmax-p)/(cfg.absmax-cfg.absmin);\n        var force=cfg.transition===\'brutal\'?0.1:cfg.transition===\'sinusoide\'?Math.sin(distRatio*Math.PI)*0.02:0.01;\n        adj+=force*(1+cfg.resist);\n        if(p>=cfg.absmax*0.99){_cy.phase=\'atmax\';var holdSec=(cfg.holdMin+Math.random()*(cfg.holdMax-cfg.holdMin))*cfg.holdUnit;_cy.holdUntil=now+holdSec*1000;addLog(\'📅\',\'Cycle: maximum atteint · maintien \'+(holdSec/60).toFixed(0)+\'min\');}\n      }\n    }\n\n    // Résistance aux bornes\n    if(p<cfg.absmin*1.05)adj+=cfg.bounce*0.05;\n    if(p>cfg.absmax*0.95)adj-=cfg.resist*0.05;\n\n    p=Math.max(cfg.absmin,Math.min(cfg.absmax,p*(1+adj)));\n    p=Math.round(p*100)/100;\n\n    // Mise à jour du statut\n    var rem=Math.max(0,Math.round((_cy.holdUntil-now)/1000));\n    var statusTxt=\'Phase: \'+_cy.phase+(_cy.holdUntil>now?\' · maintien encore \'+rem+\'s\':\'\')+\' · P(min)/tick: \'+(pToMin*100).toFixed(2)+\'% · P(max)/tick: \'+(pToMax*100).toFixed(2)+\'%\';\n    var st=$(\'cy-status\');if(st)st.textContent=statusTxt;\n\n    await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:Math.floor(Math.random()*200+20),volume24:(mkt.volume24||0)+80,trades24:(mkt.trades24||0)+1})});\n  },iv);\n}\n\nfunction stopCycles(){\n  _cy.active=false;_cy.phase=\'normal\';\n  if(_cy.int){clearInterval(_cy.int);_cy.int=null;}\n  if(tMode===\'cycles\'){tMode=null;tInt=null;}\n  $(\'cy-start-btn\').style.display=\'block\';$(\'cy-stop-btn\').style.display=\'none\';\n  var el=$(\'cy-status\');if(el)el.textContent=\'Cycles désactivés\';\n  var el2=$(\'tst\');if(el2){el2.textContent=\'⏸ Arrêté\';el2.style.color=\'var(--muted)\';}\n  addLog(\'📅\',\'Cycles désactivés\');\n}\n\nfunction previewCycle(){\n  var cfg=getCyConfig();var cv=$(\'cy-preview\');if(!cv||!window.Chart)return;\n  if(_cyPreviewObj){_cyPreviewObj.destroy();_cyPreviewObj=null;}\n  var pts=[];var p=(cfg.absmin+cfg.absmax)/2;\n  var pMin=calcProbPerTick(cfg.freqMin);var pMax=calcProbPerTick(cfg.freqMax);\n  var phase=\'normal\';var holdUntil=0;\n  for(var t=0;t<100;t++){\n    var adj=(Math.random()-0.5)*cfg.volBg*2+cfg.drift;\n    if(Math.random()<cfg.spikeProb)adj+=(Math.random()>0.5?1:-1)*cfg.spikeAmp*Math.random();\n    if(t>holdUntil){\n      if(phase!==\'tomin\'&&phase!==\'tomax\'){\n        if(Math.random()<pMin)phase=\'tomin\';\n        else if(Math.random()<pMax)phase=\'tomax\';\n        else phase=\'normal\';\n      }\n      if(phase===\'tomin\'){adj-=0.01*(1+cfg.bounce);if(p<=cfg.absmin*1.01){phase=\'atmin\';holdUntil=t+3;}}\n      if(phase===\'tomax\'){adj+=0.01*(1+cfg.resist);if(p>=cfg.absmax*0.99){phase=\'atmax\';holdUntil=t+3;}}\n    }\n    p=Math.max(cfg.absmin,Math.min(cfg.absmax,p*(1+adj)));\n    pts.push(Math.round(p*100)/100);\n  }\n  var labs=pts.map((_,i)=>\'T\'+i);\n  var ctx=cv.getContext(\'2d\');\n  var g=ctx.createLinearGradient(0,0,0,150);g.addColorStop(0,\'rgba(0,229,255,.2)\');g.addColorStop(1,\'rgba(0,229,255,0)\');\n  _cyPreviewObj=new Chart(ctx,{type:\'line\',data:{labels:labs,datasets:[\n    {data:pts,borderColor:\'#00e5ff\',backgroundColor:g,borderWidth:2,pointRadius:0,fill:true,tension:0.3},\n    {data:Array(100).fill(cfg.absmin),borderColor:\'rgba(0,255,157,.4)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[4,4]},\n    {data:Array(100).fill(cfg.absmax),borderColor:\'rgba(255,61,94,.4)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[4,4]},\n  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{display:false},y:{ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n}\n\n\n// ══ INFOS BULLES ══\nvar _infos={\n  bornes:"Les bornes sont les limites absolues du prix NXC. Le prix ne pourra jamais descendre en dessous du minimum ni monter au-dessus du maximum, quoi qu\'il arrive.",\n  freq:"Définit combien de fois le prix touchera exactement son minimum ou maximum dans chaque période. Le moteur calcule automatiquement la probabilité par tick (intervalle de 12s par défaut) pour respecter ces fréquences.",\n  "freq-min":"Par minute : combien de fois dans la prochaine minute le prix touchera son minimum (colonne verte) ou maximum (colonne rouge). 0 = jamais dans la minute.",\n  "freq-h":"Par heure : combien de fois dans la prochaine heure le prix touchera son minimum ou maximum. Ex: 2 = deux fois dans l\'heure.",\n  "freq-d":"Par jour : combien de fois dans les 24 prochaines heures le prix touchera son minimum ou maximum.",\n  "freq-w":"Par semaine : combien de fois dans les 7 prochains jours le prix touchera son minimum ou maximum.",\n  "freq-mo":"Par mois (30 jours) : combien de fois dans le mois le prix touchera son minimum ou maximum.",\n  "freq-y":"Par an (365 jours) : combien de fois dans l\'année le prix touchera son minimum ou maximum. Ex: 4 = une fois par trimestre.",\n  "freq-custom":"Durée personnalisée : définir une période sur mesure. Ex: 6 heures, 2 jours... et combien de fois le prix touchera les extrêmes dans cette durée.",\n  comportement:"Paramètres qui définissent comment le prix se comporte quand il se déplace vers un extrême.",\n  transition:"Comment le prix atteint le min ou le max. Brutal = saut instantané. Progressif = descente/montée sur plusieurs ticks. Sinusoïde = courbe douce et naturelle.",\n  hold:"Combien de temps le prix reste au minimum ou maximum avant de repartir. Une durée aléatoire entre Min et Max est choisie à chaque fois.",\n  drift:"Tendance de fond sur le long terme. +2%/j = le prix a une légère tendance à monter de 2% par jour en moyenne. 0 = aucune tendance.",\n  volbg:"Quantité de mouvement aléatoire à chaque tick, indépendant des cycles. 0% = prix totalement lisse entre les cycles. Plus élevé = plus de micro-variations.",\n  spike:"Probabilité qu\'un pic inattendu se produise à chaque tick. Ex: 5% = 1 chance sur 20 à chaque tick d\'avoir un mouvement brutal.",\n  spikeamp:"Amplitude maximale d\'un pic surprise. ±10% = le pic peut faire bouger le prix de jusqu\'à 10% instantanément.",\n  bounce:"Force du rebond quand le prix touche le plancher. 0% = s\'arrête exactement au plancher. 5% = rebondit légèrement vers le haut.",\n  resist:"Résistance quand le prix approche du plafond. 0% = monte jusqu\'au plafond facilement. 5% = plus difficile de dépasser le plafond.",\n  activation:"Active le moteur de cycles. Une fois activé, le prix suivra automatiquement les fréquences définies pour atteindre les extrêmes.",\n  preview:"Simule 100 ticks avec les paramètres actuels pour voir à quoi ressemblera le comportement du prix avant de l\'activer."\n};\n\nfunction showInfo(key){\n  var modal=$(\'info-modal\');if(!modal)return;\n  $(\'info-title\').textContent=\'ℹ️ \'+key.replace(/-/g,\' \').replace(/\\b\\w/g,c=>c.toUpperCase());\n  $(\'info-body\').textContent=_infos[key]||\'Information non disponible.\';\n  modal.style.display=\'flex\';\n}\n\n// ══ PROBABILITÉS PAR TICK ══\nfunction updateCyProb(){\n  var ticksPerMin=5,ticksPerH=300,ticksPerD=7200,ticksPerW=50400,ticksPerMo=216000,ticksPerY=2628000;\n  var customDur=parseFloat($(\'cy-custom-dur\').value)||0;\n  var customUnit=parseFloat($(\'cy-custom-unit\').value)||3600000;\n  var customMs=customDur*customUnit;\n  var customTicks=customMs/12000;\n\n  var freqMin={m:parseFloat($(\'cy-min-m\').value)||0,h:parseFloat($(\'cy-min-h\').value)||0,d:parseFloat($(\'cy-min-d\').value)||0,w:parseFloat($(\'cy-min-w\').value)||0,mo:parseFloat($(\'cy-min-mo\').value)||0,y:parseFloat($(\'cy-min-y\').value)||0,c:parseFloat($(\'cy-min-c\').value)||0};\n  var freqMax={m:parseFloat($(\'cy-max-m\').value)||0,h:parseFloat($(\'cy-max-h\').value)||0,d:parseFloat($(\'cy-max-d\').value)||0,w:parseFloat($(\'cy-max-w\').value)||0,mo:parseFloat($(\'cy-max-mo\').value)||0,y:parseFloat($(\'cy-max-y\').value)||0,c:parseFloat($(\'cy-max-c\').value)||0};\n\n  var pMin=freqMin.m/ticksPerMin+freqMin.h/ticksPerH+freqMin.d/ticksPerD+freqMin.w/ticksPerW+freqMin.mo/ticksPerMo+freqMin.y/ticksPerY+(customTicks>0?freqMin.c/customTicks:0);\n  var pMax=freqMax.m/ticksPerMin+freqMax.h/ticksPerH+freqMax.d/ticksPerD+freqMax.w/ticksPerW+freqMax.mo/ticksPerMo+freqMax.y/ticksPerY+(customTicks>0?freqMax.c/customTicks:0);\n\n  pMin=Math.min(pMin,0.8);pMax=Math.min(pMax,0.8);\n\n  // Estimation des fréquences résultantes\n  var estPerH_min=Math.round(pMin*ticksPerH*10)/10;\n  var estPerH_max=Math.round(pMax*ticksPerH*10)/10;\n  var estPerD_min=Math.round(pMin*ticksPerD);\n  var estPerD_max=Math.round(pMax*ticksPerD);\n\n  var el=$(\'cy-prob-display\');if(!el)return;\n  el.innerHTML=\n    \'<b style="color:var(--green)">MIN</b> — probabilité/tick: <b>\'+(pMin*100).toFixed(3)+\'%</b> · ~\'+estPerH_min+\'/heure · ~\'+estPerD_min+\'/jour<br>\'\n    +\'<b style="color:var(--red)">MAX</b> — probabilité/tick: <b>\'+(pMax*100).toFixed(3)+\'%</b> · ~\'+estPerH_max+\'/heure · ~\'+estPerD_max+\'/jour<br>\'\n    +(pMin+pMax>0.5?\'<span style="color:var(--red)">⚠️ Fréquences très élevées — le prix sera souvent aux extrêmes</span>\':\'<span style="color:var(--green)">✅ Fréquences réalistes</span>\');\n\n  window._cyPMin=pMin;window._cyPMax=pMax;\n}\n\nasync function loadMeanPrice(){\n  try{\n    var r=await fetch(\'/nxc/meanprice\');\n    var d=await r.json();\n    if(d.ok){mpOn=d.enabled;var t=document.getElementById(\'mp-target\');if(t)t.value=d.target;updMp();}\n  }catch(e){}\n}\nfunction updMp(){\n  var tg=document.getElementById(\'mp-tg\'),lb=document.getElementById(\'mp-lbl\');\n  if(mpOn){if(tg)tg.classList.add(\'on\');if(lb){lb.textContent=\'✅ Activé\';lb.style.color=\'#a855f7\';}}\n  else{if(tg)tg.classList.remove(\'on\');if(lb){lb.textContent=\'⏸ Désactivé\';lb.style.color=\'var(--muted)\';}}\n}\nasync function toggleMp(){mpOn=!mpOn;updMp();await saveMeanPrice();}\nasync function saveMeanPrice(){\n  var tgt=parseFloat(document.getElementById(\'mp-target\').value);\n  if(!tgt||tgt<50||tgt>100000){setMsg(\'mp-msg\',\'❌ Prix invalide (50–100 000)\',false);return;}\n  var r=await fetch(\'/nxc/meanprice\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},\n    body:JSON.stringify({master_key:KEY,enabled:mpOn,target:tgt})});\n  var res=await r.json();\n  setMsg(\'mp-msg\',res.ok?(mpOn?\'✅ Mean reversion activée → \'+fmt(tgt,0)+\' R\':\'⏸ Désactivée\'):\'❌ Erreur\',res.ok);\n  if(res.ok)addLog(\'🎯\',\'Mean reversion \'+(mpOn?\'activée → \'+fmt(tgt,0)+\' R\':\'désactivée\'));\n}\n\nasync function loadBias(){\n  try{\n    var r=await fetch(\'/nxc/bias\');\n    var d=await r.json();\n    if(d.ok){biasDrift=d.drift;biasSpd=d.speed;updBiasUI();}\n  }catch(e){}\n}\nfunction setBias(v){biasDrift=parseFloat((+v).toFixed(2));updBiasUI();saveBias();}\nfunction setSpd(v){biasSpd=parseFloat((+v).toFixed(3));updBiasUI();saveBias();}\nfunction updBiasUI(){\n  var th=document.getElementById(\'bc-thumb\');\n  var ind=document.getElementById(\'bc-ind\');\n  if(th){\n    var pct=((biasDrift+1)/2)*100;\n    th.style.left=pct+\'%\';\n    if(biasDrift>0.15){th.style.backgroundColor=\'var(--green)\';th.style.boxShadow=\'0 0 14px #00ff9d,0 0 28px #00ff9d\';}\n    else if(biasDrift<-0.15){th.style.backgroundColor=\'var(--red)\';th.style.boxShadow=\'0 0 14px #ff3d5e,0 0 28px #ff3d5e\';}\n    else{th.style.backgroundColor=\'#fff\';th.style.boxShadow=\'0 0 10px rgba(255,255,255,0.5)\';}\n  }\n  if(ind){\n    if(biasDrift>0.5){ind.className=\'bc-ind bull\';ind.textContent=\'\\uD83D\\uDE80 BULL FORT +\'+Math.round(biasDrift*100)+\'%\';}\n    else if(biasDrift>0.15){ind.className=\'bc-ind bull\';ind.textContent=\'\\uD83D\\uDCC8 Légère hausse +\'+Math.round(biasDrift*100)+\'%\';}\n    else if(biasDrift<-0.5){ind.className=\'bc-ind bear\';ind.textContent=\'\\uD83D\\uDC3B BEAR FORT \\u2212\'+Math.round(Math.abs(biasDrift)*100)+\'%\';}\n    else if(biasDrift<-0.15){ind.className=\'bc-ind bear\';ind.textContent=\'\\uD83D\\uDCC9 Légère baisse \\u2212\'+Math.round(Math.abs(biasDrift)*100)+\'%\';}\n    else{ind.className=\'bc-ind neutral\';ind.textContent=\'\\u2696 NEUTRE \\u2014 variation équilibrée\';}\n  }\n  var spdPct=((Math.log2(Math.max(0.125,biasSpd))+3)/6)*100;\n  var sf=document.getElementById(\'spd-fill\'),st=document.getElementById(\'spd-thumb\'),si=document.getElementById(\'spd-ind\');\n  if(sf)sf.style.width=Math.max(0,Math.min(100,spdPct))+\'%\';\n  if(st)st.style.left=Math.max(0,Math.min(100,spdPct))+\'%\';\n  if(si){\n    var lbl=biasSpd<=0.3?\'🐌 Très lent\':biasSpd<=0.6?\'Lent\':biasSpd<=1.2?\'Normal\':biasSpd<=2.5?\'⚡ Rapide\':biasSpd<=5?\'🔥 Très rapide\':\'💥 Extrême\';\n    si.textContent=\'× \'+biasSpd.toFixed(2)+\'  —  \'+lbl;\n    si.style.color=biasSpd>4?\'var(--red)\':biasSpd>2?\'var(--gold)\':biasSpd>1.2?\'var(--cyan)\':\'var(--muted)\';\n    si.style.textShadow=\'0 0 10px \'+si.style.color;\n  }\n}\nfunction startBiasDrag(e){\n  e.preventDefault();\n  var tr=document.getElementById(\'bc-track\');\n  if(!tr)return;\n  function move(ev){\n    var rect=tr.getBoundingClientRect();\n    var cx=ev.touches?ev.touches[0].clientX:ev.clientX;\n    var pct=Math.max(0,Math.min(1,(cx-rect.left)/rect.width));\n    biasDrift=parseFloat((pct*2-1).toFixed(2));\n    updBiasUI();\n  }\n  function up(){\n    document.removeEventListener(\'mousemove\',move);document.removeEventListener(\'mouseup\',up);\n    document.removeEventListener(\'touchmove\',move);document.removeEventListener(\'touchend\',up);\n    saveBias();\n  }\n  document.addEventListener(\'mousemove\',move);document.addEventListener(\'mouseup\',up);\n  document.addEventListener(\'touchmove\',move,{passive:false});document.addEventListener(\'touchend\',up);\n  move(e);\n}\nfunction startSpdDrag(e){\n  e.preventDefault();\n  var tr=document.getElementById(\'spd-track\');\n  if(!tr)return;\n  function move(ev){\n    var rect=tr.getBoundingClientRect();\n    var cx=ev.touches?ev.touches[0].clientX:ev.clientX;\n    var pct=Math.max(0,Math.min(1,(cx-rect.left)/rect.width));\n    biasSpd=parseFloat(Math.pow(2,pct*6-3).toFixed(3));\n    updBiasUI();\n  }\n  function up(){\n    document.removeEventListener(\'mousemove\',move);document.removeEventListener(\'mouseup\',up);\n    document.removeEventListener(\'touchmove\',move);document.removeEventListener(\'touchend\',up);\n    saveBias();\n  }\n  document.addEventListener(\'mousemove\',move);document.addEventListener(\'mouseup\',up);\n  document.addEventListener(\'touchmove\',move,{passive:false});document.addEventListener(\'touchend\',up);\n  move(e);\n}\nasync function saveBias(){\n  try{\n    var r=await fetch(\'/nxc/bias\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},\n      body:JSON.stringify({master_key:KEY,drift:biasDrift,speed:biasSpd})});\n    var res=await r.json();\n    if(res.ok){\n      setMsg(\'bias-msg\',\'✅ Biais sauvegardé\',true);\n      addLog(\'⚡\',\'Biais drift=\'+biasDrift+\'  vitesse×\'+biasSpd);\n    } else setMsg(\'bias-msg\',\'❌ Erreur\',false);\n  }catch(e){setMsg(\'bias-msg\',\'❌ Réseau\',false);}\n}\n\n</script><div class="view" id="view-prevision">\n<div class="card" style="margin-bottom:12px">\n<div class="ct" style="color:#a06bff;font-size:10px;letter-spacing:3px;margin-bottom:14px">🔮 PRÉVISION DU PRIX NXC</div>\n<div style="font-size:11px;color:var(--muted);margin-bottom:14px;line-height:1.6">Estimation basée sur le prix actuel, le biais directionnel et la cible MR. Fourchette = intervalle de confiance 90 %.</div>\n<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap">\n<div style="flex:1;min-width:80px;background:rgba(0,229,255,.06);border:1px solid var(--border);border-radius:10px;padding:10px;text-align:center">\n<div style="font-size:10px;color:var(--muted);margin-bottom:4px">PRIX ACTUEL</div>\n<div id="pv-current" style="font-size:17px;font-weight:800;color:var(--cyan);font-family:monospace">—</div></div>\n<div style="flex:1;min-width:80px;background:rgba(160,107,255,.06);border:1px solid var(--border);border-radius:10px;padding:10px;text-align:center">\n<div style="font-size:10px;color:var(--muted);margin-bottom:4px">TENDANCE</div>\n<div id="pv-trend" style="font-size:15px;font-weight:800">—</div></div>\n<div style="flex:1;min-width:80px;background:rgba(0,255,157,.06);border:1px solid var(--border);border-radius:10px;padding:10px;text-align:center">\n<div style="font-size:10px;color:var(--muted);margin-bottom:4px">CIBLE MR</div>\n<div id="pv-target" style="font-size:17px;font-weight:800;color:var(--green);font-family:monospace">—</div></div>\n</div>\n<table style="width:100%;border-collapse:collapse;font-size:12px">\n<thead><tr style="border-bottom:1px solid var(--border)">\n<th style="text-align:left;padding:8px 6px;color:var(--muted);font-size:10px;font-weight:700;letter-spacing:1px">HORIZON</th>\n<th style="text-align:right;padding:8px 6px;color:var(--muted);font-size:10px;font-weight:700;letter-spacing:1px">ESTIMÉ</th>\n<th style="text-align:right;padding:8px 6px;color:var(--red);font-size:10px;font-weight:700;letter-spacing:1px">MIN P10</th>\n<th style="text-align:right;padding:8px 6px;color:var(--green);font-size:10px;font-weight:700;letter-spacing:1px">MAX P90</th>\n<th style="text-align:center;padding:8px 6px;color:var(--muted);font-size:10px;font-weight:700;letter-spacing:1px">VAR.</th>\n</tr></thead>\n<tbody id="pv-body"><tr><td colspan="5" style="text-align:center;padding:20px;color:var(--muted)">Cliquez Recalculer…</td></tr></tbody>\n</table>\n<button class="btn full" onclick="calcPrev()" style="margin-top:14px">🔄 Recalculer</button>\n<div style="margin-top:10px;font-size:10px;color:var(--muted);text-align:center;line-height:1.5">Prévision indicative — le marché NXC est stochastique.</div>\n</div>\n<div class="card">\n<div class="ct" style="margin-bottom:12px;font-size:10px;letter-spacing:2px">📈 COURBE PRÉVISIONNELLE</div>\n<svg id="pv-svg" viewBox="0 0 320 100" style="width:100%;height:auto;display:block;background:rgba(0,0,0,.15);border-radius:8px"></svg>\n<div style="display:flex;justify-content:space-between;font-size:9px;color:var(--muted);margin-top:6px;padding:0 4px">\n<span>Maintenant</span><span>+6h</span><span>+24h</span><span>+7j</span><span>+30j</span>\n</div>\n</div>\n</div>\n\n<!-- ═══ URGENCE ═══ -->\n<div class="view" id="view-urgence">\n<div class="card red" style="border:2px solid var(--red)">\n  <div class="ct" style="color:var(--red)">🚨 CONTRÔLES D\'URGENCE</div>\n  <div style="background:rgba(255,61,94,.08);border-radius:10px;padding:12px;margin-bottom:12px">\n    <div style="font-weight:800;font-size:13px;color:var(--red);margin-bottom:6px">🧊 GEL DU PRIX</div>\n    <div style="font-size:11px;color:var(--muted);margin-bottom:10px">Fige le prix NXC — les ticks continuent mais le prix est réinitialisé chaque 500 ms.</div>\n    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">\n      <input id="urg-freeze-price" type="number" placeholder="Prix à geler (laisser vide = actuel)" style="flex:1;min-width:160px;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text)">\n      <button class="btn red" onclick="toggleFreeze(true)" id="btn-freeze">🧊 Geler</button>\n      <button class="btn" onclick="toggleFreeze(false)" id="btn-unfreeze" style="display:none">🔥 Dégeler</button>\n    </div>\n    <div id="urg-freeze-status" style="font-size:11px;margin-top:8px;color:var(--muted)">Statut : <span id="urg-freeze-val">non gelé</span></div>\n  </div>\n  <div style="background:rgba(255,61,94,.08);border-radius:10px;padding:12px;margin-bottom:12px">\n    <div style="font-weight:800;font-size:13px;color:var(--gold);margin-bottom:6px">💉 INJECTION DE PRIX</div>\n    <div style="font-size:11px;color:var(--muted);margin-bottom:10px">Force immédiatement le prix NXC à la valeur choisie.</div>\n    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">\n      <input id="urg-price-val" type="number" min="50" max="999999" placeholder="Nouveau prix (R)" style="flex:1;min-width:140px;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text)">\n      <button class="btn gold" onclick="emergencySetPrice()">💉 Injecter</button>\n    </div>\n    <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:8px">\n      <button class="btn" onclick="quickPrice(1000)" style="font-size:11px">1 000</button>\n      <button class="btn" onclick="quickPrice(5000)" style="font-size:11px">5 000</button>\n      <button class="btn" onclick="quickPrice(10000)" style="font-size:11px">10 000</button>\n      <button class="btn" onclick="quickPrice(50000)" style="font-size:11px">50 000</button>\n    </div>\n  </div>\n  <div style="background:rgba(255,61,94,.08);border-radius:10px;padding:12px">\n    <div style="font-weight:800;font-size:13px;color:#a855f7;margin-bottom:6px">🎯 FORCER VERS CIBLE MR</div>\n    <div style="font-size:11px;color:var(--muted);margin-bottom:10px">Injecte immédiatement le prix cible de la Mean Reversion (activée automatiquement).</div>\n    <button class="btn purple full" onclick="forcePriceToTarget()">🎯 Forcer vers cible MR</button>\n  </div>\n  <div id="urg-msg" style="font-size:12px;font-weight:700;min-height:16px;margin-top:10px"></div>\n</div>\n<div class="card">\n  <div class="ct">⚡ VOLATILITÉ</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:10px">Multiplie l\'amplitude des fluctuations. 1.0 = normal, 0 = prix plat, 3 = très volatile.</div>\n  <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">\n    <input id="vol-mult" type="number" min="0" max="10" step="0.1" value="1.0" style="width:100px;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text)">\n    <button class="btn cyan" onclick="saveVolatility()">✓ Appliquer</button>\n    <span id="vol-current" style="font-size:11px;color:var(--muted)">actuel: —</span>\n  </div>\n  <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:8px">\n    <button class="btn" onclick="setVol(0)" style="font-size:10px">🧊 Plat (0)</button>\n    <button class="btn" onclick="setVol(0.5)" style="font-size:10px">🌊 Calme (0.5)</button>\n    <button class="btn" onclick="setVol(1.0)" style="font-size:10px">📈 Normal (1)</button>\n    <button class="btn" onclick="setVol(2.0)" style="font-size:10px">⚡ Volatile (2)</button>\n    <button class="btn red" onclick="setVol(5.0)" style="font-size:10px">💥 Extrême (5)</button>\n  </div>\n  <div id="vol-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:8px"></div>\n</div>\n</div>\n\n<!-- ═══ DASHBOARD ═══ -->\n<div class="view" id="view-dashboard">\n<div class="card" style="border-color:rgba(0,229,255,.3)">\n  <div class="ct" style="color:var(--cyan)">📊 DASHBOARD TEMPS RÉEL</div>\n  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px">\n    <div style="background:var(--bg3);border-radius:10px;padding:12px;text-align:center">\n      <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">Prix actuel</div>\n      <div id="db-price" style="font-size:22px;font-weight:800;color:var(--cyan);margin:4px 0">—</div>\n      <div id="db-chg" style="font-size:11px;font-weight:700">—</div>\n    </div>\n    <div style="background:var(--bg3);border-radius:10px;padding:12px;text-align:center">\n      <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">Volatilité réalisée</div>\n      <div id="db-vol" style="font-size:22px;font-weight:800;color:#a855f7;margin:4px 0">—</div>\n      <div style="font-size:10px;color:var(--muted)">std des log-returns (20 ticks)</div>\n    </div>\n    <div style="background:var(--bg3);border-radius:10px;padding:12px;text-align:center">\n      <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">Haut 24h</div>\n      <div id="db-hi" style="font-size:18px;font-weight:700;color:var(--green);margin:4px 0">—</div>\n    </div>\n    <div style="background:var(--bg3);border-radius:10px;padding:12px;text-align:center">\n      <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">Bas 24h</div>\n      <div id="db-lo" style="font-size:18px;font-weight:700;color:var(--red);margin:4px 0">—</div>\n    </div>\n  </div>\n  <!-- État du marché -->\n  <div id="db-state" style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:12px"></div>\n  <!-- Mini graphique dashboard -->\n  <svg id="db-svg" viewBox="0 0 320 60" style="width:100%;height:auto;display:block;background:rgba(0,0,0,.15);border-radius:8px;margin-bottom:8px"></svg>\n  <div style="display:flex;justify-content:space-between;font-size:9px;color:var(--muted)">\n    <span>-24h</span><span>-12h</span><span>maintenant</span>\n  </div>\n</div>\n<div class="card">\n  <div class="ct">💱 FRAIS EN VIGUEUR</div>\n  <table style="width:100%;font-size:11px;border-collapse:collapse">\n  <thead><tr style="color:var(--muted);font-size:10px;border-bottom:1px solid var(--border)">\n    <th style="text-align:left;padding:5px 4px">Rôle</th>\n    <th style="text-align:center;padding:5px 4px">Achat</th>\n    <th style="text-align:center;padding:5px 4px">Vente</th>\n  </tr></thead>\n  <tbody id="db-fees"></tbody>\n  </table>\n</div>\n</div>\n\n<!-- ═══ ALERTES PRIX ═══ -->\n<div class="view" id="view-alertesp">\n<div class="card">\n  <div class="ct" style="color:var(--gold)">🎯 ALERTES PRIX NXC</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:12px">Vous recevrez une notification quand le prix franchit le seuil choisi.</div>\n  <div style="display:grid;grid-template-columns:1fr 1fr auto;gap:8px;margin-bottom:10px;align-items:end">\n    <div>\n      <div style="font-size:10px;color:var(--muted);margin-bottom:4px">TYPE</div>\n      <select id="alp-type" style="width:100%;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text)">\n        <option value="above">📈 Au-dessus de</option>\n        <option value="below">📉 En-dessous de</option>\n        <option value="change">↕️ Variation de ±</option>\n      </select>\n    </div>\n    <div>\n      <div style="font-size:10px;color:var(--muted);margin-bottom:4px">VALEUR</div>\n      <input id="alp-val" type="number" min="0" placeholder="Prix ou %" style="width:100%;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);box-sizing:border-box">\n    </div>\n    <button class="btn gold" onclick="addPriceAlert()" style="padding:8px 14px">+ Ajouter</button>\n  </div>\n  <div id="alp-list" style="max-height:280px;overflow-y:auto"></div>\n  <div id="alp-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:8px"></div>\n</div>\n<div class="card">\n  <div class="ct">🔔 HISTORIQUE ALERTES</div>\n  <div id="alp-hist" style="max-height:200px;overflow-y:auto;font-size:11px;color:var(--muted)">Aucun déclenchement.</div>\n</div>\n</div>\n\n<!-- ═══ SIMULATEUR ═══ -->\n<div class="view" id="view-simulateur">\n<div class="card">\n  <div class="ct" style="color:#a855f7">🔬 SIMULATEUR DE SCÉNARIOS</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:14px">Simule l\'évolution du prix NXC sous différentes configurations, sans affecter le serveur.</div>\n  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px">\n    <div>\n      <div style="font-size:10px;color:var(--muted);margin-bottom:4px">PRIX DE DÉPART (R)</div>\n      <input id="sim-price" type="number" value="5000" style="width:100%;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);box-sizing:border-box">\n    </div>\n    <div>\n      <div style="font-size:10px;color:var(--muted);margin-bottom:4px">CIBLE MR (R)</div>\n      <input id="sim-target" type="number" value="5000" style="width:100%;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);box-sizing:border-box">\n    </div>\n    <div>\n      <div style="font-size:10px;color:var(--muted);margin-bottom:4px">BIAIS DIRECTIONNEL (-1 à +1)</div>\n      <input id="sim-drift" type="number" min="-1" max="1" step="0.01" value="0" style="width:100%;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);box-sizing:border-box">\n    </div>\n    <div>\n      <div style="font-size:10px;color:var(--muted);margin-bottom:4px">DURÉE</div>\n      <select id="sim-dur" style="width:100%;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text)">\n        <option value="1">1 heure</option>\n        <option value="6">6 heures</option>\n        <option value="24" selected>24 heures</option>\n        <option value="168">7 jours</option>\n        <option value="720">30 jours</option>\n      </select>\n    </div>\n  </div>\n  <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px">\n    <button class="btn purple" onclick="runSim()">▶ Lancer la simulation</button>\n    <button class="btn cyan" onclick="simPreset(\'bull\')" style="font-size:11px">🚀 Bull</button>\n    <button class="btn red" onclick="simPreset(\'bear\')" style="font-size:11px">🐻 Bear</button>\n    <button class="btn" onclick="simPreset(\'stable\')" style="font-size:11px">⚖ Stable</button>\n    <button class="btn gold" onclick="simPreset(\'crash\')" style="font-size:11px">💥 Crash</button>\n    <button class="btn" onclick="simFromServer()" style="font-size:11px">🔄 Valeurs serveur</button>\n  </div>\n  <svg id="sim-svg" viewBox="0 0 320 100" style="width:100%;height:auto;display:block;background:rgba(0,0,0,.15);border-radius:8px;margin-bottom:8px"></svg>\n  <div id="sim-results" style="font-size:11px;color:var(--muted)"></div>\n</div>\n</div><!-- ═══ AVANCÉ ═══ -->\n<div class="view" id="view-avance">\n<div class="card">\n  <div class="ct" style="color:var(--cyan)">⚙️ PARAMÈTRES AVANCÉS DU MARCHÉ</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:12px">Récapitulatif des paramètres internes du modèle de prix NXC.</div>\n  <table style="width:100%;font-size:11px;border-collapse:collapse">\n  <tbody id="adv-params">\n    <tr><td style="padding:7px 4px;color:var(--muted)">Biais autotick</td><td style="text-align:right;font-family:monospace;color:var(--cyan)">+0.031% / tick</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Force mean-reversion</td><td style="text-align:right;font-family:monospace;color:var(--cyan)">4.0% / tick</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Force biais directionnel</td><td style="text-align:right;font-family:monospace;color:var(--cyan)">drift × 5.0% / tick</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Intervalle tick</td><td style="text-align:right;font-family:monospace;color:var(--cyan)">15 secondes</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Sigma autotick (moy.)</td><td style="text-align:right;font-family:monospace;color:var(--cyan)">σ ∈ [0.8%, 2.3%]</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Demi-vie mean-reversion</td><td style="text-align:right;font-family:monospace;color:var(--cyan)">~4.3 min (17 ticks)</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Prix min / max</td><td style="text-align:right;font-family:monospace;color:var(--cyan)">50 R / 100 000 R</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Multiplicateur volatilité</td><td style="text-align:right;font-family:monospace;color:var(--cyan)" id="adv-voltmult">—</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Prix gelé</td><td style="text-align:right;font-family:monospace" id="adv-frozen">non</td></tr>\n  </tbody>\n  </table>\n</div>\n<div class="card">\n  <div class="ct">📋 FORMULES CLÉS</div>\n  <div style="font-size:10px;color:var(--muted);font-family:monospace;line-height:1.8;background:var(--bg3);padding:12px;border-radius:8px">\n    <div style="color:var(--cyan)">// Autotick (toutes les 15s) :</div>\n    adj = (rand() - 0.48) × σ<br>\n    p_new = p × (1 + adj)<br><br>\n    <div style="color:var(--cyan)">// Mean Reversion (si activée, |drift| ≤ 0.05) :</div>\n    pull = (target - p) / p × 0.04<br>\n    p_new = p × (1 + pull)<br><br>\n    <div style="color:var(--cyan)">// Biais directionnel (si |drift| > 0.05) :</div>\n    force = drift × 0.05<br>\n    p_new = p × (1 + force)<br><br>\n    <div style="color:var(--cyan)">// Équilibre O-U :</div>\n    P* = target × 1.0078  </div>\n</div>\n</div>\n\n\n<!-- ═══ HISTORIQUE ═══ -->\n<div class="view" id="view-historique">\n<div class="card">\n  <div class="ct" style="color:var(--cyan)">📈 HISTORIQUE DES PRIX NXC</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:12px">Derniers 120 ticks (30 min). Actualisé chaque 15 s.</div>\n  <div style="display:flex;gap:8px;margin-bottom:10px;flex-wrap:wrap">\n    <button class="btn-s" onclick="clearHistory()">🗑️ Effacer</button>\n    <span id="hist-count" style="font-size:11px;color:var(--muted);line-height:28px">0 points</span>\n    <span id="hist-min" style="font-size:11px;color:#e74c3c;line-height:28px"></span>\n    <span id="hist-max" style="font-size:11px;color:#2ecc71;line-height:28px"></span>\n  </div>\n  <canvas id="hist-chart" style="width:100%;height:220px;display:block"></canvas>\n</div>\n<div class="card" style="margin-top:8px">\n  <div class="ct" style="font-size:13px">📊 Statistiques session</div>\n  <div class="g4" id="hist-stats" style="margin-top:8px"></div>\n</div>\n</div>\n\n<!-- ═══ CONVERTISSEUR ═══ -->\n<div class="view" id="view-convertisseur">\n<div class="card">\n  <div class="ct" style="color:var(--cyan)">💱 CONVERTISSEUR NXC ↔ R</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:14px">Conversion au prix marché actuel.</div>\n  <div style="display:grid;grid-template-columns:1fr auto 1fr;gap:10px;align-items:center;margin-bottom:16px">\n    <div>\n      <label style="font-size:11px;color:var(--muted)">Montant</label>\n      <input id="conv-in" type="number" min="0" step="any" value="1" oninput="doConvert()"\n        style="width:100%;padding:8px;background:var(--bg3);border:1px solid var(--bg3);color:var(--fg);border-radius:6px;font-size:14px">\n    </div>\n    <div style="text-align:center">\n      <div style="font-size:18px;cursor:pointer" onclick="swapConvert()">⇄</div>\n      <div style="font-size:10px;color:var(--muted)" id="conv-dir">NXC → R</div>\n    </div>\n    <div>\n      <label style="font-size:11px;color:var(--muted)">Résultat</label>\n      <div id="conv-out" style="padding:8px;background:var(--bg3);border-radius:6px;font-size:14px;color:var(--cyan);min-height:36px">—</div>\n    </div>\n  </div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:8px">Prix actuel : <span id="conv-price" style="color:var(--fg)">—</span></div>\n  <button class="btn-s" onclick="initConvertisseur()">🔄 Rafraîchir</button>\n  <div class="ct" style="font-size:12px;margin-top:16px;margin-bottom:8px">Impact des frais par rôle (pour 1 NXC)</div>\n  <div id="conv-fees-table" style="font-size:11px"></div>\n</div>\n</div>\n\n<!-- ═══ ÉVÉNEMENTS ═══ -->\n<div class="view" id="view-evenements">\n<div class="card">\n  <div class="ct" style="color:var(--cyan)">🎲 ÉVÉNEMENTS DE MARCHÉ</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:14px">Chocs de prix manuels ou automatiques.</div>\n  <div class="g4" style="margin-bottom:16px">\n    <div class="st-card"><div class="stv" id="evt-count" style="color:var(--cyan)">0</div><div class="stl">Événements déclenchés</div></div>\n    <div class="st-card"><div class="stv" id="evt-last-mag" style="color:var(--gold)">—</div><div class="stl">Dernière amplitude</div></div>\n  </div>\n  <div style="margin-bottom:14px">\n    <div style="font-size:12px;color:var(--muted);margin-bottom:8px">Choc manuel</div>\n    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">\n      <select id="evt-type" style="padding:6px;background:var(--bg3);color:var(--fg);border:none;border-radius:6px;font-size:12px">\n        <option value="up">📈 Hausse soudaine</option>\n        <option value="down">📉 Crash soudain</option>\n        <option value="spike">⚡ Volatilité</option>\n      </select>\n      <input id="evt-mag" type="number" value="5" min="0.1" max="50" step="0.1"\n        style="width:70px;padding:6px;background:var(--bg3);border:none;color:var(--fg);border-radius:6px;font-size:12px">\n      <span style="font-size:11px;color:var(--muted)">%</span>\n      <button class="btn-s" onclick="fireEvent()">🚀 Déclencher</button>\n    </div>\n  </div>\n  <div style="margin-bottom:14px">\n    <div style="font-size:12px;color:var(--muted);margin-bottom:8px">Événements auto (probabilité / tick 15s)</div>\n    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">\n      <label style="font-size:11px">Prob. hausse <input id="auto-prob-up" type="number" value="0" min="0" max="100" step="0.1"\n        style="width:55px;padding:4px;background:var(--bg3);border:none;color:var(--fg);border-radius:4px;margin-left:4px"> %</label>\n      <label style="font-size:11px">Prob. crash <input id="auto-prob-dn" type="number" value="0" min="0" max="100" step="0.1"\n        style="width:55px;padding:4px;background:var(--bg3);border:none;color:var(--fg);border-radius:4px;margin-left:4px"> %</label>\n      <label style="font-size:11px">Amplitude max <input id="auto-mag" type="number" value="3" min="0.1" max="20" step="0.1"\n        style="width:55px;padding:4px;background:var(--bg3);border:none;color:var(--fg);border-radius:4px;margin-left:4px"> %</label>\n      <button class="btn-s" onclick="saveAutoEvents()">💾 OK</button>\n    </div>\n  </div>\n  <div id="evt-log" style="font-size:11px;color:var(--muted);max-height:140px;overflow-y:auto;background:var(--bg3);padding:8px;border-radius:6px">Aucun événement déclenché.</div>\n</div>\n</div>\n\n<!-- ═══ EXPORT ═══ -->\n<div class="view" id="view-export">\n<div class="card">\n  <div class="ct" style="color:var(--cyan)">📤 EXPORT / IMPORT</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:14px">Sauvegardez et restaurez l\\\'état du marché NXC.</div>\n  <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px">\n    <button class="btn-s" onclick="exportSnapshot()">💾 Snapshot JSON</button>\n    <button class="btn-s" onclick="exportHistCSV()">📊 Historique CSV</button>\n    <button class="btn-s" onclick="exportFeesJSON()">💸 Frais JSON</button>\n  </div>\n  <div style="margin-bottom:14px">\n    <div style="font-size:12px;color:var(--muted);margin-bottom:6px">Import snapshot JSON</div>\n    <div style="display:flex;gap:8px;align-items:center">\n      <input id="import-json" type="text" placeholder=""\n        style="flex:1;padding:6px;background:var(--bg3);border:none;color:var(--fg);border-radius:6px;font-size:11px">\n      <button class="btn-s" onclick="importSnapshot()">📥 Importer</button>\n    </div>\n  </div>\n  <div id="export-status" style="font-size:11px;color:var(--cyan);min-height:20px"></div>\n  <div class="ct" style="font-size:12px;margin-top:16px;margin-bottom:8px">État actuel</div>\n  <pre id="export-preview" style="font-size:10px;color:var(--muted);background:var(--bg3);padding:10px;border-radius:6px;max-height:160px;overflow-y:auto;white-space:pre-wrap">Chargement...</pre>\n</div>\n</div>\n\n\n<!-- ═══ MÉMO ADMIN ═══ -->\n<div class="view" id="view-memo">\n<div class="card">\n  <div class="ct" style="color:var(--cyan)">📌 MÉMO ADMINISTRATEUR</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:12px">Notes de session. Non sauvegardé côté serveur — local à cette fenêtre.</div>\n  <textarea id="memo-text" rows="10" placeholder="Tapez vos notes ici..."\n    style="width:100%;padding:10px;background:var(--bg3);border:1px solid rgba(0,229,255,0.15);border-radius:8px;color:var(--fg);font-size:13px;resize:vertical;box-sizing:border-box;font-family:inherit"></textarea>\n  <div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap">\n    <button class="btn-s" onclick="memoSave()">💾 Sauvegarder</button>\n    <button class="btn-s" onclick="memoClear()">🗑️ Effacer</button>\n    <button class="btn-s" onclick="memoExport()">📤 Exporter .txt</button>\n    <span id="memo-status" style="font-size:11px;color:var(--cyan);line-height:28px"></span>\n  </div>\n</div>\n<div class="card" style="margin-top:8px">\n  <div class="ct" style="font-size:13px">🗂️ JOURNAUX DE DÉCISIONS</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:8px">Enregistrez les actions importantes du marché avec timestamp.</div>\n  <div style="display:flex;gap:8px;margin-bottom:10px;flex-wrap:wrap">\n    <input id="journal-entry" type="text" placeholder="Ex: Gel du prix à 5000 R pour événement X"\n      style="flex:1;padding:7px;background:var(--bg3);border:1px solid rgba(0,229,255,0.1);border-radius:6px;color:var(--fg);font-size:12px">\n    <button class="btn-s" onclick="journalAdd()">➕ Ajouter</button>\n  </div>\n  <div id="journal-log" style="font-size:11px;max-height:200px;overflow-y:auto;background:var(--bg3);padding:10px;border-radius:6px">\n    <div style="color:var(--muted)">Aucune entrée.</div>\n  </div>\n  <button class="btn-s" style="margin-top:8px" onclick="journalExport()">📤 Exporter journal</button>\n</div>\n<div class="card" style="margin-top:8px">\n  <div class="ct" style="font-size:13px">⏱️ MINUTEUR ADMIN</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:10px">Minuteur pour les interventions temporaires (gel, événement, etc.).</div>\n  <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:10px">\n    <input id="timer-min" type="number" value="5" min="1" max="999"\n      style="width:70px;padding:7px;background:var(--bg3);border:none;color:var(--fg);border-radius:6px;font-size:14px;text-align:center"> min\n    <button class="btn-s" onclick="timerStart()">▶ Démarrer</button>\n    <button class="btn-s" onclick="timerStop()">⏹ Arrêter</button>\n    <span id="timer-display" style="font-family:monospace;font-size:20px;color:var(--cyan);min-width:80px">--:--</span>\n  </div>\n  <div id="timer-label" style="font-size:11px;color:var(--muted)">Aucun minuteur actif.</div>\n</div>\n</div>\n\n<script>\n/*\n * calcPrev() — Prévision de prix NXC\n *\n * Paramètres réels du serveur (vérifiés dans le code Python) :\n *\n *   _nxc_autotick  (toutes les 15 s) :\n *     sigma_tick = uniform(0.008, 0.023)  →  E[sigma] ≈ 0.0155\n *     adj_auto = (random() - 0.48) * sigma_tick\n *     E[adj_auto] = 0.02 * 0.0155 = +0.00031  (biais haussier)\n *\n *   _mean_reversion_tick  (toutes les 15 s, si enabled ET |drift| ≤ 0.05) :\n *     pull = (target - p) / p * 0.04\n *     → p_{t+1} = p_t * (1 + pull) = 0.96 * p_t + 0.04 * target\n *\n *   Combinés (linéarisé) :\n *     p_{t+1} = p_t * (1 - 0.04 + 0.00031) + 0.04 * target\n *             = p_t * 0.96031 + 0.04 * target\n *     Point fixe : Peq = 0.04*target / (1-0.96031) = target * 1.0078\n *     Décroissance : decay = 0.96031 par tick\n *     Demi-vie : ln(2)/ln(1/0.96031) ≈ 17 ticks = 4.3 minutes\n *\n *   _bias_tick (toutes les 30/speed s, si drift ≠ 0) :\n *     p *= (1 + drift*0.05 + noise)\n *     Quand actif (|drift|>0.05), la MR est SUSPENDUE côté serveur\n *     Applications par tick de 15 s : speed/2\n *     → p_N(bias) = p0 * (1 + drift*0.05)^(N * speed/2)\n *\n * Variance (O-U) : sigma_eff ≈ 0.0166/tick\n *   Var[P_N] = sigma_eff² * p0² * (1 - decay^(2N)) / (1 - decay²)\n *   → plafonnée à la variance stationnaire\n */\n\nasync function calcPrev(){\n  /* ── 1. Fetch toutes les données en temps réel ── */\n  var p0, drift, speed, mrEnabled, mrTarget;\n  try{\n    var [rP, rB, rM] = await Promise.all([\n      fetch(\'/nxc/price\'),\n      fetch(\'/nxc/bias\'),\n      fetch(\'/nxc/meanprice\')\n    ]);\n    var [dP, dB, dM] = await Promise.all([rP.json(), rB.json(), rM.json()]);\n    if(dP && dP.price) mkt = dP;\n    drift     = (dB && typeof dB.drift === \'number\') ? dB.drift : (biasDrift||0);\n    speed     = (dB && typeof dB.speed === \'number\') ? dB.speed : 1.0;\n    mrEnabled = (dM && typeof dM.enabled === \'boolean\') ? dM.enabled : true;\n    mrTarget  = (dM && dM.target > 0) ? dM.target : 5000;\n  }catch(e){\n    drift    = biasDrift || 0;\n    speed    = 1.0;\n    mrEnabled = true;\n    mrTarget  = 5000;\n    var inp = document.getElementById(\'mp-target\');\n    if(inp && parseFloat(inp.value) > 0) mrTarget = parseFloat(inp.value);\n  }\n\n  p0 = parseFloat((mkt && mkt.price) || 5000);\n\n  /* ── 2. Indicateurs ── */\n  var elC = document.getElementById(\'pv-current\');\n  if(elC) elC.textContent = fmt(p0,0) + \' R\';\n  var elT = document.getElementById(\'pv-target\');\n  if(elT) elT.textContent = mrEnabled ? fmt(mrTarget,0) + \' R\' : \'— (désactivée)\';\n  var elTr = document.getElementById(\'pv-trend\');\n  if(elTr){\n    if(drift > 0.3){elTr.textContent=\'🚀 Haussière\';elTr.style.color=\'var(--green)\';}\n    else if(drift < -0.3){elTr.textContent=\'🐻 Baissière\';elTr.style.color=\'var(--red)\';}\n    else{elTr.textContent=\'⚖ Neutre\';elTr.style.color=\'var(--muted)\';}\n  }\n\n  /* ── 3. Paramètres modèle (valeurs EXACTES du serveur) ── */\n  var decay     = 0.96031;    /* par tick 15 s (= 1 - 0.04 + 0.00031) */\n  var sigmaEff  = 0.0166;     /* sigma effectif/tick (autotick + MR noise) */\n  var autoBias  = 0.00031;    /* dérive autotick E[adj] par tick */\n  var mrActive  = mrEnabled && Math.abs(drift) <= 0.05;\n\n  /* Point fixe O-U (avec autoBias) */\n  var Peq = mrActive ? (0.04 * mrTarget / (1.0 - decay)) : p0;\n\n  /* Variance stationnaire O-U */\n  var varStat = sigmaEff * sigmaEff * p0 * p0 / (1.0 - decay * decay);\n\n  var horizons = [\n    {label:\'5 minutes\',  N:20},\n    {label:\'15 minutes\', N:60},\n    {label:\'1 heure\',    N:240},\n    {label:\'6 heures\',   N:1440},\n    {label:\'24 heures\',  N:5760},\n    {label:\'7 jours\',    N:40320},\n    {label:\'30 jours\',   N:172800}\n  ];\n\n  var rows  = [];\n  var svgPts = [{h:0, p:p0, lo:p0, hi:p0}];\n\n  for(var i = 0; i < horizons.length; i++){\n    var hz = horizons[i];\n    var N  = hz.N;\n    var price, lo, hi;\n\n    if(mrActive){\n      /* Ornstein-Uhlenbeck exact :\n         p_N = Peq + (p0 - Peq) * decay^N */\n      var decayN = Math.pow(decay, N);\n      price = Peq + (p0 - Peq) * decayN;\n\n      /* Variance O-U exacte (plafonnée à stationnaire) */\n      var decay2N = Math.pow(decay, 2 * N);\n      var varN  = sigmaEff * sigmaEff * p0 * p0 * (1.0 - decay2N) / (1.0 - decay * decay);\n      var stdBase = Math.sqrt(Math.min(varN, varStat));\n      /* Incertitude de regime : IC grandit au-dela de la convergence O-U */\n      var N_h = N / 240.0;\n      var regF = N_h > 0.25 ? Math.pow(N_h / 0.25, 0.18) : 1.0;\n      var stdN  = stdBase * regF;\n      lo = price - 1.28 * stdN;\n      hi = price + 1.28 * stdN;\n\n    } else {\n      /* Biais actif — MR suspendue côté serveur\n         bias_tick toutes les 30/speed s → speed/2 applications par tick 15 s\n         p_N = p0 * (1 + drift*0.05)^(N * speed/2) * (1 + autoBias)^N */\n      var biasApps = N * speed / 2.0;\n      var biasF    = Math.pow(1.0 + drift * 0.05, biasApps);\n      var autoF    = Math.pow(1.0 + autoBias, N);\n      price = p0 * biasF * autoF;\n\n      /* Variance : marche aléatoire (pas de MR) */\n      var stdN = sigmaEff * p0 * Math.sqrt(N);\n      lo = price - 1.28 * stdN;\n      hi = price + 1.28 * stdN;\n    }\n\n    price = Math.max(50, Math.min(999999, price));\n    lo    = Math.max(50, Math.min(999999, lo));\n    hi    = Math.max(50, Math.min(999999, hi));\n\n    var pct  = (price - p0) / p0 * 100.0;\n    var sign = pct >= 0 ? \'+\' : \'\';\n    var col  = pct >  3 ? \'var(--green)\' : pct < -3 ? \'var(--red)\' : \'var(--muted)\';\n\n    rows.push(\n      \'<tr style="border-bottom:1px solid rgba(255,255,255,.04)">\'\n      +\'<td style="padding:9px 6px;color:var(--cyan);font-weight:700">\'+hz.label+\'</td>\'\n      +\'<td style="padding:9px 6px;text-align:right;font-family:monospace;font-weight:700;color:\'+col+\'">\'+fmt(price,0)+\' R</td>\'\n      +\'<td style="padding:9px 6px;text-align:right;font-family:monospace;color:var(--red)">\'+fmt(lo,0)+\'</td>\'\n      +\'<td style="padding:9px 6px;text-align:right;font-family:monospace;color:var(--green)">\'+fmt(hi,0)+\'</td>\'\n      +\'<td style="padding:9px 6px;text-align:center;font-weight:700;color:\'+col+\'">\'+sign+pct.toFixed(1)+\'%</td>\'\n      +\'</tr>\'\n    );\n    svgPts.push({h: N/240.0, p:price, lo:lo, hi:hi}); /* h en heures */\n  }\n\n  /* ── 4. Tableau ── */\n  var tbody = document.getElementById(\'pv-body\');\n  if(tbody) tbody.innerHTML = rows.join(\'\');\n\n  /* ── 5. Graphe SVG ── */\n  var svg = document.getElementById(\'pv-svg\');\n  if(!svg) return;\n  var allP = svgPts.map(function(x){return x.p;})\n    .concat(svgPts.map(function(x){return x.lo;}))\n    .concat(svgPts.map(function(x){return x.hi;}));\n  var pMin2 = Math.min.apply(null, allP) * 0.985;\n  var pMax2 = Math.max.apply(null, allP) * 1.015;\n  if(pMax2 - pMin2 < 100){ pMin2 -= 50; pMax2 += 50; }\n  var W=320, H=100, P=12;\n  var maxH = svgPts[svgPts.length-1].h || 720;\n  function sx(h){ return P + (h / maxH) * (W - 2*P); }\n  function sy(p){ return P + (1.0 - (p - pMin2) / (pMax2 - pMin2)) * (H - 2*P); }\n\n  var band = \'\';\n  for(var j=0; j<svgPts.length; j++)\n    band += (j===0?\'M\':\'L\') + sx(svgPts[j].h).toFixed(1)+\' \'+sy(svgPts[j].hi).toFixed(1);\n  for(var j=svgPts.length-1; j>=0; j--)\n    band += \'L\'+sx(svgPts[j].h).toFixed(1)+\' \'+sy(svgPts[j].lo).toFixed(1);\n  band += \'Z\';\n  var line = \'\';\n  for(var j=0; j<svgPts.length; j++)\n    line += (j===0?\'M\':\'L\')+sx(svgPts[j].h).toFixed(1)+\' \'+sy(svgPts[j].p).toFixed(1);\n\n  var ty = mrActive ? sy(mrTarget).toFixed(1) : null;\n  var tL = ty ? (\'M\'+P+\' \'+ty+\'L\'+(W-P)+\' \'+ty) : \'\';\n  var lc = drift>0.1?\'#00ff9d\' : drift<-0.1?\'#ff3d5e\' : \'#00e5ff\';\n\n  svg.innerHTML =\n    \'<defs><linearGradient id="pvG" x1="0" x2="1" y1="0" y2="0">\'\n    +\'<stop offset="0%" stop-color="\'+lc+\'" stop-opacity="0.5"/>\'\n    +\'<stop offset="100%" stop-color="\'+lc+\'" stop-opacity="0.1"/>\'\n    +\'</linearGradient></defs>\'\n    +(tL?\'<path d="\'+tL+\'" stroke="rgba(255,176,32,.5)" stroke-width="1" stroke-dasharray="5 3" fill="none"/>\':\'\')\n    +\'<path d="\'+band+\'" fill="url(#pvG)" opacity="0.35"/>\'\n    +\'<path d="\'+line+\'" stroke="\'+lc+\'" stroke-width="2" fill="none" stroke-linejoin="round"/>\'\n    +\'<circle cx="\'+sx(0).toFixed(1)+\'" cy="\'+sy(p0).toFixed(1)+\'" r="4" fill="\'+lc+\'"/>\';\n}\n\n/* Auto-rafraîchissement toutes les 15 s quand l onglet Prévision est visible */\nsetInterval(function(){\n  var v = document.getElementById(\'view-prevision\');\n  if(v && v.style && v.style.display !== \'none\') calcPrev();\n}, 15000);\n</script>\n\n<script>\n/* ═══════════════════════════════════════════════════════\n   FRAIS DE TRANSACTION NXC — gestion complète\n   ════════════════════════════════════════════════════ */\n\nvar _feesData = {};\n\nasync function loadFees(){\n  if(!KEY) return;\n  try{\n    var r = await fetch(\'/nxc/fees\');\n    var d = await r.json();\n    if(!d.ok) return;\n    _feesData = d.fees || {};\n    renderFeesTable();\n  }catch(e){}\n}\n\nfunction renderFeesTable(){\n  var tb = document.getElementById(\'fees-tbody\');\n  if(!tb) return;\n  var roleLabels = {\n    user:      {label:\'👤 Utilisateur\', col:\'var(--text)\'},\n    vip:       {label:\'⭐ VIP\',          col:\'var(--gold)\'},\n    moderator: {label:\'🛡 Modérateur\',   col:\'#a855f7\'},\n    admin:     {label:\'👑 Admin\',        col:\'var(--cyan)\'},\n    default:   {label:\'❓ Défaut\',       col:\'var(--muted)\'}\n  };\n  var rows = \'\';\n  var order = [\'user\',\'vip\',\'moderator\',\'admin\',\'default\'];\n  for(var i=0; i<order.length; i++){\n    var role = order[i];\n    var f = _feesData[role] || {buy:0, sell:0};\n    var rl = roleLabels[role] || {label:role, col:\'var(--text)\'};\n    rows += \'<tr style="border-bottom:1px solid rgba(255,255,255,.04)">\'\n      + \'<td style="padding:8px 4px;font-weight:700;color:\'+rl.col+\'">\'+rl.label+\'</td>\'\n      + \'<td style="text-align:center;padding:4px">\'\n      +   \'<input id="fee-buy-\'+role+\'" type="number" min="0" max="50" step="0.1"\'\n      +   \' value="\'+f.buy.toFixed(1)+\'"\'\n      +   \' style="width:70px;padding:5px 6px;background:var(--bg);border:1px solid rgba(0,229,255,.2);\'\n      +   \'border-radius:6px;color:var(--green);font-size:12px;text-align:center">\'\n      + \'</td>\'\n      + \'<td style="text-align:center;padding:4px">\'\n      +   \'<input id="fee-sell-\'+role+\'" type="number" min="0" max="50" step="0.1"\'\n      +   \' value="\'+f.sell.toFixed(1)+\'"\'\n      +   \' style="width:70px;padding:5px 6px;background:var(--bg);border:1px solid rgba(255,61,94,.2);\'\n      +   \'border-radius:6px;color:var(--red);font-size:12px;text-align:center">\'\n      + \'</td>\'\n      + \'<td style="text-align:center;padding:4px">\'\n      +   \'<button class="btn cyan" onclick="saveFeeRole(\\\'\'+role+\'\\\')"\'\n      +   \' style="font-size:10px;padding:5px 10px">✓</button>\'\n      + \'</td>\'\n      + \'</tr>\';\n  }\n  tb.innerHTML = rows;\n}\n\nasync function saveFeeRole(role){\n  var buyEl  = document.getElementById(\'fee-buy-\'+role);\n  var sellEl = document.getElementById(\'fee-sell-\'+role);\n  if(!buyEl || !sellEl) return;\n  var buy  = parseFloat(buyEl.value);\n  var sell = parseFloat(sellEl.value);\n  if(isNaN(buy)||isNaN(sell)||buy<0||sell<0){\n    setMsg(\'fees-msg\',\'Valeur invalide\', false); return;\n  }\n  try{\n    var r = await fetch(\'/nxc/fees\',{\n      method:\'POST\',\n      headers:{\'Content-Type\':\'application/json\'},\n      body: JSON.stringify({master_key:KEY, role:role, buy:buy, sell:sell})\n    });\n    var d = await r.json();\n    if(d.ok){\n      _feesData = d.fees;\n      renderFeesTable();\n      setMsg(\'fees-msg\',\'✅ \'+role+\' mis à jour (achat \'+buy.toFixed(1)+\'% / vente \'+sell.toFixed(1)+\'%)\', true);\n      addLog(\'💱\',\'Frais \'+role+\': achat=\'+buy.toFixed(1)+\'% vente=\'+sell.toFixed(1)+\'%\');\n    } else {\n      setMsg(\'fees-msg\',\'❌ \'+(d.error||\'Erreur\'), false);\n    }\n  }catch(e){ setMsg(\'fees-msg\',\'❌ Réseau\', false); }\n}\n\nasync function setAllFees(){\n  var buyEl  = document.getElementById(\'fee-all-buy\');\n  var sellEl = document.getElementById(\'fee-all-sell\');\n  if(!buyEl||!sellEl) return;\n  var buy  = parseFloat(buyEl.value);\n  var sell = parseFloat(sellEl.value);\n  if(isNaN(buy)||isNaN(sell)||buy<0||sell<0){\n    setMsg(\'fees-msg\',\'Valeur invalide\', false); return;\n  }\n  try{\n    var r = await fetch(\'/nxc/fees\',{\n      method:\'POST\',\n      headers:{\'Content-Type\':\'application/json\'},\n      body: JSON.stringify({master_key:KEY, set_all:true, buy:buy, sell:sell})\n    });\n    var d = await r.json();\n    if(d.ok){\n      _feesData = d.fees;\n      renderFeesTable();\n      buyEl.value  = \'\';\n      sellEl.value = \'\';\n      setMsg(\'fees-msg\',\'✅ Tous les rôles : achat=\'+buy.toFixed(1)+\'% vente=\'+sell.toFixed(1)+\'%\', true);\n      addLog(\'💱\',\'Frais TOUS rôles: achat=\'+buy.toFixed(1)+\'% vente=\'+sell.toFixed(1)+\'%\');\n    } else {\n      setMsg(\'fees-msg\',\'❌ \'+(d.error||\'Erreur\'), false);\n    }\n  }catch(e){ setMsg(\'fees-msg\',\'❌ Réseau\', false); }\n}\n\n/* setMsg helper (réutilise l\'existant si disponible, sinon inline) */\nfunction setMsg(id, txt, ok){\n  var el = document.getElementById(id);\n  if(!el) return;\n  el.textContent = txt;\n  el.style.color = ok ? \'var(--green)\' : \'var(--red)\';\n  setTimeout(function(){ if(el.textContent===txt) el.textContent=\'\'; }, 4000);\n}\n\n/* Auto-init : dès que KEY est définie (login réussi), charger les frais */\n(function(){\n  var _fInit = setInterval(function(){\n    if(KEY){\n      clearInterval(_fInit);\n      loadFees();\n      /* Aussi recharger quand on va sur config */\n      var _goOrig = window.go;\n      if(typeof _goOrig === \'function\'){\n        window.go = function(tab, btn){\n          _goOrig(tab, btn);\n          if(tab === \'config\') loadFees();\n        };\n      }\n    }\n  }, 500);\n})();\n</script>\n\n<script>\n/* ═══════════════════════════════════════════════════════════════════\n   URGENCE — gel, injection prix, forcer cible\n   ═══════════════════════════════════════════════════════════════════ */\n\nasync function loadFreeze(){\n  if(!KEY) return;\n  try{\n    var r=await fetch(\'/nxc/freeze\'); var d=await r.json();\n    if(d.ok) applyFreezeState(d.frozen, d.price);\n  }catch(e){}\n}\n\nfunction applyFreezeState(frozen, price){\n  var bf=document.getElementById(\'btn-freeze\');\n  var bu=document.getElementById(\'btn-unfreeze\');\n  var sv=document.getElementById(\'urg-freeze-val\');\n  if(bf)bf.style.display=frozen?\'none\':\'block\';\n  if(bu)bu.style.display=frozen?\'block\':\'none\';\n  if(sv)sv.textContent=frozen?(\'🧊 GELÉ à \'+fmt(price,0)+\' R\'):\'non gelé\';\n  if(sv)sv.style.color=frozen?\'var(--red)\':\'var(--muted)\';\n}\n\nasync function toggleFreeze(active){\n  if(!KEY) return;\n  var priceEl=document.getElementById(\'urg-freeze-price\');\n  var freezePrice=priceEl&&priceEl.value?parseFloat(priceEl.value):(mkt&&mkt.price||5000);\n  try{\n    var r=await fetch(\'/nxc/freeze\',{method:\'POST\',\n      headers:{\'Content-Type\':\'application/json\'},\n      body:JSON.stringify({master_key:KEY, active:active, price:freezePrice})});\n    var d=await r.json();\n    if(d.ok){\n      applyFreezeState(d.frozen, d.price);\n      setMsg(\'urg-msg\', d.frozen?(\'🧊 Prix gelé à \'+fmt(d.price,0)+\' R\'):\'🔥 Prix dégelé\', d.ok);\n      addLog(d.frozen?\'🧊\':\'🔥\', d.frozen?(\'Gel à \'+fmt(d.price,0)+\' R\'):\'Dégel du prix\');\n    }\n  }catch(e){ setMsg(\'urg-msg\',\'❌ Erreur réseau\', false); }\n}\n\nasync function emergencySetPrice(){\n  var v=parseFloat(document.getElementById(\'urg-price-val\').value);\n  if(!v||v<50||v>999999){ setMsg(\'urg-msg\',\'Prix invalide (50–999 999 R)\', false); return; }\n  try{\n    var r=await fetch(\'/nxc/price/set\',{method:\'POST\',\n      headers:{\'Content-Type\':\'application/json\'},\n      body:JSON.stringify({master_key:KEY, price:v})});\n    var d=await r.json();\n    if(d.ok){\n      setMsg(\'urg-msg\',\'💉 Prix forcé à \'+fmt(d.price,0)+\' R\', true);\n      addLog(\'💉\',\'Injection prix: \'+fmt(d.price,0)+\' R\');\n      ref();\n    } else { setMsg(\'urg-msg\',\'❌ \'+(d.error||\'Erreur\'), false); }\n  }catch(e){ setMsg(\'urg-msg\',\'❌ Erreur réseau\', false); }\n}\n\nfunction quickPrice(p){ var el=document.getElementById(\'urg-price-val\'); if(el) el.value=p; emergencySetPrice(); }\n\nasync function forcePriceToTarget(){\n  try{\n    var rm=await fetch(\'/nxc/meanprice\'); var dm=await rm.json();\n    var target=dm.target||5000;\n    if(!dm.enabled){\n      var re=await fetch(\'/nxc/meanprice\',{method:\'POST\',\n        headers:{\'Content-Type\':\'application/json\'},\n        body:JSON.stringify({master_key:KEY, enabled:true, target:target})});\n    }\n    var r=await fetch(\'/nxc/price/set\',{method:\'POST\',\n      headers:{\'Content-Type\':\'application/json\'},\n      body:JSON.stringify({master_key:KEY, price:target})});\n    var d=await r.json();\n    if(d.ok){\n      setMsg(\'urg-msg\',\'🎯 Prix forcé à la cible MR: \'+fmt(target,0)+\' R + MR activée\', true);\n      addLog(\'🎯\',\'Force vers cible MR: \'+fmt(target,0)+\' R\');\n      ref(); loadMeanPrice();\n    }\n  }catch(e){ setMsg(\'urg-msg\',\'❌ Erreur\', false); }\n}\n\nasync function saveVolatility(){\n  var v=parseFloat(document.getElementById(\'vol-mult\').value);\n  if(isNaN(v)||v<0){ setMsg(\'vol-msg\',\'Valeur invalide\', false); return; }\n  try{\n    var r=await fetch(\'/nxc/volatility\',{method:\'POST\',\n      headers:{\'Content-Type\':\'application/json\'},\n      body:JSON.stringify({master_key:KEY, value:v})});\n    var d=await r.json();\n    if(d.ok){\n      document.getElementById(\'vol-current\').textContent=\'actuel: \'+d.value.toFixed(1);\n      setMsg(\'vol-msg\',\'✅ Volatilité ×\'+d.value.toFixed(1), true);\n      addLog(\'⚡\',\'Volatilité ×\'+d.value.toFixed(1));\n    } else setMsg(\'vol-msg\',\'❌ \'+(d.error||\'Erreur\'), false);\n  }catch(e){ setMsg(\'vol-msg\',\'❌ Réseau\', false); }\n}\n\nfunction setVol(v){ var el=document.getElementById(\'vol-mult\'); if(el) el.value=v; saveVolatility(); }\n\n/* ═══════════════════════════════════════════════════════════════════\n   DASHBOARD TEMPS RÉEL\n   ═══════════════════════════════════════════════════════════════════ */\n\nasync function loadDashboard(){\n  if(!KEY) return;\n  try{\n    var r=await fetch(\'/nxc/dashboard\'); var d=await r.json();\n    if(!d.ok) return;\n    var ep=document.getElementById(\'db-price\');\n    var ec=document.getElementById(\'db-chg\');\n    var ev=document.getElementById(\'db-vol\');\n    var ehi=document.getElementById(\'db-hi\');\n    var elo=document.getElementById(\'db-lo\');\n    if(ep) ep.textContent=fmt(d.price,0)+\' R\';\n    if(ec){\n      var s=d.change24>=0?\'+\':\'\';\n      ec.textContent=s+d.change24.toFixed(2)+\'% (24h)\';\n      ec.style.color=d.change24>=0?\'var(--green)\':\'var(--red)\';\n    }\n    if(ev) ev.textContent=(d.realizedVol||0).toFixed(3)+\'%\';\n    if(ehi) ehi.textContent=fmt(d.high24,0)+\' R\';\n    if(elo) elo.textContent=fmt(d.low24,0)+\' R\';\n\n    /* Badges état */\n    var st=document.getElementById(\'db-state\');\n    if(st){\n      var badges=[];\n      badges.push(\'<span style="padding:4px 10px;border-radius:20px;font-size:10px;font-weight:700;background:\'+(d.frozen?\'rgba(255,61,94,.2)\':\'rgba(0,229,255,.1)\')\n        +\';color:\'+(d.frozen?\'var(--red)\':\'var(--cyan)\')+\';">\'+(d.frozen?\'🧊 GELÉ\':\'✅ Actif\')+\'</span>\');\n      badges.push(\'<span style="padding:4px 10px;border-radius:20px;font-size:10px;font-weight:700;background:\'+(d.mrEnabled?\'rgba(168,85,247,.2)\':\'rgba(255,255,255,.05)\')\n        +\';color:\'+(d.mrEnabled?\'#a855f7\':\'var(--muted)\')+\';">\'+(d.mrEnabled?\'🎯 MR ON\':\'⏸ MR OFF\')+\'</span>\');\n      badges.push(\'<span style="padding:4px 10px;border-radius:20px;font-size:10px;font-weight:700;background:rgba(255,176,32,.1);color:var(--gold);">\'\n        +(d.drift>0.05?\'🚀 Biais Haussier\':d.drift<-0.05?\'🐻 Biais Baissier\':\'⚖ Neutre\')+\'</span>\');\n      badges.push(\'<span style="padding:4px 10px;border-radius:20px;font-size:10px;background:rgba(255,255,255,.05);color:var(--muted);">Vol ×\'+d.volatilityMult.toFixed(1)+\'</span>\');\n      st.innerHTML=badges.join(\' \');\n    }\n\n    /* Frais dashboard */\n    var df=document.getElementById(\'db-fees\');\n    if(df && d.fees){\n      var roleLabels={user:\'👤 Utilisateur\',vip:\'⭐ VIP\',moderator:\'🛡 Modérateur\',admin:\'👑 Admin\',default:\'❓ Défaut\'};\n      var rows=\'\';\n      Object.keys(d.fees).forEach(function(role){\n        var f=d.fees[role];\n        rows+=\'<tr style="border-top:1px solid rgba(255,255,255,.04)">\'\n          +\'<td style="padding:5px 4px;color:var(--text)">\'+( roleLabels[role]||role)+\'</td>\'\n          +\'<td style="text-align:center;color:var(--green);font-family:monospace">\'+f.buy.toFixed(1)+\'%</td>\'\n          +\'<td style="text-align:center;color:var(--red);font-family:monospace">\'+f.sell.toFixed(1)+\'%</td>\'\n          +\'</tr>\';\n      });\n      df.innerHTML=rows;\n    }\n\n    /* Mini SVG */\n    drawDashGraph(d);\n\n    /* Avancé */\n    var avm=document.getElementById(\'adv-voltmult\');\n    if(avm) avm.textContent=\'×\'+d.volatilityMult.toFixed(1);\n    var afr=document.getElementById(\'adv-frozen\');\n    if(afr){ afr.textContent=d.frozen?(\'🧊 OUI — \'+fmt(d.price,0)+\' R\'):\'non\'; afr.style.color=d.frozen?\'var(--red)\':\'var(--muted)\'; }\n  }catch(e){}\n}\n\nfunction drawDashGraph(d){\n  var svg=document.getElementById(\'db-svg\'); if(!svg) return;\n  var hist=(mkt&&mkt.history)||[];\n  if(hist.length<2){ svg.innerHTML=\'<text x="50%" y="50%" text-anchor="middle" fill="#555" font-size="9">Données insuffisantes</text>\'; return; }\n  var pts=hist.slice(-576);\n  var prices=pts.map(function(x){return parseFloat(x.price||x);});\n  var mn=Math.min.apply(null,prices), mx=Math.max.apply(null,prices), rng=mx-mn||1;\n  var W=320,H=60,P=8;\n  function sx(i){return P+i/(pts.length-1)*(W-2*P);}\n  function sy(p){return P+(1-(p-mn)/rng)*(H-2*P);}\n  var line=\'\'; for(var i=0;i<prices.length;i++) line+=(i===0?\'M\':\'L\')+sx(i).toFixed(1)+\' \'+sy(prices[i]).toFixed(1);\n  var cur=prices[prices.length-1];\n  var col=d.change24>=0?\'#00ff9d\':\'#ff3d5e\';\n  var fill=line+\'L\'+(W-P)+\' \'+(H-P)+\'L\'+P+\' \'+(H-P)+\'Z\';\n  svg.innerHTML=\'<defs><linearGradient id="dbG" x1="0" x2="0" y1="0" y2="1">\'\n    +\'<stop offset="0%" stop-color="\'+col+\'" stop-opacity="0.3"/>\'\n    +\'<stop offset="100%" stop-color="\'+col+\'" stop-opacity="0.02"/>\'\n    +\'</linearGradient></defs>\'\n    +\'<path d="\'+fill+\'" fill="url(#dbG)"/>\'\n    +\'<path d="\'+line+\'" stroke="\'+col+\'" stroke-width="1.5" fill="none"/>\'\n    +\'<circle cx="\'+(W-P)+\'" cy="\'+sy(cur).toFixed(1)+\'" r="3" fill="\'+col+\'"/>\';\n}\n\n/* ═══════════════════════════════════════════════════════════════════\n   ALERTES PRIX\n   ═══════════════════════════════════════════════════════════════════ */\n\nvar _priceAlerts=[], _alertHist=[], _alertCount=0;\n\nfunction addPriceAlert(){\n  var type=document.getElementById(\'alp-type\').value;\n  var val=parseFloat(document.getElementById(\'alp-val\').value);\n  if(isNaN(val)||val<=0){ setMsg(\'alp-msg\',\'Valeur invalide\', false); return; }\n  var id=\'alp-\'+(Date.now());\n  var label= type===\'above\'?(\'Prix > \'+fmt(val,0)+\' R\')\n            :type===\'below\'?(\'Prix < \'+fmt(val,0)+\' R\')\n            :(\'Variation ≥ ±\'+val+\'%\');\n  _priceAlerts.push({id:id,type:type,value:val,label:label,triggered:false,refPrice:mkt&&mkt.price||0});\n  document.getElementById(\'alp-val\').value=\'\';\n  renderAlerts();\n  setMsg(\'alp-msg\',\'✅ Alerte ajoutée: \'+label, true);\n}\n\nfunction removeAlert(i){ _priceAlerts.splice(i,1); renderAlerts(); }\n\nfunction renderAlerts(){\n  var el=document.getElementById(\'alp-list\'); if(!el) return;\n  if(!_priceAlerts.length){ el.innerHTML=\'<div style="color:var(--muted);font-size:11px;padding:10px">Aucune alerte active.</div>\'; return; }\n  el.innerHTML=_priceAlerts.map(function(a,i){\n    var col=a.triggered?\'var(--green)\':\'var(--text)\';\n    var chk=a.triggered?\' ✅\':\'\';\n    return \'<div style="display:flex;align-items:center;gap:8px;padding:8px;background:var(--bg3);border-radius:8px;margin-bottom:6px">\'\n      +\'<span style="flex:1;font-size:11px;color:\'+col+\'">\'+a.label+chk+\'</span>\'\n      +\'<button class="btn red" onclick="removeAlert(\'+i+\')" style="font-size:10px;padding:4px 8px">✕</button>\'\n      +\'</div>\';\n  }).join(\'\');\n}\n\nfunction checkAlerts(){\n  if(!_priceAlerts.length) return;\n  var p=mkt&&parseFloat(mkt.price)||0; if(!p) return;\n  _priceAlerts.forEach(function(a){\n    if(a.triggered) return;\n    var fire=false;\n    if(a.type===\'above\' && p>a.value) fire=true;\n    if(a.type===\'below\' && p<a.value) fire=true;\n    if(a.type===\'change\'){\n      var ref=a.refPrice||p;\n      if(Math.abs((p-ref)/ref)*100>=a.value) fire=true;\n    }\n    if(fire){\n      a.triggered=true;\n      var msg=\'🎯 Alerte: \'+a.label+\' (\'+fmt(p,0)+\' R)\';\n      _alertHist.unshift({ts:Date.now(),msg:msg});\n      if(_alertHist.length>50) _alertHist.pop();\n      addLog(\'🎯\', msg);\n      renderAlerts();\n      renderAlertHist();\n      /* Notification navigateur */\n      if(window.Notification && Notification.permission===\'granted\'){\n        new Notification(\'NXC Alerte Prix\', {body:msg, icon:\'\'});\n      } else if(window.Notification && Notification.permission!==\'denied\'){\n        Notification.requestPermission().then(function(p){\n          if(p===\'granted\') new Notification(\'NXC Alerte Prix\', {body:msg});\n        });\n      }\n    }\n  });\n}\n\nfunction renderAlertHist(){\n  var el=document.getElementById(\'alp-hist\'); if(!el) return;\n  if(!_alertHist.length){ el.innerHTML=\'<div style="color:var(--muted);padding:8px">Aucun déclenchement.</div>\'; return; }\n  el.innerHTML=_alertHist.map(function(a){\n    return \'<div style="padding:6px 0;border-bottom:1px solid rgba(255,255,255,.04)">\'\n      +\'<span style="color:var(--muted);font-size:10px">\'+new Date(a.ts).toLocaleTimeString(\'fr-FR\')+\'</span> \'\n      +\'<span style="color:var(--gold)">\'+a.msg+\'</span></div>\';\n  }).join(\'\');\n}\n\n/* ═══════════════════════════════════════════════════════════════════\n   SIMULATEUR DE SCÉNARIOS\n   ═══════════════════════════════════════════════════════════════════ */\n\nfunction simPreset(name){\n  var presets={\n    bull:  {price:5000, target:5000, drift:0.8},\n    bear:  {price:5000, target:5000, drift:-0.8},\n    stable:{price:5000, target:5000, drift:0.0},\n    crash: {price:20000, target:5000, drift:-0.5}\n  };\n  var p=presets[name]; if(!p) return;\n  document.getElementById(\'sim-price\').value=p.price;\n  document.getElementById(\'sim-target\').value=p.target;\n  document.getElementById(\'sim-drift\').value=p.drift;\n  runSim();\n}\n\nasync function simFromServer(){\n  try{\n    var [rP,rB,rM]=await Promise.all([fetch(\'/nxc/price\'),fetch(\'/nxc/bias\'),fetch(\'/nxc/meanprice\')]);\n    var [dP,dB,dM]=await Promise.all([rP.json(),rB.json(),rM.json()]);\n    if(dP&&dP.price) document.getElementById(\'sim-price\').value=Math.round(dP.price);\n    if(dM&&dM.target) document.getElementById(\'sim-target\').value=Math.round(dM.target);\n    if(dB&&typeof dB.drift===\'number\') document.getElementById(\'sim-drift\').value=dB.drift.toFixed(2);\n    runSim();\n  }catch(e){}\n}\n\nfunction runSim(){\n  var p0   =parseFloat(document.getElementById(\'sim-price\').value)||5000;\n  var tgt  =parseFloat(document.getElementById(\'sim-target\').value)||5000;\n  var drift=parseFloat(document.getElementById(\'sim-drift\').value)||0;\n  var hours=parseFloat(document.getElementById(\'sim-dur\').value)||24;\n\n  /* Même modèle O-U que calcPrev */\n  var decay=0.96031, sigmaEff=0.0166, autoBias=0.00031;\n  var mrActive=Math.abs(drift)<=0.05;\n  var Peq=mrActive?(0.04*tgt/(1.0-decay)):p0;\n  var nSteps=50;\n  var dt=hours/nSteps;  /* heures par step */\n  var N_per_step=Math.round(dt*240);\n\n  var pts=[{x:0,y:p0,lo:p0,hi:p0}];\n  var price=p0;\n  for(var i=1;i<=nSteps;i++){\n    if(mrActive){\n      var decayN=Math.pow(decay,N_per_step);\n      price=Peq+(price-Peq)*decayN;\n    } else {\n      var biasApps=N_per_step*1.0/2.0;\n      price=price*Math.pow(1+drift*0.05,biasApps)*Math.pow(1+autoBias,N_per_step);\n    }\n    price=Math.max(50,Math.min(999999,price));\n    var stdN=sigmaEff*p0*Math.sqrt(i*N_per_step);\n    var varStat=sigmaEff*sigmaEff*p0*p0/(1-decay*decay);\n    var std=Math.min(stdN,Math.sqrt(varStat));\n    pts.push({x:i/nSteps*hours, y:price, lo:Math.max(50,price-1.28*std), hi:Math.min(999999,price+1.28*std)});\n  }\n\n  /* SVG */\n  var svg=document.getElementById(\'sim-svg\'); if(!svg) return;\n  var allY=pts.map(function(p){return p.y;}).concat(pts.map(function(p){return p.lo;})).concat(pts.map(function(p){return p.hi;}));\n  var mn2=Math.min.apply(null,allY)*0.98, mx2=Math.max.apply(null,allY)*1.02;\n  if(mx2-mn2<100){mn2-=50;mx2+=50;}\n  var W=320,H=100,P=10;\n  function sx2(x){return P+x/hours*(W-2*P);}\n  function sy2(y){return P+(1-(y-mn2)/(mx2-mn2))*(H-2*P);}\n  var band=\'\';\n  for(var i=0;i<pts.length;i++) band+=(i===0?\'M\':\'L\')+sx2(pts[i].x).toFixed(1)+\' \'+sy2(pts[i].hi).toFixed(1);\n  for(var i=pts.length-1;i>=0;i--) band+=\'L\'+sx2(pts[i].x).toFixed(1)+\' \'+sy2(pts[i].lo).toFixed(1);\n  band+=\'Z\';\n  var line2=\'\';\n  for(var i=0;i<pts.length;i++) line2+=(i===0?\'M\':\'L\')+sx2(pts[i].x).toFixed(1)+\' \'+sy2(pts[i].y).toFixed(1);\n  var lc2=drift>0.1?\'#00ff9d\':drift<-0.1?\'#ff3d5e\':\'#00e5ff\';\n  /* Ligne cible MR */\n  var tl=\'\'; if(mrActive && tgt>mn2 && tgt<mx2) tl=\'<path d="M\'+P+\' \'+sy2(tgt).toFixed(1)+\'L\'+(W-P)+\' \'+sy2(tgt).toFixed(1)+\'" stroke="rgba(255,176,32,.6)" stroke-width="1" stroke-dasharray="4 3" fill="none"/>\';\n  svg.innerHTML=\'<defs><linearGradient id="simG" x1="0" x2="0" y1="0" y2="1">\'\n    +\'<stop offset="0%" stop-color="\'+lc2+\'" stop-opacity="0.35"/>\'\n    +\'<stop offset="100%" stop-color="\'+lc2+\'" stop-opacity="0.05"/>\'\n    +\'</linearGradient></defs>\'\n    +tl\n    +\'<path d="\'+band+\'" fill="url(#simG)"/>\'\n    +\'<path d="\'+line2+\'" stroke="\'+lc2+\'" stroke-width="2" fill="none" stroke-linejoin="round"/>\'\n    +\'<circle cx="\'+P+\'" cy="\'+sy2(p0).toFixed(1)+\'" r="4" fill="\'+lc2+\'"/>\'\n    +\'<circle cx="\'+(W-P)+\'" cy="\'+sy2(pts[pts.length-1].y).toFixed(1)+\'" r="3" fill="\'+lc2+\'" opacity="0.7"/>\';\n\n  /* Résultats */\n  var final=pts[pts.length-1];\n  var pct=(final.y-p0)/p0*100;\n  document.getElementById(\'sim-results\').innerHTML=\n    \'<div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:8px">\'\n    +\'<span>🏁 Final: <b style="color:\'+lc2+\'">\'+fmt(final.y,0)+\' R</b></span>\'\n    +\'<span>📈 VAR: <b style="color:\'+(pct>=0?\'var(--green)\':\'var(--red)\')+\'">\'+( pct>=0?\'+\':\'\')+pct.toFixed(1)+\'%</b></span>\'\n    +\'<span>📉 Min P10: <b style="color:var(--red)">\'+fmt(final.lo,0)+\' R</b></span>\'\n    +\'<span>📈 Max P90: <b style="color:var(--green)">\'+fmt(final.hi,0)+\' R</b></span>\'\n    +\'</div>\';\n}\n\n/* ═══════════════════════════════════════════════════════════════════\n   HOOKS go() — charger les nouvelles vues\n   ═══════════════════════════════════════════════════════════════════ */\n\n(function(){\n  var _t=setInterval(function(){\n    if(!KEY) return;\n    clearInterval(_t);\n    /* Étendre go() pour les nouveaux onglets */\n    var _orig=window.go;\n    if(typeof _orig===\'function\'){\n      window.go=function(tab,btn){\n        _orig(tab,btn);\n        if(tab===\'dashboard\'){ loadDashboard(); }\n        if(tab===\'urgence\'){ loadFreeze(); }\n        if(tab===\'alertesp\'){ renderAlerts(); renderAlertHist(); }\n        if(tab===\'simulateur\'){ simFromServer(); }\n        if(tab===\'avance\'){ loadDashboard(); }\n        if(tab===\'historique\'){ setTimeout(drawHistChart,100); }\n        if(tab===\'convertisseur\'){ setTimeout(initConvertisseur,100); }\n        if(tab===\'export\'){ setTimeout(loadExportPreview,100); }\n        if(tab===\'memo\'){ setTimeout(function(){ memoLoad(); renderJournal(); },100); }\n      };\n    }\n    /* checkAlerts toutes les 15 s */\n    setInterval(checkAlerts, 15000);\n    /* Dashboard auto-refresh toutes les 15s si visible */\n    setInterval(function(){\n      var v=document.getElementById(\'view-dashboard\');\n      if(v && v.classList.contains(\'on\')) loadDashboard();\n    }, 15000);\n    /* Freeze status auto-refresh toutes les 5s si visible */\n    setInterval(function(){\n      var v=document.getElementById(\'view-urgence\');\n      if(v && v.classList.contains(\'on\')) loadFreeze();\n    }, 5000);\n  }, 600);\n})();\n</script>\n\n<script>\n/* ═══════════════════════════════════════════════════════════════\n   HISTORIQUE DES PRIX\n   ═══════════════════════════════════════════════════════════════ */\nvar _histPrices = [];\nvar _histLabels = [];\n\nfunction drawHistChart(){\n  var canvas = document.getElementById(\'hist-chart\');\n  if(!canvas) return;\n  var W = canvas.offsetWidth || 600, H = 220;\n  canvas.width = W; canvas.height = H;\n  var ctx = canvas.getContext(\'2d\');\n  var n = _histPrices.length;\n  if(n < 2){\n    ctx.fillStyle=\'rgba(100,120,160,0.5)\';\n    ctx.font=\'12px sans-serif\';\n    ctx.fillText(\'En attente de données...\', 20, H/2);\n    return;\n  }\n  ctx.clearRect(0,0,W,H);\n  var mn = Math.min.apply(null,_histPrices), mx = Math.max.apply(null,_histPrices);\n  var pad = {t:10,r:10,b:24,l:58};\n  var cw = W-pad.l-pad.r, ch = H-pad.t-pad.b;\n  var rng = mx-mn || 1;\n  function px(i){ return pad.l + i/(n-1)*cw; }\n  function py(v){ return pad.t + (1-(v-mn)/rng)*ch; }\n  // Grid lines\n  ctx.strokeStyle=\'rgba(255,255,255,0.06)\'; ctx.lineWidth=1;\n  for(var r=0;r<=4;r++){\n    var gy=pad.t+r/4*ch;\n    ctx.beginPath(); ctx.moveTo(pad.l,gy); ctx.lineTo(pad.l+cw,gy); ctx.stroke();\n    ctx.fillStyle=\'rgba(180,180,180,0.5)\'; ctx.font=\'10px sans-serif\'; ctx.textAlign=\'right\';\n    ctx.fillText((mx-r/4*rng).toFixed(2), pad.l-4, gy+4);\n  }\n  // Gradient fill\n  ctx.beginPath(); ctx.moveTo(px(0), py(_histPrices[0]));\n  for(var i=1;i<n;i++) ctx.lineTo(px(i), py(_histPrices[i]));\n  ctx.lineTo(px(n-1), H-pad.b); ctx.lineTo(px(0), H-pad.b); ctx.closePath();\n  var grad = ctx.createLinearGradient(0,pad.t,0,H-pad.b);\n  grad.addColorStop(0,\'rgba(0,200,255,0.25)\'); grad.addColorStop(1,\'rgba(0,200,255,0.02)\');\n  ctx.fillStyle=grad; ctx.fill();\n  // Line\n  ctx.beginPath(); ctx.moveTo(px(0), py(_histPrices[0]));\n  for(var i=1;i<n;i++) ctx.lineTo(px(i), py(_histPrices[i]));\n  ctx.strokeStyle=\'#00c8ff\'; ctx.lineWidth=2; ctx.stroke();\n  // X labels\n  ctx.fillStyle=\'rgba(180,180,180,0.5)\'; ctx.font=\'9px sans-serif\'; ctx.textAlign=\'center\';\n  var step = Math.max(1, Math.floor(n/6));\n  for(var i=0;i<n;i+=step) if(_histLabels[i]) ctx.fillText(_histLabels[i], px(i), H-pad.b+14);\n  // Stats\n  var el;\n  el=document.getElementById(\'hist-count\'); if(el) el.textContent=n+\' point\'+(n>1?\'s\':\'\');\n  el=document.getElementById(\'hist-min\'); if(el) el.textContent=\'Min: \'+mn.toFixed(4);\n  el=document.getElementById(\'hist-max\'); if(el) el.textContent=\'Max: \'+mx.toFixed(4);\n  var delta=_histPrices[n-1]-_histPrices[0];\n  var pct=delta/_histPrices[0]*100;\n  var avg=_histPrices.reduce(function(a,b){return a+b},0)/n;\n  var vol=0; for(var i=1;i<n;i++) vol+=Math.pow(_histPrices[i]-_histPrices[i-1],2);\n  vol=n>1?Math.sqrt(vol/(n-1)):0;\n  el=document.getElementById(\'hist-stats\');\n  if(el) el.innerHTML=\n    \'<div class="st-card"><div class="stv" style="color:\'+(delta>=0?\'#2ecc71\':\'#e74c3c\')+\'">\'+\n      (delta>=0?\'+\':\'\')+pct.toFixed(2)+\'%</div><div class="stl">Variation</div></div>\'+\n    \'<div class="st-card"><div class="stv">\'+avg.toFixed(4)+\'</div><div class="stl">Prix moyen</div></div>\'+\n    \'<div class="st-card"><div class="stv">\'+vol.toFixed(4)+\'</div><div class="stl">Volatilité tick</div></div>\'+\n    \'<div class="st-card"><div class="stv">\'+(mx-mn).toFixed(4)+\'</div><div class="stl">Amplitude</div></div>\';\n}\n\nfunction clearHistory(){ _histPrices=[]; _histLabels=[]; drawHistChart(); }\n\nfunction _recordHistPrice(p){\n  var now=new Date();\n  var ts=now.getHours()+\':\'+(now.getMinutes()<10?\'0\':\'\')+now.getMinutes()+\':\'+(now.getSeconds()<10?\'0\':\'\')+now.getSeconds();\n  _histPrices.push(p); _histLabels.push(ts);\n  if(_histPrices.length>120){ _histPrices.shift(); _histLabels.shift(); }\n  var v=document.getElementById(\'view-historique\');\n  if(v && v.classList.contains(\'on\')) drawHistChart();\n}\n\n/* ═══════════════════════════════════════════════════════════════\n   CONVERTISSEUR NXC <-> R\n   ═══════════════════════════════════════════════════════════════ */\nvar _convDir=\'nxc2r\';\nvar _convPrice=null;\n\nasync function initConvertisseur(){\n  try {\n    var r=await fetch(\'/nxc/price\'); var dp=await r.json();\n    _convPrice=dp.price;\n    var el=document.getElementById(\'conv-price\');\n    if(el) el.textContent=dp.price.toFixed(4)+\' R/NXC\';\n    doConvert();\n    var rf=await fetch(\'/nxc/fees\'); var df=await rf.json();\n    var fees=df.fees||{};\n    var amt=parseFloat(document.getElementById(\'conv-in\').value)||1;\n    var rows=\'<table style="width:100%;border-collapse:collapse;font-size:11px"><tr style="color:var(--muted)"><th style="text-align:left;padding:4px">Rôle</th><th style="text-align:right;padding:4px">Achat</th><th style="text-align:right;padding:4px">Net achat</th><th style="text-align:right;padding:4px">Vente</th><th style="text-align:right;padding:4px">Net vente</th></tr>\';\n    Object.keys(fees).forEach(function(role){\n      var fb=fees[role].buy/100, fs=fees[role].sell/100;\n      rows+=\'<tr><td style="padding:4px;text-transform:capitalize">\'+role+\'</td>\'+\n        \'<td style="text-align:right;padding:4px">\'+fees[role].buy+\'%</td>\'+\n        \'<td style="text-align:right;padding:4px;color:#2ecc71">\'+(amt*(1-fb)*dp.price).toFixed(2)+\' R</td>\'+\n        \'<td style="text-align:right;padding:4px">\'+fees[role].sell+\'%</td>\'+\n        \'<td style="text-align:right;padding:4px;color:#2ecc71">\'+(amt*dp.price*(1-fs)).toFixed(2)+\' R</td></tr>\';\n    });\n    rows+=\'</table>\';\n    var ftEl=document.getElementById(\'conv-fees-table\');\n    if(ftEl) ftEl.innerHTML=rows;\n  } catch(e){ console.error(\'conv\',e); }\n}\n\nfunction swapConvert(){\n  _convDir=_convDir===\'nxc2r\'?\'r2nxc\':\'nxc2r\';\n  var el=document.getElementById(\'conv-dir\');\n  if(el) el.textContent=_convDir===\'nxc2r\'?\'NXC → R\':\'R → NXC\';\n  doConvert();\n}\n\nfunction doConvert(){\n  var amt=parseFloat(document.getElementById(\'conv-in\').value)||0;\n  var p=_convPrice; if(!p) return;\n  var res=_convDir===\'nxc2r\'?amt*p:amt/p;\n  var unit=_convDir===\'nxc2r\'?\'R\':\'NXC\';\n  var el=document.getElementById(\'conv-out\');\n  if(el) el.textContent=res.toFixed(4)+\' \'+unit;\n}\n\n/* ═══════════════════════════════════════════════════════════════\n   ÉVÉNEMENTS DE MARCHÉ\n   ═══════════════════════════════════════════════════════════════ */\nvar _evtCount=0;\nvar _autoEvtCfg={probUp:0,probDn:0,mag:3};\n\nasync function fireEvent(){\n  var typ=document.getElementById(\'evt-type\').value;\n  var mag=parseFloat(document.getElementById(\'evt-mag\').value)||5;\n  var sign=typ===\'down\'?-1:(typ===\'spike\'?(Math.random()<0.5?1:-1):1);\n  var delta=sign*mag/100;\n  try {\n    var rp=await fetch(\'/nxc/price\'); var dp=await rp.json();\n    var newP=Math.max(50,Math.min(999999,dp.price*(1+delta)));\n    var res=await fetch(\'/nxc/price/set\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({price:newP,key:KEY})});\n    _evtCount++;\n    var el;\n    el=document.getElementById(\'evt-count\'); if(el) el.textContent=_evtCount;\n    el=document.getElementById(\'evt-last-mag\'); if(el) el.textContent=(sign>0?\'+\':\'\')+mag.toFixed(1)+\'%\';\n    var log=document.getElementById(\'evt-log\');\n    if(log){\n      var icon=typ===\'down\'?\'📉\':typ===\'spike\'?\'⚡\':\'📈\';\n      var ts=new Date().toLocaleTimeString();\n      log.innerHTML=\'<div style="margin-bottom:4px;color:var(--fg)">\'+icon+\' [\'+ts+\'] \'+(sign>0?\'+\':\'\')+mag.toFixed(1)+\'% → \'+newP.toFixed(2)+\'</div>\'+log.innerHTML;\n    }\n  } catch(e){ console.error(\'fireEvent\',e); }\n}\n\nfunction saveAutoEvents(){\n  _autoEvtCfg.probUp=parseFloat(document.getElementById(\'auto-prob-up\').value)||0;\n  _autoEvtCfg.probDn=parseFloat(document.getElementById(\'auto-prob-dn\').value)||0;\n  _autoEvtCfg.mag=parseFloat(document.getElementById(\'auto-mag\').value)||3;\n  var log=document.getElementById(\'evt-log\');\n  if(log) log.innerHTML=\'<div style="color:#2ecc71">✓ Config auto sauvegardée (h:\'+_autoEvtCfg.probUp+\'% b:\'+_autoEvtCfg.probDn+\'%)</div>\'+log.innerHTML;\n}\n\nsetInterval(async function(){\n  if(_autoEvtCfg.probUp<=0 && _autoEvtCfg.probDn<=0) return;\n  var r=Math.random()*100, sign=0;\n  if(r<_autoEvtCfg.probUp) sign=1;\n  else if(r<_autoEvtCfg.probUp+_autoEvtCfg.probDn) sign=-1;\n  if(!sign) return;\n  var mag=Math.random()*_autoEvtCfg.mag;\n  try {\n    var rp=await fetch(\'/nxc/price\'); var dp=await rp.json();\n    var newP=Math.max(50,Math.min(999999,dp.price*(1+sign*mag/100)));\n    await fetch(\'/nxc/price/set\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({price:newP,key:KEY})});\n    _evtCount++;\n    var el=document.getElementById(\'evt-count\'); if(el) el.textContent=_evtCount;\n    var log=document.getElementById(\'evt-log\');\n    if(log){\n      var ts=new Date().toLocaleTimeString();\n      log.innerHTML=\'<div style="margin-bottom:4px;color:#f39c12">🤖 [\'+ts+\'] AUTO \'+(sign>0?\'+\':\'\')+mag.toFixed(2)+\'% → \'+newP.toFixed(2)+\'</div>\'+log.innerHTML;\n    }\n  } catch(e){}\n}, 15000);\n\n/* ═══════════════════════════════════════════════════════════════\n   EXPORT / IMPORT\n   ═══════════════════════════════════════════════════════════════ */\nasync function loadExportPreview(){\n  try {\n    var rp=await fetch(\'/nxc/price\'), rb=await fetch(\'/nxc/bias\'), rf=await fetch(\'/nxc/fees\'), rv=await fetch(\'/nxc/volatility\');\n    var dp=await rp.json(), db=await rb.json(), df=await rf.json(), dv=await rv.json();\n    var snap={timestamp:new Date().toISOString(),price:dp.price,target:dp.target||null,drift:db.drift,mrEnabled:db.mrEnabled,fees:df.fees,volatilityMult:dv.value||1,histPoints:_histPrices.length};\n    var el=document.getElementById(\'export-preview\');\n    if(el) el.textContent=JSON.stringify(snap,null,2);\n  } catch(e){ var el=document.getElementById(\'export-preview\'); if(el) el.textContent=\'Erreur: \'+e; }\n}\n\nfunction exportSnapshot(){\n  fetch(\'/nxc/dashboard\').then(function(r){return r.json();}).then(function(d){\n    var blob=new Blob([JSON.stringify(d,null,2)],{type:\'application/json\'});\n    var a=document.createElement(\'a\'); a.href=URL.createObjectURL(blob);\n    a.download=\'nxc-snapshot-\'+new Date().toISOString().slice(0,19).replace(/:/g,\'-\')+\'.json\';\n    a.click(); URL.revokeObjectURL(a.href);\n    var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'✓ Snapshot exporté\';\n  }).catch(function(e){ var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'Erreur: \'+e; });\n}\n\nfunction exportHistCSV(){\n  if(!_histPrices.length){ var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'Aucun historique\'; return; }\n  var lines=[\'timestamp,price\'];\n  for(var i=0;i<_histPrices.length;i++) lines.push(_histLabels[i]+\',\'+_histPrices[i]);\n  var blob=new Blob([lines.join(\'\\n\')],{type:\'text/csv\'});\n  var a=document.createElement(\'a\'); a.href=URL.createObjectURL(blob);\n  a.download=\'nxc-historique-\'+new Date().toISOString().slice(0,10)+\'.csv\';\n  a.click(); URL.revokeObjectURL(a.href);\n  var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'✓ CSV exporté (\'+_histPrices.length+\' points)\';\n}\n\nfunction exportFeesJSON(){\n  fetch(\'/nxc/fees\').then(function(r){return r.json();}).then(function(d){\n    var blob=new Blob([JSON.stringify(d.fees,null,2)],{type:\'application/json\'});\n    var a=document.createElement(\'a\'); a.href=URL.createObjectURL(blob);\n    a.download=\'nxc-frais-\'+new Date().toISOString().slice(0,10)+\'.json\';\n    a.click(); URL.revokeObjectURL(a.href);\n    var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'✓ Frais JSON exporté\';\n  });\n}\n\nasync function importSnapshot(){\n  var inp=document.getElementById(\'import-json\');\n  if(!inp||!inp.value.trim()){ var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'Aucun JSON\'; return; }\n  try {\n    var snap=JSON.parse(inp.value.trim());\n    var ops=[];\n    if(snap.price) ops.push(fetch(\'/nxc/price/set\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({price:snap.price,key:KEY})}));\n    if(snap.fees) ops.push(fetch(\'/nxc/fees\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({key:KEY,fees:snap.fees})}));\n    await Promise.all(ops);\n    var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'✓ Importé\';\n    setTimeout(loadExportPreview,500);\n  } catch(e){ var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'Erreur JSON: \'+e; }\n}\n\n/* ═══ Extension go() pour les nouveaux onglets ═══ */\n(func\n\n/* ═══ Enregistrement prix dans historique ═══ */\n(function(){\n  setInterval(async function(){\n    try { var r=await fetch(\'/nxc/price\'); var d=await r.json(); if(d.price) _recordHistPrice(d.price); } catch(e){}\n  }, 15000);\n})();\n</script>\n\n<script>\n/* ═══════════════════════════════════════════════════════════════\n   MEMO ADMIN + JOURNAL + MINUTEUR\n   ═══════════════════════════════════════════════════════════════ */\nvar _journalEntries = [];\nvar _timerInterval = null;\nvar _timerEnd = null;\n\nfunction memoSave(){\n  var txt = document.getElementById(\'memo-text\');\n  if(!txt) return;\n  try { localStorage.setItem(\'nxc_memo\', txt.value); } catch(e){}\n  var st = document.getElementById(\'memo-status\');\n  if(st) st.textContent = \'Sauvegarde a \' + new Date().toLocaleTimeString();\n}\n\nfunction memoClear(){\n  var txt = document.getElementById(\'memo-text\');\n  if(txt) txt.value = \'\';\n  try { localStorage.removeItem(\'nxc_memo\'); } catch(e){}\n  var st = document.getElementById(\'memo-status\');\n  if(st) st.textContent = \'Efface\';\n}\n\nfunction memoExport(){\n  var txt = document.getElementById(\'memo-text\');\n  var c = txt ? txt.value : \'\';\n  var blob = new Blob([c], {type:\'text/plain\'});\n  var a = document.createElement(\'a\');\n  a.href = URL.createObjectURL(blob);\n  a.download = \'nxc-memo-\' + new Date().toISOString().slice(0,10) + \'.txt\';\n  a.click(); URL.revokeObjectURL(a.href);\n}\n\nfunction memoLoad(){\n  var txt = document.getElementById(\'memo-text\');\n  if(!txt) return;\n  try { var s = localStorage.getItem(\'nxc_memo\'); if(s) txt.value = s; } catch(e){}\n}\n\nfunction journalAdd(){\n  var inp = document.getElementById(\'journal-entry\');\n  if(!inp || !inp.value.trim()) return;\n  _journalEntries.unshift({ts: new Date().toLocaleTimeString(), text: inp.value.trim()});\n  inp.value = \'\';\n  renderJournal();\n}\n\nfunction renderJournal(){\n  var el = document.getElementById(\'journal-log\');\n  if(!el) return;\n  if(!_journalEntries.length){ el.innerHTML = \'<div style="color:var(--muted)">Aucune entree.</div>\'; return; }\n  el.innerHTML = _journalEntries.map(function(e,i){\n    return \'<div style="padding:5px 0;border-bottom:1px solid rgba(255,255,255,0.05);display:flex;gap:8px">\' +\n      \'<span style="color:var(--muted);font-size:10px;white-space:nowrap">[\' + e.ts + \']</span>\' +\n      \'<span style="flex:1;font-size:11px">\' + e.text + \'</span>\' +\n      \'<span onclick="_journalEntries.splice(\'+i+\',1);renderJournal()" style="cursor:pointer;color:#e74c3c;font-size:10px;flex-shrink:0">x</span>\' +\n      \'</div>\';\n  }).join(\'\');\n}\n\nfunction journalExport(){\n  if(!_journalEntries.length) return;\n  var lines = _journalEntries.map(function(e){ return \'[\' + e.ts + \'] \' + e.text; });\n  var blob = new Blob([lines.join(\'\\n\')], {type:\'text/plain\'});\n  var a = document.createElement(\'a\');\n  a.href = URL.createObjectURL(blob);\n  a.download = \'nxc-journal-\' + new Date().toISOString().slice(0,10) + \'.txt\';\n  a.click(); URL.revokeObjectURL(a.href);\n}\n\nfunction timerStart(){\n  if(_timerInterval) clearInterval(_timerInterval);\n  var mins = parseFloat(document.getElementById(\'timer-min\').value) || 5;\n  _timerEnd = Date.now() + mins * 60000;\n  var lbl = document.getElementById(\'timer-label\');\n  if(lbl) lbl.textContent = \'Minuteur \' + mins + \' min en cours...\';\n  var disp = document.getElementById(\'timer-display\');\n  if(disp) disp.style.color = \'var(--cyan)\';\n  _timerInterval = setInterval(function(){\n    var rem = _timerEnd - Date.now();\n    var d = document.getElementById(\'timer-display\');\n    if(rem <= 0){\n      clearInterval(_timerInterval); _timerInterval = null;\n      if(d){ d.textContent = \'00:00\'; d.style.color = \'var(--red)\'; }\n      var l = document.getElementById(\'timer-label\');\n      if(l) l.textContent = \'Minuteur termine !\';\n      return;\n    }\n    var m = Math.floor(rem/60000), s = Math.floor((rem%60000)/1000);\n    if(d){ d.textContent = (m<10?\'0\':\'\')+m+\':\'+(s<10?\'0\':\'\')+s; d.style.color = rem<60000?\'var(--red)\':\'var(--cyan)\'; }\n  }, 500);\n}\n\nfunction timerStop(){\n  if(_timerInterval){ clearInterval(_timerInterval); _timerInterval = null; }\n  var d = document.getElementById(\'timer-display\');\n  if(d){ d.textContent = \'--:--\'; d.style.color = \'var(--cyan)\'; }\n  var l = document.getElementById(\'timer-label\');\n  if(l) l.textContent = \'Arrete.\';\n}\n\n\n\n(func\n</script>\n\n</body>\n</html>\n'

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
    """Prix NXC actuel — le tick est géré uniquement par le thread _nxc_autotick."""
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


# ══ HISTORIQUE PRIX SERVEUR ══
# Stocke les 500 derniers prix pour l'API
NXC_PRICE_HISTORY = []
NXC_PRICE_HISTORY_MAX = 500

def _record_server_price(price):
    """Enregistre le prix dans l'historique serveur."""
    import datetime
    NXC_PRICE_HISTORY.append({
        "price": round(float(price), 6),
        "ts": datetime.datetime.utcnow().isoformat() + "Z"
    })
    if len(NXC_PRICE_HISTORY) > NXC_PRICE_HISTORY_MAX:
        NXC_PRICE_HISTORY.pop(0)


@app.route("/nxc/history", methods=["GET"])
def nxc_history():
    """Retourne l'historique des prix serveur."""
    try:
        n = int(request.args.get("n", 120))
        n = max(1, min(n, NXC_PRICE_HISTORY_MAX))
        data = NXC_PRICE_HISTORY[-n:]
        return jsonify({
            "ok": True,
            "count": len(data),
            "history": data,
            "current": NXC_MARKET.get("price", 0)
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/nxc/stats/extended", methods=["GET"])
def nxc_stats_extended():
    """Statistiques etendues du marche NXC."""
    try:
        import math, datetime
        prices = [h["price"] for h in NXC_PRICE_HISTORY if h.get("price")]
        cur = float(NXC_MARKET.get("price", 0))

        # Volatilite realisee
        vol = 0.0
        if len(prices) > 1:
            returns = [prices[i]/prices[i-1] - 1 for i in range(1, len(prices))]
            mean_r  = sum(returns) / len(returns)
            var_r   = sum((r - mean_r)**2 for r in returns) / len(returns)
            vol     = math.sqrt(var_r) * 100

        # Max drawdown
        max_dd = 0.0
        if prices:
            peak = prices[0]
            for p in prices:
                if p > peak:
                    peak = p
                dd = (peak - p) / peak if peak > 0 else 0
                if dd > max_dd:
                    max_dd = dd

        # Tendance lineaire (regression simple)
        trend = 0.0
        if len(prices) > 2:
            n  = len(prices)
            xs = list(range(n))
            mx = sum(xs) / n
            my = sum(prices) / n
            num = sum((xs[i] - mx) * (prices[i] - my) for i in range(n))
            den = sum((xs[i] - mx)**2 for i in range(n))
            trend = (num / den) if den != 0 else 0.0

        # Prix min / max / moyen
        p_min  = min(prices) if prices else cur
        p_max  = max(prices) if prices else cur
        p_mean = sum(prices) / len(prices) if prices else cur

        return jsonify({
            "ok": True,
            "current":       cur,
            "samples":       len(prices),
            "vol_pct":       round(vol, 4),
            "max_drawdown":  round(max_dd * 100, 4),
            "trend_per_tick": round(trend, 6),
            "price_min":     round(p_min, 4),
            "price_max":     round(p_max, 4),
            "price_mean":    round(p_mean, 4),
            "frozen":        NXC_FROZEN.get("active", False),
            "vol_mult":      NXC_VOLATILITY_MULT.get("value", 1.0),
            "fees":          NXC_FEES,
            "timestamp":     datetime.datetime.utcnow().isoformat() + "Z"
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def _record_price_hook(price):
    """Hook appele par autotick pour enregistrer le prix."""
    _record_server_price(price)



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

def _mean_reversion_tick():
    """Thread de mean reversion — ramène le prix vers la cible toutes les 15s."""
    while True:
        try:
            time.sleep(15)
            if not NXC_MEAN_PRICE["enabled"]:
                continue
            # Ne pas contrecarrer un biais directionnel actif
            if abs(NXC_BIAS["drift"]) > 0.05:
                continue
            p = NXC_MARKET["price"]
            target = NXC_MEAN_PRICE["target"]
            pull = (target - p) / max(p, 1) * 0.04
            sigma = 0.004 + _rnd.random() * 0.008
            noise = (_rnd.random() - 0.5) * sigma
            adj = pull + noise
            p = max(50.0, min(100000.0, p * (1 + adj)))
            p = round(p * 100) / 100
            NXC_MARKET["price"] = p
            NXC_MARKET["ts"] = int(time.time() * 1000)
            NXC_MARKET["history"].append({"price": p, "ts": NXC_MARKET["ts"], "vol": int(_rnd.random() * 400 + 20)})
            if len(NXC_MARKET["history"]) > 576:
                NXC_MARKET["history"] = NXC_MARKET["history"][-576:]
        except Exception:
            pass

threading.Thread(target=_mean_reversion_tick, daemon=True).start()

def _bias_tick():
    """Thread de biais directionnel — applique une dérive haussière/baissière configurable."""
    while True:
        try:
            interval = max(5.0, 30.0 / max(0.1, NXC_BIAS["speed"]))
            time.sleep(interval)
            drift = NXC_BIAS["drift"]
            if drift == 0.0:
                continue
            p = NXC_MARKET["price"]
            force = drift * 0.05
            noise = (_rnd.random() - 0.5) * 0.003
            adj = force + noise
            p = max(50.0, min(100000.0, p * (1 + adj)))
            p = round(p * 100) / 100
            NXC_MARKET["price"] = p
            NXC_MARKET["ts"] = int(time.time() * 1000)
            NXC_MARKET["history"].append({"price": p, "ts": NXC_MARKET["ts"], "vol": int(_rnd.random() * 300 + 10)})
            if len(NXC_MARKET["history"]) > 576:
                NXC_MARKET["history"] = NXC_MARKET["history"][-576:]
        except Exception:
            pass

threading.Thread(target=_bias_tick, daemon=True).start()

@app.route("/nxc/bias", methods=["GET", "POST"])
def nxc_bias():
    if request.method == "GET":
        return jsonify({"ok": True, "drift": NXC_BIAS["drift"], "speed": NXC_BIAS["speed"]})
    body = request.get_json(force=True, silent=True) or {}
    mk = (body.get("master_key") or request.args.get("master_key") or "")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    if "drift" in body:
        d = float(body["drift"])
        NXC_BIAS["drift"] = max(-1.0, min(1.0, d))
    if "speed" in body:
        s = float(body["speed"])
        NXC_BIAS["speed"] = max(0.1, min(8.0, s))
    return jsonify({"ok": True, "drift": NXC_BIAS["drift"], "speed": NXC_BIAS["speed"]})

@app.route("/nxc/meanprice", methods=["GET", "POST"])
def nxc_meanprice():
    if request.method == "GET":
        return jsonify({"ok": True, "enabled": NXC_MEAN_PRICE["enabled"], "target": NXC_MEAN_PRICE["target"]})
    body = request.get_json(force=True, silent=True) or {}
    mk = (body.get("master_key") or request.args.get("master_key") or "")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    if "enabled" in body:
        NXC_MEAN_PRICE["enabled"] = bool(body["enabled"])
    if "target" in body:
        t = float(body.get("target", 5000))
        if 50 <= t <= 100000:
            NXC_MEAN_PRICE["target"] = t
    return jsonify({"ok": True, "enabled": NXC_MEAN_PRICE["enabled"], "target": NXC_MEAN_PRICE["target"]})



@app.route("/nxc/fees", methods=["GET", "POST"])
def nxc_fees():
    """GET  : retourne les frais par rôle.
    POST {master_key, fees}         : met à jour tout NXC_FEES.
    POST {master_key, role, buy, sell} : met à jour un rôle précis.
    POST {master_key, set_all, buy, sell} : même frais pour tous les rôles."""
    if request.method == "GET":
        return jsonify({"ok": True, "fees": NXC_FEES})
    body = request.get_json(force=True, silent=True) or {}
    mk = (body.get("master_key") or request.args.get("master_key") or "")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    try:
        if body.get("set_all"):
            buy_all  = max(0.0, min(50.0, float(body.get("buy",  0))))
            sell_all = max(0.0, min(50.0, float(body.get("sell", 0))))
            for role in NXC_FEES:
                NXC_FEES[role]["buy"]  = buy_all
                NXC_FEES[role]["sell"] = sell_all
        elif "role" in body:
            role = body["role"]
            if role in NXC_FEES:
                if "buy" in body:
                    NXC_FEES[role]["buy"]  = max(0.0, min(50.0, float(body["buy"])))
                if "sell" in body:
                    NXC_FEES[role]["sell"] = max(0.0, min(50.0, float(body["sell"])))
        elif "fees" in body:
            for role, rates in (body["fees"] or {}).items():
                if role in NXC_FEES and isinstance(rates, dict):
                    if "buy"  in rates:
                        NXC_FEES[role]["buy"]  = max(0.0, min(50.0, float(rates["buy"])))
                    if "sell" in rates:
                        NXC_FEES[role]["sell"] = max(0.0, min(50.0, float(rates["sell"])))
        return jsonify({"ok": True, "fees": NXC_FEES})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/nxc/freeze", methods=["GET", "POST"])
def nxc_freeze():
    """Gel / degel d urgence du prix NXC."""
    if request.method == "GET":
        return jsonify({"ok": True, "frozen": NXC_FROZEN["active"],
                        "price": NXC_FROZEN.get("frozen_price"),
                        "since": NXC_FROZEN.get("since")})
    body = request.get_json(force=True, silent=True) or {}
    mk = (body.get("master_key") or "")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    active = bool(body.get("active", False))
    NXC_FROZEN["active"] = active
    if active:
        NXC_FROZEN["frozen_price"] = float(body.get("price", NXC_MARKET["price"]))
        NXC_FROZEN["since"] = int(time.time() * 1000)
    else:
        NXC_FROZEN["frozen_price"] = None
        NXC_FROZEN["since"] = None
    return jsonify({"ok": True, "frozen": NXC_FROZEN["active"],
                    "price": NXC_FROZEN.get("frozen_price")})


@app.route("/nxc/volatility", methods=["GET", "POST"])
def nxc_volatility():
    """Multiplicateur de volatilite (0.0 = plat, 1.0 = normal, 3.0 = tres volatile)."""
    if request.method == "GET":
        return jsonify({"ok": True, "value": NXC_VOLATILITY_MULT["value"]})
    body = request.get_json(force=True, silent=True) or {}
    mk = (body.get("master_key") or "")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    v = float(body.get("value", 1.0))
    NXC_VOLATILITY_MULT["value"] = max(0.0, min(10.0, v))
    return jsonify({"ok": True, "value": NXC_VOLATILITY_MULT["value"]})


@app.route("/nxc/price/set", methods=["POST"])
def nxc_price_set():
    """Force le prix NXC a une valeur precise (urgence / correction)."""
    body = request.get_json(force=True, silent=True) or {}
    mk = (body.get("master_key") or "")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    price = float(body.get("price", 0))
    if price < 50 or price > 999999:
        return jsonify({"ok": False, "error": "Prix hors limites (50–999999)"}), 400
    now_ms = int(time.time() * 1000)
    NXC_MARKET["price"] = round(price, 2)
    NXC_MARKET["ts"]    = now_ms
    NXC_MARKET["history"].append({"price": round(price, 2), "ts": now_ms,
                                   "vol": 0, "event": "force"})
    if len(NXC_MARKET["history"]) > 576:
        NXC_MARKET["history"] = NXC_MARKET["history"][-576:]
    return jsonify({"ok": True, "price": NXC_MARKET["price"]})


@app.route("/nxc/dashboard", methods=["GET"])
def nxc_dashboard():
    """Stats completes pour le dashboard temps reel."""
    hist = NXC_MARKET.get("history", [])
    prices = [float(h.get("price", 0)) for h in hist if h.get("price")]
    p_now  = float(NXC_MARKET.get("price", 0))
    p_24h  = prices[-576] if len(prices) >= 576 else (prices[0] if prices else p_now)
    hi_24  = max(prices[-576:]) if prices else p_now
    lo_24  = min(prices[-576:]) if prices else p_now
    vol_24 = sum(float(h.get("vol", 0)) for h in hist[-576:])
    chg_24 = ((p_now - p_24h) / max(p_24h, 1)) * 100 if p_24h else 0.0
    # Volatilite realisee (std des log-returns sur 20 derniers ticks)
    recent = [float(h.get("price", 0)) for h in hist[-21:] if h.get("price")]
    realized_vol = 0.0
    if len(recent) >= 2:
        import math
        returns = [math.log(recent[i]/recent[i-1]) for i in range(1, len(recent)) if recent[i-1] > 0]
        if returns:
            avg_r = sum(returns) / len(returns)
            realized_vol = (sum((r - avg_r)**2 for r in returns) / len(returns)) ** 0.5
    return jsonify({
        "ok": True,
        "price": p_now,
        "change24": round(chg_24, 2),
        "high24": round(hi_24, 2),
        "low24":  round(lo_24, 2),
        "volume24": round(vol_24, 0),
        "realizedVol": round(realized_vol * 100, 3),
        "frozen": NXC_FROZEN["active"],
        "volatilityMult": NXC_VOLATILITY_MULT["value"],
        "mrEnabled": NXC_MEAN_PRICE["enabled"],
        "mrTarget": NXC_MEAN_PRICE["target"],
        "drift": NXC_BIAS["drift"],
        "speed": NXC_BIAS["speed"],
        "histLen": len(hist),
        "fees": NXC_FEES
    })




# ══ BANQUE UTILISATEUR (admin panel) ══



if __name__ == "__main__":
    _load_nxc_from_db()
    print("=" * 54)
    print("  NEXUS SERVER (en ligne)  —  http://127.0.0.1:%d" % PORT)
    print("  Clé maître :", MASTER_KEY)
    print("  Prix NXC restauré : %.2f R" % NXC_MARKET["price"])
    print("=" * 54)
    app.run(host="0.0.0.0", port=PORT)
