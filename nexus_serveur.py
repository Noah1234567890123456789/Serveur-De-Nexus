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
NXC_PANEL_HTML = '<!DOCTYPE html><html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover"><title>◈ Nexus Server</title><style>\n*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent;touch-action:manipulation}:root{--bg:#02040a;--bg2:#080d1a;--bg3:#0d1428;--bg4:#111827;--cyan:#00e5ff;--green:#00ff9d;--red:#ff3d5e;--gold:#ffb020;--purple:#a06bff;--blue:#4ea8de;--pink:#ff6eb4;--muted:#5c6b8c;--text:#d4e8ff;--border:rgba(0,229,255,.12);--glow-cyan:0 0 20px rgba(0,229,255,.2);--glow-green:0 0 20px rgba(0,255,157,.2);}html{background:var(--bg);-webkit-text-size-adjust:100%;scroll-behavior:smooth}body{background:var(--bg);color:var(--text);font-family:\'Segoe UI\',system-ui,sans-serif;min-height:100vh;min-height:100dvh;overflow-x:hidden}.hud{position:fixed;top:0;left:0;right:0;height:56px;background:rgba(2,4,10,.96);border-bottom:1px solid var(--border);display:flex;align-items:center;padding:0 14px;gap:8px;z-index:200;backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px)}.hud-logo{font-family:monospace;font-size:15px;font-weight:900;color:var(--cyan);letter-spacing:2px;flex-shrink:0;text-shadow:0 0 12px rgba(0,229,255,.4)}.hud-logo em{color:#fff;opacity:.3;font-style:normal}.hud-sep{width:1px;height:22px;background:var(--border);flex-shrink:0}.hud-price{font-family:monospace;font-size:13px;font-weight:800;color:var(--cyan);flex-shrink:0}.badge{padding:2px 7px;border-radius:20px;font-size:10px;font-weight:700;flex-shrink:0;white-space:nowrap}.badge.up{background:rgba(0,255,157,.12);color:var(--green);border:1px solid rgba(0,255,157,.2)}.badge.dn{background:rgba(255,61,94,.12);color:var(--red);border:1px solid rgba(255,61,94,.2)}.badge.gold{background:rgba(255,176,32,.1);color:var(--gold);border:1px solid rgba(255,176,32,.2)}.badge.cyan{background:rgba(0,229,255,.08);color:var(--cyan);border:1px solid rgba(0,229,255,.18)}.badge.purple{background:rgba(160,107,255,.1);color:var(--purple);border:1px solid rgba(160,107,255,.2)}.hud-right{margin-left:auto;display:flex;align-items:center;gap:7px}.dot{width:7px;height:7px;border-radius:50%;background:var(--muted);flex-shrink:0;transition:.5s}.dot.on{background:var(--green);box-shadow:0 0 8px var(--green);animation:dp 2s infinite}@keyframes dp{0%,100%{opacity:1}50%{opacity:.3}}.hud-time{font-family:monospace;font-size:10px;color:var(--muted);display:none}.hud-btn{padding:5px 9px;border:1px solid var(--border);border-radius:8px;font-size:10px;font-weight:700;color:var(--muted);text-decoration:none;background:rgba(255,255,255,.03);white-space:nowrap;cursor:pointer}.hud-btn.glow{border-color:rgba(0,229,255,.3);color:var(--cyan);background:rgba(0,229,255,.06);box-shadow:0 0 12px rgba(0,229,255,.1)}#ls{position:fixed;inset:0;background:var(--bg);z-index:999;display:flex;align-items:center;justify-content:center;padding:20px}#ls::before{content:\'\';position:fixed;inset:0;background:radial-gradient(ellipse at 30% 50%,rgba(0,229,255,.04) 0%,transparent 60%),radial-gradient(ellipse at 70% 20%,rgba(160,107,255,.04) 0%,transparent 50%);pointer-events:none}.lb{background:rgba(8,13,26,.98);border:1px solid var(--border);border-radius:24px;padding:36px 28px;width:100%;max-width:380px;text-align:center;position:relative;box-shadow:0 24px 80px rgba(0,0,0,.6)}.lb::before{content:\'\';position:absolute;inset:-1px;border-radius:24px;background:linear-gradient(135deg,rgba(0,229,255,.15),transparent,rgba(160,107,255,.1));z-index:-1}.lb-logo{font-family:monospace;font-size:32px;font-weight:900;color:var(--cyan);letter-spacing:4px;margin-bottom:3px;text-shadow:0 0 20px rgba(0,229,255,.5)}.lb-sub{font-size:10px;color:var(--muted);margin-bottom:28px;letter-spacing:3px;text-transform:uppercase}.fi{width:100%;padding:13px 16px;background:var(--bg3);border:1px solid var(--border);border-radius:12px;color:var(--text);font-size:16px;margin-bottom:10px;outline:none;transition:.2s}.fi:focus{border-color:var(--cyan);box-shadow:0 0 0 3px rgba(0,229,255,.1)}.btn-login{width:100%;padding:14px;border-radius:12px;font-size:14px;font-weight:800;cursor:pointer;border:none;background:linear-gradient(135deg,var(--cyan),#0097b2);color:#000;letter-spacing:.5px;margin-top:4px;transition:.15s;box-shadow:0 4px 20px rgba(0,229,255,.2)}.btn-login:active{transform:scale(.98)}#lm{font-size:12px;color:var(--red);margin-top:10px;min-height:16px}.tabs{position:fixed;bottom:0;left:0;right:0;background:rgba(2,4,10,.97);border-top:1px solid var(--border);display:flex;z-index:100;backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);padding-bottom:env(safe-area-inset-bottom)}.tab{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:8px 2px 10px;gap:3px;cursor:pointer;border:none;background:none;color:var(--muted);font-size:8.5px;font-weight:700;letter-spacing:.2px;transition:.15s;position:relative;min-width:0}.tab-ico{font-size:19px;line-height:1;transition:.15s}.tab.on{color:var(--cyan)}.tab.on .tab-ico{filter:drop-shadow(0 0 6px rgba(0,229,255,.6))}.tab.on::before{content:\'\';position:absolute;top:0;left:15%;right:15%;height:2px;background:linear-gradient(90deg,transparent,var(--cyan),transparent);border-radius:0 0 4px 4px}.notif-dot{position:absolute;top:7px;right:calc(50% - 15px);width:8px;height:8px;background:var(--red);border-radius:50%;display:none;border:2px solid var(--bg);animation:bounce .8s ease infinite}@keyframes bounce{0%,100%{transform:scale(1)}50%{transform:scale(1.2)}}.content{padding-top:56px;padding-bottom:calc(70px + env(safe-area-inset-bottom))}.view{display:none;padding:12px;max-width:960px;margin:0 auto}.view.on{display:block}.card{background:var(--bg2);border:1px solid var(--border);border-radius:16px;padding:16px;margin-bottom:12px;position:relative;overflow:hidden}.card::before{content:\'\';position:absolute;top:0;left:0;right:0;height:1px;background:linear-gradient(90deg,transparent,rgba(0,229,255,.1),transparent)}.card.cyan{border-color:rgba(0,229,255,.22)}.card.green{border-color:rgba(0,255,157,.22)}.card.red{border-color:rgba(255,61,94,.22)}.card.gold{border-color:rgba(255,176,32,.22)}.card.purple{border-color:rgba(160,107,255,.22)}.card.pink{border-color:rgba(255,110,180,.22)}.ct{font-size:9px;letter-spacing:2px;color:var(--muted);margin-bottom:12px;font-weight:700;text-transform:uppercase;display:flex;align-items:center;justify-content:space-between}.ct-badge{font-size:9px;padding:2px 7px;border-radius:20px;font-weight:700;letter-spacing:0}.g4{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:12px}.g3{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px}.g2{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}.g1{margin-bottom:12px}.st{background:var(--bg3);border:1px solid rgba(0,229,255,.07);border-radius:12px;padding:12px 8px;text-align:center;transition:.2s;cursor:default}.st:hover{border-color:rgba(0,229,255,.18);background:rgba(13,20,40,.8)}.sv{font-family:monospace;font-size:17px;font-weight:800;color:var(--cyan);margin-bottom:2px;line-height:1.1;transition:.3s}.sl{font-size:7.5px;color:var(--muted);letter-spacing:.8px;text-transform:uppercase}.sv.gold{color:var(--gold)}.sv.green{color:var(--green)}.sv.red{color:var(--red)}.sv.purple{color:var(--purple)}.sv.pink{color:var(--pink)}.sv.blue{color:var(--blue)}input,select,textarea{width:100%;padding:12px 14px;background:var(--bg3);border:1px solid var(--border);border-radius:11px;color:var(--text);font-size:14px;margin-bottom:8px;outline:none;transition:.2s;font-family:inherit}input:focus,select:focus,textarea:focus{border-color:var(--cyan);box-shadow:0 0 0 3px rgba(0,229,255,.08)}.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.grow{flex:1;min-width:0;margin-bottom:0!important}.btn{padding:10px 14px;border-radius:10px;font-size:12px;font-weight:700;cursor:pointer;border:1px solid var(--border);background:var(--bg3);color:var(--text);white-space:nowrap;flex-shrink:0;transition:.15s}.btn:active{transform:scale(.96)}.btn.cyan{background:rgba(0,229,255,.1);border-color:rgba(0,229,255,.3);color:var(--cyan)}.btn.green{background:rgba(0,255,157,.1);border-color:rgba(0,255,157,.3);color:var(--green)}.btn.red{background:rgba(255,61,94,.1);border-color:rgba(255,61,94,.3);color:var(--red)}.btn.gold{background:rgba(255,176,32,.1);border-color:rgba(255,176,32,.3);color:var(--gold)}.btn.purple{background:rgba(160,107,255,.1);border-color:rgba(160,107,255,.3);color:var(--purple)}.btn.pink{background:rgba(255,110,180,.1);border-color:rgba(255,110,180,.3);color:var(--pink)}.btn.primary{background:linear-gradient(135deg,var(--cyan),#0097b2);color:#000;border:none;box-shadow:0 2px 12px rgba(0,229,255,.2)}.btn.full{width:100%;padding:12px;font-size:13px;margin-bottom:8px}.ab{padding:10px 13px;border-radius:10px;font-size:12px;margin-bottom:6px;line-height:1.5;display:flex;align-items:flex-start;gap:8px}.ao{background:rgba(0,255,157,.07);border:1px solid rgba(0,255,157,.15);color:var(--green)}.aw{background:rgba(255,176,32,.07);border:1px solid rgba(255,176,32,.15);color:var(--gold)}.ae{background:rgba(255,61,94,.07);border:1px solid rgba(255,61,94,.15);color:var(--red)}.ai{background:rgba(0,229,255,.07);border:1px solid rgba(0,229,255,.15);color:var(--cyan)}.msg-line{font-size:11px;font-weight:600;margin-top:6px;padding:6px 10px;border-radius:8px}.msg-ok{background:rgba(0,255,157,.1);color:var(--green)}.msg-err{background:rgba(255,61,94,.1);color:var(--red)}.chart-wrap{position:relative;margin-bottom:10px}.chart-h200{height:200px}.chart-h150{height:150px}.sec{font-size:10px;color:var(--cyan);font-weight:700;letter-spacing:2px;text-transform:uppercase;margin:14px 0 8px;padding-left:2px;border-left:2px solid var(--cyan);padding-left:8px}table{width:100%;border-collapse:collapse;font-size:11px}th,td{padding:10px 8px;text-align:left;border-bottom:1px solid rgba(0,229,255,.05)}th{color:var(--muted);font-size:9px;text-transform:uppercase;letter-spacing:.5px;font-weight:700}tr:hover td{background:rgba(0,229,255,.02)}.tbl-wrap{overflow-x:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)}.fl-item{padding:10px 12px;border-bottom:1px solid rgba(0,229,255,.05);display:flex;align-items:center;gap:8px;font-size:12px}.fl-item:last-child{border:none}.fl-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}.fl-dot.in{background:var(--green);box-shadow:0 0 6px rgba(0,255,157,.4)}.fl-dot.out{background:var(--red);box-shadow:0 0 6px rgba(255,61,94,.4)}.fl-amt{font-weight:700;font-family:monospace;flex-shrink:0}.fl-amt.in{color:var(--green)}.fl-amt.out{color:var(--red)}.fl-user{color:var(--muted);font-size:11px;flex:1}.fl-time{color:var(--muted);font-size:10px;text-align:right}.fail-item{padding:12px;border-bottom:1px solid rgba(255,61,94,.08);display:flex;flex-direction:column;gap:7px}.fail-item:last-child{border:none}.tg{width:46px;height:25px;background:rgba(255,255,255,.07);border:1px solid var(--border);border-radius:13px;cursor:pointer;position:relative;flex-shrink:0;transition:.3s}.tg.on{background:rgba(0,229,255,.2);border-color:var(--cyan);box-shadow:0 0 10px rgba(0,229,255,.15)}.tg-k{position:absolute;top:3px;left:3px;width:17px;height:17px;background:#8899aa;border-radius:50%;transition:.3s}.tg.on .tg-k{left:24px;background:var(--cyan);box-shadow:0 0 8px rgba(0,229,255,.5)}.pbar{height:6px;background:rgba(0,0,0,.4);border-radius:3px;overflow:hidden;margin-top:4px}.pbar-fill{height:100%;border-radius:3px;transition:width .5s ease;background:linear-gradient(90deg,var(--cyan),var(--purple))}.trend-timer{font-family:monospace;font-size:11px;color:var(--muted)}.log-item{padding:8px 12px;border-bottom:1px solid rgba(0,229,255,.05);font-size:11px;display:flex;gap:8px;align-items:flex-start}.log-time{color:var(--muted);font-family:monospace;flex-shrink:0;font-size:10px}.log-txt{color:var(--text);flex:1}.log-ico{flex-shrink:0}.ibar{background:var(--bg2);border:1px solid var(--border);border-radius:14px 14px 0 0;padding:10px 14px;display:flex;align-items:center;gap:10px}.iurl{flex:1;font-size:10px;color:var(--muted);font-family:monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}#nf{width:100%;height:calc(100dvh - 56px - 70px - 46px);border:none;border:1px solid var(--border);border-top:none;border-radius:0 0 14px 14px;background:var(--bg)}.sw{position:relative}.sw input{padding-left:36px;margin:0}.sw::before{content:\'🔍\';position:absolute;left:12px;top:50%;transform:translateY(-50%);font-size:14px;z-index:1;pointer-events:none}.quick-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:8px}.qb{padding:10px 4px;border-radius:10px;font-size:11px;font-weight:700;cursor:pointer;border:1px solid var(--border);background:var(--bg3);color:var(--muted);text-align:center;transition:.15s}.qb:active{transform:scale(.95)}.qb.up{border-color:rgba(0,255,157,.25);color:var(--green);background:rgba(0,255,157,.06)}.qb.dn{border-color:rgba(255,61,94,.25);color:var(--red);background:rgba(255,61,94,.06)}.alert-cfg{display:flex;align-items:center;gap:10px;padding:10px 12px;background:var(--bg3);border-radius:10px;margin-bottom:6px;font-size:12px}@media(min-width:768px){.tabs{top:56px;bottom:auto;border-top:none;border-bottom:1px solid var(--border);overflow-x:auto;scrollbar-width:none}.tabs::-webkit-scrollbar{display:none}.tab{flex-direction:row;gap:7px;padding:13px 18px;font-size:12px;flex:none}.tab-ico{font-size:15px}.tab.on::before{top:auto;bottom:0;left:10%;right:10%;border-radius:4px 4px 0 0}.content{padding-top:108px;padding-bottom:20px}.view{padding:20px}.g4{grid-template-columns:repeat(4,1fr)}.sv{font-size:20px}.chart-h200{height:240px}.hud-time{display:block}#nf{height:calc(100vh - 108px - 56px)}}@media(max-width:480px){.g4{grid-template-columns:repeat(2,1fr);gap:7px}.g3{grid-template-columns:repeat(3,1fr)}.sv{font-size:14px}.quick-grid{grid-template-columns:repeat(4,1fr)}}</style></head><body><div id="ls"><div class="lb"><div class="lb-logo">◈ NEXUS</div><div class="lb-sub">Panneau Serveur</div><input id="mk" type="password" placeholder="Clé maître" class="fi" onkeydown="if(event.key===\'Enter\')conn()"><button class="btn-login" onclick="conn()">⚡ Connexion</button><div id="lm"></div></div></div><div class="hud"><div class="hud-logo">◈ <em>N</em>XC</div><div class="hud-sep"></div><div class="hud-price" id="hp">—</div><div class="badge" id="hc" style="display:none"></div><div class="badge gold" id="ht" style="display:none"></div><div class="hud-right"><div class="dot" id="hd"></div><span class="hud-time" id="htm">—</span><a href="/panel" class="hud-btn">Admin</a><a href="https://lively-art-86d9.noah-guetta.workers.dev/nexus_coin_90.html" target="_blank" class="hud-btn glow">◈ App ↗</a></div></div><div class="tabs"><button class="tab on" onclick="go(\'marche\',this)"><span class="tab-ico">📈</span>Marché</button><button class="tab" onclick="go(\'trading\',this)"><span class="tab-ico">⚙️</span>Contrôle</button><button class="tab" onclick="go(\'banque\',this)"><span class="tab-ico">🏦</span>Banque<span class="notif-dot" id="nd-b"></span></button><button class="tab" onclick="go(\'users\',this)"><span class="tab-ico">👥</span>Comptes</button><button class="tab" onclick="go(\'stats\',this)"><span class="tab-ico">📊</span>Stats</button><button class="tab" onclick="go(\'solv\',this)"><span class="tab-ico">🛡️</span>Solva.</button><button class="tab" onclick="go(\'log\',this)"><span class="tab-ico">📋</span>Journal</button><button class="tab" onclick="go(\'nexus\',this)"><span class="tab-ico">🌐</span>App</button><button class="tab" onclick="go(\'config\',this)"><span class="tab-ico">⚙️</span>Config</button><button class="tab" onclick="go(\'notifs\',this)"><span class="tab-ico">🔔</span>Alertes</button><button class="tab" onclick="go(\'rewards\',this)"><span class="tab-ico">🏆</span>Rewards</button><button class="tab" onclick="go(\'market2\',this)"><span class="tab-ico">🔮</span>Prévisions</button><button class="tab" onclick="go(\'compare\',this)"><span class="tab-ico">⚔️</span>Comparaison</button><button class="tab" onclick="go(\'security\',this)"><span class="tab-ico">🔐</span>Sécurité</button><button class="tab" onclick="go(\'tools\',this)"><span class="tab-ico">🛠️</span>Outils</button></div><div class="content"><div class="view on" id="view-marche"><div class="g4"><div class="st"><div class="sv" id="s-p">—</div><div class="sl">Prix R/NXC</div></div><div class="st"><div class="sv gold" id="s-v">—</div><div class="sl">Vol. 24h</div></div><div class="st"><div class="sv green" id="s-t">—</div><div class="sl">Trades 24h</div></div><div class="st"><div class="sv purple" id="s-h">—</div><div class="sl">Points hist.</div></div><div class="st"><div class="sv" id="s-high" style="color:var(--green)">—</div><div class="sl">Haut 24h</div></div><div class="st"><div class="sv" id="s-low" style="color:var(--red)">—</div><div class="sl">Bas 24h</div></div><div class="st"><div class="sv blue" id="s-var">—</div><div class="sl">Variation</div></div><div class="st"><div class="sv" id="s-cap" style="color:var(--pink)">—</div><div class="sl">Cap. marché</div></div></div><div class="card cyan"><div class="ct">◈ HISTORIQUE DU COURS\n<div style="display:flex;gap:6px"><button class="btn" onclick="setRange(25)" style="padding:3px 8px;font-size:9px">25</button><button class="btn cyan" id="rb-50" onclick="setRange(50)" style="padding:3px 8px;font-size:9px">50</button><button class="btn" onclick="setRange(100)" style="padding:3px 8px;font-size:9px">100</button></div></div><div class="chart-wrap chart-h200"><canvas id="ch"></canvas></div><div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:8px"><button class="btn gold" onclick="chObj&&chObj.zoom(1.5)">🔍+</button><button class="btn gold" onclick="chObj&&chObj.zoom(0.7)">🔍−</button><button class="btn" onclick="chObj&&chObj.resetZoom()">Reset</button><button class="btn cyan" onclick="toggleChartType()">📊 Type</button><button class="btn purple" onclick="downloadChart()">⬇️ PNG</button></div></div><div class="card"><div class="ct">◈ ALERTES MARCHÉ EN TEMPS RÉEL</div><div id="al"></div></div><div class="card gold"><div class="ct">◈ MINI RSI (DERNIERS 14 TICKS)</div><div class="chart-wrap chart-h150"><canvas id="ch-rsi"></canvas></div><div style="font-size:11px;color:var(--muted);margin-top:6px">RSI >70 = surachat (rouge) · RSI <30 = survente (vert) · Zone neutre 30-70</div></div></div><div class="view" id="view-trading"><div class="card cyan"><div class="ct">◈ MODIFIER LE COURS</div><div class="sec">Raccourcis rapides</div><div class="quick-grid"><button class="qb up" onclick="adjPrice(0.05)">+5%</button><button class="qb up" onclick="adjPrice(0.02)">+2%</button><button class="qb up" onclick="adjPrice(0.01)">+1%</button><button class="qb up" onclick="adjPrice(0.005)">+0.5%</button><button class="qb dn" onclick="adjPrice(-0.005)">-0.5%</button><button class="qb dn" onclick="adjPrice(-0.01)">-1%</button><button class="qb dn" onclick="adjPrice(-0.02)">-2%</button><button class="qb dn" onclick="adjPrice(-0.05)">-5%</button></div><div class="sec">Prix exact</div><div class="row"><input id="np" type="number" min="50" max="100000" placeholder="Nouveau prix (50 – 100 000)" class="grow"><button class="btn primary" onclick="setP()">✓</button></div><div class="sec">Prix cible en %</div><div class="row"><input id="np-pct" type="number" placeholder="Ex:+10 ou -5" class="grow"><button class="btn cyan" onclick="setPct()">Appliquer %</button></div><div id="pm" style="min-height:14px;font-size:11px;margin-top:4px"></div></div><div class="card"><div class="ct">◈ TENDANCE AUTOMATIQUE\n<span class="trend-timer" id="tt-timer"></span></div><div class="sec">Vitesse</div><select id="ts" style="margin-bottom:10px"><option value="0.001">Ultra lent (0.1%)</option><option value="0.002">Très lent (0.2%)</option><option value="0.005" selected>Lent (0.5%)</option><option value="0.01">Moyen (1%)</option><option value="0.02">Rapide (2%)</option><option value="0.05">Très rapide (5%)</option><option value="0.1">Extrême (10%)</option></select><div class="sec">Intervalle</div><select id="ti" style="margin-bottom:10px"><option value="5000">5 secondes</option><option value="12000" selected>12 secondes</option><option value="30000">30 secondes</option><option value="60000">1 minute</option></select><div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px"><button class="btn green full" onclick="setT(\'up\')">📈 Hausse</button><button class="btn red full" onclick="setT(\'down\')">📉 Baisse</button><button class="btn purple full" onclick="setT(\'random\')">🎲 Aléatoire</button><button class="btn full" style="color:var(--muted)" onclick="setT(\'stop\')">⏸ Stop</button></div><div id="tst" style="font-size:12px;color:var(--muted);font-weight:600;padding:8px;background:var(--bg3);border-radius:8px;text-align:center">⏸ Arrêté</div></div><div class="card gold"><div class="ct">◈ SIMULATION DE SCÉNARIOS</div><div style="display:grid;grid-template-columns:1fr 1fr;gap:8px"><button class="btn gold" onclick="scenario(\'crash\')">💥 Crash −30%</button><button class="btn gold" onclick="scenario(\'moon\')">🚀 Moon +30%</button><button class="btn gold" onclick="scenario(\'volatile\')">⚡ Très volatil</button><button class="btn gold" onclick="scenario(\'stable\')">😴 Stabiliser</button><button class="btn gold" onclick="scenario(\'ath\')">🏆 Nouveau ATH</button><button class="btn gold" onclick="scenario(\'floor\')">🛑 Plancher 200R</button></div></div><div class="card"><div class="ct">◈ RESET &amp;MAINTENANCE</div><button class="btn full" style="color:var(--gold);border-color:rgba(255,176,32,.3);background:rgba(255,176,32,.06)" onclick="resetH()">🔄 Reset historique</button><button class="btn full red" onclick="confirmReset()">⚠️ Reset complet (prix + historique)</button></div></div><div class="view" id="view-banque"><div class="g4"><div class="st"><div class="sv" style="color:#00b4d8;font-size:14px" id="bk-r">—</div><div class="sl">Réserves R</div></div><div class="st"><div class="sv gold" style="font-size:14px" id="bk-i">—</div><div class="sl">Total entré</div></div><div class="st"><div class="sv red" style="font-size:14px" id="bk-o">—</div><div class="sl">Total sorti</div></div><div class="st"><div class="sv green" style="font-size:14px" id="bk-rt">—</div><div class="sl">Ratio</div></div><div class="st"><div class="sv purple" style="font-size:14px" id="bk-nx">—</div><div class="sl">NXC émis</div></div><div class="st"><div class="sv blue" style="font-size:14px" id="bk-vx">—</div><div class="sl">Val. stock</div></div><div class="st"><div class="sv" style="font-size:14px" id="bk-bn">—</div><div class="sl">Bénéfice net</div></div><div class="st"><div class="sv" style="font-size:14px;color:var(--pink)" id="bk-fl">—</div><div class="sl">Nb. flux</div></div></div><div class="card cyan"><div class="ct">◈ OPÉRATIONS</div><div class="row" style="margin-bottom:8px"><input id="bk-amt" type="number" min="1" placeholder="Montant (R)" class="grow"><button class="btn green" onclick="bankInject()">+ Injecter</button><button class="btn red" onclick="bankRetire()">− Retirer</button></div><div class="sec">Montants rapides</div><div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px"><button class="btn cyan" onclick="setAmt(100)" style="padding:6px 10px;font-size:11px">100</button><button class="btn cyan" onclick="setAmt(500)" style="padding:6px 10px;font-size:11px">500</button><button class="btn cyan" onclick="setAmt(1000)" style="padding:6px 10px;font-size:11px">1 000</button><button class="btn cyan" onclick="setAmt(5000)" style="padding:6px 10px;font-size:11px">5 000</button><button class="btn cyan" onclick="setAmt(10000)" style="padding:6px 10px;font-size:11px">10 000</button><button class="btn cyan" onclick="setAmt(50000)" style="padding:6px 10px;font-size:11px">50 000</button></div><div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:6px"><button class="btn gold" onclick="bankResetHist()" style="font-size:11px">🗑️ Reset hist.</button><button class="btn red" onclick="bankResetAll()" style="font-size:11px">💥 Reset complet</button><button class="btn purple" onclick="loadBank()" style="font-size:11px">🔄 Actualiser</button></div><div id="bk-msg" style="min-height:14px;font-size:11px;font-weight:600"></div></div><div class="card"><div class="ct">◈ FLUX BANCAIRES\n<div style="display:flex;gap:6px"><button class="btn" id="fl-all" onclick="filterFlux(\'all\')" style="padding:3px 8px;font-size:9px;color:var(--cyan)">Tous</button><button class="btn" id="fl-in" onclick="filterFlux(\'IN\')" style="padding:3px 8px;font-size:9px">Entrées</button><button class="btn" id="fl-out" onclick="filterFlux(\'OUT\')" style="padding:3px 8px;font-size:9px">Sorties</button></div></div><div id="bk-flux" style="max-height:240px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"></div><div style="margin-top:8px;display:flex;gap:8px"><button class="btn cyan" onclick="exportFlux()" style="font-size:11px">📊 Export CSV</button></div></div><div class="card red"><div class="ct">⚠️ TENTATIVES ÉCHOUÉES\n<span id="fails-count" class="badge" style="display:none">0</span></div><div id="bk-fails" style="max-height:220px;overflow-y:auto"></div></div></div><div class="view" id="view-users"><div class="g3"><div class="st"><div class="sv" id="u-total">—</div><div class="sl">Total comptes</div></div><div class="st"><div class="sv gold" id="u-admins">—</div><div class="sl">Admins</div></div><div class="st"><div class="sv green" id="u-rew">—</div><div class="sl">Total rewards</div></div></div><div class="card"><div class="ct">◈ UTILISATEURS\n<div style="display:flex;gap:6px"><button class="btn" id="sort-rew" onclick="sortUsers(\'rew\')" style="padding:3px 8px;font-size:9px">🏆 Rewards</button><button class="btn" id="sort-nxc" onclick="sortUsers(\'nxc\')" style="padding:3px 8px;font-size:9px">◈ NXC</button><button class="btn" id="sort-name" onclick="sortUsers(\'name\')" style="padding:3px 8px;font-size:9px">A-Z</button></div></div><div class="sw" style="margin-bottom:10px"><input id="us-q" placeholder="Rechercher un compte..." oninput="filterU()" style="margin:0"></div><div class="tbl-wrap"><table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur R</th></tr></thead><tbody id="ut"></tbody></table></div><div id="us-msg" style="font-size:11px;color:var(--muted);margin-top:8px;text-align:center"></div></div></div><div class="view" id="view-stats"><div class="card purple"><div class="ct">◈ ÉVOLUTION DU VOLUME 24H</div><div class="chart-wrap chart-h150"><canvas id="ch-vol"></canvas></div></div><div class="card gold"><div class="ct">◈ DISTRIBUTION DES REWARDS PAR UTILISATEUR</div><div id="rew-bars"></div></div><div class="card"><div class="ct">◈ SANTÉ DU MARCHÉ</div><div id="health-grid" class="g2"></div></div><div class="card cyan"><div class="ct">◈ MÉTRIQUES AVANCÉES</div><div id="metrics"></div></div></div><div class="view" id="view-solv"><div class="card"><div class="ct">◈ CONTRÔLE DE SOLVABILITÉ</div><div style="display:flex;align-items:center;gap:14px;padding:14px;background:var(--bg3);border-radius:12px;margin-bottom:14px;cursor:pointer" onclick="toggleSolv()"><div class="tg" id="stg"><div class="tg-k"></div></div><div><div id="sl" style="font-size:14px;font-weight:700;color:var(--muted)">Désactivée</div><div style="font-size:11px;color:var(--muted);margin-top:2px">Toucher pour activer / désactiver</div></div></div><div class="sec">Geste commercial automatique</div><div class="row" style="margin-bottom:8px"><input id="sg" type="number" min="0" value="50" class="grow" placeholder="Rewards offerts (R)"><button class="btn primary" onclick="saveSolv()">Sauver</button></div><div class="sec">Montants de geste rapides</div><div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px"><button class="btn cyan" onclick="$(\'sg\').value=10" style="font-size:11px">10 R</button><button class="btn cyan" onclick="$(\'sg\').value=50" style="font-size:11px">50 R</button><button class="btn cyan" onclick="$(\'sg\').value=100" style="font-size:11px">100 R</button><button class="btn cyan" onclick="$(\'sg\').value=500" style="font-size:11px">500 R</button></div><div id="sm" style="min-height:14px;font-size:11px;font-weight:600"></div></div><div class="card ai"><div class="ct">◈ COMMENT ÇA MARCHE</div><div style="font-size:12px;line-height:2;color:var(--muted)"><div>🔴 <b style="color:var(--text)">Activée</b> — vente bloquée si réserves insuffisantes</div><div>💰 <b style="color:var(--text)">Geste</b> — X rewards offerts automatiquement</div><div>📋 <b style="color:var(--text)">Journal</b> — tentatives visibles dans l\'onglet Banque</div><div>💝 <b style="color:var(--text)">Manuel</b> — bouton "Verser" pour envoyer le geste</div></div></div></div><div class="view" id="view-log"><div class="card"><div class="ct">◈ JOURNAL DES ACTIONS ADMIN\n<button class="btn red" onclick="clearLog()" style="padding:3px 8px;font-size:9px">🗑️ Vider</button></div><div id="log-list" style="max-height:400px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"><p style="color:var(--muted);padding:20px;text-align:center;font-size:12px">Aucune action enregistrée</p></div></div></div><div class="view" id="view-nexus"><div class="ibar"><span style="color:var(--cyan);font-size:13px;font-weight:800">◈ Nexus Coin</span><span class="iurl">lively-art-86d9.noah-guetta.workers.dev</span><button class="btn cyan" onclick="$(\'nf\').src=$(\'nf\').src" style="padding:6px 10px;font-size:11px">🔄</button><a href="https://lively-art-86d9.noah-guetta.workers.dev/nexus_coin_90.html" target="_blank" class="btn gold" style="text-decoration:none;padding:6px 10px;font-size:11px">↗ Ouvrir</a></div><iframe id="nf" src="https://lively-art-86d9.noah-guetta.workers.dev/nexus_coin_90.html" allow="clipboard-write"></iframe></div><div class="view" id="view-config"><div class="card purple"><div class="ct">◈ PRIX PLANCHER / PLAFOND AUTOMATIQUES</div><div class="sec">Plancher — le prix ne descendra jamais en dessous</div><div class="row" style="margin-bottom:8px"><input id="cfg-floor" type="number" min="50" placeholder="Prix minimum (R)" class="grow"><button class="btn purple" onclick="cfgFloor()">Définir</button><button class="btn red" onclick="_cfgFloor=null;updCfgMsg()" style="padding:10px">✕</button></div><div class="sec">Plafond — le prix ne montera jamais au-dessus</div><div class="row" style="margin-bottom:8px"><input id="cfg-ceil" type="number" max="100000" placeholder="Prix maximum (R)" class="grow"><button class="btn purple" onclick="cfgCeil()">Définir</button><button class="btn red" onclick="_cfgCeil=null;updCfgMsg()" style="padding:10px">✕</button></div><div id="cfg-floor-val" style="font-size:11px;color:var(--muted);padding:8px;background:var(--bg3);border-radius:8px">Plancher:non défini · Plafond:non défini</div></div><div class="card gold"><div class="ct">◈ TENDANCE PROGRAMMÉE</div><div class="sec">Démarrage (HH:MM)</div><input id="cfg-start" type="time" style="margin-bottom:8px"><div class="sec">Arrêt (HH:MM)</div><input id="cfg-stop" type="time" style="margin-bottom:8px"><div class="sec">Direction</div><select id="cfg-dir" style="margin-bottom:8px"><option value="up">Hausse</option><option value="down">Baisse</option><option value="random">Aléatoire</option></select><button class="btn gold full" onclick="scheduleT()">⏰ Programmer</button><button class="btn full" style="color:var(--muted)" onclick="if(_schedInt){clearInterval(_schedInt);_schedInt=null;setMsg(\'cfg-sched-msg\',\'Programmation annulée\',false);}">✕ Annuler</button><div id="cfg-sched-msg" style="font-size:11px;font-weight:600;margin-top:4px"></div></div><div class="card"><div class="ct">◈ PARAMÈTRES AVANCÉS DE SIMULATION</div><div class="sec">Multiplicateur de volatilité</div><div class="row" style="margin-bottom:12px"><input id="cfg-vol" type="range" min="1" max="10" value="1" oninput="$(\'cfg-vol-val\').textContent=this.value+\'x\'" style="flex:1;margin:0;background:none;border:none;padding:8px 0"><span id="cfg-vol-val" style="color:var(--purple);font-weight:700;font-size:16px;width:36px;text-align:right;flex-shrink:0">1x</span></div><div class="sec">Bruit aléatoire par tick</div><div class="row" style="margin-bottom:12px"><input id="cfg-noise" type="range" min="0" max="5" value="0.8" step="0.1" oninput="$(\'cfg-noise-val\').textContent=this.value+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:8px 0"><span id="cfg-noise-val" style="color:var(--cyan);font-weight:700;font-size:16px;width:40px;text-align:right;flex-shrink:0">0.8%</span></div><button class="btn primary full" onclick="applyAdvCfg()">✓ Appliquer la config</button><div id="cfg-adv-msg" style="font-size:11px;font-weight:600;margin-top:4px"></div></div><div class="card cyan"><div class="ct">◈ EXPORT DONNÉES</div><button class="btn cyan full" onclick="exportHist()">📥 Historique prix JSON</button><button class="btn purple full" onclick="exportStats()">📊 Rapport complet JSON</button><button class="btn gold full" onclick="exportFlux()">💰 Flux bancaires CSV</button></div></div><div class="view" id="view-notifs"><div class="card gold"><div class="ct">◈ ALERTES DE PRIX PERSONNALISÉES</div><div class="row" style="margin-bottom:8px"><input id="al-price" type="number" placeholder="Prix cible (R)" class="grow"><select id="al-dir" style="width:auto;flex-shrink:0;margin:0;font-size:12px;padding:10px 8px"><option value="above">Si prix &gt;</option><option value="below">Si prix &lt;</option></select><button class="btn gold" onclick="addAlert()">+ Alerte</button></div><div id="al-list" style="max-height:200px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"><p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune alerte configurée</p></div></div><div class="card"><div class="ct">◈ ALERTES INTELLIGENTES TEMPS RÉEL</div><div id="smart-alerts"></div></div><div class="card purple"><div class="ct">◈ HISTORIQUE DES ALERTES DÉCLENCHÉES\n<button class="btn red" onclick="_alertHist=[];renderAlertHist()" style="padding:3px 8px;font-size:9px">Vider</button></div><div id="al-hist" style="max-height:220px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"><p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune alerte déclenchée</p></div></div><div class="card ai"><div class="ct">◈ ALERTES DE VARIATIONS AUTOMATIQUES</div><div style="font-size:12px;line-height:2;color:var(--muted)"><div>🔴 <b style="color:var(--text)">Critique</b> — prix &gt;80 000 R ou &lt;500 R</div><div>🟡 <b style="color:var(--text)">Avertissement</b> — volatilité &gt;15% sur 10 ticks</div><div>🟢 <b style="color:var(--text)">Info</b> — tendance haussière ou baissière détectée</div><div>🔔 <b style="color:var(--text)">Notifs</b> — notifications navigateur si autorisées</div></div></div></div><div class="view" id="view-rewards"><div class="g3"><div class="st"><div class="sv gold" id="rw-total">—</div><div class="sl">Total rewards distribués</div></div><div class="st"><div class="sv green" id="rw-avg">—</div><div class="sl">Moyenne par compte</div></div><div class="st"><div class="sv purple" id="rw-max">—</div><div class="sl">Record rewards</div></div></div><div class="card gold"><div class="ct">◈ DONNER DES REWARDS À UN UTILISATEUR</div><div class="sec">Utilisateur cible</div><select id="rw-target" style="margin-bottom:8px"><option value="">Sélectionner...</option></select><div class="sec">Montant de rewards</div><div class="row" style="margin-bottom:8px"><input id="rw-amount" type="number" min="1" placeholder="Rewards à donner" class="grow"><button class="btn gold" onclick="giveRewards()">💰 Donner</button></div><div class="sec">Montants rapides</div><div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px"><button class="btn gold" onclick="$(\'rw-amount\').value=10" style="font-size:11px">10</button><button class="btn gold" onclick="$(\'rw-amount\').value=50" style="font-size:11px">50</button><button class="btn gold" onclick="$(\'rw-amount\').value=100" style="font-size:11px">100</button><button class="btn gold" onclick="$(\'rw-amount\').value=500" style="font-size:11px">500</button><button class="btn gold" onclick="$(\'rw-amount\').value=1000" style="font-size:11px">1 000</button><button class="btn gold" onclick="$(\'rw-amount\').value=5000" style="font-size:11px">5 000</button></div><div id="rw-msg" style="font-size:11px;font-weight:600;min-height:14px"></div></div><div class="card red"><div class="ct">◈ RETIRER DES REWARDS À UN UTILISATEUR</div><div class="row" style="margin-bottom:8px"><select id="rw-target2" style="flex:1;margin:0"><option value="">Sélectionner...</option></select><input id="rw-remove" type="number" min="1" placeholder="Montant" style="flex:1;margin:0"><button class="btn red" onclick="removeRewards()">− Retirer</button></div><div id="rw-msg2" style="font-size:11px;font-weight:600;min-height:14px"></div></div><div class="card purple"><div class="ct">◈ RESET REWARDS D\'UN UTILISATEUR</div><div class="row"><select id="rw-target3" style="flex:1;margin:0"><option value="">Sélectionner...</option></select><button class="btn red" onclick="resetRewards()">🗑️ Remettre à zéro</button></div><div id="rw-msg3" style="font-size:11px;font-weight:600;min-height:14px;margin-top:8px"></div></div><div class="card"><div class="ct">◈ CLASSEMENT REWARDS</div><div id="rw-leaderboard"></div></div></div><div class="view" id="view-market2"><div class="card purple"><div class="ct">◈ PRÉVISION TRÉSORERIE 30 JOURS</div><div class="chart-wrap chart-h200"><canvas id="ch-treso"></canvas></div><div style="display:flex;gap:12px;margin-top:10px;font-size:11px;flex-wrap:wrap"><span style="color:var(--green)">▬ Optimiste</span><span style="color:var(--cyan)">▬ Réaliste</span><span style="color:var(--red)">▬ Pessimiste</span></div></div><div class="card gold"><div class="ct">◈ SIMULATION DE COURS FUTUR</div><div class="sec">Dans combien de jours ?</div><div class="row" style="margin-bottom:8px"><input id="sim-days" type="number" min="1" max="365" value="7" placeholder="Jours" class="grow"><select id="sim-scenario" style="flex:1;margin:0"><option value="bull">Haussier (+2%/j)</option><option value="bear">Baissier (-2%/j)</option><option value="flat">Stable (+0.1%/j)</option><option value="volatile">Volatil (±5%/j)</option></select><button class="btn gold" onclick="simulateFuture()">Simuler</button></div><div id="sim-result" style="font-size:13px;font-weight:700;padding:12px;background:var(--bg3);border-radius:10px;text-align:center;min-height:44px"></div></div><div class="card cyan"><div class="ct">◈ INDICATEURS TECHNIQUES</div><div id="tech-indicators"></div></div><div class="card"><div class="ct">◈ ZONES DE SUPPORT ET RÉSISTANCE</div><div id="support-resistance"></div></div></div><div class="view" id="view-compare"><div class="card cyan"><div class="ct">◈ COMPARER DEUX UTILISATEURS</div><div class="g2" style="margin-bottom:10px"><div><div class="sec">Utilisateur A</div><select id="cmp-a" style="margin:0"><option value="">Sélectionner...</option></select></div><div><div class="sec">Utilisateur B</div><select id="cmp-b" style="margin:0"><option value="">Sélectionner...</option></select></div></div><button class="btn primary full" onclick="compareUsers()">⚔️ Comparer</button></div><div id="cmp-result"></div><div class="card gold"><div class="ct">◈ TOP 3 PAR CATÉGORIE</div><div id="top3"></div></div></div><div class="view" id="view-security"><div class="card red"><div class="ct">◈ TENTATIVES DE CONNEXION ÉCHOUÉES</div><div id="sec-fails" style="max-height:200px;overflow-y:auto;border-radius:10px;border:1px solid rgba(255,61,94,.1)"><p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Données non disponibles</p></div></div><div class="card gold"><div class="ct">◈ ACTIVITÉ RÉCENTE SUSPECTE</div><div id="sec-suspicious"></div></div><div class="card"><div class="ct">◈ STATISTIQUES DE SÉCURITÉ</div><div class="g4"><div class="st"><div class="sv green" id="sec-ok">—</div><div class="sl">Ventes OK</div></div><div class="st"><div class="sv red" id="sec-fail">—</div><div class="sl">Ventes échouées</div></div><div class="st"><div class="sv gold" id="sec-gests">—</div><div class="sl">Gestes versés</div></div><div class="st"><div class="sv purple" id="sec-ratio">—</div><div class="sl">Taux échec</div></div></div></div><div class="card purple"><div class="ct">◈ CHANGER LA CLÉ MAÎTRE (SESSION UNIQUEMENT)</div><div class="row"><input id="new-key" type="password" placeholder="Nouvelle clé (session uniquement)" class="grow"><button class="btn purple" onclick="changeKey()">🔑 Changer</button></div><div id="key-msg" style="font-size:11px;color:var(--muted);margin-top:6px">⚠️ Change uniquement en mémoire — redéployer le serveur pour rendre permanent</div></div></div><div class="view" id="view-tools"><div class="card cyan"><div class="ct">◈ CALCULATRICE NXC ↔ REWARDS</div><div class="sec">Convertir</div><div class="row" style="margin-bottom:8px"><input id="calc-nxc" type="number" placeholder="NXC" class="grow" oninput="calcNxc()"><span style="color:var(--muted);flex-shrink:0;font-size:20px">→</span><input id="calc-rew" type="number" placeholder="Rewards" class="grow" style="background:rgba(0,229,255,.05)" readonly></div><div class="row"><input id="calc-rew2" type="number" placeholder="Rewards" class="grow" oninput="calcRew()"><span style="color:var(--muted);flex-shrink:0;font-size:20px">→</span><input id="calc-nxc2" type="number" placeholder="NXC" class="grow" style="background:rgba(0,229,255,.05)" readonly></div></div><div class="card gold"><div class="ct">◈ SIMULATEUR DE VENTE</div><div class="sec">Si un utilisateur vend X NXC maintenant</div><div class="row" style="margin-bottom:8px"><input id="sell-sim-nxc" type="number" placeholder="NXC à vendre" class="grow" oninput="simSell()"><input id="sell-sim-fee" type="number" placeholder="Frais %" value="0" class="grow" oninput="simSell()"></div><div id="sell-sim-result" style="padding:12px;background:var(--bg3);border-radius:10px;font-size:13px;min-height:44px"></div></div><div class="card purple"><div class="ct">◈ GÉNÉRATEUR DE PRIX ALÉATOIRE</div><div class="row" style="margin-bottom:8px"><input id="rnd-min" type="number" placeholder="Min (R)" value="1000" class="grow"><input id="rnd-max" type="number" placeholder="Max (R)" value="10000" class="grow"><button class="btn purple" onclick="genRandPrice()">🎲 Générer</button></div><div id="rnd-result" style="font-size:24px;font-weight:900;font-family:monospace;color:var(--purple);text-align:center;padding:16px;background:var(--bg3);border-radius:10px;min-height:60px"></div><button class="btn purple full" onclick="applyRandPrice()" style="margin-top:8px">✓ Appliquer ce prix</button></div><div class="card"><div class="ct">◈ MINUTEUR ADMIN</div><div class="row" style="margin-bottom:8px"><input id="timer-min" type="number" min="0" placeholder="Minutes" value="5" class="grow"><input id="timer-sec" type="number" min="0" max="59" placeholder="Secondes" value="0" class="grow"><select id="timer-action" style="flex:1;margin:0;font-size:11px"><option value="stop">Arrêter tendance</option><option value="up">Lancer hausse</option><option value="down">Lancer baisse</option><option value="crash">Crash -30%</option><option value="moon">Moon +30%</option></select></div><button class="btn cyan full" onclick="startTimer()">⏱️ Démarrer minuteur</button><button class="btn full" style="color:var(--muted)" onclick="stopTimer()">✕ Annuler</button><div id="timer-display" style="font-family:monospace;font-size:36px;font-weight:900;color:var(--cyan);text-align:center;padding:16px;min-height:64px"></div></div><div class="card green"><div class="ct">◈ PING SERVEUR</div><button class="btn green full" onclick="pingServer()">📡 Tester la connexion</button><div id="ping-result" style="font-size:13px;font-weight:700;text-align:center;padding:12px;min-height:40px"></div></div></div></div><script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js">\n// ══ CONFIG AVANCÉE ══\nvar _cfgFloor=null,_cfgCeil=null,_cfgVolMult=1,_cfgNoise=0.008,_schedInt=null;function cfgFloor(){var v=parseFloat($(\'cfg-floor\').value);if(!v||v<50)return;_cfgFloor=v;updCfgMsg();addLog(\'⚙️\',\'Plancher fixé:\'+fmt(v,0)+\' R\');}function cfgCeil(){var v=parseFloat($(\'cfg-ceil\').value);if(!v||v>100000)return;_cfgCeil=v;updCfgMsg();addLog(\'⚙️\',\'Plafond fixé:\'+fmt(v,0)+\' R\');}function updCfgMsg(){var el=$(\'cfg-floor-val\');if(el)el.textContent=\'Plancher:\'+(_cfgFloor?fmt(_cfgFloor,0)+\' R\':\'non défini\')+\' · Plafond:\'+(_cfgCeil?fmt(_cfgCeil,0)+\' R\':\'non défini\');}function applyAdvCfg(){_cfgVolMult=parseFloat($(\'cfg-vol\').value)||1;_cfgNoise=parseFloat($(\'cfg-noise\').value)/100||0.008;setMsg(\'cfg-adv-msg\',\'✅ Volatilité x\'+_cfgVolMult+\' · bruit \'+(_cfgNoise*100).toFixed(1)+\'%\',true);addLog(\'⚙️\',\'Config:volatilité x\'+_cfgVolMult+\',bruit \'+(_cfgNoise*100).toFixed(1)+\'%\');}function scheduleT(){var start=$(\'cfg-start\').value,stop=$(\'cfg-stop\').value,dir=$(\'cfg-dir\')?$(\'cfg-dir\').value:\'up\';if(!start||!stop){setMsg(\'cfg-sched-msg\',\'Renseigner les deux heures\',false);return;}if(_schedInt)clearInterval(_schedInt);_schedInt=setInterval(function(){var now=new Date(),cur=(\'0\'+now.getHours()).slice(-2)+\':\'+(\'0\'+now.getMinutes()).slice(-2);if(cur===start&&!tMode){setT(dir);addLog(\'⏰\',\'Tendance démarrée automatiquement\');}if(cur===stop&&tMode){setT(\'stop\');addLog(\'⏰\',\'Tendance arrêtée automatiquement\');}},30000);setMsg(\'cfg-sched-msg\',\'✅ Programmé:\'+dir+\' de \'+start+\' à \'+stop,true);addLog(\'⏰\',\'Tendance programmée \'+dir+\' \'+start+\'→\'+stop);}function exportHist(){var h=mkt.history||[];var blob=new Blob([JSON.stringify({exported:new Date().toISOString(),price:mkt.price,count:h.length,history:h},null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_history_\'+Date.now()+\'.json\';a.click();addLog(\'📥\',\'Export historique JSON (\'+h.length+\' pts)\');}function exportStats(){var blob=new Blob([JSON.stringify({exported:new Date().toISOString(),market:mkt,users:_users,config:{floor:_cfgFloor,ceil:_cfgCeil,volMult:_cfgVolMult,noise:_cfgNoise}},null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_report_\'+Date.now()+\'.json\';a.click();addLog(\'📊\',\'Export rapport complet JSON\');}// ══ ALERTES PERSONNALISÉES ══\nvar _alerts=[],_alertHist=[];function addAlert(){var price=parseFloat($(\'al-price\').value),dir=$(\'al-dir\').value;if(!price||price<=0)return;_alerts.push({price:price,dir:dir,id:Date.now(),triggered:false});$(\'al-price\').value=\'\';renderAlerts();addLog(\'🔔\',\'Alerte:prix \'+(dir===\'above\'?\'>\':\'<\')+\' \'+fmt(price,0)+\' R\');}function removeAlert(id){_alerts=_alerts.filter(function(a){return a.id!==id;});renderAlerts();}function renderAlerts(){var el=$(\'al-list\');if(!el)return;el.innerHTML=_alerts.length?_alerts.map(function(a){return \'<div style="padding:10px 12px;border-bottom:1px solid rgba(0,229,255,.05);display:flex;justify-content:space-between;align-items:center;gap:8px;font-size:12px">\'\n+\'<span style="color:\'+(a.triggered?\'var(--muted)\':\'var(--gold)\')+\'">Si prix \'+(a.dir===\'above\'?\'>\':\'<\')+\' <b>\'+fmt(a.price,0)+\'</b> R\'+(a.triggered?\' ✅ déclenchée\':\'\')+\'</span>\'\n+\'<button onclick="removeAlert(\'+a.id+\')" style="padding:4px 8px;border-radius:6px;background:rgba(255,61,94,.1);border:1px solid rgba(255,61,94,.3);color:var(--red);font-size:10px;cursor:pointer;flex-shrink:0">✕</button></div>\';}).join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune alerte</p>\';}function checkAlerts(p){var triggered=false;_alerts.forEach(function(a){if(a.triggered)return;if((a.dir===\'above\'&&p>a.price)||(a.dir===\'below\'&&p<a.price)){a.triggered=true;triggered=true;var m=\'🔔 ALERTE:prix \'+(a.dir===\'above\'?\'>\':\'<\')+\' \'+fmt(a.price,0)+\' R (actuel:\'+fmt(p,0)+\' R)\';_alertHist.unshift({ts:Date.now(),msg:m});addLog(\'🔔\',m);renderAlerts();renderAlertHist();if(window.Notification&&Notification.permission===\'granted\')new Notification(\'◈ Nexus NXC\',{body:m,icon:\'/favicon.ico\'});}});// Smart alerts\nvar smart=[];if(p>80000)smart.push({c:\'ae\',m:\'🚨 CRITIQUE:Prix extrême >80 000 R\'});else if(p<500)smart.push({c:\'ae\',m:\'🚨 CRITIQUE:Effondrement <500 R\'});else if(p<2000)smart.push({c:\'aw\',m:\'⚠️ Prix bas (<2 000 R) — surveiller\'});if(tMode===\'down\'&&p<2000)smart.push({c:\'ae\',m:\'🚨 Tendance baissière + prix bas = risque critique\'});if(!smart.length)smart.push({c:\'ao\',m:\'✅ Aucune alerte critique — marché sain\'});var el=$(\'smart-alerts\');if(el)el.innerHTML=smart.map(function(s){return \'<div class="ab \'+s.c+\'">\'+s.m+\'</div>\';}).join(\'\');}function renderAlertHist(){var el=$(\'al-hist\');if(!el)return;el.innerHTML=_alertHist.length?_alertHist.map(function(a){return \'<div class="log-item"><span class="log-time">\'+fmtTime(a.ts)+\'</span><span class="log-txt" style="color:var(--gold)">\'+esc(a.msg)+\'</span></div>\';}).join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune alerte déclenchée</p>\';}// Notifications navigateur\nif(window.Notification&&Notification.permission===\'default\'){setTimeout(function(){Notification.requestPermission();},3000);}// Override ref pour intégrer les alertes et config\nvar _origRef=ref;ref=async function(){await _origRef();var p=parseFloat(mkt.price||0);if(!p)return;// Appliquer plancher/plafond\nif(_cfgFloor&&p<_cfgFloor){await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:_cfgFloor,ts:Date.now(),vol:0,volume24:mkt.volume24||0,trades24:mkt.trades24||0})});addLog(\'⚙️\',\'Plancher activé:prix remonté à \'+fmt(_cfgFloor,0)+\' R\');}if(_cfgCeil&&p>_cfgCeil){await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:_cfgCeil,ts:Date.now(),vol:0,volume24:mkt.volume24||0,trades24:mkt.trades24||0})});addLog(\'⚙️\',\'Plafond activé:prix rabaissé à \'+fmt(_cfgCeil,0)+\' R\');}checkAlerts(p);};// ══ REWARDS ADMIN ══\nasync function loadRewardsAdmin(){if(!_users.length)await loadUsers();var total=_users.reduce((s,u)=>s+u.rew,0);var avg=_users.length?Math.round(total/_users.length):0;var maxU=_users.reduce((m,u)=>u.rew>m.rew?u:m,{rew:0,n:\'—\'});var el=$(\'rw-total\');if(el)el.textContent=fmt(total,0);var ea=$(\'rw-avg\');if(ea)ea.textContent=fmt(avg,0);var em=$(\'rw-max\');if(em)em.textContent=fmt(maxU.rew,0)+\' (\'+maxU.n+\')\';// Remplir les selects\n[\'rw-target\',\'rw-target2\',\'rw-target3\',\'cmp-a\',\'cmp-b\'].forEach(function(id){var sel=$(id);if(!sel)return;var cur=sel.value;sel.innerHTML=\'<option value="">Sélectionner...</option>\'+_users.map(u=>\'<option value="\'+esc(u.n)+\'">\'+esc(u.n)+(u.role===\'admin\'?\' 👑\':\'\')+\'</option>\').join(\'\');if(cur)sel.value=cur;});// Classement\nvar lb=$(\'rw-leaderboard\');if(lb){var sorted=[..._users].sort((a,b)=>b.rew-a.rew).slice(0,10);var maxR=sorted[0]?sorted[0].rew:1;lb.innerHTML=sorted.map((u,i)=>\'<div style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid rgba(0,229,255,.05)">\'\n+\'<span style="color:var(--muted);font-size:11px;width:20px;text-align:right">\'+(i+1)+\'</span>\'\n+\'<span style="flex:1;color:var(--cyan);font-weight:700;font-size:12px">\'+esc(u.n)+\'</span>\'\n+\'<div style="flex:2"><div class="pbar"><div class="pbar-fill" style="width:\'+Math.round(u.rew/maxR*100)+\'%;background:linear-gradient(90deg,var(--gold),var(--purple))"></div></div></div>\'\n+\'<span style="color:var(--gold);font-weight:700;font-size:12px;width:70px;text-align:right">\'+fmt(u.rew,0)+\'</span></div>\'\n).join(\'\');}}async function giveRewards(){var target=$(\'rw-target\').value,amt=parseFloat($(\'rw-amount\').value);if(!target||!amt||amt<=0){setMsg(\'rw-msg\',\'Sélectionner un utilisateur et un montant\',false);return;}var r=await fetch(\'/nxc/bank/gesture\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,target:target,amount:amt,fail_ts:null})});var res=await r.json();setMsg(\'rw-msg\',res.ok?\'✅ +\'+fmt(amt,0)+\' R donnés à \'+target:\'❌ \'+(res.error||\'Erreur\'),res.ok);if(res.ok){addLog(\'🏆\',\'Rewards donnés:+\'+fmt(amt,0)+\' R → \'+target);loadUsers();}}async function removeRewards(){var target=$(\'rw-target2\').value,amt=parseFloat($(\'rw-remove\').value);if(!target||!amt){setMsg(\'rw-msg2\',\'Champs requis\',false);return;}if(!confirm(\'Retirer \'+fmt(amt,0)+\' R à \'+target+\' ?\'))return;var u=_users.find(x=>x.n===target);if(!u){setMsg(\'rw-msg2\',\'Utilisateur introuvable\',false);return;}var newR=Math.max(0,u.rew-amt);var r=await api(\'/admin/set-rewards\',{target:target,rewards:newR});setMsg(\'rw-msg2\',r&&r.ok?\'✅ Retiré \'+fmt(amt,0)+\' R à \'+target:\'❌ Route non disponible — utilisez le geste commercial\',false);addLog(\'💸\',\'Tentative retrait rewards:-\'+fmt(amt,0)+\' R de \'+target);}async function resetRewards(){var target=$(\'rw-target3\').value;if(!target){setMsg(\'rw-msg3\',\'Sélectionner un utilisateur\',false);return;}if(!confirm(\'Remettre à zéro les rewards de \'+target+\' ?\'))return;addLog(\'🗑️\',\'Reset rewards:\'+target);setMsg(\'rw-msg3\',\'⚠️ Action loguée — route directe non disponible\',false);}// ══ PRÉVISIONS ══\nvar tresoObj=null;function loadPrevisions(){var h=mkt.history||[];var p=parseFloat(mkt.price||0);if(!p)return;// Trésorerie\nvar cv=$(\'ch-treso\');if(cv&&window.Chart){var b={};try{fetch(\'/nxc/bank\').then(r=>r.json()).then(function(d){b=d.bank||{};drawTreso(p,b,h);});}catch(e){drawTreso(p,b,h);}}// Indicateurs techniques\nif(h.length>=14){var prices=h.slice(-20).map(x=>parseFloat(x.price));var sma5=prices.slice(-5).reduce((s,v)=>s+v,0)/5;var sma10=prices.slice(-10).reduce((s,v)=>s+v,0)/10;var sma20=prices.reduce((s,v)=>s+v,0)/prices.length;var el=$(\'tech-indicators\');if(el){var items=[\n[\'SMA 5 ticks\',fmt(sma5,2)+\' R\',p>sma5?\'green\':\'red\'],[\'SMA 10 ticks\',fmt(sma10,2)+\' R\',p>sma10?\'green\':\'red\'],[\'SMA 20 ticks\',fmt(sma20,2)+\' R\',p>sma20?\'green\':\'red\'],[\'Signal\',sma5>sma10?\'📈 ACHAT (SMA5>SMA10)\':\'📉 VENTE (SMA5<SMA10)\',sma5>sma10?\'green\':\'red\'],];el.innerHTML=items.map(([k,v,c])=>\'<div style="display:flex;justify-content:space-between;padding:8px 10px;border-bottom:1px solid rgba(0,229,255,.05);font-size:12px"><span style="color:var(--muted)">\'+k+\'</span><b style="color:var(--\'+c+\')">\'+v+\'</b></div>\').join(\'\');}}// Support / Résistance\nif(h.length>=10){var ps=h.slice(-20).map(x=>parseFloat(x.price));var support=Math.min.apply(null,ps);var resist=Math.max.apply(null,ps);var mid=(support+resist)/2;var el2=$(\'support-resistance\');if(el2){el2.innerHTML=[\n[\'🔴 Résistance (haut)\',fmt(resist,2)+\' R\',\'red\'],[\'🟡 Zone médiane\',fmt(mid,2)+\' R\',\'gold\'],[\'🟢 Support (bas)\',fmt(support,2)+\' R\',\'green\'],[\'📏 Amplitude\',fmt(resist-support,2)+\' R (\'+((resist-support)/support*100).toFixed(1)+\'%)\',\'cyan\'],].map(([k,v,c])=>\'<div style="display:flex;justify-content:space-between;padding:10px 12px;border-bottom:1px solid rgba(0,229,255,.05);font-size:12px"><span style="color:var(--muted)">\'+k+\'</span><b style="color:var(--\'+c+\')">\'+v+\'</b></div>\').join(\'\');}}}function drawTreso(p,b,h){var cv=$(\'ch-treso\');if(!cv||!window.Chart)return;if(tresoObj){tresoObj.destroy();tresoObj=null;}var flux=b.flux||[];var recent=flux.filter(f=>f.ts>Date.now()-604800000);var wIn=recent.filter(f=>f.type===\'IN\').reduce((s,f)=>s+(f.amount||0),0);var wOut=recent.filter(f=>f.type===\'OUT\').reduce((s,f)=>s+(f.amount||0),0);var dIn=wIn/7,dOut=wOut/7;var base=b.reserves||0;var labs=[],opt=[],real=[],pes=[];for(var d=0;d<=30;d++){labs.push(d===0?\'Auj\':\'J+\'+d);opt.push(Math.max(0,Math.round(base+d*(dIn*1.3-dOut*0.7))));real.push(Math.max(0,Math.round(base+d*(dIn-dOut))));pes.push(Math.max(0,Math.round(base+d*(dIn*0.7-dOut*1.3))));}var ctx=cv.getContext(\'2d\');tresoObj=new Chart(ctx,{type:\'line\',data:{labels:labs,datasets:[{label:\'Optimiste\',data:opt,borderColor:\'var(--green)\',borderWidth:1.5,pointRadius:0,fill:false,tension:0.4},{label:\'Réaliste\',data:real,borderColor:\'var(--cyan)\',borderWidth:2.5,pointRadius:0,fill:false,tension:0.4},{label:\'Pessimiste\',data:pes,borderColor:\'var(--red)\',borderWidth:1.5,pointRadius:0,fill:false,tension:0.4},]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:\'var(--muted)\',font:{size:10}}}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:6,font:{size:8}},grid:{color:\'rgba(0,229,255,.04)\'}},y:{min:0,ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});}var _randPrice=null;function simulateFuture(){var days=parseInt($(\'sim-days\').value)||7;var sc=$(\'sim-scenario\').value;var p=parseFloat(mkt.price||5213);var daily={bull:0.02,bear:-0.02,flat:0.001,volatile:0}[sc]||0;var simP=p;for(var d=0;d<days;d++){if(sc===\'volatile\')simP*=(1+(Math.random()-0.5)*0.1);else simP*=(1+daily);simP=Math.max(50,Math.min(100000,simP));}simP=Math.round(simP*100)/100;var chg=((simP-p)/p*100);$(\'sim-result\').innerHTML=\'Dans <b>\'+days+\'j</b> (scénario \'+sc+\'):<br><span style="font-size:22px;color:\'+(chg>=0?\'var(--green)\':\'var(--red)\')+\'">\'+fmt(simP,2)+\' R</span><span style="color:var(--muted)">(\'+( chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%)</span>\';}// ══ COMPARAISON ══\nasync function compareUsers(){var a=$(\'cmp-a\').value,b=$(\'cmp-b\').value;if(!a||!b||a===b){$(\'cmp-result\').innerHTML=\'<div class="card ae"><div class="ct">Sélectionner 2 utilisateurs différents</div></div>\';return;}var ua=_users.find(u=>u.n===a)||{},ub=_users.find(u=>u.n===b)||{};var rows=[[\'🏆 Rewards\',ua.rew,ub.rew,\'gold\'],[\'◈ NXC\',ua.nxc,ub.nxc,\'cyan\'],[\'💰 Valeur (R)\',ua.val,ub.val,\'purple\']];$(\'cmp-result\').innerHTML=\'<div class="card cyan"><div class="ct">◈ \'+esc(a)+\' vs \'+esc(b)+\'</div>\'\n+\'<div style="display:grid;grid-template-columns:1fr auto 1fr;gap:8px;text-align:center;margin-bottom:10px">\'\n+\'<div style="font-size:16px;font-weight:900;color:var(--cyan)">\'+esc(a)+\'</div><div style="color:var(--muted);font-size:20px">⚔️</div><div style="font-size:16px;font-weight:900;color:var(--purple)">\'+esc(b)+\'</div></div>\'\n+rows.map(([label,va,vb,c])=>{var winner=va>vb?\'a\':vb>va?\'b\':\'tie\';return \'<div style="display:grid;grid-template-columns:1fr auto 1fr;gap:8px;padding:10px 0;border-bottom:1px solid rgba(0,229,255,.05);align-items:center;text-align:center">\'\n+\'<b style="color:\'+(winner===\'a\'?\'var(--green)\':\'var(--muted)\')+\'">\'+fmt(va,va<10?4:0)+(winner===\'a\'?\' 🏆\':\'\')+\' </b>\'\n+\'<span style="color:var(--muted);font-size:10px">\'+label+\'</span>\'\n+\'<b style="color:\'+(winner===\'b\'?\'var(--green)\':\'var(--muted)\')+\'"> \'+(winner===\'b\'?\'🏆 \':\'\')+fmt(vb,vb<10?4:0)+\'</b></div>\';}).join(\'\')+\'</div>\';// Top 3\nvar t3=$(\'top3\');if(t3&&_users.length){var cats=[[\'🏆 Rewards\',\'rew\',\'gold\'],[\'◈ NXC\',\'nxc\',\'cyan\'],[\'💰 Valeur\',\'val\',\'purple\']];t3.innerHTML=cats.map(([lbl,key,c])=>{var sorted=[..._users].sort((a,b)=>b[key]-a[key]).slice(0,3);return \'<div style="margin-bottom:10px"><div class="sec">\'+lbl+\'</div>\'\n+sorted.map((u,i)=>\'<div style="display:flex;justify-content:space-between;padding:6px 10px;font-size:11px;border-bottom:1px solid rgba(0,229,255,.05)">\'\n+\'<span>\'+ [\'🥇\',\'🥈\',\'🥉\'][i]+\' <b style="color:var(--\'+c+\')">\'+esc(u.n)+\'</b></span>\'\n+\'<span style="color:var(--\'+c+\')">\'+fmt(u[key],key===\'nxc\'?4:0)+\'</span></div>\'\n).join(\'\')+\'</div>\';}).join(\'\');}}// ══ SÉCURITÉ ══\nasync function loadSecurity(){try{var r=await fetch(\'/nxc/bank/fail\');var d=await r.json();var fails=d.fails||[];var okSells=parseInt(mkt.trades24||0);var failN=fails.length;var el=$(\'sec-ok\');if(el)el.textContent=okSells;var ef=$(\'sec-fail\');if(ef)ef.textContent=failN;var eg=$(\'sec-gests\');if(eg)eg.textContent=fails.filter(f=>f.gesture>0).length;var er=$(\'sec-ratio\');if(er)er.textContent=(okSells+failN>0?Math.round(failN/(okSells+failN)*100):0)+\'%\';// Activité suspecte\nvar sus=$(\'sec-suspicious\');if(sus){var byUser={};fails.forEach(f=>{byUser[f.user]=(byUser[f.user]||0)+1;});var suspects=Object.entries(byUser).filter(([u,n])=>n>=2).sort((a,b)=>b[1]-a[1]);sus.innerHTML=suspects.length?suspects.map(([u,n])=>\'<div class="ab aw">⚠️ <b>\'+esc(u)+\'</b> — \'+n+\' tentatives de vente échouées</div>\').join(\'\'):\'<div class="ab ao">✅ Aucune activité suspecte détectée</div>\';}}catch(e){}}function changeKey(){var nk=$(\'new-key\').value.trim();if(!nk||nk.length<8){$(\'key-msg\').textContent=\'Clé trop courte (min 8 chars)\';return;}KEY=nk;$(\'new-key\').value=\'\';$(\'key-msg\').textContent=\'✅ Clé changée pour cette session\';$(\'key-msg\').style.color=\'var(--green)\';addLog(\'🔑\',\'Clé maître changée (session)\');}// ══ OUTILS ══\nfunction calcNxc(){var nxc=parseFloat($(\'calc-nxc\').value)||0;var p=parseFloat(mkt.price||0);$(\'calc-rew\').value=nxc&&p?Math.round(nxc*p*100)/100:\'\';}function calcRew(){var rew=parseFloat($(\'calc-rew2\').value)||0;var p=parseFloat(mkt.price||1);$(\'calc-nxc2\').value=rew&&p?(rew/p).toFixed(6):\'\';}function simSell(){var nxc=parseFloat($(\'sell-sim-nxc\').value)||0;var fee=parseFloat($(\'sell-sim-fee\').value)||0;var p=parseFloat(mkt.price||0);if(!nxc||!p){$(\'sell-sim-result\').innerHTML=\'\';return;}var gross=nxc*p;var feesR=gross*fee/100;var net=gross-feesR;$(\'sell-sim-result\').innerHTML=\'<div style="display:flex;flex-direction:column;gap:4px">\'\n+\'<div style="color:var(--muted)">Brut:<b style="color:var(--text)">\'+fmt(gross,2)+\' R</b></div>\'\n+\'<div style="color:var(--muted)">Frais (\'+fee+\'%):<b style="color:var(--red)">-\'+fmt(feesR,2)+\' R</b></div>\'\n+\'<div style="color:var(--muted)">Net reçu:<b style="color:var(--green);font-size:18px">\'+fmt(net,2)+\' R</b></div>\'\n+\'<div style="color:var(--muted);font-size:10px">Au prix actuel de \'+fmt(p,2)+\' R/NXC</div></div>\';}function genRandPrice(){var mn=parseFloat($(\'rnd-min\').value)||50;var mx=parseFloat($(\'rnd-max\').value)||10000;_randPrice=Math.round((mn+Math.random()*(mx-mn))*100)/100;$(\'rnd-result\').textContent=fmt(_randPrice,2)+\' R/NXC\';}async function applyRandPrice(){if(!_randPrice){genRandPrice();}if(!confirm(\'Appliquer le prix de \'+fmt(_randPrice,2)+\' R ?\'))return;var r=await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:_randPrice,ts:Date.now(),vol:0,volume24:mkt.volume24||0,trades24:mkt.trades24||0})});var res=await r.json();if(res.ok){addLog(\'🎲\',\'Prix aléatoire appliqué:\'+fmt(_randPrice,2)+\' R\');setTimeout(ref,500);}}var _timerInt=null,_timerEnd=null;function startTimer(){var m=parseInt($(\'timer-min\').value)||0;var s=parseInt($(\'timer-sec\').value)||0;var total=m*60+s;var action=$(\'timer-action\').value;if(!total||total<=0)return;if(_timerInt)clearInterval(_timerInt);_timerEnd=Date.now()+total*1000;addLog(\'⏱️\',\'Minuteur:action "\'+action+\'" dans \'+total+\'s\');_timerInt=setInterval(async function(){var rem=Math.max(0,Math.round((_timerEnd-Date.now())/1000));var mm=Math.floor(rem/60),ss=rem%60;var el=$(\'timer-display\');if(el)el.textContent=(\'0\'+mm).slice(-2)+\':\'+(\'0\'+ss).slice(-2);if(rem<=0){clearInterval(_timerInt);_timerInt=null;var disp=$(\'timer-display\');if(disp){disp.textContent=\'✅ Action!\';disp.style.color=\'var(--green)\';}if(action===\'stop\')setT(\'stop\');else if(action===\'up\'||action===\'down\')setT(action);else if(action===\'crash\'||action===\'moon\')scenario(action===\'crash\'?\'crash\':\'moon\');addLog(\'⏱️\',\'Minuteur déclenché:action "\'+action+\'"\');}},500);}function stopTimer(){if(_timerInt){clearInterval(_timerInt);_timerInt=null;var d=$(\'timer-display\');if(d){d.textContent=\'\';d.style.color=\'var(--cyan)\';}}addLog(\'⏱️\',\'Minuteur annulé\');}async function pingServer(){var el=$(\'ping-result\');el.textContent=\'📡 Test en cours...\';el.style.color=\'var(--muted)\';var start=Date.now();try{await fetch(\'/nxc/price\');var lat=Date.now()-start;el.textContent=\'✅ Serveur en ligne — latence:\'+lat+\' ms\';el.style.color=lat<500?\'var(--green)\':lat<1000?\'var(--gold)\':\'var(--red)\';}catch(e){el.textContent=\'❌ Serveur injoignable\';el.style.color=\'var(--red)\';}}// Override go() pour charger les données des nouveaux onglets\nvar _origGo=go;go=function(tab,btn){_origGo(tab,btn);if(tab===\'rewards\')loadRewardsAdmin();if(tab===\'market2\')loadPrevisions();if(tab===\'compare\'){loadRewardsAdmin();}if(tab===\'security\')loadSecurity();};</script><script>\nvar KEY=\'\',mkt={},tInt=null,tMode=null,tStr=0.005,tInterval=12000,chObj=null,rsiObj=null,volObj=null,solvOn=false,_users=[],_sortBy=\'rew\',_flux=[],_fluxFilter=\'all\',_log=[],_prevPrice=0,_chartType=\'line\',_chartRange=50;function $(i){return document.getElementById(i);}function fmt(n,d){return Number(n||0).toLocaleString(\'fr-FR\',{minimumFractionDigits:d||0,maximumFractionDigits:d==null?2:d});}function esc(s){return (s+\'\').replace(/[&<>"]/g,c=>({\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\'}[c]));}function fmtTime(ts){return new Date(ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\',second:\'2-digit\'});}function setMsg(id,t,ok){var e=$(id);if(!e)return;e.textContent=t;e.className=ok?\'msg-ok\':\'msg-err\';}function addLog(ico,txt){_log.unshift({ico,txt,ts:Date.now()});if(_log.length>100)_log.pop();renderLog();}function renderLog(){var el=$(\'log-list\');if(!el)return;if(!_log.length){el.innerHTML=\'<p style="color:var(--muted);padding:20px;text-align:center;font-size:12px">Aucune action</p>\';return;}el.innerHTML=_log.map(l=>\'<div class="log-item"><span class="log-time">\'+fmtTime(l.ts)+\'</span><span class="log-ico">\'+l.ico+\'</span><span class="log-txt">\'+esc(l.txt)+\'</span></div>\').join(\'\');}function clearLog(){_log=[];renderLog();}async function api(p,b){b=b||{};b.master_key=KEY;try{var r=await fetch(p,{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify(b)});return await r.json();}catch(e){return{ok:false};}}async function conn(){KEY=$(\'mk\').value.trim();$(\'lm\').textContent=\'Connexion…\';var r=await api(\'/admin/list\');if(r&&r.ok){$(\'ls\').style.display=\'none\';$(\'hd\').classList.add(\'on\');$(\'htm\').style.display=\'block\';var s=document.createElement(\'script\');s.src=\'https://cdn.jsdelivr.net/npm/hammerjs@2.0.8/hammer.min.js\';s.onload=function(){var s2=document.createElement(\'script\');s2.src=\'https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@2.0.1/dist/chartjs-plugin-zoom.min.js\';document.head.appendChild(s2);};document.head.appendChild(s);ref();loadBank();loadSolv();loadFails();setInterval(ref,15000);setInterval(function(){loadBank();loadFails();},25000);setInterval(function(){$(\'htm\').textContent=new Date().toLocaleTimeString(\'fr-FR\');},1000);addLog(\'🔑\',\'Connexion admin réussie\');}else{$(\'lm\').textContent=\'❌ Clé incorrecte\';}}function go(tab,btn){document.querySelectorAll(\'.view\').forEach(v=>v.classList.remove(\'on\'));document.querySelectorAll(\'.tab\').forEach(t=>t.classList.remove(\'on\'));var v=$(\'view-\'+tab);if(v)v.classList.add(\'on\');if(btn)btn.classList.add(\'on\');if(tab===\'users\')loadUsers();if(tab===\'stats\')loadStats();if(tab===\'banque\'){$(\'nd-b\').style.display=\'none\';}}// ══ MARCHÉ ══\nasync function ref(){try{var r=await fetch(\'/nxc/price\');var d=await r.json();mkt=d;var p=parseFloat(d.price||0),h=d.history||[];var chg=_prevPrice>0?((p-_prevPrice)/_prevPrice*100):0;var hi=h.length>1?Math.max.apply(null,h.slice(-24).map(x=>x.price)):p;var lo=h.length>1?Math.min.apply(null,h.slice(-24).map(x=>x.price)):p;var cap=p*(parseFloat((mkt.nxcEmis||0))||3);$(\'s-p\').textContent=fmt(p,2);$(\'s-v\').textContent=fmt(d.volume24||0,0);$(\'s-t\').textContent=d.trades24||0;$(\'s-h\').textContent=h.length;$(\'s-high\').textContent=fmt(hi,0);$(\'s-low\').textContent=fmt(lo,0);$(\'s-var\').textContent=(chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%\';$(\'s-var\').style.color=chg>=0?\'var(--green)\':\'var(--red)\';$(\'s-cap\').textContent=fmt(cap,0);$(\'hp\').textContent=fmt(p,2)+\' R\';var hc=$(\'hc\');if(_prevPrice>0){hc.textContent=(chg>=0?\'▲+\':\'▼\')+chg.toFixed(2)+\'%\';hc.className=\'badge \'+(chg>=0?\'up\':\'dn\');hc.style.display=\'block\';}_prevPrice=p;drawC(h);drawA(p,h);drawRSI(h);}catch(e){}}function setRange(n){_chartRange=n;[\'rb-25\',\'rb-50\',\'rb-100\'].forEach(id=>{var e=$(id);if(e)e.className=\'btn\';});var rb=$(\'rb-\'+n);if(rb)rb.className=\'btn cyan\';if(chObj){chObj.destroy();chObj=null;}ref();}function drawC(h){var cv=$(\'ch\');if(!cv||!window.Chart)return;var pts=h.slice(-_chartRange);var labs=pts.map(x=>new Date(x.ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\'}));var prices=pts.map(x=>parseFloat(x.price));if(prices.length<2)return;var mn=Math.min.apply(null,prices)*0.85,mx=Math.max.apply(null,prices)*1.15;if(chObj){chObj.data.labels=labs;chObj.data.datasets[0].data=prices;chObj.options.scales.y.min=mn;chObj.options.scales.y.max=mx;chObj.update(\'none\');return;}var ctx=cv.getContext(\'2d\');var g=ctx.createLinearGradient(0,0,0,cv.offsetHeight||200);g.addColorStop(0,\'rgba(0,229,255,.2)\');g.addColorStop(1,\'rgba(0,229,255,0)\');chObj=new Chart(ctx,{type:_chartType===\'bar\'?\'bar\':\'line\',data:{labels:labs,datasets:[{data:prices,borderColor:\'#00e5ff\',backgroundColor:_chartType===\'bar\'?\'rgba(0,229,255,.4)\':g,borderWidth:2.5,pointRadius:0,fill:_chartType!==\'bar\',tension:0.4}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},zoom:{zoom:{wheel:{enabled:true},pinch:{enabled:true},mode:\'x\'},pan:{enabled:true,mode:\'x\'}}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:5,font:{size:8}},grid:{color:\'rgba(0,229,255,.04)\'}},y:{min:mn,max:mx,ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});}function toggleChartType(){_chartType=_chartType===\'line\'?\'bar\':\'line\';if(chObj){chObj.destroy();chObj=null;}ref();}function downloadChart(){var cv=$(\'ch\');if(!cv)return;var a=document.createElement(\'a\');a.download=\'nexus_chart_\'+Date.now()+\'.png\';a.href=cv.toDataURL();a.click();addLog(\'⬇️\',\'Export graphique PNG\');}function drawRSI(h){var cv=$(\'ch-rsi\');if(!cv||!window.Chart||h.length<14)return;var prices=h.slice(-28).map(x=>parseFloat(x.price));var rsi=[];for(var i=14;i<prices.length;i++){var gains=0,losses=0;for(var j=i-14;j<i;j++){var d=prices[j+1]-prices[j];if(d>0)gains+=d;else losses-=d;}var rs=losses===0?100:gains/losses;rsi.push(Math.round(100-100/(1+rs)));}var labs=h.slice(-(rsi.length)).map(x=>new Date(x.ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\'}));if(rsiObj){rsiObj.data.labels=labs;rsiObj.data.datasets[0].data=rsi;rsiObj.update(\'none\');return;}var ctx=cv.getContext(\'2d\');rsiObj=new Chart(ctx,{type:\'line\',data:{labels:labs,datasets:[{data:rsi,borderColor:\'#a06bff\',borderWidth:2,pointRadius:0,fill:false,tension:0.4}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},annotation:{}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:4,font:{size:8}},grid:{display:false}},y:{min:0,max:100,ticks:{color:\'#5c6b8c\',stepSize:25},grid:{color:\'rgba(0,229,255,.04)\'},afterDraw:function(chart){var ctx=chart.ctx;[[70,\'rgba(255,61,94,.3)\'],[30,\'rgba(0,255,157,.3)\']].forEach(([v,c])=>{var y=chart.scales.y.getPixelForValue(v);ctx.save();ctx.strokeStyle=c;ctx.lineWidth=1;ctx.setLineDash([4,4]);ctx.beginPath();ctx.moveTo(chart.chartArea.left,y);ctx.lineTo(chart.chartArea.right,y);ctx.stroke();ctx.restore();});}}},animation:{duration:0}}});}function drawA(p,h){var el=$(\'al\'),a=[];if(p>80000)a.push({c:\'ae\',m:\'⚡ Prix très élevé (>80 000 R)\'});else if(p>50000)a.push({c:\'aw\',m:\'📊 Prix élevé (>50 000 R) — surveillance recommandée\'});else if(p<500)a.push({c:\'ae\',m:\'🔴 Prix critique (<500 R) — zone de danger\'});else if(p<2000)a.push({c:\'aw\',m:\'⚠️ Prix bas (<2 000 R) — surveiller\'});else a.push({c:\'ao\',m:\'✅ Prix dans la zone normale (\'+fmt(p,0)+\' R)\'});if(h.length>10){var rv=h.slice(-10).map(x=>x.price);var vol=(Math.max.apply(null,rv)-Math.min.apply(null,rv))/Math.min.apply(null,rv)*100;a.push(vol>30?{c:\'ae\',m:\'🌋 Volatilité extrême:\'+vol.toFixed(1)+\'%\'}:vol>15?{c:\'aw\',m:\'⚡ Forte volatilité:\'+vol.toFixed(1)+\'%\'}:{c:\'ao\',m:\'📊 Marché stable — volatilité:\'+vol.toFixed(1)+\'%\'});}if(h.length>5){var trend=h.slice(-5).map(x=>x.price);var up=trend.every((v,i)=>i===0||v>=trend[i-1]);var dn=trend.every((v,i)=>i===0||v<=trend[i-1]);if(up)a.push({c:\'ao\',m:\'📈 Tendance haussière détectée (5 derniers ticks)\'});else if(dn)a.push({c:\'ae\',m:\'📉 Tendance baissière détectée (5 derniers ticks)\'});}a.push(tMode?{c:\'aw\',m:\'⚙️ Tendance \'+tMode+\' active · \'+(tStr*100).toFixed(1)+\'%/tick · intervalle \'+(tInterval/1000)+\'s\'}:{c:\'ai\',m:\'⏸ Aucune tendance — cours libre\'});el.innerHTML=a.map(x=>\'<div class="ab \'+x.c+\'"><span>\'+x.m+\'</span></div>\').join(\'\');}// ══ CONTRÔLE ══\nasync function adjPrice(pct){var p=parseFloat(mkt.price||5213);p=Math.max(50,Math.min(100000,p*(1+pct)));p=Math.round(p*100)/100;var r=await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:0,volume24:mkt.volume24||0,trades24:mkt.trades24||0})});var res=await r.json();setMsg(\'pm\',res.ok?\'✅ \'+(pct>0?\'+\':\'\')+((pct*100).toFixed(1))+\'% → \'+fmt(p,2)+\' R\':\'❌ Erreur\',res.ok);addLog(\'📊\',\'Cours ajusté \'+(pct>0?\'+\':\'\')+((pct*100).toFixed(1))+\'% → \'+fmt(p,2)+\' R\');if(res.ok)setTimeout(ref,500);}async function setP(){var p=parseFloat($(\'np\').value);if(!p||p<50||p>100000){setMsg(\'pm\',\'Prix invalide (50–100 000)\',false);return;}var r=await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:0,volume24:mkt.volume24||0,trades24:mkt.trades24||0})});var res=await r.json();setMsg(\'pm\',res.ok?\'✅ Cours → \'+fmt(p,2)+\' R\':\'❌ Erreur\',res.ok);if(res.ok){$(\'np\').value=\'\';addLog(\'💱\',\'Cours fixé à \'+fmt(p,2)+\' R\');setTimeout(ref,500);}}async function setPct(){var pct=parseFloat($(\'np-pct\').value)/100;if(isNaN(pct)){setMsg(\'pm\',\'Pourcentage invalide\',false);return;}await adjPrice(pct);$(\'np-pct\').value=\'\';}async function resetH(){if(!confirm(\'Remettre l\\\'historique à zéro ?\'))return;await fetch(\'/nxc/reset\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY})});addLog(\'🔄\',\'Reset historique NXC\');ref();}async function confirmReset(){if(!confirm(\'Reset COMPLET ? Prix + historique remis à zéro !\'))return;await fetch(\'/nxc/reset\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY})});await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:5213,ts:Date.now(),vol:0,volume24:0,trades24:0})});addLog(\'⚠️\',\'Reset complet marché NXC\');ref();}var _tStart=null,_tTimerInt=null;function setT(m){var s=parseFloat($(\'ts\').value)||0.005;var iv=parseInt($(\'ti\').value)||12000;if(tInt){clearInterval(tInt);tInt=null;}if(_tTimerInt){clearInterval(_tTimerInt);_tTimerInt=null;}tMode=m===\'stop\'?null:m;tStr=s;tInterval=iv;_tStart=tMode?Date.now():null;var el=$(\'tst\'),ht=$(\'ht\');if(!tMode){el.textContent=\'⏸ Arrêté\';el.style.color=\'var(--muted)\';ht.style.display=\'none\';$(\'tt-timer\').textContent=\'\';addLog(\'⏸\',\'Tendance arrêtée\');return;}var lbl=m===\'up\'?\'📈 Hausse +\':m===\'down\'?\'📉 Baisse -\':\'🎲 Aléatoire \';var spd=m!==\'random\'?(s*100).toFixed(1)+\'%\':\'\';el.textContent=lbl+spd+\' · \'+(iv/1000)+\'s/tick\';el.style.color=m===\'up\'?\'var(--green)\':m===\'down\'?\'var(--red)\':\'var(--purple)\';ht.textContent=lbl+spd;ht.style.display=\'block\';addLog(m===\'up\'?\'📈\':m===\'down\'?\'📉\':\'🎲\',\'Tendance \'+m+\' activée · \'+(s*100).toFixed(1)+\'%/tick\');_tTimerInt=setInterval(function(){if(_tStart){var elapsed=Math.floor((Date.now()-_tStart)/1000);$(\'tt-timer\').textContent=\'⏱ \'+Math.floor(elapsed/60)+\'m\'+(\'0\'+(elapsed%60)).slice(-2)+\'s\';}},1000);tInt=setInterval(async function(){var p=parseFloat(mkt.price||5213);var adj=(Math.random()-0.48)*0.008;if(m===\'up\')adj+=s;else if(m===\'down\')adj-=s;p=Math.max(50,Math.min(100000,p*(1+adj)));p=Math.random()>0.03?Math.round(p*100)/100:Math.round(p);await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:Math.floor(Math.random()*300+50),volume24:(mkt.volume24||0)+100,trades24:(mkt.trades24||0)+1})});},iv);}async function scenario(sc){var p=parseFloat(mkt.price||5213);var target;if(sc===\'crash\')target=p*0.7;else if(sc===\'moon\')target=p*1.3;else if(sc===\'ath\')target=Math.min(100000,Math.max(p*1.5,90000));else if(sc===\'floor\')target=200;if(target){target=Math.max(50,Math.min(100000,Math.round(target*100)/100));await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:target,ts:Date.now(),vol:9999,volume24:(mkt.volume24||0)+5000,trades24:(mkt.trades24||0)+50})});addLog(\'🎭\',\'Scénario \'+sc+\' → \'+fmt(target,2)+\' R\');setTimeout(ref,500);}else if(sc===\'volatile\'){setT(\'random\');addLog(\'⚡\',\'Scénario volatil activé\');}else if(sc===\'stable\'){setT(\'stop\');addLog(\'😴\',\'Scénario stabilisation\');}}// ══ BANQUE ══\nfunction setAmt(v){$(\'bk-amt\').value=v;}function filterFlux(f){_fluxFilter=f;[\'fl-all\',\'fl-in\',\'fl-out\'].forEach(id=>{var e=$(id);if(e)e.className=\'btn\';});var e=$(\'fl-\'+f);if(e)e.className=\'btn cyan\';renderFlux();}function renderFlux(){var flux=(_fluxFilter===\'all\'?_flux:_flux.filter(f=>f.type===_fluxFilter)).slice(0,30);var el=$(\'bk-flux\');if(!el)return;el.innerHTML=flux.length?flux.map(f=>\'<div class="fl-item"><div class="fl-dot \'+(f.type===\'IN\'?\'in\':\'out\')+\'"></div><span class="fl-amt \'+(f.type===\'IN\'?\'in\':\'out\')+\'">\'+(f.type===\'IN\'?\'+\':\'-\')+fmt(f.amount||0,0)+\' R</span><span class="fl-user">\'+esc(f.user||\'?\')+\'</span><span class="fl-time">\'+new Date(f.ts).toLocaleTimeString(\'fr-FR\')+\'</span></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun flux</p>\';}function exportFlux(){var csv=\'Date,Type,Utilisateur,Montant (R),NXC\\n\';_flux.forEach(f=>csv+=new Date(f.ts).toLocaleString(\'fr-FR\')+\',\'+f.type+\',\'+(f.user||\'\')+\',\'+(f.amount||0)+\',\'+(f.nxc||0)+\'\\n\');var blob=new Blob([csv],{type:\'text/csv\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'banque_nexus_\'+Date.now()+\'.csv\';a.click();addLog(\'📊\',\'Export CSV flux bancaires\');}async function loadBank(){try{var r=await fetch(\'/nxc/bank\');var d=await r.json();if(!d.ok)return;var b=d.bank||{};_flux=(b.flux||[]).slice().reverse();var p=parseFloat(mkt.price||0);$(\'bk-r\').textContent=fmt(b.reserves||0,0)+\' R\';$(\'bk-i\').textContent=fmt(b.totalIn||0,0);$(\'bk-o\').textContent=fmt(b.totalOut||0,0);var ratio=b.totalIn>0?((b.reserves||0)/b.totalIn*100):100;$(\'bk-rt\').textContent=ratio.toFixed(1)+\'%\';$(\'bk-nx\').textContent=parseFloat(b.nxcEmis||0).toFixed(4)+\' NXC\';$(\'bk-vx\').textContent=fmt((b.nxcEmis||0)*p,0)+\' R\';var benef=(b.totalIn||0)-(b.totalOut||0);var bel=$(\'bk-bn\');bel.textContent=(benef>=0?\'+\':\'\')+fmt(benef,0)+\' R\';bel.style.color=benef>=0?\'var(--green)\':\'var(--red)\';$(\'bk-fl\').textContent=_flux.length;renderFlux();}catch(e){}}async function bankInject(){var amt=parseFloat($(\'bk-amt\').value);if(!amt||amt<=0){setMsg(\'bk-msg\',\'Montant invalide\',false);return;}var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{reserves:0,totalIn:0,totalOut:0,nxcEmis:0,flux:[]};b.reserves=parseFloat(((b.reserves||0)+amt).toFixed(2));b.totalIn=parseFloat(((b.totalIn||0)+amt).toFixed(2));b.flux=b.flux||[];b.flux.push({type:\'IN\',user:\'SERVEUR\',amount:amt,nxc:0,ts:Date.now()});var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:b,reset:true})});var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ +\'+fmt(amt,0)+\' R injectés\':\'❌ Erreur\',res.ok);if(res.ok){$(\'bk-amt\').value=\'\';addLog(\'💰\',\'Injection banque +\'+fmt(amt,0)+\' R\');loadBank();}}async function bankRetire(){var amt=parseFloat($(\'bk-amt\').value);if(!amt||amt<=0){setMsg(\'bk-msg\',\'Montant invalide\',false);return;}var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{reserves:0,totalIn:0,totalOut:0,nxcEmis:0,flux:[]};if(amt>(b.reserves||0)){setMsg(\'bk-msg\',\'❌ Réserves insuffisantes\',false);return;}b.reserves=parseFloat(((b.reserves||0)-amt).toFixed(2));b.totalOut=parseFloat(((b.totalOut||0)+amt).toFixed(2));b.flux=b.flux||[];b.flux.push({type:\'OUT\',user:\'SERVEUR\',amount:amt,nxc:0,ts:Date.now()});var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:b,reset:true})});var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ -\'+fmt(amt,0)+\' R retirés\':\'❌ Erreur\',res.ok);if(res.ok){$(\'bk-amt\').value=\'\';addLog(\'💸\',\'Retrait banque -\'+fmt(amt,0)+\' R\');loadBank();}}async function bankResetHist(){var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{};var g=b.reserves||0;if(!confirm(\'Effacer historique ?\\nRéserves conservées:\'+fmt(g,0)+\' R\'))return;var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:{reserves:g,nxcEmis:0,totalIn:0,totalOut:0,flux:[]},reset:true})});var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ Historique effacé\':\'❌ Erreur\',res.ok);if(res.ok){addLog(\'🗑️\',\'Reset historique banque\');loadBank();}}async function bankResetAll(){var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{};var g=confirm(\'Garder les réserves (\'+fmt(b.reserves||0,0)+\' R) ?\');if(!confirm(\'Confirmer ?\'))return;var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:{reserves:g?(b.reserves||0):0,nxcEmis:0,totalIn:0,totalOut:0,flux:[]},reset:true})});var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ Réinitialisé\':\'❌ Erreur\',res.ok);if(res.ok){addLog(\'💥\',\'Reset complet banque\');loadBank();}}async function loadFails(){try{var r=await fetch(\'/nxc/bank/fail\');var d=await r.json();var el=$(\'bk-fails\'),fc=$(\'fails-count\');if(!el)return;var fails=(d.fails||[]).slice().reverse();if(fails.length){if(fc){fc.textContent=fails.length;fc.className=\'badge red\';fc.style.display=\'block\';}$(\'nd-b\').style.display=\'block\';}if(!fails.length){el.innerHTML=\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">✅ Aucune tentative échouée</p>\';return;}el.innerHTML=fails.map(f=>\'<div class="fail-item"><div style="display:flex;justify-content:space-between;align-items:center"><span style="color:var(--red);font-weight:700;font-size:13px">❌ \'+esc(f.user)+\'</span><span style="color:var(--muted);font-size:10px;font-family:monospace">\'+new Date(f.ts).toLocaleTimeString(\'fr-FR\')+\'</span></div><div style="color:var(--muted);font-size:11px">Voulait vendre <b style="color:var(--text)">\'+f.nxc+\' NXC</b> (\'+fmt(f.amount||0,0)+\' R)</div>\'+(f.gesture>0?\'<button onclick="sendGesture(\\\'\'+esc(f.user)+\'\\\',\'+f.gesture+\',\'+f.ts+\')" style="padding:8px 14px;background:rgba(0,255,157,.1);border:1px solid rgba(0,255,157,.3);border-radius:9px;color:var(--green);font-size:12px;cursor:pointer;font-weight:700">💝 Verser +\'+f.gesture+\' R</button>\':\'\')+\'</div>\').join(\'\');}catch(e){}}async function sendGesture(user,amount,failTs){if(!confirm(\'Verser \'+amount+\' R à \'+user+\' ?\'))return;var r=await fetch(\'/nxc/bank/gesture\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,target:user,amount:amount,fail_ts:failTs})});var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ \'+amount+\' R versés à \'+user:\'❌ \'+(res.error||\'Erreur\'),res.ok);if(res.ok){addLog(\'💝\',\'Geste commercial +\'+amount+\' R → \'+user);loadBank();loadFails();}}// ══ COMPTES ══\nvar _sortDir=1;async function loadUsers(){$(\'us-msg\').textContent=\'Chargement…\';try{var r=await api(\'/admin/list\');if(!r||!r.ok){$(\'us-msg\').textContent=\'Erreur\';return;}var p=parseFloat(mkt.price||0);var rows=await Promise.all((r.users||[]).map(async u=>{var d=await api(\'/admin/get\',{target:u.username});var rew=Math.max((d.data&&d.data.nx2098&&d.data.nx2098.rewards)||0,(d.data&&d.data.rewards&&d.data.rewards.points)||0);var nxc=parseFloat((d.data&&d.data.nxcoin&&d.data.nxcoin.nxc)||0);return{n:u.username,role:u.role,rew:rew,nxc:nxc,val:nxc*p};}));_users=rows;$(\'u-total\').textContent=rows.length;$(\'u-admins\').textContent=rows.filter(r=>r.role===\'admin\').length;$(\'u-rew\').textContent=fmt(rows.reduce((s,r)=>s+r.rew,0),0);sortUsers(_sortBy);$(\'us-msg\').textContent=\'\';}catch(e){$(\'us-msg\').textContent=\'Erreur\';}}function sortUsers(by){_sortBy=by;[\'sort-rew\',\'sort-nxc\',\'sort-name\'].forEach(id=>{var e=$(id);if(e)e.className=\'btn\';});var e=$(\'sort-\'+by);if(e)e.className=\'btn cyan\';var sorted=[..._users].sort((a,b)=>by===\'name\'?a.n.localeCompare(b.n):(b[by]-a[by]));renderU(sorted);}function renderU(rows){$(\'ut\').innerHTML=rows.map((r,i)=>\'<tr><td><b style="color:var(--cyan)">\'+esc(r.n)+(r.role===\'admin\'?\' 👑\':\'\')+\'</b></td><td style="color:var(--muted);font-size:10px">\'+esc(r.role)+\'</td><td style="color:var(--gold)">\'+fmt(r.rew,0)+\'</td><td style="color:var(--cyan);font-family:monospace">\'+r.nxc.toFixed(4)+\'</td><td style="color:var(--purple)">\'+fmt(r.val,0)+\'</td></tr>\').join(\'\');}function filterU(){var q=($(\'us-q\').value||\'\').toLowerCase();renderU(q?_users.filter(r=>r.n.toLowerCase().includes(q)):_users);}// ══ STATS ══\nasync function loadStats(){var p=parseFloat(mkt.price||0),h=mkt.history||[];// Volume chart\nif(h.length>5){var cv=$(\'ch-vol\');if(cv&&window.Chart){var pts=h.slice(-20);var labs=pts.map(x=>new Date(x.ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\'}));var vols=pts.map(x=>x.vol||0);if(volObj){volObj.data.labels=labs;volObj.data.datasets[0].data=vols;volObj.update(\'none\');}else{var ctx=cv.getContext(\'2d\');volObj=new Chart(ctx,{type:\'bar\',data:{labels:labs,datasets:[{data:vols,backgroundColor:\'rgba(160,107,255,.5)\',borderColor:\'#a06bff\',borderWidth:1}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:4,font:{size:8}},grid:{display:false}},y:{ticks:{color:\'#5c6b8c\'},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});}}}// Rewards bars\nif(_users.length){var maxR=Math.max.apply(null,_users.map(u=>u.rew))||1;$(\'rew-bars\').innerHTML=_users.sort((a,b)=>b.rew-a.rew).slice(0,8).map(u=>\'<div style="margin-bottom:8px"><div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:3px"><span style="color:var(--cyan);font-weight:700">\'+esc(u.n)+\'</span><span style="color:var(--gold)">\'+fmt(u.rew,0)+\' R</span></div><div class="pbar"><div class="pbar-fill" style="width:\'+Math.round(u.rew/maxR*100)+\'%"></div></div></div>\').join(\'\');}// Health\nvar hi=h.length>1?Math.max.apply(null,h.slice(-24).map(x=>x.price)):p;var lo=h.length>1?Math.min.apply(null,h.slice(-24).map(x=>x.price)):p;var volatility=lo>0?(hi-lo)/lo*100:0;$(\'health-grid\').innerHTML=[\n[\'📈 Tendance 5t\',h.length>5?(h.slice(-5).map(x=>x.price).every((v,i,a)=>i===0||v>a[i-1])?\'<span style="color:var(--green)">Haussière</span>\':h.slice(-5).map(x=>x.price).every((v,i,a)=>i===0||v<a[i-1])?\'<span style="color:var(--red)">Baissière</span>\':\'<span style="color:var(--muted)">Neutre</span>\'):\'—\'],[\'⚡ Volatilité 24h\',volatility.toFixed(2)+\'%\'],[\'📊 Amplitude\',fmt(hi-lo,0)+\' R\'],[\'🔢 Nb. trades\',mkt.trades24||0+\' tx\'],[\'💎 Vol. total\',fmt(mkt.volume24||0,0)+\' R\'],[\'🏦 NXC total\',parseFloat((mkt.nxcEmis||0)||0).toFixed(4)]\n].map(([k,v])=>\'<div class="st"><div class="sv" style="font-size:13px">\'+v+\'</div><div class="sl">\'+k+\'</div></div>\').join(\'\');// Metrics\n$(\'metrics\').innerHTML=[\n\'📊 <b>Prix actuel:</b> \'+fmt(p,2)+\' R\',\'📈 <b>Plus haut 24h:</b> \'+fmt(hi,2)+\' R\',\'📉 <b>Plus bas 24h:</b> \'+fmt(lo,2)+\' R\',\'💰 <b>Volume 24h:</b> \'+fmt(mkt.volume24||0,0)+\' R\',\'🔢 <b>Trades 24h:</b> \'+(mkt.trades24||0),\'📋 <b>Points historique:</b> \'+h.length,].map(t=>\'<div style="padding:8px 10px;border-bottom:1px solid rgba(0,229,255,.05);font-size:12px;color:var(--muted)">\'+t+\'</div>\').join(\'\');}// ══ SOLVABILITÉ ══\nasync function loadSolv(){try{var r=await fetch(\'/nxc/solvability\');var d=await r.json();if(d.ok){solvOn=d.enabled;var inp=$(\'sg\');if(inp)inp.value=d.gesture||50;updSolv();}}catch(e){}}function updSolv(){var t=$(\'stg\'),l=$(\'sl\');if(solvOn){if(t)t.classList.add(\'on\');if(l){l.textContent=\'✅ Activée — ventes bloquées si insolvable\';l.style.color=\'var(--green)\';}}else{if(t)t.classList.remove(\'on\');if(l){l.textContent=\'⏸ Désactivée\';l.style.color=\'var(--muted)\';}}}async function toggleSolv(){solvOn=!solvOn;updSolv();await saveSolv();}async function saveSolv(){var g=parseInt($(\'sg\').value)||50;var r=await fetch(\'/nxc/solvability\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,enabled:solvOn,gesture:g})});var res=await r.json();setMsg(\'sm\',res.ok?(solvOn?\'✅ Solvabilité activée\':\'⏸ Désactivée\'):\'❌ Erreur\',res.ok);if(res.ok)addLog(\'🛡️\',\'Solvabilité \'+(solvOn?\'activée\':\'désactivée\')+\' · geste:\'+g+\' R\');}// ══ CONFIG AVANCÉE ══\nvar _cfgFloor=null,_cfgCeil=null,_cfgVolMult=1,_cfgNoise=0.008,_schedInt=null;function cfgFloor(){var v=parseFloat($(\'cfg-floor\').value);if(!v||v<50)return;_cfgFloor=v;updCfgMsg();addLog(\'⚙️\',\'Plancher fixé:\'+fmt(v,0)+\' R\');}function cfgCeil(){var v=parseFloat($(\'cfg-ceil\').value);if(!v||v>100000)return;_cfgCeil=v;updCfgMsg();addLog(\'⚙️\',\'Plafond fixé:\'+fmt(v,0)+\' R\');}function updCfgMsg(){var el=$(\'cfg-floor-val\');if(el)el.textContent=\'Plancher:\'+(_cfgFloor?fmt(_cfgFloor,0)+\' R\':\'non défini\')+\' · Plafond:\'+(_cfgCeil?fmt(_cfgCeil,0)+\' R\':\'non défini\');}function applyAdvCfg(){_cfgVolMult=parseFloat($(\'cfg-vol\').value)||1;_cfgNoise=parseFloat($(\'cfg-noise\').value)/100||0.008;setMsg(\'cfg-adv-msg\',\'✅ Volatilité x\'+_cfgVolMult+\' · bruit \'+(_cfgNoise*100).toFixed(1)+\'%\',true);addLog(\'⚙️\',\'Config:volatilité x\'+_cfgVolMult+\',bruit \'+(_cfgNoise*100).toFixed(1)+\'%\');}function scheduleT(){var start=$(\'cfg-start\').value,stop=$(\'cfg-stop\').value,dir=$(\'cfg-dir\')?$(\'cfg-dir\').value:\'up\';if(!start||!stop){setMsg(\'cfg-sched-msg\',\'Renseigner les deux heures\',false);return;}if(_schedInt)clearInterval(_schedInt);_schedInt=setInterval(function(){var now=new Date(),cur=(\'0\'+now.getHours()).slice(-2)+\':\'+(\'0\'+now.getMinutes()).slice(-2);if(cur===start&&!tMode){setT(dir);addLog(\'⏰\',\'Tendance démarrée automatiquement\');}if(cur===stop&&tMode){setT(\'stop\');addLog(\'⏰\',\'Tendance arrêtée automatiquement\');}},30000);setMsg(\'cfg-sched-msg\',\'✅ Programmé:\'+dir+\' de \'+start+\' à \'+stop,true);addLog(\'⏰\',\'Tendance programmée \'+dir+\' \'+start+\'→\'+stop);}function exportHist(){var h=mkt.history||[];var blob=new Blob([JSON.stringify({exported:new Date().toISOString(),price:mkt.price,count:h.length,history:h},null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_history_\'+Date.now()+\'.json\';a.click();addLog(\'📥\',\'Export historique JSON (\'+h.length+\' pts)\');}function exportStats(){var blob=new Blob([JSON.stringify({exported:new Date().toISOString(),market:mkt,users:_users,config:{floor:_cfgFloor,ceil:_cfgCeil,volMult:_cfgVolMult,noise:_cfgNoise}},null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_report_\'+Date.now()+\'.json\';a.click();addLog(\'📊\',\'Export rapport complet JSON\');}// ══ ALERTES PERSONNALISÉES ══\nvar _alerts=[],_alertHist=[];function addAlert(){var price=parseFloat($(\'al-price\').value),dir=$(\'al-dir\').value;if(!price||price<=0)return;_alerts.push({price:price,dir:dir,id:Date.now(),triggered:false});$(\'al-price\').value=\'\';renderAlerts();addLog(\'🔔\',\'Alerte:prix \'+(dir===\'above\'?\'>\':\'<\')+\' \'+fmt(price,0)+\' R\');}function removeAlert(id){_alerts=_alerts.filter(function(a){return a.id!==id;});renderAlerts();}function renderAlerts(){var el=$(\'al-list\');if(!el)return;el.innerHTML=_alerts.length?_alerts.map(function(a){return \'<div style="padding:10px 12px;border-bottom:1px solid rgba(0,229,255,.05);display:flex;justify-content:space-between;align-items:center;gap:8px;font-size:12px">\'\n+\'<span style="color:\'+(a.triggered?\'var(--muted)\':\'var(--gold)\')+\'">Si prix \'+(a.dir===\'above\'?\'>\':\'<\')+\' <b>\'+fmt(a.price,0)+\'</b> R\'+(a.triggered?\' ✅ déclenchée\':\'\')+\'</span>\'\n+\'<button onclick="removeAlert(\'+a.id+\')" style="padding:4px 8px;border-radius:6px;background:rgba(255,61,94,.1);border:1px solid rgba(255,61,94,.3);color:var(--red);font-size:10px;cursor:pointer;flex-shrink:0">✕</button></div>\';}).join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune alerte</p>\';}function checkAlerts(p){var triggered=false;_alerts.forEach(function(a){if(a.triggered)return;if((a.dir===\'above\'&&p>a.price)||(a.dir===\'below\'&&p<a.price)){a.triggered=true;triggered=true;var m=\'🔔 ALERTE:prix \'+(a.dir===\'above\'?\'>\':\'<\')+\' \'+fmt(a.price,0)+\' R (actuel:\'+fmt(p,0)+\' R)\';_alertHist.unshift({ts:Date.now(),msg:m});addLog(\'🔔\',m);renderAlerts();renderAlertHist();if(window.Notification&&Notification.permission===\'granted\')new Notification(\'◈ Nexus NXC\',{body:m,icon:\'/favicon.ico\'});}});// Smart alerts\nvar smart=[];if(p>80000)smart.push({c:\'ae\',m:\'🚨 CRITIQUE:Prix extrême >80 000 R\'});else if(p<500)smart.push({c:\'ae\',m:\'🚨 CRITIQUE:Effondrement <500 R\'});else if(p<2000)smart.push({c:\'aw\',m:\'⚠️ Prix bas (<2 000 R) — surveiller\'});if(tMode===\'down\'&&p<2000)smart.push({c:\'ae\',m:\'🚨 Tendance baissière + prix bas = risque critique\'});if(!smart.length)smart.push({c:\'ao\',m:\'✅ Aucune alerte critique — marché sain\'});var el=$(\'smart-alerts\');if(el)el.innerHTML=smart.map(function(s){return \'<div class="ab \'+s.c+\'">\'+s.m+\'</div>\';}).join(\'\');}function renderAlertHist(){var el=$(\'al-hist\');if(!el)return;el.innerHTML=_alertHist.length?_alertHist.map(function(a){return \'<div class="log-item"><span class="log-time">\'+fmtTime(a.ts)+\'</span><span class="log-txt" style="color:var(--gold)">\'+esc(a.msg)+\'</span></div>\';}).join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune alerte déclenchée</p>\';}// Notifications navigateur\nif(window.Notification&&Notification.permission===\'default\'){setTimeout(function(){Notification.requestPermission();},3000);}// Override ref pour intégrer les alertes et config\nvar _origRef=ref;ref=async function(){await _origRef();var p=parseFloat(mkt.price||0);if(!p)return;// Appliquer plancher/plafond\nif(_cfgFloor&&p<_cfgFloor){await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:_cfgFloor,ts:Date.now(),vol:0,volume24:mkt.volume24||0,trades24:mkt.trades24||0})});addLog(\'⚙️\',\'Plancher activé:prix remonté à \'+fmt(_cfgFloor,0)+\' R\');}if(_cfgCeil&&p>_cfgCeil){await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:_cfgCeil,ts:Date.now(),vol:0,volume24:mkt.volume24||0,trades24:mkt.trades24||0})});addLog(\'⚙️\',\'Plafond activé:prix rabaissé à \'+fmt(_cfgCeil,0)+\' R\');}checkAlerts(p);};// ══ REWARDS ADMIN ══\nasync function loadRewardsAdmin(){if(!_users.length)await loadUsers();var total=_users.reduce((s,u)=>s+u.rew,0);var avg=_users.length?Math.round(total/_users.length):0;var maxU=_users.reduce((m,u)=>u.rew>m.rew?u:m,{rew:0,n:\'—\'});var el=$(\'rw-total\');if(el)el.textContent=fmt(total,0);var ea=$(\'rw-avg\');if(ea)ea.textContent=fmt(avg,0);var em=$(\'rw-max\');if(em)em.textContent=fmt(maxU.rew,0)+\' (\'+maxU.n+\')\';// Remplir les selects\n[\'rw-target\',\'rw-target2\',\'rw-target3\',\'cmp-a\',\'cmp-b\'].forEach(function(id){var sel=$(id);if(!sel)return;var cur=sel.value;sel.innerHTML=\'<option value="">Sélectionner...</option>\'+_users.map(u=>\'<option value="\'+esc(u.n)+\'">\'+esc(u.n)+(u.role===\'admin\'?\' 👑\':\'\')+\'</option>\').join(\'\');if(cur)sel.value=cur;});// Classement\nvar lb=$(\'rw-leaderboard\');if(lb){var sorted=[..._users].sort((a,b)=>b.rew-a.rew).slice(0,10);var maxR=sorted[0]?sorted[0].rew:1;lb.innerHTML=sorted.map((u,i)=>\'<div style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid rgba(0,229,255,.05)">\'\n+\'<span style="color:var(--muted);font-size:11px;width:20px;text-align:right">\'+(i+1)+\'</span>\'\n+\'<span style="flex:1;color:var(--cyan);font-weight:700;font-size:12px">\'+esc(u.n)+\'</span>\'\n+\'<div style="flex:2"><div class="pbar"><div class="pbar-fill" style="width:\'+Math.round(u.rew/maxR*100)+\'%;background:linear-gradient(90deg,var(--gold),var(--purple))"></div></div></div>\'\n+\'<span style="color:var(--gold);font-weight:700;font-size:12px;width:70px;text-align:right">\'+fmt(u.rew,0)+\'</span></div>\'\n).join(\'\');}}async function giveRewards(){var target=$(\'rw-target\').value,amt=parseFloat($(\'rw-amount\').value);if(!target||!amt||amt<=0){setMsg(\'rw-msg\',\'Sélectionner un utilisateur et un montant\',false);return;}var r=await fetch(\'/nxc/bank/gesture\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,target:target,amount:amt,fail_ts:null})});var res=await r.json();setMsg(\'rw-msg\',res.ok?\'✅ +\'+fmt(amt,0)+\' R donnés à \'+target:\'❌ \'+(res.error||\'Erreur\'),res.ok);if(res.ok){addLog(\'🏆\',\'Rewards donnés:+\'+fmt(amt,0)+\' R → \'+target);loadUsers();}}async function removeRewards(){var target=$(\'rw-target2\').value,amt=parseFloat($(\'rw-remove\').value);if(!target||!amt){setMsg(\'rw-msg2\',\'Champs requis\',false);return;}if(!confirm(\'Retirer \'+fmt(amt,0)+\' R à \'+target+\' ?\'))return;var u=_users.find(x=>x.n===target);if(!u){setMsg(\'rw-msg2\',\'Utilisateur introuvable\',false);return;}var newR=Math.max(0,u.rew-amt);var r=await api(\'/admin/set-rewards\',{target:target,rewards:newR});setMsg(\'rw-msg2\',r&&r.ok?\'✅ Retiré \'+fmt(amt,0)+\' R à \'+target:\'❌ Route non disponible — utilisez le geste commercial\',false);addLog(\'💸\',\'Tentative retrait rewards:-\'+fmt(amt,0)+\' R de \'+target);}async function resetRewards(){var target=$(\'rw-target3\').value;if(!target){setMsg(\'rw-msg3\',\'Sélectionner un utilisateur\',false);return;}if(!confirm(\'Remettre à zéro les rewards de \'+target+\' ?\'))return;addLog(\'🗑️\',\'Reset rewards:\'+target);setMsg(\'rw-msg3\',\'⚠️ Action loguée — route directe non disponible\',false);}// ══ PRÉVISIONS ══\nvar tresoObj=null;function loadPrevisions(){var h=mkt.history||[];var p=parseFloat(mkt.price||0);if(!p)return;// Trésorerie\nvar cv=$(\'ch-treso\');if(cv&&window.Chart){var b={};try{fetch(\'/nxc/bank\').then(r=>r.json()).then(function(d){b=d.bank||{};drawTreso(p,b,h);});}catch(e){drawTreso(p,b,h);}}// Indicateurs techniques\nif(h.length>=14){var prices=h.slice(-20).map(x=>parseFloat(x.price));var sma5=prices.slice(-5).reduce((s,v)=>s+v,0)/5;var sma10=prices.slice(-10).reduce((s,v)=>s+v,0)/10;var sma20=prices.reduce((s,v)=>s+v,0)/prices.length;var el=$(\'tech-indicators\');if(el){var items=[\n[\'SMA 5 ticks\',fmt(sma5,2)+\' R\',p>sma5?\'green\':\'red\'],[\'SMA 10 ticks\',fmt(sma10,2)+\' R\',p>sma10?\'green\':\'red\'],[\'SMA 20 ticks\',fmt(sma20,2)+\' R\',p>sma20?\'green\':\'red\'],[\'Signal\',sma5>sma10?\'📈 ACHAT (SMA5>SMA10)\':\'📉 VENTE (SMA5<SMA10)\',sma5>sma10?\'green\':\'red\'],];el.innerHTML=items.map(([k,v,c])=>\'<div style="display:flex;justify-content:space-between;padding:8px 10px;border-bottom:1px solid rgba(0,229,255,.05);font-size:12px"><span style="color:var(--muted)">\'+k+\'</span><b style="color:var(--\'+c+\')">\'+v+\'</b></div>\').join(\'\');}}// Support / Résistance\nif(h.length>=10){var ps=h.slice(-20).map(x=>parseFloat(x.price));var support=Math.min.apply(null,ps);var resist=Math.max.apply(null,ps);var mid=(support+resist)/2;var el2=$(\'support-resistance\');if(el2){el2.innerHTML=[\n[\'🔴 Résistance (haut)\',fmt(resist,2)+\' R\',\'red\'],[\'🟡 Zone médiane\',fmt(mid,2)+\' R\',\'gold\'],[\'🟢 Support (bas)\',fmt(support,2)+\' R\',\'green\'],[\'📏 Amplitude\',fmt(resist-support,2)+\' R (\'+((resist-support)/support*100).toFixed(1)+\'%)\',\'cyan\'],].map(([k,v,c])=>\'<div style="display:flex;justify-content:space-between;padding:10px 12px;border-bottom:1px solid rgba(0,229,255,.05);font-size:12px"><span style="color:var(--muted)">\'+k+\'</span><b style="color:var(--\'+c+\')">\'+v+\'</b></div>\').join(\'\');}}}function drawTreso(p,b,h){var cv=$(\'ch-treso\');if(!cv||!window.Chart)return;if(tresoObj){tresoObj.destroy();tresoObj=null;}var flux=b.flux||[];var recent=flux.filter(f=>f.ts>Date.now()-604800000);var wIn=recent.filter(f=>f.type===\'IN\').reduce((s,f)=>s+(f.amount||0),0);var wOut=recent.filter(f=>f.type===\'OUT\').reduce((s,f)=>s+(f.amount||0),0);var dIn=wIn/7,dOut=wOut/7;var base=b.reserves||0;var labs=[],opt=[],real=[],pes=[];for(var d=0;d<=30;d++){labs.push(d===0?\'Auj\':\'J+\'+d);opt.push(Math.max(0,Math.round(base+d*(dIn*1.3-dOut*0.7))));real.push(Math.max(0,Math.round(base+d*(dIn-dOut))));pes.push(Math.max(0,Math.round(base+d*(dIn*0.7-dOut*1.3))));}var ctx=cv.getContext(\'2d\');tresoObj=new Chart(ctx,{type:\'line\',data:{labels:labs,datasets:[{label:\'Optimiste\',data:opt,borderColor:\'var(--green)\',borderWidth:1.5,pointRadius:0,fill:false,tension:0.4},{label:\'Réaliste\',data:real,borderColor:\'var(--cyan)\',borderWidth:2.5,pointRadius:0,fill:false,tension:0.4},{label:\'Pessimiste\',data:pes,borderColor:\'var(--red)\',borderWidth:1.5,pointRadius:0,fill:false,tension:0.4},]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:\'var(--muted)\',font:{size:10}}}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:6,font:{size:8}},grid:{color:\'rgba(0,229,255,.04)\'}},y:{min:0,ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});}var _randPrice=null;function simulateFuture(){var days=parseInt($(\'sim-days\').value)||7;var sc=$(\'sim-scenario\').value;var p=parseFloat(mkt.price||5213);var daily={bull:0.02,bear:-0.02,flat:0.001,volatile:0}[sc]||0;var simP=p;for(var d=0;d<days;d++){if(sc===\'volatile\')simP*=(1+(Math.random()-0.5)*0.1);else simP*=(1+daily);simP=Math.max(50,Math.min(100000,simP));}simP=Math.round(simP*100)/100;var chg=((simP-p)/p*100);$(\'sim-result\').innerHTML=\'Dans <b>\'+days+\'j</b> (scénario \'+sc+\'):<br><span style="font-size:22px;color:\'+(chg>=0?\'var(--green)\':\'var(--red)\')+\'">\'+fmt(simP,2)+\' R</span><span style="color:var(--muted)">(\'+( chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%)</span>\';}// ══ COMPARAISON ══\nasync function compareUsers(){var a=$(\'cmp-a\').value,b=$(\'cmp-b\').value;if(!a||!b||a===b){$(\'cmp-result\').innerHTML=\'<div class="card ae"><div class="ct">Sélectionner 2 utilisateurs différents</div></div>\';return;}var ua=_users.find(u=>u.n===a)||{},ub=_users.find(u=>u.n===b)||{};var rows=[[\'🏆 Rewards\',ua.rew,ub.rew,\'gold\'],[\'◈ NXC\',ua.nxc,ub.nxc,\'cyan\'],[\'💰 Valeur (R)\',ua.val,ub.val,\'purple\']];$(\'cmp-result\').innerHTML=\'<div class="card cyan"><div class="ct">◈ \'+esc(a)+\' vs \'+esc(b)+\'</div>\'\n+\'<div style="display:grid;grid-template-columns:1fr auto 1fr;gap:8px;text-align:center;margin-bottom:10px">\'\n+\'<div style="font-size:16px;font-weight:900;color:var(--cyan)">\'+esc(a)+\'</div><div style="color:var(--muted);font-size:20px">⚔️</div><div style="font-size:16px;font-weight:900;color:var(--purple)">\'+esc(b)+\'</div></div>\'\n+rows.map(([label,va,vb,c])=>{var winner=va>vb?\'a\':vb>va?\'b\':\'tie\';return \'<div style="display:grid;grid-template-columns:1fr auto 1fr;gap:8px;padding:10px 0;border-bottom:1px solid rgba(0,229,255,.05);align-items:center;text-align:center">\'\n+\'<b style="color:\'+(winner===\'a\'?\'var(--green)\':\'var(--muted)\')+\'">\'+fmt(va,va<10?4:0)+(winner===\'a\'?\' 🏆\':\'\')+\' </b>\'\n+\'<span style="color:var(--muted);font-size:10px">\'+label+\'</span>\'\n+\'<b style="color:\'+(winner===\'b\'?\'var(--green)\':\'var(--muted)\')+\'"> \'+(winner===\'b\'?\'🏆 \':\'\')+fmt(vb,vb<10?4:0)+\'</b></div>\';}).join(\'\')+\'</div>\';// Top 3\nvar t3=$(\'top3\');if(t3&&_users.length){var cats=[[\'🏆 Rewards\',\'rew\',\'gold\'],[\'◈ NXC\',\'nxc\',\'cyan\'],[\'💰 Valeur\',\'val\',\'purple\']];t3.innerHTML=cats.map(([lbl,key,c])=>{var sorted=[..._users].sort((a,b)=>b[key]-a[key]).slice(0,3);return \'<div style="margin-bottom:10px"><div class="sec">\'+lbl+\'</div>\'\n+sorted.map((u,i)=>\'<div style="display:flex;justify-content:space-between;padding:6px 10px;font-size:11px;border-bottom:1px solid rgba(0,229,255,.05)">\'\n+\'<span>\'+ [\'🥇\',\'🥈\',\'🥉\'][i]+\' <b style="color:var(--\'+c+\')">\'+esc(u.n)+\'</b></span>\'\n+\'<span style="color:var(--\'+c+\')">\'+fmt(u[key],key===\'nxc\'?4:0)+\'</span></div>\'\n).join(\'\')+\'</div>\';}).join(\'\');}}// ══ SÉCURITÉ ══\nasync function loadSecurity(){try{var r=await fetch(\'/nxc/bank/fail\');var d=await r.json();var fails=d.fails||[];var okSells=parseInt(mkt.trades24||0);var failN=fails.length;var el=$(\'sec-ok\');if(el)el.textContent=okSells;var ef=$(\'sec-fail\');if(ef)ef.textContent=failN;var eg=$(\'sec-gests\');if(eg)eg.textContent=fails.filter(f=>f.gesture>0).length;var er=$(\'sec-ratio\');if(er)er.textContent=(okSells+failN>0?Math.round(failN/(okSells+failN)*100):0)+\'%\';// Activité suspecte\nvar sus=$(\'sec-suspicious\');if(sus){var byUser={};fails.forEach(f=>{byUser[f.user]=(byUser[f.user]||0)+1;});var suspects=Object.entries(byUser).filter(([u,n])=>n>=2).sort((a,b)=>b[1]-a[1]);sus.innerHTML=suspects.length?suspects.map(([u,n])=>\'<div class="ab aw">⚠️ <b>\'+esc(u)+\'</b> — \'+n+\' tentatives de vente échouées</div>\').join(\'\'):\'<div class="ab ao">✅ Aucune activité suspecte détectée</div>\';}}catch(e){}}function changeKey(){var nk=$(\'new-key\').value.trim();if(!nk||nk.length<8){$(\'key-msg\').textContent=\'Clé trop courte (min 8 chars)\';return;}KEY=nk;$(\'new-key\').value=\'\';$(\'key-msg\').textContent=\'✅ Clé changée pour cette session\';$(\'key-msg\').style.color=\'var(--green)\';addLog(\'🔑\',\'Clé maître changée (session)\');}// ══ OUTILS ══\nfunction calcNxc(){var nxc=parseFloat($(\'calc-nxc\').value)||0;var p=parseFloat(mkt.price||0);$(\'calc-rew\').value=nxc&&p?Math.round(nxc*p*100)/100:\'\';}function calcRew(){var rew=parseFloat($(\'calc-rew2\').value)||0;var p=parseFloat(mkt.price||1);$(\'calc-nxc2\').value=rew&&p?(rew/p).toFixed(6):\'\';}function simSell(){var nxc=parseFloat($(\'sell-sim-nxc\').value)||0;var fee=parseFloat($(\'sell-sim-fee\').value)||0;var p=parseFloat(mkt.price||0);if(!nxc||!p){$(\'sell-sim-result\').innerHTML=\'\';return;}var gross=nxc*p;var feesR=gross*fee/100;var net=gross-feesR;$(\'sell-sim-result\').innerHTML=\'<div style="display:flex;flex-direction:column;gap:4px">\'\n+\'<div style="color:var(--muted)">Brut:<b style="color:var(--text)">\'+fmt(gross,2)+\' R</b></div>\'\n+\'<div style="color:var(--muted)">Frais (\'+fee+\'%):<b style="color:var(--red)">-\'+fmt(feesR,2)+\' R</b></div>\'\n+\'<div style="color:var(--muted)">Net reçu:<b style="color:var(--green);font-size:18px">\'+fmt(net,2)+\' R</b></div>\'\n+\'<div style="color:var(--muted);font-size:10px">Au prix actuel de \'+fmt(p,2)+\' R/NXC</div></div>\';}function genRandPrice(){var mn=parseFloat($(\'rnd-min\').value)||50;var mx=parseFloat($(\'rnd-max\').value)||10000;_randPrice=Math.round((mn+Math.random()*(mx-mn))*100)/100;$(\'rnd-result\').textContent=fmt(_randPrice,2)+\' R/NXC\';}async function applyRandPrice(){if(!_randPrice){genRandPrice();}if(!confirm(\'Appliquer le prix de \'+fmt(_randPrice,2)+\' R ?\'))return;var r=await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:_randPrice,ts:Date.now(),vol:0,volume24:mkt.volume24||0,trades24:mkt.trades24||0})});var res=await r.json();if(res.ok){addLog(\'🎲\',\'Prix aléatoire appliqué:\'+fmt(_randPrice,2)+\' R\');setTimeout(ref,500);}}var _timerInt=null,_timerEnd=null;function startTimer(){var m=parseInt($(\'timer-min\').value)||0;var s=parseInt($(\'timer-sec\').value)||0;var total=m*60+s;var action=$(\'timer-action\').value;if(!total||total<=0)return;if(_timerInt)clearInterval(_timerInt);_timerEnd=Date.now()+total*1000;addLog(\'⏱️\',\'Minuteur:action "\'+action+\'" dans \'+total+\'s\');_timerInt=setInterval(async function(){var rem=Math.max(0,Math.round((_timerEnd-Date.now())/1000));var mm=Math.floor(rem/60),ss=rem%60;var el=$(\'timer-display\');if(el)el.textContent=(\'0\'+mm).slice(-2)+\':\'+(\'0\'+ss).slice(-2);if(rem<=0){clearInterval(_timerInt);_timerInt=null;var disp=$(\'timer-display\');if(disp){disp.textContent=\'✅ Action!\';disp.style.color=\'var(--green)\';}if(action===\'stop\')setT(\'stop\');else if(action===\'up\'||action===\'down\')setT(action);else if(action===\'crash\'||action===\'moon\')scenario(action===\'crash\'?\'crash\':\'moon\');addLog(\'⏱️\',\'Minuteur déclenché:action "\'+action+\'"\');}},500);}function stopTimer(){if(_timerInt){clearInterval(_timerInt);_timerInt=null;var d=$(\'timer-display\');if(d){d.textContent=\'\';d.style.color=\'var(--cyan)\';}}addLog(\'⏱️\',\'Minuteur annulé\');}async function pingServer(){var el=$(\'ping-result\');el.textContent=\'📡 Test en cours...\';el.style.color=\'var(--muted)\';var start=Date.now();try{await fetch(\'/nxc/price\');var lat=Date.now()-start;el.textContent=\'✅ Serveur en ligne — latence:\'+lat+\' ms\';el.style.color=lat<500?\'var(--green)\':lat<1000?\'var(--gold)\':\'var(--red)\';}catch(e){el.textContent=\'❌ Serveur injoignable\';el.style.color=\'var(--red)\';}}// Override go() pour charger les données des nouveaux onglets\nvar _origGo=go;go=function(tab,btn){_origGo(tab,btn);if(tab===\'rewards\')loadRewardsAdmin();if(tab===\'market2\')loadPrevisions();if(tab===\'compare\'){loadRewardsAdmin();}if(tab===\'security\')loadSecurity();};</script></body></html>'

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
        db["users"][u]["data"] = d.get("data", {})
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
