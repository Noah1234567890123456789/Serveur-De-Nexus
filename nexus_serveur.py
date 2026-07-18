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
NXC_PANEL_HTML = '<!DOCTYPE html>\n<html lang="fr">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width,initial-scale=1">\n<title>Nexus NXC</title>\n<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>\n<script src="https://cdn.jsdelivr.net/npm/hammerjs@2.0.8/hammer.min.js"></script>\n<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@2.0.1/dist/chartjs-plugin-zoom.min.js"></script>\n<style>\n*{box-sizing:border-box;font-family:\'Segoe UI\',system-ui,sans-serif;margin:0;padding:0}\nbody{background:#02040a;color:#d4e8ff;min-height:100vh}\n.wrap{max-width:1100px;margin:0 auto;padding:18px}\nh1{font-size:18px;color:#00e5ff;letter-spacing:2px;font-family:monospace;margin-bottom:4px}\n.sub{color:#5c6b8c;font-size:12px;margin-bottom:18px}\n.card{background:#080d1a;border:1px solid rgba(0,229,255,.15);border-radius:14px;padding:18px;margin-bottom:14px}\n.ct{font-size:10px;letter-spacing:2px;color:#5c6b8c;margin-bottom:14px;font-weight:700}\ninput,select,button{font-size:13px;border-radius:9px;padding:10px 13px;border:1px solid rgba(0,229,255,.2);background:#0d1428;color:#d4e8ff;outline:none;font-family:inherit}\nbutton{cursor:pointer;font-weight:700}\n.bc{background:linear-gradient(135deg,#00e5ff,#00b4d8);color:#000;border:none}\n.bg2{background:rgba(0,255,157,.12);border-color:rgba(0,255,157,.3);color:#00ff9d}\n.br2{background:rgba(255,61,94,.12);border-color:rgba(255,61,94,.3);color:#ff3d5e}\n.bp{background:rgba(160,107,255,.12);border-color:rgba(160,107,255,.3);color:#a06bff}\n.ba{background:rgba(255,176,32,.12);border-color:rgba(255,176,32,.3);color:#ffb020}\n.g4{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:14px}\n.g2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:14px}\n.st{background:#0d1428;border:1px solid rgba(0,229,255,.1);border-radius:10px;padding:14px;text-align:center}\n.sv{font-family:monospace;font-size:22px;font-weight:700;color:#00e5ff;margin-bottom:3px}\n.sl{font-size:9px;color:#5c6b8c;letter-spacing:1px}\n.sv.gold{color:#ffb020}.sv.green{color:#00ff9d}.sv.purple{color:#a06bff}\n.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}\n.grow{flex:1;min-width:120px}\ntable{width:100%;border-collapse:collapse;font-size:12px}\nth,td{padding:9px 8px;text-align:left;border-bottom:1px solid rgba(0,229,255,.07)}\nth{color:#5c6b8c;font-size:10px;text-transform:uppercase;letter-spacing:.5px}\n.nav{display:flex;gap:8px;margin-bottom:18px}\n.nav a{padding:8px 14px;border-radius:8px;border:1px solid rgba(0,229,255,.2);color:#00e5ff;text-decoration:none;font-size:12px;font-weight:700}\n.nav a:hover{background:rgba(0,229,255,.1)}\n.ab{padding:10px 14px;border-radius:9px;font-size:12px;margin-bottom:6px}\n.ao{background:rgba(0,255,157,.08);border:1px solid rgba(0,255,157,.2);color:#00ff9d}\n.aw{background:rgba(255,176,32,.08);border:1px solid rgba(255,176,32,.2);color:#ffb020}\n.ae{background:rgba(255,61,94,.08);border:1px solid rgba(255,61,94,.2);color:#ff3d5e}\n</style>\n</head>\n<body>\n<div class="wrap">\n  <h1>&#9672; NEXUS COIN &mdash; PANNEAU SERVEUR</h1>\n  <div class="sub">Administration du march&#233; NXC &middot; Donn&#233;es en temps r&#233;el</div>\n  <div class="nav">\n    <a href="/panel">&#128737; Admin</a>\n    <a href="/nexus">&#127760; Nexus</a>\n    <a href="/nxc/price" target="_blank">&#128225; API Prix</a>\n  </div>\n\n  <div id="lb" class="card">\n    <div class="ct">&#9672; CONNEXION ADMIN</div>\n    <div class="row">\n      <input id="mk" type="password" placeholder="Cl&#233; ma&#238;tre" class="grow" onkeydown="if(event.key===\'Enter\')conn()">\n      <button class="bc" onclick="conn()">Connexion</button>\n    </div>\n    <div id="lm" style="font-size:12px;color:#ff3d5e;margin-top:8px"></div>\n  </div>\n\n  <div id="dash" style="display:none">\n    <div class="g4">\n      <div class="st"><div class="sv" id="sp">&#8212;</div><div class="sl">PRIX R/NXC</div></div>\n      <div class="st"><div class="sv gold" id="sv">&#8212;</div><div class="sl">VOLUME 24H</div></div>\n      <div class="st"><div class="sv green" id="str">&#8212;</div><div class="sl">TRADES 24H</div></div>\n      <div class="st"><div class="sv purple" id="sh">&#8212;</div><div class="sl">POINTS HIST.</div></div>\n    </div>\n\n    <div class="card">\n      <div class="ct">&#9672; HISTORIQUE DU COURS (100 derniers points)</div>\n      <div style="height:220px"><canvas id="ch"></canvas></div>\n      <div style="display:flex;gap:6px;margin-top:8px">\n        <button onclick="chObj&&chObj.zoom(1.3)" style="padding:6px 12px;font-size:11px;background:rgba(0,229,255,.1);border:1px solid rgba(0,229,255,.3);border-radius:7px;color:#00e5ff;cursor:pointer">+ Zoom</button>\n        <button onclick="chObj&&chObj.zoom(0.7)" style="padding:6px 12px;font-size:11px;background:rgba(0,229,255,.1);border:1px solid rgba(0,229,255,.3);border-radius:7px;color:#00e5ff;cursor:pointer">- Dezoom</button>\n        <button onclick="chObj&&chObj.resetZoom()" style="padding:6px 12px;font-size:11px;background:rgba(255,176,32,.1);border:1px solid rgba(255,176,32,.3);border-radius:7px;color:#ffb020;cursor:pointer">Reset</button>\n      </div>\n    </div>\n\n    <div class="g2">\n      <div class="card">\n        <div class="ct">&#9672; MODIFIER LE COURS</div>\n        <div class="row" style="margin-bottom:8px">\n          <input id="np" type="number" min="50" max="100000" placeholder="Nouveau prix (50-100000)" class="grow">\n          <button class="bc" onclick="setP()">&#10003; Appliquer</button>\n        </div>\n        <button class="ba" style="width:100%;margin-bottom:6px" onclick="resetH()">&#128260; Reset historique</button>\n        <div id="pm" style="font-size:11px;color:#00ff9d;margin-top:4px"></div>\n      </div>\n      <div class="card">\n        <div class="ct">&#9672; TENDANCE AUTOMATIQUE</div>\n        <select id="ts" style="width:100%;margin-bottom:8px">\n          <option value="0.002">Tr&#232;s lent (0.2%/tick)</option>\n          <option value="0.005" selected>Lent (0.5%/tick)</option>\n          <option value="0.01">Moyen (1%/tick)</option>\n          <option value="0.02">Rapide (2%/tick)</option>\n          <option value="0.05">Tr&#232;s rapide (5%/tick)</option>\n        </select>\n        <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px">\n          <button class="bg2" onclick="setT(\'up\')">&#128200; Hausse</button>\n          <button class="br2" onclick="setT(\'down\')">&#128201; Baisse</button>\n          <button class="bp" onclick="setT(\'random\')">&#127922; Al&#233;atoire</button>\n          <button onclick="setT(\'stop\')" style="border-color:rgba(255,255,255,.1);color:#5c6b8c">&#9208; Stop</button>\n        </div>\n        <div id="tst" style="font-size:11px;color:#5c6b8c;margin-top:8px">&#9208; Arr&#234;t&#233;</div>\n      </div>\n    </div>\n\n    <div class="card">\n      <div class="ct">&#9672; ALERTES MARCH&#201;</div>\n      <div id="al"></div>\n    </div>\n\n    <div class="card">\n      <div class="ct">&#9672; COMPTES UTILISATEURS &mdash; SOLDES NXC</div>\n      <table>\n        <thead><tr><th>Compte</th><th>R&#244;le</th><th>Rewards</th><th>NXC</th><th>Valeur (R)</th></tr></thead>\n        <tbody id="ut"></tbody>\n      </table>\n    </div>\n  </div>\n</div>\n\n<script>\nvar KEY=\'\',mkt={},tInt=null,tMode=null,tStr=0.005,chObj=null;\n\nasync function api(p,b){\n  b=b||{};b.master_key=KEY;\n  try{var r=await fetch(p,{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify(b)});return await r.json();}\n  catch(e){return{ok:false};}\n}\n\nasync function conn(){\n  KEY=document.getElementById(\'mk\').value.trim();\n  var m=document.getElementById(\'lm\');\n  m.textContent=\'Connexion...\';\n  var r=await api(\'/admin/list\');\n  if(r&&r.ok){\n    document.getElementById(\'lb\').style.display=\'none\';\n    document.getElementById(\'dash\').style.display=\'block\';\n    ref();setInterval(ref,5000);\n  }else{m.textContent=\'Cle incorrecte\';}\n}\n\nasync function ref(){\n  var r=await fetch(\'/nxc/price\');var d=await r.json();mkt=d;\n  var p=parseFloat(d.price||0),h=d.history||[];\n  document.getElementById(\'sp\').textContent=fmt(p,2);\n  document.getElementById(\'sv\').textContent=fmt(d.volume24||0,0);\n  document.getElementById(\'str\').textContent=d.trades24||0;\n  document.getElementById(\'sh\').textContent=h.length;\n  drawC(h);drawA(p,h);loadU(p);\n}\n\nfunction fmt(n,d){return Number(n||0).toLocaleString(\'fr-FR\',{minimumFractionDigits:d||0,maximumFractionDigits:d==null?2:d});}\n\nfunction drawC(h){\n  var cv=document.getElementById(\'ch\');if(!cv||!window.Chart)return;\n  var pts=h.slice(-100);\n  var labs=pts.map(function(x){return new Date(x.ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\'});});\n  var prices=pts.map(function(x){return x.price;});\n  if(chObj){chObj.data.labels=labs;chObj.data.datasets[0].data=prices;chObj.update(\'none\');return;}\n  var ctx=cv.getContext(\'2d\');\n  var g=ctx.createLinearGradient(0,0,0,200);g.addColorStop(0,\'rgba(0,229,255,.3)\');g.addColorStop(1,\'rgba(0,229,255,0)\');\n  chObj=new Chart(ctx,{type:\'line\',data:{labels:labs,datasets:[{data:prices,borderColor:\'#00e5ff\',backgroundColor:g,borderWidth:2,pointRadius:0,fill:true,tension:0.4}]},\n    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},zoom:{zoom:{wheel:{enabled:true},pinch:{enabled:true},mode:\'x\'},pan:{enabled:true,mode:\'x\'}}},\n      scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:6,font:{size:9}},grid:{color:\'rgba(0,229,255,.05)\'}},\n        y:{ticks:{color:\'#5c6b8c\',callback:function(v){return fmt(v,0);}},grid:{color:\'rgba(0,229,255,.05)\'}}},\n      animation:{duration:0}}});\n}\n\nfunction drawA(p,h){\n  var el=document.getElementById(\'al\'),a=[];\n  if(p>80000)a.push({c:\'aw\',m:\'Prix tres eleve (>80 000 R) - resistance probable\'});\n  if(p<500)a.push({c:\'ae\',m:\'Prix tres bas (<500 R) - zone critique\'});\n  if(h.length>10){\n    var r=h.slice(-10).map(function(x){return x.price;});\n    var v=(Math.max.apply(null,r)-Math.min.apply(null,r))/Math.min.apply(null,r)*100;\n    a.push(v>20?{c:\'aw\',m:\'Forte volatilite sur 10 ticks: \'+v.toFixed(1)+\'%\'}:{c:\'ao\',m:\'Marche stable - volatilite: \'+v.toFixed(1)+\'%\'});\n  }\n  a.push(tMode?{c:\'aw\',m:\'Tendance active: \'+(tMode===\'up\'?\'Hausse\':tMode===\'down\'?\'Baisse\':\'Aleatoire\')+\' \'+(tStr*100).toFixed(1)+\'%/tick\'}\n    :{c:\'ao\',m:\'Aucune tendance active - cours stable\'});\n  el.innerHTML=a.map(function(x){return \'<div class="ab \'+x.c+\'">\'+x.m+\'</div>\';}).join(\'\');\n}\n\nasync function loadU(price){\n  var r=await api(\'/admin/list\');if(!r||!r.ok)return;\n  var rows=await Promise.all((r.users||[]).map(async function(u){\n    var d=await api(\'/admin/get\',{target:u.username});\n    var rew=(d.data&&d.data.nx2098&&d.data.nx2098.rewards)||0;\n    var nxc=parseFloat((d.data&&d.data.nxcoin&&d.data.nxcoin.nxc)||0);\n    return \'<tr><td style="color:#00e5ff;font-weight:700">\'+u.username+(u.role===\'admin\'?\' &#128081;\':\'\')\n      +\'</td><td>\'+u.role+\'</td><td style="color:#ffb020">\'+fmt(rew,0)+\' R</td>\'\n      +\'<td style="color:#00e5ff">\'+nxc.toFixed(4)+\' NXC</td>\'\n      +\'<td style="color:#a06bff">\'+fmt(nxc*price,0)+\' R</td></tr>\';\n  }));\n  document.getElementById(\'ut\').innerHTML=rows.join(\'\');\n}\n\nasync function setP(){\n  var p=parseFloat(document.getElementById(\'np\').value);\n  if(!p||p<50||p>100000){document.getElementById(\'pm\').textContent=\'Prix invalide (50-100000)\';return;}\n  var r=await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},\n    body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:0,volume24:mkt.volume24||0,trades24:mkt.trades24||0})});\n  var res=await r.json();\n  document.getElementById(\'pm\').textContent=res.ok?\'OK: \'+fmt(p,2)+\' R/NXC\':\'Erreur serveur\';\n  document.getElementById(\'np\').value=\'\';\n  ref();\n}\n\nasync function resetH(){\n  if(!confirm(\'Remettre l historique a zero ?\'))return;\n  await fetch(\'/nxc/reset\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY})});\n  ref();\n}\n\nfunction setT(m){\n  var s=parseFloat(document.getElementById(\'ts\').value)||0.005;\n  if(tInt){clearInterval(tInt);tInt=null;}\n  tMode=m===\'stop\'?null:m;tStr=s;\n  var el=document.getElementById(\'tst\');\n  if(!tMode){el.textContent=\'Arrete\';return;}\n  el.textContent=(m===\'up\'?\'Hausse +\':m===\'down\'?\'Baisse -\':\'Aleatoire \')+(m!==\'random\'?(s*100).toFixed(1)+\'%/tick\':\'\');\n  tInt=setInterval(async function(){\n    var p=parseFloat(mkt.price||5213);\n    var adj=(Math.random()-0.48)*0.008;\n    if(m===\'up\')adj+=s;else if(m===\'down\')adj-=s;\n    p=Math.max(50,Math.min(100000,p*(1+adj)));\n    p=Math.random()>0.03?Math.round(p*100)/100:Math.round(p);\n    await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},\n      body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:100,\n        volume24:(mkt.volume24||0)+100,trades24:(mkt.trades24||0)+1})});\n  },12000);\n}\n</script>\n</body>\n</html>\n'

ADMIN_HTML = r"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Nexus — Administration</title>
<style>
  * { box-sizing:border-box; font-family:'Segoe UI',system-ui,Arial,sans-serif; }
  body { margin:0; background:#0a0d14; color:#eaf0fb; }
  a { color:#a06bff; }
  .wrap { max-width:920px; margin:0 auto; padding:18px; }
  h1 { font-size:22px; margin:0 0 4px; }
  .muted { color:#8a96ad; font-size:13px; }
  .card { background:#121724; border:1px solid #283046; border-radius:14px; padding:16px; margin-top:14px; }
  input, select, button { font-size:15px; border-radius:10px; padding:11px 13px; border:1px solid #283046;
    background:#1b2233; color:#eaf0fb; outline:none; }
  input:focus, select:focus { border-color:#5b9dff; }
  button { cursor:pointer; }
  button:hover { border-color:#5b9dff; }
  .accent { border:none; font-weight:700; color:#06080c;
    background:linear-gradient(90deg,#5b9dff,#a06bff); }
  .row { display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
  .grow { flex:1; min-width:120px; }
  table { width:100%; border-collapse:collapse; margin-top:8px; }
  th, td { text-align:left; padding:10px 8px; border-bottom:1px solid #1c2333; font-size:14px; }
  th { color:#8a96ad; font-size:12px; text-transform:uppercase; letter-spacing:.5px; }
  .badge { font-size:11px; padding:2px 8px; border-radius:20px; }
  .adm { background:#3b2d5e; color:#c9b6ff; }
  .usr { background:#1e3346; color:#9ec7ff; }
  .act { background:transparent; border:1px solid #283046; padding:6px 9px; font-size:13px; border-radius:8px; }
  .ok { color:#34d399; } .warn { color:#f5b740; } .off { color:#ef5d6b; }
  .hidden { display:none; }
  .overlay { position:fixed; inset:0; background:rgba(0,0,0,.6); display:flex;
    align-items:center; justify-content:center; padding:16px; }
  .modal { background:#121724; border:1px solid #283046; border-radius:16px; padding:18px;
    max-width:560px; width:100%; max-height:85vh; overflow:auto; }
  pre { white-space:pre-wrap; word-break:break-word; background:#0a0d14; border:1px solid #283046;
    border-radius:10px; padding:10px; font-size:12px; color:#c7d2e6; }
</style>
</head>
<body>
<div class="wrap">
  <h1>🛡️ Nexus — Administration</h1>
  <div class="muted">Tout ce que tu fais ici est enregistré sur le serveur en ligne et récupéré par les serveurs locaux.</div>

  <div id="login" class="card">
    <div class="row">
      <input id="mk" class="grow" type="password" placeholder="Clé maître">
      <button class="accent" onclick="connecter()">Se connecter</button>
    </div>
    <div id="loginmsg" class="muted" style="margin-top:8px"></div>
  </div>

  <div id="dash" class="hidden">
    <div class="card">
      <div class="row">
        <div class="grow"><span id="status" class="ok">Connecté</span></div>
        <button onclick="location.href='/nexus'">🌐 Nexus</button>
        <button onclick="location.href='/nxc'" style="background:#0d1428;border-color:#00e5ff;color:#00e5ff">◈ NXC</button>
        <input id="search" class="grow" placeholder="🔍 Rechercher…" oninput="render()">
        <label class="muted"><input type="checkbox" id="showHidden" onchange="render()"> voir masqués</label>
      </div>
    </div>

    <div class="card">
      <b>➕ Créer un compte</b>
      <div class="row" style="margin-top:10px">
        <input id="nu" class="grow" placeholder="Nom d'utilisateur">
        <input id="np" class="grow" type="text" placeholder="Mot de passe">
        <select id="nr"><option value="user">Utilisateur</option><option value="admin">Administrateur</option></select>
        <button class="accent" onclick="creer()">Créer</button>
      </div>
      <div id="createmsg" class="muted" style="margin-top:8px"></div>
    </div>

    <div class="card">
      <div class="row"><b class="grow">Comptes (<span id="count">0</span>)</b>
        <span class="muted" id="tick">actualisation auto…</span></div>
      <table><thead><tr><th>Compte</th><th>Rôle</th><th>Pages</th><th>Dernière connexion</th><th></th></tr></thead>
      <tbody id="tbody"></tbody></table>
    </div>
  </div>
</div>

<div id="modal"></div>

<script>
let KEY = "";
let USERS = [];

async function api(path, body) {
  body = body || {};
  body.master_key = KEY;
  const r = await fetch(path, { method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify(body) });
  return await r.json();
}

async function connecter() {
  KEY = document.getElementById("mk").value.trim();
  const msg = document.getElementById("loginmsg");
  msg.textContent = "Connexion…";
  const res = await api("/admin/list");
  if (res && res.ok) {
    document.getElementById("login").classList.add("hidden");
    document.getElementById("dash").classList.remove("hidden");
    USERS = res.users || []; render();
    if (!window._timer) window._timer = setInterval(rafraichir, 3000);
  } else {
    msg.innerHTML = "<span class='off'>Clé maître refusée.</span>";
  }
}

async function rafraichir() {
  const res = await api("/admin/list");
  if (res && res.ok) {
    USERS = res.users || [];
    document.getElementById("status").innerHTML = "<span class='ok'>● En ligne — synchronisé</span>";
    render();
    const t = document.getElementById("tick");
    t.textContent = "à jour • " + new Date().toLocaleTimeString();
  } else {
    document.getElementById("status").innerHTML = "<span class='warn'>● reconnexion…</span>";
  }
}

function render() {
  const q = document.getElementById("search").value.toLowerCase();
  const showHidden = document.getElementById("showHidden").checked;
  const tb = document.getElementById("tbody");
  tb.innerHTML = "";
  let shown = 0;
  USERS.forEach(u => {
    if (u.hidden && !showHidden) return;
    if (q && !u.username.toLowerCase().includes(q) && !(u.nickname||"").toLowerCase().includes(q)) return;
    shown++;
    const tr = document.createElement("tr");
    const nick = u.nickname ? " « "+esc(u.nickname)+" »" : "";
    const badge = u.role === "admin" ? "<span class='badge adm'>👑 admin</span>" : "<span class='badge usr'>👤 user</span>";
    const mask = u.hidden ? "🙈 " : "";
    tr.innerHTML =
      "<td>"+mask+"<b>"+esc(u.username)+"</b>"+nick+"</td>"+
      "<td>"+badge+"</td>"+
      "<td>"+u.history+"</td>"+
      "<td class='muted'>"+(u.last_login? esc(u.last_login)+" · "+esc(u.last_ip) : "jamais")+"</td>"+
      "<td class='row'>"+
        "<button class='act' onclick=\"voir('"+jsq(u.username)+"')\">Voir</button>"+
        "<button class='act' onclick=\"renommer('"+jsq(u.username)+"')\">Renommer</button>"+
        "<button class='act' onclick=\"surnom('"+jsq(u.username)+"')\">Surnom</button>"+
        "<button class='act' onclick=\"masquer('"+jsq(u.username)+"',"+(u.hidden?"false":"true")+")\">"+(u.hidden?"Afficher":"Masquer")+"</button>"+
        "<button class='act off' onclick=\"supprimer('"+jsq(u.username)+"')\">Suppr</button>"+
      "</td>";
    tb.appendChild(tr);
  });
  document.getElementById("count").textContent = shown;
}

async function creer() {
  const u = document.getElementById("nu").value.trim();
  const p = document.getElementById("np").value;
  const r = document.getElementById("nr").value;
  const msg = document.getElementById("createmsg");
  if (!u || !p) { msg.innerHTML = "<span class='warn'>Nom et mot de passe requis.</span>"; return; }
  const res = await api("/admin/create", {new_username:u, new_password:p, role:r});
  if (res.ok) {
    msg.innerHTML = "<span class='ok'>Compte « "+esc(u)+" » créé ✅</span>";
    document.getElementById("nu").value=""; document.getElementById("np").value="";
    rafraichir();
  } else { msg.innerHTML = "<span class='off'>"+esc(res.error||"erreur")+"</span>"; }
}

async function voir(name) {
  const res = await api("/admin/get", {target:name});
  if (!res.ok) return;
  const logins = (res.logins||[]).slice(0,20).map(l => "  "+l.time+"  —  "+l.ip).join("\n") || "  (aucune)";
  const nx2098 = ((res.data||{}).nx2098||{});
  const nxcoin = ((res.data||{}).nxcoin||{});
  const nxInfo = "  Rewards : "+(nx2098.rewards||0)+"\n  NXC : "+(nxcoin.nxc||0);
  openModal("<h3>"+esc(name)+"</h3>"+
    "<div class='muted'>Rôle : "+esc(res.role)+(res.nickname?" · « "+esc(res.nickname)+" »":"")+"</div>"+
    "<b>◈ NXC Coin</b><pre>"+esc(nxInfo)+"</pre>"+
    "<b>Connexions (IP + heure)</b><pre>"+esc(logins)+"</pre>"+
    "<button class='accent' onclick='closeModal()'>Fermer</button>");
}

async function renommer(name) {
  const nn = prompt("Nouveau nom pour « "+name+" » :", name);
  if (!nn || !nn.trim()) return;
  const res = await api("/admin/rename", {target:name, new_username:nn.trim()});
  if (!res.ok) alert(res.error||"erreur"); rafraichir();
}
async function surnom(name) {
  const nk = prompt("Surnom pour « "+name+" » :", "");
  if (nk === null) return;
  await api("/admin/nickname", {target:name, nickname:nk}); rafraichir();
}
async function masquer(name, hide) {
  await api("/admin/hide", {target:name, hidden:hide}); rafraichir();
}
async function supprimer(name) {
  if (!confirm("Supprimer DÉFINITIVEMENT « "+name+" » ?")) return;
  await api("/admin/purge", {target:name}); rafraichir();
}

function openModal(html) {
  document.getElementById("modal").innerHTML =
    "<div class='overlay' onclick='if(event.target===this)closeModal()'><div class='modal'>"+html+"</div></div>";
}
function closeModal(){ document.getElementById("modal").innerHTML=""; }
function esc(s){ return (s+"").replace(/[&<>"]/g, c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c])); }
function jsq(s){ return (s+"").replace(/\\/g,"\\\\").replace(/'/g,"\\'"); }
document.getElementById("mk").addEventListener("keydown", e=>{ if(e.key==="Enter") connecter(); });
</script>
</body>
</html>"""


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
    try:
        with _lock:
            db = load_db()
            noah = db.get("users", {}).get("noah")
            if noah is None:
                return jsonify({"ok": False, "error": "Compte noah introuvable"})
            current = noah.get("data", {}).get("nxcoin_bank",
                {"reserves": 0, "nxcEmis": 0, "totalIn": 0, "totalOut": 0, "flux": []})
            # Regles anti-duplication :
            # On prend le totalIn et totalOut les plus GRANDS (jamais additionner)
            new_bank = {
                "reserves": float(incoming.get("reserves", current.get("reserves", 0))),
                "nxcEmis": float(incoming.get("nxcEmis", current.get("nxcEmis", 0))),
                "totalIn": max(float(incoming.get("totalIn", 0)), float(current.get("totalIn", 0))),
                "totalOut": max(float(incoming.get("totalOut", 0)), float(current.get("totalOut", 0))),
                "flux": incoming.get("flux", current.get("flux", []))
            }
            # Fusionner les flux sans doublons (par timestamp)
            existing_ts = {f.get("ts") for f in current.get("flux", [])}
            for f in incoming.get("flux", []):
                if f.get("ts") not in existing_ts:
                    new_bank["flux"].append(f)
                    existing_ts.add(f.get("ts"))
            new_bank["flux"] = sorted(new_bank["flux"], key=lambda x: x.get("ts", 0))[-200:]
            noah.setdefault("data", {})["nxcoin_bank"] = new_bank
            save_db(db)
        return jsonify({"ok": True, "bank": new_bank})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


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
