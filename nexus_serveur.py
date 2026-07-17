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
            NXC_MARKET["ts"] = mkt.get("ts", 0)
    except Exception as e:
        pass  # Garder le prix par défaut

# Charger au démarrage (appelé après la définition des fonctions)


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
NXC_PANEL_HTML = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Nexus — NXC</title>
<style>
*{box-sizing:border-box;font-family:'Segoe UI',system-ui,sans-serif}
body{margin:0;background:#02040a;color:#d4e8ff}
.wrap{max-width:900px;margin:0 auto;padding:18px}
h1{font-size:20px;color:#00e5ff;margin:0 0 4px;letter-spacing:2px;font-family:monospace}
.muted{color:#5c6b8c;font-size:13px}
.card{background:#080d1a;border:1px solid rgba(0,229,255,.15);border-radius:14px;padding:16px;margin-top:14px}
input,button{font-size:14px;border-radius:9px;padding:10px 13px;border:1px solid rgba(0,229,255,.2);background:#0d1428;color:#d4e8ff;outline:none}
input:focus{border-color:#00e5ff}
button{cursor:pointer}
.accent{border:none;font-weight:700;color:#000;background:linear-gradient(90deg,#00e5ff,#a06bff)}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.grow{flex:1;min-width:120px}
.grid4{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:10px}
.stat{background:#0d1428;border:1px solid rgba(0,229,255,.1);border-radius:10px;padding:12px;text-align:center}
.sv{font-family:monospace;font-size:20px;font-weight:700;color:#00e5ff}
.sl{font-size:9px;color:#5c6b8c;letter-spacing:1px;margin-top:3px}
.sv.gold{color:#ffb020}.sv.red{color:#ff3d5e}.sv.green{color:#00ff9d}
canvas{width:100%!important;display:block}
table{width:100%;border-collapse:collapse;margin-top:8px;font-size:13px}
th,td{text-align:left;padding:8px 6px;border-bottom:1px solid #0d1428}
th{color:#5c6b8c;font-size:11px;text-transform:uppercase;letter-spacing:.5px}
.hidden{display:none}
.ok{color:#00ff9d}.warn{color:#ffb020}.off{color:#ff3d5e}
</style>
</head>
<body>
<div class="wrap">
  <h1>◈ NEXUS COIN — PANNEAU SERVEUR</h1>
  <div class="muted">Données de marché NXC partagées en temps réel entre tous les clients.</div>

  <div id="login" class="card">
    <div class="row">
      <input id="mk" class="grow" type="password" placeholder="Clé maître">
      <button class="accent" onclick="connecter()">Connexion</button>
    </div>
    <div id="lmsg" class="muted" style="margin-top:8px"></div>
  </div>

  <div id="dash" class="hidden">
    <div class="card">
      <div class="row">
        <b class="grow">◈ COURS ACTUEL</b>
        <span id="lastSync" class="muted"></span>
        <button onclick="rafraichir()">🔄 Actualiser</button>
        <button onclick="location.href='/panel'">🛡️ Admin</button>
        <button onclick="location.href='/nexus'">🌐 Nexus</button>
      </div>
      <div class="grid4" style="margin-top:12px">
        <div class="stat"><div class="sv" id="sPrice">—</div><div class="sl">PRIX (R/NXC)</div></div>
        <div class="stat"><div class="sv gold" id="sVol">—</div><div class="sl">VOLUME 24H</div></div>
        <div class="stat"><div class="sv green" id="sTrades">—</div><div class="sl">TRADES 24H</div></div>
        <div class="stat"><div class="sv" id="sHistory">—</div><div class="sl">POINTS HIST.</div></div>
      </div>
    </div>

    <div class="card">
      <b>◈ MODIFIER LE COURS</b>
      <div class="row" style="margin-top:10px">
        <input id="newPrice" class="grow" type="number" min="50" max="100000" placeholder="Nouveau prix (50 – 100 000)">
        <button class="accent" onclick="setPrice()">✓ Appliquer</button>
        <button onclick="resetHistory()">🔄 Reset historique</button>
      </div>
      <div id="pricemsg" class="muted" style="margin-top:6px"></div>
    </div>

    <div class="card">
      <b>◈ HISTORIQUE DU COURS (derniers 50 points)</b>
      <canvas id="chart" height="200" style="margin-top:10px"></canvas>
    </div>

    <div class="card">
      <b>◈ COMPTES UTILISATEURS — SOLDES NXC</b>
      <table>
        <thead><tr><th>Compte</th><th>Rewards (nx2098)</th><th>NXC</th><th>Valeur (R)</th></tr></thead>
        <tbody id="userTable"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
var KEY="";
var mktData={};

async function api(path,body){
  body=body||{};body.master_key=KEY;
  try{var r=await fetch(path,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});return await r.json();}
  catch(e){return{ok:false,error:"réseau"};}
}
async function getPrice(){
  var r=await fetch("/nxc/price");return await r.json();
}

async function connecter(){
  KEY=document.getElementById("mk").value.trim();
  var msg=document.getElementById("lmsg");
  msg.textContent="Connexion…";
  var r=await api("/admin/list");
  if(r&&r.ok){
    document.getElementById("login").classList.add("hidden");
    document.getElementById("dash").classList.remove("hidden");
    rafraichir();
    setInterval(rafraichir,5000);
  }else{
    msg.innerHTML="<span class='off'>Clé maître refusée.</span>";
  }
}

async function rafraichir(){
  // Prix NXC
  var r=await getPrice();
  if(r&&r.ok){
    mktData=r;
    document.getElementById("sPrice").textContent=fmt(r.price);
    document.getElementById("sVol").textContent=fmt(r.volume24);
    document.getElementById("sTrades").textContent=r.trades24||0;
    document.getElementById("sHistory").textContent=(r.history||[]).length;
    document.getElementById("lastSync").textContent="màj: "+new Date().toLocaleTimeString();
    drawChart(r.history||[]);
  }
  // Comptes
  var u=await api("/admin/list");
  if(u&&u.ok){
    var tb=document.getElementById("userTable");
    tb.innerHTML="";
    var price=r?parseFloat(r.price):5213;
    var promises=(u.users||[]).map(function(usr){return api("/admin/get",{target:usr.username});});
    var details=await Promise.all(promises);
    details.forEach(function(d,i){
      if(!d||!d.ok)return;
      var rew=(d.data&&d.data.nx2098&&d.data.nx2098.rewards)||0;
      var nxc=(d.data&&d.data.nxcoin&&parseFloat(d.data.nxcoin.nxc))||0;
      var val=(nxc*price).toFixed(0);
      var tr=document.createElement("tr");
      tr.innerHTML="<td><b>"+esc(d.username||"")+"</b>"+(d.role==="admin"?" 👑":"")+"</td>"
        +"<td style='color:#ffb020'>"+fmt(rew)+" R</td>"
        +"<td style='color:#00e5ff'>"+nxc.toFixed(4)+" NXC</td>"
        +"<td style='color:#a06bff'>"+fmt(val)+" R</td>";
      tb.appendChild(tr);
    });
  }
}

async function setPrice(){
  var p=parseFloat(document.getElementById("newPrice").value);
  if(!p||p<50||p>100000){document.getElementById("pricemsg").innerHTML="<span class='warn'>Prix entre 50 et 100 000</span>";return;}
  var r=await fetch("/nxc/tick",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:0,volume24:mktData.volume24||0,trades24:mktData.trades24||0})});
  var res=await r.json();
  if(res&&res.ok){document.getElementById("pricemsg").innerHTML="<span class='ok'>✅ Cours mis à jour : "+fmt(p)+" R/NXC</span>";rafraichir();}
  else document.getElementById("pricemsg").innerHTML="<span class='off'>Erreur</span>";
}

async function resetHistory(){
  if(!confirm("Réinitialiser l'historique du cours ?"))return;
  await fetch("/nxc/reset",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({master_key:KEY})});
  rafraichir();
}

function drawChart(history){
  var cv=document.getElementById("chart");
  if(!cv)return;
  var ctx=cv.getContext("2d");
  var w=cv.offsetWidth||600,h=200;
  cv.width=w;cv.height=h;
  ctx.clearRect(0,0,w,h);
  if(!history.length)return;
  var pts=history.slice(-50);
  var prices=pts.map(function(p){return parseFloat(p.price)||0;});
  var mn=Math.min.apply(null,prices)*0.998,mx=Math.max.apply(null,prices)*1.002;
  var pad=30;
  function px(i){return pad+(i/(pts.length-1||1))*(w-pad*2);}
  function py(v){return h-pad-(v-mn)/(mx-mn||1)*(h-pad*2);}
  // Grille
  ctx.strokeStyle="rgba(0,229,255,.06)";ctx.lineWidth=1;
  for(var i=0;i<5;i++){var y=pad+i*(h-pad*2)/4;ctx.beginPath();ctx.moveTo(pad,y);ctx.lineTo(w-pad,y);ctx.stroke();}
  // Courbe
  var grad=ctx.createLinearGradient(0,0,0,h);
  grad.addColorStop(0,"rgba(0,229,255,.3)");grad.addColorStop(1,"rgba(0,229,255,0)");
  ctx.beginPath();
  pts.forEach(function(p,i){i?ctx.lineTo(px(i),py(p.price)):ctx.moveTo(px(i),py(p.price));});
  ctx.lineTo(px(pts.length-1),h);ctx.lineTo(px(0),h);ctx.closePath();
  ctx.fillStyle=grad;ctx.fill();
  ctx.beginPath();
  pts.forEach(function(p,i){i?ctx.lineTo(px(i),py(p.price)):ctx.moveTo(px(i),py(p.price));});
  ctx.strokeStyle="#00e5ff";ctx.lineWidth=2;ctx.stroke();
  // Prix
  ctx.fillStyle="#00e5ff";ctx.font="11px monospace";ctx.textAlign="right";
  ctx.fillText(fmt(mx),w-4,pad+8);ctx.fillText(fmt(mn),w-4,h-pad);
  var last=prices[prices.length-1];
  ctx.fillStyle="#00ff9d";ctx.textAlign="left";
  ctx.fillText(fmt(last)+" R",px(pts.length-1)+4,py(last)-4);
}

function fmt(n){return Number(n||0).toLocaleString("fr-FR",{maximumFractionDigits:2});}
function esc(s){return (s+"").replace(/[&<>"]/g,function(c){return{"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c];});}

document.getElementById("mk").addEventListener("keydown",function(e){if(e.key==="Enter")connecter();});
</script>
</body>
</html>"""

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
    """Prix NXC en temps réel — accessible par tous sans auth."""
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
