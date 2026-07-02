# -*- coding: utf-8 -*-
"""
================================================================================
  NEXUS SERVER (EN LIGNE)  —  durci + journal des connexions + synchro
================================================================================
  Le cerveau central permanent. Conçu pour tourner sur un hébergeur gratuit
  (Render / PythonAnywhere) et rester en ligne non-stop.

  Sécurité incluse :
   - mots de passe hachés (PBKDF2 + sel), jamais en clair ;
   - clé maîtresse lue dans une VARIABLE D'ENVIRONNEMENT (NEXUS_MASTER_KEY) ;
   - limitation anti-bruteforce (trop d'essais = blocage temporaire) ;
   - pas de mode debug ; endpoints admin verrouillés.
  (Aucun système n'est "inviolable", mais ça couvre les attaques courantes.)

  Synchro avec le serveur LOCAL : /admin/dump et /admin/merge (clé maîtresse).
  Journal : chaque connexion enregistre l'adresse IP et l'heure.

  LANCER EN LOCAL POUR TESTER : pip install flask ; python nexus_serveur.py
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

# Clé maîtresse : en ligne, définis la variable d'environnement NEXUS_MASTER_KEY.
MASTER_KEY = os.environ.get("NEXUS_MASTER_KEY", "change-moi-cle-maitre-nexus-2026")
PORT = int(os.environ.get("PORT", "8000"))   # Render fournit PORT automatiquement.

BASE = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE, "nexus_db.json")
_lock = threading.Lock()
app = Flask(__name__)

# Anti-bruteforce : IP -> liste d'horodatages récents.
_hits = defaultdict(list)
_RATE_MAX = 30        # max requêtes sensibles
_RATE_WINDOW = 60     # par minute


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

  <!-- CONNEXION -->
  <div id="login" class="card">
    <div class="row">
      <input id="mk" class="grow" type="password" placeholder="Clé maître">
      <button class="accent" onclick="connecter()">Se connecter</button>
    </div>
    <div id="loginmsg" class="muted" style="margin-top:8px"></div>
  </div>

  <!-- TABLEAU DE BORD -->
  <div id="dash" class="hidden">
    <div class="card">
      <div class="row">
        <div class="grow"><span id="status" class="ok">Connecté</span></div>
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
    msg.innerHTML = "<span class='ok'>Compte « "+esc(u)+" » créé ✅ (synchronisé partout)</span>";
    document.getElementById("nu").value=""; document.getElementById("np").value="";
    rafraichir();
  } else { msg.innerHTML = "<span class='off'>"+esc(res.error||"erreur")+"</span>"; }
}

async function voir(name) {
  const res = await api("/admin/get", {target:name});
  if (!res.ok) return;
  const logins = (res.logins||[]).slice(0,20).map(l => "  "+l.time+"  —  "+l.ip).join("\n") || "  (aucune)";
  const hist = ((res.data||{}).history||[]).slice(0,60).map(h => "  "+(h.titre||"")+" — "+(h.url||"")).join("\n") || "  (vide)";
  openModal("<h3>"+esc(name)+"</h3>"+
    "<div class='muted'>Rôle : "+esc(res.role)+(res.nickname?" · « "+esc(res.nickname)+" »":"")+"</div>"+
    "<b>Connexions (IP + heure)</b><pre>"+esc(logins)+"</pre>"+
    "<b>Historique</b><pre>"+esc(hist)+"</pre>"+
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
  if (!confirm("Supprimer définitivement « "+name+" » ?")) return;
  await api("/admin/delete", {target:name}); rafraichir();
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
    return (f"<body style='font-family:sans-serif;background:#0b0f17;color:#eaf0fb;"
            f"text-align:center;padding-top:60px'>"
            f"<h1 style='color:#5b9dff'>Nexus Server &#9989;</h1>"
            f"<p>En ligne — {n} compte(s), {a} admin(s).</p>"
            f"<p><a style='color:#a06bff' href='/panel'>Panneau d'administration &#8594;</a></p></body>")


@app.get("/panel")
def panel():
    return Response(ADMIN_HTML, mimetype="text/html")


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


# ------------------------------- ADMIN ------------------------------------- #
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
        save_db(db)
    return jsonify(ok=True, role=role)


# =============================== FORUM ===================================== #
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
        del msgs[:-500]          # on garde les 500 derniers messages
        save_db(db)
    return jsonify(ok=True)


@app.post("/forum/list")
def forum_list():
    db = load_db()
    return jsonify(ok=True, messages=db.get("forum", [])[-200:])


# ============================= EXTENSIONS ================================== #
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
    """Liste publique (pour les navigateurs) : extensions activées + leur code."""
    db = load_db()
    out = {n: e["code"] for n, e in db.get("extensions", {}).items() if e.get("enabled", True)}
    return jsonify(ok=True, extensions=out)


# ============================ CLOUD DE FICHIERS ============================ #
FILES_DIR = os.path.join(BASE, "nexus_files")
MAX_TOTAL = 100 * 1024 ** 3   # 100 Go (la vraie limite = l'espace du disque)


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


# --------------------- SYNCHRO avec le serveur LOCAL ----------------------- #
@app.post("/admin/dump")
def admin_dump():
    """Renvoie toute la base (pour que le serveur local récupère les données)."""
    d = request.get_json(force=True, silent=True) or {}
    db = load_db()
    if not admin_ok(d, db):
        return jsonify(ok=False, error="accès refusé")
    return jsonify(ok=True, db=db)


@app.post("/admin/merge")
def admin_merge():
    """Fusionne une base entrante : union des comptes, on garde le plus récent.
    Ne supprime jamais de compte -> aucune donnée perdue.
    Fusionne aussi le forum (par heure) et les extensions."""
    d = request.get_json(force=True, silent=True) or {}
    incoming = d.get("db") or {}
    with _lock:
        db = load_db()
        if not admin_ok(d, db):
            return jsonify(ok=False, error="accès refusé")
        for name, u in (incoming.get("users", {}) or {}).items():
            cur = db["users"].get(name)
            if not cur or u.get("updated", "") > cur.get("updated", ""):
                db["users"][name] = u
        # forum : union des messages (dédoublonnés par user+time+text)
        seen = {(m["user"], m["time"], m["text"]) for m in db.get("forum", [])}
        for m in incoming.get("forum", []) or []:
            key = (m.get("user"), m.get("time"), m.get("text"))
            if key not in seen:
                db.setdefault("forum", []).append(m); seen.add(key)
        db["forum"] = sorted(db.get("forum", []), key=lambda m: m.get("time", ""))[-500:]
        # extensions : on garde la plus récente
        for n, e in (incoming.get("extensions", {}) or {}).items():
            cur = db.setdefault("extensions", {}).get(n)
            if not cur or e.get("added", "") > cur.get("added", ""):
                db["extensions"][n] = e
        save_db(db)
        merged = db
    return jsonify(ok=True, db=merged)


if __name__ == "__main__":
    print("=" * 54)
    print("  NEXUS SERVER (en ligne)  —  http://127.0.0.1:%d" % PORT)
    print("  Clé maître :", MASTER_KEY)
    print("=" * 54)
    app.run(host="0.0.0.0", port=PORT)
