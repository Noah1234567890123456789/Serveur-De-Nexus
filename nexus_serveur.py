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

from flask import Flask, request, jsonify

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


@app.get("/")
def home():
    db = load_db()
    n = len(db["users"])
    a = sum(1 for u in db["users"].values() if u.get("role") == "admin")
    return (f"<body style='font-family:sans-serif;background:#0b0f17;color:#eaf0fb;"
            f"text-align:center;padding-top:60px'>"
            f"<h1 style='color:#5b9dff'>Nexus Server &#9989;</h1>"
            f"<p>En ligne — {n} compte(s), {a} admin(s).</p></body>")


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
    Ne supprime jamais de compte -> aucune donnée perdue."""
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
        save_db(db)
        merged = db
    return jsonify(ok=True, db=merged)


if __name__ == "__main__":
    print("=" * 54)
    print("  NEXUS SERVER (en ligne)  —  http://127.0.0.1:%d" % PORT)
    print("  Clé maître :", MASTER_KEY)
    print("=" * 54)
    app.run(host="0.0.0.0", port=PORT)
