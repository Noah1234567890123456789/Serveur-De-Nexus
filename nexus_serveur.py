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

from flask import Flask, request, jsonify, send_file, Response, g

MASTER_KEY = os.environ.get("NEXUS_MASTER_KEY", "change-moi-cle-maitre-nexus-2026")
PORT = int(os.environ.get("PORT", "8000"))

# ══ UPSTASH REDIS — stockage persistant (survit aux redémarrages Render) ══
# Créer un DB gratuit sur upstash.com → copier REST URL et token dans les env vars Render
_UPSTASH_URL   = os.environ.get("UPSTASH_URL", "").rstrip("/")
_UPSTASH_TOKEN = os.environ.get("UPSTASH_TOKEN", "")
_UPSTASH_KEY   = "nexus_db_v2"

BASE = os.path.dirname(os.path.abspath(__file__))
# Ordre de priorité : /data (disque persistant Render payant) → BASE (dossier du
# script, PERSISTANT en local) → /tmp (dernier recours, EFFACÉ au redémarrage).
# ⚠️ /tmp était prioritaire avant : c'est pour ça que les points revenaient en
# arrière au redémarrage du serveur local. BASE passe désormais avant /tmp.
def _pick_data_dir():
    for d in ("/data", BASE, "/tmp"):
        try:
            test = os.path.join(d, ".write_test")
            with open(test, "w") as f:
                f.write("ok")
            os.remove(test)
            return d
        except Exception:
            continue
    return BASE
_DATA_DIR = _pick_data_dir()
DB_FILE = os.path.join(_DATA_DIR, "nexus_db.json")
_lock = threading.Lock()
app = Flask(__name__)

# ══ REQUEST LOGGING (monitor panel) ══
_req_log = []
_server_start = time.time()

@app.before_request
def _log_start():
    g._t = time.time()


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

# ══ CROISSANCE PROGRAMMÉE DU PRIX ══
# Permet de faire monter (ou descendre) le prix à un rythme précis choisi par
# l'admin : de 0,01 %/jour jusqu'à 100 %/seconde, dans les deux sens.
#   enabled       : croissance active ou non
#   rate_per_sec  : taux fractionnaire composé PAR SECONDE
#                   (ex : 0.001 = +0,1 %/s ; -0.0005 = -0,05 %/s)
#   combine       : True  = la croissance s'ajoute AUX variations normales du
#                           cours (le prix fluctue ET grimpe/descend)
#                   False = trajectoire pure (croissance seule, sans bruit)
NXC_GROWTH = {
    "enabled": False,
    "rate_per_sec": 0.0,
    "combine": True
}

# ══ BORNES DU PRIX (plancher / plafond) ══
#   auto=True  → plancher -30 % / plafond +150 % de la référence (admin_price)
#   auto=False → min/max ABSOLUS fixés à la main par l'admin
NXC_BOUNDS = {
    "auto": True,
    "min": 0.0,
    "max": 0.0
}

# ══ FRÉQUENCE DES EXTRÊMES ══
# Probabilité, à CHAQUE tick, que le prix touche son minimum (pmin) ou son
# maximum (pmax). Calculée par le panneau « Fréquence des extrêmes » à partir
# du nombre de fois voulu par minute/heure/jour/semaine/mois/an.
NXC_EXTREMES = {
    "pmin": 0.0,
    "pmax": 0.0
}

# ══ GROS MOUVEMENTS RARES (chocs) ══
# per_day : nombre de gros sauts par jour (rare). amp : ampleur (0.20 = ±20 %).
# La variation NORMALE reste douce (~±0,8 %/tick) ; ces chocs sont l'exception.
NXC_SHOCK = {
    "per_day": 2.0,
    "amp": 0.20
}
_NXC_TICKS_PER_DAY = 21600  # ticks de 4 s par 24 h

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
    "admin_price": 5213,   # prix de référence — le marché oscille autour
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
            NXC_MARKET["admin_price"] = float(mkt.get("base_price") or mkt["price"])
            NXC_MARKET["history"] = mkt.get("history", [])[-8000:]
            NXC_MARKET["volume24"] = mkt.get("volume24", 0)
            NXC_MARKET["trades24"] = mkt.get("trades24", 0)
            # Mettre ts = maintenant pour eviter le rattrapage au redemarrage
            NXC_MARKET["ts"] = int(time.time() * 1000)
    except Exception as e:
        pass  # Garder le prix par défaut

# Charger au démarrage (appelé après la définition des fonctions)

import random as _rnd

# ══════════════════════════════════════════════════════════════════════════
#  PARTAGE ENTRE WORKERS — rend le fichier .py UNIQUE robuste au multi-worker
#  (aucun Procfile ni réglage Render nécessaire).
#
#  Tous les workers gunicorn partagent le même système de fichiers. On élit UN
#  seul worker « moteur » (leader) qui fait avancer le prix ; le prix, la
#  référence et TOUS les réglages (croissance, vitesse, bornes, extrêmes...)
#  transitent par un petit fichier partagé. Résultat : Croissance/Vitesse/
#  Extrêmes agissent quel que soit le worker qui reçoit le clic, et tout le
#  monde voit le même prix.
# ══════════════════════════════════════════════════════════════════════════
NXC_SHARED = os.path.join(_DATA_DIR, "nxc_shared.json")
NXC_LOCK   = os.path.join(_DATA_DIR, "nxc_leader.lock")

def _read_shared():
    try:
        with open(NXC_SHARED, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _write_shared(d):
    try:
        tmp = NXC_SHARED + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f)
        os.replace(tmp, NXC_SHARED)
    except Exception:
        pass

def _push_control(key, value):
    """Un worker publie un réglage pour le worker-moteur (leader)."""
    try:
        with _lock:
            d = _read_shared()
            d.setdefault("controls", {})[key] = value
            _write_shared(d)
    except Exception:
        pass

def _push_price_ref(price, admin_price):
    """Un worker publie un cours/référence fixé par l'admin (bouton Fixer)."""
    try:
        with _lock:
            d = _read_shared()
            d["price"] = float(price)
            d["admin_price"] = float(admin_price)
            d["ts"] = int(time.time() * 1000)
            _write_shared(d)
    except Exception:
        pass

def _is_leader():
    """Élection de leader par fichier-verrou + battement de cœur (15 s)."""
    now = time.time()
    try:
        cur = None
        if os.path.exists(NXC_LOCK):
            try:
                with open(NXC_LOCK, "r") as f:
                    cur = json.load(f)
            except Exception:
                cur = None
        if cur and cur.get("pid") != os.getpid() and (now - cur.get("hb", 0)) < 15:
            return False
        with open(NXC_LOCK, "w") as f:
            json.dump({"pid": os.getpid(), "hb": now}, f)
        with open(NXC_LOCK, "r") as f:
            back = json.load(f)
        return back.get("pid") == os.getpid()
    except Exception:
        return True  # mode solo (app.run) : toujours leader

def _apply_shared_controls(sh):
    """Le leader récupère les réglages publiés par les autres workers."""
    ctl = (sh.get("controls") or {})
    if "growth" in ctl:     NXC_GROWTH.update(ctl["growth"])
    if "volatility" in ctl: NXC_VOLATILITY_MULT.update(ctl["volatility"])
    if "bounds" in ctl:     NXC_BOUNDS.update(ctl["bounds"])
    if "extremes" in ctl:   NXC_EXTREMES.update(ctl["extremes"])
    if "shock" in ctl:      NXC_SHOCK.update(ctl["shock"])
    if "bias" in ctl:       NXC_BIAS.update(ctl["bias"])

def _nxc_autotick():
    """Moteur de prix NXC — oscillateur à retour à la moyenne (mean-reversion).

    Principe :
      • Le prix oscille AUTOUR du prix de référence admin (admin_price).
      • Variations habituelles ~1 % par tick (bruit gaussien).
      • Chocs rares (~3 % des ticks) pouvant aller jusqu'à ±30 %.
      • Rappel doux vers la référence → le prix monte ET descend, mais ne
        s'effondre jamais éternellement (investir a du sens).
      • Plancher dur : jamais sous 70 % de la référence (anti-chute -30 %).
      • Plafond de sécurité : jamais au-dessus de 250 % de la référence.
    Un seul moteur = une seule source de vérité, aucun conflit de timers.
    """
    while True:
        try:
            time.sleep(4)
            # ── Partage multi-worker ──
            if not _is_leader():
                # Pas leader : suivre le prix du worker-moteur (cohérence)
                sh = _read_shared()
                if sh.get("price", 0) > 0:
                    NXC_MARKET["price"] = float(sh["price"])
                    NXC_MARKET["ts"] = int(sh.get("ts") or time.time() * 1000)
                    if sh.get("admin_price"):
                        NXC_MARKET["admin_price"] = float(sh["admin_price"])
                    if sh.get("history"):
                        NXC_MARKET["history"] = sh["history"]
                continue
            # Leader : récupérer les réglages + le cours partagés
            sh = _read_shared()
            _apply_shared_controls(sh)
            p = float(sh.get("price") or NXC_MARKET["price"])
            ref = float(sh.get("admin_price") or NXC_MARKET.get("admin_price") or p)
            volt = NXC_VOLATILITY_MULT.get("value", 1.0)

            # ── Croissance programmée (facteur composé sur les 4 s du tick) ──
            _g = NXC_GROWTH
            _grow_on = bool(_g.get("enabled")) and float(_g.get("rate_per_sec") or 0) != 0
            _grow_factor = (1.0 + float(_g.get("rate_per_sec", 0.0))) ** 4 if _grow_on else 1.0

            if _grow_on and not _g.get("combine", True):
                # Trajectoire pure : uniquement la croissance, aucun bruit
                p = p * _grow_factor
                ref = ref * _grow_factor
            else:
                # 1) Bruit de base — variation NORMALE douce (~±0,8 %/tick)
                sigma = 0.008 * volt
                noise = _rnd.gauss(0.0, 1.0) * sigma
                # 2) GROS choc RARE — par défaut ~2 fois/jour, ampleur ±20 %
                shock = 0.0
                _shock_prob = float(NXC_SHOCK.get("per_day", 2.0)) / _NXC_TICKS_PER_DAY
                if _rnd.random() < _shock_prob:
                    _amp = float(NXC_SHOCK.get("amp", 0.20)) * volt
                    shock = _amp if _rnd.random() < 0.5 else -_amp
                # 3) Rappel vers la référence (mean-reversion) anti-dérive
                mr = 0.0
                if ref > 0:
                    mr = ((ref - p) / p) * 0.06                     # 6 % de l'écart/tick
                # 4) Biais directionnel optionnel (panel Contrôle → tendance)
                drift_adj = NXC_BIAS.get("drift", 0) * 0.02 * NXC_BIAS.get("speed", 1.0)
                p = p * (1 + noise + shock + mr + drift_adj)
                # Croissance appliquée PAR-DESSUS l'oscillation (option combine)
                if _grow_on:
                    p = p * _grow_factor
                    ref = ref * _grow_factor

            # La référence suit la croissance → le plancher -30 % monte/descend avec
            if _grow_on and ref > 0:
                NXC_MARKET["admin_price"] = round(ref, 2)

            # 5) Bornes : auto (-30 %/+150 % de la réf) OU min/max manuels
            if NXC_BOUNDS.get("auto", True) or not (NXC_BOUNDS.get("min") or NXC_BOUNDS.get("max")):
                lo = ref * 0.70 if ref > 0 else _NXC_PRICE_MIN
                hi = ref * 2.5 if ref > 0 else _NXC_PRICE_MAX
            else:
                lo = float(NXC_BOUNDS.get("min") or _NXC_PRICE_MIN)
                hi = float(NXC_BOUNDS.get("max") or _NXC_PRICE_MAX)
            if hi <= lo:
                hi = lo * 1.5 + 1
            # 6) Fréquence des extrêmes : chance de toucher le min/max ce tick
            _pmax = float(NXC_EXTREMES.get("pmax", 0) or 0)
            _pmin = float(NXC_EXTREMES.get("pmin", 0) or 0)
            _rr = _rnd.random()
            if _pmax > 0 and _rr < _pmax:
                p = hi
            elif _pmin > 0 and _rr < _pmax + _pmin:
                p = lo
            p = max(lo, min(hi, p))
            p = max(_NXC_PRICE_MIN, min(_NXC_PRICE_MAX, p))     # garde-fous absolus
            p = round(p * 100) / 100

            NXC_MARKET["price"] = p
            NXC_MARKET["ts"] = int(time.time() * 1000)
            NXC_MARKET["history"].append({"price": p, "ts": NXC_MARKET["ts"],
                                          "vol": int(_rnd.random() * 800 + 30)})
            if len(NXC_MARKET["history"]) > 8000:
                NXC_MARKET["history"] = NXC_MARKET["history"][-8000:]
            # Publier le cours pour les autres workers (en gardant leurs réglages)
            sh["price"] = p
            sh["admin_price"] = NXC_MARKET.get("admin_price", p)
            sh["ts"] = NXC_MARKET["ts"]
            sh["history"] = NXC_MARKET["history"][-2000:]
            _write_shared(sh)
            if len(NXC_MARKET["history"]) % 8 == 0:
                with _lock:
                    db = load_db()
                    noah = db.get("users", {}).get("noah")
                    if noah is not None:
                        noah.setdefault("data", {})["nxcoin_market"] = {
                            "price": p, "history": NXC_MARKET["history"][-4000:],
                            "volume24": NXC_MARKET["volume24"],
                            "trades24": NXC_MARKET["trades24"],
                            "ts": NXC_MARKET["ts"],
                            "base_price": NXC_MARKET.get("admin_price", p)}
                        save_db(db)
        except Exception:
            pass

_tick_started = False
_tick_lock = threading.Lock()

def _ensure_tick():
    """Démarre le thread de prix NXC si pas encore démarré."""
    global _tick_started
    if _tick_started:
        return
    with _tick_lock:
        if not _tick_started:
            threading.Thread(target=_nxc_autotick, daemon=True).start()
            _tick_started = True

_ensure_tick()  # démarrer le thread au lancement du serveur

# ══ AUTO-KEEP-ALIVE : ping toutes les 60s pour rester éveillé sur Render ══
# NOTE : lancé via before_request pour survivre au fork Gunicorn multi-worker
_ping_started = False
_ping_lock = threading.Lock()

def _self_ping_loop():
    import urllib.request as _ur
    import time as _t
    _t.sleep(20)  # attendre que le serveur soit prêt
    ext_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    loc_url = "http://127.0.0.1:" + str(PORT)
    urls = []
    if ext_url:
        urls.append(ext_url + "/ping")
    urls.append(loc_url + "/ping")
    while True:
        for u in urls:
            try:
                _ur.urlopen(u, timeout=10)
                break  # succès sur l'un → pas besoin d'essayer les autres
            except Exception:
                pass
        _t.sleep(60)  # toutes les 60 secondes

def _ensure_ping():
    global _ping_started
    if _ping_started:
        return
    with _ping_lock:
        if not _ping_started:
            _ping_started = True
            import threading as _th2
            _th2.Thread(target=_self_ping_loop, daemon=True).start()


# Restaurer le prix NXC au démarrage (Gunicorn + local)
try:
    _load_nxc_from_db()
except Exception:
    pass

# Anti-dérive automatique : la mean reversion est activée dès le démarrage
# pour neutraliser le biais haussier naturel de l'autotick (+0.02*sigma par tick).
# L'admin peut toujours la désactiver / changer la cible depuis le panel Contrôle.
NXC_MEAN_PRICE["enabled"] = False  # désactivée — la cible suit le prix admin

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
    try:
        elapsed = round((time.time() - g._t) * 1000)
        ip = (request.headers.get("X-Forwarded-For","").split(",")[0].strip() or request.remote_addr or "?")
        _req_log.append({"ts": int(time.time()), "method": request.method, "path": request.path,
                          "status": resp.status_code, "ms": elapsed, "ip": ip})
        if len(_req_log) > 500:
            _req_log.pop(0)
    except Exception:
        pass
    return resp

_hits = defaultdict(list)
_RATE_MAX = 200
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

_BACKUP_FILE = DB_FILE + ".bak"

# ── Upstash helpers ────────────────────────────────────────────────────────────
def _upstash_get():
    """Charge la DB depuis Upstash Redis via REST API (ne lève jamais d'exception)."""
    if not (_UPSTASH_URL and _UPSTASH_TOKEN):
        return None
    import urllib.request as _ur
    try:
        cmd = json.dumps(["GET", _UPSTASH_KEY]).encode()
        req = _ur.Request(
            _UPSTASH_URL,
            data=cmd,
            headers={"Authorization": "Bearer " + _UPSTASH_TOKEN,
                     "Content-Type": "application/json"}
        )
        resp = _ur.urlopen(req, timeout=12)
        result = json.loads(resp.read()).get("result")
        if result:
            data = json.loads(result)
            if isinstance(data, dict) and "users" in data:
                return data
    except Exception:
        pass
    return None

def _upstash_set(db):
    """Sauvegarde la DB dans Upstash Redis (appelé dans un thread, ne bloque pas)."""
    if not (_UPSTASH_URL and _UPSTASH_TOKEN):
        return
    import urllib.request as _ur
    try:
        val = json.dumps(db, ensure_ascii=False)
        cmd = json.dumps(["SET", _UPSTASH_KEY, val]).encode()
        req = _ur.Request(
            _UPSTASH_URL,
            data=cmd,
            headers={"Authorization": "Bearer " + _UPSTASH_TOKEN,
                     "Content-Type": "application/json"}
        )
        _ur.urlopen(req, timeout=15)
    except Exception:
        pass

# ══ CACHE MÉMOIRE = SOURCE DE VÉRITÉ (corrige la perte de points) ══
# Avant, load_db() relisait Upstash à CHAQUE appel. Comme save_db() écrit sur
# Upstash en asynchrone (avec un léger délai), un thread de fond (autotick) qui
# faisait load_db()→save_db() juste après un ajout de points relisait une version
# PÉRIMÉE d'Upstash et réécrasait les points. Résultat : points qui reviennent en
# arrière au rafraîchissement.
#
# Solution : la DB vit en mémoire (_DB_CACHE). On lit Upstash/fichier UNE SEULE
# fois au démarrage. Ensuite load_db() rend le cache (jamais périmé) et save_db()
# écrit à travers (mémoire → fichier → Upstash async). Plus aucune course.
# NB : conçu pour tourner avec 1 worker (comme NXC_MARKET, déjà en mémoire).
_DB_CACHE = None

def _db_load_source():
    """Charge la DB depuis Upstash (prioritaire) ou le fichier local — au démarrage seulement."""
    data = _upstash_get()
    if data:
        try:
            with open(DB_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
        return data
    for path in (DB_FILE, _BACKUP_FILE):
        try:
            with open(path, "r", encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, dict) and "users" in d:
                return d
        except Exception:
            pass
    return {"users": {}}

_DB_CACHE_MTIME = 0.0

def load_db():
    """Cache mémoire, mais relit le fichier partagé s'il a changé (multi-worker safe).

    Le fichier local nexus_db.json est partagé par tous les workers gunicorn du
    même conteneur. En surveillant sa date de modification, chaque worker voit
    immédiatement les écritures des autres — tout en évitant de relire Upstash
    (asynchrone, source de la course qui effaçait points/comptes)."""
    global _DB_CACHE, _DB_CACHE_MTIME
    try:
        mt = os.path.getmtime(DB_FILE)
    except Exception:
        mt = 0.0
    if _DB_CACHE is None:
        _DB_CACHE = _db_load_source()
        try:
            _DB_CACHE_MTIME = os.path.getmtime(DB_FILE)
        except Exception:
            _DB_CACHE_MTIME = mt
    elif mt > _DB_CACHE_MTIME:
        # Un autre worker a écrit → recharger depuis le fichier partagé
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, dict) and "users" in d:
                _DB_CACHE = d
                _DB_CACHE_MTIME = mt
        except Exception:
            pass
    return _DB_CACHE

def save_db(db):
    global _DB_CACHE, _DB_CACHE_MTIME
    _DB_CACHE = db
    # 1. Fichier local (avec backup atomique)
    try:
        tmp = DB_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(db, f, ensure_ascii=False, indent=2)
        os.replace(tmp, DB_FILE)
        try:
            _DB_CACHE_MTIME = os.path.getmtime(DB_FILE)
        except Exception:
            pass
        try:
            import shutil
            shutil.copy2(DB_FILE, _BACKUP_FILE)
        except Exception:
            pass
    except Exception:
        pass
    # 2. Upstash (async — ne bloque pas la réponse HTTP)
    threading.Thread(target=_upstash_set, args=(db,), daemon=True).start()

def hash_pw(pw, salt):
    return hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"), bytes.fromhex(salt), 200_000).hex()

def make_user(pw, role):
    salt = secrets.token_hex(16)
    return {"role": role, "salt": salt, "pass_hash": hash_pw(pw, salt),
            "nickname": "", "hidden": False, "data": {}, "logins": [],
            "created": now_iso(), "updated": now_iso()}

def check(db, u, p):
    try:
        x = db["users"].get(u)
        if not x or not x.get("salt") or not x.get("pass_hash"):
            return False
        return secrets.compare_digest(x["pass_hash"], hash_pw(p, x["salt"]))
    except Exception:
        return False

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
NXC_PANEL_HTML = '<!DOCTYPE html>\n<html lang="fr">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">\n<title>◈ Nexus</title>\n<style>\n*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent;touch-action:manipulation}\n:root{--bg:#02040a;--bg2:#080d1a;--bg3:#0d1428;--cyan:#00e5ff;--green:#00ff9d;--red:#ff3d5e;--gold:#ffb020;--purple:#a06bff;--muted:#5c6b8c;--text:#d4e8ff;--fg:#d4e8ff;\n.active-btn { background: var(--cyan) !important; color: #000 !important; font-weight: 700; }\n.dd-item:hover { background: var(--bg3); color: var(--cyan); }\n.card { animation: fadeInCard 0.18s ease; }\n@keyframes fadeInCard { from { opacity:0; transform:translateY(6px); } to { opacity:1; transform:none; } }\n--border:rgba(0,229,255,.12)}/* THEMES INTERFACE (onglet Parametre) — bleu Nexus conserve dans tous */\nbody[data-theme="terminal"]{--bg:#00060a;--bg2:#001016;--bg3:#00202c;--text:#c9fff0;--muted:#4f8f86;font-family:"Courier New",monospace}\nbody[data-theme="terminal"] .card{border-radius:4px;border:1px solid rgba(0,229,255,.25)}\nbody[data-theme="terminal"] .ct{text-shadow:0 0 6px var(--cyan)}\nbody[data-theme="terminal"]::after{content:"";position:fixed;inset:0;pointer-events:none;z-index:9998;background:repeating-linear-gradient(rgba(0,229,255,.045) 0 1px,transparent 1px 3px)}\nbody[data-theme="glass"]{--bg:#060b18;--bg2:#0a1428;--bg3:#0f1c38}\nbody[data-theme="glass"] .card{background:rgba(20,32,64,.5);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);border:1px solid rgba(180,220,255,.14);border-radius:18px;box-shadow:0 8px 30px rgba(0,0,0,.35)}\nbody[data-theme="bloomberg"]{--bg:#080b10;--bg2:#0c1016;--bg3:#12181f;font-size:12.5px;font-family:"Consolas","Courier New",monospace}\nbody[data-theme="bloomberg"] .card{border-radius:2px;padding:9px;border:1px solid rgba(255,176,32,.18)}\nbody[data-theme="bloomberg"] .ct{color:var(--gold);letter-spacing:.5px}\nbody[data-theme="neon"]{--bg:#04030f;--bg2:#0a0820;--bg3:#100c2e}\nbody[data-theme="neon"]::before{content:"";position:fixed;inset:0;pointer-events:none;z-index:0;background:linear-gradient(rgba(0,229,255,.06) 1px,transparent 1px),linear-gradient(90deg,rgba(0,229,255,.06) 1px,transparent 1px);background-size:34px 34px}\nbody[data-theme="neon"] .card{border:1px solid var(--cyan);box-shadow:0 0 14px rgba(0,229,255,.3),inset 0 0 10px rgba(0,229,255,.06);border-radius:12px}\nbody[data-theme="neon"] .ct{text-shadow:0 0 8px var(--cyan),0 0 16px var(--cyan)}\nbody[data-theme="minimal"]{--bg:#0b0f1a;--bg2:#111725;--bg3:#171f31;--border:rgba(255,255,255,.08)}\nbody[data-theme="minimal"] .card{border-radius:14px;box-shadow:none;border:1px solid rgba(255,255,255,.07);padding:18px}\n.style-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px}\n.style-card{border:1px solid var(--border);border-radius:12px;padding:14px;cursor:pointer;transition:.15s;background:var(--bg3)}\n.style-card:hover{border-color:var(--cyan);transform:translateY(-2px)}\n.style-card.sel{border-color:var(--cyan);box-shadow:0 0 0 2px rgba(0,229,255,.3)}\n.style-name{font-weight:700;font-size:14px;margin-bottom:4px}\n.style-desc{font-size:11px;color:var(--muted);line-height:1.4}\n.style-swatch{height:38px;border-radius:8px;margin-bottom:10px;border:1px solid rgba(255,255,255,.1)}body.no-anim *{animation:none!important;transition:none!important}\nbody.no-anim .ct,body.no-anim .hud-logo{text-shadow:none!important}\nbody.no-round .card,body.no-round .btn,body.no-round .st,body.no-round input,body.no-round select,body.no-round .style-card{border-radius:0!important}\nbody.compact .card{padding:8px!important}\nbody.compact .g4{gap:6px!important}\nbody.compact .sec{margin:6px 0 4px!important}\nbody.hide-hud .hud{display:none!important}\nbody.hide-more #btn-more{display:none!important}\n#screensaver{position:fixed;inset:0;background:rgba(2,4,10,.94);z-index:99999;display:none;flex-direction:column;align-items:center;justify-content:center;color:var(--cyan);cursor:pointer}\n#screensaver.on{display:flex}\n#ss-clock{font-size:min(18vw,120px);font-weight:800;font-family:monospace;letter-spacing:4px;text-shadow:0 0 22px var(--cyan)}\n#ss-sub{font-size:13px;color:var(--muted);margin-top:14px}\n.opt-row{display:flex;align-items:center;justify-content:space-between;padding:9px 0;border-bottom:1px solid var(--border)}\n.opt-row label:first-child{font-size:13px;cursor:pointer;flex:1}\n.switch{position:relative;width:44px;height:24px;flex-shrink:0}\n.switch input{opacity:0;width:0;height:0}\n.switch span{position:absolute;inset:0;background:var(--bg3);border:1px solid var(--border);border-radius:24px;transition:.2s;cursor:pointer}\n.switch span:before{content:"";position:absolute;height:16px;width:16px;left:3px;top:3px;background:var(--muted);border-radius:50%;transition:.2s}\n.switch input:checked+span{background:rgba(0,229,255,.3);border-color:var(--cyan)}\n.switch input:checked+span:before{transform:translateX(20px);background:var(--cyan)}#bg-layer{position:fixed;inset:0;z-index:-2;pointer-events:none;overflow:hidden}\n#bg-canvas{position:fixed;inset:0;z-index:-1;pointer-events:none}\n.bg-grid{background:linear-gradient(rgba(0,229,255,.05) 1px,transparent 1px),linear-gradient(90deg,rgba(0,229,255,.05) 1px,transparent 1px);background-size:42px 42px;animation:bgGridMove 9s linear infinite}\n@keyframes bgGridMove{from{background-position:0 0}to{background-position:42px 42px}}\n.bg-gradient{background:linear-gradient(-45deg,#02040a,#0a1428,#06122a,#050a1e);background-size:400% 400%;animation:bgGrad 20s ease infinite}\n@keyframes bgGrad{0%{background-position:0% 50%}50%{background-position:100% 50%}100%{background-position:0% 50%}}\nbody.hud-bottom .hud{top:auto!important;bottom:0!important}\n#toast-wrap{position:fixed;top:58px;right:12px;z-index:99990;display:flex;flex-direction:column;gap:8px;pointer-events:none;max-width:70vw}\n.toast{background:var(--bg3);border:1px solid var(--cyan);border-radius:10px;padding:10px 14px;font-size:12px;font-weight:600;color:var(--text);box-shadow:0 6px 20px rgba(0,0,0,.45);animation:toastIn .3s}\n@keyframes toastIn{from{transform:translateX(40px);opacity:0}to{transform:none;opacity:1}}\n@media(max-width:640px){.style-grid{grid-template-columns:repeat(2,1fr)}}\nhtml,body{background:var(--bg);color:var(--text);font-family:\'Segoe UI\',system-ui,sans-serif;min-height:100dvh;overflow-x:hidden;-webkit-text-size-adjust:100%}\n\n/* LOGIN */\n#ls{position:fixed;inset:0;background:var(--bg);z-index:9999;display:flex;align-items:center;justify-content:center;padding:20px}\n.lb{background:var(--bg2);border:1px solid var(--border);border-radius:22px;padding:32px 24px;width:100%;max-width:340px;text-align:center;box-shadow:0 24px 80px rgba(0,0,0,.6)}\n.lb-logo{font-family:monospace;font-size:30px;font-weight:900;color:var(--cyan);letter-spacing:4px;margin-bottom:4px;text-shadow:0 0 20px rgba(0,229,255,.4)}\n.lb-sub{font-size:10px;color:var(--muted);margin-bottom:24px;letter-spacing:3px;text-transform:uppercase}\n.fi{width:100%;padding:13px 16px;background:var(--bg3);border:1px solid var(--border);border-radius:12px;color:var(--text);font-size:16px;margin-bottom:10px;outline:none}\n.fi:focus{border-color:var(--cyan)}\n.btn-login{width:100%;padding:14px;border-radius:12px;font-size:15px;font-weight:800;cursor:pointer;border:none;background:linear-gradient(135deg,var(--cyan),#0097b2);color:#000;letter-spacing:.5px}\n#lm{font-size:12px;color:var(--red);margin-top:8px;min-height:16px}\n\n/* HUD */\n.hud{position:fixed;top:0;left:0;right:0;height:52px;background:rgba(2,4,10,.97);border-bottom:1px solid var(--border);display:flex;align-items:center;padding:0 14px;gap:10px;z-index:100;backdrop-filter:blur(20px)}\n.hud-logo{font-family:monospace;font-size:15px;font-weight:900;color:var(--cyan);letter-spacing:2px;flex-shrink:0}\n.hud-price{font-family:monospace;font-size:12px;font-weight:800;color:var(--cyan)}\n.hud-chg{font-size:10px;font-weight:700;padding:2px 7px;border-radius:20px}\n.hud-chg.up{background:rgba(0,255,157,.12);color:var(--green);border:1px solid rgba(0,255,157,.2)}\n.hud-chg.dn{background:rgba(255,61,94,.12);color:var(--red);border:1px solid rgba(255,61,94,.2)}\n.hud-right{margin-left:auto;display:flex;align-items:center;gap:8px}\n.dot{width:7px;height:7px;border-radius:50%;background:var(--muted);flex-shrink:0}\n.dot.on{background:var(--green);box-shadow:0 0 8px var(--green);animation:dp 2s infinite}\n@keyframes dp{0%,100%{opacity:1}50%{opacity:.3}}\n.hud-time{font-family:monospace;font-size:10px;color:var(--muted)}\n\n/* TABS */\n.tabs{position:fixed;top:52px;left:0;right:0;background:rgba(2,4,10,.97);border-bottom:1px solid var(--border);display:flex;z-index:99;backdrop-filter:blur(20px);overflow-x:auto;scrollbar-width:none}\n.tabs::-webkit-scrollbar{display:none}\n.tab{flex:0 0 auto;padding:12px 18px;font-size:12px;font-weight:700;color:var(--muted);cursor:pointer;border:none;background:none;border-bottom:2px solid transparent;white-space:nowrap;transition:.15s}\n.tab.on{color:var(--cyan);border-bottom-color:var(--cyan)}\n.tab-more{flex:0 0 auto;padding:12px 16px;font-size:16px;color:var(--muted);cursor:pointer;border:none;background:none;border-bottom:2px solid transparent;margin-left:auto}\n.tab-more.on{color:var(--cyan)}\n\n/* DROPDOWN MENU */\n.dropdown{position:fixed;top:52px;right:0;background:var(--bg2);border:1px solid var(--border);border-radius:0 0 0 14px;z-index:200;min-width:180px;display:none;box-shadow:0 8px 32px rgba(0,0,0,.5)}\n.dropdown.show{display:block}\n.dd-item{padding:12px 18px;font-size:13px;font-weight:600;color:var(--muted);cursor:pointer;border-bottom:1px solid rgba(0,229,255,.06);display:flex;align-items:center;gap:10px}\n.dd-item:hover{background:rgba(0,229,255,.05);color:var(--text)}\n.dd-item:last-child{border:none}\n\n/* CONTENT */\n.content{padding-top:100px;padding-bottom:20px}\n.view{display:none;padding:14px;max-width:960px;margin:0 auto}\n.view.on{display:block}\n#view-nexus{display:none;flex-direction:column;padding:0;max-width:none}\n#view-nexus.on{display:flex}\n\n/* CARDS */\n.card{background:var(--bg2);border:1px solid var(--border);border-radius:16px;padding:16px;margin-bottom:12px}\n.card.cyan{border-color:rgba(0,229,255,.22)}.card.green{border-color:rgba(0,255,157,.22)}.card.red{border-color:rgba(255,61,94,.22)}.card.gold{border-color:rgba(255,176,32,.22)}.card.purple{border-color:rgba(160,107,255,.22)}\n.ct{font-size:9px;letter-spacing:2px;color:var(--muted);margin-bottom:12px;font-weight:700;text-transform:uppercase;display:flex;align-items:center;justify-content:space-between}\n.g4{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:12px}\n.g3{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px}\n.g2{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}\n.st{background:var(--bg3);border:1px solid rgba(0,229,255,.07);border-radius:12px;padding:12px 8px;text-align:center}\n.sv{font-family:monospace;font-size:16px;font-weight:800;color:var(--cyan);margin-bottom:2px}\n.sl{font-size:8px;color:var(--muted);letter-spacing:.8px;text-transform:uppercase}\n.sv.gold{color:var(--gold)}.sv.green{color:var(--green)}.sv.red{color:var(--red)}.sv.purple{color:var(--purple)}\n.sec{font-size:10px;color:var(--cyan);font-weight:700;letter-spacing:1px;text-transform:uppercase;margin:12px 0 6px;border-left:2px solid var(--cyan);padding-left:8px}\ninput,select,textarea{width:100%;padding:12px 13px;background:var(--bg3);border:1px solid var(--border);border-radius:11px;color:var(--text);font-size:14px;margin-bottom:8px;outline:none;font-family:inherit}\ninput:focus,select:focus{border-color:var(--cyan)}\n.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}\n.grow{flex:1;min-width:0;margin-bottom:0!important}\n.btn{padding:10px 14px;border-radius:10px;font-size:12px;font-weight:700;cursor:pointer;border:1px solid var(--border);background:var(--bg3);color:var(--text);white-space:nowrap;flex-shrink:0;transition:.15s}\n.btn:active{transform:scale(.96)}\n.btn.cyan{background:rgba(0,229,255,.1);border-color:rgba(0,229,255,.3);color:var(--cyan)}\n.btn.green{background:rgba(0,255,157,.1);border-color:rgba(0,255,157,.3);color:var(--green)}\n.btn.red{background:rgba(255,61,94,.1);border-color:rgba(255,61,94,.3);color:var(--red)}\n.btn.gold{background:rgba(255,176,32,.1);border-color:rgba(255,176,32,.3);color:var(--gold)}\n.btn.purple{background:rgba(160,107,255,.1);border-color:rgba(160,107,255,.3);color:var(--purple)}\n.btn.primary{background:linear-gradient(135deg,var(--cyan),#0097b2);color:#000;border:none}\n.btn.full{width:100%;padding:12px;font-size:13px;margin-bottom:8px;display:block}\n.ab{padding:10px 13px;border-radius:10px;font-size:12px;margin-bottom:6px}\n.ao{background:rgba(0,255,157,.07);border:1px solid rgba(0,255,157,.15);color:var(--green)}\n.aw{background:rgba(255,176,32,.07);border:1px solid rgba(255,176,32,.15);color:var(--gold)}\n.ae{background:rgba(255,61,94,.07);border:1px solid rgba(255,61,94,.15);color:var(--red)}\n.ai{background:rgba(0,229,255,.07);border:1px solid rgba(0,229,255,.15);color:var(--cyan)}\n.chart-wrap{position:relative;margin-bottom:10px}\n.ch200{height:200px}.ch150{height:150px}\n.fl-item{padding:10px 12px;border-bottom:1px solid rgba(0,229,255,.05);display:flex;align-items:center;gap:8px;font-size:12px}\n.fl-item:last-child{border:none}\n.tg{width:46px;height:25px;background:rgba(255,255,255,.07);border:1px solid var(--border);border-radius:13px;cursor:pointer;position:relative;flex-shrink:0;transition:.3s}\n.tg.on{background:rgba(0,229,255,.2);border-color:var(--cyan)}\n.tg-k{position:absolute;top:3px;left:3px;width:17px;height:17px;background:#8899aa;border-radius:50%;transition:.3s}\n.tg.on .tg-k{left:24px;background:var(--cyan)}\n.pbar{height:6px;background:rgba(0,0,0,.4);border-radius:3px;overflow:hidden;margin-top:4px}\n.pbar-fill{height:100%;border-radius:3px;background:linear-gradient(90deg,var(--cyan),var(--purple));transition:width .5s}\n.log-item{padding:7px 12px;border-bottom:1px solid rgba(0,229,255,.04);font-size:11px;display:flex;gap:8px}\n.log-time{color:var(--muted);font-family:monospace;flex-shrink:0;font-size:10px}\n.tbl-wrap{overflow-x:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)}\ntable{width:100%;border-collapse:collapse;font-size:11px}\nth,td{padding:9px 8px;text-align:left;border-bottom:1px solid rgba(0,229,255,.05)}\nth{color:var(--muted);font-size:9px;text-transform:uppercase;letter-spacing:.5px;font-weight:700}\n.ibar{background:var(--bg2);border-bottom:1px solid var(--border);padding:10px 14px;display:flex;align-items:center;gap:10px}\n.iurl{flex:1;font-size:10px;color:var(--muted);font-family:monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#nf{flex:1;border:none;width:100%;background:var(--bg)}\n.sw{position:relative}\n.sw input{padding-left:34px;margin:0}\n.sw::before{content:\'🔍\';position:absolute;left:10px;top:50%;transform:translateY(-50%);font-size:13px;pointer-events:none;z-index:1}\n.notif{position:absolute;top:6px;right:calc(50% - 12px);width:8px;height:8px;background:var(--red);border-radius:50%;display:none;border:2px solid var(--bg);animation:blink .8s ease infinite}\n@keyframes blink{0%,100%{transform:scale(1)}50%{transform:scale(1.3)}}\n@media(max-width:480px){.g4{grid-template-columns:repeat(2,1fr)}.sv{font-size:14px}.content{padding-top:96px}}\n@media(min-width:768px){.sv{font-size:20px}.ch200{height:240px}}\n</style>\n</head>\n<body>\n\n<!-- LOGIN -->\n<div id="ls">\n<div class="lb">\n<div class="lb-logo">◈ NEXUS</div>\n<div class="lb-sub">Panneau Serveur</div>\n<input id="mk" type="password" placeholder="Clé maître" class="fi" onkeydown="if(event.key===\'Enter\')doLogin()">\n<button class="btn-login" onclick="doLogin()">⚡ Connexion</button>\n<div id="lm"></div>\n</div>\n</div>\n\n<!-- HUD -->\n<div class="hud">\n<div class="hud-logo">◈ NXC</div>\n<div class="hud-price" id="hp">—</div>\n<div class="hud-chg" id="hc" style="display:none"></div>\n<div class="hud-right">\n<div class="dot" id="hd"></div>\n<span class="hud-time" id="htm">—</span>\n</div>\n</div>\n\n<!-- TABS -->\n<div class="tabs" id="main-tabs">\n<button class="tab on" onclick="go(\'marche\',this)">📈 Marché</button>\n<button class="tab" onclick="go(\'banque\',this)">🏦 Banque<span class="notif" id="nd-b"></span></button>\n<button class="tab" onclick="go(\'nexus\',this)">🌐 App</button>\n<button class="tab" onclick="go(\'admin\',this)">👑 Admin</button>\n<button class="tab" onclick="go(\'parametre\',this)">⚙ Réglages</button>\n<button class="tab-more" id="btn-more" onclick="toggleMore()">•••</button>\n</div>\n\n<!-- DROPDOWN MENU -->\n<div class="dropdown" id="dropdown">\n<div class="dd-item" onclick="go(\'trading\',null);toggleMore()">⚙️ Contrôle</div>\n<div class="dd-item" onclick="go(\'users\',null);toggleMore()">👥 Comptes</div>\n<div class="dd-item" onclick="go(\'stats\',null);toggleMore()">📊 Stats</div>\n<div class="dd-item" onclick="go(\'solv\',null);toggleMore()">🛡️ Solvabilité</div>\n<div class="dd-item" onclick="go(\'tools\',null);toggleMore()">🛠️ Outils</div>\n<div class="dd-item" onclick="go(\'log\',null);toggleMore()">📋 Journal</div>\n<div class="dd-item" onclick="go(\'config\',null);toggleMore()">⚙️ Config</div>\n<div class="dd-item" onclick="go(\'notifs\',null);toggleMore()">🔔 Alertes</div>\n<div class="dd-item" onclick="go(\'cycles\',null);toggleMore()">📅 Cycles de marché</div>\n<div class=\"dd-item\" onclick=\"go(\'prevision\',null);toggleMore()\">🔮 Prévision</div>\n<div class="dd-item" onclick="go(\'urgence\',null);toggleMore()">🚨 Urgence</div>\n<div class="dd-item" onclick="go(\'dashboard\',null);toggleMore()">📊 Dashboard</div>\n<div class="dd-item" onclick="go(\'alertesp\',null);toggleMore()">🎯 Alertes Prix</div>\n<div class="dd-item" onclick="go(\'simulateur\',null);toggleMore()">🔬 Simulateur</div>\n<div class="dd-item" onclick="go(\'avance\',null);toggleMore()">⚙️ Avancé</div>\n<div class="dd-item" onclick="go(\'historique\',null);toggleMore()">📈 Historique</div>\n<div class="dd-item" onclick="go(\'convertisseur\',null);toggleMore()">💱 Convertisseur</div>\n<div class="dd-item" onclick="go(\'evenements\',null);toggleMore()">🎲 Événements</div>\n<div class="dd-item" onclick="go(\'export\',null);toggleMore()">📤 Export</div>\n<div class="dd-item" onclick="go(\'memo\',null);toggleMore()">📌 Mémo Admin</div>\n</div>\n\n<div class="content">\n\n<div class="view" id="view-parametre">\n<div class="card">\n<div class="ct">◈ GESTION DES ONGLETS</div>\n<div class="sec">« Favori » = dans la barre principale • « Masquer » = caché. Synchronisé sur tous tes appareils.</div>\n<div id="tab-manager"></div>\n</div><div class="card">\n<div class="ct">◈ STYLE DE L\'INTERFACE</div>\n<div class="sec">Choisis l\'ambiance visuelle — le bleu Nexus est conservé partout. Ton choix est mémorisé sur cet appareil.</div>\n<div class="style-grid" id="style-grid">\n<div class="style-card" data-th="classic" onclick="setTheme(\'classic\')"><div class="style-swatch" style="background:linear-gradient(135deg,#02040a,#0d1428)"></div><div class="style-name">Nexus Classic</div><div class="style-desc">Le thème d\'origine, sombre et net.</div></div>\n<div class="style-card" data-th="terminal" onclick="setTheme(\'terminal\')"><div class="style-swatch" style="background:repeating-linear-gradient(#00202c 0 2px,#00060a 2px 5px)"></div><div class="style-name">Terminal / Hacker</div><div class="style-desc">Monospace, scanlines CRT, texte lumineux.</div></div>\n<div class="style-card" data-th="glass" onclick="setTheme(\'glass\')"><div class="style-swatch" style="background:linear-gradient(135deg,rgba(120,180,255,.4),rgba(20,30,60,.7))"></div><div class="style-name">Verre dépoli</div><div class="style-desc">Panneaux translucides, flou, profondeur.</div></div>\n<div class="style-card" data-th="bloomberg" onclick="setTheme(\'bloomberg\')"><div class="style-swatch" style="background:linear-gradient(135deg,#080b10,#12181f);border-color:#ffb020"></div><div class="style-name">Bloomberg Pro</div><div class="style-desc">Dense, compact, salle des marchés.</div></div>\n<div class="style-card" data-th="neon" onclick="setTheme(\'neon\')"><div class="style-swatch" style="background:#04030f;box-shadow:inset 0 0 12px #00e5ff"></div><div class="style-name">Néon Cyberpunk</div><div class="style-desc">Bords néon, grille de fond, glow.</div></div>\n<div class="style-card" data-th="minimal" onclick="setTheme(\'minimal\')"><div class="style-swatch" style="background:#171f31"></div><div class="style-name">Minimal</div><div class="style-desc">Épuré, aéré, sans ombres.</div></div>\n</div>\n<div id="theme-msg" style="font-size:12px;color:var(--muted);margin-top:12px">Style actuel : Nexus Classic</div>\n</div>\n<div class="card">\n<div class="ct">◈ AFFICHAGE & CONFORT</div>\n<div class="sec">Zoom de l\'interface</div>\n<div class="row" style="gap:10px;align-items:center">\n<input id="opt-zoom" type="range" min="70" max="150" value="100" style="flex:1" oninput="setZoom(this.value)">\n<span id="opt-zoom-v" style="font-size:12px;min-width:42px;text-align:right">100%</span>\n</div>\n<div class="opt-row"><label>Animations & effets</label><label class="switch"><input type="checkbox" id="opt-anim" checked onchange="setOpt(\'anim\',this.checked)"><span></span></label></div>\n<div class="opt-row"><label>Coins arrondis</label><label class="switch"><input type="checkbox" id="opt-round" checked onchange="setOpt(\'round\',this.checked)"><span></span></label></div>\n<div class="opt-row"><label>Mode compact (densité)</label><label class="switch"><input type="checkbox" id="opt-compact" onchange="setOpt(\'compact\',this.checked)"><span></span></label></div>\n<div class="opt-row"><label>Masquer le bandeau du haut (HUD)</label><label class="switch"><input type="checkbox" id="opt-hud" onchange="setOpt(\'hud\',this.checked)"><span></span></label></div>\n<div class="opt-row"><label>Masquer le menu latéral (•••)</label><label class="switch"><input type="checkbox" id="opt-more" onchange="setOpt(\'more\',this.checked)"><span></span></label></div>\n<div class="row" style="margin-top:10px"><button class="btn primary grow" onclick="toggleFullscreen()">⛶ Plein écran</button></div>\n</div>\n<div class="card">\n<div class="ct">◈ HORLOGE, SONS & VEILLE</div>\n<div class="opt-row"><label>Horloge 12h (AM/PM)</label><label class="switch"><input type="checkbox" id="opt-clock12" onchange="setOpt(\'clock12\',this.checked)"><span></span></label></div>\n<div class="opt-row"><label>Sons d\'interface</label><label class="switch"><input type="checkbox" id="opt-sound" onchange="setOpt(\'sound\',this.checked)"><span></span></label></div>\n<div class="opt-row"><label>Veille anti-burn après inactivité</label><label class="switch"><input type="checkbox" id="opt-saver" checked onchange="setOpt(\'saver\',this.checked)"><span></span></label></div>\n<div class="sec" style="margin-top:8px">Langue</div>\n<div class="row" style="gap:6px">\n<button class="btn grow" onclick="setLang(\'fr\')">🇫🇷 Français</button>\n<button class="btn grow" onclick="setLang(\'en\')">🇬🇧 English</button>\n</div>\n</div>\n<div class="card">\n<div class="ct">◈ FOND, RACCOURCIS & ALERTES</div>\n<div class="sec">Fond d\'écran animé</div>\n<select id="opt-bg" onchange="setBg(this.value)" style="width:100%;padding:9px;border-radius:9px;background:var(--bg3);color:var(--text);border:1px solid var(--border)">\n<option value="none">Aucun</option>\n<option value="particles">Particules connectées</option>\n<option value="stars">Étoiles scintillantes</option>\n<option value="grid">Grille animée</option>\n<option value="gradient">Dégradé mouvant</option>\n</select>\n<div class="opt-row" style="margin-top:8px"><label>Raccourcis clavier (1-9 = onglets)</label><label class="switch"><input type="checkbox" id="opt-keys" onchange="setOpt(\'keys\',this.checked)"><span></span></label></div>\n<div class="opt-row"><label>HUD en bas de l\'écran</label><label class="switch"><input type="checkbox" id="opt-hudbottom" onchange="setOpt(\'hudbottom\',this.checked)"><span></span></label></div>\n<div class="opt-row"><label>Notifications (mouvements de prix)</label><label class="switch"><input type="checkbox" id="opt-notif" onchange="setOpt(\'notif\',this.checked)"><span></span></label></div>\n</div></div>\n<!-- MARCHÉ -->\n<div class="view on" id="view-marche">\n<div class="g4">\n<div class="st"><div class="sv" id="s-p">—</div><div class="sl">Prix R/NXC</div></div>\n<div class="st"><div class="sv gold" id="s-v">—</div><div class="sl">Vol. 24h</div></div>\n<div class="st"><div class="sv green" id="s-t">—</div><div class="sl">Trades 24h</div></div>\n<div class="st"><div class="sv purple" id="s-h">—</div><div class="sl">Hist. pts</div></div>\n<div class="st"><div class="sv green" id="s-hi">—</div><div class="sl">Haut 24h</div></div>\n<div class="st"><div class="sv red" id="s-lo">—</div><div class="sl">Bas 24h</div></div>\n<div class="st"><div class="sv" id="s-var">—</div><div class="sl">Variation</div></div>\n<div class="st"><div class="sv" style="color:#ff6eb4" id="s-cap">—</div><div class="sl">Cap. marché</div></div>\n</div>\n<div class="card cyan">\n<div class="ct">◈ HISTORIQUE DU COURS\n<div style="display:flex;gap:5px">\n<button class="btn" onclick="setRange(25)" style="padding:3px 8px;font-size:9px">25</button>\n<button class="btn cyan" onclick="setRange(50)" style="padding:3px 8px;font-size:9px">50</button>\n<button class="btn" onclick="setRange(100)" style="padding:3px 8px;font-size:9px">100</button>\n</div>\n</div>\n<div class="chart-wrap ch200"><canvas id="ch"></canvas></div>\n<div style="display:flex;gap:6px;margin-top:8px;flex-wrap:wrap">\n<button class="btn gold" onclick="chObj&&chObj.zoom(1.5)">🔍+</button>\n<button class="btn gold" onclick="chObj&&chObj.zoom(0.7)">🔍−</button>\n<button class="btn" onclick="chObj&&chObj.resetZoom()">Reset</button>\n<button class="btn cyan" onclick="toggleChartType()">📊 Type</button>\n<button class="btn purple" onclick="dlChart()">⬇️ PNG</button>\n</div>\n</div>\n<div class="card"><div class="ct">◈ ALERTES MARCHÉ</div><div id="al"></div></div>\n<div class="card gold"><div class="ct">◈ RSI (14 ticks)</div><div class="chart-wrap ch150"><canvas id="ch-rsi"></canvas></div><div style="font-size:10px;color:var(--muted);margin-top:4px">RSI >70 = surachat · RSI <30 = survente</div></div>\n</div>\n\n<!-- CONTRÔLE -->\n<div class="view" id="view-trading">\n<div class="card cyan">\n<div class="ct">◈ MODIFIER LE COURS</div>\n<div class="sec">Raccourcis ±%</div>\n<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:10px">\n<button class="btn green" onclick="adjP(.05)">+5%</button>\n<button class="btn green" onclick="adjP(.02)">+2%</button>\n<button class="btn green" onclick="adjP(.01)">+1%</button>\n<button class="btn green" onclick="adjP(.005)">+0.5%</button>\n<button class="btn red" onclick="adjP(-.005)">-0.5%</button>\n<button class="btn red" onclick="adjP(-.01)">-1%</button>\n<button class="btn red" onclick="adjP(-.02)">-2%</button>\n<button class="btn red" onclick="adjP(-.05)">-5%</button>\n</div>\n<div class="sec">Prix exact</div>\n<div class="row"><input id="np" type="number" min="1" max="999999" placeholder="Prix (ex: 100000)" class="grow"><button class="btn primary" onclick="setP()">✓</button></div>\n<div class="sec">Variation %</div>\n<div class="row"><input id="np-pct" type="number" placeholder="Ex: +10 ou -5" class="grow"><button class="btn cyan" onclick="setPct()">Appliquer</button></div>\n<div id="pm" style="font-size:11px;font-weight:600;min-height:14px;margin-top:4px"></div>\n</div>\n<div class="card">\n<div class="ct">◈ VITESSE DE VARIATION</div>\n<div class="sec">Amplitude des fluctuations du cours</div>\n<div class="row" style="gap:10px;align-items:center">\n<input id="vol-range" type="range" min="0" max="5" step="0.1" value="1" style="flex:1" oninput="setVolatility(this.value)">\n<span id="vol-v" style="font-size:12px;min-width:82px;text-align:right">Normal</span>\n</div>\n<div style="font-size:11px;color:var(--muted);margin-top:4px">0 = figé • 1 = normal • 5 = extrême</div>\n</div><div class="card">\n<div class="ct">◈ CROISSANCE PROGRAMMÉE</div>\n<div class="sec">Rythme — de 0.01%/jour à 100%/seconde</div>\n<div class="row" style="gap:6px">\n<input id="gr-pct" type="number" step="0.01" min="0" placeholder="Ex: 5" class="grow">\n<select id="gr-unit" class="grow" style="padding:9px;border-radius:9px;background:var(--bg3);color:var(--text);border:1px solid var(--border)">\n<option value="1">par seconde</option>\n<option value="60">par minute</option>\n<option value="3600">par heure</option>\n<option value="86400" selected>par jour</option>\n</select>\n</div>\n<div class="row" style="gap:6px;margin-top:6px">\n<button class="btn green grow" onclick="setGrowth(1)">▲ Hausse</button>\n<button class="btn red grow" onclick="setGrowth(-1)">▼ Baisse</button>\n</div>\n<label style="display:flex;align-items:center;gap:8px;margin-top:9px;font-size:12px;cursor:pointer">\n<input id="gr-combine" type="checkbox" checked> En même temps que les variations du cours</label>\n<div class="row" style="gap:5px;margin-top:6px;flex-wrap:wrap">\n<button class="btn cyan" style="font-size:10px;padding:5px 8px" onclick="quickGrowth(10)">+10%/j</button>\n<button class="btn cyan" style="font-size:10px;padding:5px 8px" onclick="quickGrowth(50)">+50%/j</button>\n<button class="btn cyan" style="font-size:10px;padding:5px 8px" onclick="quickGrowth(100)">×2/j</button>\n<button class="btn red" style="font-size:10px;padding:5px 8px" onclick="quickGrowth(-30)">-30%/j</button>\n</div>\n<div class="row" style="margin-top:8px;gap:6px">\n<button class="btn grow" onclick="stopGrowth()">⏸ Stop</button>\n<button class="btn grow" onclick="diagGrowth()">🔍 Diagnostic</button>\n</div>\n<div id="gr-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:5px"></div>\n<div id="gr-status" style="font-size:11px;color:var(--muted);margin-top:2px">○ Inactive</div>\n</div>\n<div class="card">\n<div class="ct">◈ TENDANCE AUTO <span id="tt-timer" style="font-family:monospace;font-size:10px;color:var(--muted)"></span></div>\n<select id="ts" style="margin-bottom:8px">\n<option value="0.001">Ultra lent 0.1%</option>\n<option value="0.002">Très lent 0.2%</option>\n<option value="0.005" selected>Lent 0.5%</option>\n<option value="0.01">Moyen 1%</option>\n<option value="0.02">Rapide 2%</option>\n<option value="0.05">Très rapide 5%</option>\n<option value="0.1">Extrême 10%</option>\n</select>\n<select id="ti" style="margin-bottom:8px">\n<option value="5000">5s</option>\n<option value="12000" selected>12s</option>\n<option value="30000">30s</option>\n<option value="60000">1min</option>\n</select>\n<div class="sec">Amplitude de variation par tick</div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="noise-slider" type="range" min="1" max="10" value="4" oninput="updateNoise(this.value)" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--cyan)">\n<span id="noise-val" style="color:var(--cyan);font-weight:700;font-size:13px;width:48px;text-align:right;flex-shrink:0">0.4%</span>\n</div>\n<div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:8px">\n<button class="btn green" onclick="setT(\'up\')">📈 Hausse</button>\n<button class="btn red" onclick="setT(\'down\')">📉 Baisse</button>\n<button class="btn purple" onclick="setT(\'random\')">🎲 Aléatoire</button>\n<button class="btn" onclick="setT(\'stop\')" style="color:var(--muted)">⏸ Stop</button>\n</div>\n<div id="tst" style="font-size:12px;color:var(--muted);font-weight:600;padding:8px;background:var(--bg3);border-radius:8px;text-align:center">⏸ Arrêté</div>\n</div>\n<div class="card" style="border-color:rgba(0,229,255,.25)">\n<div class="ct" style="color:var(--cyan)">◈ FRAIS DE TRANSACTION NXC</div>\n<div style="font-size:11px;color:var(--muted);margin-bottom:10px">\n  Frais prélevés à l\'achat et à la vente de NXC, selon le rôle de l\'utilisateur.\n</div>\n<!-- Appliquer à tous -->\n<div style="background:var(--bg3);border-radius:10px;padding:10px;margin-bottom:10px">\n  <div style="font-weight:700;font-size:11px;color:var(--gold);margin-bottom:6px">⚡ Appliquer à TOUS les rôles</div>\n  <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">\n    <input id="fee-all-buy"  type="number" min="0" max="50" step="0.1" placeholder="Achat %" style="width:90px;padding:6px 8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:12px">\n    <input id="fee-all-sell" type="number" min="0" max="50" step="0.1" placeholder="Vente %" style="width:90px;padding:6px 8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:12px">\n    <button class="btn gold" onclick="setAllFees()" style="font-size:11px">Tout mettre à jour</button>\n  </div>\n</div>\n<!-- Tableau par rôle -->\n<table style="width:100%;border-collapse:collapse;font-size:12px">\n<thead><tr style="color:var(--muted);font-size:10px;text-transform:uppercase;border-bottom:1px solid var(--border)">\n  <th style="text-align:left;padding:6px 4px">Rôle</th>\n  <th style="text-align:center;padding:6px 4px">Achat (%)</th>\n  <th style="text-align:center;padding:6px 4px">Vente (%)</th>\n  <th style="text-align:center;padding:6px 4px">Action</th>\n</tr></thead>\n<tbody id="fees-tbody">\n  <tr><td colspan="4" style="color:var(--muted);font-size:11px;padding:10px;text-align:center">Chargement…</td></tr>\n</tbody>\n</table>\n<div id="fees-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:8px"></div>\n</div>\n\n<div class="card gold">\n<div class="ct">◈ SCÉNARIOS</div>\n<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">\n<button class="btn gold" onclick="scenario(\'crash\')">💥 Crash −30%</button>\n<button class="btn gold" onclick="scenario(\'moon\')">🚀 Moon +30%</button>\n<button class="btn gold" onclick="scenario(\'volatile\')">⚡ Volatil</button>\n<button class="btn gold" onclick="scenario(\'stable\')">😴 Stabiliser</button>\n<button class="btn gold" onclick="scenario(\'ath\')">🏆 ATH</button>\n<button class="btn gold" onclick="scenario(\'floor\')">🛑 Plancher 200R</button>\n</div>\n</div>\n<div class="card green">\n<div class="ct">◈ COURS NORMAL (PLANCHER + PLAFOND)</div>\n<div class="row" style="margin-bottom:8px">\n<input id="t-floor" type="number" placeholder="Plancher min (R)" class="grow">\n<button class="btn green" onclick="setFloor()">✓ Plancher</button>\n<button class="btn red" onclick="_cfgFloor=null;updFloorDisplay()" style="padding:10px 12px">✕</button>\n</div>\n<div class="row" style="margin-bottom:8px">\n<input id="t-ceil" type="number" placeholder="Plafond max (R)" class="grow">\n<button class="btn green" onclick="setCeil()">✓ Plafond</button>\n<button class="btn red" onclick="_cfgCeil=null;updFloorDisplay()" style="padding:10px 12px">✕</button>\n</div>\n<div id="floor-display" style="font-size:11px;padding:8px;background:var(--bg3);border-radius:8px;color:var(--muted)">Plancher: non défini · Plafond: non défini</div>\n<button class="btn green full" style="margin-top:8px" onclick="setNormalMode()">📊 Activer cours normal</button>\n<div style="font-size:10px;color:var(--muted);margin-top:4px">Le prix fluctue librement mais reste entre le plancher et le plafond</div>\n</div>\n<div class="card" style="border-color:#a855f7;">\n<div class="ct" style="color:#a855f7;">◈ PRIX MOYEN (MEAN REVERSION)</div>\n<div style="display:flex;align-items:center;gap:14px;margin-bottom:10px;">\n<div id="mp-tg" class="tg" onclick="toggleMp()"><div class="tg-k"></div></div>\n<span id="mp-lbl" style="color:var(--muted);font-size:13px;">⏸ Désactivé</span>\n</div>\n<label style="font-size:12px;color:var(--muted);margin-bottom:4px;display:block;">Prix moyen cible (R)</label>\n<input id="mp-target" type="number" min="50" max="100000" step="1" placeholder="Ex: 5000">\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px;">\n<button class="btn" onclick="document.getElementById(\'mp-target\').value=Math.round(mkt.price||5000);saveMeanPrice();">Prix actuel</button>\n<button class="btn" onclick="document.getElementById(\'mp-target\').value=1000;saveMeanPrice();">1 000 R</button>\n<button class="btn" onclick="document.getElementById(\'mp-target\').value=5000;saveMeanPrice();">5 000 R</button>\n<button class="btn" onclick="document.getElementById(\'mp-target\').value=10000;saveMeanPrice();">10 000 R</button>\n</div>\n<button class="btn full" onclick="saveMeanPrice()">💾 Sauvegarder</button>\n<div id="mp-msg" style="margin-top:6px;font-size:12px;"></div>\n</div>\n<style>\n.bc-track{position:relative;height:10px;background:linear-gradient(90deg,#ff3d5e 0%,rgba(255,255,255,0.06) 50%,#00ff9d 100%);border-radius:5px;margin:16px 0 8px;cursor:pointer;box-shadow:inset 0 0 6px rgba(0,0,0,.3)}\n.bc-thumb{position:absolute;top:50%;width:26px;height:26px;background:#fff;border-radius:50%;transform:translate(-50%,-50%);box-shadow:0 0 14px rgba(255,255,255,.7);cursor:grab;z-index:2;transition:background-color .25s,box-shadow .25s;border:2px solid rgba(255,255,255,.3)}\n.bc-thumb:active{cursor:grabbing;transform:translate(-50%,-50%) scale(1.15)}\n.bc-zones{display:flex;justify-content:space-between;font-size:9px;color:var(--muted);margin-top:4px;letter-spacing:.5px;text-transform:uppercase}\n.bc-ind{text-align:center;padding:11px;border-radius:10px;margin-bottom:10px;font-weight:800;font-size:13px;letter-spacing:.5px;transition:all .4s}\n.bc-ind.bear{background:rgba(255,61,94,.14);border:1px solid rgba(255,61,94,.35);color:var(--red);text-shadow:0 0 10px var(--red)}\n.bc-ind.bull{background:rgba(0,255,157,.14);border:1px solid rgba(0,255,157,.35);color:var(--green);text-shadow:0 0 10px var(--green)}\n.bc-ind.neutral{background:rgba(0,229,255,.06);border:1px solid var(--border);color:var(--muted)}\n.spd-track{position:relative;height:6px;background:rgba(255,255,255,.06);border-radius:3px;margin:12px 0 6px;cursor:pointer}\n.spd-fill{height:100%;border-radius:3px;background:linear-gradient(90deg,var(--cyan),var(--purple));transition:width .15s}\n.spd-thumb{position:absolute;top:50%;width:18px;height:18px;background:var(--cyan);border-radius:50%;transform:translate(-50%,-50%);box-shadow:0 0 8px var(--cyan);cursor:grab;z-index:2}\n.spd-thumb:active{cursor:grabbing}\n#bc-info-modal{display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:9999;align-items:center;justify-content:center}\n#bc-info-modal.show{display:flex}\n.bc-info-box{background:#1a1a2e;border:1px solid rgba(160,107,255,.4);border-radius:16px;padding:24px;max-width:320px;color:#e0e0e0;font-size:13px;line-height:1.6;position:relative}\n.bc-info-box h3{margin:0 0 12px;color:#a06bff;font-size:15px}\n.bc-info-close{position:absolute;top:10px;right:14px;cursor:pointer;font-size:18px;color:var(--muted);background:none;border:none}\n</style>\n\n<div id="bc-info-modal" onclick="if(event.target===this)this.classList.remove(\'show\')">\n  <div class="bc-info-box">\n    <button class="bc-info-close" onclick="document.getElementById(\'bc-info-modal\').classList.remove(\'show\')">✕</button>\n    <h3>⚡ Dynamique du Prix</h3>\n    <b>Direction (curseur haut)</b><br>\n    Pousse le prix vers le haut (Bull 🚀) ou vers le bas (Bear 🐻). Neutre = variation équilibrée sans dérive.<br><br>\n    <b>Vitesse (curseur bas)</b><br>\n    Multiplie la fréquence de variation. ×1 = normal (tick toutes les 30s), ×8 = extrême (tick toutes les 4s).<br><br>\n    <b>Note</b> : quand un biais est actif, la correction anti-dérive est suspendue automatiquement.\n  </div>\n</div>\n\n<div class="card" style="background:linear-gradient(135deg,rgba(0,229,255,.03) 0%,rgba(160,107,255,.07) 50%,rgba(255,61,94,.03) 100%);border-color:rgba(160,107,255,.28);position:relative;overflow:hidden">\n<div style="position:absolute;top:-50px;right:-50px;width:140px;height:140px;background:radial-gradient(circle,rgba(160,107,255,.15),transparent 70%);pointer-events:none"></div>\n<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px">\n  <div class="ct" style="color:#a06bff;font-size:10px;letter-spacing:3px;margin:0">⚡ DYNAMIQUE DU PRIX</div>\n  <button onclick="document.getElementById(\'bc-info-modal\').classList.add(\'show\')" style="width:22px;height:22px;border-radius:50%;background:rgba(160,107,255,.15);border:1px solid rgba(160,107,255,.4);color:#a06bff;font-size:11px;font-weight:700;cursor:pointer;display:flex;align-items:center;justify-content:center;line-height:1">i</button>\n</div>\n<div style="font-size:10px;color:var(--muted);margin-bottom:8px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase">↕ Direction du marché</div>\n<div id="bc-ind" class="bc-ind neutral">⚖ NEUTRE — variation équilibrée</div>\n<div class="bc-track" id="bc-track" onmousedown="startBiasDrag(event)" ontouchstart="startBiasDrag(event)">\n  <div class="bc-thumb" id="bc-thumb" style="left:50%"></div>\n</div>\n<div class="bc-zones"><span style="color:var(--red)">🐻 Bear</span><span>⚖ Neutre</span><span style="color:var(--green)">🚀 Bull</span></div>\n<div style="display:flex;gap:6px;margin-top:10px;margin-bottom:4px">\n<button class="btn red" style="flex:1;font-size:11px" onclick="setBias(-1)">🐻 Full Bear</button>\n<button class="btn" style="flex:1;font-size:11px" onclick="setBias(0)">⚖ Neutre</button>\n<button class="btn green" style="flex:1;font-size:11px" onclick="setBias(1)">🚀 Full Bull</button>\n</div>\n<div style="height:1px;background:rgba(255,255,255,.05);margin:14px 0"></div>\n<div style="font-size:10px;color:var(--muted);margin-bottom:8px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase">⏱ Vitesse de variation</div>\n<div id="spd-ind" style="text-align:center;font-family:monospace;font-size:13px;font-weight:800;color:var(--muted);margin-bottom:6px">× 1.00  —  Normal</div>\n<div class="spd-track" id="spd-track" onmousedown="startSpdDrag(event)" ontouchstart="startSpdDrag(event)">\n  <div class="spd-fill" id="spd-fill" style="width:50%"></div>\n  <div class="spd-thumb" id="spd-thumb" style="left:50%"></div>\n</div>\n<div style="display:flex;justify-content:space-between;font-size:9px;color:var(--muted);margin-bottom:10px"><span>🐌</span><span>Lent</span><span>Normal</span><span>⚡</span><span>🔥</span></div>\n<div style="display:flex;gap:4px;flex-wrap:wrap">\n<button class="btn" style="flex:1;min-width:0;font-size:10px;padding:8px 4px" onclick="setSpd(0.25)">×¼</button>\n<button class="btn" style="flex:1;min-width:0;font-size:10px;padding:8px 4px" onclick="setSpd(0.5)">×½</button>\n<button class="btn cyan" style="flex:1;min-width:0;font-size:10px;padding:8px 4px" onclick="setSpd(1)">×1</button>\n<button class="btn gold" style="flex:1;min-width:0;font-size:10px;padding:8px 4px" onclick="setSpd(2)">×2</button>\n<button class="btn red" style="flex:1;min-width:0;font-size:10px;padding:8px 4px" onclick="setSpd(4)">×4</button>\n<button class="btn" style="flex:1;min-width:0;font-size:10px;padding:8px 4px;background:rgba(255,61,94,.2);border-color:rgba(255,61,94,.5);color:var(--red)" onclick="setSpd(8)">×8🔥</button>\n</div>\n<div id="bias-msg" style="margin-top:8px;font-size:11px;text-align:center;min-height:14px"></div>\n</div>\n<div class="card"><div class="ct">◈ RESET</div>\n<button class="btn full" style="color:var(--gold);border-color:rgba(255,176,32,.3);background:rgba(255,176,32,.06)" onclick="resetH()">🔄 Reset historique</button>\n<button class="btn full red" onclick="if(confirm(\'Reset complet ?\'))resetH()">⚠️ Reset complet</button>\n</div>\n</div>\n\n<!-- BANQUE -->\n<div class="view" id="view-banque">\n<div class="g4">\n<div class="st"><div class="sv" style="color:#00b4d8;font-size:14px" id="bk-r">—</div><div class="sl">Réserves</div></div>\n<div class="st"><div class="sv gold" style="font-size:14px" id="bk-i">—</div><div class="sl">Total entré</div></div>\n<div class="st"><div class="sv red" style="font-size:14px" id="bk-o">—</div><div class="sl">Total sorti</div></div>\n<div class="st"><div class="sv green" style="font-size:14px" id="bk-rt">—</div><div class="sl">Ratio</div></div>\n<div class="st"><div class="sv purple" style="font-size:14px" id="bk-nx">—</div><div class="sl">NXC émis</div></div>\n<div class="st"><div class="sv" style="font-size:14px;color:#4ea8de" id="bk-vx">—</div><div class="sl">Val. stock</div></div>\n<div class="st"><div class="sv" style="font-size:14px" id="bk-bn">—</div><div class="sl">Bénéfice</div></div>\n<div class="st"><div class="sv" style="font-size:14px;color:#ff6eb4" id="bk-fl">—</div><div class="sl">Nb flux</div></div>\n</div>\n<div class="card cyan">\n<div class="ct">◈ OPÉRATIONS</div>\n<div class="row" style="margin-bottom:8px">\n<input id="bk-amt" type="number" placeholder="Montant (R)" class="grow">\n<button class="btn green" onclick="bankOp(\'in\')">+ Injecter</button>\n<button class="btn red" onclick="bankOp(\'out\')">− Retirer</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px">\n<button class="btn cyan" onclick="setAmt(100)" style="font-size:11px;padding:6px 10px">100</button>\n<button class="btn cyan" onclick="setAmt(500)" style="font-size:11px;padding:6px 10px">500</button>\n<button class="btn cyan" onclick="setAmt(1000)" style="font-size:11px;padding:6px 10px">1 000</button>\n<button class="btn cyan" onclick="setAmt(5000)" style="font-size:11px;padding:6px 10px">5 000</button>\n<button class="btn cyan" onclick="setAmt(10000)" style="font-size:11px;padding:6px 10px">10 000</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap">\n<button class="btn gold" onclick="bankResetHist()" style="font-size:11px">🗑️ Reset hist.</button>\n<button class="btn red" onclick="bankResetAll()" style="font-size:11px">💥 Reset complet</button>\n<button class="btn purple" onclick="loadBank()" style="font-size:11px">🔄 Actualiser</button>\n<button class="btn" onclick="exportFlux()" style="font-size:11px">📊 CSV</button>\n</div>\n<div id="bk-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:8px"></div>\n</div>\n<div class="card">\n<div class="ct">◈ COMPTES</div>\n<div class="g4" style="grid-template-columns:repeat(2,1fr);gap:8px;margin-bottom:10px">\n<div class="st"><div class="sv" style="color:var(--cyan);font-size:14px" id="ac-reserves">—</div><div class="sl">Réserves</div></div>\n<div class="st"><div class="sv" style="color:#4ea8de;font-size:14px" id="ac-courant">—</div><div class="sl">Courant</div></div>\n<div class="st"><div class="sv green" style="font-size:14px" id="ac-epargne">—</div><div class="sl">Épargne</div></div>\n<div class="st"><div class="sv gold" style="font-size:14px" id="ac-coffre">—</div><div class="sl">Coffre</div></div>\n</div>\n<div class="sec">Virement interne</div>\n<input id="tr-amt" type="number" placeholder="Montant (R)" class="grow" style="width:100%;margin-bottom:6px">\n<div class="row" style="gap:6px">\n<select id="tr-from" class="grow" style="padding:9px;border-radius:9px;background:var(--bg3);color:var(--text);border:1px solid var(--border)"><option value="reserves">Réserves</option><option value="courant">Courant</option><option value="epargne">Épargne</option><option value="coffre">Coffre</option></select>\n<span style="align-self:center">→</span>\n<select id="tr-to" class="grow" style="padding:9px;border-radius:9px;background:var(--bg3);color:var(--text);border:1px solid var(--border)"><option value="courant">Courant</option><option value="reserves">Réserves</option><option value="epargne">Épargne</option><option value="coffre">Coffre</option></select>\n<button class="btn primary" onclick="bankTransfer()">✓</button>\n</div>\n<div id="tr-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:5px"></div>\n</div>\n<div class="card">\n<div class="ct">◈ ÉPARGNE — OBJECTIF</div>\n<div class="row" style="gap:6px">\n<input id="sv-goal" type="number" placeholder="Objectif (R)" class="grow">\n<button class="btn cyan" onclick="setSavingsGoal()">Définir</button>\n</div>\n<div style="height:14px;background:var(--bg3);border-radius:8px;overflow:hidden;margin-top:10px;border:1px solid var(--border)"><div id="sv-bar" style="height:100%;width:0%;background:linear-gradient(90deg,var(--green),var(--cyan));transition:.4s"></div></div>\n<div id="sv-txt" style="font-size:11px;color:var(--muted);margin-top:5px">Aucun objectif défini</div>\n<div class="sec" style="margin-top:8px">Taux d\'intérêt de l\'épargne</div>\n<div class="row" style="gap:6px">\n<input id="sv-rate" type="number" step="0.01" placeholder="Ex: 5" class="grow">\n<select id="sv-unit" class="grow" style="padding:9px;border-radius:9px;background:var(--bg3);color:var(--text);border:1px solid var(--border)"><option value="86400">par jour</option><option value="604800">par semaine</option><option value="2592000">par mois</option><option value="31536000" selected>par an</option></select>\n<button class="btn cyan" onclick="setSavingsRate()">✓</button>\n</div>\n<div id="sv-rate-txt" style="font-size:10px;color:var(--muted);margin-top:3px">Intérêts versés automatiquement.</div>\n</div>\n<div class="card">\n<div class="ct">◈ PRÊTS & CRÉDIT</div>\n<div class="sec">Accorder un prêt</div>\n<div class="row" style="gap:6px">\n<input id="ln-who" type="text" placeholder="Emprunteur" class="grow">\n<input id="ln-amt" type="number" placeholder="Montant (R)" class="grow">\n</div>\n<div class="row" style="gap:6px;margin-top:6px">\n<input id="ln-rate" type="number" step="0.1" placeholder="Taux %/an" class="grow">\n<input id="ln-days" type="number" placeholder="Durée (j)" class="grow">\n<button class="btn green" onclick="grantLoan()">Accorder</button>\n</div>\n<div id="ln-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:5px"></div>\n<div class="sec" style="margin-top:8px">Prêts en cours — dette totale <span id="ln-total">0 R</span></div>\n<div id="ln-list" style="max-height:220px;overflow-y:auto"></div>\n</div>\n<div class="card">\n<div class="ct">◈ ANALYTICS <button class="btn" onclick="printBankReport()" style="font-size:10px;padding:4px 8px">🖨️ Relevé complet</button></div>\n<div style="position:relative;height:160px;margin-top:6px"><canvas id="bk-analytics"></canvas></div>\n<div class="g4" style="grid-template-columns:repeat(3,1fr);gap:8px;margin-top:10px">\n<div class="st"><div class="sv green" style="font-size:13px" id="an-in">—</div><div class="sl">Entrées</div></div>\n<div class="st"><div class="sv red" style="font-size:13px" id="an-out">—</div><div class="sl">Sorties</div></div>\n<div class="st"><div class="sv" style="font-size:13px" id="an-net">—</div><div class="sl">Net</div></div>\n</div>\n</div><div class="card">\n<div class="ct">◈ FLUX\n<div style="display:flex;gap:4px">\n<button class="btn cyan" id="fl-all" onclick="filterFlux(\'all\')" style="padding:3px 7px;font-size:9px">Tous</button>\n<button class="btn" id="fl-in" onclick="filterFlux(\'IN\')" style="padding:3px 7px;font-size:9px">Entrées</button>\n<button class="btn" id="fl-out" onclick="filterFlux(\'OUT\')" style="padding:3px 7px;font-size:9px">Sorties</button>\n</div>\n</div>\n<div id="bk-flux" style="max-height:220px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"></div>\n</div>\n<div class="card red">\n<div class="ct" style="color:var(--red)">⚠️ TENTATIVES ÉCHOUÉES <span id="fails-ct" style="display:none;background:var(--red);color:#000;border-radius:20px;padding:1px 7px;font-size:9px"></span></div>\n<div id="bk-fails" style="max-height:220px;overflow-y:auto"></div>\n</div>\n<div class="card" style="margin-top:12px">\n<div class="ct" style="font-size:10px;letter-spacing:2px;margin-bottom:10px">📈 PRIX NXC — HISTORIQUE</div>\n<svg id="bk-svg" viewBox="0 0 320 90" style="width:100%;height:auto;display:block;background:rgba(0,0,0,.15);border-radius:8px"></svg>\n<div style="display:flex;justify-content:space-between;font-size:9px;color:var(--muted);margin-top:5px">\n<span id="bk-svg-lo">-</span><span id="bk-svg-cur" style="color:var(--cyan);font-weight:700">-</span><span id="bk-svg-hi">-</span>\n</div>\n</div>\n<script>\nfunction drawBkGraph(){\n  var hist=(mkt&&mkt.history)||[];\n  var svg=document.getElementById(\'bk-svg\');\n  if(!svg)return;\n  if(hist.length<2){svg.innerHTML=\'<text x="50%" y="50%" text-anchor="middle" fill="#555" font-size="11">Pas encore de données</text>\';return;}\n  var pts=hist.slice(-80);\n  var prices=pts.map(function(x){return parseFloat(x.price||x);});\n  var mn=Math.min.apply(null,prices);\n  var mx=Math.max.apply(null,prices);\n  var rng=mx-mn||1;\n  var W=320,H=90,P=10;\n  function sx(i){return P+i/(pts.length-1)*(W-2*P);}\n  function sy(p){return P+(1-(p-mn)/rng)*(H-2*P);}\n  var d=\'\';\n  for(var i=0;i<prices.length;i++){d+=(i===0?\'M\':\'L\')+sx(i).toFixed(1)+\' \'+sy(prices[i]).toFixed(1);}\n  var fill=d+\'L\'+(W-P)+\' \'+(H-P)+\'L\'+P+\' \'+(H-P)+\'Z\';\n  var cur=prices[prices.length-1];\n  var col=cur>=prices[0]?\'#00ff9d\':\'#ff3d5e\';\n  svg.innerHTML=\'<defs><linearGradient id="bkG" x1="0" x2="0" y1="0" y2="1">\'\n    +\'<stop offset="0%" stop-color="\'+col+\'" stop-opacity="0.35"/>\'\n    +\'<stop offset="100%" stop-color="\'+col+\'" stop-opacity="0.02"/>\'\n    +\'</linearGradient></defs>\'\n    +\'<path d="\'+fill+\'" fill="url(#bkG)"/>\'\n    +\'<path d="\'+d+\'" stroke="\'+col+\'" stroke-width="1.5" fill="none" stroke-linejoin="round"/>\'\n    +\'<circle cx="\'+(W-P)+\'" cy="\'+sy(cur)+\'" r="3" fill="\'+col+\'"/>\';\n  var lo=document.getElementById(\'bk-svg-lo\');\n  var hi=document.getElementById(\'bk-svg-hi\');\n  var cc=document.getElementById(\'bk-svg-cur\');\n  if(lo)lo.textContent=fmt(mn,0)+\' R\';\n  if(hi)hi.textContent=fmt(mx,0)+\' R\';\n  if(cc)cc.textContent=fmt(cur,0)+\' R\';\n}\n</script>\n</div>\n\n<!-- APP -->\n<div class="view" id="view-nexus">\n<div style="padding:12px;background:var(--bg2);border-bottom:1px solid var(--border)">\n<div id="pinned-bar" style="display:none;gap:6px;flex-wrap:wrap;margin-bottom:8px;padding:6px;background:rgba(255,176,32,.05);border:1px solid rgba(255,176,32,.15);border-radius:10px"></div>\n<div class="row" style="margin-bottom:8px">\n<input id="iframe-in" type="url" placeholder="https://..." class="grow" onkeydown="if(event.key===\'Enter\')goUrl()">\n<button class="btn primary" onclick="goUrl()">▶</button>\n</div>\n<div id="saved-sites" style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px"></div>\n<div class="row">\n<input id="site-lbl" placeholder="Nom" style="flex:1;margin:0;font-size:12px;padding:8px 10px">\n<button class="btn gold" onclick="saveSite()" style="font-size:11px">💾 Sauver</button>\n<button class="btn cyan" onclick="reloadF()" style="font-size:11px">🔄</button>\n<button class="btn" onclick="openNewTab()" style="font-size:11px">↗</button>\n</div>\n</div>\n<div class="ibar">\n<span style="color:var(--cyan);font-size:12px;font-weight:800" id="if-title">◈ App</span>\n<span class="iurl" id="if-url">—</span>\n</div>\n<iframe id="nf" src="about:blank" allow="clipboard-write" style="flex:1;border:none;width:100%;min-height:calc(100dvh - 200px)"></iframe>\n</div>\n\n<!-- ADMIN -->\n<div class="view" id="view-admin">\n<div class="card cyan">\n<div class="ct">◈ STATISTIQUES SERVEUR EN TEMPS RÉEL</div>\n<div class="g4" id="adm-stats">\n<div class="st"><div class="sv" id="adm-price">—</div><div class="sl">Prix actuel</div></div>\n<div class="st"><div class="sv gold" id="adm-vol">—</div><div class="sl">Vol. 24h</div></div>\n<div class="st"><div class="sv green" id="adm-trades">—</div><div class="sl">Trades</div></div>\n<div class="st"><div class="sv purple" id="adm-users">—</div><div class="sl">Utilisateurs</div></div>\n<div class="st"><div class="sv" style="color:#00b4d8" id="adm-res">—</div><div class="sl">Réserves</div></div>\n<div class="st"><div class="sv gold" id="adm-nxc">—</div><div class="sl">NXC émis</div></div>\n<div class="st"><div class="sv green" id="adm-fails">—</div><div class="sl">Tentatives échouées</div></div>\n<div class="st"><div class="sv" id="adm-hist">—</div><div class="sl">Points hist.</div></div>\n</div>\n<button class="btn cyan" onclick="refreshAdminStats()" style="width:100%;margin-top:4px;padding:10px">🔄 Actualiser tout</button>\n</div>\n<div class="card green"><div class="ct">◈ SAUVEGARDE ET IMPORT DES DONNÉES</div><button class="btn green full" onclick="saveAllData()">💾 Sauvegarder toutes les données (JSON)</button><button class="btn cyan full" onclick="importData()">📥 Importer depuis un fichier JSON</button><button class="btn purple full" onclick="printDashboard()">🖨️ Imprimer le tableau de bord</button><div id="data-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:4px"></div></div>\n<div class="card gold">\n<div class="ct">◈ DONNER DES REWARDS À UN UTILISATEUR</div>\n<div class="row" style="margin-bottom:8px">\n<select id="rw-u" class="grow" style="margin:0"><option value="">Utilisateur...</option></select>\n<input id="rw-amt" type="number" placeholder="Montant" style="width:100px;margin:0;flex-shrink:0">\n<button class="btn gold" onclick="giveRewards()">💰 Donner</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:4px">\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=50" style="font-size:11px;padding:6px 10px">50</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=100" style="font-size:11px;padding:6px 10px">100</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=500" style="font-size:11px;padding:6px 10px">500</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=1000" style="font-size:11px;padding:6px 10px">1 000</button>\n<button class="btn gold" onclick="document.getElementById(\'rw-amt\').value=5000" style="font-size:11px;padding:6px 10px">5 000</button>\n</div>\n<div id="rw-msg" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n<div class="card purple">\n<div class="ct">◈ CHANGER LE RÔLE D\'UN UTILISATEUR</div>\n<div class="row">\n<select id="role-u" class="grow" style="margin:0"><option value="">Utilisateur...</option></select>\n<select id="role-v" style="width:auto;margin:0;flex-shrink:0;padding:12px 8px">\n<option value="user">user</option>\n<option value="admin">admin</option>\n<option value="moderator">moderator</option>\n<option value="vip">vip</option>\n</select>\n<button class="btn purple" onclick="changeRole()">✓</button>\n</div>\n<div id="role-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:6px"></div>\n</div>\n<div class="card">\n<div class="ct">◈ LISTE COMPLÈTE DES UTILISATEURS</div>\n<div class="sw" style="margin-bottom:8px"><input id="adm-q" placeholder="Rechercher..." oninput="filterAdmUsers()"></div>\n<div class="tbl-wrap">\n<table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur</th></tr></thead>\n<tbody id="adm-ut"></tbody></table>\n</div>\n</div>\n<div class="card red">\n<div class="ct">◈ ACTIONS DE MAINTENANCE</div>\n<button class="btn full" style="color:var(--gold);border-color:rgba(255,176,32,.3);background:rgba(255,176,32,.06)" onclick="pruneHistory()">✂️ Réduire historique NXC (100 pts)</button>\n<button class="btn full red" onclick="resetAllTrades()">🗑️ Reset trades 24h</button>\n<button class="btn full" style="color:var(--cyan);border-color:rgba(0,229,255,.3);background:rgba(0,229,255,.06)" onclick="backupDB()">💾 Backup base de données JSON</button>\n<button class="btn full" style="color:var(--purple);border-color:rgba(160,107,255,.3);background:rgba(160,107,255,.06)" onclick="pingServer()">📡 Ping serveur</button>\n<div id="maint-msg" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n<div class="card purple">\n<div class="ct">◈ LOGS SYSTÈME</div>\n<div style="display:flex;gap:6px;margin-bottom:8px">\n<button class="btn purple" onclick="renderLog()" style="font-size:11px">🔄 Actualiser</button>\n<button class="btn red" onclick="_log=[];renderLog()" style="font-size:11px">🗑️ Vider</button>\n</div>\n<div id="log-list" style="max-height:250px;overflow-y:auto;border-radius:10px;border:1px solid rgba(160,107,255,.1)">\n<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun log</p>\n</div>\n</div>\n</div>\n\n<!-- UTILISATEURS -->\n<div class="view" id="view-users">\n<div class="g3">\n<div class="st"><div class="sv" id="u-total">—</div><div class="sl">Comptes</div></div>\n<div class="st"><div class="sv gold" id="u-admins">—</div><div class="sl">Admins</div></div>\n<div class="st"><div class="sv green" id="u-rew">—</div><div class="sl">Total rewards</div></div>\n</div>\n<div class="card">\n<div class="ct">◈ UTILISATEURS\n<div style="display:flex;gap:4px">\n<button class="btn cyan" onclick="sortU(\'rew\')" style="padding:3px 7px;font-size:9px">Rewards</button>\n<button class="btn" onclick="sortU(\'nxc\')" style="padding:3px 7px;font-size:9px">NXC</button>\n<button class="btn" onclick="sortU(\'name\')" style="padding:3px 7px;font-size:9px">A-Z</button>\n</div>\n</div>\n<div class="sw" style="margin-bottom:8px"><input id="us-q" placeholder="Rechercher..." oninput="filterU()"></div>\n<div class="tbl-wrap">\n<table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur R</th></tr></thead>\n<tbody id="ut"></tbody></table>\n</div>\n<div id="us-msg" style="font-size:11px;color:var(--muted);margin-top:8px;text-align:center"></div>\n</div>\n</div>\n\n<!-- STATS -->\n<div class="view" id="view-stats">\n<div class="card purple"><div class="ct">◈ VOLUME 24H</div><div class="chart-wrap ch150"><canvas id="ch-vol"></canvas></div></div>\n<div class="card gold"><div class="ct">◈ REWARDS PAR UTILISATEUR</div><div id="rew-bars"></div></div>\n<div class="card"><div class="ct">◈ SANTÉ DU MARCHÉ</div><div class="g2" id="health-grid"></div></div>\n</div>\n\n<!-- SOLVABILITÉ -->\n<div class="view" id="view-solv">\n<div class="card">\n<div class="ct">◈ SOLVABILITÉ</div>\n<div style="display:flex;align-items:center;gap:14px;padding:14px;background:var(--bg3);border-radius:12px;margin-bottom:12px;cursor:pointer" onclick="toggleSolv()">\n<div class="tg" id="stg"><div class="tg-k"></div></div>\n<div id="sl" style="font-size:14px;font-weight:700;color:var(--muted)">Désactivée</div>\n</div>\n<div class="row" style="margin-bottom:8px">\n<span style="font-size:12px;color:var(--muted);white-space:nowrap;flex-shrink:0">Geste commercial :</span>\n<input id="sg" type="number" value="50" class="grow">\n<button class="btn primary" onclick="saveSolv()">Sauver</button>\n</div>\n<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:6px">\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=10" style="font-size:11px">10R</button>\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=50" style="font-size:11px">50R</button>\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=100" style="font-size:11px">100R</button>\n<button class="btn cyan" onclick="document.getElementById(\'sg\').value=500" style="font-size:11px">500R</button>\n</div>\n<div id="sm" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n</div>\n\n<!-- OUTILS -->\n<div class="view" id="view-tools">\n<div class="card cyan">\n<div class="ct">◈ CALCULATRICE NXC ↔ REWARDS</div>\n<div class="row" style="margin-bottom:8px">\n<input id="c-nxc" type="number" placeholder="NXC" class="grow" oninput="calcN()">\n<span style="color:var(--muted);font-size:18px">→</span>\n<input id="c-rew" type="number" placeholder="Rewards R" class="grow" readonly style="background:rgba(0,229,255,.05)">\n</div>\n<div class="row">\n<input id="c-rew2" type="number" placeholder="Rewards R" class="grow" oninput="calcR()">\n<span style="color:var(--muted);font-size:18px">→</span>\n<input id="c-nxc2" type="number" placeholder="NXC" class="grow" readonly style="background:rgba(0,229,255,.05)">\n</div>\n</div>\n<div class="card gold">\n<div class="ct">◈ SIMULATEUR DE VENTE</div>\n<div class="row" style="margin-bottom:8px">\n<input id="ss-nxc" type="number" placeholder="NXC à vendre" class="grow" oninput="simS()">\n<input id="ss-fee" type="number" placeholder="Frais %" value="0" style="width:90px;margin:0;flex-shrink:0" oninput="simS()">\n</div>\n<div id="ss-res" style="padding:12px;background:var(--bg3);border-radius:10px;min-height:44px;font-size:13px"></div>\n</div>\n<div class="card purple">\n<div class="ct">◈ MINUTEUR ADMIN</div>\n<div class="row" style="margin-bottom:8px">\n<input id="tm-m" type="number" placeholder="Min" value="5" class="grow">\n<input id="tm-s" type="number" placeholder="Sec" value="0" class="grow">\n<select id="tm-a" style="flex:1;margin:0;font-size:12px">\n<option value="stop">Arrêter tendance</option>\n<option value="up">Lancer hausse</option>\n<option value="down">Lancer baisse</option>\n<option value="crash">Crash -30%</option>\n<option value="moon">Moon +30%</option>\n</select>\n</div>\n<button class="btn cyan full" onclick="startTimer()">⏱️ Démarrer</button>\n<button class="btn full" style="color:var(--muted)" onclick="stopTimer()">✕ Annuler</button>\n<div id="tm-disp" style="font-family:monospace;font-size:36px;font-weight:900;color:var(--cyan);text-align:center;padding:10px;min-height:56px"></div>\n</div>\n<div class="card green">\n<div class="ct">◈ PING SERVEUR</div>\n<button class="btn green full" onclick="pingServer()">📡 Tester</button>\n<div id="ping-res" style="font-size:13px;font-weight:700;text-align:center;padding:10px;min-height:36px"></div>\n</div>\n</div>\n\n<!-- JOURNAL -->\n<div class="view" id="view-log">\n<div class="card">\n<div class="ct">◈ JOURNAL ADMIN <button class="btn red" onclick="_log=[];renderLog()" style="padding:3px 8px;font-size:9px">Vider</button></div>\n<div id="log-list2" style="max-height:500px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)">\n<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun log</p>\n</div>\n</div>\n</div>\n\n<!-- CONFIG -->\n<div class="view" id="view-config">\n<div class="card purple">\n<div class="ct">◈ PLANCHER / PLAFOND AUTOMATIQUES</div>\n<div class="row" style="margin-bottom:8px">\n<input id="cfg-fl" type="number" placeholder="Plancher min (R)" class="grow">\n<button class="btn purple" onclick="_cfgFloor=parseFloat(document.getElementById(\'cfg-fl\').value)||null;updCfg();updFloorDisplay()">✓ Plancher</button>\n<button class="btn red" onclick="_cfgFloor=null;updCfg();updFloorDisplay()" style="padding:10px">✕</button>\n</div>\n<div class="row" style="margin-bottom:8px">\n<input id="cfg-cl" type="number" placeholder="Plafond max (R)" class="grow">\n<button class="btn purple" onclick="_cfgCeil=parseFloat(document.getElementById(\'cfg-cl\').value)||null;updCfg();updFloorDisplay()">✓ Plafond</button>\n<button class="btn red" onclick="_cfgCeil=null;updCfg();updFloorDisplay()" style="padding:10px">✕</button>\n</div>\n<div id="cfg-info" style="font-size:11px;color:var(--muted);padding:8px;background:var(--bg3);border-radius:8px">Plancher: non défini · Plafond: non défini</div>\n</div>\n<div class="card gold">\n<div class="ct">◈ TENDANCE PROGRAMMÉE</div>\n<div class="row" style="margin-bottom:8px">\n<input id="cfg-st" type="time" class="grow">\n<input id="cfg-sp" type="time" class="grow">\n<select id="cfg-sd" style="flex:1;margin:0"><option value="up">Hausse</option><option value="down">Baisse</option><option value="random">Aléatoire</option></select>\n</div>\n<button class="btn gold full" onclick="scheduleT()">⏰ Programmer</button>\n<button class="btn full" style="color:var(--muted)" onclick="if(_schedInt){clearInterval(_schedInt);_schedInt=null;document.getElementById(\'cfg-sch-msg\').textContent=\'Annulé\';}">✕ Annuler</button>\n<div id="cfg-sch-msg" style="font-size:11px;font-weight:600;min-height:14px"></div>\n</div>\n<div class="card cyan">\n<div class="ct">◈ EXPORTS</div>\n<button class="btn cyan full" onclick="exportHist()">📥 Historique JSON</button>\n<button class="btn purple full" onclick="exportStats()">📊 Rapport complet JSON</button>\n<button class="btn gold full" onclick="exportFlux()">💰 Flux bancaires CSV</button>\n</div>\n</div>\n\n<!-- ALERTES -->\n<div class="view" id="view-notifs">\n<div class="card gold">\n<div class="ct">◈ ALERTES DE PRIX</div>\n<div class="row" style="margin-bottom:8px">\n<input id="al-p" type="number" placeholder="Prix cible (R)" class="grow">\n<select id="al-d" style="width:auto;flex-shrink:0;margin:0;font-size:12px;padding:10px 8px"><option value="above">Si &gt;</option><option value="below">Si &lt;</option></select>\n<button class="btn gold" onclick="addAlert()">+ Alerte</button>\n</div>\n<div id="al-list" style="max-height:200px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"><p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune alerte</p></div>\n</div>\n<div class="card"><div class="ct">◈ ALERTES INTELLIGENTES</div><div id="smart-al"></div></div>\n<div class="card purple">\n<div class="ct">◈ HISTORIQUE ALERTES <button class="btn red" onclick="_alHist=[];renderAlHist()" style="padding:3px 7px;font-size:9px">Vider</button></div>\n<div id="al-hist" style="max-height:200px;overflow-y:auto;border-radius:10px;border:1px solid rgba(0,229,255,.06)"><p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune</p></div>\n</div>\n</div>\n\n\n<!-- CYCLES DE MARCHÉ -->\n<div class="view" id="view-cycles">\n\n<!-- MODAL INFO -->\n<div id="info-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:500;align-items:center;justify-content:center;padding:20px" onclick="this.style.display=\'none\'">\n<div style="background:var(--bg2);border:1px solid var(--border);border-radius:16px;padding:20px;max-width:340px;width:100%" onclick="event.stopPropagation()">\n<div style="font-weight:700;color:var(--cyan);margin-bottom:10px;font-size:14px" id="info-title">Info</div>\n<div style="font-size:13px;color:var(--muted);line-height:1.7" id="info-body"></div>\n<button onclick="$(\'info-modal\').style.display=\'none\'" style="margin-top:14px;width:100%;padding:10px;background:var(--bg3);border:1px solid var(--border);border-radius:10px;color:var(--text);cursor:pointer;font-weight:700">Fermer</button>\n</div>\n</div>\n\n<div class="card cyan">\n<div class="ct">◈ BORNES DU NXC <button onclick="showInfo(\'bornes\')" style="background:none;border:1px solid rgba(0,229,255,.3);border-radius:50%;width:18px;height:18px;color:var(--cyan);font-size:9px;cursor:pointer;padding:0">i</button></div>\n<div class="g2">\n<div>\n<div class="sec">Prix minimum absolu (R)</div>\n<div class="row"><input id="cy-absmin" type="number" min="1" placeholder="Ex: 100" class="grow"><button class="btn cyan" onclick="setCyVal(\'absmin\')">✓</button></div>\n<div id="cy-absmin-disp" style="font-size:10px;color:var(--green);margin-top:2px">Non défini</div>\n</div>\n<div>\n<div class="sec">Prix maximum absolu (R)</div>\n<div class="row"><input id="cy-absmax" type="number" placeholder="Ex: 50000" class="grow"><button class="btn cyan" onclick="setCyVal(\'absmax\')">✓</button></div>\n<div id="cy-absmax-disp" style="font-size:10px;color:var(--red);margin-top:2px">Non défini</div>\n</div>\n</div>\n</div>\n\n<div class="card gold">\n<div class="ct">◈ FRÉQUENCE DES EXTRÊMES PAR PÉRIODE <button onclick="showInfo(\'freq\')" style="background:none;border:1px solid rgba(255,176,32,.3);border-radius:50%;width:18px;height:18px;color:var(--gold);font-size:9px;cursor:pointer;padding:0">i</button></div>\n<div style="font-size:11px;color:var(--muted);margin-bottom:12px;padding:8px;background:var(--bg3);border-radius:8px">\nDéfinir combien de fois le NXC touchera son <b style="color:var(--green)">minimum</b> ou son <b style="color:var(--red)">maximum</b> dans chaque période. Le moteur calcule automatiquement la probabilité par tick.\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:4px">\n<span style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">Période</span>\n<span style="font-size:9px;color:var(--green);text-transform:uppercase;letter-spacing:1px;text-align:center">× Min</span>\n<span style="font-size:9px;color:var(--red);text-transform:uppercase;letter-spacing:1px;text-align:center">× Max</span>\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">📅 Par minute <button onclick="showInfo(\'freq-min\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-m" type="number" min="0" value="0" placeholder="0" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-m" type="number" min="0" value="0" placeholder="0" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">🕐 Par heure <button onclick="showInfo(\'freq-h\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-h" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-h" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">📆 Par jour <button onclick="showInfo(\'freq-d\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-d" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-d" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">📅 Par semaine <button onclick="showInfo(\'freq-w\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-w" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-w" type="number" min="0" value="1" placeholder="1" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">🗓️ Par mois <button onclick="showInfo(\'freq-mo\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-mo" type="number" min="0" value="2" placeholder="2" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-mo" type="number" min="0" value="2" placeholder="2" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="display:grid;grid-template-columns:auto 1fr 1fr;gap:8px;align-items:center;margin-bottom:6px">\n<span style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap">📅 Par an <button onclick="showInfo(\'freq-y\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0;vertical-align:middle">i</button></span>\n<input id="cy-min-y" type="number" min="0" value="4" placeholder="4" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-y" type="number" min="0" value="4" placeholder="4" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n\n<div style="margin-top:10px;padding-top:10px;border-top:1px solid rgba(255,176,32,.15)">\n<div style="font-size:10px;color:var(--muted);margin-bottom:6px;display:flex;align-items:center;gap:6px">Durée personnalisée <button onclick="showInfo(\'freq-custom\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:grid;grid-template-columns:auto auto 1fr 1fr;gap:8px;align-items:center">\n<input id="cy-custom-dur" type="number" placeholder="X" style="width:60px;padding:8px;margin:0;text-align:center">\n<select id="cy-custom-unit" style="width:auto;margin:0;font-size:11px;padding:8px 6px">\n<option value="60000">min</option>\n<option value="3600000" selected>h</option>\n<option value="86400000">j</option>\n<option value="604800000">sem</option>\n</select>\n<input id="cy-min-c" type="number" min="0" value="0" placeholder="Min" style="text-align:center;padding:8px;font-size:13px;margin:0">\n<input id="cy-max-c" type="number" min="0" value="0" placeholder="Max" style="text-align:center;padding:8px;font-size:13px;margin:0">\n</div>\n</div>\n\n<button class="btn gold" onclick="updateCyProb()" style="width:100%;margin-top:12px;padding:10px">🔄 Calculer les probabilités par tick</button>\n<div id="cy-prob-display" style="font-size:11px;color:var(--muted);margin-top:8px;padding:8px;background:var(--bg3);border-radius:8px;line-height:1.8"></div>\n</div>\n\n<div class="card purple">\n<div class="ct">◈ COMPORTEMENT DES CYCLES <button onclick="showInfo(\'comportement\')" style="background:none;border:1px solid rgba(160,107,255,.3);border-radius:50%;width:18px;height:18px;color:var(--purple);font-size:9px;cursor:pointer;padding:0">i</button></div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Transition vers extrême <button onclick="showInfo(\'transition\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<select id="cy-transition" style="margin-bottom:10px">\n<option value="brutal">Brutal (saut immédiat)</option>\n<option value="progressif" selected>Progressif (descente/montée graduelle)</option>\n<option value="sinusoide">Sinusoïde (courbe naturelle)</option>\n</select>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Durée de maintien au min/max <button onclick="showInfo(\'hold\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div class="row" style="margin-bottom:10px">\n<input id="cy-hold-min" type="number" min="0" value="1" placeholder="Min" style="width:70px;flex-shrink:0;margin:0">\n<span style="color:var(--muted);font-size:12px;flex-shrink:0">à</span>\n<input id="cy-hold-max" type="number" min="0" value="3" placeholder="Max" style="width:70px;flex-shrink:0;margin:0">\n<select id="cy-hold-unit" style="flex:1;margin:0;font-size:12px;padding:10px 8px">\n<option value="1">ticks</option>\n<option value="5" selected>minutes</option>\n<option value="300">heures</option>\n</select>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Drift de fond (tendance long terme) <button onclick="showInfo(\'drift\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="cy-drift" type="range" min="-5" max="5" value="0" step="0.5" oninput="$(\'cy-drift-val\').textContent=this.value>0?\'+\'+this.value+\'%/j\':this.value+\'%/j\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--purple)">\n<span id="cy-drift-val" style="color:var(--purple);font-weight:700;font-size:13px;width:60px;text-align:right;flex-shrink:0">0%/j</span>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Volatilité de fond <button onclick="showInfo(\'volbg\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="cy-vol-bg" type="range" min="0" max="10" value="2" oninput="$(\'cy-vol-bg-val\').textContent=(this.value/10).toFixed(1)+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--cyan)">\n<span id="cy-vol-bg-val" style="color:var(--cyan);font-weight:700;font-size:13px;width:40px;text-align:right;flex-shrink:0">0.2%</span>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Probabilité de pic surprise <button onclick="showInfo(\'spike\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="cy-spike" type="range" min="0" max="20" value="2" oninput="$(\'cy-spike-val\').textContent=this.value+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--red)">\n<span id="cy-spike-val" style="color:var(--red);font-weight:700;font-size:13px;width:32px;text-align:right;flex-shrink:0">2%</span>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Amplitude des pics <button onclick="showInfo(\'spikeamp\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="cy-spike-amp" type="range" min="1" max="30" value="10" oninput="$(\'cy-spike-amp-val\').textContent=\'±\'+this.value+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--red)">\n<span id="cy-spike-amp-val" style="color:var(--red);font-weight:700;font-size:13px;width:40px;text-align:right;flex-shrink:0">±10%</span>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Rebond au plancher <button onclick="showInfo(\'bounce\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">\n<input id="cy-bounce" type="range" min="0" max="10" value="3" oninput="$(\'cy-bounce-val\').textContent=this.value+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--green)">\n<span id="cy-bounce-val" style="color:var(--green);font-weight:700;font-size:13px;width:32px;text-align:right;flex-shrink:0">3%</span>\n</div>\n\n<div class="sec" style="display:flex;align-items:center;gap:6px">Résistance au plafond <button onclick="showInfo(\'resist\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:16px;height:16px;color:var(--cyan);font-size:8px;cursor:pointer;padding:0">i</button></div>\n<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">\n<input id="cy-resist" type="range" min="0" max="10" value="3" oninput="$(\'cy-resist-val\').textContent=this.value+\'%\'" style="flex:1;margin:0;background:none;border:none;padding:6px 0;accent-color:var(--gold)">\n<span id="cy-resist-val" style="color:var(--gold);font-weight:700;font-size:13px;width:32px;text-align:right;flex-shrink:0">3%</span>\n</div>\n</div>\n\n<div class="card green">\n<div class="ct">◈ ACTIVATION <button onclick="showInfo(\'activation\')" style="background:none;border:1px solid rgba(0,255,157,.3);border-radius:50%;width:18px;height:18px;color:var(--green);font-size:9px;cursor:pointer;padding:0">i</button></div>\n<button class="btn green full" onclick="startCycles()" id="cy-start-btn">▶ Activer les cycles de marché</button>\n<button class="btn red full" onclick="stopCycles()" style="display:none" id="cy-stop-btn">⏸ Désactiver les cycles</button>\n<div id="cy-status" style="font-size:12px;padding:10px;background:var(--bg3);border-radius:10px;color:var(--muted);min-height:40px">Cycles désactivés</div>\n<div id="cy-next" style="font-size:11px;color:var(--muted);margin-top:6px"></div>\n</div>\n\n<div class="card">\n<div class="ct">◈ PRÉVISUALISATION <button onclick="showInfo(\'preview\')" style="background:none;border:1px solid rgba(0,229,255,.2);border-radius:50%;width:18px;height:18px;color:var(--cyan);font-size:9px;cursor:pointer;padding:0">i</button></div>\n<div class="chart-wrap ch150"><canvas id="cy-preview"></canvas></div>\n<button class="btn cyan" onclick="previewCycle()" style="width:100%;margin-top:8px;padding:10px">🔮 Générer prévisualisation (100 ticks simulés)</button>\n</div>\n</div>\n</div><!-- end content -->\n\n<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js">\n// ══ ÉPINGLAGE SITES (sync cross-device via serveur) ══\nvar _pinnedSites=[];\n\nasync function loadPinnedSites(){\n  try{\n    var r=await fetch(\'/admin/pinned-sites\');var d=await r.json();\n    if(d.ok){_pinnedSites=d.sites||[];renderSavedSites();}\n  }catch(e){_pinnedSites=JSON.parse(localStorage.getItem(\'nxc_pinned\')||\'[]\');}\n}\n\nasync function togglePin(url,label){\n  var idx=_pinnedSites.findIndex(s=>s.url===url);\n  if(idx>=0)_pinnedSites.splice(idx,1);\n  else _pinnedSites.push({url,label});\n  // Sauvegarder sur le serveur ET en local\n  localStorage.setItem(\'nxc_pinned\',JSON.stringify(_pinnedSites));\n  try{await fetch(\'/admin/pinned-sites\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,sites:_pinnedSites})});}catch(e){}\n  renderSavedSites();\n  addLog(\'📌\',(idx>=0?\'Désépinglé\':\'Épinglé\')+\': \'+label);\n}\n\nfunction renderPinnedBar(){\n  var el=$(\'pinned-bar\');if(!el)return;\n  if(!_pinnedSites.length){el.style.display=\'none\';return;}\n  el.style.display=\'flex\';\n  el.innerHTML=_pinnedSites.map(s=>\'<button onclick="loadSite(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" style="padding:5px 12px;background:rgba(255,176,32,.12);border:1px solid rgba(255,176,32,.3);border-radius:8px;color:var(--gold);font-size:11px;font-weight:700;cursor:pointer;white-space:nowrap">📌 \'+esc(s.label)+\'</button>\').join(\'\');\n}\n\n// ══ SAUVEGARDE / IMPORT DONNÉES GLOBALES ══\nasync function saveAllData(){\n  try{\n    var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'export\'})});\n    var d=await r.json();\n    if(!d.ok){setMsg(\'data-msg\',\'❌ Erreur export\',false);return;}\n    var blob=new Blob([JSON.stringify(d.data,null,2)],{type:\'application/json\'});\n    var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_full_backup_\'+Date.now()+\'.json\';a.click();\n    setMsg(\'data-msg\',\'✅ Backup complet téléchargé\',true);\n    addLog(\'💾\',\'Sauvegarde complète téléchargée\');\n  }catch(e){setMsg(\'data-msg\',\'❌ Erreur: \'+e.message,false);}\n}\n\nfunction importData(){\n  var input=document.createElement(\'input\');input.type=\'file\';input.accept=\'.json\';\n  input.onchange=async function(e){\n    var file=e.target.files[0];if(!file)return;\n    var text=await file.text();\n    try{\n      var data=JSON.parse(text);\n      if(!confirm(\'Importer ces données ? Cela écrasera les données actuelles du serveur.\'))return;\n      var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'import\',data:data})});\n      var res=await r.json();\n      setMsg(\'data-msg\',res.ok?\'✅ Données importées avec succès\':\'❌ \'+(res.error||\'Erreur import\'),res.ok);\n      if(res.ok){addLog(\'📥\',\'Données importées depuis fichier\');setTimeout(function(){ref();loadBank();},1000);}\n    }catch(ex){setMsg(\'data-msg\',\'❌ Fichier JSON invalide\',false);}\n  };\n  input.click();\n}\n\n// ══ IMPRESSION ══\nfunction printDashboard(){\n  var p=parseFloat(mkt.price||0);var h=mkt.history||[];\n  var hi=h.length>1?Math.max(...h.slice(-24).map(x=>x.price)):p;\n  var lo=h.length>1?Math.min(...h.slice(-24).map(x=>x.price)):p;\n  var chg=_prevP>0?((p-_prevP)/_prevP*100):0;\n  // Capturer le graphique en PNG\n  function _snap(id){var cv=$(id);if(!cv||!cv.width||!cv.height)return \'\';try{var tmp=document.createElement(\'canvas\');tmp.width=cv.width;tmp.height=cv.height;var x=tmp.getContext(\'2d\');x.fillStyle=\'#ffffff\';x.fillRect(0,0,tmp.width,tmp.height);x.drawImage(cv,0,0);return tmp.toDataURL(\'image/png\');}catch(e){return \'\';}}\n  var chartImg=_snap(\'ch\');\n  var rsiImg=_snap(\'ch-rsi\');\n  var now=new Date().toLocaleString(\'fr-FR\');\n  var win=window.open(\'\',\'_blank\');\n  if(!win){alert(\'Autorise les pop-ups pour imprimer le rapport.\');return;}\n  win.document.write(\'<!DOCTYPE html><html><head><meta charset="utf-8"><title>◈ Nexus NXC — Rapport \'+now+\'</title><style>*{font-family:Arial,sans-serif;box-sizing:border-box}body{background:#fff;color:#000;padding:20px;max-width:900px;margin:0 auto}.header{text-align:center;border-bottom:3px solid #000;padding-bottom:16px;margin-bottom:20px}.title{font-size:28px;font-weight:900;letter-spacing:3px}.date{font-size:12px;color:#666;margin-top:4px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}.stat{border:1px solid #ddd;border-radius:8px;padding:12px;text-align:center}.stat-val{font-size:20px;font-weight:700;margin-bottom:4px}.stat-lbl{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:#666}img{max-width:100%;border:1px solid #ddd;border-radius:8px;margin-bottom:12px}h3{margin:16px 0 8px;font-size:14px;border-bottom:1px solid #eee;padding-bottom:4px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:8px;text-align:left;border:1px solid #ddd}th{background:#f5f5f5;font-weight:700}@media print{.no-print{display:none}}</style></head><body>\');\n  win.document.write(\'<div class="header"><div class="title">◈ NEXUS NXC</div><div class="date">Rapport généré le \'+now+\'</div></div>\');\n  win.document.write(\'<div class="grid">\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(p,2)+\' R</div><div class="stat-lbl">Prix actuel</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%</div><div class="stat-lbl">Variation</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(hi,0)+\' R</div><div class="stat-lbl">Haut 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(lo,0)+\' R</div><div class="stat-lbl">Bas 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(mkt.volume24||0,0)+\' R</div><div class="stat-lbl">Volume 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(mkt.trades24||0)+\'</div><div class="stat-lbl">Trades 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+h.length+\'</div><div class="stat-lbl">Points hist.</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(_users.length||0)+\'</div><div class="stat-lbl">Utilisateurs</div></div>\');\n  win.document.write(\'</div>\');\n  if(chartImg)win.document.write(\'<h3>Historique du cours (\'+_ctRange+\' derniers points)</h3><img src="\'+chartImg+\'">\');\n  if(rsiImg)win.document.write(\'<h3>RSI (14 ticks)</h3><img src="\'+rsiImg+\'">\');\n  if(_users.length){\n    win.document.write(\'<h3>Utilisateurs</h3><table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur (R)</th></tr></thead><tbody>\');\n    _users.forEach(u=>{win.document.write(\'<tr><td>\'+esc(u.n)+\'</td><td>\'+esc(u.role)+\'</td><td>\'+fmt(u.rew,0)+\'</td><td>\'+u.nxc.toFixed(4)+\'</td><td>\'+fmt(u.val,0)+\'</td></tr>\');});\n    win.document.write(\'</tbody></table>\');\n  }\n  win.document.write(\'<h3>Derniers logs</h3><table><thead><tr><th>Heure</th><th>Action</th></tr></thead><tbody>\');\n  _log.slice(0,20).forEach(l=>{win.document.write(\'<tr><td>\'+fmtT(l.ts)+\'</td><td>\'+l.ico+\' \'+esc(l.txt)+\'</td></tr>\');});\n  win.document.write(\'</tbody></table>\');\n    var bR=document.getElementById(\'bk-r\')?document.getElementById(\'bk-r\').textContent:\'—\';\n  var bI=document.getElementById(\'bk-i\')?document.getElementById(\'bk-i\').textContent:\'—\';\n  var bO=document.getElementById(\'bk-o\')?document.getElementById(\'bk-o\').textContent:\'—\';\n  var bRt=document.getElementById(\'bk-rt\')?document.getElementById(\'bk-rt\').textContent:\'—\';\n  var bNx=document.getElementById(\'bk-nx\')?document.getElementById(\'bk-nx\').textContent:\'—\';\n  var bVx=document.getElementById(\'bk-vx\')?document.getElementById(\'bk-vx\').textContent:\'—\';\n  var bBn=document.getElementById(\'bk-bn\')?document.getElementById(\'bk-bn\').textContent:\'—\';\n  var bFl=document.getElementById(\'bk-fl\')?document.getElementById(\'bk-fl\').textContent:\'—\';\n  win.document.write(\'<hr style="margin:20px 0;border:none;border-top:2px solid #6366f1">\')\n  win.document.write(\'<h2 style="font-family:monospace;color:#6366f1;margin:0 0 12px">◈ BANQUE NXC</h2>\')\n  win.document.write(\'<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px">\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Réserves</div><div style="font-weight:bold">\'+ bR +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Entrées</div><div style="font-weight:bold">\'+ bI +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Sorties</div><div style="font-weight:bold">\'+ bO +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Taux</div><div style="font-weight:bold">\'+ bRt +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">NXC circ.</div><div style="font-weight:bold">\'+ bNx +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Valeur NXC</div><div style="font-weight:bold">\'+ bVx +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Billets</div><div style="font-weight:bold">\'+ bBn +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Flux total</div><div style="font-weight:bold">\'+ bFl +\'</div></div>\')\n  win.document.write(\'</div>\')\n  if(typeof _flux!==\'undefined\'&&_flux&&_flux.length>0){\n    win.document.write(\'<h3 style="font-family:monospace;margin:0 0 8px">Flux récents</h3>\')\n    win.document.write(\'<table style="width:100%;border-collapse:collapse;font-size:11px"><thead><tr style="background:#6366f1;color:#fff"><th style="padding:4px 8px;text-align:left">Date</th><th>Type</th><th>Utilisateur</th><th>Montant</th><th>Solde</th></tr></thead><tbody>\')\n    _flux.slice(0,50).forEach(function(f){\n      var fd=new Date(f.ts).toLocaleString(\'fr-FR\');\n      win.document.write(\'<tr style="border-bottom:1px solid #eee"><td style="padding:3px 8px">\'+fd+\'</td><td>\'+esc(f.type||\'\')+ \'</td><td>\'+esc(f.user||\'\')+ \'</td><td>\'+esc(String(f.amount||\'\'))+ \'</td><td>\'+esc(String(f.balance||\'\'))+ \'</td></tr>\')\n    });\n    win.document.write(\'</tbody></table>\')\n  }\n  win.document.write(\'</body></html>\');\n  win.document.close();\n  setTimeout(function(){win.print();},500);\n  addLog(\'🖨️\',\'Impression du tableau de bord\');\n}\n\n\n// ══ CYCLES DE MARCHÉ ══\nvar _cy={absmin:null,absmax:null,active:false,int:null,phase:\'normal\',phaseStart:Date.now(),holdUntil:0};\nvar _cyPreviewObj=null;\n\nfunction setCyVal(key){\n  var v=parseFloat($(\'cy-\'+key).value);\n  if(isNaN(v)||v<=0)return;\n  _cy[key]=v;\n  var el=$(\'cy-\'+key+\'-disp\');if(el)el.textContent=fmt(v,0)+\' R\';\n  // Sync avec _cfgFloor/_cfgCeil\n  if(key===\'absmin\'){_cfgFloor=v;updFloorDisplay();}\n  if(key===\'absmax\'){_cfgCeil=v;updFloorDisplay();}\n  addLog(\'📅\',\'Borne \'+key+\': \'+fmt(v,0)+\' R\');\n}\n\nfunction getCyConfig(){\n  return {\n    absmin: _cy.absmin||parseFloat($(\'cy-absmin\').value)||50,\n    absmax: _cy.absmax||parseFloat($(\'cy-absmax\').value)||100000,\n    transition: $(\'cy-transition\').value,\n    holdMin: parseFloat($(\'cy-hold-min\').value)||1,\n    holdMax: parseFloat($(\'cy-hold-max\').value)||3,\n    holdUnit: parseFloat($(\'cy-hold-unit\').value)||60,\n    drift: parseFloat($(\'cy-drift\').value)/100/1440,\n    volBg: parseFloat($(\'cy-vol-bg\').value)/1000,\n    spikeProb: parseFloat($(\'cy-spike\').value)/100,\n    spikeAmp: parseFloat($(\'cy-spike-amp\').value)/100,\n    bounce: parseFloat($(\'cy-bounce\').value)/100,\n    resist: parseFloat($(\'cy-resist\').value)/100,\n    // Fréquences par période → probabilité par tick (tick = 12s)\n    freqMin: {\n      m: parseFloat($(\'cy-min-m\').value)||0,\n      h: parseFloat($(\'cy-min-h\').value)||1,\n      d: parseFloat($(\'cy-min-d\').value)||1,\n      w: parseFloat($(\'cy-min-w\').value)||1,\n      mo: parseFloat($(\'cy-min-mo\').value)||2,\n      y: parseFloat($(\'cy-min-y\').value)||4,\n    },\n    freqMax: {\n      m: parseFloat($(\'cy-max-m\').value)||0,\n      h: parseFloat($(\'cy-max-h\').value)||1,\n      d: parseFloat($(\'cy-max-d\').value)||1,\n      w: parseFloat($(\'cy-max-w\').value)||1,\n      mo: parseFloat($(\'cy-max-mo\').value)||2,\n      y: parseFloat($(\'cy-max-y\').value)||4,\n    },\n  };\n}\n\nfunction calcProbPerTick(freqObj){\n  // Convertir les fréquences en probabilité par tick (12s)\n  var ticksPerMin=5,ticksPerH=300,ticksPerD=7200,ticksPerW=50400,ticksPerMo=216000,ticksPerY=2628000;\n  var pMin=freqObj.m/ticksPerMin+freqObj.h/ticksPerH+freqObj.d/ticksPerD+freqObj.w/ticksPerW+freqObj.mo/ticksPerMo+freqObj.y/ticksPerY;\n  return Math.min(pMin,0.5); // max 50% par tick\n}\n\nfunction startCycles(){\n  var cfg=getCyConfig();\n  if(cfg.absmin>=cfg.absmax){alert(\'Le plancher doit être inférieur au plafond\');return;}\n  _cy.active=true;_cy.phase=\'normal\';_cy.holdUntil=0;\n  $(\'cy-start-btn\').style.display=\'none\';$(\'cy-stop-btn\').style.display=\'block\';\n  var iv=parseInt($(\'ti\').value)||12000;\n  if(tInt){clearInterval(tInt);tInt=null;}\n  tMode=\'cycles\';\n  var el=$(\'tst\');el.textContent=\'📅 Cycles actifs · \'+fmt(cfg.absmin,0)+\'R – \'+fmt(cfg.absmax,0)+\'R\';el.style.color=\'var(--cyan)\';\n  addLog(\'📅\',\'Cycles de marché activés\');\n\n  var pToMin=calcProbPerTick(cfg.freqMin);\n  var pToMax=calcProbPerTick(cfg.freqMax);\n\n  updateCyProb();var _e=$(\'tst\');if(_e){_e.textContent=\'📅 Cycles serveur actifs (bornes + extrêmes)\';_e.style.color=\'var(--cyan)\';}addLog(\'📅\',\'Cycles → serveur\');return;\n  _cy.int=setInterval(async function(){\n    var p=parseFloat(mkt.price||5213);\n    var now=Date.now();\n    var adj=0;\n\n    // Drift de fond\n    adj+=cfg.drift;\n    // Volatilité de fond\n    adj+=(Math.random()-0.5)*cfg.volBg*2;\n\n    // Pics surprises\n    if(Math.random()<cfg.spikeProb){\n      var dir=Math.random()>0.5?1:-1;\n      adj+=dir*cfg.spikeAmp*(Math.random()*0.5+0.5);\n      addLog(\'⚡\',\'Pic surprise: \'+(dir>0?\'+\':\'\')+((adj*100).toFixed(1))+\'%\');\n    }\n\n    // Gestion des phases\n    if(now<_cy.holdUntil){\n      // Maintien en position (min ou max)\n      if(_cy.phase===\'atmin\')adj=Math.max(0,(Math.random()-0.3)*0.001);\n      if(_cy.phase===\'atmax\')adj=Math.min(0,(Math.random()-0.7)*0.001);\n    } else {\n      // Décider si on va vers le min ou le max\n      if(_cy.phase!==\'tomin\'&&_cy.phase!==\'tomax\'){\n        var goMin=Math.random()<pToMin;\n        var goMax=Math.random()<pToMax;\n        if(goMin&&!goMax){_cy.phase=\'tomin\';addLog(\'📅\',\'Cycle → minimum\');}\n        else if(goMax&&!goMin){_cy.phase=\'tomax\';addLog(\'📅\',\'Cycle → maximum\');}\n        else _cy.phase=\'normal\';\n      }\n      if(_cy.phase===\'tomin\'){\n        // Descente vers le min\n        var distRatio=(p-cfg.absmin)/(cfg.absmax-cfg.absmin);\n        var force=cfg.transition===\'brutal\'?-0.1:cfg.transition===\'sinusoide\'?-Math.sin(distRatio*Math.PI)*0.02:-0.01;\n        adj+=force*(1+cfg.bounce);\n        if(p<=cfg.absmin*1.01){_cy.phase=\'atmin\';var holdSec=(cfg.holdMin+Math.random()*(cfg.holdMax-cfg.holdMin))*cfg.holdUnit;_cy.holdUntil=now+holdSec*1000;addLog(\'📅\',\'Cycle: minimum atteint · maintien \'+(holdSec/60).toFixed(0)+\'min\');}\n      }\n      if(_cy.phase===\'tomax\'){\n        // Montée vers le max\n        var distRatio=(cfg.absmax-p)/(cfg.absmax-cfg.absmin);\n        var force=cfg.transition===\'brutal\'?0.1:cfg.transition===\'sinusoide\'?Math.sin(distRatio*Math.PI)*0.02:0.01;\n        adj+=force*(1+cfg.resist);\n        if(p>=cfg.absmax*0.99){_cy.phase=\'atmax\';var holdSec=(cfg.holdMin+Math.random()*(cfg.holdMax-cfg.holdMin))*cfg.holdUnit;_cy.holdUntil=now+holdSec*1000;addLog(\'📅\',\'Cycle: maximum atteint · maintien \'+(holdSec/60).toFixed(0)+\'min\');}\n      }\n    }\n\n    // Résistance aux bornes\n    if(p<cfg.absmin*1.05)adj+=cfg.bounce*0.05;\n    if(p>cfg.absmax*0.95)adj-=cfg.resist*0.05;\n\n    p=Math.max(cfg.absmin,Math.min(cfg.absmax,p*(1+adj)));\n    p=Math.round(p*100)/100;\n\n    // Mise à jour du statut\n    var rem=Math.max(0,Math.round((_cy.holdUntil-now)/1000));\n    var statusTxt=\'Phase: \'+_cy.phase+(_cy.holdUntil>now?\' · maintien encore \'+rem+\'s\':\'\')+\' · P(min)/tick: \'+(pToMin*100).toFixed(2)+\'% · P(max)/tick: \'+(pToMax*100).toFixed(2)+\'%\';\n    var st=$(\'cy-status\');if(st)st.textContent=statusTxt;\n\n    await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:Math.floor(Math.random()*200+20),volume24:(mkt.volume24||0)+80,trades24:(mkt.trades24||0)+1})});\n  },iv);\n}\n\nfunction stopCycles(){\n  _cy.active=false;_cy.phase=\'normal\';\n  if(_cy.int){clearInterval(_cy.int);_cy.int=null;}\n  if(tMode===\'cycles\'){tMode=null;tInt=null;}\n  $(\'cy-start-btn\').style.display=\'block\';$(\'cy-stop-btn\').style.display=\'none\';\n  var el=$(\'cy-status\');if(el)el.textContent=\'Cycles désactivés\';\n  var el2=$(\'tst\');if(el2){el2.textContent=\'⏸ Arrêté\';el2.style.color=\'var(--muted)\';}\n  addLog(\'📅\',\'Cycles désactivés\');\n}\n\nfunction previewCycle(){\n  var cfg=getCyConfig();var cv=$(\'cy-preview\');if(!cv||!window.Chart)return;\n  if(_cyPreviewObj){_cyPreviewObj.destroy();_cyPreviewObj=null;}\n  var pts=[];var p=(cfg.absmin+cfg.absmax)/2;\n  var pMin=calcProbPerTick(cfg.freqMin);var pMax=calcProbPerTick(cfg.freqMax);\n  var phase=\'normal\';var holdUntil=0;\n  for(var t=0;t<100;t++){\n    var adj=(Math.random()-0.5)*cfg.volBg*2+cfg.drift;\n    if(Math.random()<cfg.spikeProb)adj+=(Math.random()>0.5?1:-1)*cfg.spikeAmp*Math.random();\n    if(t>holdUntil){\n      if(phase!==\'tomin\'&&phase!==\'tomax\'){\n        if(Math.random()<pMin)phase=\'tomin\';\n        else if(Math.random()<pMax)phase=\'tomax\';\n        else phase=\'normal\';\n      }\n      if(phase===\'tomin\'){adj-=0.01*(1+cfg.bounce);if(p<=cfg.absmin*1.01){phase=\'atmin\';holdUntil=t+3;}}\n      if(phase===\'tomax\'){adj+=0.01*(1+cfg.resist);if(p>=cfg.absmax*0.99){phase=\'atmax\';holdUntil=t+3;}}\n    }\n    p=Math.max(cfg.absmin,Math.min(cfg.absmax,p*(1+adj)));\n    pts.push(Math.round(p*100)/100);\n  }\n  var labs=pts.map((_,i)=>\'T\'+i);\n  var ctx=cv.getContext(\'2d\');\n  var g=ctx.createLinearGradient(0,0,0,150);g.addColorStop(0,\'rgba(0,229,255,.2)\');g.addColorStop(1,\'rgba(0,229,255,0)\');\n  _cyPreviewObj=new Chart(ctx,{type:\'line\',data:{labels:labs,datasets:[\n    {data:pts,borderColor:\'#00e5ff\',backgroundColor:g,borderWidth:2,pointRadius:0,fill:true,tension:0.3},\n    {data:Array(100).fill(cfg.absmin),borderColor:\'rgba(0,255,157,.4)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[4,4]},\n    {data:Array(100).fill(cfg.absmax),borderColor:\'rgba(255,61,94,.4)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[4,4]},\n  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{display:false},y:{ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n}\n\n\n// ══ INFOS BULLES ══\nvar _infos={\n  bornes:"Les bornes sont les limites absolues du prix NXC. Le prix ne pourra jamais descendre en dessous du minimum ni monter au-dessus du maximum, quoi qu\'il arrive.",\n  freq:"Définit combien de fois le prix touchera exactement son minimum ou maximum dans chaque période. Le moteur calcule automatiquement la probabilité par tick (intervalle de 12s par défaut) pour respecter ces fréquences.",\n  "freq-min":"Par minute : combien de fois dans la prochaine minute le prix touchera son minimum (colonne verte) ou maximum (colonne rouge). 0 = jamais dans la minute.",\n  "freq-h":"Par heure : combien de fois dans la prochaine heure le prix touchera son minimum ou maximum. Ex: 2 = deux fois dans l\'heure.",\n  "freq-d":"Par jour : combien de fois dans les 24 prochaines heures le prix touchera son minimum ou maximum.",\n  "freq-w":"Par semaine : combien de fois dans les 7 prochains jours le prix touchera son minimum ou maximum.",\n  "freq-mo":"Par mois (30 jours) : combien de fois dans le mois le prix touchera son minimum ou maximum.",\n  "freq-y":"Par an (365 jours) : combien de fois dans l\'année le prix touchera son minimum ou maximum. Ex: 4 = une fois par trimestre.",\n  "freq-custom":"Durée personnalisée : définir une période sur mesure. Ex: 6 heures, 2 jours... et combien de fois le prix touchera les extrêmes dans cette durée.",\n  comportement:"Paramètres qui définissent comment le prix se comporte quand il se déplace vers un extrême.",\n  transition:"Comment le prix atteint le min ou le max. Brutal = saut instantané. Progressif = descente/montée sur plusieurs ticks. Sinusoïde = courbe douce et naturelle.",\n  hold:"Combien de temps le prix reste au minimum ou maximum avant de repartir. Une durée aléatoire entre Min et Max est choisie à chaque fois.",\n  drift:"Tendance de fond sur le long terme. +2%/j = le prix a une légère tendance à monter de 2% par jour en moyenne. 0 = aucune tendance.",\n  volbg:"Quantité de mouvement aléatoire à chaque tick, indépendant des cycles. 0% = prix totalement lisse entre les cycles. Plus élevé = plus de micro-variations.",\n  spike:"Probabilité qu\'un pic inattendu se produise à chaque tick. Ex: 5% = 1 chance sur 20 à chaque tick d\'avoir un mouvement brutal.",\n  spikeamp:"Amplitude maximale d\'un pic surprise. ±10% = le pic peut faire bouger le prix de jusqu\'à 10% instantanément.",\n  bounce:"Force du rebond quand le prix touche le plancher. 0% = s\'arrête exactement au plancher. 5% = rebondit légèrement vers le haut.",\n  resist:"Résistance quand le prix approche du plafond. 0% = monte jusqu\'au plafond facilement. 5% = plus difficile de dépasser le plafond.",\n  activation:"Active le moteur de cycles. Une fois activé, le prix suivra automatiquement les fréquences définies pour atteindre les extrêmes.",\n  preview:"Simule 100 ticks avec les paramètres actuels pour voir à quoi ressemblera le comportement du prix avant de l\'activer."\n};\n\nfunction showInfo(key){\n  var modal=$(\'info-modal\');if(!modal)return;\n  $(\'info-title\').textContent=\'ℹ️ \'+key.replace(/-/g,\' \').replace(/\\b\\w/g,c=>c.toUpperCase());\n  $(\'info-body\').textContent=_infos[key]||\'Information non disponible.\';\n  modal.style.display=\'flex\';\n}\n\n// ══ PROBABILITÉS PAR TICK ══\nfunction updateCyProb(){\n  var ticksPerMin=5,ticksPerH=300,ticksPerD=7200,ticksPerW=50400,ticksPerMo=216000,ticksPerY=2628000;\n  var customDur=parseFloat($(\'cy-custom-dur\').value)||0;\n  var customUnit=parseFloat($(\'cy-custom-unit\').value)||3600000;\n  var customMs=customDur*customUnit;\n  var customTicks=customMs/12000;\n\n  var freqMin={m:parseFloat($(\'cy-min-m\').value)||0,h:parseFloat($(\'cy-min-h\').value)||0,d:parseFloat($(\'cy-min-d\').value)||0,w:parseFloat($(\'cy-min-w\').value)||0,mo:parseFloat($(\'cy-min-mo\').value)||0,y:parseFloat($(\'cy-min-y\').value)||0,c:parseFloat($(\'cy-min-c\').value)||0};\n  var freqMax={m:parseFloat($(\'cy-max-m\').value)||0,h:parseFloat($(\'cy-max-h\').value)||0,d:parseFloat($(\'cy-max-d\').value)||0,w:parseFloat($(\'cy-max-w\').value)||0,mo:parseFloat($(\'cy-max-mo\').value)||0,y:parseFloat($(\'cy-max-y\').value)||0,c:parseFloat($(\'cy-max-c\').value)||0};\n\n  var pMin=freqMin.m/ticksPerMin+freqMin.h/ticksPerH+freqMin.d/ticksPerD+freqMin.w/ticksPerW+freqMin.mo/ticksPerMo+freqMin.y/ticksPerY+(customTicks>0?freqMin.c/customTicks:0);\n  var pMax=freqMax.m/ticksPerMin+freqMax.h/ticksPerH+freqMax.d/ticksPerD+freqMax.w/ticksPerW+freqMax.mo/ticksPerMo+freqMax.y/ticksPerY+(customTicks>0?freqMax.c/customTicks:0);\n\n  pMin=Math.min(pMin,0.8);pMax=Math.min(pMax,0.8);\n\n  // Estimation des fréquences résultantes\n  var estPerH_min=Math.round(pMin*ticksPerH*10)/10;\n  var estPerH_max=Math.round(pMax*ticksPerH*10)/10;\n  var estPerD_min=Math.round(pMin*ticksPerD);\n  var estPerD_max=Math.round(pMax*ticksPerD);\n\n  var el=$(\'cy-prob-display\');if(!el)return;\n  el.innerHTML=\n    \'<b style="color:var(--green)">MIN</b> — probabilité/tick: <b>\'+(pMin*100).toFixed(3)+\'%</b> · ~\'+estPerH_min+\'/heure · ~\'+estPerD_min+\'/jour<br>\'\n    +\'<b style="color:var(--red)">MAX</b> — probabilité/tick: <b>\'+(pMax*100).toFixed(3)+\'%</b> · ~\'+estPerH_max+\'/heure · ~\'+estPerD_max+\'/jour<br>\'\n    +(pMin+pMax>0.5?\'<span style="color:var(--red)">⚠️ Fréquences très élevées — le prix sera souvent aux extrêmes</span>\':\'<span style="color:var(--green)">✅ Fréquences réalistes</span>\');\n\n  window._cyPMin=pMin;window._cyPMax=pMax;\n  var _amin=parseFloat($(\'cy-absmin\').value)||0,_amax=parseFloat($(\'cy-absmax\').value)||0;\n  try{fetch(\'/nxc/extremes\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,pmin:pMin/3,pmax:pMax/3})});\n  if(_amin>0&&_amax>_amin)fetch(\'/nxc/bounds\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,auto:false,min:_amin,max:_amax})});\n  else fetch(\'/nxc/bounds\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,auto:true})});\n  addLog(\'🎯\',\'Extrêmes + bornes → serveur\');}catch(e){}\n}\n\n</script>\n<script>\nvar KEY=\'\',mkt={},tInt=null,tMode=null,tStr=0.005,tIv=12000,chObj=null,rsiObj=null,volObj=null;\nvar solvOn=false,mpOn=false,biasDrift=0,biasSpd=1.0,_users=[],_flux=[],_fluxF=\'all\',_log=[],_alerts=[],_alHist=[];\nvar _prevP=0,_ctType=\'line\',_ctRange=50,_cfgFloor=null,_cfgCeil=null,_schedInt=null;\nvar _tmInt=null,_randP=null,_savedSites=JSON.parse(localStorage.getItem(\'nxc_sites\')||\'[]\'),_curUrl=\'\';\n\nfunction $(i){return document.getElementById(i);}\nfunction fmt(n,d){return Number(n||0).toLocaleString(\'fr-FR\',{minimumFractionDigits:d||0,maximumFractionDigits:d==null?2:d});}\nfunction esc(s){return (s+\'\').replace(/[&<>"]/g,c=>({\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\'}[c]));}\nfunction fmtT(ts){return new Date(ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\',second:\'2-digit\'});}\nfunction setMsg(id,t,ok){var e=$(id);if(!e)return;e.textContent=t;e.style.color=ok?\'var(--green)\':\'var(--red)\';}\nfunction addLog(ico,txt){_log.unshift({ico,txt,ts:Date.now()});if(_log.length>200)_log.pop();renderLog();}\nfunction renderLog(){\n  var h=_log.length?_log.map(l=>\'<div class="log-item"><span class="log-time">\'+fmtT(l.ts)+\'</span><span>\'+l.ico+\'</span><span style="color:var(--text);flex:1">\'+esc(l.txt)+\'</span></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun log</p>\';\n  var l=$(\'log-list\');if(l)l.innerHTML=h;\n  var l2=$(\'log-list2\');if(l2)l2.innerHTML=h;\n}\nasync function api(p,b){b=b||{};b.master_key=KEY;try{var r=await fetch(p,{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify(b)});return await r.json();}catch(e){return{ok:false};}}\n\n// LOGIN\nfunction doLogin(){\n  var k=$(\'mk\');if(!k)return;\n  KEY=k.value.trim();\n  if(!KEY){$(\'lm\').textContent=\'Entrer la clé\';return;}\n  $(\'lm\').textContent=\'Connexion… (si premier lancement: attendre 45s)\';\n  var ctrl=new AbortController();var tid=setTimeout(function(){ctrl.abort();},65000);\n  fetch(\'/nxc/price\',{signal:ctrl.signal}).then(function(r){return r.json();}).then(function(d){\n    // Tester avec admin/list\n    return fetch(\'/admin/list\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY}),signal:ctrl.signal});\n  }).then(function(r){return r.json();}).then(function(d){\n    clearTimeout(tid);\n    if(d&&d.ok){\n      $(\'ls\').style.display=\'none\';\n      $(\'hd\').classList.add(\'on\');\n      $(\'htm\').style.display=\'block\';\n      addLog(\'🔑\',\'Connexion admin réussie\');loadPinnedSites();\n      ref();loadBank();loadSolv();loadMeanPrice();loadBias();loadFails();\n      setInterval(ref,4000);try{setInterval(function(){if(typeof mkt!==\'undefined\'&&mkt.price)notifCheck(parseFloat(mkt.price));},8000);}catch(e){}try{loadTabConfig();}catch(e){}try{loadVolatility();}catch(e){}try{applySavedOpts();}catch(e){}try{loadBank();}catch(e){}try{applySavedTheme();}catch(e){}try{updGrowthStatus();}catch(e){}\n      setInterval(function(){loadBank();loadFails();},25000);\n      setInterval(function(){$(\'htm\').textContent=fmtClock();},1000);\n      // Charger les sites sauvegardés\n      if(!_savedSites.length){\n        _savedSites=[\n          {label:\'Nexus Coin\',url:\'https://lively-art-86d9.noah-guetta.workers.dev\'},\n          {label:\'Panel Admin\',url:location.origin+\'/panel\'},\n          {label:\'GitHub\',url:\'https://github.com/Noah1234567890123456789\'}\n        ];\n        localStorage.setItem(\'nxc_sites\',JSON.stringify(_savedSites));\n      }\n      renderSavedSites();\n    }else{\n      $(\'lm\').textContent=\'❌ Clé incorrecte\';KEY=\'\';\n    }\n  }).catch(function(e){clearTimeout(tid);if(e&&e.name===\'AbortError\'){$(\'lm\').textContent=\'⏳ Serveur en veille — réessaie dans 45s\';}else{$(\'lm\').textContent=\'❌ Serveur inaccessible\';}KEY=\'\';});\n}\n\n// TABS\nfunction toggleMore(){var d=$(\'dropdown\');d.classList.toggle(\'show\');}\ndocument.addEventListener(\'click\',function(e){if(!e.target.closest(\'#dropdown\')&&!e.target.closest(\'#btn-more\'))$(\'dropdown\').classList.remove(\'show\');});\n\nfunction go(tab,btn){\n  document.querySelectorAll(\'.view\').forEach(v=>v.classList.remove(\'on\'));\n  document.querySelectorAll(\'.tab\').forEach(t=>t.classList.remove(\'on\'));\n  var v=$(\'view-\'+tab);if(v)v.classList.add(\'on\');\n  if(btn)btn.classList.add(\'on\');\n  if(tab===\'users\')loadUsers();\n  if(tab===\'stats\')loadStats();\n  if(tab===\'admin\'){refreshAdminStats();loadAdmUsers();}\n  if(tab===\'banque\')$(\'nd-b\').style.display=\'none\';\n  if(tab===\'banque\')drawBkGraph();\n  if(tab===\'prevision\')calcPrev();\n}\n\n// MARCHÉ\nasync function ref(){\n  try{\n    var r=await fetch(\'/nxc/price\');var d=await r.json();mkt=d;\n    var p=parseFloat(d.price||0),h=d.history||[];\n    var chg=_prevP>0?((p-_prevP)/_prevP*100):0;\n    var hi=h.length>1?Math.max(...h.slice(-24).map(x=>x.price)):p;\n    var lo=h.length>1?Math.min(...h.slice(-24).map(x=>x.price)):p;\n    $(\'s-p\').textContent=fmt(p,2);$(\'s-v\').textContent=fmt(d.volume24||0,0);\n    $(\'s-t\').textContent=d.trades24||0;$(\'s-h\').textContent=h.length;\n    $(\'s-hi\').textContent=fmt(hi,0);$(\'s-lo\').textContent=fmt(lo,0);\n    $(\'s-var\').textContent=(chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%\';$(\'s-var\').style.color=chg>=0?\'var(--green)\':\'var(--red)\';\n    $(\'s-cap\').textContent=fmt(p*3,0);\n    $(\'hp\').textContent=fmt(p,2)+\' R\';\n    var hc=$(\'hc\');if(_prevP>0){hc.textContent=(chg>=0?\'▲+\':\'▼\')+chg.toFixed(2)+\'%\';hc.className=\'hud-chg \'+(chg>=0?\'up\':\'dn\');hc.style.display=\'block\';}\n    _prevP=p;\n    drawC(h);drawA(p,h);drawRSI(h);\n    checkAlerts(p);\n    // plancher\/plafond gérés uniquement par le mode Normal — pas ici\n  }catch(e){}\n}\n\nasync function tick(p){await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:0,volume24:mkt.volume24||0,trades24:mkt.trades24||0})});}\n\nfunction setRange(n){_ctRange=n;if(chObj){chObj.destroy();chObj=null;}ref();}\nfunction toggleChartType(){_ctType=_ctType===\'line\'?\'bar\':\'line\';if(chObj){chObj.destroy();chObj=null;}ref();}\nfunction dlChart(){var cv=$(\'ch\');if(!cv)return;var a=document.createElement(\'a\');a.download=\'nxc_\'+Date.now()+\'.png\';a.href=cv.toDataURL();a.click();}\n\nfunction drawC(h){\n  var cv=$(\'ch\');if(!cv||!window.Chart)return;\n  var pts=h.slice(-_ctRange);\n  var labs=pts.map(x=>new Date(x.ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\'}));\n  var prices=pts.map(x=>parseFloat(x.price));\n  if(prices.length<2)return;\n  var mn=Math.min(...prices)*0.85,mx=Math.max(...prices)*1.15;\n  if(chObj){chObj.data.labels=labs;chObj.data.datasets[0].data=prices;chObj.options.scales.y.min=mn;chObj.options.scales.y.max=mx;chObj.update(\'none\');return;}\n  var ctx=cv.getContext(\'2d\');\n  var g=ctx.createLinearGradient(0,0,0,cv.offsetHeight||200);g.addColorStop(0,\'rgba(0,229,255,.2)\');g.addColorStop(1,\'rgba(0,229,255,0)\');\n  chObj=new Chart(ctx,{type:_ctType===\'bar\'?\'bar\':\'line\',data:{labels:labs,datasets:[{data:prices,borderColor:\'#00e5ff\',backgroundColor:_ctType===\'bar\'?\'rgba(0,229,255,.4)\':g,borderWidth:2.5,pointRadius:0,fill:_ctType!==\'bar\',tension:0.4}]},\n    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},\n      scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:5,font:{size:8}},grid:{color:\'rgba(0,229,255,.04)\'}},\n        y:{min:mn,max:mx,ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n}\n\nfunction drawRSI(h){\n  var cv=$(\'ch-rsi\');if(!cv||!window.Chart||h.length<15)return;\n  var prices=h.slice(-28).map(x=>parseFloat(x.price));\n  var rsi=[];\n  for(var i=14;i<prices.length;i++){\n    var g=0,l=0;for(var j=i-14;j<i;j++){var dv=prices[j+1]-prices[j];if(dv>0)g+=dv;else l-=dv;}\n    rsi.push(Math.round(l===0?100:100-100/(1+(g/l))));\n  }\n  var labs=h.slice(-rsi.length).map(x=>new Date(x.ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\'}));\n  if(rsiObj){rsiObj.data.labels=labs;rsiObj.data.datasets[0].data=rsi;rsiObj.update(\'none\');return;}\n  var ctx=cv.getContext(\'2d\');\n  rsiObj=new Chart(ctx,{type:\'line\',data:{labels:labs,datasets:[{data:rsi,borderColor:\'#a06bff\',borderWidth:2,pointRadius:0,fill:false,tension:0.4}]},\n    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},\n      scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:4,font:{size:8}},grid:{display:false}},y:{min:0,max:100,ticks:{color:\'#5c6b8c\',stepSize:25},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n}\n\nfunction drawA(p,h){\n  var el=$(\'al\'),a=[];\n  if(p>80000)a.push({c:\'ae\',m:\'⚡ Prix critique >80 000 R\'});\n  else if(p<500)a.push({c:\'ae\',m:\'🔴 Prix effondrement <500 R\'});\n  else a.push({c:\'ao\',m:\'✅ Prix normal: \'+fmt(p,0)+\' R\'});\n  if(h.length>10){var rv=h.slice(-10).map(x=>x.price);var vol=(Math.max(...rv)-Math.min(...rv))/Math.min(...rv)*100;a.push(vol>20?{c:\'aw\',m:\'⚡ Volatilité: \'+vol.toFixed(1)+\'%\'}:{c:\'ao\',m:\'📊 Stable — volatilité: \'+vol.toFixed(1)+\'%\'});}\n  a.push(tMode?{c:\'aw\',m:\'📊 Tendance \'+tMode+\' · \'+(tStr*100).toFixed(1)+\'%/tick\'}:{c:\'ai\',m:\'⏸ Aucune tendance\'});\n  if(el)el.innerHTML=a.map(x=>\'<div class="ab \'+x.c+\'">\'+x.m+\'</div>\').join(\'\');\n  // Smart alerts\n  var sa=$(\'smart-al\');if(sa)sa.innerHTML=a.map(x=>\'<div class="ab \'+x.c+\'">\'+x.m+\'</div>\').join(\'\');\n}\n\n// CONTRÔLE\nasync function adjP(pct){var p=Math.max(50,Math.min(9999999,parseFloat(mkt.price||5213)*(1+pct)));p=Math.round(p*100)/100;await tick(p);setMsg(\'pm\',\'✅ \'+(pct>0?\'+\':\'\')+((pct*100).toFixed(1))+\'% → \'+fmt(p,2)+\' R\',true);addLog(\'📊\',\'Cours \'+(pct>0?\'+\':\'\')+((pct*100).toFixed(1))+\'%\');setTimeout(ref,500);}\nasync function setP(){var p=parseFloat($(\'np\').value);if(!p||p<1){setMsg(\'pm\',\'Prix invalide\',false);return;}await fetch(\'/nxc/price/set\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p})});setMsg(\'pm\',\'✅ Référence → \'+fmt(p,2)+\' R (oscille autour, plancher -30%)\',true);$(\'np\').value=\'\';addLog(\'💱\',\'Référence fixée: \'+fmt(p,2)+\' R\');setTimeout(ref,500);}\nfunction setTheme(name){\n  document.body.setAttribute(\'data-theme\',name===\'classic\'?\'\':name);\n  try{localStorage.setItem(\'nxc_theme\',name);}catch(e){}\n  var names={classic:\'Nexus Classic\',terminal:\'Terminal / Hacker\',glass:\'Verre dépoli\',bloomberg:\'Bloomberg Pro\',neon:\'Néon Cyberpunk\',minimal:\'Minimal\'};\n  var m=$(\'theme-msg\');if(m)m.textContent=\'Style actuel : \'+(names[name]||name);\n  document.querySelectorAll(\'.style-card\').forEach(function(el){el.classList.toggle(\'sel\',el.getAttribute(\'data-th\')===name);});\n}\nfunction applySavedTheme(){\n  var t=\'classic\';try{t=localStorage.getItem(\'nxc_theme\')||\'classic\';}catch(e){}\n  setTheme(t);\n}\nvar _bank={};var _bkChart=null;\nfunction bankEnsure(){\n  if(!_bank.accounts)_bank.accounts={courant:0,epargne:0,coffre:0};\n  [\'courant\',\'epargne\',\'coffre\'].forEach(function(k){if(typeof _bank.accounts[k]!==\'number\')_bank.accounts[k]=0;});\n  if(!_bank.loans)_bank.loans=[];\n  if(!_bank.savings)_bank.savings={target:0};if(_bank.savings.ratePct===undefined)_bank.savings.ratePct=5;if(!_bank.savings.unitSecs)_bank.savings.unitSecs=31536000;\n  if(!_bank.lastInterestTs)_bank.lastInterestTs=Date.now();\n}\nfunction accrueInterest(){\n  bankEnsure();\n  var now=Date.now();var sv=_bank.savings;\n  var rate=(sv.ratePct!==undefined?sv.ratePct:5);var unit=(sv.unitSecs||31536000);\n  var units=((now-_bank.lastInterestTs)/1000)/unit;\n  if(units>0 && _bank.accounts.epargne>0 && rate!==0){\n    _bank.accounts.epargne=parseFloat((_bank.accounts.epargne*Math.pow(1+rate/100,units)).toFixed(2));\n  }\n  _bank.lastInterestTs=now;\n}\nasync function bankSave(){\n  try{await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:_bank})});}catch(e){}\n}\nfunction loanOwed(ln){\n  var years=Math.max(0,(Date.now()-ln.issued)/(365*86400000));\n  var interest=ln.principal*(ln.rate/100)*years;\n  return Math.max(0,parseFloat((ln.principal+interest-(ln.repaid||0)).toFixed(2)));\n}\nasync function bankTransfer(){\n  var amt=parseFloat($(\'tr-amt\').value);var from=$(\'tr-from\').value,to=$(\'tr-to\').value;\n  if(!amt||amt<=0){setMsg(\'tr-msg\',\'Montant invalide\',false);return;}\n  if(from===to){setMsg(\'tr-msg\',\'Comptes identiques\',false);return;}\n  bankEnsure();\n  var bal=from===\'reserves\'?(_bank.reserves||0):_bank.accounts[from];\n  if((bal||0)<amt){setMsg(\'tr-msg\',\'❌ Solde insuffisant\',false);return;}\n  if(from===\'reserves\')_bank.reserves=parseFloat(((_bank.reserves||0)-amt).toFixed(2));else _bank.accounts[from]=parseFloat((_bank.accounts[from]-amt).toFixed(2));\n  if(to===\'reserves\')_bank.reserves=parseFloat(((_bank.reserves||0)+amt).toFixed(2));else _bank.accounts[to]=parseFloat(((_bank.accounts[to]||0)+amt).toFixed(2));\n  _bank.flux=_bank.flux||[];_bank.flux.push({ts:Date.now(),type:\'TRANSFER\',amount:amt,note:from+\' → \'+to});\n  await bankSave();setMsg(\'tr-msg\',\'✅ Virement effectué\',true);$(\'tr-amt\').value=\'\';addLog(\'🔀\',\'Virement \'+fmt(amt,0)+\' R\');loadBank();\n}\nasync function grantLoan(){\n  var who=($(\'ln-who\').value||\'\').trim();var amt=parseFloat($(\'ln-amt\').value);\n  var rate=parseFloat($(\'ln-rate\').value);var days=parseFloat($(\'ln-days\').value);\n  if(!who){setMsg(\'ln-msg\',\'Nom emprunteur requis\',false);return;}\n  if(!amt||amt<=0){setMsg(\'ln-msg\',\'Montant invalide\',false);return;}\n  if(isNaN(rate)||rate<0)rate=0;if(isNaN(days)||days<=0)days=30;\n  bankEnsure();\n  if((_bank.reserves||0)<amt){setMsg(\'ln-msg\',\'❌ Réserves insuffisantes\',false);return;}\n  _bank.reserves=parseFloat(((_bank.reserves||0)-amt).toFixed(2));\n  _bank.loans.push({id:\'L\'+Date.now(),who:who,principal:amt,rate:rate,days:days,issued:Date.now(),repaid:0});\n  _bank.flux=_bank.flux||[];_bank.flux.push({ts:Date.now(),type:\'LOAN\',amount:amt,note:\'Prêt à \'+who});\n  await bankSave();setMsg(\'ln-msg\',\'✅ Prêt accordé à \'+who,true);$(\'ln-who\').value=\'\';$(\'ln-amt\').value=\'\';addLog(\'🏦\',\'Prêt \'+fmt(amt,0)+\' R à \'+who);loadBank();\n}\nasync function repayLoan(id){\n  bankEnsure();var ln=_bank.loans.find(function(x){return x.id===id;});if(!ln)return;\n  var owed=loanOwed(ln);\n  _bank.reserves=parseFloat(((_bank.reserves||0)+owed).toFixed(2));\n  _bank.flux=_bank.flux||[];_bank.flux.push({ts:Date.now(),type:\'REPAY\',amount:owed,note:\'Remb. \'+ln.who});\n  _bank.loans=_bank.loans.filter(function(x){return x.id!==id;});\n  await bankSave();addLog(\'✅\',\'Prêt remboursé : \'+fmt(owed,0)+\' R\');loadBank();\n}\nasync function setSavingsRate(){var r=parseFloat($(\'sv-rate\').value);var u=parseFloat($(\'sv-unit\').value)||31536000;if(isNaN(r)||r<0)return;bankEnsure();_bank.savings.ratePct=r;_bank.savings.unitSecs=u;await bankSave();$(\'sv-rate\').value=\'\';addLog(\'💰\',\'Taux épargne : \'+r+\'%\');renderBank2();}\nasync function setSavingsGoal(){\n  var g=parseFloat($(\'sv-goal\').value);if(isNaN(g)||g<0)return;\n  bankEnsure();_bank.savings.target=g;await bankSave();$(\'sv-goal\').value=\'\';renderBank2();\n}\nfunction renderBank2(){\n  bankEnsure();accrueInterest();\n  var R=$(\'ac-reserves\');if(!R)return;\n  R.textContent=fmt(_bank.reserves||0,0)+\' R\';\n  $(\'ac-courant\').textContent=fmt(_bank.accounts.courant,0)+\' R\';\n  $(\'ac-epargne\').textContent=fmt(_bank.accounts.epargne,0)+\' R\';\n  $(\'ac-coffre\').textContent=fmt(_bank.accounts.coffre,0)+\' R\';\n  var t=_bank.savings.target||0;var ep=_bank.accounts.epargne||0;var pct=t>0?Math.min(100,ep/t*100):0;\n  var bar=$(\'sv-bar\');if(bar)bar.style.width=pct.toFixed(1)+\'%\';\n  var sr=(_bank.savings.ratePct!==undefined?_bank.savings.ratePct:5);var su=_bank.savings.unitSecs||31536000;var un={86400:\'jour\',604800:\'semaine\',2592000:\'mois\',31536000:\'an\'}[su]||\'an\';var srt=$(\'sv-rate-txt\');if(srt)srt.textContent=\'Taux actuel : \'+sr+\'%/\'+un+\' — versé automatiquement.\';var sri=$(\'sv-rate\');if(sri&&!sri.value)sri.placeholder=\'Actuel: \'+sr;var st=$(\'sv-txt\');if(st)st.textContent=t>0?(fmt(ep,0)+\' / \'+fmt(t,0)+\' R (\'+pct.toFixed(1)+\'%)\'):\'Aucun objectif défini\';\n  var tot=0,html=\'\';\n  _bank.loans.forEach(function(ln){var owed=loanOwed(ln);tot+=owed;\n    html+=\'<div style="display:flex;justify-content:space-between;align-items:center;padding:8px;border-bottom:1px solid rgba(0,229,255,.06)"><div><div style="font-weight:700;font-size:12px">\'+esc(ln.who)+\'</div><div style="font-size:9px;color:var(--muted)">\'+fmt(ln.principal,0)+\' R • \'+ln.rate+\'%/an • doit \'+fmt(owed,0)+\' R</div></div><button class="btn green" data-lid="\'+ln.id+\'" style="font-size:10px;padding:4px 8px">Rembourser</button></div>\';});\n  var ll=$(\'ln-list\');if(ll){ll.innerHTML=html||\'<div style="padding:10px;color:var(--muted);font-size:11px">Aucun prêt en cours</div>\';\n    ll.querySelectorAll(\'[data-lid]\').forEach(function(btn){btn.onclick=function(){repayLoan(btn.getAttribute(\'data-lid\'));};});}\n  var lt=$(\'ln-total\');if(lt)lt.textContent=fmt(tot,0)+\' R\';\n  $(\'an-in\').textContent=fmt(_bank.totalIn||0,0);\n  $(\'an-out\').textContent=fmt(_bank.totalOut||0,0);\n  var net=(_bank.totalIn||0)-(_bank.totalOut||0);var ne=$(\'an-net\');if(ne){ne.textContent=(net>=0?\'+\':\'\')+fmt(net,0);ne.style.color=net>=0?\'var(--green)\':\'var(--red)\';}\n  drawBankAnalytics();\n}\nfunction drawBankAnalytics(){\n  var cv=$(\'bk-analytics\');if(!cv||!window.Chart)return;\n  var flux=(_bank.flux||[]).slice().sort(function(a,b){return a.ts-b.ts;});\n  var run=0,labels=[],data=[];\n  flux.forEach(function(f){var a=f.amount||0;var ty=(f.type||\'\').toUpperCase();\n    if(ty===\'IN\'||ty===\'REPAY\')run+=a;else if(ty===\'OUT\'||ty===\'LOAN\')run-=a;\n    labels.push(\'\');data.push(parseFloat(run.toFixed(2)));});\n  if(_bkChart){_bkChart.destroy();}\n  _bkChart=new Chart(cv.getContext(\'2d\'),{type:\'line\',data:{labels:labels,datasets:[{data:data,borderColor:\'#00e5ff\',backgroundColor:\'rgba(0,229,255,.12)\',borderWidth:2,pointRadius:0,fill:true,tension:0.3}]},options:{plugins:{legend:{display:false}},scales:{x:{display:false},y:{ticks:{color:\'#5c6b8c\',font:{size:9}},grid:{color:\'rgba(255,255,255,.04)\'}}},maintainAspectRatio:false}});\n}\nasync function printBankReport(){\n  bankEnsure();\n  var now=new Date().toLocaleString(\'fr-FR\');var num=\'NXB-\'+Date.now();\n  var win=window.open(\'\',\'_blank\');if(!win){alert(\'Autorise les pop-ups pour imprimer le relevé.\');return;}\n  var accRows=\'\';[[\'Réserves\',_bank.reserves||0],[\'Courant\',_bank.accounts.courant],[\'Épargne\',_bank.accounts.epargne],[\'Coffre\',_bank.accounts.coffre]].forEach(function(a){accRows+=\'<tr><td>\'+a[0]+\'</td><td style="text-align:right">\'+fmt(a[1],2)+\' R</td></tr>\';});\n  var loanRows=\'\',totOwed=0;_bank.loans.forEach(function(ln){var o=loanOwed(ln);totOwed+=o;loanRows+=\'<tr><td>\'+esc(ln.who)+\'</td><td style="text-align:right">\'+fmt(ln.principal,0)+\'</td><td style="text-align:right">\'+ln.rate+\'%</td><td style="text-align:right">\'+fmt(o,0)+\'</td></tr>\';});\n  if(!loanRows)loanRows=\'<tr><td colspan="4" style="text-align:center;color:#888">Aucun prêt en cours</td></tr>\';\n  var flux=(_bank.flux||[]).slice().reverse(),fRows=\'\';\n  flux.slice(0,300).forEach(function(f){var d=new Date(f.ts).toLocaleString(\'fr-FR\');fRows+=\'<tr><td>\'+d+\'</td><td>\'+(f.type||\'\')+\'</td><td style="text-align:right">\'+fmt(f.amount||0,2)+\'</td><td>\'+esc(f.note||\'\')+\'</td></tr>\';});\n  var net=(_bank.totalIn||0)-(_bank.totalOut||0);\n  var css=\'*{font-family:Arial,sans-serif;box-sizing:border-box}body{padding:24px;max-width:900px;margin:0 auto;color:#111}h1{letter-spacing:3px;font-size:26px;margin:0}h2{font-size:15px;border-bottom:2px solid #111;padding-bottom:4px;margin-top:26px}.head{text-align:center;border-bottom:3px solid #111;padding-bottom:14px}.muted{color:#666;font-size:12px}table{width:100%;border-collapse:collapse;font-size:12px;margin-top:8px}td,th{border:1px solid #ddd;padding:6px 8px}.big{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:12px}.b{border:1px solid #ddd;border-radius:8px;padding:10px;text-align:center}.bv{font-size:18px;font-weight:800}.bl{font-size:9px;text-transform:uppercase;color:#666}\';\n  win.document.write(\'<!DOCTYPE html><html><head><meta charset="utf-8"><title>Relevé Nexus \'+num+\'</title><style>\'+css+\'</style></head><body>\');\n  win.document.write(\'<div class="head"><h1>◈ NEXUS BANK</h1><div class="muted">Relevé complet \'+num+\' — \'+now+\'</div></div>\');\n  win.document.write(\'<div class="big"><div class="b"><div class="bv">\'+fmt(_bank.reserves||0,0)+\'</div><div class="bl">Réserves</div></div><div class="b"><div class="bv">\'+fmt(_bank.totalIn||0,0)+\'</div><div class="bl">Total entré</div></div><div class="b"><div class="bv">\'+fmt(_bank.totalOut||0,0)+\'</div><div class="bl">Total sorti</div></div><div class="b"><div class="bv">\'+(net>=0?\'+\':\'\')+fmt(net,0)+\'</div><div class="bl">Bénéfice net</div></div></div>\');\n  win.document.write(\'<h2>Comptes</h2><table><tr><th>Compte</th><th style="text-align:right">Solde</th></tr>\'+accRows+\'</table>\');\n  win.document.write(\'<h2>Prêts en cours — dette totale \'+fmt(totOwed,0)+\' R</h2><table><tr><th>Emprunteur</th><th style="text-align:right">Principal</th><th style="text-align:right">Taux</th><th style="text-align:right">Dû</th></tr>\'+loanRows+\'</table>\');\n  win.document.write(\'<h2>Historique des opérations (\'+flux.length+\')</h2><table><tr><th>Date</th><th>Type</th><th style="text-align:right">Montant</th><th>Note</th></tr>\'+fRows+\'</table>\');\n  var _ulist=[];\n  try{var ur=await api(\'/admin/list\');\n    if(ur&&ur.ok){_ulist=await Promise.all((ur.users||[]).map(async function(u){\n      var g=await api(\'/admin/get\',{target:u.username});\n      var pts=Math.max((g.data&&g.data.nx2098&&g.data.nx2098.rewards)||0,(g.data&&g.data.rewards&&g.data.rewards.points)||0);\n      var nxc=parseFloat((g.data&&g.data.nx2098&&g.data.nx2098.balance)||0)||0;\n      return {name:u.username,role:u.role||\'user\',pts:pts,nxc:nxc};}));}\n  }catch(e){}\n  var uRows=\'\',admins=0,usersN=0;\n  _ulist.forEach(function(u){if((u.role||\'\').toLowerCase().indexOf(\'admin\')>=0)admins++;else usersN++;\n    uRows+=\'<tr><td>\'+esc(u.name)+\'</td><td>\'+esc(u.role)+\'</td><td style="text-align:right">\'+fmt(u.pts,0)+\'</td><td style="text-align:right">\'+u.nxc.toFixed(4)+\'</td></tr>\';});\n  if(!uRows)uRows=\'<tr><td colspan="4" style="text-align:center;color:#888">Aucun utilisateur</td></tr>\';\n  win.document.write(\'<h2>Utilisateurs & Admins (\'+_ulist.length+\' comptes — \'+admins+\' admin, \'+usersN+\' users)</h2><table><tr><th>Compte</th><th>Rôle</th><th style="text-align:right">Points</th><th style="text-align:right">NXC</th></tr>\'+uRows+\'</table>\');\n  \n  win.document.write(\'<p class="muted" style="margin-top:20px;text-align:center">Nexus Bank — document généré automatiquement</p></body></html>\');\n  win.document.close();setTimeout(function(){try{win.print();}catch(e){}},400);\n}\nvar _opts={};\nfunction _optSave(){try{localStorage.setItem(\'nxc_opts\',JSON.stringify(_opts));}catch(e){}}\nfunction setZoom(v){_opts.zoom=parseInt(v)||100;document.body.style.zoom=(_opts.zoom/100);var e=$(\'opt-zoom-v\');if(e)e.textContent=_opts.zoom+\'%\';var s=$(\'opt-zoom\');if(s&&parseInt(s.value)!==_opts.zoom)s.value=_opts.zoom;_optSave();}\nfunction setOpt(k,val){\n  _opts[k]=val;\n  if(k===\'anim\')document.body.classList.toggle(\'no-anim\',!val);\n  if(k===\'round\')document.body.classList.toggle(\'no-round\',!val);\n  if(k===\'compact\')document.body.classList.toggle(\'compact\',val);\n  if(k===\'hud\')document.body.classList.toggle(\'hide-hud\',val);\n  if(k===\'more\')document.body.classList.toggle(\'hide-more\',val);if(k===\'hudbottom\')document.body.classList.toggle(\'hud-bottom\',val);\n  if(k===\'saver\')_ssArm();\n  _optSave();\n}\nfunction toggleFullscreen(){try{if(!document.fullscreenElement)document.documentElement.requestFullscreen();else document.exitFullscreen();}catch(e){}}\nfunction fmtClock(){var d=new Date();if(_opts.clock12)return d.toLocaleTimeString(\'en-US\',{hour12:true});return d.toLocaleTimeString(\'fr-FR\');}\nvar _actx=null;\nfunction _beep(){if(!_opts.sound)return;try{_actx=_actx||new (window.AudioContext||window.webkitAudioContext)();var o=_actx.createOscillator(),g=_actx.createGain();o.frequency.value=660;o.connect(g);g.connect(_actx.destination);g.gain.value=0.04;o.start();g.gain.exponentialRampToValueAtTime(0.0001,_actx.currentTime+0.08);o.stop(_actx.currentTime+0.09);}catch(e){}}\ndocument.addEventListener(\'click\',function(ev){if(ev.target&&ev.target.closest&&ev.target.closest(\'button\'))_beep();},true);\nvar _ssTimer=null;\nfunction _ensureSaver(){if($(\'screensaver\'))return;var el=document.createElement(\'div\');el.id=\'screensaver\';el.innerHTML=\'<div id="ss-clock">--:--</div><div id="ss-sub">Touchez pour reprendre</div>\';document.body.appendChild(el);el.addEventListener(\'click\',function(){el.classList.remove(\'on\');_ssArm();});}\nfunction _ssShow(){_ensureSaver();var el=$(\'screensaver\');if(!el)return;el.classList.add(\'on\');var c=$(\'ss-clock\');function u(){if(c)c.textContent=fmtClock();}u();if(el._i)clearInterval(el._i);el._i=setInterval(u,1000);}\nfunction _ssArm(){if(_ssTimer)clearTimeout(_ssTimer);var ss=$(\'screensaver\');if(ss)ss.classList.remove(\'on\');if(_opts.saver===false)return;_ssTimer=setTimeout(_ssShow,90000);}\n[\'mousemove\',\'keydown\',\'touchstart\'].forEach(function(e){document.addEventListener(e,function(){var ss=$(\'screensaver\');if(ss&&ss.classList.contains(\'on\'))return;_ssArm();},true);});\nvar _i18n={en:{marche:\'📈 Market\',banque:\'🏦 Bank\',nexus:\'🌐 App\',admin:\'👑 Admin\',parametre:\'⚙ Settings\'},fr:{marche:\'📈 Marché\',banque:\'🏦 Banque\',nexus:\'🌐 App\',admin:\'👑 Admin\',parametre:\'⚙ Réglages\'}};\nfunction setLang(l){_opts.lang=l;_optSave();var m=_i18n[l]||_i18n.fr;\n  document.querySelectorAll(\'#main-tabs .tab\').forEach(function(b){var oc=b.getAttribute(\'onclick\')||\'\';\n    var key=null;[\'marche\',\'banque\',\'nexus\',\'admin\',\'parametre\'].forEach(function(k){if(oc.indexOf(k)>=0)key=k;});\n    if(key&&b.childNodes[0])b.childNodes[0].nodeValue=m[key];});\n}\nfunction applySavedOpts(){\n  try{_opts=JSON.parse(localStorage.getItem(\'nxc_opts\')||\'{}\');}catch(e){_opts={};}\n  if(_opts.anim===undefined)_opts.anim=true;if(_opts.round===undefined)_opts.round=true;if(_opts.saver===undefined)_opts.saver=true;\n  if(_opts.zoom)setZoom(_opts.zoom);\n  document.body.classList.toggle(\'no-anim\',_opts.anim===false);\n  document.body.classList.toggle(\'no-round\',_opts.round===false);\n  document.body.classList.toggle(\'compact\',!!_opts.compact);\n  document.body.classList.toggle(\'hide-hud\',!!_opts.hud);\n  document.body.classList.toggle(\'hide-more\',!!_opts.more);\n  var cb={anim:_opts.anim!==false,round:_opts.round!==false,compact:!!_opts.compact,hud:!!_opts.hud,more:!!_opts.more,clock12:!!_opts.clock12,sound:!!_opts.sound,saver:_opts.saver!==false};\n  Object.keys(cb).forEach(function(k){var el=$(\'opt-\'+k);if(el)el.checked=cb[k];});\n  if(_opts.lang)setLang(_opts.lang);document.body.classList.toggle(\'hud-bottom\',!!_opts.hudbottom);[{id:\'opt-keys\',v:!!_opts.keys},{id:\'opt-hudbottom\',v:!!_opts.hudbottom},{id:\'opt-notif\',v:!!_opts.notif}].forEach(function(o){var el=$(o.id);if(el)el.checked=o.v;});if(_opts.bg){var _bs=$(\'opt-bg\');if(_bs)_bs.value=_opts.bg;setBg(_opts.bg);}\n  _ssArm();\n}\nfunction _volLabel(v){if(v<=0.05)return \'Figé\';if(v<0.6)return \'Calme\';if(v<1.4)return \'Normal\';if(v<2.5)return \'Agité\';if(v<4)return \'Très agité\';return \'Extrême\';}\nasync function setVolatility(v){var val=parseFloat(v)||0;var e=$(\'vol-v\');if(e)e.textContent=_volLabel(val)+\' (\'+val.toFixed(1)+\')\';\n  try{await fetch(\'/nxc/volatility\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,value:val})});}catch(e){}}\nasync function loadVolatility(){try{var r=await fetch(\'/nxc/volatility\');var d=await r.json();var s=$(\'vol-range\');if(s)s.value=d.value;var e=$(\'vol-v\');if(e)e.textContent=_volLabel(parseFloat(d.value))+\' (\'+parseFloat(d.value).toFixed(1)+\')\';}catch(e){}}\nvar ALL_TABS=[\n {id:\'marche\',label:\'📈 Marché\',core:true},\n {id:\'banque\',label:\'🏦 Banque\',core:true},\n {id:\'nexus\',label:\'🌐 App\',core:true},\n {id:\'admin\',label:\'👑 Admin\',core:true},\n {id:\'parametre\',label:\'⚙ Réglages\',core:true},\n {id:\'trading\',label:\'⚙️ Contrôle\'},\n {id:\'users\',label:\'👥 Comptes\'},\n {id:\'stats\',label:\'📊 Stats\'},\n {id:\'solv\',label:\'🛡️ Solvabilité\'},\n {id:\'tools\',label:\'🛠️ Outils\'},\n {id:\'log\',label:\'📋 Journal\'},\n {id:\'config\',label:\'⚙️ Config\'},\n {id:\'notifs\',label:\'🔔 Alertes\'},\n {id:\'cycles\',label:\'📅 Cycles\'},\n {id:\'prevision\',label:\'🔮 Prévision\'},\n {id:\'urgence\',label:\'🚨 Urgence\'},\n {id:\'dashboard\',label:\'📊 Dashboard\'},\n {id:\'alertesp\',label:\'🎯 Alertes Prix\'},\n {id:\'simulateur\',label:\'🔬 Simulateur\'},\n {id:\'avance\',label:\'⚙️ Avancé\'},\n {id:\'historique\',label:\'📈 Historique\'},\n {id:\'convertisseur\',label:\'💱 Convertisseur\'},\n {id:\'evenements\',label:\'🎲 Événements\'},\n {id:\'export\',label:\'📤 Export\'},\n {id:\'memo\',label:\'📌 Mémo\'}\n];\nvar _tabcfg={favorites:[],hidden:[\'historique\',\'convertisseur\',\'evenements\']};\nfunction _byId(id){return ALL_TABS.find(function(t){return t.id===id;});}\nfunction _ddItemFor(id){var f=null;var needle=\'go(\'+String.fromCharCode(39)+id+String.fromCharCode(39);document.querySelectorAll(\'#dropdown .dd-item\').forEach(function(it){if((it.getAttribute(\'onclick\')||\'\').indexOf(needle)>=0)f=it;});return f;}\nfunction _mainBtnFor(id,cb){var needle=\'go(\'+String.fromCharCode(39)+id+String.fromCharCode(39);document.querySelectorAll(\'#main-tabs .tab\').forEach(function(b){if((b.getAttribute(\'onclick\')||\'\').indexOf(needle)>=0)cb(b);});}\nasync function loadTabConfig(){\n  try{var r=await fetch(\'/nxc/tabconfig\');var d=await r.json();if(d.ok&&d.config)_tabcfg=d.config;}catch(e){}\n  if(!_tabcfg.favorites)_tabcfg.favorites=[];\n  if(!_tabcfg.hidden)_tabcfg.hidden=[\'historique\',\'convertisseur\',\'evenements\'];\n  applyTabConfig();renderTabManager();\n}\nasync function saveTabConfig(){try{await fetch(\'/nxc/tabconfig\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,config:_tabcfg})});}catch(e){}}\nfunction applyTabConfig(){\n  document.querySelectorAll(\'.fav-tab\').forEach(function(b){b.remove();});\n  var more=document.getElementById(\'btn-more\');\n  ALL_TABS.forEach(function(t){\n    var hidden=_tabcfg.hidden.indexOf(t.id)>=0;\n    var fav=_tabcfg.favorites.indexOf(t.id)>=0;\n    var dd=_ddItemFor(t.id);\n    if(dd)dd.style.display=(hidden||fav)?\'none\':\'\';\n    _mainBtnFor(t.id,function(b){if(!b.classList.contains(\'tab-more\'))b.style.display=hidden?\'none\':\'\';});\n  });\n  _tabcfg.favorites.forEach(function(id){\n    if(_tabcfg.hidden.indexOf(id)>=0)return;\n    var t=_byId(id);if(!t||t.core)return;\n    var btn=document.createElement(\'button\');btn.className=\'tab fav-tab\';btn.textContent=t.label;\n    btn.onclick=function(){go(id,btn);};\n    if(more&&more.parentNode)more.parentNode.insertBefore(btn,more);\n  });\n}\nfunction toggleFav(id,val){var i=_tabcfg.favorites.indexOf(id);if(val&&i<0)_tabcfg.favorites.push(id);if(!val&&i>=0)_tabcfg.favorites.splice(i,1);saveTabConfig();applyTabConfig();}\nfunction toggleHide(id,val){var i=_tabcfg.hidden.indexOf(id);if(val&&i<0)_tabcfg.hidden.push(id);if(!val&&i>=0)_tabcfg.hidden.splice(i,1);if(val){var j=_tabcfg.favorites.indexOf(id);if(j>=0)_tabcfg.favorites.splice(j,1);}saveTabConfig();applyTabConfig();renderTabManager();}\nfunction renderTabManager(){\n  var box=document.getElementById(\'tab-manager\');if(!box)return;var h=\'\';\n  ALL_TABS.forEach(function(t){\n    var fav=_tabcfg.favorites.indexOf(t.id)>=0;var hid=_tabcfg.hidden.indexOf(t.id)>=0;\n    h+=\'<div class="opt-row"><label style="flex:1">\'+t.label+(t.core?\' <span style="color:var(--muted);font-size:9px">(fixe)</span>\':\'\')+\'</label>\'\n      +\'<label style="font-size:10px;color:var(--muted);margin-right:10px;cursor:pointer">⭐<input type="checkbox" data-fav="\'+t.id+\'"\'+(fav?\' checked\':\'\')+(t.core?\' disabled\':\'\')+\' style="margin-left:3px"></label>\'\n      +\'<label style="font-size:10px;color:var(--muted);cursor:pointer">🚫<input type="checkbox" data-hide="\'+t.id+\'"\'+(hid?\' checked\':\'\')+\' style="margin-left:3px"></label></div>\';\n  });\n  box.innerHTML=h;\n  box.querySelectorAll(\'[data-fav]\').forEach(function(cb){cb.onchange=function(){toggleFav(cb.getAttribute(\'data-fav\'),cb.checked);};});\n  box.querySelectorAll(\'[data-hide]\').forEach(function(cb){cb.onchange=function(){toggleHide(cb.getAttribute(\'data-hide\'),cb.checked);};});\n}\nfunction _ensureBgLayer(){if($(\'bg-layer\'))return;var d=document.createElement(\'div\');d.id=\'bg-layer\';document.body.appendChild(d);var cv=document.createElement(\'canvas\');cv.id=\'bg-canvas\';document.body.appendChild(cv);}\nvar _bgRAF=null,_bgParts=[];\nfunction setBg(type){_opts.bg=type;_optSave();_ensureBgLayer();var d=$(\'bg-layer\'),cv=$(\'bg-canvas\');\n  d.className=\'\';if(_bgRAF){cancelAnimationFrame(_bgRAF);_bgRAF=null;}var cx=cv.getContext(\'2d\');if(cx)cx.clearRect(0,0,cv.width,cv.height);cv.style.display=\'none\';\n  var sel=$(\'opt-bg\');if(sel&&sel.value!==type)sel.value=type;\n  if(type===\'grid\')d.className=\'bg-grid\';\n  else if(type===\'gradient\')d.className=\'bg-gradient\';\n  else if(type===\'particles\'||type===\'stars\'){cv.style.display=\'block\';_bgStart(type,cv);}\n}\nfunction _bgStart(type,cv){function rs(){cv.width=window.innerWidth;cv.height=window.innerHeight;}rs();window.addEventListener(\'resize\',rs);\n  var n=type===\'stars\'?90:55;_bgParts=[];for(var i=0;i<n;i++)_bgParts.push({x:Math.random()*cv.width,y:Math.random()*cv.height,r:Math.random()*(type===\'stars\'?1.6:2.5)+.4,vx:(Math.random()-.5)*(type===\'stars\'?.15:.5),vy:(Math.random()-.5)*(type===\'stars\'?.15:.5),a:Math.random()});\n  var cx=cv.getContext(\'2d\');if(!cx)return;\n  function loop(){cx.clearRect(0,0,cv.width,cv.height);\n    for(var k=0;k<_bgParts.length;k++){var p=_bgParts[k];p.x+=p.vx;p.y+=p.vy;if(p.x<0)p.x=cv.width;if(p.x>cv.width)p.x=0;if(p.y<0)p.y=cv.height;if(p.y>cv.height)p.y=0;\n      cx.beginPath();cx.arc(p.x,p.y,p.r,0,6.283);cx.fillStyle=\'rgba(0,229,255,\'+(type===\'stars\'?(.3+Math.abs(Math.sin(Date.now()/700+p.a))*.6):.5)+\')\';cx.fill();}\n    if(type===\'particles\'){for(var i=0;i<_bgParts.length;i++)for(var j=i+1;j<_bgParts.length;j++){var a=_bgParts[i],b=_bgParts[j],dx=a.x-b.x,dy=a.y-b.y,dd=dx*dx+dy*dy;if(dd<9000){cx.strokeStyle=\'rgba(0,229,255,\'+(.12*(1-dd/9000))+\')\';cx.beginPath();cx.moveTo(a.x,a.y);cx.lineTo(b.x,b.y);cx.stroke();}}}\n    _bgRAF=requestAnimationFrame(loop);}\n  loop();\n}\nfunction toast(msg,color){var w=$(\'toast-wrap\');if(!w){w=document.createElement(\'div\');w.id=\'toast-wrap\';document.body.appendChild(w);}var t=document.createElement(\'div\');t.className=\'toast\';if(color)t.style.borderColor=color;t.textContent=msg;w.appendChild(t);setTimeout(function(){t.style.opacity=\'0\';setTimeout(function(){t.remove();},400);},4200);}\nvar _lastNotifP=0;\nfunction notifCheck(p){if(!_opts.notif||!p)return;if(_lastNotifP>0){var ch=(p-_lastNotifP)/_lastNotifP*100;if(Math.abs(ch)>=3)toast((ch>0?\'📈 +\':\'📉 \')+ch.toFixed(1)+\'% — NXC à \'+fmt(p,2)+\' R\',ch>0?\'var(--green)\':\'var(--red)\');}_lastNotifP=p;}\ndocument.addEventListener(\'keydown\',function(e){if(!_opts.keys)return;var tn=(e.target&&e.target.tagName)||\'\';if(/INPUT|TEXTAREA|SELECT/.test(tn))return;\n  if(e.key>=\'1\'&&e.key<=\'9\'){var vis=[];document.querySelectorAll(\'#main-tabs .tab\').forEach(function(b){if(b.style.display!==\'none\'&&!b.classList.contains(\'tab-more\'))vis.push(b);});var idx=parseInt(e.key)-1;if(vis[idx])vis[idx].click();}\n  else if(e.key===\'Escape\'){var ss=$(\'screensaver\');if(ss)ss.classList.remove(\'on\');}\n});\n\nasync function quickGrowth(pctDay){\n  var dir=pctDay>=0?1:-1;var pct=Math.abs(pctDay);var secs=86400;\n  var f=dir>0?(1+pct/100):Math.max(0.0001,1-pct/100);\n  var rate=Math.pow(f,1/secs)-1;\n  try{await fetch(\'/nxc/growth\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,rate_per_sec:rate,combine:true,enabled:true})});\n    setMsg(\'gr-msg\',\'✅ \'+(dir>0?\'+\':\'\')+pctDay+\'%/jour activé\',true);updGrowthStatus();}catch(e){setMsg(\'gr-msg\',\'❌ Erreur\',false);}\n}\nasync function diagGrowth(){\n  try{\n    setMsg(\'gr-msg\',\'⏳ Diagnostic (4s)…\',true);\n    var g=await (await fetch(\'/nxc/growth\')).json();\n    var p1=(await (await fetch(\'/nxc/price\')).json()).price;\n    await new Promise(function(r){setTimeout(r,4500);});\n    var p2=(await (await fetch(\'/nxc/price\')).json()).price;\n    var moved=p2-p1;var v;\n    if(!g.enabled)v=\'❌ DÉSACTIVÉE côté serveur (clé maître ? mauvais serveur ?)\';\n    else if(Math.abs(moved)<0.001)v=\'⚠️ Active mais prix FIGÉ → sûrement plusieurs workers Render (mets 1 worker)\';\n    else if((g.rate_per_sec>0&&moved>0)||(g.rate_per_sec<0&&moved<0))v=\'✅ La croissance agit bien sur le prix !\';\n    else v=\'⚠️ Le prix bouge en sens inverse (oscillation domine)\';\n    var el=$(\'gr-status\');if(el){el.textContent=\'DIAG · enabled=\'+g.enabled+\' · rate/s=\'+g.rate_per_sec+\' · \'+fmt(p1,2)+\'→\'+fmt(p2,2)+\' (\'+(moved>=0?\'+\':\'\')+moved.toFixed(2)+\' R) · \'+v;el.style.color=(g.enabled&&Math.abs(moved)>=0.001)?\'var(--green)\':\'var(--red)\';}\n    setMsg(\'gr-msg\',\'Diagnostic terminé\',true);\n  }catch(e){setMsg(\'gr-msg\',\'Erreur diag: \'+e.message,false);}\n}\n\nasync function setGrowth(dir){\n  var pct=parseFloat($(\'gr-pct\').value);\n  if(isNaN(pct)||pct<0){setMsg(\'gr-msg\',\'Valeur invalide\',false);return;}\n  var secs=parseFloat($(\'gr-unit\').value)||86400;\n  var f=dir>0?(1+pct/100):Math.max(0.0001,1-pct/100);\n  var rate=Math.pow(f,1/secs)-1;\n  var combine=$(\'gr-combine\').checked;\n  try{\n    var r=await fetch(\'/nxc/growth\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,rate_per_sec:rate,combine:combine,enabled:true})});\n    var d=await r.json();\n    if(d.ok){\n      var u={\'1\':\'seconde\',\'60\':\'minute\',\'3600\':\'heure\',\'86400\':\'jour\'}[$(\'gr-unit\').value]||\'jour\';\n      setMsg(\'gr-msg\',(dir>0?\'✅ +\':\'✅ -\')+pct+\'% / \'+u+(combine?\' (avec le cours)\':\' (trajectoire pure)\'),true);\n      addLog(dir>0?\'📈\':\'📉\',\'Croissance \'+(dir>0?\'+\':\'-\')+pct+\'%/\'+u);\n      updGrowthStatus();\n    } else setMsg(\'gr-msg\',\'❌ \'+(d.error||\'Erreur\'),false);\n  }catch(e){setMsg(\'gr-msg\',\'❌ Erreur réseau\',false);}\n}\nasync function stopGrowth(){\n  try{\n    var r=await fetch(\'/nxc/growth\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,rate_per_sec:0,enabled:false})});\n    var d=await r.json();\n    if(d.ok){setMsg(\'gr-msg\',\'⏸ Croissance arrêtée\',true);addLog(\'⏸\',\'Croissance stoppée\');updGrowthStatus();}\n  }catch(e){}\n}\nasync function updGrowthStatus(){\n  try{\n    var r=await fetch(\'/nxc/growth\');var d=await r.json();var el=$(\'gr-status\');if(!el)return;\n    if(d.enabled&&d.rate_per_sec){\n      var rps=d.rate_per_sec;var pd=(Math.pow(1+rps,86400)-1)*100;var txt=(rps>0?\'+\':\'\')+(Math.abs(rps*100)>=1?(rps*100).toFixed(2):(rps*100).toFixed(5))+\'%/s\';if(isFinite(pd)&&Math.abs(pd)<1000000)txt+=\' ≈ \'+(pd>0?\'+\':\'\')+(Math.abs(pd)<10?pd.toFixed(2):pd.toFixed(0))+\'%/jour\';\n      el.textContent=\'● Active : \'+txt+(d.combine?\' • avec le cours\':\' • trajectoire pure\');\n      el.style.color=d.rate_per_sec>0?\'var(--green)\':\'var(--red)\';\n    } else {el.textContent=\'○ Inactive\';el.style.color=\'var(--muted)\';}\n  }catch(e){}\n}\nasync function setPct(){var pct=parseFloat($(\'np-pct\').value)/100;if(isNaN(pct)){setMsg(\'pm\',\'% invalide\',false);return;}await adjP(pct);$(\'np-pct\').value=\'\';}\nasync function resetH(){if(!confirm(\'Reset historique ?\'))return;await fetch(\'/nxc/reset\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY})});addLog(\'🔄\',\'Reset historique NXC\');ref();}\n\nvar _tStart=null,_tTimerInt=null;\nfunction setT(m){\n  var s=parseFloat($(\'ts\').value)||0.005,iv=parseInt($(\'ti\').value)||12000;\n  if(tInt){clearInterval(tInt);tInt=null;}if(_tTimerInt){clearInterval(_tTimerInt);_tTimerInt=null;}\n  tMode=m===\'stop\'?null:m;tStr=s;tIv=iv;_tStart=tMode?Date.now():null;\n  var el=$(\'tst\'),ht=$(\'hc\');\n  if(!tMode){el.textContent=\'⏸ Arrêté\';el.style.color=\'var(--muted)\';if($(\'tt-timer\'))$(\'tt-timer\').textContent=\'\';addLog(\'⏸\',\'Tendance arrêtée\');return;}\n  var lbl=m===\'up\'?\'📈 Hausse +\':m===\'down\'?\'📉 Baisse -\':\'🎲 Aléatoire\';var spd=m!==\'random\'?(s*100).toFixed(1)+\'%\':\'\';\n  el.textContent=lbl+spd+\' · \'+(iv/1000)+\'s/tick\';el.style.color=m===\'up\'?\'var(--green)\':m===\'down\'?\'var(--red)\':\'var(--purple)\';\n  addLog(m===\'up\'?\'📈\':m===\'down\'?\'📉\':\'🎲\',\'Tendance \'+m+\' · \'+(s*100).toFixed(1)+\'%\');\n  _tTimerInt=setInterval(function(){if(_tStart){var el=elapsed=Math.floor((Date.now()-_tStart)/1000);$(\'tt-timer\').textContent=\'⏱ \'+Math.floor(el/60)+\'m\'+(\'0\'+(el%60)).slice(-2)+\'s\';}},1000);\n  if(tMode===\'up\'||tMode===\'down\'){fetch(\'/nxc/growth\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,rate_per_sec:(tMode===\'up\'?1:-1)*Math.min(1,s/12),combine:true,enabled:true})});}else{fetch(\'/nxc/growth\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,enabled:false,rate_per_sec:0})});}return;\n  tInt=setInterval(async function(){\n    var p=parseFloat(mkt.price||5213);var adj=(Math.random()-0.5)*_noiseLevel*2;\n    if(m===\'up\')adj+=s;else if(m===\'down\')adj-=s;\n    p=Math.max(parseFloat(_cfgFloor)||50,Math.min(parseFloat(_cfgCeil)||9999999,p*(1+adj)));\n    p=Math.random()>.03?Math.round(p*100)/100:Math.round(p);\n    await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:Math.floor(Math.random()*300+50),volume24:(mkt.volume24||0)+100,trades24:(mkt.trades24||0)+1})});\n  },iv);\n}\n\nasync function scenario(sc){\n  var p=parseFloat(mkt.price||5213),t;\n  if(sc===\'crash\')t=p*.7;else if(sc===\'moon\')t=p*1.3;else if(sc===\'ath\')t=Math.min(9999999,Math.max(p*1.5,90000));else if(sc===\'floor\')t=200;\n  if(t){t=Math.max(50,Math.min(9999999,Math.round(t*100)/100));await tick(t);addLog(\'🎭\',\'Scénario \'+sc+\' → \'+fmt(t,2)+\' R\');setTimeout(ref,500);}\n  else if(sc===\'volatile\'){setT(\'random\');addLog(\'⚡\',\'Scénario volatil\');}\n  else if(sc===\'stable\'){setT(\'stop\');addLog(\'😴\',\'Stabilisation\');}\n}\n\n// BANQUE\nfunction setAmt(v){$(\'bk-amt\').value=v;}\nfunction filterFlux(f){_fluxF=f;[\'fl-all\',\'fl-in\',\'fl-out\'].forEach(id=>{var e=$(id);if(e)e.className=\'btn\';});var e=$(\'fl-\'+f);if(e)e.className=\'btn cyan\';renderFlux();}\nfunction renderFlux(){\n  var flux=(_fluxF===\'all\'?_flux:_flux.filter(f=>f.type===_fluxF)).slice(0,30);\n  var el=$(\'bk-flux\');if(!el)return;\n  el.innerHTML=flux.length?flux.map(f=>\'<div class="fl-item"><div style="width:8px;height:8px;border-radius:50%;flex-shrink:0;background:\'+(f.type===\'IN\'?\'var(--green)\':\'var(--red)\')+\';box-shadow:0 0 6px \'+(f.type===\'IN\'?\'rgba(0,255,157,.4)\':\'rgba(255,61,94,.4)\')+\'"></div><span style="font-weight:700;color:\'+(f.type===\'IN\'?\'var(--green)\':\'var(--red)\')+\';flex-shrink:0">\'+(f.type===\'IN\'?\'+\':\'-\')+fmt(f.amount||0,0)+\' R</span><span style="color:var(--muted);flex:1">\'+esc(f.user||\'?\')+\'</span><span style="color:var(--muted);font-size:10px">\'+new Date(f.ts).toLocaleTimeString(\'fr-FR\')+\'</span></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucun flux</p>\';\n}\n\nfunction exportFlux(){var csv=\'Date,Type,User,Montant\\n\';_flux.forEach(f=>csv+=new Date(f.ts).toLocaleString(\'fr-FR\')+\',\'+f.type+\',\'+(f.user||\'\')+\',\'+(f.amount||0)+\'\\n\');var b=new Blob([csv],{type:\'text/csv\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(b);a.download=\'flux_\'+Date.now()+\'.csv\';a.click();addLog(\'📊\',\'Export CSV flux\');}\n\nasync function loadBank(){\n  try{\n    var r=await fetch(\'/nxc/bank\');var d=await r.json();if(!d.ok)return;var b=d.bank||{};\n    _flux=(b.flux||[]).slice().reverse();\n    var p=parseFloat(mkt.price||0);\n    $(\'bk-r\').textContent=fmt(b.reserves||0,0)+\' R\';$(\'bk-i\').textContent=fmt(b.totalIn||0,0);\n    $(\'bk-o\').textContent=fmt(b.totalOut||0,0);\n    $(\'bk-rt\').textContent=(b.totalIn>0?((b.reserves||0)/b.totalIn*100):100).toFixed(1)+\'%\';\n    $(\'bk-nx\').textContent=parseFloat(b.nxcEmis||0).toFixed(4)+\' NXC\';\n    $(\'bk-vx\').textContent=fmt((b.nxcEmis||0)*p,0)+\' R\';\n    var bn=(b.totalIn||0)-(b.totalOut||0);var el=$(\'bk-bn\');el.textContent=(bn>=0?\'+\':\'\')+fmt(bn,0)+\' R\';el.style.color=bn>=0?\'var(--green)\':\'var(--red)\';\n    $(\'bk-fl\').textContent=_flux.length;\n    renderFlux();_bank=b;try{renderBank2();}catch(e){}\n  }catch(e){}\n}\n\nasync function bankOp(type){\n  var amt=parseFloat($(\'bk-amt\').value);if(!amt||amt<=0){setMsg(\'bk-msg\',\'Montant invalide\',false);return;}\n  var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{reserves:0,totalIn:0,totalOut:0,nxcEmis:0,flux:[]};\n  if(type===\'out\'&&amt>(b.reserves||0)){setMsg(\'bk-msg\',\'❌ Réserves insuffisantes\',false);return;}\n  if(type===\'in\'){b.reserves=parseFloat(((b.reserves||0)+amt).toFixed(2));b.totalIn=parseFloat(((b.totalIn||0)+amt).toFixed(2));}\n  else{b.reserves=parseFloat(((b.reserves||0)-amt).toFixed(2));b.totalOut=parseFloat(((b.totalOut||0)+amt).toFixed(2));}\n  b.flux=b.flux||[];b.flux.push({type:type===\'in\'?\'IN\':\'OUT\',user:\'SERVEUR\',amount:amt,nxc:0,ts:Date.now()});\n  var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:b,reset:true})});\n  var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ \'+(type===\'in\'?\'+\':\'-\')+fmt(amt,0)+\' R\':\'❌ Erreur\',res.ok);\n  if(res.ok){$(\'bk-amt\').value=\'\';addLog(type===\'in\'?\'💰\':\'💸\',(type===\'in\'?\'Injection +\':\'Retrait -\')+fmt(amt,0)+\' R\');loadBank();}\n}\n\nasync function bankResetHist(){var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{};if(!confirm(\'Reset historique ? Réserves: \'+fmt(b.reserves||0,0)+\' R conservées\'))return;var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:{reserves:b.reserves||0,nxcEmis:0,totalIn:0,totalOut:0,flux:[]},reset:true})});var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ Historique effacé\':\'❌ Erreur\',res.ok);if(res.ok){addLog(\'🗑️\',\'Reset historique banque\');loadBank();}}\nasync function bankResetAll(){var cur=await(await fetch(\'/nxc/bank\')).json();var b=cur.bank||{};var g=confirm(\'Garder réserves (\'+fmt(b.reserves||0,0)+\' R) ?\');if(!confirm(\'Confirmer ?\'))return;var r=await fetch(\'/nxc/bank\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,bank:{reserves:g?(b.reserves||0):0,nxcEmis:0,totalIn:0,totalOut:0,flux:[]},reset:true})});var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ Réinitialisé\':\'❌ Erreur\',res.ok);if(res.ok){addLog(\'💥\',\'Reset complet banque\');loadBank();}}\n\nasync function loadFails(){\n  try{\n    var r=await fetch(\'/nxc/bank/fail\');var d=await r.json();\n    var el=$(\'bk-fails\'),fc=$(\'fails-ct\');if(!el)return;\n    var fails=(d.fails||[]).slice().reverse();\n    if(fails.length&&fc){fc.textContent=fails.length;fc.style.display=\'block\';}$(\'nd-b\').style.display=fails.length?\'block\':\'none\';\n    el.innerHTML=fails.length?fails.map(f=>\'<div style="padding:12px;border-bottom:1px solid rgba(255,61,94,.08);display:flex;flex-direction:column;gap:6px"><div style="display:flex;justify-content:space-between"><span style="color:var(--red);font-weight:700">❌ \'+esc(f.user)+\'</span><span style="color:var(--muted);font-size:10px;font-family:monospace">\'+new Date(f.ts).toLocaleTimeString(\'fr-FR\')+\'</span></div><div style="color:var(--muted);font-size:11px">Voulait vendre <b style="color:var(--text)">\'+f.nxc+\' NXC</b> (\'+fmt(f.amount||0,0)+\' R)</div>\'+(f.gesture>0?\'<button onclick="sendGesture(\\\'\'+esc(f.user)+\'\\\',\'+f.gesture+\',\'+f.ts+\')" style="padding:8px 14px;background:rgba(0,255,157,.1);border:1px solid rgba(0,255,157,.3);border-radius:9px;color:var(--green);font-size:12px;cursor:pointer;font-weight:700;align-self:flex-start">💝 Verser +\'+f.gesture+\' R</button>\':\'\')+\'</div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">✅ Aucune tentative</p>\';\n  }catch(e){}\n}\n\nasync function sendGesture(user,amount,failTs){\n  if(!confirm(\'Verser \'+amount+\' R à \'+user+\' ?\'))return;\n  var r=await fetch(\'/nxc/bank/gesture\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,target:user,amount:amount,fail_ts:failTs})});\n  var res=await r.json();setMsg(\'bk-msg\',res.ok?\'✅ \'+amount+\' R versés à \'+user:\'❌ \'+(res.error||\'Erreur\'),res.ok);\n  if(res.ok){addLog(\'💝\',\'Geste +\'+amount+\' R → \'+user);loadBank();loadFails();}\n}\n\n// APP (iframe configurable)\nfunction renderSavedSites(){\n  var el=$(\'saved-sites\');if(!el)return;\n  var pinned=_pinnedSites.map(s=>s.url);\n  el.innerHTML=_savedSites.length?_savedSites.map(s=>{\n    var isPinned=pinned.includes(s.url);\n    return \'<div style="display:flex;align-items:center;gap:4px;background:var(--bg3);border:1px solid \'+(isPinned?\'rgba(255,176,32,.4)\':\'var(--border)\')+\';border-radius:8px;padding:4px 8px;white-space:nowrap">\'\n      +\'<button onclick="loadSite(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" style="background:none;border:none;color:\'+(isPinned?\'var(--gold)\':\'var(--cyan)\')+\';font-size:11px;font-weight:700;cursor:pointer;padding:0">\'+(isPinned?\'📌 \':\'\')+esc(s.label)+\'</button>\'\n      +\'<button onclick="togglePin(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" title="\'+(isPinned?\'Désépingler\':\'Épingler\')+\'" style="background:none;border:none;color:\'+(isPinned?\'var(--gold)\':\'var(--muted)\')+\';font-size:11px;cursor:pointer;padding:0;margin-left:2px">\'+(isPinned?\'📌\':\'📍\')+\'</button>\'\n      +\'<button onclick="deleteSite(\\\'\'+esc(s.url)+\'\\\')" style="background:none;border:none;color:var(--red);font-size:12px;cursor:pointer;padding:0;margin-left:2px">✕</button>\'\n      +\'</div>\';\n  }).join(\'\'):\'<span style="color:var(--muted);font-size:11px">Aucun site sauvegardé</span>\';\n  // Afficher les sites épinglés en premier si existants\n  renderPinnedBar();\n}\nfunction goUrl(){var url=$(\'iframe-in\').value.trim();if(!url)return;if(!url.startsWith(\'http\'))url=\'https://\'+url;loadSite(url,null);$(\'iframe-in\').value=\'\';}\nfunction loadSite(url,label){_curUrl=url;var f=$(\'nf\');if(f)f.src=url;var t=$(\'if-title\');if(t)t.textContent=\'◈ \'+(label||url.replace(\'https://\',\'\').split(\'/\')[0]);var u=$(\'if-url\');if(u)u.textContent=url.replace(\'https://\',\'\').replace(\'http://\',\'\');}\nfunction saveSite(){var url=$(\'iframe-in\').value.trim()||_curUrl;var lbl=$(\'site-lbl\').value.trim()||url.replace(\'https://\',\'\').split(\'/\')[0];if(!url)return;if(!url.startsWith(\'http\'))url=\'https://\'+url;_savedSites=_savedSites.filter(s=>s.url!==url);_savedSites.unshift({label:lbl,url});if(_savedSites.length>8)_savedSites.pop();localStorage.setItem(\'nxc_sites\',JSON.stringify(_savedSites));$(\'site-lbl\').value=\'\';$(\'iframe-in\').value=\'\';renderSavedSites();addLog(\'💾\',\'Site sauvegardé: \'+lbl);}\nfunction deleteSite(url){_savedSites=_savedSites.filter(s=>s.url!==url);localStorage.setItem(\'nxc_sites\',JSON.stringify(_savedSites));renderSavedSites();}\nfunction reloadF(){var f=$(\'nf\');if(f)f.src=f.src;}\nfunction openNewTab(){if(_curUrl)window.open(_curUrl,\'_blank\');}\n\n// ADMIN\nasync function refreshAdminStats(){\n  try{\n    var pd=await fetch(\'/nxc/price\').then(r=>r.json());\n    var bd=await fetch(\'/nxc/bank\').then(r=>r.json());\n    var fd=await fetch(\'/nxc/bank/fail\').then(r=>r.json());\n    var b=bd.bank||{};var p=parseFloat(pd.price||0);\n    $(\'adm-price\').textContent=fmt(p,2)+\' R\';\n    $(\'adm-vol\').textContent=fmt(pd.volume24||0,0)+\' R\';\n    $(\'adm-trades\').textContent=pd.trades24||0;\n    $(\'adm-res\').textContent=fmt(b.reserves||0,0)+\' R\';\n    $(\'adm-nxc\').textContent=parseFloat(b.nxcEmis||0).toFixed(4);\n    $(\'adm-fails\').textContent=(fd.fails||[]).length;\n    $(\'adm-hist\').textContent=(pd.history||[]).length;\n    if(_users.length)$(\'adm-users\').textContent=_users.length;\n    addLog(\'📊\',\'Stats admin actualisées\');\n  }catch(e){}\n}\n\nasync function loadAdmUsers(){\n  if(!_users.length)await loadUsers();\n  var sel1=$(\'rw-u\'),sel2=$(\'role-u\');\n  [sel1,sel2].forEach(sel=>{if(sel)sel.innerHTML=\'<option value="">Utilisateur...</option>\'+_users.map(u=>\'<option value="\'+esc(u.n)+\'">\'+esc(u.n)+(u.role===\'admin\'?\' 👑\':u.role===\'moderator\'?\' 🛡️\':u.role===\'vip\'?\' ⭐\':\'\')+\'</option>\').join(\'\');});\n  $(\'adm-users\').textContent=_users.length;\n  renderAdmUsers(_users);\n}\n\nfunction renderAdmUsers(rows){\n  var el=$(\'adm-ut\');if(!el)return;\n  el.innerHTML=rows.map(r=>\'<tr><td style="font-weight:700;color:var(--cyan)">\'+esc(r.n)+(r.role===\'admin\'?\' 👑\':r.role===\'moderator\'?\' 🛡️\':r.role===\'vip\'?\' ⭐\':\'\')+\'</td><td style="color:var(--muted);font-size:10px">\'+esc(r.role)+\'</td><td style="color:var(--gold)">\'+fmt(r.rew,0)+\'</td><td style="color:var(--cyan);font-family:monospace">\'+r.nxc.toFixed(4)+\'</td><td style="color:var(--purple)">\'+fmt(r.val,0)+\'</td></tr>\').join(\'\');\n}\nfunction filterAdmUsers(){var q=($(\'adm-q\').value||\'\').toLowerCase();renderAdmUsers(q?_users.filter(u=>u.n.toLowerCase().includes(q)):_users);}\n\nasync function giveRewards(){\n  var target=$(\'rw-u\').value,amt=parseFloat($(\'rw-amt\').value);\n  if(!target||!amt||amt<=0){setMsg(\'rw-msg\',\'Remplir tous les champs\',false);return;}\n  var r=await fetch(\'/admin/give-rewards\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,target:target,amount:amt})});\n  var res=await r.json();\n  setMsg(\'rw-msg\',res.ok?\'✅ +\'+fmt(amt,0)+\' R donnés à \'+target+\' (total: \'+fmt(res.new_rewards||0,0)+\' R)\':\'❌ \'+(res.error||\'Erreur\'),res.ok);\n  if(res.ok){addLog(\'🏆\',\'Rewards +\'+fmt(amt,0)+\' R → \'+target);}\n}\n\nasync function changeRole(){\n  var u=$(\'role-u\').value,role=$(\'role-v\').value;\n  if(!u){setMsg(\'role-msg\',\'Sélectionner un utilisateur\',false);return;}\n  var r=await fetch(\'/admin/set-role\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,target:u,role:role})});\n  var res=await r.json();\n  setMsg(\'role-msg\',res.ok?\'✅ Rôle de \'+u+\' changé en \'+role:\'❌ \'+(res.error||\'Erreur\'),res.ok);\n  if(res.ok)addLog(\'👑\',\'Rôle \'+u+\' → \'+role);\n}\n\nasync function pruneHistory(){if(!confirm(\'Réduire historique à 100 points ?\'))return;await fetch(\'/nxc/reset\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY})});setMsg(\'maint-msg\',\'✅ Historique réduit\',true);addLog(\'✂️\',\'Historique NXC réduit\');}\nasync function resetAllTrades(){if(!confirm(\'Reset trades 24h ?\'))return;await tick(parseFloat(mkt.price||5213));setMsg(\'maint-msg\',\'✅ Trades remis à zéro\',true);addLog(\'🗑️\',\'Reset trades 24h\');}\n\nasync function backupDB(){\n  try{var p=await(await fetch(\'/nxc/price\')).json();var b=await(await fetch(\'/nxc/bank\')).json();var u=await api(\'/admin/list\');var data={date:new Date().toISOString(),market:p,bank:b.bank||{},users:u.users||[]};var blob=new Blob([JSON.stringify(data,null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_backup_\'+Date.now()+\'.json\';a.click();setMsg(\'maint-msg\',\'✅ Backup téléchargé\',true);addLog(\'💾\',\'Backup DB téléchargé\');}catch(e){setMsg(\'maint-msg\',\'❌ Erreur backup\',false);}\n}\n\nasync function pingServer(){\n  var el=$(\'ping-res\');if(el){el.textContent=\'📡 Test...\';el.style.color=\'var(--muted)\';}\n  var t=Date.now();\n  try{await fetch(\'/nxc/price\');var lat=Date.now()-t;var c=lat<500?\'var(--green)\':lat<1000?\'var(--gold)\':\'var(--red)\';if(el){el.textContent=\'✅ En ligne — \'+lat+\' ms\';el.style.color=c;}}\n  catch(e){if(el){el.textContent=\'❌ Inaccessible\';el.style.color=\'var(--red)\';}}\n}\n\n// USERS\nasync function loadUsers(){\n  $(\'us-msg\').textContent=\'Chargement…\';\n  try{\n    var r=await api(\'/admin/list\');if(!r||!r.ok){$(\'us-msg\').textContent=\'Erreur\';return;}\n    var p=parseFloat(mkt.price||0);\n    var rows=await Promise.all((r.users||[]).map(async u=>{\n      var d=await api(\'/admin/get\',{target:u.username});\n      var rew=Math.max((d.data&&d.data.nx2098&&d.data.nx2098.rewards)||0,(d.data&&d.data.rewards&&d.data.rewards.points)||0);\n      var nxc=parseFloat((d.data&&d.data.nxcoin&&d.data.nxcoin.nxc)||0);\n      return {n:u.username,role:u.role,rew,nxc,val:nxc*p};\n    }));\n    _users=rows;\n    $(\'u-total\').textContent=rows.length;$(\'u-admins\').textContent=rows.filter(r=>r.role===\'admin\').length;\n    $(\'u-rew\').textContent=fmt(rows.reduce((s,r)=>s+r.rew,0),0);\n    sortU(\'rew\');$(\'us-msg\').textContent=\'\';\n    if($(\'adm-ut\'))loadAdmUsers();\n  }catch(e){$(\'us-msg\').textContent=\'Erreur\';}\n}\nfunction sortU(by){_users.sort((a,b)=>by===\'name\'?a.n.localeCompare(b.n):(b[by]-a[by]));renderU(_users);}\nfunction renderU(rows){var el=$(\'ut\');if(!el)return;el.innerHTML=rows.map(r=>\'<tr><td style="font-weight:700;color:var(--cyan)">\'+esc(r.n)+(r.role===\'admin\'?\' 👑\':r.role===\'moderator\'?\' 🛡️\':r.role===\'vip\'?\' ⭐\':\'\')+\'</td><td style="color:var(--muted);font-size:10px">\'+esc(r.role)+\'</td><td style="color:var(--gold)">\'+fmt(r.rew,0)+\'</td><td style="color:var(--cyan);font-family:monospace">\'+r.nxc.toFixed(4)+\'</td><td style="color:var(--purple)">\'+fmt(r.val,0)+\'</td></tr>\').join(\'\');}\nfunction filterU(){var q=($(\'us-q\').value||\'\').toLowerCase();renderU(q?_users.filter(r=>r.n.toLowerCase().includes(q)):_users);}\n\n// STATS\nvar volObj=null;\nasync function loadStats(){\n  if(!_users.length)await loadUsers();\n  var h=mkt.history||[];var p=parseFloat(mkt.price||0);\n  if(h.length>5){var cv=$(\'ch-vol\');if(cv&&window.Chart){var pts=h.slice(-20);var labs=pts.map(x=>new Date(x.ts).toLocaleTimeString(\'fr-FR\',{hour:\'2-digit\',minute:\'2-digit\'}));var vols=pts.map(x=>x.vol||0);if(volObj){volObj.data.labels=labs;volObj.data.datasets[0].data=vols;volObj.update(\'none\');}else{var ctx=cv.getContext(\'2d\');volObj=new Chart(ctx,{type:\'bar\',data:{labels:labs,datasets:[{data:vols,backgroundColor:\'rgba(160,107,255,.5)\',borderColor:\'#a06bff\',borderWidth:1}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:\'#5c6b8c\',maxTicksLimit:4,font:{size:8}},grid:{display:false}},y:{ticks:{color:\'#5c6b8c\'},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});}}}\n  var el=$(\'rew-bars\');if(el&&_users.length){var maxR=Math.max(..._users.map(u=>u.rew))||1;el.innerHTML=[..._users].sort((a,b)=>b.rew-a.rew).slice(0,8).map(u=>\'<div style="margin-bottom:8px"><div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:3px"><span style="color:var(--cyan);font-weight:700">\'+esc(u.n)+\'</span><span style="color:var(--gold)">\'+fmt(u.rew,0)+\' R</span></div><div class="pbar"><div class="pbar-fill" style="width:\'+Math.round(u.rew/maxR*100)+\'%"></div></div></div>\').join(\'\');}\n  var hi=h.length>1?Math.max(...h.slice(-24).map(x=>x.price)):p;var lo=h.length>1?Math.min(...h.slice(-24).map(x=>x.price)):p;var vol=lo>0?(hi-lo)/lo*100:0;\n  var hg=$(\'health-grid\');if(hg)hg.innerHTML=[[\'📈 Tendance\',h.length>5?(h.slice(-5).map(x=>x.price).every((v,i,a)=>i===0||v>a[i-1])?\'<span style="color:var(--green)">Haussière</span>\':h.slice(-5).map(x=>x.price).every((v,i,a)=>i===0||v<a[i-1])?\'<span style="color:var(--red)">Baissière</span>\':\'<span style="color:var(--muted)">Neutre</span>\'):\'—\'],[\'⚡ Volatilité\',vol.toFixed(2)+\'%\'],[\'📊 Amplitude\',fmt(hi-lo,0)+\' R\'],[\'🔢 Trades\',mkt.trades24||0]].map(([k,v])=>\'<div class="st"><div class="sv" style="font-size:12px">\'+v+\'</div><div class="sl">\'+k+\'</div></div>\').join(\'\');\n}\n\n// SOLVABILITÉ\nasync function loadSolv(){try{var r=await fetch(\'/nxc/solvability\');var d=await r.json();if(d.ok){solvOn=d.enabled;var inp=$(\'sg\');if(inp)inp.value=d.gesture||50;updSolv();}}catch(e){}}\nfunction updSolv(){var t=$(\'stg\'),l=$(\'sl\');if(solvOn){if(t)t.classList.add(\'on\');if(l){l.textContent=\'✅ Activée\';l.style.color=\'var(--green)\';}}else{if(t)t.classList.remove(\'on\');if(l){l.textContent=\'⏸ Désactivée\';l.style.color=\'var(--muted)\';}}}\nasync function toggleSolv(){solvOn=!solvOn;updSolv();await saveSolv();}\nasync function saveSolv(){var g=parseInt($(\'sg\').value)||50;var r=await fetch(\'/nxc/solvability\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,enabled:solvOn,gesture:g})});var res=await r.json();setMsg(\'sm\',res.ok?(solvOn?\'✅ Activée\':\'⏸ Désactivée\'):\'❌ Erreur\',res.ok);if(res.ok)addLog(\'🛡️\',\'Solvabilité \'+(solvOn?\'activée\':\'désactivée\'));}\n\n// OUTILS\nvar _noiseLevel=0.004;\nfunction updateNoise(v){\n  _noiseLevel=parseFloat(v)/1000;\n  var el=$(\'noise-val\');if(el)el.textContent=(parseFloat(v)/10).toFixed(1)+\'%\';\n}\nfunction calcN(){var n=parseFloat($(\'c-nxc\').value)||0;var p=parseFloat(mkt.price||0);$(\'c-rew\').value=n&&p?Math.round(n*p*100)/100:\'\';}\nfunction calcR(){var r=parseFloat($(\'c-rew2\').value)||0;var p=parseFloat(mkt.price||1);$(\'c-nxc2\').value=r&&p?(r/p).toFixed(6):\'\';}\nfunction simS(){var n=parseFloat($(\'ss-nxc\').value)||0;var fee=parseFloat($(\'ss-fee\').value)||0;var p=parseFloat(mkt.price||0);if(!n||!p){$(\'ss-res\').innerHTML=\'\';return;}var gross=n*p;var fees=gross*fee/100;var net=gross-fees;$(\'ss-res\').innerHTML=\'Brut: <b style="color:var(--text)">\'+fmt(gross,2)+\' R</b> · Frais: <b style="color:var(--red)">-\'+fmt(fees,2)+\' R</b> · <b style="color:var(--green);font-size:16px">Net: \'+fmt(net,2)+\' R</b>\';}\n\nvar _tmEnd=null;\nfunction startTimer(){var m=parseInt($(\'tm-m\').value)||0;var s=parseInt($(\'tm-s\').value)||0;var total=m*60+s;var action=$(\'tm-a\').value;if(!total)return;if(_tmInt)clearInterval(_tmInt);_tmEnd=Date.now()+total*1000;addLog(\'⏱️\',\'Minuteur: \'+action+\' dans \'+total+\'s\');_tmInt=setInterval(async function(){var rem=Math.max(0,Math.round((_tmEnd-Date.now())/1000));var el=$(\'tm-disp\');if(el)el.textContent=(\'0\'+Math.floor(rem/60)).slice(-2)+\':\'+(\'0\'+(rem%60)).slice(-2);if(rem<=0){clearInterval(_tmInt);_tmInt=null;if(el){el.textContent=\'✅\';el.style.color=\'var(--green)\';}if(action===\'stop\')setT(\'stop\');else if(action===\'up\'||action===\'down\')setT(action);else if(action===\'crash\'||action===\'moon\')scenario(action);addLog(\'⏱️\',\'Minuteur déclenché: \'+action);}},500);}\nfunction stopTimer(){if(_tmInt){clearInterval(_tmInt);_tmInt=null;var d=$(\'tm-disp\');if(d)d.textContent=\'\';}}\n\n// CONFIG\nfunction updCfg(){\n  var txt=\'Plancher: \'+(_cfgFloor?fmt(_cfgFloor,0)+\' R\':\'non défini\')+\' · Plafond: \'+(_cfgCeil?fmt(_cfgCeil,0)+\' R\':\'non défini\');\n  var el=$(\'cfg-info\');if(el)el.textContent=txt;\n  updFloorDisplay();\n}\nfunction updFloorDisplay(){\n  var txt=\'Plancher: \'+(_cfgFloor?fmt(_cfgFloor,0)+\' R\':\'non défini\')+\' · Plafond: \'+(_cfgCeil?fmt(_cfgCeil,0)+\' R\':\'non défini\');\n  var el=$(\'floor-display\');if(el)el.textContent=txt;\n  var ec=$(\'cfg-info\');if(ec)ec.textContent=txt;\n}\nfunction setFloor(){\n  var v=parseFloat($(\'t-floor\').value);if(!v||v<50){alert(\'Plancher invalide (min 50R)\');return;}\n  _cfgFloor=v;updFloorDisplay();addLog(\'⚙️\',\'Plancher: \'+fmt(v,0)+\' R\');\n}\nfunction setCeil(){\n  var v=parseFloat($(\'t-ceil\').value);if(!v||v<1){alert(\'Plafond invalide\');return;}\n  _cfgCeil=v;updFloorDisplay();addLog(\'⚙️\',\'Plafond: \'+fmt(v,0)+\' R\');\n}\nfunction setNormalMode(){\n  // Cours normal = tendance aléatoire légère avec plancher/plafond actifs\n  if(!_cfgFloor&&!_cfgCeil){alert(\'Définir au moins un plancher ou un plafond\');return;}\n  setT(\'stop\'); // Arrêter toute tendance\n  // Lancer une légère variation aléatoire neutre\n  var iv=parseInt($(\'ti\').value)||12000;\n  if(tInt){clearInterval(tInt);tInt=null;}\n  tMode=\'normal\';\n  var el=$(\'tst\');el.textContent=\'📊 Cours normal · plancher: \'+(_cfgFloor?fmt(_cfgFloor,0)+\'R\':\'—\')+\' · plafond: \'+(_cfgCeil?fmt(_cfgCeil,0)+\'R\':\'—\');el.style.color=\'var(--cyan)\';\n  addLog(\'📊\',\'Cours normal activé\');\n  if(tMode===\'up\'||tMode===\'down\'){fetch(\'/nxc/growth\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,rate_per_sec:(tMode===\'up\'?1:-1)*Math.min(1,s/12),combine:true,enabled:true})});}else{fetch(\'/nxc/growth\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,enabled:false,rate_per_sec:0})});}return;\n  tInt=setInterval(async function(){\n    var p=parseFloat(mkt.price||5213);\n    var adj=(Math.random()-0.5)*_noiseLevel*0.5; // très légère variation\n    p=Math.max(_cfgFloor||50,Math.min(_cfgCeil||9999999,p*(1+adj)));\n    p=Math.round(p*100)/100;\n    await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:Math.floor(Math.random()*100+10),volume24:(mkt.volume24||0)+50,trades24:(mkt.trades24||0)+1})});\n  },iv);\n}\nfunction scheduleT(){var st=$(\'cfg-st\').value,sp=$(\'cfg-sp\').value,dir=$(\'cfg-sd\').value;if(!st||!sp){setMsg(\'cfg-sch-msg\',\'Renseigner les deux heures\',false);return;}if(_schedInt)clearInterval(_schedInt);_schedInt=setInterval(function(){var now=new Date();var cur=(\'0\'+now.getHours()).slice(-2)+\':\'+(\'0\'+now.getMinutes()).slice(-2);if(cur===st&&!tMode)setT(dir);if(cur===sp&&tMode)setT(\'stop\');},30000);setMsg(\'cfg-sch-msg\',\'✅ Programmé: \'+dir+\' \'+st+\'→\'+sp,true);addLog(\'⏰\',\'Tendance programmée \'+dir+\' \'+st+\'→\'+sp);}\n\nfunction exportHist(){var h=mkt.history||[];var b=new Blob([JSON.stringify({date:new Date().toISOString(),price:mkt.price,history:h},null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(b);a.download=\'nxc_history_\'+Date.now()+\'.json\';a.click();addLog(\'📥\',\'Export historique JSON\');}\nfunction exportStats(){var b=new Blob([JSON.stringify({date:new Date().toISOString(),market:mkt,users:_users},null,2)],{type:\'application/json\'});var a=document.createElement(\'a\');a.href=URL.createObjectURL(b);a.download=\'nxc_report_\'+Date.now()+\'.json\';a.click();addLog(\'📊\',\'Export rapport JSON\');}\n\n// ALERTES\nfunction addAlert(){var price=parseFloat($(\'al-p\').value),dir=$(\'al-d\').value;if(!price)return;_alerts.push({price,dir,id:Date.now(),triggered:false});$(\'al-p\').value=\'\';renderAlerts();addLog(\'🔔\',\'Alerte: prix \'+(dir===\'above\'?\'>\':\'<\')+\' \'+fmt(price,0)+\' R\');}\nfunction removeAlert(id){_alerts=_alerts.filter(a=>a.id!==id);renderAlerts();}\nfunction renderAlerts(){var el=$(\'al-list\');if(!el)return;el.innerHTML=_alerts.length?_alerts.map(a=>\'<div style="padding:10px 12px;border-bottom:1px solid rgba(0,229,255,.05);display:flex;justify-content:space-between;align-items:center;font-size:12px"><span style="color:\'+(a.triggered?\'var(--muted)\':\'var(--gold)\')+\'">Prix \'+(a.dir===\'above\'?\'>\':\'<\')+\' \'+fmt(a.price,0)+\' R\'+(a.triggered?\' ✅\':\'\')+\'</span><button onclick="removeAlert(\'+a.id+\')" style="padding:4px 8px;border-radius:6px;background:rgba(255,61,94,.1);border:1px solid rgba(255,61,94,.3);color:var(--red);font-size:10px;cursor:pointer">✕</button></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune alerte</p>\';}\nfunction checkAlerts(p){_alerts.forEach(function(a){if(a.triggered)return;if((a.dir===\'above\'&&p>a.price)||(a.dir===\'below\'&&p<a.price)){a.triggered=true;var m=\'🔔 Prix \'+(a.dir===\'above\'?\'>\':\'<\')+\' \'+fmt(a.price,0)+\' R (actuel: \'+fmt(p,0)+\' R)\';_alHist.unshift({ts:Date.now(),msg:m});addLog(\'🔔\',m);renderAlerts();renderAlHist();if(window.Notification&&Notification.permission===\'granted\')new Notification(\'◈ Nexus NXC\',{body:m});}});}\nfunction renderAlHist(){var el=$(\'al-hist\');if(!el)return;el.innerHTML=_alHist.length?_alHist.map(a=>\'<div class="log-item"><span class="log-time">\'+fmtT(a.ts)+\'</span><span style="color:var(--gold)">\'+esc(a.msg)+\'</span></div>\').join(\'\'):\'<p style="color:var(--muted);padding:16px;text-align:center;font-size:12px">Aucune</p>\';}\nif(window.Notification&&Notification.permission===\'default\')setTimeout(function(){Notification.requestPermission();},3000);\n\n// ══ ÉPINGLAGE SITES (sync cross-device via serveur) ══\nvar _pinnedSites=[];\n\nasync function loadPinnedSites(){\n  try{\n    var r=await fetch(\'/admin/pinned-sites\');var d=await r.json();\n    if(d.ok){_pinnedSites=d.sites||[];renderSavedSites();}\n  }catch(e){_pinnedSites=JSON.parse(localStorage.getItem(\'nxc_pinned\')||\'[]\');}\n}\n\nasync function togglePin(url,label){\n  var idx=_pinnedSites.findIndex(s=>s.url===url);\n  if(idx>=0)_pinnedSites.splice(idx,1);\n  else _pinnedSites.push({url,label});\n  // Sauvegarder sur le serveur ET en local\n  localStorage.setItem(\'nxc_pinned\',JSON.stringify(_pinnedSites));\n  try{await fetch(\'/admin/pinned-sites\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,sites:_pinnedSites})});}catch(e){}\n  renderSavedSites();\n  addLog(\'📌\',(idx>=0?\'Désépinglé\':\'Épinglé\')+\': \'+label);\n}\n\nfunction renderPinnedBar(){\n  var el=$(\'pinned-bar\');if(!el)return;\n  if(!_pinnedSites.length){el.style.display=\'none\';return;}\n  el.style.display=\'flex\';\n  el.innerHTML=_pinnedSites.map(s=>\'<button onclick="loadSite(\\\'\'+esc(s.url)+\'\\\',\\\'\'+esc(s.label)+\'\\\')" style="padding:5px 12px;background:rgba(255,176,32,.12);border:1px solid rgba(255,176,32,.3);border-radius:8px;color:var(--gold);font-size:11px;font-weight:700;cursor:pointer;white-space:nowrap">📌 \'+esc(s.label)+\'</button>\').join(\'\');\n}\n\n// ══ SAUVEGARDE / IMPORT DONNÉES GLOBALES ══\nasync function saveAllData(){\n  try{\n    var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'export\'})});\n    var d=await r.json();\n    if(!d.ok){setMsg(\'data-msg\',\'❌ Erreur export\',false);return;}\n    var blob=new Blob([JSON.stringify(d.data,null,2)],{type:\'application/json\'});\n    var a=document.createElement(\'a\');a.href=URL.createObjectURL(blob);a.download=\'nexus_full_backup_\'+Date.now()+\'.json\';a.click();\n    setMsg(\'data-msg\',\'✅ Backup complet téléchargé\',true);\n    addLog(\'💾\',\'Sauvegarde complète téléchargée\');\n  }catch(e){setMsg(\'data-msg\',\'❌ Erreur: \'+e.message,false);}\n}\n\nfunction importData(){\n  var input=document.createElement(\'input\');input.type=\'file\';input.accept=\'.json\';\n  input.onchange=async function(e){\n    var file=e.target.files[0];if(!file)return;\n    var text=await file.text();\n    try{\n      var data=JSON.parse(text);\n      if(!confirm(\'Importer ces données ? Cela écrasera les données actuelles du serveur.\'))return;\n      var r=await fetch(\'/admin/save-data\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,action:\'import\',data:data})});\n      var res=await r.json();\n      setMsg(\'data-msg\',res.ok?\'✅ Données importées avec succès\':\'❌ \'+(res.error||\'Erreur import\'),res.ok);\n      if(res.ok){addLog(\'📥\',\'Données importées depuis fichier\');setTimeout(function(){ref();loadBank();},1000);}\n    }catch(ex){setMsg(\'data-msg\',\'❌ Fichier JSON invalide\',false);}\n  };\n  input.click();\n}\n\n// ══ IMPRESSION ══\nfunction printDashboard(){\n  var p=parseFloat(mkt.price||0);var h=mkt.history||[];\n  var hi=h.length>1?Math.max(...h.slice(-24).map(x=>x.price)):p;\n  var lo=h.length>1?Math.min(...h.slice(-24).map(x=>x.price)):p;\n  var chg=_prevP>0?((p-_prevP)/_prevP*100):0;\n  // Capturer le graphique en PNG\n  function _snap(id){var cv=$(id);if(!cv||!cv.width||!cv.height)return \'\';try{var tmp=document.createElement(\'canvas\');tmp.width=cv.width;tmp.height=cv.height;var x=tmp.getContext(\'2d\');x.fillStyle=\'#ffffff\';x.fillRect(0,0,tmp.width,tmp.height);x.drawImage(cv,0,0);return tmp.toDataURL(\'image/png\');}catch(e){return \'\';}}\n  var chartImg=_snap(\'ch\');\n  var rsiImg=_snap(\'ch-rsi\');\n  var now=new Date().toLocaleString(\'fr-FR\');\n  var win=window.open(\'\',\'_blank\');\n  if(!win){alert(\'Autorise les pop-ups pour imprimer le rapport.\');return;}\n  win.document.write(\'<!DOCTYPE html><html><head><meta charset="utf-8"><title>◈ Nexus NXC — Rapport \'+now+\'</title><style>*{font-family:Arial,sans-serif;box-sizing:border-box}body{background:#fff;color:#000;padding:20px;max-width:900px;margin:0 auto}.header{text-align:center;border-bottom:3px solid #000;padding-bottom:16px;margin-bottom:20px}.title{font-size:28px;font-weight:900;letter-spacing:3px}.date{font-size:12px;color:#666;margin-top:4px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}.stat{border:1px solid #ddd;border-radius:8px;padding:12px;text-align:center}.stat-val{font-size:20px;font-weight:700;margin-bottom:4px}.stat-lbl{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:#666}img{max-width:100%;border:1px solid #ddd;border-radius:8px;margin-bottom:12px}h3{margin:16px 0 8px;font-size:14px;border-bottom:1px solid #eee;padding-bottom:4px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:8px;text-align:left;border:1px solid #ddd}th{background:#f5f5f5;font-weight:700}@media print{.no-print{display:none}}</style></head><body>\');\n  win.document.write(\'<div class="header"><div class="title">◈ NEXUS NXC</div><div class="date">Rapport généré le \'+now+\'</div></div>\');\n  win.document.write(\'<div class="grid">\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(p,2)+\' R</div><div class="stat-lbl">Prix actuel</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(chg>=0?\'+\':\'\')+chg.toFixed(2)+\'%</div><div class="stat-lbl">Variation</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(hi,0)+\' R</div><div class="stat-lbl">Haut 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(lo,0)+\' R</div><div class="stat-lbl">Bas 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+fmt(mkt.volume24||0,0)+\' R</div><div class="stat-lbl">Volume 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(mkt.trades24||0)+\'</div><div class="stat-lbl">Trades 24h</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+h.length+\'</div><div class="stat-lbl">Points hist.</div></div>\');\n  win.document.write(\'<div class="stat"><div class="stat-val">\'+(_users.length||0)+\'</div><div class="stat-lbl">Utilisateurs</div></div>\');\n  win.document.write(\'</div>\');\n  if(chartImg)win.document.write(\'<h3>Historique du cours (\'+_ctRange+\' derniers points)</h3><img src="\'+chartImg+\'">\');\n  if(rsiImg)win.document.write(\'<h3>RSI (14 ticks)</h3><img src="\'+rsiImg+\'">\');\n  if(_users.length){\n    win.document.write(\'<h3>Utilisateurs</h3><table><thead><tr><th>Compte</th><th>Rôle</th><th>Rewards</th><th>NXC</th><th>Valeur (R)</th></tr></thead><tbody>\');\n    _users.forEach(u=>{win.document.write(\'<tr><td>\'+esc(u.n)+\'</td><td>\'+esc(u.role)+\'</td><td>\'+fmt(u.rew,0)+\'</td><td>\'+u.nxc.toFixed(4)+\'</td><td>\'+fmt(u.val,0)+\'</td></tr>\');});\n    win.document.write(\'</tbody></table>\');\n  }\n  win.document.write(\'<h3>Derniers logs</h3><table><thead><tr><th>Heure</th><th>Action</th></tr></thead><tbody>\');\n  _log.slice(0,20).forEach(l=>{win.document.write(\'<tr><td>\'+fmtT(l.ts)+\'</td><td>\'+l.ico+\' \'+esc(l.txt)+\'</td></tr>\');});\n  win.document.write(\'</tbody></table>\');\n    var bR=document.getElementById(\'bk-r\')?document.getElementById(\'bk-r\').textContent:\'—\';\n  var bI=document.getElementById(\'bk-i\')?document.getElementById(\'bk-i\').textContent:\'—\';\n  var bO=document.getElementById(\'bk-o\')?document.getElementById(\'bk-o\').textContent:\'—\';\n  var bRt=document.getElementById(\'bk-rt\')?document.getElementById(\'bk-rt\').textContent:\'—\';\n  var bNx=document.getElementById(\'bk-nx\')?document.getElementById(\'bk-nx\').textContent:\'—\';\n  var bVx=document.getElementById(\'bk-vx\')?document.getElementById(\'bk-vx\').textContent:\'—\';\n  var bBn=document.getElementById(\'bk-bn\')?document.getElementById(\'bk-bn\').textContent:\'—\';\n  var bFl=document.getElementById(\'bk-fl\')?document.getElementById(\'bk-fl\').textContent:\'—\';\n  win.document.write(\'<hr style="margin:20px 0;border:none;border-top:2px solid #6366f1">\')\n  win.document.write(\'<h2 style="font-family:monospace;color:#6366f1;margin:0 0 12px">◈ BANQUE NXC</h2>\')\n  win.document.write(\'<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px">\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Réserves</div><div style="font-weight:bold">\'+ bR +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Entrées</div><div style="font-weight:bold">\'+ bI +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Sorties</div><div style="font-weight:bold">\'+ bO +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Taux</div><div style="font-weight:bold">\'+ bRt +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">NXC circ.</div><div style="font-weight:bold">\'+ bNx +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Valeur NXC</div><div style="font-weight:bold">\'+ bVx +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Billets</div><div style="font-weight:bold">\'+ bBn +\'</div></div>\')\n  win.document.write(\'<div style="background:#f8f8ff;padding:10px;border-radius:6px;border:1px solid #ddd"><div style="font-size:11px;color:#888">Flux total</div><div style="font-weight:bold">\'+ bFl +\'</div></div>\')\n  win.document.write(\'</div>\')\n  if(typeof _flux!==\'undefined\'&&_flux&&_flux.length>0){\n    win.document.write(\'<h3 style="font-family:monospace;margin:0 0 8px">Flux récents</h3>\')\n    win.document.write(\'<table style="width:100%;border-collapse:collapse;font-size:11px"><thead><tr style="background:#6366f1;color:#fff"><th style="padding:4px 8px;text-align:left">Date</th><th>Type</th><th>Utilisateur</th><th>Montant</th><th>Solde</th></tr></thead><tbody>\')\n    _flux.slice(0,50).forEach(function(f){\n      var fd=new Date(f.ts).toLocaleString(\'fr-FR\');\n      win.document.write(\'<tr style="border-bottom:1px solid #eee"><td style="padding:3px 8px">\'+fd+\'</td><td>\'+esc(f.type||\'\')+ \'</td><td>\'+esc(f.user||\'\')+ \'</td><td>\'+esc(String(f.amount||\'\'))+ \'</td><td>\'+esc(String(f.balance||\'\'))+ \'</td></tr>\')\n    });\n    win.document.write(\'</tbody></table>\')\n  }\n  win.document.write(\'</body></html>\');\n  win.document.close();\n  setTimeout(function(){win.print();},500);\n  addLog(\'🖨️\',\'Impression du tableau de bord\');\n}\n\n\n// ══ CYCLES DE MARCHÉ ══\nvar _cy={absmin:null,absmax:null,active:false,int:null,phase:\'normal\',phaseStart:Date.now(),holdUntil:0};\nvar _cyPreviewObj=null;\n\nfunction setCyVal(key){\n  var v=parseFloat($(\'cy-\'+key).value);\n  if(isNaN(v)||v<=0)return;\n  _cy[key]=v;\n  var el=$(\'cy-\'+key+\'-disp\');if(el)el.textContent=fmt(v,0)+\' R\';\n  // Sync avec _cfgFloor/_cfgCeil\n  if(key===\'absmin\'){_cfgFloor=v;updFloorDisplay();}\n  if(key===\'absmax\'){_cfgCeil=v;updFloorDisplay();}\n  addLog(\'📅\',\'Borne \'+key+\': \'+fmt(v,0)+\' R\');\n}\n\nfunction getCyConfig(){\n  return {\n    absmin: _cy.absmin||parseFloat($(\'cy-absmin\').value)||50,\n    absmax: _cy.absmax||parseFloat($(\'cy-absmax\').value)||100000,\n    transition: $(\'cy-transition\').value,\n    holdMin: parseFloat($(\'cy-hold-min\').value)||1,\n    holdMax: parseFloat($(\'cy-hold-max\').value)||3,\n    holdUnit: parseFloat($(\'cy-hold-unit\').value)||60,\n    drift: parseFloat($(\'cy-drift\').value)/100/1440,\n    volBg: parseFloat($(\'cy-vol-bg\').value)/1000,\n    spikeProb: parseFloat($(\'cy-spike\').value)/100,\n    spikeAmp: parseFloat($(\'cy-spike-amp\').value)/100,\n    bounce: parseFloat($(\'cy-bounce\').value)/100,\n    resist: parseFloat($(\'cy-resist\').value)/100,\n    // Fréquences par période → probabilité par tick (tick = 12s)\n    freqMin: {\n      m: parseFloat($(\'cy-min-m\').value)||0,\n      h: parseFloat($(\'cy-min-h\').value)||1,\n      d: parseFloat($(\'cy-min-d\').value)||1,\n      w: parseFloat($(\'cy-min-w\').value)||1,\n      mo: parseFloat($(\'cy-min-mo\').value)||2,\n      y: parseFloat($(\'cy-min-y\').value)||4,\n    },\n    freqMax: {\n      m: parseFloat($(\'cy-max-m\').value)||0,\n      h: parseFloat($(\'cy-max-h\').value)||1,\n      d: parseFloat($(\'cy-max-d\').value)||1,\n      w: parseFloat($(\'cy-max-w\').value)||1,\n      mo: parseFloat($(\'cy-max-mo\').value)||2,\n      y: parseFloat($(\'cy-max-y\').value)||4,\n    },\n  };\n}\n\nfunction calcProbPerTick(freqObj){\n  // Convertir les fréquences en probabilité par tick (12s)\n  var ticksPerMin=5,ticksPerH=300,ticksPerD=7200,ticksPerW=50400,ticksPerMo=216000,ticksPerY=2628000;\n  var pMin=freqObj.m/ticksPerMin+freqObj.h/ticksPerH+freqObj.d/ticksPerD+freqObj.w/ticksPerW+freqObj.mo/ticksPerMo+freqObj.y/ticksPerY;\n  return Math.min(pMin,0.5); // max 50% par tick\n}\n\nfunction startCycles(){\n  var cfg=getCyConfig();\n  if(cfg.absmin>=cfg.absmax){alert(\'Le plancher doit être inférieur au plafond\');return;}\n  _cy.active=true;_cy.phase=\'normal\';_cy.holdUntil=0;\n  $(\'cy-start-btn\').style.display=\'none\';$(\'cy-stop-btn\').style.display=\'block\';\n  var iv=parseInt($(\'ti\').value)||12000;\n  if(tInt){clearInterval(tInt);tInt=null;}\n  tMode=\'cycles\';\n  var el=$(\'tst\');el.textContent=\'📅 Cycles actifs · \'+fmt(cfg.absmin,0)+\'R – \'+fmt(cfg.absmax,0)+\'R\';el.style.color=\'var(--cyan)\';\n  addLog(\'📅\',\'Cycles de marché activés\');\n\n  var pToMin=calcProbPerTick(cfg.freqMin);\n  var pToMax=calcProbPerTick(cfg.freqMax);\n\n  updateCyProb();var _e=$(\'tst\');if(_e){_e.textContent=\'📅 Cycles serveur actifs (bornes + extrêmes)\';_e.style.color=\'var(--cyan)\';}addLog(\'📅\',\'Cycles → serveur\');return;\n  _cy.int=setInterval(async function(){\n    var p=parseFloat(mkt.price||5213);\n    var now=Date.now();\n    var adj=0;\n\n    // Drift de fond\n    adj+=cfg.drift;\n    // Volatilité de fond\n    adj+=(Math.random()-0.5)*cfg.volBg*2;\n\n    // Pics surprises\n    if(Math.random()<cfg.spikeProb){\n      var dir=Math.random()>0.5?1:-1;\n      adj+=dir*cfg.spikeAmp*(Math.random()*0.5+0.5);\n      addLog(\'⚡\',\'Pic surprise: \'+(dir>0?\'+\':\'\')+((adj*100).toFixed(1))+\'%\');\n    }\n\n    // Gestion des phases\n    if(now<_cy.holdUntil){\n      // Maintien en position (min ou max)\n      if(_cy.phase===\'atmin\')adj=Math.max(0,(Math.random()-0.3)*0.001);\n      if(_cy.phase===\'atmax\')adj=Math.min(0,(Math.random()-0.7)*0.001);\n    } else {\n      // Décider si on va vers le min ou le max\n      if(_cy.phase!==\'tomin\'&&_cy.phase!==\'tomax\'){\n        var goMin=Math.random()<pToMin;\n        var goMax=Math.random()<pToMax;\n        if(goMin&&!goMax){_cy.phase=\'tomin\';addLog(\'📅\',\'Cycle → minimum\');}\n        else if(goMax&&!goMin){_cy.phase=\'tomax\';addLog(\'📅\',\'Cycle → maximum\');}\n        else _cy.phase=\'normal\';\n      }\n      if(_cy.phase===\'tomin\'){\n        // Descente vers le min\n        var distRatio=(p-cfg.absmin)/(cfg.absmax-cfg.absmin);\n        var force=cfg.transition===\'brutal\'?-0.1:cfg.transition===\'sinusoide\'?-Math.sin(distRatio*Math.PI)*0.02:-0.01;\n        adj+=force*(1+cfg.bounce);\n        if(p<=cfg.absmin*1.01){_cy.phase=\'atmin\';var holdSec=(cfg.holdMin+Math.random()*(cfg.holdMax-cfg.holdMin))*cfg.holdUnit;_cy.holdUntil=now+holdSec*1000;addLog(\'📅\',\'Cycle: minimum atteint · maintien \'+(holdSec/60).toFixed(0)+\'min\');}\n      }\n      if(_cy.phase===\'tomax\'){\n        // Montée vers le max\n        var distRatio=(cfg.absmax-p)/(cfg.absmax-cfg.absmin);\n        var force=cfg.transition===\'brutal\'?0.1:cfg.transition===\'sinusoide\'?Math.sin(distRatio*Math.PI)*0.02:0.01;\n        adj+=force*(1+cfg.resist);\n        if(p>=cfg.absmax*0.99){_cy.phase=\'atmax\';var holdSec=(cfg.holdMin+Math.random()*(cfg.holdMax-cfg.holdMin))*cfg.holdUnit;_cy.holdUntil=now+holdSec*1000;addLog(\'📅\',\'Cycle: maximum atteint · maintien \'+(holdSec/60).toFixed(0)+\'min\');}\n      }\n    }\n\n    // Résistance aux bornes\n    if(p<cfg.absmin*1.05)adj+=cfg.bounce*0.05;\n    if(p>cfg.absmax*0.95)adj-=cfg.resist*0.05;\n\n    p=Math.max(cfg.absmin,Math.min(cfg.absmax,p*(1+adj)));\n    p=Math.round(p*100)/100;\n\n    // Mise à jour du statut\n    var rem=Math.max(0,Math.round((_cy.holdUntil-now)/1000));\n    var statusTxt=\'Phase: \'+_cy.phase+(_cy.holdUntil>now?\' · maintien encore \'+rem+\'s\':\'\')+\' · P(min)/tick: \'+(pToMin*100).toFixed(2)+\'% · P(max)/tick: \'+(pToMax*100).toFixed(2)+\'%\';\n    var st=$(\'cy-status\');if(st)st.textContent=statusTxt;\n\n    await fetch(\'/nxc/tick\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,price:p,ts:Date.now(),vol:Math.floor(Math.random()*200+20),volume24:(mkt.volume24||0)+80,trades24:(mkt.trades24||0)+1})});\n  },iv);\n}\n\nfunction stopCycles(){\n  _cy.active=false;_cy.phase=\'normal\';\n  if(_cy.int){clearInterval(_cy.int);_cy.int=null;}\n  if(tMode===\'cycles\'){tMode=null;tInt=null;}\n  $(\'cy-start-btn\').style.display=\'block\';$(\'cy-stop-btn\').style.display=\'none\';\n  var el=$(\'cy-status\');if(el)el.textContent=\'Cycles désactivés\';\n  var el2=$(\'tst\');if(el2){el2.textContent=\'⏸ Arrêté\';el2.style.color=\'var(--muted)\';}\n  addLog(\'📅\',\'Cycles désactivés\');\n}\n\nfunction previewCycle(){\n  var cfg=getCyConfig();var cv=$(\'cy-preview\');if(!cv||!window.Chart)return;\n  if(_cyPreviewObj){_cyPreviewObj.destroy();_cyPreviewObj=null;}\n  var pts=[];var p=(cfg.absmin+cfg.absmax)/2;\n  var pMin=calcProbPerTick(cfg.freqMin);var pMax=calcProbPerTick(cfg.freqMax);\n  var phase=\'normal\';var holdUntil=0;\n  for(var t=0;t<100;t++){\n    var adj=(Math.random()-0.5)*cfg.volBg*2+cfg.drift;\n    if(Math.random()<cfg.spikeProb)adj+=(Math.random()>0.5?1:-1)*cfg.spikeAmp*Math.random();\n    if(t>holdUntil){\n      if(phase!==\'tomin\'&&phase!==\'tomax\'){\n        if(Math.random()<pMin)phase=\'tomin\';\n        else if(Math.random()<pMax)phase=\'tomax\';\n        else phase=\'normal\';\n      }\n      if(phase===\'tomin\'){adj-=0.01*(1+cfg.bounce);if(p<=cfg.absmin*1.01){phase=\'atmin\';holdUntil=t+3;}}\n      if(phase===\'tomax\'){adj+=0.01*(1+cfg.resist);if(p>=cfg.absmax*0.99){phase=\'atmax\';holdUntil=t+3;}}\n    }\n    p=Math.max(cfg.absmin,Math.min(cfg.absmax,p*(1+adj)));\n    pts.push(Math.round(p*100)/100);\n  }\n  var labs=pts.map((_,i)=>\'T\'+i);\n  var ctx=cv.getContext(\'2d\');\n  var g=ctx.createLinearGradient(0,0,0,150);g.addColorStop(0,\'rgba(0,229,255,.2)\');g.addColorStop(1,\'rgba(0,229,255,0)\');\n  _cyPreviewObj=new Chart(ctx,{type:\'line\',data:{labels:labs,datasets:[\n    {data:pts,borderColor:\'#00e5ff\',backgroundColor:g,borderWidth:2,pointRadius:0,fill:true,tension:0.3},\n    {data:Array(100).fill(cfg.absmin),borderColor:\'rgba(0,255,157,.4)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[4,4]},\n    {data:Array(100).fill(cfg.absmax),borderColor:\'rgba(255,61,94,.4)\',borderWidth:1,pointRadius:0,fill:false,borderDash:[4,4]},\n  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{display:false},y:{ticks:{color:\'#5c6b8c\',callback:v=>fmt(v,0)},grid:{color:\'rgba(0,229,255,.04)\'}}},animation:{duration:0}}});\n}\n\n\n// ══ INFOS BULLES ══\nvar _infos={\n  bornes:"Les bornes sont les limites absolues du prix NXC. Le prix ne pourra jamais descendre en dessous du minimum ni monter au-dessus du maximum, quoi qu\'il arrive.",\n  freq:"Définit combien de fois le prix touchera exactement son minimum ou maximum dans chaque période. Le moteur calcule automatiquement la probabilité par tick (intervalle de 12s par défaut) pour respecter ces fréquences.",\n  "freq-min":"Par minute : combien de fois dans la prochaine minute le prix touchera son minimum (colonne verte) ou maximum (colonne rouge). 0 = jamais dans la minute.",\n  "freq-h":"Par heure : combien de fois dans la prochaine heure le prix touchera son minimum ou maximum. Ex: 2 = deux fois dans l\'heure.",\n  "freq-d":"Par jour : combien de fois dans les 24 prochaines heures le prix touchera son minimum ou maximum.",\n  "freq-w":"Par semaine : combien de fois dans les 7 prochains jours le prix touchera son minimum ou maximum.",\n  "freq-mo":"Par mois (30 jours) : combien de fois dans le mois le prix touchera son minimum ou maximum.",\n  "freq-y":"Par an (365 jours) : combien de fois dans l\'année le prix touchera son minimum ou maximum. Ex: 4 = une fois par trimestre.",\n  "freq-custom":"Durée personnalisée : définir une période sur mesure. Ex: 6 heures, 2 jours... et combien de fois le prix touchera les extrêmes dans cette durée.",\n  comportement:"Paramètres qui définissent comment le prix se comporte quand il se déplace vers un extrême.",\n  transition:"Comment le prix atteint le min ou le max. Brutal = saut instantané. Progressif = descente/montée sur plusieurs ticks. Sinusoïde = courbe douce et naturelle.",\n  hold:"Combien de temps le prix reste au minimum ou maximum avant de repartir. Une durée aléatoire entre Min et Max est choisie à chaque fois.",\n  drift:"Tendance de fond sur le long terme. +2%/j = le prix a une légère tendance à monter de 2% par jour en moyenne. 0 = aucune tendance.",\n  volbg:"Quantité de mouvement aléatoire à chaque tick, indépendant des cycles. 0% = prix totalement lisse entre les cycles. Plus élevé = plus de micro-variations.",\n  spike:"Probabilité qu\'un pic inattendu se produise à chaque tick. Ex: 5% = 1 chance sur 20 à chaque tick d\'avoir un mouvement brutal.",\n  spikeamp:"Amplitude maximale d\'un pic surprise. ±10% = le pic peut faire bouger le prix de jusqu\'à 10% instantanément.",\n  bounce:"Force du rebond quand le prix touche le plancher. 0% = s\'arrête exactement au plancher. 5% = rebondit légèrement vers le haut.",\n  resist:"Résistance quand le prix approche du plafond. 0% = monte jusqu\'au plafond facilement. 5% = plus difficile de dépasser le plafond.",\n  activation:"Active le moteur de cycles. Une fois activé, le prix suivra automatiquement les fréquences définies pour atteindre les extrêmes.",\n  preview:"Simule 100 ticks avec les paramètres actuels pour voir à quoi ressemblera le comportement du prix avant de l\'activer."\n};\n\nfunction showInfo(key){\n  var modal=$(\'info-modal\');if(!modal)return;\n  $(\'info-title\').textContent=\'ℹ️ \'+key.replace(/-/g,\' \').replace(/\\b\\w/g,c=>c.toUpperCase());\n  $(\'info-body\').textContent=_infos[key]||\'Information non disponible.\';\n  modal.style.display=\'flex\';\n}\n\n// ══ PROBABILITÉS PAR TICK ══\nfunction updateCyProb(){\n  var ticksPerMin=5,ticksPerH=300,ticksPerD=7200,ticksPerW=50400,ticksPerMo=216000,ticksPerY=2628000;\n  var customDur=parseFloat($(\'cy-custom-dur\').value)||0;\n  var customUnit=parseFloat($(\'cy-custom-unit\').value)||3600000;\n  var customMs=customDur*customUnit;\n  var customTicks=customMs/12000;\n\n  var freqMin={m:parseFloat($(\'cy-min-m\').value)||0,h:parseFloat($(\'cy-min-h\').value)||0,d:parseFloat($(\'cy-min-d\').value)||0,w:parseFloat($(\'cy-min-w\').value)||0,mo:parseFloat($(\'cy-min-mo\').value)||0,y:parseFloat($(\'cy-min-y\').value)||0,c:parseFloat($(\'cy-min-c\').value)||0};\n  var freqMax={m:parseFloat($(\'cy-max-m\').value)||0,h:parseFloat($(\'cy-max-h\').value)||0,d:parseFloat($(\'cy-max-d\').value)||0,w:parseFloat($(\'cy-max-w\').value)||0,mo:parseFloat($(\'cy-max-mo\').value)||0,y:parseFloat($(\'cy-max-y\').value)||0,c:parseFloat($(\'cy-max-c\').value)||0};\n\n  var pMin=freqMin.m/ticksPerMin+freqMin.h/ticksPerH+freqMin.d/ticksPerD+freqMin.w/ticksPerW+freqMin.mo/ticksPerMo+freqMin.y/ticksPerY+(customTicks>0?freqMin.c/customTicks:0);\n  var pMax=freqMax.m/ticksPerMin+freqMax.h/ticksPerH+freqMax.d/ticksPerD+freqMax.w/ticksPerW+freqMax.mo/ticksPerMo+freqMax.y/ticksPerY+(customTicks>0?freqMax.c/customTicks:0);\n\n  pMin=Math.min(pMin,0.8);pMax=Math.min(pMax,0.8);\n\n  // Estimation des fréquences résultantes\n  var estPerH_min=Math.round(pMin*ticksPerH*10)/10;\n  var estPerH_max=Math.round(pMax*ticksPerH*10)/10;\n  var estPerD_min=Math.round(pMin*ticksPerD);\n  var estPerD_max=Math.round(pMax*ticksPerD);\n\n  var el=$(\'cy-prob-display\');if(!el)return;\n  el.innerHTML=\n    \'<b style="color:var(--green)">MIN</b> — probabilité/tick: <b>\'+(pMin*100).toFixed(3)+\'%</b> · ~\'+estPerH_min+\'/heure · ~\'+estPerD_min+\'/jour<br>\'\n    +\'<b style="color:var(--red)">MAX</b> — probabilité/tick: <b>\'+(pMax*100).toFixed(3)+\'%</b> · ~\'+estPerH_max+\'/heure · ~\'+estPerD_max+\'/jour<br>\'\n    +(pMin+pMax>0.5?\'<span style="color:var(--red)">⚠️ Fréquences très élevées — le prix sera souvent aux extrêmes</span>\':\'<span style="color:var(--green)">✅ Fréquences réalistes</span>\');\n\n  window._cyPMin=pMin;window._cyPMax=pMax;\n  var _amin=parseFloat($(\'cy-absmin\').value)||0,_amax=parseFloat($(\'cy-absmax\').value)||0;\n  try{fetch(\'/nxc/extremes\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,pmin:pMin/3,pmax:pMax/3})});\n  if(_amin>0&&_amax>_amin)fetch(\'/nxc/bounds\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,auto:false,min:_amin,max:_amax})});\n  else fetch(\'/nxc/bounds\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({master_key:KEY,auto:true})});\n  addLog(\'🎯\',\'Extrêmes + bornes → serveur\');}catch(e){}\n}\n\nasync function loadMeanPrice(){\n  try{\n    var r=await fetch(\'/nxc/meanprice\');\n    var d=await r.json();\n    if(d.ok){mpOn=d.enabled;var t=document.getElementById(\'mp-target\');if(t)t.value=d.target;updMp();}\n  }catch(e){}\n}\nfunction updMp(){\n  var tg=document.getElementById(\'mp-tg\'),lb=document.getElementById(\'mp-lbl\');\n  if(mpOn){if(tg)tg.classList.add(\'on\');if(lb){lb.textContent=\'✅ Activé\';lb.style.color=\'#a855f7\';}}\n  else{if(tg)tg.classList.remove(\'on\');if(lb){lb.textContent=\'⏸ Désactivé\';lb.style.color=\'var(--muted)\';}}\n}\nasync function toggleMp(){mpOn=!mpOn;updMp();await saveMeanPrice();}\nasync function saveMeanPrice(){\n  var tgt=parseFloat(document.getElementById(\'mp-target\').value);\n  if(!tgt||tgt<1){setMsg(\'mp-msg\',\'❌ Prix invalide\',false);return;}\n  var r=await fetch(\'/nxc/meanprice\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},\n    body:JSON.stringify({master_key:KEY,enabled:mpOn,target:tgt})});\n  var res=await r.json();\n  setMsg(\'mp-msg\',res.ok?(mpOn?\'✅ Mean reversion activée → \'+fmt(tgt,0)+\' R\':\'⏸ Désactivée\'):\'❌ Erreur\',res.ok);\n  if(res.ok)addLog(\'🎯\',\'Mean reversion \'+(mpOn?\'activée → \'+fmt(tgt,0)+\' R\':\'désactivée\'));\n}\n\nasync function loadBias(){\n  try{\n    var r=await fetch(\'/nxc/bias\');\n    var d=await r.json();\n    if(d.ok){biasDrift=d.drift;biasSpd=d.speed;updBiasUI();}\n  }catch(e){}\n}\nfunction setBias(v){biasDrift=parseFloat((+v).toFixed(2));updBiasUI();saveBias();}\nfunction setSpd(v){biasSpd=parseFloat((+v).toFixed(3));updBiasUI();saveBias();}\nfunction updBiasUI(){\n  var th=document.getElementById(\'bc-thumb\');\n  var ind=document.getElementById(\'bc-ind\');\n  if(th){\n    var pct=((biasDrift+1)/2)*100;\n    th.style.left=pct+\'%\';\n    if(biasDrift>0.15){th.style.backgroundColor=\'var(--green)\';th.style.boxShadow=\'0 0 14px #00ff9d,0 0 28px #00ff9d\';}\n    else if(biasDrift<-0.15){th.style.backgroundColor=\'var(--red)\';th.style.boxShadow=\'0 0 14px #ff3d5e,0 0 28px #ff3d5e\';}\n    else{th.style.backgroundColor=\'#fff\';th.style.boxShadow=\'0 0 10px rgba(255,255,255,0.5)\';}\n  }\n  if(ind){\n    if(biasDrift>0.5){ind.className=\'bc-ind bull\';ind.textContent=\'\\uD83D\\uDE80 BULL FORT +\'+Math.round(biasDrift*100)+\'%\';}\n    else if(biasDrift>0.15){ind.className=\'bc-ind bull\';ind.textContent=\'\\uD83D\\uDCC8 Légère hausse +\'+Math.round(biasDrift*100)+\'%\';}\n    else if(biasDrift<-0.5){ind.className=\'bc-ind bear\';ind.textContent=\'\\uD83D\\uDC3B BEAR FORT \\u2212\'+Math.round(Math.abs(biasDrift)*100)+\'%\';}\n    else if(biasDrift<-0.15){ind.className=\'bc-ind bear\';ind.textContent=\'\\uD83D\\uDCC9 Légère baisse \\u2212\'+Math.round(Math.abs(biasDrift)*100)+\'%\';}\n    else{ind.className=\'bc-ind neutral\';ind.textContent=\'\\u2696 NEUTRE \\u2014 variation équilibrée\';}\n  }\n  var spdPct=((Math.log2(Math.max(0.125,biasSpd))+3)/6)*100;\n  var sf=document.getElementById(\'spd-fill\'),st=document.getElementById(\'spd-thumb\'),si=document.getElementById(\'spd-ind\');\n  if(sf)sf.style.width=Math.max(0,Math.min(100,spdPct))+\'%\';\n  if(st)st.style.left=Math.max(0,Math.min(100,spdPct))+\'%\';\n  if(si){\n    var lbl=biasSpd<=0.3?\'🐌 Très lent\':biasSpd<=0.6?\'Lent\':biasSpd<=1.2?\'Normal\':biasSpd<=2.5?\'⚡ Rapide\':biasSpd<=5?\'🔥 Très rapide\':\'💥 Extrême\';\n    si.textContent=\'× \'+biasSpd.toFixed(2)+\'  —  \'+lbl;\n    si.style.color=biasSpd>4?\'var(--red)\':biasSpd>2?\'var(--gold)\':biasSpd>1.2?\'var(--cyan)\':\'var(--muted)\';\n    si.style.textShadow=\'0 0 10px \'+si.style.color;\n  }\n}\nfunction startBiasDrag(e){\n  e.preventDefault();\n  var tr=document.getElementById(\'bc-track\');\n  if(!tr)return;\n  function move(ev){\n    var rect=tr.getBoundingClientRect();\n    var cx=ev.touches?ev.touches[0].clientX:ev.clientX;\n    var pct=Math.max(0,Math.min(1,(cx-rect.left)/rect.width));\n    biasDrift=parseFloat((pct*2-1).toFixed(2));\n    updBiasUI();\n  }\n  function up(){\n    document.removeEventListener(\'mousemove\',move);document.removeEventListener(\'mouseup\',up);\n    document.removeEventListener(\'touchmove\',move);document.removeEventListener(\'touchend\',up);\n    saveBias();\n  }\n  document.addEventListener(\'mousemove\',move);document.addEventListener(\'mouseup\',up);\n  document.addEventListener(\'touchmove\',move,{passive:false});document.addEventListener(\'touchend\',up);\n  move(e);\n}\nfunction startSpdDrag(e){\n  e.preventDefault();\n  var tr=document.getElementById(\'spd-track\');\n  if(!tr)return;\n  function move(ev){\n    var rect=tr.getBoundingClientRect();\n    var cx=ev.touches?ev.touches[0].clientX:ev.clientX;\n    var pct=Math.max(0,Math.min(1,(cx-rect.left)/rect.width));\n    biasSpd=parseFloat(Math.pow(2,pct*6-3).toFixed(3));\n    updBiasUI();\n  }\n  function up(){\n    document.removeEventListener(\'mousemove\',move);document.removeEventListener(\'mouseup\',up);\n    document.removeEventListener(\'touchmove\',move);document.removeEventListener(\'touchend\',up);\n    saveBias();\n  }\n  document.addEventListener(\'mousemove\',move);document.addEventListener(\'mouseup\',up);\n  document.addEventListener(\'touchmove\',move,{passive:false});document.addEventListener(\'touchend\',up);\n  move(e);\n}\nasync function saveBias(){\n  try{\n    var r=await fetch(\'/nxc/bias\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},\n      body:JSON.stringify({master_key:KEY,drift:biasDrift,speed:biasSpd})});\n    var res=await r.json();\n    if(res.ok){\n      setMsg(\'bias-msg\',\'✅ Biais sauvegardé\',true);\n      addLog(\'⚡\',\'Biais drift=\'+biasDrift+\'  vitesse×\'+biasSpd);\n    } else setMsg(\'bias-msg\',\'❌ Erreur\',false);\n  }catch(e){setMsg(\'bias-msg\',\'❌ Réseau\',false);}\n}\n\n</script><div class="view" id="view-prevision">\n<div class="card" style="margin-bottom:12px">\n<div class="ct" style="color:#a06bff;font-size:10px;letter-spacing:3px;margin-bottom:14px">🔮 PRÉVISION DU PRIX NXC</div>\n<div style="font-size:11px;color:var(--muted);margin-bottom:14px;line-height:1.6">Estimation basée sur le prix actuel, le biais directionnel et la cible MR. Fourchette = intervalle de confiance 90 %.</div>\n<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap">\n<div style="flex:1;min-width:80px;background:rgba(0,229,255,.06);border:1px solid var(--border);border-radius:10px;padding:10px;text-align:center">\n<div style="font-size:10px;color:var(--muted);margin-bottom:4px">PRIX ACTUEL</div>\n<div id="pv-current" style="font-size:17px;font-weight:800;color:var(--cyan);font-family:monospace">—</div></div>\n<div style="flex:1;min-width:80px;background:rgba(160,107,255,.06);border:1px solid var(--border);border-radius:10px;padding:10px;text-align:center">\n<div style="font-size:10px;color:var(--muted);margin-bottom:4px">TENDANCE</div>\n<div id="pv-trend" style="font-size:15px;font-weight:800">—</div></div>\n<div style="flex:1;min-width:80px;background:rgba(0,255,157,.06);border:1px solid var(--border);border-radius:10px;padding:10px;text-align:center">\n<div style="font-size:10px;color:var(--muted);margin-bottom:4px">CIBLE MR</div>\n<div id="pv-target" style="font-size:17px;font-weight:800;color:var(--green);font-family:monospace">—</div></div>\n</div>\n<table style="width:100%;border-collapse:collapse;font-size:12px">\n<thead><tr style="border-bottom:1px solid var(--border)">\n<th style="text-align:left;padding:8px 6px;color:var(--muted);font-size:10px;font-weight:700;letter-spacing:1px">HORIZON</th>\n<th style="text-align:right;padding:8px 6px;color:var(--muted);font-size:10px;font-weight:700;letter-spacing:1px">ESTIMÉ</th>\n<th style="text-align:right;padding:8px 6px;color:var(--red);font-size:10px;font-weight:700;letter-spacing:1px">MIN P10</th>\n<th style="text-align:right;padding:8px 6px;color:var(--green);font-size:10px;font-weight:700;letter-spacing:1px">MAX P90</th>\n<th style="text-align:center;padding:8px 6px;color:var(--muted);font-size:10px;font-weight:700;letter-spacing:1px">VAR.</th>\n</tr></thead>\n<tbody id="pv-body"><tr><td colspan="5" style="text-align:center;padding:20px;color:var(--muted)">Cliquez Recalculer…</td></tr></tbody>\n</table>\n<button class="btn full" onclick="calcPrev()" style="margin-top:14px">🔄 Recalculer</button>\n<div style="margin-top:10px;font-size:10px;color:var(--muted);text-align:center;line-height:1.5">Prévision indicative — le marché NXC est stochastique.</div>\n</div>\n<div class="card">\n<div class="ct" style="margin-bottom:12px;font-size:10px;letter-spacing:2px">📈 COURBE PRÉVISIONNELLE</div>\n<svg id="pv-svg" viewBox="0 0 320 100" style="width:100%;height:auto;display:block;background:rgba(0,0,0,.15);border-radius:8px"></svg>\n<div style="display:flex;justify-content:space-between;font-size:9px;color:var(--muted);margin-top:6px;padding:0 4px">\n<span>Maintenant</span><span>+6h</span><span>+24h</span><span>+7j</span><span>+30j</span>\n</div>\n</div>\n</div>\n\n<!-- ═══ URGENCE ═══ -->\n<div class="view" id="view-urgence">\n<div class="card red" style="border:2px solid var(--red)">\n  <div class="ct" style="color:var(--red)">🚨 CONTRÔLES D\'URGENCE</div>\n  <div style="background:rgba(255,61,94,.08);border-radius:10px;padding:12px;margin-bottom:12px">\n    <div style="font-weight:800;font-size:13px;color:var(--red);margin-bottom:6px">🧊 GEL DU PRIX</div>\n    <div style="font-size:11px;color:var(--muted);margin-bottom:10px">Fige le prix NXC — les ticks continuent mais le prix est réinitialisé chaque 500 ms.</div>\n    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">\n      <input id="urg-freeze-price" type="number" placeholder="Prix à geler (laisser vide = actuel)" style="flex:1;min-width:160px;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text)">\n      <button class="btn red" onclick="toggleFreeze(true)" id="btn-freeze">🧊 Geler</button>\n      <button class="btn" onclick="toggleFreeze(false)" id="btn-unfreeze" style="display:none">🔥 Dégeler</button>\n    </div>\n    <div id="urg-freeze-status" style="font-size:11px;margin-top:8px;color:var(--muted)">Statut : <span id="urg-freeze-val">non gelé</span></div>\n  </div>\n  <div style="background:rgba(255,61,94,.08);border-radius:10px;padding:12px;margin-bottom:12px">\n    <div style="font-weight:800;font-size:13px;color:var(--gold);margin-bottom:6px">💉 INJECTION DE PRIX</div>\n    <div style="font-size:11px;color:var(--muted);margin-bottom:10px">Force immédiatement le prix NXC à la valeur choisie.</div>\n    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">\n      <input id="urg-price-val" type="number" min="50" max="999999" placeholder="Nouveau prix (R)" style="flex:1;min-width:140px;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text)">\n      <button class="btn gold" onclick="emergencySetPrice()">💉 Injecter</button>\n    </div>\n    <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:8px">\n      <button class="btn" onclick="quickPrice(1000)" style="font-size:11px">1 000</button>\n      <button class="btn" onclick="quickPrice(5000)" style="font-size:11px">5 000</button>\n      <button class="btn" onclick="quickPrice(10000)" style="font-size:11px">10 000</button>\n      <button class="btn" onclick="quickPrice(50000)" style="font-size:11px">50 000</button>\n    </div>\n  </div>\n  <div style="background:rgba(255,61,94,.08);border-radius:10px;padding:12px">\n    <div style="font-weight:800;font-size:13px;color:#a855f7;margin-bottom:6px">🎯 FORCER VERS CIBLE MR</div>\n    <div style="font-size:11px;color:var(--muted);margin-bottom:10px">Injecte immédiatement le prix cible de la Mean Reversion (activée automatiquement).</div>\n    <button class="btn purple full" onclick="forcePriceToTarget()">🎯 Forcer vers cible MR</button>\n  </div>\n  <div id="urg-msg" style="font-size:12px;font-weight:700;min-height:16px;margin-top:10px"></div>\n</div>\n<div class="card">\n  <div class="ct">⚡ VOLATILITÉ</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:10px">Multiplie l\'amplitude des fluctuations. 1.0 = normal, 0 = prix plat, 3 = très volatile.</div>\n  <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">\n    <input id="vol-mult" type="number" min="0" max="10" step="0.1" value="1.0" style="width:100px;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text)">\n    <button class="btn cyan" onclick="saveVolatility()">✓ Appliquer</button>\n    <span id="vol-current" style="font-size:11px;color:var(--muted)">actuel: —</span>\n  </div>\n  <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:8px">\n    <button class="btn" onclick="setVol(0)" style="font-size:10px">🧊 Plat (0)</button>\n    <button class="btn" onclick="setVol(0.5)" style="font-size:10px">🌊 Calme (0.5)</button>\n    <button class="btn" onclick="setVol(1.0)" style="font-size:10px">📈 Normal (1)</button>\n    <button class="btn" onclick="setVol(2.0)" style="font-size:10px">⚡ Volatile (2)</button>\n    <button class="btn red" onclick="setVol(5.0)" style="font-size:10px">💥 Extrême (5)</button>\n  </div>\n  <div id="vol-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:8px"></div>\n</div>\n</div>\n\n<!-- ═══ DASHBOARD ═══ -->\n<div class="view" id="view-dashboard">\n<div class="card" style="border-color:rgba(0,229,255,.3)">\n  <div class="ct" style="color:var(--cyan)">📊 DASHBOARD TEMPS RÉEL</div>\n  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px">\n    <div style="background:var(--bg3);border-radius:10px;padding:12px;text-align:center">\n      <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">Prix actuel</div>\n      <div id="db-price" style="font-size:22px;font-weight:800;color:var(--cyan);margin:4px 0">—</div>\n      <div id="db-chg" style="font-size:11px;font-weight:700">—</div>\n    </div>\n    <div style="background:var(--bg3);border-radius:10px;padding:12px;text-align:center">\n      <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">Volatilité réalisée</div>\n      <div id="db-vol" style="font-size:22px;font-weight:800;color:#a855f7;margin:4px 0">—</div>\n      <div style="font-size:10px;color:var(--muted)">std des log-returns (20 ticks)</div>\n    </div>\n    <div style="background:var(--bg3);border-radius:10px;padding:12px;text-align:center">\n      <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">Haut 24h</div>\n      <div id="db-hi" style="font-size:18px;font-weight:700;color:var(--green);margin:4px 0">—</div>\n    </div>\n    <div style="background:var(--bg3);border-radius:10px;padding:12px;text-align:center">\n      <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">Bas 24h</div>\n      <div id="db-lo" style="font-size:18px;font-weight:700;color:var(--red);margin:4px 0">—</div>\n    </div>\n  </div>\n  <!-- État du marché -->\n  <div id="db-state" style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:12px"></div>\n  <!-- Mini graphique dashboard -->\n  <svg id="db-svg" viewBox="0 0 320 60" style="width:100%;height:auto;display:block;background:rgba(0,0,0,.15);border-radius:8px;margin-bottom:8px"></svg>\n  <div style="display:flex;justify-content:space-between;font-size:9px;color:var(--muted)">\n    <span>-24h</span><span>-12h</span><span>maintenant</span>\n  </div>\n</div>\n<div class="card">\n  <div class="ct">💱 FRAIS EN VIGUEUR</div>\n  <table style="width:100%;font-size:11px;border-collapse:collapse">\n  <thead><tr style="color:var(--muted);font-size:10px;border-bottom:1px solid var(--border)">\n    <th style="text-align:left;padding:5px 4px">Rôle</th>\n    <th style="text-align:center;padding:5px 4px">Achat</th>\n    <th style="text-align:center;padding:5px 4px">Vente</th>\n  </tr></thead>\n  <tbody id="db-fees"></tbody>\n  </table>\n</div>\n</div>\n\n<!-- ═══ ALERTES PRIX ═══ -->\n<div class="view" id="view-alertesp">\n<div class="card">\n  <div class="ct" style="color:var(--gold)">🎯 ALERTES PRIX NXC</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:12px">Vous recevrez une notification quand le prix franchit le seuil choisi.</div>\n  <div style="display:grid;grid-template-columns:1fr 1fr auto;gap:8px;margin-bottom:10px;align-items:end">\n    <div>\n      <div style="font-size:10px;color:var(--muted);margin-bottom:4px">TYPE</div>\n      <select id="alp-type" style="width:100%;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text)">\n        <option value="above">📈 Au-dessus de</option>\n        <option value="below">📉 En-dessous de</option>\n        <option value="change">↕️ Variation de ±</option>\n      </select>\n    </div>\n    <div>\n      <div style="font-size:10px;color:var(--muted);margin-bottom:4px">VALEUR</div>\n      <input id="alp-val" type="number" min="0" placeholder="Prix ou %" style="width:100%;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);box-sizing:border-box">\n    </div>\n    <button class="btn gold" onclick="addPriceAlert()" style="padding:8px 14px">+ Ajouter</button>\n  </div>\n  <div id="alp-list" style="max-height:280px;overflow-y:auto"></div>\n  <div id="alp-msg" style="font-size:11px;font-weight:600;min-height:14px;margin-top:8px"></div>\n</div>\n<div class="card">\n  <div class="ct">🔔 HISTORIQUE ALERTES</div>\n  <div id="alp-hist" style="max-height:200px;overflow-y:auto;font-size:11px;color:var(--muted)">Aucun déclenchement.</div>\n</div>\n</div>\n\n<!-- ═══ SIMULATEUR ═══ -->\n<div class="view" id="view-simulateur">\n<div class="card">\n  <div class="ct" style="color:#a855f7">🔬 SIMULATEUR DE SCÉNARIOS</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:14px">Simule l\'évolution du prix NXC sous différentes configurations, sans affecter le serveur.</div>\n  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px">\n    <div>\n      <div style="font-size:10px;color:var(--muted);margin-bottom:4px">PRIX DE DÉPART (R)</div>\n      <input id="sim-price" type="number" value="5000" style="width:100%;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);box-sizing:border-box">\n    </div>\n    <div>\n      <div style="font-size:10px;color:var(--muted);margin-bottom:4px">CIBLE MR (R)</div>\n      <input id="sim-target" type="number" value="5000" style="width:100%;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);box-sizing:border-box">\n    </div>\n    <div>\n      <div style="font-size:10px;color:var(--muted);margin-bottom:4px">BIAIS DIRECTIONNEL (-1 à +1)</div>\n      <input id="sim-drift" type="number" min="-1" max="1" step="0.01" value="0" style="width:100%;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);box-sizing:border-box">\n    </div>\n    <div>\n      <div style="font-size:10px;color:var(--muted);margin-bottom:4px">DURÉE</div>\n      <select id="sim-dur" style="width:100%;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text)">\n        <option value="1">1 heure</option>\n        <option value="6">6 heures</option>\n        <option value="24" selected>24 heures</option>\n        <option value="168">7 jours</option>\n        <option value="720">30 jours</option>\n      </select>\n    </div>\n  </div>\n  <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px">\n    <button class="btn purple" onclick="runSim()">▶ Lancer la simulation</button>\n    <button class="btn cyan" onclick="simPreset(\'bull\')" style="font-size:11px">🚀 Bull</button>\n    <button class="btn red" onclick="simPreset(\'bear\')" style="font-size:11px">🐻 Bear</button>\n    <button class="btn" onclick="simPreset(\'stable\')" style="font-size:11px">⚖ Stable</button>\n    <button class="btn gold" onclick="simPreset(\'crash\')" style="font-size:11px">💥 Crash</button>\n    <button class="btn" onclick="simFromServer()" style="font-size:11px">🔄 Valeurs serveur</button>\n  </div>\n  <svg id="sim-svg" viewBox="0 0 320 100" style="width:100%;height:auto;display:block;background:rgba(0,0,0,.15);border-radius:8px;margin-bottom:8px"></svg>\n  <div id="sim-results" style="font-size:11px;color:var(--muted)"></div>\n</div>\n</div><!-- ═══ AVANCÉ ═══ -->\n<div class="view" id="view-avance">\n<div class="card">\n  <div class="ct" style="color:var(--cyan)">⚙️ PARAMÈTRES AVANCÉS DU MARCHÉ</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:12px">Récapitulatif des paramètres internes du modèle de prix NXC.</div>\n  <table style="width:100%;font-size:11px;border-collapse:collapse">\n  <tbody id="adv-params">\n    <tr><td style="padding:7px 4px;color:var(--muted)">Biais autotick</td><td style="text-align:right;font-family:monospace;color:var(--cyan)">+0.031% / tick</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Force mean-reversion</td><td style="text-align:right;font-family:monospace;color:var(--cyan)">4.0% / tick</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Force biais directionnel</td><td style="text-align:right;font-family:monospace;color:var(--cyan)">drift × 5.0% / tick</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Intervalle tick</td><td style="text-align:right;font-family:monospace;color:var(--cyan)">15 secondes</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Sigma autotick (moy.)</td><td style="text-align:right;font-family:monospace;color:var(--cyan)">σ ∈ [0.8%, 2.3%]</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Demi-vie mean-reversion</td><td style="text-align:right;font-family:monospace;color:var(--cyan)">~4.3 min (17 ticks)</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Prix min / max</td><td style="text-align:right;font-family:monospace;color:var(--cyan)">50 R / 100 000 R</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Multiplicateur volatilité</td><td style="text-align:right;font-family:monospace;color:var(--cyan)" id="adv-voltmult">—</td></tr>\n    <tr style="border-top:1px solid rgba(255,255,255,.04)"><td style="padding:7px 4px;color:var(--muted)">Prix gelé</td><td style="text-align:right;font-family:monospace" id="adv-frozen">non</td></tr>\n  </tbody>\n  </table>\n</div>\n<div class="card">\n  <div class="ct">📋 FORMULES CLÉS</div>\n  <div style="font-size:10px;color:var(--muted);font-family:monospace;line-height:1.8;background:var(--bg3);padding:12px;border-radius:8px">\n    <div style="color:var(--cyan)">// Autotick (toutes les 15s) :</div>\n    adj = (rand() - 0.48) × σ<br>\n    p_new = p × (1 + adj)<br><br>\n    <div style="color:var(--cyan)">// Mean Reversion (si activée, |drift| ≤ 0.05) :</div>\n    pull = (target - p) / p × 0.04<br>\n    p_new = p × (1 + pull)<br><br>\n    <div style="color:var(--cyan)">// Biais directionnel (si |drift| > 0.05) :</div>\n    force = drift × 0.05<br>\n    p_new = p × (1 + force)<br><br>\n    <div style="color:var(--cyan)">// Équilibre O-U :</div>\n    P* = target × 1.0078  </div>\n</div>\n</div>\n\n\n<!-- ═══ HISTORIQUE ═══ -->\n<div class="view" id="view-historique">\n<div class="card">\n  <div class="ct" style="color:var(--cyan)">📈 HISTORIQUE DES PRIX NXC</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:12px">Derniers 120 ticks (30 min). Actualisé chaque 15 s.</div>\n  <div style="display:flex;gap:8px;margin-bottom:10px;flex-wrap:wrap">\n    <button class="btn-s" onclick="clearHistory()">🗑️ Effacer</button>\n    <span id="hist-count" style="font-size:11px;color:var(--muted);line-height:28px">0 points</span>\n    <span id="hist-min" style="font-size:11px;color:#e74c3c;line-height:28px"></span>\n    <span id="hist-max" style="font-size:11px;color:#2ecc71;line-height:28px"></span>\n  </div>\n  <canvas id="hist-chart" style="width:100%;height:220px;display:block"></canvas>\n</div>\n<div class="card" style="margin-top:8px">\n  <div class="ct" style="font-size:13px">📊 Statistiques session</div>\n  <div class="g4" id="hist-stats" style="margin-top:8px"></div>\n</div>\n</div>\n\n<!-- ═══ CONVERTISSEUR ═══ -->\n<div class="view" id="view-convertisseur">\n<div class="card">\n  <div class="ct" style="color:var(--cyan)">💱 CONVERTISSEUR NXC ↔ R</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:14px">Conversion au prix marché actuel.</div>\n  <div style="display:grid;grid-template-columns:1fr auto 1fr;gap:10px;align-items:center;margin-bottom:16px">\n    <div>\n      <label style="font-size:11px;color:var(--muted)">Montant</label>\n      <input id="conv-in" type="number" min="0" step="any" value="1" oninput="doConvert()"\n        style="width:100%;padding:8px;background:var(--bg3);border:1px solid var(--bg3);color:var(--fg);border-radius:6px;font-size:14px">\n    </div>\n    <div style="text-align:center">\n      <div style="font-size:18px;cursor:pointer" onclick="swapConvert()">⇄</div>\n      <div style="font-size:10px;color:var(--muted)" id="conv-dir">NXC → R</div>\n    </div>\n    <div>\n      <label style="font-size:11px;color:var(--muted)">Résultat</label>\n      <div id="conv-out" style="padding:8px;background:var(--bg3);border-radius:6px;font-size:14px;color:var(--cyan);min-height:36px">—</div>\n    </div>\n  </div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:8px">Prix actuel : <span id="conv-price" style="color:var(--fg)">—</span></div>\n  <button class="btn-s" onclick="initConvertisseur()">🔄 Rafraîchir</button>\n  <div class="ct" style="font-size:12px;margin-top:16px;margin-bottom:8px">Impact des frais par rôle (pour 1 NXC)</div>\n  <div id="conv-fees-table" style="font-size:11px"></div>\n</div>\n</div>\n\n<!-- ═══ ÉVÉNEMENTS ═══ -->\n<div class="view" id="view-evenements">\n<div class="card">\n  <div class="ct" style="color:var(--cyan)">🎲 ÉVÉNEMENTS DE MARCHÉ</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:14px">Chocs de prix manuels ou automatiques.</div>\n  <div class="g4" style="margin-bottom:16px">\n    <div class="st-card"><div class="stv" id="evt-count" style="color:var(--cyan)">0</div><div class="stl">Événements déclenchés</div></div>\n    <div class="st-card"><div class="stv" id="evt-last-mag" style="color:var(--gold)">—</div><div class="stl">Dernière amplitude</div></div>\n  </div>\n  <div style="margin-bottom:14px">\n    <div style="font-size:12px;color:var(--muted);margin-bottom:8px">Choc manuel</div>\n    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">\n      <select id="evt-type" style="padding:6px;background:var(--bg3);color:var(--fg);border:none;border-radius:6px;font-size:12px">\n        <option value="up">📈 Hausse soudaine</option>\n        <option value="down">📉 Crash soudain</option>\n        <option value="spike">⚡ Volatilité</option>\n      </select>\n      <input id="evt-mag" type="number" value="5" min="0.1" max="50" step="0.1"\n        style="width:70px;padding:6px;background:var(--bg3);border:none;color:var(--fg);border-radius:6px;font-size:12px">\n      <span style="font-size:11px;color:var(--muted)">%</span>\n      <button class="btn-s" onclick="fireEvent()">🚀 Déclencher</button>\n    </div>\n  </div>\n  <div style="margin-bottom:14px">\n    <div style="font-size:12px;color:var(--muted);margin-bottom:8px">Événements auto (probabilité / tick 15s)</div>\n    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">\n      <label style="font-size:11px">Prob. hausse <input id="auto-prob-up" type="number" value="0" min="0" max="100" step="0.1"\n        style="width:55px;padding:4px;background:var(--bg3);border:none;color:var(--fg);border-radius:4px;margin-left:4px"> %</label>\n      <label style="font-size:11px">Prob. crash <input id="auto-prob-dn" type="number" value="0" min="0" max="100" step="0.1"\n        style="width:55px;padding:4px;background:var(--bg3);border:none;color:var(--fg);border-radius:4px;margin-left:4px"> %</label>\n      <label style="font-size:11px">Amplitude max <input id="auto-mag" type="number" value="3" min="0.1" max="20" step="0.1"\n        style="width:55px;padding:4px;background:var(--bg3);border:none;color:var(--fg);border-radius:4px;margin-left:4px"> %</label>\n      <button class="btn-s" onclick="saveAutoEvents()">💾 OK</button>\n    </div>\n  </div>\n  <div id="evt-log" style="font-size:11px;color:var(--muted);max-height:140px;overflow-y:auto;background:var(--bg3);padding:8px;border-radius:6px">Aucun événement déclenché.</div>\n</div>\n</div>\n\n<!-- ═══ EXPORT ═══ -->\n<div class="view" id="view-export">\n<div class="card">\n  <div class="ct" style="color:var(--cyan)">📤 EXPORT / IMPORT</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:14px">Sauvegardez et restaurez l\\\'état du marché NXC.</div>\n  <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px">\n    <button class="btn-s" onclick="exportSnapshot()">💾 Snapshot JSON</button>\n    <button class="btn-s" onclick="exportHistCSV()">📊 Historique CSV</button>\n    <button class="btn-s" onclick="exportFeesJSON()">💸 Frais JSON</button>\n  </div>\n  <div style="margin-bottom:14px">\n    <div style="font-size:12px;color:var(--muted);margin-bottom:6px">Import snapshot JSON</div>\n    <div style="display:flex;gap:8px;align-items:center">\n      <input id="import-json" type="text" placeholder=""\n        style="flex:1;padding:6px;background:var(--bg3);border:none;color:var(--fg);border-radius:6px;font-size:11px">\n      <button class="btn-s" onclick="importSnapshot()">📥 Importer</button>\n    </div>\n  </div>\n  <div id="export-status" style="font-size:11px;color:var(--cyan);min-height:20px"></div>\n  <div class="ct" style="font-size:12px;margin-top:16px;margin-bottom:8px">État actuel</div>\n  <pre id="export-preview" style="font-size:10px;color:var(--muted);background:var(--bg3);padding:10px;border-radius:6px;max-height:160px;overflow-y:auto;white-space:pre-wrap">Chargement...</pre>\n</div>\n</div>\n\n\n<!-- ═══ MÉMO ADMIN ═══ -->\n<div class="view" id="view-memo">\n<div class="card">\n  <div class="ct" style="color:var(--cyan)">📌 MÉMO ADMINISTRATEUR</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:12px">Notes de session. Non sauvegardé côté serveur — local à cette fenêtre.</div>\n  <textarea id="memo-text" rows="10" placeholder="Tapez vos notes ici..."\n    style="width:100%;padding:10px;background:var(--bg3);border:1px solid rgba(0,229,255,0.15);border-radius:8px;color:var(--fg);font-size:13px;resize:vertical;box-sizing:border-box;font-family:inherit"></textarea>\n  <div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap">\n    <button class="btn-s" onclick="memoSave()">💾 Sauvegarder</button>\n    <button class="btn-s" onclick="memoClear()">🗑️ Effacer</button>\n    <button class="btn-s" onclick="memoExport()">📤 Exporter .txt</button>\n    <span id="memo-status" style="font-size:11px;color:var(--cyan);line-height:28px"></span>\n  </div>\n</div>\n<div class="card" style="margin-top:8px">\n  <div class="ct" style="font-size:13px">🗂️ JOURNAUX DE DÉCISIONS</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:8px">Enregistrez les actions importantes du marché avec timestamp.</div>\n  <div style="display:flex;gap:8px;margin-bottom:10px;flex-wrap:wrap">\n    <input id="journal-entry" type="text" placeholder="Ex: Gel du prix à 5000 R pour événement X"\n      style="flex:1;padding:7px;background:var(--bg3);border:1px solid rgba(0,229,255,0.1);border-radius:6px;color:var(--fg);font-size:12px">\n    <button class="btn-s" onclick="journalAdd()">➕ Ajouter</button>\n  </div>\n  <div id="journal-log" style="font-size:11px;max-height:200px;overflow-y:auto;background:var(--bg3);padding:10px;border-radius:6px">\n    <div style="color:var(--muted)">Aucune entrée.</div>\n  </div>\n  <button class="btn-s" style="margin-top:8px" onclick="journalExport()">📤 Exporter journal</button>\n</div>\n<div class="card" style="margin-top:8px">\n  <div class="ct" style="font-size:13px">⏱️ MINUTEUR ADMIN</div>\n  <div style="font-size:11px;color:var(--muted);margin-bottom:10px">Minuteur pour les interventions temporaires (gel, événement, etc.).</div>\n  <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:10px">\n    <input id="timer-min" type="number" value="5" min="1" max="999"\n      style="width:70px;padding:7px;background:var(--bg3);border:none;color:var(--fg);border-radius:6px;font-size:14px;text-align:center"> min\n    <button class="btn-s" onclick="timerStart()">▶ Démarrer</button>\n    <button class="btn-s" onclick="timerStop()">⏹ Arrêter</button>\n    <span id="timer-display" style="font-family:monospace;font-size:20px;color:var(--cyan);min-width:80px">--:--</span>\n  </div>\n  <div id="timer-label" style="font-size:11px;color:var(--muted)">Aucun minuteur actif.</div>\n</div>\n</div>\n\n<script>\n/*\n * calcPrev() — Prévision de prix NXC\n *\n * Paramètres réels du serveur (vérifiés dans le code Python) :\n *\n *   _nxc_autotick  (toutes les 15 s) :\n *     sigma_tick = uniform(0.008, 0.023)  →  E[sigma] ≈ 0.0155\n *     adj_auto = (random() - 0.48) * sigma_tick\n *     E[adj_auto] = 0.02 * 0.0155 = +0.00031  (biais haussier)\n *\n *   _mean_reversion_tick  (toutes les 15 s, si enabled ET |drift| ≤ 0.05) :\n *     pull = (target - p) / p * 0.04\n *     → p_{t+1} = p_t * (1 + pull) = 0.96 * p_t + 0.04 * target\n *\n *   Combinés (linéarisé) :\n *     p_{t+1} = p_t * (1 - 0.04 + 0.00031) + 0.04 * target\n *             = p_t * 0.96031 + 0.04 * target\n *     Point fixe : Peq = 0.04*target / (1-0.96031) = target * 1.0078\n *     Décroissance : decay = 0.96031 par tick\n *     Demi-vie : ln(2)/ln(1/0.96031) ≈ 17 ticks = 4.3 minutes\n *\n *   _bias_tick (toutes les 30/speed s, si drift ≠ 0) :\n *     p *= (1 + drift*0.05 + noise)\n *     Quand actif (|drift|>0.05), la MR est SUSPENDUE côté serveur\n *     Applications par tick de 15 s : speed/2\n *     → p_N(bias) = p0 * (1 + drift*0.05)^(N * speed/2)\n *\n * Variance (O-U) : sigma_eff ≈ 0.0166/tick\n *   Var[P_N] = sigma_eff² * p0² * (1 - decay^(2N)) / (1 - decay²)\n *   → plafonnée à la variance stationnaire\n */\n\nasync function calcPrev(){\n  /* ── 1. Fetch toutes les données en temps réel ── */\n  var p0, drift, speed, mrEnabled, mrTarget;\n  try{\n    var [rP, rB, rM] = await Promise.all([\n      fetch(\'/nxc/price\'),\n      fetch(\'/nxc/bias\'),\n      fetch(\'/nxc/meanprice\')\n    ]);\n    var [dP, dB, dM] = await Promise.all([rP.json(), rB.json(), rM.json()]);\n    if(dP && dP.price) mkt = dP;\n    drift     = (dB && typeof dB.drift === \'number\') ? dB.drift : (biasDrift||0);\n    speed     = (dB && typeof dB.speed === \'number\') ? dB.speed : 1.0;\n    mrEnabled = (dM && typeof dM.enabled === \'boolean\') ? dM.enabled : true;\n    mrTarget  = (dM && dM.target > 0) ? dM.target : 5000;\n  }catch(e){\n    drift    = biasDrift || 0;\n    speed    = 1.0;\n    mrEnabled = true;\n    mrTarget  = 5000;\n    var inp = document.getElementById(\'mp-target\');\n    if(inp && parseFloat(inp.value) > 0) mrTarget = parseFloat(inp.value);\n  }\n\n  p0 = parseFloat((mkt && mkt.price) || 5000);\n\n  /* ── 2. Indicateurs ── */\n  var elC = document.getElementById(\'pv-current\');\n  if(elC) elC.textContent = fmt(p0,0) + \' R\';\n  var elT = document.getElementById(\'pv-target\');\n  if(elT) elT.textContent = mrEnabled ? fmt(mrTarget,0) + \' R\' : \'— (désactivée)\';\n  var elTr = document.getElementById(\'pv-trend\');\n  if(elTr){\n    if(drift > 0.3){elTr.textContent=\'🚀 Haussière\';elTr.style.color=\'var(--green)\';}\n    else if(drift < -0.3){elTr.textContent=\'🐻 Baissière\';elTr.style.color=\'var(--red)\';}\n    else{elTr.textContent=\'⚖ Neutre\';elTr.style.color=\'var(--muted)\';}\n  }\n\n  /* ── 3. Paramètres modèle (valeurs EXACTES du serveur) ── */\n  var decay     = 0.96031;    /* par tick 15 s (= 1 - 0.04 + 0.00031) */\n  var sigmaEff  = 0.0166;     /* sigma effectif/tick (autotick + MR noise) */\n  var autoBias  = 0.00031;    /* dérive autotick E[adj] par tick */\n  var mrActive  = mrEnabled && Math.abs(drift) <= 0.05;\n\n  /* Point fixe O-U (avec autoBias) */\n  var Peq = mrActive ? (0.04 * mrTarget / (1.0 - decay)) : p0;\n\n  /* Variance stationnaire O-U */\n  var varStat = sigmaEff * sigmaEff * p0 * p0 / (1.0 - decay * decay);\n\n  var horizons = [\n    {label:\'5 minutes\',  N:20},\n    {label:\'15 minutes\', N:60},\n    {label:\'1 heure\',    N:240},\n    {label:\'6 heures\',   N:1440},\n    {label:\'24 heures\',  N:5760},\n    {label:\'7 jours\',    N:40320},\n    {label:\'30 jours\',   N:172800}\n  ];\n\n  var rows  = [];\n  var svgPts = [{h:0, p:p0, lo:p0, hi:p0}];\n\n  for(var i = 0; i < horizons.length; i++){\n    var hz = horizons[i];\n    var N  = hz.N;\n    var price, lo, hi;\n\n    if(mrActive){\n      /* Ornstein-Uhlenbeck exact :\n         p_N = Peq + (p0 - Peq) * decay^N */\n      var decayN = Math.pow(decay, N);\n      price = Peq + (p0 - Peq) * decayN;\n\n      /* Variance O-U exacte (plafonnée à stationnaire) */\n      var decay2N = Math.pow(decay, 2 * N);\n      var varN  = sigmaEff * sigmaEff * p0 * p0 * (1.0 - decay2N) / (1.0 - decay * decay);\n      var stdBase = Math.sqrt(Math.min(varN, varStat));\n      /* Incertitude de regime : IC grandit au-dela de la convergence O-U */\n      var N_h = N / 240.0;\n      var regF = N_h > 0.25 ? Math.pow(N_h / 0.25, 0.18) : 1.0;\n      var stdN  = stdBase * regF;\n      lo = price - 1.28 * stdN;\n      hi = price + 1.28 * stdN;\n\n    } else {\n      /* Biais actif — MR suspendue côté serveur\n         bias_tick toutes les 30/speed s → speed/2 applications par tick 15 s\n         p_N = p0 * (1 + drift*0.05)^(N * speed/2) * (1 + autoBias)^N */\n      var biasApps = N * speed / 2.0;\n      var biasF    = Math.pow(1.0 + drift * 0.05, biasApps);\n      var autoF    = Math.pow(1.0 + autoBias, N);\n      price = p0 * biasF * autoF;\n\n      /* Variance : marche aléatoire (pas de MR) */\n      var stdN = sigmaEff * p0 * Math.sqrt(N);\n      lo = price - 1.28 * stdN;\n      hi = price + 1.28 * stdN;\n    }\n\n    price = Math.max(50, Math.min(999999, price));\n    lo    = Math.max(50, Math.min(999999, lo));\n    hi    = Math.max(50, Math.min(999999, hi));\n\n    var pct  = (price - p0) / p0 * 100.0;\n    var sign = pct >= 0 ? \'+\' : \'\';\n    var col  = pct >  3 ? \'var(--green)\' : pct < -3 ? \'var(--red)\' : \'var(--muted)\';\n\n    rows.push(\n      \'<tr style="border-bottom:1px solid rgba(255,255,255,.04)">\'\n      +\'<td style="padding:9px 6px;color:var(--cyan);font-weight:700">\'+hz.label+\'</td>\'\n      +\'<td style="padding:9px 6px;text-align:right;font-family:monospace;font-weight:700;color:\'+col+\'">\'+fmt(price,0)+\' R</td>\'\n      +\'<td style="padding:9px 6px;text-align:right;font-family:monospace;color:var(--red)">\'+fmt(lo,0)+\'</td>\'\n      +\'<td style="padding:9px 6px;text-align:right;font-family:monospace;color:var(--green)">\'+fmt(hi,0)+\'</td>\'\n      +\'<td style="padding:9px 6px;text-align:center;font-weight:700;color:\'+col+\'">\'+sign+pct.toFixed(1)+\'%</td>\'\n      +\'</tr>\'\n    );\n    svgPts.push({h: N/240.0, p:price, lo:lo, hi:hi}); /* h en heures */\n  }\n\n  /* ── 4. Tableau ── */\n  var tbody = document.getElementById(\'pv-body\');\n  if(tbody) tbody.innerHTML = rows.join(\'\');\n\n  /* ── 5. Graphe SVG ── */\n  var svg = document.getElementById(\'pv-svg\');\n  if(!svg) return;\n  var allP = svgPts.map(function(x){return x.p;})\n    .concat(svgPts.map(function(x){return x.lo;}))\n    .concat(svgPts.map(function(x){return x.hi;}));\n  var pMin2 = Math.min.apply(null, allP) * 0.985;\n  var pMax2 = Math.max.apply(null, allP) * 1.015;\n  if(pMax2 - pMin2 < 100){ pMin2 -= 50; pMax2 += 50; }\n  var W=320, H=100, P=12;\n  var maxH = svgPts[svgPts.length-1].h || 720;\n  function sx(h){ return P + (h / maxH) * (W - 2*P); }\n  function sy(p){ return P + (1.0 - (p - pMin2) / (pMax2 - pMin2)) * (H - 2*P); }\n\n  var band = \'\';\n  for(var j=0; j<svgPts.length; j++)\n    band += (j===0?\'M\':\'L\') + sx(svgPts[j].h).toFixed(1)+\' \'+sy(svgPts[j].hi).toFixed(1);\n  for(var j=svgPts.length-1; j>=0; j--)\n    band += \'L\'+sx(svgPts[j].h).toFixed(1)+\' \'+sy(svgPts[j].lo).toFixed(1);\n  band += \'Z\';\n  var line = \'\';\n  for(var j=0; j<svgPts.length; j++)\n    line += (j===0?\'M\':\'L\')+sx(svgPts[j].h).toFixed(1)+\' \'+sy(svgPts[j].p).toFixed(1);\n\n  var ty = mrActive ? sy(mrTarget).toFixed(1) : null;\n  var tL = ty ? (\'M\'+P+\' \'+ty+\'L\'+(W-P)+\' \'+ty) : \'\';\n  var lc = drift>0.1?\'#00ff9d\' : drift<-0.1?\'#ff3d5e\' : \'#00e5ff\';\n\n  svg.innerHTML =\n    \'<defs><linearGradient id="pvG" x1="0" x2="1" y1="0" y2="0">\'\n    +\'<stop offset="0%" stop-color="\'+lc+\'" stop-opacity="0.5"/>\'\n    +\'<stop offset="100%" stop-color="\'+lc+\'" stop-opacity="0.1"/>\'\n    +\'</linearGradient></defs>\'\n    +(tL?\'<path d="\'+tL+\'" stroke="rgba(255,176,32,.5)" stroke-width="1" stroke-dasharray="5 3" fill="none"/>\':\'\')\n    +\'<path d="\'+band+\'" fill="url(#pvG)" opacity="0.35"/>\'\n    +\'<path d="\'+line+\'" stroke="\'+lc+\'" stroke-width="2" fill="none" stroke-linejoin="round"/>\'\n    +\'<circle cx="\'+sx(0).toFixed(1)+\'" cy="\'+sy(p0).toFixed(1)+\'" r="4" fill="\'+lc+\'"/>\';\n}\n\n/* Auto-rafraîchissement toutes les 15 s quand l onglet Prévision est visible */\nsetInterval(function(){\n  var v = document.getElementById(\'view-prevision\');\n  if(v && v.style && v.style.display !== \'none\') calcPrev();\n}, 15000);\n</script>\n\n<script>\n/* ═══════════════════════════════════════════════════════\n   FRAIS DE TRANSACTION NXC — gestion complète\n   ════════════════════════════════════════════════════ */\n\nvar _feesData = {};\n\nasync function loadFees(){\n  if(!KEY) return;\n  try{\n    var r = await fetch(\'/nxc/fees\');\n    var d = await r.json();\n    if(!d.ok) return;\n    _feesData = d.fees || {};\n    renderFeesTable();\n  }catch(e){}\n}\n\nfunction renderFeesTable(){\n  var tb = document.getElementById(\'fees-tbody\');\n  if(!tb) return;\n  var roleLabels = {\n    user:      {label:\'👤 Utilisateur\', col:\'var(--text)\'},\n    vip:       {label:\'⭐ VIP\',          col:\'var(--gold)\'},\n    moderator: {label:\'🛡 Modérateur\',   col:\'#a855f7\'},\n    admin:     {label:\'👑 Admin\',        col:\'var(--cyan)\'},\n    default:   {label:\'❓ Défaut\',       col:\'var(--muted)\'}\n  };\n  var rows = \'\';\n  var order = [\'user\',\'vip\',\'moderator\',\'admin\',\'default\'];\n  for(var i=0; i<order.length; i++){\n    var role = order[i];\n    var f = _feesData[role] || {buy:0, sell:0};\n    var rl = roleLabels[role] || {label:role, col:\'var(--text)\'};\n    rows += \'<tr style="border-bottom:1px solid rgba(255,255,255,.04)">\'\n      + \'<td style="padding:8px 4px;font-weight:700;color:\'+rl.col+\'">\'+rl.label+\'</td>\'\n      + \'<td style="text-align:center;padding:4px">\'\n      +   \'<input id="fee-buy-\'+role+\'" type="number" min="0" max="50" step="0.1"\'\n      +   \' value="\'+f.buy.toFixed(1)+\'"\'\n      +   \' style="width:70px;padding:5px 6px;background:var(--bg);border:1px solid rgba(0,229,255,.2);\'\n      +   \'border-radius:6px;color:var(--green);font-size:12px;text-align:center">\'\n      + \'</td>\'\n      + \'<td style="text-align:center;padding:4px">\'\n      +   \'<input id="fee-sell-\'+role+\'" type="number" min="0" max="50" step="0.1"\'\n      +   \' value="\'+f.sell.toFixed(1)+\'"\'\n      +   \' style="width:70px;padding:5px 6px;background:var(--bg);border:1px solid rgba(255,61,94,.2);\'\n      +   \'border-radius:6px;color:var(--red);font-size:12px;text-align:center">\'\n      + \'</td>\'\n      + \'<td style="text-align:center;padding:4px">\'\n      +   \'<button class="btn cyan" onclick="saveFeeRole(\\\'\'+role+\'\\\')"\'\n      +   \' style="font-size:10px;padding:5px 10px">✓</button>\'\n      + \'</td>\'\n      + \'</tr>\';\n  }\n  tb.innerHTML = rows;\n}\n\nasync function saveFeeRole(role){\n  var buyEl  = document.getElementById(\'fee-buy-\'+role);\n  var sellEl = document.getElementById(\'fee-sell-\'+role);\n  if(!buyEl || !sellEl) return;\n  var buy  = parseFloat(buyEl.value);\n  var sell = parseFloat(sellEl.value);\n  if(isNaN(buy)||isNaN(sell)||buy<0||sell<0){\n    setMsg(\'fees-msg\',\'Valeur invalide\', false); return;\n  }\n  try{\n    var r = await fetch(\'/nxc/fees\',{\n      method:\'POST\',\n      headers:{\'Content-Type\':\'application/json\'},\n      body: JSON.stringify({master_key:KEY, role:role, buy:buy, sell:sell})\n    });\n    var d = await r.json();\n    if(d.ok){\n      _feesData = d.fees;\n      renderFeesTable();\n      setMsg(\'fees-msg\',\'✅ \'+role+\' mis à jour (achat \'+buy.toFixed(1)+\'% / vente \'+sell.toFixed(1)+\'%)\', true);\n      addLog(\'💱\',\'Frais \'+role+\': achat=\'+buy.toFixed(1)+\'% vente=\'+sell.toFixed(1)+\'%\');\n    } else {\n      setMsg(\'fees-msg\',\'❌ \'+(d.error||\'Erreur\'), false);\n    }\n  }catch(e){ setMsg(\'fees-msg\',\'❌ Réseau\', false); }\n}\n\nasync function setAllFees(){\n  var buyEl  = document.getElementById(\'fee-all-buy\');\n  var sellEl = document.getElementById(\'fee-all-sell\');\n  if(!buyEl||!sellEl) return;\n  var buy  = parseFloat(buyEl.value);\n  var sell = parseFloat(sellEl.value);\n  if(isNaN(buy)||isNaN(sell)||buy<0||sell<0){\n    setMsg(\'fees-msg\',\'Valeur invalide\', false); return;\n  }\n  try{\n    var r = await fetch(\'/nxc/fees\',{\n      method:\'POST\',\n      headers:{\'Content-Type\':\'application/json\'},\n      body: JSON.stringify({master_key:KEY, set_all:true, buy:buy, sell:sell})\n    });\n    var d = await r.json();\n    if(d.ok){\n      _feesData = d.fees;\n      renderFeesTable();\n      buyEl.value  = \'\';\n      sellEl.value = \'\';\n      setMsg(\'fees-msg\',\'✅ Tous les rôles : achat=\'+buy.toFixed(1)+\'% vente=\'+sell.toFixed(1)+\'%\', true);\n      addLog(\'💱\',\'Frais TOUS rôles: achat=\'+buy.toFixed(1)+\'% vente=\'+sell.toFixed(1)+\'%\');\n    } else {\n      setMsg(\'fees-msg\',\'❌ \'+(d.error||\'Erreur\'), false);\n    }\n  }catch(e){ setMsg(\'fees-msg\',\'❌ Réseau\', false); }\n}\n\n/* setMsg helper (réutilise l\'existant si disponible, sinon inline) */\nfunction setMsg(id, txt, ok){\n  var el = document.getElementById(id);\n  if(!el) return;\n  el.textContent = txt;\n  el.style.color = ok ? \'var(--green)\' : \'var(--red)\';\n  setTimeout(function(){ if(el.textContent===txt) el.textContent=\'\'; }, 4000);\n}\n\n/* Auto-init : dès que KEY est définie (login réussi), charger les frais */\n(function(){\n  var _fInit = setInterval(function(){\n    if(KEY){\n      clearInterval(_fInit);\n      loadFees();\n      /* Aussi recharger quand on va sur config */\n      var _goOrig = window.go;\n      if(typeof _goOrig === \'function\'){\n        window.go = function(tab, btn){\n          _goOrig(tab, btn);\n          if(tab === \'config\') loadFees();\n        };\n      }\n    }\n  }, 500);\n})();\n</script>\n\n<script>\n/* ═══════════════════════════════════════════════════════════════════\n   URGENCE — gel, injection prix, forcer cible\n   ═══════════════════════════════════════════════════════════════════ */\n\nasync function loadFreeze(){\n  if(!KEY) return;\n  try{\n    var r=await fetch(\'/nxc/freeze\'); var d=await r.json();\n    if(d.ok) applyFreezeState(d.frozen, d.price);\n  }catch(e){}\n}\n\nfunction applyFreezeState(frozen, price){\n  var bf=document.getElementById(\'btn-freeze\');\n  var bu=document.getElementById(\'btn-unfreeze\');\n  var sv=document.getElementById(\'urg-freeze-val\');\n  if(bf)bf.style.display=frozen?\'none\':\'block\';\n  if(bu)bu.style.display=frozen?\'block\':\'none\';\n  if(sv)sv.textContent=frozen?(\'🧊 GELÉ à \'+fmt(price,0)+\' R\'):\'non gelé\';\n  if(sv)sv.style.color=frozen?\'var(--red)\':\'var(--muted)\';\n}\n\nasync function toggleFreeze(active){\n  if(!KEY) return;\n  var priceEl=document.getElementById(\'urg-freeze-price\');\n  var freezePrice=priceEl&&priceEl.value?parseFloat(priceEl.value):(mkt&&mkt.price||5000);\n  try{\n    var r=await fetch(\'/nxc/freeze\',{method:\'POST\',\n      headers:{\'Content-Type\':\'application/json\'},\n      body:JSON.stringify({master_key:KEY, active:active, price:freezePrice})});\n    var d=await r.json();\n    if(d.ok){\n      applyFreezeState(d.frozen, d.price);\n      setMsg(\'urg-msg\', d.frozen?(\'🧊 Prix gelé à \'+fmt(d.price,0)+\' R\'):\'🔥 Prix dégelé\', d.ok);\n      addLog(d.frozen?\'🧊\':\'🔥\', d.frozen?(\'Gel à \'+fmt(d.price,0)+\' R\'):\'Dégel du prix\');\n    }\n  }catch(e){ setMsg(\'urg-msg\',\'❌ Erreur réseau\', false); }\n}\n\nasync function emergencySetPrice(){\n  var v=parseFloat(document.getElementById(\'urg-price-val\').value);\n  if(!v||v<50||v>999999){ setMsg(\'urg-msg\',\'Prix invalide (50–999 999 R)\', false); return; }\n  try{\n    var r=await fetch(\'/nxc/price/set\',{method:\'POST\',\n      headers:{\'Content-Type\':\'application/json\'},\n      body:JSON.stringify({master_key:KEY, price:v})});\n    var d=await r.json();\n    if(d.ok){\n      setMsg(\'urg-msg\',\'💉 Prix forcé à \'+fmt(d.price,0)+\' R\', true);\n      addLog(\'💉\',\'Injection prix: \'+fmt(d.price,0)+\' R\');\n      ref();\n    } else { setMsg(\'urg-msg\',\'❌ \'+(d.error||\'Erreur\'), false); }\n  }catch(e){ setMsg(\'urg-msg\',\'❌ Erreur réseau\', false); }\n}\n\nfunction quickPrice(p){ var el=document.getElementById(\'urg-price-val\'); if(el) el.value=p; emergencySetPrice(); }\n\nasync function forcePriceToTarget(){\n  try{\n    var rm=await fetch(\'/nxc/meanprice\'); var dm=await rm.json();\n    var target=dm.target||5000;\n    if(!dm.enabled){\n      var re=await fetch(\'/nxc/meanprice\',{method:\'POST\',\n        headers:{\'Content-Type\':\'application/json\'},\n        body:JSON.stringify({master_key:KEY, enabled:true, target:target})});\n    }\n    var r=await fetch(\'/nxc/price/set\',{method:\'POST\',\n      headers:{\'Content-Type\':\'application/json\'},\n      body:JSON.stringify({master_key:KEY, price:target})});\n    var d=await r.json();\n    if(d.ok){\n      setMsg(\'urg-msg\',\'🎯 Prix forcé à la cible MR: \'+fmt(target,0)+\' R + MR activée\', true);\n      addLog(\'🎯\',\'Force vers cible MR: \'+fmt(target,0)+\' R\');\n      ref(); loadMeanPrice();\n    }\n  }catch(e){ setMsg(\'urg-msg\',\'❌ Erreur\', false); }\n}\n\nasync function saveVolatility(){\n  var v=parseFloat(document.getElementById(\'vol-mult\').value);\n  if(isNaN(v)||v<0){ setMsg(\'vol-msg\',\'Valeur invalide\', false); return; }\n  try{\n    var r=await fetch(\'/nxc/volatility\',{method:\'POST\',\n      headers:{\'Content-Type\':\'application/json\'},\n      body:JSON.stringify({master_key:KEY, value:v})});\n    var d=await r.json();\n    if(d.ok){\n      document.getElementById(\'vol-current\').textContent=\'actuel: \'+d.value.toFixed(1);\n      setMsg(\'vol-msg\',\'✅ Volatilité ×\'+d.value.toFixed(1), true);\n      addLog(\'⚡\',\'Volatilité ×\'+d.value.toFixed(1));\n    } else setMsg(\'vol-msg\',\'❌ \'+(d.error||\'Erreur\'), false);\n  }catch(e){ setMsg(\'vol-msg\',\'❌ Réseau\', false); }\n}\n\nfunction setVol(v){ var el=document.getElementById(\'vol-mult\'); if(el) el.value=v; saveVolatility(); }\n\n/* ═══════════════════════════════════════════════════════════════════\n   DASHBOARD TEMPS RÉEL\n   ═══════════════════════════════════════════════════════════════════ */\n\nasync function loadDashboard(){\n  if(!KEY) return;\n  try{\n    var r=await fetch(\'/nxc/dashboard\'); var d=await r.json();\n    if(!d.ok) return;\n    var ep=document.getElementById(\'db-price\');\n    var ec=document.getElementById(\'db-chg\');\n    var ev=document.getElementById(\'db-vol\');\n    var ehi=document.getElementById(\'db-hi\');\n    var elo=document.getElementById(\'db-lo\');\n    if(ep) ep.textContent=fmt(d.price,0)+\' R\';\n    if(ec){\n      var s=d.change24>=0?\'+\':\'\';\n      ec.textContent=s+d.change24.toFixed(2)+\'% (24h)\';\n      ec.style.color=d.change24>=0?\'var(--green)\':\'var(--red)\';\n    }\n    if(ev) ev.textContent=(d.realizedVol||0).toFixed(3)+\'%\';\n    if(ehi) ehi.textContent=fmt(d.high24,0)+\' R\';\n    if(elo) elo.textContent=fmt(d.low24,0)+\' R\';\n\n    /* Badges état */\n    var st=document.getElementById(\'db-state\');\n    if(st){\n      var badges=[];\n      badges.push(\'<span style="padding:4px 10px;border-radius:20px;font-size:10px;font-weight:700;background:\'+(d.frozen?\'rgba(255,61,94,.2)\':\'rgba(0,229,255,.1)\')\n        +\';color:\'+(d.frozen?\'var(--red)\':\'var(--cyan)\')+\';">\'+(d.frozen?\'🧊 GELÉ\':\'✅ Actif\')+\'</span>\');\n      badges.push(\'<span style="padding:4px 10px;border-radius:20px;font-size:10px;font-weight:700;background:\'+(d.mrEnabled?\'rgba(168,85,247,.2)\':\'rgba(255,255,255,.05)\')\n        +\';color:\'+(d.mrEnabled?\'#a855f7\':\'var(--muted)\')+\';">\'+(d.mrEnabled?\'🎯 MR ON\':\'⏸ MR OFF\')+\'</span>\');\n      badges.push(\'<span style="padding:4px 10px;border-radius:20px;font-size:10px;font-weight:700;background:rgba(255,176,32,.1);color:var(--gold);">\'\n        +(d.drift>0.05?\'🚀 Biais Haussier\':d.drift<-0.05?\'🐻 Biais Baissier\':\'⚖ Neutre\')+\'</span>\');\n      badges.push(\'<span style="padding:4px 10px;border-radius:20px;font-size:10px;background:rgba(255,255,255,.05);color:var(--muted);">Vol ×\'+d.volatilityMult.toFixed(1)+\'</span>\');\n      st.innerHTML=badges.join(\' \');\n    }\n\n    /* Frais dashboard */\n    var df=document.getElementById(\'db-fees\');\n    if(df && d.fees){\n      var roleLabels={user:\'👤 Utilisateur\',vip:\'⭐ VIP\',moderator:\'🛡 Modérateur\',admin:\'👑 Admin\',default:\'❓ Défaut\'};\n      var rows=\'\';\n      Object.keys(d.fees).forEach(function(role){\n        var f=d.fees[role];\n        rows+=\'<tr style="border-top:1px solid rgba(255,255,255,.04)">\'\n          +\'<td style="padding:5px 4px;color:var(--text)">\'+( roleLabels[role]||role)+\'</td>\'\n          +\'<td style="text-align:center;color:var(--green);font-family:monospace">\'+f.buy.toFixed(1)+\'%</td>\'\n          +\'<td style="text-align:center;color:var(--red);font-family:monospace">\'+f.sell.toFixed(1)+\'%</td>\'\n          +\'</tr>\';\n      });\n      df.innerHTML=rows;\n    }\n\n    /* Mini SVG */\n    drawDashGraph(d);\n\n    /* Avancé */\n    var avm=document.getElementById(\'adv-voltmult\');\n    if(avm) avm.textContent=\'×\'+d.volatilityMult.toFixed(1);\n    var afr=document.getElementById(\'adv-frozen\');\n    if(afr){ afr.textContent=d.frozen?(\'🧊 OUI — \'+fmt(d.price,0)+\' R\'):\'non\'; afr.style.color=d.frozen?\'var(--red)\':\'var(--muted)\'; }\n  }catch(e){}\n}\n\nfunction drawDashGraph(d){\n  var svg=document.getElementById(\'db-svg\'); if(!svg) return;\n  var hist=(mkt&&mkt.history)||[];\n  if(hist.length<2){ svg.innerHTML=\'<text x="50%" y="50%" text-anchor="middle" fill="#555" font-size="9">Données insuffisantes</text>\'; return; }\n  var pts=hist.slice(-576);\n  var prices=pts.map(function(x){return parseFloat(x.price||x);});\n  var mn=Math.min.apply(null,prices), mx=Math.max.apply(null,prices), rng=mx-mn||1;\n  var W=320,H=60,P=8;\n  function sx(i){return P+i/(pts.length-1)*(W-2*P);}\n  function sy(p){return P+(1-(p-mn)/rng)*(H-2*P);}\n  var line=\'\'; for(var i=0;i<prices.length;i++) line+=(i===0?\'M\':\'L\')+sx(i).toFixed(1)+\' \'+sy(prices[i]).toFixed(1);\n  var cur=prices[prices.length-1];\n  var col=d.change24>=0?\'#00ff9d\':\'#ff3d5e\';\n  var fill=line+\'L\'+(W-P)+\' \'+(H-P)+\'L\'+P+\' \'+(H-P)+\'Z\';\n  svg.innerHTML=\'<defs><linearGradient id="dbG" x1="0" x2="0" y1="0" y2="1">\'\n    +\'<stop offset="0%" stop-color="\'+col+\'" stop-opacity="0.3"/>\'\n    +\'<stop offset="100%" stop-color="\'+col+\'" stop-opacity="0.02"/>\'\n    +\'</linearGradient></defs>\'\n    +\'<path d="\'+fill+\'" fill="url(#dbG)"/>\'\n    +\'<path d="\'+line+\'" stroke="\'+col+\'" stroke-width="1.5" fill="none"/>\'\n    +\'<circle cx="\'+(W-P)+\'" cy="\'+sy(cur).toFixed(1)+\'" r="3" fill="\'+col+\'"/>\';\n}\n\n/* ═══════════════════════════════════════════════════════════════════\n   ALERTES PRIX\n   ═══════════════════════════════════════════════════════════════════ */\n\nvar _priceAlerts=[], _alertHist=[], _alertCount=0;\n\nfunction addPriceAlert(){\n  var type=document.getElementById(\'alp-type\').value;\n  var val=parseFloat(document.getElementById(\'alp-val\').value);\n  if(isNaN(val)||val<=0){ setMsg(\'alp-msg\',\'Valeur invalide\', false); return; }\n  var id=\'alp-\'+(Date.now());\n  var label= type===\'above\'?(\'Prix > \'+fmt(val,0)+\' R\')\n            :type===\'below\'?(\'Prix < \'+fmt(val,0)+\' R\')\n            :(\'Variation ≥ ±\'+val+\'%\');\n  _priceAlerts.push({id:id,type:type,value:val,label:label,triggered:false,refPrice:mkt&&mkt.price||0});\n  document.getElementById(\'alp-val\').value=\'\';\n  renderAlerts();\n  setMsg(\'alp-msg\',\'✅ Alerte ajoutée: \'+label, true);\n}\n\nfunction removeAlert(i){ _priceAlerts.splice(i,1); renderAlerts(); }\n\nfunction renderAlerts(){\n  var el=document.getElementById(\'alp-list\'); if(!el) return;\n  if(!_priceAlerts.length){ el.innerHTML=\'<div style="color:var(--muted);font-size:11px;padding:10px">Aucune alerte active.</div>\'; return; }\n  el.innerHTML=_priceAlerts.map(function(a,i){\n    var col=a.triggered?\'var(--green)\':\'var(--text)\';\n    var chk=a.triggered?\' ✅\':\'\';\n    return \'<div style="display:flex;align-items:center;gap:8px;padding:8px;background:var(--bg3);border-radius:8px;margin-bottom:6px">\'\n      +\'<span style="flex:1;font-size:11px;color:\'+col+\'">\'+a.label+chk+\'</span>\'\n      +\'<button class="btn red" onclick="removeAlert(\'+i+\')" style="font-size:10px;padding:4px 8px">✕</button>\'\n      +\'</div>\';\n  }).join(\'\');\n}\n\nfunction checkAlerts(){\n  if(!_priceAlerts.length) return;\n  var p=mkt&&parseFloat(mkt.price)||0; if(!p) return;\n  _priceAlerts.forEach(function(a){\n    if(a.triggered) return;\n    var fire=false;\n    if(a.type===\'above\' && p>a.value) fire=true;\n    if(a.type===\'below\' && p<a.value) fire=true;\n    if(a.type===\'change\'){\n      var ref=a.refPrice||p;\n      if(Math.abs((p-ref)/ref)*100>=a.value) fire=true;\n    }\n    if(fire){\n      a.triggered=true;\n      var msg=\'🎯 Alerte: \'+a.label+\' (\'+fmt(p,0)+\' R)\';\n      _alertHist.unshift({ts:Date.now(),msg:msg});\n      if(_alertHist.length>50) _alertHist.pop();\n      addLog(\'🎯\', msg);\n      renderAlerts();\n      renderAlertHist();\n      /* Notification navigateur */\n      if(window.Notification && Notification.permission===\'granted\'){\n        new Notification(\'NXC Alerte Prix\', {body:msg, icon:\'\'});\n      } else if(window.Notification && Notification.permission!==\'denied\'){\n        Notification.requestPermission().then(function(p){\n          if(p===\'granted\') new Notification(\'NXC Alerte Prix\', {body:msg});\n        });\n      }\n    }\n  });\n}\n\nfunction renderAlertHist(){\n  var el=document.getElementById(\'alp-hist\'); if(!el) return;\n  if(!_alertHist.length){ el.innerHTML=\'<div style="color:var(--muted);padding:8px">Aucun déclenchement.</div>\'; return; }\n  el.innerHTML=_alertHist.map(function(a){\n    return \'<div style="padding:6px 0;border-bottom:1px solid rgba(255,255,255,.04)">\'\n      +\'<span style="color:var(--muted);font-size:10px">\'+new Date(a.ts).toLocaleTimeString(\'fr-FR\')+\'</span> \'\n      +\'<span style="color:var(--gold)">\'+a.msg+\'</span></div>\';\n  }).join(\'\');\n}\n\n/* ═══════════════════════════════════════════════════════════════════\n   SIMULATEUR DE SCÉNARIOS\n   ═══════════════════════════════════════════════════════════════════ */\n\nfunction simPreset(name){\n  var presets={\n    bull:  {price:5000, target:5000, drift:0.8},\n    bear:  {price:5000, target:5000, drift:-0.8},\n    stable:{price:5000, target:5000, drift:0.0},\n    crash: {price:20000, target:5000, drift:-0.5}\n  };\n  var p=presets[name]; if(!p) return;\n  document.getElementById(\'sim-price\').value=p.price;\n  document.getElementById(\'sim-target\').value=p.target;\n  document.getElementById(\'sim-drift\').value=p.drift;\n  runSim();\n}\n\nasync function simFromServer(){\n  try{\n    var [rP,rB,rM]=await Promise.all([fetch(\'/nxc/price\'),fetch(\'/nxc/bias\'),fetch(\'/nxc/meanprice\')]);\n    var [dP,dB,dM]=await Promise.all([rP.json(),rB.json(),rM.json()]);\n    if(dP&&dP.price) document.getElementById(\'sim-price\').value=Math.round(dP.price);\n    if(dM&&dM.target) document.getElementById(\'sim-target\').value=Math.round(dM.target);\n    if(dB&&typeof dB.drift===\'number\') document.getElementById(\'sim-drift\').value=dB.drift.toFixed(2);\n    runSim();\n  }catch(e){}\n}\n\nfunction runSim(){\n  var p0   =parseFloat(document.getElementById(\'sim-price\').value)||5000;\n  var tgt  =parseFloat(document.getElementById(\'sim-target\').value)||5000;\n  var drift=parseFloat(document.getElementById(\'sim-drift\').value)||0;\n  var hours=parseFloat(document.getElementById(\'sim-dur\').value)||24;\n\n  /* Même modèle O-U que calcPrev */\n  var decay=0.96031, sigmaEff=0.0166, autoBias=0.00031;\n  var mrActive=Math.abs(drift)<=0.05;\n  var Peq=mrActive?(0.04*tgt/(1.0-decay)):p0;\n  var nSteps=50;\n  var dt=hours/nSteps;  /* heures par step */\n  var N_per_step=Math.round(dt*240);\n\n  var pts=[{x:0,y:p0,lo:p0,hi:p0}];\n  var price=p0;\n  for(var i=1;i<=nSteps;i++){\n    if(mrActive){\n      var decayN=Math.pow(decay,N_per_step);\n      price=Peq+(price-Peq)*decayN;\n    } else {\n      var biasApps=N_per_step*1.0/2.0;\n      price=price*Math.pow(1+drift*0.05,biasApps)*Math.pow(1+autoBias,N_per_step);\n    }\n    price=Math.max(50,Math.min(999999,price));\n    var stdN=sigmaEff*p0*Math.sqrt(i*N_per_step);\n    var varStat=sigmaEff*sigmaEff*p0*p0/(1-decay*decay);\n    var std=Math.min(stdN,Math.sqrt(varStat));\n    pts.push({x:i/nSteps*hours, y:price, lo:Math.max(50,price-1.28*std), hi:Math.min(999999,price+1.28*std)});\n  }\n\n  /* SVG */\n  var svg=document.getElementById(\'sim-svg\'); if(!svg) return;\n  var allY=pts.map(function(p){return p.y;}).concat(pts.map(function(p){return p.lo;})).concat(pts.map(function(p){return p.hi;}));\n  var mn2=Math.min.apply(null,allY)*0.98, mx2=Math.max.apply(null,allY)*1.02;\n  if(mx2-mn2<100){mn2-=50;mx2+=50;}\n  var W=320,H=100,P=10;\n  function sx2(x){return P+x/hours*(W-2*P);}\n  function sy2(y){return P+(1-(y-mn2)/(mx2-mn2))*(H-2*P);}\n  var band=\'\';\n  for(var i=0;i<pts.length;i++) band+=(i===0?\'M\':\'L\')+sx2(pts[i].x).toFixed(1)+\' \'+sy2(pts[i].hi).toFixed(1);\n  for(var i=pts.length-1;i>=0;i--) band+=\'L\'+sx2(pts[i].x).toFixed(1)+\' \'+sy2(pts[i].lo).toFixed(1);\n  band+=\'Z\';\n  var line2=\'\';\n  for(var i=0;i<pts.length;i++) line2+=(i===0?\'M\':\'L\')+sx2(pts[i].x).toFixed(1)+\' \'+sy2(pts[i].y).toFixed(1);\n  var lc2=drift>0.1?\'#00ff9d\':drift<-0.1?\'#ff3d5e\':\'#00e5ff\';\n  /* Ligne cible MR */\n  var tl=\'\'; if(mrActive && tgt>mn2 && tgt<mx2) tl=\'<path d="M\'+P+\' \'+sy2(tgt).toFixed(1)+\'L\'+(W-P)+\' \'+sy2(tgt).toFixed(1)+\'" stroke="rgba(255,176,32,.6)" stroke-width="1" stroke-dasharray="4 3" fill="none"/>\';\n  svg.innerHTML=\'<defs><linearGradient id="simG" x1="0" x2="0" y1="0" y2="1">\'\n    +\'<stop offset="0%" stop-color="\'+lc2+\'" stop-opacity="0.35"/>\'\n    +\'<stop offset="100%" stop-color="\'+lc2+\'" stop-opacity="0.05"/>\'\n    +\'</linearGradient></defs>\'\n    +tl\n    +\'<path d="\'+band+\'" fill="url(#simG)"/>\'\n    +\'<path d="\'+line2+\'" stroke="\'+lc2+\'" stroke-width="2" fill="none" stroke-linejoin="round"/>\'\n    +\'<circle cx="\'+P+\'" cy="\'+sy2(p0).toFixed(1)+\'" r="4" fill="\'+lc2+\'"/>\'\n    +\'<circle cx="\'+(W-P)+\'" cy="\'+sy2(pts[pts.length-1].y).toFixed(1)+\'" r="3" fill="\'+lc2+\'" opacity="0.7"/>\';\n\n  /* Résultats */\n  var final=pts[pts.length-1];\n  var pct=(final.y-p0)/p0*100;\n  document.getElementById(\'sim-results\').innerHTML=\n    \'<div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:8px">\'\n    +\'<span>🏁 Final: <b style="color:\'+lc2+\'">\'+fmt(final.y,0)+\' R</b></span>\'\n    +\'<span>📈 VAR: <b style="color:\'+(pct>=0?\'var(--green)\':\'var(--red)\')+\'">\'+( pct>=0?\'+\':\'\')+pct.toFixed(1)+\'%</b></span>\'\n    +\'<span>📉 Min P10: <b style="color:var(--red)">\'+fmt(final.lo,0)+\' R</b></span>\'\n    +\'<span>📈 Max P90: <b style="color:var(--green)">\'+fmt(final.hi,0)+\' R</b></span>\'\n    +\'</div>\';\n}\n\n/* ═══════════════════════════════════════════════════════════════════\n   HOOKS go() — charger les nouvelles vues\n   ═══════════════════════════════════════════════════════════════════ */\n\n(function(){\n  var _t=setInterval(function(){\n    if(!KEY) return;\n    clearInterval(_t);\n    /* Étendre go() pour les nouveaux onglets */\n    var _orig=window.go;\n    if(typeof _orig===\'function\'){\n      window.go=function(tab,btn){\n        _orig(tab,btn);\n        if(tab===\'dashboard\'){ loadDashboard(); }\n        if(tab===\'urgence\'){ loadFreeze(); }\n        if(tab===\'alertesp\'){ renderAlerts(); renderAlertHist(); }\n        if(tab===\'simulateur\'){ simFromServer(); }\n        if(tab===\'avance\'){ loadDashboard(); }\n        if(tab===\'historique\'){ setTimeout(drawHistChart,100); }\n        if(tab===\'convertisseur\'){ setTimeout(initConvertisseur,100); }\n        if(tab===\'export\'){ setTimeout(loadExportPreview,100); }\n        if(tab===\'memo\'){ setTimeout(function(){ memoLoad(); renderJournal(); },100); }\n      };\n    }\n    /* checkAlerts toutes les 15 s */\n    setInterval(checkAlerts, 15000);\n    /* Dashboard auto-refresh toutes les 15s si visible */\n    setInterval(function(){\n      var v=document.getElementById(\'view-dashboard\');\n      if(v && v.classList.contains(\'on\')) loadDashboard();\n    }, 15000);\n    /* Freeze status auto-refresh toutes les 5s si visible */\n    setInterval(function(){\n      var v=document.getElementById(\'view-urgence\');\n      if(v && v.classList.contains(\'on\')) loadFreeze();\n    }, 5000);\n  }, 600);\n})();\n</script>\n\n<script>\n/* ═══════════════════════════════════════════════════════════════\n   HISTORIQUE DES PRIX\n   ═══════════════════════════════════════════════════════════════ */\nvar _histPrices = [];\nvar _histLabels = [];\n\nfunction drawHistChart(){\n  var canvas = document.getElementById(\'hist-chart\');\n  if(!canvas) return;\n  var W = canvas.offsetWidth || 600, H = 220;\n  canvas.width = W; canvas.height = H;\n  var ctx = canvas.getContext(\'2d\');\n  var n = _histPrices.length;\n  if(n < 2){\n    ctx.fillStyle=\'rgba(100,120,160,0.5)\';\n    ctx.font=\'12px sans-serif\';\n    ctx.fillText(\'En attente de données...\', 20, H/2);\n    return;\n  }\n  ctx.clearRect(0,0,W,H);\n  var mn = Math.min.apply(null,_histPrices), mx = Math.max.apply(null,_histPrices);\n  var pad = {t:10,r:10,b:24,l:58};\n  var cw = W-pad.l-pad.r, ch = H-pad.t-pad.b;\n  var rng = mx-mn || 1;\n  function px(i){ return pad.l + i/(n-1)*cw; }\n  function py(v){ return pad.t + (1-(v-mn)/rng)*ch; }\n  // Grid lines\n  ctx.strokeStyle=\'rgba(255,255,255,0.06)\'; ctx.lineWidth=1;\n  for(var r=0;r<=4;r++){\n    var gy=pad.t+r/4*ch;\n    ctx.beginPath(); ctx.moveTo(pad.l,gy); ctx.lineTo(pad.l+cw,gy); ctx.stroke();\n    ctx.fillStyle=\'rgba(180,180,180,0.5)\'; ctx.font=\'10px sans-serif\'; ctx.textAlign=\'right\';\n    ctx.fillText((mx-r/4*rng).toFixed(2), pad.l-4, gy+4);\n  }\n  // Gradient fill\n  ctx.beginPath(); ctx.moveTo(px(0), py(_histPrices[0]));\n  for(var i=1;i<n;i++) ctx.lineTo(px(i), py(_histPrices[i]));\n  ctx.lineTo(px(n-1), H-pad.b); ctx.lineTo(px(0), H-pad.b); ctx.closePath();\n  var grad = ctx.createLinearGradient(0,pad.t,0,H-pad.b);\n  grad.addColorStop(0,\'rgba(0,200,255,0.25)\'); grad.addColorStop(1,\'rgba(0,200,255,0.02)\');\n  ctx.fillStyle=grad; ctx.fill();\n  // Line\n  ctx.beginPath(); ctx.moveTo(px(0), py(_histPrices[0]));\n  for(var i=1;i<n;i++) ctx.lineTo(px(i), py(_histPrices[i]));\n  ctx.strokeStyle=\'#00c8ff\'; ctx.lineWidth=2; ctx.stroke();\n  // X labels\n  ctx.fillStyle=\'rgba(180,180,180,0.5)\'; ctx.font=\'9px sans-serif\'; ctx.textAlign=\'center\';\n  var step = Math.max(1, Math.floor(n/6));\n  for(var i=0;i<n;i+=step) if(_histLabels[i]) ctx.fillText(_histLabels[i], px(i), H-pad.b+14);\n  // Stats\n  var el;\n  el=document.getElementById(\'hist-count\'); if(el) el.textContent=n+\' point\'+(n>1?\'s\':\'\');\n  el=document.getElementById(\'hist-min\'); if(el) el.textContent=\'Min: \'+mn.toFixed(4);\n  el=document.getElementById(\'hist-max\'); if(el) el.textContent=\'Max: \'+mx.toFixed(4);\n  var delta=_histPrices[n-1]-_histPrices[0];\n  var pct=delta/_histPrices[0]*100;\n  var avg=_histPrices.reduce(function(a,b){return a+b},0)/n;\n  var vol=0; for(var i=1;i<n;i++) vol+=Math.pow(_histPrices[i]-_histPrices[i-1],2);\n  vol=n>1?Math.sqrt(vol/(n-1)):0;\n  el=document.getElementById(\'hist-stats\');\n  if(el) el.innerHTML=\n    \'<div class="st-card"><div class="stv" style="color:\'+(delta>=0?\'#2ecc71\':\'#e74c3c\')+\'">\'+\n      (delta>=0?\'+\':\'\')+pct.toFixed(2)+\'%</div><div class="stl">Variation</div></div>\'+\n    \'<div class="st-card"><div class="stv">\'+avg.toFixed(4)+\'</div><div class="stl">Prix moyen</div></div>\'+\n    \'<div class="st-card"><div class="stv">\'+vol.toFixed(4)+\'</div><div class="stl">Volatilité tick</div></div>\'+\n    \'<div class="st-card"><div class="stv">\'+(mx-mn).toFixed(4)+\'</div><div class="stl">Amplitude</div></div>\';\n}\n\nfunction clearHistory(){ _histPrices=[]; _histLabels=[]; drawHistChart(); }\n\nfunction _recordHistPrice(p){\n  var now=new Date();\n  var ts=now.getHours()+\':\'+(now.getMinutes()<10?\'0\':\'\')+now.getMinutes()+\':\'+(now.getSeconds()<10?\'0\':\'\')+now.getSeconds();\n  _histPrices.push(p); _histLabels.push(ts);\n  if(_histPrices.length>120){ _histPrices.shift(); _histLabels.shift(); }\n  var v=document.getElementById(\'view-historique\');\n  if(v && v.classList.contains(\'on\')) drawHistChart();\n}\n\n/* ═══════════════════════════════════════════════════════════════\n   CONVERTISSEUR NXC <-> R\n   ═══════════════════════════════════════════════════════════════ */\nvar _convDir=\'nxc2r\';\nvar _convPrice=null;\n\nasync function initConvertisseur(){\n  try {\n    var r=await fetch(\'/nxc/price\'); var dp=await r.json();\n    _convPrice=dp.price;\n    var el=document.getElementById(\'conv-price\');\n    if(el) el.textContent=dp.price.toFixed(4)+\' R/NXC\';\n    doConvert();\n    var rf=await fetch(\'/nxc/fees\'); var df=await rf.json();\n    var fees=df.fees||{};\n    var amt=parseFloat(document.getElementById(\'conv-in\').value)||1;\n    var rows=\'<table style="width:100%;border-collapse:collapse;font-size:11px"><tr style="color:var(--muted)"><th style="text-align:left;padding:4px">Rôle</th><th style="text-align:right;padding:4px">Achat</th><th style="text-align:right;padding:4px">Net achat</th><th style="text-align:right;padding:4px">Vente</th><th style="text-align:right;padding:4px">Net vente</th></tr>\';\n    Object.keys(fees).forEach(function(role){\n      var fb=fees[role].buy/100, fs=fees[role].sell/100;\n      rows+=\'<tr><td style="padding:4px;text-transform:capitalize">\'+role+\'</td>\'+\n        \'<td style="text-align:right;padding:4px">\'+fees[role].buy+\'%</td>\'+\n        \'<td style="text-align:right;padding:4px;color:#2ecc71">\'+(amt*(1-fb)*dp.price).toFixed(2)+\' R</td>\'+\n        \'<td style="text-align:right;padding:4px">\'+fees[role].sell+\'%</td>\'+\n        \'<td style="text-align:right;padding:4px;color:#2ecc71">\'+(amt*dp.price*(1-fs)).toFixed(2)+\' R</td></tr>\';\n    });\n    rows+=\'</table>\';\n    var ftEl=document.getElementById(\'conv-fees-table\');\n    if(ftEl) ftEl.innerHTML=rows;\n  } catch(e){ console.error(\'conv\',e); }\n}\n\nfunction swapConvert(){\n  _convDir=_convDir===\'nxc2r\'?\'r2nxc\':\'nxc2r\';\n  var el=document.getElementById(\'conv-dir\');\n  if(el) el.textContent=_convDir===\'nxc2r\'?\'NXC → R\':\'R → NXC\';\n  doConvert();\n}\n\nfunction doConvert(){\n  var amt=parseFloat(document.getElementById(\'conv-in\').value)||0;\n  var p=_convPrice; if(!p) return;\n  var res=_convDir===\'nxc2r\'?amt*p:amt/p;\n  var unit=_convDir===\'nxc2r\'?\'R\':\'NXC\';\n  var el=document.getElementById(\'conv-out\');\n  if(el) el.textContent=res.toFixed(4)+\' \'+unit;\n}\n\n/* ═══════════════════════════════════════════════════════════════\n   ÉVÉNEMENTS DE MARCHÉ\n   ═══════════════════════════════════════════════════════════════ */\nvar _evtCount=0;\nvar _autoEvtCfg={probUp:0,probDn:0,mag:3};\n\nasync function fireEvent(){\n  var typ=document.getElementById(\'evt-type\').value;\n  var mag=parseFloat(document.getElementById(\'evt-mag\').value)||5;\n  var sign=typ===\'down\'?-1:(typ===\'spike\'?(Math.random()<0.5?1:-1):1);\n  var delta=sign*mag/100;\n  try {\n    var rp=await fetch(\'/nxc/price\'); var dp=await rp.json();\n    var newP=Math.max(50,Math.min(999999,dp.price*(1+delta)));\n    var res=await fetch(\'/nxc/price/set\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({price:newP,key:KEY})});\n    _evtCount++;\n    var el;\n    el=document.getElementById(\'evt-count\'); if(el) el.textContent=_evtCount;\n    el=document.getElementById(\'evt-last-mag\'); if(el) el.textContent=(sign>0?\'+\':\'\')+mag.toFixed(1)+\'%\';\n    var log=document.getElementById(\'evt-log\');\n    if(log){\n      var icon=typ===\'down\'?\'📉\':typ===\'spike\'?\'⚡\':\'📈\';\n      var ts=new Date().toLocaleTimeString();\n      log.innerHTML=\'<div style="margin-bottom:4px;color:var(--fg)">\'+icon+\' [\'+ts+\'] \'+(sign>0?\'+\':\'\')+mag.toFixed(1)+\'% → \'+newP.toFixed(2)+\'</div>\'+log.innerHTML;\n    }\n  } catch(e){ console.error(\'fireEvent\',e); }\n}\n\nfunction saveAutoEvents(){\n  _autoEvtCfg.probUp=parseFloat(document.getElementById(\'auto-prob-up\').value)||0;\n  _autoEvtCfg.probDn=parseFloat(document.getElementById(\'auto-prob-dn\').value)||0;\n  _autoEvtCfg.mag=parseFloat(document.getElementById(\'auto-mag\').value)||3;\n  var log=document.getElementById(\'evt-log\');\n  if(log) log.innerHTML=\'<div style="color:#2ecc71">✓ Config auto sauvegardée (h:\'+_autoEvtCfg.probUp+\'% b:\'+_autoEvtCfg.probDn+\'%)</div>\'+log.innerHTML;\n}\n\nsetInterval(async function(){\n  if(_autoEvtCfg.probUp<=0 && _autoEvtCfg.probDn<=0) return;\n  var r=Math.random()*100, sign=0;\n  if(r<_autoEvtCfg.probUp) sign=1;\n  else if(r<_autoEvtCfg.probUp+_autoEvtCfg.probDn) sign=-1;\n  if(!sign) return;\n  var mag=Math.random()*_autoEvtCfg.mag;\n  try {\n    var rp=await fetch(\'/nxc/price\'); var dp=await rp.json();\n    var newP=Math.max(50,Math.min(999999,dp.price*(1+sign*mag/100)));\n    await fetch(\'/nxc/price/set\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({price:newP,key:KEY})});\n    _evtCount++;\n    var el=document.getElementById(\'evt-count\'); if(el) el.textContent=_evtCount;\n    var log=document.getElementById(\'evt-log\');\n    if(log){\n      var ts=new Date().toLocaleTimeString();\n      log.innerHTML=\'<div style="margin-bottom:4px;color:#f39c12">🤖 [\'+ts+\'] AUTO \'+(sign>0?\'+\':\'\')+mag.toFixed(2)+\'% → \'+newP.toFixed(2)+\'</div>\'+log.innerHTML;\n    }\n  } catch(e){}\n}, 15000);\n\n/* ═══════════════════════════════════════════════════════════════\n   EXPORT / IMPORT\n   ═══════════════════════════════════════════════════════════════ */\nasync function loadExportPreview(){\n  try {\n    var rp=await fetch(\'/nxc/price\'), rb=await fetch(\'/nxc/bias\'), rf=await fetch(\'/nxc/fees\'), rv=await fetch(\'/nxc/volatility\');\n    var dp=await rp.json(), db=await rb.json(), df=await rf.json(), dv=await rv.json();\n    var snap={timestamp:new Date().toISOString(),price:dp.price,target:dp.target||null,drift:db.drift,mrEnabled:db.mrEnabled,fees:df.fees,volatilityMult:dv.value||1,histPoints:_histPrices.length};\n    var el=document.getElementById(\'export-preview\');\n    if(el) el.textContent=JSON.stringify(snap,null,2);\n  } catch(e){ var el=document.getElementById(\'export-preview\'); if(el) el.textContent=\'Erreur: \'+e; }\n}\n\nfunction exportSnapshot(){\n  fetch(\'/nxc/dashboard\').then(function(r){return r.json();}).then(function(d){\n    var blob=new Blob([JSON.stringify(d,null,2)],{type:\'application/json\'});\n    var a=document.createElement(\'a\'); a.href=URL.createObjectURL(blob);\n    a.download=\'nxc-snapshot-\'+new Date().toISOString().slice(0,19).replace(/:/g,\'-\')+\'.json\';\n    a.click(); URL.revokeObjectURL(a.href);\n    var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'✓ Snapshot exporté\';\n  }).catch(function(e){ var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'Erreur: \'+e; });\n}\n\nfunction exportHistCSV(){\n  if(!_histPrices.length){ var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'Aucun historique\'; return; }\n  var lines=[\'timestamp,price\'];\n  for(var i=0;i<_histPrices.length;i++) lines.push(_histLabels[i]+\',\'+_histPrices[i]);\n  var blob=new Blob([lines.join(\'\\n\')],{type:\'text/csv\'});\n  var a=document.createElement(\'a\'); a.href=URL.createObjectURL(blob);\n  a.download=\'nxc-historique-\'+new Date().toISOString().slice(0,10)+\'.csv\';\n  a.click(); URL.revokeObjectURL(a.href);\n  var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'✓ CSV exporté (\'+_histPrices.length+\' points)\';\n}\n\nfunction exportFeesJSON(){\n  fetch(\'/nxc/fees\').then(function(r){return r.json();}).then(function(d){\n    var blob=new Blob([JSON.stringify(d.fees,null,2)],{type:\'application/json\'});\n    var a=document.createElement(\'a\'); a.href=URL.createObjectURL(blob);\n    a.download=\'nxc-frais-\'+new Date().toISOString().slice(0,10)+\'.json\';\n    a.click(); URL.revokeObjectURL(a.href);\n    var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'✓ Frais JSON exporté\';\n  });\n}\n\nasync function importSnapshot(){\n  var inp=document.getElementById(\'import-json\');\n  if(!inp||!inp.value.trim()){ var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'Aucun JSON\'; return; }\n  try {\n    var snap=JSON.parse(inp.value.trim());\n    var ops=[];\n    if(snap.price) ops.push(fetch(\'/nxc/price/set\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({price:snap.price,key:KEY})}));\n    if(snap.fees) ops.push(fetch(\'/nxc/fees\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({key:KEY,fees:snap.fees})}));\n    await Promise.all(ops);\n    var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'✓ Importé\';\n    setTimeout(loadExportPreview,500);\n  } catch(e){ var el=document.getElementById(\'export-status\'); if(el) el.textContent=\'Erreur JSON: \'+e; }\n}\n\n/* ═══ Extension go() pour les nouveaux onglets ═══ */\n(func\n\n/* ═══ Enregistrement prix dans historique ═══ */\n(function(){\n  setInterval(async function(){\n    try { var r=await fetch(\'/nxc/price\'); var d=await r.json(); if(d.price) _recordHistPrice(d.price); } catch(e){}\n  }, 15000);\n})();\n</script>\n\n<script>\n/* ═══════════════════════════════════════════════════════════════\n   MEMO ADMIN + JOURNAL + MINUTEUR\n   ═══════════════════════════════════════════════════════════════ */\nvar _journalEntries = [];\nvar _timerInterval = null;\nvar _timerEnd = null;\n\nfunction memoSave(){\n  var txt = document.getElementById(\'memo-text\');\n  if(!txt) return;\n  try { localStorage.setItem(\'nxc_memo\', txt.value); } catch(e){}\n  var st = document.getElementById(\'memo-status\');\n  if(st) st.textContent = \'Sauvegarde a \' + new Date().toLocaleTimeString();\n}\n\nfunction memoClear(){\n  var txt = document.getElementById(\'memo-text\');\n  if(txt) txt.value = \'\';\n  try { localStorage.removeItem(\'nxc_memo\'); } catch(e){}\n  var st = document.getElementById(\'memo-status\');\n  if(st) st.textContent = \'Efface\';\n}\n\nfunction memoExport(){\n  var txt = document.getElementById(\'memo-text\');\n  var c = txt ? txt.value : \'\';\n  var blob = new Blob([c], {type:\'text/plain\'});\n  var a = document.createElement(\'a\');\n  a.href = URL.createObjectURL(blob);\n  a.download = \'nxc-memo-\' + new Date().toISOString().slice(0,10) + \'.txt\';\n  a.click(); URL.revokeObjectURL(a.href);\n}\n\nfunction memoLoad(){\n  var txt = document.getElementById(\'memo-text\');\n  if(!txt) return;\n  try { var s = localStorage.getItem(\'nxc_memo\'); if(s) txt.value = s; } catch(e){}\n}\n\nfunction journalAdd(){\n  var inp = document.getElementById(\'journal-entry\');\n  if(!inp || !inp.value.trim()) return;\n  _journalEntries.unshift({ts: new Date().toLocaleTimeString(), text: inp.value.trim()});\n  inp.value = \'\';\n  renderJournal();\n}\n\nfunction renderJournal(){\n  var el = document.getElementById(\'journal-log\');\n  if(!el) return;\n  if(!_journalEntries.length){ el.innerHTML = \'<div style="color:var(--muted)">Aucune entree.</div>\'; return; }\n  el.innerHTML = _journalEntries.map(function(e,i){\n    return \'<div style="padding:5px 0;border-bottom:1px solid rgba(255,255,255,0.05);display:flex;gap:8px">\' +\n      \'<span style="color:var(--muted);font-size:10px;white-space:nowrap">[\' + e.ts + \']</span>\' +\n      \'<span style="flex:1;font-size:11px">\' + e.text + \'</span>\' +\n      \'<span onclick="_journalEntries.splice(\'+i+\',1);renderJournal()" style="cursor:pointer;color:#e74c3c;font-size:10px;flex-shrink:0">x</span>\' +\n      \'</div>\';\n  }).join(\'\');\n}\n\nfunction journalExport(){\n  if(!_journalEntries.length) return;\n  var lines = _journalEntries.map(function(e){ return \'[\' + e.ts + \'] \' + e.text; });\n  var blob = new Blob([lines.join(\'\\n\')], {type:\'text/plain\'});\n  var a = document.createElement(\'a\');\n  a.href = URL.createObjectURL(blob);\n  a.download = \'nxc-journal-\' + new Date().toISOString().slice(0,10) + \'.txt\';\n  a.click(); URL.revokeObjectURL(a.href);\n}\n\nfunction timerStart(){\n  if(_timerInterval) clearInterval(_timerInterval);\n  var mins = parseFloat(document.getElementById(\'timer-min\').value) || 5;\n  _timerEnd = Date.now() + mins * 60000;\n  var lbl = document.getElementById(\'timer-label\');\n  if(lbl) lbl.textContent = \'Minuteur \' + mins + \' min en cours...\';\n  var disp = document.getElementById(\'timer-display\');\n  if(disp) disp.style.color = \'var(--cyan)\';\n  _timerInterval = setInterval(function(){\n    var rem = _timerEnd - Date.now();\n    var d = document.getElementById(\'timer-display\');\n    if(rem <= 0){\n      clearInterval(_timerInterval); _timerInterval = null;\n      if(d){ d.textContent = \'00:00\'; d.style.color = \'var(--red)\'; }\n      var l = document.getElementById(\'timer-label\');\n      if(l) l.textContent = \'Minuteur termine !\';\n      return;\n    }\n    var m = Math.floor(rem/60000), s = Math.floor((rem%60000)/1000);\n    if(d){ d.textContent = (m<10?\'0\':\'\')+m+\':\'+(s<10?\'0\':\'\')+s; d.style.color = rem<60000?\'var(--red)\':\'var(--cyan)\'; }\n  }, 500);\n}\n\nfunction timerStop(){\n  if(_timerInterval){ clearInterval(_timerInterval); _timerInterval = null; }\n  var d = document.getElementById(\'timer-display\');\n  if(d){ d.textContent = \'--:--\'; d.style.color = \'var(--cyan)\'; }\n  var l = document.getElementById(\'timer-label\');\n  if(l) l.textContent = \'Arrete.\';\n}\n\n\n\n(func\n</script>\n\n</body>\n</html>\n'


STORE_HTML = """\
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>◈ Nexus Store</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#05080f;--bg2:#0c1120;--bg3:#111928;
  --border:#1e2d46;--border2:#283660;
  --blue:#5b9dff;--purple:#a06bff;--cyan:#00e5ff;
  --green:#34d399;--red:#ef5d6b;--orange:#f5b740;
  --text:#eaf0fb;--muted:#7a8aaa;
  --card:#0e1726;--card2:#121f33;
}
body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;overflow-x:hidden}
#particles{position:fixed;inset:0;pointer-events:none;z-index:0;overflow:hidden}
.star{position:absolute;border-radius:50%;background:#fff;animation:twinkle linear infinite}
@keyframes twinkle{0%,100%{opacity:.1}50%{opacity:.8}}
.wrap{position:relative;z-index:1;max-width:1200px;margin:0 auto;padding:0 20px 60px}
header{display:flex;align-items:center;justify-content:space-between;padding:20px 0 24px;gap:16px;flex-wrap:wrap}
.logo{display:flex;align-items:center;gap:10px}
.logo-icon{width:44px;height:44px;border-radius:12px;background:linear-gradient(135deg,#1a2a55,#2a1a55);border:1px solid var(--border2);display:flex;align-items:center;justify-content:center;font-size:22px;box-shadow:0 0 20px rgba(91,157,255,.2)}
.logo-text{font-size:22px;font-weight:800;letter-spacing:-.5px}
.logo-text span{background:linear-gradient(90deg,var(--blue),var(--purple));-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}
.logo-sub{font-size:12px;color:var(--muted);margin-top:2px}
.header-right{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
#userBadge{display:none;align-items:center;gap:8px;background:var(--card);border:1px solid var(--border);padding:8px 14px;border-radius:30px;font-size:13px}
#userBadge .dot{width:7px;height:7px;border-radius:50%;background:var(--green);box-shadow:0 0 8px var(--green)}
.btn{cursor:pointer;font-size:14px;border-radius:12px;padding:10px 18px;border:1px solid var(--border2);background:var(--card);color:var(--text);transition:.2s}
.btn:hover{border-color:var(--blue);background:var(--bg3)}
.btn-accent{background:linear-gradient(90deg,var(--blue),var(--purple));border:none;color:#fff;font-weight:700;box-shadow:0 4px 20px rgba(91,157,255,.25)}
.btn-accent:hover{box-shadow:0 4px 30px rgba(91,157,255,.45);transform:translateY(-1px)}
.btn-sm{font-size:12px;padding:7px 14px;border-radius:8px}
.tabs{display:flex;gap:4px;margin-bottom:28px;background:var(--bg2);border:1px solid var(--border);border-radius:14px;padding:5px;width:fit-content}
.tab{padding:9px 20px;border-radius:10px;cursor:pointer;font-size:14px;font-weight:600;color:var(--muted);transition:.2s;border:none;background:transparent}
.tab.active{background:var(--card2);color:var(--text);box-shadow:0 2px 12px rgba(0,0,0,.4)}
.section-title{font-size:24px;font-weight:800;margin-bottom:6px}
.section-sub{color:var(--muted);font-size:14px;margin-bottom:28px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:20px}
.item-card{background:var(--card);border:1px solid var(--border);border-radius:20px;padding:24px;display:flex;flex-direction:column;gap:14px;transition:.25s;position:relative;overflow:hidden}
.item-card::before{content:'';position:absolute;inset:0;opacity:0;transition:.3s;pointer-events:none;background:radial-gradient(circle at 50% -20%,rgba(91,157,255,.08),transparent 60%)}
.item-card:hover{border-color:var(--border2);transform:translateY(-3px);box-shadow:0 8px 40px rgba(0,0,0,.4)}
.item-card:hover::before{opacity:1}
.item-card.unlocked{border-color:rgba(52,211,153,.3)}
.item-card.unlocked::before{background:radial-gradient(circle at 50% -20%,rgba(52,211,153,.06),transparent 60%);opacity:1}
.item-card.video-card{border-color:rgba(239,93,107,.2)}
.item-card.note-card{border-color:rgba(160,107,255,.2)}
.card-top{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}
.card-icon{width:52px;height:52px;border-radius:14px;display:flex;align-items:center;justify-content:center;font-size:26px;flex-shrink:0}
.icon-file{background:rgba(91,157,255,.12);box-shadow:0 0 20px rgba(91,157,255,.1)}
.icon-note{background:rgba(160,107,255,.12);box-shadow:0 0 20px rgba(160,107,255,.1)}
.icon-video{background:rgba(239,93,107,.12);box-shadow:0 0 20px rgba(239,93,107,.1)}
.icon-unlocked{background:rgba(52,211,153,.12);box-shadow:0 0 20px rgba(52,211,153,.15)}
.type-badge{font-size:10px;font-weight:700;letter-spacing:.5px;padding:4px 10px;border-radius:20px;text-transform:uppercase}
.badge-file{background:rgba(91,157,255,.15);color:var(--blue)}
.badge-note{background:rgba(160,107,255,.15);color:var(--purple)}
.badge-video{background:rgba(239,93,107,.15);color:var(--red)}
.badge-unlocked{background:rgba(52,211,153,.15);color:var(--green)}
.card-name{font-size:17px;font-weight:700;line-height:1.3}
.card-desc{font-size:13px;color:var(--muted);line-height:1.6;flex:1}
.unlock-zone{display:flex;flex-direction:column;gap:8px}
.code-row{display:flex;gap:8px}
.code-input{flex:1;padding:11px 14px;font-size:14px;background:var(--bg2);border:1px solid var(--border);border-radius:10px;color:var(--text);outline:none;font-family:monospace;letter-spacing:2px;transition:.2s}
.code-input:focus{border-color:var(--blue)}
.code-input.error{border-color:var(--red);animation:shake .3s}
.code-input.success{border-color:var(--green)}
@keyframes shake{0%,100%{transform:translateX(0)}25%{transform:translateX(-6px)}75%{transform:translateX(6px)}}
.code-msg{font-size:12px;min-height:16px}
.code-msg.err{color:var(--red)}
.code-msg.ok{color:var(--green)}
.access-zone{display:flex;flex-direction:column;gap:8px}
.unlocked-label{display:flex;align-items:center;gap:6px;font-size:12px;color:var(--green);font-weight:600}
.expires-label{font-size:11px;color:var(--orange)}
.expires-label.expired{color:var(--red)}
.access-btns{display:flex;gap:8px;flex-wrap:wrap}
.btn-download{flex:1;padding:11px;border-radius:10px;font-size:14px;font-weight:600;cursor:pointer;background:linear-gradient(90deg,var(--blue),var(--purple));color:#fff;border:none;transition:.2s}
.btn-download:hover{box-shadow:0 4px 20px rgba(91,157,255,.35);transform:translateY(-1px)}
.btn-watch{flex:1;padding:11px;border-radius:10px;font-size:14px;font-weight:600;cursor:pointer;background:linear-gradient(90deg,#ef5d6b,#ff9966);color:#fff;border:none;transition:.2s}
.btn-watch:hover{box-shadow:0 4px 20px rgba(239,93,107,.35);transform:translateY(-1px)}
.btn-read{flex:1;padding:11px;border-radius:10px;font-size:14px;font-weight:600;cursor:pointer;background:linear-gradient(90deg,var(--purple),#6b4fff);color:#fff;border:none;transition:.2s}
.btn-read:hover{box-shadow:0 4px 20px rgba(160,107,255,.35);transform:translateY(-1px)}
.empty{text-align:center;padding:80px 20px;color:var(--muted)}
.empty .empty-icon{font-size:48px;margin-bottom:12px}
.overlay{position:fixed;inset:0;background:rgba(0,0,0,.75);backdrop-filter:blur(4px);z-index:100;display:flex;align-items:center;justify-content:center;padding:16px}
.modal{background:var(--card);border:1px solid var(--border2);border-radius:24px;padding:28px;max-width:540px;width:100%;max-height:90vh;overflow-y:auto;position:relative}
.modal-close{position:absolute;top:16px;right:16px;background:var(--bg2);border:none;color:var(--muted);font-size:18px;width:32px;height:32px;border-radius:8px;cursor:pointer;display:flex;align-items:center;justify-content:center}
.modal-close:hover{color:var(--text)}
.modal h2{font-size:20px;font-weight:800;margin-bottom:16px}
.login-modal-icon{width:64px;height:64px;border-radius:20px;margin:0 auto 16px;background:linear-gradient(135deg,rgba(91,157,255,.2),rgba(160,107,255,.2));border:1px solid var(--border2);display:flex;align-items:center;justify-content:center;font-size:30px}
.field{display:flex;flex-direction:column;gap:6px;margin-bottom:14px}
.field label{font-size:12px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
.field input{padding:12px 14px;background:var(--bg2);border:1px solid var(--border);border-radius:12px;color:var(--text);font-size:15px;outline:none;transition:.2s}
.field input:focus{border-color:var(--blue)}
.note-content{background:var(--bg2);border:1px solid var(--border);border-radius:14px;padding:20px;line-height:1.8;white-space:pre-wrap;font-size:14px;max-height:50vh;overflow-y:auto;color:var(--text)}
.video-wrap{position:relative;padding-bottom:56.25%;height:0;border-radius:14px;overflow:hidden;background:#000}
.video-wrap iframe{position:absolute;inset:0;width:100%;height:100%;border:none}
#toast{position:fixed;bottom:30px;left:50%;transform:translateX(-50%) translateY(80px);background:var(--card);border:1px solid var(--border2);color:var(--text);padding:12px 20px;border-radius:14px;font-size:14px;z-index:999;transition:.3s;white-space:nowrap;box-shadow:0 8px 32px rgba(0,0,0,.4)}
#toast.show{transform:translateX(-50%) translateY(0)}
@media(max-width:600px){.grid{grid-template-columns:1fr}}
</style>
</head>
<body>
<div id="particles"></div>
<div class="wrap">
  <header>
    <div class="logo">
      <div class="logo-icon">◈</div>
      <div>
        <div class="logo-text">NEXUS <span>STORE</span></div>
        <div class="logo-sub">Boutique officielle · Récompenses exclusives</div>
      </div>
    </div>
    <div class="header-right">
      <div id="userBadge">
        <div class="dot"></div>
        <span id="userBadgeName"></span>
        <button class="btn btn-sm" onclick="logout()">Déconnexion</button>
      </div>
      <button class="btn btn-accent" id="loginBtn" onclick="openLogin()">🔐 Se connecter</button>
    </div>
  </header>
  <div class="tabs">
    <button class="tab active" id="tab-store" onclick="showTab('store')">🏪 Boutique</button>
    <button class="tab" id="tab-mine" onclick="showTab('mine')">🔓 Mes achats</button>
  </div>
  <div id="pane-store">
    <div class="section-title">Toutes les récompenses</div>
    <div class="section-sub">Entre le code pour débloquer un article · Tes achats sont permanents</div>
    <div class="grid" id="storeGrid"><div class="empty"><div class="empty-icon">◈</div><div>Chargement…</div></div></div>
  </div>
  <div id="pane-mine" style="display:none">
    <div class="section-title">Mes achats</div>
    <div class="section-sub">Tous tes articles débloqués · Re-télécharge à tout moment</div>
    <div class="grid" id="myGrid"><div class="empty"><div class="empty-icon">🔓</div><div>Connecte-toi pour voir tes achats</div></div></div>
  </div>
</div>
<div id="toast"></div>
<div id="modalRoot"></div>
<script>
let USER=null,PASS=null,ROLE=null,ALL_ITEMS=[],MY_ITEMS={},pendingUnlock=null;
(function(){const c=document.getElementById('particles');for(let i=0;i<80;i++){const s=document.createElement('div');s.className='star';const sz=Math.random()*2.5+.5;s.style.cssText=`width:${sz}px;height:${sz}px;left:${Math.random()*100}%;top:${Math.random()*100}%;opacity:${Math.random()*.6+.1};animation-duration:${Math.random()*4+2}s;animation-delay:${Math.random()*4}s`;c.appendChild(s);}})();
async function api(path,body){const r=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});return r.json();}
function updateHeader(){if(USER){document.getElementById('userBadge').style.display='flex';document.getElementById('userBadgeName').textContent=USER;document.getElementById('loginBtn').style.display='none';}else{document.getElementById('userBadge').style.display='none';document.getElementById('loginBtn').style.display='';}}
function logout(){USER=null;PASS=null;ROLE=null;MY_ITEMS={};updateHeader();renderStore();toast('Déconnecté');}
function creds(){return USER?{username:USER,password:PASS}:{};}
let _toastTimer;
function toast(msg,type){const t=document.getElementById('toast');t.textContent=msg;t.style.borderColor=type==='ok'?'rgba(52,211,153,.4)':type==='err'?'rgba(239,93,107,.4)':'var(--border2)';t.classList.add('show');clearTimeout(_toastTimer);_toastTimer=setTimeout(()=>t.classList.remove('show'),3000);}
function showTab(tab){document.getElementById('pane-store').style.display=tab==='store'?'':'none';document.getElementById('pane-mine').style.display=tab==='mine'?'':'none';document.getElementById('tab-store').classList.toggle('active',tab==='store');document.getElementById('tab-mine').classList.toggle('active',tab==='mine');if(tab==='mine')loadMyItems();}
async function loadItems(){const res=await api('/store/api/items',{});if(res.ok){ALL_ITEMS=res.items;renderStore();}}
async function loadMyItems(){if(!USER){document.getElementById('myGrid').innerHTML='<div class="empty"><div class="empty-icon">🔐</div><div>Connecte-toi pour voir tes achats</div></div>';return;}const res=await api('/store/api/my_items',creds());if(res.ok){MY_ITEMS={};res.items.forEach(i=>MY_ITEMS[i.id]=i);renderStore();renderMine(res.items);}}
function typeIcon(t){return t==='note'?'📝':t==='video_rental'?'🎬':'📁';}
function typeBadge(t){if(t==='note')return '<span class="type-badge badge-note">Note</span>';if(t==='video_rental')return '<span class="type-badge badge-video">📡 Location 48h</span>';return '<span class="type-badge badge-file">Fichier</span>';}
function cardClass(t){return t==='note'?'note-card':t==='video_rental'?'video-card':'';}
function iconClass(t){return t==='note'?'icon-note':t==='video_rental'?'icon-video':'icon-file';}
function renderStore(){
  const g=document.getElementById('storeGrid');
  if(!ALL_ITEMS.length){g.innerHTML='<div class="empty"><div class="empty-icon">◈</div><div>Aucun article disponible</div></div>';return;}
  g.innerHTML=ALL_ITEMS.map(item=>{
    const unlocked=MY_ITEMS[item.id];const expired=unlocked&&unlocked.expired;
    let accessZone='';
    if(unlocked&&!expired){
      let timeLabel='';
      if(item.type==='video_rental'&&unlocked.expires_at){const rem=unlocked.expires_at-Math.floor(Date.now()/1000);const h=Math.floor(rem/3600),m=Math.floor((rem%3600)/60);timeLabel=`<div class="expires-label">⏱ Expire dans ${h}h ${m}m</div>`;}
      let btn='';
      if(item.type==='file')btn=`<button class="btn-download" onclick="doDownload('${item.id}')">⬇ Télécharger</button>`;
      else if(item.type==='video_rental')btn=`<button class="btn-watch" onclick="doWatch('${item.id}')">▶ Regarder</button>`;
      else if(item.type==='note')btn=`<button class="btn-read" onclick="doNote('${item.id}')">📖 Lire la note</button>`;
      accessZone=`<div class="access-zone"><div class="unlocked-label">✅ DÉBLOQUÉ</div>${timeLabel}<div class="access-btns">${btn}</div></div>`;
    }else if(expired){
      accessZone=`<div class="access-zone"><div class="unlocked-label" style="color:var(--red)">⛔ Location expirée</div></div>`;
    }else{
      accessZone=`<div class="unlock-zone" id="zone-${item.id}"><div class="code-row"><input class="code-input" id="code-${item.id}" placeholder="CODE D'ACCÈS" onkeydown="if(event.key==='Enter')tryUnlock('${item.id}')"><button class="btn btn-sm" onclick="tryUnlock('${item.id}')">🔓</button></div><div class="code-msg" id="msg-${item.id}"></div></div>`;
    }
    return `<div class="item-card ${cardClass(item.type)} ${unlocked&&!expired?'unlocked':''}"><div class="card-top"><div class="card-icon ${unlocked&&!expired?'icon-unlocked':iconClass(item.type)}">${unlocked&&!expired?'✨':typeIcon(item.type)}</div>${typeBadge(item.type)}</div><div class="card-name">${esc(item.name)}</div>${item.description?`<div class="card-desc">${esc(item.description)}</div>`:''}${accessZone}</div>`;
  }).join('');
}
function renderMine(items){
  const g=document.getElementById('myGrid');
  if(!items.length){g.innerHTML='<div class="empty"><div class="empty-icon">🔓</div><div>Tu n\'as encore rien débloqué</div></div>';return;}
  g.innerHTML=items.map(item=>{
    const expired=item.expired;
    let btn='';
    if(!expired){if(item.type==='file')btn=`<button class="btn-download" onclick="doDownload('${item.id}')">⬇ Re-télécharger</button>`;else if(item.type==='video_rental')btn=`<button class="btn-watch" onclick="doWatch('${item.id}')">▶ Regarder</button>`;else if(item.type==='note')btn=`<button class="btn-read" onclick="doNote('${item.id}')">📖 Lire</button>`;}
    let timeLabel='';
    if(item.type==='video_rental'&&item.expires_at&&!expired){const rem=item.expires_at-Math.floor(Date.now()/1000);const h=Math.floor(rem/3600),m=Math.floor((rem%3600)/60);timeLabel=`<div class="expires-label">⏱ ${h}h ${m}m restant</div>`;}
    return `<div class="item-card ${cardClass(item.type)} ${!expired?'unlocked':''}"><div class="card-top"><div class="card-icon ${!expired?'icon-unlocked':iconClass(item.type)}">${!expired?'✨':typeIcon(item.type)}</div>${typeBadge(item.type)}</div><div class="card-name">${esc(item.name)}</div>${item.description?`<div class="card-desc">${esc(item.description)}</div>`:''}
<div class="access-zone">${expired?'<div class="unlocked-label" style="color:var(--red)">⛔ Location expirée</div>':'<div class="unlocked-label">✅ DÉBLOQUÉ</div>'}${timeLabel}${btn?`<div class="access-btns">${btn}</div>`:''}</div></div>`;
  }).join('');
}
async function tryUnlock(item_id){
  if(!USER){pendingUnlock=item_id;openLogin();return;}
  const input=document.getElementById('code-'+item_id);const msg=document.getElementById('msg-'+item_id);
  const code=input?input.value.trim():'';
  if(!code){if(msg)msg.innerHTML='<span class="err">Entre un code</span>';return;}
  if(msg)msg.textContent='';
  const res=await api('/store/api/unlock',{...creds(),item_id,code});
  if(res.ok){toast('✅ Article débloqué !','ok');await loadMyItems();renderStore();}
  else{if(input){input.classList.add('error');setTimeout(()=>input.classList.remove('error'),400);}if(msg)msg.innerHTML=`<span class="err">${esc(res.error||'Erreur')}</span>`;toast(res.error||'Code incorrect','err');}
}
async function doDownload(item_id){
  toast('Préparation…');const res=await api('/store/api/download',{...creds(),item_id});
  if(res.ok){const a=document.createElement('a');a.href=res.url;a.download=res.filename||'download';a.click();toast('⬇ Téléchargement lancé','ok');}
  else toast(res.error||'Erreur','err');
}
async function doWatch(item_id){
  toast('Connexion au flux…');const res=await api('/store/api/stream',{...creds(),item_id});
  if(res.ok){const rem=res.expires_at?Math.floor((res.expires_at-Date.now()/1000)/3600):48;openModal(`<button class="modal-close" onclick="closeModal()">✕</button><h2>🎬 Lecture</h2><div style="color:var(--orange);font-size:12px;margin-bottom:12px">⏱ Location · expire dans ~${rem}h · Protection Cloudflare</div><div class="video-wrap" oncontextmenu="return false"><iframe src="${res.iframe_url}" allow="accelerometer;gyroscope;autoplay;encrypted-media;picture-in-picture" allowfullscreen></iframe></div><div style="color:var(--muted);font-size:11px;margin-top:10px;text-align:center">Téléchargement impossible · Protégé par Cloudflare Stream</div>`);}
  else if(res.expired)toast('⛔ Location expirée','err');
  else toast(res.error||'Erreur lecture','err');
}
async function doNote(item_id){
  toast('Chargement…');const res=await api('/store/api/note',{...creds(),item_id});
  if(res.ok){window._currentNote={name:res.name,content:res.content};openModal(`<button class="modal-close" onclick="closeModal()">✕</button><h2>📝 ${esc(res.name)}</h2><div class="note-content">${esc(res.content)}</div><div style="display:flex;gap:10px;margin-top:14px"><button class="btn btn-accent" style="flex:1" onclick="downloadNotePDF()">⬇ PDF</button><button class="btn" onclick="closeModal()">Fermer</button></div>`);}
  else toast(res.error||'Erreur','err');
}
function downloadNotePDF(){
  const note=window._currentNote;if(!note)return;
  const s=document.createElement('script');s.src='https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js';
  s.onload=function(){
    const{jsPDF}=window.jspdf;const doc=new jsPDF({unit:'mm',format:'a4'});
    doc.setFillColor(5,8,15);doc.rect(0,0,210,297,'F');
    doc.setFillColor(14,23,38);doc.rect(0,0,210,35,'F');
    doc.setTextColor(91,157,255);doc.setFontSize(22);doc.setFont('helvetica','bold');doc.text('◈ NEXUS STORE',14,16);
    doc.setTextColor(160,107,255);doc.setFontSize(13);doc.text(note.name,14,26);
    doc.setTextColor(122,138,170);doc.setFontSize(9);doc.text('Document confidentiel · Nexus · '+new Date().toLocaleDateString('fr-FR'),14,32);
    doc.setTextColor(234,240,251);doc.setFontSize(11);doc.setFont('helvetica','normal');
    const lines=doc.splitTextToSize(note.content,182);doc.text(lines,14,48);
    doc.save((note.name||'note').replace(/[^a-z0-9]/gi,'_')+'.pdf');toast('📄 PDF téléchargé','ok');
  };
  document.head.appendChild(s);
}
function openLogin(){openModal(`<button class="modal-close" onclick="closeModal()">✕</button><div class="login-modal-icon">🛡️</div><h2 style="text-align:center;margin-bottom:6px">Connexion Nexus</h2><p style="color:var(--muted);font-size:13px;text-align:center;margin-bottom:20px">Utilise ton compte Nexus habituel</p><div class="field"><label>Nom d\'utilisateur</label><input id="loginUser" autocomplete="username" onkeydown="if(event.key===\'Enter\')document.getElementById(\'loginPass\').focus()"></div><div class="field"><label>Mot de passe</label><input id="loginPass" type="password" autocomplete="current-password" onkeydown="if(event.key===\'Enter\')doLogin()"></div><div id="loginErr" style="color:var(--red);font-size:13px;min-height:18px;margin-bottom:10px"></div><button class="btn btn-accent" style="width:100%;padding:13px" onclick="doLogin()">Se connecter</button>`);setTimeout(()=>{const u=document.getElementById('loginUser');if(u)u.focus();},100);}
async function doLogin(){
  const user=(document.getElementById('loginUser')||{}).value||'';
  const pass=(document.getElementById('loginPass')||{}).value||'';
  const err=document.getElementById('loginErr');
  if(!user||!pass){if(err)err.textContent='Remplis tous les champs';return;}
  if(err)err.textContent='Connexion…';
  const res=await api('/store/api/my_items',{username:user.trim(),password:pass});
  if(res.ok){USER=user.trim();PASS=pass;ROLE=res.role;MY_ITEMS={};res.items.forEach(i=>MY_ITEMS[i.id]=i);closeModal();updateHeader();renderStore();toast(`Bienvenue ${USER} ✨`,'ok');if(pendingUnlock){const id=pendingUnlock;pendingUnlock=null;tryUnlock(id);}}
  else{if(err)err.textContent=res.error||'Erreur de connexion';}
}
function openModal(html){document.getElementById('modalRoot').innerHTML=`<div class="overlay" onclick="if(event.target===this)closeModal()"><div class="modal">${html}</div></div>`;}
function closeModal(){document.getElementById('modalRoot').innerHTML='';}
function esc(s){return(s||'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
loadItems();
</script>
</body>
</html>
"""

ADMIN_HTML = """\
<!DOCTYPE html><html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Nexus — Administration</title><style>
*{box-sizing:border-box;font-family:'Segoe UI',system-ui,Arial,sans-serif}
body{margin:0;background:#0a0d14;color:#eaf0fb}
.wrap{max-width:920px;margin:0 auto;padding:18px}
h1{font-size:22px;margin:0 0 4px}
.muted{color:#8a96ad;font-size:13px}
.card{background:#121724;border:1px solid #283046;border-radius:14px;padding:16px;margin-top:14px}
input,select,button{font-size:15px;border-radius:10px;padding:11px 13px;border:1px solid #283046;background:#1b2233;color:#eaf0fb;outline:none}
input:focus,select:focus{border-color:#5b9dff}
button{cursor:pointer}
button:hover{border-color:#5b9dff}
.accent{border:none;font-weight:700;color:#06080c;background:linear-gradient(90deg,#5b9dff,#a06bff)}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.grow{flex:1;min-width:120px}
table{width:100%;border-collapse:collapse;margin-top:8px}
th,td{text-align:left;padding:10px 8px;border-bottom:1px solid #1c2333;font-size:14px}
th{color:#8a96ad;font-size:12px;text-transform:uppercase;letter-spacing:.5px}
.badge{font-size:11px;padding:2px 8px;border-radius:20px}
.adm{background:#3b2d5e;color:#c9b6ff}
.usr{background:#1e3346;color:#9ec7ff}
.act{background:transparent;border:1px solid #283046;padding:6px 9px;font-size:13px;border-radius:8px}
.ok{color:#34d399}
.warn{color:#f5b740}
.off{color:#ef5d6b}
.hidden{display:none}
.overlay{position:fixed;inset:0;background:rgba(0,0,0,.6);display:flex;align-items:center;justify-content:center;padding:16px}
.modal{background:#121724;border:1px solid #283046;border-radius:16px;padding:18px;max-width:560px;width:100%;max-height:85vh;overflow:auto}
pre{white-space:pre-wrap;word-break:break-word;background:#0a0d14;border:1px solid #283046;border-radius:10px;padding:10px;font-size:12px;color:#c7d2e6}
.conn-card{background:#0d1320;border:1px solid #1e2d44;border-radius:10px;padding:12px;margin-bottom:10px}
.conn-user{font-weight:700;color:#5b9dff;font-size:15px;margin-bottom:6px}
.conn-item{display:flex;gap:10px;align-items:center;padding:6px 0;border-bottom:1px solid #1a2236;font-size:13px}
.conn-item:last-child{border-bottom:none}
.conn-ip{color:#00e5ff;font-family:monospace}
.conn-time{color:#8a96ad;font-size:11px}
.conn-app{color:#a06bff;font-size:11px}
.piratage-toolbar{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px}

/* Store admin */
.store-form{display:flex;flex-direction:column;gap:10px;margin-top:12px}
.store-form input,.store-form select,.store-form textarea{font-size:14px;border-radius:10px;padding:10px 13px;border:1px solid #283046;background:#0d1320;color:#eaf0fb;outline:none;width:100%}
.store-form textarea{resize:vertical;min-height:80px;font-family:inherit}
.store-form input:focus,.store-form select:focus,.store-form textarea:focus{border-color:#5b9dff}
.store-item-row{background:#0d1320;border:1px solid #1e2d44;border-radius:10px;padding:12px;margin-bottom:8px}
.store-item-row .s-name{font-weight:700;color:#eaf0fb;margin-bottom:4px}
.store-item-row .s-meta{font-size:12px;color:#7a8aaa;margin-bottom:6px}
.store-item-row .s-btns{display:flex;gap:6px;flex-wrap:wrap}
.s-badge{display:inline-block;font-size:10px;padding:2px 8px;border-radius:20px;font-weight:700;margin-left:6px}
.s-file{background:rgba(91,157,255,.15);color:#5b9dff}
.s-note{background:rgba(160,107,255,.15);color:#a06bff}
.s-video{background:rgba(239,93,107,.15);color:#ef5d6b}
.upload-progress{height:4px;background:#1e2d44;border-radius:2px;overflow:hidden;margin-top:6px}
.upload-progress-bar{height:100%;background:linear-gradient(90deg,#5b9dff,#a06bff);width:0%;transition:.3s}
</style></head><body>
<div class="wrap">
  <h1>🛡️ Nexus — Administration</h1>
  <div class="muted">Tout ce que tu fais ici est enregistré sur le serveur en ligne.</div>
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
        <button onclick="showSection('comptes')" id="btn-comptes" class="accent" style="padding:8px 12px;font-size:13px">👥 Comptes</button>
        <button onclick="showSection('piratage')" id="btn-piratage" style="padding:8px 12px;font-size:13px;background:#0d1428;border-color:#a06bff;color:#a06bff">📡 Piratage</button>
        <button onclick="showSection('store')" id="btn-store" style="padding:8px 12px;font-size:13px;background:#0d1428;border-color:#f5b740;color:#f5b740">🛒 Store</button>
        <input id="search" class="grow" placeholder="🔍 Rechercher…" oninput="render()">
        <label class="muted"><input type="checkbox" id="showHidden" onchange="render()"> voir masqués</label>
      </div>
    </div>
    <div id="section-comptes">
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
        <div class="row"><b class="grow">Comptes (<span id="count">0</span>)</b><span class="muted" id="tick">actualisation auto…</span></div>
        <table><thead><tr><th>Compte</th><th>Rôle</th><th>Pages</th><th>Dernière connexion</th><th></th></tr></thead>
        <tbody id="tbody"></tbody></table>
      </div>
    </div>
    <div id="section-piratage" class="hidden">
      <div class="card">
        <b>📡 Connexions — Données complètes</b>
        <div class="piratage-toolbar" style="margin-top:12px">
          <button onclick="loadAllConns()" class="accent">🔄 Charger toutes les connexions</button>
          <button onclick="exportConnsJSON()" style="border-color:#f5b740;color:#f5b740;background:#1a1500">⬇ Exporter JSON</button>
          <label style="border:1px solid #a06bff;color:#a06bff;background:#0d0a1a;padding:8px 12px;border-radius:10px;cursor:pointer;font-size:13px">
            📥 Importer JSON<input type="file" accept=".json" style="display:none" onchange="importConnsJSON(this)">
          </label>
          <select id="filterUser" onchange="renderConns()" style="flex:1;min-width:120px"><option value="">Tous les utilisateurs</option></select>
        </div>
        <div id="connCount" class="muted" style="margin-bottom:10px"></div>
        <div id="connList"></div>
      </div>
    </div>
  </div>


    <div id="section-store" class="hidden">
      <div class="card">
        <div class="row" style="margin-bottom:12px">
          <b class="grow">🛒 Articles du Store (<span id="store-count">0</span>)</b>
          <button class="accent" onclick="storeShowCreate()">➕ Nouvel article</button>
          <button class="act" onclick="storeLoadItems()">🔄</button>
          <button class="act" onclick="storeShowStats()">📊 Stats achats</button>
        </div>
        <div id="store-items-list"><div class="muted" style="font-size:13px">Chargement…</div></div>
      </div>
    </div>
</div>
<div id="modal"></div>
<script>
let KEY="",USERS=[],_connData={};
async function api(path,body){body=body||{};body.master_key=KEY;const r=await fetch(path,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});return await r.json();}
function showSection(sec){
  document.getElementById("section-comptes").classList.toggle("hidden",sec!=="comptes");
  document.getElementById("section-piratage").classList.toggle("hidden",sec!=="piratage");
  document.getElementById("btn-comptes").className=sec==="comptes"?"accent":"act";
  document.getElementById("btn-piratage").style.cssText=sec==="piratage"?"padding:8px 12px;font-size:13px;background:#1a0d2a;border-color:#a06bff;color:#fff":"padding:8px 12px;font-size:13px;background:#0d1428;border-color:#a06bff;color:#a06bff";
  if(sec==="piratage"&&Object.keys(_connData).length===0)loadAllConns();
  document.getElementById("section-store").classList.toggle("hidden",sec!=="store");
  document.getElementById("btn-store").style.cssText=sec==="store"?"padding:8px 12px;font-size:13px;background:#1a1500;border-color:#f5b740;color:#fff":"padding:8px 12px;font-size:13px;background:#0d1428;border-color:#f5b740;color:#f5b740";
  if(sec==="store")storeLoadItems();
}
async function connecter(){KEY=document.getElementById("mk").value.trim();const msg=document.getElementById("loginmsg");msg.textContent="Connexion…";const res=await api("/admin/list");if(res&&res.ok){document.getElementById("login").classList.add("hidden");document.getElementById("dash").classList.remove("hidden");USERS=res.users||[];render();if(!window._timer)window._timer=setInterval(rafraichir,3000);}else{msg.innerHTML="<span class='off'>Clé maître refusée.</span>";}}
async function rafraichir(){const res=await api("/admin/list");if(res&&res.ok){USERS=res.users||[];document.getElementById("status").innerHTML="<span class='ok'>● En ligne — synchronisé</span>";render();const t=document.getElementById("tick");t.textContent="à jour • "+new Date().toLocaleTimeString();}else{document.getElementById("status").innerHTML="<span class='warn'>● reconnexion…</span>";}}
function render(){const q=document.getElementById("search").value.toLowerCase();const showH=document.getElementById("showHidden").checked;const tb=document.getElementById("tbody");tb.innerHTML="";let shown=0;USERS.forEach(u=>{if(u.hidden&&!showH)return;if(q&&!u.username.toLowerCase().includes(q)&&!(u.nickname||"").toLowerCase().includes(q))return;shown++;const tr=document.createElement("tr");const nick=u.nickname?" « "+esc(u.nickname)+" »":"";const badge=u.role==="admin"?"<span class='badge adm'>👑 admin</span>":"<span class='badge usr'>👤 user</span>";const mask=u.hidden?"🙈 ":"";tr.innerHTML="<td>"+mask+"<b>"+esc(u.username)+"</b>"+nick+"</td><td>"+badge+"</td><td>"+u.history+"</td><td class='muted'>"+(u.last_login?esc(u.last_login)+" · "+esc(u.last_ip):"jamais")+"</td><td class='row'><button class='act' onclick="voir('"+jsq(u.username)+"')">Voir</button><button class='act' onclick="renommer('"+jsq(u.username)+"')">Renommer</button><button class='act' onclick="surnom('"+jsq(u.username)+"')">Surnom</button><button class='act' onclick="masquer('"+jsq(u.username)+"',"+(!u.hidden)+")">"+( u.hidden?"Afficher":"Masquer")+"</button><button class='act off' onclick="supprimer('"+jsq(u.username)+"')">Suppr</button></td>";tb.appendChild(tr);});document.getElementById("count").textContent=shown;}
async function creer(){const u=document.getElementById("nu").value.trim();const p=document.getElementById("np").value;const r=document.getElementById("nr").value;const msg=document.getElementById("createmsg");if(!u||!p){msg.innerHTML="<span class='warn'>Nom et mot de passe requis.</span>";return;}const res=await api("/admin/create",{new_username:u,new_password:p,role:r});if(res.ok){msg.innerHTML="<span class='ok'>Compte « "+esc(u)+" » créé ✅</span>";document.getElementById("nu").value="";document.getElementById("np").value="";rafraichir();}else{msg.innerHTML="<span class='off'>"+esc(res.error||"erreur")+"</span>";}}
async function voir(name){const res=await api("/admin/get",{target:name});if(!res.ok)return;const logins=(res.logins||[]).slice(0,20).map(l=>" "+l.time+" — "+l.ip).join("
")||" (aucune)";const nx2098=((res.data||{}).nx2098||{});const nxcoin=((res.data||{}).nxcoin||{});const nxInfo=" Rewards:"+(nx2098.rewards||0)+"
 NXC:"+(nxcoin.nxc||0);openModal("<h3>"+esc(name)+"</h3><div class='muted'>Rôle: "+esc(res.role)+(res.nickname?" · « "+esc(res.nickname)+" »":"")+
"</div><b>◈ NXC Coin</b><pre>"+esc(nxInfo)+"</pre><b>Connexions (IP + heure)</b><pre>"+esc(logins)+"</pre><button class='accent' onclick='closeModal()'>Fermer</button>");}
async function renommer(name){const nn=prompt("Nouveau nom pour « "+name+" »:",name);if(!nn||!nn.trim())return;const res=await api("/admin/rename",{target:name,new_username:nn.trim()});if(!res.ok)alert(res.error||"erreur");rafraichir();}
async function surnom(name){const nk=prompt("Surnom pour « "+name+" »:","");if(nk===null)return;await api("/admin/nickname",{target:name,nickname:nk});rafraichir();}
async function masquer(name,hide){await api("/admin/hide",{target:name,hidden:hide});rafraichir();}
async function supprimer(name){if(!confirm("Supprimer DÉFINITIVEMENT « "+name+" » ?"))return;await api("/admin/purge",{target:name});rafraichir();}
function openModal(html){document.getElementById("modal").innerHTML="<div class='overlay' onclick='if(event.target===this)closeModal()'><div class='modal'>"+html+"</div></div>";}
function closeModal(){document.getElementById("modal").innerHTML="";}
function esc(s){return (s+"").replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":" &quot;"}[c]));}
function jsq(s){return (s+"").replace(/\/g,"\\").replace(/'/g,"\'");}
document.getElementById("mk").addEventListener("keydown",e=>{if(e.key==="Enter")connecter();});
async function loadAllConns(){document.getElementById("connCount").textContent="Chargement…";document.getElementById("connList").innerHTML="";_connData={};const listRes=await api("/admin/list");if(!listRes.ok){document.getElementById("connCount").textContent="Erreur chargement";return;}const users=listRes.users||[];const sel=document.getElementById("filterUser");sel.innerHTML='<option value="">Tous les utilisateurs</option>';for(const u of users){const opt=document.createElement("option");opt.value=u.username;opt.textContent=u.username;sel.appendChild(opt);const details=await api("/admin/get",{target:u.username});if(!details.ok)continue;const logins=details.logins||[];const conns=(details.data&&details.data.connections)||[];const allConns=[...logins.map(l=>({ts:l.time,ip:l.ip,app:"Nexus",type:"login",user:u.username})),...conns.map(c=>({...c,user:u.username}))];allConns.sort((a,b)=>(b.ts||0)>(a.ts||0)?1:-1);_connData[u.username]=allConns;}renderConns();}
function renderConns(){const filter=document.getElementById("filterUser").value;const list=document.getElementById("connList");list.innerHTML="";let total=0;const users=filter?[filter]:Object.keys(_connData);for(const uname of users){const conns=_connData[uname]||[];if(!conns.length)continue;total+=conns.length;const div=document.createElement("div");div.className="conn-card";let html='<div class="conn-user">'+esc(uname)+' <span class="muted" style="font-size:11px;font-weight:normal">('+conns.length+' connexions)</span>&nbsp;<button onclick="printUser(\''+jsq(uname)+'\')" class="act" style="font-size:10px;padding:3px 8px">🖨️</button></div>';for(const c of conns.slice(0,50)){const ts=c.ts||c.time||"";const ip=c.ip||"?";const app=c.app||c.type||"Nexus";const country=c.country||"";const device=c.device||c.ua||"";html+='<div class="conn-item"><span class="conn-ip">'+esc(ip)+'</span>'+(country?'<span style="font-size:11px">🌍 '+esc(country)+'</span>':'')+'<span class="conn-app">'+esc(app)+'</span><span class="conn-time">'+esc(ts)+'</span>'+(device?'<span class="muted" style="font-size:10px;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+esc(device)+'">'+esc(device.substring(0,40))+'</span>':'')+'</div>';}if(conns.length>50)html+='<div class="muted" style="font-size:11px;padding:4px">… et '+(conns.length-50)+' de plus</div>';div.innerHTML=html;list.appendChild(div);}document.getElementById("connCount").textContent=total+" connexions — "+users.length+" utilisateur(s)";}
function exportConnsJSON(){const data=JSON.stringify({date:new Date().toISOString(),connections:_connData},null,2);const blob=new Blob([data],{type:"application/json"});const a=document.createElement("a");a.href=URL.createObjectURL(blob);a.download="nexus_connections_"+Date.now()+".json";a.click();}
async function importConnsJSON(input){const file=input.files[0];if(!file)return;const text=await file.text();let data;try{data=JSON.parse(text);}catch(e){alert("JSON invalide");return;}const connsToImport=data.connections||data;if(!confirm("Fusionner ces connexions avec la base serveur ?"))return;let imported=0;for(const [uname,conns] of Object.entries(connsToImport)){if(!Array.isArray(conns))continue;const res=await fetch("/admin/connections/import",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({master_key:KEY,target:uname,connections:conns})});const d=await res.json();if(d.ok)imported+=d.added||0;}alert("✅ "+imported+" connexion(s) importée(s).");await loadAllConns();}
function printUser(uname){const conns=_connData[uname]||[];const win=window.open("","_blank");win.document.write("<!DOCTYPE html><html><head><meta charset='utf-8'><title>Connexions — "+uname+"</title><style>body{font-family:monospace;padding:20px}h1{font-size:18px}table{border-collapse:collapse;width:100%}th,td{border:1px solid #ccc;padding:6px 10px;text-align:left}th{background:#eee}</style></head><body><h1>📡 Connexions — "+uname+"</h1><p>Exporté le "+new Date().toLocaleString("fr-FR")+" · "+conns.length+" entrée(s)</p><table><thead><tr><th>Date/Heure</th><th>IP</th><th>Pays</th><th>App</th><th>Appareil</th></tr></thead><tbody>"+conns.map(c=>"<tr><td>"+(c.ts||c.time||"")+"</td><td>"+(c.ip||"?")+"</td><td>"+(c.country||"")+"</td><td>"+(c.app||c.type||"")+"</td><td>"+(c.device||c.ua||"")+"</td></tr>").join("")+"</tbody></table></body></html>");win.document.close();win.print();}
</script></body></html>
"""

@app.get("/panel")
def panel():
    return Response(ADMIN_HTML, mimetype="text/html")


@app.get("/admin/logs")
def admin_logs():
    mk = request.args.get("k","")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    return jsonify({"ok": True, "logs": list(reversed(_req_log[-50:])), "uptime": round(time.time()-_server_start)})

@app.get("/admin/stats")
def admin_stats():
    mk = request.args.get("k","")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    with _lock:
        db = load_db()
    users = db.get("users", {})
    total_rewards = 0.0
    total_nxc = 0.0
    countries = {}
    apps = {}
    conns_by_day = {}
    top_rewards = []
    roles = {"user": 0, "admin": 0, "moderator": 0, "vip": 0}
    for uname, ud in users.items():
        role = ud.get("role", "user")
        roles[role] = roles.get(role, 0) + 1
        d = ud.get("data", {})
        r = float((d.get("nx2098") or {}).get("rewards", 0) or 0)
        n = float((d.get("nxcoin") or {}).get("nxc", 0) or 0)
        total_rewards += r
        total_nxc += n
        top_rewards.append({"user": uname, "rewards": r, "nxc": n, "role": role})
        for conn in d.get("connections", []):
            ct = conn.get("country","Inconnu") or "Inconnu"
            countries[ct] = countries.get(ct, 0) + 1
            ap = conn.get("app","Nexus") or "Nexus"
            apps[ap] = apps.get(ap, 0) + 1
            ts = conn.get("ts", 0)
            if ts:
                try:
                    day = datetime.datetime.fromtimestamp(ts/1000).strftime("%Y-%m-%d")
                    conns_by_day[day] = conns_by_day.get(day, 0) + 1
                except Exception:
                    pass
    top_rewards.sort(key=lambda x: x["rewards"], reverse=True)
    return jsonify({"ok": True, "total_users": len(users), "total_rewards": round(total_rewards,2),
        "total_nxc": round(total_nxc,4), "roles": roles,
        "countries": sorted(countries.items(), key=lambda x: x[1], reverse=True)[:15],
        "apps": sorted(apps.items(), key=lambda x: x[1], reverse=True),
        "conns_by_day": sorted(conns_by_day.items())[-60:], "top_rewards": top_rewards[:10]})

@app.route("/admin/message", methods=["POST"])
def admin_message():
    body = request.get_json(force=True, silent=True) or {}
    mk = body.get("master_key","")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    target = body.get("target","")
    text = (body.get("text") or "").strip()
    if not text:
        return jsonify({"ok": False, "error": "Message vide"})
    msg = {"id": secrets.token_hex(8), "text": text, "from": "Admin", "ts": int(time.time()*1000), "read": False}
    with _lock:
        db = load_db()
        users = db.get("users", {})
        targets = list(users.keys()) if target == "__all__" else [target]
        count = 0
        for t in targets:
            if t not in users:
                continue
            d = users[t].get("data", {})
            msgs = d.get("admin_messages", [])
            msgs.append(msg)
            if len(msgs) > 50:
                msgs = msgs[-50:]
            d["admin_messages"] = msgs
            users[t]["data"] = d
            count += 1
        db["users"] = users
        save_db(db)
    return jsonify({"ok": True, "sent_to": count})

@app.route("/admin/connections/import", methods=["POST"])
def admin_connections_import():
    body = request.get_json(force=True, silent=True) or {}
    mk = body.get("master_key","")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    target = body.get("target","")
    new_conns = body.get("connections", [])
    if not target or not isinstance(new_conns, list):
        return jsonify({"ok": False, "error": "Parametres invalides"})
    with _lock:
        db = load_db()
        if target not in db.get("users", {}):
            return jsonify({"ok": False, "error": "Utilisateur introuvable"})
        d = db["users"][target].get("data", {})
        existing = d.get("connections", [])
        existing_ts = {conn.get("ts") for conn in existing if conn.get("ts")}
        added = 0
        for conn in new_conns:
            if conn.get("ts") not in existing_ts:
                existing.append(conn)
                added += 1
        if len(existing) > 1000:
            existing = existing[-1000:]
        d["connections"] = existing
        db["users"][target]["data"] = d
        save_db(db)
    return jsonify({"ok": True, "added": added, "total": len(existing)})


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
    """Prix NXC en temps réel — tick-à-la-lecture, neutre, avec légère mean-reversion."""
    now_ms = int(time.time() * 1000)
    # Multi-worker : prendre le cours publié par le worker-moteur s'il est frais
    try:
        sh = _read_shared()
        if sh.get("price", 0) > 0 and (now_ms - int(sh.get("ts") or 0)) < 12000:
            NXC_MARKET["price"] = float(sh["price"])
            NXC_MARKET["ts"] = int(sh.get("ts") or now_ms)
            if sh.get("admin_price"):
                NXC_MARKET["admin_price"] = float(sh["admin_price"])
            if sh.get("history"):
                NXC_MARKET["history"] = sh["history"]
    except Exception:
        pass
    last_ts = NXC_MARKET.get("ts") or 0
    if last_ts <= 0:
        NXC_MARKET["ts"] = now_ms
        last_ts = now_ms
    # Tick de secours si le thread n'a pas tourné depuis 10s
    with _lock:
        last_ts = NXC_MARKET.get("ts") or 0
        if now_ms - last_ts > 10000:
            # Tick de secours — même moteur que _nxc_autotick (mean-reversion + plancher)
            volt = NXC_VOLATILITY_MULT.get("value", 1.0)
            p = float(NXC_MARKET["price"])
            ref = float(NXC_MARKET.get("admin_price") or p)
            sigma = 0.008 * volt
            noise = _rnd.gauss(0.0, 1.0) * sigma
            shock = 0.0
            if _rnd.random() < (float(NXC_SHOCK.get("per_day", 2.0)) / _NXC_TICKS_PER_DAY):
                _amp = float(NXC_SHOCK.get("amp", 0.20)) * volt
                shock = _amp if _rnd.random() < 0.5 else -_amp
            mr = ((ref - p) / p) * 0.06 if ref > 0 else 0.0
            change = noise + shock + mr
            p = p * (1 + change)
            if ref > 0:
                p = max(ref * 0.70, min(ref * 2.5, p))
            p = max(_NXC_PRICE_MIN, min(_NXC_PRICE_MAX, p))
            NXC_MARKET["price"] = round(p, 2)
            NXC_MARKET["ts"] = now_ms
            hist = NXC_MARKET.setdefault("history", [])
            hist.append({"price": NXC_MARKET["price"], "ts": now_ms, "vol": int(_rnd.random()*800+30)})
            if len(hist) > 8000:
                del hist[:-8000]
    return jsonify({
        "ok": True,
        "price": NXC_MARKET["price"],
        "ts": NXC_MARKET["ts"],
        "volume24": NXC_MARKET["volume24"],
        "trades24": NXC_MARKET["trades24"],
        "history": NXC_MARKET["history"][-4000:],
        "v": "v3-oscillation"
    })


@app.route("/nxc/tick", methods=["POST"])
def nxc_tick():
    """Mise à jour du prix NXC — requiert master_key."""
    body = request.get_json(force=True, silent=True) or {}
    mk = body.get("master_key") or ""
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify(ok=False, error="Unauthorized"), 403
    price = float(body.get("price", 0))
    if price < 1:
        return jsonify(ok=False, error="Prix invalide"), 400
    # NOTE : /nxc/tick ne modifie QUE le prix courant, jamais la référence admin.
    # La référence (admin_price) n'est changée que par /nxc/price/set (bouton Fixer).
    # Ainsi les tendances/cycles auto ne font PAS dériver le plancher -30 %.
    # set_ref=True permet à un appel explicite de redéfinir aussi la référence.
    if body.get("set_ref"):
        NXC_MARKET["admin_price"] = price
    NXC_MARKET["price"] = price
    NXC_MARKET["ts"] = body.get("ts", int(time.time() * 1000))
    NXC_MARKET["volume24"] = body.get("volume24", NXC_MARKET["volume24"])
    NXC_MARKET["trades24"] = body.get("trades24", NXC_MARKET["trades24"])
    entry = {"price": price, "ts": NXC_MARKET["ts"], "vol": body.get("vol", 100)}
    NXC_MARKET["history"].append(entry)
    if len(NXC_MARKET["history"]) > 8000:
        NXC_MARKET["history"] = NXC_MARKET["history"][-8000:]
    # Persister dans Redis pour survivre aux redémarrages Render
    try:
        with _lock:
            db = load_db()
            noah = db.get("users", {}).get("noah")
            if noah is not None:
                noah.setdefault("data", {})["nxcoin_market"] = {
                    "price": price,
                    "history": NXC_MARKET["history"][-4000:],
                    "volume24": NXC_MARKET.get("volume24", 0),
                    "trades24": NXC_MARKET.get("trades24", 0),
                    "ts": NXC_MARKET["ts"],
                    "base_price": NXC_MARKET.get("admin_price", price)
                }
                save_db(db)
    except Exception:
        pass
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
            # Champs étendus (banque 2.0) : comptes, prêts, épargne, intérêts
            def _pick(key, default):
                return incoming.get(key, current.get(key, default))
            if force_reset:
                # Reset : ecraser completement sans fusion
                new_bank = {
                    "reserves": round(float(incoming.get("reserves", 0)), 2),
                    "nxcEmis": round(float(incoming.get("nxcEmis", 0)), 4),
                    "totalIn": round(float(incoming.get("totalIn", 0)), 2),
                    "totalOut": round(float(incoming.get("totalOut", 0)), 2),
                    "flux": incoming.get("flux", []),
                    "accounts": incoming.get("accounts", {}),
                    "loans": incoming.get("loans", []),
                    "savings": incoming.get("savings", {}),
                    "lastInterestTs": incoming.get("lastInterestTs", 0)
                }
            else:
                # Mode normal : fusion anti-duplication
                all_flux = list(current.get("flux", []))
                existing_ts = {f.get("ts") for f in all_flux}
                for f in incoming.get("flux", []):
                    if f.get("ts") not in existing_ts:
                        all_flux.append(f)
                        existing_ts.add(f.get("ts"))
                all_flux = sorted(all_flux, key=lambda x: x.get("ts", 0))[-2000:]
                new_bank = {
                    "reserves": round(float(incoming.get("reserves", current.get("reserves", 0))), 2),
                    "nxcEmis": round(max(float(incoming.get("nxcEmis", 0)), float(current.get("nxcEmis", 0))), 4),
                    "totalIn": round(max(float(incoming.get("totalIn", 0)), float(current.get("totalIn", 0))), 2),
                    "totalOut": round(max(float(incoming.get("totalOut", 0)), float(current.get("totalOut", 0))), 2),
                    "flux": all_flux,
                    # Ces champs sont pilotés par le client → on prend l'entrant s'il existe
                    "accounts": _pick("accounts", {}),
                    "loans": _pick("loans", []),
                    "savings": _pick("savings", {}),
                    "lastInterestTs": _pick("lastInterestTs", 0)
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
    try:
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
    except Exception as e:
        return jsonify(ok=False, error="Erreur serveur: " + str(e)), 500


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
        # ⚠️ CORRECTIF ACHAT : avant, /sync forçait rewards = MAX(ancien, nouveau).
        # Comme un ACHAT DIMINUE les rewards, le MAX restaurait l'ancienne valeur
        # → l'argent dépensé "revenait" et le NXC acheté disparaissait.
        # Désormais /sync NE TOUCHE PLUS aux rewards : la valeur actuelle du serveur
        # fait autorité (elle est mise à jour par les achats via /admin/merge et par
        # /admin/give-rewards). /sync ne gère que nxc, forum, bank, etc.
        cur_rew = float((old_data.get("nx2098") or {}).get("rewards") or 0)
        cur_pts = float((old_data.get("rewards") or {}).get("points") or 0)
        keep_rew = cur_rew if cur_rew else cur_pts   # valeur serveur = vérité
        # Appliquer les nouvelles donnees (nxc, historique, forum, bank...)
        db["users"][u]["data"] = new_data
        # Restaurer les rewards du serveur (jamais écrasés par une valeur périmée)
        db["users"][u]["data"].setdefault("nx2098", {})["rewards"] = keep_rew
        db["users"][u]["data"].setdefault("rewards", {})["points"] = keep_rew
        db["users"][u]["updated"] = now_iso()
        save_db(db)
    return jsonify(ok=True)


@app.post("/rewards/add")
def rewards_add():
    """Ajoute/retire des points de récompense de façon ATOMIQUE (serveur = vérité).

    Utilisé par TOUTES les apps (navigateur Nexus, coin...) pour gagner/dépenser
    des points sans s'écraser mutuellement. On envoie un delta (+5 pour une
    recherche, -prix pour une récompense), le serveur calcule le nouveau total.
    Les points sont ainsi stockés au même endroit que le reste (compte serveur)
    et survivent au rafraîchissement.
    """
    d = request.get_json(force=True, silent=True) or {}
    u = (d.get("username") or "").strip()
    p = d.get("password") or ""
    try:
        delta = float(d.get("delta", 0))
    except (TypeError, ValueError):
        delta = 0.0
    with _lock:
        db = load_db()
        if not check(db, u, p):
            return jsonify(ok=False, error="identifiants invalides")
        data = db["users"][u].setdefault("data", {})
        cur = (data.get("rewards") or {}).get("points")
        if cur is None:
            cur = (data.get("nx2098") or {}).get("rewards") or 0
        new = max(0.0, round(float(cur) + delta, 2))
        data.setdefault("rewards", {})["points"] = new
        data.setdefault("nx2098", {})["rewards"] = new
        db["users"][u]["updated"] = now_iso()
        save_db(db)
    return jsonify(ok=True, points=new)


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
                if cur:
                    merged = dict(cur)
                    merged.update({k: v for k, v in u.items() if k not in ("ok", "username")})
                    for f in ("password", "salt", "pass_hash"):
                        if f in cur and f not in u:
                            merged[f] = cur[f]
                    db["users"][name] = merged
                else:
                    db["users"][name] = u
        seen = {(m["user"], m["time"], m["text"]) for m in db.get("forum", [])}
        for m in incoming.get("forum", []) or []:
            key = (m.get("user"), m.get("time"), m.get("text"))
            if key not in seen:
                db.setdefault("forum", []).append(m); seen.add(key)
        db["forum"] = sorted(db.get("forum", []), key=lambda m: m.get("time", ""))[-8000:]
        for n, e in (incoming.get("extensions", {}) or {}).items():
            cur = db.setdefault("extensions", {}).get(n)
            if not cur or e.get("added", "") > cur.get("added", ""):
                db["extensions"][n] = e
        save_db(db)
        merged = db
    return jsonify(ok=True, db=merged)

def _mean_reversion_tick():
    """Thread de mean reversion — ramène le prix vers la cible toutes les 8s."""
    while True:
        try:
            time.sleep(8)
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
            p = max(50.0, min(9999999.0, p * (1 + adj)))
            p = round(p * 100) / 100
            NXC_MARKET["price"] = p
            NXC_MARKET["ts"] = int(time.time() * 1000)
            NXC_MARKET["history"].append({"price": p, "ts": NXC_MARKET["ts"], "vol": int(_rnd.random() * 400 + 20)})
            if len(NXC_MARKET["history"]) > 8000:
                NXC_MARKET["history"] = NXC_MARKET["history"][-8000:]
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
            p = p * (1 + adj)
            # Respecter le plancher -30 % / plafond +150 % de la référence
            _ref = float(NXC_MARKET.get("admin_price") or 0)
            if _ref > 0:
                p = max(_ref * 0.70, min(_ref * 2.5, p))
            p = max(50.0, min(9999999.0, p))
            p = round(p * 100) / 100
            NXC_MARKET["price"] = p
            NXC_MARKET["ts"] = int(time.time() * 1000)
            NXC_MARKET["history"].append({"price": p, "ts": NXC_MARKET["ts"], "vol": int(_rnd.random() * 300 + 10)})
            if len(NXC_MARKET["history"]) > 8000:
                NXC_MARKET["history"] = NXC_MARKET["history"][-8000:]
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
    _push_control("bias", dict(NXC_BIAS))
    return jsonify({"ok": True, "drift": NXC_BIAS["drift"], "speed": NXC_BIAS["speed"]})


@app.route("/nxc/growth", methods=["GET", "POST"])
def nxc_growth():
    """Croissance programmée du prix (de 0,01 %/jour à 100 %/seconde, +/-).

    POST {master_key, rate_per_sec, combine, enabled}
      rate_per_sec : taux composé PAR SECONDE en fraction (0.001 = +0,1 %/s).
                     Le client convertit son %/unité de temps en /seconde.
      combine      : True = s'ajoute aux variations normales du cours.
      enabled      : active/désactive la croissance.
    """
    if request.method == "GET":
        return jsonify({"ok": True, **NXC_GROWTH})
    body = request.get_json(force=True, silent=True) or {}
    mk = (body.get("master_key") or request.args.get("master_key") or "")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    if "rate_per_sec" in body:
        try:
            r = float(body.get("rate_per_sec") or 0.0)
        except (TypeError, ValueError):
            r = 0.0
        # Borne dure : ±100 %/seconde maximum (|r| <= 1.0)
        NXC_GROWTH["rate_per_sec"] = max(-1.0, min(1.0, r))
    if "combine" in body:
        NXC_GROWTH["combine"] = bool(body["combine"])
    if "enabled" in body:
        NXC_GROWTH["enabled"] = bool(body["enabled"])
    _push_control("growth", dict(NXC_GROWTH))
    return jsonify({"ok": True, **NXC_GROWTH})


# Config des onglets (favoris + masqués) — synchronisée sur tous les appareils
_TABCFG_DEFAULT = {"favorites": [], "hidden": ["historique", "convertisseur", "evenements"]}

@app.route("/nexus/config", methods=["GET", "POST"])
def nexus_config():
    """Config du navigateur Nexus (points recherche, boutique de récompenses,
    demandes en cours). GET public (tous les utilisateurs la lisent),
    POST réservé à l'admin (clé maître)."""
    default = {
        "searchPoints": 3, "appOpenBase": 1, "newAccountBonus": 100,
        "rewards": [], "requests": []
    }
    if request.method == "GET":
        with _lock:
            db = load_db()
            cfg = db.get("nexus_config") or dict(default)
        for k, v in default.items():
            cfg.setdefault(k, v)
        return jsonify({"ok": True, "config": cfg})
    body = request.get_json(force=True, silent=True) or {}
    mk = (body.get("master_key") or "")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    incoming = body.get("config") or {}
    with _lock:
        db = load_db()
        cur = db.get("nexus_config") or dict(default)
        for k in ("searchPoints", "appOpenBase", "newAccountBonus"):
            if k in incoming:
                try:
                    cur[k] = float(incoming[k])
                except (TypeError, ValueError):
                    pass
        if "rewards" in incoming:
            cur["rewards"] = incoming["rewards"][:100]
        if "requests" in incoming:
            cur["requests"] = incoming["requests"][:300]
        db["nexus_config"] = cur
        save_db(db)
    return jsonify({"ok": True, "config": cur})


@app.post("/nexus/request")
def nexus_request():
    """Un utilisateur demande une récompense À VALIDER : on débite ses points
    (atomique) et on ajoute une demande en attente que l'admin traitera."""
    d = request.get_json(force=True, silent=True) or {}
    u = (d.get("username") or "").strip()
    p = d.get("password") or ""
    try:
        price = float(d.get("price", 0))
    except (TypeError, ValueError):
        price = 0.0
    name = (d.get("rewardName") or "")[:80]
    rid = (d.get("rewardId") or "")[:80]
    with _lock:
        db = load_db()
        if not check(db, u, p):
            return jsonify(ok=False, error="identifiants invalides")
        data = db["users"][u].setdefault("data", {})
        cur = (data.get("rewards") or {}).get("points")
        if cur is None:
            cur = (data.get("nx2098") or {}).get("rewards") or 0
        cur = float(cur)
        if cur < price:
            return jsonify(ok=False, error="Points insuffisants")
        new = round(cur - price, 2)
        data.setdefault("rewards", {})["points"] = new
        data.setdefault("nx2098", {})["rewards"] = new
        cfg = db.get("nexus_config") or {}
        reqs = cfg.get("requests") or []
        reqs.append({
            "id": secrets.token_hex(6), "user": u, "rewardId": rid,
            "rewardName": name, "price": price, "ts": int(time.time() * 1000),
            "status": "pending", "code": ""
        })
        cfg["requests"] = reqs[-300:]
        db["nexus_config"] = cfg
        db["users"][u]["updated"] = now_iso()
        save_db(db)
    return jsonify(ok=True, points=new)


@app.route("/nxc/bounds", methods=["GET", "POST"])
def nxc_bounds():
    """Plancher/plafond du prix — auto (-30 %/+150 %) ou min/max manuels."""
    if request.method == "GET":
        return jsonify({"ok": True, **NXC_BOUNDS})
    body = request.get_json(force=True, silent=True) or {}
    mk = (body.get("master_key") or request.args.get("master_key") or "")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    if "auto" in body:
        NXC_BOUNDS["auto"] = bool(body["auto"])
    if "min" in body:
        try: NXC_BOUNDS["min"] = max(0.0, float(body["min"]))
        except (TypeError, ValueError): pass
    if "max" in body:
        try: NXC_BOUNDS["max"] = max(0.0, float(body["max"]))
        except (TypeError, ValueError): pass
    _push_control("bounds", dict(NXC_BOUNDS))
    return jsonify({"ok": True, **NXC_BOUNDS})


@app.route("/nxc/extremes", methods=["GET", "POST"])
def nxc_extremes():
    """Fréquence des extrêmes : probabilité par tick de toucher le min/max."""
    if request.method == "GET":
        return jsonify({"ok": True, **NXC_EXTREMES})
    body = request.get_json(force=True, silent=True) or {}
    mk = (body.get("master_key") or request.args.get("master_key") or "")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    for key in ("pmin", "pmax"):
        if key in body:
            try:
                NXC_EXTREMES[key] = max(0.0, min(1.0, float(body[key])))
            except (TypeError, ValueError):
                pass
    _push_control("extremes", dict(NXC_EXTREMES))
    return jsonify({"ok": True, **NXC_EXTREMES})


@app.route("/nxc/tabconfig", methods=["GET", "POST"])
def nxc_tabconfig():
    """Favoris et onglets masqués — stockés dans la DB → identiques sur tous les appareils."""
    if request.method == "GET":
        with _lock:
            db = load_db()
            cfg = db.get("nxc_tabconfig") or dict(_TABCFG_DEFAULT)
        return jsonify({"ok": True, "config": cfg})
    body = request.get_json(force=True, silent=True) or {}
    mk = (body.get("master_key") or request.args.get("master_key") or "")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    cfg = body.get("config") or {}
    fav = [str(x) for x in (cfg.get("favorites") or [])][:40]
    hid = [str(x) for x in (cfg.get("hidden") or [])][:40]
    with _lock:
        db = load_db()
        db["nxc_tabconfig"] = {"favorites": fav, "hidden": hid}
        save_db(db)
    return jsonify({"ok": True, "config": db["nxc_tabconfig"]})


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
    _push_control("volatility", dict(NXC_VOLATILITY_MULT))
    return jsonify({"ok": True, "value": NXC_VOLATILITY_MULT["value"]})


@app.route("/nxc/price/set", methods=["POST"])
def nxc_price_set():
    """Force le prix NXC a une valeur precise — devient le nouveau prix de référence.
    Le marché fluctuera ensuite autour de ce prix.
    Le nouveau prix est immédiatement persisté dans la DB pour survivre aux redémarrages."""
    body = request.get_json(force=True, silent=True) or {}
    mk = (body.get("master_key") or "")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    price = float(body.get("price", 0))
    if price < 50 or price > 999999:
        return jsonify({"ok": False, "error": "Prix hors limites (50–999999)"}), 400
    now_ms = int(time.time() * 1000)
    p = round(price, 2)
    NXC_MARKET["price"] = p
    NXC_MARKET["admin_price"] = p  # référence pour le plancher -30%
    NXC_MARKET["ts"]    = now_ms
    _push_price_ref(p, p)
    NXC_MARKET["history"].append({"price": p, "ts": now_ms, "vol": 0, "event": "force_set"})
    if len(NXC_MARKET["history"]) > 8000:
        NXC_MARKET["history"] = NXC_MARKET["history"][-8000:]
    # Persister immédiatement dans la DB (sinon perdu au prochain redémarrage)
    try:
        with _lock:
            db = load_db()
            noah = db.get("users", {}).get("noah")
            if noah is not None:
                noah.setdefault("data", {})["nxcoin_market"] = {
                    "price": p,
                    "history": NXC_MARKET["history"][-4000:],
                    "volume24": NXC_MARKET.get("volume24", 0),
                    "trades24": NXC_MARKET.get("trades24", 0),
                    "ts": now_ms,
                    "base_price": p   # prix de référence mémorisé
                }
                save_db(db)
    except Exception as _e:
        pass  # Ne pas bloquer la réponse si la DB est indisponible
    return jsonify({"ok": True, "price": p, "message": "Prix défini et sauvegardé"})


@app.route("/nxc/dashboard", methods=["GET"])
def nxc_dashboard():
    """Stats completes pour le dashboard temps reel."""
    hist = NXC_MARKET.get("history", [])
    prices = [float(h.get("price", 0)) for h in hist if h.get("price")]
    p_now  = float(NXC_MARKET.get("price", 0))
    p_24h  = prices[-576] if len(prices) >= 576 else (prices[0] if prices else p_now)
    hi_24  = max(prices[-8000:]) if prices else p_now
    lo_24  = min(prices[-8000:]) if prices else p_now
    vol_24 = sum(float(h.get("vol", 0)) for h in hist[-8000:])
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




# ══════════════════════════════════════════════
# NOUVELLES FONCTIONNALITÉS NXC — EXTENSION v2
# ══════════════════════════════════════════════

import datetime as _dt

# ── Statistiques globales de la plateforme ──
@app.post("/nxc/stats")
def nxc_stats():
    with _lock:
        db = load_db()
        users = db.get("users", {})
        total_users = len(users)
        total_nxc = sum(float((u.get("nx2098") or {}).get("balance", 0)) for u in users.values())
        total_rewards = sum(float((u.get("rewards") or {}).get("points", 0)) for u in users.values())
        active = sum(1 for u in users.values() if u.get("updated", "") > (_dt.datetime.utcnow() - _dt.timedelta(days=7)).isoformat())
    return jsonify(ok=True, stats={
        "total_users": total_users,
        "total_nxc_supply": round(total_nxc, 4),
        "total_rewards": round(total_rewards, 2),
        "active_users_7d": active,
        "timestamp": _dt.datetime.utcnow().isoformat()
    })


# ── Classement des portefeuilles ──
@app.post("/nxc/leaderboard")
def nxc_leaderboard():
    d = request.get_json(force=True, silent=True) or {}
    limit = min(int(d.get("limit", 10)), 50)
    with _lock:
        db = load_db()
        users = db.get("users", {})
        ranked = []
        for name, u in users.items():
            bal = float((u.get("nx2098") or {}).get("balance", 0))
            pts = float((u.get("rewards") or {}).get("points", 0))
            ranked.append({"user": name, "balance": round(bal, 4), "rewards": round(pts, 2)})
        ranked.sort(key=lambda x: x["balance"], reverse=True)
    return jsonify(ok=True, leaderboard=ranked[:limit])


# ── Ping / healthcheck étendu ──
@app.get("/nxc/ping")
def nxc_ping():
    with _lock:
        db = load_db()
        n = len(db.get("users", {}))
    return jsonify(ok=True, users=n, ts=_dt.datetime.utcnow().isoformat(), version="2.0")


# ── Historique de prix (export JSON) ──
@app.get("/nxc/history")
def nxc_history_export():
    with _lock:
        hist = list(_price_hist)
    return jsonify(ok=True, history=hist, count=len(hist))


# ── Transfert NXC entre utilisateurs ──
@app.post("/nxc/transfer")
def nxc_transfer():
    d = request.get_json(force=True, silent=True) or {}
    src = (d.get("from") or "").strip()
    dst = (d.get("to") or "").strip()
    amount = float(d.get("amount", 0))
    pw = d.get("password", "")
    if not src or not dst or amount <= 0:
        return jsonify(ok=False, error="Paramètres invalides")
    with _lock:
        db = load_db()
        if src not in db["users"]:
            return jsonify(ok=False, error="Expéditeur inconnu")
        if not verify_pw(db, src, pw):
            return jsonify(ok=False, error="Mot de passe incorrect")
        if dst not in db["users"]:
            return jsonify(ok=False, error="Destinataire inconnu")
        src_bal = float((db["users"][src].get("nx2098") or {}).get("balance", 0))
        fee = round(amount * NXC_FEES.get("transfer", 0.005), 6)
        total = amount + fee
        if src_bal < total:
            return jsonify(ok=False, error=f"Solde insuffisant (besoin {total:.4f} NXC)")
        db["users"][src].setdefault("nx2098", {})["balance"] = round(src_bal - total, 6)
        dst_bal = float((db["users"][dst].get("nx2098") or {}).get("balance", 0))
        db["users"][dst].setdefault("nx2098", {})["balance"] = round(dst_bal + amount, 6)
        db["users"][src]["updated"] = _dt.datetime.utcnow().isoformat()
        db["users"][dst]["updated"] = _dt.datetime.utcnow().isoformat()
        save_db(db)
    return jsonify(ok=True, transferred=amount, fee=fee, from_user=src, to_user=dst)


# ── Récompenses: convertir points en NXC ──
@app.post("/nxc/rewards/redeem")
def rewards_redeem():
    d = request.get_json(force=True, silent=True) or {}
    user = (d.get("user") or "").strip()
    pts = float(d.get("points", 0))
    pw = d.get("password", "")
    rate = 0.01  # 1 point = 0.01 NXC
    if not user or pts <= 0:
        return jsonify(ok=False, error="Paramètres invalides")
    with _lock:
        db = load_db()
        if user not in db["users"]:
            return jsonify(ok=False, error="Utilisateur inconnu")
        if not verify_pw(db, user, pw):
            return jsonify(ok=False, error="Mot de passe incorrect")
        cur_pts = float((db["users"][user].get("rewards") or {}).get("points", 0))
        if cur_pts < pts:
            return jsonify(ok=False, error="Points insuffisants")
        nxc_earned = round(pts * rate, 6)
        db["users"][user].setdefault("rewards", {})["points"] = round(cur_pts - pts, 2)
        cur_bal = float((db["users"][user].get("nx2098") or {}).get("balance", 0))
        db["users"][user].setdefault("nx2098", {})["balance"] = round(cur_bal + nxc_earned, 6)
        db["users"][user]["updated"] = _dt.datetime.utcnow().isoformat()
        save_db(db)
    return jsonify(ok=True, points_spent=pts, nxc_earned=nxc_earned, user=user)


# ── Admin: réinitialiser le mot de passe d'un utilisateur ──
@app.post("/admin/reset_pw")
def admin_reset_pw():
    d = request.get_json(force=True, silent=True) or {}
    with _lock:
        db = load_db()
        if not admin_ok(d, db):
            return jsonify(ok=False, error="accès refusé")
        user = (d.get("user") or "").strip()
        new_pw = (d.get("new_password") or "").strip()
        if not user or not new_pw or len(new_pw) < 4:
            return jsonify(ok=False, error="user et new_password requis (min 4 car)")
        if user not in db["users"]:
            return jsonify(ok=False, error="Utilisateur introuvable")
        salt = os.urandom(16).hex()
        db["users"][user]["salt"] = salt
        db["users"][user]["pass_hash"] = hash_pw(new_pw, salt)
        db["users"][user]["updated"] = _dt.datetime.utcnow().isoformat()
        save_db(db)
    return jsonify(ok=True, user=user, msg="Mot de passe réinitialisé")


# ── Admin: message de broadcast (stocké en DB) ──
@app.post("/admin/broadcast")
def admin_broadcast():
    d = request.get_json(force=True, silent=True) or {}
    with _lock:
        db = load_db()
        if not admin_ok(d, db):
            return jsonify(ok=False, error="accès refusé")
        msg = (d.get("message") or "").strip()
        if not msg:
            return jsonify(ok=False, error="message vide")
        db.setdefault("broadcasts", []).append({
            "text": msg,
            "ts": _dt.datetime.utcnow().isoformat(),
            "by": "admin"
        })
        save_db(db)
    return jsonify(ok=True, msg="Broadcast enregistré")


# ── Lecture du dernier broadcast (public) ──
@app.get("/nxc/broadcast")
def get_broadcast():
    with _lock:
        db = load_db()
        msgs = db.get("broadcasts", [])
        last = msgs[-1] if msgs else None
    return jsonify(ok=True, broadcast=last)




@app.route("/", methods=["GET"])
def index():
    return Response(STORE_HTML, mimetype="text/html")

@app.before_request
def _start_ping_on_first_request():
    _ensure_ping()

@app.route("/ping", methods=["GET"])
@app.route("/nxc/ping_ext", methods=["GET"])
def ping_health():
    return jsonify(ok=True, ts=int(time.time()*1000)), 200



# ══════════════════════════════════════════════════════════════════════════════
# DOCUMENTATION INTERNE — ARCHITECTURE NEXUS SERVER
# ══════════════════════════════════════════════════════════════════════════════
#
# FLUX DE DONNÉES :
#   Client HTML  ──►  Flask Routes  ──►  DB JSON (nexus_db.json)
#                          │
#                     NXC_MARKET (dict en mémoire)
#                          │
#                    _nxc_autotick() (thread daemon, toutes les 15s)
#
# BASE DE DONNÉES :
#   Format : JSON sur disque  →  /data/nexus_db.json  (Render disk)
#   Structure :
#     {
#       "users": {
#         "noah": {
#           "pass_hash": "...", "salt": "...",
#           "rewards": 0, "nxc": 0,
#           "data": { "nxcoin_market": { "price": ..., "history": [...] } }
#         }
#       }
#     }
#
# PRIX NXC :
#   - NXC_MARKET["price"]  →  prix courant en mémoire
#   - /nxc/price/set       →  force un nouveau prix ET le persiste en DB
#   - _load_nxc_from_db()  →  restaure le dernier prix au démarrage
#   - _nxc_autotick()      →  fait fluctuer le prix toutes les 15s
#                             et persiste en DB toutes les ~2 min (8 ticks)
#
# KEEP-ALIVE :
#   - UptimeRobot ping /ping toutes les 5 min
#   - Le serveur répond {"ok": true, "ts": <timestamp>}
#   - Empêche Render free tier de mettre le serveur en veille
#
# SÉCURITÉ :
#   - MASTER_KEY : variable d'env NEXUS_MASTER_KEY sur Render
#   - Passwords : PBKDF2-HMAC-SHA256 avec salt aléatoire 32 bytes
#   - Rate limiting : _RATE_MAX = 200 req/min par IP
#   - CORS : Access-Control-Allow-Origin: * (Cloudflare Pages → Render)
#
# ROUTES PRINCIPALES :
#   POST /login               →  authentification utilisateur
#   GET  /nxc/price           →  prix actuel + historique
#   POST /nxc/price/set       →  forcer un nouveau prix (admin)
#   POST /nxc/bank            →  acheter / vendre du NXC
#   GET  /ping                →  health check UptimeRobot
#   GET  /                    →  index / health check
#   POST /admin/merge         →  modifier un compte utilisateur
#   GET  /admin/get           →  lire un compte utilisateur
#
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/nxc/market/status", methods=["GET"])
def nxc_market_status():
    """Retourne un résumé complet de l'état du marché NXC.
    Utile pour déboguer ou afficher un dashboard rapide."""
    hist = NXC_MARKET.get("history", [])
    prices = [float(h.get("price", 0)) for h in hist if h.get("price")]
    p_now = float(NXC_MARKET.get("price", 0))
    p_open = prices[0] if prices else p_now
    p_hi = max(prices) if prices else p_now
    p_lo = min(prices) if prices else p_now
    chg = round(((p_now - p_open) / max(p_open, 1)) * 100, 2) if p_open else 0
    return jsonify({
        "ok": True,
        "price": p_now,
        "open": p_open,
        "high": p_hi,
        "low": p_lo,
        "change_pct": chg,
        "ticks": len(hist),
        "ts": NXC_MARKET.get("ts", 0),
        "db_path": DB_FILE
    })


if __name__ == "__main__":
    _load_nxc_from_db()
    print("=" * 54)
    print("  NEXUS SERVER (en ligne)  —  http://127.0.0.1:%d" % PORT)
    print("  Clé maître :", MASTER_KEY)
    print("  Prix NXC restauré : %.2f R" % NXC_MARKET["price"])
    print("=" * 54)
    app.run(host="0.0.0.0", port=PORT)

# ══════════════════════════════════════════════════════════════════════════════
# ROUTES UTILITAIRES SUPPLÉMENTAIRES — statistiques avancées, audit, debug
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/nxc/stats/volatility", methods=["GET"])
def nxc_stats_volatility():
    """Volatilité réalisée sur N derniers ticks (annualisée et brute)."""
    import math
    n = min(int(request.args.get("n", 20)), 576)
    hist = NXC_MARKET.get("history", [])
    prices = [float(h["price"]) for h in hist[-n+1:] if h.get("price")]
    if len(prices) < 2:
        return jsonify({"ok": True, "vol_brute": 0, "vol_annualisee": 0, "n": 0})
    rets = [math.log(prices[i] / prices[i-1]) for i in range(1, len(prices)) if prices[i-1] > 0]
    if not rets:
        return jsonify({"ok": True, "vol_brute": 0, "vol_annualisee": 0, "n": 0})
    mu = sum(rets) / len(rets)
    var = sum((r - mu) ** 2 for r in rets) / len(rets)
    vol = math.sqrt(var)
    ticks_per_year = 365 * 24 * 3600 / 8  # tick toutes les 8s
    vol_ann = vol * math.sqrt(ticks_per_year)
    return jsonify({
        "ok": True,
        "vol_brute": round(vol * 100, 4),
        "vol_annualisee": round(vol_ann * 100, 2),
        "n_ticks": len(rets),
        "drift_moyen": round(mu * 100, 5),
        "prix_actuel": NXC_MARKET["price"]
    })


@app.route("/nxc/stats/drawdown", methods=["GET"])
def nxc_stats_drawdown():
    """Max drawdown et max runup sur l'historique disponible."""
    hist = NXC_MARKET.get("history", [])
    prices = [float(h["price"]) for h in hist if h.get("price")]
    if len(prices) < 2:
        return jsonify({"ok": True, "max_drawdown_pct": 0, "max_runup_pct": 0})
    peak = prices[0]
    trough = prices[0]
    max_dd = 0.0
    max_ru = 0.0
    running_low = prices[0]
    for p in prices[1:]:
        if p > peak:
            peak = p
        dd = (peak - p) / peak * 100
        if dd > max_dd:
            max_dd = dd
        if p < running_low:
            running_low = p
        ru = (p - running_low) / max(running_low, 1) * 100
        if ru > max_ru:
            max_ru = ru
    return jsonify({
        "ok": True,
        "max_drawdown_pct": round(max_dd, 2),
        "max_runup_pct": round(max_ru, 2),
        "prix_min": round(min(prices), 2),
        "prix_max": round(max(prices), 2),
        "prix_actuel": NXC_MARKET["price"],
        "n_ticks": len(prices)
    })


@app.route("/nxc/stats/trend", methods=["GET"])
def nxc_stats_trend():
    """Tendance récente : régression linéaire sur les N derniers prix."""
    import math
    n = min(int(request.args.get("n", 30)), 576)
    hist = NXC_MARKET.get("history", [])
    prices = [float(h["price"]) for h in hist[-n:] if h.get("price")]
    if len(prices) < 3:
        return jsonify({"ok": True, "slope_pct_per_tick": 0, "r2": 0, "trend": "neutre"})
    xs = list(range(len(prices)))
    n2 = len(xs)
    mx = sum(xs) / n2
    my = sum(prices) / n2
    num = sum((xs[i]-mx)*(prices[i]-my) for i in range(n2))
    den = sum((x-mx)**2 for x in xs)
    slope = num / den if den else 0
    slope_pct = slope / max(prices[0], 1) * 100
    # R²
    ss_res = sum((prices[i] - (my + slope*(xs[i]-mx)))**2 for i in range(n2))
    ss_tot = sum((p - my)**2 for p in prices)
    r2 = 1 - ss_res/ss_tot if ss_tot else 0
    trend = "hausse" if slope_pct > 0.05 else "baisse" if slope_pct < -0.05 else "neutre"
    return jsonify({
        "ok": True,
        "slope_pct_per_tick": round(slope_pct, 4),
        "r2": round(r2, 4),
        "trend": trend,
        "prix_debut": round(prices[0], 2),
        "prix_fin": round(prices[-1], 2),
        "variation_pct": round((prices[-1]-prices[0])/max(prices[0],1)*100, 2)
    })


@app.route("/nxc/ping", methods=["GET"])
def nxc_ping_ext():
    """Health-check rapide du serveur NXC."""
    import time as _t
    return jsonify({
        "ok": True,
        "status": "en_ligne",
        "prix": NXC_MARKET["price"],
        "frozen": NXC_FROZEN.get("active", False),
        "mr_enabled": NXC_MEAN_PRICE.get("enabled", False),
        "mr_target": NXC_MEAN_PRICE.get("target", 0),
        "bias_drift": NXC_BIAS.get("drift", 0),
        "volatility_mult": NXC_VOLATILITY_MULT.get("value", 1.0),
        "hist_len": len(NXC_MARKET.get("history", [])),
        "server_time": int(_t.time() * 1000)
    })


# ══════════════════════════════════════════════════════════════════════════════
# ROUTES ADMIN SUPPLÉMENTAIRES v3
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/nxc/market/info", methods=["GET"])
def nxc_market_info():
    """Info complètes sur le marché + paramètres actifs."""
    import math as _m
    hist = NXC_MARKET.get("history", [])
    prices = [float(h["price"]) for h in hist if h.get("price")]
    p = float(NXC_MARKET["price"])
    tick_idx = len(hist)
    cycle_period = 150
    cycle_phase = (tick_idx % cycle_period) / cycle_period * 100
    cycle_dir = "montée" if _m.sin(2*_m.pi*tick_idx/cycle_period) > 0 else "descente"
    return jsonify({
        "ok": True,
        "price": p,
        "tick_index": tick_idx,
        "cycle_phase_pct": round(cycle_phase, 1),
        "cycle_direction": cycle_dir,
        "mr_enabled": NXC_MEAN_PRICE.get("enabled"),
        "mr_target": NXC_MEAN_PRICE.get("target"),
        "bias_drift": NXC_BIAS.get("drift"),
        "volatility_mult": NXC_VOLATILITY_MULT.get("value"),
        "frozen": NXC_FROZEN.get("active"),
        "prix_min_hist": round(min(prices), 2) if prices else p,
        "prix_max_hist": round(max(prices), 2) if prices else p,
        "nb_ticks": tick_idx,
        "version": "v3-oscillation"
    })


@app.route("/nxc/market/reset-history", methods=["POST"])
def nxc_market_reset_history():
    """Réinitialise l'historique des prix (garde le prix actuel)."""
    body = request.get_json(force=True, silent=True) or {}
    mk = body.get("master_key", "")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    p = NXC_MARKET["price"]
    ts = int(time.time() * 1000)
    NXC_MARKET["history"] = [{"price": p, "ts": ts, "vol": 0}]
    NXC_MARKET["ts"] = ts
    NXC_MARKET["volume24"] = 0
    NXC_MARKET["trades24"] = 0
    return jsonify({"ok": True, "price": p, "message": "Historique réinitialisé"})


@app.route("/nxc/market/simulate", methods=["POST"])
def nxc_market_simulate():
    """Simule N ticks et retourne la trajectoire (sans modifier le vrai prix)."""
    body = request.get_json(force=True, silent=True) or {}
    mk = body.get("master_key", "")
    if not mk or not secrets.compare_digest(mk, MASTER_KEY):
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    import math as _m, random as _r
    n = min(int(body.get("n", 100)), 500)
    p0 = float(body.get("price", NXC_MARKET["price"]))
    target = float(body.get("target", NXC_MEAN_PRICE.get("target", p0)))
    drift = float(body.get("drift", 0))
    tick0 = len(NXC_MARKET["history"])
    p = p0
    trajectory = [round(p, 2)]
    for i in range(n):
        sigma = 0.008 + _r.random() * 0.012
        noise = (_r.random() - 0.50) * sigma
        cycle = 0.006 * _m.sin(2 * _m.pi * (tick0 + i) / 150)
        mr = (target - p) / max(p, 1) * 0.002
        adj = noise + cycle + mr + drift * 0.025
        p = max(50.0, min(9999999.0, p * (1 + adj)))
        trajectory.append(round(p, 2))
    return jsonify({
        "ok": True,
        "n": n,
        "prix_depart": p0,
        "prix_final": trajectory[-1],
        "prix_min": round(min(trajectory), 2),
        "prix_max": round(max(trajectory), 2),
        "variation_pct": round((trajectory[-1] - p0) / max(p0, 1) * 100, 2),
        "trajectory": trajectory
    })


@app.route("/nxc/config/summary", methods=["GET"])
def nxc_config_summary():
    """Résumé de toute la configuration NXC active."""
    return jsonify({
        "ok": True,
        "marche": {
            "prix": NXC_MARKET["price"],
            "historique_ticks": len(NXC_MARKET.get("history", [])),
            "volume24": NXC_MARKET.get("volume24", 0),
            "gel_actif": NXC_FROZEN.get("active", False)
        },
        "dynamique": {
            "biais_drift": NXC_BIAS.get("drift", 0),
            "biais_speed": NXC_BIAS.get("speed", 1),
            "mr_actif": NXC_MEAN_PRICE.get("enabled", False),
            "mr_cible": NXC_MEAN_PRICE.get("target", 5000),
            "volatilite_mult": NXC_VOLATILITY_MULT.get("value", 1),
            "formule": "noise(±1.8-4%) + sinus(±1.8%) + mr(0.2%) + drift"
        },
        "frais": NXC_FEES,
        "version": "v3-oscillation"
    })


# ════════════════════════════════════════════════════════════════════════════════
#  NEXUS EXCHANGE  —  Marché boursier simulé multijoueur
#  Portfolios · Order Book · Matching Engine · Bots · Events · Classement · Badges
# ════════════════════════════════════════════════════════════════════════════════

import math as _exmath
import uuid as _uuid

# ── Données Exchange (en mémoire) ─────────────────────────────────────────────
NXC_EX = {
    "portfolios": {},   # username -> {nxd, nxc, open_orders, trades, badges, joined}
    "orders":     [],   # limit orders actifs: {id, user, side, price, qty, ts}
    "trades":     [],   # trades exécutés (max 500)
    "events":     [],   # événements de marché (max 50)
    "candles":    [],   # bougies OHLC (max 200)
}
_EX_LOCK = threading.Lock()

# Frais de transaction (0.5%)
_EX_FEE = 0.005

# ── Bots traders ──────────────────────────────────────────────────────────────
_EX_BOTS = [
    {"name": "🤖 AlphaBot",  "nxd": 80000.0, "nxc": 500.0,  "style": "aggressive"},
    {"name": "🛡️ SafeBot",   "nxd": 60000.0, "nxc": 300.0,  "style": "conservative"},
    {"name": "📈 SwingBot",  "nxd": 50000.0, "nxc": 200.0,  "style": "swing"},
    {"name": "🎲 ChaosBot",  "nxd": 40000.0, "nxc": 150.0,  "style": "chaos"},
]

def _ex_save():
    """Persiste l'exchange dans la DB."""
    try:
        db = load_db()
        db.setdefault("exchange", {})
        db["exchange"]["portfolios"] = NXC_EX["portfolios"]
        db["exchange"]["trades"]     = NXC_EX["trades"][-200:]
        db["exchange"]["events"]     = NXC_EX["events"][-50:]
        save_db(db)
    except Exception as e:
        print(f"[EX] save error: {e}")

def _ex_load():
    """Charge l'exchange depuis la DB."""
    try:
        db = load_db()
        ex = db.get("exchange", {})
        if ex.get("portfolios"):
            NXC_EX["portfolios"] = ex["portfolios"]
        if ex.get("trades"):
            NXC_EX["trades"] = ex["trades"]
        if ex.get("events"):
            NXC_EX["events"] = ex["events"]
        # Initialise les bots dans les portfolios
        for bot in _EX_BOTS:
            n = bot["name"]
            if n not in NXC_EX["portfolios"]:
                NXC_EX["portfolios"][n] = {
                    "nxd": bot["nxd"], "nxc": bot["nxc"],
                    "open_orders": [], "trades": [], "badges": [], "joined": now_iso(), "is_bot": True
                }
    except Exception as e:
        print(f"[EX] load error: {e}")

def _check_badges(username):
    """Attribue des badges selon les performances."""
    p = NXC_EX["portfolios"].get(username)
    if not p: return
    badges = set(p.get("badges", []))
    trades = p.get("trades", [])
    nxc_price = NXC_MARKET.get("price", 5000)
    total_val = p["nxd"] + p["nxc"] * nxc_price
    if len(trades) >= 1:    badges.add("🏆 Premier Trade")
    if len(trades) >= 10:   badges.add("📊 Trader Actif")
    if len(trades) >= 50:   badges.add("⚡ Trader Pro")
    if len(trades) >= 100:  badges.add("🔥 Trader Légendaire")
    if total_val >= 20000:  badges.add("💰 Riche")
    if total_val >= 50000:  badges.add("💎 Millionnaire NXD")
    if total_val >= 100000: badges.add("👑 Nexus King")
    if p["nxc"] >= 1000:    badges.add("🪙 Whale NXC")
    if p["nxc"] == 0 and len(trades) > 0: badges.add("💸 Tout vendu")
    p["badges"] = list(badges)

def _execute_trade(buyer, seller, price, qty, source="limit"):
    """Exécute un trade entre buyer et seller."""
    cost = price * qty
    fee_buyer  = cost * _EX_FEE
    fee_seller = cost * _EX_FEE
    bp = NXC_EX["portfolios"].get(buyer)
    sp = NXC_EX["portfolios"].get(seller)
    if not bp or not sp: return False
    if bp["nxd"] < cost + fee_buyer: return False
    if sp["nxc"] < qty: return False
    bp["nxd"] -= cost + fee_buyer
    bp["nxc"] += qty
    sp["nxd"] += cost - fee_seller
    sp["nxc"] -= qty
    ts = int(time.time() * 1000)
    trade = {"id": str(_uuid.uuid4())[:8], "buyer": buyer, "seller": seller,
             "price": round(price, 4), "qty": round(qty, 6),
             "total": round(cost, 2), "ts": ts, "source": source}
    NXC_EX["trades"].append(trade)
    if len(NXC_EX["trades"]) > 500: NXC_EX["trades"] = NXC_EX["trades"][-8000:]
    bp.setdefault("trades", []).append(trade)
    sp.setdefault("trades", []).append(trade)
    _check_badges(buyer)
    _check_badges(seller)
    return True

def _match_orders():
    """Matching engine : apparie les ordres d'achat et de vente."""
    buys  = sorted([o for o in NXC_EX["orders"] if o["side"] == "buy"],
                   key=lambda x: -x["price"])
    sells = sorted([o for o in NXC_EX["orders"] if o["side"] == "sell"],
                   key=lambda x: x["price"])
    matched_ids = set()
    for buy in buys:
        for sell in sells:
            if sell["id"] in matched_ids or buy["id"] in matched_ids: continue
            if buy["user"] == sell["user"]: continue
            if buy["price"] >= sell["price"]:
                exec_price = (buy["price"] + sell["price"]) / 2
                exec_qty   = min(buy["qty"], sell["qty"])
                if _execute_trade(buy["user"], sell["user"], exec_price, exec_qty, "limit"):
                    buy["qty"]  -= exec_qty
                    sell["qty"] -= exec_qty
                    if buy["qty"]  <= 0.0001: matched_ids.add(buy["id"])
                    if sell["qty"] <= 0.0001: matched_ids.add(sell["id"])
    NXC_EX["orders"] = [o for o in NXC_EX["orders"] if o["id"] not in matched_ids and o["qty"] > 0.0001]

def _update_candles():
    """Met à jour les bougies OHLC toutes les 30s."""
    p = NXC_MARKET.get("price", 0)
    ts = int(time.time())
    period = 30
    bucket = (ts // period) * period * 1000
    candles = NXC_EX["candles"]
    if candles and candles[-1]["t"] == bucket:
        c = candles[-1]
        c["h"] = max(c["h"], p)
        c["l"] = min(c["l"], p)
        c["c"] = p
        c["v"] = c.get("v", 0) + _rnd.random() * 200
    else:
        prev_close = candles[-1]["c"] if candles else p
        candles.append({"t": bucket, "o": prev_close, "h": max(prev_close, p),
                         "l": min(prev_close, p), "c": p, "v": _rnd.random() * 500 + 50})
        if len(candles) > 200: NXC_EX["candles"] = candles[-200:]

def _bot_tick():
    """Bots placent des ordres basés sur le marché."""
    while True:
        try:
            time.sleep(_rnd.uniform(8, 20))
            with _EX_LOCK:
                price = NXC_MARKET.get("price", 5000)
                _update_candles()
                for bot in _EX_BOTS:
                    bp = NXC_EX["portfolios"].get(bot["name"])
                    if not bp: continue
                    style = bot["style"]
                    spread = 0.02 if style == "aggressive" else 0.005 if style == "conservative" else 0.01
                    action = _rnd.choice(["buy", "sell", "none", "none"])
                    if style == "chaos": action = _rnd.choice(["buy", "sell"])
                    if action == "buy" and bp["nxd"] > 500:
                        bid = round(price * (1 - spread * _rnd.random()), 2)
                        qty = round(_rnd.uniform(0.1, min(5.0, bp["nxd"] / max(bid, 1))), 4)
                        if qty > 0 and bid > 0:
                            order = {"id": str(_uuid.uuid4())[:8], "user": bot["name"],
                                     "side": "buy", "price": bid, "qty": qty,
                                     "ts": int(time.time() * 1000)}
                            NXC_EX["orders"].append(order)
                    elif action == "sell" and bp["nxc"] > 0.1:
                        ask = round(price * (1 + spread * _rnd.random()), 2)
                        qty = round(_rnd.uniform(0.05, min(2.0, bp["nxc"])), 4)
                        if qty > 0:
                            order = {"id": str(_uuid.uuid4())[:8], "user": bot["name"],
                                     "side": "sell", "price": ask, "qty": qty,
                                     "ts": int(time.time() * 1000)}
                            NXC_EX["orders"].append(order)
                    # Nettoyage vieux ordres bots (>2min)
                    now_ms = int(time.time() * 1000)
                    NXC_EX["orders"] = [o for o in NXC_EX["orders"]
                                        if not (o["user"] == bot["name"] and now_ms - o["ts"] > 120000)]
                # Market orders auto entre bots (exécution directe)
                for bot in _EX_BOTS:
                    bp = NXC_EX["portfolios"].get(bot["name"])
                    if not bp: continue
                    if _rnd.random() < 0.3:
                        sellers = [b for b in _EX_BOTS if b["name"] != bot["name"] and
                                   NXC_EX["portfolios"].get(b["name"], {}).get("nxc", 0) > 0.5]
                        if sellers and bp["nxd"] > 200:
                            seller = _rnd.choice(sellers)
                            sp = NXC_EX["portfolios"][seller["name"]]
                            exec_price = round(price * (1 + (_rnd.random() - 0.5) * 0.01), 2)
                            qty = round(_rnd.uniform(0.05, min(1.0, sp["nxc"], bp["nxd"] / max(exec_price, 1))), 4)
                            if qty > 0.01:
                                _execute_trade(bot["name"], seller["name"], exec_price, qty, "market-bot")
                _match_orders()
                if _rnd.random() < 0.1:
                    _ex_save()
        except Exception as e:
            print(f"[BOT] error: {e}")

threading.Thread(target=_bot_tick, daemon=True).start()
_ex_load()

# ── Routes Exchange ────────────────────────────────────────────────────────────

@app.route("/exchange/state", methods=["GET"])
def exchange_state():
    """État complet du marché : prix, order book, classement, candles, events."""
    with _EX_LOCK:
        price = NXC_MARKET.get("price", 5000)
        buys  = sorted([o for o in NXC_EX["orders"] if o["side"] == "buy"],
                       key=lambda x: -x["price"])[:20]
        sells = sorted([o for o in NXC_EX["orders"] if o["side"] == "sell"],
                       key=lambda x: x["price"])[:20]
        # Classement
        leaderboard = []
        for name, p in NXC_EX["portfolios"].items():
            total = round(p["nxd"] + p["nxc"] * price, 2)
            leaderboard.append({"name": name, "total": total, "nxd": round(p["nxd"], 2),
                                 "nxc": round(p["nxc"], 6), "badges": p.get("badges", []),
                                 "is_bot": p.get("is_bot", False), "trades": len(p.get("trades", []))})
        leaderboard.sort(key=lambda x: -x["total"])
        return jsonify({
            "ok": True,
            "price": price,
            "bids": [{"price": o["price"], "qty": round(o["qty"], 4), "user": o["user"]} for o in buys],
            "asks": [{"price": o["price"], "qty": round(o["qty"], 4), "user": o["user"]} for o in sells],
            "trades": NXC_EX["trades"][-30:][::-1],
            "candles": NXC_EX["candles"][-100:],
            "events":  NXC_EX["events"][-10:][::-1],
            "leaderboard": leaderboard[:20],
            "total_users": len(NXC_EX["portfolios"]),
        })

@app.route("/exchange/portfolio/<username>", methods=["GET"])
def exchange_portfolio(username):
    """Portfolio d'un utilisateur."""
    with _EX_LOCK:
        p = NXC_EX["portfolios"].get(username)
        if not p:
            return jsonify({"ok": False, "error": "Compte non trouvé dans l'exchange"}), 404
        price = NXC_MARKET.get("price", 5000)
        my_orders = [o for o in NXC_EX["orders"] if o["user"] == username]
        return jsonify({
            "ok": True,
            "username": username,
            "nxd": round(p["nxd"], 2),
            "nxc": round(p["nxc"], 6),
            "total_value": round(p["nxd"] + p["nxc"] * price, 2),
            "open_orders": my_orders,
            "trades": p.get("trades", [])[-50:][::-1],
            "badges": p.get("badges", []),
            "joined": p.get("joined", ""),
        })

@app.route("/exchange/join", methods=["POST"])
def exchange_join():
    """Rejoint l'exchange avec un compte Nexus existant."""
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    if not username or not password:
        return jsonify({"ok": False, "error": "Identifiants manquants"}), 400
    db = load_db()
    if not check(db, username, password):
        return jsonify({"ok": False, "error": "Identifiants incorrects"}), 403
    with _EX_LOCK:
        if username not in NXC_EX["portfolios"]:
            NXC_EX["portfolios"][username] = {
                "nxd": 5000.0, "nxc": 1.0,
                "open_orders": [], "trades": [], "badges": ["🎉 Bienvenue"],
                "joined": now_iso(), "is_bot": False
            }
            _ex_save()
            return jsonify({"ok": True, "new": True, "bonus_nxd": 5000, "bonus_nxc": 1})
        return jsonify({"ok": True, "new": False})

@app.route("/exchange/order", methods=["POST"])
def exchange_order():
    """Place un ordre d'achat ou de vente."""
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    side     = (body.get("side") or "").strip()       # "buy" | "sell"
    order_type = body.get("type", "limit")            # "limit" | "market"
    try:
        price = float(body.get("price", 0))
        qty   = float(body.get("qty", 0))
    except Exception:
        return jsonify({"ok": False, "error": "Prix/quantité invalides"}), 400
    if side not in ("buy", "sell"): return jsonify({"ok": False, "error": "Side invalide"}), 400
    if qty <= 0: return jsonify({"ok": False, "error": "Quantité doit être > 0"}), 400
    db = load_db()
    if not check(db, username, password):
        return jsonify({"ok": False, "error": "Identifiants incorrects"}), 403
    with _EX_LOCK:
        p = NXC_EX["portfolios"].get(username)
        if not p: return jsonify({"ok": False, "error": "Rejoins l'exchange d'abord"}), 400
        mkt_price = NXC_MARKET.get("price", 5000)
        if order_type == "market":
            # Exécution immédiate au prix du marché
            exec_price = mkt_price * (1.005 if side == "buy" else 0.995)
            if side == "buy":
                cost = exec_price * qty * (1 + _EX_FEE)
                if p["nxd"] < cost:
                    return jsonify({"ok": False, "error": f"NXD insuffisants (besoin: {cost:.2f})"}), 400
                # Cherche un vendeur parmi les bots
                sellers = [b for b in _EX_BOTS if NXC_EX["portfolios"].get(b["name"], {}).get("nxc", 0) >= qty]
                if sellers:
                    seller = _rnd.choice(sellers)
                    _execute_trade(username, seller["name"], exec_price, qty, "market")
                else:
                    # Exécution directe contre le marché
                    fee = exec_price * qty * _EX_FEE
                    p["nxd"] -= exec_price * qty + fee
                    p["nxc"] += qty
                    ts = int(time.time() * 1000)
                    trade = {"id": str(_uuid.uuid4())[:8], "buyer": username, "seller": "🏦 Market",
                             "price": round(exec_price, 4), "qty": round(qty, 6),
                             "total": round(exec_price * qty, 2), "ts": ts, "source": "market"}
                    NXC_EX["trades"].append(trade)
                    p.setdefault("trades", []).append(trade)
                    _check_badges(username)
            else:  # sell
                if p["nxc"] < qty:
                    return jsonify({"ok": False, "error": f"NXC insuffisants (tu as: {p['nxc']:.4f})"}), 400
                fee = exec_price * qty * _EX_FEE
                p["nxc"] -= qty
                p["nxd"] += exec_price * qty - fee
                ts = int(time.time() * 1000)
                trade = {"id": str(_uuid.uuid4())[:8], "buyer": "🏦 Market", "seller": username,
                         "price": round(exec_price, 4), "qty": round(qty, 6),
                         "total": round(exec_price * qty, 2), "ts": ts, "source": "market"}
                NXC_EX["trades"].append(trade)
                p.setdefault("trades", []).append(trade)
                _check_badges(username)
            _ex_save()
            return jsonify({"ok": True, "executed": True, "price": round(exec_price, 2), "qty": qty})
        else:
            # Ordre limite
            if price <= 0: return jsonify({"ok": False, "error": "Prix limite invalide"}), 400
            if side == "buy":
                cost = price * qty * (1 + _EX_FEE)
                if p["nxd"] < cost:
                    return jsonify({"ok": False, "error": f"NXD insuffisants (besoin: {cost:.2f})"}), 400
            else:
                if p["nxc"] < qty:
                    return jsonify({"ok": False, "error": f"NXC insuffisants (tu as: {p['nxc']:.4f})"}), 400
            order = {"id": str(_uuid.uuid4())[:8], "user": username, "side": side,
                     "price": round(price, 4), "qty": round(qty, 6), "ts": int(time.time() * 1000)}
            NXC_EX["orders"].append(order)
            _match_orders()
            _ex_save()
            return jsonify({"ok": True, "executed": False, "order_id": order["id"]})

@app.route("/exchange/order/<order_id>", methods=["DELETE"])
def exchange_cancel_order(order_id):
    """Annule un ordre limite."""
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    db = load_db()
    if not check(db, username, password):
        return jsonify({"ok": False, "error": "Identifiants incorrects"}), 403
    with _EX_LOCK:
        before = len(NXC_EX["orders"])
        NXC_EX["orders"] = [o for o in NXC_EX["orders"]
                             if not (o["id"] == order_id and o["user"] == username)]
        if len(NXC_EX["orders"]) < before:
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": "Ordre non trouvé"}), 404

@app.route("/exchange/event", methods=["POST"])
def exchange_event():
    """Admin : déclenche un événement de marché."""
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    db = load_db()
    if not is_admin(db, username, password):
        return jsonify({"ok": False, "error": "Admin requis"}), 403
    title   = body.get("title", "Événement")
    desc    = body.get("desc", "")
    impact  = float(body.get("impact", 0))  # -0.3 à +0.3 (variation prix)
    with _EX_LOCK:
        ev = {"id": str(_uuid.uuid4())[:8], "title": title, "desc": desc,
              "impact": impact, "ts": int(time.time() * 1000)}
        NXC_EX["events"].append(ev)
        if len(NXC_EX["events"]) > 50: NXC_EX["events"] = NXC_EX["events"][-50:]
        # Applique l'impact sur le prix
        if impact != 0:
            NXC_MARKET["price"] = max(1, NXC_MARKET["price"] * (1 + impact))
    return jsonify({"ok": True, "event": ev})

@app.route("/exchange/deposit", methods=["POST"])
def exchange_deposit():
    """Admin : donne des NXD ou NXC à un compte."""
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    target   = (body.get("target") or "").strip()
    nxd      = float(body.get("nxd", 0))
    nxc      = float(body.get("nxc", 0))
    db = load_db()
    if not is_admin(db, username, password):
        return jsonify({"ok": False, "error": "Admin requis"}), 403
    with _EX_LOCK:
        p = NXC_EX["portfolios"].get(target)
        if not p: return jsonify({"ok": False, "error": "Compte non trouvé"}), 404
        p["nxd"] += nxd
        p["nxc"] += nxc
        _check_badges(target)
        _ex_save()
    return jsonify({"ok": True, "target": target, "nxd_added": nxd, "nxc_added": nxc})

# ── Fin NEXUS EXCHANGE ────────────────────────────────────────────────────────

# ════════════════════════════════════════════════════════════════════════════════
#  NEXUS OS  —  Sauvegarde / Chargement de l'état de l'OS en ligne
# ════════════════════════════════════════════════════════════════════════════════

@app.route("/nexusos/load", methods=["POST"])
def nexusos_load():
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    db = load_db()
    if not check(db, username, password):
        return jsonify({"ok": False, "error": "Identifiants incorrects"}), 403
    user_data = db["users"][username].get("data", {})
    os_state = user_data.get("nexusos", {})
    return jsonify({"ok": True, "state": os_state, "username": username,
                    "role": db["users"][username].get("role", "user")})

@app.route("/nexusos/save", methods=["POST"])
def nexusos_save():
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    state    = body.get("state", {})
    db = load_db()
    if not check(db, username, password):
        return jsonify({"ok": False, "error": "Identifiants incorrects"}), 403
    db["users"][username].setdefault("data", {})["nexusos"] = state
    db["users"][username]["updated"] = now_iso()
    save_db(db)
    return jsonify({"ok": True})

# ── Fin NEXUS OS ──────────────────────────────────────────────────────────────


# ════════════════════════════════════════════════════════════════════════════════
#  NEXUS STORE  —  Boutique avec codes de déblocage
#  Stockage : Upstash Redis (0 stockage Render)
#  Fichiers  : Cloudflare R2 (presigned URLs)
#  Vidéos    : Cloudflare Stream (48h)
# ════════════════════════════════════════════════════════════════════════════════

import hashlib as _hashlib

_R2_ACCOUNT_ID  = os.environ.get("R2_ACCOUNT_ID", "")
_R2_ACCESS_KEY  = os.environ.get("R2_ACCESS_KEY_ID", "")
_R2_SECRET_KEY  = os.environ.get("R2_SECRET_ACCESS_KEY", "")
_R2_BUCKET      = os.environ.get("R2_BUCKET_NAME", "")
_CF_STREAM_CUST = os.environ.get("CF_STREAM_CUSTOMER", "")
_CF_API_TOKEN   = os.environ.get("CF_API_TOKEN", "")

_STORE_ITEMS_KEY   = "nexus_store_items"
_STORE_UNLOCKS_KEY = "nexus_store_unlocks"

def _store_redis_get(key):
    if not (_UPSTASH_URL and _UPSTASH_TOKEN):
        return None
    import urllib.request as _ur
    try:
        cmd = json.dumps(["GET", key]).encode()
        req = _ur.Request(_UPSTASH_URL, data=cmd,
                          headers={"Authorization": "Bearer " + _UPSTASH_TOKEN,
                                   "Content-Type": "application/json"})
        result = json.loads(_ur.urlopen(req, timeout=12).read()).get("result")
        if result:
            return json.loads(result)
    except Exception:
        pass
    return None

def _store_redis_set(key, value):
    if not (_UPSTASH_URL and _UPSTASH_TOKEN):
        return
    import urllib.request as _ur
    try:
        cmd = json.dumps(["SET", key, json.dumps(value, ensure_ascii=False)]).encode()
        req = _ur.Request(_UPSTASH_URL, data=cmd,
                          headers={"Authorization": "Bearer " + _UPSTASH_TOKEN,
                                   "Content-Type": "application/json"})
        _ur.urlopen(req, timeout=15)
    except Exception:
        pass

def _store_hash_code(code):
    return _hashlib.sha256(code.strip().lower().encode()).hexdigest()

def _store_r2_presigned(r2_key, filename="fichier", expires=3600):
    if not (_R2_ACCOUNT_ID and _R2_ACCESS_KEY and _R2_SECRET_KEY and _R2_BUCKET):
        return None
    try:
        import boto3
        from botocore.config import Config as _BotoConfig
        s3 = boto3.client(
            "s3",
            endpoint_url=f"https://{_R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=_R2_ACCESS_KEY,
            aws_secret_access_key=_R2_SECRET_KEY,
            config=_BotoConfig(signature_version="s3v4"),
            region_name="auto"
        )
        return s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": _R2_BUCKET, "Key": r2_key,
                    "ResponseContentDisposition": f'attachment; filename="{filename}"'},
            ExpiresIn=expires
        )
    except Exception:
        return None

def _store_stream_url(video_id):
    if not _CF_STREAM_CUST:
        return None
    return f"https://{_CF_STREAM_CUST}/{video_id}/iframe"

# ── Route principale ──────────────────────────────────────────────────────────

@app.route("/store", methods=["GET"])
def store_page():
    return Response(STORE_HTML, mimetype="text/html")

# ── Routes API utilisateur ────────────────────────────────────────────────────

@app.route("/store/api/items", methods=["POST"])
def store_api_items():
    """Liste les items publics du store."""
    items = _store_redis_get(_STORE_ITEMS_KEY) or {}
    result = []
    for iid, item in items.items():
        result.append({
            "id":          iid,
            "name":        item.get("name", ""),
            "description": item.get("description", ""),
            "type":        item.get("type", "file"),
        })
    return jsonify({"ok": True, "items": result})

@app.route("/store/api/my_items", methods=["POST"])
def store_api_my_items():
    """Items débloqués de l'utilisateur connecté."""
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    db = load_db()
    if not check(db, username, password):
        return jsonify({"ok": False, "error": "Identifiants incorrects"}), 403
    role    = db["users"][username].get("role", "user")
    items   = _store_redis_get(_STORE_ITEMS_KEY) or {}
    unlocks = _store_redis_get(_STORE_UNLOCKS_KEY) or {}
    user_ids = unlocks.get(username, [])
    result = []
    for iid in user_ids:
        item = items.get(iid)
        if not item:
            continue
        itype = item.get("type", "file")
        entry = {"id": iid, "name": item.get("name", ""),
                 "description": item.get("description", ""), "type": itype}
        if itype == "video_rental":
            exp = unlocks.get(f"{username}_{iid}_exp") or 0
            entry["expires_at"] = exp
            entry["expired"]    = bool(exp and time.time() > exp)
        result.append(entry)
    return jsonify({"ok": True, "items": result, "role": role})

@app.route("/store/api/unlock", methods=["POST"])
def store_api_unlock():
    """Débloque un item avec son code."""
    body     = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    item_id  = (body.get("item_id") or "").strip()
    code     = (body.get("code") or "").strip()
    db = load_db()
    if not check(db, username, password):
        return jsonify({"ok": False, "error": "Identifiants incorrects"}), 403
    if not code or not item_id:
        return jsonify({"ok": False, "error": "Données manquantes"}), 400
    items = _store_redis_get(_STORE_ITEMS_KEY) or {}
    item  = items.get(item_id)
    if not item:
        return jsonify({"ok": False, "error": "Item introuvable"}), 404
    hashed = _store_hash_code(code)
    if item.get("code_hash") != hashed:
        return jsonify({"ok": False, "error": "Code incorrect"}), 400
    unlocks  = _store_redis_get(_STORE_UNLOCKS_KEY) or {}
    user_ids = unlocks.get(username, [])
    if item_id in user_ids:
        return jsonify({"ok": False, "error": "Déjà débloqué"}), 400
    user_ids.append(item_id)
    unlocks[username] = user_ids
    if item.get("type") == "video_rental":
        unlocks[f"{username}_{item_id}_exp"] = int(time.time()) + 48 * 3600
    _store_redis_set(_STORE_UNLOCKS_KEY, unlocks)
    return jsonify({"ok": True, "item_id": item_id, "item_name": item.get("name", "")})

@app.route("/store/api/download", methods=["POST"])
def store_api_download():
    """Retourne une presigned URL R2 pour télécharger le fichier."""
    body     = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    item_id  = (body.get("item_id") or "").strip()
    db = load_db()
    if not check(db, username, password):
        return jsonify({"ok": False, "error": "Identifiants incorrects"}), 403
    unlocks = _store_redis_get(_STORE_UNLOCKS_KEY) or {}
    if item_id not in unlocks.get(username, []):
        return jsonify({"ok": False, "error": "Non débloqué"}), 403
    items = _store_redis_get(_STORE_ITEMS_KEY) or {}
    item  = items.get(item_id)
    if not item:
        return jsonify({"ok": False, "error": "Item introuvable"}), 404
    r2_key = item.get("r2_key", "")
    url    = _store_r2_presigned(r2_key, filename=item.get("name", "fichier")) if r2_key else None
    return jsonify({"ok": True, "url": url})

@app.route("/store/api/stream", methods=["POST"])
def store_api_stream():
    """Retourne l'URL de la vidéo Stream si pas expirée."""
    body     = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    item_id  = (body.get("item_id") or "").strip()
    db = load_db()
    if not check(db, username, password):
        return jsonify({"ok": False, "error": "Identifiants incorrects"}), 403
    unlocks = _store_redis_get(_STORE_UNLOCKS_KEY) or {}
    if item_id not in unlocks.get(username, []):
        return jsonify({"ok": False, "error": "Non débloqué"}), 403
    exp = unlocks.get(f"{username}_{item_id}_exp") or 0
    if exp and time.time() > exp:
        return jsonify({"ok": False, "error": "Location expirée"}), 403
    items = _store_redis_get(_STORE_ITEMS_KEY) or {}
    item  = items.get(item_id)
    if not item:
        return jsonify({"ok": False, "error": "Item introuvable"}), 404
    vid = item.get("stream_video_id", "")
    url = _store_stream_url(vid) if vid else None
    return jsonify({"ok": True, "url": url, "expires_at": exp})

@app.route("/store/api/note", methods=["POST"])
def store_api_note():
    """Retourne le contenu texte d'une note débloquée."""
    body     = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    item_id  = (body.get("item_id") or "").strip()
    db = load_db()
    if not check(db, username, password):
        return jsonify({"ok": False, "error": "Identifiants incorrects"}), 403
    unlocks = _store_redis_get(_STORE_UNLOCKS_KEY) or {}
    if item_id not in unlocks.get(username, []):
        return jsonify({"ok": False, "error": "Non débloqué"}), 403
    items = _store_redis_get(_STORE_ITEMS_KEY) or {}
    item  = items.get(item_id)
    if not item:
        return jsonify({"ok": False, "error": "Item introuvable"}), 404
    return jsonify({"ok": True, "content": item.get("content", ""), "name": item.get("name", "")})

# ── Routes admin ──────────────────────────────────────────────────────────────

@app.route("/store/admin/item", methods=["POST"])
def store_admin_create_item():
    """Admin : crée ou met à jour un item."""
    body     = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    db = load_db()
    if not is_admin(db, username, password):
        return jsonify({"ok": False, "error": "Admin requis"}), 403
    iid   = (body.get("id") or str(_uuid.uuid4())[:8]).strip()
    itype = (body.get("type") or "file").strip()
    if itype not in ("file", "note", "video_rental"):
        return jsonify({"ok": False, "error": "Type invalide"}), 400
    items = _store_redis_get(_STORE_ITEMS_KEY) or {}
    item  = items.get(iid, {})
    item["name"]        = (body.get("name") or item.get("name", "")).strip()
    item["description"] = (body.get("description") or item.get("description", "")).strip()
    item["type"]        = itype
    code = (body.get("code") or "").strip()
    if code:
        item["code_hash"] = _store_hash_code(code)
    if itype == "note":
        item["content"] = body.get("content", item.get("content", ""))
    elif itype == "file" and body.get("r2_key"):
        item["r2_key"] = body["r2_key"].strip()
    elif itype == "video_rental" and body.get("stream_video_id"):
        item["stream_video_id"] = body["stream_video_id"].strip()
    items[iid] = item
    _store_redis_set(_STORE_ITEMS_KEY, items)
    return jsonify({"ok": True, "id": iid, "item": item})

@app.route("/store/admin/item", methods=["DELETE"])
def store_admin_delete_item():
    """Admin : supprime un item."""
    body     = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    iid      = (body.get("id") or "").strip()
    db = load_db()
    if not is_admin(db, username, password):
        return jsonify({"ok": False, "error": "Admin requis"}), 403
    if not iid:
        return jsonify({"ok": False, "error": "id manquant"}), 400
    items = _store_redis_get(_STORE_ITEMS_KEY) or {}
    if iid not in items:
        return jsonify({"ok": False, "error": "Item non trouvé"}), 404
    del items[iid]
    _store_redis_set(_STORE_ITEMS_KEY, items)
    return jsonify({"ok": True})

@app.route("/store/admin/items", methods=["POST"])
def store_admin_list_items():
    """Admin : liste tous les items."""
    body     = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    db = load_db()
    if not is_admin(db, username, password):
        return jsonify({"ok": False, "error": "Admin requis"}), 403
    return jsonify({"ok": True, "items": _store_redis_get(_STORE_ITEMS_KEY) or {}})

@app.route("/store/admin/unlocks", methods=["POST"])
def store_admin_unlocks():
    """Admin : tous les déblocages."""
    body     = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()
    db = load_db()
    if not is_admin(db, username, password):
        return jsonify({"ok": False, "error": "Admin requis"}), 403
    return jsonify({"ok": True, "unlocks": _store_redis_get(_STORE_UNLOCKS_KEY) or {}})

# ── Fin NEXUS STORE ───────────────────────────────────────────────────────────
