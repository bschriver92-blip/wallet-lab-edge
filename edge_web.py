"""EDGE WEB - supervisor for the executor on a free PaaS box (Render / Koyeb).

Why a web server: free instances only stay awake while they receive inbound HTTP
(Render: 15 min idle -> spin down; Koyeb: 1 h). FORGE pings `/` every 5 min. The same
server is how the PC gets the box's work back, since the box has no SSH:
    GET /              health + heartbeat age (the keep-alive target)
    GET /rows?since=T  exec_sim rows with t_seen > T   (header X-Edge-Key must match EDGE_KEY)
    GET /stats         execd_stats + counts
The seed (watch list + pool table) is a public sqlite file on GitHub (SEED_URL), refreshed
every SEED_EVERY seconds and applied by edge_apply_seed.py. The stream token comes from the
GSTREAM_TOKEN env var (never in the repo). execd.py runs as a child process and is
restarted if it dies. Dry-run only: there is no key file in this image, ever.
"""
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

APP = os.path.dirname(os.path.abspath(__file__))
CB = os.path.join(APP, "copybot")
LAB = os.path.dirname(os.environ.get("COPYBOT_DB", "/tmp/lab/copybot.db"))
DB = os.environ.get("COPYBOT_DB", "/tmp/lab/copybot.db")
SEED_URL = os.environ.get("SEED_URL", "")
SEED_EVERY = int(os.environ.get("SEED_EVERY", "600"))
EDGE_KEY = os.environ.get("EDGE_KEY", "")
PORT = int(os.environ.get("PORT", "10000"))
state = {"started": time.time(), "seed_ok": None, "seed_err": None, "execd_restarts": 0, "execd_pid": None}


def write_gstream_txt():
    tok = os.environ.get("GSTREAM_TOKEN", "").strip()
    url = os.environ.get("GSTREAM_URL", "solana-yellowstone-grpc.publicnode.com:443").strip()
    lines = []
    for i, suffix in enumerate(("", "2", "3")):
        lines += [f"url{suffix}={url}", f"token{suffix}={tok}", f"name{suffix}=publicnode{('-' + 'bc'[i - 1]) if i else ''}"]
    with open(os.path.join(CB, "gstream.txt"), "w") as f:
        f.write("\n".join(lines) + "\n")
    os.chmod(os.path.join(CB, "gstream.txt"), 0o600)
    return bool(tok)


def fetch_seed():
    if not SEED_URL:
        return False, "no SEED_URL"
    os.makedirs(LAB, exist_ok=True)
    try:
        req = urllib.request.Request(SEED_URL, headers={"User-Agent": "wallet-lab-edge/1.0", "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=60) as r:
            data = r.read()
        if len(data) < 1024:
            return False, f"seed too small ({len(data)} B)"
        with open(os.path.join(LAB, "seed.db"), "wb") as f:
            f.write(data)
        r = subprocess.run([sys.executable, os.path.join(APP, "edge_apply_seed.py")], capture_output=True, text=True, timeout=120,
                           env=dict(os.environ, HOME=os.path.dirname(LAB), COPYBOT_DB=DB))
        return r.returncode == 0, (r.stdout or r.stderr).strip()[-160:]
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:120]}"


def seed_loop():
    while True:
        ok, msg = fetch_seed()
        state["seed_ok"], state["seed_err"], state["seed_t"] = ok, None if ok else msg, time.time()
        print(f"seed: {'ok' if ok else 'FAIL'} {msg}", flush=True)
        time.sleep(SEED_EVERY)


def execd_loop():
    while True:
        p = subprocess.Popen([sys.executable, "execd.py"], cwd=CB, env=dict(os.environ, COPYBOT_DB=DB))
        state["execd_pid"] = p.pid
        p.wait()
        state["execd_restarts"] += 1
        print(f"execd exited {p.returncode}; restarting in 5 s", flush=True)
        time.sleep(5)


def kv(key):
    try:
        c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=5)
        r = c.execute("SELECT value FROM lab_kv WHERE key=?", (key,)).fetchone()
        c.close()
        return r[0] if r else None
    except Exception:
        return None


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path, _, qs = self.path.partition("?")
        q = dict(p.split("=", 1) for p in qs.split("&") if "=" in p)
        hb = kv("execd_hb")
        if path == "/":
            return self._json(200, {"ok": True, "up_s": round(time.time() - state["started"]), "hb_age": round(time.time() - float(hb), 1) if hb else None,
                                    "seed_ok": state["seed_ok"], "seed_err": state["seed_err"], "execd_restarts": state["execd_restarts"]})
        if EDGE_KEY and self.headers.get("X-Edge-Key") != EDGE_KEY:
            return self._json(403, {"error": "key"})
        if path == "/stats":
            return self._json(200, {"hb_age": round(time.time() - float(hb), 1) if hb else None, "stats": kv("execd_stats"), "state": state})
        if path == "/rows":
            since = float(q.get("since", "0") or 0)
            try:
                c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=10)
                cols = [r[1] for r in c.execute("PRAGMA table_info(exec_sim)")]
                rows = c.execute(f"SELECT {','.join(cols)} FROM exec_sim WHERE t_seen > ? ORDER BY t_seen LIMIT 5000", (since,)).fetchall()
                c.close()
            except Exception as e:
                return self._json(500, {"error": f"{type(e).__name__}: {str(e)[:100]}"})
            return self._json(200, {"cols": cols, "rows": rows, "max_t": max((r[cols.index("t_seen")] for r in rows), default=since),
                                    "hb_age": round(time.time() - float(hb), 1) if hb else None})
        return self._json(404, {"error": "no such path"})


if __name__ == "__main__":
    os.makedirs(LAB, exist_ok=True)
    has_tok = write_gstream_txt()
    print(f"edge_web: token {'set' if has_tok else 'MISSING'}, seed {SEED_URL or '-'}, db {DB}", flush=True)
    ok, msg = fetch_seed()
    print(f"first seed: {'ok' if ok else 'FAIL'} {msg}", flush=True)
    state["seed_ok"], state["seed_err"] = ok, None if ok else msg
    threading.Thread(target=seed_loop, daemon=True).start()
    threading.Thread(target=execd_loop, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", PORT), H).serve_forever()
