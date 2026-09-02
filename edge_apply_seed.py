"""EDGE APPLY SEED - box side of sync_edge.py.

    python edge_apply_seed.py                 merge ~/lab/seed.db into ~/lab/copybot.db (lab_wallets)
                                              and ~/lab/tape.db (pools); creates both if missing
    python edge_apply_seed.py dump SINCE OUT  write exec_sim rows with t_seen > SINCE to OUT (json)
"""
import json
import os
import sqlite3
import sys
import time

LAB = os.path.expanduser("~/lab")
SEED = os.path.join(LAB, "seed.db")
CDB = os.environ.get("COPYBOT_DB", os.path.join(LAB, "copybot.db"))
TDB = os.path.join(LAB, "tape.db")


def merge():
    os.makedirs(LAB, exist_ok=True)
    s = sqlite3.connect(SEED)
    c = sqlite3.connect(CDB, timeout=30)
    c.execute("CREATE TABLE IF NOT EXISTS lab_wallets(address TEXT PRIMARY KEY, state TEXT, score REAL, note TEXT, updated REAL)")
    c.execute("""CREATE TABLE IF NOT EXISTS signals(sig TEXT PRIMARY KEY, wallet TEXT,
  ts INTEGER, seen REAL)""")   # the paper-book signals table execd banks into (lab.init makes it on the PC)
    c.execute("""CREATE TABLE IF NOT EXISTS lab_kv(key TEXT PRIMARY KEY, value TEXT)""")   # heartbeat / single-executor guard / stats live here
    rows = s.execute("SELECT address, state, score, note FROM lab_wallets").fetchall()
    c.executemany("INSERT OR REPLACE INTO lab_wallets(address, state, score, note, updated) VALUES(?,?,?,?,?)",
                  [(a, st, sc, n, time.time()) for a, st, sc, n in rows])
    keep = {r[0] for r in rows}
    for (a,) in c.execute("SELECT address FROM lab_wallets").fetchall():
        if a not in keep:
            c.execute("UPDATE lab_wallets SET state='retired' WHERE address=?", (a,))
    c.commit()
    c.close()
    t = sqlite3.connect(TDB, timeout=30)
    t.execute("CREATE TABLE IF NOT EXISTS pools(pool TEXT PRIMARY KEY, mint TEXT, quote TEXT, ts REAL, flip INTEGER)")
    t.execute("CREATE TABLE IF NOT EXISTS pool_todo(pool TEXT PRIMARY KEY, ts REAL)")
    prow = s.execute("SELECT pool, mint, quote, ts, flip FROM pools").fetchall()
    t.executemany("INSERT OR IGNORE INTO pools VALUES(?,?,?,?,?)", prow)
    t.commit()
    t.close()
    s.close()
    print(f"seed applied: {len(rows)} wallets, {len(prow)} pools")


def dump(since, out):
    c = sqlite3.connect(f"file:{CDB}?mode=ro", uri=True, timeout=30)
    cols = [r[1] for r in c.execute("PRAGMA table_info(exec_sim)")]
    rows = c.execute(f"SELECT {','.join(cols)} FROM exec_sim WHERE t_seen > ? ORDER BY t_seen", (since,)).fetchall()
    hb = None
    try:
        r = c.execute("SELECT value FROM lab_kv WHERE key='execd_hb'").fetchone()
        hb = round(time.time() - float(r[0]), 1) if r else None
    except Exception:
        pass
    c.close()
    json.dump({"cols": cols, "rows": rows, "max_t": max((r[cols.index("t_seen")] for r in rows), default=since), "hb_age": hb}, open(out, "w"))
    print(f"dumped {len(rows)} rows since {since:.0f}; heartbeat age {hb}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "dump":
        dump(float(sys.argv[2]), sys.argv[3])
    else:
        merge()
