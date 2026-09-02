"""PumpSwap pool -> coin mint. The missing link for graduated-coin trades.

PumpSwap's Buy/Sell events name the POOL, not the coin. The tape decodes
those events for free (~4.9M trades/day) but could not join them to anything
- so it counted them and threw them away, and the paper book was blind to a
watched wallet's trades on graduated coins.

The pool account itself holds base_mint and quote_mint at fixed offsets
(pump_amm `Pool` struct: 8-byte discriminator, pool_bump u8, index u16,
creator 32, base_mint 32, quote_mint 32, ...). One free RPC read per pool,
cached forever in tape.db `pools`. Self-check: quote_mint must be WSOL.

  python pools.py <pool> [<pool> ...]     resolve and print
  python pools.py --pending [n]           resolve up to n pools the tape queued
"""
import base64
import json
import os
import sqlite3
import sys
import time
import urllib.request

import tape

WSOL = "So11111111111111111111111111111111111111112"
# mainnet-beta first: leorpc's node lags on brand-new accounts ("NOT FOUND"
# on a pool beta already had, 21:58 09-01)
RPC = ["https://api.mainnet-beta.solana.com", "https://solana.leorpc.com/?api_key=FREE"]
UA = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}
OFF_BASE, OFF_QUOTE = 43, 75

SCHEMA = """
CREATE TABLE IF NOT EXISTS pools(pool TEXT PRIMARY KEY, mint TEXT, quote TEXT, ts INTEGER,
  flip INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS pool_todo(pool TEXT PRIMARY KEY, first_seen INTEGER, tries INTEGER DEFAULT 0);
"""


def init():
    c = tape.db()
    c.executescript(SCHEMA)
    try:
        c.execute("ALTER TABLE pools ADD COLUMN flip INTEGER DEFAULT 0")
    except Exception:
        pass
    c.commit()
    c.close()


def _account(pool, url):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
                       "params": [pool, {"encoding": "base64"}]}).encode()
    req = urllib.request.Request(url, data=body, headers=UA)
    with urllib.request.urlopen(req, timeout=20) as r:
        d = json.loads(r.read())
    v = (d.get("result") or {}).get("value")
    if not v:
        return None
    return base64.b64decode(v["data"][0])


def resolve(pool):
    """(mint, quote_side, flip) for a pool, or None.

    ⚠️ Some PumpSwap pools are REVERSED: base = WSOL, quote = the coin
    (measured 21:58 09-01 - about half the 'failures' were exactly this).
    `flip`=1 tells the tape to swap the SOL/token legs and invert the price
    for that pool's events, which decode_pswap read the other way round.
    A pool with SOL on neither side is not ours - return None, do not guess.
    """
    for url in RPC:
        try:
            raw = _account(pool, url)
        except Exception:
            continue
        if raw is None:
            continue                      # this node has not seen it yet; try the next
        if len(raw) < OFF_QUOTE + 32:
            return None
        base = tape.b58(raw[OFF_BASE:OFF_BASE + 32])
        quote = tape.b58(raw[OFF_QUOTE:OFF_QUOTE + 32])
        if quote == WSOL:
            return base, quote, 0
        if base == WSOL:
            return quote, base, 1
        return None
    return None


def lookup(pools):
    """{pool: (mint, flip)} from cache only (no network) - what the tape calls."""
    if not pools:
        return {}
    c = tape.db()
    out = {}
    ps = list(pools)
    for i in range(0, len(ps), 500):
        part = ps[i:i + 500]
        q = "SELECT pool, mint, flip FROM pools WHERE pool IN (%s)" % ",".join("?" * len(part))
        for r in c.execute(q, part):
            out[r["pool"]] = (r["mint"], r["flip"])
    c.close()
    return out


def remember(pool, mint, quote, flip):
    """cache a pool the executor resolved on the fly (same row resolve_pending writes)."""
    c = tape.db()
    c.execute("INSERT OR REPLACE INTO pools(pool, mint, quote, ts, flip) VALUES(?,?,?,?,?)",
              (pool, mint, quote, int(time.time()), int(flip)))
    c.commit()
    c.close()


def enqueue(pools):
    c = tape.db()
    now = int(time.time())
    c.executemany("INSERT OR IGNORE INTO pool_todo(pool, first_seen) VALUES(?,?)",
                  [(p, now) for p in pools])
    c.commit()
    c.close()


def resolve_pending(n=120, pause=0.5):
    """Resolve up to n queued pools (public RPC, polite). Returns (ok, failed)."""
    init()
    c = tape.db()
    # pools that failed 3 times are non-SOL pairs or unreadable - drop them so
    # the queue count means "waiting", not "dead"
    c.execute("DELETE FROM pool_todo WHERE tries >= 3")
    c.commit()
    todo = [r["pool"] for r in c.execute(
        "SELECT pool FROM pool_todo WHERE tries < 3 ORDER BY first_seen LIMIT ?", (n,))]
    c.close()
    ok = bad = 0
    for p in todo:
        r = resolve(p)
        c = tape.db()
        if r:
            c.execute("INSERT OR REPLACE INTO pools(pool, mint, quote, ts, flip) VALUES(?,?,?,?,?)",
                      (p, r[0], r[1], int(time.time()), r[2]))
            c.execute("DELETE FROM pool_todo WHERE pool=?", (p,))
            ok += 1
        else:
            c.execute("UPDATE pool_todo SET tries=tries+1 WHERE pool=?", (p,))
            bad += 1
        c.commit()
        c.close()
        time.sleep(pause)
    return ok, bad


if __name__ == "__main__":
    a = sys.argv[1:]
    init()
    if a and a[0] == "--pending":
        print(resolve_pending(int(a[1]) if len(a) > 1 else 120))
    elif a:
        for p in a:
            print(p, "->", resolve(p))
    else:
        print(__doc__)
