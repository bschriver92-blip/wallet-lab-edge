"""DEEP CANDLES - hourly price paths for the coins in the deep histories (GeckoTerminal, keyless).

Time-based exit rules (HOLD80, trail25) need each coin's price AFTER the wallet's entry, which
the wallet's own prints don't give. GeckoTerminal serves paged hourly OHLCV per pool without a
key:  /networks/solana/pools/<pool>/ohlcv/hour?limit=1000&currency=token&before_timestamp=<ts>
(09-03 probe: page 2 reached the pool's birth 71 d back). Its limit is per IP and lower than the
documented 30/min in practice (10 x429 in 30 calls at 27/min) -> 3.2 s gaps, and the fan-out
over GitHub Actions runners (deep_candles.yml, one IP per shard) for thousands of coins.
pump.fun's swap-api returns only the newest 1000 candles (no paging) - not used.

Cache: E:/MemeCoin/lab_history/deep/candles.db  (or --out FILE)
    pools(mint PK, pool, dex, quote, reserve_usd, ts)          pool = the most liquid pool for the coin
    candles(pool, ts, o, h, l, c, v)  PK(pool, ts)             ts = candle open, seconds, hourly
    cover(pool PK, oldest, newest, exhausted)                  how far back the cache reaches
Resumable; every run fetches what is needed and the cache lacks, within --max-calls.

    python deep_candles.py [DIR] [--max-calls 1500] [--after-hours 72]        needs from DIR/deep_*.db
    python deep_candles.py --coins FILE [--shard i/n] [--out FILE] [...]       needs from a coin list
    python deep_candles.py --export DIR FILE                                   write the coin list (mint lo hi)
    python deep_candles.py --merge SRC.db DST.db                                merge two caches
"""
import glob
import hashlib
import os
import sqlite3
import sys
import time

import httpx

DIR = "E:/MemeCoin/lab_history/deep"
CACHE = os.path.join(DIR, "candles.db")
GT = "https://api.geckoterminal.com/api/v2/networks/solana"
H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/128 Safari/537.36", "Accept": "application/json"}
GAP_S = float(os.environ.get("CANDLE_GAP", "3.2"))
_last = [0.0]
STATS = {"calls": 0, "429": 0, "slow": 0, "secs": 0.0}


def get(url, tries=4):
    for i in range(tries):
        wait = _last[0] + GAP_S - time.time()
        if wait > 0:
            time.sleep(wait)
        _last[0] = time.time()
        t = time.time()
        try:
            r = httpx.get(url, headers=H, timeout=60)
        except Exception:
            time.sleep(3 + 3 * i)
            continue
        STATS["calls"] += 1
        STATS["secs"] += time.time() - t
        if time.time() - t > 5:
            STATS["slow"] += 1
        if r.status_code == 429:
            STATS["429"] += 1
            time.sleep(10 + 10 * i)
            continue
        if r.status_code == 404:
            return None
        if r.status_code != 200:
            time.sleep(3 + 3 * i)
            continue
        try:
            return r.json()
        except Exception:
            return None
    return None


def needs_from_dbs(d, after_hours):
    """mint -> (lo, hi): the span of prices the backtest needs, merged across wallets"""
    need = {}
    for f in glob.glob(os.path.join(d, "deep_*.db")):
        rows = []
        for i in range(5):                      # the deep DBs may be mid-write by deep_pull: retry on a lock
            try:
                c = sqlite3.connect("file:%s?mode=ro" % f, uri=True, timeout=10)
                rows = c.execute("SELECT mint, MIN(ts), MAX(ts) FROM trades WHERE mint NOT LIKE 'pool:%' AND venue != 'transfer' GROUP BY mint").fetchall()
                c.close()
                break
            except sqlite3.OperationalError:
                time.sleep(2 + i)
        for mint, lo, hi in rows:
            lo, hi = lo - 3600, hi + after_hours * 3600
            if mint in need:
                need[mint] = (min(need[mint][0], lo), max(need[mint][1], hi))
            else:
                need[mint] = (lo, hi)
    return need


def needs_from_file(path):
    need = {}
    for ln in open(path):
        p = ln.split()
        if len(p) >= 3 and not ln.startswith("#"):
            need[p[0]] = (int(float(p[1])), int(float(p[2])))
    return need


def db(path):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    c = sqlite3.connect(path, timeout=30)
    c.execute("CREATE TABLE IF NOT EXISTS pools(mint TEXT PRIMARY KEY, pool TEXT, dex TEXT, quote TEXT, reserve_usd REAL, ts REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS candles(pool TEXT, ts INTEGER, o REAL, h REAL, l REAL, c REAL, v REAL, PRIMARY KEY(pool, ts))")
    c.execute("CREATE TABLE IF NOT EXISTS cover(pool TEXT PRIMARY KEY, oldest INTEGER, newest INTEGER, exhausted INTEGER)")
    return c


def find_pool(c, mint):
    r = c.execute("SELECT pool, dex, quote FROM pools WHERE mint=?", (mint,)).fetchone()
    if r:
        return r
    j = get("%s/tokens/%s/pools?page=1" % (GT, mint))
    best = None
    for p in (j or {}).get("data") or []:
        a = p.get("attributes") or {}
        try:
            res = float(a.get("reserve_in_usd") or 0)
        except Exception:
            res = 0.0
        dex = ((p.get("relationships") or {}).get("dex") or {}).get("data", {}).get("id")
        quote = ((p.get("relationships") or {}).get("quote_token") or {}).get("data", {}).get("id", "")
        if best is None or res > best[3]:
            best = (a.get("address") or p.get("id", "").split("_", 1)[-1], dex, quote.replace("solana_", ""), res)
    c.execute("INSERT OR REPLACE INTO pools VALUES(?,?,?,?,?,?)", (mint, best[0] if best else None, best[1] if best else None, best[2] if best else None, best[3] if best else 0.0, time.time()))
    c.commit()
    return (best[0], best[1], best[2]) if best else (None, None, None)


def fetch_back(c, pool, lo, budget):
    """page hourly candles back until the cache covers `lo`; returns calls used"""
    cov = c.execute("SELECT oldest, newest, exhausted FROM cover WHERE pool=?", (pool,)).fetchone()
    if cov and (cov[2] or cov[0] <= lo):
        return 0
    before, calls = (cov[0] if cov else None), 0
    oldest, newest, exhausted = (cov[0] if cov else None), (cov[1] if cov else None), 0
    while calls < budget:
        url = "%s/pools/%s/ohlcv/hour?aggregate=1&limit=1000&currency=token" % (GT, pool)
        if before:
            url += "&before_timestamp=%d" % before
        j = get(url)
        calls += 1
        rows = (((j or {}).get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []
        if not rows:
            exhausted = 1
            break
        c.executemany("INSERT OR IGNORE INTO candles VALUES(?,?,?,?,?,?,?)", [(pool, int(r[0]), r[1], r[2], r[3], r[4], r[5]) for r in rows])
        ts = [int(r[0]) for r in rows]
        oldest = min(ts) if oldest is None else min(oldest, min(ts))
        newest = max(ts) if newest is None else max(newest, max(ts))
        before = min(ts)
        if len(rows) < 1000:
            exhausted = 1
            break
        if oldest <= lo:
            break
    c.execute("INSERT OR REPLACE INTO cover VALUES(?,?,?,?)", (pool, oldest, newest, exhausted))
    c.commit()
    return calls


def run(need, cache, max_calls, shard=(0, 1)):
    i_s, n_s = shard
    if n_s > 1:
        need = {m: v for m, v in need.items() if int(hashlib.md5(m.encode()).hexdigest(), 16) % n_s == i_s}
    c = db(cache)
    print("deep candles: %d coins needed (shard %d/%d), cache %s" % (len(need), i_s, n_s, cache), flush=True)
    calls = n_pool = n_nopool = n_fetched = n_have = 0
    t0 = time.time()
    for i, (mint, (lo, hi)) in enumerate(sorted(need.items(), key=lambda kv: kv[1][0], reverse=True)):   # newest first
        if calls >= max_calls:
            break
        pool, dex, quote = find_pool(c, mint)
        if not pool:
            n_nopool += 1
            continue
        n_pool += 1
        used = fetch_back(c, pool, lo, max(1, max_calls - calls))
        calls += used
        if used:
            n_fetched += 1
        else:
            n_have += 1
        if (i + 1) % 25 == 0:
            print("  %d/%d coins, %d calls, %.0f s" % (i + 1, len(need), calls, time.time() - t0), flush=True)
    tot = c.execute("SELECT COUNT(*), COUNT(DISTINCT pool) FROM candles").fetchone()
    print("DONE coins %d | with pool %d, no pool %d | fetched %d, cached %d | calls %d | cache: %d candles, %d pools | %.0f s | http: %d calls, %d x429, %d slow(>5s), mean %.1f s" % (
        len(need), n_pool, n_nopool, n_fetched, n_have, calls, tot[0], tot[1], time.time() - t0,
        STATS["calls"], STATS["429"], STATS["slow"], STATS["secs"] / max(1, STATS["calls"])), flush=True)
    c.close()


def merge(src, dst):
    c = db(dst)
    c.execute("ATTACH DATABASE ? AS s", (src,))
    b = c.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    c.execute("INSERT OR IGNORE INTO pools SELECT * FROM s.pools")
    c.execute("INSERT OR IGNORE INTO candles SELECT * FROM s.candles")
    # coverage: keep the widest span; exhausted if either side reached the pool's birth
    for pool, oldest, newest, exhausted in c.execute("SELECT pool, oldest, newest, exhausted FROM s.cover").fetchall():
        r = c.execute("SELECT oldest, newest, exhausted FROM cover WHERE pool=?", (pool,)).fetchone()
        if r:
            oldest, newest, exhausted = min(oldest, r[0]), max(newest, r[1]), max(exhausted, r[2])
        c.execute("INSERT OR REPLACE INTO cover VALUES(?,?,?,?)", (pool, oldest, newest, exhausted))
    a = c.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    c.commit()
    c.close()
    print("merged %s -> %s: candles %d -> %d" % (os.path.basename(src), os.path.basename(dst), b, a))


def main():
    a = sys.argv[1:]
    if a and a[0] == "--merge":
        merge(a[1], a[2])
        return
    if a and a[0] == "--export":
        need = needs_from_dbs(a[1], float(a[a.index("--after-hours") + 1]) if "--after-hours" in a else 72)
        with open(a[2], "w") as fh:
            for m, (lo, hi) in sorted(need.items(), key=lambda kv: kv[1][0], reverse=True):
                fh.write("%s %d %d\n" % (m, lo, hi))
        print("exported %d coins -> %s" % (len(need), a[2]))
        return
    max_calls = int(a[a.index("--max-calls") + 1]) if "--max-calls" in a else 1500
    after_h = float(a[a.index("--after-hours") + 1]) if "--after-hours" in a else 72
    cache = a[a.index("--out") + 1] if "--out" in a else CACHE
    shard = tuple(map(int, a[a.index("--shard") + 1].split("/"))) if "--shard" in a else (0, 1)
    if "--coins" in a:
        need = needs_from_file(a[a.index("--coins") + 1])
    else:
        d = a[0] if a and not a[0].startswith("--") else DIR
        need = needs_from_dbs(d, after_h)
    run(need, cache, max_calls, shard)


if __name__ == "__main__":
    main()
