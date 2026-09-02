"""DEEP PULL - a wallet's real trade history from FREE full-history RPCs (2 runners = 2 IPs).

09-03 finding: PublicNode's getSignaturesForAddress index is ~2 DAYS deep (ledger only, no
bigtable) - the first "180-day" pull was a 2-day pull. Full history, free, no key:
    api.mainnet-beta.solana.com      ~4 getTransaction/s per IP (40 per 10 s per method), 429 past it
    solana.leorpc.com/?api_key=FREE  ~2-3/s per IP, no JSON-RPC batching (mainnet-beta batch: 2 of 20)
Each runner (one IP) fetches ~4-6 tx/s across both endpoints with an adaptive pacer (429 -> x0.7,
+10 % every 50 ok). --shard i/n splits the signature list by hash so the PC and the Ashburn box
share one job; the per-wallet sqlite files merge with INSERT OR IGNORE (--merge).

Rows: one per venue EVENT whose `user` is the wallet (Dune's trader_id - the pump.fun / PumpSwap
event user, NOT the fee payer: fleet/router wallets pay from elsewhere), fallback fee-payer balance
deltas (chain.parse_swap). Busy wallets past MAX_SIGS are refused: their months live in Dune.

    python deep_pull.py <wallet> [days] [out.db]
    python deep_pull.py --list wallets.txt [--days 180] [--out DIR] [--shard i/n]
    python deep_pull.py --merge SRC_DIR DST_DIR
"""
import base64
import glob
import hashlib
import json
import os
import queue
import shutil
import sqlite3
import sys
import threading
import time

import httpx

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import chain
try:
    import tape as _tape
except Exception as _e:                       # without the tape decoders only the fee-payer fallback runs - say so loudly
    _tape = None
    print("WARNING: tape decoders unavailable (%s) - venue events will NOT be decoded" % str(_e)[:80], file=sys.stderr, flush=True)
    if os.environ.get("DEEP_REQUIRE_TAPE"):
        sys.exit(2)

H = {"User-Agent": "wallet-lab-deep/1.1", "Content-Type": "application/json"}
# leorpc (FREE key) has full history but returns getTransaction WITHOUT logMessages / innerInstructions
# (09-03 probe) - useless for event decoding, so the pool is mainnet-beta only unless DEEP_RPCS says otherwise
ENDPOINTS = [e for e in os.environ.get("DEEP_RPCS", "https://api.mainnet-beta.solana.com").split(",") if e]
WALK_RPC = ENDPOINTS[0]
MAX_SIGS = int(os.environ.get("DEEP_MAX_SIGS", "60000"))
PER_EP = int(os.environ.get("DEEP_PER_EP", "3"))          # concurrent requests per endpoint
START_RATE = float(os.environ.get("DEEP_RATE", "3.0"))    # requests/s per endpoint to start
MAX_RATE = float(os.environ.get("DEEP_MAX_RATE", "4.0"))  # mainnet-beta: 40 per 10 s per method


def is_limited(st, err):
    """HTTP 429, or mainnet-beta's in-body {'code': 429, 'message': 'Too many requests for a specific RPC call'}"""
    return st == 429 or (isinstance(err, dict) and err.get("code") == 429)
TX_OPTS = {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}


class Pacer:
    """per-endpoint rate control shared by its workers: 429 -> rate x0.7 and a 3 s pause; +10 % per 50 ok"""

    def __init__(self, rate):
        self.rate, self.next, self.lock, self.n429, self.nok = rate, 0.0, threading.Lock(), 0, 0

    def wait(self):
        with self.lock:
            t = max(time.time(), self.next)
            self.next = t + 1.0 / self.rate
        d = t - time.time()
        if d > 0:
            time.sleep(d)

    def limited(self):
        with self.lock:
            self.n429 += 1
            self.rate = max(0.3, self.rate * 0.7)
            self.next = max(self.next, time.time() + 3.0)

    def ok(self):
        with self.lock:
            self.nok += 1
            if self.nok % 50 == 0:
                self.rate = min(MAX_RATE, self.rate * 1.1)


PACERS = {e: Pacer(START_RATE) for e in ENDPOINTS}


def rpc(url, m, p, timeout=60):
    """(http status, result, error)"""
    try:
        r = httpx.post(url, json={"jsonrpc": "2.0", "id": 1, "method": m, "params": p}, headers=H, timeout=timeout)
    except Exception as e:
        return None, None, str(e)[:80]
    if r.status_code != 200:
        return r.status_code, None, r.text[:80]
    try:
        j = r.json()
    except Exception:
        return 200, None, r.text[:80]
    return 200, j.get("result"), j.get("error")


def walk_signatures(wallet, days):
    """every signature mentioning the wallet in the last `days` (newest first), from the full-history RPC"""
    since = time.time() - days * 86400
    before, out = None, []
    while len(out) < MAX_SIGS:
        p = {"limit": 1000}
        if before:
            p["before"] = before
        res = None
        for i in range(6):
            PACERS[WALK_RPC].wait()
            st, res, err = rpc(WALK_RPC, "getSignaturesForAddress", [wallet, p])
            if st == 200 and res is not None:
                break
            if is_limited(st, err):
                PACERS[WALK_RPC].limited()
            time.sleep(2 * (i + 1))
        if not res:
            break
        out += [(s["signature"], s.get("blockTime") or 0, s.get("err")) for s in res]
        before = res[-1]["signature"]
        if (res[-1].get("blockTime") or 0) < since or len(res) < 1000:
            break
    keep = [(s, t) for s, t, err in out if t >= since and not err]
    return keep, len(out) >= MAX_SIGS


def fetch_all(sigs, on_tx, label=""):
    """fetch every signature through the endpoint pool; on_tx(sig, tx_or_None) runs in the caller's thread"""
    q, out, stop = queue.Queue(), queue.Queue(), threading.Event()
    for s in sigs:
        q.put((s, 0))

    def worker(url):
        pc = PACERS[url]
        while not stop.is_set():
            try:
                sig, tries = q.get(timeout=1)
            except queue.Empty:
                continue
            pc.wait()
            st, res, err = rpc(url, "getTransaction", [sig, TX_OPTS])
            if st == 200 and err is None:
                pc.ok()
                out.put((sig, res))
                continue
            if is_limited(st, err):
                pc.limited()
            if tries < 6:
                q.put((sig, tries + 1))
            else:
                out.put((sig, None))

    threads = [threading.Thread(target=worker, args=(u,), daemon=True) for u in ENDPOINTS for _ in range(PER_EP)]
    for t in threads:
        t.start()
    t0, last = time.time(), time.time()
    for i in range(len(sigs)):
        sig, tx = out.get()
        on_tx(sig, tx)
        if time.time() - last > 120:
            last = time.time()
            rates = " ".join("%s=%.1f/s(429:%d)" % (u.split("/")[2][:12], PACERS[u].rate, PACERS[u].n429) for u in ENDPOINTS)
            print("  %s %d/%d %.1f tx/s | %s" % (label, i + 1, len(sigs), (i + 1) / (time.time() - t0), rates), flush=True)
    stop.set()


STABLES = {"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}
B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58(raw):
    n, s = int.from_bytes(raw, "big"), ""
    while n:
        n, r = divmod(n, 58)
        s = B58[r] + s
    return "1" * (len(raw) - len(raw.lstrip(b"\0"))) + s


def pool_lookup(pool):
    """(mint, flip) from the PumpSwap Pool account: base_mint @43, quote_mint @75 (pump_amm IDL,
    same read as execd.resolve_fast). None = not a SOL pool or the account is gone."""
    for i in range(4):
        PACERS[WALK_RPC].wait()
        st, res, err = rpc(WALK_RPC, "getAccountInfo", [pool, {"encoding": "base64", "dataSlice": {"offset": 43, "length": 64}}])
        if st == 200 and err is None:
            val = (res or {}).get("value")
            if not val:
                return None
            raw = base64.b64decode(val["data"][0])
            base, quote = b58(raw[:32]), b58(raw[32:64])
            if quote == chain.WSOL:
                return base, 0
            if base == chain.WSOL:
                return quote, 1
            return None
        if is_limited(st, err):
            PACERS[WALK_RPC].limited()
        time.sleep(1 + i)
    return None


def decode_rows(wallet, sig, tx, resolve):
    """rows (side, mint, sol, tok, price, venue, src) for this transaction and wallet; sol/tok absolute"""
    if not tx or (tx.get("meta") or {}).get("err"):
        return []
    if _tape:
        for ln in (tx.get("meta") or {}).get("logMessages") or []:
            if not ln.startswith("Program data: "):
                continue
            try:
                raw = base64.b64decode(ln[14:])
                d = _tape.decode_pump(raw, fresh=False) or _tape.decode_pswap(raw, fresh=False)
            except Exception:
                d = None
            if d and d.get("user") == wallet and d.get("tok"):
                sol, tok, mint = abs(d["sol"]), abs(d["tok"]), d.get("mint")
                if mint and mint.startswith("pool:"):
                    hit = resolve(mint[5:])
                    if not hit:
                        return []
                    mint, flip = hit
                    if flip:                      # reversed pool: the decoder read the legs swapped (9 vs 6 decimals)
                        sol, tok = tok / 1000.0, sol * 1000.0
                if sol <= 0 or tok <= 0:
                    return []
                return [(d["side"], mint, sol, tok, sol / tok, d.get("venue", "pump"), "event")]
    if chain.fee_payer(tx) != wallet:
        return []
    sw = chain.parse_swap(tx, wallet)
    if not sw or sw.get("mint") in STABLES or sw.get("mint") == chain.WSOL:
        return []
    sol, tok = abs(sw.get("sol") or 0), abs(sw.get("tokens") or 0)
    venue = venues_in(tx)
    if sol < 0.01 and venue != "transfer":
        # 09-03: 6Ys82T6Q26 buys coins WITH USDC through Jupiter -> its own SOL delta is 0.002 (rent)
        # while the pool's WSOL vault moved 0.98. The venue event's user is the router, not the
        # wallet, so the SOL leg = the largest WSOL balance change in the transaction.
        sol = max(sol, wsol_leg(tx))
    if sol <= 0 or tok <= 0:
        return []
    return [(sw.get("side"), sw.get("mint"), sol, tok, sol / tok, venue, "payer")]


VENUE_PROGRAMS = {
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "pump",
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA": "pswap",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "rayv4",
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C": "raycpmm",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "rayclmm",
    "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj": "raylaunch",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": "dlmm",
    "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN": "dbc",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "orca",
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "jup",
}


def venues_in(tx):
    """venue programs invoked anywhere in the transaction (top-level + inner), else 'transfer'"""
    ids = set()
    msg = (tx.get("transaction") or {}).get("message") or {}
    for ix in msg.get("instructions") or []:
        ids.add(ix.get("programId"))
    for inner in (tx.get("meta") or {}).get("innerInstructions") or []:
        for ix in inner.get("instructions") or []:
            ids.add(ix.get("programId"))
    v = sorted({VENUE_PROGRAMS[i] for i in ids if i in VENUE_PROGRAMS})
    return "+".join(v) if v else "transfer"


def wsol_leg(tx):
    """largest WSOL balance change of any account in the transaction (the pool vault's SOL leg)"""
    meta = tx.get("meta") or {}
    pre = {b.get("accountIndex"): float(b["uiTokenAmount"]["uiAmount"] or 0) for b in meta.get("preTokenBalances", []) if b.get("mint") == chain.WSOL}
    post = {b.get("accountIndex"): float(b["uiTokenAmount"]["uiAmount"] or 0) for b in meta.get("postTokenBalances", []) if b.get("mint") == chain.WSOL}
    return max([abs(post.get(k, 0) - pre.get(k, 0)) for k in set(pre) | set(post)] or [0.0])


def pull(wallet, days=180, out=None, shard=(0, 1)):
    out = out or os.path.join(os.path.expanduser("~"), "lab", "deep", "deep_%s.db" % wallet[:10])
    os.makedirs(os.path.dirname(out), exist_ok=True)
    c = sqlite3.connect(out)
    c.execute("CREATE TABLE IF NOT EXISTS trades(sig TEXT PRIMARY KEY, wallet TEXT, ts INTEGER, side TEXT, mint TEXT, sol REAL, tok REAL, price REAL, venue TEXT, src TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS seen(sig TEXT PRIMARY KEY, ts INTEGER)")       # fetched, not a trade
    c.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
    have = {r[0] for r in c.execute("SELECT sig FROM trades")} | {r[0] for r in c.execute("SELECT sig FROM seen")}
    t0 = time.time()
    sigs, capped = walk_signatures(wallet, days)
    i, n = shard
    mine = [(s, t) for s, t in sigs if n == 1 or int(hashlib.md5(s.encode()).hexdigest(), 16) % n == i]
    todo = [(s, t) for s, t in mine if s not in have]
    span = ("%.0f d deep" % ((time.time() - min(t for _, t in sigs)) / 86400)) if sigs else "empty"
    print("%s: %d signatures in %d d (%s), shard %d/%d: %d, to fetch %d" % (
        wallet[:10], len(sigs), days, "CAPPED - busy wallet, use Dune" if capped else span, i, n, len(mine), len(todo)), flush=True)
    c.execute("INSERT OR REPLACE INTO meta VALUES('capped', ?)", ("1" if capped else "0",))
    c.execute("INSERT OR REPLACE INTO meta VALUES('sigs_180d', ?)", (str(len(sigs)),))
    c.commit()
    if capped:
        c.close()
        return {"wallet": wallet, "capped": True, "sigs": len(sigs)}
    ts_of = dict(todo)
    st = {"event": 0, "payer": 0, "null": 0, "n": 0}
    c.execute("CREATE TABLE IF NOT EXISTS pools(pool TEXT PRIMARY KEY, mint TEXT, flip INTEGER)")
    pcache = {r[0]: ((r[1], r[2]) if r[1] else None) for r in c.execute("SELECT pool, mint, flip FROM pools")}

    def resolve(pool):
        if pool not in pcache:
            pcache[pool] = pool_lookup(pool)
            hit = pcache[pool]
            c.execute("INSERT OR REPLACE INTO pools VALUES(?,?,?)", (pool, hit[0] if hit else None, hit[1] if hit else None))
        return pcache[pool]

    def on_tx(sig, tx):
        st["n"] += 1
        if tx is None:
            st["null"] += 1
        rows = decode_rows(wallet, sig, tx, resolve)
        for side, mint, sol, tok, price, venue, src in rows:
            c.execute("INSERT OR IGNORE INTO trades VALUES(?,?,?,?,?,?,?,?,?,?)",
                      (sig, wallet, (tx or {}).get("blockTime") or ts_of.get(sig, 0), side, mint, sol, tok, price, venue, src))
            st[src] += 1
        if not rows and tx is not None:
            c.execute("INSERT OR IGNORE INTO seen VALUES(?,?)", (sig, tx.get("blockTime") or ts_of.get(sig, 0)))
        if st["n"] % 200 == 0:
            c.commit()

    fetch_all([s for s, _ in todo], on_tx, label=wallet[:10])
    c.execute("INSERT OR REPLACE INTO meta VALUES('pulled', ?)", (str(time.time()),))
    c.commit()
    total = c.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    c.close()
    return {"wallet": wallet, "sigs": len(sigs), "shard_sigs": len(mine), "fetched": st["n"], "null": st["null"],
            "event_trades": st["event"], "payer_trades": st["payer"], "total_rows": total, "secs": round(time.time() - t0), "db": out}


def merge(src_dir, dst_dir):
    os.makedirs(dst_dir, exist_ok=True)
    for f in sorted(glob.glob(os.path.join(src_dir, "deep_*.db"))):
        dst = os.path.join(dst_dir, os.path.basename(f))
        if not os.path.exists(dst):
            shutil.copy(f, dst)
            print("copied", os.path.basename(f))
            continue
        c = sqlite3.connect(dst)
        c.execute("ATTACH DATABASE ? AS s", (f,))
        before = c.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        c.execute("INSERT OR IGNORE INTO trades SELECT * FROM s.trades")
        c.execute("INSERT OR IGNORE INTO seen SELECT * FROM s.seen")
        after = c.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        c.commit()
        c.close()
        print("merged %s: %d -> %d rows" % (os.path.basename(f), before, after))


if __name__ == "__main__":
    a = sys.argv[1:]
    if a and a[0] == "--merge":
        merge(a[1], a[2])
    elif a and a[0] == "--list":
        path = a[1]
        days = int(a[a.index("--days") + 1]) if "--days" in a else 180
        outdir = a[a.index("--out") + 1] if "--out" in a else os.path.join(os.path.expanduser("~"), "lab", "deep")
        i, n = (map(int, a[a.index("--shard") + 1].split("/"))) if "--shard" in a else (0, 1)
        ws = [l.strip() for l in open(path) if l.strip() and not l.startswith("#")]
        t0, res = time.time(), []
        for w in ws:
            r = pull(w, days, os.path.join(outdir, "deep_%s.db" % w[:10]), (i, n))
            print(json.dumps(r), flush=True)
            res.append(r)
        print("DONE " + json.dumps({"wallets": len(ws), "secs": round(time.time() - t0), "rows": sum(r.get("total_rows", 0) for r in res),
                                    "capped": [r["wallet"][:10] for r in res if r.get("capped")]}), flush=True)
    else:
        w = a[0]
        d = int(a[1]) if len(a) > 1 else 180
        o = a[2] if len(a) > 2 else None
        print(json.dumps(pull(w, d, o)))
