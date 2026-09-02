"""TAPE - the whole memecoin market, live, attributed, priced, for $0.

THE THING THIS RESTS ON (measured 2026-09-01)
  pump.fun writes every trade into its program logs as an anchor event
  ("Program data: vdt/007mYe..."). Decoding it gives mint, user, SOL, tokens,
  side and the curve reserves - i.e. the exact price - with NO getTransaction.
  Cross-checked against the chain: user == feePayer, mint in the tx.
  The free public websocket delivers ~440 messages/s with the first one 0.4s
  after subscribing. So the tape needs zero RPC and cannot run out of quota.

  PumpSwap (graduated coins) emits BuyEvent/SellEvent the same way; a newer
  layout variant garbles some rows, so those pass a sanity gate and the rest
  are dropped rather than stored wrong.

WHAT IT PRODUCES (tape.db)
  trades   every decoded trade, rolling 24h      -> HUNT reads this
  coins    per-mint activity aggregates          -> HUNT + paper pricing
  meta     heartbeat, rates                       -> FORGE guard + TUI
and for wallets the lab cares about (candidate/watched in copybot.db), every
trade is ALSO appended permanently to copybot.trades + signals(ts_seen), so a
wallet's history keeps growing for as long as it matters.

  python tape.py            run forever (FORGE's tape_guard job starts this)
  python tape.py --test 15  run 15s and print what it saw
"""
import base64
import hashlib
import json
import os
import sqlite3
import struct
import sys
import threading
import time
from collections import defaultdict

from websocket import WebSocketApp

import pools
import store

HERE = os.path.dirname(os.path.abspath(__file__))
# ON THE SSD. E: is a 2 TB Seagate HDD; the tape's random index writes
# there took 13-18 s per one-second flush (measured 20:46 09-01) and every
# heartbeat queued behind them. C: is the Samsung 860 EVO SSD.
DB = os.environ.get("TAPE_DB", r"C:\Users\Brady\lab\tape.db")
WS = os.environ.get("WS_URL", "wss://api.mainnet-beta.solana.com")
PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PSWAP = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
KEEP_H = 12                 # rolling window for the raw tape (pump only)
FLUSH_S = 1.0
PRUNE_S = 600

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades(
  sig TEXT, ts INTEGER, seen REAL, venue TEXT, mint TEXT, user TEXT,
  side TEXT, sol REAL, tok REAL, price REAL, PRIMARY KEY(sig, mint, user));
CREATE INDEX IF NOT EXISTS tr_user ON trades(user, ts);
CREATE INDEX IF NOT EXISTS tr_mint ON trades(mint, ts);
CREATE INDEX IF NOT EXISTS tr_ts ON trades(ts);
CREATE INDEX IF NOT EXISTS tr_seen ON trades(seen);
CREATE TABLE IF NOT EXISTS coins(
  mint TEXT PRIMARY KEY, venue TEXT, first_seen INTEGER, last_seen INTEGER,
  n_trades INTEGER DEFAULT 0, vol_sol REAL DEFAULT 0, last_price REAL);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
"""


def disc(name):
    return hashlib.sha256(f"event:{name}".encode()).digest()[:8]


D_TRADE, D_BUY, D_SELL = disc("TradeEvent"), disc("BuyEvent"), disc("SellEvent")
ALPH = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58(b):
    n = int.from_bytes(b, "big")
    out = b""
    while n:
        n, r = divmod(n, 58)
        out = ALPH[r:r + 1] + out
    return (ALPH[0:1] * (len(b) - len(b.lstrip(b"\0"))) + out).decode()


def db():
    c = sqlite3.connect(DB, timeout=60)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c


def init():
    c = db()
    c.executescript(SCHEMA)
    c.commit()
    c.close()


def meta_set(c, k, v):
    c.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (k, str(v)))


# ------------------------------------------------------------- decoders
def decode_pump(b, fresh=True):
    """pump.fun TradeEvent -> dict, or None. Layout verified on-chain 09-01."""
    if b[:8] != D_TRADE or len(b) < 129:
        return None
    mint = b58(b[8:40])
    sol, tok = struct.unpack_from("<QQ", b, 40)
    is_buy = b[56]
    user = b58(b[57:89])
    ts, vsol, vtok = struct.unpack_from("<qQQ", b, 89)
    if not vtok or not tok:
        return None
    # a layout variant (147-byte TradeEvent, 02:40 09-02) put garbage here and
    # one row with ts ~ 9e18 poisoned the lab's clock offset for 20 minutes
    if fresh and abs(time.time() - ts) > 600:
        return None
    return {"venue": "pump", "mint": mint, "user": user,
            "side": "buy" if is_buy else "sell", "sol": sol / 1e9,
            "tok": tok / 1e6, "price": (vsol / 1e9) / (vtok / 1e6), "ts": ts}


def decode_pswap(b, fresh=True):
    """PumpSwap Buy/SellEvent -> dict or None. Best effort: a newer layout
    variant shifts the fields, so anything that fails the sanity gate is
    dropped instead of stored wrong. pool -> mint is resolved lazily."""
    if len(b) < 8 + 14 * 8 + 64:
        return None
    if b[:8] == D_BUY:
        ts, base, _mq, _ubr, _uqr, pbr, pqr, quote = struct.unpack_from("<qQQQQQQQ", b, 8)
        side = "buy"
    elif b[:8] == D_SELL:
        ts, base, _mq, _ubr, _uqr, pbr, pqr, quote = struct.unpack_from("<qQQQQQQQ", b, 8)
        side = "sell"
    else:
        return None
    off = 8 + 14 * 8
    pool, user = b58(b[off:off + 32]), b58(b[off + 32:off + 64])
    if base <= 0 or quote <= 0 or pbr <= 0 or pqr <= 0:
        return None
    price = (pqr / 1e9) / (pbr / 1e6)
    trade_px = (quote / 1e9) / (base / 1e6)
    # sanity: the trade's own price must sit near the pool price, and the
    # timestamp must be current - a shifted layout fails both
    if not (0.2 <= trade_px / price <= 5.0) or (fresh and abs(time.time() - ts) > 600):
        return None
    return {"venue": "pswap", "mint": "pool:" + pool, "user": user, "side": side,
            "sol": quote / 1e9, "tok": base / 1e6, "price": price, "ts": ts}


# ------------------------------------------------------------------ tape
class Tape:
    def __init__(self, seconds=None):
        init()
        # no store.init() here: that is an executescript on copybot.db (a
        # write lock) and the tape must never wait on that database
        self.buf = []
        self.coins = {}
        self.lock = threading.Lock()
        self.n = defaultdict(int)
        self.t0 = time.time()
        self.seconds = seconds
        self.subs = {}
        self.tracked = set()
        self.tracked_at = 0
        self.pools = {}                 # pool -> coin mint, from pools.py's table
        self.pools_at = 0
        self.stop = False
        # ONE persistent writer. Opening a fresh connection every second into a
        # multi-hundred-MB WAL database (header read + PRAGMAs + shm map each
        # time) is a real cost, and the checkpoint work lands on whichever
        # commit crosses the threshold - keep it on one connection we control.
        # created INSIDE the ticker thread (see run): sqlite connections are
        # bound to the thread that opened them
        self.c = None
        self._load_tracked()

    def _load_tracked(self):
        """Wallets the lab cares about - READ-ONLY, short timeout, never a lock.

        ⛔ This used to open copybot.db normally and run a CREATE TABLE IF NOT
        EXISTS - a write-lock statement - every 60 s from the ticker thread.
        Whenever a lab job held that database the ticker stalled for the full
        30 s busy timeout, the heartbeat went stale, and the guard killed a
        healthy tape. The tape now never takes any lock on copybot.db.
        """
        try:
            c = sqlite3.connect(f"file:{store.DB}?mode=ro", uri=True, timeout=2)
            self.tracked = {r[0] for r in c.execute(
                "SELECT address FROM lab_wallets WHERE state IN ('candidate','watched')")}
            c.close()
        except Exception:
            pass                      # keep the last set; try again next minute
        self.tracked_at = time.time()

    def on_open(self, ws):
        ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                            "params": [{"mentions": [PUMP]}, {"commitment": "processed"}]}))
        ws.send(json.dumps({"jsonrpc": "2.0", "id": 2, "method": "logsSubscribe",
                            "params": [{"mentions": [PSWAP]}, {"commitment": "processed"}]}))

    def on_message(self, ws, raw):
        try:
            m = json.loads(raw)
        except Exception:
            return
        if "result" in m and "id" in m:
            self.subs[m["result"]] = m["id"]
            return
        v = ((m.get("params") or {}).get("result") or {}).get("value") or {}
        if not v or v.get("err"):
            return
        seen = time.time()
        sig = v.get("signature")
        self.n["msgs"] += 1
        for ln in v.get("logs") or []:
            if not ln.startswith("Program data: "):
                continue
            try:
                b = base64.b64decode(ln[14:])
            except Exception:
                continue
            d = decode_pump(b) or decode_pswap(b)
            if not d:
                continue
            d["sig"] = sig
            d["seen"] = seen
            self.n[d["venue"]] += 1
            # PumpSwap rows (~80% of the firehose) are kept now: flush() joins
            # each pool to its coin through pools.py (resolved once, cached),
            # stores them keyed by the real mint with a 1 h retention, and
            # queues unknown pools for the resolver. The earlier 354 MB/hour
            # blow-up was the HDD + 24 h retention, not PumpSwap itself.
            with self.lock:
                self.buf.append(d)
        if self.seconds and seen - self.t0 > self.seconds:
            ws.close()

    def flush(self):
        with self.lock:
            rows, self.buf = self.buf, []
        if not rows:
            return
        # PumpSwap rows name a POOL; join to the coin through the resolver's
        # cache. Unknown pools are queued (pool_resolve fills them within a
        # minute) and this tick's rows for them are dropped - a trade we cannot
        # attribute to a coin is worth nothing to hunt or paper.
        pw = [r for r in rows if r["venue"] == "pswap"]
        if pw:
            miss = {r["mint"][5:] for r in pw if r["mint"][5:] not in self.pools}
            if miss:
                try:
                    pools.enqueue(miss)
                except Exception:
                    pass
            kept = []
            for r in pw:
                hit = self.pools.get(r["mint"][5:])
                if not hit:
                    continue
                mint, flip = hit
                if flip:
                    # reversed pool: decode_pswap read SOL as the coin leg and
                    # the coin as the SOL leg (9 vs 6 decimals) - swap back
                    sol_true = r["tok"] / 1000.0
                    tok_true = r["sol"] * 1000.0
                    if sol_true <= 0 or tok_true <= 0 or r["price"] <= 0:
                        continue
                    r["sol"], r["tok"] = sol_true, tok_true
                    r["price"] = 1e-6 / r["price"]
                r["mint"] = mint
                kept.append(r)
            self.n["pswap_stored"] += len(kept)
            rows = [r for r in rows if r["venue"] != "pswap"] + kept
            if not rows:
                return
        c = self.c
        c.executemany(
            "INSERT OR IGNORE INTO trades VALUES(?,?,?,?,?,?,?,?,?,?)",
            [(r["sig"], r["ts"], r["seen"], r["venue"], r["mint"], r["user"],
              r["side"], r["sol"], r["tok"], r["price"]) for r in rows])
        agg = {}
        for r in rows:
            a = agg.setdefault(r["mint"], [r["venue"], r["ts"], r["ts"], 0, 0.0, r["price"]])
            a[1] = min(a[1], r["ts"])
            a[2] = max(a[2], r["ts"])
            a[3] += 1
            a[4] += r["sol"]
            a[5] = r["price"]
        c.executemany(
            "INSERT INTO coins(mint,venue,first_seen,last_seen,n_trades,vol_sol,last_price)"
            " VALUES(?,?,?,?,?,?,?) ON CONFLICT(mint) DO UPDATE SET"
            " last_seen=excluded.last_seen, n_trades=n_trades+excluded.n_trades,"
            " vol_sol=vol_sol+excluded.vol_sol, last_price=excluded.last_price",
            [(m, a[0], a[1], a[2], a[3], a[4], a[5]) for m, a in agg.items()])
        meta_set(c, "flushed", time.time())
        meta_set(c, "msgs", self.n["msgs"])
        meta_set(c, "pump", self.n["pump"])
        meta_set(c, "pswap", self.n["pswap"])
        meta_set(c, "pswap_stored", self.n["pswap_stored"])
        meta_set(c, "since", self.t0)
        c.commit()
        # ⛔ The tape touches NO other database. It used to copy tracked
        # wallets' trades into copybot.db here, and any lock on that file
        # (FORGE jobs, the TUI, a Claude session) stalled this flush for up
        # to 30 s, starved the heartbeat, and made the guard kill a healthy
        # tape once a minute. lab.bank() now pulls from tape.db instead.
        self.n["mine"] += sum(1 for r in rows if r["user"] in self.tracked)

    def _stop_requested(self):
        """Graceful swap: `python tape.py --stop` sets meta.stop; we finish the
        current flush, close the socket and exit, so the next process never
        replays our WAL (a hard kill cost the successor ~50 s)."""
        try:
            r = self.c.execute("SELECT value FROM meta WHERE key='stop'").fetchone()
            if r and r[0] == "1":
                self.c.execute("DELETE FROM meta WHERE key='stop'")
                self.c.commit()
                return True
        except Exception:
            pass
        return False

    def prune(self):
        """Drop rows older than KEEP_H in bounded batches, then checkpoint.

        One giant DELETE writes the whole deleted range into the WAL at once;
        batches keep each transaction small, and a PASSIVE checkpoint (never
        blocks anyone) drains the WAL at a moment we choose instead of inside
        a random one-second flush.
        """
        c = self.c
        # pump.fun rows: KEEP_H (hunt reads them). PumpSwap rows: 1 h - they
        # only serve the paper book's fills, which happen within minutes of a
        # signal, and they are ~80% of the volume.
        for venue, keep_h in (("pump", KEEP_H), ("pswap", 1)):
            cut = int(time.time() - keep_h * 3600)
            for _ in range(50):
                n = c.execute("DELETE FROM trades WHERE rowid IN "
                              "(SELECT rowid FROM trades WHERE venue=? AND ts < ? LIMIT 20000)",
                              (venue, cut)).rowcount
                c.commit()
                if n < 20000:
                    break
        try:
            c.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except Exception:
            pass

    def run(self):
        self.last_prune = time.time()

        def timed(label, fn):
            """Run a phase; log it if it took more than 2 s. This is how the
            next stall gets a name instead of a guess."""
            t = time.time()
            try:
                fn()
            except Exception as e:
                print(f"{time.strftime('%H:%M:%S')} {label}: {repr(e)[:90]}", flush=True)
            dt = time.time() - t
            if dt > 2:
                print(f"{time.strftime('%H:%M:%S')} SLOW {label} {dt:.1f}s", flush=True)

        def heartbeat():
            """Its OWN thread and its OWN connection. It does nothing else, so
            no flush, prune, reload or checkpoint can ever sit in front of it.
            `heartbeat` = the process is alive and sqlite is writable;
            `flushed` (set by the worker) = data is actually landing."""
            hc = db()
            while not self.stop:
                try:
                    meta_set(hc, "heartbeat", time.time())
                    hc.commit()
                except Exception as e:
                    print(f"{time.strftime('%H:%M:%S')} heartbeat: {repr(e)[:60]}", flush=True)
                time.sleep(1.0)

        def load_pools():
            """pool -> mint cache from the resolver's table (a few thousand rows)."""
            try:
                c = db()
                self.pools = {r["pool"]: (r["mint"], r["flip"] or 0)
                              for r in c.execute("SELECT pool, mint, flip FROM pools")}
                c.close()
            except Exception:
                pass
            self.pools_at = time.time()

        def worker():
            load_pools()
            self.c = db()
            # 256 MB page cache: hot index pages stay in RAM, so a flush is
            # memory work and the disk only sees sequential WAL appends
            self.c.execute("PRAGMA cache_size=-262144")
            self.c.execute("PRAGMA temp_store=MEMORY")
            # checkpoints happen HERE, on our schedule, never inside a flush
            self.c.execute("PRAGMA wal_autocheckpoint=0")
            nflush = 0
            while not self.stop:
                time.sleep(FLUSH_S)
                timed("flush", self.flush)
                nflush += 1
                if nflush % 120 == 0:            # ~every 2 min, passive, never blocks
                    # PASSIVE never blocks but also never shrinks the WAL while
                    # readers are constantly on it (463 MB by 01:20 09-02);
                    # every 10th time ask for TRUNCATE (bounded: RESTART/TRUNCATE
                    # wait for readers only up to the busy timeout)
                    self.ckpt_n = getattr(self, "ckpt_n", 0) + 1
                    mode = "TRUNCATE" if self.ckpt_n % 10 == 0 else "PASSIVE"
                    timed("checkpoint",
                          lambda: self.c.execute(f"PRAGMA wal_checkpoint({mode})"))
                if nflush % 10 == 0 and self._stop_requested():
                    print(f"{time.strftime('%H:%M:%S')} stop requested - exiting cleanly",
                          flush=True)
                    self.stop = True
                    try:
                        self.app.close()
                    except Exception:
                        pass
                if time.time() - self.tracked_at > 60:
                    timed("load_tracked", self._load_tracked)
                if time.time() - self.pools_at > 60:
                    timed("load_pools", load_pools)
                if time.time() - self.last_prune > PRUNE_S:
                    timed("prune", self.prune)
                    self.last_prune = time.time()

        threading.Thread(target=heartbeat, daemon=True).start()
        threading.Thread(target=worker, daemon=True).start()
        while not self.stop:
            try:
                self.app = WebSocketApp(WS, on_open=self.on_open, on_message=self.on_message,
                                        on_error=lambda ws, e: print("ws:", repr(e)[:80], flush=True))
                self.app.run_forever(ping_interval=30)
            except KeyboardInterrupt:
                break
            if self.seconds or self.stop:
                break
            print("tape: reconnecting in 1s", flush=True)
            time.sleep(1)
        self.stop = True
        time.sleep(FLUSH_S * 2)      # let the ticker drain its last batch


if __name__ == "__main__":
    secs = None
    if "--stop" in sys.argv:
        init()
        c = db()
        meta_set(c, "stop", "1")
        c.commit()
        c.close()
        print("stop requested; the running tape exits within ~10s", flush=True)
        sys.exit()
    if "--test" in sys.argv:
        i = sys.argv.index("--test")
        secs = int(sys.argv[i + 1]) if len(sys.argv) > i + 1 else 15
    t = Tape(secs)
    print(f"TAPE online -> {DB}" + (f" (test {secs}s)" if secs else ""), flush=True)
    t.run()
    el = time.time() - t.t0
    print(f"{el:.0f}s: {t.n['msgs']} msgs, pump.fun {t.n['pump']} trades, "
          f"PumpSwap {t.n['pswap']} trades, {t.n['mine']} for tracked wallets "
          f"({(t.n['pump'] + t.n['pswap']) / max(el, 1):.0f} trades/s)")
