"""EXECD v2 - the copy EXECUTOR, local-first. Dry-run: measures how fast and
how right we are, on the wallets we copy, with zero paid infrastructure.

WHAT CHANGED (09-01 night, "kill the Jupiter hop")
  v1 detected a whale trade, then asked Jupiter for a quote (79-152 ms) and
  would have asked it to build the transaction (83 ms). v2 does neither on
  the critical path:
    1. three sockets to the public node, first arrival wins (12 ms gain)
    2. decode the whale's event -> mint, reserves, fee bps, creator, fee
       recipient, token program: everything a swap needs
    3. PRICE LOCALLY from those reserves (microseconds; proven exact against
       the program's own simulated output on both venues, 09-01 23:40)
    4. BUILD the swap transaction locally (0.3 ms) with txbuild
  From "message on the wire" to "signed-shape transaction in hand" is now
  well under a millisecond. What remains is physics: the block itself
  (~0.28 s) and landing (1-2 slots), see STATE.md 5e.

STILL DRY, AND MEASURING
  Off the critical path, every whale trade is then (a) SIMULATED as the
  whale himself - sigVerify off lets his funded wallet be the fee payer -
  so the program tells us whether our exact transaction would have
  executed and how many tokens it returns (`sim_ok`, `sim_tok`), and
  (b) quoted on Jupiter for comparison (`jup_tok`). By morning `exec_sim`
  holds a proof per trade: our local price vs the program's truth vs
  Jupiter, and the milliseconds each path took.

WHAT IT REFUSES TO DO
  Sign or send. That needs `execd_key.txt` (a dedicated hot wallet Brady
  creates and funds), `forge.py arm N`, and `forge.py live` - and the send
  path gets its own dry-run session first. Brady arms real funds.

  python execd.py             run (FORGE's execd_guard starts this)
  python execd.py --test 60   run 60 s and print what it saw
"""
import base64
import collections
import json
import os
import sqlite3
import sys
import threading
import time

import httpx
from solders.pubkey import Pubkey
from websocket import WebSocketApp

import generic
import pools
import store
import tape
import txbuild as tb

HERE = os.path.dirname(os.path.abspath(__file__))
WS = os.environ.get("WS_URL", "wss://api.mainnet-beta.solana.com")
RPC = "https://api.mainnet-beta.solana.com"
RPC2 = "https://solana-rpc.publicnode.com"      # off-path work goes here first: beta 429s at 80 wallets
JUP = "https://lite-api.jup.ag/swap/v1/quote"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) wallet-lab/1.0", "Accept": "application/json"}
WSOL = "So11111111111111111111111111111111111111112"
SIZE_SOL = 0.05          # dry-run size per copied buy
SLIPPAGE_BPS = 300
SOCKETS = 5              # each socket carries a slice of the watch list (~32 subscriptions at 160 wallets)
FORGE_DB = os.path.normpath(os.path.join(HERE, "..", "forge", "forge.db"))
KEYFILE = os.path.join(HERE, "execd_key.txt")

SCHEMA = """
CREATE TABLE IF NOT EXISTS exec_sim(
  sig TEXT PRIMARY KEY, wallet TEXT, mint TEXT, side TEXT, venue TEXT,
  t_chain INTEGER, t_seen REAL, t_quote REAL, det_s REAL, quote_ms INTEGER,
  their_sol REAL, their_tok REAL, whale_price REAL, our_sol REAL, our_tok REAL,
  our_price REAL, gap_pct REAL, impact_pct REAL, route TEXT, mode TEXT, note TEXT);
"""
EXTRA = [("local_ms", "REAL"), ("build_ms", "REAL"), ("tx_bytes", "INTEGER"), ("sim_ok", "INTEGER"),
         ("sim_err", "TEXT"), ("sim_units", "INTEGER"), ("sim_tok", "REAL"), ("sim_ratio", "REAL"),
         ("sim_ms", "INTEGER"), ("jup_tok", "REAL"), ("jup_ms", "INTEGER"), ("jup_ratio", "REAL"),
         ("fee_bps", "INTEGER"), ("cashback", "INTEGER"), ("slot", "INTEGER"), ("sim_as", "TEXT"),
         ("res_a", "REAL"), ("res_b", "REAL"),     # post-trade reserves for the paper book's exact fills
         ("lat_slot", "REAL"), ("cu_price", "INTEGER"), ("jito_tip", "INTEGER")]                     # seconds after the node's first-shred notice for the trade's slot (the honest ruler)
# simulation-only stand-in payer for buys when the whale cannot afford our copy
# (an exchange hot wallet with millions of SOL; sigVerify is off in simulation,
# nothing is ever signed or sent)
RICH_PAYER = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"


def kv(key, val=None):
    c = store.db()
    if val is not None:
        c.execute("INSERT OR REPLACE INTO lab_kv VALUES(?,?)", (key, str(val)))
        c.commit()
        c.close()
        return val
    r = c.execute("SELECT value FROM lab_kv WHERE key=?", (key,)).fetchone()
    c.close()
    return r[0] if r else None


def forge_kv(key, default=None):
    try:
        c = sqlite3.connect(f"file:{FORGE_DB}?mode=ro", uri=True, timeout=2)
        r = c.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        c.close()
        return r[0] if r else default
    except Exception:
        return default


def may_go_live():
    """All three locks must be open. None are."""
    if not os.path.exists(KEYFILE):
        return False
    if forge_kv("dry_run", "1") == "1":
        return False
    try:
        return time.time() < float(forge_kv("armed_until", "0"))
    except Exception:
        return False


class Execd:
    def __init__(self, seconds=None):
        store.init()
        c = store.db()
        c.executescript(SCHEMA)
        have = {r[1] for r in c.execute("PRAGMA table_info(exec_sim)")}
        c.execute("CREATE TABLE IF NOT EXISTS precursors(sig TEXT, wallet TEXT, mint TEXT, kind TEXT, t_seen REAL, slot INTEGER, PRIMARY KEY(sig, mint))")
        for col, typ in EXTRA:
            if col not in have:
                c.execute(f"ALTER TABLE exec_sim ADD COLUMN {col} {typ}")
        c.commit()
        c.close()
        self.seconds = seconds
        self.t0 = time.time()
        self.watched = {}
        self.pools = {}
        self.seen = set()
        self.done = set()                                  # signatures already recorded (either feed)
        self.lat = collections.deque()                     # (arrival, arrival - block ts) for the latency floor
        self.shred_seen = {}                               # slot -> our arrival of the node's first-shred notice
        self.lock = threading.Lock()
        self.stop = False
        self.n = {"msgs": 0, "trades": 0, "local": 0, "sim_ok": 0, "sim_fail": 0, "jup": 0, "skipped": 0, "dup": 0}
        self.apps = []
        self.http = httpx.Client(headers=UA, timeout=10)      # keep-alive: warm quotes/sims
        self.blockhash = None
        self.sim_lock = threading.Lock()
        self.wlist = []
        self.submap = {}                                   # id(ws) -> {server subscription id: wallet}
        self.reload()

    def reload(self):
        try:
            c = sqlite3.connect(f"file:{store.DB}?mode=ro", uri=True, timeout=2)
            # 'measure' = execd-only wallets (e.g. known scalpers): priced, built and
            # simulated like the rest so their real copy gap gets measured, but the
            # paper book does not copy them
            # plus strong candidates: watching is free, so every gate-passer gets
            # its copy gap measured before capital ever touches it
            self.watched = {r[0]: i for i, r in enumerate(c.execute(
                "SELECT address FROM lab_wallets WHERE state IN ('watched','measure') "
                "OR (state='candidate' AND (score > 0.3 OR note LIKE 'kol:famous%')) "
                "ORDER BY CASE WHEN note LIKE 'kol:famous%' THEN 1 ELSE 0 END DESC, score DESC LIMIT 160"))}
            self.wlist = list(self.watched)          # subscription id i+1 -> wallet (same order on every socket)
            c.close()
        except Exception:
            pass
        try:
            c = tape.db()
            self.pools = {r["pool"]: (r["mint"], r["flip"] or 0) for r in c.execute("SELECT pool, mint, flip FROM pools")}
            c.close()
        except Exception:
            pass
        self.reloaded = time.time()

    # ------------------------------------------------------------ sockets
    def _sub(self, ws):
        # 92 wallets x 3 duplicate sockets got the node closing us with 1013
        # "Rate limit" (02:45 09-02). Each socket now carries a SLICE of the
        # list (socket k: wallets k, k+3, ...) - no duplication, a third of the
        # subscriptions per connection; the 12 ms first-arrival race is gone
        # but the feed stays up. Request id i+1 still maps to wlist[i].
        k = getattr(ws, "_slice", 0)
        for i, w in enumerate(self.wlist):
            if i % SOCKETS != k:
                continue
            ws.send(json.dumps({"jsonrpc": "2.0", "id": i + 1, "method": "logsSubscribe",
                                "params": [{"mentions": [w]}, {"commitment": "processed"}]}))

    def on_message(self, ws, raw):
        if '"logsNotification"' not in raw[:80]:
            # subscribe replies: {"result": <server subscription id>, "id": <our request id>}
            # - the notification's `subscription` is the SERVER id, per socket
            if '"result"' in raw[:60]:
                try:
                    j = json.loads(raw)
                    rid, srv = j.get("id"), j.get("result")
                    if isinstance(rid, int) and isinstance(srv, int) and 0 < rid <= len(self.wlist):
                        self.submap.setdefault(id(ws), {})[srv] = self.wlist[rid - 1]
                except Exception:
                    pass
            return
        seen = time.time()
        try:
            p = json.loads(raw)["params"]
            m = p["result"]
            sub_id = p.get("subscription")
            v = m["value"]
            slot = m["context"]["slot"]
        except Exception:
            return
        if v.get("err"):
            return
        self.n["msgs"] += 1
        sig = v.get("signature")
        if not sig:
            return
        with self.lock:
            if sig in self.seen:
                self.n["dup"] += 1
                return
            self.seen.add(sig)
            if len(self.seen) > 20000:
                self.seen = set(list(self.seen)[-5000:])
        logs = v.get("logs") or []
        handled = False
        for ln in logs:
            if not ln.startswith("Program data: "):
                continue
            try:
                b = base64.b64decode(ln[14:])
            except Exception:
                continue
            d = tape.decode_pump(b) or tape.decode_pswap(b)
            if not d or d["user"] not in self.watched:
                continue
            d.update({"sig": sig, "seen": seen, "slot": slot, "raw": b, "logs": logs})
            self.n["trades"] += 1
            threading.Thread(target=self.act, args=(d,), daemon=True).start()
            handled = True
            break
        if not handled:
            # not a pump venue: the subscription id tells us which wallet this is;
            # a cheap log filter keeps spam (strangers' token-account creations that
            # mention the wallet) off the RPC, then the tx's own balance deltas
            # tell the truth on any DEX (generic.py)
            venue = generic.filter(logs)
            # with a live gRPC stream the wallet's transaction arrives whole (no fetch) ~0.2 s
            # BEFORE this websocket notice; the fetch here is only a fallback for a dead stream
            if venue and time.time() - getattr(self, "grpc_last", 0) < 5:
                self.n["g_skipped_grpc"] = self.n.get("g_skipped_grpc", 0) + 1
                venue = None
            if venue:
                wallet = self.submap.get(id(ws), {}).get(sub_id)
                if wallet:
                    self.n["generic"] = self.n.get("generic", 0) + 1
                    threading.Thread(target=self.act_generic, args=(wallet, sig, slot, seen, venue), daemon=True).start()
        if self.seconds and seen - self.t0 > self.seconds:
            ws.close()

    # ------------------------------------------------------------ pricing + build (the critical path)
    def price_and_build(self, d, as_user=None):
        """local price + local transaction. Returns (row-dict, tx or None).
        `as_user` swaps the transaction's user (simulation-only: a funded
        payer for buys, a current holder for sells); pricing is unchanged."""
        user = Pubkey.from_string(as_user or d["user"])
        spend = int(SIZE_SOL * 1e9)
        out = {"fee_bps": None, "cashback": 0, "route": "local:" + d["venue"]}
        if d["venue"] == "pump":
            e = tb.parse_trade_event(d["raw"])
            mint, creator = Pubkey.from_string(e["mint"]), Pubkey.from_string(e["creator"])
            fr = Pubkey.from_string(e["fee_recipient"])
            prog = tb.token_program_from_logs(d["logs"])
            out["fee_bps"] = e["fee_total_bps"]
            out["res_a"], out["res_b"] = e["vsol"], e["vtok"]          # post-trade virtual reserves
            whale_px = (e["sol"] / 1e9) / (e["tok"] / 1e6) if e["tok"] else 0
            if d["side"] == "buy":
                tok = tb.pump_tokens_for_sol(e["vsol"], e["vtok"], spend, e["fee_total_bps"])
                out.update({"our_sol": SIZE_SOL, "our_tok": tok / 1e6, "our_price": SIZE_SOL / (tok / 1e6) if tok else 0})
                ixs = [tb.ata_create_idempotent(user, user, mint, prog),
                       tb.pump_buy_exact_sol(user, mint, creator, spend, max(1, tok * (10_000 - SLIPPAGE_BPS) // 10_000),
                                             prog, fr, e["mayhem"])]
            else:
                qty = int(min(e["tok"], (SIZE_SOL / whale_px) * 1e6)) if whale_px else 0
                if qty <= 0:
                    return None, None
                sol = tb.pump_sol_for_tokens(e["vsol"], e["vtok"], qty, e["fee_total_bps"])
                out.update({"our_sol": sol / 1e9, "our_tok": qty / 1e6, "our_price": (sol / 1e9) / (qty / 1e6)})
                ixs = [tb.pump_sell(user, mint, creator, qty, max(1, sol * (10_000 - SLIPPAGE_BPS) // 10_000),
                                    prog, fr, e["mayhem"])]
                if as_user == RICH_PAYER:
                    # simulation stand-in holds no tokens: buy them first in the same tx
                    ixs = [tb.ata_create_idempotent(user, user, mint, prog),
                           tb.pump_buy(user, mint, creator, qty * 102 // 100, max(spend, sol * 3), prog, fr, e["mayhem"])] + ixs
            out["whale_px"] = whale_px
        else:
            e = tb.parse_pamm_event(d["raw"])
            hit = self.pools.get(e["pool"])
            if not hit:
                r = self.resolve_fast(e["pool"])      # one warm RPC read (~40 ms), cached for good
                if r:
                    hit = (r[0], r[2])
                    self.pools[e["pool"]] = hit
                    try:
                        pools.remember(e["pool"], r[0], r[1], r[2])
                    except Exception:
                        pass
            if not hit:
                out["note"] = "pool unresolved"
                return out, None
            mint_s, flip = hit
            if flip:
                out["note"] = "reversed pool (WSOL base) - pricing only"
            mint = Pubkey.from_string(mint_s)
            cc, pfr = Pubkey.from_string(e["coin_creator"]), Pubkey.from_string(e["protocol_fee_recipient"])
            cb = e.get("cashback_bps", 0) > 0
            out["fee_bps"], out["cashback"] = e["fee_total_bps"], int(cb)
            # the event reports PRE-trade reserves: apply the whale's own trade (proven exact 09-01 23:40)
            if e["side"] == "buy":
                pbr, pqr = e["pbr"] - e["base"], e["pqr"] + e["quote"]
            else:
                pbr, pqr = e["pbr"] + e["base"], e["pqr"] - e["quote"]
            q_eff = pqr + e["vquote"]
            out["res_a"], out["res_b"] = pbr, q_eff                     # post-trade base / effective quote
            if flip:
                # base = WSOL, quote = token: the coin's price is base/quote
                whale_px = (e["base"] / 1e9) / (e["quote"] / 1e6) if e["quote"] else 0
                if d["side"] == "buy":
                    tok = tb.pamm_quote_for_base(pbr, q_eff, spend, e["fee_total_bps"])
                    out.update({"our_sol": SIZE_SOL, "our_tok": tok / 1e6, "our_price": SIZE_SOL / (tok / 1e6) if tok else 0})
                else:
                    qty = int(min(e["quote"], (SIZE_SOL / whale_px) * 1e6)) if whale_px else 0
                    sol = tb.pamm_base_for_quote(pbr, q_eff, qty, e["fee_total_bps"]) if qty else 0
                    out.update({"our_sol": sol / 1e9, "our_tok": qty / 1e6, "our_price": (sol / 1e9) / (qty / 1e6) if qty else 0})
                out["whale_px"] = whale_px
                return out, None
            whale_px = (e["quote"] / 1e9) / (e["base"] / 1e6) if e["base"] else 0
            prog = tb.TOKEN22 if str(tb.ata(user, mint, tb.TOKEN22)) == e["user_base_ta"] else tb.TOKEN
            if d["side"] == "buy":
                tok = tb.pamm_base_for_quote(pbr, q_eff, spend, e["fee_total_bps"])
                out.update({"our_sol": SIZE_SOL, "our_tok": tok / 1e6, "our_price": SIZE_SOL / (tok / 1e6) if tok else 0})
                ixs = tb.wrap_sol(user, spend) + ([tb.cashback_prep(user)] if cb else []) + [
                    tb.ata_create_idempotent(user, user, mint, prog),
                    tb.pamm_buy_exact_quote_in(user, Pubkey.from_string(e["pool"]), mint, cc, spend,
                                               max(1, tok * (10_000 - SLIPPAGE_BPS) // 10_000), prog,
                                               protocol_fee_recipient=pfr, cashback=cb)]
            else:
                qty = int(min(e["base"], (SIZE_SOL / whale_px) * 1e6)) if whale_px else 0
                if qty <= 0:
                    return None, None
                sol = tb.pamm_quote_for_base(pbr, q_eff, qty, e["fee_total_bps"])
                out.update({"our_sol": sol / 1e9, "our_tok": qty / 1e6, "our_price": (sol / 1e9) / (qty / 1e6)})
                pre = []
                if as_user == RICH_PAYER:
                    # simulation stand-in holds no tokens: buy them first in the same tx
                    pre = tb.wrap_sol(user, max(spend, sol * 3)) + [
                        tb.ata_create_idempotent(user, user, mint, prog),
                        tb.pamm_buy(user, Pubkey.from_string(e["pool"]), mint, cc, qty * 102 // 100, max(spend, sol * 3),
                                    prog, protocol_fee_recipient=pfr, cashback=cb)]
                ixs = pre + [tb.ata_create_idempotent(user, user, tb.WSOL)] + ([tb.cashback_prep(user)] if cb else []) + [
                    tb.pamm_sell(user, Pubkey.from_string(e["pool"]), mint, cc, qty,
                                 max(1, sol * (10_000 - SLIPPAGE_BPS) // 10_000), prog,
                                 protocol_fee_recipient=pfr, cashback=cb)]
            out["whale_px"] = whale_px
            d["mint"] = mint_s
        t = time.perf_counter()
        tx = tb.build_tx(user, ixs, self.blockhash)
        out["build_ms"] = (time.perf_counter() - t) * 1000
        out["tx_bytes"] = len(bytes(tx))
        out["ixs"] = ixs
        return out, tx

    # ------------------------------------------------------------- action
    def act(self, d):
        t_in = time.perf_counter()
        if d["venue"] == "pump":
            d["mint"] = d["mint"]
        if d["sol"] < 0.001 and d["venue"] == "pump":
            self.n["skipped"] += 1
            self.record(d, None, note="dust")
            return
        try:
            out, tx = self.price_and_build(d)
        except Exception as e:
            self.n["skipped"] += 1
            self.record(d, None, note=f"build failed {type(e).__name__}: {str(e)[:60]}")
            return
        if out is None:
            self.n["skipped"] += 1
            self.record(d, None, note="dust")
            return
        out["local_ms"] = (time.perf_counter() - t_in) * 1000
        if tx is None and "note" in out and "unresolved" in out["note"]:
            self.n["skipped"] += 1
            self.record(d, None, note=out["note"])
            return
        self.n["local"] += 1
        if d.get("feed") == "grpc":
            fn = d.get("feed_name", "")
            out["note"] = (out.get("note", "") + " grpc" + ("" if fn in ("", "grpc", "publicnode") else ":" + fn)).strip()
        self.record(d, out, note=out.get("note", ""))
        # the paper book must never miss a watched wallet's trade: the tape stores
        # only ~70 % of PumpSwap trades (unresolved pools), and a missed SELL left
        # 3.6 SOL of dead coins in the book on 09-01. The executor decoded this
        # trade exactly - bank it (duplicates with the tape are ignored by sig).
        self._bank_generic(d)
        # ---- off the critical path: the program's truth, then Jupiter's opinion
        if tx is not None:
            try:
                with self.sim_lock:
                    r = self.sim(tx)
                why = ""
                if r["err"]:
                    bad = [l for l in r["logs"] if "failed" in l or "Error" in l or "insufficient" in l]
                    why = " | " + (bad[0][-140:] if bad else "")
                sim_as = None
                # the whale often cannot afford OUR copy right after his own trade
                # (spent his SOL / sold every token). Simulation only: re-run it as
                # a funded payer (buys) or as a recent holder of the coin (sells).
                errs = json.dumps(r["err"]) if r["err"] else ""
                # Custom 1 = token/system insufficient funds; 6023 = pump.fun NotEnoughTokensToSell
                # 3012 = Anchor AccountNotInitialized (the whale closed his token account after selling out)
                if r["err"] and ("insufficient" in why.lower() or "Custom\": 1}" in errs or "Custom\": 6023}" in errs
                                 or "Custom\": 3012}" in errs):
                    alt = RICH_PAYER if d["side"] == "buy" else (self.recent_holder(d["mint"], out["our_tok"]) or RICH_PAYER)
                    if alt:
                        try:
                            _, tx2 = self.price_and_build(d, as_user=alt)
                            if tx2 is not None:
                                r2 = self.sim(tx2)
                                if not r2["err"]:
                                    r, why, sim_as = r2, "", alt
                        except Exception:
                            pass
                evs = tb.event_from_logs(r["logs"])
                upd = {"sim_ok": 0 if r["err"] else 1, "sim_err": (json.dumps(r["err"])[:80] + why) if r["err"] else None,
                       "sim_units": r["units"], "sim_ms": r["ms"], "sim_as": sim_as}
                if not r["err"]:
                    self.n["sim_ok"] += 1
                    for venue, e in evs:
                        if d["side"] == "buy" and (e.get("is_buy") or e.get("side") == "buy"):
                            got = (e["tok"] if venue == "pump" else e["base"]) / 1e6
                            upd["sim_tok"] = got
                            upd["sim_ratio"] = out["our_tok"] / got if got else None
                            break
                        if d["side"] == "sell" and (e.get("is_buy") is False or e.get("side") == "sell"):
                            if venue == "pump":
                                got = (e["sol"] - e.get("fee", 0) - e.get("creator_fee", 0)) / 1e9
                            else:
                                got = e["quote"] / 1e9
                            upd["sim_tok"] = got
                            upd["sim_ratio"] = out["our_sol"] / got if got else None
                            break
                else:
                    self.n["sim_fail"] += 1
                self.update(d["sig"], upd)
            except Exception as e:
                self.update(d["sig"], {"sim_err": f"sim exception {type(e).__name__}"[:100]})
        try:
            if d["side"] == "buy":
                jt, ms = self.jup(WSOL, d["mint"], SIZE_SOL * 1e9)
                jt = jt / 1e6
                ratio = out["our_tok"] / jt if jt else None
            else:
                jt, ms = self.jup(d["mint"], WSOL, out["our_tok"] * 1e6)
                jt = jt / 1e9
                ratio = out["our_sol"] / jt if jt else None
            self.n["jup"] += 1
            self.update(d["sig"], {"jup_tok": jt, "jup_ms": ms, "jup_ratio": ratio})
        except Exception:
            pass
        if may_go_live() and tx is not None and not out.get("note"):
            self.go_live(d, out)

    # ------------------------------------------------------------- any other DEX
    def act_generic(self, wallet, sig, slot, seen, venue_hint):
        """A watched wallet traded somewhere the tape cannot decode (Raydium /
        Meteora / Orca / Jupiter ...). Fetch the confirmed transaction, read
        its balance deltas, quote OUR copy on Jupiter (the 44 ms hop is the
        price of venue-agnosticism), record it, hand it to the paper book, and
        - off the path - simulate the Jupiter-built swap AS THE WHALE."""
        t_in = time.perf_counter()
        tx = generic.fetch(sig, self.http)
        if not tx:
            self.n["g_fetch_fail"] = self.n.get("g_fetch_fail", 0) + 1
            return
        d = generic.decode(tx, wallet)
        if not d:
            # spam, token-to-token, or not his trade - count why, cheaply
            try:
                payer = tx["transaction"]["message"]["accountKeys"][0]
                k = "g_not_payer" if payer != wallet else "g_decode_none"
            except Exception:
                k = "g_decode_none"
            self.n[k] = self.n.get(k, 0) + 1
            return
        self.n["g_ok"] = self.n.get("g_ok", 0) + 1
        d.update({"sig": sig, "user": wallet, "seen": seen, "slot": slot or tx.get("slot"), "ts": d["ts"] or int(seen)})
        fetch_ms = (time.perf_counter() - t_in) * 1000
        self.after_generic(d, t_in, fetch_ms)

    def after_generic(self, d, t_in, fetch_ms):
        """quote, record, bank, simulate - for a decoded non-pump trade, whether
        it came from a fetched transaction (websocket lane) or straight from a
        gRPC message (gstream lane, fetch_ms = 0)."""
        if d["sig"] in self.done:
            self.n["dup"] += 1                       # the other feed already recorded it
            return
        self.n["trades"] += 1
        whale_px = d["price"]
        out = {"route": "jup:" + d["venue"], "whale_px": whale_px, "fee_bps": None}
        try:
            if d["side"] == "buy":
                q, amount, route, ms = generic.quote(self.http, WSOL, d["mint"], SIZE_SOL * 1e9)
                our_tok = amount / (10 ** d["decimals"])
                out.update({"our_sol": SIZE_SOL, "our_tok": our_tok, "our_price": SIZE_SOL / our_tok if our_tok else 0})
            else:
                qty = min(d["tok"], SIZE_SOL / whale_px) if whale_px else 0
                if qty <= 0:
                    self.record(d, None, note="dust")
                    return
                q, amount, route, ms = generic.quote(self.http, d["mint"], WSOL, qty * (10 ** d["decimals"]))
                our_sol = amount / 1e9
                out.update({"our_sol": our_sol, "our_tok": qty, "our_price": our_sol / qty if qty else 0})
            out["route"] = f"jup:{d['venue']}:{route}"
            out["local_ms"] = (time.perf_counter() - t_in) * 1000
            out["build_ms"] = ms
        except Exception as e:
            self.n["skipped"] += 1
            self.record(d, None, note=f"generic {d['venue']} quote failed {type(e).__name__}")
            return
        self.n["local"] += 1
        fn = d.get("feed_name", "")
        self.record(d, out, note=f"generic fetch {fetch_ms:.0f}ms" +
                    ((" grpc" + ("" if fn in ("", "grpc", "publicnode") else ":" + fn)) if d.get("feed") == "grpc" else ""))
        self._bank_generic(d)
        # off the path: the whale-as-user simulation of the Jupiter-built swap
        wallet, sig = d["user"], d["sig"]
        try:
            tx_b64, sms = generic.swap_tx(self.http, q, wallet)
            ata = d.get("ata")
            if tx_b64 and ata and d["side"] == "buy":
                with self.sim_lock:
                    r = generic.sim_tokens_out(self.http, tx_b64, ata)
                upd = {"sim_ok": 0 if r["err"] else 1, "sim_ms": r["ms"], "sim_units": r["units"],
                       "sim_err": (json.dumps(r["err"])[:80] + " | " + " ".join(r["logs"])[-120:]) if r["err"] else None}
                if not r["err"] and r["balance"] is not None:
                    got = (r["balance"] - d["post_raw"]) / (10 ** d["decimals"])
                    if got > 0:
                        upd["sim_tok"] = got
                        upd["sim_ratio"] = out["our_tok"] / got
                self.n["sim_ok" if not r["err"] else "sim_fail"] += 1
                upd["jup_ms"] = sms
                self.update(sig, upd)
        except Exception as e:
            self.update(sig, {"sim_err": f"generic sim exception {type(e).__name__}"[:100]})

    def _bank_generic(self, d):
        """the paper book reads copybot.trades + signals; the tape cannot see
        these venues, so the executor banks the trade itself."""
        try:
            c = store.db()
            c.execute("INSERT OR IGNORE INTO trades(sig, wallet, mint, side, tokens, sol, ts, slot) VALUES(?,?,?,?,?,?,?,?)",
                      (d["sig"], d["user"], d["mint"], d["side"],
                       d["tok"] if d["side"] == "buy" else -d["tok"],
                       -d["sol"] if d["side"] == "buy" else d["sol"], int(d["ts"]), d.get("slot")))
            c.execute("INSERT OR IGNORE INTO signals(sig, wallet, ts, seen) VALUES(?,?,?,?)",
                      (d["sig"], d["user"], int(d["ts"]), d["seen"]))
            c.commit()
            c.close()
        except Exception as e:
            print(f"{time.strftime('%H:%M:%S')} bank_generic: {repr(e)[:80]}", flush=True)

    # ------------------------------------------------------------- THE live path
    def go_live(self, d, out):
        """Only reachable with all three locks open: keyfile + armed + live.
        Rebuilds the very same swap for OUR key, adds the lane tips, fires it
        at every lane at once, times the landing. Everything is recorded in
        `sends`. Brady arms real funds; this code never decides to."""
        import sendpath
        try:
            kp = sendpath.load_keypair(KEYFILE)
        except Exception as e:
            print(f"{time.strftime('%H:%M:%S')} execd LIVE: keyfile unreadable ({type(e).__name__}) - staying dry", flush=True)
            return
        try:
            out2, tx_ours = self.price_and_build(d, as_user=str(kp.pubkey()))
        except Exception as e:
            print(f"{time.strftime('%H:%M:%S')} execd LIVE: rebuild for our key failed ({type(e).__name__}) - not sent", flush=True)
            return
        if tx_ours is None or not self.blockhash or not out2.get("ixs"):
            return
        try:
            row = sendpath.execute(kp, out2["ixs"], self.blockhash, kind=f"{d['venue']}-{d['side']}", whale_sig=d["sig"],
                                   whale_slot=d.get("slot"), client=self.http, wait=True)
            self.n["sent"] = self.n.get("sent", 0) + 1
            print(f"{time.strftime('%H:%M:%S')} LIVE {d['venue']} {d['side']} {d['mint'][:8]}: sig {row['sig'][:12]} "
                  f"status {row.get('status')} slot {row.get('slot_landed')} behind {row.get('slots_behind')} "
                  f"lanes {[(k, v.get('ms'), bool(v.get('result'))) for k, v in row['responses'].items()]}", flush=True)
        except Exception as e:
            print(f"{time.strftime('%H:%M:%S')} execd LIVE send failed: {type(e).__name__}: {str(e)[:100]}", flush=True)

    def sim(self, tx):
        """simulate on publicnode first; on a 429/transport error fall back to beta."""
        r = tb.simulate(tx, self.http, RPC2)
        e = json.dumps(r.get("err")) if r.get("err") else ""
        if "429" in e or "rate" in e.lower() or "-32" in e[:12]:
            time.sleep(0.3)
            r = tb.simulate(tx, self.http, RPC)
        return r

    def resolve_fast(self, pool):
        """pool -> (mint, quote, flip) with the warm keep-alive client: the
        Pool account's base_mint @43 and quote_mint @75 (pump_amm IDL)."""
        try:
            r = self.http.post(RPC2, json={"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
                                          "params": [pool, {"encoding": "base64", "commitment": "processed",
                                                            "dataSlice": {"offset": 43, "length": 64}}]}).json()
            val = (r.get("result") or {}).get("value")
            if not val:
                return pools.resolve(pool)
            raw = base64.b64decode(val["data"][0])
            base, quote = str(Pubkey(raw[:32])), str(Pubkey(raw[32:64]))
            if quote == WSOL:
                return base, quote, 0
            if base == WSOL:
                return quote, base, 1
            return None
        except Exception:
            return pools.resolve(pool)

    def recent_holder(self, mint, qty_tok):
        """a wallet that bought >= qty of this coin recently (tape, read-only) -
        a simulation stand-in for a sell the whale himself can no longer fund."""
        try:
            c = sqlite3.connect(f"file:{tape.DB}?mode=ro", uri=True, timeout=2)
            rows = c.execute("SELECT user, tok FROM trades WHERE mint=? AND side='buy' ORDER BY ts DESC LIMIT 25",
                             (mint,)).fetchall()
            c.close()
            for user, tok in rows:
                if user != RICH_PAYER and tok >= qty_tok * 1.05:
                    return user
        except Exception:
            pass
        return None

    def jup(self, in_mint, out_mint, amount_raw):
        t = time.perf_counter()
        r = self.http.get(JUP, params={"inputMint": in_mint, "outputMint": out_mint, "amount": int(amount_raw),
                                       "slippageBps": SLIPPAGE_BPS})
        ms = int((time.perf_counter() - t) * 1000)
        if r.status_code != 200:
            raise RuntimeError(f"jup {r.status_code}")
        return int(r.json().get("outAmount", 0)), ms

    # ------------------------------------------------------------- storage
    def det_floor(self, seen, ts):
        """Latency relative to the executor's OWN fastest arrivals over the
        last 15 min. Absolute latency vs the block timestamp is not
        measurable: block timestamps are whole seconds and the cluster
        clock drifts ~0.5 s from real time (03:30 09-02: NTP-based det
        clipped websocket arrivals to 0.0 and put gRPC at 1.1 s)."""
        gap = seen - ts
        # only CHAIN-stamped rows feed the floor: the PC runs ~159 s ahead, so a
        # real (block ts, arrival) gap is ~159-162 s; gRPC generic rows carry the
        # local clock as ts (gap ~0) and must not drag the floor down (03:45 09-02)
        if 100 < gap < 3600:
            self.lat.append((seen, gap))
            while self.lat and seen - self.lat[0][0] > 900:
                self.lat.popleft()
        else:
            return None                     # no chain timestamp: latency unknown, not zero
        if len(self.lat) >= 5:
            floor = min(g for _, g in self.lat)
            if int(seen) % 30 == 0:
                try:
                    kv("exec_floor", f"{floor:.3f}")
                except Exception:
                    pass
            return max(0.0, gap - floor)
        return max(0.0, gap - float(kv("clock_offset") or 0))

    def record(self, d, q, note=""):
        now = time.time()
        det = self.det_floor(d["seen"], d["ts"])
        whale_px = (q or {}).get("whale_px") or (d["sol"] / d["tok"] if d["tok"] else None)
        gap = None
        if q and q.get("our_price") and whale_px:
            gap = (q["our_price"] / whale_px - 1) * 100 * (1 if d["side"] == "buy" else -1)
        row = (d["sig"], d["user"], d["mint"], d["side"], d["venue"], d["ts"], d["seen"], now, det,
               (q or {}).get("local_ms"), d["sol"], d["tok"], whale_px,
               (q or {}).get("our_sol"), (q or {}).get("our_tok"), (q or {}).get("our_price"), gap,
               None, (q or {}).get("route"), "dry-local", note,
               (q or {}).get("local_ms"), (q or {}).get("build_ms"), (q or {}).get("tx_bytes"),
               (q or {}).get("fee_bps"), (q or {}).get("cashback"), d.get("slot"),
               (q or {}).get("res_a"), (q or {}).get("res_b"),
               (d["seen"] - self.shred_seen[d["slot"]]) if d.get("slot") in self.shred_seen else None,
               self._fee_now.get("cu_price"), self._fee_now.get("jito_tip"))
        try:
            c = store.db()
            c.execute("INSERT OR REPLACE INTO exec_sim(sig,wallet,mint,side,venue,t_chain,t_seen,t_quote,det_s,quote_ms,"
                      "their_sol,their_tok,whale_price,our_sol,our_tok,our_price,gap_pct,impact_pct,route,mode,note,"
                      "local_ms,build_ms,tx_bytes,fee_bps,cashback,slot,res_a,res_b,lat_slot,cu_price,jito_tip) VALUES(" + ",".join("?" * 32) + ")", row)
            c.commit()
            c.close()
        except Exception as e:
            print(f"{time.strftime('%H:%M:%S')} execd record: {repr(e)[:80]}", flush=True)
            return
        self.done.add(d["sig"])
        if len(self.done) > 20000:
            self.done = set(list(self.done)[-5000:])
        if q:
            print(f"{time.strftime('%H:%M:%S')} {d['user'][:8]} {d['side']:<4} {d['mint'][:8]} {d['venue']} "
                  f"whale {whale_px:.3e} ours {q.get('our_price', 0):.3e} gap {gap if gap is None else round(gap, 2)}% "
                  f"det {'n/a' if det is None else format(det, '.2f') + 's'} local {q.get('local_ms', 0):.2f}ms build {q.get('build_ms', 0) or 0:.2f}ms {note}", flush=True)
        else:
            print(f"{time.strftime('%H:%M:%S')} {d['user'][:8]} {d['side']:<4} {d['mint'][:8]} skipped: {note}", flush=True)

    def update(self, sig, fields):
        try:
            c = store.db()
            c.execute("UPDATE exec_sim SET " + ", ".join(f"{k}=?" for k in fields) + " WHERE sig=?",
                      list(fields.values()) + [sig])
            c.commit()
            c.close()
            if "sim_ok" in fields:
                print(f"{time.strftime('%H:%M:%S')}    sim {'OK' if fields['sim_ok'] else 'FAIL ' + str(fields.get('sim_err'))[:60]} "
                      f"{fields.get('sim_ms')}ms units {fields.get('sim_units')} ratio {fields.get('sim_ratio')}", flush=True)
            if "jup_ms" in fields:
                print(f"{time.strftime('%H:%M:%S')}    jup {fields['jup_ms']}ms ratio local/jup {fields.get('jup_ratio')}", flush=True)
        except Exception as e:
            print(f"{time.strftime('%H:%M:%S')} execd update: {repr(e)[:80]}", flush=True)

    # ---------------------------------------------------------------- run
    def _blockhash_loop(self):
        while not self.stop:
            try:
                r = self.http.post(RPC2, json={"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash",
                                              "params": [{"commitment": "confirmed"}]}).json()
                self.blockhash = r["result"]["value"]["blockhash"]
            except Exception:
                pass
            time.sleep(5)

    def _hb_loop(self):
        while not self.stop:
            try:
                kv("execd_hb", time.time())
                kv("execd_stats", json.dumps(self.n))
            except Exception:
                pass
            if time.time() - self.reloaded > 60:
                before = set(self.watched)
                self.reload()
                if set(self.watched) != before:
                    for a in self.apps:
                        try:
                            a.close()          # resubscribe with the new set
                        except Exception:
                            pass
            time.sleep(2)

    def _sock_loop(self, i):
        backoff = 1
        while not self.stop:
            limited = {"v": False}

            def on_err(ws, e, _i=i, _l=limited):
                s = repr(e)
                if "1013" in s or "Rate limit" in s or "429" in s:
                    _l["v"] = True
                print(f"ws{_i}:", s[:80], flush=True)
            try:
                app = WebSocketApp(WS, header=[f"User-Agent: {UA['User-Agent']}"], on_open=self._sub,
                                   on_message=self.on_message, on_error=on_err)
                app._slice = i
                self.apps.append(app)
                app.run_forever(ping_interval=30)
                try:
                    self.apps.remove(app)
                except ValueError:
                    pass
            except Exception as e:
                print(f"ws{i} loop: {repr(e)[:80]}", flush=True)
            if self.seconds and time.time() - self.t0 > self.seconds:
                break
            # a rate-limited close means: fewer, slower reconnects, not a storm
            backoff = min(60, backoff * 2) if limited["v"] else 1
            time.sleep(backoff)

    # ------------------------------------------------------------ gRPC feed (needs a token)
    def _grpc_loop(self, ep):
        """Yellowstone gRPC: each watched wallet's transaction at PROCESSED,
        while the block is still replaying - no 0.28 s block wait, no
        `confirmed` fetch. One loop per configured endpoint (gstream.txt:
        url/token, url2/token2 ...); first arrival wins, signatures dedupe,
        so a second vendor can be raced against the first for free."""
        import gstream
        url, tok, name = ep["url"], ep["token"], ep["name"]
        print(f"{time.strftime('%H:%M:%S')} execd: gRPC feed '{name}' on {url}", flush=True)
        while not self.stop:
            try:
                ch = gstream.channel(url, tok)
                import geyser_pb2_grpc
                stub = geyser_pb2_grpc.GeyserStub(ch)
                wl = list(self.watched)
                n_upd, t_open, t_last = 0, time.time(), time.time()
                print(f"{time.strftime('%H:%M:%S')} gRPC[{name}]: subscribing {len(wl)} wallets", flush=True)
                for upd in stub.Subscribe(iter([gstream.subscribe_request(wl, failed=True)])):
                    if self.stop:
                        break
                    n_upd += 1
                    if n_upd == 1:
                        print(f"{time.strftime('%H:%M:%S')} gRPC[{name}]: first update {time.time() - t_open:.1f}s after subscribe", flush=True)
                    if time.time() - t_last > 120:
                        print(f"{time.strftime('%H:%M:%S')} gRPC[{name}]: {n_upd} updates so far, {self.n.get('grpc', 0)} trades fed", flush=True)
                        t_last = time.time()
                    self.grpc_last = time.time()
                    if upd.HasField("transaction"):
                        self.on_grpc(upd, set(wl), name)
                    if set(self.watched) != set(wl):
                        break                       # resubscribe with the new set
                print(f"{time.strftime('%H:%M:%S')} gRPC[{name}]: stream ended after {n_upd} updates", flush=True)
                ch.close()
            except Exception as e:
                s = str(e).replace("\n", " ").replace("\t", " ")
                print(f"{time.strftime('%H:%M:%S')} gRPC[{name}] feed: {type(e).__name__}: {s[:160]}", flush=True)
                time.sleep(5)

    def on_grpc(self, upd, watched, feed="grpc"):
        import gstream
        seen = time.time()
        # SubscribeUpdate.transaction = SubscribeUpdateTransaction{transaction: Info, slot};
        # Info = {signature, is_vote, transaction, meta, index}
        tx = upd.transaction.transaction
        sig = gstream.b58(tx.signature) if tx.signature else None
        if not sig or sig in self.done:
            return
        self.n["grpc"] = self.n.get("grpc", 0) + 1
        # PRECURSORS (09-03 idea): a watched wallet's FAILED swap, or a non-trade tx that
        # touches a mint (ATA creation, wrap, transfer), may precede its real buy by
        # 0.2-2 s and already names the mint. Record them; precursor_study.py measures
        # how often and how far ahead they run before we ever act on one.
        try:
            failed = bool(tx.meta.err.err)
        except Exception:
            failed = False
        if failed:
            self._precursor(tx, upd, watched, "failed", seen, sig)
            return
        logs = list(tx.meta.log_messages)
        # pump venues: the exact 0.3 ms path, straight from the event in the logs -
        # first notification wins (websocket or gRPC, both instant)
        pump_d = None
        for ln in logs:
            if not ln.startswith("Program data: "):
                continue
            try:
                b = base64.b64decode(ln[14:])
            except Exception:
                continue
            d = tape.decode_pump(b) or tape.decode_pswap(b)
            if d and d["user"] in watched:
                pump_d = (d, b)
                break
        if pump_d:
            with self.lock:
                if sig in self.seen:
                    self.n["dup"] += 1
                    return
                self.seen.add(sig)
            d, b = pump_d
            d.update({"sig": sig, "seen": seen, "slot": upd.transaction.slot, "raw": b, "logs": logs, "feed": "grpc", "feed_name": feed})
            self.n["trades"] += 1
            threading.Thread(target=self.act, args=(d,), daemon=True).start()
            return
        # anything else: balance deltas from the message itself, no fetch. The
        # websocket lane may have SEEN this signature already but it is still
        # ~0.8 s away from fetching it - gRPC wins that race by construction,
        # so it does not defer to `seen` here; whoever records first wins (`done`).
        try:
            d = gstream.decode_update(upd, watched)
        except Exception as e:
            self.n["grpc_gen_err"] = self.n.get("grpc_gen_err", 0) + 1
            if self.n["grpc_gen_err"] <= 3:
                print(f"{time.strftime('%H:%M:%S')} gRPC generic decode error: {type(e).__name__}: {str(e)[:120]}", flush=True)
            return
        if d:
            self.n["grpc_gen_ok"] = self.n.get("grpc_gen_ok", 0) + 1
            d.update({"user": d["wallet"], "ts": int(seen), "seen": seen, "sig": sig, "slot": upd.transaction.slot,
                      "venue": generic.filter(logs) or "swap?", "price": d["sol"] / d["tok"] if d["tok"] else 0,
                      "feed": "grpc", "feed_name": feed})
            self.n["trades"] += 1
            threading.Thread(target=self.after_generic, args=(d, time.perf_counter(), 0.0), daemon=True).start()
        else:
            self.n["grpc_gen_none"] = self.n.get("grpc_gen_none", 0) + 1
            self._precursor(tx, upd, watched, "nontrade", seen, sig)

    def _precursor(self, tx, upd, watched, kind, seen, sig):
        """record (wallet, mint) pairs a watched wallet touched in a failed or non-trade tx"""
        import gstream
        try:
            keys = [gstream.b58(k) for k in tx.transaction.message.account_keys]
            wallet = keys[0] if keys and keys[0] in watched else None
            if not wallet:
                return
            skip = {gstream.WSOL, "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}
            mints = {b.mint for b in list(tx.meta.pre_token_balances) + list(tx.meta.post_token_balances)
                     if b.owner == wallet and b.mint not in skip}
            if not mints:
                return
            c = store.db()
            c.executemany("INSERT OR IGNORE INTO precursors VALUES(?,?,?,?,?,?)",
                          [(sig, wallet, m, kind, seen, upd.transaction.slot) for m in mints])
            c.commit()
            c.close()
            self.n["precursors"] = self.n.get("precursors", 0) + len(mints)
        except Exception as e:
            self.n["precursor_err"] = self.n.get("precursor_err", 0) + 1

    # ------------------------------------------------------------ the honest ruler
    def _slots_loop(self):
        """slotsUpdatesSubscribe on the public node: the moment it received the
        FIRST SHRED of each slot, on our clock. A trade's latency = its arrival
        minus that moment - milliseconds, no whole-second block timestamps.
        Measured 09-02 11:35: the block completes 0.254 s after the first shred;
        websocket trades arrive 0.318 s after it, PublicNode gRPC 0.123 s."""
        while not self.stop:
            try:
                def on_open(ws):
                    ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "slotsUpdatesSubscribe"}))

                def on_message(ws, raw):
                    t = time.time()
                    if '"slotsUpdatesNotification"' not in raw[:80]:
                        return
                    i = raw.find('"type":"firstShredReceived"')
                    if i < 0:
                        return
                    j = raw.find('"slot":')
                    if j < 0:
                        return
                    k = j + 7
                    while k < len(raw) and raw[k].isdigit():
                        k += 1
                    try:
                        slot = int(raw[j + 7:k])
                    except ValueError:
                        return
                    self.shred_seen.setdefault(slot, t)
                    if len(self.shred_seen) > 4000:
                        for s in sorted(self.shred_seen)[:1000]:
                            self.shred_seen.pop(s, None)
                app = WebSocketApp(WS, header=[f"User-Agent: {UA['User-Agent']}"], on_open=on_open, on_message=on_message)
                app.run_forever(ping_interval=30)
            except Exception as e:
                print(f"{time.strftime('%H:%M:%S')} slots feed: {repr(e)[:80]}", flush=True)
            time.sleep(3)

    @property
    def _fee_now(self):
        """landing price right now (fees.py: Jito tip floor p75 + recent priority
        fee p75), constants when the poller has nothing yet"""
        try:
            import fees
            return fees.suggest()
        except Exception:
            return {}

    def run(self):
        threading.Thread(target=self._hb_loop, daemon=True).start()
        threading.Thread(target=self._blockhash_loop, daemon=True).start()
        try:
            import fees
            fees.start()
        except Exception as e:
            print(f"{time.strftime('%H:%M:%S')} execd: fees poller not started: {repr(e)[:80]}", flush=True)
        import gstream
        for ep in gstream.endpoints():
            threading.Thread(target=self._grpc_loop, args=(ep,), daemon=True).start()
        threading.Thread(target=self._slots_loop, daemon=True).start()
        threads = [threading.Thread(target=self._sock_loop, args=(i,), daemon=True) for i in range(SOCKETS)]
        for t in threads:
            t.start()
            time.sleep(0.2)
        try:
            while not self.stop:
                if self.seconds and time.time() - self.t0 > self.seconds + 2:
                    break
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        self.stop = True
        for a in self.apps:
            try:
                a.close()
            except Exception:
                pass


if __name__ == "__main__":
    secs = None
    if "--test" in sys.argv:
        i = sys.argv.index("--test")
        secs = int(sys.argv[i + 1]) if len(sys.argv) > i + 1 else 60
    # exactly one executor: twice tonight a guard-spawned instance overlapped a
    # session-launched one (double quotes, double Jupiter calls). A fresh
    # heartbeat means another executor is alive - leave.
    if not secs:
        try:
            hb = float(kv("execd_hb") or 0)
            if time.time() - hb < 15:
                print(f"execd: another executor heartbeat {time.time() - hb:.0f}s old - not starting a second one", flush=True)
                sys.exit(0)
        except SystemExit:
            raise
        except Exception:
            pass
    e = Execd(secs)
    print(f"EXECD v2 local-first dry-run online: {len(e.watched)} watched wallets, {len(e.pools)} pools known, "
          f"{SOCKETS} sockets" + (f" (test {secs}s)" if secs else ""), flush=True)
    e.run()
    print(f"{time.time()-e.t0:.0f}s: {e.n}", flush=True)
