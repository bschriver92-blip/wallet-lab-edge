"""FEES - live landing prices: Jito tip floor + recent priority fees.

WHY: landing is an auction. A tip below the floor loses the bundle race;
a compute-unit price below what the block's other writers pay gets our
copy scheduled after theirs. sendpath.py shipped with constants (Jito
10,000 lamports, 50,000 micro-lamports/CU). Measured 09-02 15:42 UTC: the
25th percentile of LANDED Jito tips was 15,865 lamports - our constant lost
to three quarters of the field. Both numbers are free to read, so read them
every few seconds and let the send path ask for the current level.

    python fees.py --probe            print the current levels once
    python fees.py --log 600          sample every 5 s for 600 s into copybot.db fee_samples

API: fees.start() (background poller) then fees.suggest(accounts=None) ->
{"jito_tip": lamports, "cu_price": micro-lamports/CU, "helius_tip": lamports, "age": s, "src": ...}
"""
import json
import os
import sqlite3
import statistics
import sys
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.environ.get("COPYBOT_DB", r"C:\Users\Brady\lab\copybot.db")
TIP_FLOOR_URL = "https://bundles.jito.wtf/api/v1/bundles/tip_floor"
RPC = "https://solana-rpc.publicnode.com"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) wallet-lab/1.0"
PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PAMM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

# policy: tip at the 75th percentile of landed tips (lands most of the time,
# still ~0.00004 SOL), never below 15,000 or above 0.001 SOL; CU price = the
# 75th percentile of the last 150 slots' minimum fee on the accounts we touch,
# with a floor of 50,000 micro-lamports/CU (the old constant) and a cap.
TIP_PCT = "landed_tips_75th_percentile"
TIP_MIN, TIP_MAX = 15_000, 1_000_000
CU_MIN, CU_MAX = 50_000, 5_000_000
HELIUS_TIP = 5_000
POLL_S = 4.0
# "first" mode: pay what the FIRST copier after a whale paid in our own
# in-block study (inblock.py -> speed_results/inblock.json, rank-1 p75),
# capped in absolute SOL so a fee spike can never cost more than the edge.
# Selected by env FEE_MODE=first (default: market p75 above).
FEE_MODE = os.environ.get("FEE_MODE", "p75")
MAX_FEE_LAMPORTS = 300_000          # 0.0003 SOL per trade, all-in cap (tip + cu_price x cu_limit)
INBLOCK = os.path.join(HERE, "speed_results", "inblock.json")
_inblock = {"t": 0.0, "rank1": None}


def first_copier_levels():
    """(cu_price, tip) the rank-1 copier paid at p75 in the last study, or None."""
    try:
        st = os.stat(INBLOCK).st_mtime
        if st != _inblock["t"]:
            j = json.load(open(INBLOCK, encoding="utf-8"))
            r1 = next((r for r in j.get("by_rank", []) if r["rank"] == 1), None)
            _inblock.update({"t": st, "rank1": r1})
        r1 = _inblock["rank1"]
        return (int(r1["cu_price_p75"] or 0), int(r1["tip_p75"] or 0)) if r1 else None
    except Exception:
        return None

_state = {"tip": None, "tip_t": 0.0, "fees": None, "fees_t": 0.0, "err": None}
_lock = threading.Lock()
_started = False


def _get(url, data=None, timeout=6):
    req = urllib.request.Request(url, data=data, headers={"User-Agent": UA, "content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def poll_tip():
    j = _get(TIP_FLOOR_URL)
    row = j[0] if isinstance(j, list) else j
    out = {k: int(round(float(v) * 1e9)) for k, v in row.items() if k.startswith("landed_tips") or k.startswith("ema")}
    out["time"] = row.get("time")
    return out


def poll_fees(accounts=(PUMP, PAMM)):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "getRecentPrioritizationFees",
                       "params": [list(accounts)]}).encode()
    r = _get(RPC, body)["result"]
    fees = sorted(int(x["prioritizationFee"]) for x in r)
    if not fees:
        return None
    p = lambda q: fees[min(len(fees) - 1, int(q * len(fees)))]
    return {"n": len(fees), "p50": p(0.5), "p75": p(0.75), "p90": p(0.9), "max": fees[-1],
            "nonzero": sum(1 for f in fees if f > 0)}


def _loop():
    while True:
        try:
            t = poll_tip()
            with _lock:
                _state["tip"], _state["tip_t"], _state["err"] = t, time.time(), None
        except Exception as e:
            with _lock:
                _state["err"] = f"tip: {type(e).__name__}"
        try:
            f = poll_fees()
            with _lock:
                _state["fees"], _state["fees_t"] = f, time.time()
        except Exception as e:
            with _lock:
                _state["err"] = f"fees: {type(e).__name__}"
        time.sleep(POLL_S)


def start():
    global _started
    if not _started:
        _started = True
        threading.Thread(target=_loop, daemon=True, name="fees").start()


def suggest():
    """Current landing prices, or the old constants when nothing is cached."""
    with _lock:
        tip, tip_t, fees, fees_t = _state["tip"], _state["tip_t"], _state["fees"], _state["fees_t"]
    now = time.time()
    jito = 10_000
    src = "const"
    if tip and now - tip_t < 60:
        jito = int(min(TIP_MAX, max(TIP_MIN, tip.get(TIP_PCT) or 0)))
        src = "live"
    cu = CU_MIN
    if fees and now - fees_t < 60:
        cu = int(min(CU_MAX, max(CU_MIN, fees["p75"])))
    out = {"jito_tip": jito, "cu_price": cu, "helius_tip": HELIUS_TIP, "src": src,
           "age": round(now - max(tip_t, fees_t), 1) if max(tip_t, fees_t) else None}
    fc = first_copier_levels()
    if fc:
        out["first_cu_price"], out["first_tip"] = fc
    if FEE_MODE == "first" and fc:
        cu_first, tip_first = max(fc[0], cu), max(fc[1], jito)
        # all-in cap: tip + cu_price x 160k CU (the biggest venue limit) <= MAX_FEE_LAMPORTS
        budget = MAX_FEE_LAMPORTS - HELIUS_TIP
        if tip_first + cu_first * 160_000 / 1e6 > budget:
            tip_first = min(tip_first, int(budget * 0.5))
            cu_first = int(max(CU_MIN, (budget - tip_first) * 1e6 / 160_000))
        out.update({"jito_tip": int(min(TIP_MAX, tip_first)), "cu_price": int(min(CU_MAX, cu_first)), "src": src + "+first"})
    return out


def _ensure_table(c):
    c.execute("CREATE TABLE IF NOT EXISTS fee_samples(t REAL, tip25 INTEGER, tip50 INTEGER, tip75 INTEGER, tip95 INTEGER, "
              "tip99 INTEGER, cu_p50 INTEGER, cu_p75 INTEGER, cu_p90 INTEGER, cu_max INTEGER, cu_nonzero INTEGER)")


def log(seconds, every=5.0):
    c = sqlite3.connect(DB, timeout=10)
    _ensure_table(c)
    t_end = time.time() + seconds
    n = 0
    while time.time() < t_end:
        try:
            t, f = poll_tip(), poll_fees()
            c.execute("INSERT INTO fee_samples VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                      (time.time(), t.get("landed_tips_25th_percentile"), t.get("landed_tips_50th_percentile"),
                       t.get("landed_tips_75th_percentile"), t.get("landed_tips_95th_percentile"),
                       t.get("landed_tips_99th_percentile"), f["p50"], f["p75"], f["p90"], f["max"], f["nonzero"]))
            c.commit()
            n += 1
        except Exception as e:
            print("sample error", type(e).__name__, str(e)[:80])
        time.sleep(every)
    c.close()
    return n


def sample_once():
    """one row into fee_samples (FORGE job lab_fees, every 5 min): the tip and
    priority-fee landscape over days, free - what a landed copy costs, so the
    paper book can be charged a real landing price later."""
    t, f = poll_tip(), poll_fees()
    c = sqlite3.connect(DB, timeout=10)
    _ensure_table(c)
    c.execute("INSERT INTO fee_samples VALUES(?,?,?,?,?,?,?,?,?,?,?)",
              (time.time(), t.get("landed_tips_25th_percentile"), t.get("landed_tips_50th_percentile"),
               t.get("landed_tips_75th_percentile"), t.get("landed_tips_95th_percentile"),
               t.get("landed_tips_99th_percentile"), f["p50"], f["p75"], f["p90"], f["max"], f["nonzero"]))
    c.commit()
    c.close()
    return {"tip50": t.get("landed_tips_50th_percentile"), "tip75": t.get("landed_tips_75th_percentile"),
            "cu_p75": f["p75"], "cu_p90": f["p90"]}


if __name__ == "__main__":
    if "--log" in sys.argv:
        secs = int(sys.argv[sys.argv.index("--log") + 1])
        print("samples:", log(secs))
    else:
        t = poll_tip()
        f = poll_fees()
        print("Jito landed tips (lamports): " + ", ".join(f"{k.replace('landed_tips_', '').replace('_percentile', '')}={v:,}"
                                                          for k, v in t.items() if k.startswith("landed")))
        print(f"recent priority fees on pump.fun+PumpSwap (micro-lamports/CU, last {f['n']} slots): "
              f"p50 {f['p50']:,} p75 {f['p75']:,} p90 {f['p90']:,} max {f['max']:,} nonzero {f['nonzero']}/{f['n']}")
        _state.update({"tip": t, "tip_t": time.time(), "fees": f, "fees_t": time.time()})
        s = suggest()
        print(f"suggest: jito_tip {s['jito_tip']:,} lamports ({s['jito_tip'] / 1e9:.6f} SOL), cu_price {s['cu_price']:,}, src {s['src']}")
