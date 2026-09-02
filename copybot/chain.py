"""COPYBOT chain layer - parallel Solana RPC that actually uses the machine.

16 cores / 16 GB here, so every bulk fetch runs through a worker pool with
adaptive backoff. Public RPC rate-limits hard (429s), so workers self-throttle
rather than hammering; RPC_URL env var overrides the endpoint if we ever pay
for one.
"""
import json, os, random, threading, time, urllib.error, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

def _endpoint():
    """Helius key (set once via setkey.py) > RPC_URL env > slow public RPC."""
    kf = os.path.join(os.path.dirname(os.path.abspath(__file__)), "key.txt")
    if os.path.exists(kf):
        k = open(kf, encoding="utf-8").read().strip()
        if k:
            return f"https://mainnet.helius-rpc.com/?api-key={k}"
    return os.environ.get("RPC_URL", "https://api.mainnet-beta.solana.com")

RPC = _endpoint()
UA = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}
PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
WSOL = "So11111111111111111111111111111111111111112"

_throttle = threading.Semaphore(1)
_stats = {"calls": 0, "429s": 0, "errors": 0}
_lock = threading.Lock()

def rpc(method, params, tries=6):
    """One RPC call with exponential backoff. Raises after `tries` failures."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1,
                       "method": method, "params": params}).encode()
    delay = 0.6
    for attempt in range(tries):
        try:
            req = urllib.request.Request(RPC, data=body, headers=UA)
            with urllib.request.urlopen(req, timeout=45) as r:
                out = json.loads(r.read())
            with _lock:
                _stats["calls"] += 1
            if "result" in out:
                return out["result"]
            if "error" in out:                      # -32005 = rate limited
                if out["error"].get("code") == -32005:
                    with _lock:
                        _stats["429s"] += 1
                    time.sleep(delay + random.random())
                    delay *= 1.8
                    continue
                raise RuntimeError(out["error"])
        except urllib.error.HTTPError as e:
            if e.code == 429:
                with _lock:
                    _stats["429s"] += 1
                time.sleep(delay + random.random())
                delay *= 1.8
                continue
            with _lock:
                _stats["errors"] += 1
            if attempt == tries - 1:
                raise
            time.sleep(delay); delay *= 1.8
        except Exception:
            with _lock:
                _stats["errors"] += 1
            if attempt == tries - 1:
                raise
            time.sleep(delay); delay *= 1.8
    raise RuntimeError(f"rpc {method} failed after {tries} tries")

def stats():
    with _lock:
        return dict(_stats)

def _api_key():
    kf = os.path.join(os.path.dirname(os.path.abspath(__file__)), "key.txt")
    if os.path.exists(kf):
        return open(kf, encoding="utf-8").read().strip()
    return None

def txs_batch(sigs, tries=4):
    """⭐ 100 transactions per call, pre-parsed, instead of one round trip each.

    Fetching singly ran ~6/s against the free tier's rate limit - 6,000 round
    trips for one discovery job. This endpoint returns up to 100 parsed
    transactions at once, so the same job becomes ~60 calls.

    Returns a dict {signature: parsed_tx}. Falls back to {} without a key.
    """
    key = _api_key()
    if not key or not sigs:
        return {}
    url = f"https://api.helius.xyz/v0/transactions?api-key={key}"
    out = {}
    for i in range(0, len(sigs), 100):
        chunk = list(sigs[i:i + 100])
        body = json.dumps({"transactions": chunk}).encode()
        delay = 0.8
        for attempt in range(tries):
            try:
                req = urllib.request.Request(url, data=body, headers=UA)
                with urllib.request.urlopen(req, timeout=60) as r:
                    data = json.loads(r.read())
                with _lock:
                    _stats["calls"] += 1
                for t in data or []:
                    if t and t.get("signature"):
                        out[t["signature"]] = t
                break
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    with _lock:
                        _stats["429s"] += 1
                    time.sleep(delay); delay *= 2
                    continue
                with _lock:
                    _stats["errors"] += 1
                break
            except Exception:
                with _lock:
                    _stats["errors"] += 1
                time.sleep(delay); delay *= 2
    return out

def parse_enhanced(t):
    """Token movements from a Helius *enhanced* transaction (different shape
    from raw getTransaction: tokenTransfers instead of pre/post balances)."""
    if not t or t.get("transactionError"):
        return []
    moves = []
    for tr in t.get("tokenTransfers") or []:
        mint = tr.get("mint")
        amt = tr.get("tokenAmount")
        if not mint or amt in (None, 0):
            continue
        frm, to = tr.get("fromUserAccount"), tr.get("toUserAccount")
        ts = t.get("timestamp")
        if to:
            moves.append({"mint": mint, "owner": to, "tokens": float(amt),
                          "side": "buy", "ts": ts, "slot": t.get("slot")})
        if frm:
            moves.append({"mint": mint, "owner": frm, "tokens": -float(amt),
                          "side": "sell", "ts": ts, "slot": t.get("slot")})
    return moves

def pmap(fn, items, workers=8, label="", quiet=False):
    """Run fn over items across a worker pool. Returns results in input order
    (None where the call failed). This is where the 16 cores get used."""
    items = list(items)
    out = [None] * len(items)
    done = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fn, it): i for i, it in enumerate(items)}
        for f in as_completed(futs):
            i = futs[f]
            try:
                out[i] = f.result()
            except Exception:
                out[i] = None
            done += 1
            if not quiet and (done % 25 == 0 or done == len(items)):
                el = time.time() - t0
                rate = done / el if el else 0
                eta = (len(items) - done) / rate if rate else 0
                print(f"\r  {label} {done}/{len(items)}  "
                      f"{rate:.1f}/s  eta {eta:5.0f}s  "
                      f"[429s {_stats['429s']}]", end="", flush=True)
    if not quiet and items:
        print()
    return out

# ------------------------------------------------------------------ helpers
def signatures(address, limit=1000, before=None, until=None):
    p = {"limit": limit}
    if before:
        p["before"] = before
    if until:
        p["until"] = until
    return rpc("getSignaturesForAddress", [address, p]) or []

def tx(sig, commitment="confirmed"):
    """⚠️ getTransaction defaults to FINALIZED. A signature seen live at
    'processed' commitment does not exist there yet (~13s behind), so the call
    fails 6 times and looks like a rate-limit problem. Always ask for the
    commitment level you actually saw the signature at."""
    return rpc("getTransaction", [sig, {"encoding": "jsonParsed",
                                        "maxSupportedTransactionVersion": 0,
                                        "commitment": commitment}])

def fee_payer(t):
    """The signer who paid - the ONLY reliable 'whose transaction is this'.

    ⛔ Our own forensics: getSignaturesForAddress(wallet) returns every tx that
    merely MENTIONS the wallet; 0 of 70 sampled 'his' txs were actually signed
    by him - the rest was spam. Always check this before crediting a trade.
    """
    try:
        keys = t["transaction"]["message"]["accountKeys"]
        for k in keys:
            if isinstance(k, dict):
                if k.get("signer") and k.get("writable"):
                    return k["pubkey"]
            else:
                return k
        return keys[0]["pubkey"] if isinstance(keys[0], dict) else keys[0]
    except Exception:
        return None

def parse_fleet(t):
    """Every token movement in a transaction, grouped by the wallet that got it.

    ⭐ MEASURED 2026-08-31: top traders run a FLEET - one operator wallet signs
    and pays, while tokens land in many sub-wallets (we saw 63 accounts in a
    single transaction, the tracked address sitting at index 2, never signing).
    Parsing only the signer's own balances returns NOTHING for these. Reading
    every owner's delta turns ONE subscription to the operator into full
    visibility of the whole fleet.
    """
    meta = t.get("meta") or {}
    if meta.get("err"):
        return []
    pre, post = {}, {}
    for b in meta.get("preTokenBalances", []):
        pre[(b["mint"], b.get("owner"))] = float(b["uiTokenAmount"]["uiAmount"] or 0)
    for b in meta.get("postTokenBalances", []):
        post[(b["mint"], b.get("owner"))] = float(b["uiTokenAmount"]["uiAmount"] or 0)
    moves = []
    for key in set(pre) | set(post):
        mint, owner = key
        if mint == WSOL or not owner:
            continue
        d = post.get(key, 0) - pre.get(key, 0)
        if abs(d) > 1e-9:
            moves.append({"mint": mint, "owner": owner, "tokens": d,
                          "side": "buy" if d > 0 else "sell",
                          "ts": t.get("blockTime"), "slot": t.get("slot")})
    # total SOL the transaction moved (operator's cost/proceeds)
    try:
        sol = (meta["postBalances"][0] - meta["preBalances"][0]
               + meta.get("fee", 0)) / 1e9
    except Exception:
        sol = 0.0
    for m in moves:
        m["tx_sol"] = sol
        m["n_wallets"] = len({x["owner"] for x in moves})
    return moves

def parse_swap(t, wallet):
    """Extract (mint, token_delta, sol_delta) for `wallet` from a transaction.

    Venue-agnostic on purpose: reads pre/post balances rather than decoding
    pump.fun / PumpSwap / Raydium instruction layouts, so it keeps working when
    the wallet trades somewhere new.
    """
    meta = t.get("meta") or {}
    if meta.get("err"):
        return None
    pre = {(b["mint"], b.get("owner")): float(b["uiTokenAmount"]["uiAmount"] or 0)
           for b in meta.get("preTokenBalances", [])}
    post = {(b["mint"], b.get("owner")): float(b["uiTokenAmount"]["uiAmount"] or 0)
            for b in meta.get("postTokenBalances", [])}
    moved = None
    for key in set(pre) | set(post):
        mint, owner = key
        if owner != wallet or mint == WSOL:
            continue
        d = post.get(key, 0) - pre.get(key, 0)
        if abs(d) > 1e-9 and (moved is None or abs(d) > abs(moved[1])):
            moved = (mint, d)
    if not moved:
        return None
    # SOL delta for the fee payer (index 0), fee added back so it isn't
    # miscounted as part of the trade
    try:
        sol = (meta["postBalances"][0] - meta["preBalances"][0]
               + meta.get("fee", 0)) / 1e9
    except Exception:
        sol = 0.0
    return {"mint": moved[0], "tokens": moved[1], "sol": sol,
            "ts": t.get("blockTime"), "slot": t.get("slot"),
            "side": "buy" if moved[1] > 0 else "sell"}
