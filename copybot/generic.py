"""GENERIC swap decoder - the wallets the pump-only tape cannot see.

The executor already subscribes to every watched wallet's transactions. For
a swap on Raydium / Meteora / Orca / Jupiter / anything, there is no event
we know how to decode - but the transaction itself tells the truth: the
wallet's token-balance deltas (ledger #836: within 1 % of the real fill).

  filter(logs)         -> venue label if the logs show a DEX swap, else None
                          (cheap; keeps spam - strangers' token-account
                          creations that mention the wallet - off the RPC)
  fetch(sig)           -> the confirmed transaction (publicnode, retried:
                          processed -> confirmed takes ~0.4-1 s)
  decode(tx, wallet)   -> {mint, side, sol, tok, price, ts, slot, venue} or None
  quote(...)           -> Jupiter quote for our size (throttled, keep-alive)
  swap_tx(quote, user) -> Jupiter-built transaction for `user` (for the
                          whale in dry-run = simulate-as-the-whale on any DEX)
  sim_tokens_out(...)  -> the simulated post-swap token balance of the
                          user's ATA = ground truth for tokens out

Nothing here signs or sends.
"""
import base64
import json
import threading
import time

import httpx

RPC2 = "https://solana-rpc.publicnode.com"
RPC = "https://api.mainnet-beta.solana.com"
JUP_Q = "https://lite-api.jup.ag/swap/v1/quote"
JUP_S = "https://lite-api.jup.ag/swap/v1/swap"
WSOL = "So11111111111111111111111111111111111111112"
STABLES = {"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) wallet-lab/1.0", "Accept": "application/json"}
ATA_RENT = 2_039_280
PUMP_PROGS = {"6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"}

DEX = {
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "jupiter",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "raydium-amm",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "raydium-clmm",
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C": "raydium-cpmm",
    "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj": "raydium-launchlab",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": "meteora-dlmm",
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB": "meteora-amm",
    "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG": "meteora-damm2",
    "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN": "meteora-dbc",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "orca",
    "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP": "orca-v2",
    "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY": "phoenix",
    "opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb": "openbook",
    "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG": "moonshot",
    "boop8hVGQGqehUK2iVEMEnMrL5RbjywRzHKBmBE7ry4": "boop",
    "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c": "lifinity",
    "SoLFiHG9TfgtdUXUjWAxi3LtvYuFv3Ju2H1QKdpump": "solfi",
    "ZERor4xhbUycZ6gb9ntrhqscUcZmAbQVRbdaF5P4V2o": "zerofi",
    "obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y": "obric",
}
SWAP_WORDS = ("Instruction: Swap", "Instruction: Route", "Instruction: SharedAccountsRoute", "Instruction: swap",
              "Instruction: ExactOutRoute", "Instruction: Buy", "Instruction: Sell", "Instruction: TradeV2")

_jlock = threading.Lock()
_jlast = [0.0]

# tip accounts: Jito (8), Helius Sender (3 documented) - transfers to these in
# a swap tx are the trader's landing tips, not part of the fill price
TIP_ACCOUNTS = {
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe", "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5", "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt", "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY", "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
    "4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE", "D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ",
    "9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta",
}
SYSTEM = "11111111111111111111111111111111"
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58decode(s):
    n = 0
    for ch in s:
        n = n * 58 + _B58.index(ch)
    out = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    pad = len(s) - len(s.lstrip("1"))
    return b"\x00" * pad + out


def _tips(msg, keys, wallet, sol_raw=0):
    """lamports the wallet paid on the side in this tx: landing tips (Jito /
    Helius accounts) and trading-bot fees (any small SystemProgram transfer
    out of the wallet). A transfer bigger than 10 % of the SOL moved is not a
    fee - it is the WSOL wrap of a temporary account - and is left alone."""
    total = 0
    cap = max(0.10 * abs(sol_raw), 1)
    ixs = list(msg.get("instructions") or [])
    for inner in (msg.get("_inner") or []):
        ixs += inner
    for ix in ixs:
        try:
            if keys[ix["programIdIndex"]] != SYSTEM:
                continue
            data = b58decode(ix["data"])
            if len(data) >= 12 and int.from_bytes(data[:4], "little") == 2:
                src, dst = keys[ix["accounts"][0]], keys[ix["accounts"][1]]
                amt = int.from_bytes(data[4:12], "little")
                if src == wallet and dst != wallet and (dst in TIP_ACCOUNTS or amt <= cap):
                    total += amt
        except Exception:
            continue
    return total


def filter(logs):
    """venue label if these logs look like a non-pump DEX swap, else None."""
    venue = None
    for ln in logs:
        if ln.startswith("Program ") and " invoke [" in ln:
            pid = ln[8:ln.find(" invoke")]
            if pid in PUMP_PROGS:
                return None                       # the tape/execd decode these exactly
            if pid in DEX and venue is None:
                venue = DEX[pid]
    if venue:
        return venue
    for ln in logs:
        if any(w in ln for w in SWAP_WORDS):
            return "swap?"
    return None


def fetch(sig, client=None, tries=8, pause=0.5):
    """getTransaction at confirmed (processed is not served); retried while it lands."""
    c = client or httpx.Client(headers=UA, timeout=8)
    body = {"jsonrpc": "2.0", "id": 1, "method": "getTransaction",
            "params": [sig, {"encoding": "json", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}]}
    for i in range(tries):
        for url in (RPC2, RPC):
            try:
                r = c.post(url, json=body).json()
                if r.get("result"):
                    return r["result"]
            except Exception:
                pass
        time.sleep(pause)
    return None


def decode(tx, wallet):
    """the wallet's own trade from its balance deltas. None if it is not a
    one-token-vs-SOL trade by this wallet (fee payer)."""
    try:
        msg = tx["transaction"]["message"]
        keys = list(msg["accountKeys"])
        la = (tx.get("meta") or {}).get("loadedAddresses") or {}
        keys += la.get("writable", []) + la.get("readonly", [])
        meta = tx["meta"]
    except Exception:
        return None
    if not keys or keys[0] != wallet or meta.get("err"):
        return None
    pre = {(b["accountIndex"]): b for b in meta.get("preTokenBalances") or []}
    post = {(b["accountIndex"]): b for b in meta.get("postTokenBalances") or []}
    delta = {}                                  # mint -> (raw delta, decimals)
    acct = {}                                   # mint -> the wallet's token account address
    created = 0
    for idx in set(pre) | set(post):
        b = post.get(idx) or pre.get(idx)
        if b.get("owner") != wallet:
            continue
        p0 = int((pre.get(idx) or {}).get("uiTokenAmount", {}).get("amount", 0) or 0)
        p1 = int((post.get(idx) or {}).get("uiTokenAmount", {}).get("amount", 0) or 0)
        if idx not in pre and idx in post:
            created += 1
        d, dec = delta.get(b["mint"], (0, b["uiTokenAmount"]["decimals"]))
        delta[b["mint"]] = (d + (p1 - p0), b["uiTokenAmount"]["decimals"])
        if idx < len(keys):
            acct[b["mint"]] = keys[idx]
    fee = int(meta.get("fee", 0))
    msg = dict(msg)
    msg["_inner"] = [i.get("instructions") or [] for i in (meta.get("innerInstructions") or [])]
    sol_raw = int(meta["postBalances"][0]) - int(meta["preBalances"][0]) + fee + created * ATA_RENT + delta.get(WSOL, (0, 9))[0]
    tips = _tips(msg, keys, wallet, sol_raw)    # landing tips + bot fees paid on the side
    sol_native = int(meta["postBalances"][0]) - int(meta["preBalances"][0]) + fee + created * ATA_RENT + tips
    sol_wsol = delta.pop(WSOL, (0, 9))[0]
    sol = sol_native + sol_wsol                 # lamports gained (negative = spent), tips excluded
    toks = {m: d for m, d in delta.items() if d[0] != 0 and m not in STABLES}
    if len(toks) != 1:
        return None                             # token-to-token or multi-leg: not our lane
    mint, (dtok, dec) = next(iter(toks.items()))
    if dtok > 0 and sol < 0:
        side = "buy"
    elif dtok < 0 and sol > 0:
        side = "sell"
    else:
        return None
    sol_f, tok_f = abs(sol) / 1e9, abs(dtok) / (10 ** dec)
    if sol_f < 0.0005 or tok_f <= 0:
        return None
    venue = None
    for ln in meta.get("logMessages") or []:
        if ln.startswith("Program ") and " invoke [" in ln:
            pid = ln[8:ln.find(" invoke")]
            if pid in DEX:
                venue = DEX[pid]
                break
    return {"mint": mint, "side": side, "sol": sol_f, "tok": tok_f, "price": sol_f / tok_f,
            "ts": tx.get("blockTime"), "slot": tx.get("slot"), "venue": venue or "swap?", "decimals": dec,
            "ata": acct.get(mint), "post_raw": int((post.get(next((i for i in post if post[i].get("owner") == wallet and post[i]["mint"] == mint), -1)) or {}).get("uiTokenAmount", {}).get("amount", 0) or 0)}


def _throttle():
    with _jlock:
        wait = 0.7 - (time.time() - _jlast[0])
        if wait > 0:
            time.sleep(wait)
        _jlast[0] = time.time()


def quote(client, in_mint, out_mint, amount_raw, slippage_bps=300):
    _throttle()
    t = time.perf_counter()
    r = client.get(JUP_Q, params={"inputMint": in_mint, "outputMint": out_mint, "amount": int(amount_raw),
                                  "slippageBps": slippage_bps})
    ms = int((time.perf_counter() - t) * 1000)
    if r.status_code != 200:
        raise RuntimeError(f"jup quote {r.status_code}")
    q = r.json()
    route = "+".join(s.get("swapInfo", {}).get("label", "?") for s in q.get("routePlan", []))
    return q, int(q.get("outAmount", 0)), route, ms


def swap_tx(client, q, user):
    """Jupiter builds the whole transaction for `user` (unsigned, base64)."""
    _throttle()
    t = time.perf_counter()
    r = client.post(JUP_S, json={"quoteResponse": q, "userPublicKey": user, "wrapAndUnwrapSol": True,
                                 "dynamicComputeUnitLimit": True})
    ms = int((time.perf_counter() - t) * 1000)
    if r.status_code != 200:
        raise RuntimeError(f"jup swap {r.status_code}")
    return r.json().get("swapTransaction"), ms


def sim_tokens_out(client, tx_b64, ata, rpc=RPC2):
    """simulate (sigVerify off) and read the user's ATA balance afterwards."""
    body = {"jsonrpc": "2.0", "id": 1, "method": "simulateTransaction",
            "params": [tx_b64, {"sigVerify": False, "replaceRecentBlockhash": True, "encoding": "base64",
                                "commitment": "processed",
                                "accounts": {"encoding": "jsonParsed", "addresses": [ata]}}]}
    t = time.perf_counter()
    r = client.post(rpc, json=body).json()
    ms = int((time.perf_counter() - t) * 1000)
    v = (r.get("result") or {}).get("value") or {}
    err = v.get("err") if v else r.get("error")
    bal = None
    try:
        acc = (v.get("accounts") or [None])[0]
        bal = int(acc["data"]["parsed"]["info"]["tokenAmount"]["amount"])
    except Exception:
        pass
    return {"err": err, "balance": bal, "units": v.get("unitsConsumed"), "ms": ms,
            "logs": (v.get("logs") or [])[-4:]}


if __name__ == "__main__":
    # self-test: decode recent trades of an all-DEX wallet from copybot.trades and
    # compare with what Dune said (side / sol / tok)
    import sqlite3
    import sys
    import store
    c = sqlite3.connect(f"file:{store.DB}?mode=ro", uri=True, timeout=3)
    rows = c.execute("SELECT t.sig, t.wallet, t.side, t.sol, t.tokens, t.mint FROM trades t JOIN lab_wallets w ON w.address=t.wallet "
                     "WHERE w.note LIKE 'dune:alldex%' ORDER BY t.ts DESC LIMIT 8").fetchall()
    c.close()
    if not rows:
        print("no all-DEX trades stored yet")
        sys.exit()
    h = httpx.Client(headers=UA, timeout=10)
    ok = 0
    for sig, w, side, sol, tok, mint in rows:
        t = time.perf_counter()
        tx = fetch(sig, h, tries=2)
        ms = (time.perf_counter() - t) * 1000
        if not tx:
            print(f"  {sig[:12]} fetch failed ({ms:.0f} ms)"); continue
        d = decode(tx, w)
        if not d:
            print(f"  {sig[:12]} decode None (dune said {side} {sol:.3f} SOL {tok:.1f} tok {mint[:8]})"); continue
        # copybot.trades stores signed legs (buys: sol < 0; sells: tokens < 0); Dune
        # rows are per swap leg while we decode the whole tx, so compare PRICE
        sol, tok = abs(sol), abs(tok)
        dune_px = sol / tok if tok else 0
        good = d["side"] == side and d["mint"] == mint and dune_px and abs(d["price"] / dune_px - 1) < 0.02
        ok += good
        print(f"  {sig[:12]} {d['venue']:14} {d['side']} {d['sol']:.4f} SOL {d['tok']:.2f} tok px {d['price']:.3e}  vs dune {side} "
              f"{sol:.4f} {tok:.2f} px {dune_px:.3e}  {'OK' if good else 'MISMATCH'}  fetch {ms:.0f} ms")
    print(f"decoder agrees with Dune on {ok}/{len(rows)}")
