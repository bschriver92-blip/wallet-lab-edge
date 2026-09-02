"""SENDPATH - sign a locally built swap, fire it at the network through every
free lane at once, and time its landing.

Lanes (one signed transaction, same bytes everywhere):
  helius-ewr  Helius Sender, Newark, SWQOS-only  (staked lane; tip 0.000005 SOL, no plan/credits)
  jito-ny     Jito block engine transactions endpoint (tip to a Jito tip account; 1 req/s/IP)
  beta        api.mainnet-beta.solana.com sendTransaction (unstaked)
  publicnode  solana-rpc.publicnode.com sendTransaction (unstaked)
Both tips ride in the same transaction so every lane accepts it:
  ~5,000 (Helius) + ~10,000 (Jito) lamports + priority fee ~12,500 = ~0.00003 SOL a trade.

SAFETY
  Nothing here holds a funded key. `execute()` is the only function that
  sends, and execd calls it only when all three locks are open (keyfile +
  `forge.py arm N` + `forge.py live`). `--test` signs with a throwaway,
  UNFUNDED keypair generated in memory: it proves the signature and the
  wire format (every lane answers "insufficient funds", which is the point)
  and measures each lane's round trip. It can never land.

    python sendpath.py --test          # plumbing test with an unfunded throwaway key
"""
import base64
import json
import os
import random
import sqlite3
import sys
import threading
import time

import httpx
from solders import compute_budget
from solders.hash import Hash
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction

HERE = os.path.dirname(os.path.abspath(__file__))
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) wallet-lab/1.0", "Content-Type": "application/json"}
RPC = "https://api.mainnet-beta.solana.com"

HELIUS_TIPS = ["4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE", "D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ",
               "9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta"]
JITO_TIPS = ["HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe", "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
             "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5", "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
             "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt", "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
             "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY", "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT"]
LANES = {
    # measured from this PC 09-02 (TCP RTT): ewr 40 ms, jito-ny 58, EU ingress 104-117.
    # EU holds ~2/3 of stake: the EU lanes are a free fan-out for leaders there -
    # one signature, every ingress, the leader dedupes.
    "helius-ewr": "http://ewr-sender.helius-rpc.com/fast?swqos_only=true",
    "jito-ny": "https://ny.mainnet.block-engine.jito.wtf/api/v1/transactions",
    "helius-fra": "http://fra-sender.helius-rpc.com/fast?swqos_only=true",
    "jito-fra": "https://frankfurt.mainnet.block-engine.jito.wtf/api/v1/transactions",
    "helius-ams": "http://ams-sender.helius-rpc.com/fast?swqos_only=true",
    "beta": RPC,
    "publicnode": "https://solana-rpc.publicnode.com",
}
HELIUS_TIP = 5_000        # lamports, SWQOS-only minimum
JITO_TIP = 10_000         # lamports, ~p75 of landed tips at a quiet hour (floor API: bundles.jito.wtf)
CU_LIMIT = 250_000
# per-venue limits from 36 h of our own simulations (09-02): pump buy median
# 94k / p90 108k, pump sell 76k / 84k, PumpSwap buy 106k / 121k, PumpSwap
# sell 98k / 185k. A tight limit makes every micro-lamport of priority fee
# buy more position (the leader prices fee PER CU REQUESTED) and packs
# earlier when blocks are near their CU cap. p90 x 1.3, never below p90 x 1.2.
CU_LIMITS = {"pump-buy": 140_000, "pump-sell": 110_000, "pswap-buy": 160_000, "pswap-sell": 240_000}


def cu_limit_for(kind):
    return CU_LIMITS.get(kind, CU_LIMIT)
CU_PRICE = 50_000         # micro-lamports per CU -> 12,500 lamports at the limit
# live landing prices (fees.py polls the Jito tip floor + recent priority fees);
# the constants above are the fallback when nothing is cached
try:
    import fees
except Exception:              # pragma: no cover
    fees = None
LAST = {"cu_price": CU_PRICE, "jito_tip": JITO_TIP, "src": "const"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS sends(
  sig TEXT PRIMARY KEY, kind TEXT, whale_sig TEXT, whale_slot INTEGER, t_built REAL, t_sent REAL,
  responses TEXT, t_landed REAL, slot_landed INTEGER, slots_behind INTEGER, status TEXT, err TEXT,
  tip_lamports INTEGER, note TEXT);
"""


def load_keypair(path):
    """solana-keygen JSON array, or a base58 secret on one line."""
    raw = open(path, encoding="utf-8").read().strip()
    if raw.startswith("["):
        return Keypair.from_bytes(bytes(json.loads(raw)))
    return Keypair.from_base58_string(raw)


def tip_ixs(payer, helius=HELIUS_TIP, jito=JITO_TIP):
    return [transfer(TransferParams(from_pubkey=payer, to_pubkey=Pubkey.from_string(random.choice(HELIUS_TIPS)), lamports=helius)),
            transfer(TransferParams(from_pubkey=payer, to_pubkey=Pubkey.from_string(random.choice(JITO_TIPS)), lamports=jito))]


def build_signed(kp, ixs, blockhash, cu_limit=CU_LIMIT, cu_price=None, tips=True, jito_tip=None):
    """cu_price / jito_tip default to the LIVE market level (fees.suggest():
    75th percentile of landed Jito tips, 75th percentile of recent priority
    fees on the pump programs), falling back to the constants."""
    if cu_price is None or jito_tip is None:
        sug = fees.suggest() if fees else {"cu_price": CU_PRICE, "jito_tip": JITO_TIP, "src": "const"}
        cu_price = sug["cu_price"] if cu_price is None else cu_price
        jito_tip = sug["jito_tip"] if jito_tip is None else jito_tip
        LAST.update({"cu_price": cu_price, "jito_tip": jito_tip, "src": sug.get("src", "?")})
    front = [compute_budget.set_compute_unit_limit(cu_limit), compute_budget.set_compute_unit_price(cu_price)]
    tail = tip_ixs(kp.pubkey(), jito=jito_tip) if tips else []
    msg = MessageV0.try_compile(kp.pubkey(), front + list(ixs) + tail, [], Hash.from_string(blockhash))
    return VersionedTransaction(msg, [kp])


def _post(client, url, body, out, name):
    t = time.perf_counter()
    try:
        r = client.post(url, json=body)
        ms = (time.perf_counter() - t) * 1000
        try:
            j = r.json()
        except Exception:
            j = {"raw": r.text[:200]}
        out[name] = {"ms": round(ms), "code": r.status_code,
                     "result": j.get("result"), "error": (j.get("error") or {}).get("message", j.get("error")) if isinstance(j, dict) else str(j)[:200]}
    except Exception as e:
        out[name] = {"ms": round((time.perf_counter() - t) * 1000), "code": None, "error": f"{type(e).__name__}: {str(e)[:120]}"}


def send(tx, client=None, lanes=LANES):
    """fire the same signed transaction at every lane at once; returns per-lane responses."""
    b64 = base64.b64encode(bytes(tx)).decode()
    body = {"jsonrpc": "2.0", "id": 1, "method": "sendTransaction",
            "params": [b64, {"encoding": "base64", "skipPreflight": True, "maxRetries": 0}]}
    c = client or httpx.Client(headers=UA, timeout=8)
    out, threads = {}, []
    for name, url in lanes.items():
        th = threading.Thread(target=_post, args=(c, url, body, out, name), daemon=True)
        th.start()
        threads.append(th)
    for th in threads:
        th.join(timeout=8)
    return out


def await_landing(sig, client=None, timeout=25.0, every=0.35):
    """poll getSignatureStatuses until the signature shows a slot."""
    c = client or httpx.Client(headers=UA, timeout=8)
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            r = c.post(RPC, json={"jsonrpc": "2.0", "id": 1, "method": "getSignatureStatuses",
                                  "params": [[sig], {"searchTransactionHistory": False}]}).json()
            v = (r.get("result") or {}).get("value") or [None]
            if v and v[0]:
                return {"t_landed": time.time(), "slot": v[0].get("slot"), "status": v[0].get("confirmationStatus"),
                        "err": json.dumps(v[0].get("err")) if v[0].get("err") else None}
        except Exception:
            pass
        time.sleep(every)
    return None


def record(row):
    import store
    c = store.db()
    c.executescript(SCHEMA)
    c.execute("INSERT OR REPLACE INTO sends(sig,kind,whale_sig,whale_slot,t_built,t_sent,responses,t_landed,slot_landed,"
              "slots_behind,status,err,tip_lamports,note) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
              (row.get("sig"), row.get("kind"), row.get("whale_sig"), row.get("whale_slot"), row.get("t_built"),
               row.get("t_sent"), json.dumps(row.get("responses")), row.get("t_landed"), row.get("slot_landed"),
               row.get("slots_behind"), row.get("status"), row.get("err"), row.get("tip_lamports"), row.get("note")))
    c.commit()
    c.close()


def execute(kp, ixs, blockhash, kind="swap", whale_sig=None, whale_slot=None, client=None, wait=True, note=""):
    """THE live path: build, sign, fan out, time the landing, record. Only execd
    calls this, only with the real keyfile, only when armed and live."""
    t0 = time.time()
    tx = build_signed(kp, ixs, blockhash, cu_limit=cu_limit_for(kind))
    sig = str(tx.signatures[0])
    t1 = time.time()
    resp = send(tx, client)
    t2 = time.time()
    row = {"sig": sig, "kind": kind, "whale_sig": whale_sig, "whale_slot": whale_slot, "t_built": t1,
           "t_sent": t2, "responses": resp, "tip_lamports": HELIUS_TIP + LAST["jito_tip"],
           "note": f"{note} cu={LAST['cu_price']} lim={cu_limit_for(kind)} tip={LAST['jito_tip']} {LAST['src']}".strip()}
    accepted = [n for n, r in resp.items() if r.get("result")]
    if wait and accepted:
        land = await_landing(sig, client)
        if land:
            row.update({"t_landed": land["t_landed"], "slot_landed": land["slot"], "status": land["status"], "err": land["err"],
                        "slots_behind": (land["slot"] - whale_slot) if (whale_slot and land["slot"]) else None})
        else:
            row["status"] = "not landed in 25s"
    elif not accepted:
        row["status"] = "rejected by every lane"
    try:
        record(row)
    except Exception as e:
        row["record_error"] = str(e)[:100]
    return row


def blockhash(client=None):
    c = client or httpx.Client(headers=UA, timeout=8)
    return c.post(RPC, json={"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash",
                             "params": [{"commitment": "confirmed"}]}).json()["result"]["value"]["blockhash"]


def simulate_signed(tx, client=None):
    """sigVerify ON: proves our signature is valid (a bad one fails here, not on-chain)."""
    c = client or httpx.Client(headers=UA, timeout=10)
    r = c.post(RPC, json={"jsonrpc": "2.0", "id": 1, "method": "simulateTransaction",
                          "params": [base64.b64encode(bytes(tx)).decode(),
                                     {"sigVerify": True, "encoding": "base64", "commitment": "processed"}]}).json()
    v = (r.get("result") or {}).get("value") or {}
    return {"err": v.get("err") if v else r.get("error"), "logs": (v.get("logs") or [])[-3:]}


if __name__ == "__main__" and "--test" in sys.argv:
    # plumbing test with an unfunded throwaway key: proves signing + format + each lane's RTT
    kp = Keypair()
    c = httpx.Client(headers=UA, timeout=8)
    bh = blockhash(c)
    ixs = [transfer(TransferParams(from_pubkey=kp.pubkey(), to_pubkey=kp.pubkey(), lamports=1))]
    t = time.perf_counter()
    tx = build_signed(kp, ixs, bh)
    print(f"signed in {(time.perf_counter() - t) * 1000:.2f} ms, {len(bytes(tx))} bytes, sig {str(tx.signatures[0])[:16]}..., payer {str(kp.pubkey())[:8]} (unfunded, throwaway)")
    s = simulate_signed(tx, c)
    print("simulate with sigVerify=true:", s["err"], "| expected: an insufficient-funds style error, NOT a signature error")
    # warm the connections once so the timed fan-out measures the send, not the handshake
    for name, url in LANES.items():
        try:
            c.post(url, json={"jsonrpc": "2.0", "id": 1, "method": "getHealth"})
        except Exception:
            pass
    t = time.perf_counter()
    resp = send(tx, c)
    print(f"fan-out to {len(LANES)} lanes took {(time.perf_counter() - t) * 1000:.0f} ms wall:")
    for name, r in resp.items():
        print(f"  {name:11} {r['ms']:4} ms  code {r['code']}  result {str(r.get('result'))[:20]}  error {str(r.get('error'))[:110]}")
