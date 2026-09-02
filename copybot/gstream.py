"""GSTREAM - Yellowstone gRPC (Geyser) transaction stream for the executor.

WHY: a websocket hands us a trade when its BLOCK is complete (~0.28 s after
the first shred). A Geyser gRPC stream emits each transaction while the bank
is still replaying the block, at PROCESSED commitment, and the message
carries the full transaction + meta (pre/post token balances). That is the
paid bots' detection feed, and it also removes the generic lane's wait for
`confirmed` (~0.5-1 s) on Raydium / Meteora / Orca.

WHAT IT COSTS: every seller with a monthly plan is $98-499/mo, but two
pay-per-byte options exist with no minimum (Alchemy $75/TB; Triton
$0.08/GB after a $125 deposit) and PublicNode issues personal tokens at
allnodes.com/publicnode. A few hundred filtered wallets is megabytes a
month. Brady creates the account; this client takes the token/URL from
the environment or `gstream.txt` and everything else is ready.

    python gstream.py --probe            connect to each configured endpoint, report auth/first message
    python gstream.py --watch W1,W2 --secs 60   stream those wallets' transactions, print decoded trades

Config (first found wins): env GSTREAM_URL / GSTREAM_TOKEN, or copybot/gstream.txt with
    url=solana-yellowstone-grpc.publicnode.com:443
    token=...
"""
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "yellowstone"))
import grpc
import geyser_pb2
import geyser_pb2_grpc

WSOL = "So11111111111111111111111111111111111111112"
DEFAULTS = [("publicnode", "solana-yellowstone-grpc.publicnode.com:443", None)]


def config():
    """(url, token) of the primary stream - kept for the tools; see endpoints()."""
    eps = endpoints()
    return (eps[0]["url"], eps[0]["token"]) if eps else (None, None)


def endpoints():
    """Every configured stream, in file order: gstream.txt lines
        url=host:443           token=...        (primary)
        url2=host:443          token2=...       (a second vendor to race, e.g. Alchemy / Triton Deshred)
    or env GSTREAM_URL / GSTREAM_TOKEN. The executor runs one stream per
    endpoint and the first arrival wins (signatures are deduped)."""
    out = []
    if os.environ.get("GSTREAM_URL"):
        out.append({"name": "env", "url": os.environ["GSTREAM_URL"], "token": os.environ.get("GSTREAM_TOKEN") or None})
    p = os.path.join(HERE, "gstream.txt")
    if os.path.exists(p):
        kv = {}
        for ln in open(p, encoding="utf-8"):
            k, _, v = ln.strip().partition("=")
            if k:
                kv[k.strip()] = v.strip()
        for suffix in ("", "2", "3", "4"):
            u = kv.get("url" + suffix)
            if u:
                out.append({"name": kv.get("name" + suffix) or u.split(":")[0].split(".")[0] or ("ep" + suffix),
                            "url": u, "token": kv.get("token" + suffix) or None})
    return out


def channel(url, token=None):
    creds = grpc.ssl_channel_credentials()
    if token:
        call = grpc.metadata_call_credentials(lambda ctx, cb: cb((("x-token", token),), None))
        creds = grpc.composite_channel_credentials(creds, call)
    return grpc.secure_channel(url, creds, options=[("grpc.max_receive_message_length", 64 * 1024 * 1024),
                                                   ("grpc.keepalive_time_ms", 20000)])


def subscribe_request(wallets, commitment="PROCESSED", failed=False):
    """failed=True also streams the wallets' FAILED transactions: a whale's failed buy
    (slippage / stale blockhash) names the mint and size ~1-2 s before the retry lands."""
    req = geyser_pb2.SubscribeRequest()
    f = req.transactions["watched"]
    f.vote = False
    f.failed = failed
    for w in wallets:
        f.account_include.append(w)
    req.commitment = geyser_pb2.CommitmentLevel.Value(commitment)
    return req


def b58(b):
    """base58 for 32-byte pubkeys AND 64-byte signatures (gRPC gives raw bytes)."""
    b = bytes(b)
    if len(b) == 32:
        from solders.pubkey import Pubkey
        return str(Pubkey(b))
    if len(b) == 64:
        from solders.signature import Signature
        return str(Signature.from_bytes(b))
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    n = int.from_bytes(b, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = alphabet[r] + out
    return "1" * (len(b) - len(b.lstrip(b"\x00"))) + out


TIP_ACCOUNTS = {
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe", "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5", "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt", "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY", "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
    "4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE", "D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ",
    "9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta",
}
SYSTEM = "11111111111111111111111111111111"


def _tips(tx, msg, keys, wallet, sol_raw):
    """lamports the wallet paid on the side in this transaction: landing tips and
    trading-bot fees (SystemProgram transfers out of the wallet; anything bigger
    than 10 % of the SOL moved is a WSOL wrap, not a fee). Proto instructions
    carry raw bytes: data[0:4] = 2 (transfer), lamports u64 at 4..12."""
    total = 0
    cap = max(0.10 * abs(sol_raw), 1)
    ixs = list(msg.instructions)
    for inner in tx.meta.inner_instructions:
        ixs += list(inner.instructions)
    for ix in ixs:
        try:
            if ix.program_id_index >= len(keys) or keys[ix.program_id_index] != SYSTEM:
                continue
            data = bytes(ix.data)
            if len(data) >= 12 and int.from_bytes(data[:4], "little") == 2:
                acc = bytes(ix.accounts)
                src, dst = keys[acc[0]], keys[acc[1]]
                amt = int.from_bytes(data[4:12], "little")
                if src == wallet and dst != wallet and (dst in TIP_ACCOUNTS or amt <= cap):
                    total += amt
        except Exception:
            continue
    return total


def decode_update(upd, watched):
    """SubscribeUpdateTransaction -> the watched wallet's trade from balance
    deltas (same rules as generic.decode) or None."""
    # SubscribeUpdate.transaction -> SubscribeUpdateTransaction{transaction: Info, slot}
    # Info -> {signature, is_vote, transaction: Transaction{message}, meta, index}
    tx = upd.transaction.transaction
    msg = tx.transaction.message
    keys = [b58(k) for k in msg.account_keys]
    keys += [b58(k) for k in tx.meta.loaded_writable_addresses] + [b58(k) for k in tx.meta.loaded_readonly_addresses]
    if not keys or tx.meta.err.err:
        return None
    wallet = keys[0] if keys[0] in watched else None
    if not wallet:
        return None
    pre = {b.account_index: b for b in tx.meta.pre_token_balances}
    post = {b.account_index: b for b in tx.meta.post_token_balances}
    delta, created, acct, post_raw = {}, 0, {}, {}
    for idx in set(pre) | set(post):
        b = post.get(idx) or pre.get(idx)
        if b.owner != wallet:
            continue
        p0 = int(pre[idx].ui_token_amount.amount or 0) if idx in pre else 0
        p1 = int(post[idx].ui_token_amount.amount or 0) if idx in post else 0
        if idx not in pre and idx in post:
            created += 1
        d, dec = delta.get(b.mint, (0, b.ui_token_amount.decimals))
        delta[b.mint] = (d + p1 - p0, b.ui_token_amount.decimals)
        if idx < len(keys):
            acct[b.mint] = keys[idx]
        post_raw[b.mint] = p1
    sol_raw = int(tx.meta.post_balances[0]) - int(tx.meta.pre_balances[0]) + int(tx.meta.fee) + created * 2_039_280
    sol_raw += delta.get(WSOL, (0, 9))[0]
    # landing tips and bot fees paid on the side (SystemProgram transfers out of the
    # wallet to tip accounts, or any small transfer) are not part of the fill price -
    # same rule as generic.decode; without it the overnight gRPC rows read +190 %
    sol = sol_raw + _tips(tx, msg, keys, wallet, sol_raw)
    delta.pop(WSOL, None)
    STABLES = {"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}
    toks = {m: d for m, d in delta.items() if d[0] != 0 and m not in STABLES}
    if len(toks) != 1:
        return None
    mint, (dtok, dec) = next(iter(toks.items()))
    if dtok > 0 and sol < 0:
        side = "buy"
    elif dtok < 0 and sol > 0:
        side = "sell"
    else:
        return None
    if abs(sol) < 500_000 or abs(dtok) <= 0:     # dust / fee-only movements are not trades
        return None
    return {"wallet": wallet, "mint": mint, "side": side, "sol": abs(sol) / 1e9, "tok": abs(dtok) / 10 ** dec,
            "slot": upd.transaction.slot, "sig": b58(tx.signature) if tx.signature else None,
            "decimals": dec, "ata": acct.get(mint), "post_raw": post_raw.get(mint, 0)}


def stream(url, token, wallets, secs=60, on_trade=print):
    ch = channel(url, token)
    stub = geyser_pb2_grpc.GeyserStub(ch)
    req = subscribe_request(wallets)
    t0 = time.time()
    n = 0
    for upd in stub.Subscribe(iter([req]), timeout=secs + 5):
        n += 1
        if upd.HasField("transaction"):
            d = decode_update(upd, set(wallets))
            if d:
                d["seen"] = time.time()
                on_trade(d)
        if time.time() - t0 > secs:
            break
    ch.close()
    return n


def probe():
    url, tok = config()
    eps = ([("configured", url, tok)] if url else []) + DEFAULTS
    for name, u, t in eps:
        print(f"{name:11} {u}  token={'yes' if t else 'no'}")
        try:
            ch = channel(u, t)
            stub = geyser_pb2_grpc.GeyserStub(ch)
            t0 = time.time()
            v = stub.GetVersion(geyser_pb2.GetVersionRequest(), timeout=10)
            print(f"   OK GetVersion in {(time.time() - t0) * 1000:.0f} ms: {v.version[:80]}")
            ch.close()
        except grpc.RpcError as e:
            print(f"   {e.code().name}: {e.details()[:140]}")
        except Exception as e:
            print(f"   {type(e).__name__}: {str(e)[:120]}")


if __name__ == "__main__":
    if "--probe" in sys.argv or len(sys.argv) == 1:
        probe()
    elif "--watch" in sys.argv:
        ws = sys.argv[sys.argv.index("--watch") + 1].split(",")
        secs = int(sys.argv[sys.argv.index("--secs") + 1]) if "--secs" in sys.argv else 60
        url, tok = config()
        if not url:
            url, tok = DEFAULTS[0][1], None
        print("streaming", len(ws), "wallets from", url, "for", secs, "s")
        try:
            n = stream(url, tok, ws, secs)
            print("updates:", n)
        except grpc.RpcError as e:
            print(f"{e.code().name}: {e.details()[:200]}")
