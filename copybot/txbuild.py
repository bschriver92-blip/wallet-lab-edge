"""TXBUILD - build pump.fun and PumpSwap swaps LOCALLY, price them LOCALLY,
and prove them for free by simulating AS THE WHALE.

Why: the whale's own trade event (decoded from the free websocket) carries
everything a swap needs - mint, curve/pool reserves, fee bps, creator, fee
recipient - so a copy can be priced and built in well under a millisecond
with zero network hops. Jupiter's quote (44 ms warm) + /swap build (83 ms)
are dead weight, and its free tier is 1 request/s anyway.

Validation without money: `simulateTransaction` with sigVerify off lets the
fee payer be ANY funded account. Building our exact instruction with the
whale as the payer and simulating it against live state proves the account
list, the data layout and the local price - the program's own emitted event
in the simulation logs is the ground truth for tokens-out.

Layouts from pump-public-docs IDLs (pump.json 2026-05-18, pump_amm.json
2026-07-15). Nothing here signs or sends anything.
"""
import base64
import json
import struct
import time

import httpx
from solders import compute_budget
from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction

PK = Pubkey.from_string
PUMP = PK("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
PAMM = PK("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA")
PFEE = PK("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ")
WSOL = PK("So11111111111111111111111111111111111111112")
TOKEN = PK("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
TOKEN22 = PK("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
ATA_PROG = PK("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
SYSTEM = PK("11111111111111111111111111111111")

NORMAL_FEE = [PK(x) for x in (
    "62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV", "7VtfL8fvgNfhz17qKRMjzQEXgbdpnHHHQRh54R9jP2RJ",
    "7hTckgnGnLQR6sdH7YkqFTAA7VwTfYFaZ6EhEsU3saCX", "9rPYyANsfQZw3DnDmKE3YCQF5E8oD89UXoHn9JFEhJUz",
    "AVmoTthdrX6tKt4nDjco2D775W2YK3sDhxPcMmzUAmTY", "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM",
    "FWsW1xNtWscwNmKv6wVsU1iTzRN6wmmk3MjxRP5tT7hz", "G5UZAVbAf46s7cKWoyKu8kYTip9DGTpbLZ2qa9Aq69dP")]
RESERVED_FEE = [PK(x) for x in (
    "GesfTA3X2arioaHp8bbKdjG9vJtskViWACZoYvxp4twS", "4budycTjhs9fD6xw62VBducVTNgMgJJ5BgtKq7mAZwn6",
    "8SBKzEQU4nLSzcwF4a74F2iaUDQyTfjGndn6qUWBnrpR", "4UQeTP1T39KZ9Sfxzo3WR5skgsaP6NZa87BAkuazLEKH",
    "8sNeir4QsLsJdYpc9RZacohhK1Y5FLU3nC5LXgYB4aa6", "Fh9HmeLNUMVCvejxCtCL2DbYaRyBFVJ5xrWkLnMH6fdk",
    "463MEnMeGyJekNZFQSTUABBEbLnvMTALbT6ZmsxAbAdq", "6AUH3WEHucYZyC61hqpqYUWVto5qA5hjHuNQ32GNnNxA")]
BUYBACK_FEE = [PK(x) for x in (
    "5YxQFdt3Tr9zJLvkFccqXVUwhdTWJQc1fFg2YPbxvxeD", "9M4giFFMxmFGXtc3feFzRai56WbBqehoSeRE5GK7gf7",
    "GXPFM2caqTtQYC2cJ5yJRi9VDkpsYZXzYdwYpGnLmtDL", "3BpXnfJaUTiwXnJNe7Ej1rcbzqTTQUvLShZaWazebsVR",
    "5cjcW9wExnJJiqgLjq7DEG75Pm6JBgE1hNv4B2vHXUW6", "EHAAiTxcdDwQ3U4bU6YcMsQGaekdzLS3B5SmYo46kJtL",
    "5eHhjP8JaYkz83CWwvGU2uMUXefd3AazWGx4gpcuEEYD", "A7hAgCzFw14fejgCp387JUJRMNyz4j89JKnhtKU8piqW")]
PAMM_PROTOCOL_FEE = [PK(x) for x in (
    "62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV", "7VtfL8fvgNfhz17qKRMjzQEXgbdpnHHHQRh54R9jP2RJ",
    "7hTckgnGnLQR6sdH7YkqFTAA7VwTfYFaZ6EhEsU3saCX", "9rPYyANsfQZw3DnDmKE3YCQF5E8oD89UXoHn9JFEhJUz",
    "AVmoTthdrX6tKt4nDjco2D775W2YK3sDhxPcMmzUAmTY", "FWsW1xNtWscwNmKv6wVsU1iTzRN6wmmk3MjxRP5tT7hz",
    "G5UZAVbAf46s7cKWoyKu8kYTip9DGTpbLZ2qa9Aq69dP", "JCRGumoE9Qi5BBgULTgdgTLjSgkCMSbF62ZZfGs84JeU")]


def pda(seeds, prog):
    return Pubkey.find_program_address(seeds, prog)[0]


GLOBAL = pda([b"global"], PUMP)
EVAUTH_PUMP = pda([b"__event_authority"], PUMP)
EVAUTH_PAMM = pda([b"__event_authority"], PAMM)
GVA_PUMP = pda([b"global_volume_accumulator"], PUMP)
GVA_PAMM = pda([b"global_volume_accumulator"], PAMM)
FEECFG_PUMP = pda([b"fee_config", bytes(PUMP)], PFEE)
FEECFG_PAMM = pda([b"fee_config", bytes(PAMM)], PFEE)
GLOBAL_CONFIG = pda([b"global_config"], PAMM)
assert str(GLOBAL_CONFIG) == "ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw"

D_BUY_V2 = bytes([184, 23, 238, 97, 103, 197, 211, 61])
D_BUY_EXACT_V2 = bytes([194, 171, 28, 70, 104, 77, 91, 47])
D_SELL_V2 = bytes([93, 246, 130, 60, 231, 233, 64, 178])
D_PAMM_BUY = bytes([102, 6, 61, 18, 1, 218, 235, 234])
D_PAMM_BUY_EXACT = bytes([198, 46, 21, 82, 180, 217, 232, 112])
D_PAMM_SELL = bytes([51, 230, 133, 164, 1, 127, 131, 173])


def ata(owner, mint, prog=TOKEN):
    return pda([bytes(owner), bytes(prog), bytes(mint)], ATA_PROG)


def bonding_curve(mint):
    return pda([b"bonding-curve", bytes(mint)], PUMP)


def creator_vault(creator):
    return pda([b"creator-vault", bytes(creator)], PUMP)


def sharing_config(mint):
    return pda([b"sharing-config", bytes(mint)], PFEE)


def uva(user, prog):
    return pda([b"user_volume_accumulator", bytes(user)], prog)


def pamm_creator_vault_auth(coin_creator):
    return pda([b"creator_vault", bytes(coin_creator)], PAMM)


def _m(pk, w=False, s=False):
    return AccountMeta(pk, is_signer=s, is_writable=w)


# ------------------------------------------------------------- token helpers
def ata_create_idempotent(payer, owner, mint, prog=TOKEN):
    return Instruction(ATA_PROG, bytes([1]), [
        _m(payer, w=True, s=True), _m(ata(owner, mint, prog), w=True), _m(owner), _m(mint),
        _m(SYSTEM), _m(prog)])


def sync_native(acct):
    return Instruction(TOKEN, bytes([17]), [_m(acct, w=True)])


def close_account(acct, dest, owner):
    return Instruction(TOKEN, bytes([9]), [_m(acct, w=True), _m(dest, w=True), _m(owner, s=True)])


def wrap_sol(user, lamports):
    """create the user's WSOL ATA if needed, fund it, sync - three instructions."""
    w = ata(user, WSOL)
    return [ata_create_idempotent(user, user, WSOL),
            transfer(TransferParams(from_pubkey=user, to_pubkey=w, lamports=int(lamports))),
            sync_native(w)]


# ------------------------------------------------------------- pump.fun v2
def _pump_v2_accounts(user, mint, creator, base_prog, fee_recipient, buyback, with_gva):
    bc = bonding_curve(mint)
    cv = creator_vault(creator)
    u = uva(user, PUMP)
    accs = [
        _m(GLOBAL), _m(mint), _m(WSOL), _m(base_prog), _m(TOKEN), _m(ATA_PROG),
        _m(fee_recipient, w=True), _m(ata(fee_recipient, WSOL), w=True),
        _m(buyback, w=True), _m(ata(buyback, WSOL), w=True),
        _m(bc, w=True), _m(ata(bc, mint, base_prog), w=True), _m(ata(bc, WSOL), w=True),
        _m(user, w=True, s=True), _m(ata(user, mint, base_prog), w=True), _m(ata(user, WSOL), w=True),
        _m(cv, w=True), _m(ata(cv, WSOL), w=True),
        _m(sharing_config(mint)),
    ]
    if with_gva:
        accs.append(_m(GVA_PUMP))
    accs += [_m(u, w=True), _m(ata(u, WSOL), w=True), _m(FEECFG_PUMP), _m(PFEE), _m(SYSTEM),
             _m(EVAUTH_PUMP), _m(PUMP)]
    return accs


def pump_buy_exact_sol(user, mint, creator, spend_lamports, min_tokens_out, base_prog=TOKEN,
                       fee_recipient=None, mayhem=False, idx=0):
    """buy_exact_quote_in_v2: spend exactly `spend_lamports` (fees included),
    receive at least `min_tokens_out` base units. SOL-paired coins."""
    fr = fee_recipient or (RESERVED_FEE if mayhem else NORMAL_FEE)[idx % 8]
    data = D_BUY_EXACT_V2 + struct.pack("<QQ", int(spend_lamports), int(min_tokens_out))
    return Instruction(PUMP, data, _pump_v2_accounts(user, mint, creator, base_prog, fr, BUYBACK_FEE[idx % 8], True))


def pump_buy(user, mint, creator, amount_tokens, max_sol_cost, base_prog=TOKEN, fee_recipient=None, mayhem=False, idx=0):
    fr = fee_recipient or (RESERVED_FEE if mayhem else NORMAL_FEE)[idx % 8]
    data = D_BUY_V2 + struct.pack("<QQ", int(amount_tokens), int(max_sol_cost))
    return Instruction(PUMP, data, _pump_v2_accounts(user, mint, creator, base_prog, fr, BUYBACK_FEE[idx % 8], True))


def pump_sell(user, mint, creator, amount_tokens, min_sol_output, base_prog=TOKEN, fee_recipient=None, mayhem=False, idx=0):
    fr = fee_recipient or (RESERVED_FEE if mayhem else NORMAL_FEE)[idx % 8]
    data = D_SELL_V2 + struct.pack("<QQ", int(amount_tokens), int(min_sol_output))
    return Instruction(PUMP, data, _pump_v2_accounts(user, mint, creator, base_prog, fr, BUYBACK_FEE[idx % 8], False))


# ------------------------------------------------------------- PumpSwap
def _pamm_accounts(user, pool, base_mint, quote_mint, coin_creator, protocol_fee_recipient,
                   base_prog, quote_prog, with_volume, cashback=False):
    cva = pamm_creator_vault_auth(coin_creator)
    accs = [
        _m(pool, w=True), _m(user, w=True, s=True), _m(GLOBAL_CONFIG), _m(base_mint), _m(quote_mint),
        _m(ata(user, base_mint, base_prog), w=True), _m(ata(user, quote_mint, quote_prog), w=True),
        _m(ata(pool, base_mint, base_prog), w=True), _m(ata(pool, quote_mint, quote_prog), w=True),
        _m(protocol_fee_recipient), _m(ata(protocol_fee_recipient, quote_mint, quote_prog), w=True),
        _m(base_prog), _m(quote_prog), _m(SYSTEM), _m(ATA_PROG), _m(EVAUTH_PAMM), _m(PAMM),
        _m(ata(cva, quote_mint, quote_prog), w=True), _m(cva),
    ]
    if with_volume:
        accs += [_m(GVA_PAMM), _m(uva(user, PAMM), w=True)]
    accs += [_m(FEECFG_PAMM), _m(PFEE)]
    # remaining accounts the on-chain program now requires but the 07-15 IDL
    # does not list - found by diffing a real buy (sig 3vkoSEgj..., 09-01 23:30):
    # pool_v2 = PDA["pool-v2", base_mint] under PumpSwap, then a buyback fee
    # recipient and its WSOL ATA
    bb = BUYBACK_FEE[idx_of(user) % 8]
    if cashback:
        # cashback coins (creator fee rebated to the trader): the WSOL ATA of
        # the AMM's user_volume_accumulator comes FIRST in remaining accounts
        # (buy), and the accumulator itself second on sells - PUMP_CASHBACK_README
        ua = uva(user, PAMM)
        accs.append(_m(ata(ua, quote_mint, quote_prog), w=True))
        if not with_volume:
            accs.append(_m(ua, w=True))
    accs += [_m(pool_v2(base_mint), w=True), _m(bb, w=True), _m(ata(bb, quote_mint, quote_prog), w=True)]
    return accs


def pool_v2(base_mint):
    return pda([b"pool-v2", bytes(base_mint)], PAMM)


def idx_of(user):
    return bytes(user)[0]


def pamm_buy_exact_quote_in(user, pool, base_mint, coin_creator, spendable_quote_in, min_base_out,
                            base_prog=TOKEN, quote_mint=WSOL, quote_prog=TOKEN, protocol_fee_recipient=None,
                            idx=0, cashback=False):
    """spend exactly `spendable_quote_in` quote units (fees included), get >= min_base_out."""
    pfr = protocol_fee_recipient or PAMM_PROTOCOL_FEE[idx % 8]
    data = D_PAMM_BUY_EXACT + struct.pack("<QQ", int(spendable_quote_in), int(min_base_out)) + bytes([0])  # OptionBool: none
    return Instruction(PAMM, data, _pamm_accounts(user, pool, base_mint, quote_mint, coin_creator, pfr,
                                                   base_prog, quote_prog, True, cashback))


def pamm_buy(user, pool, base_mint, coin_creator, base_amount_out, max_quote_in,
             base_prog=TOKEN, quote_mint=WSOL, quote_prog=TOKEN, protocol_fee_recipient=None,
             idx=0, cashback=False):
    """buy exactly `base_amount_out` base units for at most `max_quote_in` quote."""
    pfr = protocol_fee_recipient or PAMM_PROTOCOL_FEE[idx % 8]
    data = D_PAMM_BUY + struct.pack("<QQ", int(base_amount_out), int(max_quote_in)) + bytes([0])
    return Instruction(PAMM, data, _pamm_accounts(user, pool, base_mint, quote_mint, coin_creator, pfr,
                                                   base_prog, quote_prog, True, cashback))


def pamm_sell(user, pool, base_mint, coin_creator, base_amount_in, min_quote_out,
              base_prog=TOKEN, quote_mint=WSOL, quote_prog=TOKEN, protocol_fee_recipient=None,
              idx=0, cashback=False):
    pfr = protocol_fee_recipient or PAMM_PROTOCOL_FEE[idx % 8]
    data = D_PAMM_SELL + struct.pack("<QQ", int(base_amount_in), int(min_quote_out))
    return Instruction(PAMM, data, _pamm_accounts(user, pool, base_mint, quote_mint, coin_creator, pfr,
                                                   base_prog, quote_prog, False, cashback))


def cashback_prep(user, quote_mint=WSOL, quote_prog=TOKEN):
    """the cashback ATA must exist before a cashback-coin swap - idempotent create."""
    ua = uva(user, PAMM)
    return ata_create_idempotent(user, ua, quote_mint, quote_prog)


# ------------------------------------------------------------- local pricing
def pump_tokens_for_sol(vsol, vtok, spend_lamports, fee_bps_total):
    """tokens out on the curve for a total spend (fees on top of the curve input)."""
    sol_in = int(spend_lamports) * 10_000 // (10_000 + int(fee_bps_total))
    return vtok - (vsol * vtok) // (vsol + sol_in)


def pump_sol_for_tokens(vsol, vtok, tokens, fee_bps_total):
    """SOL received for selling `tokens` on the curve, after fees."""
    sol_out = vsol - (vsol * vtok) // (vtok + int(tokens))
    return sol_out - (sol_out * int(fee_bps_total) + 9_999) // 10_000


def pamm_base_for_quote(pbr, pqr_eff, spend_quote, fee_bps_total):
    q_in = int(spend_quote) * 10_000 // (10_000 + int(fee_bps_total))
    return pbr - (pbr * pqr_eff) // (pqr_eff + q_in)


def pamm_quote_for_base(pbr, pqr_eff, base_in, fee_bps_total):
    q_out = pqr_eff - (pbr * pqr_eff) // (pbr + int(base_in))
    return q_out - (q_out * int(fee_bps_total) + 9_999) // 10_000


# ------------------------------------------------------------- event parsing
def _b58(b):
    return str(Pubkey(b))


def _str(b, off):
    n = struct.unpack_from("<I", b, off)[0]
    return b[off + 4:off + 4 + n].decode("utf-8", "replace"), off + 4 + n


def parse_trade_event(b):
    """pump.fun TradeEvent, full 2026 layout (variable tail parsed defensively)."""
    d = {"mint": _b58(b[8:40]), "user": _b58(b[57:89])}
    d["sol"], d["tok"] = struct.unpack_from("<QQ", b, 40)
    d["is_buy"] = bool(b[56])
    d["ts"], d["vsol"], d["vtok"], d["rsol"], d["rtok"] = struct.unpack_from("<qQQQQ", b, 89)
    d["fee_recipient"] = _b58(b[129:161])
    d["fee_bps"], d["fee"] = struct.unpack_from("<QQ", b, 161)
    d["creator"] = _b58(b[177:209])
    d["creator_bps"], d["creator_fee"] = struct.unpack_from("<QQ", b, 209)
    d["track_volume"] = bool(b[225])
    d["mayhem"] = False
    d["cashback_bps"] = d["buyback_bps"] = 0
    try:
        ix, off = _str(b, 258)
        d["ix_name"] = ix
        d["mayhem"] = bool(b[off]); off += 1
        d["cashback_bps"], d["cashback"], d["buyback_bps"], d["buyback_fee"] = struct.unpack_from("<QQQQ", b, off); off += 32
        n = struct.unpack_from("<I", b, off)[0]; off += 4 + n * 34
        d["quote_mint"] = _b58(b[off:off + 32]); off += 32
        d["quote_amount"], d["vquote"], d["rquote"] = struct.unpack_from("<QQQ", b, off)
    except Exception:
        pass
    d["fee_total_bps"] = d["fee_bps"] + d["creator_bps"]
    return d


def parse_pamm_event(b):
    """PumpSwap BuyEvent / SellEvent incl. virtual_quote_reserves (effective
    quote reserve = vault + virtual). Returns None for other events."""
    from tape import D_BUY, D_SELL
    if b[:8] == D_BUY:
        side = "buy"
    elif b[:8] == D_SELL:
        side = "sell"
    else:
        return None
    f = struct.unpack_from("<q" + "Q" * 13, b, 8)
    d = {"side": side, "ts": f[0], "base": f[1], "pbr": f[5], "pqr": f[6], "quote": f[7],
         "lp_bps": f[8], "protocol_bps": f[10]}
    d["pool"] = _b58(b[120:152]); d["user"] = _b58(b[152:184])
    d["user_base_ta"] = _b58(b[184:216])
    d["protocol_fee_recipient"] = _b58(b[248:280])
    d["coin_creator"] = _b58(b[312:344])
    d["creator_bps"] = struct.unpack_from("<Q", b, 344)[0]
    d["vquote"] = 0
    try:
        if side == "buy":
            _, off = _str(b, 401)
            d["cashback_bps"], d["cashback"], d["buyback_bps"], d["buyback_fee"] = struct.unpack_from("<QQQQ", b, off); off += 32
            d["vquote"] = int.from_bytes(b[off:off + 16], "little", signed=True)
        else:
            d["cashback_bps"], d["cashback"], d["buyback_bps"], d["buyback_fee"] = struct.unpack_from("<QQQQ", b, 360)
            d["vquote"] = int.from_bytes(b[392:408], "little", signed=True)
    except Exception:
        pass
    d["fee_total_bps"] = d["lp_bps"] + d["protocol_bps"] + d["creator_bps"]
    return d


def token_program_from_logs(logs):
    """which token program the whale's tx touched for the base mint - free."""
    for ln in logs:
        if ln.startswith("Program TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb invoke"):
            return TOKEN22
    return TOKEN


# ------------------------------------------------------------- tx + simulate
def build_tx(payer, ixs, blockhash=None, cu_limit=250_000, cu_price=0):
    front = [compute_budget.set_compute_unit_limit(cu_limit)]
    if cu_price:
        front.append(compute_budget.set_compute_unit_price(cu_price))
    bh = Hash.from_string(blockhash) if isinstance(blockhash, str) else (blockhash or Hash.default())
    msg = MessageV0.try_compile(payer, front + list(ixs), [], bh)
    return VersionedTransaction.populate(msg, [Signature.default()] * msg.header.num_required_signatures)


def simulate(tx, client=None, rpc="https://api.mainnet-beta.solana.com"):
    """simulateTransaction with sigVerify off: any funded payer works."""
    body = {"jsonrpc": "2.0", "id": 1, "method": "simulateTransaction",
            "params": [base64.b64encode(bytes(tx)).decode(),
                       {"sigVerify": False, "replaceRecentBlockhash": True, "encoding": "base64",
                        "commitment": "processed"}]}
    c = client or httpx.Client(headers={"User-Agent": "Mozilla/5.0 wallet-lab/1.0"}, timeout=10)
    t = time.perf_counter()
    r = c.post(rpc, json=body)
    ms = (time.perf_counter() - t) * 1000
    j = r.json()
    v = (j.get("result") or {}).get("value") or {}
    return {"err": v.get("err") if v else j.get("error"), "logs": v.get("logs") or [],
            "units": v.get("unitsConsumed"), "ms": round(ms)}


def event_from_logs(logs):
    """the program's own emitted trade event inside simulation logs = ground truth."""
    from tape import D_TRADE
    out = []
    for ln in logs:
        if ln.startswith("Program data: "):
            try:
                b = base64.b64decode(ln[14:])
            except Exception:
                continue
            if b[:8] == D_TRADE:
                out.append(("pump", parse_trade_event(b)))
            else:
                e = parse_pamm_event(b)
                if e:
                    out.append(("pswap", e))
    return out


if __name__ == "__main__":
    # self-test: build one of each and time it (no network)
    from solders.keypair import Keypair
    u = Keypair().pubkey()
    m = Keypair().pubkey()
    t = time.perf_counter()
    for _ in range(100):
        tx = build_tx(u, [ata_create_idempotent(u, u, m), pump_buy_exact_sol(u, m, u, 50_000_000, 1)])
    print(f"pump buy tx build: {(time.perf_counter() - t) * 10:.3f} ms each, {len(bytes(tx))} bytes")
    t = time.perf_counter()
    for _ in range(100):
        tx = build_tx(u, wrap_sol(u, 50_000_000) + [ata_create_idempotent(u, u, m),
                                                    pamm_buy_exact_quote_in(u, m, m, u, 50_000_000, 1)])
    print(f"pswap buy tx build: {(time.perf_counter() - t) * 10:.3f} ms each, {len(bytes(tx))} bytes")
