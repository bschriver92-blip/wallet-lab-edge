"""COPYBOT store - everything the bot learns, on disk, queryable later."""
import os, sqlite3, time

# ON THE SSD (C:). E: is a spinning disk: hunt's bulk inserts held this file
# for 30 s+ and every other lab job died `database is locked` (21:07-21:10
# 09-01). Env COPYBOT_DB overrides; the old E: copy is left in place as backup.
DB = os.environ.get("COPYBOT_DB", r"C:\Users\Brady\lab\copybot.db")
# BULK HISTORY ON THE HARD DRIVE (E:, 750 GB free) - Brady 09-02: "store less
# on C:". Dune pulls (millions of rows, written once, read by the scorer) go
# to `hist.trades`; copybot.db on the SSD keeps only the hot working set
# (watched wallets' trades, paper book, executor). Every connection attaches it.
HIST_DB = os.environ.get("COPYBOT_HIST", r"E:\MemeCoin\lab_history\history.db")
HIST_SCHEMA = """
CREATE TABLE IF NOT EXISTS hist.trades(
  sig TEXT PRIMARY KEY, wallet TEXT, mint TEXT, side TEXT,
  tokens REAL, sol REAL, ts INTEGER, slot INTEGER);
CREATE INDEX IF NOT EXISTS hist.trades_w ON trades(wallet, ts);
"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS wallets(
  address TEXT PRIMARY KEY, label TEXT, source TEXT,
  first_seen TEXT, last_pull TEXT,
  n_trades INTEGER DEFAULT 0, verdict TEXT, notes TEXT);
CREATE TABLE IF NOT EXISTS trades(
  sig TEXT PRIMARY KEY, wallet TEXT, mint TEXT, side TEXT,
  tokens REAL, sol REAL, ts INTEGER, slot INTEGER);
CREATE INDEX IF NOT EXISTS trades_w ON trades(wallet, ts);
CREATE INDEX IF NOT EXISTS trades_m ON trades(mint, ts);
CREATE TABLE IF NOT EXISTS vet(
  wallet TEXT, check_name TEXT, passed INTEGER, detail TEXT, ts TEXT);
CREATE TABLE IF NOT EXISTS runs(ts TEXT, kind TEXT, detail TEXT);
"""

def db():
    c = sqlite3.connect(DB, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    try:
        os.makedirs(os.path.dirname(HIST_DB), exist_ok=True)
        c.execute("ATTACH DATABASE ? AS hist", (HIST_DB,))
    except Exception:
        pass                                    # readers without E: still work on the hot set
    return c

def init():
    c = db(); c.executescript(SCHEMA)
    # touch the history db only when it has no trades table yet: the schema
    # script + journal_mode PRAGMA on the attached db wait for its write lock,
    # and during a bulk pull on the E: HDD that blocked every lab job for
    # minutes (lab_paper TimeoutError, 'database is locked', 09-02 morning)
    try:
        have = c.execute("SELECT name FROM hist.sqlite_master WHERE type='table' AND name='trades'").fetchone()
        if not have:
            c.executescript(HIST_SCHEMA)
            c.execute("PRAGMA hist.journal_mode=WAL")
    except Exception:
        pass
    c.commit(); c.close()

def now():
    return time.strftime("%Y-%m-%d %H:%M")

def add_wallet(addr, label="", source=""):
    c = db()
    c.execute("INSERT OR IGNORE INTO wallets(address,label,source,first_seen)"
              " VALUES(?,?,?,?)", (addr, label, source, now()))
    c.commit(); c.close()

def save_trades(wallet, rows):
    """Dune pulls land in the HISTORY db on E: (bulk, write-once); the hot
    copybot.db on C: never grows from a pull."""
    c = db()
    c.executemany("INSERT OR REPLACE INTO hist.trades VALUES(?,?,?,?,?,?,?,?)",
                  [(r["sig"], wallet, r["mint"], r["side"], r["tokens"],
                    r["sol"], r["ts"], r["slot"]) for r in rows])
    c.commit()          # release everything before touching the hot db: a 500k-row
                        # hist insert held copybot.db's write lock for 7 min (02:00 09-02)
    n = c.execute("SELECT COUNT(*) c FROM hist.trades WHERE wallet=?", (wallet,)).fetchone()["c"]
    c.execute("UPDATE wallets SET last_pull=?, n_trades=? WHERE address=?",
              (now(), n, wallet))
    c.commit(); c.close()
    return n

def log_run(kind, detail):
    c = db()
    c.execute("INSERT INTO runs VALUES(?,?,?)", (now(), kind, detail))
    c.commit(); c.close()
