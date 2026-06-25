"""AgentTx minimal coordinator core: Turn WAL + Tool Gateway + Coordinator + crash clock.

This is the real end-to-end orchestration whose crash-safety Gate-2 audits. The two
supported tool classes:
  * SQL (transactional)  : effect + action-key record committed in ONE sqlite transaction
  * FS  (overlay)        : content-addressed by the action key (write temp -> atomic rename
                           to committed/<key>); re-execution is a no-op (idempotent)
Both are exactly-once across a crash at ANY point. The coordinator writes a Turn WAL and
recovers by re-running the turn -- correctness comes from the gateway's action-key dedup,
not from the WAL replay (the WAL gives prefix-consistent recovery + provenance).
"""
from __future__ import annotations

import hashlib
import os
import sqlite3


class Clock:
    """Crash injection: tick() at every durable boundary. hard=True => os._exit (faithful
    subprocess crash); hard=False => raise Crash (fast in-process model -- disk state
    survives via SQLite-WAL+fsync and atomic rename; in-memory state discarded)."""
    def __init__(self, kill_at: int = 0, hard: bool = False):
        self.n = 0
        self.kill_at = kill_at
        self.hard = hard

    def tick(self):
        self.n += 1
        if self.kill_at and self.n == self.kill_at:
            if self.hard:
                os._exit(137)
            raise Crash()


def _conn(path):
    c = sqlite3.connect(path, isolation_level=None, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=FULL")     # durability (fsync on commit)
    return c


def action_key(turn, tool, args):
    return hashlib.sha1(f"{turn}|{tool}|{args}".encode()).hexdigest()[:16]


class TurnWAL:
    def __init__(self, store):
        self.db = _conn(os.path.join(store, "wal.db"))
        self.db.execute("CREATE TABLE IF NOT EXISTS wal("
                        "lsn INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, turn TEXT, key TEXT)")

    def append(self, type, turn, key=""):
        self.db.execute("INSERT INTO wal(type,turn,key) VALUES(?,?,?)", (type, turn, key))

    def has(self, type, turn, key=None):
        if key is None:
            return self.db.execute("SELECT 1 FROM wal WHERE type=? AND turn=? LIMIT 1",
                                   (type, turn)).fetchone() is not None
        return self.db.execute("SELECT 1 FROM wal WHERE type=? AND turn=? AND key=? LIMIT 1",
                               (type, turn, key)).fetchone() is not None


class ToolGateway:
    def __init__(self, store):
        self.store = store
        self.db = _conn(os.path.join(store, "app.db"))
        self.db.execute("CREATE TABLE IF NOT EXISTS charges(order_id TEXT, amount INT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS gw_keys(key TEXT PRIMARY KEY, result TEXT)")
        os.makedirs(os.path.join(store, "committed"), exist_ok=True)
        os.makedirs(os.path.join(store, "overlay"), exist_ok=True)

    def _seen(self, key):
        r = self.db.execute("SELECT result FROM gw_keys WHERE key=?", (key,)).fetchone()
        return r[0] if r else None

    # SQL transactional tool: effect + key-record committed atomically
    def sql_charge(self, key, order_id, amount, clock: Clock):
        seen = self._seen(key)
        if seen is not None:
            return seen                                  # dedup: already committed
        clock.tick()                                     # crash BEFORE effect -> nothing happened
        self.db.execute("BEGIN")
        self.db.execute("INSERT INTO charges(order_id,amount) VALUES(?,?)", (order_id, amount))
        self.db.execute("INSERT INTO gw_keys(key,result) VALUES(?,?)", (key, f"charged:{order_id}"))
        self.db.execute("COMMIT")                         # atomic: effect AND key visible together
        clock.tick()                                     # crash AFTER commit -> dedup will skip
        return f"charged:{order_id}"

    # FS overlay tool: content-addressed by action key (idempotent atomic rename)
    def fs_receipt(self, key, order_id, clock: Clock):
        final = os.path.join(self.store, "committed", f"{key}.receipt")
        if os.path.exists(final):
            return final                                 # idempotent: already produced
        tmp = os.path.join(self.store, "overlay", f"{key}.tmp")
        with open(tmp, "w") as f:
            f.write(f"{order_id}\n"); f.flush(); os.fsync(f.fileno())
        clock.tick()                                     # crash AFTER write, BEFORE rename -> orphan tmp
        os.replace(tmp, final)                            # atomic; same name on re-run -> 1 file
        clock.tick()
        return final


class Coordinator:
    def __init__(self, store):
        self.store = store
        self.wal = TurnWAL(store)
        self.gw = ToolGateway(store)

    def run_turn(self, turn, plan, clock: Clock):
        """plan = [(tool, args), ...] (the committed tool calls the LLM decided this turn)."""
        if not self.wal.has("BEGIN_TURN", turn):
            self.wal.append("BEGIN_TURN", turn)
        clock.tick()
        for tool, args in plan:
            key = action_key(turn, tool, args)
            self.wal.append("ACTION_PREPARED", turn, key); clock.tick()
            if tool == "sql":
                self.gw.sql_charge(key, args[0], args[1], clock)
            elif tool == "fs":
                self.gw.fs_receipt(key, args[0], clock)
            self.wal.append("OBSERVATION_COMMITTED", turn, key); clock.tick()
        if not self.wal.has("TURN_COMMITTED", turn):
            self.wal.append("TURN_COMMITTED", turn)
        clock.tick()

    # recovery == re-run the turn; gateway action-key dedup makes every effect exactly-once
    def recover(self, turn, plan, clock: Clock):
        self.run_turn(turn, plan, clock)


def oracle(store, order_id):
    """Count the REAL external effects: charges rows + receipt files for the order."""
    db = _conn(os.path.join(store, "app.db"))
    n_charge = db.execute("SELECT COUNT(*) FROM charges WHERE order_id=?", (order_id,)).fetchone()[0]
    import glob
    n_receipt = 0
    for p in glob.glob(os.path.join(store, "committed", "*.receipt")):
        if open(p).read().strip() == order_id:
            n_receipt += 1
    return n_charge, n_receipt


class Crash(Exception):
    """In-process crash signal (faithful: disk state survives, in-memory discarded)."""


def close_coord(coord):
    for c in (coord.wal.db, coord.gw.db):
        try:
            c.rollback(); c.close()      # uncommitted tx -> rolled back (== crash-before-commit)
        except Exception:                # noqa: BLE001
            pass
