"""AgentTx Tool Gateway + tool-class taxonomy.

Every side-effecting tool call goes through the gateway, which computes a deterministic
action key = H(session, turn, tool, canonical_args) and enforces the per-class exactly-once
mechanism. The taxonomy is honest about what is achievable:

  ToolClass      guarantee                       mechanism
  -----------    -----------------------------   --------------------------------------------
  PURE           exactly-once (== cache)         read-only; cache result by key
  IDEMPOTENT     exactly-once                    pass action key as the external idempotency key
  TRANSACTIONAL  exactly-once                    effect + key-record committed in ONE db tx
  OVERLAY        exactly-once                    write temp -> atomic rename to committed/<key>
  COMPENSATABLE  committed-or-compensated        saga: execute + compensation log; undo on abort
  IRREVERSIBLE   fail-closed UNCERTAIN           durable 'prepared' before execute; on a crash in
                                                 the prepared->committed window, recovery returns
                                                 UNCERTAIN and does NOT auto-retry

(The IRREVERSIBLE row is exactly the case the exhaustive model checker (agenttx/protocol.py)
proves NO orchestrator can make exactly-once -> we fail closed instead of silently retrying.)
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from enum import Enum


class Guarantee(Enum):
    EXACTLY_ONCE = "exactly_once"
    COMMITTED_OR_COMPENSATED = "committed_or_compensated"
    FAIL_CLOSED_UNCERTAIN = "fail_closed_uncertain"


class ToolClass(Enum):
    PURE = "pure"
    IDEMPOTENT = "idempotent"
    TRANSACTIONAL = "transactional"
    OVERLAY = "overlay"
    COMPENSATABLE = "compensatable"
    IRREVERSIBLE = "irreversible"


CLASS_GUARANTEE = {
    ToolClass.PURE: Guarantee.EXACTLY_ONCE,
    ToolClass.IDEMPOTENT: Guarantee.EXACTLY_ONCE,
    ToolClass.TRANSACTIONAL: Guarantee.EXACTLY_ONCE,
    ToolClass.OVERLAY: Guarantee.EXACTLY_ONCE,
    ToolClass.COMPENSATABLE: Guarantee.COMMITTED_OR_COMPENSATED,
    ToolClass.IRREVERSIBLE: Guarantee.FAIL_CLOSED_UNCERTAIN,
}


def action_key(session, turn, tool, args):
    canon = json.dumps(args, sort_keys=True, default=str)
    return hashlib.sha1(f"{session}|{turn}|{tool}|{canon}".encode()).hexdigest()[:20]


@dataclass
class Result:
    status: str          # committed | dedup_hit | uncertain | compensated
    value: object
    key: str
    klass: str


class Tool:
    """A side-effecting tool. Subclasses implement effect(); the class decides the mechanism.
    db_effect(db, args) runs INSIDE a transaction (for TRANSACTIONAL); effect(args) runs
    outside (OVERLAY/IDEMPOTENT/IRREVERSIBLE/COMPENSATABLE)."""
    name = "tool"
    klass = ToolClass.IDEMPOTENT

    def db_effect(self, db, key, args):     # for TRANSACTIONAL
        raise NotImplementedError

    def effect(self, gw, key, args):        # for non-transactional classes
        raise NotImplementedError

    def compensate(self, gw, key, args):    # for COMPENSATABLE
        pass


class Gateway:
    def __init__(self, db, store):
        self.db = db
        self.store = store
        os.makedirs(os.path.join(store, "overlay"), exist_ok=True)
        os.makedirs(os.path.join(store, "committed"), exist_ok=True)
        db.execute("CREATE TABLE IF NOT EXISTS gw_keys("
                   "key TEXT PRIMARY KEY, status TEXT, klass TEXT, result TEXT)")
        db.commit()

    def _seen(self, key):
        r = self.db.fetchone("SELECT status, result FROM gw_keys WHERE key=?", (key,))
        return (r[0], r[1]) if r else None

    def _record(self, key, status, klass, result, in_tx=False):
        sql = ("INSERT INTO gw_keys(key,status,klass,result) VALUES(?,?,?,?) "
               "ON CONFLICT(key) DO UPDATE SET status=excluded.status, result=excluded.result")
        self.db.execute(sql, (key, status, klass, result))
        if not in_tx:
            self.db.commit()

    def call(self, session, turn, tool: Tool, args, clock):
        key = action_key(session, turn, tool.name, args)
        seen = self._seen(key)
        if seen:
            st, res = seen
            if st == "committed":
                return Result("dedup_hit", res, key, tool.klass.value)
            if st == "prepared" and tool.klass == ToolClass.IRREVERSIBLE:
                return Result("uncertain", None, key, tool.klass.value)   # FAIL CLOSED
            if st == "prepared" and tool.klass == ToolClass.COMPENSATABLE:
                tool.compensate(self, key, args)
                self._record(key, "compensated", tool.klass.value, None)
                return Result("compensated", None, key, tool.klass.value)

        k = tool.klass
        if k == ToolClass.TRANSACTIONAL:
            clock.tick()                                    # crash before tx -> nothing
            with self.db.transaction() as db:
                res = tool.db_effect(db, key, args)
                self._record(key, "committed", k.value, res, in_tx=True)  # atomic with effect
            clock.tick()
            return Result("committed", res, key, k.value)

        if k in (ToolClass.OVERLAY, ToolClass.IDEMPOTENT, ToolClass.PURE):
            res = tool.effect(self, key, args)              # idempotent by construction
            clock.tick()
            self._record(key, "committed", k.value, res)    # record-after is safe (idempotent)
            clock.tick()
            return Result("committed", res, key, k.value)

        if k == ToolClass.COMPENSATABLE:
            self._record(key, "prepared", k.value, None)
            clock.tick()
            res = tool.effect(self, key, args)
            self._record(key, "committed", k.value, res)
            clock.tick()
            return Result("committed", res, key, k.value)

        if k == ToolClass.IRREVERSIBLE:
            # durable intent BEFORE the irreversible act; if we crash before 'committed',
            # recovery sees 'prepared' and returns UNCERTAIN (no auto-retry) -- fail closed.
            self._record(key, "prepared", k.value, None)
            clock.tick()                                  # crash: prepared, no effect -> UNCERTAIN
            res = tool.effect(self, key, args)
            clock.tick()                                  # crash: effect fired, NOT committed ->
            #                                               recovery sees 'prepared' -> UNCERTAIN,
            #                                               and we NEVER re-send (no silent double)
            self._record(key, "committed", k.value, res)
            clock.tick()
            return Result("committed", res, key, k.value)

        raise ValueError(k)
