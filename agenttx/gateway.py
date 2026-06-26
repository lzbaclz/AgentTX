"""AgentTx Tool Gateway + tool-class taxonomy.

Every side-effecting tool call goes through the gateway. Its dedup identity is POSITIONAL --
``action_id = H(session, turn, model_output_commit_id, ordinal)`` from `agenttx/identity.py`, the
SAME scheme the distributed protocol (`agenttx/dtx.py`) uses -- NOT a tool+args hash. So two
legitimate identical calls in one turn (different ordinals) both execute, and the args fingerprint
is a content check only (a mismatch on the same action id => corruption, raises ContentMismatch).

"Effectively-once" below = at-most-once EXECUTION + deduplicated retry. Only TRANSACTIONAL is true
exactly-once (effect + record in one ACID commit); the others are effectively-once and, for
IDEMPOTENT, conditional on the external service honoring the key.

  ToolClass      guarantee                       mechanism
  -----------    -----------------------------   --------------------------------------------
  PURE           effectively-once (== cache)     read-only; cache result by action id
  IDEMPOTENT     effectively-once (conditional)  pass action id as the external idempotency key
  TRANSACTIONAL  exactly-once                    effect + id-record committed in ONE db tx
  OVERLAY        effectively-once                write temp -> atomic rename to committed/<id>
  COMPENSATABLE  committed-or-compensated        saga prepared -> effect_started -> committed;
                                                 crash@prepared re-runs cleanly, only effect_started
                                                 (ambiguous) compensates. PROTOTYPE: logic only, no
                                                 concrete tool / executed crash audit yet.
  IRREVERSIBLE   fail-closed UNCERTAIN           durable 'prepared' before execute; on a crash in
                                                 the prepared->committed window, recovery returns
                                                 UNCERTAIN and does NOT auto-retry

(The IRREVERSIBLE row is exactly the case the model checker (agenttx/protocol.py) proves NO
orchestrator can make exactly-once -> we fail closed instead of silently retrying.)
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from enum import Enum

from agenttx.identity import ContentMismatch, action_id, args_fingerprint  # noqa: F401


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
    """DEPRECATED content-based key (tool+args). Kept ONLY for the τ²-bench harnesses
    (phase6), where each tool call is distinct so content keying is safe. The gateway itself
    no longer dedups on this -- it uses the positional action_id (see agenttx/identity.py).
    Full sha256, no truncation."""
    canon = json.dumps(args, sort_keys=True, default=str)
    return hashlib.sha256(f"{session}|{turn}|{tool}|{canon}".encode()).hexdigest()


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
                   "key TEXT PRIMARY KEY, args_fp TEXT, status TEXT, klass TEXT, result TEXT)")
        db.commit()

    def _seen(self, key):
        r = self.db.fetchone("SELECT status, result, args_fp FROM gw_keys WHERE key=?", (key,))
        return (r[0], r[1], r[2]) if r else None

    def _record(self, key, args_fp, status, klass, result, in_tx=False):
        sql = ("INSERT INTO gw_keys(key,args_fp,status,klass,result) VALUES(?,?,?,?,?) "
               "ON CONFLICT(key) DO UPDATE SET status=excluded.status, result=excluded.result")
        self.db.execute(sql, (key, args_fp, status, klass, result))
        if not in_tx:
            self.db.commit()

    def call(self, session, turn, tool: Tool, args, clock, *, ordinal, commit_id):
        """ordinal + commit_id are REQUIRED (keyword-only): they are the action's positional
        identity. The dedup key is action_id(session,turn,commit_id,ordinal); the args fingerprint
        is a content check -- a mismatch on the same id is corruption, not a dedup."""
        key = action_id(session, turn, commit_id, ordinal)
        afp = args_fingerprint(args)
        seen = self._seen(key)
        if seen:
            st, res, stored_fp = seen
            if stored_fp is not None and stored_fp != afp:
                raise ContentMismatch(f"{key}: args fingerprint changed on replay")  # FAIL CLOSED
            if st == "committed":
                return Result("dedup_hit", res, key, tool.klass.value)
            if st == "compensated":
                return Result("compensated", None, key, tool.klass.value)
            if tool.klass == ToolClass.IRREVERSIBLE and st in ("prepared", "effect_started"):
                return Result("uncertain", None, key, tool.klass.value)   # FAIL CLOSED
            if tool.klass == ToolClass.COMPENSATABLE:
                # 'prepared' means the effect had NOT started yet (recorded before effect_started)
                # -> safe to re-run; do NOT compensate a non-existent effect (the advisor's bug).
                # 'effect_started' is the only ambiguous state -> idempotent compensation + receipt.
                if st == "effect_started":
                    tool.compensate(self, key, args)
                    self._record(key, afp, "compensated", tool.klass.value, None)
                    return Result("compensated", None, key, tool.klass.value)
                # st == "prepared": fall through and re-run from a clean state

        k = tool.klass
        if k == ToolClass.TRANSACTIONAL:
            clock.tick()                                    # crash before tx -> nothing
            with self.db.transaction() as db:
                res = tool.db_effect(db, key, args)
                self._record(key, afp, "committed", k.value, res, in_tx=True)  # atomic with effect
            clock.tick()
            return Result("committed", res, key, k.value)

        if k in (ToolClass.OVERLAY, ToolClass.IDEMPOTENT, ToolClass.PURE):
            res = tool.effect(self, key, args)              # idempotent by construction
            clock.tick()
            self._record(key, afp, "committed", k.value, res)    # record-after is safe (idempotent)
            clock.tick()
            return Result("committed", res, key, k.value)

        if k == ToolClass.COMPENSATABLE:
            self._record(key, afp, "prepared", k.value, None)        # intent only, effect not yet attempted
            clock.tick()                                        # crash@prepared -> recovery re-runs cleanly
            self._record(key, afp, "effect_started", k.value, None)  # about to run the (non-idempotent) effect
            clock.tick()                                        # crash@effect_started -> ambiguous ->
            res = tool.effect(self, key, args)                  #   recovery compensates idempotently
            self._record(key, afp, "committed", k.value, res)
            clock.tick()                                        # crash@committed -> dedup
            return Result("committed", res, key, k.value)

        if k == ToolClass.IRREVERSIBLE:
            # durable intent BEFORE the irreversible act; if we crash before 'committed',
            # recovery sees 'prepared' and returns UNCERTAIN (no auto-retry) -- fail closed.
            self._record(key, afp, "prepared", k.value, None)
            clock.tick()                                  # crash: prepared, no effect -> UNCERTAIN
            res = tool.effect(self, key, args)
            clock.tick()                                  # crash: effect fired, NOT committed ->
            #                                               recovery sees 'prepared' -> UNCERTAIN,
            #                                               and we NEVER re-send (no silent double)
            self._record(key, afp, "committed", k.value, res)
            clock.tick()
            return Result("committed", res, key, k.value)

        raise ValueError(k)
