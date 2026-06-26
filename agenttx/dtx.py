"""Distributed turn transactions (the hardened, multi-coordinator protocol).

Fixes the single-owner gaps the advisor flagged in agenttx/gateway.py:

1. ACTION IDENTITY is (session, turn, model_output_commit_id, action_ordinal) -- NOT the tool+args
   hash. Two legitimate identical calls in one turn have different ordinals -> different ids -> both
   execute. The args fingerprint is kept only as a CONTENT check (mismatch => corruption, not dedup).

2. EXACTLY-ONCE UNDER CONCURRENCY via an ATOMIC CLAIM: the action row (PRIMARY KEY action_id) is
   INSERTed `ON CONFLICT DO NOTHING RETURNING` in the SAME transaction as the effect. Only the
   coordinator that wins the claim runs the effect; everyone else dedups. A crash before COMMIT rolls
   back BOTH the claim and the effect -> clean re-claim. Two coordinators can never double-execute.

3. FENCING: each coordinator holds an owner_epoch (a monotonic lease token). Every action takes the
   turn_owner row FOR UPDATE and rejects any holder whose epoch != the current epoch -> a resurrected
   stale coordinator cannot commit. acquire() bumps the epoch atomically.

4. WAL IS THE SOURCE OF TRUTH: begin_turn() persists the full plan (ordinal, tool, class, canonical
   args) + the model_output_commit_id. recover() reloads the plan FROM THE DB -- no in-memory plan.
"""
from __future__ import annotations

import json
import os

# Single shared identity (full sha256, ordinal-based). Re-exported so phase7/phase8 keep importing
# `action_id`/`commit_id_of` from here while the SEMANTICS live in one place (agenttx/identity.py).
from agenttx.identity import (  # noqa: F401
    ContentMismatch,
    action_id,
    args_fingerprint,
    commit_id_of,
)


class StaleOwner(Exception):
    pass


class Crash(Exception):
    pass


def _canon(args):
    from agenttx.identity import canonical_args
    return canonical_args(args)


def _fsync_dir(path):
    """fsync a directory so a rename into it is durable across a crash."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class DTX:
    def __init__(self, db):
        self.db = db
        self.pg = db.backend == "postgres"
        db.execute("CREATE TABLE IF NOT EXISTS turns("
                   "session TEXT, turn TEXT, commit_id TEXT, plan TEXT, state TEXT, "
                   "PRIMARY KEY(session,turn))")
        db.execute("CREATE TABLE IF NOT EXISTS turn_owner("
                   "session TEXT, turn TEXT, owner_epoch BIGINT, PRIMARY KEY(session,turn))")
        db.execute("CREATE TABLE IF NOT EXISTS actions("
                   "action_id TEXT PRIMARY KEY, session TEXT, turn TEXT, ordinal INTEGER, "
                   "tool TEXT, args_fp TEXT, klass TEXT, state TEXT, result TEXT, owner_epoch BIGINT)")
        db.execute("CREATE TABLE IF NOT EXISTS charges("
                   + ("id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " if self.pg
                      else "id INTEGER PRIMARY KEY AUTOINCREMENT, ")
                   + "order_id TEXT, amount INTEGER, action_id TEXT)")
        db.commit()

    # ---- ownership / fencing ----
    def acquire(self, session, turn):
        """Atomically bump + return the owner_epoch (the caller's fencing token)."""
        cur = self.db.execute(
            "INSERT INTO turn_owner(session,turn,owner_epoch) VALUES(?,?,1) "
            "ON CONFLICT(session,turn) DO UPDATE SET owner_epoch = turn_owner.owner_epoch + 1 "
            "RETURNING owner_epoch", (session, turn))
        ep = cur.fetchone()[0]
        self.db.commit()
        return ep

    # ---- durable plan (source of truth) ----
    def begin_turn(self, session, turn, plan):
        """plan = [{ordinal,tool,klass,args}, ...]. Persist it durably so recovery is self-contained."""
        cid = commit_id_of(plan)
        self.db.execute("INSERT INTO turns(session,turn,commit_id,plan,state) VALUES(?,?,?,?,'open') "
                        "ON CONFLICT(session,turn) DO NOTHING", (session, turn, cid, json.dumps(plan)))
        self.db.commit()
        return cid

    def load_plan(self, session, turn):
        r = self.db.fetchone("SELECT commit_id, plan FROM turns WHERE session=? AND turn=?", (session, turn))
        return (r[0], json.loads(r[1])) if r else (None, None)

    # ---- the atomic claim + transactional effect ----
    def do_charge(self, session, turn, commit_id, ordinal, args, epoch, crash=None):
        aid = action_id(session, turn, commit_id, ordinal)
        afp = args_fingerprint(args)
        forupd = " FOR UPDATE" if self.pg else ""
        with self.db.transaction() as db:
            r = db.execute(f"SELECT owner_epoch FROM turn_owner WHERE session=? AND turn=?{forupd}",
                           (session, turn)).fetchone()
            if not r or r[0] != epoch:                       # FENCE: stale owner rejected
                raise StaleOwner((r[0] if r else None, epoch))
            row = db.execute(                                # ATOMIC CLAIM
                "INSERT INTO actions(action_id,session,turn,ordinal,tool,args_fp,klass,state,owner_epoch) "
                "VALUES(?,?,?,?,?,?,?, 'committed', ?) ON CONFLICT(action_id) DO NOTHING RETURNING action_id",
                (aid, session, turn, ordinal, "charge", afp, "transactional", epoch)).fetchone()
            if row is None:                                  # someone already did this action -> dedup
                ex = db.execute("SELECT args_fp,result FROM actions WHERE action_id=?", (aid,)).fetchone()
                if ex[0] != afp:
                    raise ContentMismatch(aid)
                return ("dedup", json.loads(ex[1]) if ex[1] else None)
            # we WON the claim -> run the effect in the SAME tx
            db.execute("INSERT INTO charges(order_id,amount,action_id) VALUES(?,?,?)",
                       (args["order"], args["amount"], aid))
            if crash == "mid":                               # in-process crash AFTER effect, BEFORE commit
                raise Crash()                                # -> tx rolls back claim+effect -> clean re-claim
            if crash == "hard_mid":                          # process DEATH mid-tx (most faithful)
                os._exit(7)                                  # -> connection drops -> Postgres rolls back
            res = {"order": args["order"], "amount": args["amount"]}
            db.execute("UPDATE actions SET result=? WHERE action_id=?", (json.dumps(res), aid))
        return ("committed", res)

    # ---- distributed OVERLAY (non-transactional FS effect) ----------------------------------
    # An overlay effect is NOT a DB row, so it cannot share the claim's transaction. Instead it is
    # made idempotent by construction: the committed file is named by the action_id and published
    # by an ATOMIC rename. Multiple racing coordinators (and post-crash recovery) all target the
    # SAME final path with IDENTICAL content -> at most one committed file per action, re-runs are
    # no-ops. Correctness therefore needs no fence (a stale coordinator re-doing it is harmless);
    # the DB action row is written AFTER the file is durable, so a committed row always has its file
    # (no ghost), and a crash before the row just means recovery re-publishes/records.
    def do_overlay(self, session, turn, commit_id, ordinal, args, epoch, store, crash=None):
        aid = action_id(session, turn, commit_id, ordinal)
        afp = args_fingerprint(args)
        committed_dir = os.path.join(store, "committed")
        overlay_dir = os.path.join(store, "overlay")
        final = os.path.join(committed_dir, f"{aid}.receipt")
        if os.path.exists(final):                            # idempotent dedup: already published
            self._record_overlay(session, turn, aid, ordinal, afp, epoch)
            return ("dedup", final)
        # write a coordinator-UNIQUE temp (so racers never clobber each other mid-write), fsync,
        # then atomically rename to the action-id'd final name.
        tmp = os.path.join(overlay_dir, f"{aid}.{os.getpid()}.{epoch}.tmp")
        with open(tmp, "w") as f:
            f.write(_canon(args)); f.flush(); os.fsync(f.fileno())
        if crash == "hard_after_write":                      # DEATH after temp, BEFORE publish ->
            os._exit(7)                                      #   orphan tmp, NO final -> recovery republishes
        os.replace(tmp, final)                               # ATOMIC publish (idempotent under races)
        _fsync_dir(committed_dir)
        if crash == "hard_after_publish":                    # DEATH after file, BEFORE DB row ->
            os._exit(7)                                      #   recovery sees the file -> dedups + records
        self._record_overlay(session, turn, aid, ordinal, afp, epoch)
        return ("committed", final)

    def _record_overlay(self, session, turn, aid, ordinal, afp, epoch):
        """Durable observation that this overlay action committed. Written only AFTER the file is
        durable; ON CONFLICT DO NOTHING so concurrent coordinators don't fight."""
        self.db.execute(
            "INSERT INTO actions(action_id,session,turn,ordinal,tool,args_fp,klass,state,owner_epoch) "
            "VALUES(?,?,?,?,?,?,'overlay','committed',?) ON CONFLICT(action_id) DO NOTHING",
            (aid, session, turn, ordinal, "receipt", afp, epoch))
        self.db.commit()

    # ---- self-contained recovery: reload plan from the DB, re-run (dedups) ----
    def run_turn(self, session, turn, plan=None, epoch=None, crash_at=None):
        if epoch is None:
            epoch = self.acquire(session, turn)
        if plan is not None:
            cid = self.begin_turn(session, turn, plan)
        else:
            cid, plan = self.load_plan(session, turn)        # recovery: plan comes from the WAL/DB
            if plan is None:
                raise ValueError("no durable plan for turn")
        results = []
        for step in plan:
            crash = "mid" if (crash_at is not None and step["ordinal"] == crash_at) else None
            results.append(self.do_charge(session, turn, cid, step["ordinal"], step["args"], epoch, crash=crash))
        self.db.execute("UPDATE turns SET state='committed' WHERE session=? AND turn=?", (session, turn))
        self.db.commit()
        return results
