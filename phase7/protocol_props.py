"""Phase-7 protocol properties: explicit, deterministic assertions for each mechanism the
advisor required (action identity, atomic claim/dedup, fencing/stale-owner rejection, crash
rollback, WAL-as-source-of-truth). Deterministic on SQLite.

Run: python phase7/protocol_props.py
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agenttx.db import open_sqlite
from agenttx.dtx import DTX, StaleOwner, Crash, action_id, commit_id_of


def fresh():
    path = tempfile.mktemp(suffix=".db", dir="/dev/shm" if os.path.isdir("/dev/shm") else None)
    return open_sqlite(path), path


def charges_for(db, order):
    return db.fetchone("SELECT COUNT(*) FROM charges WHERE order_id=?", (order,))[0]


def main():
    R = {}

    # P1 ACTION IDENTITY: two identical calls at different ordinals BOTH execute (the bug fix).
    db, _ = fresh(); dtx = DTX(db)
    plan = [{"ordinal": 0, "args": {"order": "X", "amount": 100}},
            {"ordinal": 1, "args": {"order": "X", "amount": 100}}]   # identical args
    dtx.run_turn("s", "t1", plan=plan)
    R["P1_identity_two_identical_calls_both_execute"] = (charges_for(db, "X") == 2)
    db.close()

    # P2 ATOMIC CLAIM / DEDUP: re-running the same turn re-claims nothing -> no extra effect.
    db, _ = fresh(); dtx = DTX(db)
    p = [{"ordinal": 0, "args": {"order": "Y", "amount": 10}}]
    ep = dtx.acquire("s", "t2"); cid = dtx.begin_turn("s", "t2", p)
    dtx.do_charge("s", "t2", cid, 0, p[0]["args"], ep)
    again = dtx.do_charge("s", "t2", cid, 0, p[0]["args"], ep)            # replay
    R["P2_claim_dedup_replay_no_double"] = (charges_for(db, "Y") == 1 and again[0] == "dedup")
    db.close()

    # P3 FENCING: a stale-epoch coordinator is rejected (no effect).
    db, _ = fresh(); dtx = DTX(db)
    p = [{"ordinal": 0, "args": {"order": "Z", "amount": 5}}]
    e1 = dtx.acquire("s", "t3"); cid = dtx.begin_turn("s", "t3", p)
    e2 = dtx.acquire("s", "t3")                                          # new owner -> e1 is stale
    stale_rejected = False
    try:
        dtx.do_charge("s", "t3", cid, 0, p[0]["args"], e1)              # stale epoch
    except StaleOwner:
        stale_rejected = True
    dtx.do_charge("s", "t3", cid, 0, p[0]["args"], e2)                  # current owner proceeds
    R["P3_fencing_stale_owner_rejected"] = (stale_rejected and charges_for(db, "Z") == 1)
    db.close()

    # P4 CRASH ROLLBACK: crash mid-tx (after effect, before commit) leaves NO effect; retry -> once.
    db, _ = fresh(); dtx = DTX(db)
    p = [{"ordinal": 0, "args": {"order": "W", "amount": 7}}]
    ep = dtx.acquire("s", "t4"); cid = dtx.begin_turn("s", "t4", p)
    crashed = False
    try:
        dtx.do_charge("s", "t4", cid, 0, p[0]["args"], ep, crash="mid")
    except Crash:
        crashed = True
    mid = charges_for(db, "W")                                          # must be 0 (rolled back)
    dtx.do_charge("s", "t4", cid, 0, p[0]["args"], ep)                  # recovery re-runs
    R["P4_crash_before_commit_rolls_back_then_exactly_once"] = (crashed and mid == 0 and charges_for(db, "W") == 1)
    db.close()

    # P5 WAL IS THE SOURCE OF TRUTH: recover with NO in-memory plan -> reload from DB and finish.
    db, path = fresh(); dtx = DTX(db)
    p = [{"ordinal": 0, "args": {"order": "V", "amount": 1}},
         {"ordinal": 1, "args": {"order": "V", "amount": 2}}]
    dtx.begin_turn("s", "t5", p)                                        # only the PLAN is persisted
    del p, dtx                                                          # drop all in-memory plan state
    dtx2 = DTX(db)                                                      # a fresh coordinator
    res = dtx2.run_turn("s", "t5", plan=None)                           # reload plan FROM THE DB
    R["P5_wal_self_contained_recovery_no_external_plan"] = (len(res) == 2 and charges_for(db, "V") == 2)
    db.close()

    R["PROTOCOL_PROPS_PASS"] = all(v for k, v in R.items())
    os.makedirs("phase7/results", exist_ok=True)
    json.dump(R, open("phase7/results/protocol_props.json", "w"), indent=2)
    print(json.dumps(R, indent=2))
    return 0 if R["PROTOCOL_PROPS_PASS"] else 1


if __name__ == "__main__":
    sys.exit(main())
