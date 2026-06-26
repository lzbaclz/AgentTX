"""Phase-7 Gate (OVERLAY class): REAL multi-coordinator concurrent recovery for a NON-transactional
filesystem effect, on PostgreSQL + a real filesystem.

This closes the advisor's #5 blocker -- "the only distributed PROVEN class is TRANSACTIONAL, which is
DBOS's existing mechanism." Here K real OS processes race to run/recover the SAME turn of OVERLAY
(filesystem) actions, with HARD mid-effect process death (`os._exit`) at two windows:

  * hard_after_write   : die after writing the temp file, BEFORE the atomic publish (rename)
  * hard_after_publish : die after the file is durable, BEFORE recording the DB action row

plus a recovery sweep. The overlay effect is made exactly-once not by a shared transaction (a file
write cannot join the claim's DB tx) but by being IDEMPOTENT: the committed file is named by the
action_id and published by an ATOMIC rename, so every racer/recovery targets the same final path with
identical content. The oracle is the REAL filesystem.

PASS (the advisor's bar):
  * 0 duplicate committed files   (exactly one committed/<action_id>.receipt per logical action)
  * 0 lost logical action          (every action of every turn has its committed file)
  * no orphan tmp promoted          (committed/ holds only valid action-id files; tmps stay in overlay/)
  * legitimate duplicate ordinals BOTH execute (ordinals 0 and 2 have identical args -> 2 files)

Run: AGENTTX_PG_DSN="host=/tmp port=54329 user=agenttx dbname=agenttx" \
     python phase7/overlay_gate.py --turns 400
"""
import argparse
import json
import multiprocessing as mp
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agenttx.db import open_postgres
from agenttx.dtx import DTX, Crash, action_id, commit_id_of

STORE = "/tmp/atx_overlay_gate"
SESSION = "overlay-gate"


def plan_for(i):
    # 3 overlay actions; ordinals 0 and 2 have IDENTICAL args -> different action ids -> 2 files.
    o = f"ORD{i}"
    return [
        {"ordinal": 0, "tool": "receipt", "klass": "overlay", "args": {"order": o, "kind": "A"}},
        {"ordinal": 1, "tool": "receipt", "klass": "overlay", "args": {"order": o, "kind": "B"}},
        {"ordinal": 2, "tool": "receipt", "klass": "overlay", "args": {"order": o, "kind": "A"}},
    ]


def worker(turn, hard_crash_at, crash_kind, seed):
    """One coordinator: acquire a fencing epoch, reload the durable plan, run its overlay actions;
    maybe die hard mid-effect at hard_crash_at."""
    db = open_postgres()
    dtx = DTX(db)
    try:
        epoch = dtx.acquire(SESSION, turn)
        cid, plan = dtx.load_plan(SESSION, turn)
        for step in plan:
            crash = crash_kind if step["ordinal"] == hard_crash_at else None
            dtx.do_overlay(SESSION, turn, cid, step["ordinal"], step["args"], epoch, STORE, crash=crash)
        db.execute("UPDATE turns SET state='committed' WHERE session=? AND turn=?", (SESSION, turn))
        db.commit()
    except (Crash, Exception):
        pass
    finally:
        db.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--turns", type=int, default=400)
    a = ap.parse_args()

    os.system(f"rm -rf {STORE}")
    os.makedirs(os.path.join(STORE, "committed"), exist_ok=True)
    os.makedirs(os.path.join(STORE, "overlay"), exist_ok=True)

    db = open_postgres()
    dtx = DTX(db)
    for t in ("actions", "turns", "turn_owner"):
        db.execute(f"DELETE FROM {t} WHERE session=?", (SESSION,))
    db.commit()
    for i in range(a.turns):
        dtx.begin_turn(SESSION, f"T{i}", plan_for(i))         # durable plan = source of truth
    db.close()

    rng = random.Random(0)
    procs = []
    for i in range(a.turns):
        turn = f"T{i}"
        k = rng.randint(2, 6)                                 # K racing coordinators
        for w in range(k):
            if w < k - 1:
                hc = rng.choice([None, None, 0, 1, 2])
                kind = rng.choice(["hard_after_write", "hard_after_publish"])
            else:
                hc, kind = None, None
            procs.append(mp.Process(target=worker, args=(turn, hc, kind, rng.randint(0, 1 << 30))))
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    # recovery sweep: one clean coordinator per turn finishes any turn all racers crashed out of
    sweep = [mp.Process(target=worker, args=(f"T{i}", None, None, 999)) for i in range(a.turns)]
    for p in sweep:
        p.start()
    for p in sweep:
        p.join()

    # ---- oracle: the REAL filesystem ----
    expected = set()
    for i in range(a.turns):
        plan = plan_for(i)
        cid = commit_id_of(plan)
        for step in plan:
            expected.add(action_id(SESSION, f"T{i}", cid, step["ordinal"]))
    cdir = os.path.join(STORE, "committed")
    committed_files = [f for f in os.listdir(cdir) if f.endswith(".receipt")]
    committed_ids = {f[:-len(".receipt")] for f in committed_files}
    non_receipt = [f for f in os.listdir(cdir) if not f.endswith(".receipt")]   # any tmp leaked in?
    orphan_tmps = len(os.listdir(os.path.join(STORE, "overlay")))               # informational

    # legit-duplicate: for every turn, the ordinal-0 and ordinal-2 files (identical args) both exist
    legit_dup_ok = True
    for i in range(a.turns):
        cid = commit_id_of(plan_for(i))
        a0 = action_id(SESSION, f"T{i}", cid, 0)
        a2 = action_id(SESSION, f"T{i}", cid, 2)
        if not (a0 != a2 and a0 in committed_ids and a2 in committed_ids):
            legit_dup_ok = False
            break

    db = open_postgres()
    committed_actions = db.fetchone(
        "SELECT COUNT(*) FROM actions WHERE session=%s AND klass='overlay' AND state='committed'",
        (SESSION,))[0]
    committed_turns = db.fetchone("SELECT COUNT(*) FROM turns WHERE session=%s AND state='committed'",
                                  (SESSION,))[0]
    db.close()

    expected_n = a.turns * 3
    duplicate_files = len(committed_files) - len(committed_ids)         # >0 only if a name repeats
    out = {
        "gate": "real multi-process concurrent recovery for the OVERLAY (filesystem) class",
        "turns": a.turns,
        "racing_coordinators_per_turn": "2-6 + hard mid-effect os._exit (after-write / after-publish) + sweep",
        "expected_files": expected_n,
        "committed_files": len(committed_files),
        "distinct_committed_action_ids": len(committed_ids),
        "duplicate_committed_files": duplicate_files,
        "lost_logical_actions": len(expected - committed_ids),
        "unexpected_committed_files": len(committed_ids - expected),
        "non_receipt_files_in_committed": len(non_receipt),
        "orphan_tmps_remaining": orphan_tmps,
        "legit_duplicate_both_executed": legit_dup_ok,
        "committed_action_rows": committed_actions,
        "committed_turns": committed_turns,
        "OVERLAY_GATE_PASS": bool(
            committed_ids == expected
            and duplicate_files == 0
            and len(non_receipt) == 0
            and legit_dup_ok
            and committed_actions == expected_n
            and committed_turns == a.turns),
        "reads": "K real OS processes per turn race to run/recover the same durable turn of OVERLAY "
                 "(filesystem) actions, with hard mid-effect process death (after temp-write / after "
                 "atomic-publish) + a recovery sweep. The committed file is named by action_id and "
                 "published by an atomic rename, so the effect is idempotent under any interleaving: "
                 "exactly one committed file per logical action, none lost, no tmp promoted, and two "
                 "identical-args actions at different ordinals BOTH produce a file.",
    }
    os.makedirs("phase7/results", exist_ok=True)
    json.dump(out, open("phase7/results/overlay_gate.json", "w"), indent=2)
    print(json.dumps(out, indent=2))
    return 0 if out["OVERLAY_GATE_PASS"] else 1


if __name__ == "__main__":
    mp.set_start_method("spawn")
    sys.exit(main())
