"""Identity-guard regression test (advisor P0.1/P0.2): the Gateway dedups by POSITIONAL action
identity (session, turn, commit_id, ordinal), NOT by a tool+args hash, and verifies args content
on replay. This pins the two failure modes the old tool+args identity had:

  G1  two LEGITIMATELY identical calls in one turn (different ordinals) BOTH execute
      -- the old H(session,turn,tool,args) key silently collapsed them into one (a LOST effect).
  G2  replaying the SAME action id with the SAME args dedups (exactly-once across recovery).
  G3  replaying the SAME action id with DIFFERENT args FAILS CLOSED (ContentMismatch),
      never silently returns the cached result -- catches corruption / non-determinism / a
      hash collision instead of dropping or mis-attributing an effect.

Runs on SQLite (no Postgres needed). Run: python phase3/identity_guard.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agenttx.core import Clock
from agenttx.db import open_sqlite
from agenttx.gateway import Gateway
from agenttx.identity import ContentMismatch
from agenttx.tools import ChargeTool


def main():
    store = "/tmp/atx_identity_guard"
    os.system(f"rm -rf {store} && mkdir -p {store}")
    db = open_sqlite(f"{store}/app.db")
    gw = Gateway(db, store)
    charge = ChargeTool()
    NOCRASH = Clock(0)

    def n_charges(order):
        return db.execute("SELECT COUNT(*) FROM charges WHERE order_id=?", (order,)).fetchone()[0]

    # G1: identical args at ordinals 0 and 1 -> two distinct action ids -> BOTH execute.
    a = {"order": "O", "amount": 100}
    r0 = gw.call("s", "t1", charge, a, NOCRASH, ordinal=0, commit_id="c")
    r1 = gw.call("s", "t1", charge, a, NOCRASH, ordinal=1, commit_id="c")
    g1 = (n_charges("O") == 2 and r0.key != r1.key and r0.status == "committed" and r1.status == "committed")

    # G2: replay ordinal 0 with the SAME args -> dedup, no new effect.
    r0b = gw.call("s", "t1", charge, a, NOCRASH, ordinal=0, commit_id="c")
    g2 = (r0b.status == "dedup_hit" and n_charges("O") == 2)

    # G3: replay ordinal 0 with DIFFERENT args -> fail closed (ContentMismatch), no new effect.
    try:
        gw.call("s", "t1", charge, {"order": "O", "amount": 999}, NOCRASH, ordinal=0, commit_id="c")
        g3 = False                                          # should NOT silently dedup/return
    except ContentMismatch:
        g3 = (n_charges("O") == 2)

    # also: a different commit_id (a re-decided plan) is a different action id -> executes.
    r_new = gw.call("s", "t1", charge, a, NOCRASH, ordinal=0, commit_id="c2")
    g4 = (n_charges("O") == 3 and r_new.status == "committed")
    db.close()

    out = {
        "G1_identical_args_diff_ordinal_both_execute": g1,
        "G2_same_id_same_args_dedups": g2,
        "G3_same_id_diff_args_fail_closed_ContentMismatch": g3,
        "G4_diff_commit_id_is_a_distinct_action": g4,
        "IDENTITY_GUARD_PASS": bool(g1 and g2 and g3 and g4),
    }
    os.makedirs("phase3/results", exist_ok=True)
    json.dump(out, open("phase3/results/identity_guard.json", "w"), indent=2)
    print(json.dumps(out, indent=2))
    return 0 if out["IDENTITY_GUARD_PASS"] else 1


if __name__ == "__main__":
    sys.exit(main())
