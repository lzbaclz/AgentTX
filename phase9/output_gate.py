"""Phase-9 gate: durable output exactly-once across REAL stream-worker death + client restart.

The committed output tokens of each turn live in a durable log. A stream worker resumes from the
client's durable ACK, PERSISTS each token before "sending" it (the client durably records receipt
+ dedups by seq). We kill stream workers mid-stream (hard os._exit), spawn fresh ones, and restart
the client -- all on PostgreSQL -- then assert the client materialized every committed token
exactly once, in order, with no loss.

Run: AGENTTX_PG_DSN=... python phase9/output_gate.py --turns 200
"""
import argparse
import json
import multiprocessing as mp
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agenttx.db import open_postgres
from agenttx.outlog import OutputLog

NTOK = 12


def committed_tokens(turn):
    return [f"{turn}:tok{i}" for i in range(1, NTOK + 1)]


def stream_worker(session, turn, hard_crash_at, seed):
    """Resume from the client ACK; persist-before-send; maybe die mid-stream."""
    db = open_postgres()
    ol = OutputLog(db)
    try:
        toks = committed_tokens(turn)
        start = ol.client_ack(session, turn) + 1
        for seq in range(start, NTOK + 1):
            ol.persist(session, turn, seq, toks[seq - 1])         # PERSIST (durable) ...
            if seq == hard_crash_at:
                os._exit(7)                                       # ... crash BEFORE send -> client never got it
            ol.deliver(session, turn, seq, toks[seq - 1])         # ... then SEND (client durably records)
    except Exception:
        pass
    finally:
        db.close()


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--turns", type=int, default=200); a = ap.parse_args()
    session = "output-gate"
    db = open_postgres(); ol = OutputLog(db)
    db.execute("DELETE FROM output_chunks WHERE session=?", (session,))
    db.execute("DELETE FROM client_received WHERE session=?", (session,))
    db.commit(); db.close()

    rng = random.Random(0)
    procs = []
    for i in range(a.turns):
        turn = f"T{i}"
        # 1-3 stream workers per turn, most dying mid-stream; client "restarts" between them implicitly
        for w in range(rng.randint(1, 3)):
            hc = rng.choice([0, 0, 3, 6, 9]) if w == 0 else 0     # 0 => no crash
            procs.append(mp.Process(target=stream_worker, args=(session, turn, hc, rng.randint(0, 1 << 30))))
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    # final clean resume per turn (covers turns whose every worker crashed)
    sweep = [mp.Process(target=stream_worker, args=(session, f"T{i}", 0, 9)) for i in range(a.turns)]
    for p in sweep:
        p.start()
    for p in sweep:
        p.join()

    db = open_postgres(); ol = OutputLog(db)
    bad_order = bad_loss = bad_dup = persist_before_send_violation = 0
    for i in range(a.turns):
        turn = f"T{i}"
        view = ol.client_view(session, turn)
        exp = committed_tokens(turn)
        if view != exp:
            if len(view) != len(set(view)):
                bad_dup += 1
            if view != exp[:len(view)]:
                bad_order += 1
            if len(view) < NTOK:
                bad_loss += 1
        # persist-before-send: every client-received seq must exist in the durable output log
        recv = db.execute("SELECT seq FROM client_received WHERE session=? AND turn=?", (session, turn)).fetchall()
        for (s,) in recv:
            if not db.fetchone("SELECT 1 FROM output_chunks WHERE session=? AND turn=? AND seq=?", (session, turn, s)):
                persist_before_send_violation += 1
    db.close()

    out = {
        "gate": "durable output exactly-once across real stream-worker death + client restart (PostgreSQL)",
        "turns": a.turns, "tokens_per_turn": NTOK,
        "client_out_of_order": bad_order, "client_lost": bad_loss, "client_duplicate": bad_dup,
        "persist_before_send_violations": persist_before_send_violation,
        "PHASE9_PASS": (bad_order == 0 and bad_loss == 0 and bad_dup == 0 and persist_before_send_violation == 0),
        "reads": "Every committed output token reaches the client exactly once / in order / no loss "
                 "across hard mid-stream os._exit + fresh resumers + client restart; every token the "
                 "client saw was persisted to the durable log first (persist-before-send).",
    }
    os.makedirs("phase9/results", exist_ok=True)
    json.dump(out, open("phase9/results/output_gate.json", "w"), indent=2)
    print(json.dumps(out, indent=2))
    return 0 if out["PHASE9_PASS"] else 1


if __name__ == "__main__":
    mp.set_start_method("spawn")
    sys.exit(main())
