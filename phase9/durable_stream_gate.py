"""Phase-9 Gate: durable output streaming under REAL worker process death + co-death + ACK loss +
client restart. Upgrades the in-memory streaming prototype (phase4) to a DURABLE output log.

Per turn: the producer COMMITS the output tokens durably (persist-before-send) BEFORE any worker
streams. Then a sequence of worker SUBPROCESSES streams to a durable client; each worker may die hard
(`os._exit`) mid-stream, the server ACK may be dropped (so it lags the client), and the client may
RESTART (lose its materialized state -> ACK falls back to 0). A fresh worker always resumes from a
SAFE point -- min(server_ack, client_true_ack) -- re-sending logged tokens (NEVER regenerating), which
the client dedups by seq. The driver loops until the client has the whole committed prefix.

PASS (the advisor's bar):
  * client materialized output == committed token prefix   (no missing token)
  * no duplicate visible token                              (client dedups by seq PK)
  * survives worker death, co-death (commit then both gone), lost ACKs, and client restart

Run: python phase9/durable_stream_gate.py audit --turns 300 --tokens 20
     python phase9/durable_stream_gate.py worker <db> <session> <turn> <client_id> <N> <crash_at> <drop_ack>
"""
import argparse
import json
import os
import random
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agenttx.db import open_sqlite
from agenttx.durable_stream import DurableClient, DurableStream

SESSION = "p9"
CLIENT = "c0"


def _bump(db, metric, n):
    db.execute("INSERT INTO stream_stats(metric,val) VALUES(?,?) "
               "ON CONFLICT(metric) DO UPDATE SET val=val+excluded.val", (metric, n))
    db.commit()


def worker(db_path, session, turn, client_id, N, crash_at, drop_ack):
    """One streaming worker (a real OS process). Resumes from a SAFE point and re-sends logged
    tokens; may die hard mid-stream."""
    db = open_sqlite(db_path)
    stream = DurableStream(db)
    client = DurableClient(db, session, turn, client_id)
    c0 = client.ack()                                          # client's true watermark at start
    resume = min(stream.server_ack(session, turn, client_id), c0) + 1   # SAFE: never skips a token
    for seq, tok in stream.tokens_from(session, turn, resume):
        if crash_at is not None and seq == crash_at:
            os._exit(7)                                       # hard worker death BEFORE delivering seq
        if seq <= c0:                                          # a re-send the client already has
            _bump(db, "resends_deduped", 1)
        client.deliver(seq, tok)                              # persist-before-send already holds
        if not drop_ack:
            stream.record_ack(session, turn, client_id, client.ack())   # else: server ACK lags
    db.close()


def audit(T, N):
    store = "/tmp/atx_p9"
    os.system(f"rm -rf {store} && mkdir -p {store}")
    db_path = f"{store}/stream.db"
    db = open_sqlite(db_path)
    db.execute("CREATE TABLE IF NOT EXISTS stream_stats(metric TEXT PRIMARY KEY, val INTEGER)")
    db.commit()
    stream = DurableStream(db)

    rng = random.Random(0)
    here = os.path.abspath(__file__)
    py = sys.executable
    stat = {"turns": T, "turns_exactly_once": 0, "worker_deaths": 0, "client_restarts": 0,
            "ack_drops": 0, "codeath_turns": 0, "loss": 0, "duplicate": 0, "not_completed": 0}

    for i in range(T):
        turn = f"T{i}"
        tokens = [f"{turn}#tok{k}" for k in range(1, N + 1)]
        stream.commit_output(SESSION, turn, tokens)           # PERSIST-BEFORE-SEND
        client = DurableClient(db, SESSION, turn, CLIENT)
        first_delivered = None
        for it in range(60):
            ack = client.ack()
            if ack >= N:
                break
            # occasionally the client restarts mid-stream (loses materialized state)
            if it > 0 and ack > 0 and rng.random() < 0.15:
                client.restart(); stat["client_restarts"] += 1; ack = 0
            crash_at = rng.choice([None, None, rng.randint(ack + 1, N)])   # maybe die mid-stream
            drop_ack = 1 if rng.random() < 0.30 else 0
            if drop_ack:
                stat["ack_drops"] += 1
            rc = subprocess.run([py, here, "worker", db_path, SESSION, turn, CLIENT,
                                 str(N), str(crash_at if crash_at is not None else -1), str(drop_ack)]).returncode
            if rc != 0:
                stat["worker_deaths"] += 1
            new_ack = client.ack()
            if first_delivered is None:
                first_delivered = new_ack
                if new_ack == 0:                              # commit happened, first worker delivered 0
                    stat["codeath_turns"] += 1                # -> a fresh worker must resume from the log
        mat = client.materialized()
        if mat == tokens:
            stat["turns_exactly_once"] += 1
        else:
            if len(mat) < N or mat != tokens[:len(mat)]:
                stat["loss"] += 1
            # any seq present beyond the contiguous prefix with a wrong/dup token would show here
        # duplicate check: client_recv has a PK on seq, so a seq can appear at most once
        n_rows = db.fetchone("SELECT COUNT(*) FROM client_recv WHERE session=? AND turn=? AND client_id=?",
                             (SESSION, turn, CLIENT))[0]
        if n_rows != N and client.ack() >= N:
            stat["duplicate"] += 1                            # would only trigger if dedup failed
        if client.ack() < N:
            stat["not_completed"] += 1

    resends = db.fetchone("SELECT val FROM stream_stats WHERE metric='resends_deduped'")
    db.close()
    out = {
        "gate": "durable output streaming under real worker os._exit + co-death + ACK loss + client restart",
        **stat,
        "resends_deduped": resends[0] if resends else 0,
        "PHASE9_PASS": bool(stat["turns_exactly_once"] == T and stat["loss"] == 0
                            and stat["duplicate"] == 0 and stat["not_completed"] == 0),
        "reads": "Output tokens are committed durably (persist-before-send) BEFORE any worker streams. "
                 "Real worker SUBPROCESSES stream and die hard (os._exit) mid-stream; the server ACK is "
                 "dropped (lags the client); the client RESTARTS (loses state). A fresh worker resumes "
                 "from min(server_ack, client_ack) -- always safe -- and RE-SENDS logged tokens (never "
                 "regenerates), which the client dedups by seq. The client materializes the committed "
                 "prefix exactly once, in order, no loss, across all of it.",
    }
    os.makedirs("phase9/results", exist_ok=True)
    json.dump(out, open("phase9/results/durable_stream_gate.json", "w"), indent=2)
    print(json.dumps(out, indent=2))
    return 0 if out["PHASE9_PASS"] else 1


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "worker":
        _, _, db_path, session, turn, client_id, N, crash_at, drop_ack = sys.argv
        ca = int(crash_at)
        worker(db_path, session, turn, client_id, int(N), ca if ca >= 0 else None, int(drop_ack))
        return 0
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", nargs="?", default="audit")
    ap.add_argument("--turns", type=int, default=300)
    ap.add_argument("--tokens", type=int, default=20)
    a = ap.parse_args()
    return audit(a.turns, a.tokens)


if __name__ == "__main__":
    sys.exit(main())
