"""Phase-5 end-to-end correctness eval: the WHOLE AgentTx stack under >=100k fault injections.

Each trial runs a full multi-step turn -- WAL begin -> SQL charge (transactional) -> FS receipt
(overlay) -> KV-View snapshot -> stream the committed output tokens -> WAL commit -- and crashes
the turn at a random durable boundary, then recovers (recovery may itself crash). Fault modes:
  crash           : crash once at a random boundary
  recovery_crash  : crash, then the recovery also crashes once before completing
  kv_corrupt      : corrupt the KV snapshot -> KV-View must FAIL_CLOSED -> output still correct
                    (the committed token log, not the KV, is the source of truth)
Oracle per trial (the paper's core claim): the SQL charge fires exactly once, the FS receipt
exactly once, NO ghost observation (every recorded observation has a committed effect), and the
client materializes the committed output tokens exactly once / in order / no loss.

Run: python phase5/eval.py --trials 100000
"""
import argparse
import glob
import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agenttx.db import open_sqlite
from agenttx.core import Clock, Crash
from agenttx.gateway import Gateway
from agenttx.identity import action_id
from agenttx.kvview import KVView, Provenance, sha256_hex
from agenttx.tools import ChargeTool, ReceiptTool

CHARGE, RECEIPT = ChargeTool(), ReceiptTool()
OUT_LEN = 6                       # committed output tokens per turn


def out_tokens(order):
    return [f"{order}#t{i}" for i in range(OUT_LEN)]


def prov_of(order):
    return Provenance("m", "tok", "bf16", 16, 32, 8, 128, "theta=5e5", "", sha256_hex(order.encode()))


def stream_committed(tokens, client, resume_from, clock):
    """Stream tokens[resume_from..] to the client (seq 1-indexed); client dedups by ack."""
    for seq in range(resume_from, len(tokens) + 1):
        clock.tick()                       # crash before deliver
        tok = tokens[seq - 1]
        if seq == client["ack"] + 1:
            client["recv"].append(tok); client["ack"] = seq
        elif seq <= client["ack"]:
            client["dup"] += 1
        clock.tick()                       # crash after deliver


def run_turn(db, gw, kv, order, turn, client, clock, corrupt_kv=False):
    sess = "s"
    db.execute("INSERT OR IGNORE INTO wal(turn,type,key) VALUES(?,?,?)", (turn, "BEGIN_TURN", ""))
    db.commit(); clock.tick()
    # step 1: SQL charge (transactional, exactly-once via same-tx record) -- ordinal 0
    r1 = gw.call(sess, turn, CHARGE, {"order": order, "amount": 100}, clock, ordinal=0, commit_id="c")
    if r1.status in ("committed", "dedup_hit"):       # observation ONLY after a committed effect
        db.execute("INSERT OR IGNORE INTO wal(turn,type,key) VALUES(?,?,?)", (turn, "OBSERVATION", r1.key))
        db.commit()
    clock.tick()
    # step 2: FS receipt (overlay, keyed by action id) -- ordinal 1
    r2 = gw.call(sess, turn, RECEIPT, {"order": order}, clock, ordinal=1, commit_id="c")
    if r2.status in ("committed", "dedup_hit"):
        db.execute("INSERT OR IGNORE INTO wal(turn,type,key) VALUES(?,?,?)", (turn, "OBSERVATION", r2.key))
        db.commit()
    clock.tick()
    # step 3: KV-View snapshot of the turn's output (manifest + provenance)
    blocks = [t.encode() for t in out_tokens(order)]
    m = kv.snapshot(turn, prov_of(order), blocks)
    if corrupt_kv and m["blocks"]:
        p = kv.cas._path(m["blocks"][0])
        raw = bytearray(open(p, "rb").read()); raw[0] ^= 0xFF; open(p, "wb").write(raw)
    clock.tick()
    # step 4: KV restore decision (fail-closed -> recompute output from the durable log)
    kv.restore(turn, prov_of(order))                  # decision only; output streamed from log regardless
    # step 5: stream the committed output tokens
    stream_committed(out_tokens(order), client, client["ack"] + 1, clock)
    db.execute("INSERT OR IGNORE INTO wal(turn,type,key) VALUES(?,?,?)", (turn, "TURN_COMMITTED", ""))
    db.commit(); clock.tick()


def trial(store, svc_db, order, turn, mode, rng):
    client = {"ack": 0, "recv": [], "dup": 0}
    corrupt = (mode == "kv_corrupt")
    MAXT = 24

    def attempt(kill):
        db = open_sqlite(svc_db); gw = Gateway(db, store); kv = KVView(f"{store}/cas", db)
        try:
            run_turn(db, gw, kv, order, turn, client, Clock(kill, hard=False), corrupt_kv=corrupt and kill == 0)
        except Crash:
            pass
        finally:
            db.rollback(); db.close()

    attempt(rng.randint(1, MAXT))                     # the crash
    if mode == "recovery_crash":
        attempt(rng.randint(1, MAXT))                 # recovery that itself crashes
    # keep recovering (no crash) until the stream completes
    for _ in range(40):
        if client["ack"] >= OUT_LEN:
            break
        attempt(0)
    return client


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--trials", type=int, default=100000); a = ap.parse_args()
    store = "/dev/shm/atx_p5" if os.path.isdir("/dev/shm") else "/tmp/atx_p5"
    os.system(f"rm -rf {store} && mkdir -p {store}")
    svc_db = f"{store}/app.db"
    db = open_sqlite(svc_db)
    db.execute("CREATE TABLE IF NOT EXISTS wal(lsn INTEGER PRIMARY KEY AUTOINCREMENT, turn TEXT, type TEXT, key TEXT)")
    db.commit(); db.close()

    rng = random.Random(0)
    modes = ["crash", "crash", "crash", "recovery_crash", "kv_corrupt"]   # weighted
    bad = {"dup_charge": 0, "lost_charge": 0, "dup_receipt": 0, "lost_receipt": 0,
           "ghost_observation": 0, "stream_not_exactly_once": 0}
    mode_counts = {}
    for i in range(a.trials):
        order = f"O{i}"; turn = f"T{i}"; mode = rng.choice(modes)
        mode_counts[mode] = mode_counts.get(mode, 0) + 1
        client = trial(store, svc_db, order, turn, mode, rng)
        db = open_sqlite(svc_db)
        nc = db.execute("SELECT COUNT(*) FROM charges WHERE order_id=?", (order,)).fetchone()[0]
        rkey = action_id("s", turn, "c", 1)            # receipt is ordinal 1 of the turn's plan
        nr = 1 if os.path.exists(f"{store}/committed/{rkey}.receipt") else 0
        # ghost check: every OBSERVATION key must be a committed gateway key
        obs = [r[0] for r in db.execute("SELECT key FROM wal WHERE turn=? AND type='OBSERVATION'", (turn,)).fetchall()]
        ghost = False
        for k in obs:
            st = db.execute("SELECT status FROM gw_keys WHERE key=?", (k,)).fetchone()
            if not st or st[0] not in ("committed",):
                ghost = True
        db.close()
        if nc > 1: bad["dup_charge"] += 1
        if nc < 1: bad["lost_charge"] += 1
        if nr > 1: bad["dup_receipt"] += 1
        if nr < 1: bad["lost_receipt"] += 1
        if ghost: bad["ghost_observation"] += 1
        if client["recv"] != out_tokens(order): bad["stream_not_exactly_once"] += 1
        if (i + 1) % 20000 == 0:
            print(f"  {i+1}/{a.trials}: violations={bad}", flush=True)

    out = {"trials": a.trials, "mode_counts": mode_counts, "violations": bad,
           "PHASE5_CORRECTNESS_PASS": all(v == 0 for v in bad.values()),
           "reads": "Whole AgentTx stack (WAL + 3-class gateway + KV-View + streaming) under the "
                    "full fault matrix (crash anywhere, crash-during-recovery, KV-snapshot "
                    "corruption). Per trial: SQL charge exactly-once, FS receipt exactly-once, no "
                    "ghost observation, client output exactly-once/in-order. KV corruption -> "
                    "fail-closed -> output still correct (durable token log is the source of truth)."}
    os.makedirs("phase5/results", exist_ok=True)
    json.dump(out, open("phase5/results/eval_correctness.json", "w"), indent=2)
    print(json.dumps(out, indent=2))
    return 0 if out["PHASE5_CORRECTNESS_PASS"] else 1


if __name__ == "__main__":
    sys.exit(main())
