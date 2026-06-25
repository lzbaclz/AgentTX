"""AgentTx Gate-1a: failure-window correctness audit (REAL process crashes).

A minimal agent turn produces TWO real side effects:
  * charge(order)        -> a row in SQLite `charges`        (TRANSACTIONAL effect)
  * write_receipt(order) -> a receipt file on disk           (NON-TRANSACTIONAL effect)

We crash the worker process (os._exit) at named points around each effect, then recover,
then an ORACLE inspects the real DB/FS state to count: duplicate effects, lost effects,
correct turns. Four orchestrators (the baselines to beat + the AgentTx-min target):

  none            : no persistence; recovery re-runs the turn from scratch
  checkpoint      : save a JSON state file after each step (~ LangGraph checkpointer)
  dbos            : faithful re-impl of DBOS's mechanism -- the step-completion record is
                    written IN THE SAME DB TRANSACTION as a transactional effect (=>
                    exactly-once for `charge`); for the non-transactional `write_receipt`
                    the record is written AFTER the effect (DBOS's documented limitation
                    for non-transactional steps)
  agenttx         : per-turn idempotency key + content-addressed atomic FS commit (overlay
                    + atomic rename keyed by turn) + transactional WAL record

Hypothesis (the novelty wedge): none/checkpoint duplicate BOTH effects at crash windows;
dbos gets the transactional `charge` exactly-once but DUPLICATES the non-transactional
`write_receipt`; agenttx gets BOTH exactly-once. If dbos already gets both -> KILL.

Run:  python3 gate1/failure_audit.py audit --trials 20
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sqlite3
import subprocess
import sys

ORDER_ID = "ORD-1"
AMOUNT = 100
# named crash points (None = no crash)
POINTS = [
    "none",
    "pre_charge",
    "post_charge_pre_record",   # transactional-effect window
    "post_charge_record",
    "post_receipt_pre_record",  # NON-transactional-effect window (the key one)
    "post_receipt_record",
]
ORCHS = ["none", "checkpoint", "dbos", "agenttx"]


def _db(store):
    c = sqlite3.connect(os.path.join(store, "app.db"), isolation_level=None, timeout=10)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("CREATE TABLE IF NOT EXISTS charges(order_id TEXT, amount INT)")
    c.execute("CREATE TABLE IF NOT EXISTS steps(name TEXT PRIMARY KEY)")
    c.execute("CREATE TABLE IF NOT EXISTS wal(turn TEXT, step TEXT, PRIMARY KEY(turn, step))")
    return c


def _crash(point, kill_at):
    if point == kill_at:
        os._exit(137)


def _receipt_dir(store):
    d = os.path.join(store, "receipts")
    os.makedirs(d, exist_ok=True)
    return d


# ----- the two real side-effecting tools -----
def charge(db, order_id, amount, also_record=None):
    """Transactional charge. If also_record given, the step record is committed in the
    SAME transaction (the DBOS/AgentTx mechanism) -> atomic with the effect."""
    db.execute("BEGIN")
    db.execute("INSERT INTO charges(order_id, amount) VALUES(?,?)", (order_id, amount))
    if also_record is not None:
        table, key = also_record
        db.execute(f"INSERT OR IGNORE INTO {table} VALUES(?{',?' if table=='wal' else ''})",
                   key if isinstance(key, tuple) else (key,))
    db.execute("COMMIT")


def write_receipt_unique(store):
    """Non-idempotent FS effect: a NEW uniquely-named file each call (models a
    non-idempotent external effect, e.g. 'send email'). Re-run => a 2nd file => duplicate."""
    import uuid
    p = os.path.join(_receipt_dir(store), f"receipt_{uuid.uuid4().hex}.txt")
    with open(p, "w") as f:
        f.write(f"{ORDER_ID}\n"); f.flush(); os.fsync(f.fileno())
    return p


def write_receipt_keyed(store, turn_key):
    """AgentTx FS commit: write to a temp then ATOMIC rename to a name keyed by the turn
    idempotency key. Re-run overwrites the SAME name (idempotent) => exactly-once."""
    d = _receipt_dir(store)
    tmp = os.path.join(d, f".tmp_{turn_key}")
    final = os.path.join(d, f"receipt_{turn_key}.txt")
    with open(tmp, "w") as f:
        f.write(f"{ORDER_ID}\n"); f.flush(); os.fsync(f.fileno())
    os.replace(tmp, final)   # atomic
    return final


# ----------------------------- orchestrators -----------------------------
def run_none(store, kill_at, recover=False):
    db = _db(store)
    # no persistence: 'recover' just re-runs the whole turn from scratch
    _crash("pre_charge", kill_at)
    charge(db, ORDER_ID, AMOUNT)
    _crash("post_charge_pre_record", kill_at)
    _crash("post_charge_record", kill_at)
    write_receipt_unique(store)
    _crash("post_receipt_pre_record", kill_at)
    _crash("post_receipt_record", kill_at)


def run_checkpoint(store, kill_at, recover=False):
    db = _db(store)
    sp = os.path.join(store, "ckpt.json")
    done = set(json.load(open(sp)).get("done", [])) if os.path.exists(sp) else set()

    def save():
        with open(sp + ".tmp", "w") as f:
            json.dump({"done": sorted(done)}, f); f.flush(); os.fsync(f.fileno())
        os.replace(sp + ".tmp", sp)

    if "charge" not in done:
        _crash("pre_charge", kill_at)
        charge(db, ORDER_ID, AMOUNT)          # effect first
        _crash("post_charge_pre_record", kill_at)   # crash here => state NOT saved => dup on recover
        done.add("charge"); save()
        _crash("post_charge_record", kill_at)
    if "receipt" not in done:
        write_receipt_unique(store)           # effect first
        _crash("post_receipt_pre_record", kill_at)  # crash here => state NOT saved => dup on recover
        done.add("receipt"); save()
        _crash("post_receipt_record", kill_at)


def run_dbos(store, kill_at, recover=False):
    """Faithful DBOS: transactional effect's step-record is in the SAME tx (atomic);
    non-transactional effect's record is written AFTER the effect (non-atomic)."""
    db = _db(store)
    done = {r[0] for r in db.execute("SELECT name FROM steps").fetchall()}
    if "charge" not in done:
        _crash("pre_charge", kill_at)
        charge(db, ORDER_ID, AMOUNT, also_record=("steps", "charge"))  # ATOMIC effect+record
        _crash("post_charge_pre_record", kill_at)   # impossible to be inconsistent (atomic)
        _crash("post_charge_record", kill_at)
    if "receipt" not in done:
        write_receipt_unique(store)            # non-transactional effect
        _crash("post_receipt_pre_record", kill_at)  # crash => record NOT written => dup on recover
        db.execute("INSERT OR IGNORE INTO steps VALUES('receipt')")
        _crash("post_receipt_record", kill_at)


def run_agenttx(store, kill_at, recover=False):
    """AgentTx-min: turn idempotency key + content-addressed atomic FS commit + tx WAL."""
    db = _db(store)
    turn = "T1"
    done = {r[0] for r in db.execute("SELECT step FROM wal WHERE turn=?", (turn,)).fetchall()}
    if "charge" not in done:
        _crash("pre_charge", kill_at)
        charge(db, ORDER_ID, AMOUNT, also_record=("wal", (turn, "charge")))  # atomic
        _crash("post_charge_pre_record", kill_at)
        _crash("post_charge_record", kill_at)
    if "receipt" not in done:
        write_receipt_keyed(store, turn)       # idempotent: same name on re-run
        _crash("post_receipt_pre_record", kill_at)   # crash => re-run overwrites same file (no dup)
        db.execute("INSERT OR IGNORE INTO wal VALUES(?,?)", (turn, "receipt"))
        _crash("post_receipt_record", kill_at)


RUNNERS = {"none": run_none, "checkpoint": run_checkpoint, "dbos": run_dbos, "agenttx": run_agenttx}


# ------------------------------- oracle -------------------------------
def oracle(store):
    db = _db(store)
    n_charge = db.execute("SELECT COUNT(*) FROM charges WHERE order_id=?", (ORDER_ID,)).fetchone()[0]
    n_receipt = len(glob.glob(os.path.join(_receipt_dir(store), "receipt_*.txt")))
    return n_charge, n_receipt


def classify(store):
    nc, nr = oracle(store)
    dup = nc > 1 or nr > 1
    lost = nc < 1 or nr < 1
    if dup:
        return "DUPLICATE", nc, nr
    if lost:
        return "LOST", nc, nr
    return "CORRECT", nc, nr


# ------------------------------- driver -------------------------------
def worker_main(a):
    RUNNERS[a.orch](a.store, a.kill_at, recover=(a.phase == "recover"))


def audit(trials):
    here = os.path.abspath(__file__)
    results = {o: {p: {"CORRECT": 0, "DUPLICATE": 0, "LOST": 0} for p in POINTS} for o in ORCHS}
    for orch in ORCHS:
        for point in POINTS:
            for t in range(trials):
                store = f"/tmp/agenttx_g1/{orch}_{point}_{t}"
                subprocess.run(["rm", "-rf", store]); os.makedirs(store, exist_ok=True)
                # phase 1: run with a crash injected at `point`
                rc = subprocess.run([sys.executable, here, "worker", "--orch", orch,
                                     "--store", store, "--kill-at", point, "--phase", "run"]).returncode
                # phase 2: recovery runs ONLY if the worker actually crashed (faithful: a
                # cleanly-completed turn needs no recovery). os._exit(137) => rc != 0.
                if rc != 0:
                    subprocess.run([sys.executable, here, "worker", "--orch", orch,
                                    "--store", store, "--kill-at", "none", "--phase", "recover"])
                results[orch][point][classify(store)[0]] += 1
    return results


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("worker")
    w.add_argument("--orch", required=True); w.add_argument("--store", required=True)
    w.add_argument("--kill-at", default="none"); w.add_argument("--phase", default="run")
    au = sub.add_parser("audit"); au.add_argument("--trials", type=int, default=20)
    a = ap.parse_args()
    if a.cmd == "worker":
        worker_main(a)
        return
    res = audit(a.trials)
    # report: per orch x point, the dominant outcome; and a correctness summary
    print("\n=== Gate-1a failure-window audit (trials=%d) ===" % a.trials)
    hdr = "orchestrator      " + "".join(f"{p[:13]:>15}" for p in POINTS)
    print(hdr)
    for o in ORCHS:
        row = f"{o:<16}  "
        for p in POINTS:
            r = res[o][p]
            tag = "OK" if r["CORRECT"] == sum(r.values()) else (
                f"DUP{r['DUPLICATE']}" if r["DUPLICATE"] else f"LOST{r['LOST']}")
            row += f"{tag:>15}"
        print(row)
    # verdict
    def fails(o):
        return any(res[o][p]["DUPLICATE"] or res[o][p]["LOST"] for p in POINTS)
    verdict = {
        "naive_baselines_fail": fails("none") and fails("checkpoint"),
        "dbos_fails_on_nontransactional": res["dbos"]["post_receipt_pre_record"]["DUPLICATE"] > 0,
        "dbos_exactly_once_on_transactional": res["dbos"]["post_charge_pre_record"]["CORRECT"] == a.trials,
        "agenttx_exactly_once_both": not fails("agenttx"),
    }
    verdict["GATE1A_PASS"] = (verdict["naive_baselines_fail"]
                              and verdict["dbos_fails_on_nontransactional"]
                              and verdict["agenttx_exactly_once_both"])
    out = {"trials": a.trials, "results": res, "verdict": verdict}
    os.makedirs("gate1/results", exist_ok=True)
    json.dump(out, open("gate1/results/gate1a_failure_audit.json", "w"), indent=2)
    print("\nverdict:", json.dumps(verdict, indent=2))
    print("wrote gate1/results/gate1a_failure_audit.json")


if __name__ == "__main__":
    main()
