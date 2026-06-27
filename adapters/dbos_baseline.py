"""REAL DBOS baseline (closes the Gate-1a 'faithful re-impl' caveat).

A DBOS turn workflow with a TRANSACTIONAL charge step (@DBOS.transaction -> effect+checkpoint
in one Postgres tx) and a NON-transactional receipt step (@DBOS.step -> a unique file, the
result stored AFTER the effect). We crash (os._exit) inside the receipt step after the file
write but before DBOS records the step result, then restart DBOS (which auto-recovers the
pending workflow). Question: does REAL DBOS re-run the receipt step -> duplicate file?

  python adapters/dbos_baseline.py run      # runs the workflow, crashes in receipt step
  python adapters/dbos_baseline.py recover  # DBOS.launch() recovers the pending workflow
  python adapters/dbos_baseline.py audit    # driver: run -> recover -> inspect (subprocess)
"""
from __future__ import annotations

import glob
import os
import subprocess
import sys
import time

import psycopg
from sqlalchemy import text

from dbos import DBOS, DBOSConfig, SetWorkflowID

PG = os.environ.get("AGENTTX_PG", "host=/tmp port=54329 user=agenttx dbname=agenttx")
SA_URL = "postgresql+psycopg://agenttx@/agenttx?host=/tmp&port=54329"
STORE = "/tmp/atx_dbos"
WFID = os.environ.get("AGENTTX_WFID", "turn-dbos-1")   # fresh per audit (else DBOS idempotency skips)
ORDER = "DBOS-ORD"

# DBOS must be constructed BEFORE the @DBOS.workflow/transaction/step decorators register.
DBOS(config=DBOSConfig(name="agenttx_dbos", database_url=SA_URL))


@DBOS.transaction()
def charge_tx(order, amount):
    DBOS.sql_session.execute(text("INSERT INTO charges(order_id,amount) VALUES(:o,:a)"),
                             {"o": order, "a": amount})       # effect + DBOS step checkpoint, 1 tx


@DBOS.step()
def receipt_step(order):
    d = os.path.join(STORE, "receipts"); os.makedirs(d, exist_ok=True)
    if os.environ.get("IDEMPOTENT_RECEIPT") == "1":
        # DBOS's RECOMMENDED pattern: make the effect idempotent (content/id-addressed) so a
        # step re-run on recovery overwrites the SAME file instead of creating a duplicate.
        p = os.path.join(d, f"{WFID}.txt")
    else:
        import uuid
        p = os.path.join(d, f"{uuid.uuid4().hex}.txt")        # naked non-idempotent: a new file each call
    with open(p, "w") as f:
        f.write(order); f.flush(); os.fsync(f.fileno())
    if os.environ.get("CRASH_RECEIPT") == "1":
        os._exit(137)                                          # crash AFTER effect, before step record
    return p


@DBOS.workflow()
def turn(order):
    charge_tx(order, 100)
    receipt_step(order)


def inspect():
    c = psycopg.connect(PG)
    nc = c.execute("SELECT COUNT(*) FROM charges WHERE order_id=%s", (ORDER,)).fetchone()[0]
    c.close()
    nr = len(glob.glob(os.path.join(STORE, "receipts", "*.txt")))
    return nc, nr


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "audit"
    if cmd == "run":
        DBOS.launch()
        with SetWorkflowID(WFID):
            turn(ORDER)
        return
    if cmd == "recover":
        DBOS.launch()                                # auto-recovers pending workflows
        for _ in range(60):
            try:
                st = DBOS.get_workflow_status(WFID)
                if st and st.status == "SUCCESS":
                    break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.5)
        return

    # audit driver
    os.system(f"rm -rf {STORE} && mkdir -p {STORE}/receipts")
    c = psycopg.connect(PG, autocommit=True)
    for t in ("charges",):
        c.execute(f"DROP TABLE IF EXISTS {t}")
    c.execute("CREATE TABLE charges(order_id TEXT, amount INT)")
    # reset DBOS system tables for a clean workflow id
    for t in ("dbos.workflow_status", "dbos.operation_outputs", "dbos.workflow_inputs"):
        try:
            c.execute(f"TRUNCATE {t} CASCADE")
        except Exception:  # noqa: BLE001
            pass
    c.close()
    here = os.path.abspath(__file__)
    import uuid
    wfid = f"turn-dbos-{uuid.uuid4().hex[:8]}"          # fresh id => DBOS must really execute
    env = dict(os.environ, CRASH_RECEIPT="1", AGENTTX_WFID=wfid,
               PYTHONPATH=os.path.dirname(os.path.dirname(here)))
    rc1 = subprocess.run([sys.executable, here, "run"], env=env).returncode   # crashes in receipt step
    env2 = dict(env, CRASH_RECEIPT="0")
    subprocess.run([sys.executable, here, "recover"], env=env2)        # DBOS recovers
    print(f"(run crashed with rc={rc1}; wfid={wfid})")
    nc, nr = inspect()
    import json
    idem = os.environ.get("IDEMPOTENT_RECEIPT") == "1"
    out = {"baseline": "real-DBOS" + ("+idempotent-effect" if idem else " (naked step)"),
           "dbos_version": __import__("importlib.metadata", fromlist=["version"]).version("dbos"),
           "idempotent_effect": idem, "charges": nc, "receipts": nr,
           "transactional_exactly_once": nc == 1,
           "nontransactional_duplicated": nr > 1,
           "reads": ("REAL DBOS + DBOS's recommended idempotent (content/id-addressed) effect: the "
                     "receipt step re-runs on recovery but overwrites the SAME file -> exactly-once "
                     "(1 file). A careful DBOS user reaches exactly-once for THIS effect."
                     if idem else
                     "REAL DBOS naked step: the non-transactional receipt @DBOS.step re-runs on "
                     "recovery after a crash in the effect/record window -> duplicate file. This is "
                     "the FAILURE EXAMPLE, not the strong baseline.")}
    os.makedirs("gate1/results", exist_ok=True)
    fn = "gate1/results/real_dbos_idempotent.json" if idem else "gate1/results/real_dbos_baseline.json"
    json.dump(out, open(fn, "w"), indent=2)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
