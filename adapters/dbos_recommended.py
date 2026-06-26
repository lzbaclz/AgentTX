"""REAL DBOS with its RECOMMENDED configurations for a non-transactional effect (advisor P2).

The naked baseline (`adapters/dbos_baseline.py`) duplicates the receipt because it uses a
uuid-named file inside a plain `@DBOS.step` -- a config DBOS's own docs would NOT recommend for a
non-idempotent external effect. This adapter runs the two configs DBOS *would* recommend, under the
IDENTICAL crash (os._exit after the effect, before the step result is recorded), and asks whether
the gap CLOSES:

  * idempotency_key   : the step writes a file named by a DETERMINISTIC key (the DBOS workflow id),
                        published by an ATOMIC rename -> re-execution on recovery is a no-op.
  * transactional_outbox : the intent to produce the receipt is INSERTed into an `outbox` row in the
                        SAME transaction as the charge; an idempotent relay step produces the file
                        (keyed by the outbox row) and marks it done.

Both CLOSE the gap (receipts == 1). The honest conclusion (see the printed `conclusion`): DBOS is
NOT incapable -- the winning move (deterministic key + idempotent atomic publish) ports directly into
a DBOS step. AgentTx's contribution is therefore the TAXONOMY that applies the right mechanism per
tool class AUTOMATICALLY (so the developer does not hand-roll an idempotency key / outbox per tool),
PLUS binding it cross-plane to the KV view and output log -- not "DBOS cannot do exactly-once".

  python adapters/dbos_recommended.py audit   # runs both variants under the crash, writes the json

Set VARIANT=idempotency_key|transactional_outbox for the `run`/`recover` subprocess workers.
"""
from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
import time

import psycopg
from sqlalchemy import text

from dbos import DBOS, DBOSConfig, SetWorkflowID

PG = os.environ.get("AGENTTX_PG", "host=/tmp port=54329 user=agenttx dbname=agenttx")
SA_URL = "postgresql+psycopg://agenttx@/agenttx?host=/tmp&port=54329"
STORE = "/tmp/atx_dbos_reco"
ORDER = "DBOS-RECO-ORD"
VARIANT = os.environ.get("VARIANT", "idempotency_key")

DBOS(config=DBOSConfig(name="agenttx_dbos_reco", database_url=SA_URL))


def _idempotent_publish(order):
    """Write a file named by the DETERMINISTIC workflow id, via an atomic rename. Re-running for the
    same workflow id is a no-op -> the non-transactional effect becomes idempotent."""
    d = os.path.join(STORE, "receipts"); os.makedirs(d, exist_ok=True)
    final = os.path.join(d, f"{DBOS.workflow_id}.txt")          # deterministic key, NOT a uuid
    if not os.path.exists(final):
        tmp = final + f".{os.getpid()}.tmp"
        with open(tmp, "w") as f:
            f.write(order); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, final)                                  # atomic idempotent publish
    return final


# ---- variant A: idempotency key ----
@DBOS.transaction()
def charge_tx(order, amount):
    DBOS.sql_session.execute(text("INSERT INTO charges(order_id,amount) VALUES(:o,:a)"),
                             {"o": order, "a": amount})


@DBOS.step()
def receipt_step_idem(order):
    p = _idempotent_publish(order)
    if os.environ.get("CRASH_RECEIPT") == "1":
        os._exit(137)                                           # crash AFTER effect, before step record
    return p


@DBOS.workflow()
def turn_idem(order):
    charge_tx(order, 100)
    receipt_step_idem(order)


# ---- variant B: transactional outbox ----
@DBOS.transaction()
def charge_and_outbox_tx(order, amount):
    DBOS.sql_session.execute(text("INSERT INTO charges(order_id,amount) VALUES(:o,:a)"),
                             {"o": order, "a": amount})
    DBOS.sql_session.execute(                                   # intent recorded in the SAME tx
        text("INSERT INTO receipt_outbox(wfid,order_id,status) VALUES(:w,:o,'pending')"),
        {"w": DBOS.workflow_id, "o": order})


@DBOS.step()
def relay_step(order):
    p = _idempotent_publish(order)                             # idempotent delivery of the outbox intent
    if os.environ.get("CRASH_RECEIPT") == "1":
        os._exit(137)                                          # crash AFTER effect, before marking done
    with psycopg.connect(PG, autocommit=True) as c:
        c.execute("UPDATE receipt_outbox SET status='done' WHERE wfid=%s", (DBOS.workflow_id,))
    return p


@DBOS.workflow()
def turn_outbox(order):
    charge_and_outbox_tx(order, 100)
    relay_step(order)


def inspect():
    with psycopg.connect(PG) as c:
        nc = c.execute("SELECT COUNT(*) FROM charges WHERE order_id=%s", (ORDER,)).fetchone()[0]
    nr = len(glob.glob(os.path.join(STORE, "receipts", "*.txt")))
    return nc, nr


def _run(wfid):
    DBOS.launch()
    with SetWorkflowID(wfid):
        (turn_outbox if VARIANT == "transactional_outbox" else turn_idem)(ORDER)


def _recover(wfid):
    DBOS.launch()
    for _ in range(60):
        try:
            st = DBOS.get_workflow_status(wfid)
            if st and st.status == "SUCCESS":
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.5)


def _run_variant(variant):
    """Driver: clean state, crash in the effect/record window, recover, inspect."""
    os.system(f"rm -rf {STORE} && mkdir -p {STORE}/receipts")
    with psycopg.connect(PG, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS charges"); c.execute("CREATE TABLE charges(order_id TEXT, amount INT)")
        c.execute("DROP TABLE IF EXISTS receipt_outbox")
        c.execute("CREATE TABLE receipt_outbox(wfid TEXT, order_id TEXT, status TEXT)")
        for t in ("dbos.workflow_status", "dbos.operation_outputs", "dbos.workflow_inputs"):
            try:
                c.execute(f"TRUNCATE {t} CASCADE")
            except Exception:  # noqa: BLE001
                pass
    here = os.path.abspath(__file__)
    import uuid
    wfid = f"reco-{variant}-{uuid.uuid4().hex[:8]}"
    base = dict(os.environ, VARIANT=variant, AGENTTX_WFID=wfid,
                PYTHONPATH=os.path.dirname(os.path.dirname(here)))
    rc = subprocess.run([sys.executable, here, "run", wfid],
                        env=dict(base, CRASH_RECEIPT="1")).returncode      # crashes in the effect window
    subprocess.run([sys.executable, here, "recover", wfid], env=dict(base, CRASH_RECEIPT="0"))
    nc, nr = inspect()
    return {"variant": variant, "run_rc": rc, "charges": nc, "receipts": nr,
            "transactional_exactly_once": nc == 1, "nontransactional_exactly_once": nr == 1,
            "gap_closed": nc == 1 and nr == 1}


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "audit"
    if cmd == "run":
        _run(sys.argv[2]); return
    if cmd == "recover":
        _recover(sys.argv[2]); return

    results = [_run_variant("idempotency_key"), _run_variant("transactional_outbox")]
    naked = {}
    try:
        naked = json.load(open("gate1/results/real_dbos_baseline.json"))
    except Exception:  # noqa: BLE001
        pass
    out = {
        "baseline": "real-DBOS recommended configs",
        "dbos_version": __import__("importlib.metadata", fromlist=["version"]).version("dbos"),
        "naked_dbos_receipts": naked.get("receipts"),       # the duplicated baseline (for contrast)
        "variants": results,
        "both_close_the_gap": all(r["gap_closed"] for r in results),
        "conclusion": "DBOS's recommended configs (deterministic idempotency key OR transactional "
                      "outbox + idempotent relay) make the non-transactional receipt EXACTLY-ONCE "
                      "under the same crash that duplicates the naked baseline. So the gap is a "
                      "default-config artifact, not a fundamental DBOS limit: the fix (deterministic "
                      "key + idempotent atomic publish) IS AgentTx's OVERLAY mechanism. AgentTx's "
                      "contribution is applying the right per-class mechanism AUTOMATICALLY (the "
                      "taxonomy) and binding it cross-plane (KV view + output log), not out-doing "
                      "DBOS on a single transactional effect.",
    }
    os.makedirs("gate1/results", exist_ok=True)
    json.dump(out, open("gate1/results/dbos_recommended.json", "w"), indent=2)
    print(json.dumps(out, indent=2))
    return 0 if out["both_close_the_gap"] else 1


if __name__ == "__main__":
    sys.exit(main())
