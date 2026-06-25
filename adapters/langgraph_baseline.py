"""REAL LangGraph baseline (Postgres checkpointer).

A LangGraph turn graph: charge_node (DB insert) -> receipt_node (unique file). LangGraph
checkpoints state AFTER each node completes; the effect inside a node is NOT atomic with the
checkpoint. We crash (os._exit) inside receipt_node after the file write but before the node
returns (so no checkpoint for it), then resume the same thread_id. Question: does LangGraph
re-run the node -> duplicate file? (And the charge: crash between its effect and checkpoint
also re-runs -> duplicate.)

  python adapters/langgraph_baseline.py audit
"""
from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
import uuid
from typing import TypedDict

import psycopg

PG = os.environ.get("AGENTTX_PG", "host=/tmp port=54329 user=agenttx dbname=agenttx")
DSN = "postgresql://agenttx@/agenttx?host=/tmp&port=54329"
STORE = "/tmp/atx_lg"
ORDER = "LG-ORD"


class S(TypedDict):
    order: str
    charged: bool
    receipt: str


def charge_node(state: S):
    c = psycopg.connect(PG, autocommit=True)
    c.execute("INSERT INTO charges(order_id,amount) VALUES(%s,%s)", (state["order"], 100))
    c.close()
    if os.environ.get("CRASH_AT") == "charge":
        os._exit(137)                     # crash after effect, before node-checkpoint
    return {"charged": True}


def receipt_node(state: S):
    d = os.path.join(STORE, "receipts"); os.makedirs(d, exist_ok=True)
    p = os.path.join(d, f"{uuid.uuid4().hex}.txt")    # non-idempotent: new file each call
    with open(p, "w") as f:
        f.write(state["order"]); f.flush(); os.fsync(f.fileno())
    if os.environ.get("CRASH_AT") == "receipt":
        os._exit(137)                     # crash after effect, before node-checkpoint
    return {"receipt": p}


def build(cp):
    from langgraph.graph import END, START, StateGraph
    g = StateGraph(S)
    g.add_node("charge", charge_node)
    g.add_node("receipt", receipt_node)
    g.add_edge(START, "charge")
    g.add_edge("charge", "receipt")
    g.add_edge("receipt", END)
    return g.compile(checkpointer=cp)


def run(thread_id, crash_at):
    from langgraph.checkpoint.postgres import PostgresSaver
    os.environ["CRASH_AT"] = crash_at
    with PostgresSaver.from_conn_string(DSN) as cp:
        cp.setup()
        graph = build(cp)
        cfg = {"configurable": {"thread_id": thread_id}}
        graph.invoke({"order": ORDER, "charged": False, "receipt": ""}, cfg)


def inspect():
    c = psycopg.connect(PG)
    nc = c.execute("SELECT COUNT(*) FROM charges WHERE order_id=%s", (ORDER,)).fetchone()[0]
    c.close()
    return nc, len(glob.glob(os.path.join(STORE, "receipts", "*.txt")))


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "audit"
    if cmd == "run":
        run(os.environ["TID"], os.environ.get("CRASH_AT", ""))
        return
    # audit
    os.system(f"rm -rf {STORE} && mkdir -p {STORE}/receipts")
    c = psycopg.connect(PG, autocommit=True)
    c.execute("DROP TABLE IF EXISTS charges"); c.execute("CREATE TABLE charges(order_id TEXT, amount INT)")
    c.close()
    here = os.path.abspath(__file__)
    tid = f"lg-{uuid.uuid4().hex[:8]}"
    base = dict(os.environ, PYTHONPATH=os.path.dirname(os.path.dirname(here)), TID=tid)
    rc = subprocess.run([sys.executable, here, "run"], env=dict(base, CRASH_AT="receipt")).returncode
    subprocess.run([sys.executable, here, "run"], env=dict(base, CRASH_AT=""))   # resume thread
    nc, nr = inspect()
    out = {"baseline": "real-LangGraph", "langgraph_version": __import__("importlib.metadata", fromlist=["version"]).version("langgraph"),
           "checkpointer": "PostgresSaver", "run_crashed_rc": rc, "charges": nc, "receipts": nr,
           "effect_duplicated_on_resume": nr > 1 or nc > 1,
           "reads": "REAL LangGraph + PostgresSaver: a node's side effect is NOT atomic with the "
                    "post-node checkpoint; a crash in the effect/checkpoint window re-runs the node "
                    "on resume -> duplicate effect. Same gap as the Gate-1a naive-checkpoint baseline."}
    os.makedirs("gate1/results", exist_ok=True)
    json.dump(out, open("gate1/results/real_langgraph_baseline.json", "w"), indent=2)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
