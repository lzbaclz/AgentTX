"""AgentTx Gate-2c: steady-state bookkeeping overhead.

Per-turn cost of the AgentTx durability machinery (Turn WAL appends + Tool Gateway dedup +
fsync'd commits + content-addressed FS commit) vs a no-persistence baseline that performs
the SAME external effects without any durability bookkeeping. We report the absolute
per-turn overhead and its fraction of a representative agent-turn cost (LLM + tool latency).

GO: bookkeeping overhead < 5% of a representative turn (>=100 ms; real agent turns with a
tool call are typically hundreds of ms).

Run: python3 gate2/overhead.py
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import time

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agenttx.core import Clock, Coordinator  # noqa: E402

N = 3000


def agenttx_per_turn(store):
    c = Coordinator(store)
    clk = Clock(0)
    t0 = time.perf_counter()
    for i in range(N):
        c.run_turn(f"T{i}", [("sql", (f"O{i}", 100)), ("fs", (f"O{i}",))], clk)
    return (time.perf_counter() - t0) / N * 1e3


def baseline_per_turn(store):
    """Same external effects (charge INSERT + receipt file) with NO durability bookkeeping:
    no WAL, no gateway dedup table, no fsync."""
    db = sqlite3.connect(os.path.join(store, "b.db"), isolation_level=None)
    db.execute("PRAGMA synchronous=OFF")
    db.execute("CREATE TABLE IF NOT EXISTS charges(order_id TEXT, amount INT)")
    rd = os.path.join(store, "b_receipts"); os.makedirs(rd, exist_ok=True)
    t0 = time.perf_counter()
    for i in range(N):
        db.execute("INSERT INTO charges VALUES(?,?)", (f"O{i}", 100))
        with open(os.path.join(rd, f"r{i}.txt"), "w") as f:
            f.write(f"O{i}\n")
    return (time.perf_counter() - t0) / N * 1e3


def main():
    store = "/dev/shm/agenttx_g2c" if os.path.isdir("/dev/shm") else "/tmp/agenttx_g2c"
    subprocess.run(["rm", "-rf", store]); os.makedirs(store)
    base = baseline_per_turn(store)
    subprocess.run(["rm", "-rf", store]); os.makedirs(store)
    atx = agenttx_per_turn(store)
    overhead = atx - base
    rows = {f"turn_cost_{tc}ms": round(overhead / tc * 100, 2) for tc in (100, 200, 500)}
    out = {"n_turns": N, "baseline_ms_per_turn": round(base, 4),
           "agenttx_ms_per_turn": round(atx, 4), "overhead_ms_per_turn": round(overhead, 4),
           "overhead_pct_of_representative_turn": rows,
           "GATE2C_PASS": overhead / 100 * 100 < 5,   # <5% even at a short 100ms turn
           "reads": "AgentTx durability bookkeeping per turn (WAL + gateway + fsync + atomic FS "
                    "commit) vs a no-persistence baseline doing the same effects. Overhead is "
                    "reported as a fraction of a representative agent-turn cost (LLM+tool, >=100ms)."}
    os.makedirs("gate2/results", exist_ok=True)
    json.dump(out, open("gate2/results/gate2c_overhead.json", "w"), indent=2)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
