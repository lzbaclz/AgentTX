"""AgentTx Gate-2a: 10k random fault injections on the full coordinator -> 0 dup/lost.

Each trial: run the turn with a crash at a random durable boundary; then RECOVER (which may
ALSO crash, at a random boundary, with prob p -- testing crash-during-recovery); repeat
recovery until it completes cleanly; then the oracle counts the REAL external effects
(SQLite charges + receipt files) for the order. Supported tools (SQL transactional + FS
overlay) must be EXACTLY-ONCE: 0 duplicate, 0 lost, across all trials.

  python3 gate2/fault_audit.py audit  --trials 10000   # fast in-process crash model
  python3 gate2/fault_audit.py xcheck --trials 500      # faithful subprocess os._exit cross-check
"""
from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agenttx.core import Clock, Coordinator, Crash, close_coord, oracle  # noqa: E402

TURN = "T1"
ORDER = "ORD"
PLAN = [("sql", (ORDER, 100)), ("fs", (ORDER,))]
MAX_TICK = 12          # > number of durable boundaries in one turn
RECOVER_CRASH_P = 0.4  # probability recovery itself crashes (tests crash-during-recovery)


def classify(store):
    nc, nr = oracle(store, ORDER)
    if nc > 1 or nr > 1:
        return "DUPLICATE", nc, nr
    if nc < 1 or nr < 1:
        return "LOST", nc, nr
    return "CORRECT", nc, nr


# ---------------- fast in-process crash model ----------------
def trial_inproc(rng, store):
    subprocess.run(["rm", "-rf", store]); os.makedirs(store, exist_ok=True)
    coord = Coordinator(store)
    try:
        coord.run_turn(TURN, PLAN, Clock(rng.randint(1, MAX_TICK)))
    except Crash:
        pass
    close_coord(coord)
    for _ in range(25):                      # recover (may crash) until clean
        coord = Coordinator(store)
        ka = rng.randint(1, MAX_TICK) if rng.random() < RECOVER_CRASH_P else 0
        try:
            coord.recover(TURN, PLAN, Clock(ka)); close_coord(coord); break
        except Crash:
            close_coord(coord)
    return classify(store)


# ---------------- faithful subprocess os._exit ----------------
def worker(a):
    coord = Coordinator(a.store)
    clock = Clock(a.kill_at, hard=True)
    if a.phase == "run":
        try:
            coord.run_turn(TURN, PLAN, clock)
        except Crash:
            pass
    else:
        coord.recover(TURN, PLAN, clock)


def trial_subproc(rng, store, here):
    subprocess.run(["rm", "-rf", store]); os.makedirs(store, exist_ok=True)
    subprocess.run([sys.executable, here, "worker", "--phase", "run", "--store", store,
                    "--kill-at", str(rng.randint(1, MAX_TICK))])
    for _ in range(25):
        ka = rng.randint(1, MAX_TICK) if rng.random() < RECOVER_CRASH_P else 0
        rc = subprocess.run([sys.executable, here, "worker", "--phase", "recover",
                             "--store", store, "--kill-at", str(ka)]).returncode
        if rc == 0:
            break
    return classify(store)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("worker")
    w.add_argument("--phase", required=True); w.add_argument("--store", required=True)
    w.add_argument("--kill-at", type=int, default=0)
    au = sub.add_parser("audit"); au.add_argument("--trials", type=int, default=10000)
    xc = sub.add_parser("xcheck"); xc.add_argument("--trials", type=int, default=500)
    a = ap.parse_args()
    if a.cmd == "worker":
        worker(a); return

    rng = random.Random(0)
    tally = {"CORRECT": 0, "DUPLICATE": 0, "LOST": 0}
    if a.cmd == "audit":
        store = "/dev/shm/agenttx_g2/t" if os.path.isdir("/dev/shm") else "/tmp/agenttx_g2/t"
        for i in range(a.trials):
            tally[trial_inproc(rng, store)[0]] += 1
            if (i + 1) % 2000 == 0:
                print(f"  {i+1}/{a.trials}: {tally}", flush=True)
        mode = "in-process"
    else:
        here = os.path.abspath(__file__)
        store = "/tmp/agenttx_g2x/t"
        for i in range(a.trials):
            tally[trial_subproc(rng, store, here)[0]] += 1
            if (i + 1) % 100 == 0:
                print(f"  {i+1}/{a.trials}: {tally}", flush=True)
        mode = "subprocess os._exit"

    pas = tally["DUPLICATE"] == 0 and tally["LOST"] == 0
    out = {"mode": mode, "trials": a.trials, "recover_crash_prob": RECOVER_CRASH_P,
           "tally": tally, "supported_tools": ["sql(transactional)", "fs(overlay/content-addressed)"],
           "GATE2A_PASS": pas}
    os.makedirs("gate2/results", exist_ok=True)
    fn = f"gate2/results/gate2a_{a.cmd}.json"
    json.dump(out, open(fn, "w"), indent=2)
    print(f"\n[{mode}] trials={a.trials}: {tally} -> GATE2A_PASS={pas}")
    print("wrote", fn)


if __name__ == "__main__":
    main()
