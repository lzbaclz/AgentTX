"""Phase-4: streaming exactly-once + multi-worker reroute, fault-injected.

Each trial streams a turn's committed token log to a client while crashing the streaming
WORKER at a random mid-stream boundary; a fresh worker (multi-worker reroute) resumes from a
possibly-STALE server-known ACK (forcing re-sends), repeating until the stream completes. We
then assert the client materialized the committed tokens EXACTLY ONCE, in order, with no
loss -- across all the crashes, reroutes, and re-sends.

Run: python phase4/stream_audit.py
"""
import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agenttx.stream import CrashClock, StreamClient, StreamLog, WorkerCrash, serve


def run_turn(tokens, rng, max_workers=200):
    log = StreamLog(tokens)
    client = StreamClient()
    workers = 0
    while client.ack < len(log) and workers < max_workers:
        workers += 1
        # a fresh worker resumes from the server-known ACK, which may LAG the client's real
        # ACK (network ACK not yet propagated) -> it re-sends already-received tokens.
        server_known = rng.randint(max(0, client.ack - 3), client.ack)
        kill = rng.randint(1, 2 * len(log) + 2)        # > 2*len => no crash this attempt
        try:
            serve(log, client, server_known + 1, CrashClock(kill))
            break                                       # streamed to the end
        except WorkerCrash:
            continue                                    # reroute to another worker
    return client, workers


def main():
    rng = random.Random(0)
    N = 20000
    exact = 0
    bad = {"loss": 0, "duplicate_in_received": 0, "out_of_order": 0, "not_completed": 0}
    total_workers = 0
    total_resends = 0
    max_workers = 0
    for _ in range(N):
        L = rng.randint(1, 80)
        tokens = [f"tok{i}" for i in range(L)]
        client, workers = run_turn(tokens, rng)
        total_workers += workers
        total_resends += client.dup_dropped
        max_workers = max(max_workers, workers)
        if client.received == tokens:
            exact += 1
        else:
            if client.ack < L:
                bad["not_completed"] += 1
            if len(client.received) != len(set(map(id, client.received))) and len(client.received) != len(tokens):
                bad["duplicate_in_received"] += 1
            if client.received != tokens[: len(client.received)]:
                bad["out_of_order"] += 1
            if len(client.received) < L and client.ack >= L:
                bad["loss"] += 1
    out = {
        "trials": N,
        "exactly_once_in_order_no_loss": exact,
        "violations": bad,
        "avg_workers_per_turn": round(total_workers / N, 3),
        "max_workers_in_a_turn": max_workers,
        "total_network_resends_deduped": total_resends,
        "PHASE4_PASS": exact == N and all(v == 0 for v in bad.values()),
        "reads": "Every committed token reaches the client exactly once, in order, with no "
                 "loss, across random mid-stream worker crashes, multi-worker reroutes, and "
                 "stale-ACK re-sends. Re-sent (duplicate) tokens are deduped by the client's "
                 "seq watermark; crashes reroute to a fresh worker that resumes from the "
                 "durable log + the client ACK.",
    }
    os.makedirs("phase4/results", exist_ok=True)
    json.dump(out, open("phase4/results/stream_audit.json", "w"), indent=2)
    print(json.dumps(out, indent=2))
    return 0 if out["PHASE4_PASS"] else 1


if __name__ == "__main__":
    sys.exit(main())
