# Phase 4 — streaming exactly-once + multi-worker reroute

A turn's committed output tokens live in a durable log (the source of truth, Phase 1/2). A
**stateless** worker streams them tagged `(session, turn, seq)`; the client keeps the highest
CONTIGUOUS seq it has received (its **ACK watermark**) and dedups by seq. On a worker crash,
**another worker** resumes from the server-known ACK (which may lag the client's real ACK →
harmless re-sends the client dedups). The client therefore materializes every committed token
**exactly once, in order, with no loss**, across any number of mid-stream crashes, reroutes,
and re-sends. (`agenttx/stream.py`.)

## Protocol audit (`phase4/stream_audit.py`, `phase4/results/stream_audit.json`)
20,000 turns; each streamed while crashing the worker at a random mid-stream boundary,
rerouting to a fresh worker that resumes from a possibly-stale ACK, until completion:

- **exactly-once, in order, no loss: 20000 / 20000** (0 loss, 0 duplicate-in-received,
  0 out-of-order, 0 not-completed).
- multi-worker reroute exercised: **avg 2.66 workers/turn, max 10**.
- **45,531** network re-sends (stale-ACK) **deduped** by the client's seq watermark.

## Real-socket cross-check (`phase4/stream_realsocket.py`, `phase4/results/stream_realsocket.json`)
60 tokens streamed over **real HTTP** (ndjson, flushed per token); the worker process is
**killed twice mid-stream** and a fresh worker is started each time (3 workers used). The
client reconnects with its ACK and resumes → **received 60/60, exactly-once, in order**. The
protocol survives real connection drops + real worker reroutes; the heavy re-send-dedup path
is covered by the 20k in-process audit.

PHASE4_PASS = true (protocol) and PHASE4_REALSOCKET_PASS = true (real HTTP).

## Contract
Combined with the durable turn log (Phase 1) and the Tool Gateway (Phase 3), a worker can
crash at ANY point and the turn resumes on another worker with: tools exactly-once (gateway),
KV restored-or-recomputed (KV-View, Phase 2), and **client output exactly-once** (this phase)
— no duplicate charges, no lost effects, no duplicated or lost client tokens.
