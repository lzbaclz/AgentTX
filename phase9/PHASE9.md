# Phase 9 — durable output log (persist-before-send)

Upgrades the in-memory streaming prototype (phase4) to a **durable output plane**, closing the
advisor's P3 / open-issue #4: the client materializes the committed token prefix **exactly once** —
no loss, no duplicate — across real worker crashes, coordinator+streamer **co-death**, lost ACKs, and
**client restart**, by re-sending **logged** tokens (never regenerating).

## Mechanism (`agenttx/durable_stream.py`)
- **`output_log(session, turn, seq, token)`** — the committed output tokens, **fsync-durable BEFORE
  any worker streams** (persist-before-send). Any worker can therefore re-send from the log.
- **`stream_ack(session, turn, client_id, ack_seq)`** — the server's known ACK watermark (monotonic;
  may *lag* the client when ACKs are lost).
- **`client_recv(...)`** — the client's durable receipt log; the client dedups re-sends by the seq
  primary key and exposes its **contiguous** ACK watermark. A client *restart* clears this (ACK → 0).
- **Resume point** = `min(server_ack, client_ack) + 1` — always `≤ client_ack`, so a worker never
  skips a token (no loss); when `server_ack < client_ack` it re-sends the gap, which the client
  dedups; after a client restart (`client_ack = 0`) it replays the whole prefix from the durable log.
- **No regeneration** — a re-routed worker re-sends *logged* tokens; it never re-decodes (free
  generation diverges across the KV-reuse boundary, cf. phase2).

## Gate (`phase9/durable_stream_gate.py`, PASS)
300 turns × 20 tokens. Per turn: commit the output durably, then real worker **subprocesses** stream
and die hard (`os._exit`) mid-stream, with random ACK drops and client restarts, until the client has
the whole prefix.

| metric | value |
|---|---|
| turns exactly-once | **300 / 300** |
| loss / duplicate / not-completed | **0 / 0 / 0** |
| real worker deaths (`os._exit`) | 163 |
| client restarts | 24 |
| ACK drops (server watermark lags) | 134 |
| co-death turns (commit, then first worker delivered 0) | 9 |
| re-sends deduped by the client | 386 |

## Honest scope
- A **single durable store** (SQLite, `synchronous=FULL`); a replicated/distributed output log is the
  remaining step (TARGET).
- The client is modeled by a durable receipt log, not a real network endpoint; the real-socket
  reroute path is cross-checked separately in phase4.
- `os._exit` is a clean process kill, not a torn-sector/power-loss model.
