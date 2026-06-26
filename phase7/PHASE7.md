# Phase 7 — distributed turn-recovery protocol under REAL multi-process crashes

The gate the advisor required before any more benchmarking: not in-process single-owner schedules,
but **K real OS processes** racing to run/recover the **same** durable turn on PostgreSQL, with
**hard mid-effect process death (`os._exit`)**, stale owners, and a recovery sweep. Exactly-once is
asserted on the **real external state** (DB rows / filesystem), not on bookkeeping.

## Protocol (`agenttx/dtx.py`)
- **Positional action identity** `action_id = H(session, turn, model_output_commit_id, ordinal)`
  (shared with the Gateway via `agenttx/identity.py`). Two identical calls in one turn → different
  ordinals → both execute; the args fingerprint is a content check only (mismatch → `ContentMismatch`).
- **Atomic claim** — the action row (`action_id` PK) is `INSERT … ON CONFLICT DO NOTHING RETURNING`
  in the **same transaction** as a transactional effect: only the claim-winner runs the effect; a
  crash before COMMIT rolls back claim **and** effect → clean re-claim. Two coordinators can never
  double-execute.
- **Owner-epoch fencing** — each coordinator holds a monotonic `owner_epoch`; every action takes the
  `turn_owner` row `FOR UPDATE` and rejects a stale epoch → a resurrected coordinator cannot commit.
- **WAL is the source of truth** — `begin_turn()` persists the full plan; `recover()` reloads it from
  the DB (no in-memory plan).

## Gates (all PASS)

| gate | class | crash model | result |
|---|---|---|---|
| `concurrent_gate.py` | **TRANSACTIONAL** (Postgres `charges`) | 400 turns × 2–6 racing procs + hard mid-tx `os._exit` + sweep | **1200/1200 actions, 0 double, 0 lost**; fence fires; claim dedups; legit-duplicate ordinals both execute |
| `overlay_gate.py` | **OVERLAY** (filesystem receipts) | same, with hard death at **after-write** AND **after-publish** | **1200/1200 committed files, 0 duplicate, 0 lost, 0 tmp-promoted**; legit-duplicate ordinals both execute |
| `protocol_props.py` | — | targeted | **P1–P5 PASS** (identity, claim-dedup, fencing, rollback-then-once, WAL self-contained recovery) |

## Why OVERLAY needs a different mechanism than TRANSACTIONAL
A filesystem write cannot join the claim's DB transaction, so OVERLAY is made exactly-once by being
**idempotent**: the committed file is named by the `action_id` and published by an **atomic rename**.
Every racer and every recovery targets the *same* final path with *identical* content → at most one
committed file per logical action, re-runs are no-ops, and a crash after the temp-write (before
publish) just leaves an orphan tmp that recovery re-publishes. The DB action row is written only
**after** the file is durable, so a committed row always has its file (no ghost). (The orphan tmps
counted in the result are the footprint of real after-write crashes — harmless, never promoted.)

## Honest scope
- Distributed-concurrent hard-crash evidence now covers **TRANSACTIONAL + OVERLAY**. **IDEMPOTENT**
  (needs a durable external idempotency store under the same gate) and **COMPENSATABLE** (needs a
  concrete tool + executed crash audit; currently PROTOTYPE) are still single-owner.
- `os._exit` is a clean process kill, not a torn-sector / power-loss model; a fault-injected
  torn-write test is future work (see `docs/CLAIM_LEDGER.md`).
