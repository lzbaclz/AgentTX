# Phase 3 — Tool Gateway: three real classes + fail-closed UNCERTAIN

Every side-effecting tool goes through the gateway, which computes a deterministic
action_key = H(session, turn, tool, args) and applies the per-class exactly-once mechanism.
Phase 3 lands the three representative classes against REAL backends (Postgres 18.4 + a real
external HTTP service) and crash-tests them, plus the honest fail-closed boundary.

| class | backend | mechanism | guarantee |
|---|---|---|---|
| TRANSACTIONAL | PostgreSQL | effect + action-key record in ONE tx | exactly-once |
| OVERLAY | filesystem | write temp → atomic rename to `committed/<key>` | exactly-once |
| IDEMPOTENT | external HTTP | action_key sent as the `Idempotency-Key` header | exactly-once (service dedups) |
| IRREVERSIBLE | external HTTP (no idempotency) | durable `prepared` before the act; crash in the act/commit window → `UNCERTAIN` | **committed-or-uncertain, never silent double** |

## Audit (`phase3/gateway_audit.py`, `phase3/results/gateway_audit.json`)
A persistent external HTTP service (`phase3/mock_service.py`: idempotent `/charge`,
non-idempotent `/send_unsafe`) + real Postgres. The gateway is crashed at a random durable
boundary every trial, then recovers; the oracle is the REAL effect counter. 300 trials/class:

- **TRANSACTIONAL (Postgres):** 0 duplicate, 0 lost → exactly-once.
- **OVERLAY (FS):** 300 receipts for 300 trials → exactly-once.
- **IDEMPOTENT (HTTP):** server charge delta = 300 → exactly-once (the action key, replayed
  on recovery, is deduped by the external service).
- **IRREVERSIBLE (non-idempotent HTTP):** **no silent double** in any trial; 65 crashes in
  the effect/commit window surfaced as **UNCERTAIN** (the gateway never auto-re-sends a
  non-idempotent irreversible effect — it fail-closes for a human/retry-policy to reconcile).

PHASE3_PASS = true.

## Why this is the honest contract
For an idempotent / transactional / overlay-able effect, exactly-once is achievable and
proven. For a truly non-idempotent irreversible external effect, NO orchestrator (DBOS,
LangGraph, or AgentTx) can guarantee exactly-once — AgentTx fail-closes it as `UNCERTAIN`
rather than silently retrying (the exhaustive model checker `agenttx/protocol.py` proves the
impossibility; this audit proves the gateway honors it on a real API).
