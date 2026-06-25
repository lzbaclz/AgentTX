# AgentTx death-gate — VERDICT: GO

The cheap, decisive falsify-before-invest gate (the methodology that kept the prior project
honest). Three gates, all PASS.

## Gate 0 — novelty kill-check: PASS
No surveyed system provides turn-level atomicity across {LLM token/KV, workflow, tool
effect, client output}. The strongest baseline, **DBOS**, provides exactly-once **only for
transactional effects** (step checkpoint piggybacked in the same Postgres tx) and recovers
by **deterministic transcript replay** (stores step *text*, not KV). KV-recovery serving
systems (DéjàVu, KevlarFlow, Continuum, CONCUR) recover/reuse KV but ignore tool-effect
exactly-once; sandbox checkpointers (Crab, DeltaBox) save sandbox state unbound from
KV/effects/output. The bundle is open. (`gate0/GATE0_NOVELTY.md`, web-verified.)

## Gate 1a — failure-window correctness audit: PASS
Real process crashes (os._exit in a subprocess) at named points around two real side effects
(SQLite charge = transactional; receipt file = non-transactional), recovered, oracle counts
duplicate/lost effects on the real DB/FS state. 40 trials/cell.

|             | post_charge_pre_record (Tx) | post_receipt_pre_record (NonTx) |
|-------------|:---:|:---:|
| none        | DUP | DUP |
| checkpoint  | DUP | DUP |
| **dbos (faithful)** | **OK** | **DUP** ← the gap |
| **agenttx** | OK | OK |

The standard strong baseline (DBOS) gets transactional effects exactly-once but
**double-executes non-transactional effects** at the effect-done/record-not-yet window.
AgentTx's per-turn idempotency key + content-addressed atomic FS commit closes it.
(`gate1/failure_audit.py`, `gate1/results/gate1a_failure_audit.json`.)

## Gate 1b — recovery-cost gap: PASS
KV-snapshot restore (KV as a materialized view; restored from the CPU offload tier) vs
full transcript-replay re-prefill (how DBOS/LangGraph resume), resume-latency at 4K/16K/32K
on Llama-3.1-8B:

| ctx | re-prefill | KV restore | speedup |
|---|---|---|---|
| 4K | 308 ms | 169 ms | 1.8× |
| 16K | 1521 ms | 122 ms | 12.5× |
| 32K | 3830 ms | 226 ms | **17.0×** |

The win grows with context (re-prefill ~quadratic, restore linear-bandwidth) and is an
order of magnitude exactly where recovery cost matters. (`gate1/recovery_cost.py`,
`gate1/results/gate1b_recovery_cost.json`.)

## Decision: GO — commit to AgentTx

All three gates pass: a real, unfilled correctness gap (Gate 1a) + a real recovery-cost gap
(Gate 1b) that the strong baselines (DBOS/LangGraph) do not close, in a problem no system
currently bundles (Gate 0). KVQ / KV-movement line is **frozen** (tags `kvq-rx-kill-artifact`,
`no-free-move-artifact`, `peerkv-active-artifact` on the PeerKV repo).

### Honest scope (carried into the paper, fail-closed)
- "DBOS" here is a faithful re-impl of its documented same-tx-step-record + idempotency
  mechanism; **real-DBOS / real-LangGraph validation is the first Phase-1 task**.
- AgentTx does **not** claim exactly-once for arbitrary irreversible non-idempotent APIs —
  those are **fail-closed `UNCERTAIN`** in the tool taxonomy.
- Gate-1a uses a scripted agent (deterministic, to isolate orchestration crash-safety from
  LLM nondeterminism); a real-LLM end-to-end audit is Phase-2.
- Gate-1b restore is from the CPU offload tier (a durable store); SSD/remote snapshot +
  provenance/fail-closed verification is Phase-2 KV-View work.

### Next (Phase 1, the transaction core)
Turn WAL + state machine + invariants (TLA+/model-checker) + Tool Gateway (SQL/FS/HTTP) +
recovery coordinator + LangGraph/DBOS adapters as real baselines. Then Phase-2 vLLM KV-View
(turn_lsn → block manifest, content-addressed + checksum + fail-closed provenance).
