# AgentTx Gate-0: novelty kill-check

**Question:** does any existing system already provide turn-level atomicity across the four
state planes AgentTx targets — LLM token/KV state + workflow control + tool side-effect +
client output? If "DBOS + idempotency key + a KV checkpoint" already bundles it, AgentTx is
KILLED.

**Method:** capability matrix over the systems the advisor flagged + web verification of the
strongest baseline (DBOS) + the empirical Gate-1 evidence (which demonstrates the DBOS gap
concretely, not just from prose).

## Capability matrix

Columns = the four planes' turn-atomicity guarantees AgentTx wants.
`Tx-effect` = exactly-once for a *transactional* side effect across a crash.
`NonTx-effect` = exactly-once for a *non-transactional* side effect (FS/email/API).
`KV-recovery` = resume without re-prefilling the whole committed context.
`Stream-once` = no duplicate/lost client tokens across recovery.
`Unified turn boundary` = all four commit/recover as one atomic turn.

| system | Tx-effect | NonTx-effect | KV-recovery | Stream-once | Unified turn |
|---|---|---|---|---|---|
| LangGraph checkpointer | ✗ (state saved after step) | ✗ | ✗ (replay→re-prefill) | ✗ | ✗ |
| **DBOS durable workflow** | **✓ (step record in same Postgres tx)** | ✗ (result stored *after* effect) | ✗ (deterministic replay stores *text*, not KV) | ✗ | ✗ (LLM call is an opaque step) |
| Temporal | ✓ (via activities+idempotency) | ✗ | ✗ | ✗ | ✗ |
| Continuum / CONCUR (KV TTL, agent admission) | — | — | partial (retain KV across tool gaps) | — | ✗ (no effect semantics) |
| DéjàVu / KevlarFlow (serving fault tolerance) | — | — | ✓ (KV streaming / node-failure MTTR) | — | ✗ (no tool effects) |
| Crab / DeltaBox (sandbox checkpoint) | — | partial (sandbox COW) | ✗ | ✗ | ✗ (sandbox only, unbound from KV/effects/output) |
| TVCACHE (tool-value cache) | — | — | — | — | ✗ (post-training cache, not crash recovery) |
| LLM-42 (deterministic inference) | — | — | — | — | ✗ (determinism, not durability) |
| **AgentTx (proposed)** | ✓ | ✓ (per-turn idempotency key + atomic FS commit) | ✓ (KV as materialized view of the turn log) | ✓ ((session,turn,seq) dedup) | ✓ |

No existing row has ✓ across all five. The closest, DBOS, owns `Tx-effect` only.

## DBOS verification (the strong baseline)

Verified from DBOS docs/blog (June 2026): DBOS guarantees exactly-once **for transactional
steps** by piggybacking the step checkpoint inside the *same Postgres transaction* as the
effect; a workflow ID is an idempotency key; recovery resumes from the last completed step
via **deterministic replay** using *durably stored step results*.

Three consequences — each a gap AgentTx targets, and **Gate-1 demonstrates the first two
empirically, not just from prose:**
1. **Non-transactional effects are not atomic with their step record** (the result is stored
   *after* the effect) ⇒ a crash in the effect-done/record-not-yet window re-executes on
   recovery. **Gate-1a measured this**: faithful-DBOS DUPLICATES the non-transactional
   receipt at `post_receipt_pre_record` (40/40 trials), while exactly-once on the
   transactional charge.
2. **Recovery replays the transcript**, and the durable step result for an LLM call is its
   *text*, not its KV — so resuming a long turn re-prefills the whole committed context.
   **Gate-1b measures this** as the re-prefill vs KV-snapshot-restore latency gap.
3. **Determinism requirement**: LLM sampling is non-deterministic, so the LLM call cannot be
   a plain replayable step; and DBOS has no notion of binding KV + tool-effect + streamed
   output into one turn commit boundary.

## Verdict

**Gate-0 PASS.** No surveyed system provides the turn-atomicity bundle; the strongest
baseline (DBOS) provides exactly-once only for transactional effects and recovers by
transcript replay (re-prefill). The novelty wedge — *unify the four planes into a turn
transaction, with the KV cache demoted to a materialized view of a durable turn log* — is
open. The honest scoping (must be kept): AgentTx does **not** claim exactly-once for
arbitrary irreversible non-idempotent APIs; those are **fail-closed `UNCERTAIN`** in the
tool taxonomy. DBOS/Temporal remain strong baselines and DBOS's same-tx mechanism is reused
for the transactional-effect class.

Sources: DBOS docs/blog (durable workflows, exactly-once, deterministic replay); arXiv
Continuum 2511.02230, DéjàVu 2403.01876, KevlarFlow 2601.22438, CONCUR 2601.22705; advisor's
cited Crab/DeltaBox/Concordia/GhostServe/TVCACHE/LLM-42.
