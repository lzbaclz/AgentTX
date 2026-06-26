# Gate 0, reopened (novelty)

The original Gate 0 concluded "no system commits and recovers all four planes as one atomic turn
boundary." That framing is no longer sufficient: by mid-2026 two systems directly target
transactional tool use for agents, and several target the adjacent recovery planes. We must
confront them and re-scope the contribution. (Verified to exist via web search, 2026-06-26.)

## The competitive landscape
| system | plane it owns | what it does | relation to AgentTx |
|---|---|---|---|
| **Atomix** (arXiv [2602.14849](https://arxiv.org/abs/2602.14849), MPI-DSG) | tool-effect settlement | epoch-tagged calls, per-resource progress frontiers, commit only when progress-safe; bufferable effects delayed, externalized effects compensated on abort; fault-injected on real workloads; single-process prototype | **precedes us on "transactional tool use."** We should treat Atomix-style settlement as a *backend* for the effect plane, not reinvent it. |
| **Cordon** (arXiv [2606.17573](https://arxiv.org/abs/2606.17573)) | semantic tool transactions | lifts the unit from RPC to model-driven stateful task; intent / lineage / staged external effect / authority / audit under one commit boundary | also precedes us on "agent transactions." Another effect-plane backend. |
| **Concordia** (arXiv [2606.23521](https://arxiv.org/abs/2606.23521)) | GPU execution state | JIT-compiled persistent-kernel checkpoint/restore of KV + scheduler + comm state for fault-tolerant inference | a **KV/exec-state backend** below AgentTx, not a competitor — compose, don't compete. |
| **Crab** (arXiv [2604.28138](https://arxiv.org/abs/2604.28138)) | agent sandbox state | eBPF semantics-aware C/R aligned to turn boundaries; 8%→100% recovery correctness, ≤1.9% overhead | a **sandbox snapshot backend**. |
| **DeltaBox** (arXiv [2605.22781](https://arxiv.org/abs/2605.22781)) | agent sandbox state | ms-level incremental sandbox checkpoint/rollback | sandbox snapshot backend. |
| **DBOS / Temporal** | durable workflow | at-least-once steps + exactly-once transactions (DBOS); event-history deterministic replay (Temporal) | workflow-plane backend; also the baselines we must beat (with their *recommended* idempotency features, not naked). |

## What this does to our novelty
We **cannot** claim "first agent transaction runtime." Atomix and Cordon own that.

What remains genuinely open, and what none of the above provides as a single contract:

> **Cross-plane crash consistency**: after ANY component crashes (LLM/KV worker, tool gateway,
> coordinator, output streamer), recover the agent to a single *committed turn prefix* — so the
> tool-effect plane (Atomix/Cordon), the KV/exec plane (Concordia), the sandbox plane (Crab/DeltaBox),
> and the workflow plane (DBOS/Temporal) all agree on the same boundary, with no effect/observation
> skew, no double effect across concurrent recovery, no client-visible duplicate/lost token, and the
> KV/sandbox treated as a *verifiable, discardable* materialized view that is never the source of truth.

Each existing system hardens ONE plane and assumes the others away (Atomix/Cordon: single-process,
no distributed crash recovery; Concordia/Crab/DeltaBox: state C/R with no tool-effect or output
semantics; DBOS/Temporal: durable text workflow, no KV/output plane). AgentTx's unit of novelty is
the **contract and protocol that make them consistent across a crash**, not any one plane.

## Revised title / framing
Drop: *"Exactly-Once Turn Transactions for Fault-Tolerant LLM Agents"* (overclaims primacy + scope).

Adopt one of:
- **AgentTx: Cross-Plane Crash Consistency for Stateful LLM Agents**
- **AgentTx: Recovering Stateful LLM Agents to a Committed Turn Prefix**

## Three contributions (revised)
1. **Turn-prefix recovery contract** — a formal definition of consistent recovery across workflow,
   tool-effect, client-output and LLM-materialized-view planes (any recovered state == a complete
   committed-turn prefix; no effect/observation skew; no cross-plane divergence).
2. **Distributed recovery protocol** — owner epoch + fencing + atomic action claim + effect receipt +
   UNCERTAIN outcome + stale-worker rejection (implemented + gated under real multi-process concurrency
   in `phase7/`).
3. **Durable, verifiable materialized views** — KV / sandbox snapshots are content-addressed,
   provenance-checked, fail-closed, and *discardable* accelerators, never the semantic source of truth;
   pluggable over Concordia / Crab / DeltaBox backends.
