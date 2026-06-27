# AgentTx: Cross-Plane Crash Consistency for Stateful LLM Agents
### Internal design draft (not a submission draft) — revised per the 2026-06-26 advisor review

## Abstract
A modern LLM-agent *turn* mutates four state planes that today crash-recover **separately**: the
LLM/KV state, the workflow control state, the external tool side-effects, and the client-visible
output stream. When any component fails, these planes desynchronize — a tool effect with no recorded
observation, a duplicated charge on retry, a client token the server forgot, a KV cache that belongs
to a different turn. AgentTx defines and enforces a single **turn-prefix recovery contract**: after
any crash, every plane recovers to the *same committed turn prefix*. We contribute (1) the contract,
(2) a **distributed recovery protocol** — owner-epoch fencing + atomic action claim + WAL-as-source-
of-truth — that is exactly-once under real multi-process concurrent recovery, and (3) **durable,
verifiable materialized views**: KV/sandbox snapshots that are content-addressed, fail-closed, and
*discardable* accelerators, never the source of truth. We compose, rather than compete with,
per-plane systems (DBOS/Temporal for workflow; Atomix/Cordon for effect settlement; Concordia for
GPU state; Crab/DeltaBox for sandboxes).

## 1. Problem
| plane | durability owner today | failure mode |
|---|---|---|
| LLM / KV | serving KV-recovery / GPU C-R (Concordia) | recovers KV but ignores tool effects |
| workflow | DBOS / Temporal / LangGraph | at-least-once steps; recovers *text*, not KV |
| tool effect | the orchestrator, partially | duplicate / lost effect at the crash window |
| client output | usually nobody | duplicated or lost tokens after reconnect |

**Measured gap (real systems, this repo):** real DBOS 2.25 makes a *transactional* effect exactly-once
but a naked `@DBOS.step` non-transactional effect duplicates on recovery (charges 1 / receipts 2);
real LangGraph 1.2.6 + PostgresSaver duplicates both (2/2). A *careful* DBOS user reaches exactly-once
with idempotent effects (measured 1/1) — so naked DBOS is a failure example, not the bar. The bar is
that **no system makes all four planes recover to one boundary, and none survives distributed
concurrent recovery.**

## 2. The turn-prefix recovery contract
Let a session be a sequence of committed turns `T0, T1, …`. Define `RecoveredState ≡ Prefix(T0…Tk)`:
any state reachable after recovery must equal the state after some complete committed-turn prefix.
Concretely, no reachable post-recovery state may exhibit:
- an effect with no committed observation, or an observation with no committed effect (**no skew**);
- the same logical action applied twice (**no double**) or a committed action missing (**no loss**);
- a client-visible token the server cannot reproduce (**output ⊆ durable log**);
- a KV view bound to a different model/turn/config used as truth (**views are fail-closed**).

## 3. Design
**3.1 Action identity & atomic claim.** Each action has identity `H(session, turn,
model_output_commit_id, ordinal)` — *not* a tool+args hash (so two legitimate identical calls both
run). Exactly-once is enforced by an **atomic claim**: the action row (PK = action_id) is inserted
`ON CONFLICT DO NOTHING RETURNING` in the *same transaction* as the effect; only the claim winner
runs the effect; a crash before commit rolls back both.

**3.2 Fencing.** Each coordinator holds a monotone `owner_epoch` lease; every action takes the turn-
owner row `FOR UPDATE` and rejects stale epochs — a resurrected coordinator cannot commit.

**3.3 WAL = source of truth.** The turn's plan (ordinals, canonical args, class), the committed model
output, and the results are persisted; recovery reloads the plan from the WAL with no in-memory state.

**3.4 Tool-class taxonomy (honest about what is achievable).** PURE/IDEMPOTENT/TRANSACTIONAL/OVERLAY →
exactly-once via per-class mechanism; COMPENSATABLE → committed-or-compensated (states prepared →
effect_started → committed; only the ambiguous middle compensates, idempotently); IRREVERSIBLE →
fail-closed `UNCERTAIN`, never a silent re-send.

**3.5 Durable materialized views.** KV snapshots are content-addressed (sha256 = address = checksum)
+ provenance-fingerprinted; restore is fail-closed on any mismatch and *discardable* — the durable
token log is teacher-forced on recovery regardless, so the KV only changes recovery *speed*.

## 4. Evaluation — status (graded; see `docs/CLAIM_LEDGER.md`)
**PROVEN**
- Distributed protocol: 400 turns × 2–6 real racing OS processes + hard mid-tx `os._exit` + recovery
  sweep on PostgreSQL → 1200/1200 actions, 0 double, 0 lost (`phase7`). Exhaustive model checker:
  all 2-coordinator interleavings/class, 0 violations, non-vacuous (304 caught when guards removed).
- Real DBOS/LangGraph failure window + DBOS-with-idempotency strong baseline (`phase10`).
- τ²-bench retail mid-effect crash, scored by its own DB evaluator: naive 10/15 (5 double-refunds)
  vs AgentTx 15/15 (`phase6`). Gateway runs inside the LIVE tau2 orchestrator with real fault
  injection → 0 double (`phase8/tau2_live_ft`).
- Durable output plane: persist-before-send, exactly-once across worker death + client restart
  (`phase9`). Durable KV CAS survives `SIGKILL`, byte-exact + fail-closed reload in a fresh process
  (`phase8/kv_durable`). Cross-process recovery from the durable token log on a fresh vLLM engine
  (`phase8/xproc_recovery`).

**MEASURED-PROXY / TARGET**
- KV recovery speedups are via vLLM's in-process CPU-offload tier (proxy), not yet AgentTx's durable
  CAS injected into a fresh engine's attention (TARGET). Full Temporal/Atomix/Cordon runs under our
  harness, a 2nd hardware platform, and high agent-task-success with a stronger model are TARGET.

**Planned eval matrix (camera-ready):** workloads τ²-bench (retail/airline/telecom), SWE-bench
(overlay/sandbox), a SQL-agent; baselines none/checkpoint, DBOS{+idempotency,+outbox}, Temporal,
LangGraph, Atomix, Cordon, AgentTx{−KV,full}; faults kill worker/coordinator/gateway, ACK loss,
network-after-success, KV corruption, recovery-crash, ≥2 coordinators; metrics duplicate/lost/ghost/
client-divergence/task-success + JCT overhead, recovery latency, replayed tokens, snapshot bytes.

## 5. Related work (reopened novelty — `docs/GATE0_REOPEN.md`)
Atomix (2602.14849) and Cordon (2606.17573) precede us on transactional/semantic tool use (single-
process); Concordia (2606.23521) checkpoints GPU exec state; Crab (2604.28138)/DeltaBox (2605.22781)
checkpoint sandboxes; DBOS/Temporal own durable workflow. **AgentTx is not "the first agent
transaction runtime."** Its unit of novelty is the cross-plane crash-consistent recovery *contract +
distributed protocol* that makes those per-plane systems agree on one turn boundary.

## 6. Limitations (honest)
Single node (dual-A100); KV-into-fresh-attention injection unimplemented; effect-plane settlement is
simpler than Atomix/Cordon (we compose them); agent-task-success bounded by the local model;
COMPENSATABLE/IRREVERSIBLE concurrent hardening proven by model checker, not yet at scale on real
external APIs.
