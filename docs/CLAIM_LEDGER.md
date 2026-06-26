# Claim Ledger

Every claim in this repo, graded against its evidence. Written after an adversarial review
(advisor, 2026-06-26) that correctly flagged earlier overclaims. Grades:

- **PROVEN** — directly demonstrated with rigorous, reproducible evidence in this repo.
- **MEASURED-PROXY** — a real measurement, but through a *proxy/stand-in*, not the production path.
- **PROTOTYPE** — implemented and tested, but at single-owner / small scale or with an in-memory part.
- **TARGET** — designed and/or claimed, **not yet** built or proven. Do not cite as a result.

## PROVEN
| claim | evidence |
|---|---|
| Real **DBOS 2.25.0** and **LangGraph 1.2.6** do NOT make non-transactional effects exactly-once (DBOS charges 1 / receipts 2; LangGraph 2/2) | `gate1/REAL_BASELINES.md`, `gate1/results/real_*.json` |
| **Distributed turn-recovery protocol**: action-ordinal identity, atomic claim (`action_id` PK + `ON CONFLICT DO NOTHING` in the effect tx), owner-epoch fencing, WAL-as-source-of-truth — exactly-once under REAL multi-process concurrency + hard mid-tx `os._exit` + recovery sweep | `phase7/concurrent_gate.py` (400 turns × 2–6 racing OS processes → 1200/1200 actions, 0 double, 0 lost), `phase7/protocol_props.py` (P1–P5) |
| Tool Gateway taxonomy (4 exactly-once / 1 committed-or-compensated / 1 fail-closed-UNCERTAIN); single-owner crash audit | `phase3/` (300 crashes/class), `gate1/failure_audit.py` |
| **COMPENSATABLE** only compensates the ambiguous `effect_started` state; `prepared` (effect not begun) re-runs cleanly | `agenttx/gateway.py`, verified crash@{prepared→re-run, effect_started→compensate, committed→dedup} |
| KV-View **content-addressed CAS is byte-exact** (`torch.equal`, 48 MB) and **fail-closed** on corruption / provenance mismatch | `phase2/kvview_gpu.py`, `phase2/results/kvview_gpu.json` |
| On the **real τ²-bench retail** benchmark, scored by its own DB evaluator, a mid-effect crash makes naive recovery double-refund (10/15) while AgentTx's transactional wrap is exactly-once (15/15) | `phase6/tau2_midcrash.py` |

## MEASURED-PROXY (downgraded from earlier overclaims)
| claim | what's actually true |
|---|---|
| "KV recovery 1.82×/12.45×/16.97× (Gate-1b), 7.81× (Gate-2b), 4.84×/3.79× (Phase-5)" | Real TTFT measurements, but the KV is restored by **vLLM's own `CPUOffloadingSpec`** in the **same process**, NOT by AgentTx's durable CAS, and NOT across a real worker crash. It measures the *performance potential* of KV-as-view; the durable AgentTx integration is a TARGET (phase8). |
| "100,000 full-stack fault injections" | **Single-owner, in-process protocol-model schedules**: Python-exception crashes, one SQLite file, the client object persists in memory across recovery, output is a fixed 6-string function, and `kv.restore`'s result does not affect the streamed output. Rigorous as a state-machine randomized test; **not** whole-stack. Real-process cross-checks are smaller (Gate-2a xcheck 500; Phase-7 400×K processes). |
| Gate-2b / Phase-5 "recover elsewhere / worker crash" | **Same-process** recovery path (`del coord`); the vLLM engine, CUDA context and CPU-offload tier survive. Not a worker crash. |

## PROTOTYPE
| claim | scope |
|---|---|
| Streaming exactly-once + multi-worker reroute | **output sequence/dedup protocol prototype**: `StreamLog` is an in-memory list and the client ACK is an in-memory field. The 20k audit + real-HTTP cross-check prove reconnect + seq-dedup ("exactly-once-visible output for a deduplicating client"), NOT persist-before-send, durable output log, or coordinator+stream-worker co-death recovery (TARGET = phase9). |
| Tool Gateway non-transactional classes (OVERLAY/IDEMPOTENT/IRREVERSIBLE) | proven exactly-once / fail-closed at **single owner**; concurrent-recovery hardening done for TRANSACTIONAL (phase7), pending for the others. |

## DOWN-PAYMENT (partially closed in phase8; rest still TARGET)
- **Gateway inside the LIVE orchestrator** — DONE at the mechanism level: `phase8/tau2_live_ft.py` monkeypatches the AgentTx transactional wrap into tau2's `Environment.get_response`, driven by a **real live Qwen agent**; a real mid-turn crash on the agent's own money-moving call → **0 double-applied (4/4 tasks)**. Still TARGET: high task-success (stronger model), all classes live.
- **Durable cross-process KV** — durability+integrity DONE: `phase8/kv_durable.py` `SIGKILL`s the producer; a **fresh process** reloads 32 MB **byte-exact + fail-closed** from the durable CAS (survives the crash vLLM's in-process CPU-offload tier does not). Still TARGET: injecting bytes into a **new vLLM engine's attention** to resume decoding.

## TARGET (not yet built/proven — never cite as a result)
- Restore durable KV **into a fresh vLLM worker's attention** so decoding resumes (the 4.84–17× speedups do NOT yet ride on this path).
- Durable output log (persist-before-send) + stream-worker/coordinator co-death + client restart.
- Full SOTA matrix: DBOS + workflow-id / + idempotency key / + transactional outbox; Temporal; Atomix; Cordon.
- Concurrent-recovery hardening for the NON-transactional classes + a TLA+ model for COMPENSATABLE/IRREVERSIBLE.
- A second hardware platform / GPU topology.

## Positioning
The headline is NOT "first agent transaction runtime" — **Atomix** (arXiv 2602.14849) and **Cordon**
(2606.17573) precede us on effect-level transactional tool use. See `docs/GATE0_REOPEN.md` for the
reopened novelty analysis and the revised positioning (cross-plane crash-consistent recovery to a
committed turn prefix; competitors as composable backends).
