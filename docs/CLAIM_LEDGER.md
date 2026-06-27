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
| Real **DBOS 2.25.0** and **LangGraph 1.2.6** in their *naked* config do NOT make non-transactional effects exactly-once (DBOS charges 1 / receipts 2; LangGraph 2/2) | `gate1/REAL_BASELINES.md`, `gate1/results/real_*.json` |
| DBOS's **recommended** configs (deterministic idempotency key; transactional outbox + idempotent relay) **CLOSE** the non-transactional gap (receipts 1/1) under the same crash — so the gap is a default-config artifact, not fundamental; AgentTx's contribution is the **automatic per-class taxonomy + cross-plane binding**, not out-doing DBOS on one effect | `adapters/dbos_recommended.py`, `gate1/results/dbos_recommended.json` |
| **Distributed turn-recovery protocol** (TRANSACTIONAL class): action-ordinal identity, atomic claim (`action_id` PK + `ON CONFLICT DO NOTHING` in the effect tx), owner-epoch fencing, WAL-as-source-of-truth — exactly-once under REAL multi-process concurrency + hard mid-tx `os._exit` + recovery sweep | `phase7/concurrent_gate.py` (400 turns × 2–6 racing OS processes → 1200/1200 actions, 0 double, 0 lost), `phase7/protocol_props.py` (P1–P5) |
| **Distributed OVERLAY class** (NON-transactional filesystem effect): exactly-once under REAL multi-process concurrency + hard mid-effect `os._exit` (after-write AND after-publish) + recovery sweep, via idempotent action-id-named atomic-rename publish | `phase7/overlay_gate.py` (400 turns × 2–6 racing OS processes → 1200/1200 committed files, 0 duplicate, 0 lost, 0 tmp-promoted, legit-duplicate ordinals both execute) |
| Tool Gateway: **TRANSACTIONAL exactly-once**, **OVERLAY/IDEMPOTENT effectively-once**, **IRREVERSIBLE fail-closed-UNCERTAIN** under single-owner crash audit (300 crashes/class) | `phase3/gateway_audit.py`, `phase3/results/gateway_audit.json` |
| **Unified positional action identity** — `action_id = H(session, turn, commit_id, ordinal)` (full sha256), shared by the single-owner Gateway and the distributed `dtx.py`. Two identical-args calls at different ordinals BOTH execute (no silent lost effect); a replayed action whose args fingerprint changed FAILS CLOSED (`ContentMismatch`) | `agenttx/identity.py`, `phase3/identity_guard.py` (G1–G4 PASS) |
| KV-View **content-addressed CAS is byte-exact** (`torch.equal`, 48 MB) and **fail-closed** on corruption / provenance mismatch | `phase2/kvview_gpu.py`, `phase2/results/kvview_gpu.json` |
| **Durable output log (persist-before-send)**: the client materializes the committed token prefix exactly once — no loss, no duplicate — across REAL worker `os._exit`, coordinator+streamer co-death, lost ACKs, and client restart, by re-sending LOGGED tokens (never regenerating) | `agenttx/durable_stream.py`, `phase9/durable_stream_gate.py` (300 turns: 163 worker deaths, 24 client restarts, 134 ACK drops, 9 co-deaths → 300/300 exactly-once, 0 loss/dup, 386 resends deduped) |
| On the **real τ²-bench retail** benchmark, scored by its own DB evaluator, a mid-effect crash makes naive recovery double-refund (10/15) while AgentTx's transactional wrap is exactly-once (15/15) | `phase6/tau2_midcrash.py` |
| **Exhaustive distributed-protocol model checker**: every 2-coordinator interleaving (acquire/do/crash/recover) per tool class — 0 invariant violations (no double / no stale-owner effect / irreversible-never-doubles / committed⇒effect); **non-vacuous** (304 violations caught when the fence+claim guards are removed) | `agenttx/dmodel.py`, `phase7/results/model_check.json` |
| **Cross-process recovery correctness**: a FRESH vLLM engine on a **different GPU/worker** reconstructs the turn from the durable token log alone after the producer is hard-`SIGKILL`ed — committed prefix byte-intact, no dup/gap (closes "recover elsewhere", which Gate-2b/Phase-5 only did same-process) | `phase8/xproc_recovery.py` |
| **SOTA head-to-head**: same crash workload across real none/DBOS-naked(2)/DBOS-recommended(1)/LangGraph(2/2)/AgentTx(1) + a capability matrix — DBOS+idempotency TIES AgentTx on simple effects; AgentTx differentiates on distributed concurrent recovery, mid-effect non-atomic, durable output, and the cross-plane contract | `phase10/sota_matrix.py`, `docs/RELATED_BASELINES.md` |
| **Durable KV → a FRESH worker's ATTENTION**: a custom-configured vLLM connector (`OffloadingConnector` + `TieringOffloadingSpec` + `fs_python` durable content-addressed tier) lets a fresh vLLM engine on a DIFFERENT GPU load KV from the on-disk CAS written by a `SIGKILL`ed worker — full cross-process hit (warm `num_cached_tokens`=ctx, cold=0), valid output, **1.45×@2K → 1.92×@8K** faster than reprefill (grows with ctx). KEY: pin `PYTHONHASHSEED` (vLLM seeds block hashes with `os.urandom` otherwise → no cross-process match) | `phase11/kv_cas_xproc.py`, `phase11/results/kv_cas_xproc_*.json` |

## MEASURED-PROXY (downgraded from earlier overclaims)
| claim | what's actually true |
|---|---|
| "KV recovery 1.82×/12.45×/16.97× (Gate-1b), 7.81× (Gate-2b), 4.84×/3.79× (Phase-5)" | Real TTFT measurements, but the KV is restored by **vLLM's own `CPUOffloadingSpec`** in the **same process**. These specific numbers are same-process proxies. NOTE: the durable cross-process path is now separately PROVEN in `phase11` (a fresh engine on another GPU loading from the durable CAS, 1.45×@2K → 1.92×@8K) — these older same-process numbers are kept as the in-process performance-potential reference. |
| "100,000 full-stack fault injections" | **Single-owner, in-process protocol-model schedules**: Python-exception crashes, one SQLite file, the client object persists in memory across recovery, output is a fixed 6-string function, and `kv.restore`'s result does not affect the streamed output. Rigorous as a state-machine randomized test; **not** whole-stack. Real-process cross-checks are smaller (Gate-2a xcheck 500; Phase-7 400×K processes). |
| Gate-2b / Phase-5 "recover elsewhere / worker crash" | Those specific runs are **same-process** (`del coord`). NOTE: genuine cross-process/cross-device recovery is now separately PROVEN in `phase8/xproc_recovery.py` (fresh engine on another GPU reconstructs from the durable token log after a real `SIGKILL`); the Gate-2b *KV-speed* number remains same-process MEASURED-PROXY. |

## PROTOTYPE
| claim | scope |
|---|---|
| Streaming exactly-once + multi-worker reroute | The original phase4 `StreamLog`/ACK are in-memory; **the durable version now exists** (`agenttx/durable_stream.py`, phase9 PROVEN above). Remaining prototype edges: phase9 uses a single SQLite durable store (not yet a replicated/distributed log), and the client is modeled by a durable receipt log rather than a real network endpoint. |
| Tool Gateway IDEMPOTENT / IRREVERSIBLE classes | proven effectively-once / fail-closed at **single owner** (phase3, 300 crashes/class); concurrent-recovery hardening done for TRANSACTIONAL + OVERLAY (phase7), **pending for IDEMPOTENT**. |
| **COMPENSATABLE** saga (`prepared → effect_started → committed`; only the ambiguous `effect_started` compensates, `prepared` re-runs cleanly) | **logic in `agenttx/gateway.py` only** — there is no concrete compensatable tool and no executed crash matrix yet. **Downgraded from PROVEN** (advisor): a "verified" grade requires an executed test, which this class does not have. |

## DOWN-PAYMENT (partially closed in phase8; rest still TARGET)
- **Gateway inside the LIVE orchestrator** — DONE at the mechanism level: `phase8/tau2_live_ft.py` monkeypatches the AgentTx transactional wrap into tau2's `Environment.get_response`, driven by a **real live Qwen agent**; a real mid-turn crash on the agent's own money-moving call → **0 double-applied (4/4 tasks)**. Still TARGET: high task-success (stronger model), all classes live.
- **Durable cross-process KV** — durability+integrity DONE: `phase8/kv_durable.py` `SIGKILL`s the producer; a **fresh process** reloads 32 MB **byte-exact + fail-closed** from the durable CAS (survives the crash vLLM's in-process CPU-offload tier does not). Still TARGET: injecting bytes into a **new vLLM engine's attention** to resume decoding.

## TARGET (not yet built/proven — never cite as a result)
- ~~Restore durable KV into a fresh vLLM worker's attention~~ **DONE — `phase11` (durable CAS tier; cross-process hit into attention, 1.45×@2K → 1.92×@8K).** Remaining polish: AgentTx provenance (model/dtype/RoPE/LoRA) fail-closed verification *on top of* the CAS, and turn-LSN-addressed manifests (the CAS path already namespaces by model dir).
- Replicated/distributed durable output log (phase9 proves persist-before-send + co-death + restart on a single durable store; a multi-node replicated log is the remaining step).
- Full SOTA matrix: **DBOS + idempotency-key / + transactional-outbox now measured** (both close the single-plane gap, `adapters/dbos_recommended.py`); **Temporal, Atomix, Cordon still TARGET** (qualitative/code-characterized for now).
- Concurrent-recovery hardening for the **IDEMPOTENT** class (TRANSACTIONAL + OVERLAY done in phase7) + a TLA+ model for COMPENSATABLE/IRREVERSIBLE.
- A second hardware platform / GPU topology.

## Positioning
The headline is NOT "first agent transaction runtime" — **Atomix** (arXiv 2602.14849) and **Cordon**
(2606.17573) precede us on effect-level transactional tool use. See `docs/GATE0_REOPEN.md` for the
reopened novelty analysis and the revised positioning (cross-plane crash-consistent recovery to a
committed turn prefix; competitors as composable backends).
