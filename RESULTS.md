# AgentTx — results & evidence map (Cross-Plane Crash Consistency for Stateful LLM Agents)

**AgentTx: Cross-Plane Crash Consistency for Stateful LLM Agents.** A turn (LLM generation + tool
side-effects + conversation/KV state + client output) is one cross-layer transaction. A **durable
turn log** is the single source of truth; the **KV cache is a materialized view** (rebuildable,
fail-closed-verified). Goal: after any component crashes, recover to a single **committed turn
prefix** — no duplicate/lost effects, no ghost observations, no duplicated/lost client tokens.

> ⚠️ **Every row below is graded in [`docs/CLAIM_LEDGER.md`](docs/CLAIM_LEDGER.md)** (PROVEN /
> MEASURED-PROXY / PROTOTYPE / TARGET). The death-gate/Phase-1–6 tables below are the *original*
> build log; the **graded headline** (next section) is the honest, advisor-reviewed summary.
> In particular: KV speedups are MEASURED-PROXY (vLLM CPU offload, same process), the "100k/120,900"
> are single-owner protocol-model schedules, "resumes elsewhere" is same-process — the PROVEN
> distributed result is `phase7/`. This is not "first agent transaction runtime" (Atomix/Cordon).

## Death gate (falsify-before-invest) — all PASS
| gate | question | result |
|---|---|---|
| Gate 0 | does any system bundle turn-atomicity across {LLM/KV, workflow, tool effect, output}? | **no** — DBOS owns only transactional exactly-once; KV-recovery systems ignore tool effects (`gate0/`) |
| Gate 1a | does the strong baseline have a real correctness gap? | **yes** — DBOS (faithful + **real 2.25**) duplicates non-transactional effects at the crash window (`gate1/`) |
| Gate 1b | is KV-as-materialized-view a real recovery win? | **yes** — restore vs re-prefill 12.5x@16K, 17x@32K |
| Gate 2 | does a minimal AgentTx deliver end-to-end? | **yes** — 10k crashes 0 dup/lost, 7.8x recovery, 0.7% overhead (`gate2/`) |

## System (Phases 1–5)
| phase | component | evidence |
|---|---|---|
| 1 | Postgres WAL + Tool Gateway 6-class taxonomy (4 exactly-once: PURE/IDEMPOTENT/TRANSACTIONAL/OVERLAY; 1 committed-or-compensated: COMPENSATABLE; 1 fail-closed-UNCERTAIN: IRREVERSIBLE) + **real DBOS & LangGraph baselines** | real DBOS 2.25.0 (charges 1 / receipts 2) / real LangGraph 1.2.6 (2/2 dup) vs AgentTx exactly-once verified in gate1a (40/40 both effects) + Phase 3/5 (`gate1/REAL_BASELINES.md`) |
| 2 | vLLM KV-View (provenance + content-addressed CAS + checksum + fail-closed) | byte-exact `torch.equal` (48 MB); every fail-closed path; 3.37x e2e (`phase2/`) |
| 3 | 3 real tool classes (Postgres tx / FS overlay / HTTP idempotency proxy) + fail-closed | 300 crashes/class -> exactly-once; non-idempotent API -> UNCERTAIN (`phase3/`) |
| 4 | streaming exactly-once + multi-worker reroute | 20k turns 100% exactly-once, 45k re-sends deduped; real-HTTP cross-check (`phase4/`) |
| 5 | end-to-end eval | **100,000 full-stack fault injections, 0 violations** (120,900 grand total w/ Phase 3+4); 2 models; strong-gate scorecard (`phase5/`) |
| 6 | **real tau2-bench (τ²) retail** tool environment | mid-effect crash on real `cancel_pending_order`, scored by tau2's own DB evaluator: naive **10/15 (5 double-refunds)** vs AgentTx **15/15 (0)**; full-replay honest finding: tools self-guard (85/85); LLM agent path runs end-to-end via local vLLM, 0 integration errors (`phase6/`) |

## Headline numbers — graded (see [`docs/CLAIM_LEDGER.md`](docs/CLAIM_LEDGER.md))
- **[PROVEN] Distributed turn-recovery protocol** (`phase7/`): 400 turns × 2–6 **real racing OS
  processes** + hard mid-tx `os._exit` + recovery sweep on PostgreSQL → **1200/1200 actions
  exactly-once, 0 double, 0 lost**; 5/5 protocol properties (identity, claim-dedup, fencing,
  crash-rollback, WAL-self-contained-recovery).
- **[PROVEN]** Real **DBOS 2.25.0** (charges 1 / receipts 2) and **LangGraph 1.2.6** (2/2) duplicate
  non-transactional effects; AgentTx is exactly-once. Tool taxonomy: 4 exactly-once / 1
  committed-or-compensated / 1 fail-closed-UNCERTAIN.
- **[PROVEN]** Real **τ²-bench retail**, scored by its own DB evaluator: mid-effect crash → naive
  10/15 (5 double-refunds) vs AgentTx 15/15 (0).
- **[MEASURED-PROXY]** KV recovery speedup 1.82×@4K / 12.45×@16K / 16.97×@32K (Gate-1b), 7.81×@16K
  (Gate-2b), 4.84×/3.79×@8K (Phase-5) — measured via **vLLM's own CPU-offload tier in the same
  process**, NOT AgentTx durable CAS, NOT across a real worker crash. Performance *potential* only.
  (The CAS itself is byte-exact + fail-closed — `phase2/kvview_gpu.py` — that part is PROVEN.)
- **[MEASURED-PROXY]** "100,000 / 120,900 fault injections" = **single-owner, in-process,
  protocol-model schedules** (Python-exception crashes, one SQLite, fixed output). Rigorous as a
  state-machine test; NOT whole-stack. Real-process concurrency evidence is `phase7/` (400×K procs).
- **[PROVEN]** Durable **output plane** (`phase9/durable_stream_gate.py`): persist-before-send, 300
  turns (163 worker deaths, 24 client restarts, 134 ACK drops, 9 co-deaths) → 300/300 exactly-once,
  0 loss/dup. (Upgrades the phase4 in-memory streaming prototype.)
- **[PROVEN]** **Cross-process recovery** (`phase8/xproc_recovery.py`): a fresh vLLM engine on a
  *different GPU* reconstructs the turn from the durable token log after a real `SIGKILL` — committed
  prefix byte-intact, no dup/gap. (Closes "recover elsewhere"; Gate-2b/Phase-5 were same-process.)
- **[PROVEN]** **Exhaustive model checker** (`agenttx/dmodel.py`): all 2-coordinator interleavings
  per class, 0 violations, non-vacuous (304 caught when guards removed).
- **[PROVEN]** **SOTA head-to-head** (`phase10/`, `docs/RELATED_BASELINES.md`): DBOS+idempotency ties
  AgentTx on simple effects; AgentTx differentiates on distributed/ mid-effect/ durable-output/ cross-plane.
- **[PROVEN]** **Durable KV → fresh worker's attention** (`phase11/`): a custom-configured vLLM
  connector + durable on-disk content-addressed tier lets a fresh engine on a *different GPU* load KV
  written by a `SIGKILL`ed worker — full cross-process hit, **1.45×@2K → 1.92×@8K** vs reprefill
  (grows with ctx). The KV-speed path is now durable + cross-process, not a same-process proxy.
- **[PROVEN]** **AgentTx provenance fail-closed on the CAS** (`phase11b/`, custom `agenttx_cas` tier):
  matching provenance loads (cached=2048); a mismatched provenance fails closed (cached=0 → recompute)
  even when vLLM's own block hashes match — KV under a different model/config is never silently loaded.
- Steady-state durability overhead: **0.70 ms/turn (~0.7%)** (Gate-2c).

## Honest scope — what is NOT done (TARGET)
- ~~Durable KV restore into a fresh vLLM worker's attention~~ **DONE (`phase11/`)**: a custom-configured
  vLLM connector (`OffloadingConnector` + `TieringOffloadingSpec` + `fs_python` durable CAS) loads KV
  from an on-disk content-addressed store written by a `SIGKILL`ed worker into a FRESH engine's
  attention on a DIFFERENT GPU — full cross-process hit, **1.45×@2K → 1.92×@8K** (grows with ctx).
  (Required pinning `PYTHONHASHSEED` — vLLM block hashes are `os.urandom`-seeded otherwise.)
  **AgentTx provenance fail-closed is also DONE** (`phase11b`, `agenttx_cas` tier): a mismatched
  provenance (model/dtype/RoPE/adapter) fails closed → recompute, so KV under a different config is
  never silently loaded. Remaining polish: turn-LSN-addressed manifests.
- **Run** Temporal / Atomix / Cordon under our exact fault harness (their rows in the SOTA capability
  matrix are from their papers; DBOS{naked,+idempotency,+outbox} + LangGraph + AgentTx are measured).
- **High agent-task-success** on τ²-bench with a stronger model (live FT exactly-once is proven in
  `phase8/tau2_live_ft.py`; raw task reward is bounded by the local 8B model).
- A 2nd hardware/topology. Stack: dual A100, vLLM 0.22.1, Postgres 18.4, DBOS 2.25.0, LangGraph 1.2.6.

This is **not** "first agent transaction runtime" (Atomix/Cordon precede us); positioning =
cross-plane crash-consistent recovery to a committed turn prefix ([`docs/GATE0_REOPEN.md`](docs/GATE0_REOPEN.md)).
