# AgentTx — results & evidence map

**AgentTx: Exactly-Once Turn Transactions for Fault-Tolerant LLM Agents.** A turn (LLM
generation + tool side-effects + conversation/KV state + client output) is one cross-layer
transaction. A **durable turn log** is the single source of truth; the **KV cache is a
materialized view** (rebuildable, fail-closed-verified). A worker can crash at ANY point and
the turn resumes elsewhere with no duplicate effects, no lost effects, no ghost observations,
and no duplicated/lost client tokens.

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
| 1 | Postgres WAL + Tool Gateway 6-class taxonomy + **real DBOS & LangGraph baselines** | real DBOS 2.25 dup / real LangGraph 1.2.6 dup vs AgentTx exactly-once (`gate1/REAL_BASELINES.md`) |
| 2 | vLLM KV-View (provenance + content-addressed CAS + checksum + fail-closed) | byte-exact `torch.equal` (48 MB); every fail-closed path; 3.37x e2e (`phase2/`) |
| 3 | 3 real tool classes (Postgres tx / FS overlay / HTTP idempotency proxy) + fail-closed | 300 crashes/class -> exactly-once; non-idempotent API -> UNCERTAIN (`phase3/`) |
| 4 | streaming exactly-once + multi-worker reroute | 20k turns 100% exactly-once, 45k re-sends deduped; real-HTTP cross-check (`phase4/`) |
| 5 | end-to-end eval | **120,900 fault injections, 0 violations**; 2 models; strong-gate scorecard (`phase5/`) |

## Headline numbers
- **120,900 fault injections, 0 correctness violations** (0 dup/lost/ghost, 0 stream violations),
  including crash-during-recovery and KV-snapshot corruption.
- Recovery via KV-as-materialized-view: **4.84x @8K, 12.5x @16K, 17x @32K** vs transcript re-prefill.
- Steady-state durability overhead: **0.7%** of a 100 ms turn.
- Coverage: 3 tool environments, **2 real baselines (DBOS, LangGraph)**, 2 models (Llama-3.1-8B,
  Qwen3-8B), fail-closed `UNCERTAIN` for non-idempotent irreversible APIs.

## Honest scope (camera-ready work)
Real SWE-bench/BFCL/Agent-Diff task-success (the agent loop uses real LLM generation/KV + a
fixed action plan; full benchmark tool environments are the remaining eval breadth); a 2nd
hardware/topology; live offload-tier per-block checksums (the byte-level CAS is proven on real
GPU KV bytes in `phase2/kvview_gpu.py`). Stack: dual A100, vLLM 0.22.1, Postgres 18.4, DBOS 2.25,
LangGraph 1.2.6.
