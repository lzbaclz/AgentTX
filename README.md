# AgentTx — Exactly-Once Turn Transactions for Fault-Tolerant LLM Agents

**A single agent *turn* — the LLM generation, the conversation/KV state, the tool side-effects,
and the streamed client output — is one cross-layer transaction.** A **durable turn log** is the
single source of truth; the **KV cache is a materialized view** of that log (rebuildable,
fail-closed-verified). A worker can crash at **any** point and the turn resumes elsewhere with
**no duplicate effects, no lost effects, no ghost observations, and no duplicated or lost client
tokens**.

> **120,900 fault injections, 0 correctness violations.** Recovery via KV-as-materialized-view is
> **4.84× @8K, 12.5× @16K, 17× @32K** faster than transcript re-prefill, at **0.7%** steady-state
> durability overhead.

---

## The problem

A modern LLM-agent turn touches four independent state planes that crash-recover *separately*:

| plane | what it is | who owns durability today |
|---|---|---|
| **LLM / KV state** | the attention KV cache for the committed context | serving-layer KV-recovery systems (DéjàVu, KevlarFlow, Continuum) |
| **workflow control** | which step of the turn we are on | durable-workflow engines (DBOS, Temporal, LangGraph) |
| **tool side-effect** | the DB write, the file, the HTTP `POST`, the email | the tool/orchestrator — *partially* |
| **client output** | the streamed tokens the user already saw | nobody, typically |

No existing system commits and recovers all four as **one atomic turn boundary**. The strongest
baseline, **DBOS**, gives exactly-once **only for transactional effects** (the step checkpoint
piggybacks the *same* Postgres transaction as the effect) and recovers by **deterministic
transcript replay** — it stores step *text*, not KV. The consequences are two concrete, *measured*
gaps:

1. **Non-transactional effects re-execute.** The step result is recorded *after* the effect, so a
   crash in the effect-done / record-not-yet window double-executes on recovery. Measured: real
   **DBOS 2.25** duplicates a non-transactional receipt (and real **LangGraph 1.2.6 +
   PostgresSaver** duplicates *both* effects).
2. **Recovery re-prefills the whole context.** Replaying the transcript re-runs the LLM over the
   entire committed context — quadratic in context length, exactly where recovery cost hurts.

AgentTx closes both for the tool classes where exactly-once is *achievable*, and is **honest** —
fail-closed `UNCERTAIN` — about the one class where no orchestrator can guarantee it.

## The idea

- **The durable turn log is the single source of truth.** Every turn appends an ordered WAL of
  typed records (`BEGIN_TURN → ACTION_PREPARED → ACTION_COMMITTED → OBSERVATION_COMMITTED →
  TURN_COMMITTED`) to Postgres/SQLite. Recovery re-runs the turn against this log.
- **Every side-effect is keyed and deduped.** The Tool Gateway computes a deterministic
  `action_key = H(session, turn, tool, canonical_args)` and applies a *per-class* exactly-once
  mechanism (below). Re-running a turn on recovery can never re-fire a committed effect.
- **The KV cache is a materialized view.** A turn's KV snapshot binds a **provenance fingerprint**
  + a **content-addressed block manifest**; restore verifies both and **fails closed** (recompute
  from the durable token log) on any mismatch. A lost snapshot costs only *speed*; a corrupt or
  stale snapshot can *never* produce wrong output.
- **Output is streamed exactly-once.** Tokens are tagged `(session, turn, seq)`; the client keeps
  an ACK watermark and dedups, so a mid-stream crash + reroute to another worker re-sends harmless
  duplicates the client drops — every committed token materializes once, in order, no loss.

### Turn invariants (exhaustively model-checked)

[`agenttx/protocol.py`](agenttx/protocol.py) enumerates a crash after **every** prefix of a turn's
operations, recovers, and asserts:

- **I1 — Action Uniqueness:** an external effect fires at most once.
- **I2 — No Ghost Observation:** an observation is recorded only *after* its effect committed.
- **I3 — No Lost Effect:** a committed effect is eventually observed after recovery.
- **I5 — Prefix-Consistent Recovery:** a finished turn ends in `TURN_COMMITTED`.

The checker proves the guaranteed classes (transactional + idempotent-overlay) hold for every crash
point, and that a *raw non-idempotent* effect **cannot** be made exactly-once by any orchestrator —
which is exactly why that class is fail-closed.

## The Tool-Gateway taxonomy

Every side-effecting tool declares its class; the gateway enforces the matching mechanism
([`agenttx/gateway.py`](agenttx/gateway.py)):

| class | guarantee | mechanism |
|---|---|---|
| `PURE` | exactly-once (= cache) | read-only; cache result by key |
| `IDEMPOTENT` | exactly-once | pass the action key as the external **Idempotency-Key** header |
| `TRANSACTIONAL` | exactly-once | effect **+** action-key record committed in **one** DB transaction |
| `OVERLAY` | exactly-once | write temp → **atomic rename** to `committed/<key>` (idempotent: same action key → one file) |
| `COMPENSATABLE` | committed-or-compensated | saga: durable `prepared` → execute → `committed`; recovery of a `prepared`-but-uncommitted action compensates (undo) |
| `IRREVERSIBLE` | **fail-closed `UNCERTAIN`** | durable `prepared` before the act; crash in the act/commit window → `UNCERTAIN`, **never** a silent re-send |

> **The honest boundary.** For a truly non-idempotent irreversible external effect (wire money,
> send an email once), *no* orchestrator — DBOS, LangGraph, or AgentTx — can guarantee exactly-once.
> AgentTx fail-closes it as `UNCERTAIN` for a human/retry-policy to reconcile, instead of silently
> double-firing. DBOS's same-transaction mechanism *is* AgentTx's `TRANSACTIONAL` class; AgentTx
> adds the other five classes + KV-as-materialized-view recovery.

---

## Results

### Headline

- **120,900 fault injections, 0 correctness violations** — 0 duplicate/lost charge, 0
  duplicate/lost receipt, **0 ghost observation**, 0 stream-not-exactly-once — including
  crash-during-recovery and KV-snapshot corruption.
- **Recovery speedup (KV restore vs transcript re-prefill):** 4.84× @8K, **12.5× @16K, 17× @32K**.
  The win grows with context (re-prefill ≈ quadratic, restore ≈ linear-bandwidth).
- **Steady-state durability overhead: 0.7%** of a 100 ms turn (0.70 ms/turn of WAL appends +
  gateway dedup + fsync'd commits), 0.14% of a 500 ms turn.
- **Coverage:** 3 tool environments (PostgreSQL / filesystem / HTTP), **2 real baselines**
  (DBOS 2.25, LangGraph 1.2.6), 2 models (Llama-3.1-8B, Qwen3-8B), fail-closed `UNCERTAIN` for
  non-idempotent irreversible APIs.

### Strong-gate scorecard ([`phase5/results/summary.json`](phase5/results/summary.json))

| bar | target | result |
|---|---|---|
| dup/lost on supported tools | 0 | **0** (across 120,900 fault injections) |
| ghost observations | 0 | **0** |
| steady-state overhead | ≤ 5% | **0.7%** |
| recovery speedup | ≥ 5× | **12.5–17× @16–32K** (4.84× @8K) |
| fault injections | ≥ 100k | **120,900** |
| tool environments | ≥ 3 | **3** (PostgreSQL / filesystem / HTTP) |
| real frameworks compared | ≥ 2 | **2** (DBOS 2.25 + LangGraph 1.2.6, both real) |
| models | ≥ 2 | **2** (Llama-3.1-8B + Qwen3-8B) |
| irreversible API | fail-closed | **`UNCERTAIN`**, never silent double |

### Baselines, head-to-head (same crash: effect fires, framework crashes before recording it)

| system | version | transactional charge | non-transactional receipt |
|---|---|---|---|
| real DBOS | 2.25.0 | **1 (exactly-once)** | **2 (DUPLICATED)** ← the gap |
| real LangGraph | 1.2.6 (PostgresSaver) | **2 (DUPLICATED)** | **2 (DUPLICATED)** |
| **AgentTx** | — | **1 (exactly-once)** | **1 (exactly-once)** |

(AgentTx verified over 10,000 random crashes including crash-during-recovery;
[`gate1/REAL_BASELINES.md`](gate1/REAL_BASELINES.md).)

---

## How it was built — the death gate, then five phases

AgentTx was developed *falsify-before-invest*: a cheap, decisive **death gate** had to pass before
committing to the full system.

### Death gate ([`docs/GATE_VERDICT.md`](docs/GATE_VERDICT.md)) — verdict **GO**

| gate | question | result |
|---|---|---|
| **Gate 0** | Does *any* system bundle turn-atomicity across {LLM/KV, workflow, tool effect, output}? | **No** — DBOS owns only transactional exactly-once; KV-recovery systems ignore tool effects ([`gate0/`](gate0/GATE0_NOVELTY.md)) |
| **Gate 1a** | Does the strong baseline have a *real* correctness gap? | **Yes** — DBOS duplicates non-transactional effects at the crash window ([`gate1/`](gate1/)) |
| **Gate 1b** | Is KV-as-materialized-view a *real* recovery win? | **Yes** — restore vs re-prefill **1.8× @4K, 12.5× @16K, 17× @32K** |
| **Gate 2** | Does a minimal AgentTx deliver end-to-end? | **Yes** — 10k crashes 0 dup/lost, **7.8×** recovery @16K, **0.7%** overhead ([`gate2/`](gate2/GATE2_VERDICT.md)) |

### Phases 1–5 — the full system

| phase | component | evidence |
|---|---|---|
| **1** | Postgres WAL + Tool-Gateway taxonomy + **real DBOS & LangGraph baselines** | real DBOS 2.25 / LangGraph 1.2.6 duplicate vs AgentTx exactly-once ([`gate1/REAL_BASELINES.md`](gate1/REAL_BASELINES.md)) |
| **2** | vLLM **KV-View** (provenance fingerprint + content-addressed CAS + checksum + fail-closed) | byte-exact `torch.equal` over 48 MB of real GPU KV bytes; every fail-closed path; 3.37× e2e @8K ([`phase2/`](phase2/PHASE2_KVVIEW.md)) |
| **3** | **3 exactly-once tool classes** (Postgres tx / FS overlay / HTTP idempotency proxy) + the fail-closed `IRREVERSIBLE` class | 300 crashes/class → exactly-once; non-idempotent API → 65 `UNCERTAIN`, 0 silent double ([`phase3/`](phase3/PHASE3_GATEWAY.md)) |
| **4** | **streaming exactly-once** + multi-worker reroute | 20,000 turns 100% exactly-once, 45,531 re-sends deduped, avg 2.66 workers/turn; real-HTTP cross-check ([`phase4/`](phase4/PHASE4_STREAMING.md)) |
| **5** | **end-to-end eval** | **120,900 fault injections, 0 violations**; 2 models; strong-gate scorecard ([`phase5/`](phase5/PHASE5_EVAL.md)) |

---

## Architecture

The `agenttx/` package is the protocol core; each `gate*/` and `phase*/` directory is a
self-contained, reproducible experiment that audits it.

```
agenttx/
  protocol.py     # turn record types + state machine + invariants + EXHAUSTIVE crash model-checker
  wal.py          # Turn WAL — durable, append-only source of truth (Postgres or SQLite)
  gateway.py      # Tool Gateway + the 6-class taxonomy + action-key dedup
  tools.py        # concrete tools, one per class (charge / receipt / email / http_charge / ...)
  db.py           # thin DB adapter: same code on SQLite (gates/tests) and PostgreSQL (production)
  kvview.py       # KV-View: Provenance fingerprint, BlockCAS (content-addressed), fail-closed restore
  recovery.py     # RecoveryCoordinator: KV restore-or-recompute + tool replay
  coordinator.py  # drives a turn through WAL + Gateway; recovery == re-run the turn
  core.py         # minimal standalone coordinator + Clock crash-injector (audited by Gate-2)
  stream.py       # streaming exactly-once: StreamLog + StreamClient ACK watermark + multi-worker resume

adapters/         # REAL DBOS 2.25 and LangGraph 1.2.6 baselines (Postgres), audited under the same crash
gate0/            # novelty kill-check (capability matrix, web-verified)
gate1/            # 1a failure-window correctness audit + 1b recovery-cost gap + real baselines
gate2/            # minimal end-to-end AgentTx: 10k fault audit, real-LLM e2e, overhead
phase2..5/        # KV-View / Tool Gateway / streaming / end-to-end eval — each with results/*.json
docs/             # GATE_VERDICT.md
scripts/          # pg_start.sh (Postgres bring-up; data dir lives OUTSIDE the repo)
RESULTS.md        # the one-page evidence map
```

### Recovery, end to end

On a crash the coordinator re-runs the turn. (1) The KV-View decides the KV path first —
**RESTORE** the byte-exact snapshot if provenance + every block checksum verify, else
**FAIL_CLOSED → recompute** from the committed token log. (2) The turn's tool plan is replayed; the
gateway's action-key dedup makes every supported effect exactly-once. (3) A fresh worker resumes
the output stream from the ACK watermark. **Correctness never depends on the KV snapshot** — it only
accelerates recovery.

---

## Reproducing

This is a research artifact, not a packaged library — there is no `requirements.txt`. Two
environments are used: a conda env (`agenttx`) for the orchestration + Postgres experiments, and a
vLLM environment for the GPU/KV experiments. Stack: dual **A100**, **vLLM 0.22.1**, **PostgreSQL
18.4**, **DBOS 2.25.0**, **LangGraph 1.2.6** (`psycopg`, PyTorch).

**Postgres** (data directory lives *outside* the repo; never versioned):

```bash
bash scripts/pg_start.sh                 # starts the cluster on port 54329
export AGENTTX_PG_DSN="postgresql://localhost:54329/agenttx"   # used by db.open_postgres()
```

**No-GPU experiments** (pure orchestration / crash-safety — run anywhere):

```bash
python agenttx/protocol.py                    # exhaustive crash-recovery model check
python gate1/failure_audit.py audit --trials 20      # Gate-1a: real-crash failure-window audit
python adapters/dbos_baseline.py audit        # real DBOS 2.25 — reproduces the non-tx duplicate
python adapters/langgraph_baseline.py audit   # real LangGraph 1.2.6 — duplicates both effects
python gate2/fault_audit.py audit --trials 10000     # Gate-2a: 10k fault injections, 0 dup/lost
python phase2/test_kvview.py             # KV-View logic: every fail-closed path + CAS dedup
python phase3/gateway_audit.py           # Phase-3: 3 tool classes vs real Postgres + HTTP (needs mock_service.py)
python phase4/stream_audit.py            # Phase-4: 20k streaming turns, exactly-once
python phase4/stream_realsocket.py       # Phase-4: real-HTTP worker reroute, 60/60 exactly-once
python phase5/eval.py --trials 100000    # Phase-5: full-stack 100k fault injections → 0 violations
python phase5/summary.py                 # aggregate every result into the strong-gate scorecard
```

**GPU experiments** (real vLLM + real KV bytes — need an A100 and a model path):

```bash
PYTHONPATH=. <peerkv-venv>/bin/python gate1/recovery_cost.py          # 1b: KV restore vs re-prefill @4K/16K/32K
PYTHONPATH=. <peerkv-venv>/bin/python gate2/e2e_llm.py                # 2b: real-LLM e2e, KV-restore recovery
PYTHONPATH=. <peerkv-venv>/bin/python phase2/kvview_gpu.py            # snapshot/restore 48 MB real GPU KV, torch.equal
PYTHONPATH=. <peerkv-venv>/bin/python phase2/kvview_e2e.py            # real vLLM e2e, 3.37× restore vs re-prefill
PYTHONPATH=. <peerkv-venv>/bin/python phase5/agent_e2e.py <model_path>  # full-stack agent turn on Llama-3.1-8B / Qwen3-8B
```

> The vLLM environment is the sibling project's venv (`<peerkv-venv>`); model paths are absolute
> (e.g. `/public/model_zoo/Llama-3.1-8B-Instruct`, `/public/model_zoo/Qwen3-8B`).

Each experiment writes machine-readable evidence to its `results/*.json`; [`RESULTS.md`](RESULTS.md)
is the one-page map from claim → file.

---

## Honest scope (carried to camera-ready)

- **Benchmark task-success.** The agent loop drives a real LLM (real generation + real KV) with a
  fixed action plan; wiring full SWE-bench / BFCL / Agent-Diff tool environments for end-to-end
  task-success is the remaining eval breadth.
- **A second hardware / topology** (results are on dual A100 today).
- **Live offload-tier per-block checksums** in the engine path — the byte-level content-addressed
  CAS is already proven on real GPU KV bytes ([`phase2/kvview_gpu.py`](phase2/kvview_gpu.py));
  productionizing wires those checksums into the live offload tier, plus SSD/remote snapshot tiers
  and GC of unreferenced blocks.
- **Irreversible non-idempotent APIs are not claimed exactly-once** — by design they are fail-closed
  `UNCERTAIN`. This is a provable boundary, not a missing feature.
