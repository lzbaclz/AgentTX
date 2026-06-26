# AgentTx — Cross-Plane Crash Consistency for Stateful LLM Agents

> ### ⚠️ Prototype status (read first)
> This is a **research prototype + evidence package**, not a finished system. Honest scope (see
> [`docs/CLAIM_LEDGER.md`](docs/CLAIM_LEDGER.md) for every claim graded PROVEN / MEASURED-PROXY /
> PROTOTYPE / TARGET):
> - **PROVEN:** the durable, multi-coordinator **turn-recovery protocol** (action-ordinal identity,
>   atomic claim, owner-epoch fencing, WAL-as-source-of-truth) — exactly-once under **real
>   multi-process concurrency + hard mid-tx `os._exit`** (`phase7/`); real DBOS/LangGraph failure
>   window; the tool-class taxonomy; byte-exact fail-closed KV CAS; τ²-bench mid-effect-crash result.
> - **MEASURED-PROXY:** KV recovery speedups are measured via **vLLM's own CPU-offload tier**, not
>   AgentTx's durable CAS, and not across a real worker crash. The "100k fault injections" are
>   **single-owner in-process protocol-model schedules**, not whole-stack.
> - **PROTOTYPE / TARGET:** durable cross-process KV restore into a fresh vLLM worker, durable output
>   log, full SOTA matrix, end-to-end agent-task success in the live orchestrator loop. **Not done.**
>
> This is **not** "the first agent transaction runtime" — **Atomix** and **Cordon** precede us on
> transactional tool use ([`docs/GATE0_REOPEN.md`](docs/GATE0_REOPEN.md)). Our unit of novelty is the
> **cross-plane crash-consistent recovery contract + protocol**, with those systems as backends.

**A single agent *turn* — the LLM generation, the conversation/KV state, the tool side-effects,
and the streamed client output — is one cross-layer transaction.** A **durable turn log** is the
single source of truth; the **KV cache is a materialized view** of that log (rebuildable,
fail-closed-verified). **The target contract:** after any component crashes, recover the agent to a
single **committed turn prefix** — no duplicate effects, no lost effects, no ghost observations, no
duplicated/lost client tokens.

**What is PROVEN today vs. target.** This contract is proven for the **transactional tool class
under real multi-process crashes** (`phase7/`: 1200/1200 actions, 0 double/lost) and for the
**OVERLAY (filesystem) class** (`phase7/overlay_gate.py`, below). The other non-transactional
classes are proven single-owner; durable cross-process KV recovery and a durable output log are
**TARGET** — see the status box above and [`docs/CLAIM_LEDGER.md`](docs/CLAIM_LEDGER.md). AgentTx is
a **prototype + evidence package**, not a finished runtime.

> **Headline (PROVEN):** the distributed turn-recovery protocol survives **400 turns × 2–6 real
> racing OS processes + hard mid-transaction `os._exit` + recovery sweep on PostgreSQL** with
> **1200/1200 actions exactly-once, 0 double, 0 lost** (`phase7/`). Other numbers (KV speedup,
> 100k schedules) are MEASURED-PROXY / single-owner — see the ledger.

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
- **Output is streamed exactly-once** *(against worker reroute; PROTOTYPE)*. Tokens tagged
  `(session, turn, seq)`; the client keeps an ACK watermark and dedups, so a mid-stream crash +
  reroute re-sends duplicates the client drops. Proven only for a deduplicating client vs reroute —
  **not** against loss of the output log or coordinator co-death, and today the log/ACK are
  *in-memory* ([`stream.py`](agenttx/stream.py)), not a durable persist-before-send. Correctness also
  requires re-sending *logged* tokens (not regenerating — free generation diverges across the
  KV-reuse boundary).

### Turn invariants (abstract model crash-enumerated)

[`agenttx/protocol.py`](agenttx/protocol.py) enumerates a crash after **every** prefix of one
turn's operations, recovers, and asserts:

- **I1 — Action Uniqueness:** an external effect fires at most once.
- **I2 — No Ghost Observation:** an observation is recorded only *after* its effect committed.
- **I3 — No Lost Effect:** a committed effect is eventually observed after recovery.
- **I5 — Prefix-Consistent Recovery:** a finished turn ends in `TURN_COMMITTED`.

The checker shows a *raw non-idempotent* effect **cannot** be made exactly-once by any orchestrator —
which is why that class is fail-closed. *Scope (honest):* this is an **abstract** single-key,
single-action, sequential model — it omits the `ACTION_PREPARED` window the real coordinator writes,
and does **not** model concurrency or torn writes (those are tested empirically in
[`phase7/`](phase7/), not formally). The implementation is verified separately by the crash audits,
not by this checker; a full TLA+ spec over the real record set + concurrent owners is future work.

## The Tool-Gateway taxonomy

Every side-effecting tool declares its class; the gateway enforces the matching mechanism
([`agenttx/gateway.py`](agenttx/gateway.py)):

"Effectively-once" below = at-most-once *execution* + deduplicated retry; only `TRANSACTIONAL`
achieves true exactly-once (effect + record in one ACID commit). Distributed-concurrent hard-crash
evidence today exists only for `TRANSACTIONAL` ([`phase7/`](phase7/)); the others are proven
single-owner.

| class | guarantee | mechanism |
|---|---|---|
| `PURE` | effectively-once (= cache) | read-only; cache result by key |
| `IDEMPOTENT` | effectively-once **(conditional)** | pass the action key as the external **Idempotency-Key** header — *relies on the external service honoring it with a TTL ≥ max recovery latency*; tested against a cooperative mock, not a real API |
| `TRANSACTIONAL` | **exactly-once** | effect **+** action-key record committed in **one** DB transaction |
| `OVERLAY` | effectively-once | write temp → **atomic rename** to `committed/<key>` (idempotent: same action key → one file) |
| `COMPENSATABLE` | committed-or-compensated | saga `prepared → effect_started → committed`; recovery re-runs cleanly from `prepared` (effect not begun) and **compensates only the ambiguous `effect_started`** state. *Logic present in [`gateway.py`](agenttx/gateway.py); no concrete tool / executed crash audit yet — graded **PROTOTYPE**.* |
| `IRREVERSIBLE` | **fail-closed `UNCERTAIN`** | durable `prepared` before the act; crash in the act/commit window → `UNCERTAIN`, **never** a silent re-send |

> **The honest boundary.** For a truly non-idempotent irreversible external effect (wire money,
> send an email once), *no* orchestrator — DBOS, LangGraph, or AgentTx — can guarantee exactly-once.
> AgentTx fail-closes it as `UNCERTAIN` for a human/retry-policy to reconcile, instead of silently
> double-firing. DBOS's same-transaction mechanism *is* AgentTx's `TRANSACTIONAL` class; AgentTx
> adds the other five classes + KV-as-materialized-view recovery.

---

## Results

> Every number below is graded to match [`docs/CLAIM_LEDGER.md`](docs/CLAIM_LEDGER.md). **PROVEN**
> = real multi-process `os._exit` on Postgres. **MEASURED-PROXY** = real measurement, but of a
> *proxy* (vLLM's own offload tier / single-owner in-process schedules), not of AgentTx's durable
> recovery across a worker crash. Do not cite a MEASURED-PROXY figure without its label.

- **(PROVEN) Distributed exactly-once, real crashes — two classes.** 400 turns × 2–6 racing OS
  processes + hard `os._exit` + recovery sweep on PostgreSQL: the **`TRANSACTIONAL`** class
  ([`phase7/concurrent_gate.py`](phase7/concurrent_gate.py): 1200/1200 actions, 0 double/lost) **and**
  the non-transactional **`OVERLAY`** filesystem class ([`phase7/overlay_gate.py`](phase7/overlay_gate.py):
  1200/1200 committed files, 0 duplicate, 0 lost, 0 tmp-promoted, legit-duplicate ordinals both
  execute), with hard death at both the after-write and after-publish windows. (`IDEMPOTENT` /
  `COMPENSATABLE` distributed hardening is still open.)
- **(PROVEN) Real-framework failure window.** Same crash (effect fires, framework records *after*):
  real **DBOS 2.25** duplicates the non-transactional receipt; real **LangGraph 1.2.6** duplicates
  *both* effects; AgentTx is exactly-once ([`gate1/REAL_BASELINES.md`](gate1/REAL_BASELINES.md)).
- **(PROVEN) Byte-exact fail-closed KV CAS.** `torch.equal` over 48 MB of real GPU KV bytes; every
  provenance / checksum mismatch fails closed to recompute ([`phase2/kvview_gpu.py`](phase2/kvview_gpu.py)).
- **(MEASURED-PROXY) ~100k single-process protocol-model schedules, 0 violations.** Crash via an
  *in-process exception* (not `os._exit`) at random durable boundaries; oracle finds 0 dup/lost/ghost.
  A randomized exploration of the protocol state machine, **not** a whole-stack crash-consistency
  result. (The `os._exit` cross-check is a 500-trial subset.)
- **(MEASURED-PROXY) KV recovery speedup 4.84× @8K, 12.5× @16K, 17× @32K** — measured as vLLM's own
  **CPU-offload-tier restore vs cold re-prefill of fresh tokens**, *same process, no worker crash,
  AgentTx's durable CAS not on the path, snapshot/hash cost excluded*. It bounds the *potential* of
  KV-as-view, not AgentTx's measured recovery cost (real cross-process restore is `phase8/` TARGET).
- **(MEASURED-PROXY) Durability bookkeeping 0.70 ms/turn** (≈14.5× a 0.05 ms no-persist baseline, on
  a `/dev/shm` ramdisk; the KV-snapshot GPU→host copy + per-block sha256 is **not** included). "0.7%"
  is that 0.70 ms over a *stipulated* 100 ms turn, not a measured end-to-end turn.
- **(PROVEN) τ²-bench mid-effect crash.** On a constructed mid-refund crash of the one non-atomic
  retail tool, naive **double-refunds 5/15**, AgentTx **0/15** ([`phase6/`](phase6/)). On the *full*
  τ²-retail benchmark the tools self-guard, so there is **no gap** (`PHASE6_PASS=false`,
  naive double-charged 0) — both facts stated, neither hidden.
- **Coverage:** 3 tool environments (PostgreSQL / filesystem / HTTP), 2 real baselines (DBOS 2.25,
  LangGraph 1.2.6), 2 models (Llama-3.1-8B, Qwen3-8B), fail-closed `UNCERTAIN` for non-idempotent
  irreversible APIs.

### Scorecard ([`phase5/results/summary.json`](phase5/results/summary.json), graded)

| bar | result | grade & crash model |
|---|---|---|
| distributed exactly-once (Tx class) | **1200/1200 actions, 0 dup/lost** | **PROVEN** — real multi-process `os._exit`, Postgres ([`phase7/concurrent_gate.py`](phase7/concurrent_gate.py)) |
| distributed exactly-once (OVERLAY class) | **1200/1200 files, 0 dup/lost/promoted** | **PROVEN** — real multi-process `os._exit` (after-write + after-publish) ([`phase7/overlay_gate.py`](phase7/overlay_gate.py)) |
| dup/lost/ghost, single-owner protocol | **0** | **MEASURED-PROXY** — ~100k in-process schedules + 500 `os._exit` xcheck |
| ghost observations | **0** | MEASURED-PROXY (single-owner) |
| recovery speedup | **12.5–17× @16–32K** (4.84× @8K) | **MEASURED-PROXY** — vLLM offload-tier restore vs cold re-prefill, no worker crash |
| durability bookkeeping | **0.70 ms/turn** (≈14.5× no-persist; "0.7%" of a stipulated 100 ms turn) | MEASURED-PROXY — ramdisk, KV-snapshot cost excluded |
| tool environments | **3** (PostgreSQL / filesystem / HTTP) | — |
| real frameworks compared | **2** (DBOS 2.25 + LangGraph 1.2.6) | PROVEN failure-window |
| models | **2** (Llama-3.1-8B + Qwen3-8B) | — |
| irreversible API | **`UNCERTAIN`**, never silent double | PROVEN (fail-closed) |

### Baselines, head-to-head (same crash: effect fires, framework crashes before recording it)

| system / config | version | transactional charge | non-transactional receipt |
|---|---|---|---|
| real DBOS — *naked* (`@DBOS.step`, uuid file) | 2.25.0 | **1 (exactly-once)** | **2 (DUPLICATED)** ← default-config gap |
| real DBOS — **+ idempotency key** | 2.25.0 | **1** | **1 (exactly-once)** — gap CLOSED |
| real DBOS — **+ transactional outbox** | 2.25.0 | **1** | **1 (exactly-once)** — gap CLOSED |
| real LangGraph — *naked* (PostgresSaver) | 1.2.6 | **2 (DUPLICATED)** | **2 (DUPLICATED)** |
| **AgentTx** | — | **1 (exactly-once)** | **1 (exactly-once)** |

(All rows measured: [`adapters/dbos_baseline.py`](adapters/dbos_baseline.py),
[`adapters/dbos_recommended.py`](adapters/dbos_recommended.py), [`gate1/results/`](gate1/results/).)

> **Honest takeaway (not "DBOS is incapable").** DBOS's *recommended* configs — a deterministic
> idempotency key, or a transactional outbox + idempotent relay — make the receipt exactly-once
> under the very crash that duplicates the naked config. The fix (deterministic key + idempotent
> atomic publish) **is** AgentTx's `OVERLAY` mechanism. So AgentTx's contribution is **not** beating
> DBOS on one transactional effect; it is (1) applying the right mechanism **per tool class
> automatically** (the taxonomy), so a developer need not hand-roll an idempotency key/outbox per
> tool, and (2) binding that to the KV view + output log as **one cross-plane turn recovery**.
> Atomix/Cordon remain unmeasured prior art ([`docs/GATE0_REOPEN.md`](docs/GATE0_REOPEN.md)).

---

## How it was built — the death gate, then eight phases

AgentTx was developed *falsify-before-invest*: a cheap, decisive **death gate** had to pass before
committing to the full system.

### Death gate ([`docs/GATE_VERDICT.md`](docs/GATE_VERDICT.md)) — verdict **GO**

| gate | question | result |
|---|---|---|
| **Gate 0** | Does *any* system bundle turn-atomicity across {LLM/KV, workflow, tool effect, output}? | **Reopened** — DBOS owns only transactional exactly-once and KV-recovery systems ignore tool effects, *but* **Atomix/Cordon precede us on transactional tool use**, so the novelty narrows to the **cross-plane recovery contract**, not "first agent transaction" ([`docs/GATE0_REOPEN.md`](docs/GATE0_REOPEN.md)) |
| **Gate 1a** | Does the strong baseline have a *real* correctness gap? | **Yes** — DBOS duplicates non-transactional effects at the crash window ([`gate1/`](gate1/)) |
| **Gate 1b** | Is KV-as-materialized-view a *real* recovery win? | **Yes (MEASURED-PROXY)** — offload-tier restore vs cold re-prefill **1.8× @4K, 12.5× @16K, 17× @32K** (no worker crash on the measured path) |
| **Gate 2** | Does a minimal AgentTx deliver end-to-end? | **Yes** — 10k *in-process* crashes 0 dup/lost (+500 `os._exit` xcheck), 7.8× proxy recovery @16K, 0.70 ms/turn bookkeeping ([`gate2/`](gate2/GATE2_VERDICT.md)) |

### Phases 1–8

| phase | component | evidence (grade) |
|---|---|---|
| **1** | Postgres WAL + Tool-Gateway taxonomy + **real DBOS & LangGraph baselines** | DBOS 2.25 / LangGraph 1.2.6 duplicate vs AgentTx exactly-once — **PROVEN** ([`gate1/REAL_BASELINES.md`](gate1/REAL_BASELINES.md)) |
| **2** | vLLM **KV-View** (provenance fingerprint + content-addressed CAS + checksum + fail-closed) | byte-exact `torch.equal` over 48 MB real GPU KV — **PROVEN**; 3.37× e2e @8K — **MEASURED-PROXY** ([`phase2/`](phase2/PHASE2_KVVIEW.md)) |
| **3** | **3 exactly-once tool classes** (Postgres tx / FS overlay / HTTP idempotency proxy) + fail-closed `IRREVERSIBLE` | 300 crashes/class → exactly-once; 65 `UNCERTAIN`, 0 silent double — single-owner ([`phase3/`](phase3/PHASE3_GATEWAY.md)) |
| **4** | **streaming exactly-once** + multi-worker reroute | 20k turns exactly-once vs worker reroute — **PROTOTYPE** (in-memory log/ACK, not durable persist-before-send) ([`phase4/`](phase4/PHASE4_STREAMING.md)) |
| **5** | end-to-end correctness eval | ~100k single-process schedules, 0 violations — **MEASURED-PROXY** ([`phase5/`](phase5/PHASE5_EVAL.md)) |
| **6** | AgentTx on the **real τ²-bench** retail environment | constructed mid-refund crash: naive 5/15 double, AgentTx 0/15; full bench self-guards (no gap) — **PROVEN/honest** ([`phase6/`](phase6/)) |
| **7** | **distributed turn-recovery protocol** (ordinal identity, atomic claim, owner-epoch fencing) | TRANSACTIONAL **and** OVERLAY classes exactly-once under real multi-process `os._exit`, Postgres — 1200/1200 each — **PROVEN** ([`phase7/`](phase7/)) |
| **8** | live-orchestrator FT + durable cross-process KV — **down-payments** | partial; real cross-process KV restore into a fresh vLLM worker is **TARGET, not done** ([`phase8/`](phase8/)) |

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
gate0/            # novelty kill-check (capability matrix) — see also docs/GATE0_REOPEN.md
gate1/            # 1a failure-window correctness audit + 1b recovery-cost gap + real baselines
gate2/            # minimal end-to-end AgentTx: 10k fault audit, real-LLM e2e, overhead
phase2../phase5/  # KV-View / Tool Gateway / streaming / end-to-end eval — each with results/*.json
phase6/           # AgentTx on the real τ²-bench retail environment
phase7/           # distributed turn-recovery protocol (dtx.py, concurrent_gate.py) — real multi-process os._exit
phase8/           # live-orchestrator FT + durable cross-process KV — down-payments (TARGET work)
docs/             # CLAIM_LEDGER.md (every claim graded) · GATE0_REOPEN.md (Atomix/Cordon prior art) · GATE_VERDICT.md
scripts/          # pg_start.sh (Postgres bring-up; data dir lives OUTSIDE the repo)
RESULTS.md        # the one-page evidence map
```

> **Two action-identity schemes exist:** `gateway.py` keys effects by `H(session,turn,tool,args)`
> (single-owner audits); the distributed protocol in `phase7/dtx.py` uses an **ordinal** action
> identity + content-fingerprint check. The rigorously crash-tested path is the latter; reconciling
> the two into one is open work.

### Recovery, end to end

On a crash the coordinator re-runs the turn. (1) The KV-View decides the KV path first —
**RESTORE** the byte-exact snapshot if provenance + every block checksum verify, else
**FAIL_CLOSED → recompute** from the committed token log. (2) The turn's tool plan is replayed; the
gateway's action-key dedup makes every supported effect exactly-once. (3) A fresh worker resumes
the output stream from the ACK watermark. **Correctness never depends on the KV snapshot** — it only
accelerates recovery.

> This is the *target* recovery contract. What is proven end-to-end today vs. modeled single-owner
> vs. not-yet-built is enumerated in **[Open issues](#open-issues-what-is-not-yet-proven--see-docsclaim_ledgermd)**
> and the ledger — e.g. step (3) is an in-memory prototype, and step (1)'s cross-process restore is TARGET.

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

## Open issues (what is *not* yet proven — see [`docs/CLAIM_LEDGER.md`](docs/CLAIM_LEDGER.md))

Ordered by how load-bearing they are to the headline claims:

1. **Distributed hard-crash evidence now covers `TRANSACTIONAL` + `OVERLAY`** (the latter a novel
   non-transactional class, [`phase7/overlay_gate.py`](phase7/overlay_gate.py)). Still single-owner
   only: **`IDEMPOTENT`** and **`COMPENSATABLE`**. *Fix: harden `IDEMPOTENT` under the `phase7/`
   multi-process gate (needs a durable external idempotency store); give `COMPENSATABLE` a concrete tool + audit.*
2. **The recovery-speedup is a proxy.** It measures vLLM's offload-tier restore vs cold re-prefill of
   *fresh* tokens, in one process, with no worker crash and AgentTx's CAS off the path; snapshot/hash
   cost is excluded. *Fix: honest baseline = replay the identical context with prefix caching; then
   implement real cross-process restore (SIGKILL → fresh vLLM → load CAS → resume) and account for snapshot cost.*
3. **`COMPENSATABLE` has logic but no executed test** and no concrete tool. *Fix: implement a real
   compensatable tool + run it through the `phase3/` hard-crash matrix, or grade it TARGET.*
4. **The streaming/output plane is in-memory**, not a durable persist-before-send log; no
   coordinator+stream-worker co-death test. *Fix: durable output WAL + co-death recovery test.*
5. **Crash fidelity & statistics.** ~99.6% of the "fault injections" are in-process exceptions on a
   live DB connection (graceful abort), single RNG seed, no CIs, no torn-write/fsync-fault test.
   *Fix: re-run a meaningful fraction under `os._exit`, multiple seeds + Clopper-Pearson bounds, add a torn-WAL test.*
6. **Baseline fairness.** Only naked DBOS/LangGraph are compared; DBOS+idempotency-key / +outbox and
   Atomix/Cordon are unmeasured. *Fix: add the recommended configs and a characterized prior-art comparison.*
7. **Determinism / "correct output on recovery"** is asserted via teacher-forcing but never measured
   on a GPU path; provenance fingerprint stubs LoRA/RoPE. *Fix: measure teacher-forced post-recovery
   token-id equality; pull real adapter/RoPE config + fail-closed unit tests.*
8. **No real agent-task success** (fixed scripted plan, no SWE-bench/BFCL), and **no `requirements.txt`
   / Dockerfile / env-var paths** — reproduction needs hardcoded absolute paths today.

**Irreversible non-idempotent APIs are intentionally `UNCERTAIN`** (fail-closed) — a provable
boundary, not a gap.
