# Phase 6 — camera-ready eval breadth: AgentTx on the REAL tau2-bench tool environment

We connected the published **tau2-bench** (τ²-bench, Sierra) `retail` domain — real tools, real
order/user/payment DB, 114 real tasks, and the benchmark's own evaluator criterion (a task's DB
reward = *final DB hash == the gold DB hash*, the gold being a replay of the reference actions).
Install: `.venv_tau2b` (editable `/tmp/tau2bench` + `audioop-lts`); LLM via litellm → local vLLM.

## Experiment 1 — full-call replay (`phase6/tau2_agenttx.py`, `tau2_agenttx.json`)
97 tasks touch money-moving tools; 85 had a clean gold. Worker crashes after a money-moving
tool's effect, before the result is recorded; recovery re-issues the *whole* call.
- **Naive recovery: 85/85 success, 0 double-effects.** Honest finding: tau2's money-moving tools
  **self-guard against full replay** (`cancel_pending_order` checks `status!="pending"`;
  `modify_pending_order_payment` checks `len(payment_history)!=1`) — a completed call re-issued
  verbatim is refused. AgentTx: also 85/85. Per-tool guards suffice for *full* replay.

## Experiment 2 — mid-effect crash (the decisive case) (`phase6/tau2_midcrash.py`, `tau2_midcrash.json`)
But those tools are **non-atomic**: `cancel_pending_order` credits the gift-card refund *inside a
loop* and only sets `status="cancelled"` at the end. A crash AFTER the credit, BEFORE the status
update leaves the guard reading "pending" → the guard cannot catch the retry. We inject that real
internal crash point (`CrashableRetailTools`) and score by the real evaluator's DB criterion:

| recovery | task success (DB==gold) | double-refunds |
|---|---|---|
| **naive** (mutate live DB, re-run) | **10/15 (66.7%)** | **5** (gift-card orders double-credited) |
| **AgentTx** (transactional gateway: execute on a DB snapshot, atomic-swap on completion; mid-effect crash discards the snapshot; recovery re-runs once on the clean DB) | **15/15 (100%)** | **0** |

**Result:** on a real published benchmark, scored by its own evaluator, AgentTx's transactional
exactly-once turns a 5-task double-refund / 33%-task-success loss into 0 / 100%. The value is
framework-level exactly-once that does **not** depend on per-tool defensive guards or tool
atomicity.

## LLM-driven agent path (`phase6/tau2_llm_smoke`)
`tau2 run --domain retail --agent-llm openai/qwen --user-llm openai/qwen` against the local vLLM
(Qwen2.5-7B-Instruct, hermes tool-parser): 2 tasks ran end-to-end, agent+user-simulator conversed,
tools dispatched, evaluator scored — **0 agent errors, 0 user errors**. Raw avg reward 0.0 (an 8B
model is weak on τ²-bench; capability, not integration). The agent path is wired; the gateway
chokepoint is `Environment.get_response`, which Experiments 1–2 already wrap.

## Honest scope
Experiments 1–2 drive the real environment with each task's GOLD action sequence to isolate
AgentTx's fault-tolerance from LLM capability; metric == the real evaluator's DB reward. Remaining
camera-ready: run the gateway + fault injection inside the live LLM orchestrator loop end-to-end;
a stronger agent model for non-zero task reward; airline/telecom domains; SWE-bench for the
coding-agent (OVERLAY-class) regime.

PHASE6_MIDCRASH_PASS = true.
