# AgentTx Gate-2 (minimal end-to-end system) — VERDICT: PASS

The last gate before committing to the full system: a real, minimal-but-end-to-end AgentTx
(Turn WAL + Tool Gateway + action keys + single-turn vLLM KV snapshot + crash recovery) must
clear three bars. All three pass.

## Gate 2a — 10k fault injections, 0 duplicate/lost: PASS
The full coordinator (`agenttx/core.py`) crashed at a random durable boundary every trial,
**including crash-during-recovery** (p=0.4), then recovered to completion; an oracle counts
the real external effects (SQLite `charges` + receipt files) per order.

| mode | trials | CORRECT | DUPLICATE | LOST |
|---|---|---|---|---|
| in-process crash model | 10,000 | 10,000 | **0** | **0** |
| subprocess `os._exit` (faithful real crash) | 500 | 500 | **0** | **0** |

Supported tools (SQL transactional + FS overlay/content-addressed) are exactly-once under
every crash; the subprocess cross-check confirms the fast in-process model matches real
crashes. (`gate2/results/gate2a_*.json`.)

## Gate 2b — real-LLM end-to-end (exactly-once + KV-restore recovery): PASS
A live vLLM (Llama-3.1-8B) processes a real 16K-token agent context, the Tool Gateway
executes the decided tool call, the turn's KV is snapshotted to the durable CPU offload tier,
then the **coordinator crashes** after the effect / before turn-commit. On recovery:

- **tool exactly-once across the crash**: charge count = 1 before crash, **still 1 after
  recovery** (gateway action-key dedup — no double charge).
- **KV restored from the snapshot** (2.15 GB) instead of re-prefilled: recovery
  **201.8 ms vs 1576 ms re-prefill = 7.81× faster** at 16K (consistent with Gate-1b's
  12–17× at 16–32K). (`gate2/results/gate2b_e2e_llm.json`.)

## Gate 2c — steady-state overhead < 5%: PASS
Durability bookkeeping (WAL appends + gateway dedup + fsync'd commits + atomic FS commit) is
**0.70 ms/turn** vs a 0.05 ms no-persistence baseline doing the same effects →
**0.7% of a 100 ms turn, 0.14% of a 500 ms turn**. Negligible vs any real LLM+tool turn.
(`gate2/results/gate2c_overhead.json`.)

## Decision: PASS — build the full system

Gate-0 (novel) + Gate-1 (the problem is real: DBOS non-tx gap + 17× recovery gap) +
**Gate-2 (a minimal AgentTx actually delivers exactly-once tools across 10k crashes, KV-view
recovery 7.8×, <1% overhead)** all clear. The minimal system works end-to-end; the full
build is justified.

### Honest scope carried forward (Phase-1/2 must close)
- "DBOS" is a faithful re-impl of its same-tx-step-record + idempotency mechanism — Phase-1
  validates against **real DBOS + real LangGraph**.
- Gate-2a uses a fixed scripted plan (the tool calls), to isolate orchestration crash-safety;
  Gate-2b drives a **real LLM**. Phase-2 makes the LLM *decide* the tool calls end-to-end and
  audits with real agent tasks (SWE-bench/BFCL/Agent-Diff).
- KV snapshot = the CPU offload tier (a durable store) — Phase-2 adds SSD/remote snapshots,
  the turn_lsn→block-manifest content-addressing, and fail-closed provenance
  (model/tokenizer/LoRA/RoPE/prefix-hash) verification.
- Streaming exactly-once (client (session,turn,seq) dedup) and multi-worker reroute are
  Phase-4.

### Next (Phase 1): the transaction core, hardened
Real DBOS/LangGraph baselines; Postgres-backed WAL; Tool Gateway HTTP idempotency proxy +
the full tool-class taxonomy (incl. fail-closed `UNCERTAIN`); TLA+/model-checker of the
protocol (`agenttx/protocol.py` already does an exhaustive Python crash-interleaving check).
