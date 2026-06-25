# Real-framework validation — closes the Gate-1a "faithful re-impl" caveat

Gate-1a used a faithful re-implementation of DBOS's mechanism. Phase-1 closes that caveat by
running the SAME failure (crash after a side effect, before the framework records it) against
the **actual** DBOS and LangGraph packages on **real Postgres 18.4**.

| system | version | crash | charges (transactional) | receipts (non-transactional) | verdict |
|---|---|---|---|---|---|
| **real DBOS** | 2.25.0 | rc=137 in receipt `@DBOS.step` | **1 (exactly-once)** | **2 (DUPLICATED)** | transactional exactly-once, non-transactional re-runs on recovery |
| **real LangGraph** | 1.2.6 (PostgresSaver) | rc=137 in `receipt_node` | **2 (DUPLICATED)** | **2 (DUPLICATED)** | node effect not atomic with post-node checkpoint → re-runs on resume |
| **AgentTx** (Gate-2a) | — | 10,000 random crashes incl. crash-during-recovery | **1 (exactly-once)** | **1 (exactly-once)** | per-class mechanism: tx record same-tx + FS overlay content-addressed |

(`gate1/results/real_dbos_baseline.json`, `real_langgraph_baseline.json`; AgentTx in
`gate2/results/gate2a_*.json`.)

## What this establishes

1. **The Gate-1a wedge is real, not an emulation artifact.** Actual DBOS 2.25 reproduces it
   exactly: it gets transactional effects exactly-once (step checkpoint piggybacked in the
   same Postgres tx) but **re-executes the non-transactional `@DBOS.step` on recovery** after
   a crash in the effect/record window. Actual LangGraph + a real Postgres checkpointer
   duplicates **both** effects (state checkpoint is not atomic with the in-node side effect).
2. **AgentTx closes it** for the supported tool classes: the Tool Gateway gives the
   transactional class a same-tx action-key record (matching DBOS) AND the non-transactional
   class a per-turn content-addressed atomic FS commit (which DBOS/LangGraph lack), so BOTH
   are exactly-once — proven across 10k random crashes including crash-during-recovery.
3. **Honest boundary preserved:** for a truly irreversible non-idempotent effect, NO system
   (DBOS, LangGraph, or AgentTx) can guarantee exactly-once; AgentTx's gateway fail-closes it
   as `UNCERTAIN` (taxonomy in `agenttx/gateway.py`) instead of silently re-executing.

DBOS and LangGraph remain strong baselines and reusable layers (DBOS's same-tx mechanism is
exactly AgentTx's TRANSACTIONAL class; AgentTx adds the OVERLAY/IDEMPOTENT/COMPENSATABLE/
IRREVERSIBLE classes + the KV-as-materialized-view recovery).
