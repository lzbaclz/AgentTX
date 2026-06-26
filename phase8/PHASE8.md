# Phase 8 — answering the advisor: live-orchestrator FT (task1) + durable-KV down-payment

## task1 — AgentTx gateway INSIDE the live LLM orchestrator, with real fault injection
`phase8/tau2_live_ft.py` monkeypatches `Environment.get_response` (tau2's single tool-execution
chokepoint) to route every state-mutating retail tool call through an AgentTx TRANSACTIONAL wrap
(snapshot DB -> execute -> atomic commit {DB', action_id}). The tool calls are decided by a REAL
live Qwen agent running in tau2's real orchestrator + user simulator. On the first money-moving
call of each task we inject a real mid-turn crash (roll back, drop the in-flight result) and
recover (re-run once).

**Result (4 retail tasks):** 4/4 crashes injected on the live agent's own money-moving calls
(`exchange_delivered_order_items`), **0 actions double-applied** (max durable per action = 1);
naive (no wrap) would double-apply on each. Task reward 0.0 (an 8B agent is weak on τ²-bench --
capability, not the FT mechanism; exactly-once holds regardless). This closes the advisor's
"run the gateway inside the live orchestrator loop" item -- the gateway is no longer only driven
by gold/fixed plans.

## P2 down-payment — durable KV survives a real crash (the part the speedups lacked)
The advisor correctly flagged that the KV speedups used vLLM's OWN in-process CPU-offload tier,
which does not survive a worker crash. `phase8/kv_durable.py` isolates and proves the contested
property:

- Process A builds 32 MB of real GPU KV-cache-shaped fp16 blocks, writes them to a content-addressed
  durable on-disk CAS (sha256 = address = checksum + provenance manifest), `fsync`s, then **`SIGKILL`s
  itself** (no clean shutdown, CUDA context live -- the most brutal crash).
- Process B (a FRESH process that never touched A's memory) reloads from the CAS: **16/16 blocks,
  32 MB, byte-exact**, and **fail-closed** on a corrupted block.

PASS: `producer_hard_killed_SIGKILL=true`, `durable_restore_in_fresh_process_byte_exact=true`,
`fail_closed_on_corruption=true`.

**Honest scope (still TARGET):** injecting these durable bytes into a NEW vLLM engine's paged
attention to resume decoding is deep vLLM surgery and is not done. The durable token log remains the
source of truth (teacher-forced on recovery); the KV is a discardable, verifiable accelerator. This
proves the *durability + integrity* the same-process speedup numbers lacked, not yet the end-to-end
restore-into-attention.
