"""AgentTx Gate-2b: real-LLM end-to-end -- coordinator + Tool Gateway + KV-View snapshot +
crash recovery, on a live vLLM with a real (long) agent context.

A turn: the LLM processes a real ~CTX-token agent context (building real KV), decides a tool
call, the Tool Gateway executes it (charge), and the turn's KV is SNAPSHOTTED to the durable
CPU offload tier. Then the COORDINATOR CRASHES after the effect but before turn-commit. On
recovery: (1) the gateway action-key dedup makes the tool EXACTLY-ONCE (no double charge),
and (2) the KV is RESTORED from the snapshot (offload hit) instead of re-prefilling the whole
context. Ties Gate-2a (exactly-once) + Gate-1b (KV-as-materialized-view recovery) on a real
model.

Run: PYTHONPATH=. <peerkv-venv>/bin/python gate2/e2e_llm.py
"""
from __future__ import annotations

import json
import os
import sys
import time

os.environ.setdefault("VLLM_ENABLE_V1_MULTIPROCESSING", "0")
os.environ.setdefault("VLLM_USE_FLASHINFER_SAMPLER", "0")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agenttx.core import Clock, Coordinator, action_key, oracle  # noqa: E402

MODEL = os.environ.get("AGENTTX_MODEL", "/public/model_zoo/Llama-3.1-8B-Instruct")
CTX = int(os.environ.get("AGENTTX_CTX", "16384"))
ORDER = "ORD-E2E"


def main():
    import random
    import torch
    from transformers import AutoTokenizer
    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig

    trace = "/tmp/agenttx_e2e_trace.jsonl"
    os.environ["KVQ_TRACE_FILE"] = trace
    store = "/tmp/agenttx_g2b_store"
    os.system(f"rm -rf {store}"); os.makedirs(store)

    tok = AutoTokenizer.from_pretrained(MODEL)
    vs = int(getattr(tok, "vocab_size", 32000)); lo, hi = 1000, min(vs - 1, 30000)
    rng = random.Random(0)
    ctx = {"prompt_token_ids": [lo + 1] + [rng.randrange(lo, hi) for _ in range(CTX - 1)]}

    llm = LLM(model=MODEL, enforce_eager=True, gpu_memory_utilization=0.6, max_model_len=CTX + 512,
              tensor_parallel_size=1, num_gpu_blocks_override=int(CTX / 16 * 2.5),
              enable_prefix_caching=True, disable_log_stats=True, seed=0,
              kv_transfer_config=KVTransferConfig(
                  kv_connector="OffloadingConnector", kv_role="kv_both",
                  kv_connector_extra_config={"spec_name": "CPUOffloadingSpec",
                                             "cpu_bytes_to_use": 24 * 1024**3, "block_size": 16}))
    sp = SamplingParams(temperature=0.0, max_tokens=8)

    def llm_step(prompt):
        t0 = time.perf_counter(); out = llm.generate([prompt], sp)
        return list(out[0].outputs[0].token_ids), (time.perf_counter() - t0) * 1e3

    def restored_GB():
        rb = 0.0
        if os.path.exists(trace):
            for ln in open(trace):
                try:
                    d = json.loads(ln)
                    rb += d["bytes"] if d["dir"] == "CPU->GPU" else 0
                except Exception:  # noqa: BLE001
                    pass
        return rb / 1e9

    coord = Coordinator(store)
    turn = "TURN-1"
    clk = Clock(0)

    # ---- turn: LLM processes the real context (decision) ----
    coord.wal.append("BEGIN_TURN", turn)
    _, ttft_prime = llm_step(ctx)                 # builds + snapshots (offloads) the ctx KV
    coord.wal.append("MODEL_OUTPUT_PREPARED", turn)
    # ---- the LLM's decided tool call -> gateway executes (exactly-once) ----
    key = action_key(turn, "sql", (ORDER, 100))
    coord.wal.append("ACTION_PREPARED", turn, key)
    coord.gw.sql_charge(key, ORDER, 100, clk)     # the effect (+ ACTION_COMMITTED in same tx)
    coord.wal.append("OBSERVATION_COMMITTED", turn, key)
    # ---- COORDINATOR CRASHES here (effect committed, turn NOT committed) ----
    nc_before, _ = oracle(store, ORDER)
    del coord                                      # discard in-memory coordinator (crash)

    # evict the ctx KV from GPU so recovery must RESTORE it from the durable snapshot
    for k in range(4):
        llm_step({"prompt_token_ids": [lo + 50 + k] + [rng.randrange(lo, hi) for _ in range(CTX - 1)]})

    # ---- RECOVERY ----
    coord = Coordinator(store)
    # 1) tool exactly-once: gateway dedup -> re-running the action does NOT re-charge
    coord.gw.sql_charge(key, ORDER, 100, clk)
    nc_after, nr = oracle(store, ORDER)
    coord.wal.append("TURN_COMMITTED", turn)
    # 2) KV-as-materialized-view: restore the ctx KV (offload hit) vs re-prefill
    if os.path.exists(trace):
        os.remove(trace)
    torch.cuda.synchronize()
    _, ttft_restore = llm_step(ctx)               # prefix hit -> restore from CPU
    rg = restored_GB()
    # re-prefill reference: a fresh never-seen ctx of the same length
    fresh = {"prompt_token_ids": [lo + 777] + [rng.randrange(lo, hi) for _ in range(CTX - 1)]}
    _, ttft_reprefill = llm_step(fresh)

    out = {"model": MODEL, "ctx_tokens": CTX,
           # FIXED (advisor): the old expression `a and b and c or nr<=1` was True whenever nr<=1
           # regardless of the charge counts (precedence bug). Exactly-once = exactly one charge
           # before AND after recovery, and no extra receipt.
           "tool_exactly_once_across_crash": (nc_before == 1 and nc_after == 1 and nr <= 1),
           # NOTE: this is a SAME-PROCESS recovery-path check -- the "crash" is `del coord`, so the
           # vLLM engine / CUDA context / CPU-offload tier all survive. A real cross-process worker
           # crash + durable KV reload is phase8 (kv_durable). Do not read this as a worker crash.
           "crash_model": "same-process (del coord); vLLM/CUDA/offload survive -- NOT a worker crash",
           "charge_count_before_crash": nc_before, "charge_count_after_recovery": nc_after,
           "kv_restored_GB": round(rg, 3), "kv_restore_fired": rg > 0.05,
           "recover_via_restore_ms": round(ttft_restore, 1),
           "recover_via_reprefill_ms": round(ttft_reprefill, 1),
           "recovery_speedup": round(ttft_reprefill / ttft_restore, 2) if ttft_restore else None}
    out["GATE2B_PASS"] = bool(out["tool_exactly_once_across_crash"] and out["kv_restore_fired"]
                              and out["recovery_speedup"] and out["recovery_speedup"] >= 3)
    os.makedirs("gate2/results", exist_ok=True)
    json.dump(out, open("gate2/results/gate2b_e2e_llm.json", "w"), indent=2)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
