"""AgentTx Gate-1b: the recovery-cost gap.

Generic durable-workflow recovery (DBOS / LangGraph) resumes a turn by REPLAYING the
transcript -> the LLM re-prefills the entire committed context. AgentTx treats the KV cache
as a materialized VIEW of the durable turn log: on recovery it RESTORES the KV snapshot
(here: from the CPU offload tier, a durable store) instead of recomputing it. We measure
the resume latency (TTFT of a 1-token continuation) both ways at 4K/16K/32K context.

GO: KV-snapshot restore is >=3x faster than full re-prefill at long context -> "KV as
materialized view" is a real recovery-cost win that the transcript-replay baselines lack.

Run: PYTHONPATH=. <peerkv-venv>/bin/python gate1/recovery_cost.py
"""
from __future__ import annotations

import json
import os
import time

os.environ.setdefault("VLLM_ENABLE_V1_MULTIPROCESSING", "0")
os.environ.setdefault("VLLM_USE_FLASHINFER_SAMPLER", "0")

MODEL = os.environ.get("AGENTTX_MODEL", "/public/model_zoo/Llama-3.1-8B-Instruct")
LENGTHS = [4096, 16384, 32768]
GPU_BLOCKS = 5200          # ~83K tokens: holds ~2.5x a 32K ctx -> priming others evicts target
OFFLOAD_GB = 40
MAXLEN = 33024
RESDIR = "gate1/results"


def main():
    import torch
    from transformers import AutoTokenizer
    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig

    trace = "/tmp/agenttx_recov_trace.jsonl"
    os.environ["KVQ_TRACE_FILE"] = trace          # records restore FCT (inline offload path)

    tok = AutoTokenizer.from_pretrained(MODEL)
    vs = int(getattr(tok, "vocab_size", 32000)); lo, hi = 1000, min(vs - 1, 30000)
    import random
    rng = random.Random(0)

    def mk(n, lead):
        return {"prompt_token_ids": [lo + lead] + [rng.randrange(lo, hi) for _ in range(n - 1)]}

    llm = LLM(model=MODEL, enforce_eager=True, gpu_memory_utilization=0.6, max_model_len=MAXLEN,
              tensor_parallel_size=1, num_gpu_blocks_override=GPU_BLOCKS,
              enable_prefix_caching=True, disable_log_stats=True, seed=0,
              kv_transfer_config=KVTransferConfig(
                  kv_connector="OffloadingConnector", kv_role="kv_both",
                  kv_connector_extra_config={"spec_name": "CPUOffloadingSpec",
                                             "cpu_bytes_to_use": OFFLOAD_GB * 1024**3, "block_size": 16}))
    sp = SamplingParams(temperature=0.0, max_tokens=1)

    def ttft(prompt):
        t0 = time.perf_counter(); llm.generate([prompt], sp); return (time.perf_counter() - t0) * 1e3

    def restored_GB():
        rb = 0.0
        if os.path.exists(trace):
            for line in open(trace):
                try:
                    d = json.loads(line)
                    if d["dir"] == "CPU->GPU":
                        rb += d["bytes"]
                except Exception:  # noqa: BLE001
                    pass
        return rb / 1e9

    rows = []
    lead = 0
    for L in LENGTHS:
        lead += 1
        # 1) re-prefill cost: a FRESH never-seen L-token context, full prefill
        reprefill_ms = ttft(mk(L, 100 + lead))
        reprefill_ms = ttft(mk(L, 200 + lead))      # 2nd fresh, steady-state compute

        # 2) prime the target, then evict it to the CPU tier by priming larger others
        target = mk(L, lead)
        ttft(target)                                  # prime + offload
        for k in range(4):
            ttft(mk(32768, 900 + k * 7 + lead))       # evict target (LRU) to CPU
        # 3) restore cost: re-issue target -> KV restored from CPU (no re-prefill)
        if os.path.exists(trace):
            os.remove(trace)
        torch.cuda.synchronize()
        restore_ms = ttft(target)
        rg = restored_GB()
        rows.append({"ctx_tokens": L, "reprefill_ms": round(reprefill_ms, 1),
                     "restore_ms": round(restore_ms, 1),
                     "speedup": round(reprefill_ms / restore_ms, 2) if restore_ms else None,
                     "restored_GB": round(rg, 3), "restore_fired": rg > 0.05})
        print(f"  L={L:>6}: reprefill {reprefill_ms:7.1f}ms | restore {restore_ms:7.1f}ms "
              f"| {rows[-1]['speedup']}x | restored {rg:.2f}GB (fired={rows[-1]['restore_fired']})",
              flush=True)

    ok = [r for r in rows if r["restore_fired"]]
    # the recovery-cost win is meaningful at LONG context (short re-prefill is cheap either
    # way); gate on the longest restored context being >=3x faster.
    longest = max(ok, key=lambda r: r["ctx_tokens"]) if ok else None
    out = {"model": MODEL, "rows": rows,
           "verdict": {"GATE1B_PASS": bool(longest) and longest["speedup"] >= 3,
                       "longest_ctx": longest["ctx_tokens"] if longest else None,
                       "longest_speedup": longest["speedup"] if longest else None,
                       "speedup_grows_with_ctx": bool(ok) and ok == sorted(ok, key=lambda r: r["ctx_tokens"])
                                                  and all(ok[i]["speedup"] <= ok[i + 1]["speedup"] for i in range(len(ok) - 1)),
                       "reads": "KV-snapshot restore (materialized view) vs transcript-replay re-prefill. "
                                "GATE1B_PASS iff restore is >=3x faster at the longest restored context "
                                "(where recovery cost is significant); short ctx is cheap either way."}}
    os.makedirs(RESDIR, exist_ok=True)
    json.dump(out, open(os.path.join(RESDIR, "gate1b_recovery_cost.json"), "w"), indent=2)
    print("\nverdict:", json.dumps(out["verdict"], indent=2))
    print("wrote gate1/results/gate1b_recovery_cost.json")


if __name__ == "__main__":
    main()
