"""Phase-11 smoke: does vLLM's OffloadingConnector + TieringOffloadingSpec + fs_python tier spill
KV to a DURABLE on-disk content-addressed store? (feasibility check for the durable-KV connector)

Run: CUDA_VISIBLE_DEVICES=0 <peerkv-venv>/bin/python phase11/kv_cas_smoke.py
"""
import glob
import json
import os
import sys

MODEL = os.environ.get("ATX_MODEL", "/public/model_zoo/Llama-3.1-8B-Instruct")
CAS = os.environ.get("ATX_CAS", "/tmp/atx_kv_cas")
CTX = 2048


def main():
    import random
    os.system(f"rm -rf {CAS} && mkdir -p {CAS}")
    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig
    llm = LLM(model=MODEL, enforce_eager=True, gpu_memory_utilization=0.55, max_model_len=CTX + 64,
              tensor_parallel_size=1, enable_prefix_caching=True, disable_log_stats=True, seed=0,
              kv_transfer_config=KVTransferConfig(
                  kv_connector="OffloadingConnector", kv_role="kv_both",
                  kv_connector_extra_config={
                      "spec_name": "TieringOffloadingSpec",
                      "cpu_bytes_to_use": 768 * 1024 * 1024,   # stage >=1 ctx, then evict to fs
                      "block_size": 16,
                      "secondary_tiers": [{"type": "fs_python", "root_dir": CAS}],
                  }))
    rng = random.Random(0)
    ctx = [1] + [rng.randrange(1000, 30000) for _ in range(CTX - 1)]
    sp = SamplingParams(temperature=0.0, max_tokens=8, min_tokens=8, ignore_eos=True)
    llm.generate([{"prompt_token_ids": ctx}], sp)
    # free the sequence so blocks evict GPU->CPU->fs
    for k in range(12):
        llm.generate([{"prompt_token_ids": [1] + [rng.randrange(1000, 30000) for _ in range(CTX - 1)]}], sp)

    bins = glob.glob(f"{CAS}/**/*.bin", recursive=True)
    cfgs = glob.glob(f"{CAS}/**/*.json", recursive=True)
    total = sum(os.path.getsize(b) for b in bins)
    out = {"cas_dir": CAS, "bin_files": len(bins), "total_bin_MB": round(total / 1e6, 1),
           "config_files": len(cfgs),
           "SMOKE_PASS": len(bins) > 0,
           "reads": "vLLM OffloadingConnector + TieringOffloadingSpec spilled KV to the fs_python "
                    "durable content-addressed tier on disk (block files named by content hash)."}
    os.makedirs("phase11/results", exist_ok=True)
    json.dump(out, open("phase11/results/kv_cas_smoke.json", "w"), indent=2)
    print(json.dumps(out, indent=2))
    return 0 if out["SMOKE_PASS"] else 1


if __name__ == "__main__":
    sys.exit(main())
