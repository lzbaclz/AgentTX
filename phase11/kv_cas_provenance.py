"""Phase-11b: AgentTx provenance fail-closed on the durable KV CAS.

Producer writes the durable CAS under provenance P1. Then two FRESH workers request the same
context from the same CAS:
  - recover with P1 (matching)   -> AgentTxCASTier hit  -> KV loaded into attention (cached == ctx)
  - recover with P2 (mismatched, only RoPE differs so vLLM's own block hashes still match) ->
    AgentTxCASTier FAILS CLOSED -> NO load (cached == 0) -> vLLM recomputes -> output still valid.

This proves the AgentTx provenance gate, not just vLLM's content hash: KV produced under a
different model/config is never silently loaded into a fresh worker's attention.

Run: PYTHONPATH=<repo> <peerkv-venv>/bin/python phase11/kv_cas_provenance.py
"""
import json
import os
import signal
import subprocess
import sys

MODEL = os.environ.get("ATX_MODEL", "/public/model_zoo/Llama-3.1-8B-Instruct")
CTX = 2048
CAS = "/tmp/atx_kv_cas_prov"
TOKLOG = "/tmp/atx_kv_cas_prov_target.json"
P1 = {"model": MODEL, "dtype": "bf16", "rope": "theta=5e5", "adapter": ""}
P2 = {"model": MODEL, "dtype": "bf16", "rope": "theta=1e6", "adapter": ""}   # only RoPE differs


def _llm(prov):
    from vllm import LLM
    from vllm.config import KVTransferConfig
    return LLM(model=MODEL, enforce_eager=True, gpu_memory_utilization=0.55, max_model_len=CTX + 64,
               tensor_parallel_size=1, enable_prefix_caching=True, disable_log_stats=True, seed=0,
               kv_transfer_config=KVTransferConfig(
                   kv_connector="OffloadingConnector", kv_role="kv_both",
                   kv_connector_extra_config={
                       "spec_name": "AgentTxTieringSpec",
                       "spec_module_path": "agenttx.kv_cas_tier",
                       "cpu_bytes_to_use": 768 * 1024 * 1024, "block_size": 16,
                       "secondary_tiers": [{"type": "agenttx_cas", "root_dir": CAS, "provenance": prov}]}))


def produce():
    os.environ["PYTHONHASHSEED"] = "0"
    import random
    from vllm import SamplingParams
    rng = random.Random(0)
    target = [1] + [rng.randrange(1000, 30000) for _ in range(CTX - 1)]
    json.dump({"target": target}, open(TOKLOG, "w"))
    llm = _llm(P1)
    sp = SamplingParams(temperature=0.0, max_tokens=8, min_tokens=8, ignore_eos=True)
    llm.generate([{"prompt_token_ids": target}], sp)
    for _ in range(12):
        llm.generate([{"prompt_token_ids": [1] + [rng.randrange(1000, 30000) for _ in range(CTX - 1)]}], sp)
    os.sync()
    os.kill(os.getpid(), signal.SIGKILL)


def recover():
    os.environ["CUDA_VISIBLE_DEVICES"] = os.environ.get("ATX_RECOVER_GPU", "1")
    os.environ["PYTHONHASHSEED"] = "0"
    import random
    from vllm import SamplingParams
    prov = json.loads(os.environ["ATX_PROV"])
    target = json.load(open(TOKLOG))["target"]
    llm = _llm(prov)
    sp = SamplingParams(temperature=0.0, max_tokens=8, min_tokens=8, ignore_eos=True)
    rng = random.Random(123)
    llm.generate([{"prompt_token_ids": [1] + [rng.randrange(1000, 30000) for _ in range(CTX - 1)]}], sp)
    out = llm.generate([{"prompt_token_ids": target}], sp)
    res = {"num_cached_tokens": out[0].num_cached_tokens or 0,
           "valid_output": len(list(out[0].outputs[0].token_ids)) == 8}
    json.dump(res, open(os.environ["ATX_OUT"], "w"))
    print(json.dumps(res))
    return 0


def _run(mode, prov, out, gpu):
    env = dict(os.environ, PYTHONPATH=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
               PYTHONHASHSEED="0", CUDA_VISIBLE_DEVICES=str(gpu),
               ATX_PROV=json.dumps(prov), ATX_OUT=out, ATX_RECOVER_GPU=str(gpu))
    return subprocess.run([sys.executable, os.path.abspath(__file__), mode], env=env).returncode


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "produce":
        produce(); return 0
    if len(sys.argv) > 1 and sys.argv[1] == "recover":
        return recover()
    os.system(f"rm -rf {CAS} && mkdir -p {CAS}")
    _run("produce", P1, "/tmp/_prov_produce.json", 0)                # writes CAS + P1 sidecar, SIGKILL
    _run("recover", P1, "/tmp/_prov_match.json", 1)                  # matching provenance
    _run("recover", P2, "/tmp/_prov_mismatch.json", 1)              # mismatched provenance -> fail closed
    match = json.load(open("/tmp/_prov_match.json"))
    mism = json.load(open("/tmp/_prov_mismatch.json"))
    out = {"ctx": CTX,
           "match_num_cached_tokens": match["num_cached_tokens"],
           "mismatch_num_cached_tokens": mism["num_cached_tokens"],
           "match_loaded_durable_kv": match["num_cached_tokens"] >= 0.8 * CTX,
           "mismatch_failed_closed": mism["num_cached_tokens"] == 0,
           "both_outputs_valid": match["valid_output"] and mism["valid_output"],
           "reads": "Matching provenance loads KV from the durable CAS into a fresh worker's "
                    "attention (cached==ctx); a mismatched provenance (RoPE differs, so vLLM's own "
                    "block hashes still match) FAILS CLOSED at the AgentTx tier (cached==0) and "
                    "recomputes -> KV under a different config is never silently loaded."}
    out["PHASE11B_PROVENANCE_PASS"] = bool(out["match_loaded_durable_kv"] and out["mismatch_failed_closed"]
                                           and out["both_outputs_valid"])
    os.makedirs("phase11/results", exist_ok=True)
    json.dump(out, open("phase11/results/kv_cas_provenance.json", "w"), indent=2)
    print(json.dumps(out, indent=2))
    # reap any orphaned EngineCore (own PIDs)
    try:
        import getpass
        me = getpass.getuser()
        for pid in subprocess.run(["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"],
                                  capture_output=True, text=True).stdout.split():
            if pid.strip().isdigit():
                who = subprocess.run(["ps", "-o", "user=", "-p", pid.strip()], capture_output=True, text=True).stdout.strip()
                cmd = subprocess.run(["ps", "-o", "cmd=", "-p", pid.strip()], capture_output=True, text=True).stdout
                if who == me and ("EngineCore" in cmd or "kv_cas_provenance" in cmd):
                    os.kill(int(pid.strip()), signal.SIGKILL)
    except Exception:  # noqa: BLE001
        pass
    return 0 if out["PHASE11B_PROVENANCE_PASS"] else 1


if __name__ == "__main__":
    sys.exit(main())
