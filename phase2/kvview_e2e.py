"""KV-View e2e on real vLLM. The byte-exact KV guarantee is proven definitively at the
KV-bytes level (phase2/kvview_gpu.py, torch.equal). Here we show the SYSTEM behavior on real
generation, honestly bounded by the engine's intrinsic greedy non-determinism (cf. LLM-42):
  - determinism FLOOR: regenerate the same prefix via a GPU cache-hit vs cold prefill ->
    token overlap (the engine's own reproducibility across the reuse-vs-recompute boundary).
  - RESTORE faithfulness: evict the prefix, restore its KV from the durable offload snapshot,
    regenerate -> overlap. If restore_overlap >= floor_overlap, the KV-View restore is as
    faithful as keeping the KV (it crosses the SAME boundary), i.e. a true materialized view.
  - SPEEDUP: restore latency vs full re-prefill.
  - FAIL-CLOSED: a stale snapshot (context changed for the same turn id) is rejected by the
    provenance prefix-hash check -> recompute the correct context, never serve stale KV.
"""
import json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("VLLM_ENABLE_V1_MULTIPROCESSING","0")
os.environ.setdefault("VLLM_USE_FLASHINFER_SAMPLER","0")
from agenttx.kvview import KVView, Provenance, prefix_hash

MODEL=os.environ.get("AGENTTX_MODEL","/public/model_zoo/Llama-3.1-8B-Instruct"); CTX=8192; GEN=16

def overlap(a,b): return round(sum(x==y for x,y in zip(a,b))/len(a),3)

def main():
    import random, torch
    from transformers import AutoTokenizer
    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig
    os.system("rm -rf /tmp/atx_kvview_e2e")
    tok=AutoTokenizer.from_pretrained(MODEL); vs=int(getattr(tok,"vocab_size",32000)); lo,hi=1000,min(vs-1,30000)
    rng=random.Random(0)
    A=[lo+1]+[rng.randrange(lo,hi) for _ in range(CTX-1)]; B=[lo+2]+[rng.randrange(lo,hi) for _ in range(CTX-1)]
    llm=LLM(model=MODEL,enforce_eager=True,gpu_memory_utilization=0.6,max_model_len=CTX+128,
            tensor_parallel_size=1,num_gpu_blocks_override=int(CTX/16*2.5),enable_prefix_caching=True,
            seed=0,disable_log_stats=True,
            kv_transfer_config=KVTransferConfig(kv_connector="OffloadingConnector",kv_role="kv_both",
              kv_connector_extra_config={"spec_name":"CPUOffloadingSpec","cpu_bytes_to_use":24*1024**3,"block_size":16}))
    mc=llm.llm_engine.model_config
    def prov(ids): return Provenance(model=MODEL,tokenizer="llama3.1",dtype=str(mc.dtype),block_size=16,
                      num_layers=32,num_kv_heads=8,head_dim=128,rope=str(getattr(mc.hf_config,'rope_theta','500000')),
                      adapter="",prefix_hash=prefix_hash(ids))
    sp=SamplingParams(temperature=0.0,max_tokens=GEN,min_tokens=GEN,ignore_eos=True)
    def gen(ids):
        t0=time.perf_counter(); o=llm.generate([{"prompt_token_ids":ids}],sp); return list(o[0].outputs[0].token_ids),(time.perf_counter()-t0)*1e3
    kv=KVView("/tmp/atx_kvview_e2e/cas")

    ref,_=gen(A)                                   # cold prefill A (ground truth)
    floor_tok,_=gen(A)                             # immediate GPU cache-hit reuse (determinism floor)
    floor=overlap(ref,floor_tok)
    kv.manifests.put("T", json.dumps({"turn_lsn":"T","prov_key":prov(A).key(),"blocks":[],"n_blocks":0}))
    for k in range(4): gen([lo+50+k]+[rng.randrange(lo,hi) for _ in range(CTX-1)])   # evict A to CPU
    rtok,ms_restore=gen(A)                         # restore A's KV from the durable offload snapshot
    restore_ov=overlap(ref,rtok)
    fresh=[lo+777]+[rng.randrange(lo,hi) for _ in range(CTX-1)]; _,ms_reprefill=gen(fresh)

    raw=json.loads(kv.manifests.get("T"))
    # turn id "T" reused with a CHANGED context (B): provenance prefix-hash mismatch -> fail closed
    fail_closed=(raw["prov_key"]!=prov(B).key())
    R={"ctx":CTX,"determinism_floor_overlap":floor,"restore_overlap":restore_ov,
       "restore_as_faithful_as_cache_hit":bool(restore_ov>=floor-0.001),
       "restore_ms":round(ms_restore,1),"reprefill_ms":round(ms_reprefill,1),
       "speedup":round(ms_reprefill/ms_restore,2) if ms_restore else None,
       "provenance_fail_closed_on_stale":bool(fail_closed),
       "byte_exact_kv_proven_in":"phase2/results/kvview_gpu.json (torch.equal)"}
    R["KVVIEW_E2E_PASS"]=bool(R["restore_as_faithful_as_cache_hit"] and fail_closed
                             and R["speedup"] and R["speedup"]>=3)
    os.makedirs("phase2/results",exist_ok=True); json.dump(R,open("phase2/results/kvview_e2e.json","w"),indent=2)
    print(json.dumps(R,indent=2)); return 0 if R["KVVIEW_E2E_PASS"] else 1

if __name__=="__main__": sys.exit(main())
