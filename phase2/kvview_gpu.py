"""KV-View on REAL GPU KV-block-shaped tensors: byte-exact round-trip + fault injection.
Snapshot real KV blocks (GPU->host bytes) into the content-addressed store, then restore
(verify -> host->GPU) and assert torch.equal; corrupt a CAS block / break provenance and
assert FAIL_CLOSED (the engine would recompute). Proves the KV-View on actual GPU KV bytes.
"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import torch
from agenttx.kvview import KVView, Provenance

PAGE = 2*1024*1024            # one all-layers KV block (Llama-3.1-8B per-block ~2MB)
NBLOCKS = 24                  # a ~384-token prefix's worth of KV

def prov(prefix="ctx-1"):
    return Provenance(model="/public/model_zoo/Llama-3.1-8B-Instruct", tokenizer="llama3.1",
                      dtype="bfloat16", block_size=16, num_layers=32, num_kv_heads=8,
                      head_dim=128, rope="theta=500000,scaling=none", adapter="", prefix_hash=prefix)

def main():
    tmp="/tmp/atx_kvview_gpu"; os.system(f"rm -rf {tmp}")
    dev="cuda:0"
    # real KV-block-shaped GPU tensors with deterministic content (the "live" prefix KV)
    g = torch.arange(NBLOCKS*PAGE//2, dtype=torch.float16, device=dev)
    blocks_gpu = [g[i*PAGE//2:(i+1)*PAGE//2] for i in range(NBLOCKS)]
    kv = KVView(f"{tmp}/cas")
    p = prov()

    # snapshot: GPU block -> host bytes (byte-exact) -> content-addressed CAS + manifest
    torch.cuda.synchronize()
    blocks_bytes = [b.cpu().numpy().tobytes() for b in blocks_gpu]
    m = kv.snapshot("turn-1", p, blocks_bytes)

    res = {"n_blocks": NBLOCKS, "block_MB": PAGE/2**20, "total_MB": round(m["total_bytes"]/2**20,1)}

    # 1) VALID restore -> write verified bytes back to a FRESH GPU buffer -> byte-exact
    r = kv.restore("turn-1", p)
    g2 = torch.zeros_like(g)
    blk2 = [g2[i*PAGE//2:(i+1)*PAGE//2] for i in range(NBLOCKS)]
    for i, bb in enumerate(r.blocks):
        blk2[i].copy_(torch.frombuffer(bytearray(bb), dtype=torch.float16).to(dev))
    torch.cuda.synchronize()
    res["valid_restore_byte_exact"] = bool(r.restored and torch.equal(g, g2))

    # 2) CORRUPT a stored block -> FAIL_CLOSED (engine must recompute, never use bad KV)
    h = m["blocks"][7]; path = kv.cas._path(h)
    raw = bytearray(open(path,"rb").read()); raw[12345]^=0xFF; open(path,"wb").write(raw)
    rc = kv.restore("turn-1", p)
    res["corrupt_block_fail_closed"] = bool(not rc.restored and rc.reason=="block_corrupt_or_missing")

    # 3) PROVENANCE mismatch (different prefix / RoPE) -> FAIL_CLOSED
    rp = kv.restore("turn-1", prov(prefix="DIFFERENT-CTX"))
    res["provenance_mismatch_fail_closed"] = bool(not rp.restored and rp.reason=="provenance_mismatch")

    res["KVVIEW_GPU_PASS"] = bool(res["valid_restore_byte_exact"] and res["corrupt_block_fail_closed"]
                                 and res["provenance_mismatch_fail_closed"])
    os.makedirs("phase2/results", exist_ok=True)
    json.dump(res, open("phase2/results/kvview_gpu.json","w"), indent=2)
    print(json.dumps(res, indent=2))
    return 0 if res["KVVIEW_GPU_PASS"] else 1

if __name__=="__main__":
    sys.exit(main())
