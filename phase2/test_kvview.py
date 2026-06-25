"""KV-View unit tests: exhaustive fail-closed coverage + dedup + byte-exact valid restore."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agenttx.kvview import KVView, Provenance, BlockCAS, prefix_hash

def prov(prefix="p1", model="m", rope="theta=500000", adapter=""):
    return Provenance(model=model, tokenizer="tok", dtype="bfloat16", block_size=16,
                      num_layers=32, num_kv_heads=8, head_dim=128, rope=rope,
                      adapter=adapter, prefix_hash=prefix)

def fresh(tmp):
    os.system(f"rm -rf {tmp} && mkdir -p {tmp}")
    return KVView(f"{tmp}/cas")

def main():
    tmp="/tmp/atx_kvview"; ok=True; results={}
    blocks=[os.urandom(2*1024*1024) for _ in range(8)]   # 8 real KV-block-sized blobs (2MB)
    p=prov()

    # 1) valid restore is byte-exact
    kv=fresh(tmp); kv.snapshot("t1", p, blocks)
    r=kv.restore("t1", p)
    c1 = r.restored and r.blocks==blocks
    results["valid_restore_byte_exact"]=c1; ok&=c1

    # 2) provenance mismatch on EACH field -> fail closed
    for field,bad in [("prefix",prov(prefix="DIFFERENT")),("model",prov(model="other")),
                      ("rope",prov(rope="theta=10000")),("adapter",prov(adapter="lora-A"))]:
        r=kv.restore("t1", bad)
        c = (not r.restored) and r.reason=="provenance_mismatch"
        results[f"provenance_{field}_fail_closed"]=c; ok&=c

    # 3) block corruption -> fail closed (flip a byte in a stored CAS object)
    kv=fresh(tmp); m=kv.snapshot("t1", p, blocks)
    h=m["blocks"][3]; path=kv.cas._path(h)
    raw=bytearray(open(path,"rb").read()); raw[100]^=0xFF; open(path,"wb").write(raw)
    r=kv.restore("t1", p)
    c = (not r.restored) and r.reason=="block_corrupt_or_missing"
    results["block_corruption_fail_closed"]=c; ok&=c

    # 4) block missing -> fail closed
    kv=fresh(tmp); m=kv.snapshot("t1", p, blocks)
    os.remove(kv.cas._path(m["blocks"][5]))
    r=kv.restore("t1", p)
    c = (not r.restored) and r.reason=="block_corrupt_or_missing"
    results["block_missing_fail_closed"]=c; ok&=c

    # 5) manifest missing -> fail closed
    kv=fresh(tmp)
    r=kv.restore("nonexistent", p)
    c = (not r.restored) and r.reason=="manifest_missing"
    results["manifest_missing_fail_closed"]=c; ok&=c

    # 6) CAS dedup: two turns sharing a prefix's blocks store shared blocks ONCE
    kv=fresh(tmp)
    shared=blocks[:4]; t1b=shared+[os.urandom(2*1024*1024)]; t2b=shared+[os.urandom(2*1024*1024)]
    kv.snapshot("t1", prov(prefix="t1"), t1b); kv.snapshot("t2", prov(prefix="t2"), t2b)
    import glob; n_cas=len(glob.glob(f"{tmp}/cas/*/*"))
    c = n_cas==6   # 4 shared + 1 unique each = 6, not 10
    results["cas_dedup_shared_prefix"]=c; results["_cas_objects"]=n_cas; ok&=c

    print("=== KV-View unit tests ===")
    for k,v in results.items(): print(f"  {k}: {v}")
    print("ALL_PASS:", ok)
    return 0 if ok else 1

if __name__=="__main__":
    sys.exit(main())
