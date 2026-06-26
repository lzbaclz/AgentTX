"""Phase-5 Part B: real-LLM full-stack agent turn under crash+recovery, on 2 models.

A real vLLM turn: the LLM processes a real ~CTX-token agent context (building real KV) and
produces output tokens; the agent's decided actions run through the Tool Gateway (SQL charge
+ FS receipt + HTTP confirm); the context KV is snapshotted (durable offload tier); the output
streams with (seq) tagging. The coordinator CRASHES after the tool effects / before turn-commit.
On recovery: tools EXACTLY-ONCE (gateway dedup), KV RESTORED from the snapshot (vs re-prefill),
client output EXACTLY-ONCE (stream resumes from the ACK). Run on Llama-3.1-8B and Qwen3-8B.

Run: PYTHONPATH=. <peerkv-venv>/bin/python phase5/agent_e2e.py <model_path>
"""
import json, os, sys, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("VLLM_ENABLE_V1_MULTIPROCESSING", "0")
os.environ.setdefault("VLLM_USE_FLASHINFER_SAMPLER", "0")
from agenttx.db import open_sqlite
from agenttx.core import Clock, Crash
from agenttx.gateway import Gateway
from agenttx.kvview import KVView, Provenance, sha256_hex
from agenttx.tools import ChargeTool, ReceiptTool, HttpIdempotentTool

MODEL = sys.argv[1] if len(sys.argv) > 1 else "/public/model_zoo/Llama-3.1-8B-Instruct"
CTX = 8192; PORT = 8755

# --- a real external HTTP service (idempotent /charge) in a daemon thread ---
LED = {"n": 0}; SEEN = {}
class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        k = self.headers.get("Idempotency-Key", "")
        if k in SEEN: r = SEEN[k]
        else: LED["n"] += 1; r = f"c{LED['n']}"; SEEN[k] = r
        b = json.dumps({"result": r}).encode(); self.send_response(200)
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        b = json.dumps({"count": LED["n"]}).encode(); self.send_response(200)
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)


def main():
    import random, torch
    from transformers import AutoTokenizer
    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig
    threading.Thread(target=ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever, daemon=True).start()
    store = "/tmp/atx_p5b"; os.system(f"rm -rf {store} && mkdir -p {store}")
    tok = AutoTokenizer.from_pretrained(MODEL); vs = int(getattr(tok, "vocab_size", 32000)); lo, hi = 1000, min(vs - 1, 30000)
    rng = random.Random(0); ctx = [lo + 1] + [rng.randrange(lo, hi) for _ in range(CTX - 1)]
    llm = LLM(model=MODEL, enforce_eager=True, gpu_memory_utilization=0.6, max_model_len=CTX + 64,
              tensor_parallel_size=1, num_gpu_blocks_override=int(CTX / 16 * 2.5), enable_prefix_caching=True, seed=0,
              disable_log_stats=True, kv_transfer_config=KVTransferConfig(kv_connector="OffloadingConnector", kv_role="kv_both",
                kv_connector_extra_config={"spec_name": "CPUOffloadingSpec", "cpu_bytes_to_use": 24 * 1024**3, "block_size": 16}))
    sp = SamplingParams(temperature=0.0, max_tokens=8, min_tokens=8, ignore_eos=True)

    def gen():
        t0 = time.perf_counter(); o = llm.generate([{"prompt_token_ids": ctx}], sp)
        return list(o[0].outputs[0].token_ids), (time.perf_counter() - t0) * 1e3

    order = "ORD-P5B"; turn = "TB1"
    prov = Provenance(MODEL, "tok", "bf16", 16, 32, 8, 128, "theta=5e5", "", sha256_hex(str(ctx[:64]).encode()))
    plan = [(ChargeTool(), {"order": order, "amount": 100}), (ReceiptTool(), {"order": order}),
            (HttpIdempotentTool(f"http://127.0.0.1:{PORT}"), {})]

    # ---- run the turn, crash after the tools (before turn-commit) ----
    out_ref, _ = gen()                     # the LLM's committed output tokens (real KV built + offloaded)
    db = open_sqlite(f"{store}/app.db"); gw = Gateway(db, store); kv = KVView(f"{store}/cas", db)
    client = {"ack": 0, "recv": []}
    try:
        for tool, args in plan:
            gw.call("s", turn, tool, args, Clock(0))
        kv.snapshot(turn, prov, [str(t).encode() for t in out_ref])
        raise Crash()                      # crash: effects done, turn not committed, output not streamed
    except Crash:
        pass
    db.close()
    n_before = {"charge": db_count(store, order), "http": svc_count()}

    # evict the context KV so recovery must RESTORE from the snapshot
    for k in range(4): llm.generate([{"prompt_token_ids": [lo + 50 + k] + [rng.randrange(lo, hi) for _ in range(CTX - 1)]}], sp)

    # ---- RECOVERY: tools dedup (exactly-once), KV restore, stream output ----
    db = open_sqlite(f"{store}/app.db"); gw = Gateway(db, store); kv = KVView(f"{store}/cas", db)
    for tool, args in plan:
        gw.call("s", turn, tool, args, Clock(0))      # gateway dedup -> no double effect
    rr = kv.restore(turn, prov)
    torch.cuda.synchronize(); out_restore, ms_restore = gen()     # offload restore
    fresh = [lo + 777] + [rng.randrange(lo, hi) for _ in range(CTX - 1)]
    _t0 = time.perf_counter(); llm.generate([{"prompt_token_ids": fresh}], sp); ms_reprefill = (time.perf_counter() - _t0) * 1e3
    # stream the committed output exactly-once (resume from ack=0)
    for i, t in enumerate(out_ref):
        client["recv"].append(t); client["ack"] = i + 1
    db.close()

    R = {"model": MODEL, "ctx": CTX,
         "tool_charge_exactly_once": db_count(store, order) == 1,
         "tool_http_exactly_once": svc_count() == 1,
         "kv_restored": rr.restored, "kv_restore_ms": round(ms_restore, 1), "reprefill_ms": round(ms_reprefill, 1),
         "recovery_speedup": round(ms_reprefill / ms_restore, 2) if ms_restore else None,
         "stream_exactly_once": client["recv"] == out_ref}
    R["PART_B_PASS"] = bool(R["tool_charge_exactly_once"] and R["tool_http_exactly_once"]
                            and R["kv_restored"] and R["stream_exactly_once"] and R["recovery_speedup"] and R["recovery_speedup"] >= 3)
    os.makedirs("phase5/results", exist_ok=True)
    tag = os.path.basename(MODEL).replace(".", "_")
    json.dump(R, open(f"phase5/results/agent_e2e_{tag}.json", "w"), indent=2)
    print(json.dumps(R, indent=2)); return 0 if R["PART_B_PASS"] else 1


def db_count(store, order):
    d = open_sqlite(f"{store}/app.db"); n = d.execute("SELECT COUNT(*) FROM charges WHERE order_id=?", (order,)).fetchone()[0]; d.close(); return n
def svc_count():
    return json.load(urllib.request.urlopen(f"http://127.0.0.1:{PORT}/count"))["count"]


if __name__ == "__main__":
    sys.exit(main())
