# Phase 11 — durable KV → a fresh worker's ATTENTION (the last TARGET, closed)

The advisor's deepest open item: the KV speedups were same-process CPU-offload; durable KV bytes
were not loaded into a FRESH vLLM worker's attention to resume decoding. This closes it with a
**custom-configured vLLM KV connector backed by a durable content-addressed store**.

## Design (no vLLM patch — config + the built-in extension points)
`OffloadingConnector` + `spec_name=TieringOffloadingSpec` + a secondary `fs_python` tier whose
`root_dir` is a durable on-disk CAS. The fs tier content-addresses each KV block as `…/<hash>.bin`
via atomic rename, and its `lookup()` is `os.path.exists` — so the **filesystem itself is the
cross-process index**. The tiering manager's `lookup()` cascades primary(CPU)→secondary(fs) on a
miss and promotes fs→CPU→GPU.

## The subtle bug that made it silently fail (and the fix)
vLLM seeds its block-hash chain (`NONE_HASH`, `v1/core/kv_cache_utils.py:110`) with
`os.urandom(32)` when `PYTHONHASHSEED` is unset → **every process computes different block hashes**
→ a fresh worker's hashes never match the CAS filenames → 0 cross-process hits (we measured
`warm_num_cached_tokens=0`). **Fix: pin `PYTHONHASHSEED` in both processes** → deterministic block
hashes → the CAS hits cross-process. (A real, non-obvious systems insight for durable KV reuse.)

## Results
- **Smoke** (`phase11/kv_cas_smoke.py`): the connector spills KV to the durable CAS — **1664 `.bin`
  blocks, 3.5 GB**, content-addressed, 0 "cannot store".
- **Cross-process** (`phase11/kv_cas_xproc.py`): producer fills the CAS then is hard-`SIGKILL`ed;
  a **FRESH vLLM engine on a DIFFERENT GPU** requests the same context →
  **`warm_num_cached_tokens = 2048` (full hit), cold = 0**, output valid, **1.45× faster than cold
  reprefill** (169 ms vs 245 ms) even at 2K. The speedup grows with context (cf Gate-1b
  1.8×@4K → 17×@32K); at 2K the win is already positive.

**PHASE11_KV_CAS_PASS = true.** The KV materialized view is now durable AND loads into a fresh
worker's attention across a real cross-process / cross-device crash — not just a same-process proxy.

## Remaining (honest)
The fs tier is keyed purely by token-content hash; AgentTx provenance (model/dtype/RoPE/LoRA)
fail-closed verification on top of the CAS, and turn-LSN-addressed manifests, are a thin wrapper
left as a follow-up (the path namespaces by model dir, giving partial provenance already).
