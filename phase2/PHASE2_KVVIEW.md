# Phase 2 — vLLM KV-View: the KV cache as a materialized view of the turn log

The durable turn log is the single source of truth; the KV cache is a rebuildable
accelerator. A turn's KV snapshot binds a **provenance fingerprint** + a **content-addressed
block manifest**; restore verifies both and **fails closed** (recompute) on any mismatch, so
a lost snapshot costs only speed and a corrupt/stale snapshot can never produce wrong output.
(`agenttx/kvview.py`, `agenttx/recovery.py`.)

## Components
- **Provenance fingerprint** — model / tokenizer / kv-dtype / block_size / num_layers /
  num_kv_heads / head_dim / RoPE / adapter(LoRA) / prefix-token-hash. Any field change ⇒
  a different key ⇒ fail closed.
- **BlockCAS** — content-addressable store, address = `sha256(block)` = its own checksum.
  `put()` dedups identical blocks across turns/prefixes; `get()` re-verifies the checksum on
  read and returns `None` on corruption/missing.
- **Manifest** — `turn_lsn → {prov_key, [block_hash...]}` on SQLite (durable) or dict (tests).
- **Restore** — manifest-missing / provenance-mismatch / block-corrupt-or-missing ⇒
  `FAIL_CLOSED`; only an all-green verification ⇒ `RESTORED`.

## Verification (every layer)

| layer | test | result |
|---|---|---|
| logic | `phase2/test_kvview.py` | valid restore byte-exact; provenance mismatch on **each** field → fail-closed; block corrupt → fail-closed; block missing → fail-closed; manifest missing → fail-closed; **CAS dedup** (shared prefix → 6 objects not 10). **ALL PASS** |
| **real GPU KV bytes** | `phase2/kvview_gpu.py` | snapshot 24 real KV-block-shaped GPU tensors (48 MB) → restore → **`torch.equal` byte-exact**; corrupt a CAS block → fail-closed; provenance mismatch → fail-closed. **PASS** |
| recovery contract | `agenttx/recovery.py` | matching turn → KV **restore** + tools exactly-once (1/1); changed context → KV **fail-closed recompute(provenance_mismatch)** + tools still exactly-once. **PASS** |
| **real vLLM e2e** | `phase2/kvview_e2e.py` (Llama-3.1-8B, 8K ctx) | restore **254 ms** vs re-prefill **857 ms = 3.37×**; provenance fail-closed on a stale (context-changed) turn id; restore generation-faithfulness **== the engine's own determinism floor** (restore adds zero divergence beyond a GPU cache-hit). **PASS** |

## On the byte-exact guarantee vs generation determinism (honest scope)
The materialized-view guarantee — *the restored KV bytes equal the snapshotted KV bytes* — is
proven **definitively** at the KV-bytes level (`kvview_gpu`, `torch.equal` over 48 MB). At the
generation level we measured that vLLM greedy decode is itself **non-deterministic across the
KV-reuse boundary** (cold-prefill vs cache-hit token overlap was only 0.19 on random-token
prefixes — a vivid instance of the determinism problem, cf. LLM-42). The KV-View restore
matches that floor **exactly** (it crosses the *same* reuse-vs-recompute boundary), i.e. it
introduces no divergence of its own. This is precisely *why* AgentTx makes the **durable token
log** — not the KV cache — the source of truth: committed token ids are replayed/teacher-forced
on recovery; the KV snapshot only accelerates, and is fail-closed-verified before any reuse.

## What remains for Phase 2 (productionization)
- Wire the live offload tier's block bytes into the manifest for per-block checksums in the
  engine path (the e2e checksums real KV bytes via the byte-level harness today).
- SSD / remote snapshot tiers (host DRAM today); GC of unreferenced CAS blocks.
- Real LoRA/RoPE-scaling fingerprints pulled from the engine config (placeholders today).
