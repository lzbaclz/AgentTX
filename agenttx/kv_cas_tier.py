"""AgentTx provenance-gated, fail-closed durable KV CAS tier for vLLM.

A thin layer over vLLM's built-in `fs_python` filesystem secondary tier (a durable, content-
addressed on-disk KV store). It adds AgentTx's KV-View semantics: a fresh worker only LOADS KV
from the durable CAS if the store's PROVENANCE fingerprint (model / dtype / RoPE / adapter /
tokenizer / block geometry) matches the current run. On any mismatch it FAILS CLOSED -- reports
no hit, so vLLM recomputes -- so KV produced under a different model/config is never silently
loaded into attention (which would be wrong).

Registered as secondary tier type "agenttx_cas". To make vLLM's EngineCore import + register it,
point the offloading spec at THIS module via the spec factory's import hook:

    kv_connector_extra_config = {
        "spec_name": "AgentTxTieringSpec",          # resolved from this module
        "spec_module_path": "agenttx.kv_cas_tier",  # EngineCore imports this -> runs register_tier
        "cpu_bytes_to_use": <bytes>, "block_size": 16,
        "secondary_tiers": [{"type": "agenttx_cas", "root_dir": <durable CAS>,
                             "provenance": {"model": ..., "dtype": ..., "rope": ..., "adapter": ...}}],
    }
"""
from __future__ import annotations

import hashlib
import json
import os

from vllm.logger import init_logger
from vllm.v1.kv_offload.tiering.factory import SecondaryTierFactory
from vllm.v1.kv_offload.tiering.fs.manager import FileSystemTierManager
from vllm.v1.kv_offload.tiering.spec import TieringOffloadingSpec

logger = init_logger(__name__)

PROV_SIDECAR = "agenttx_provenance.json"


def provenance_fingerprint(prov: dict | None) -> str:
    return hashlib.sha256(json.dumps(prov or {}, sort_keys=True, default=str).encode()).hexdigest()


class AgentTxCASTier(FileSystemTierManager):
    """fs_python durable CAS + AgentTx provenance fail-closed gate."""

    def __init__(self, offloading_spec, primary_kv_view, tier_type, root_dir, provenance=None, **kw):
        super().__init__(offloading_spec, primary_kv_view, tier_type, root_dir, **kw)
        self._fp = provenance_fingerprint(provenance)
        self._fail_closed = False
        self._reason = "ok"
        sidecar = os.path.join(root_dir, PROV_SIDECAR)
        try:
            os.makedirs(root_dir, exist_ok=True)
            if os.path.exists(sidecar):
                stored = json.load(open(sidecar))
                if stored.get("fingerprint") != self._fp:
                    self._fail_closed = True
                    self._reason = "provenance_mismatch"
            else:
                tmp = sidecar + ".tmp"
                with open(tmp, "w") as f:
                    json.dump({"fingerprint": self._fp, "provenance": provenance or {}}, f)
                    f.flush(); os.fsync(f.fileno())
                os.replace(tmp, sidecar)               # atomic publish of the provenance record
        except Exception as e:                          # noqa: BLE001 -- any provenance error => fail closed
            self._fail_closed = True
            self._reason = f"provenance_error:{type(e).__name__}"
        if self._fail_closed:
            logger.warning("AgentTxCASTier FAIL-CLOSED (%s): refusing ALL durable-CAS loads from %s "
                           "-> vLLM will recompute KV.", self._reason, root_dir)
        else:
            logger.info("AgentTxCASTier provenance OK (fp=%s) at %s", self._fp[:12], root_dir)

    def lookup(self, key, req_context=None):
        if self._fail_closed:
            return False                                # never load KV under a mismatched provenance
        return super().lookup(key, req_context)


class AgentTxTieringSpec(TieringOffloadingSpec):
    """Identical to TieringOffloadingSpec; exists so the spec factory's `spec_module_path` import
    loads THIS module in the EngineCore process, which registers the agenttx_cas tier below."""


# Register the tier when this module is imported (idempotent across the spec being created twice).
if "agenttx_cas" not in SecondaryTierFactory._registry:
    SecondaryTierFactory.register_tier("agenttx_cas", "agenttx.kv_cas_tier", "AgentTxCASTier")
