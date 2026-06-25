"""AgentTx KV-View: the KV cache as a MATERIALIZED VIEW of the durable turn log.

Design principle: the durable turn log is the single source of truth; the KV cache is a
rebuildable accelerator. A turn's KV snapshot binds (1) a PROVENANCE fingerprint
(model/tokenizer/dtype/RoPE/adapter/geometry/prefix-hash) and (2) a content-addressed
block manifest (turn_lsn -> [sha256(block)...]). Restore verifies BOTH:
  * provenance mismatch        -> FAIL_CLOSED (recompute from transcript)
  * any block missing/corrupt  -> FAIL_CLOSED (recompute)
  * manifest missing           -> FAIL_CLOSED (recompute)
Only an all-green verification RESTORES the KV. So a lost snapshot costs speed, never
correctness; a corrupt/stale snapshot can NEVER produce wrong output -- it is detected and
the engine recomputes. Content addressing also dedups blocks shared across turns/prefixes.
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass


def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


@dataclass(frozen=True)
class Provenance:
    """Everything that must match for a restored KV to be valid for the current engine."""
    model: str            # model identity (path/hash)
    tokenizer: str        # tokenizer identity/hash
    dtype: str            # kv dtype (e.g. bfloat16)
    block_size: int       # tokens per KV block
    num_layers: int
    num_kv_heads: int
    head_dim: int
    rope: str             # serialized RoPE config (theta + scaling)
    adapter: str          # LoRA/adapter id ("" if none)
    prefix_hash: str      # sha256 of the committed prefix token ids (the turn's context)

    def key(self) -> str:
        return sha256_hex(json.dumps(asdict(self), sort_keys=True).encode())


class BlockCAS:
    """Content-addressable block store: address = sha256(content) = its own checksum.
    put() dedups (identical blocks across turns/prefixes stored once); get() re-verifies the
    checksum on read and returns None on corruption/missing."""
    def __init__(self, root: str):
        self.root = root
        os.makedirs(root, exist_ok=True)

    def _path(self, h):
        return os.path.join(self.root, h[:2], h)

    def put(self, content: bytes) -> str:
        h = sha256_hex(content)
        p = self._path(h)
        if not os.path.exists(p):
            os.makedirs(os.path.dirname(p), exist_ok=True)
            tmp = p + ".tmp"
            with open(tmp, "wb") as f:
                f.write(content); f.flush(); os.fsync(f.fileno())
            os.replace(tmp, p)          # atomic publish
        return h

    def get(self, h: str):
        p = self._path(h)
        if not os.path.exists(p):
            return None                  # missing
        b = open(p, "rb").read()
        if sha256_hex(b) != h:
            return None                  # checksum mismatch -> corrupt
        return b

    def exists(self, h):
        return os.path.exists(self._path(h))


@dataclass
class RestoreResult:
    status: str        # "RESTORED" | "FAIL_CLOSED"
    reason: str        # "" | provenance_mismatch | block_corrupt_or_missing | manifest_missing
    blocks: list       # verified block bytes (when RESTORED), else []

    @property
    def restored(self):
        return self.status == "RESTORED"


class ManifestStore:
    """turn_lsn -> manifest JSON, on SQLite (durable) or a dict (tests)."""
    def __init__(self, db=None):
        self._db = db
        if db is not None:
            db.execute("CREATE TABLE IF NOT EXISTS kv_manifest(turn_lsn TEXT PRIMARY KEY, manifest TEXT)")
            db.commit()
        else:
            self._mem = {}

    def put(self, turn_lsn, manifest_json):
        if self._db is not None:
            self._db.execute(
                "INSERT INTO kv_manifest(turn_lsn,manifest) VALUES(?,?) "
                "ON CONFLICT(turn_lsn) DO UPDATE SET manifest=excluded.manifest",
                (str(turn_lsn), manifest_json))
            self._db.commit()
        else:
            self._mem[str(turn_lsn)] = manifest_json

    def get(self, turn_lsn):
        if self._db is not None:
            r = self._db.fetchone("SELECT manifest FROM kv_manifest WHERE turn_lsn=?", (str(turn_lsn),))
            return r[0] if r else None
        return self._mem.get(str(turn_lsn))


class KVView:
    def __init__(self, cas_root, manifest_db=None):
        self.cas = BlockCAS(cas_root)
        self.manifests = ManifestStore(manifest_db)

    def snapshot(self, turn_lsn, prov: Provenance, blocks: list) -> dict:
        """Store the turn's KV blocks content-addressed (dedup + checksum) and write a
        manifest binding the provenance fingerprint. Returns the manifest."""
        hashes = [self.cas.put(b) for b in blocks]
        manifest = {"turn_lsn": str(turn_lsn), "prov": asdict(prov), "prov_key": prov.key(),
                    "blocks": hashes, "n_blocks": len(hashes),
                    "total_bytes": sum(len(b) for b in blocks)}
        self.manifests.put(turn_lsn, json.dumps(manifest))
        return manifest

    def restore(self, turn_lsn, current: Provenance) -> RestoreResult:
        """Verify provenance + every block checksum; RESTORE only if all-green, else
        FAIL_CLOSED (the caller must recompute from the transcript)."""
        raw = self.manifests.get(turn_lsn)
        if raw is None:
            return RestoreResult("FAIL_CLOSED", "manifest_missing", [])
        m = json.loads(raw)
        if m["prov_key"] != current.key():
            return RestoreResult("FAIL_CLOSED", "provenance_mismatch", [])
        out = []
        for h in m["blocks"]:
            b = self.cas.get(h)                       # re-verifies sha256 on read
            if b is None:
                return RestoreResult("FAIL_CLOSED", "block_corrupt_or_missing", [])
            out.append(b)
        return RestoreResult("RESTORED", "", out)


def prefix_hash(token_ids) -> str:
    import struct
    return sha256_hex(b"".join(struct.pack("<i", int(t)) for t in token_ids))
