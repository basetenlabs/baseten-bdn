"""One resolved volume version and the verified object reads a file's bytes take."""

from __future__ import annotations

from dataclasses import dataclass

from baseten.bdn.volumes import _manifest, _s3
from baseten.bdn.volumes._errors import VolumeIntegrityError
from baseten.bdn.volumes._manifest import (
    ChunkEntry,
    ChunkFileEntry,
    FileEntry,
    Manifest,
)
from baseten.bdn.volumes._ref import VolumeRef


@dataclass(frozen=True)
class Version:
    """One resolved version: its pin, where its objects live, and its manifest."""

    ref: VolumeRef
    """The volume pinned to this version, with no path."""

    org_id: str
    store: _s3.ObjectStore
    manifest: Manifest

    def key(self, relative_key: str) -> str:
        return object_key(self.org_id, self.ref.namespace, relative_key)

    def chunks(self, entry: FileEntry) -> list[ChunkEntry]:
        """A file's chunks in offset order, reading and verifying its chunkmap if it has one."""
        if isinstance(entry, ChunkFileEntry):
            # An empty file's chunk is the empty digest; nothing to fetch.
            return [entry.chunk] if entry.chunk.length else []
        body, content_type = self.store.get(self.key(entry.target.relative_key))
        data = _s3.decode_object(
            body, content_type, expected_kind="chunkmap", expected_digest=entry.digest
        )
        return list(_manifest.parse_chunkmap(data, entry.size))

    def read_chunk(self, chunk: ChunkEntry) -> bytes:
        """One chunk's decoded bytes, verified against its recorded digest and length.

        The stored body and the decoded bytes coexist until this returns, so
        callers bounding memory charge twice the chunk's length.
        """
        body, content_type = self.store.get(self.key(chunk.target.relative_key))
        data = _s3.decode_object(
            body, content_type, expected_kind="chunk", expected_digest=chunk.digest
        )
        if len(data) != chunk.length:
            raise VolumeIntegrityError(
                f"chunk {chunk.digest} is {len(data)} bytes, the record says {chunk.length}"
            )
        return data


def object_key(org_id: str, namespace: str, relative_key: str) -> str:
    return f"bdn/{org_id}/{namespace}/{relative_key}"
