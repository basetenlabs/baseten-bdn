"""Builds a synthetic BDN volume the way cannery stores one.

Produces canonical JSONL manifests and chunkmaps, zstd-compresses what the
origin compresses, computes real BLAKE3 digests, and serves the objects from
an httpx MockTransport alongside fake token and resolve endpoints. Tests
therefore exercise the real decode and verification paths.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass, field
from typing import Any

import httpx

from baseten.bdn.volumes import _manifest
from baseten.bdn.volumes._cannery import ObjectTarget
from baseten.bdn.volumes._s3 import digest_of

try:
    from compression import zstd as _zstd  # ty: ignore[unresolved-import]
except ImportError:
    from backports import zstd as _zstd  # ty: ignore[unresolved-import]

zstd_compress = _zstd.compress

ORG_ID = "org_2qRk4dB"
NAMESPACE = "loops"
VOLUME = "sampler-abc123"
BUCKET = "bt-bdn-origin"
REGION = "us-west-2"
API_HOST = "api.test"
BDN_HOST = "bdn.test"
S3_HOST = f"{BUCKET}.s3-accelerate.amazonaws.com"
API_KEY = "test-api-key"
CANNERY_TOKEN = "jwt.cannery.token"

CHUNK = "application/vnd.baseten.bdn.chunk.v1"
CHUNK_ZSTD = CHUNK + "+zstd"
CHUNKMAP_ZSTD = "application/vnd.baseten.bdn.chunkmap.v1+zstd"
MANIFEST_ZSTD = "application/vnd.baseten.bdn.manifest.v1+zstd"


def relative_key(digest: str) -> str:
    hex_digest = digest.removeprefix("b3:")
    return f"objects/b3/{hex_digest[:2]}/{hex_digest[2:4]}/{hex_digest}"


def full_key(digest: str) -> str:
    return f"bdn/{ORG_ID}/{NAMESPACE}/{relative_key(digest)}"


@dataclass
class File:
    data: bytes
    mode: str = "0644"
    chunk_size: int | None = None
    """Split into a chunkmap with chunks of this size; ``None`` stores one chunk."""
    link_group: int | None = None
    compress: bool = False


@dataclass
class Dir:
    mode: str = "0755"


@dataclass
class Symlink:
    target: str


@dataclass
class Volume:
    """A built volume: stored objects plus the manifest digest cannery would resolve to."""

    objects: dict[str, tuple[bytes, str]] = field(default_factory=dict)
    manifest_digest: str = ""
    manifest_lines: list[dict[str, Any]] = field(default_factory=list)

    def put(self, data: bytes, content_type: str, *, compress: bool) -> str:
        digest = digest_of(data)
        stored = zstd_compress(data) if compress else data
        self.objects[full_key(digest)] = (
            stored,
            content_type
            + ("+zstd" if compress and not content_type.endswith("+zstd") else ""),
        )
        return digest


def build_volume(tree: dict[str, File | Dir | Symlink]) -> Volume:
    volume = Volume()
    records: list[dict[str, Any]] = []
    total = 0
    for path, spec in tree.items():
        if isinstance(spec, Dir):
            records.append({"_type": "directory", "mode": spec.mode, "path": path})
        elif isinstance(spec, Symlink):
            records.append(
                {
                    "_type": "symlink",
                    "mode": "0777",
                    "path": path,
                    "target": spec.target,
                }
            )
        else:
            total += len(spec.data)
            record: dict[str, Any] = {"_type": "file", "mode": spec.mode, "path": path}
            if spec.link_group is not None:
                record["link_group"] = spec.link_group
            if spec.chunk_size is None:
                record["_kind"] = "chunk"
                if spec.data:
                    digest = volume.put(spec.data, CHUNK, compress=spec.compress)
                    record["chunk"] = {
                        "digest": digest,
                        "length": len(spec.data),
                        "offset": 0,
                        "target": {"relative_key": relative_key(digest)},
                    }
            else:
                chunks = []
                for offset in range(0, len(spec.data), spec.chunk_size):
                    piece = spec.data[offset : offset + spec.chunk_size]
                    digest = volume.put(piece, CHUNK, compress=spec.compress)
                    chunks.append(
                        {
                            "_type": "chunk",
                            "digest": digest,
                            "length": len(piece),
                            "offset": offset,
                            "target": {"relative_key": relative_key(digest)},
                        }
                    )
                chunkmap = jsonl(
                    [
                        {
                            "_type": "chunkmap_header",
                            "chunk_count": len(chunks),
                            "file_size": len(spec.data),
                        },
                        *chunks,
                    ]
                )
                digest = volume.put(
                    chunkmap, "application/vnd.baseten.bdn.chunkmap.v1", compress=True
                )
                record.update(
                    {
                        "_kind": "chunkmap",
                        "digest": digest,
                        "size": len(spec.data),
                        "target": {"relative_key": relative_key(digest)},
                    }
                )
            records.append(record)
    header = {
        "_type": "manifest_header",
        "entry_count": len(records),
        "manifest_schema": "v1",
        "total_size": total,
    }
    provenance = {
        "_type": "provenance",
        "source_fingerprint": "x",
        "source_fingerprint_type": "sha256",
        "source_uri": "s3://fixture",
    }
    volume.manifest_lines = [header, provenance, *records]
    manifest = jsonl(volume.manifest_lines)
    volume.manifest_digest = volume.put(
        manifest, "application/vnd.baseten.bdn.manifest.v1", compress=True
    )
    return volume


def jsonl(records: list[dict[str, Any]]) -> bytes:
    return b"".join(
        json.dumps(record, separators=(",", ":")).encode() + b"\n" for record in records
    )


@dataclass
class FakeServices:
    """Token endpoint, cannery resolve, and S3, all behind one MockTransport."""

    volume: Volume
    token_expires_in: dt.timedelta = dt.timedelta(hours=1)
    credentials_expire_in: dt.timedelta | None = dt.timedelta(minutes=30)
    resolve_error: tuple[int, Any] | None = None
    token_error: tuple[int, Any] | None = None
    s3_failures: dict[str, list[int]] = field(default_factory=dict)
    """Per-key list of status codes to answer with before serving the object."""
    requests: list[httpx.Request] = field(default_factory=list)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        host = request.url.host
        if host == API_HOST:
            return self._token(request)
        if host == BDN_HOST:
            return self._resolve(request)
        if host == S3_HOST:
            return self._s3(request)
        return httpx.Response(500, text=f"unexpected host {host}")

    def _token(self, request: httpx.Request) -> httpx.Response:
        if self.token_error is not None:
            status, body = self.token_error
            return httpx.Response(status, json=body)
        return httpx.Response(
            200,
            json={
                "token": CANNERY_TOKEN,
                "expires_at": (now() + self.token_expires_in).isoformat(),
                "scopes": ["PULL"],
                "namespaces": [NAMESPACE],
                "volumes": [VOLUME],
                "bdn_endpoint": f"https://{BDN_HOST}",
            },
        )

    def _resolve(self, request: httpx.Request) -> httpx.Response:
        if self.resolve_error is not None:
            status, body = self.resolve_error
            return (
                httpx.Response(status, json=body)
                if not isinstance(body, str)
                else httpx.Response(status, text=body)
            )
        ref = request.url.params["ref"]
        origin: dict[str, Any] = {
            "endpoint": "",
            "region": REGION,
            "bucket": BUCKET,
            "access_key_id": "ASIAEXAMPLE",
            "secret_access_key": "secret",
            "session_token": "sts-session-token",
        }
        if self.credentials_expire_in is not None:
            origin["expires_at"] = (now() + self.credentials_expire_in).isoformat()
        return httpx.Response(
            200,
            json={
                "resolved": {
                    "reference": ref,
                    "org_id": ORG_ID,
                    "origin_digest": self.volume.manifest_digest,
                    "kind": "manifest",
                    "target": {
                        "relative_key": relative_key(self.volume.manifest_digest)
                    },
                    "sequence": 7,
                    "resolved_from": "tag"
                    if ":" in ref
                    else "pin"
                    if "@" in ref
                    else "head",
                },
                "origin": origin,
            },
            headers={"cache-control": "no-store"},
        )

    def _s3(self, request: httpx.Request) -> httpx.Response:
        key = request.url.path.lstrip("/")
        pending = self.s3_failures.get(key)
        if pending:
            return httpx.Response(pending.pop(0), text="<Error>SlowDown</Error>")
        stored = self.volume.objects.get(key)
        if stored is None:
            return httpx.Response(404, text="<Error>NoSuchKey</Error>")
        body, content_type = stored
        return httpx.Response(
            200,
            content=body,
            headers={"content-type": content_type, "content-length": str(len(body))},
        )

    def http_client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self.handle))

    def s3_requests(self) -> list[httpx.Request]:
        return [request for request in self.requests if request.url.host == S3_HOST]

    def resolve_requests(self) -> list[httpx.Request]:
        return [request for request in self.requests if request.url.host == BDN_HOST]

    def token_requests(self) -> list[httpx.Request]:
        return [request for request in self.requests if request.url.host == API_HOST]


def now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def manifest_record(record: _manifest.PathEntry) -> dict[str, Any]:
    return json.loads(_manifest.dump_record(record))


__all__ = ["ObjectTarget"]
