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
from baseten.client import ManagementClient

from baseten.bdn.volumes import VolumeClient
from baseten.bdn.volumes._s3 import digest_of, zstd

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
CHUNKMAP = "application/vnd.baseten.bdn.chunkmap.v1"
MANIFEST = "application/vnd.baseten.bdn.manifest.v1"

EMPTY_DIGEST = digest_of(b"")
MTIME = "2026-09-15T12:34:56.123456789Z"
MTIME_NS = 1789475696_123456789


def relative_key(digest: str) -> str:
    hex_digest = digest.removeprefix("b3:")
    return f"objects/b3/{hex_digest[:2]}/{hex_digest[2:4]}/{hex_digest}"


def full_key(digest: str) -> str:
    return f"bdn/{ORG_ID}/{NAMESPACE}/{relative_key(digest)}"


def chunk_record(data: bytes, offset: int = 0) -> dict[str, Any]:
    digest = digest_of(data)
    return {
        "digest": digest,
        "length": len(data),
        "offset": offset,
        "target": {"relative_key": relative_key(digest)},
    }


@dataclass
class File:
    data: bytes
    mode: str = "0644"
    chunk_size: int | None = None
    """Split into a chunkmap with chunks of this size; ``None`` stores one chunk."""
    link_group: int | None = None
    compress: bool = False
    mtime: str | None = None


@dataclass
class Dir:
    mode: str = "0755"
    mtime: str | None = None


@dataclass
class Symlink:
    target: str


@dataclass
class Volume:
    """A built volume: stored objects plus the manifest digest cannery would resolve to."""

    objects: dict[str, tuple[bytes, str]] = field(default_factory=dict)
    manifest_digest: str = ""

    def put(self, data: bytes, content_type: str, *, compress: bool) -> str:
        digest = digest_of(data)
        stored = zstd.compress(data) if compress else data
        self.objects[full_key(digest)] = (
            stored,
            content_type + ("+zstd" if compress else ""),
        )
        return digest

    def put_manifest(self, records: list[dict[str, Any]]) -> None:
        self.objects = {
            k: v for k, v in self.objects.items() if k != full_key(self.manifest_digest)
        }
        self.manifest_digest = self.put(jsonl(records), MANIFEST, compress=True)


def build_volume(tree: dict[str, File | Dir | Symlink]) -> Volume:
    volume = Volume()
    records: list[dict[str, Any]] = []
    total = 0
    for path, spec in tree.items():
        if isinstance(spec, Dir):
            record: dict[str, Any] = {
                "_type": "directory",
                "mode": spec.mode,
                "path": path,
            }
            if spec.mtime:
                record["mtime"] = spec.mtime
            records.append(record)
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
            record = {"_type": "file", "mode": spec.mode, "path": path}
            if spec.mtime:
                record["mtime"] = spec.mtime
            if spec.link_group is not None:
                record["link_group"] = spec.link_group
            if spec.chunk_size is None:
                # Push records even an empty file as one chunk: the empty digest.
                volume.put(spec.data, CHUNK, compress=spec.compress)
                record["_kind"] = "chunk"
                record["chunk"] = chunk_record(spec.data)
            else:
                chunks = []
                for offset in range(0, len(spec.data), spec.chunk_size):
                    piece = spec.data[offset : offset + spec.chunk_size]
                    volume.put(piece, CHUNK, compress=spec.compress)
                    chunks.append({"_type": "chunk", **chunk_record(piece, offset)})
                header = {
                    "_type": "chunkmap_header",
                    "chunk_count": len(chunks),
                    "file_size": len(spec.data),
                }
                digest = volume.put(jsonl([header, *chunks]), CHUNKMAP, compress=True)
                record.update(
                    {
                        "_kind": "chunkmap",
                        "digest": digest,
                        "size": len(spec.data),
                        "target": {"relative_key": relative_key(digest)},
                    }
                )
            records.append(record)
    volume.put_manifest([manifest_header(len(records), total), PROVENANCE, *records])
    return volume


def manifest_header(entries: int, total: int = 0) -> dict[str, Any]:
    return {
        "_type": "manifest_header",
        "entry_count": entries,
        "manifest_schema": "v1",
        "total_size": total,
    }


PROVENANCE = {
    "_type": "provenance",
    "source_fingerprint": "x",
    "source_fingerprint_type": "sha256",
    "source_uri": "s3://fixture",
}


def jsonl(records: list[dict[str, Any]]) -> bytes:
    return b"".join(
        json.dumps(record, separators=(",", ":")).encode() + b"\n" for record in records
    )


def s3_error(code: str) -> str:
    return f"<?xml version='1.0'?><Error><Code>{code}</Code><Message>{code}</Message></Error>"


def api_version(digest: str, **overrides: Any) -> dict[str, Any]:
    """A version as the Baseten API's version endpoints render one."""
    return {
        "namespace": NAMESPACE,
        "volume": VOLUME,
        "version_ref": f"bdn:{NAMESPACE}/{VOLUME}@{digest}",
        "digest": digest,
        "sequence": 7,
        "lifecycle": "ALIVE",
        "is_head": True,
        "tags": ["step-100"],
        "total_size_bytes": 1234,
        "created_at": "2026-09-15T12:34:56Z",
        "tombstoned_at": None,
        "delete_after": None,
        **overrides,
    }


def api_volume(name: str, head_digest: str | None, **overrides: Any) -> dict[str, Any]:
    """A volume as the Baseten API's volume endpoints render one."""
    head = (
        None
        if head_digest is None
        else {
            "digest": head_digest,
            "total_size_bytes": 1234,
            "created_at": "2026-09-15T12:34:56Z",
        }
    )
    return {
        "namespace": NAMESPACE,
        "name": name,
        "version_ref": f"bdn:{NAMESPACE}/{name}",
        "sequence": 9,
        "updated_at": "2026-09-16T00:00:00Z",
        "head": head,
        "tags": []
        if head_digest is None
        else [{"name": "step-100", "digest": head_digest}],
        "tag_count": 0 if head_digest is None else 2,
        "versions_alive": 3,
        "versions_tombstoned": 1,
        "versions_untagged": 1,
        **overrides,
    }


@dataclass
class FakeServices:
    """Baseten API volume endpoints, cannery resolve, and S3, all behind one MockTransport."""

    volume: Volume
    token_expires_in: dt.timedelta = dt.timedelta(hours=1)
    credentials_expire_in: dt.timedelta | None = dt.timedelta(minutes=30)
    resolve_error: tuple[int, Any] | None = None
    resolve_failures: list[int] = field(default_factory=list)
    """Status codes to answer resolve with before answering normally."""
    token_error: tuple[int, Any] | None = None
    token_failures: list[int] = field(default_factory=list)
    bdn_endpoint: str | None = f"https://{BDN_HOST}"
    s3_failures: dict[str, list[tuple[int, str]]] = field(default_factory=dict)
    """Per-key (status, body) answers to give before serving the object."""
    requests: list[httpx.Request] = field(default_factory=list)
    resolve_count: int = 0
    namespaces: list[str] = field(default_factory=lambda: [NAMESPACE])
    volumes: list[dict[str, Any]] | None = None
    """``GET /v1/volumes`` items; ``None`` lists one volume whose head is ``volume``."""
    versions: list[dict[str, Any]] | None = None
    """``GET .../versions`` items, tombstoned ones included; ``None`` is ``volume`` alone."""
    page_size: int = 1000
    """Items per page on the paginated listings, capped by the request's limit."""
    api_error: tuple[int, Any] | None = None
    """Answer for every Baseten API request other than the token mint."""

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        host = request.url.host
        if host == API_HOST:
            if request.url.path == "/v1/volumes/token":
                return self._token(request)
            return self._inventory(request)
        if host == BDN_HOST:
            return self._resolve(request)
        if host == S3_HOST:
            return self._s3(request)
        return httpx.Response(500, text=f"unexpected host {host}")

    def _token(self, request: httpx.Request) -> httpx.Response:
        if self.token_failures:
            return httpx.Response(self.token_failures.pop(0), text="upstream hiccup")
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
                "bdn_endpoint": self.bdn_endpoint,
            },
        )

    def _inventory(self, request: httpx.Request) -> httpx.Response:
        if self.api_error is not None:
            status, body = self.api_error
            return httpx.Response(status, json=body)
        segments = request.url.path.removeprefix("/v1/volumes").strip("/").split("/")
        match segments:
            case ["namespaces"]:
                return self._page(request, self.namespaces)
            case [""]:
                assert request.url.params["namespace"] == NAMESPACE
                return self._page(request, self._volumes())
            case [namespace, name] if (namespace, name) == (NAMESPACE, VOLUME):
                return httpx.Response(200, json=self._volumes()[0])
            case [namespace, name, "versions"] if (namespace, name) == (
                NAMESPACE,
                VOLUME,
            ):
                versions = self._versions()
                if request.url.params.get("include_tombstoned") != "true":
                    versions = [v for v in versions if v["lifecycle"] != "TOMBSTONED"]
                return httpx.Response(
                    200, json={"versions": versions, "volume_sequence": 9}
                )
            case [namespace, name, "versions", selector] if (namespace, name) == (
                NAMESPACE,
                VOLUME,
            ):
                for version in self._versions():
                    if (
                        selector == "head"
                        and version["is_head"]
                        or selector.startswith(":")
                        and selector[1:] in version["tags"]
                        or selector.startswith("@")
                        and version["digest"]
                        .removeprefix("b3:")
                        .startswith(selector[1:].removeprefix("b3:"))
                    ):
                        return httpx.Response(
                            200,
                            json={**version, "entry_count": 10, "volume_sequence": 9},
                        )
        return httpx.Response(404, json={"error": {"message": "not found"}})

    def _volumes(self) -> list[dict[str, Any]]:
        if self.volumes is not None:
            return self.volumes
        return [api_volume(VOLUME, self.volume.manifest_digest)]

    def _versions(self) -> list[dict[str, Any]]:
        if self.versions is not None:
            return self.versions
        return [api_version(self.volume.manifest_digest)]

    def _page(self, request: httpx.Request, items: list[Any]) -> httpx.Response:
        start = int(request.url.params.get("cursor", "0"))
        size = min(self.page_size, int(request.url.params["limit"]))
        end = start + size
        has_more = end < len(items)
        return httpx.Response(
            200,
            json={
                "items": items[start:end],
                "pagination": {
                    "has_more": has_more,
                    "cursor": str(end) if has_more else None,
                },
            },
        )

    def _resolve(self, request: httpx.Request) -> httpx.Response:
        if self.resolve_failures:
            return httpx.Response(self.resolve_failures.pop(0), text="upstream hiccup")
        if self.resolve_error is not None:
            status, body = self.resolve_error
            if isinstance(body, str):
                return httpx.Response(status, text=body)
            return httpx.Response(status, json=body)
        self.resolve_count += 1
        ref = request.url.params["ref"]
        origin: dict[str, Any] = {
            "endpoint": "",
            "region": REGION,
            "bucket": BUCKET,
            "access_key_id": f"ASIA{self.resolve_count}",
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
            status, body = pending.pop(0)
            return httpx.Response(status, text=body)
        stored = self.volume.objects.get(key)
        if stored is None:
            return httpx.Response(404, text=s3_error("NoSuchKey"))
        body, content_type = stored
        return httpx.Response(
            200,
            content=body,
            headers={"content-type": content_type, "content-length": str(len(body))},
        )

    def http_client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self.handle))

    def management_client(self) -> ManagementClient:
        # The generated client posts relative paths, so its own httpx client
        # carries the base URL and the bearer header, as ManagementClient
        # would set them for a client it builds itself.
        return ManagementClient(
            api_key=API_KEY,
            base_url_override=f"https://{API_HOST}",
            http_client_override=httpx.Client(
                transport=httpx.MockTransport(self.handle),
                base_url=f"https://{API_HOST}",
                headers={"Authorization": f"Bearer {API_KEY}"},
            ),
        )

    def client(self, **kwargs: Any) -> VolumeClient:
        return VolumeClient(
            api_key=API_KEY,
            base_url_override=f"https://{API_HOST}",
            http_client_override=self.http_client(),
            management_client_override=self.management_client(),
            **kwargs,
        )

    def s3_requests(self) -> list[httpx.Request]:
        return [request for request in self.requests if request.url.host == S3_HOST]

    def resolve_requests(self) -> list[httpx.Request]:
        return [request for request in self.requests if request.url.host == BDN_HOST]

    def token_requests(self) -> list[httpx.Request]:
        return [
            request
            for request in self.requests
            if request.url.host == API_HOST and request.url.path == "/v1/volumes/token"
        ]

    def inventory_requests(self) -> list[httpx.Request]:
        return [
            request
            for request in self.requests
            if request.url.host == API_HOST and request.url.path != "/v1/volumes/token"
        ]


def now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)
