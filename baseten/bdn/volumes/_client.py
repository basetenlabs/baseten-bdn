"""Client-side reads of BDN volumes: resolve, list, and pull to a directory.

The read path touches three services:

1. The Baseten API mints a one-hour cannery token scoped to one volume
   (``POST /v1/volumes/token``), authenticated with the caller's API key.
2. Cannery resolves the ref to a manifest digest and returns short-lived
   credentials for the origin bucket (``POST /v1/volumes/resolve``).
3. The origin bucket serves the manifest, chunkmaps, and chunks, read
   directly with those credentials. Cannery is not in the data path.
"""

from __future__ import annotations

import datetime as dt
import logging
import threading
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Self

import httpx

from baseten.bdn.volumes import _cannery, _manifest, _materialize, _s3
from baseten.bdn.volumes._cannery import (
    OriginCredentials,
    ResolveResponse,
    TokenResponse,
    VolumeRef,
)
from baseten.bdn.volumes._manifest import (
    ChunkFileEntry,
    ChunkmapFileEntry,
    DirectoryEntry,
    Manifest,
    SlabmapFileEntry,
    SymlinkEntry,
)
from baseten.bdn.volumes._models import (
    FileInfo,
    PullResult,
    ResolvedVolume,
    VolumeConnectionError,
    VolumeIntegrityError,
)

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.baseten.co"
DEFAULT_MAX_CONCURRENCY = 16
DEFAULT_MAX_BYTES_IN_FLIGHT = 1 << 30
DEFAULT_REQUEST_TIMEOUT_SEC = 60.0

_TOKEN_PATH = "/v1/volumes/token"
_RESOLVE_PATH = "/v1/volumes/resolve"
# Tokens and STS sessions are re-minted this long before they expire, so an
# in-flight request never presents a credential that lapses mid-request.
_EXPIRY_MARGIN = dt.timedelta(minutes=5)
_CONNECT_TIMEOUT_SEC = 10.0


@dataclass(frozen=True)
class VolumesClientOptions:
    """Options for :class:`VolumesClient`."""

    api_key: str
    base_url_override: str | None = None
    """Baseten API base URL; ``None`` means ``https://api.baseten.co``."""

    bdn_endpoint_override: str | None = None
    """Cannery base URL; ``None`` uses the one the token response names."""

    max_concurrency: int = DEFAULT_MAX_CONCURRENCY
    """In-flight object reads during a pull."""

    max_bytes_in_flight: int = DEFAULT_MAX_BYTES_IN_FLIGHT
    """Bound on decompressed chunk bytes buffered in memory during a pull."""

    request_timeout_sec: float = DEFAULT_REQUEST_TIMEOUT_SEC
    """Read bound on one HTTP request to any of the three services."""

    def __post_init__(self) -> None:
        if not self.api_key:
            raise ValueError("api_key must not be empty")
        if self.max_concurrency <= 0:
            raise ValueError("max_concurrency must be positive")
        if self.max_bytes_in_flight <= 0:
            raise ValueError("max_bytes_in_flight must be positive")
        if self.request_timeout_sec <= 0:
            raise ValueError("request_timeout_sec must be positive")

    @property
    def base_url(self) -> str:
        return self.base_url_override or DEFAULT_BASE_URL


class VolumesClient:
    """Synchronous client for reading BDN volumes from anywhere with a Baseten API key.

    Usable as a context manager; exiting closes the HTTP client.
    """

    def __init__(
        self,
        *,
        api_key: str,
        base_url_override: str | None = None,
        bdn_endpoint_override: str | None = None,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_bytes_in_flight: int = DEFAULT_MAX_BYTES_IN_FLIGHT,
        request_timeout_sec: float = DEFAULT_REQUEST_TIMEOUT_SEC,
        http_client_override: httpx.Client | None = None,
        close_http_client_on_close: bool | None = None,
    ) -> None:
        """Create a volumes client.

        Args:
            api_key: Baseten API key; used only against the Baseten API to
                mint cannery tokens.
            base_url_override: Baseten API base URL.
            bdn_endpoint_override: Cannery base URL, when not the one the
                token response names.
            max_concurrency: In-flight object reads during a pull.
            max_bytes_in_flight: Bound on chunk bytes buffered in memory.
            request_timeout_sec: Read bound on one HTTP request.
            http_client_override: Pre-configured httpx client used for all
                three services; the caller owns its transport.
            close_http_client_on_close: Whether :meth:`close` closes the HTTP
                client. Defaults to ``True`` for a client created here and
                ``False`` for *http_client_override*.
        """
        self._options = VolumesClientOptions(
            api_key=api_key,
            base_url_override=base_url_override,
            bdn_endpoint_override=bdn_endpoint_override,
            max_concurrency=max_concurrency,
            max_bytes_in_flight=max_bytes_in_flight,
            request_timeout_sec=request_timeout_sec,
        )
        self._timeout = httpx.Timeout(request_timeout_sec, connect=_CONNECT_TIMEOUT_SEC)
        self._http_client = (
            httpx.Client(timeout=self._timeout, headers={"User-Agent": _user_agent()})
            if http_client_override is None
            else http_client_override
        )
        self.close_http_client_on_close = (
            http_client_override is None
            if close_http_client_on_close is None
            else close_http_client_on_close
        )
        self._tokens: dict[tuple[str, str], TokenResponse] = {}
        self._tokens_lock = threading.Lock()

    @property
    def options(self) -> VolumesClientOptions:
        """The options this client was constructed with."""
        return self._options

    @property
    def http_client(self) -> httpx.Client:
        """The underlying httpx client."""
        return self._http_client

    def resolve(self, ref: str) -> ResolvedVolume:
        """Resolve a ref to the version it names, without downloading anything.

        Args:
            ref: ``bdn:<namespace>/<volume>`` optionally with ``:<tag>`` or
                ``@<digest prefix>`` (12 to 64 hex characters). A bare ref
                resolves to the volume's head.
        """
        return _public_resolved(self._resolve(VolumeRef.parse(ref)))

    def list_files(self, ref: str) -> list[FileInfo]:
        """List the entries of the version ``ref`` names. Downloads only the manifest."""
        parsed = VolumeRef.parse(ref)
        resolution = self._resolve(parsed)
        store = self._object_store(parsed, resolution)
        return self._fetch_manifest(store, parsed, resolution).file_infos()

    def pull(self, ref: str, dest_dir: str | Path) -> PullResult:
        """Materialize the version ``ref`` names below ``dest_dir``.

        Writes in place: an existing tree at ``dest_dir`` is overwritten entry
        by entry, and a failure leaves what was written so far. Every object
        is verified against its recorded BLAKE3 digest before it is written.

        Raises:
            VolumePathError: The manifest would place an entry outside
                ``dest_dir``; nothing is written.
            VolumeIntegrityError: A downloaded object failed its digest or
                length check.
            VolumeAPIError: The Baseten API, cannery, or the origin bucket
                rejected a request.
            NotImplementedError: The manifest uses a feature this client does
                not support (slabmap files, or symlinks and hardlinks on
                Windows).
        """
        started = time.perf_counter()
        parsed = VolumeRef.parse(ref)
        root = Path(dest_dir)
        resolution = self._resolve(parsed)
        store = self._object_store(parsed, resolution)
        manifest = self._fetch_manifest(store, parsed, resolution)
        contained = _manifest.ContainedPaths(manifest.entries)
        groups = _manifest.hardlink_groups(manifest.entries)
        linked = {path for paths in groups.values() for path in paths[1:]}

        logger.info(
            "pull %s: %d entries, %d bytes -> %s",
            resolution.resolved.reference,
            len(manifest.entries),
            manifest.header.total_size,
            root,
        )
        root.mkdir(parents=True, exist_ok=True)
        # Directories first so every file has a parent; their modes last, so a
        # read-only directory cannot block its own children.
        directory_modes: list[tuple[Path, int]] = []
        for entry in manifest.entries:
            if isinstance(entry, DirectoryEntry):
                path = _materialize.ensure_dir(root, entry.clean_path)
                directory_modes.append((path, entry.mode_bits))
        for entry in manifest.entries:
            if isinstance(entry, SymlinkEntry):
                path = _materialize.contained_join(root, entry.clean_path)
                _materialize.ensure_dir(root, entry.clean_path.rpartition("/")[0])
                _materialize.create_symlink(
                    path, contained.rendered_symlink_target(entry)
                )

        files = [
            entry
            for entry in manifest.entries
            if isinstance(entry, (ChunkFileEntry, ChunkmapFileEntry, SlabmapFileEntry))
            and entry.clean_path not in linked
        ]
        for entry in files:
            if isinstance(entry, SlabmapFileEntry):
                raise NotImplementedError(
                    f"slabmap files are not supported: {entry.path!r}"
                )
        written = _PullWorker(store, parsed, resolution, root, self._options).run(files)

        for paths in groups.values():
            source = _materialize.contained_join(root, paths[0])
            for path in paths[1:]:
                _materialize.ensure_dir(root, path.rpartition("/")[0])
                _materialize.create_hardlink(
                    _materialize.contained_join(root, path), source
                )
        for path, mode in sorted(
            directory_modes, key=lambda item: len(item[0].parts), reverse=True
        ):
            _materialize.apply_mode(path, mode)

        return PullResult(
            reference=resolution.resolved.reference,
            digest=resolution.resolved.origin_digest,
            dest_dir=root,
            files=len(files) + len(linked),
            bytes=written,
            duration_sec=time.perf_counter() - started,
        )

    def close(self) -> None:
        """Close the HTTP client if this object owns it."""
        if self.close_http_client_on_close:
            self._http_client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _token(self, ref: VolumeRef) -> TokenResponse:
        key = (ref.namespace, ref.volume)
        with self._tokens_lock:
            cached = self._tokens.get(key)
            if cached is not None and cached.expires_at - _now() > _EXPIRY_MARGIN:
                return cached
            try:
                response = self._http_client.post(
                    f"{self._options.base_url}{_TOKEN_PATH}",
                    json=_cannery.token_request(ref),
                    headers={"Authorization": f"Bearer {self._options.api_key}"},
                    timeout=self._timeout,
                )
            except httpx.HTTPError as error:
                raise _cannery.connection_error("the Baseten API", error) from error
            _cannery.raise_for_baseten_error(response)
            token = _cannery.parse_json(TokenResponse, response, "volume token")
            self._tokens[key] = token
            return token

    def _bdn_endpoint(self, token: TokenResponse) -> str:
        endpoint = self._options.bdn_endpoint_override or token.bdn_endpoint
        if not endpoint:
            raise VolumeConnectionError(
                "the token response names no BDN endpoint; pass bdn_endpoint_override"
            )
        return endpoint.rstrip("/")

    def _resolve(self, ref: VolumeRef) -> ResolveResponse:
        token = self._token(ref)
        try:
            response = self._http_client.post(
                f"{self._bdn_endpoint(token)}{_RESOLVE_PATH}",
                params={"ref": ref.canonical()},
                headers={"Authorization": f"Bearer {token.token}"},
                timeout=self._timeout,
            )
        except httpx.HTTPError as error:
            raise _cannery.connection_error("cannery", error) from error
        _cannery.raise_for_cannery_error(response)
        return _cannery.parse_json(ResolveResponse, response, "resolve")

    def _object_store(self, ref: VolumeRef, first: ResolveResponse) -> _s3.ObjectStore:
        current = {"resolution": first}
        lock = threading.Lock()

        def credentials() -> OriginCredentials:
            with lock:
                origin = current["resolution"].origin
                if (
                    origin.expires_at is not None
                    and origin.expires_at - _now() <= _EXPIRY_MARGIN
                ):
                    logger.info(
                        "origin credentials near expiry; resolving %s again",
                        ref.canonical(),
                    )
                    current["resolution"] = self._resolve(ref)
                    origin = current["resolution"].origin
                return origin

        return _s3.ObjectStore(self._http_client, credentials, timeout=self._timeout)

    def _fetch_manifest(
        self, store: _s3.ObjectStore, ref: VolumeRef, resolution: ResolveResponse
    ) -> Manifest:
        resolved = resolution.resolved
        body, content_type = store.get(
            resolved.target.key(resolved.org_id, ref.namespace)
        )
        data = _s3.decode_object(
            body,
            content_type,
            expected_kind="manifest",
            expected_digest=resolved.origin_digest,
        )
        return _manifest.parse_manifest(data)


class _PullWorker:
    """Fans chunk reads out over a thread pool, bounded by a byte budget."""

    def __init__(
        self,
        store: _s3.ObjectStore,
        ref: VolumeRef,
        resolution: ResolveResponse,
        root: Path,
        options: VolumesClientOptions,
    ) -> None:
        self._store = store
        self._namespace = ref.namespace
        self._org_id = resolution.resolved.org_id
        self._root = root
        self._options = options
        self._budget = _materialize.ByteBudget(options.max_bytes_in_flight)

    def run(
        self, files: Sequence[ChunkFileEntry | ChunkmapFileEntry | SlabmapFileEntry]
    ) -> int:
        # Chunkmaps are fetched first so chunk work is one flat list; nesting
        # pool submissions would let workers block on workers.
        with ThreadPoolExecutor(max_workers=self._options.max_concurrency) as pool:
            plans = list(pool.map(self._plan_file, files))
            chunk_jobs = [(path, chunk) for path, chunks in plans for chunk in chunks]
            # Consume the iterator so a worker's exception propagates here.
            for _ in pool.map(self._fetch_chunk, chunk_jobs):
                pass
        for entry in files:
            _materialize.apply_mode(
                _materialize.contained_join(self._root, entry.clean_path),
                entry.mode_bits,
            )
        return sum(chunk.length for _, chunk in chunk_jobs)

    def _plan_file(
        self, entry: ChunkFileEntry | ChunkmapFileEntry | SlabmapFileEntry
    ) -> tuple[Path, list[_manifest.ChunkEntry]]:
        path = _materialize.contained_join(self._root, entry.clean_path)
        _materialize.ensure_dir(self._root, entry.clean_path.rpartition("/")[0])
        if isinstance(entry, ChunkFileEntry):
            _materialize.create_file(path, entry.size)
            return path, [entry.chunk] if entry.chunk else []
        if isinstance(entry, ChunkmapFileEntry):
            body, content_type = self._store.get(
                entry.target.key(self._org_id, self._namespace)
            )
            data = _s3.decode_object(
                body,
                content_type,
                expected_kind="chunkmap",
                expected_digest=entry.digest,
            )
            chunks = _manifest.parse_chunkmap(data, entry.size)
            _materialize.create_file(path, entry.size)
            return path, list(chunks)
        raise NotImplementedError(f"slabmap files are not supported: {entry.path!r}")

    def _fetch_chunk(self, job: tuple[Path, _manifest.ChunkEntry]) -> None:
        path, chunk = job
        self._budget.acquire(chunk.length)
        try:
            body, content_type = self._store.get(
                chunk.target.key(self._org_id, self._namespace)
            )
            data = _s3.decode_object(
                body, content_type, expected_kind="chunk", expected_digest=chunk.digest
            )
            if len(data) != chunk.length:
                raise VolumeIntegrityError(
                    f"chunk {chunk.digest} is {len(data)} bytes, the record says {chunk.length}"
                )
            _materialize.write_at(path, chunk.offset, data)
        finally:
            self._budget.release(chunk.length)


def _public_resolved(resolution: ResolveResponse) -> ResolvedVolume:
    resolved = resolution.resolved
    return ResolvedVolume(
        reference=resolved.reference,
        org_id=resolved.org_id,
        digest=resolved.origin_digest,
        resolved_from=resolved.resolved_from,
        sequence=resolved.sequence,
    )


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def _user_agent() -> str:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return f"baseten-bdn/{version('baseten-bdn')}"
    except PackageNotFoundError:
        return "baseten-bdn/unknown"
