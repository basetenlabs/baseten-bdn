"""Client-side reads of BDN volumes: list, describe, fetch a manifest, pull to a directory.

Namespace and volume inventory, volume and version descriptions, and version
history come from the Baseten API alone. Reading a version's content touches
three services:

1. The Baseten API mints a one-hour cannery token scoped to one volume
   (``POST /v1/volumes/token``), through baseten-python's ``ManagementClient``.
2. Cannery resolves the ref to a manifest digest and returns short-lived
   credentials for the origin bucket (``POST /v1/volumes/resolve``).
3. The origin bucket serves the manifest, chunkmaps, and chunks, read
   directly with those credentials. Cannery is not in the data path.
"""

from __future__ import annotations

import builtins
import datetime as dt
import functools
import logging
import secrets
import shutil
import sys
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Self, TypeVar, overload

import httpx
import pydantic
from baseten.client import ManagementClient
from baseten.client import managementapi as api
from baseten.client.managementapi import (
    CreateVolumeTokenRequest,
    CreateVolumeTokenResponse,
    ResponseError,
    VolumeTokenScope,
)

from baseten.bdn._useragent import user_agent
from baseten.bdn.volumes import _cannery, _manifest, _materialize, _s3
from baseten.bdn.volumes._cannery import OriginCredentials, ResolveResponse
from baseten.bdn.volumes._errors import (
    VolumeAPIError,
    VolumeConnectionError,
    VolumeDestinationError,
    VolumeProtocolError,
    VolumeRefError,
    VolumeUnsupportedError,
)
from baseten.bdn.volumes._manifest import (
    ChunkEntry,
    ChunkFileEntry,
    ChunkmapFileEntry,
    DirectoryEntry,
    FileEntry,
    Manifest,
    SymlinkEntry,
)
from baseten.bdn.volumes._models import (
    NamespaceListing,
    PullResult,
    Volume,
    VolumeEntryListing,
    VolumeHead,
    VolumeListing,
    VolumeManifest,
    VolumeNamespace,
    VolumeTag,
    VolumeVersion,
    VolumeVersionDetail,
    VolumeVersionListing,
)
from baseten.bdn.volumes._reader import VolumeFileReader
from baseten.bdn.volumes._ref import VolumeRef, VolumeRefLevel
from baseten.bdn.volumes._version import Version, object_key

logger = logging.getLogger(__name__)
_T = TypeVar("_T")

DEFAULT_MAX_CONCURRENCY = 16
DEFAULT_MAX_BYTES_IN_FLIGHT = 1 << 30
DEFAULT_REQUEST_TIMEOUT_SEC = 60.0

_RESOLVE_PATH = "/v1/volumes/resolve"
# The API's maximum, so a large inventory costs as few pages as possible.
_PAGE_LIMIT = 1000
# Tokens and STS sessions are re-minted this long before they expire, so an
# in-flight request never presents a credential that lapses mid-request.
_EXPIRY_MARGIN = dt.timedelta(minutes=5)
_CONNECT_TIMEOUT_SEC = 10.0


@dataclass(frozen=True)
class VolumeClientOptions:
    """Options for :class:`VolumeClient`."""

    api_key: str
    base_url_override: str | None = None
    """Baseten API base URL; ``None`` means the public API."""

    bdn_endpoint_override: str | None = None
    """Cannery base URL; ``None`` uses the one the token response names."""

    max_concurrency: int = DEFAULT_MAX_CONCURRENCY
    """In-flight object reads during a pull."""

    max_bytes_in_flight: int = DEFAULT_MAX_BYTES_IN_FLIGHT
    """Bound on chunk bytes buffered in memory during a pull, compressed and decoded copies included."""

    request_timeout_sec: float = DEFAULT_REQUEST_TIMEOUT_SEC
    """Read bound on one HTTP request to cannery or the origin bucket."""

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
        """The Baseten API base URL in effect."""
        return self.base_url_override or ManagementClient.default_base_url()


class VolumeClient:
    """Synchronous client for reading BDN volumes from anywhere with a Baseten API key.

    Usable as a context manager; exiting closes the HTTP clients this object owns.
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
        management_client_override: ManagementClient | None = None,
        close_http_client_on_close: bool | None = None,
    ) -> None:
        """Create a volume client.

        Args:
            api_key: Baseten API key, for the Baseten API's volume endpoints
                and the cannery tokens they mint.
            base_url_override: Baseten API base URL.
            bdn_endpoint_override: Cannery base URL, when not the one the
                token response names.
            max_concurrency: In-flight object reads during a pull.
            max_bytes_in_flight: Bound on chunk bytes buffered in memory.
            request_timeout_sec: Read bound on one HTTP request to cannery
                or the origin bucket.
            http_client_override: Pre-configured httpx client for cannery and
                the origin bucket; the caller owns its transport.
            management_client_override: Pre-configured baseten-python
                :class:`ManagementClient` for the Baseten API; the caller owns
                its lifetime.
            close_http_client_on_close: Whether :meth:`close` closes the
                clients. Defaults to ``True`` for clients created here and
                ``False`` for overrides.
        """
        self._options = VolumeClientOptions(
            api_key=api_key,
            base_url_override=base_url_override,
            bdn_endpoint_override=bdn_endpoint_override,
            max_concurrency=max_concurrency,
            max_bytes_in_flight=max_bytes_in_flight,
            request_timeout_sec=request_timeout_sec,
        )
        self._timeout = httpx.Timeout(request_timeout_sec, connect=_CONNECT_TIMEOUT_SEC)
        self._owns_http_client = http_client_override is None
        self._http_client = (
            httpx.Client(timeout=self._timeout, headers={"User-Agent": user_agent()})
            if http_client_override is None
            else http_client_override
        )
        self._owns_management_client = management_client_override is None
        self._management_client = (
            ManagementClient(api_key=api_key, base_url_override=base_url_override)
            if management_client_override is None
            else management_client_override
        )
        self.close_http_client_on_close = (
            (self._owns_http_client and self._owns_management_client)
            if close_http_client_on_close is None
            else close_http_client_on_close
        )
        self._tokens: dict[tuple[str, str], CreateVolumeTokenResponse] = {}
        self._tokens_lock = threading.Lock()

    @property
    def options(self) -> VolumeClientOptions:
        """The options this client was constructed with."""
        return self._options

    @property
    def http_client(self) -> httpx.Client:
        """The httpx client used for cannery and the origin bucket."""
        return self._http_client

    @property
    def management_client(self) -> ManagementClient:
        """The baseten-python client used to mint volume tokens."""
        return self._management_client

    def fetch_manifest(self, ref: str | VolumeRef) -> VolumeManifest:
        """Read the manifest of the version ``ref`` names, without downloading content.

        A path on the ref narrows ``entries`` to that path and what is under
        it; ``entry_count`` and ``total_size`` still describe the whole version.
        """
        parsed = _volume_or_point(ref)
        version = self._open_version(parsed)
        manifest = version.manifest
        paths = (
            _manifest.select_paths(manifest.entries, _ref_include(parsed))
            if parsed.path
            else None
        )
        return VolumeManifest(
            version_ref=version.ref,
            entry_count=len(manifest.entries),
            total_size=manifest.header.total_size,
            entries=manifest.public_entries(paths),
        )

    @overload
    def list(
        self, ref: None = None, *, recursive: bool = False
    ) -> NamespaceListing: ...

    @overload
    def list(
        self, ref: str | VolumeRef, *, recursive: bool = False
    ) -> NamespaceListing | VolumeListing | VolumeEntryListing: ...

    def list(
        self, ref: str | VolumeRef | None = None, *, recursive: bool = False
    ) -> NamespaceListing | VolumeListing | VolumeEntryListing:
        """List what ``ref`` names, dispatching on how much of a ref it is.

        ============================  ==================================
        ``ref``                       Result
        ============================  ==================================
        ``None`` or ``"bdn:"``        :class:`NamespaceListing`
        ``bdn:ns``                    :class:`VolumeListing`
        ``bdn:ns/vol``                entries at the root of the head
        ``bdn:ns/vol:tag`` or ``@d``  entries at the root of that version
        ``.../path``                  entries beneath that directory
        ============================  ==================================

        Entry listings are :class:`VolumeEntryListing` and hold a directory's
        immediate children, or with ``recursive`` everything beneath it. A
        head or tag is resolved once, and ``version_ref`` pins the version
        that was listed. Namespace and volume listings walk every page.

        Raises:
            VolumeRefError: ``ref`` does not parse, or ``recursive`` was
                passed for a namespace or volume inventory.
            VolumePathError: The path names nothing in the version, or names
                a file or symlink, which has no entries beneath it.
            VolumeAPIError: The Baseten API or cannery rejected a request.
        """
        if ref is None or (isinstance(ref, str) and ref.strip() == "bdn:"):
            _refuse_recursive_inventory(recursive, "namespaces")
            return NamespaceListing(items=self._list_namespaces())
        parsed = _parse(ref)
        if parsed.level is VolumeRefLevel.NAMESPACE:
            _refuse_recursive_inventory(recursive, f"the volumes in {parsed}")
            return VolumeListing(
                namespace=parsed.namespace, items=self._list_volumes(parsed.namespace)
            )
        version = self._open_version(parsed)
        return VolumeEntryListing(
            version_ref=version.ref,
            items=_manifest.listed_entries(
                version.manifest.entries, parsed.path, recursive=recursive
            ),
        )

    def describe(self, ref: str | VolumeRef) -> Volume | VolumeVersionDetail:
        """Describe a volume or one of its versions.

        A volume ref (``bdn:ns/vol``) returns a :class:`Volume`; a tag or
        digest ref returns a :class:`VolumeVersionDetail` whose
        ``version_ref`` pins the version the tag pointed at when it was read.
        Narrow the result on ``kind``: ``"volume"`` or ``"version"``.

        Raises:
            VolumeRefError: ``ref`` names a namespace or a path. Entry
                metadata comes from :meth:`list` or :meth:`fetch_manifest`.
            VolumeAPIError: The Baseten API rejected the request, including
                ``status_code`` 404 for a volume or version that does not exist.
        """
        parsed = _parse(ref)
        namespace, volume = parsed.namespace, parsed.volume
        match parsed.level:
            case VolumeRefLevel.NAMESPACE:
                raise VolumeRefError(
                    f"ref {parsed} names a namespace, which has nothing to describe; "
                    f"list its volumes with list({str(parsed)!r})"
                )
            case VolumeRefLevel.PATH:
                raise VolumeRefError(
                    f"ref {parsed} names a path; describe() covers volumes and versions, "
                    "and entry metadata comes from list() or fetch_manifest()"
                )
            case VolumeRefLevel.VOLUME:
                assert volume is not None, "a volume-level ref names a volume"
                return _volume(
                    self._api(
                        lambda: self._management_client.api.get_volumes_volume_name(
                            volume_namespace=namespace, volume_name=volume
                        )
                    )
                )
            case VolumeRefLevel.POINT:
                assert volume is not None, "a point-level ref names a volume"
                selector = _version_selector(parsed)
                detail = self._api(
                    lambda: (
                        self._management_client.api.get_volumes_versions_volume_version(
                            volume_namespace=namespace,
                            volume_name=volume,
                            volume_version=selector,
                        )
                    )
                )
                return VolumeVersionDetail.model_validate(
                    {**_version_fields(detail), "entry_count": detail.entry_count}
                )
        raise AssertionError(f"unhandled ref level {parsed.level}")

    def list_versions(
        self, ref: str | VolumeRef, *, include_tombstoned: bool = False
    ) -> VolumeVersionListing:
        """A volume's version history, newest first.

        ``include_tombstoned`` adds deleted versions, whose ``lifecycle`` is
        ``TOMBSTONED``; this client reads them and never restores them.

        Raises:
            VolumeRefError: ``ref`` does not name exactly a volume.
            VolumeAPIError: The Baseten API rejected the request.
        """
        parsed = _parse(ref)
        if parsed.level is not VolumeRefLevel.VOLUME:
            raise VolumeRefError(
                f"ref {parsed} names a {parsed.level}; version history belongs to a volume, "
                f"so write 'bdn:{parsed.namespace}/{parsed.volume or '<volume>'}'"
            )
        namespace, volume = parsed.namespace, parsed.volume
        assert volume is not None, "a volume-level ref names a volume"
        request = api.GetVolumesVersionsRequest(include_tombstoned=include_tombstoned)
        # Unpaged upstream: one response carries the whole history.
        response = self._api(
            lambda: self._management_client.api.get_volumes_versions(
                volume_namespace=namespace, volume_name=volume, request=request
            )
        )
        return VolumeVersionListing(
            volume_ref=parsed,
            items=[
                VolumeVersion.model_validate(_version_fields(v))
                for v in response.versions
            ],
        )

    def open(self, ref: str | VolumeRef) -> VolumeFileReader:
        """Open the file ``ref`` names for reading, without writing anything to disk.

        The ref's path must name a regular file, or a symlink that leads to
        one inside the version; a head or tag is resolved once, and the
        reader's ``version_ref`` pins what is read. The reader streams the
        file in bounded memory; see :class:`VolumeFileReader`. Close it, or use
        it as a context manager, to stop fetching early.

        Raises:
            VolumeRefError: ``ref`` carries no path, or only ``/``.
            VolumePathError: The path names nothing, a directory, or a symlink
                whose target escapes the version or leads to no file.
            VolumeIntegrityError: The chunkmap failed its digest check; a
                chunk that fails its check raises from the read that reaches it.
            VolumeAPIError: The Baseten API or cannery rejected a request.
            VolumeStorageError: The origin bucket refused an object read.
            VolumeUnsupportedError: The volume uses slabmap files.
        """
        parsed = _volume_or_point(ref)
        if parsed.path is None or parsed.path == "/":
            raise VolumeRefError(
                f"ref {parsed} names no file to read; add the path of a file within "
                f"the version, as in '{parsed.without_path()}/config.json'"
            )
        version = self._open_version(parsed)
        contained = _manifest.ContainedPaths(version.manifest.entries)
        entry = contained.file_at(parsed.path)
        return VolumeFileReader(
            version,
            _manifest.public_entry(entry),
            version.chunks(entry),
            max_concurrency=self._options.max_concurrency,
            max_bytes_in_flight=self._options.max_bytes_in_flight,
        )

    def read_bytes(self, ref: str | VolumeRef) -> bytes:
        """The whole verified content of the file ``ref`` names, held in memory.

        Raises what :meth:`open` and reading do.
        """
        with self.open(ref) as source:
            return source.read()

    def read_text(
        self, ref: str | VolumeRef, *, encoding: str = "utf-8", errors: str = "strict"
    ) -> str:
        """The file ``ref`` names, verified and then decoded with ``encoding`` and ``errors``.

        Raises what :meth:`read_bytes` does, and :class:`UnicodeDecodeError`
        when the bytes do not decode under ``errors="strict"``.
        """
        return self.read_bytes(ref).decode(encoding, errors)

    def pull(
        self,
        ref: str | VolumeRef,
        dest_dir: str | Path,
        *,
        overwrite: bool = False,
        include: Sequence[str] = (),
        strip_prefix: bool = False,
    ) -> PullResult:
        """Materialize the version ``ref`` names at ``dest_dir``.

        Unless ``overwrite`` is set, ``dest_dir`` must not exist or must be
        empty; the tree is assembled beside it and moved into place only once
        complete, so a failed pull leaves nothing behind. With ``overwrite``
        the tree is written into ``dest_dir`` in place, entry by entry, and a
        failure leaves what was written so far. Every object is verified
        against its recorded BLAKE3 digest before it is written.

        A path on ``ref`` and any ``include`` entries narrow the pull to those
        paths and what is under them, matched on slash boundaries relative to
        the volume root. Narrowing does not move anything: an entry lands at
        its own path below ``dest_dir``. An include that matches nothing is an
        error rather than a smaller pull.

        ``strip_prefix`` drops the path on ``ref`` from where entries land,
        and changes nothing about what is selected. Pulling
        ``bdn:ns/vol:tag/tokenizer`` into ``./out`` writes
        ``./out/tokenizer.json`` with it and ``./out/tokenizer/tokenizer.json``
        without it. A file ref lands by its basename. Everything selected must
        then be within the ref's path, so an ``include`` elsewhere, or a
        symlink whose target resolves above the path, is refused before
        anything is written.

        Raises:
            VolumeRefError: ``strip_prefix`` was passed with no path on ``ref``.
            VolumePathError: The manifest would place an entry outside
                ``dest_dir``, an include names nothing, or ``strip_prefix``
                has no place for a selected entry; nothing is written.
            VolumeDestinationError: ``dest_dir`` exists and is not empty
                without ``overwrite``, has too little free space, or has a
                file where a directory is needed.
            VolumeIntegrityError: A downloaded object failed its digest or
                length check.
            VolumeAPIError: The Baseten API or cannery rejected a request.
            VolumeStorageError: The origin bucket refused an object read.
            VolumeUnsupportedError: The volume uses slabmap files, or this is
                not a POSIX platform.
        """
        if sys.platform == "win32":
            raise VolumeUnsupportedError(
                "pulling volumes is supported on Linux and macOS only"
            )
        started = time.perf_counter()
        parsed = _volume_or_point(ref)
        ref_path = _ref_include(parsed)
        if strip_prefix and not ref_path:
            raise VolumeRefError(
                f"ref {parsed} names no path for strip_prefix to strip; "
                "add the directory or file to drop, as in 'bdn:ns/vol:tag/tokenizer'"
            )
        dest = Path(dest_dir)
        _check_destination(dest, overwrite)
        version = self._open_version(parsed)
        version_ref, manifest = version.ref, version.manifest
        contained = _manifest.ContainedPaths(manifest.entries)
        selected = _manifest.select_paths(manifest.entries, [*ref_path, *include])
        if strip_prefix:
            assert selected is not None, "a ref path always narrows"
            layout = _manifest.Layout.stripped(contained, ref_path[0], selected)
        else:
            layout = _manifest.Layout(contained)
        _check_free_space(dest, manifest.header.total_size)
        logger.info(
            "pull %s: %d entries, %d bytes -> %s",
            version_ref,
            len(manifest.entries),
            manifest.header.total_size,
            dest,
        )

        staging = None if overwrite else _staging_dir(dest)
        root = staging or dest
        try:
            root.mkdir(parents=True, exist_ok=True)
            materializer = _Materializer(version, root, layout, self._options)
            written, file_count, chunks = materializer.run(manifest, selected)
            if staging is not None:
                if dest.exists():
                    dest.rmdir()
                staging.rename(dest)
        except BaseException:
            if staging is not None:
                shutil.rmtree(staging, ignore_errors=True)
            raise

        duration = time.perf_counter() - started
        total_files = len(manifest.files())
        logger.info(
            "pull %s done: %d files, %d bytes in %.1fs -> %s",
            version_ref,
            file_count,
            written,
            duration,
            dest,
        )
        return PullResult(
            version_ref=version_ref,
            dest_dir=dest,
            file_count=file_count,
            bytes_written=written,
            selected_file_count=file_count,
            total_file_count=total_files,
            chunks_fetched=chunks,
            duration_sec=duration,
        )

    def close(self) -> None:
        """Close the HTTP clients this object owns."""
        if self.close_http_client_on_close:
            self._http_client.close()
            self._management_client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _token(self, ref: VolumeRef) -> CreateVolumeTokenResponse:
        if ref.volume is None:
            raise VolumeRefError(f"ref {ref} names no volume to mint a token for")
        key = (ref.namespace, ref.volume)
        with self._tokens_lock:
            cached = self._tokens.get(key)
            if cached is not None and cached.expires_at - _now() > _EXPIRY_MARGIN:
                return cached
            request = CreateVolumeTokenRequest(
                scopes=[VolumeTokenScope.PULL],
                namespaces=[ref.namespace],
                volumes=[ref.volume],
            )
            token = self._with_retry(
                "Baseten API",
                lambda: self._management_client.api.post_volumes_token(request=request),
            )
            self._tokens[key] = token
            return token

    def _bdn_endpoint(self, token: CreateVolumeTokenResponse) -> str:
        endpoint = self._options.bdn_endpoint_override or token.bdn_endpoint
        if not endpoint:
            raise VolumeConnectionError(
                "the token response names no BDN endpoint; pass bdn_endpoint_override"
            )
        return endpoint.rstrip("/")

    def _resolve(self, ref: VolumeRef) -> ResolveResponse:
        token = self._token(ref)

        def post() -> httpx.Response:
            response = self._http_client.post(
                f"{self._bdn_endpoint(token)}{_RESOLVE_PATH}",
                params={"ref": str(ref.without_path())},
                headers={"Authorization": f"Bearer {token.token}"},
                timeout=self._timeout,
            )
            # Surface a retryable status the same way the generated client does.
            if _s3.is_retryable_status(response.status_code):
                raise ResponseError(
                    status_code=response.status_code, body=response.text
                )
            return response

        try:
            response = self._with_retry("cannery", post)
        except VolumeAPIError as error:
            # The last attempt's retryable status; report it through the
            # cannery envelope translation like any other rejection.
            raise VolumeAPIError("cannery", error.status_code, error.message) from None
        _cannery.raise_for_cannery_error(response)
        return _cannery.parse_json(ResolveResponse, response, "resolve")

    def _with_retry(self, service: str, call: Callable[[], _T]) -> _T:
        # Token mint and resolve are safe to repeat: neither changes state
        # beyond issuing another short-lived credential.
        for attempt in range(1, _s3.ATTEMPTS + 1):
            try:
                return call()
            except ResponseError as error:
                if attempt == _s3.ATTEMPTS or not _s3.is_retryable_status(
                    error.status_code
                ):
                    raise VolumeAPIError(
                        service, error.status_code, error.body
                    ) from error
            except httpx.TransportError as error:
                if attempt == _s3.ATTEMPTS:
                    raise _cannery.connection_error(service, error) from error
            except httpx.HTTPError as error:
                raise _cannery.connection_error(service, error) from error
            time.sleep(_s3.backoff_sec(attempt))
        raise AssertionError("unreachable: the retry loop returns or raises")

    def _api(self, call: Callable[[], _T]) -> _T:
        """One read against the Baseten API, retried and translated into volume errors."""
        try:
            return self._with_retry("Baseten API", call)
        # The generated client raises ValueError for a non-JSON body and
        # ValidationError for JSON that does not match its models.
        except (pydantic.ValidationError, ValueError) as error:
            raise VolumeProtocolError(
                f"Baseten API response is off contract: {error}"
            ) from error

    # builtins.list: the list() method shadows the builtin in this class body.
    def _list_namespaces(self) -> builtins.list[VolumeNamespace]:
        def page(
            cursor: str | None,
        ) -> tuple[builtins.list[str], api.PaginationResponse]:
            request = api.GetVolumesNamespacesRequest(limit=_PAGE_LIMIT, cursor=cursor)
            response = self._api(
                functools.partial(
                    self._management_client.api.get_volumes_namespaces, request=request
                )
            )
            return response.items, response.pagination

        return [VolumeNamespace(name=name) for name in _paginate(page)]

    def _list_volumes(self, namespace: str) -> builtins.list[Volume]:
        def page(
            cursor: str | None,
        ) -> tuple[builtins.list[api.Volume], api.PaginationResponse]:
            request = api.GetVolumesRequest(
                namespace=namespace, limit=_PAGE_LIMIT, cursor=cursor
            )
            response = self._api(
                functools.partial(
                    self._management_client.api.get_volumes, request=request
                )
            )
            return response.items, response.pagination

        return [_volume(volume) for volume in _paginate(page)]

    def _open_version(self, ref: VolumeRef) -> Version:
        """Resolve ``ref`` once and read the manifest of the version it names."""
        resolution = self._resolve(ref)
        store = self._object_store(ref, resolution)
        return Version(
            ref=ref.pinned(resolution.resolved.origin_digest),
            org_id=resolution.resolved.org_id,
            store=store,
            manifest=self._fetch_manifest(store, ref, resolution),
        )

    def _object_store(self, ref: VolumeRef, first: ResolveResponse) -> _s3.ObjectStore:
        # Refreshes re-resolve the pinned digest, never the tag: the tag may
        # have moved or vanished while this version is still being read.
        pinned = ref.pinned(first.resolved.origin_digest)
        source = _CredentialSource(first.origin, lambda: self._resolve(pinned).origin)
        return _s3.ObjectStore(self._http_client, source, timeout=self._timeout)

    def _fetch_manifest(
        self, store: _s3.ObjectStore, ref: VolumeRef, resolution: ResolveResponse
    ) -> Manifest:
        resolved = resolution.resolved
        key = object_key(resolved.org_id, ref.namespace, resolved.target.relative_key)
        body, content_type = store.get(key)
        data = _s3.decode_object(
            body,
            content_type,
            expected_kind="manifest",
            expected_digest=resolved.origin_digest,
        )
        return _manifest.parse_manifest(data)


class _CredentialSource:
    """Current origin credentials, re-resolved before they expire or when the bucket rejects them."""

    def __init__(
        self, first: OriginCredentials, resolve: Callable[[], OriginCredentials]
    ) -> None:
        self._current = first
        self._resolve = resolve
        self._lock = threading.Lock()

    def current(self) -> OriginCredentials:
        with self._lock:
            expires_at = self._current.expires_at
            if expires_at is not None and expires_at - _now() <= _EXPIRY_MARGIN:
                logger.info("origin credentials near expiry; resolving again")
                self._current = self._resolve()
            return self._current

    def refresh(self) -> OriginCredentials:
        with self._lock:
            logger.info("origin bucket rejected the credentials; resolving again")
            self._current = self._resolve()
            return self._current


class _Materializer:
    """Writes one manifest below ``root``, fanning chunk reads out over a thread pool."""

    def __init__(
        self,
        version: Version,
        root: Path,
        layout: _manifest.Layout,
        options: VolumeClientOptions,
    ) -> None:
        self._version = version
        self._root = root
        self._layout = layout
        self._options = options
        self._budget = _materialize.ByteBudget(options.max_bytes_in_flight)

    def run(
        self, manifest: Manifest, selected: set[str] | None
    ) -> tuple[int, int, int]:
        """Return bytes written, regular files created (hardlinks included), and chunks fetched."""
        # The layout places every selected entry but the directories a
        # stripped prefix elides.
        entries = [
            e
            for e in manifest.entries
            if (selected is None or e.clean_path in selected)
            and self._layout.dest(e.clean_path) is not None
        ]
        groups = _manifest.hardlink_groups(entries)
        linked = {path for paths in groups.values() for path in paths[1:]}
        files: list[FileEntry] = [
            e
            for e in entries
            if isinstance(e, (ChunkFileEntry, ChunkmapFileEntry))
            and e.clean_path not in linked
        ]

        # Every directory first, recorded or implied, on one thread: the pool
        # then never races on mkdir. Modes and times come last, deepest first,
        # so a read-only directory cannot block its own children and creating
        # children cannot bump a parent's stamped mtime.
        for path in _manifest.implicit_directories(
            self._dest(e.clean_path) for e in entries
        ):
            _materialize.ensure_dir(self._root, path)
        directories: list[DirectoryEntry] = []
        for entry in entries:
            if isinstance(entry, DirectoryEntry):
                _materialize.ensure_dir(self._root, self._dest(entry.clean_path))
                directories.append(entry)
            elif isinstance(entry, SymlinkEntry):
                _materialize.create_symlink(
                    self._join(entry.clean_path),
                    self._layout.symlink_target(entry),
                )

        written, chunks = self._fetch_files(files)

        for paths in groups.values():
            source = self._join(paths[0])
            for path in paths[1:]:
                _materialize.create_hardlink(self._join(path), source)
        for entry in files:
            self._stamp(entry)
        for entry in sorted(
            directories, key=lambda d: d.clean_path.count("/"), reverse=True
        ):
            self._stamp(entry)
        return written, len(files) + len(linked), chunks

    def _stamp(self, entry: DirectoryEntry | FileEntry) -> None:
        path = self._join(entry.clean_path)
        if entry.mtime_ns is not None:
            _materialize.apply_mtime(path, entry.mtime_ns)
        _materialize.apply_mode(path, entry.mode_bits)

    def _dest(self, entry_path: str) -> str:
        dest = self._layout.dest(entry_path)
        assert dest is not None, f"{entry_path} was filtered out of the run"
        return dest

    def _join(self, entry_path: str) -> Path:
        return _materialize.contained_join(self._root, self._dest(entry_path))

    def _fetch_files(self, files: Sequence[FileEntry]) -> tuple[int, int]:
        # Chunkmap reads are planned on the pool and their chunks submitted as
        # each plan lands, so payload transfer starts before every chunkmap is
        # in. Only the main thread submits, so no worker ever waits on another.
        with ThreadPoolExecutor(max_workers=self._options.max_concurrency) as pool:
            plans = [pool.submit(self._plan_file, entry) for entry in files]
            fetches: list[Future[int]] = []
            for plan in as_completed(plans):
                path, chunks = plan.result()
                fetches.extend(
                    pool.submit(self._fetch_chunk, path, chunk) for chunk in chunks
                )
            return sum(fetch.result() for fetch in fetches), len(fetches)

    def _plan_file(self, entry: FileEntry) -> tuple[Path, list[ChunkEntry]]:
        chunks = self._version.chunks(entry)
        path = self._join(entry.clean_path)
        _materialize.create_file(path, entry.size)
        return path, chunks

    def _fetch_chunk(self, path: Path, chunk: ChunkEntry) -> int:
        charge = 2 * chunk.length
        self._budget.acquire(charge)
        try:
            _materialize.write_at(path, chunk.offset, self._version.read_chunk(chunk))
            return chunk.length
        finally:
            self._budget.release(charge)


def _parse(ref: str | VolumeRef) -> VolumeRef:
    return VolumeRef.parse(ref) if isinstance(ref, str) else ref


def _volume_or_point(ref: str | VolumeRef) -> VolumeRef:
    parsed = _parse(ref)
    if parsed.level is VolumeRefLevel.NAMESPACE:
        raise VolumeRefError(
            f"ref {parsed} names a namespace; this operation needs a volume"
        )
    return parsed


def _ref_include(ref: VolumeRef) -> list[str]:
    """A ref path narrows exactly as an include entry would; ``/`` narrows nothing."""
    return [ref.path.removeprefix("/")] if ref.path and ref.path != "/" else []


def _paginate(
    page: Callable[[str | None], tuple[list[_T], api.PaginationResponse]],
) -> list[_T]:
    """Every item across a cursor-paginated listing."""
    items: list[_T] = []
    cursor: str | None = None
    seen: set[str] = set()
    while True:
        batch, pagination = page(cursor)
        items.extend(batch)
        if not pagination.has_more or pagination.cursor is None:
            return items
        if pagination.cursor in seen:
            raise VolumeProtocolError(
                f"Baseten API repeated pagination cursor {pagination.cursor!r}"
            )
        seen.add(pagination.cursor)
        cursor = pagination.cursor


def _refuse_recursive_inventory(recursive: bool, what: str) -> None:
    if recursive:
        raise VolumeRefError(
            f"recursive=True lists entries within a version; listing {what} takes no recursion"
        )


def _version_selector(ref: VolumeRef) -> str:
    """The version ``ref`` selects, spelled the way the API's path segment takes it."""
    if ref.digest is not None:
        return f"@{ref.digest}"
    if ref.tag is not None:
        return f":{ref.tag}"
    return "head"


def _pinned_ref(namespace: str, volume: str, digest: str) -> VolumeRef:
    try:
        return VolumeRef(namespace=namespace, volume=volume).pinned(digest)
    except VolumeRefError as error:
        raise VolumeProtocolError(
            f"Baseten API returned an unusable digest: {error}"
        ) from error


def _volume(volume: api.Volume) -> Volume:
    head = volume.head
    return Volume(
        ref=VolumeRef(namespace=volume.namespace, volume=volume.name),
        sequence=volume.sequence,
        updated_at=volume.updated_at,
        head=None
        if head is None
        else VolumeHead(
            version_ref=_pinned_ref(volume.namespace, volume.name, head.digest),
            digest=head.digest,
            total_size=head.total_size_bytes,
            created_at=head.created_at,
        ),
        tags=[VolumeTag(name=tag.name, digest=tag.digest) for tag in volume.tags],
        tag_count=volume.tag_count,
        versions_alive=volume.versions_alive,
        versions_tombstoned=volume.versions_tombstoned,
        versions_untagged=volume.versions_untagged,
    )


def _version_fields(
    version: api.VolumeVersion | api.VolumeVersionDetail,
) -> dict[str, object]:
    """The fields :class:`VolumeVersion` shares with :class:`VolumeVersionDetail`."""
    return {
        "version_ref": _pinned_ref(version.namespace, version.volume, version.digest),
        "digest": version.digest,
        "sequence": version.sequence,
        "lifecycle": version.lifecycle,
        "is_head": version.is_head,
        "tags": version.tags,
        "total_size": version.total_size_bytes,
        "created_at": version.created_at,
        "tombstoned_at": version.tombstoned_at,
        "delete_after": version.delete_after,
    }


def _staging_dir(dest: Path) -> Path:
    return dest.parent / f".{dest.name}.partial-{secrets.token_hex(4)}"


def _check_destination(dest: Path, overwrite: bool) -> None:
    if not dest.exists():
        return
    if not dest.is_dir():
        raise VolumeDestinationError(f"{dest} exists and is not a directory")
    if not overwrite and any(dest.iterdir()):
        raise VolumeDestinationError(
            f"{dest} is not empty; pass overwrite=True to write into it in place"
        )


def _check_free_space(dest: Path, total_size: int) -> None:
    probe = (
        dest
        if dest.exists()
        else next(parent for parent in dest.parents if parent.exists())
    )
    free = shutil.disk_usage(probe).free
    if free < total_size:
        raise VolumeDestinationError(
            f"{dest} has {free} bytes free, the volume needs {total_size}; nothing was written"
        )


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)
