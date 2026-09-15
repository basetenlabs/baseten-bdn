"""Clients for the pod-local BDN Hot Load API.

Hot Load gives an opted-in model pod a read-only front door at ``/bdn``: the
Unix socket ``/bdn/hotload.sock`` and the directory ``/bdn/mounts``. An attach
names a BDN volume ref and a target directory name; the node resolves the ref
to an immutable version, binds that view at ``/bdn/mounts/<target>``, and only
then answers, so a successful attach is readable as soon as it returns.

Wire contract (HTTP+JSON over the socket):

* ``POST /v1/hotload/volumes`` with ``{"source", "target", "include"?,
  "exclude"?}`` and a required ``Idempotency-Key`` header answers ``202`` with
  the attachment. The same key with the same body replays the stored
  attachment; the same key with a different body is ``409``.
* ``GET /v1/hotload/volumes`` answers ``{"volumes": [...]}``.
* ``GET`` / ``DELETE /v1/hotload/volumes/{id}`` read or detach one attachment.
* ``GET /healthz`` answers ``204``.
* Errors carry ``{"code", "message", "retryable"}``.
"""

from __future__ import annotations

import asyncio
import re
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self, TypeVar

import httpx
from pydantic import BaseModel, ValidationError

from baseten.bdn._useragent import user_agent
from baseten.bdn.hotload._models import (
    AttachmentState,
    ErrorBody,
    HotLoadAPIError,
    HotLoadAttachError,
    HotLoadConnectionError,
    HotLoadError,
    HotLoadProtocolError,
    HotLoadTimeoutError,
    VolumeAttachment,
)

DEFAULT_SOCKET_PATH = Path("/bdn/hotload.sock")
DEFAULT_REQUEST_TIMEOUT_SEC = 600.0
DEFAULT_MAX_RETRIES = 2
DEFAULT_RETRY_INTERVAL_SEC = 0.1

# httpx requires an absolute URL even when the transport is a Unix socket; the
# host is never resolved, so any stable placeholder works.
SOCKET_BASE_URL = "http://bdn.local"
# A local socket connects at once or not at all. Without this bound the async
# transport spins on a full listen backlog until the read timeout expires.
_CONNECT_TIMEOUT_SEC = 5.0
# Reads are in-memory lookups on the daemon; only attach and detach touch mounts.
_LOOKUP_TIMEOUT_SEC = 10.0

_VOLUMES_PATH = "/v1/hotload/volumes"
_HEALTH_PATH = "/healthz"
_IDEMPOTENCY_HEADER = "Idempotency-Key"
# Daemon ids are `vol_` followed by a ULID; anything else never names an
# attachment, and `.`/`?`/`#` would be reinterpreted by URL parsing.
_ATTACHMENT_ID = re.compile(r"[A-Za-z0-9_-]+")
_INTERRUPTED_HINT = (
    " The daemon may have finished or abandoned it; an abandoned attach stays listed"
    " as FAILED and holds its target until detached. Inspect list_attachments before"
    " retrying."
)

_ModelT = TypeVar("_ModelT", bound=BaseModel)


@dataclass(frozen=True)
class HotLoadClientOptions:
    """Options for :class:`HotLoadClient` and :class:`AsyncHotLoadClient`."""

    socket_path: Path = DEFAULT_SOCKET_PATH
    """The pod's Hot Load socket."""

    request_timeout_sec: float = DEFAULT_REQUEST_TIMEOUT_SEC
    """Bound on one attach or detach. An attach resolves the source, acquires
    the shared view, and binds it before answering, so this bounds a cold
    attach, not a round trip. Reads use a short fixed bound."""

    max_retries: int = DEFAULT_MAX_RETRIES
    """Retries after the first attempt, for failures that are safe to repeat."""

    retry_interval_sec: float = DEFAULT_RETRY_INTERVAL_SEC
    """Pause between attempts."""

    def __post_init__(self) -> None:
        if self.request_timeout_sec <= 0:
            raise ValueError("request_timeout_sec must be positive")
        if self.max_retries < 0:
            raise ValueError("max_retries must not be negative")
        if self.retry_interval_sec < 0:
            raise ValueError("retry_interval_sec must not be negative")


class HotLoadClient:
    """Synchronous client for one pod's Hot Load socket.

    Usable as a context manager; exiting closes the HTTP client and nothing
    else. Attachments belong to the pod, not to this object, and stay mounted
    until :meth:`detach` or pod teardown.
    """

    def __init__(
        self,
        *,
        socket_path: str | Path = DEFAULT_SOCKET_PATH,
        request_timeout_sec: float = DEFAULT_REQUEST_TIMEOUT_SEC,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_interval_sec: float = DEFAULT_RETRY_INTERVAL_SEC,
        http_client_override: httpx.Client | None = None,
        close_http_client_on_close: bool | None = None,
    ) -> None:
        """Create a synchronous Hot Load client.

        Args:
            socket_path: The pod's Hot Load socket.
            request_timeout_sec: Bound on one attach or detach; see
                :attr:`HotLoadClientOptions.request_timeout_sec`.
            max_retries: Retries after the first attempt.
            retry_interval_sec: Pause between attempts.
            http_client_override: Pre-configured httpx client. The caller owns
                its transport, base URL, and headers; timeouts are still set
                per request from *request_timeout_sec*.
            close_http_client_on_close: Whether :meth:`close` closes the HTTP
                client. Defaults to ``True`` for a client created here and
                ``False`` for *http_client_override*.
        """
        self._options = HotLoadClientOptions(
            socket_path=Path(socket_path),
            request_timeout_sec=request_timeout_sec,
            max_retries=max_retries,
            retry_interval_sec=retry_interval_sec,
        )
        self._http_client = (
            _unix_socket_client(
                self._options.socket_path, timeout_sec=request_timeout_sec
            )
            if http_client_override is None
            else http_client_override
        )
        self.close_http_client_on_close = (
            http_client_override is None
            if close_http_client_on_close is None
            else close_http_client_on_close
        )

    @property
    def options(self) -> HotLoadClientOptions:
        """The options this client was constructed with."""
        return self._options

    @property
    def http_client(self) -> httpx.Client:
        """The underlying httpx client."""
        return self._http_client

    def attach(
        self,
        source: str,
        *,
        target: str,
        include: Sequence[str] = (),
        exclude: Sequence[str] = (),
    ) -> VolumeAttachment:
        """Attach ``source`` at ``/bdn/mounts/<target>`` and return it once readable.

        Each call mints one idempotency key and reuses it across its own
        retries, so a retried request cannot attach the same volume twice.

        Args:
            source: A BDN volume ref, ``bdn:<namespace>/<volume>`` optionally
                pinned with ``@<digest>`` or ``:<tag>``. Paths inside a volume
                are not accepted; narrow with *include* instead.
            target: One directory name below ``/bdn/mounts``: letters, digits,
                ``.``, ``_``, ``-``, starting alphanumeric, at most 128 bytes.
                Each name can hold one attachment at a time.
            include: Glob filters selecting the files to materialize.
            exclude: Glob filters removing files from the selection.

        Raises:
            HotLoadAttachError: The daemon answered with a ``FAILED``
                attachment, which still holds the target; detach it first.
            HotLoadAPIError: The daemon rejected the request.
            HotLoadTimeoutError: No answer within the request timeout. The
                attach may have completed or been abandoned on the node;
                check :meth:`list_attachments`.
        """
        body = self._request(
            "POST",
            _VOLUMES_PATH,
            json=_attach_request(source, target, include, exclude),
            headers={_IDEMPOTENCY_HEADER: uuid.uuid4().hex},
        )
        return _ready_attachment(_parse(VolumeAttachment, body))

    def list_attachments(self) -> list[VolumeAttachment]:
        """List this pod's attachments, including ``FAILED`` ones."""
        body = self._request("GET", _VOLUMES_PATH, timeout_sec=_LOOKUP_TIMEOUT_SEC)
        return _parse(_AttachmentList, body).volumes

    def get_attachment(self, attachment_id: str) -> VolumeAttachment:
        """Return one attachment by id, whatever its state."""
        body = self._request(
            "GET", _volume_path(attachment_id), timeout_sec=_LOOKUP_TIMEOUT_SEC
        )
        return _parse(VolumeAttachment, body)

    def detach(self, attachment_id: str) -> None:
        """Remove the attachment's bind from ``/bdn/mounts``. Returns once it is gone."""
        self._request("DELETE", _volume_path(attachment_id))

    def healthy(self) -> bool:
        """Whether the pod's Hot Load daemon answers on its socket right now.

        One short probe, no retries. Any failure, including the daemon
        reporting itself unavailable, reads as unhealthy.
        """
        try:
            self._request(
                "GET", _HEALTH_PATH, timeout_sec=_LOOKUP_TIMEOUT_SEC, max_retries=0
            )
        except HotLoadError:
            return False
        return True

    def close(self) -> None:
        """Close the HTTP client if this object owns it. Attachments are unaffected."""
        if self.close_http_client_on_close:
            self._http_client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> bytes:
        attempts = _attempts(self._options, max_retries)
        timeout = _timeout(self._options, timeout_sec)
        for attempt in range(1, attempts + 1):
            try:
                response = self._http_client.request(
                    method, path, json=json, headers=headers, timeout=timeout
                )
                return _classify_response(response)
            except httpx.HTTPError as error:
                if attempt == attempts or not _retry_http_error(error, method):
                    raise _http_failure(
                        error, method, self._options.socket_path
                    ) from error
            except HotLoadAPIError as error:
                if attempt == attempts or not error.retryable:
                    raise
            time.sleep(self._options.retry_interval_sec)
        raise AssertionError("unreachable: the retry loop returns or raises")


class AsyncHotLoadClient:
    """Asynchronous client for one pod's Hot Load socket.

    Same surface as :class:`HotLoadClient`; usable as an async context manager.
    """

    def __init__(
        self,
        *,
        socket_path: str | Path = DEFAULT_SOCKET_PATH,
        request_timeout_sec: float = DEFAULT_REQUEST_TIMEOUT_SEC,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_interval_sec: float = DEFAULT_RETRY_INTERVAL_SEC,
        http_client_override: httpx.AsyncClient | None = None,
        close_http_client_on_close: bool | None = None,
    ) -> None:
        """Create an asynchronous Hot Load client. Arguments as :class:`HotLoadClient`."""
        self._options = HotLoadClientOptions(
            socket_path=Path(socket_path),
            request_timeout_sec=request_timeout_sec,
            max_retries=max_retries,
            retry_interval_sec=retry_interval_sec,
        )
        self._http_client = (
            _unix_socket_async_client(
                self._options.socket_path, timeout_sec=request_timeout_sec
            )
            if http_client_override is None
            else http_client_override
        )
        self.close_http_client_on_close = (
            http_client_override is None
            if close_http_client_on_close is None
            else close_http_client_on_close
        )

    @property
    def options(self) -> HotLoadClientOptions:
        """The options this client was constructed with."""
        return self._options

    @property
    def http_client(self) -> httpx.AsyncClient:
        """The underlying httpx client."""
        return self._http_client

    async def attach(
        self,
        source: str,
        *,
        target: str,
        include: Sequence[str] = (),
        exclude: Sequence[str] = (),
    ) -> VolumeAttachment:
        """Attach ``source`` at ``/bdn/mounts/<target>``. See :meth:`HotLoadClient.attach`."""
        body = await self._request(
            "POST",
            _VOLUMES_PATH,
            json=_attach_request(source, target, include, exclude),
            headers={_IDEMPOTENCY_HEADER: uuid.uuid4().hex},
        )
        return _ready_attachment(_parse(VolumeAttachment, body))

    async def list_attachments(self) -> list[VolumeAttachment]:
        """List this pod's attachments, including ``FAILED`` ones."""
        body = await self._request(
            "GET", _VOLUMES_PATH, timeout_sec=_LOOKUP_TIMEOUT_SEC
        )
        return _parse(_AttachmentList, body).volumes

    async def get_attachment(self, attachment_id: str) -> VolumeAttachment:
        """Return one attachment by id, whatever its state."""
        body = await self._request(
            "GET", _volume_path(attachment_id), timeout_sec=_LOOKUP_TIMEOUT_SEC
        )
        return _parse(VolumeAttachment, body)

    async def detach(self, attachment_id: str) -> None:
        """Remove the attachment's bind from ``/bdn/mounts``. Returns once it is gone."""
        await self._request("DELETE", _volume_path(attachment_id))

    async def healthy(self) -> bool:
        """Whether the daemon answers right now. See :meth:`HotLoadClient.healthy`."""
        try:
            await self._request(
                "GET", _HEALTH_PATH, timeout_sec=_LOOKUP_TIMEOUT_SEC, max_retries=0
            )
        except HotLoadError:
            return False
        return True

    async def close(self) -> None:
        """Close the HTTP client if this object owns it. Attachments are unaffected."""
        if self.close_http_client_on_close:
            await self._http_client.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> bytes:
        attempts = _attempts(self._options, max_retries)
        timeout = _timeout(self._options, timeout_sec)
        for attempt in range(1, attempts + 1):
            try:
                response = await self._http_client.request(
                    method, path, json=json, headers=headers, timeout=timeout
                )
                return _classify_response(response)
            except httpx.HTTPError as error:
                if attempt == attempts or not _retry_http_error(error, method):
                    raise _http_failure(
                        error, method, self._options.socket_path
                    ) from error
            except HotLoadAPIError as error:
                if attempt == attempts or not error.retryable:
                    raise
            await asyncio.sleep(self._options.retry_interval_sec)
        raise AssertionError("unreachable: the retry loop returns or raises")


class _AttachmentList(BaseModel):
    volumes: list[VolumeAttachment]


def _unix_socket_client(socket_path: Path, *, timeout_sec: float) -> httpx.Client:
    return httpx.Client(
        transport=httpx.HTTPTransport(uds=str(socket_path)),
        base_url=SOCKET_BASE_URL,
        timeout=_request_timeout(timeout_sec),
        headers=_default_headers(),
    )


def _unix_socket_async_client(
    socket_path: Path, *, timeout_sec: float
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.AsyncHTTPTransport(uds=str(socket_path)),
        base_url=SOCKET_BASE_URL,
        timeout=_request_timeout(timeout_sec),
        headers=_default_headers(),
    )


def _request_timeout(read_timeout_sec: float) -> httpx.Timeout:
    return httpx.Timeout(read_timeout_sec, connect=_CONNECT_TIMEOUT_SEC)


def _default_headers() -> dict[str, str]:
    return {"Accept": "application/json", "User-Agent": user_agent()}


def _attempts(options: HotLoadClientOptions, max_retries: int | None) -> int:
    return (options.max_retries if max_retries is None else max_retries) + 1


def _timeout(options: HotLoadClientOptions, timeout_sec: float | None) -> httpx.Timeout:
    return _request_timeout(
        options.request_timeout_sec if timeout_sec is None else timeout_sec
    )


def _attach_request(
    source: str, target: str, include: Sequence[str], exclude: Sequence[str]
) -> dict[str, Any]:
    body: dict[str, Any] = {"source": source, "target": target}
    if include:
        body["include"] = list(include)
    if exclude:
        body["exclude"] = list(exclude)
    return body


def _volume_path(attachment_id: str) -> str:
    if not _ATTACHMENT_ID.fullmatch(attachment_id):
        raise ValueError(
            "attachment_id must be one path segment of letters, digits, '_' or '-'"
        )
    return f"{_VOLUMES_PATH}/{attachment_id}"


def _never_sent(error: httpx.HTTPError) -> bool:
    return isinstance(
        error, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)
    )


def _retry_http_error(error: httpx.HTTPError, method: str) -> bool:
    # A request the daemon never saw can be repeated by any method. Once it may
    # have been sent, only GET is safe: repeating an attach would replay the
    # key onto a record the daemon may have abandoned, and a repeated detach
    # would report NOT_FOUND for a delete that went through.
    if not isinstance(error, httpx.TransportError):
        return False
    return _never_sent(error) or method == "GET"


def _http_failure(
    error: httpx.HTTPError, method: str, socket_path: Path
) -> HotLoadError:
    if not isinstance(error, httpx.TransportError):
        return HotLoadProtocolError(
            f"Hot Load {method} through {socket_path} returned an unusable response: {error}"
        )
    if _never_sent(error):
        return HotLoadConnectionError(
            f"could not connect to Hot Load through {socket_path}: {error}"
        )
    hint = "" if method == "GET" else _INTERRUPTED_HINT
    if isinstance(error, httpx.TimeoutException):
        return HotLoadTimeoutError(
            f"Hot Load {method} through {socket_path} timed out.{hint}"
        )
    return HotLoadConnectionError(
        f"Hot Load {method} through {socket_path} was interrupted: {error}.{hint}"
    )


def _classify_response(response: httpx.Response) -> bytes:
    """Return the 2xx body, or raise the daemon's error."""
    if 200 <= response.status_code < 300:
        return response.content
    try:
        parsed = ErrorBody.model_validate_json(response.content)
    except ValidationError as error:
        raise HotLoadProtocolError(
            f"Hot Load returned HTTP {response.status_code} without a valid error body"
        ) from error
    raise HotLoadAPIError(
        response.status_code, parsed.code, parsed.message, parsed.retryable
    )


def _parse(model: type[_ModelT], body: bytes) -> _ModelT:
    try:
        return model.model_validate_json(body)
    except ValidationError as error:
        raise HotLoadProtocolError(
            f"Hot Load returned an invalid {model.__name__}: {error}"
        ) from error


def _ready_attachment(attachment: VolumeAttachment) -> VolumeAttachment:
    if attachment.state is not AttachmentState.READY:
        raise HotLoadAttachError(attachment)
    return attachment
