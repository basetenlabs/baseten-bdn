"""HTTP plumbing shared by the BDN node-local APIs.

Every node-local BDN service speaks HTTP+JSON over a Unix socket the pod sees
under ``/bdn`` and reports failures with one error body. This module owns those
two facts so each API client only adds its routes and resource types.
"""

from __future__ import annotations

import functools
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import httpx
from pydantic import BaseModel

# httpx requires an absolute URL even when the transport is a Unix socket; the
# host is never resolved, so any stable placeholder works.
SOCKET_BASE_URL = "http://bdn.local"

# A local socket connects at once or not at all. Without this bound the async
# transport spins on a full listen backlog until the read timeout expires.
CONNECT_TIMEOUT_SEC = 5.0


class ErrorBody(BaseModel):
    """The error body every node-local BDN API returns on a non-2xx status.

    Unknown fields are ignored: the daemon ships in the node image and this
    package in the model image, so an additive daemon field must not turn a
    retryable error into a protocol error.
    """

    code: str
    message: str
    retryable: bool


def request_timeout(read_timeout_sec: float) -> httpx.Timeout:
    return httpx.Timeout(read_timeout_sec, connect=CONNECT_TIMEOUT_SEC)


@functools.cache
def user_agent() -> str:
    try:
        return f"baseten-bdn/{version('baseten-bdn')}"
    except PackageNotFoundError:
        return "baseten-bdn/unknown"


def unix_socket_client(socket_path: Path, *, timeout_sec: float) -> httpx.Client:
    return httpx.Client(
        transport=httpx.HTTPTransport(uds=str(socket_path)),
        base_url=SOCKET_BASE_URL,
        timeout=request_timeout(timeout_sec),
        headers=_default_headers(),
    )


def unix_socket_async_client(
    socket_path: Path, *, timeout_sec: float
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.AsyncHTTPTransport(uds=str(socket_path)),
        base_url=SOCKET_BASE_URL,
        timeout=request_timeout(timeout_sec),
        headers=_default_headers(),
    )


def _default_headers() -> dict[str, str]:
    return {"Accept": "application/json", "User-Agent": user_agent()}
