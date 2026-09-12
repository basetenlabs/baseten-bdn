from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import httpx

from baseten.bdn import _http


@dataclass
class CapturedRequest:
    method: str
    path: str
    headers: dict[str, str]
    body: Any


@dataclass
class Reply:
    """One scripted answer: an HTTP response, or an httpx error to raise."""

    status_code: int = 200
    body: Any = None
    error: httpx.HTTPError | None = None


@dataclass
class FakeTransport:
    """Scripted stand-in for the Hot Load daemon.

    Answers requests in order from ``replies`` and repeats the last one, so a
    single-reply script is a fixed response and a longer script drives retries.
    Every request is captured, including ones answered with an error.
    """

    replies: Sequence[Reply]
    requests: list[CapturedRequest] = field(default_factory=list)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        content = request.content
        self.requests.append(
            CapturedRequest(
                method=request.method,
                path=request.url.raw_path.decode("ascii"),
                headers=dict(request.headers),
                body=json.loads(content) if content else None,
            )
        )
        reply = self.replies[min(len(self.requests), len(self.replies)) - 1]
        if reply.error is not None:
            raise reply.error
        if reply.body is None:
            return httpx.Response(reply.status_code)
        return httpx.Response(reply.status_code, json=reply.body)

    def sync_client(self) -> httpx.Client:
        return httpx.Client(
            transport=httpx.MockTransport(self._handle), base_url=_http.SOCKET_BASE_URL
        )

    def async_client(self) -> httpx.AsyncClient:
        async def handle(request: httpx.Request) -> httpx.Response:
            return self._handle(request)

        return httpx.AsyncClient(
            transport=httpx.MockTransport(handle), base_url=_http.SOCKET_BASE_URL
        )
