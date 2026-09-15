"""Cannery and Baseten API wire types for the volume read path.

Cannery is the BDN control service. A client never reads volume bytes through
it: it mints a token at the Baseten API, asks cannery to resolve a ref, and
gets back the version's manifest digest plus scoped credentials for the
origin bucket.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, TypeVar

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from baseten.bdn.volumes._models import (
    ResolvedFrom,
    VolumeAPIError,
    VolumeConnectionError,
    VolumeProtocolError,
    VolumeRefError,
)

_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._-]{0,255}")
_HEX_PREFIX = re.compile(r"[0-9a-f]{12,64}")
_TAG = re.compile(r"[^\s/:@]+")
# pydantic's `pattern` searches rather than matches, so these are anchored.
RELATIVE_KEY_PATTERN = r"^objects/b3/[0-9a-f]{2}/[0-9a-f]{2}/[0-9a-f]{64}$"
DIGEST_PATTERN = r"^b3:[0-9a-f]{64}$"

_ModelT = TypeVar("_ModelT", bound=BaseModel)


class VolumeRef(BaseModel):
    """``bdn:<ns>/<vol>`` with an optional ``:tag`` or ``@<digest prefix>`` selector.

    Parses both spellings, ``bdn:`` and the legacy ``bdn://``, and renders
    the canonical form cannery produces. ``@`` always means a digest pin, so
    it is split off before ``:``. Namespace and volume are case-folded the
    way every server hop folds them.
    """

    model_config = ConfigDict(frozen=True)

    namespace: str
    volume: str
    tag: str | None = None
    pin: str | None = None
    """Lowercase hex digest prefix, 12 to 64 characters, without ``b3:``."""

    @classmethod
    def parse(cls, ref: str) -> VolumeRef:
        rest = ref
        for scheme in ("bdn://", "bdn:"):
            if rest.startswith(scheme):
                rest = rest[len(scheme) :]
                break
        else:
            raise VolumeRefError(
                f"volume ref must start with bdn: or bdn://, got {ref!r}"
            )
        pin: str | None = None
        tag: str | None = None
        if "@" in rest:
            rest, raw_pin = rest.split("@", 1)
            raw_pin = raw_pin.removeprefix("b3:").lower()
            if not _HEX_PREFIX.fullmatch(raw_pin):
                raise VolumeRefError(
                    f"digest pin must be 12 to 64 hex characters, got {raw_pin!r} in {ref!r}"
                )
            pin = raw_pin
        elif ":" in rest:
            rest, tag = rest.split(":", 1)
            if not _TAG.fullmatch(tag):
                raise VolumeRefError(
                    f"tag must not be empty or contain whitespace, '/', ':' or '@': {ref!r}"
                )
        parts = rest.lower().split("/")
        if len(parts) != 2:
            raise VolumeRefError(
                f"volume ref must be bdn:<namespace>/<volume>, got {ref!r}"
            )
        namespace, volume = parts
        for label, value in (("namespace", namespace), ("volume", volume)):
            if not _IDENTIFIER.fullmatch(value):
                raise VolumeRefError(
                    f"{label} must be letters, digits, '.', '_' or '-': {value!r}"
                )
        return cls(namespace=namespace, volume=volume, tag=tag, pin=pin)

    def pinned(self, digest: str) -> VolumeRef:
        """The same volume pinned to ``digest`` (``b3:<hex>``), so it names one version forever."""
        return VolumeRef(
            namespace=self.namespace, volume=self.volume, pin=digest.removeprefix("b3:")
        )

    def canonical(self) -> str:
        selector = f"@{self.pin}" if self.pin else f":{self.tag}" if self.tag else ""
        return f"bdn://{self.namespace}/{self.volume}{selector}"


class ObjectTarget(BaseModel):
    """Where an object lives, relative to the namespace's key prefix."""

    model_config = ConfigDict(frozen=True)

    relative_key: str = Field(pattern=RELATIVE_KEY_PATTERN)


class ResolvedRef(BaseModel):
    reference: str
    org_id: str
    origin_digest: str = Field(pattern=DIGEST_PATTERN)
    kind: str
    target: ObjectTarget
    sequence: int | None = None
    resolved_from: ResolvedFrom


class OriginCredentials(BaseModel):
    """Scoped, short-lived credentials for the origin bucket."""

    model_config = ConfigDict(frozen=True)

    endpoint: str
    """Empty for AWS S3; a URL for any other S3-compatible origin."""

    region: str
    bucket: str
    access_key_id: str
    secret_access_key: str
    session_token: str | None = None
    expires_at: datetime | None = None


class ResolveResponse(BaseModel):
    resolved: ResolvedRef
    origin: OriginCredentials


class TokenResponse(BaseModel):
    token: str
    expires_at: datetime
    bdn_endpoint: str | None = None


class _CanneryErrorBody(BaseModel):
    message: str
    code: str | None = None
    reason: str | None = None


class _CanneryErrorEnvelope(BaseModel):
    error: _CanneryErrorBody


class _BasetenErrorBody(BaseModel):
    code: str | None = None
    message: str | None = None
    detail: str | None = None
    error: str | None = None


def token_request(ref: VolumeRef) -> dict[str, Any]:
    # The Baseten API spells scopes in SCREAMING_SNAKE; cannery's grants are lowercase.
    return {"scopes": ["PULL"], "namespaces": [ref.namespace], "volumes": [ref.volume]}


def raise_for_baseten_error(response: httpx.Response) -> None:
    if response.is_success:
        return
    message = response.text
    code = None
    try:
        body = _BasetenErrorBody.model_validate_json(response.content)
        code = body.code
        message = body.message or body.detail or body.error or message
    except ValidationError:
        pass
    raise VolumeAPIError("Baseten API", response.status_code, message, code=code)


def raise_for_cannery_error(response: httpx.Response) -> None:
    if response.is_success:
        return
    # A proxy in front of cannery may answer without the envelope; keep the
    # raw text rather than hiding the status behind a protocol error.
    try:
        envelope = _CanneryErrorEnvelope.model_validate_json(response.content)
    except ValidationError:
        raise VolumeAPIError("cannery", response.status_code, response.text) from None
    raise VolumeAPIError(
        "cannery",
        response.status_code,
        envelope.error.message,
        code=envelope.error.code,
        reason=envelope.error.reason,
    )


def parse_json(model: type[_ModelT], response: httpx.Response, what: str) -> _ModelT:
    try:
        return model.model_validate_json(response.content)
    except ValidationError as error:
        raise VolumeProtocolError(
            f"{what} response is off contract: {error}"
        ) from error


def connection_error(service: str, error: httpx.HTTPError) -> VolumeConnectionError:
    return VolumeConnectionError(f"could not reach {service}: {error}")
