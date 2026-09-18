"""Cannery wire types for the volume read path. Nothing here is exported.

These models mirror cannery's resolve response field for field; the public
surface is :class:`VolumeRef`, :class:`VolumeManifest`, and friends in
``_models``. Cannery is the BDN control service. A client never reads volume bytes through
it: it asks cannery to resolve a ref and gets back the version's manifest
digest plus scoped credentials for the origin bucket. The token cannery
expects is minted through the Baseten API, which baseten-python wraps.
"""

from __future__ import annotations

from datetime import datetime
from typing import TypeVar

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from baseten.bdn.volumes._errors import (
    VolumeAPIError,
    VolumeConnectionError,
    VolumeProtocolError,
)

# pydantic's `pattern` searches rather than matches, so these are anchored.
RELATIVE_KEY_PATTERN = r"^objects/b3/[0-9a-f]{2}/[0-9a-f]{2}/[0-9a-f]{64}$"
DIGEST_PATTERN = r"^b3:[0-9a-f]{64}$"

_ModelT = TypeVar("_ModelT", bound=BaseModel)


class ObjectTarget(BaseModel):
    """Where an object lives in the origin bucket, relative to the namespace's key prefix.

    The full key is ``bdn/<org_id>/<namespace>/<relative_key>``; every chunk,
    chunkmap, and manifest is addressed this way.
    """

    model_config = ConfigDict(frozen=True)

    relative_key: str = Field(pattern=RELATIVE_KEY_PATTERN)


class ResolvedRef(BaseModel):
    """The version a ref resolved to, as cannery reports it."""

    reference: str
    """The ref as cannery parsed it."""

    org_id: str
    """Organization the token belongs to; part of every object key."""

    origin_digest: str = Field(pattern=DIGEST_PATTERN)
    """BLAKE3 digest of the version's manifest; the pin the public API returns."""

    kind: str
    """Always ``manifest`` from resolve."""

    target: ObjectTarget
    """Where the manifest object lives."""

    sequence: int | None = None
    """Snapshot sequence the version was committed at; absent for old versions."""

    resolved_from: str
    """``head``, ``tag``, or ``pin``."""


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


class _CanneryErrorBody(BaseModel):
    message: str
    code: str | None = None
    reason: str | None = None


class _CanneryErrorEnvelope(BaseModel):
    error: _CanneryErrorBody


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


def connection_error(service: str, error: Exception) -> VolumeConnectionError:
    return VolumeConnectionError(f"could not reach {service}: {error}")
