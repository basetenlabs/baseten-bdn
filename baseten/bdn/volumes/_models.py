from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class ResolvedFrom(StrEnum):
    HEAD = "head"
    TAG = "tag"
    PIN = "pin"


class ResolvedVolume(BaseModel):
    """One volume version, as cannery resolved it."""

    model_config = ConfigDict(frozen=True)

    reference: str
    """The canonical ref that was resolved, ``bdn://<ns>/<vol>[:tag|@hex]``."""

    org_id: str
    """Organization the token resolved to; part of every object key."""

    digest: str
    """BLAKE3 digest of the version's manifest, ``b3:<64 hex>``."""

    resolved_from: ResolvedFrom

    sequence: int | None = None
    """Snapshot sequence the version was committed at; absent for old versions."""


class EntryKind(StrEnum):
    FILE = "file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"


class FileInfo(BaseModel):
    """One manifest entry, for callers that inspect before pulling."""

    model_config = ConfigDict(frozen=True)

    path: str
    """Path inside the volume, no leading slash."""

    kind: EntryKind
    size: int = Field(ge=0)
    """Bytes for a file; zero for directories and symlinks."""

    mode: int
    """POSIX permission bits."""

    link_target: str | None = None
    """Recorded symlink target, only for ``kind == "symlink"``."""


class PullResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    reference: str
    digest: str
    dest_dir: Path
    files: int = Field(ge=0)
    """Regular files written, hardlinks included."""

    bytes: int = Field(ge=0)
    """Bytes of file content written."""

    duration_sec: float = Field(ge=0)


class VolumeError(Exception):
    """Base class for every error raised by the volumes client."""


class VolumeConnectionError(VolumeError):
    """A request to the Baseten API, cannery, or S3 could not complete."""


class VolumeAPIError(VolumeError):
    """The Baseten API or cannery rejected a request.

    ``reason`` is cannery's stable identifier (``NOT_FOUND``,
    ``PERMISSION_DENIED``, ``AMBIGUOUS_PREFIX``, ...) when the failure came
    from cannery; ``code`` is the Baseten API's classification.
    """

    def __init__(
        self,
        service: str,
        status_code: int,
        message: str,
        *,
        code: str | None = None,
        reason: str | None = None,
    ) -> None:
        super().__init__(service, status_code, message, code, reason)
        self.service = service
        self.status_code = status_code
        self.message = message
        self.code = code
        self.reason = reason

    def __str__(self) -> str:
        label = self.reason or self.code or "error"
        return f"{self.service} request failed with HTTP {self.status_code} ({label}): {self.message}"


class VolumeProtocolError(VolumeError):
    """A response does not match the contract: bad JSON, unknown content type, bad manifest."""


class VolumeIntegrityError(VolumeError):
    """Downloaded bytes do not match the recorded digest or length."""


class VolumePathError(VolumeError):
    """A manifest entry would land outside the destination directory."""
