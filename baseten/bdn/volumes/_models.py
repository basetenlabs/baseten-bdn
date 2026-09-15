from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class ResolvedFrom(StrEnum):
    """How cannery chose the version: the volume head, a tag, or a digest pin."""

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
    """What a manifest entry materializes as."""

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
    """What a completed pull wrote."""

    model_config = ConfigDict(frozen=True)

    reference: str
    """The canonical ref that was pulled."""

    digest: str
    """Manifest digest of the version that was pulled; pin this to pull the same bytes again."""

    dest_dir: Path
    file_count: int = Field(ge=0)
    """Regular files written, hardlinks included."""

    bytes_written: int = Field(ge=0)
    """Bytes of file content written."""

    duration_sec: float = Field(ge=0)


class VolumeError(Exception):
    """Base class for every error raised by the volumes client."""


class VolumeRefError(VolumeError, ValueError):
    """A volume ref string does not parse."""


class VolumeUnsupportedError(VolumeError, NotImplementedError):
    """The volume or platform uses something this client does not support yet."""


class VolumeConnectionError(VolumeError):
    """A request to the Baseten API, cannery, or the origin bucket could not complete."""


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


class VolumeStorageError(VolumeError):
    """The origin bucket refused an object read.

    ``code`` is the S3 error code (``AccessDenied``, ``NoSuchKey``, ...) when
    the response carried one.
    """

    def __init__(
        self, status_code: int, key: str, message: str, *, code: str | None = None
    ) -> None:
        super().__init__(status_code, key, message, code)
        self.status_code = status_code
        self.key = key
        self.message = message
        self.code = code

    def __str__(self) -> str:
        return f"origin bucket answered HTTP {self.status_code} ({self.code or 'error'}) for {self.key}: {self.message}"


class VolumeProtocolError(VolumeError):
    """A response does not match the contract: bad JSON, unknown content type, bad manifest."""


class VolumeIntegrityError(VolumeError):
    """Downloaded bytes do not match the recorded digest or length."""


class VolumePathError(VolumeError):
    """A manifest entry would land outside the destination directory."""


class VolumeDestinationError(VolumeError):
    """The destination directory cannot take the volume: not a directory, no space, or unwritable."""
