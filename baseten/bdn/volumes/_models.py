from __future__ import annotations

import datetime as dt
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from baseten.bdn.volumes._errors import (
    VolumeAPIError,
    VolumeConnectionError,
    VolumeDestinationError,
    VolumeError,
    VolumeIntegrityError,
    VolumePathError,
    VolumeProtocolError,
    VolumeRefError,
    VolumeStorageError,
    VolumeUnsupportedError,
)
from baseten.bdn.volumes._ref import VolumeRef

__all__ = [
    "PullResult",
    "VolumeAPIError",
    "VolumeConnectionError",
    "VolumeDestinationError",
    "VolumeEntry",
    "VolumeEntryKind",
    "VolumeError",
    "VolumeIntegrityError",
    "VolumeManifest",
    "VolumePathError",
    "VolumeProtocolError",
    "VolumeRefError",
    "VolumeStorageError",
    "VolumeUnsupportedError",
]


class VolumeEntryKind(StrEnum):
    """What a manifest entry is."""

    FILE = "file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"


class VolumeEntry(BaseModel):
    """One entry of a volume version: what it is and what it looks like, not how it is stored."""

    model_config = ConfigDict(frozen=True)

    path: str
    """Full path within the version, slash-prefixed with no trailing slash, spelled the
    way :attr:`VolumeRef.path` is, so ``ref.with_path(entry.path)`` names this entry."""

    kind: VolumeEntryKind

    size: int = Field(ge=0)
    """A file's length in bytes; zero for directories and symlinks."""

    mode: int
    """Recorded permission bits, including setuid, setgid, and sticky."""

    mtime: dt.datetime | None = None
    """Modification time recorded when the version was published, if any."""

    link_target: str | None = None
    """A symlink's target exactly as recorded; ``None`` for every other kind."""


class VolumeManifest(BaseModel):
    """A volume version's entries, read from its manifest."""

    model_config = ConfigDict(frozen=True)

    version_ref: VolumeRef
    """The volume pinned to the version that was read; quote it to read the same tree again."""

    entry_count: int = Field(ge=0)
    """Entries in the whole version, whatever a ref path narrowed ``entries`` to."""

    total_size: int = Field(ge=0)
    """Bytes across the whole version's files."""

    entries: list[VolumeEntry]
    """The entries kept after narrowing, in path order: a directory precedes what is under it."""


class PullResult(BaseModel):
    """What a completed pull wrote."""

    model_config = ConfigDict(frozen=True)

    version_ref: VolumeRef
    """The volume pinned to the version that was pulled; quote it to pull the same bytes again."""

    dest_dir: Path

    file_count: int = Field(ge=0)
    """Regular files written, hardlinks included."""

    bytes_written: int = Field(ge=0)

    selected_file_count: int = Field(ge=0)
    """Files the ref path and ``include`` narrowed to; equals ``total_file_count`` for a whole pull."""

    total_file_count: int = Field(ge=0)

    duration_sec: float = Field(ge=0)
