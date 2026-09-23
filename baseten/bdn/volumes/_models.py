from __future__ import annotations

import datetime as dt
from enum import StrEnum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from baseten.bdn.volumes._ref import VolumeRef


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

    size: int | None = Field(default=None, ge=0)
    """A file's length in bytes; ``None`` for directories and symlinks, which have none of their own."""

    mode: int | None = None
    """Recorded permission bits, including setuid, setgid, and sticky; ``None`` when unrecorded."""

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


class VolumeNamespace(BaseModel):
    """A volume namespace the API key can read."""

    model_config = ConfigDict(frozen=True)

    name: str

    @property
    def ref(self) -> VolumeRef:
        return VolumeRef(namespace=self.name)


class VolumeTag(BaseModel):
    """A tag and the digest of the version it points at."""

    model_config = ConfigDict(frozen=True)

    name: str
    """Tag name; tags are case-sensitive."""

    digest: str


class VolumeHead(BaseModel):
    """The version a volume's head points at, which a ref with no tag or digest reads."""

    model_config = ConfigDict(frozen=True)

    version_ref: VolumeRef
    """The volume pinned to its head version."""

    digest: str
    total_size: int = Field(ge=0)
    created_at: dt.datetime


class Volume(BaseModel):
    """A volume: its head, its tags, and how many versions it holds."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["volume"] = "volume"

    ref: VolumeRef
    """``bdn:<namespace>/<volume>``, which reads the head."""

    sequence: int
    """Revision counter, bumped on every commit and tag change."""

    updated_at: dt.datetime

    head: VolumeHead | None
    """``None`` when the volume has no head, or the API key cannot read it."""

    tags: list[VolumeTag]
    """Tags the API key can read."""

    tag_count: int = Field(ge=0)
    """Every tag on the volume, which exceeds ``len(tags)`` when some are unreadable."""

    versions_alive: int = Field(ge=0)
    versions_tombstoned: int = Field(ge=0)
    versions_untagged: int = Field(ge=0)

    @property
    def name(self) -> str:
        assert self.ref.volume is not None, "built from a volume ref"
        return self.ref.volume


class VolumeVersion(BaseModel):
    """One committed version of a volume, as version history lists it."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["version"] = "version"

    version_ref: VolumeRef
    """The volume pinned to this version; quote it to read the same tree again."""

    digest: str

    sequence: int | None
    """Revision the version was committed at; ``None`` for versions older than the record."""

    lifecycle: str
    """For example ``ALIVE`` or ``TOMBSTONED``; a tombstoned version stays restorable until ``delete_after``."""

    is_head: bool

    tags: list[str]
    """Tags pointing at this version that the API key can read."""

    total_size: int | None = Field(ge=0)
    """Bytes across the version's files; ``None`` when unrecorded."""

    created_at: dt.datetime
    tombstoned_at: dt.datetime | None
    delete_after: dt.datetime | None


class VolumeVersionDetail(VolumeVersion):
    """One version of a volume, described on its own."""

    entry_count: int | None = Field(ge=0)
    """Files in the version; ``None`` when unrecorded."""


class NamespaceListing(BaseModel):
    """Every volume namespace the API key can read."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["namespaces"] = "namespaces"
    items: list[VolumeNamespace]


class VolumeListing(BaseModel):
    """Every volume in one namespace."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["volumes"] = "volumes"
    namespace: str
    items: list[Volume]


class VolumeEntryListing(BaseModel):
    """Entries beneath one position in a version's tree."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["entries"] = "entries"

    version_ref: VolumeRef
    """The volume pinned to the version that was listed."""

    items: list[VolumeEntry]
    """In path order. A directory no record describes, implied by the paths beneath it,
    is listed with ``mode`` and ``mtime`` of ``None``."""


class VolumeVersionListing(BaseModel):
    """A volume's version history, newest first."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["versions"] = "versions"
    volume_ref: VolumeRef
    items: list[VolumeVersion]


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

    chunks_fetched: int = Field(default=0, ge=0)
    """Chunk objects read from the origin bucket; empty files and hardlinked copies cost none."""

    duration_sec: float = Field(ge=0)
