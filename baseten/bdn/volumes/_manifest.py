"""Manifest and chunkmap records, the containment gate, and entry selection.

A manifest is JSONL: one minified record per line, discriminated on ``_type``
and, for files, ``_kind``. It is flat; every entry carries its full path and
records may arrive in any order. A chunkmap is a second JSONL object that a
large file's record points at.
"""

from __future__ import annotations

import datetime as dt
import posixpath
import re
from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    model_validator,
)

from baseten.bdn.volumes._cannery import DIGEST_PATTERN, ObjectTarget
from baseten.bdn.volumes._models import (
    VolumeEntry,
    VolumeEntryKind,
    VolumePathError,
    VolumeProtocolError,
    VolumeUnsupportedError,
)

MAX_SYMLINK_HOPS = 40
# RFC 3339 with up to nanosecond precision, as the manifest records it.
_RFC3339 = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$"
)


class _Record(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)


class ManifestHeader(_Record):
    type: Literal["manifest_header"] = Field(alias="_type")
    entry_count: int = Field(ge=0)
    manifest_schema: Literal["v1"]
    total_size: int = Field(ge=0)


class Provenance(_Record):
    type: Literal["provenance", "prefix_provenance", "path_provenance"] = Field(
        alias="_type"
    )


class ChunkEntry(_Record):
    digest: str = Field(pattern=DIGEST_PATTERN)
    length: int = Field(ge=0)
    offset: int = Field(ge=0)
    target: ObjectTarget


class _PathEntry(_Record):
    path: str
    mode: str = Field(pattern=r"^[0-7]{3,7}$")
    mtime: str | None = None

    @model_validator(mode="after")
    def _mtime_parses(self) -> _PathEntry:
        if self.mtime is not None and not _RFC3339.fullmatch(self.mtime):
            raise ValueError(f"mtime {self.mtime!r} is not an RFC 3339 timestamp")
        return self

    @property
    def mode_bits(self) -> int:
        return int(self.mode, 8)

    @property
    def clean_path(self) -> str:
        # Pre-rule manifests carry a leading slash; every join strips it.
        return self.path.lstrip("/")

    @property
    def mtime_ns(self) -> int | None:
        """Recorded modification time as nanoseconds since the epoch."""
        if self.mtime is None:
            return None
        match = _RFC3339.fullmatch(self.mtime)
        assert match is not None, "validated on construction"
        base, fraction, zone = match.groups()
        seconds = dt.datetime.fromisoformat(base + ("+00:00" if zone == "Z" else zone))
        return int(seconds.timestamp()) * 1_000_000_000 + int(
            (fraction or "").ljust(9, "0")
        )

    @property
    def mtime_datetime(self) -> dt.datetime | None:
        ns = self.mtime_ns
        if ns is None:
            return None
        # Integer arithmetic: a float of nanoseconds since the epoch rounds the microseconds.
        seconds, remainder = divmod(ns, 1_000_000_000)
        return dt.datetime.fromtimestamp(seconds, tz=dt.UTC) + dt.timedelta(
            microseconds=remainder // 1000
        )


class DirectoryEntry(_PathEntry):
    type: Literal["directory"] = Field(alias="_type")


class SymlinkEntry(_PathEntry):
    type: Literal["symlink"] = Field(alias="_type")
    target: str


class ChunkFileEntry(_PathEntry):
    """A file stored as one chunk; an empty file has a zero-length chunk."""

    type: Literal["file"] = Field(alias="_type")
    kind: Literal["chunk"] = Field(alias="_kind")
    chunk: ChunkEntry
    link_group: int | None = None

    @model_validator(mode="after")
    def _chunk_starts_at_zero(self) -> ChunkFileEntry:
        if self.chunk.offset != 0:
            raise ValueError(
                f"single-chunk file {self.path!r} has chunk offset {self.chunk.offset}"
            )
        return self

    @property
    def size(self) -> int:
        return self.chunk.length


class ChunkmapFileEntry(_PathEntry):
    """A file whose chunks are listed in a separate chunkmap object."""

    type: Literal["file"] = Field(alias="_type")
    kind: Literal["chunkmap"] = Field(alias="_kind")
    digest: str = Field(pattern=DIGEST_PATTERN)
    size: int = Field(ge=0)
    target: ObjectTarget
    link_group: int | None = None


class SlabmapFileEntry(_PathEntry):
    """Recognized so the manifest parses; rejected because nothing reads slabmaps yet."""

    type: Literal["file"] = Field(alias="_type")
    kind: Literal["slabmap"] = Field(alias="_kind")


FileEntry = ChunkFileEntry | ChunkmapFileEntry
PathEntry = DirectoryEntry | SymlinkEntry | ChunkFileEntry | ChunkmapFileEntry
_ManifestRecord = Annotated[
    ManifestHeader
    | Provenance
    | DirectoryEntry
    | SymlinkEntry
    | Annotated[
        ChunkFileEntry | ChunkmapFileEntry | SlabmapFileEntry,
        Field(discriminator="kind"),
    ],
    Field(discriminator="type"),
]
_MANIFEST_RECORD = TypeAdapter(_ManifestRecord)


class ChunkmapHeader(_Record):
    type: Literal["chunkmap_header"] = Field(alias="_type")
    chunk_count: int = Field(ge=0)
    file_size: int = Field(ge=0)


class ChunkmapChunk(ChunkEntry):
    type: Literal["chunk"] = Field(alias="_type")


_ChunkmapRecord = Annotated[ChunkmapHeader | ChunkmapChunk, Field(discriminator="type")]
_CHUNKMAP_RECORD = TypeAdapter(_ChunkmapRecord)


class Manifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    header: ManifestHeader
    entries: tuple[PathEntry, ...]

    def public_entries(self, paths: Iterable[str] | None = None) -> list[VolumeEntry]:
        """Entries as callers see them, slash-prefixed and in path order, optionally only ``paths``."""
        wanted = None if paths is None else set(paths)
        selected = [e for e in self.entries if wanted is None or e.clean_path in wanted]
        selected.sort(key=lambda entry: entry.clean_path.split("/"))
        return [_public_entry(entry) for entry in selected]

    def files(self) -> list[FileEntry]:
        return [
            e
            for e in self.entries
            if isinstance(e, (ChunkFileEntry, ChunkmapFileEntry))
        ]


def _public_entry(entry: PathEntry) -> VolumeEntry:
    if isinstance(entry, DirectoryEntry):
        kind, size, target = VolumeEntryKind.DIRECTORY, 0, None
    elif isinstance(entry, SymlinkEntry):
        kind, size, target = VolumeEntryKind.SYMLINK, 0, entry.target
    else:
        kind, size, target = VolumeEntryKind.FILE, entry.size, None
    return VolumeEntry(
        path="/" + entry.clean_path,
        kind=kind,
        size=size,
        mode=entry.mode_bits,
        mtime=entry.mtime_datetime,
        link_target=target,
    )


def _lines(body: bytes, what: str) -> list[bytes]:
    lines = [line for line in body.split(b"\n") if line]
    if not lines:
        raise VolumeProtocolError(f"{what} is empty")
    return lines


def parse_manifest(body: bytes) -> Manifest:
    header: ManifestHeader | None = None
    entries: list[PathEntry] = []
    for number, line in enumerate(_lines(body, "manifest"), start=1):
        try:
            record = _MANIFEST_RECORD.validate_json(line)
        except ValidationError as error:
            raise VolumeProtocolError(
                f"manifest line {number} is off contract: {error}"
            ) from error
        if isinstance(record, ManifestHeader):
            if header is not None:
                raise VolumeProtocolError("manifest carries two headers")
            header = record
        elif isinstance(record, Provenance):
            continue
        elif isinstance(record, SlabmapFileEntry):
            raise VolumeUnsupportedError(
                f"slabmap files are not supported yet: {record.path!r}"
            )
        else:
            entries.append(record)
    if header is None:
        raise VolumeProtocolError("manifest has no manifest_header record")
    return Manifest(header=header, entries=tuple(entries))


def parse_chunkmap(body: bytes, expected_size: int) -> list[ChunkmapChunk]:
    header: ChunkmapHeader | None = None
    chunks: list[ChunkmapChunk] = []
    for number, line in enumerate(_lines(body, "chunkmap"), start=1):
        try:
            record = _CHUNKMAP_RECORD.validate_json(line)
        except ValidationError as error:
            raise VolumeProtocolError(
                f"chunkmap line {number} is off contract: {error}"
            ) from error
        if isinstance(record, ChunkmapHeader):
            if header is not None:
                raise VolumeProtocolError("chunkmap carries two headers")
            header = record
        else:
            chunks.append(record)
    if header is None:
        raise VolumeProtocolError("chunkmap has no chunkmap_header record")
    if header.chunk_count != len(chunks):
        raise VolumeProtocolError(
            f"chunkmap header counts {header.chunk_count} chunks, found {len(chunks)}"
        )
    if header.file_size != expected_size:
        raise VolumeProtocolError(
            f"chunkmap file_size {header.file_size} does not match the file record's {expected_size}"
        )
    # Contiguous from zero: a gap or overlap would leave the preallocated
    # file with zeros or torn bytes that no digest check would catch.
    expected_offset = 0
    for chunk in chunks:
        if chunk.offset != expected_offset:
            raise VolumeProtocolError(
                f"chunkmap chunk at offset {chunk.offset} is not contiguous (expected {expected_offset})"
            )
        expected_offset += chunk.length
    if expected_offset != expected_size:
        raise VolumeProtocolError(
            f"chunkmap chunks sum to {expected_offset} bytes, file is {expected_size}"
        )
    return chunks


def _normalize(path: str) -> str:
    """Collapse ``.`` and ``//`` without following ``..`` above the root."""
    parts: list[str] = []
    for part in path.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if not parts:
                raise VolumePathError(f"path escapes the volume root: {path!r}")
            parts.pop()
        else:
            parts.append(part)
    return "/".join(parts)


class ContainedPaths:
    """Every entry path and symlink target, checked to stay inside the tree.

    Runs before anything is written, so a hostile or corrupt manifest cannot
    place a single byte outside ``dest_dir``. Symlink targets are walked one
    component at a time with symlinks followed at every component, so a
    ``..`` after a link that itself points upward is counted against the real
    location, not the spelled one. Also renders each symlink's target
    relative to the link, the only encoding that survives relocating the tree.
    """

    def __init__(self, entries: tuple[PathEntry, ...]) -> None:
        self.by_path: dict[str, PathEntry] = {}
        for entry in entries:
            path = entry.clean_path
            if not path or "\x00" in path:
                raise VolumePathError(
                    f"entry has an empty path or a NUL byte: {entry.path!r}"
                )
            if _normalize(path) != path:
                raise VolumePathError(f"entry path is not normalized: {entry.path!r}")
            if path in self.by_path:
                raise VolumePathError(f"entry path appears twice: {path!r}")
            self.by_path[path] = entry
        self._rendered: dict[str, str] = {}
        for path, entry in self.by_path.items():
            self._check_ancestors(path)
            if isinstance(entry, SymlinkEntry):
                self._real_target(path, entry.target, hops=0)
                self._rendered[path] = self._render(path, entry.target)

    def rendered_symlink_target(self, entry: SymlinkEntry) -> str:
        """The target relative to the link's directory."""
        return self._rendered[entry.clean_path]

    def _check_ancestors(self, path: str) -> None:
        parent = posixpath.dirname(path)
        while parent:
            ancestor = self.by_path.get(parent)
            if ancestor is not None and not isinstance(ancestor, DirectoryEntry):
                raise VolumePathError(
                    f"{path!r} is nested beneath the non-directory {parent!r}"
                )
            parent = posixpath.dirname(parent)

    def _real_target(self, link_path: str, target: str, *, hops: int) -> list[str]:
        """Resolve ``target`` from ``link_path`` to real components inside the tree."""
        if not target or "\x00" in target:
            raise VolumePathError(
                f"symlink {link_path!r} has an empty target or a NUL byte"
            )
        if hops > MAX_SYMLINK_HOPS:
            raise VolumePathError(
                f"symlink {link_path!r} forms a chain longer than {MAX_SYMLINK_HOPS} hops"
            )
        real = (
            []
            if target.startswith("/")
            else [p for p in posixpath.dirname(link_path).split("/") if p]
        )
        for part in target.split("/"):
            if part in ("", "."):
                continue
            if part == "..":
                if not real:
                    raise VolumePathError(
                        f"symlink {link_path!r} -> {target!r} escapes the volume root"
                    )
                real.pop()
                continue
            real.append(part)
            here = "/".join(real)
            entry = self.by_path.get(here)
            # A symlink at any component redirects the rest of the walk, so
            # the depth that later `..` components pop from is the real one.
            if isinstance(entry, SymlinkEntry):
                real = self._real_target(here, entry.target, hops=hops + 1)
        return real

    def _render(self, link_path: str, target: str) -> str:
        if not target.startswith("/"):
            return target
        resolved = _normalize(target)
        link_dir = posixpath.dirname(link_path)
        return posixpath.relpath(resolved or ".", start=link_dir or ".")


def hardlink_groups(entries: Iterable[PathEntry]) -> dict[int, list[str]]:
    """Paths per ``link_group``, in manifest order; the first one is materialized."""
    groups: dict[int, list[str]] = defaultdict(list)
    for entry in entries:
        if (
            isinstance(entry, (ChunkFileEntry, ChunkmapFileEntry))
            and entry.link_group is not None
        ):
            groups[entry.link_group].append(entry.clean_path)
    return {group: paths for group, paths in groups.items() if len(paths) > 1}


def implicit_directories(paths: Iterable[str]) -> list[str]:
    """Every ancestor directory the given entry paths need, shallowest first."""
    seen: set[str] = set()
    for path in paths:
        parent = posixpath.dirname(path)
        while parent and parent not in seen:
            seen.add(parent)
            parent = posixpath.dirname(parent)
    return sorted(seen, key=lambda path: path.count("/"))


def select_paths(
    entries: tuple[PathEntry, ...], include: Sequence[str]
) -> set[str] | None:
    """Entry paths named by ``include``, or ``None`` when nothing narrows.

    Each include is an exact path or a directory whose contents are wanted,
    matched on slash boundaries and relative to the volume root. Recorded
    ancestors of a selected entry are selected too, so their modes apply. An
    include that matches nothing is an error rather than a smaller pull.
    """
    prefixes = {item.strip("/") for item in include}
    prefixes.discard("")
    if not prefixes:
        return None
    by_path = {entry.clean_path: entry for entry in entries}
    selected: set[str] = set()
    for prefix in prefixes:
        matched = {
            path for path in by_path if path == prefix or path.startswith(prefix + "/")
        }
        if not matched:
            raise VolumePathError(f"include {prefix!r} matches no entry in the volume")
        selected |= matched
    for path in list(selected):
        parent = posixpath.dirname(path)
        while parent:
            if parent in by_path:
                selected.add(parent)
            parent = posixpath.dirname(parent)
    return selected
