"""Manifest and chunkmap records, and the containment gate over their paths.

A manifest is JSONL: one minified record per line, discriminated on ``_type``
and, for files, ``_kind``. It is flat; every entry carries its full path and
records may arrive in any order. A chunkmap is a second JSONL object that a
large file's record points at.
"""

from __future__ import annotations

import json
import posixpath
from collections import defaultdict
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

from baseten.bdn.volumes._cannery import DIGEST_PATTERN, ObjectTarget
from baseten.bdn.volumes._models import (
    EntryKind,
    FileInfo,
    VolumePathError,
    VolumeProtocolError,
)

MAX_SYMLINK_HOPS = 40


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

    @property
    def mode_bits(self) -> int:
        return int(self.mode, 8)

    @property
    def clean_path(self) -> str:
        # Pre-rule manifests carry a leading slash; every join strips it.
        return self.path.lstrip("/")


class DirectoryEntry(_PathEntry):
    type: Literal["directory"] = Field(alias="_type")


class SymlinkEntry(_PathEntry):
    type: Literal["symlink"] = Field(alias="_type")
    target: str


class ChunkFileEntry(_PathEntry):
    """A file of at most one chunk; an empty file has none."""

    type: Literal["file"] = Field(alias="_type")
    kind: Literal["chunk"] = Field(alias="_kind")
    chunk: ChunkEntry | None = None
    link_group: int | None = None

    @property
    def size(self) -> int:
        return self.chunk.length if self.chunk else 0


class ChunkmapFileEntry(_PathEntry):
    """A file whose chunks are listed in a separate chunkmap object."""

    type: Literal["file"] = Field(alias="_type")
    kind: Literal["chunkmap"] = Field(alias="_kind")
    digest: str = Field(pattern=DIGEST_PATTERN)
    size: int = Field(ge=0)
    target: ObjectTarget
    link_group: int | None = None


class SlabmapFileEntry(_PathEntry):
    type: Literal["file"] = Field(alias="_type")
    kind: Literal["slabmap"] = Field(alias="_kind")
    link_group: int | None = None


FileEntry = Annotated[
    ChunkFileEntry | ChunkmapFileEntry | SlabmapFileEntry, Field(discriminator="kind")
]
PathEntry = (
    DirectoryEntry
    | SymlinkEntry
    | ChunkFileEntry
    | ChunkmapFileEntry
    | SlabmapFileEntry
)
ManifestRecord = Annotated[
    ManifestHeader | Provenance | DirectoryEntry | SymlinkEntry | FileEntry,
    Field(discriminator="type"),
]
_MANIFEST_RECORD = TypeAdapter(ManifestRecord)


class ChunkmapHeader(_Record):
    type: Literal["chunkmap_header"] = Field(alias="_type")
    chunk_count: int = Field(ge=0)
    file_size: int = Field(ge=0)


class ChunkmapChunk(ChunkEntry):
    type: Literal["chunk"] = Field(alias="_type")


ChunkmapRecord = Annotated[ChunkmapHeader | ChunkmapChunk, Field(discriminator="type")]
_CHUNKMAP_RECORD = TypeAdapter(ChunkmapRecord)


class Manifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    header: ManifestHeader
    entries: tuple[PathEntry, ...]

    def file_infos(self) -> list[FileInfo]:
        infos: list[FileInfo] = []
        for entry in self.entries:
            if isinstance(entry, DirectoryEntry):
                infos.append(
                    FileInfo(
                        path=entry.clean_path,
                        kind=EntryKind.DIRECTORY,
                        size=0,
                        mode=entry.mode_bits,
                    )
                )
            elif isinstance(entry, SymlinkEntry):
                infos.append(
                    FileInfo(
                        path=entry.clean_path,
                        kind=EntryKind.SYMLINK,
                        size=0,
                        mode=entry.mode_bits,
                        link_target=entry.target,
                    )
                )
            elif isinstance(entry, SlabmapFileEntry):
                raise NotImplementedError(
                    f"slabmap files are not supported: {entry.path!r}"
                )
            else:
                infos.append(
                    FileInfo(
                        path=entry.clean_path,
                        kind=EntryKind.FILE,
                        size=entry.size,
                        mode=entry.mode_bits,
                    )
                )
        return infos


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
        else:
            entries.append(record)
    if header is None:
        raise VolumeProtocolError("manifest has no manifest_header record")
    if header.entry_count != len(entries):
        raise VolumeProtocolError(
            f"manifest header counts {header.entry_count} entries, found {len(entries)}"
        )
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
    place a single byte outside ``dest_dir``. Also renders each symlink's
    target relative to the link, the only encoding that survives relocating
    the tree.
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
        for path, entry in self.by_path.items():
            self._check_ancestors(path)
            if isinstance(entry, SymlinkEntry):
                self._resolve_symlink(path, entry)

    def _check_ancestors(self, path: str) -> None:
        parent = posixpath.dirname(path)
        while parent:
            ancestor = self.by_path.get(parent)
            if ancestor is not None and not isinstance(ancestor, DirectoryEntry):
                raise VolumePathError(
                    f"{path!r} is nested beneath the non-directory {parent!r}"
                )
            parent = posixpath.dirname(parent)

    def _target_path(self, link_path: str, target: str) -> str:
        if "\x00" in target or not target:
            raise VolumePathError(
                f"symlink {link_path!r} has an empty target or a NUL byte"
            )
        base = "" if target.startswith("/") else posixpath.dirname(link_path)
        try:
            return _normalize(posixpath.join(base, target))
        except VolumePathError:
            raise VolumePathError(
                f"symlink {link_path!r} -> {target!r} escapes the volume root"
            ) from None

    def _resolve_symlink(self, link_path: str, entry: SymlinkEntry) -> None:
        current = link_path
        target = entry.target
        for _ in range(MAX_SYMLINK_HOPS):
            resolved = self._target_path(current, target)
            # Every ancestor of the target that is itself a symlink redirects
            # the walk; follow it so a chain through directories is checked too.
            hop = self.by_path.get(resolved)
            if isinstance(hop, SymlinkEntry):
                current, target = resolved, hop.target
                continue
            return
        raise VolumePathError(
            f"symlink {link_path!r} forms a chain longer than {MAX_SYMLINK_HOPS} hops"
        )

    def rendered_symlink_target(self, entry: SymlinkEntry) -> str:
        """The target relative to the link's directory."""
        link_path = entry.clean_path
        if not entry.target.startswith("/"):
            return entry.target
        resolved = self._target_path(link_path, entry.target)
        link_dir = posixpath.dirname(link_path)
        return posixpath.relpath(resolved, start=link_dir) if link_dir else resolved


def hardlink_groups(entries: tuple[PathEntry, ...]) -> dict[int, list[str]]:
    """Paths per ``link_group``, in manifest order; the first one is materialized."""
    groups: dict[int, list[str]] = defaultdict(list)
    for entry in entries:
        if (
            isinstance(entry, (ChunkFileEntry, ChunkmapFileEntry, SlabmapFileEntry))
            and entry.link_group is not None
        ):
            groups[entry.link_group].append(entry.clean_path)
    return {group: paths for group, paths in groups.items() if len(paths) > 1}


def dump_record(record: BaseModel) -> bytes:
    """One canonical JSONL line: minified, aliases, no nulls. Used by tests and future push."""
    payload = record.model_dump(by_alias=True, exclude_none=True, mode="json")
    return json.dumps(payload, separators=(",", ":"), sort_keys=False).encode() + b"\n"
