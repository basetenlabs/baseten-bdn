"""Writing a manifest's tree below one destination directory (POSIX only).

Every path is joined below ``root`` after the containment gate has approved
it. Anything already at a file's path is unlinked before the file is created,
so a stale symlink cannot redirect the write and a stale hardlink cannot
share the new bytes with another path. Modes are applied after the bytes,
since a mode passed to ``open`` is masked by the umask.
"""

from __future__ import annotations

import os
import stat
import sys
import threading
from pathlib import Path

from baseten.bdn.volumes._models import VolumeDestinationError, VolumeUnsupportedError

_CREATE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
_WRITE_FLAGS = os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0)


def contained_join(root: Path, entry_path: str) -> Path:
    # Path.joinpath with an absolute argument replaces the base; the strip is
    # what keeps a pre-rule manifest path below the root.
    return root.joinpath(*entry_path.lstrip("/").split("/"))


def ensure_dir(root: Path, entry_path: str) -> Path:
    """Create ``entry_path`` below ``root`` one component at a time.

    Component-wise because ``mkdir`` follows a symlink at an intermediate
    component. A symlink found where a directory belongs is replaced, and an
    existing directory left read-only by an earlier pull is made writable
    again so children can be created; final modes are applied afterwards.
    """
    current = root
    for component in entry_path.lstrip("/").split("/"):
        if not component:
            continue
        current = current / component
        try:
            os.mkdir(current)
        except FileExistsError:
            info = os.lstat(current)
            if stat.S_ISLNK(info.st_mode):
                os.unlink(current)
                os.mkdir(current)
            elif not stat.S_ISDIR(info.st_mode):
                raise VolumeDestinationError(
                    f"{current} exists and is not a directory"
                ) from None
            elif info.st_mode & 0o300 != 0o300:
                os.chmod(current, stat.S_IMODE(info.st_mode) | 0o300)
    return current


def remove_if_present(path: Path) -> None:
    """Unlink a file or symlink at ``path``; a directory there is an error."""
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return
    if stat.S_ISDIR(info.st_mode):
        raise VolumeDestinationError(
            f"{path} is a directory but the volume has a file there"
        )
    os.unlink(path)


def create_file(path: Path, size: int) -> None:
    """Create ``path`` fresh and preallocate ``size`` bytes."""
    remove_if_present(path)
    fd = os.open(path, _CREATE_FLAGS, 0o600)
    try:
        if size:
            os.ftruncate(fd, size)
    finally:
        os.close(fd)


def write_at(path: Path, offset: int, data: bytes) -> None:
    """Write ``data`` at ``offset`` into an already created file."""
    if sys.platform == "win32":
        raise VolumeUnsupportedError(
            "pulling volumes is supported on Linux and macOS only"
        )
    fd = os.open(path, _WRITE_FLAGS)
    try:
        view = memoryview(data)
        while view:
            written = os.pwrite(fd, view, offset)
            view = view[written:]
            offset += written
    finally:
        os.close(fd)


def create_symlink(path: Path, target: str) -> None:
    remove_if_present(path)
    os.symlink(target, path)


def create_hardlink(path: Path, source: Path) -> None:
    remove_if_present(path)
    os.link(source, path)


def apply_mode(path: Path, mode: int) -> None:
    os.chmod(path, mode)


class ByteBudget:
    """Bounds bytes held in memory across worker threads.

    A caller acquires what it is about to buffer and releases it after
    writing. Requests larger than the budget are allowed through alone rather
    than deadlocking.
    """

    def __init__(self, limit: int) -> None:
        if limit <= 0:
            raise ValueError("byte budget must be positive")
        self._limit = limit
        self._in_flight = 0
        self._condition = threading.Condition()

    def acquire(self, size: int) -> None:
        with self._condition:
            while self._in_flight and self._in_flight + size > self._limit:
                self._condition.wait()
            self._in_flight += size

    def release(self, size: int) -> None:
        with self._condition:
            self._in_flight -= size
            self._condition.notify_all()
