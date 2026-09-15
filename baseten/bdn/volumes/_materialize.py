"""Writing a manifest's tree below one destination directory.

Every path is joined below ``root`` after the containment gate has approved
it. Files are created with ``O_NOFOLLOW`` so a symlink left at the path by an
earlier pull cannot redirect the write, and modes are applied after the
bytes, since a mode passed to ``open`` is masked by the umask.
"""

from __future__ import annotations

import errno
import os
import sys
import threading
from pathlib import Path

_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_WRITE_FLAGS = os.O_WRONLY | os.O_CREAT | _O_NOFOLLOW | getattr(os, "O_BINARY", 0)
_IS_WINDOWS = sys.platform == "win32"


def contained_join(root: Path, entry_path: str) -> Path:
    # Path.joinpath with an absolute argument replaces the base; the strip is
    # what keeps a pre-rule manifest path below the root.
    return root.joinpath(*entry_path.lstrip("/").split("/"))


def ensure_dir(root: Path, entry_path: str) -> Path:
    """Create ``entry_path`` below ``root`` one component at a time.

    Component-wise because ``mkdir`` follows a symlink at an intermediate
    component. A symlink found where a directory belongs is replaced.
    """
    current = root
    for component in entry_path.lstrip("/").split("/"):
        if not component:
            continue
        current = current / component
        try:
            os.mkdir(current)
        except FileExistsError:
            if current.is_symlink():
                os.unlink(current)
                os.mkdir(current)
            elif not current.is_dir():
                raise NotADirectoryError(
                    errno.ENOTDIR, f"{current} exists and is not a directory"
                )
    return current


def create_file(path: Path, size: int) -> None:
    """Create or truncate ``path`` and preallocate ``size`` bytes."""
    fd = _open_nofollow(path, _WRITE_FLAGS | os.O_TRUNC)
    try:
        if size:
            os.ftruncate(fd, size)
    finally:
        os.close(fd)


def write_at(path: Path, offset: int, data: bytes) -> None:
    """Write ``data`` at ``offset`` into an already created file."""
    fd = _open_nofollow(path, _WRITE_FLAGS)
    try:
        if _IS_WINDOWS:
            os.lseek(fd, offset, os.SEEK_SET)
            _write_all(fd, data)
        else:
            written = 0
            while written < len(data):
                written += os.pwrite(fd, data[written:], offset + written)
    finally:
        os.close(fd)


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view) :]


def _open_nofollow(path: Path, flags: int) -> int:
    try:
        return os.open(path, flags, 0o600)
    except OSError as error:
        # ELOOP: a symlink sits at the final component. Replace it with a file.
        if error.errno != errno.ELOOP:
            raise
        os.unlink(path)
        return os.open(path, flags, 0o600)


def create_symlink(path: Path, target: str) -> None:
    if _IS_WINDOWS:
        raise NotImplementedError(
            "pulling a volume with symlinks is not supported on Windows"
        )
    if path.is_symlink() or path.is_file():
        os.unlink(path)
    os.symlink(target, path)


def create_hardlink(path: Path, source: Path) -> None:
    if _IS_WINDOWS:
        raise NotImplementedError(
            "pulling a volume with hardlinks is not supported on Windows"
        )
    if path.is_symlink() or path.exists():
        os.unlink(path)
    os.link(source, path)


def apply_mode(path: Path, mode: int) -> None:
    if _IS_WINDOWS:
        return
    os.chmod(
        path, mode, follow_symlinks=False
    ) if os.chmod in os.supports_follow_symlinks else os.chmod(path, mode)


class ByteBudget:
    """Bounds bytes held in memory across worker threads.

    A caller acquires the decompressed size it is about to buffer and
    releases it after writing. Requests larger than the budget are allowed
    through alone rather than deadlocking.
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
