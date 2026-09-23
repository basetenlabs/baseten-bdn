"""A forward-only binary stream over one file in a pinned volume version."""

from __future__ import annotations

import io
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor

from baseten.bdn.volumes._manifest import ChunkEntry
from baseten.bdn.volumes._models import VolumeEntry
from baseten.bdn.volumes._ref import VolumeRef
from baseten.bdn.volumes._version import Version


class VolumeFileReader(io.BufferedIOBase):
    """Reads one file of a volume version front to back, fetching chunks ahead of the reader.

    Returned by :meth:`VolumeClient.open`. Every chunk is verified against its
    recorded digest and length before any of its bytes are returned, so a
    corrupt chunk raises from the ``read`` that would first have returned
    them. Chunks are fetched on a thread pool up to ``max_concurrency`` at a
    time. Beyond the chunk being read, those in flight or fetched are bounded
    by ``max_bytes_in_flight``, charged twice per chunk for its stored and
    decoded copies; a chunk larger than that is fetched alone.

    ``read``, ``read1``, ``readinto``, ``readline``, ``peek``, and iteration
    work; the stream is not seekable. It is not safe to share between threads.
    Closing it cancels fetches that have not started and lets running ones
    finish in the background; it never closes the :class:`VolumeClient`.
    """

    def __init__(
        self,
        version: Version,
        entry: VolumeEntry,
        chunks: list[ChunkEntry],
        *,
        max_concurrency: int,
        max_bytes_in_flight: int,
    ) -> None:
        super().__init__()
        self._version_ref = version.ref
        self._entry = entry
        self._version = version
        self._unrequested = deque(chunks)
        self._requested: deque[tuple[ChunkEntry, Future[bytes]]] = deque()
        self._requested_bytes = 0
        self._max_bytes_in_flight = max_bytes_in_flight
        self._pool = ThreadPoolExecutor(
            max_workers=max_concurrency, thread_name_prefix="bdn-volume-read"
        )
        self._current = memoryview(b"")
        self._position = 0
        self._request_ahead()

    @property
    def version_ref(self) -> VolumeRef:
        """The volume pinned to the version being read."""
        return self._version_ref

    @property
    def entry(self) -> VolumeEntry:
        """The file being read; its ``path`` is where any symlinks on the ref led."""
        return self._entry

    def readable(self) -> bool:
        self._check_open()
        return True

    def tell(self) -> int:
        self._check_open()
        return self._position

    def read(self, size: int | None = -1) -> bytes:
        self._check_open()
        wanted = self._remaining() if size is None or size < 0 else size
        out = bytearray()
        while len(out) < wanted and (self._current or self._next_chunk()):
            taken = self._current[: wanted - len(out)]
            out += taken
            self._current = self._current[len(taken) :]
        self._position += len(out)
        return bytes(out)

    def read1(self, size: int = -1) -> bytes:
        """At most ``size`` bytes, fetching no more than the next chunk."""
        self._check_open()
        if not self._current:
            self._next_chunk()
        taken = self._current if size < 0 else self._current[:size]
        self._current = self._current[len(taken) :]
        self._position += len(taken)
        return bytes(taken)

    def peek(self, size: int = 0) -> bytes:
        """Buffered bytes without consuming them; lets ``readline`` avoid reading byte by byte."""
        self._check_open()
        if not self._current:
            self._next_chunk()
        return bytes(self._current)

    def close(self) -> None:
        if not self.closed:
            for _, future in self._requested:
                future.cancel()
            self._pool.shutdown(wait=False, cancel_futures=True)
            self._requested.clear()
            self._unrequested.clear()
            self._current = memoryview(b"")
        super().close()

    def _check_open(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on a closed volume file")

    def _remaining(self) -> int:
        return (self._entry.size or 0) - self._position

    def _request_ahead(self) -> None:
        while self._unrequested:
            charge = 2 * self._unrequested[0].length
            if (
                self._requested
                and self._requested_bytes + charge > self._max_bytes_in_flight
            ):
                return
            chunk = self._unrequested.popleft()
            self._requested.append(
                (chunk, self._pool.submit(self._version.read_chunk, chunk))
            )
            self._requested_bytes += charge

    def _next_chunk(self) -> bool:
        """Make the next verified chunk current; ``False`` at the end of the file."""
        if not self._requested:
            return False
        chunk, future = self._requested.popleft()
        self._requested_bytes -= 2 * chunk.length
        try:
            data = future.result()
        except BaseException:
            self.close()
            raise
        self._request_ahead()
        self._current = memoryview(data)
        return True
