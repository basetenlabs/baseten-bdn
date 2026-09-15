"""Client-side reads of BDN volumes.

Use :class:`VolumesClient` with a Baseten API key to resolve a volume ref,
list its files, or pull a version into a local directory.
"""

from baseten.bdn.volumes._client import VolumesClient, VolumesClientOptions
from baseten.bdn.volumes._models import (
    EntryKind,
    FileInfo,
    PullResult,
    ResolvedFrom,
    ResolvedVolume,
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

__all__ = [
    "EntryKind",
    "FileInfo",
    "PullResult",
    "ResolvedFrom",
    "ResolvedVolume",
    "VolumeAPIError",
    "VolumeConnectionError",
    "VolumeDestinationError",
    "VolumeError",
    "VolumeIntegrityError",
    "VolumePathError",
    "VolumeProtocolError",
    "VolumeRefError",
    "VolumeStorageError",
    "VolumeUnsupportedError",
    "VolumesClient",
    "VolumesClientOptions",
]
