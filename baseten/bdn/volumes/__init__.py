"""Client-side reads of BDN volumes.

Use :class:`VolumeClient` with a Baseten API key to resolve a volume ref,
read a version's manifest, or pull a version into a local directory.
:class:`VolumeRef` parses and renders refs in the grammar every Baseten
tool shares.
"""

from baseten.bdn.volumes._client import VolumeClient, VolumeClientOptions
from baseten.bdn.volumes._models import (
    PullResult,
    VolumeAPIError,
    VolumeConnectionError,
    VolumeDestinationError,
    VolumeEntry,
    VolumeEntryKind,
    VolumeError,
    VolumeIntegrityError,
    VolumeManifest,
    VolumePathError,
    VolumeProtocolError,
    VolumeRefError,
    VolumeStorageError,
    VolumeUnsupportedError,
)
from baseten.bdn.volumes._ref import VolumeRef, VolumeRefLevel

__all__ = [
    "PullResult",
    "VolumeAPIError",
    "VolumeClient",
    "VolumeClientOptions",
    "VolumeConnectionError",
    "VolumeDestinationError",
    "VolumeEntry",
    "VolumeEntryKind",
    "VolumeError",
    "VolumeIntegrityError",
    "VolumeManifest",
    "VolumePathError",
    "VolumeProtocolError",
    "VolumeRef",
    "VolumeRefError",
    "VolumeRefLevel",
    "VolumeStorageError",
    "VolumeUnsupportedError",
]
