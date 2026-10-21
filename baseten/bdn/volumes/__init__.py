"""Client-side reads of BDN volumes.

Use :class:`VolumeClient` with a Baseten API key to list namespaces, volumes,
and a version's entries, describe a volume or version, read version history,
read a version's manifest, or pull a version into a local directory. Every
operation is read-only.
:class:`VolumeRef` parses and renders refs in the grammar every Baseten
tool shares.
"""

from baseten.bdn.volumes._client import VolumeClient, VolumeClientOptions
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
from baseten.bdn.volumes._models import (
    NamespaceListing,
    PullResult,
    Volume,
    VolumeEntry,
    VolumeEntryKind,
    VolumeEntryListing,
    VolumeHead,
    VolumeListing,
    VolumeManifest,
    VolumeNamespace,
    VolumeTag,
    VolumeVersion,
    VolumeVersionDetail,
    VolumeVersionListing,
)
from baseten.bdn.volumes._ref import VolumeRef, VolumeRefLevel

__all__ = [
    "NamespaceListing",
    "PullResult",
    "Volume",
    "VolumeAPIError",
    "VolumeClient",
    "VolumeClientOptions",
    "VolumeConnectionError",
    "VolumeDestinationError",
    "VolumeEntry",
    "VolumeEntryKind",
    "VolumeEntryListing",
    "VolumeError",
    "VolumeHead",
    "VolumeIntegrityError",
    "VolumeListing",
    "VolumeManifest",
    "VolumeNamespace",
    "VolumePathError",
    "VolumeProtocolError",
    "VolumeRef",
    "VolumeRefError",
    "VolumeRefLevel",
    "VolumeStorageError",
    "VolumeTag",
    "VolumeUnsupportedError",
    "VolumeVersion",
    "VolumeVersionDetail",
    "VolumeVersionListing",
]
