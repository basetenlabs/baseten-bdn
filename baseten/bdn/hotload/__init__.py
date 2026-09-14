"""Client for the pod-local BDN Hot Load API.

Use :class:`HotLoadClient` or :class:`AsyncHotLoadClient` from inside an
opted-in model pod to attach BDN volumes below ``/bdn/mounts`` at runtime.
"""

from baseten.bdn.hotload._client import (
    AsyncHotLoadClient,
    HotLoadClient,
    HotLoadClientOptions,
)
from baseten.bdn.hotload._models import (
    AttachmentState,
    HotLoadAPIError,
    HotLoadAttachError,
    HotLoadConnectionError,
    HotLoadError,
    HotLoadProtocolError,
    HotLoadTimeoutError,
    VolumeAttachment,
)

__all__ = [
    "AsyncHotLoadClient",
    "AttachmentState",
    "HotLoadAPIError",
    "HotLoadAttachError",
    "HotLoadClient",
    "HotLoadClientOptions",
    "HotLoadConnectionError",
    "HotLoadError",
    "HotLoadProtocolError",
    "HotLoadTimeoutError",
    "VolumeAttachment",
]
