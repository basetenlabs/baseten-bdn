from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class AttachmentState(StrEnum):
    """Whether the node bound the volume view below ``/bdn/mounts``.

    The daemon journals an attachment as ``FAILED`` until the bind succeeds and
    keeps that record if the request is interrupted, so a ``FAILED``
    attachment may be an abandoned attach that still holds its target name.
    Detach it before attaching to that target again.
    """

    READY = "READY"
    FAILED = "FAILED"


class VolumeAttachment(BaseModel):
    """One attached volume view, as the node reports it.

    Mirrors the daemon's resource. Unknown fields are ignored because the
    daemon and this package ship in different images; unknown ``state`` values
    are still rejected, since the caller cannot act on them.
    """

    model_config = ConfigDict(frozen=True)

    id: str
    """Attachment id, ``vol_<ulid>``. Scoped to this pod."""

    revision: int = Field(ge=0)
    """Node-local change counter; increases on every attachment change."""

    source: str
    """The volume ref as requested, e.g. ``bdn:adapters/sql-lora``."""

    pinned_source: str
    """The resolved, immutable ref the view serves, always carrying a full digest."""

    target: str
    """Directory name below ``/bdn/mounts``."""

    path: str
    """Container path of the attached view, ``/bdn/mounts/<target>``."""

    state: AttachmentState

    include: tuple[str, ...] = ()
    """Glob filters the view was narrowed to; empty means the whole volume."""

    exclude: tuple[str, ...] = ()


class ErrorBody(BaseModel):
    """The error body the daemon returns on a non-2xx status.

    Unknown fields are ignored so an additive daemon field can never turn a
    retryable error into a protocol error.
    """

    code: str
    message: str
    retryable: bool


class HotLoadError(Exception):
    """Base class for every error raised by the Hot Load client."""


class HotLoadConnectionError(HotLoadError):
    """The pod's Hot Load socket could not complete the request.

    Raised when the socket is missing (the pod is not opted in to Hot Load),
    refuses connections, or drops mid-request. The message says whether the
    daemon may have seen the request.
    """


class HotLoadTimeoutError(HotLoadError):
    """The daemon did not answer within the request timeout.

    For an attach this does not mean the attach failed: the daemon may have
    finished it, or abandoned it and left a ``FAILED`` attachment holding the
    target name. Inspect ``list_attachments`` and detach any leftover before
    retrying.
    """


class HotLoadProtocolError(HotLoadError):
    """The daemon's response does not match the Hot Load API contract."""


class HotLoadAPIError(HotLoadError):
    """The daemon rejected a request.

    ``code`` is the stable identifier to branch on; ``message`` is for humans.
    ``retryable`` is the daemon's own judgement and drives the client's retries.
    """

    def __init__(
        self, status_code: int, code: str, message: str, retryable: bool
    ) -> None:
        super().__init__(status_code, code, message, retryable)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.retryable = retryable

    def __str__(self) -> str:
        return f"Hot Load request failed with HTTP {self.status_code} ({self.code}): {self.message}"


class HotLoadAttachError(HotLoadError):
    """The daemon answered the attach with a ``FAILED`` attachment.

    The attachment still holds its target name. Detach it by id, then attach
    again.
    """

    def __init__(self, attachment: VolumeAttachment) -> None:
        super().__init__(attachment)
        self.attachment = attachment

    def __str__(self) -> str:
        attachment = self.attachment
        return (
            f"Hot Load attachment {attachment.id} for {attachment.source} at "
            f"{attachment.path} is {attachment.state.value}; detach it before retrying"
        )
