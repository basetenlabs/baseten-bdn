from __future__ import annotations


class VolumeError(Exception):
    """Base class for every error raised by the volumes client."""


class VolumeRefError(VolumeError, ValueError):
    """A volume ref does not parse or names the wrong level for the operation."""


class VolumeUnsupportedError(VolumeError, NotImplementedError):
    """The volume or platform uses something this client does not support yet."""


class VolumeConnectionError(VolumeError):
    """A request to the Baseten API, cannery, or the origin bucket could not complete."""


class VolumeAPIError(VolumeError):
    """The Baseten API or cannery rejected a request.

    ``reason`` is cannery's stable identifier (``NOT_FOUND``,
    ``PERMISSION_DENIED``, ``AMBIGUOUS_PREFIX``, ...) when the failure came
    from cannery; ``code`` is the Baseten API's classification.
    """

    def __init__(
        self,
        service: str,
        status_code: int,
        message: str,
        *,
        code: str | None = None,
        reason: str | None = None,
    ) -> None:
        super().__init__(service, status_code, message, code, reason)
        self.service = service
        self.status_code = status_code
        self.message = message
        self.code = code
        self.reason = reason

    def __str__(self) -> str:
        label = self.reason or self.code or "error"
        return f"{self.service} request failed with HTTP {self.status_code} ({label}): {self.message}"


class VolumeStorageError(VolumeError):
    """The origin bucket refused an object read.

    ``code`` is the S3 error code (``AccessDenied``, ``NoSuchKey``, ...) when
    the response carried one.
    """

    def __init__(
        self, status_code: int, key: str, message: str, *, code: str | None = None
    ) -> None:
        super().__init__(status_code, key, message, code)
        self.status_code = status_code
        self.key = key
        self.message = message
        self.code = code

    def __str__(self) -> str:
        return f"origin bucket answered HTTP {self.status_code} ({self.code or 'error'}) for {self.key}: {self.message}"


class VolumeProtocolError(VolumeError):
    """A response does not match the contract: bad JSON, unknown content type, bad manifest."""


class VolumeIntegrityError(VolumeError):
    """Downloaded bytes do not match the recorded digest or length."""


class VolumePathError(VolumeError):
    """A manifest entry would land outside the destination, or a selected path names nothing."""


class VolumeDestinationError(VolumeError):
    """The destination directory cannot take the volume: not empty, not a directory, or no space."""
