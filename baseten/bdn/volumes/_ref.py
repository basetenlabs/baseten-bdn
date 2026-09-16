"""Volume refs: ``bdn:<namespace>/<volume>[:tag|@digest][/path]``.

The grammar and the canonical rendering match the Go SDK's ``VolumeRef`` and
the CLI, so a ref printed by one tool can be pasted into another. A ref is
relative to an organization, which is never in the ref and arrives with the
authenticated request.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from urllib.parse import quote, unquote

from baseten.bdn.volumes._errors import VolumeRefError

_SCHEME = "bdn:"
_OBJECT_SCHEME = "bdn+obj://"
_DIGEST_ALGORITHM = "b3:"
_NAME_MIN, _NAME_MAX = 2, 256
_TAG_MAX = 128
_DIGEST_MAX = 64
# The shortest prefix the service resolves, so a shorthand can be pasted back in.
_DIGEST_SHORTHAND = 12
# Literal path segments the service routes before it matches a name.
_RESERVED_NAMESPACES = frozenset({"namespaces", "resolve"})
_RESERVED_VOLUMES = frozenset({"manifests", "objects"})
_SEGMENT_SAFE = "-._~"


class VolumeRefLevel(StrEnum):
    """The most specific component a ref names."""

    NAMESPACE = "namespace"
    """``bdn:ns``"""

    VOLUME = "volume"
    """``bdn:ns/vol``, the volume itself; operations read its head."""

    POINT = "point"
    """``bdn:ns/vol:tag`` or ``bdn:ns/vol@digest``, one version."""

    PATH = "path"
    """``bdn:ns/vol/x/y`` or ``bdn:ns/vol:tag/x/y``, a position in that version's tree."""


@dataclass(frozen=True)
class VolumeRef:
    """A parsed volume ref.

    A ref with no selector names the volume's head, which moves. A ref with a
    tag names whatever the tag points at, which also moves. Only a ref pinned
    to a digest names one fixed version, which is why every transfer resolves
    once and then works from the pin.
    """

    namespace: str
    volume: str = ""
    tag: str = ""
    digest: str = ""
    """Lowercase hex, 1 to 64 characters, optionally prefixed ``b3:``, kept as read.
    Fewer than 64 characters is a prefix; whether it is long enough or unique is
    decided by the service. Every digest this package writes carries the prefix
    and all 64 characters."""

    path: str = ""
    """Position within the version, spelled like a URL path: empty means no path,
    ``/`` the root, otherwise slash-prefixed with no trailing slash."""

    def __post_init__(self) -> None:
        if not self.namespace:
            raise VolumeRefError("ref names no namespace")
        if not self.volume and (self.tag or self.digest or self.path):
            raise VolumeRefError(f"ref {self} names no volume to select within")
        if self.tag and self.digest:
            raise VolumeRefError(
                f"ref {self} names both a tag and a digest; one version cannot be both"
            )

    @classmethod
    def parse(cls, text: str) -> VolumeRef:
        """Parse a ref. Namespace and volume are folded to lowercase; tags are not."""
        trimmed = text.strip()
        if trimmed.startswith(_OBJECT_SCHEME):
            raise VolumeRefError(
                f"ref {text!r}: object refs name a stored object, not a volume"
            )
        if trimmed.startswith("bdn://"):
            raise VolumeRefError(
                f"ref {text!r}: bdn:// is not supported; write bdn:ns/vol"
            )
        if not trimmed.startswith(_SCHEME):
            raise VolumeRefError(f"ref {text!r}: a ref begins with {_SCHEME!r}")
        # First segment is the namespace, second the volume with its selector,
        # everything after the second slash is the path.
        segments = trimmed[len(_SCHEME) :].split("/", 2)
        if any(c in segments[0] for c in ":@"):
            raise VolumeRefError(f"ref {text!r}: a namespace ref takes no selector")
        namespace = _name(text, "namespace", segments[0], _RESERVED_NAMESPACES)
        if len(segments) == 1 or (len(segments) == 2 and segments[1] == ""):
            return cls(namespace=namespace)
        volume_segment = segments[1]
        tag = digest = ""
        # The first "@" wins over any ":", so "vol:a:b" is volume "vol" with
        # the invalid tag "a:b" rather than volume "vol:a" with tag "b".
        if (at := volume_segment.find("@")) >= 0:
            digest = _digest(text, volume_segment[at + 1 :])
            volume_segment = volume_segment[:at]
        elif (colon := volume_segment.find(":")) >= 0:
            tag = _tag(text, volume_segment[colon + 1 :])
            volume_segment = volume_segment[:colon]
        volume = _name(text, "volume", volume_segment, _RESERVED_VOLUMES)
        path = _path(text, segments[2]) if len(segments) == 3 else ""
        return cls(
            namespace=namespace, volume=volume, tag=tag, digest=digest, path=path
        )

    @property
    def level(self) -> VolumeRefLevel:
        if self.path:
            return VolumeRefLevel.PATH
        if self.tag or self.digest:
            return VolumeRefLevel.POINT
        if self.volume:
            return VolumeRefLevel.VOLUME
        return VolumeRefLevel.NAMESPACE

    def pinned(self, digest: str) -> VolumeRef:
        """This volume pinned to a full ``b3:<64 hex>`` digest, with no path."""
        hex_digest = digest.removeprefix(_DIGEST_ALGORITHM).lower()
        if len(hex_digest) != _DIGEST_MAX:
            raise VolumeRefError(
                f"a pin needs the full {_DIGEST_MAX}-character digest, got {digest!r}"
            )
        return VolumeRef(
            namespace=self.namespace,
            volume=self.volume,
            digest=_DIGEST_ALGORITHM + hex_digest,
        )

    def with_path(self, path: str) -> VolumeRef:
        """The same point with ``path`` (``/a/b`` or ``/``) as its position."""
        return replace(self, path=_path(str(self), path.removeprefix("/")))

    def without_path(self) -> VolumeRef:
        return replace(self, path="")

    def shorthand(self) -> str:
        """The canonical form with a full digest cut to its first 12 characters."""
        hex_digest = self.digest.removeprefix(_DIGEST_ALGORITHM)
        if len(hex_digest) == _DIGEST_MAX:
            return str(replace(self, digest=hex_digest[:_DIGEST_SHORTHAND]))
        return str(self)

    def __str__(self) -> str:
        text = f"{_SCHEME}{self.namespace}/"
        if not self.volume:
            return text
        text += self.volume
        if self.digest:
            text += f"@{self.digest}"
        elif self.tag:
            text += f":{self.tag}"
        if self.path == "/":
            return text + "/"
        for segment in self.path.split("/"):
            if segment:
                text += "/" + quote(segment, safe=_SEGMENT_SAFE)
        return text


def _name(text: str, kind: str, segment: str, reserved: frozenset[str]) -> str:
    if not segment:
        raise VolumeRefError(f"ref {text!r}: no {kind}")
    name = segment.lower()
    if not _NAME_MIN <= len(name) <= _NAME_MAX:
        raise VolumeRefError(
            f"ref {text!r}: {kind} {segment!r} must be {_NAME_MIN} to {_NAME_MAX} characters"
        )
    if not (name[0].isascii() and name[0].isalpha()) or not all(
        c.isascii() and (c.isalnum() or c == "-") for c in name
    ):
        raise VolumeRefError(
            f"ref {text!r}: {kind} {segment!r} must begin with a letter and hold only letters, digits, and hyphens"
        )
    if name in reserved:
        raise VolumeRefError(f"ref {text!r}: {kind} {segment!r} is reserved")
    return name


def _tag(text: str, tag: str) -> str:
    if not tag:
        raise VolumeRefError(f"ref {text!r}: no tag after ':'")
    if len(tag) > _TAG_MAX:
        raise VolumeRefError(
            f"ref {text!r}: tag {tag!r} is longer than {_TAG_MAX} characters"
        )
    for index, c in enumerate(tag):
        if c.isascii() and (c.isalnum() or c == "_"):
            continue
        if index > 0 and c in ".-":
            continue
        raise VolumeRefError(
            f"ref {text!r}: tag {tag!r} must begin with a letter, digit, or underscore and hold only those plus dots and hyphens"
        )
    return tag


def _digest(text: str, digest: str) -> str:
    lowered = digest.lower()
    if not lowered:
        raise VolumeRefError(f"ref {text!r}: no digest after '@'")
    hex_digest = lowered.removeprefix(_DIGEST_ALGORITHM)
    if not hex_digest:
        raise VolumeRefError(f"ref {text!r}: no digest after {_DIGEST_ALGORITHM!r}")
    if len(hex_digest) > _DIGEST_MAX:
        raise VolumeRefError(
            f"ref {text!r}: digest {digest!r} is longer than {_DIGEST_MAX} hex characters"
        )
    if any(c not in "0123456789abcdef" for c in hex_digest):
        raise VolumeRefError(f"ref {text!r}: digest {digest!r} is not hex")
    return lowered


def _path(text: str, rest: str) -> str:
    # A trailing slash says "directory"; nothing here distinguishes "x" from
    # "x/", so it is dropped. The root keeps its slash as its whole spelling.
    trimmed = rest.rstrip("/") if rest != "/" else ""
    if not trimmed:
        return "/"
    parts: list[str] = []
    for segment in trimmed.split("/"):
        if not segment:
            raise VolumeRefError(f"ref {text!r}: path has an empty segment")
        decoded = unquote(segment)
        # Checked after decoding so "%2e%2e" and "a%2F.." cannot smuggle a traversal.
        if "/" in decoded:
            raise VolumeRefError(
                f"ref {text!r}: path segment {segment!r} decodes to a slash"
            )
        if decoded in (".", ".."):
            raise VolumeRefError(
                f"ref {text!r}: path segment {decoded!r} is not allowed"
            )
        parts.append(decoded)
    return "/" + "/".join(parts)
