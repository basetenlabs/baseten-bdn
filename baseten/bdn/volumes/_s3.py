"""Reads from the origin bucket: SigV4 signing, decode by content type, digest checks.

Every object is stored raw or zstd-compressed, declared by its Content-Type
and never by its key, and every digest is BLAKE3 over the decompressed bytes.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import random
import re
import time
from collections.abc import Mapping
from typing import Protocol
from urllib.parse import quote

import blake3
import httpx

from baseten.bdn.volumes._cannery import OriginCredentials
from baseten.bdn.volumes._errors import (
    VolumeConnectionError,
    VolumeIntegrityError,
    VolumeProtocolError,
    VolumeStorageError,
)

# The stdlib module exists from 3.14; the backport carries the same API below that.
try:
    from compression import zstd  # ty: ignore[unresolved-import]
except ImportError:
    try:
        from backports import zstd  # ty: ignore[unresolved-import]
    except ImportError as error:
        raise ImportError(
            "baseten.bdn.volumes needs zstd support: this Python was built without "
            "compression.zstd and the backports.zstd package is not installed"
        ) from error

_UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"
_SIGV4_SAFE = "-_.~"
ATTEMPTS = 5
_BACKOFF_BASE_SEC = 0.1
_BACKOFF_CAP_SEC = 2.0
_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
# S3 answers these with 400 or 403; both are safe to repeat.
_RETRYABLE_CODES = frozenset(
    {"RequestTimeout", "RequestTimeTooSkewed", "SlowDown", "InternalError"}
)
_EXPIRED_CODES = frozenset({"ExpiredToken", "ExpiredTokenException", "InvalidToken"})
_ERROR_CODE = re.compile(rb"<Code>([^<]{1,64})</Code>")

# Content types the origin uses for the objects a pull reads. The suffix is
# the storage encoding; the kind is checked against what the record expects.
_CONTENT_TYPE_PREFIX = "application/vnd.baseten.bdn."


class CredentialSource(Protocol):
    def current(self) -> OriginCredentials: ...

    def refresh(self) -> OriginCredentials: ...


def sigv4_headers(
    *,
    method: str,
    url: str,
    headers: Mapping[str, str],
    credentials: OriginCredentials,
    now: dt.datetime,
    payload_hash: str = _UNSIGNED_PAYLOAD,
) -> dict[str, str]:
    """Return ``headers`` plus the SigV4 ``Authorization`` and ``x-amz-*`` headers.

    Signs every header passed in, so callers add ``Range`` and friends before
    calling. Pure function of its inputs; tests pin it to AWS's published vector.
    """
    parsed = httpx.URL(url)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date = amz_date[:8]
    signed: dict[str, str] = {
        **{key.lower(): value.strip() for key, value in headers.items()},
        # httpx drops a default port from the Host it sends; sign what it sends.
        "host": parsed.netloc.decode("ascii"),
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if credentials.session_token:
        signed["x-amz-security-token"] = credentials.session_token
    names = sorted(signed)
    canonical_headers = "".join(f"{name}:{signed[name]}\n" for name in names)
    signed_headers = ";".join(names)
    canonical_uri = quote(parsed.path or "/", safe="/" + _SIGV4_SAFE)
    canonical_query = "&".join(
        sorted(
            f"{quote(key, safe=_SIGV4_SAFE)}={quote(value, safe=_SIGV4_SAFE)}"
            for key, value in parsed.params.multi_items()
        )
    )
    canonical_request = (
        f"{method}\n{canonical_uri}\n{canonical_query}\n"
        f"{canonical_headers}\n{signed_headers}\n{payload_hash}"
    )
    scope = f"{date}/{credentials.region}/s3/aws4_request"
    request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()
    string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{request_hash}"
    key = f"AWS4{credentials.secret_access_key}".encode()
    for message in (date, credentials.region, "s3", "aws4_request"):
        key = hmac.new(key, message.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={credentials.access_key_id}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return {
        **headers,
        "Authorization": authorization,
        **{name: value for name, value in signed.items() if name.startswith("x-amz-")},
    }


def object_url(credentials: OriginCredentials, key: str) -> str:
    """Virtual-hosted with Transfer Acceleration on AWS; path-style elsewhere."""
    encoded_key = quote(key, safe="/" + _SIGV4_SAFE)
    if not credentials.endpoint:
        return f"https://{credentials.bucket}.s3-accelerate.amazonaws.com/{encoded_key}"
    return f"{credentials.endpoint.rstrip('/')}/{credentials.bucket}/{encoded_key}"


def digest_of(data: bytes) -> str:
    return "b3:" + blake3.blake3(data).hexdigest()


def decode_object(
    body: bytes, content_type: str | None, *, expected_kind: str, expected_digest: str
) -> bytes:
    """Decompress per Content-Type and verify the BLAKE3 digest of the result."""
    if not content_type or not content_type.startswith(_CONTENT_TYPE_PREFIX):
        raise VolumeProtocolError(
            f"object has an unknown Content-Type {content_type!r}"
        )
    kind, _, encoding = content_type[len(_CONTENT_TYPE_PREFIX) :].partition("+")
    if kind != f"{expected_kind}.v1":
        raise VolumeProtocolError(
            f"expected a {expected_kind} object, got Content-Type {content_type!r}"
        )
    if encoding == "":
        data = body
    elif encoding == "zstd":
        try:
            data = zstd.decompress(body)
        except Exception as error:
            raise VolumeIntegrityError(
                f"{expected_kind} object is not valid zstd: {error}"
            ) from error
    else:
        raise VolumeProtocolError(
            f"object has an unknown encoding in Content-Type {content_type!r}"
        )
    actual = digest_of(data)
    if actual != expected_digest:
        raise VolumeIntegrityError(
            f"{expected_kind} object digest {actual} does not match the recorded {expected_digest}"
        )
    return data


class ObjectStore:
    """Authenticated GETs against the origin bucket with the credentials cannery issued.

    ``credentials`` hands out the current STS credentials and refreshes them
    when the bucket reports them expired, so a pull can outlive one session.
    """

    def __init__(
        self,
        http_client: httpx.Client,
        credentials: CredentialSource,
        *,
        timeout: httpx.Timeout,
    ) -> None:
        self._http_client = http_client
        self._credentials = credentials
        self._timeout = timeout

    def get(self, key: str) -> tuple[bytes, str | None]:
        """Return the stored bytes and Content-Type of one object key."""
        credentials = self._credentials.current()
        for attempt in range(1, ATTEMPTS + 1):
            url = object_url(credentials, key)
            headers = sigv4_headers(
                method="GET",
                url=url,
                headers={},
                credentials=credentials,
                now=dt.datetime.now(dt.UTC),
            )
            try:
                response = self._http_client.get(
                    url, headers=headers, timeout=self._timeout
                )
            except httpx.HTTPError as error:
                if attempt == ATTEMPTS or not isinstance(error, httpx.TransportError):
                    raise VolumeConnectionError(
                        f"could not read {key} from the origin bucket: {error}"
                    ) from error
            else:
                if response.is_success:
                    return _checked_body(response, key), response.headers.get(
                        "content-type"
                    )
                code = _error_code(response)
                if code in _EXPIRED_CODES:
                    credentials = self._credentials.refresh()
                elif attempt == ATTEMPTS or not (
                    response.status_code in _RETRYABLE_STATUSES
                    or code in _RETRYABLE_CODES
                ):
                    raise VolumeStorageError(
                        response.status_code,
                        key,
                        response.text.strip()[:300],
                        code=code,
                    )
            time.sleep(backoff_sec(attempt))
        raise AssertionError("unreachable: the retry loop returns or raises")


def _checked_body(response: httpx.Response, key: str) -> bytes:
    body = response.content
    declared = response.headers.get("content-length")
    if declared is None:
        raise VolumeProtocolError(
            f"origin bucket answered for {key} without Content-Length"
        )
    if int(declared) != len(body):
        raise VolumeIntegrityError(
            f"origin bucket sent {len(body)} bytes for {key}, Content-Length said {declared}"
        )
    return body


def _error_code(response: httpx.Response) -> str | None:
    match = _ERROR_CODE.search(response.content)
    return match.group(1).decode("ascii", "replace") if match else None


def is_retryable_status(status_code: int) -> bool:
    return status_code in _RETRYABLE_STATUSES


def backoff_sec(attempt: int) -> float:
    return min(
        _BACKOFF_CAP_SEC, _BACKOFF_BASE_SEC * 2 ** (attempt - 1)
    ) * random.uniform(0.5, 1.0)
