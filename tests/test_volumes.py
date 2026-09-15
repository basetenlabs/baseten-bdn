from __future__ import annotations

import datetime as dt
import os
import stat
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest

from baseten.bdn.volumes import (
    EntryKind,
    ResolvedFrom,
    VolumeAPIError,
    VolumeIntegrityError,
    VolumePathError,
    VolumeProtocolError,
    VolumesClient,
    VolumesClientOptions,
    _manifest,
    _s3,
)
from baseten.bdn.volumes._cannery import OriginCredentials, VolumeRef
from tests.volume_fixtures import (
    API_HOST,
    API_KEY,
    CANNERY_TOKEN,
    CHUNK,
    NAMESPACE,
    VOLUME,
    Dir,
    FakeServices,
    File,
    Symlink,
    Volume,
    build_volume,
    full_key,
    jsonl,
    relative_key,
)

REF = f"bdn:{NAMESPACE}/{VOLUME}:step-100"
posix_only = pytest.mark.skipif(
    sys.platform == "win32", reason="POSIX filesystem semantics"
)


def client(services: FakeServices, **kwargs: Any) -> VolumesClient:
    return VolumesClient(
        api_key=API_KEY,
        base_url_override=f"https://{API_HOST}",
        http_client_override=services.http_client(),
        **kwargs,
    )


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_s3, "_backoff_sec", lambda attempt: 0.0)


@pytest.mark.parametrize(
    ("ref", "canonical", "tag", "pin"),
    [
        ("bdn:loops/sampler-abc", "bdn://loops/sampler-abc", None, None),
        ("bdn://loops/sampler-abc", "bdn://loops/sampler-abc", None, None),
        (
            "bdn:loops/sampler-abc:step-100",
            "bdn://loops/sampler-abc:step-100",
            "step-100",
            None,
        ),
        (
            "bdn:loops/sampler-abc@b3:ABCDEF012345",
            "bdn://loops/sampler-abc@abcdef012345",
            None,
            "abcdef012345",
        ),
        (
            "bdn:loops/sampler-abc@" + "0" * 64,
            "bdn://loops/sampler-abc@" + "0" * 64,
            None,
            "0" * 64,
        ),
    ],
)
def test_volume_ref_parses_both_spellings(
    ref: str, canonical: str, tag: str | None, pin: str | None
) -> None:
    parsed = VolumeRef.parse(ref)
    assert parsed.canonical() == canonical
    assert (parsed.namespace, parsed.volume, parsed.tag, parsed.pin) == (
        "loops",
        "sampler-abc",
        tag,
        pin,
    )


@pytest.mark.parametrize(
    "ref",
    [
        "loops/vol",
        "bdn:loops",
        "bdn:loops/vol/extra",
        "bdn:Loops/vol",
        "bdn:loops/vol@abc",
        "bdn:loops/vol:",
        "bdn:loops/vol:a/b",
    ],
)
def test_volume_ref_rejects_malformed_refs(ref: str) -> None:
    with pytest.raises(ValueError):
        VolumeRef.parse(ref)


def test_sigv4_matches_the_published_aws_vector() -> None:
    # AWS "GET Object" example: examplebucket, us-east-1, 24 May 2013.
    credentials = OriginCredentials(
        endpoint="",
        region="us-east-1",
        bucket="examplebucket",
        access_key_id="AKIAIOSFODNN7EXAMPLE",
        secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )
    headers = _s3.sigv4_headers(
        method="GET",
        url="https://examplebucket.s3.amazonaws.com/test.txt",
        headers={"Range": "bytes=0-9"},
        credentials=credentials,
        now=dt.datetime(2013, 5, 24, tzinfo=dt.UTC),
        payload_hash="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    )
    assert headers["Authorization"] == (
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, "
        "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, "
        "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
    )
    assert headers["x-amz-date"] == "20130524T000000Z"
    assert "x-amz-security-token" not in headers


def test_sigv4_signs_the_session_token_when_present() -> None:
    credentials = OriginCredentials(
        endpoint="",
        region="us-west-2",
        bucket="b",
        access_key_id="ASIA",
        secret_access_key="s",
        session_token="tok",
    )
    headers = _s3.sigv4_headers(
        method="GET",
        url="https://b.s3-accelerate.amazonaws.com/k",
        headers={},
        credentials=credentials,
        now=dt.datetime(2026, 9, 15, tzinfo=dt.UTC),
    )
    assert headers["x-amz-security-token"] == "tok"
    assert (
        "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token,"
        in headers["Authorization"]
    )


def test_object_url_is_accelerated_on_aws_and_path_style_elsewhere() -> None:
    aws = OriginCredentials(
        endpoint="", region="r", bucket="b", access_key_id="a", secret_access_key="s"
    )
    minio = OriginCredentials(
        endpoint="http://minio:9000/",
        region="r",
        bucket="b",
        access_key_id="a",
        secret_access_key="s",
    )
    assert (
        _s3.object_url(aws, "bdn/o/n/objects/b3/aa/bb/cc")
        == "https://b.s3-accelerate.amazonaws.com/bdn/o/n/objects/b3/aa/bb/cc"
    )
    assert _s3.object_url(minio, "bdn/o/n/x") == "http://minio:9000/b/bdn/o/n/x"


def test_decode_object_verifies_kind_encoding_and_digest() -> None:
    data = b"hello world" * 100
    digest = _s3.digest_of(data)
    assert (
        _s3.decode_object(data, CHUNK, expected_kind="chunk", expected_digest=digest)
        == data
    )
    from tests.volume_fixtures import zstd_compress

    assert (
        _s3.decode_object(
            zstd_compress(data),
            CHUNK + "+zstd",
            expected_kind="chunk",
            expected_digest=digest,
        )
        == data
    )
    with pytest.raises(VolumeProtocolError, match="expected a manifest"):
        _s3.decode_object(data, CHUNK, expected_kind="manifest", expected_digest=digest)
    with pytest.raises(VolumeProtocolError, match="unknown Content-Type"):
        _s3.decode_object(
            data,
            "application/octet-stream",
            expected_kind="chunk",
            expected_digest=digest,
        )
    with pytest.raises(VolumeProtocolError, match="unknown Content-Type"):
        _s3.decode_object(data, None, expected_kind="chunk", expected_digest=digest)
    with pytest.raises(VolumeProtocolError, match="unknown encoding"):
        _s3.decode_object(
            data, CHUNK + "+gzip", expected_kind="chunk", expected_digest=digest
        )
    with pytest.raises(VolumeIntegrityError, match="does not match"):
        _s3.decode_object(
            data, CHUNK, expected_kind="chunk", expected_digest="b3:" + "0" * 64
        )
    with pytest.raises(VolumeIntegrityError, match="not valid zstd"):
        _s3.decode_object(
            b"not zstd", CHUNK + "+zstd", expected_kind="chunk", expected_digest=digest
        )


def chunk_record(path: str, data: bytes, **extra: Any) -> dict[str, Any]:
    digest = _s3.digest_of(data)
    return {
        "_type": "file",
        "_kind": "chunk",
        "mode": "0644",
        "path": path,
        "chunk": {
            "digest": digest,
            "length": len(data),
            "offset": 0,
            "target": {"relative_key": relative_key(digest)},
        },
        **extra,
    }


def header(entries: int, total: int = 0) -> dict[str, Any]:
    return {
        "_type": "manifest_header",
        "entry_count": entries,
        "manifest_schema": "v1",
        "total_size": total,
    }


def test_parse_manifest_accepts_any_record_order_and_skips_provenance() -> None:
    body = jsonl(
        [
            chunk_record("a/x", b"x"),
            {
                "_type": "path_provenance",
                "path": "a",
                "source_uri": "s3://x",
                "source_fingerprint": "f",
                "source_fingerprint_type": "sha256",
            },
            {"_type": "directory", "mode": "0755", "path": "a"},
            header(2, 1),
        ]
    )
    manifest = _manifest.parse_manifest(body)
    assert [entry.clean_path for entry in manifest.entries] == ["a/x", "a"]
    assert manifest.header.total_size == 1


@pytest.mark.parametrize(
    ("lines", "match"),
    [
        ([chunk_record("a", b"x")], "no manifest_header"),
        ([header(2), chunk_record("a", b"x")], "counts 2 entries, found 1"),
        ([header(1), {"_type": "hologram", "path": "a"}], "line 2 is off contract"),
        (
            [
                header(1),
                {"_type": "file", "_kind": "chunk", "mode": "rw-r--r--", "path": "a"},
            ],
            "off contract",
        ),
        ([header(1), header(1)], "two headers"),
        (
            [
                header(0),
                {
                    "_type": "manifest_header",
                    "entry_count": 0,
                    "manifest_schema": "v2",
                    "total_size": 0,
                },
            ],
            "off contract",
        ),
    ],
)
def test_parse_manifest_rejects_off_contract_documents(
    lines: list[dict[str, Any]], match: str
) -> None:
    with pytest.raises(VolumeProtocolError, match=match):
        _manifest.parse_manifest(jsonl(lines))


def test_parse_chunkmap_requires_contiguous_chunks_summing_to_the_file_size() -> None:
    def chunk(offset: int, length: int) -> dict[str, Any]:
        digest = "b3:" + "ab" * 32
        return {
            "_type": "chunk",
            "digest": digest,
            "length": length,
            "offset": offset,
            "target": {"relative_key": relative_key(digest)},
        }

    good = jsonl(
        [
            {"_type": "chunkmap_header", "chunk_count": 2, "file_size": 10},
            chunk(0, 6),
            chunk(6, 4),
        ]
    )
    assert [c.offset for c in _manifest.parse_chunkmap(good, 10)] == [0, 6]
    with pytest.raises(VolumeProtocolError, match="not contiguous"):
        _manifest.parse_chunkmap(
            jsonl(
                [
                    {"_type": "chunkmap_header", "chunk_count": 2, "file_size": 10},
                    chunk(0, 5),
                    chunk(6, 4),
                ]
            ),
            10,
        )
    with pytest.raises(VolumeProtocolError, match="sum to 9"):
        _manifest.parse_chunkmap(
            jsonl(
                [
                    {"_type": "chunkmap_header", "chunk_count": 2, "file_size": 10},
                    chunk(0, 5),
                    chunk(5, 4),
                ]
            ),
            10,
        )
    with pytest.raises(VolumeProtocolError, match="does not match the file record"):
        _manifest.parse_chunkmap(good, 11)
    with pytest.raises(VolumeProtocolError, match="counts 3 chunks"):
        _manifest.parse_chunkmap(
            jsonl(
                [
                    {"_type": "chunkmap_header", "chunk_count": 3, "file_size": 10},
                    chunk(0, 6),
                    chunk(6, 4),
                ]
            ),
            10,
        )


def entries(*records: dict[str, Any]) -> tuple[_manifest.PathEntry, ...]:
    return _manifest.parse_manifest(jsonl([header(len(records)), *records])).entries


@pytest.mark.parametrize(
    ("records", "match"),
    [
        ([chunk_record("../escape", b"x")], "escapes the volume root"),
        ([chunk_record("a/./b", b"x")], "not normalized"),
        ([chunk_record("a//b", b"x")], "not normalized"),
        ([chunk_record("a", b"x"), chunk_record("a", b"y")], "appears twice"),
        (
            [chunk_record("f", b"x"), chunk_record("f/child", b"y")],
            "nested beneath the non-directory",
        ),
        (
            [
                {
                    "_type": "symlink",
                    "mode": "0777",
                    "path": "l",
                    "target": "../../etc/passwd",
                }
            ],
            "escapes the volume root",
        ),
        (
            [{"_type": "symlink", "mode": "0777", "path": "l", "target": ""}],
            "empty target",
        ),
        (
            [
                {"_type": "symlink", "mode": "0777", "path": "a", "target": "b"},
                {"_type": "symlink", "mode": "0777", "path": "b", "target": "a"},
            ],
            "longer than 40 hops",
        ),
    ],
)
def test_containment_gate_rejects_escapes_before_any_write(
    records: list[dict[str, Any]], match: str
) -> None:
    with pytest.raises(VolumePathError, match=match):
        _manifest.ContainedPaths(entries(*records))


def test_containment_gate_renders_absolute_symlink_targets_relative_to_the_link() -> (
    None
):
    contained = _manifest.ContainedPaths(
        entries(
            {"_type": "directory", "mode": "0755", "path": "models/a"},
            {
                "_type": "symlink",
                "mode": "0777",
                "path": "models/a/current",
                "target": "/models/b/weights.bin",
            },
            {
                "_type": "symlink",
                "mode": "0777",
                "path": "models/a/rel",
                "target": "../b/weights.bin",
            },
            {"_type": "symlink", "mode": "0777", "path": "top", "target": "/models/a"},
        )
    )
    link = contained.by_path["models/a/current"]
    assert isinstance(link, _manifest.SymlinkEntry)
    assert contained.rendered_symlink_target(link) == "../b/weights.bin"
    rel = contained.by_path["models/a/rel"]
    assert isinstance(rel, _manifest.SymlinkEntry)
    assert contained.rendered_symlink_target(rel) == "../b/weights.bin"
    top = contained.by_path["top"]
    assert isinstance(top, _manifest.SymlinkEntry)
    assert contained.rendered_symlink_target(top) == "models/a"


def sample_tree() -> dict[str, File | Dir | Symlink]:
    big = bytes(range(256)) * 64  # 16 KiB, split into 5 chunks below
    return {
        "adapter": Dir(),
        "adapter/config.json": File(b'{"r": 16}\n', compress=True),
        "adapter/empty.marker": File(b""),
        "adapter/weights.bin": File(big, chunk_size=3000),
        "adapter/sub": Dir(mode="0555"),
        "adapter/sub/note.txt": File(b"read-only dir child\n", mode="0600"),
    }


def posix_tree() -> dict[str, File | Dir | Symlink]:
    tree = sample_tree()
    tree["adapter/latest"] = Symlink("weights.bin")
    tree["adapter/abs"] = Symlink("/adapter/config.json")
    tree["adapter/hardlink-a"] = File(b"shared inode\n", link_group=7)
    tree["adapter/hardlink-b"] = File(b"shared inode\n", link_group=7)
    return tree


def test_pull_materializes_the_tree_and_talks_to_all_three_services(
    tmp_path: Path,
) -> None:
    services = FakeServices(build_volume(sample_tree()))
    dest = tmp_path / "ckpt"

    result = client(services).pull(REF, dest)

    assert (dest / "adapter/config.json").read_bytes() == b'{"r": 16}\n'
    assert (dest / "adapter/empty.marker").read_bytes() == b""
    assert (dest / "adapter/weights.bin").read_bytes() == bytes(range(256)) * 64
    assert (dest / "adapter/sub/note.txt").read_bytes() == b"read-only dir child\n"
    assert result.reference == f"bdn://{NAMESPACE}/{VOLUME}:step-100"
    assert result.digest == services.volume.manifest_digest
    assert result.files == 4
    assert result.bytes == 10 + 0 + 16384 + 20
    assert result.dest_dir == dest

    (token_request,) = services.token_requests()
    assert token_request.headers["authorization"] == f"Bearer {API_KEY}"
    assert token_request.url.path == "/v1/volumes/token"
    import json

    assert json.loads(token_request.content) == {
        "scopes": ["PULL"],
        "namespaces": [NAMESPACE],
        "volumes": [VOLUME],
    }
    (resolve_request,) = services.resolve_requests()
    assert resolve_request.headers["authorization"] == f"Bearer {CANNERY_TOKEN}"
    assert resolve_request.url.params["ref"] == f"bdn://{NAMESPACE}/{VOLUME}:step-100"
    assert resolve_request.content == b""
    s3 = services.s3_requests()
    # manifest + chunkmap + config chunk + note chunk + 6 weight chunks; the empty file needs none
    assert len(s3) == 2 + 2 + 6
    for request in s3:
        assert request.headers["authorization"].startswith(
            "AWS4-HMAC-SHA256 Credential=ASIAEXAMPLE/"
        )
        assert request.headers["x-amz-security-token"] == "sts-session-token"
        assert request.headers["x-amz-content-sha256"] == "UNSIGNED-PAYLOAD"
        assert request.url.path.startswith(
            f"/bdn/{services.volume and 'org_2qRk4dB'}/{NAMESPACE}/objects/b3/"
        )


@posix_only
def test_pull_restores_modes_symlinks_and_hardlinks(tmp_path: Path) -> None:
    services = FakeServices(build_volume(posix_tree()))
    dest = tmp_path / "ckpt"

    result = client(services).pull(REF, dest)

    assert stat.S_IMODE((dest / "adapter/sub").stat().st_mode) == 0o555
    assert stat.S_IMODE((dest / "adapter/sub/note.txt").stat().st_mode) == 0o600
    assert stat.S_IMODE((dest / "adapter/config.json").stat().st_mode) == 0o644
    assert os.readlink(dest / "adapter/latest") == "weights.bin"
    assert os.readlink(dest / "adapter/abs") == "config.json"
    assert (dest / "adapter/abs").read_bytes() == b'{"r": 16}\n'
    a, b = (dest / "adapter/hardlink-a").stat(), (dest / "adapter/hardlink-b").stat()
    assert a.st_ino == b.st_ino
    assert result.files == 6
    # The second hardlink member is not fetched again.
    assert (
        sum(1 for r in services.s3_requests() if "objects" in r.url.path)
        == 2 + 2 + 6 + 1
    )
    os.chmod(dest / "adapter/sub", 0o755)


@posix_only
def test_pull_overwrites_a_stale_symlink_instead_of_following_it(
    tmp_path: Path,
) -> None:
    services = FakeServices(build_volume(sample_tree()))
    dest = tmp_path / "ckpt"
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"do not touch")
    (dest / "adapter").mkdir(parents=True)
    os.symlink(outside, dest / "adapter/config.json")
    os.symlink(tmp_path / "elsewhere", dest / "adapter/sub")

    client(services).pull(REF, dest)

    assert outside.read_bytes() == b"do not touch"
    assert not (dest / "adapter/config.json").is_symlink()
    assert (dest / "adapter/config.json").read_bytes() == b'{"r": 16}\n'
    assert (dest / "adapter/sub").is_dir() and not (dest / "adapter/sub").is_symlink()
    os.chmod(dest / "adapter/sub", 0o755)


def test_list_files_and_resolve_download_only_the_manifest() -> None:
    services = FakeServices(build_volume(sample_tree()))
    volumes = client(services)

    resolved = volumes.resolve(REF)
    assert resolved.digest == services.volume.manifest_digest
    assert resolved.resolved_from is ResolvedFrom.TAG
    assert resolved.sequence == 7
    assert services.s3_requests() == []

    files = volumes.list_files(REF)
    assert {(f.path, f.kind, f.size) for f in files} == {
        ("adapter", EntryKind.DIRECTORY, 0),
        ("adapter/config.json", EntryKind.FILE, 10),
        ("adapter/empty.marker", EntryKind.FILE, 0),
        ("adapter/weights.bin", EntryKind.FILE, 16384),
        ("adapter/sub", EntryKind.DIRECTORY, 0),
        ("adapter/sub/note.txt", EntryKind.FILE, 20),
    }
    assert len(services.s3_requests()) == 1
    # One token covers both calls; each call resolves anew.
    assert len(services.token_requests()) == 1
    assert len(services.resolve_requests()) == 2


def test_expired_token_is_minted_again(monkeypatch: pytest.MonkeyPatch) -> None:
    services = FakeServices(
        build_volume(sample_tree()), token_expires_in=dt.timedelta(minutes=4)
    )
    volumes = client(services)
    volumes.resolve(REF)
    volumes.resolve(REF)
    assert len(services.token_requests()) == 2


def test_credentials_near_expiry_trigger_a_fresh_resolve_mid_pull(
    tmp_path: Path,
) -> None:
    services = FakeServices(
        build_volume(sample_tree()), credentials_expire_in=dt.timedelta(minutes=1)
    )

    client(services, max_concurrency=1).pull(REF, tmp_path)

    # Initial resolve plus one refresh before the first object read; the
    # refreshed credentials are then reused for the rest of the pull.
    assert len(services.resolve_requests()) >= 2
    assert (tmp_path / "adapter/weights.bin").stat().st_size == 16384


def test_nothing_is_written_when_the_manifest_escapes(tmp_path: Path) -> None:
    volume = build_volume({"ok.txt": File(b"fine")})
    # Replace the manifest with one that escapes; the digest must still match what resolve reports.
    body = jsonl(
        [
            header(2, 8),
            chunk_record("ok.txt", b"fine"),
            chunk_record("../evil.txt", b"fine"),
        ]
    )
    volume.objects.clear()
    volume.manifest_digest = volume.put(
        body, "application/vnd.baseten.bdn.manifest.v1", compress=True
    )
    services = FakeServices(volume)

    with pytest.raises(VolumePathError, match="escapes the volume root"):
        client(services).pull(REF, tmp_path / "out")
    assert not (tmp_path / "out" / "ok.txt").exists()
    assert not (tmp_path / "evil.txt").exists()


def test_corrupt_chunk_fails_the_pull_with_an_integrity_error(tmp_path: Path) -> None:
    volume = build_volume({"a.bin": File(b"correct bytes")})
    key = next(k for k in volume.objects if k != full_key(volume.manifest_digest))
    volume.objects[key] = (b"tampered bytes", CHUNK)
    services = FakeServices(volume)

    with pytest.raises(VolumeIntegrityError, match="does not match the recorded"):
        client(services).pull(REF, tmp_path)


def test_short_body_fails_the_content_length_check(tmp_path: Path) -> None:
    volume = build_volume({"a.bin": File(b"correct bytes")})
    services = FakeServices(volume)
    key = next(k for k in volume.objects if k != full_key(volume.manifest_digest))

    def truncating(request: httpx.Request) -> httpx.Response:
        response = services.handle(request)
        if request.url.path.lstrip("/") == key:
            response.headers["content-length"] = str(len(response.content) + 5)
        return response

    volumes = VolumesClient(
        api_key=API_KEY,
        base_url_override=f"https://{API_HOST}",
        http_client_override=httpx.Client(transport=httpx.MockTransport(truncating)),
    )
    with pytest.raises(VolumeIntegrityError, match="Content-Length said"):
        volumes.pull(REF, tmp_path)


def test_transient_s3_errors_are_retried(tmp_path: Path) -> None:
    volume = build_volume({"a.bin": File(b"payload")})
    key = next(k for k in volume.objects if k != full_key(volume.manifest_digest))
    services = FakeServices(volume, s3_failures={key: [503, 500]})

    client(services).pull(REF, tmp_path)

    assert (tmp_path / "a.bin").read_bytes() == b"payload"
    assert sum(1 for r in services.s3_requests() if r.url.path.lstrip("/") == key) == 3


def test_persistent_s3_error_surfaces_with_the_status(tmp_path: Path) -> None:
    volume = build_volume({"a.bin": File(b"payload")})
    key = next(k for k in volume.objects if k != full_key(volume.manifest_digest))
    services = FakeServices(volume, s3_failures={key: [403]})

    with pytest.raises(VolumeAPIError) as raised:
        client(services).pull(REF, tmp_path)
    assert raised.value.service == "origin bucket"
    assert raised.value.status_code == 403
    assert sum(1 for r in services.s3_requests() if r.url.path.lstrip("/") == key) == 1


def test_cannery_errors_carry_the_reason() -> None:
    services = FakeServices(
        build_volume({}),
        resolve_error=(
            404,
            {
                "error": {
                    "code": "NOT_FOUND",
                    "message": "tag not found: step-100",
                    "reason": "NOT_FOUND",
                    "domain": "bdn.baseten.co",
                }
            },
        ),
    )
    with pytest.raises(VolumeAPIError) as raised:
        client(services).resolve(REF)
    assert (raised.value.service, raised.value.status_code, raised.value.reason) == (
        "cannery",
        404,
        "NOT_FOUND",
    )
    assert "tag not found" in str(raised.value)


def test_non_envelope_cannery_errors_keep_the_body() -> None:
    services = FakeServices(
        build_volume({}), resolve_error=(502, "<html>bad gateway</html>")
    )
    with pytest.raises(VolumeAPIError) as raised:
        client(services).resolve(REF)
    assert raised.value.status_code == 502
    assert raised.value.reason is None
    assert "bad gateway" in raised.value.message


def test_baseten_api_errors_name_the_service() -> None:
    services = FakeServices(
        build_volume({}),
        token_error=(
            403,
            {
                "code": "FORBIDDEN",
                "message": "volumes are not enabled for this organization",
            },
        ),
    )
    with pytest.raises(VolumeAPIError) as raised:
        client(services).resolve(REF)
    assert (raised.value.service, raised.value.status_code, raised.value.code) == (
        "Baseten API",
        403,
        "FORBIDDEN",
    )
    assert services.resolve_requests() == []


def test_missing_bdn_endpoint_is_an_error_unless_overridden() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "token": "t",
                "expires_at": (
                    dt.datetime.now(dt.UTC) + dt.timedelta(hours=1)
                ).isoformat(),
                "bdn_endpoint": None,
            },
        )

    volumes = VolumesClient(
        api_key=API_KEY,
        base_url_override=f"https://{API_HOST}",
        http_client_override=httpx.Client(transport=httpx.MockTransport(handle)),
    )
    from baseten.bdn.volumes import VolumeConnectionError

    with pytest.raises(VolumeConnectionError, match="bdn_endpoint_override"):
        volumes.resolve(REF)


def test_slabmap_files_are_reported_as_unsupported(tmp_path: Path) -> None:
    volume = Volume()
    body = jsonl(
        [
            header(1, 0),
            {"_type": "file", "_kind": "slabmap", "mode": "0644", "path": "big.bin"},
        ]
    )
    volume.manifest_digest = volume.put(
        body, "application/vnd.baseten.bdn.manifest.v1", compress=True
    )
    services = FakeServices(volume)
    with pytest.raises(NotImplementedError, match="slabmap"):
        client(services).pull(REF, tmp_path)
    with pytest.raises(NotImplementedError, match="slabmap"):
        client(services).list_files(REF)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"api_key": ""},
        {"api_key": "k", "max_concurrency": 0},
        {"api_key": "k", "max_bytes_in_flight": 0},
        {"api_key": "k", "request_timeout_sec": 0},
    ],
)
def test_options_reject_nonsense_values(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        VolumesClientOptions(**kwargs)


def test_default_client_targets_the_public_api() -> None:
    volumes = VolumesClient(api_key="k")
    try:
        assert volumes.options.base_url == "https://api.baseten.co"
        assert volumes.close_http_client_on_close is True
        assert volumes.http_client.headers["user-agent"].startswith("baseten-bdn/")
    finally:
        volumes.close()


def test_byte_budget_admits_oversized_requests_alone() -> None:
    from baseten.bdn.volumes._materialize import ByteBudget

    budget = ByteBudget(10)
    budget.acquire(8)
    budget.release(8)
    budget.acquire(50)  # larger than the budget, admitted because nothing is in flight
    budget.release(50)
    with pytest.raises(ValueError):
        ByteBudget(0)
