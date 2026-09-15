from __future__ import annotations

import datetime as dt
import json
import os
import shutil
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
    VolumeConnectionError,
    VolumeDestinationError,
    VolumeError,
    VolumeIntegrityError,
    VolumePathError,
    VolumeProtocolError,
    VolumeRefError,
    VolumesClient,
    VolumesClientOptions,
    VolumeStorageError,
    VolumeUnsupportedError,
    _client,
    _manifest,
    _s3,
)
from baseten.bdn.volumes._cannery import OriginCredentials, VolumeRef
from baseten.bdn.volumes._materialize import ByteBudget
from tests.volume_fixtures import (
    API_HOST,
    API_KEY,
    CANNERY_TOKEN,
    CHUNK,
    EMPTY_DIGEST,
    NAMESPACE,
    ORG_ID,
    PROVENANCE,
    VOLUME,
    Dir,
    FakeServices,
    File,
    Symlink,
    Volume,
    build_volume,
    chunk_record,
    full_key,
    jsonl,
    manifest_header,
    relative_key,
    s3_error,
    zstd,
)

REF = f"bdn:{NAMESPACE}/{VOLUME}:step-100"
CANONICAL = f"bdn://{NAMESPACE}/{VOLUME}:step-100"
posix_only = pytest.mark.skipif(sys.platform == "win32", reason="pull is POSIX only")


def client(services: FakeServices, **kwargs: Any) -> VolumesClient:
    return VolumesClient(
        api_key=API_KEY,
        base_url_override=f"https://{API_HOST}",
        http_client_override=services.http_client(),
        **kwargs,
    )


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_s3, "backoff_sec", lambda attempt: 0.0)


def file_record(path: str, data: bytes, **extra: Any) -> dict[str, Any]:
    return {
        "_type": "file",
        "_kind": "chunk",
        "mode": "0644",
        "path": path,
        "chunk": chunk_record(data),
        **extra,
    }


def entries(*records: dict[str, Any]) -> tuple[_manifest.PathEntry, ...]:
    return _manifest.parse_manifest(
        jsonl([manifest_header(len(records)), *records])
    ).entries


# --- refs -------------------------------------------------------------------


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
        ("bdn:Loops/Sampler-ABC:Step", "bdn://loops/sampler-abc:Step", "Step", None),
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
def test_volume_ref_parses_both_spellings_and_folds_case(
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
        "bdn:loops/vol@abc",
        "bdn:loops/vol:",
        "bdn:loops/vol:a/b",
        "bdn:lo ops/vol",
    ],
)
def test_volume_ref_rejects_malformed_refs_as_volume_errors(ref: str) -> None:
    with pytest.raises(VolumeRefError) as raised:
        VolumeRef.parse(ref)
    assert isinstance(raised.value, VolumeError)
    assert isinstance(raised.value, ValueError)


def test_pinned_ref_names_one_version() -> None:
    digest = "b3:" + "ab" * 32
    assert (
        VolumeRef.parse(REF).pinned(digest).canonical()
        == f"bdn://{NAMESPACE}/{VOLUME}@" + "ab" * 32
    )


# --- SigV4 and object URLs --------------------------------------------------


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


def test_sigv4_signs_the_session_token_and_the_host_httpx_sends() -> None:
    credentials = OriginCredentials(
        endpoint="https://minio.test:443",
        region="us-west-2",
        bucket="b",
        access_key_id="ASIA",
        secret_access_key="s",
        session_token="tok",
    )
    url = _s3.object_url(credentials, "k")
    headers = _s3.sigv4_headers(
        method="GET",
        url=url,
        headers={},
        credentials=credentials,
        now=dt.datetime(2026, 9, 15, tzinfo=dt.UTC),
    )
    assert headers["x-amz-security-token"] == "tok"
    assert (
        "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token,"
        in headers["Authorization"]
    )
    # The default port is dropped from the signed host, as httpx drops it from the request.
    request = httpx.Client().build_request("GET", url, headers=headers)
    assert request.headers["host"] == "minio.test"
    resigned = _s3.sigv4_headers(
        method="GET",
        url="https://minio.test/b/k",
        headers={},
        credentials=credentials,
        now=dt.datetime(2026, 9, 15, tzinfo=dt.UTC),
    )
    assert resigned["Authorization"] == headers["Authorization"]


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
    assert (
        _s3.decode_object(
            zstd.compress(data),
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


# --- manifest parsing -------------------------------------------------------


def test_parse_manifest_accepts_any_record_order_and_skips_provenance() -> None:
    body = jsonl(
        [
            file_record("a/x", b"x"),
            {
                "_type": "path_provenance",
                "path": "a",
                "source_uri": "s3://x",
                "source_fingerprint": "f",
                "source_fingerprint_type": "sha256",
            },
            {"_type": "directory", "mode": "0755", "path": "a"},
            manifest_header(2, 1),
        ]
    )
    manifest = _manifest.parse_manifest(body)
    assert [entry.clean_path for entry in manifest.entries] == ["a/x", "a"]
    assert manifest.header.total_size == 1


@pytest.mark.parametrize(
    ("lines", "match"),
    [
        ([file_record("a", b"x")], "no manifest_header"),
        (
            [manifest_header(1), {"_type": "hologram", "path": "a"}],
            "line 2 is off contract",
        ),
        (
            [
                manifest_header(1),
                {
                    "_type": "file",
                    "_kind": "chunk",
                    "mode": "rw-r--r--",
                    "path": "a",
                    "chunk": chunk_record(b"x"),
                },
            ],
            "off contract",
        ),
        (
            [
                manifest_header(1),
                {"_type": "file", "_kind": "chunk", "mode": "0644", "path": "a"},
            ],
            "chunk",
        ),
        (
            [
                manifest_header(1),
                {
                    "_type": "file",
                    "_kind": "chunk",
                    "mode": "0644",
                    "path": "a",
                    "chunk": chunk_record(b"x", offset=8),
                },
            ],
            "chunk offset 8",
        ),
        ([manifest_header(1), manifest_header(1)], "two headers"),
        (
            [
                manifest_header(0),
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
    ids=[
        "no-header",
        "unknown-type",
        "bad-mode",
        "missing-chunk",
        "chunk-offset",
        "two-headers",
        "schema-v2",
    ],
)
def test_parse_manifest_rejects_off_contract_documents(
    lines: list[dict[str, Any]], match: str
) -> None:
    with pytest.raises(VolumeProtocolError, match=match):
        _manifest.parse_manifest(jsonl(lines))


def test_slabmap_records_are_unsupported_not_malformed() -> None:
    body = jsonl(
        [
            manifest_header(1),
            {"_type": "file", "_kind": "slabmap", "mode": "0644", "path": "big.bin"},
        ]
    )
    with pytest.raises(VolumeUnsupportedError, match="slabmap") as raised:
        _manifest.parse_manifest(body)
    assert isinstance(raised.value, NotImplementedError)


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

    def header(count: int, size: int = 10) -> dict[str, Any]:
        return {"_type": "chunkmap_header", "chunk_count": count, "file_size": size}

    assert [
        c.offset
        for c in _manifest.parse_chunkmap(
            jsonl([header(2), chunk(0, 6), chunk(6, 4)]), 10
        )
    ] == [0, 6]
    with pytest.raises(VolumeProtocolError, match="not contiguous"):
        _manifest.parse_chunkmap(jsonl([header(2), chunk(0, 5), chunk(6, 4)]), 10)
    with pytest.raises(VolumeProtocolError, match="sum to 9"):
        _manifest.parse_chunkmap(jsonl([header(2), chunk(0, 5), chunk(5, 4)]), 10)
    with pytest.raises(VolumeProtocolError, match="does not match the file record"):
        _manifest.parse_chunkmap(jsonl([header(2), chunk(0, 6), chunk(6, 4)]), 11)
    with pytest.raises(VolumeProtocolError, match="counts 3 chunks"):
        _manifest.parse_chunkmap(jsonl([header(3), chunk(0, 6), chunk(6, 4)]), 10)


# --- containment gate -------------------------------------------------------


def symlink(path: str, target: str) -> dict[str, Any]:
    return {"_type": "symlink", "mode": "0777", "path": path, "target": target}


def directory(path: str, mode: str = "0755") -> dict[str, Any]:
    return {"_type": "directory", "mode": mode, "path": path}


@pytest.mark.parametrize(
    ("records", "match"),
    [
        ([file_record("../escape", b"x")], "escapes the volume root"),
        ([file_record("a/./b", b"x")], "not normalized"),
        ([file_record("a//b", b"x")], "not normalized"),
        ([file_record("a", b"x"), file_record("a", b"y")], "appears twice"),
        (
            [file_record("f", b"x"), file_record("f/child", b"y")],
            "nested beneath the non-directory",
        ),
        ([symlink("l", "../../etc/passwd")], "escapes the volume root"),
        ([symlink("l", "")], "empty target"),
        ([symlink("a", "b"), symlink("b", "a")], "longer than 40 hops"),
        # `..` after a link that already points upward counts against the real location.
        (
            [directory("a"), symlink("a/up", ".."), symlink("x", "a/up/../secret")],
            "escapes the volume root",
        ),
        (
            [
                directory("a"),
                symlink("a/up", ".."),
                symlink("a/esc", "up/../../marker"),
            ],
            "escapes the volume root",
        ),
    ],
    ids=[
        "dotdot",
        "dot-segment",
        "double-slash",
        "duplicate",
        "under-file",
        "link-escape",
        "empty-target",
        "loop",
        "chased-dotdot",
        "chased-dotdot-relative",
    ],
)
def test_containment_gate_rejects_escapes_before_any_write(
    records: list[dict[str, Any]], match: str
) -> None:
    with pytest.raises(VolumePathError, match=match):
        _manifest.ContainedPaths(entries(*records))


def test_containment_gate_renders_symlink_targets_relative_to_the_link() -> None:
    contained = _manifest.ContainedPaths(
        entries(
            directory("models/a"),
            symlink("models/a/current", "/models/b/weights.bin"),
            symlink("models/a/rel", "../b/weights.bin"),
            symlink("top", "/models/a"),
            symlink("models/a/root", "/"),
            symlink("rootlink", "/"),
            directory("a"),
            symlink("a/up", ".."),
            symlink("via-up", "a/up/models/a/rel"),
        )
    )

    def rendered(path: str) -> str:
        entry = contained.by_path[path]
        assert isinstance(entry, _manifest.SymlinkEntry)
        return contained.rendered_symlink_target(entry)

    assert rendered("models/a/current") == "../b/weights.bin"
    assert rendered("models/a/rel") == "../b/weights.bin"
    assert rendered("top") == "models/a"
    assert rendered("models/a/root") == "../.."
    assert rendered("rootlink") == "."
    assert rendered("via-up") == "a/up/models/a/rel"


def test_implicit_directories_are_every_ancestor_shallowest_first() -> None:
    assert _manifest.implicit_directories(
        entries(
            file_record("a/b/c", b"x"),
            file_record("a/d", b"y"),
            file_record("top", b"z"),
        )
    ) == ["a", "a/b"]


# --- pull -------------------------------------------------------------------


def sample_tree() -> dict[str, File | Dir | Symlink]:
    big = bytes(range(256)) * 64  # 16 KiB, split into 6 chunks below
    return {
        "adapter": Dir(),
        "adapter/config.json": File(b'{"r": 16}\n', compress=True),
        "adapter/empty.marker": File(b""),
        "adapter/weights.bin": File(big, chunk_size=3000),
        "adapter/sub": Dir(mode="0555"),
        "adapter/sub/note.txt": File(b"read-only dir child\n", mode="0600"),
        "adapter/latest": Symlink("weights.bin"),
        "adapter/abs": Symlink("/adapter/config.json"),
        "adapter/hardlink-a": File(b"shared inode\n", link_group=7),
        "adapter/hardlink-b": File(b"shared inode\n", link_group=7),
    }


SAMPLE_BYTES = 10 + 0 + 16384 + 20 + 13
SAMPLE_OBJECT_GETS = (
    1 + 1 + 1 + 6 + 1 + 1
)  # manifest, chunkmap, config, weights x6, note, hardlink-a


def assert_sample_tree(dest: Path) -> None:
    assert (dest / "adapter/config.json").read_bytes() == b'{"r": 16}\n'
    assert (dest / "adapter/empty.marker").read_bytes() == b""
    assert (dest / "adapter/weights.bin").read_bytes() == bytes(range(256)) * 64
    assert (dest / "adapter/sub/note.txt").read_bytes() == b"read-only dir child\n"
    assert stat.S_IMODE((dest / "adapter/sub").stat().st_mode) == 0o555
    assert stat.S_IMODE((dest / "adapter/sub/note.txt").stat().st_mode) == 0o600
    assert stat.S_IMODE((dest / "adapter/config.json").stat().st_mode) == 0o644
    assert os.readlink(dest / "adapter/latest") == "weights.bin"
    assert os.readlink(dest / "adapter/abs") == "config.json"
    assert (dest / "adapter/abs").read_bytes() == b'{"r": 16}\n'
    assert (dest / "adapter/hardlink-a").stat().st_ino == (
        dest / "adapter/hardlink-b"
    ).stat().st_ino


@posix_only
def test_pull_materializes_the_tree_and_talks_to_all_three_services(
    tmp_path: Path,
) -> None:
    services = FakeServices(build_volume(sample_tree()))
    dest = tmp_path / "ckpt"

    result = client(services).pull(REF, dest)

    assert_sample_tree(dest)
    assert result.reference == CANONICAL
    assert result.digest == services.volume.manifest_digest
    assert result.file_count == 6
    assert result.bytes_written == SAMPLE_BYTES
    assert result.dest_dir == dest
    assert [p.name for p in tmp_path.iterdir()] == ["ckpt"], (
        "no staging directory left behind"
    )

    (token_request,) = services.token_requests()
    assert token_request.headers["authorization"] == f"Bearer {API_KEY}"
    assert token_request.url.path == "/v1/volumes/token"
    assert json.loads(token_request.content) == {
        "scopes": ["PULL"],
        "namespaces": [NAMESPACE],
        "volumes": [VOLUME],
    }
    (resolve_request,) = services.resolve_requests()
    assert resolve_request.headers["authorization"] == f"Bearer {CANNERY_TOKEN}"
    assert resolve_request.url.params["ref"] == CANONICAL
    assert resolve_request.content == b""
    s3 = services.s3_requests()
    assert len(s3) == SAMPLE_OBJECT_GETS
    for request in s3:
        assert request.headers["authorization"].startswith(
            "AWS4-HMAC-SHA256 Credential=ASIA1/"
        )
        assert request.headers["x-amz-security-token"] == "sts-session-token"
        assert request.headers["x-amz-content-sha256"] == "UNSIGNED-PAYLOAD"
        assert request.url.path.startswith(f"/bdn/{ORG_ID}/{NAMESPACE}/objects/b3/")


@posix_only
def test_pull_into_an_existing_directory_overwrites_and_survives_read_only_dirs(
    tmp_path: Path,
) -> None:
    dest = tmp_path / "ckpt"
    client(FakeServices(build_volume(sample_tree()))).pull(REF, dest)

    changed = sample_tree()
    changed["adapter/hardlink-a"] = File(b"AAAA")
    changed["adapter/hardlink-b"] = File(b"BBBB")
    changed["adapter/sub/new.txt"] = File(b"new child in a 0555 dir\n")
    changed["adapter/config.json"] = File(b'{"r": 32}\n')
    services = FakeServices(build_volume(changed))

    result = client(services).pull(REF, dest)

    assert (dest / "adapter/hardlink-a").read_bytes() == b"AAAA"
    assert (dest / "adapter/hardlink-b").read_bytes() == b"BBBB"
    assert (dest / "adapter/hardlink-a").stat().st_nlink == 1
    assert (dest / "adapter/sub/new.txt").read_bytes() == b"new child in a 0555 dir\n"
    assert (dest / "adapter/config.json").read_bytes() == b'{"r": 32}\n'
    assert stat.S_IMODE((dest / "adapter/sub").stat().st_mode) == 0o555
    assert result.file_count == 7


@posix_only
def test_pull_replaces_stale_symlinks_instead_of_following_them(tmp_path: Path) -> None:
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
    assert (dest / "adapter/sub").is_dir() and not (dest / "adapter/sub").is_symlink()
    assert_sample_tree(dest)


@posix_only
def test_failed_pull_into_a_new_directory_leaves_nothing(tmp_path: Path) -> None:
    volume = build_volume(sample_tree())
    key = full_key(_s3.digest_of(b"read-only dir child\n"))
    volume.objects[key] = (b"tampered", CHUNK)
    services = FakeServices(volume)
    dest = tmp_path / "ckpt"

    with pytest.raises(VolumeIntegrityError):
        client(services).pull(REF, dest)

    assert not dest.exists()
    assert list(tmp_path.iterdir()) == []


@posix_only
def test_pull_into_a_path_whose_parent_is_a_file_is_a_destination_error(
    tmp_path: Path,
) -> None:
    services = FakeServices(build_volume({"a": Dir(), "a/b": File(b"x")}))
    dest = tmp_path / "ckpt"
    dest.mkdir()
    (dest / "a").write_bytes(b"a file, not a directory")

    with pytest.raises(VolumeDestinationError, match="is not a directory"):
        client(services).pull(REF, dest)


@posix_only
def test_pull_refuses_when_the_disk_is_too_small(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    services = FakeServices(build_volume(sample_tree()))
    monkeypatch.setattr(
        _client.shutil, "disk_usage", lambda path: shutil._ntuple_diskusage(100, 90, 10)
    )

    with pytest.raises(VolumeDestinationError, match="nothing was written"):
        client(services).pull(REF, tmp_path / "ckpt")
    assert not (tmp_path / "ckpt").exists()
    assert services.s3_requests()[-1].url.path.endswith(
        relative_key(services.volume.manifest_digest)
    ), "only the manifest was read"


@posix_only
def test_empty_files_cost_no_object_read(tmp_path: Path) -> None:
    services = FakeServices(build_volume({"a": File(b""), "b": File(b"")}))

    client(services).pull(REF, tmp_path / "out")

    assert (tmp_path / "out/a").read_bytes() == b"" and (
        tmp_path / "out/b"
    ).read_bytes() == b""
    assert len(services.s3_requests()) == 1
    assert EMPTY_DIGEST.startswith("b3:")


def test_list_files_and_resolve_download_only_the_manifest() -> None:
    services = FakeServices(build_volume(sample_tree()))
    volumes = client(services)

    resolved = volumes.resolve(REF)
    assert resolved.digest == services.volume.manifest_digest
    assert resolved.resolved_from is ResolvedFrom.TAG
    assert resolved.sequence == 7
    assert services.s3_requests() == []

    files = volumes.list_files(REF)
    assert {(f.path, f.kind, f.size, f.link_target) for f in files} == {
        ("adapter", EntryKind.DIRECTORY, 0, None),
        ("adapter/config.json", EntryKind.FILE, 10, None),
        ("adapter/empty.marker", EntryKind.FILE, 0, None),
        ("adapter/weights.bin", EntryKind.FILE, 16384, None),
        ("adapter/sub", EntryKind.DIRECTORY, 0, None),
        ("adapter/sub/note.txt", EntryKind.FILE, 20, None),
        ("adapter/latest", EntryKind.SYMLINK, 0, "weights.bin"),
        ("adapter/abs", EntryKind.SYMLINK, 0, "/adapter/config.json"),
        ("adapter/hardlink-a", EntryKind.FILE, 13, None),
        ("adapter/hardlink-b", EntryKind.FILE, 13, None),
    }
    assert len(services.s3_requests()) == 1
    assert len(services.token_requests()) == 1
    assert len(services.resolve_requests()) == 2


def test_expired_token_is_minted_again() -> None:
    services = FakeServices(build_volume({}), token_expires_in=dt.timedelta(minutes=4))
    volumes = client(services)
    volumes.resolve(REF)
    volumes.resolve(REF)
    assert len(services.token_requests()) == 2


@posix_only
def test_credentials_near_expiry_are_refreshed_by_pinned_digest(tmp_path: Path) -> None:
    services = FakeServices(
        build_volume(sample_tree()), credentials_expire_in=dt.timedelta(minutes=1)
    )

    client(services, max_concurrency=1).pull(REF, tmp_path / "out")

    refreshes = services.resolve_requests()[1:]
    assert refreshes, "the near-expiry credentials were resolved again"
    for request in refreshes:
        assert (
            request.url.params["ref"]
            == f"bdn://{NAMESPACE}/{VOLUME}@{services.volume.manifest_digest.removeprefix('b3:')}"
        )
    assert_sample_tree(tmp_path / "out")


@posix_only
def test_expired_token_from_the_bucket_forces_a_refresh_and_retry(
    tmp_path: Path,
) -> None:
    volume = build_volume({"a.bin": File(b"payload")})
    key = full_key(_s3.digest_of(b"payload"))
    services = FakeServices(
        volume, s3_failures={key: [(400, s3_error("ExpiredToken"))]}
    )

    client(services).pull(REF, tmp_path / "out")

    assert (tmp_path / "out/a.bin").read_bytes() == b"payload"
    assert len(services.resolve_requests()) == 2
    credentials_used = [
        r.headers["authorization"].split("Credential=")[1].split("/")[0]
        for r in services.s3_requests()
    ]
    assert credentials_used[-1] == "ASIA2"


@posix_only
def test_transient_s3_errors_are_retried(tmp_path: Path) -> None:
    volume = build_volume({"a.bin": File(b"payload")})
    key = full_key(_s3.digest_of(b"payload"))
    services = FakeServices(
        volume,
        s3_failures={
            key: [
                (503, s3_error("SlowDown")),
                (500, ""),
                (400, s3_error("RequestTimeout")),
            ]
        },
    )

    client(services).pull(REF, tmp_path / "out")

    assert (tmp_path / "out/a.bin").read_bytes() == b"payload"
    assert sum(1 for r in services.s3_requests() if r.url.path.lstrip("/") == key) == 4


@posix_only
def test_persistent_s3_error_surfaces_as_a_storage_error(tmp_path: Path) -> None:
    volume = build_volume({"a.bin": File(b"payload")})
    key = full_key(_s3.digest_of(b"payload"))
    services = FakeServices(
        volume, s3_failures={key: [(403, s3_error("AccessDenied"))]}
    )

    with pytest.raises(VolumeStorageError) as raised:
        client(services).pull(REF, tmp_path / "out")
    assert (raised.value.status_code, raised.value.code, raised.value.key) == (
        403,
        "AccessDenied",
        key,
    )
    assert sum(1 for r in services.s3_requests() if r.url.path.lstrip("/") == key) == 1
    assert not (tmp_path / "out").exists()


@posix_only
def test_corrupt_chunk_fails_the_pull_with_an_integrity_error(tmp_path: Path) -> None:
    volume = build_volume({"a.bin": File(b"correct bytes")})
    volume.objects[full_key(_s3.digest_of(b"correct bytes"))] = (
        b"tampered bytes",
        CHUNK,
    )

    with pytest.raises(VolumeIntegrityError, match="does not match the recorded"):
        client(FakeServices(volume)).pull(REF, tmp_path / "out")


@posix_only
def test_short_body_fails_the_content_length_check(tmp_path: Path) -> None:
    volume = build_volume({"a.bin": File(b"correct bytes")})
    services = FakeServices(volume)
    key = full_key(_s3.digest_of(b"correct bytes"))

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
        volumes.pull(REF, tmp_path / "out")


@posix_only
def test_nothing_is_written_when_the_manifest_escapes(tmp_path: Path) -> None:
    volume = build_volume({"ok.txt": File(b"fine")})
    volume.put_manifest(
        [
            manifest_header(2, 8),
            PROVENANCE,
            file_record("ok.txt", b"fine"),
            file_record("../evil.txt", b"fine"),
        ]
    )

    with pytest.raises(VolumePathError, match="escapes the volume root"):
        client(FakeServices(volume)).pull(REF, tmp_path / "out")
    assert list(tmp_path.iterdir()) == []


def test_transient_resolve_errors_are_retried() -> None:
    services = FakeServices(build_volume({}), resolve_failures=[503, 502])
    assert client(services).resolve(REF).digest == services.volume.manifest_digest
    assert len(services.resolve_requests()) == 3


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
    # 502 is retried before it surfaces.
    assert len(services.resolve_requests()) == _s3.ATTEMPTS


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
    with pytest.raises(VolumeConnectionError, match="bdn_endpoint_override"):
        volumes.resolve(REF)


def test_slabmap_volumes_are_unsupported_for_listing_and_pulling(
    tmp_path: Path,
) -> None:
    volume = Volume()
    volume.put_manifest(
        [
            manifest_header(1),
            {"_type": "file", "_kind": "slabmap", "mode": "0644", "path": "big.bin"},
        ]
    )
    services = FakeServices(volume)
    with pytest.raises(VolumeUnsupportedError, match="slabmap"):
        client(services).list_files(REF)
    if sys.platform != "win32":
        with pytest.raises(VolumeUnsupportedError, match="slabmap"):
            client(services).pull(REF, tmp_path / "out")
        assert not (tmp_path / "out").exists()


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
    budget = ByteBudget(10)
    budget.acquire(8)
    budget.release(8)
    budget.acquire(50)
    budget.release(50)
    with pytest.raises(ValueError):
        ByteBudget(0)
