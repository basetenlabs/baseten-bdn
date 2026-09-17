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
    VolumeAPIError,
    VolumeClient,
    VolumeClientOptions,
    VolumeConnectionError,
    VolumeDestinationError,
    VolumeEntryKind,
    VolumeError,
    VolumeIntegrityError,
    VolumePathError,
    VolumeProtocolError,
    VolumeRef,
    VolumeRefError,
    VolumeRefLevel,
    VolumeStorageError,
    VolumeUnsupportedError,
    _client,
    _manifest,
    _s3,
)
from baseten.bdn.volumes._cannery import OriginCredentials
from baseten.bdn.volumes._materialize import ByteBudget
from tests.volume_fixtures import (
    API_KEY,
    CANNERY_TOKEN,
    CHUNK,
    MTIME,
    MTIME_NS,
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
posix_only = pytest.mark.skipif(sys.platform == "win32", reason="pull is POSIX only")


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


def pinned(services: FakeServices) -> VolumeRef:
    return VolumeRef(
        namespace=NAMESPACE, volume=VOLUME, digest=services.volume.manifest_digest
    )


# --- refs -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected", "canonical", "level"),
    [
        ("bdn:loops", VolumeRef("loops"), "bdn:loops/", VolumeRefLevel.NAMESPACE),
        ("bdn:loops/", VolumeRef("loops"), "bdn:loops/", VolumeRefLevel.NAMESPACE),
        (
            "bdn:loops/sampler-abc",
            VolumeRef("loops", "sampler-abc"),
            "bdn:loops/sampler-abc",
            VolumeRefLevel.VOLUME,
        ),
        (
            "bdn:Loops/Sampler-ABC:Step_1.0",
            VolumeRef("loops", "sampler-abc", tag="Step_1.0"),
            "bdn:loops/sampler-abc:Step_1.0",
            VolumeRefLevel.POINT,
        ),
        (
            "bdn:loops/vol@B3:ABCDEF012345",
            VolumeRef("loops", "vol", digest="b3:abcdef012345"),
            "bdn:loops/vol@b3:abcdef012345",
            VolumeRefLevel.POINT,
        ),
        (
            "bdn:loops/vol@abcdef",
            VolumeRef("loops", "vol", digest="abcdef"),
            "bdn:loops/vol@abcdef",
            VolumeRefLevel.POINT,
        ),
        (
            "bdn:loops/vol/",
            VolumeRef("loops", "vol", path="/"),
            "bdn:loops/vol/",
            VolumeRefLevel.PATH,
        ),
        (
            "bdn:loops/vol:tag/config/x.json",
            VolumeRef("loops", "vol", tag="tag", path="/config/x.json"),
            "bdn:loops/vol:tag/config/x.json",
            VolumeRefLevel.PATH,
        ),
        (
            "bdn:loops/vol/a%20b/c/",
            VolumeRef("loops", "vol", path="/a b/c"),
            "bdn:loops/vol/a%20b/c",
            VolumeRefLevel.PATH,
        ),
    ],
)
def test_volume_ref_parses_and_renders_the_shared_grammar(
    text: str, expected: VolumeRef, canonical: str, level: VolumeRefLevel
) -> None:
    parsed = VolumeRef.parse(text)
    assert parsed == expected
    assert str(parsed) == canonical
    assert parsed.level is level
    assert VolumeRef.parse(canonical) == parsed


@pytest.mark.parametrize(
    ("text", "match"),
    [
        ("loops/vol", "begins with"),
        ("bdn://loops/vol", "bdn:// is not supported"),
        ("bdn+obj://loops/manifest@b3:00", "object refs"),
        ("bdn:loops:tag/vol", "takes no selector"),
        ("bdn:l/vol", "2 to 256"),
        ("bdn:1oops/vol", "begin with a letter"),
        ("bdn:lo_ops/vol", "letters, digits, and hyphens"),
        ("bdn:resolve/vol", "reserved"),
        ("bdn:loops/objects", "reserved"),
        ("bdn:loops/vol:", "no tag"),
        ("bdn:loops/vol:-bad", "tag"),
        ("bdn:loops/vol:a:b", "tag"),
        ("bdn:loops/vol@", "no digest"),
        ("bdn:loops/vol@b3:", "no digest"),
        ("bdn:loops/vol@xyz", "not hex"),
        ("bdn:loops/vol@" + "a" * 65, "longer than 64"),
        ("bdn:loops/vol/a//b", "empty segment"),
        ("bdn:loops/vol/a/../b", "not allowed"),
        ("bdn:loops/vol/%2e%2e", "not allowed"),
        ("bdn:loops/vol/a%2Fb", "decodes to a slash"),
    ],
)
def test_volume_ref_rejects_malformed_refs(text: str, match: str) -> None:
    with pytest.raises(VolumeRefError, match=match) as raised:
        VolumeRef.parse(text)
    assert isinstance(raised.value, VolumeError) and isinstance(
        raised.value, ValueError
    )


def test_volume_ref_constructor_checks_consistency() -> None:
    with pytest.raises(VolumeRefError, match="both a tag and a digest"):
        VolumeRef("loops", "vol", tag="t", digest="ab")
    with pytest.raises(VolumeRefError, match="no volume"):
        VolumeRef("loops", path="/x")


def test_volume_ref_pinned_shorthand_and_paths() -> None:
    full = "b3:" + "ab" * 32
    ref = VolumeRef.parse("bdn:loops/vol:tag/config")
    assert ref.pinned(full) == VolumeRef("loops", "vol", digest=full)
    assert ref.pinned("AB" * 32).digest == full
    with pytest.raises(VolumeRefError, match="full"):
        ref.pinned("abcdef")
    assert ref.pinned(full).shorthand() == "bdn:loops/vol@" + "ab" * 6
    assert ref.shorthand() == str(ref)
    assert ref.without_path() == VolumeRef("loops", "vol", tag="tag")
    assert ref.with_path("/weights/model.bin").path == "/weights/model.bin"
    assert ref.with_path("/").path == "/"


# --- SigV4 and object URLs --------------------------------------------------


def test_sigv4_matches_the_published_aws_vector() -> None:
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
    when = dt.datetime(2026, 9, 15, tzinfo=dt.UTC)
    headers = _s3.sigv4_headers(
        method="GET", url=url, headers={}, credentials=credentials, now=when
    )
    assert headers["x-amz-security-token"] == "tok"
    assert (
        httpx.Client().build_request("GET", url, headers=headers).headers["host"]
        == "minio.test"
    )
    resigned = _s3.sigv4_headers(
        method="GET",
        url="https://minio.test/b/k",
        headers={},
        credentials=credentials,
        now=when,
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
    manifest = _manifest.parse_manifest(
        jsonl(
            [
                file_record("a/x", b"x", mtime=MTIME),
                PROVENANCE,
                {"_type": "directory", "mode": "0755", "path": "a"},
                manifest_header(2, 1),
            ]
        )
    )
    assert [entry.clean_path for entry in manifest.entries] == ["a/x", "a"]
    assert manifest.entries[0].mtime_ns == MTIME_NS
    assert manifest.entries[0].mtime_datetime == dt.datetime(
        2026, 9, 15, 12, 34, 56, 123456, tzinfo=dt.UTC
    )
    public = manifest.public_entries()
    assert [e.path for e in public] == ["/a", "/a/x"], "path order, slash-prefixed"
    assert public[1].mtime == manifest.entries[0].mtime_datetime


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
        ([manifest_header(1), file_record("a", b"x", mtime="yesterday")], "RFC 3339"),
        ([manifest_header(1), manifest_header(1)], "two headers"),
    ],
    ids=[
        "no-header",
        "unknown-type",
        "missing-chunk",
        "chunk-offset",
        "bad-mtime",
        "two-headers",
    ],
)
def test_parse_manifest_rejects_off_contract_documents(
    lines: list[dict[str, Any]], match: str
) -> None:
    with pytest.raises(VolumeProtocolError, match=match):
        _manifest.parse_manifest(jsonl(lines))


def test_slabmap_records_are_unsupported_not_malformed() -> None:
    with pytest.raises(VolumeUnsupportedError, match="slabmap"):
        _manifest.parse_manifest(
            jsonl(
                [
                    manifest_header(1),
                    {
                        "_type": "file",
                        "_kind": "slabmap",
                        "mode": "0644",
                        "path": "big.bin",
                    },
                ]
            )
        )


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


# --- containment and selection ----------------------------------------------


def symlink(path: str, target: str) -> dict[str, Any]:
    return {"_type": "symlink", "mode": "0777", "path": path, "target": target}


def directory(path: str, mode: str = "0755") -> dict[str, Any]:
    return {"_type": "directory", "mode": mode, "path": path}


@pytest.mark.parametrize(
    ("records", "match"),
    [
        ([file_record("../escape", b"x")], "escapes the volume root"),
        ([file_record("a/./b", b"x")], "not normalized"),
        ([file_record("a", b"x"), file_record("a", b"y")], "appears twice"),
        (
            [file_record("f", b"x"), file_record("f/child", b"y")],
            "nested beneath the non-directory",
        ),
        ([symlink("l", "../../etc/passwd")], "escapes the volume root"),
        ([symlink("l", "")], "empty target"),
        ([symlink("a", "b"), symlink("b", "a")], "longer than 40 hops"),
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
            symlink("top", "/models/a"),
            symlink("models/a/root", "/"),
            symlink("rootlink", "/"),
        )
    )

    def rendered(path: str) -> str:
        entry = contained.by_path[path]
        assert isinstance(entry, _manifest.SymlinkEntry)
        return contained.rendered_symlink_target(entry)

    assert rendered("models/a/current") == "../b/weights.bin"
    assert rendered("top") == "models/a"
    assert rendered("models/a/root") == "../.."
    assert rendered("rootlink") == "."


def test_select_paths_matches_on_slash_boundaries_and_keeps_recorded_ancestors() -> (
    None
):
    tree = entries(
        directory("a", "0555"),
        directory("a/b"),
        file_record("a/b/x", b"1"),
        file_record("a/bb", b"2"),
        file_record("c", b"3"),
    )
    assert _manifest.select_paths(tree, []) is None
    assert _manifest.select_paths(tree, ["/"]) is None
    assert _manifest.select_paths(tree, ["a/b"]) == {"a", "a/b", "a/b/x"}
    assert _manifest.select_paths(tree, ["/a/b/", "c"]) == {"a", "a/b", "a/b/x", "c"}
    with pytest.raises(VolumePathError, match="matches no entry"):
        _manifest.select_paths(tree, ["a/nope"])


# --- pull -------------------------------------------------------------------


def sample_tree() -> dict[str, File | Dir | Symlink]:
    big = bytes(range(256)) * 64  # 16 KiB, split into 6 chunks below
    return {
        "adapter": Dir(mtime=MTIME),
        "adapter/config.json": File(b'{"r": 16}\n', compress=True, mtime=MTIME),
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
# The manifest total counts every file record, both hardlink members included.
SAMPLE_TOTAL_SIZE = SAMPLE_BYTES + 13
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
    assert (dest / "adapter/hardlink-a").stat().st_ino == (
        dest / "adapter/hardlink-b"
    ).stat().st_ino
    # mtime restored on the file and on the directory, after its children were created.
    assert (dest / "adapter/config.json").stat().st_mtime_ns == MTIME_NS
    assert (dest / "adapter").stat().st_mtime_ns == MTIME_NS


@posix_only
def test_pull_materializes_the_tree_and_talks_to_all_three_services(
    tmp_path: Path,
) -> None:
    services = FakeServices(build_volume(sample_tree()))
    dest = tmp_path / "ckpt"

    result = services.client().pull(REF, dest)

    assert_sample_tree(dest)
    assert result.version_ref == pinned(services)
    assert (
        str(result.version_ref)
        == f"bdn:{NAMESPACE}/{VOLUME}@{services.volume.manifest_digest}"
    )
    assert (result.file_count, result.selected_file_count, result.total_file_count) == (
        6,
        6,
        6,
    )
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
    assert resolve_request.url.params["ref"] == f"bdn:{NAMESPACE}/{VOLUME}:step-100"
    s3 = services.s3_requests()
    assert len(s3) == SAMPLE_OBJECT_GETS
    for request in s3:
        assert request.headers["authorization"].startswith(
            "AWS4-HMAC-SHA256 Credential=ASIA1/"
        )
        assert request.headers["x-amz-security-token"] == "sts-session-token"
        assert request.url.path.startswith(f"/bdn/{ORG_ID}/{NAMESPACE}/objects/b3/")


@posix_only
def test_pull_narrows_by_ref_path_and_include_without_moving_entries(
    tmp_path: Path,
) -> None:
    services = FakeServices(build_volume(sample_tree()))

    result = services.client().pull(
        f"{REF}/adapter/sub", tmp_path / "out", include=["adapter/config.json"]
    )

    assert sorted(
        p.relative_to(tmp_path / "out").as_posix()
        for p in (tmp_path / "out").rglob("*")
    ) == [
        "adapter",
        "adapter/config.json",
        "adapter/sub",
        "adapter/sub/note.txt",
    ]
    assert stat.S_IMODE((tmp_path / "out/adapter/sub").stat().st_mode) == 0o555, (
        "recorded mode of the narrowing's ancestor"
    )
    assert (result.file_count, result.selected_file_count, result.total_file_count) == (
        2,
        2,
        6,
    )
    assert result.version_ref.path is None, "the version ref carries no path"
    assert len(services.s3_requests()) == 1 + 1 + 1
    with pytest.raises(VolumePathError, match="matches no entry"):
        services.client().pull(REF, tmp_path / "out2", include=["adapter/missing"])
    assert not (tmp_path / "out2").exists()


@posix_only
def test_pull_refuses_a_non_empty_destination_unless_overwrite(tmp_path: Path) -> None:
    services = FakeServices(build_volume(sample_tree()))
    dest = tmp_path / "ckpt"
    dest.mkdir()
    (dest / "stale.txt").write_bytes(b"old")

    with pytest.raises(VolumeDestinationError, match="not empty"):
        services.client().pull(REF, dest)
    assert services.requests == [], "refused before any request"

    services.client().pull(REF, dest, overwrite=True)
    assert_sample_tree(dest)
    assert (dest / "stale.txt").read_bytes() == b"old", (
        "files the volume does not describe are left alone"
    )


@posix_only
def test_pull_into_an_empty_existing_directory_is_still_staged(tmp_path: Path) -> None:
    services = FakeServices(build_volume(sample_tree()))
    dest = tmp_path / "ckpt"
    dest.mkdir()

    services.client().pull(REF, dest)

    assert_sample_tree(dest)
    assert [p.name for p in tmp_path.iterdir()] == ["ckpt"]


@posix_only
def test_overwrite_pull_replaces_hardlinks_and_survives_read_only_dirs(
    tmp_path: Path,
) -> None:
    dest = tmp_path / "ckpt"
    FakeServices(build_volume(sample_tree())).client().pull(REF, dest)

    changed = sample_tree()
    changed["adapter/hardlink-a"] = File(b"AAAA")
    changed["adapter/hardlink-b"] = File(b"BBBB")
    changed["adapter/sub/new.txt"] = File(b"new child in a 0555 dir\n")
    services = FakeServices(build_volume(changed))

    result = services.client().pull(REF, dest, overwrite=True)

    assert (dest / "adapter/hardlink-a").read_bytes() == b"AAAA"
    assert (dest / "adapter/hardlink-b").read_bytes() == b"BBBB"
    assert (dest / "adapter/hardlink-a").stat().st_nlink == 1
    assert (dest / "adapter/sub/new.txt").read_bytes() == b"new child in a 0555 dir\n"
    assert stat.S_IMODE((dest / "adapter/sub").stat().st_mode) == 0o555
    assert result.file_count == 7


@posix_only
def test_overwrite_pull_replaces_stale_symlinks_instead_of_following_them(
    tmp_path: Path,
) -> None:
    services = FakeServices(build_volume(sample_tree()))
    dest = tmp_path / "ckpt"
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"do not touch")
    (dest / "adapter").mkdir(parents=True)
    os.symlink(outside, dest / "adapter/config.json")
    os.symlink(tmp_path / "elsewhere", dest / "adapter/sub")

    services.client().pull(REF, dest, overwrite=True)

    assert outside.read_bytes() == b"do not touch"
    assert not (dest / "adapter/config.json").is_symlink()
    assert (dest / "adapter/sub").is_dir() and not (dest / "adapter/sub").is_symlink()
    assert_sample_tree(dest)


@posix_only
def test_failed_pull_into_a_new_directory_leaves_nothing(tmp_path: Path) -> None:
    volume = build_volume(sample_tree())
    volume.objects[full_key(_s3.digest_of(b"read-only dir child\n"))] = (
        b"tampered",
        CHUNK,
    )

    with pytest.raises(VolumeIntegrityError):
        FakeServices(volume).client().pull(REF, tmp_path / "ckpt")

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
        services.client().pull(REF, dest, overwrite=True)


@posix_only
def test_pull_refuses_when_the_disk_is_too_small(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    services = FakeServices(build_volume(sample_tree()))
    monkeypatch.setattr(
        _client.shutil, "disk_usage", lambda path: shutil._ntuple_diskusage(100, 90, 10)
    )

    with pytest.raises(VolumeDestinationError, match="nothing was written"):
        services.client().pull(REF, tmp_path / "ckpt")
    assert not (tmp_path / "ckpt").exists()
    assert len(services.s3_requests()) == 1, "only the manifest was read"


@posix_only
def test_empty_files_cost_no_object_read(tmp_path: Path) -> None:
    services = FakeServices(build_volume({"a": File(b""), "b": File(b"")}))
    services.client().pull(REF, tmp_path / "out")
    assert (tmp_path / "out/a").read_bytes() == b"" and (
        tmp_path / "out/b"
    ).read_bytes() == b""
    assert len(services.s3_requests()) == 1


def test_resolve_returns_the_pinned_ref_and_fetch_manifest_reads_only_the_manifest() -> (
    None
):
    services = FakeServices(build_volume(sample_tree()))
    volumes = services.client()

    resolved = volumes.resolve(REF)
    assert resolved == pinned(services)
    assert resolved.level is VolumeRefLevel.POINT
    assert services.s3_requests() == []

    manifest = volumes.fetch_manifest(REF)
    assert manifest.version_ref == pinned(services)
    assert (manifest.entry_count, manifest.total_size) == (10, SAMPLE_TOTAL_SIZE)
    assert [e.path for e in manifest.entries][:4] == [
        "/adapter",
        "/adapter/abs",
        "/adapter/config.json",
        "/adapter/empty.marker",
    ]
    by_path = {e.path: e for e in manifest.entries}
    assert (
        by_path["/adapter/latest"].kind is VolumeEntryKind.SYMLINK
        and by_path["/adapter/latest"].link_target == "weights.bin"
    )
    assert by_path["/adapter/weights.bin"].size == 16384
    assert by_path["/adapter/config.json"].mtime == dt.datetime(
        2026, 9, 15, 12, 34, 56, 123456, tzinfo=dt.UTC
    )
    assert len(services.s3_requests()) == 1
    assert len(services.token_requests()) == 1, "one token covers both calls"

    narrowed = volumes.fetch_manifest(f"{REF}/adapter/sub")
    assert [e.path for e in narrowed.entries] == [
        "/adapter",
        "/adapter/sub",
        "/adapter/sub/note.txt",
    ]
    assert (narrowed.entry_count, narrowed.total_size) == (10, SAMPLE_TOTAL_SIZE), (
        "counts describe the whole version"
    )


def test_namespace_refs_are_refused_for_volume_operations() -> None:
    with pytest.raises(VolumeRefError, match="names a namespace"):
        FakeServices(build_volume({})).client().resolve("bdn:loops")


def test_expired_token_is_minted_again() -> None:
    services = FakeServices(build_volume({}), token_expires_in=dt.timedelta(minutes=4))
    volumes = services.client()
    volumes.resolve(REF)
    volumes.resolve(REF)
    assert len(services.token_requests()) == 2


def test_token_mint_retries_transient_errors_and_reports_rejections() -> None:
    flaky = FakeServices(build_volume({}), token_failures=[503, 502])
    flaky.client().resolve(REF)
    assert len(flaky.token_requests()) == 3

    forbidden = FakeServices(
        build_volume({}),
        token_error=(403, {"code": "FORBIDDEN", "message": "volumes are not enabled"}),
    )
    with pytest.raises(VolumeAPIError) as raised:
        forbidden.client().resolve(REF)
    assert (raised.value.service, raised.value.status_code) == ("Baseten API", 403)
    assert "volumes are not enabled" in raised.value.message
    assert forbidden.resolve_requests() == []


@posix_only
def test_credentials_near_expiry_are_refreshed_by_pinned_digest(tmp_path: Path) -> None:
    services = FakeServices(
        build_volume(sample_tree()), credentials_expire_in=dt.timedelta(minutes=1)
    )

    services.client(max_concurrency=1).pull(REF, tmp_path / "out")

    refreshes = services.resolve_requests()[1:]
    assert refreshes
    for request in refreshes:
        assert (
            request.url.params["ref"]
            == f"bdn:{NAMESPACE}/{VOLUME}@{services.volume.manifest_digest}"
        )
    assert_sample_tree(tmp_path / "out")


@posix_only
def test_expired_token_from_the_bucket_forces_a_refresh_and_retry(
    tmp_path: Path,
) -> None:
    key = full_key(_s3.digest_of(b"payload"))
    services = FakeServices(
        build_volume({"a.bin": File(b"payload")}),
        s3_failures={key: [(400, s3_error("ExpiredToken"))]},
    )

    services.client().pull(REF, tmp_path / "out")

    assert (tmp_path / "out/a.bin").read_bytes() == b"payload"
    assert len(services.resolve_requests()) == 2
    assert (
        services.s3_requests()[-1]
        .headers["authorization"]
        .startswith("AWS4-HMAC-SHA256 Credential=ASIA2/")
    )


@posix_only
def test_transient_s3_errors_are_retried(tmp_path: Path) -> None:
    key = full_key(_s3.digest_of(b"payload"))
    services = FakeServices(
        build_volume({"a.bin": File(b"payload")}),
        s3_failures={
            key: [
                (503, s3_error("SlowDown")),
                (500, ""),
                (400, s3_error("RequestTimeout")),
            ]
        },
    )

    services.client().pull(REF, tmp_path / "out")

    assert (tmp_path / "out/a.bin").read_bytes() == b"payload"
    assert sum(1 for r in services.s3_requests() if r.url.path.lstrip("/") == key) == 4


@posix_only
def test_persistent_s3_error_surfaces_as_a_storage_error(tmp_path: Path) -> None:
    key = full_key(_s3.digest_of(b"payload"))
    services = FakeServices(
        build_volume({"a.bin": File(b"payload")}),
        s3_failures={key: [(403, s3_error("AccessDenied"))]},
    )

    with pytest.raises(VolumeStorageError) as raised:
        services.client().pull(REF, tmp_path / "out")
    assert (raised.value.status_code, raised.value.code, raised.value.key) == (
        403,
        "AccessDenied",
        key,
    )
    assert not (tmp_path / "out").exists()


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

    volumes = VolumeClient(
        api_key=API_KEY,
        http_client_override=httpx.Client(transport=httpx.MockTransport(truncating)),
        management_client_override=services.management_client(),
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
        FakeServices(volume).client().pull(REF, tmp_path / "out")
    assert list(tmp_path.iterdir()) == []


def test_transient_resolve_errors_are_retried() -> None:
    services = FakeServices(build_volume({}), resolve_failures=[503, 502])
    assert services.client().resolve(REF) == pinned(services)
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
        services.client().resolve(REF)
    assert (raised.value.service, raised.value.status_code, raised.value.reason) == (
        "cannery",
        404,
        "NOT_FOUND",
    )


def test_non_envelope_cannery_errors_keep_the_body() -> None:
    services = FakeServices(
        build_volume({}), resolve_error=(502, "<html>bad gateway</html>")
    )
    with pytest.raises(VolumeAPIError) as raised:
        services.client().resolve(REF)
    assert (raised.value.service, raised.value.status_code) == ("cannery", 502)
    assert "bad gateway" in raised.value.message
    assert len(services.resolve_requests()) == _s3.ATTEMPTS, (
        "502 is retried before it surfaces"
    )


def test_missing_bdn_endpoint_is_an_error_unless_overridden() -> None:
    services = FakeServices(build_volume({}), bdn_endpoint=None)
    with pytest.raises(VolumeConnectionError, match="bdn_endpoint_override"):
        services.client().resolve(REF)
    assert services.client(bdn_endpoint_override="https://bdn.test").resolve(
        REF
    ) == pinned(services)


def test_slabmap_volumes_are_unsupported(tmp_path: Path) -> None:
    volume = Volume()
    volume.put_manifest(
        [
            manifest_header(1),
            {"_type": "file", "_kind": "slabmap", "mode": "0644", "path": "big.bin"},
        ]
    )
    services = FakeServices(volume)
    with pytest.raises(VolumeUnsupportedError, match="slabmap"):
        services.client().fetch_manifest(REF)


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
        VolumeClientOptions(**kwargs)


def test_default_client_targets_the_public_api() -> None:
    volumes = VolumeClient(api_key="k")
    try:
        assert volumes.options.base_url == "https://api.baseten.co"
        assert volumes.management_client.options.base_url == "https://api.baseten.co"
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
