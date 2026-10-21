"""Read-only checks against a real BDN volume; skipped unless the environment names one.

Set ``BASETEN_API_KEY`` and ``BASETEN_BDN_E2E_REF`` (a tag or digest ref to a
version holding at least one directory) to run them, and ``BASETEN_BASE_URL``
to point at an environment other than the public API.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from baseten.bdn.volumes import (
    NamespaceListing,
    Volume,
    VolumeClient,
    VolumeEntryKind,
    VolumeEntryListing,
    VolumeListing,
    VolumeRef,
    VolumeVersionDetail,
)

API_KEY = os.environ.get("BASETEN_API_KEY", "")
E2E_REF = os.environ.get("BASETEN_BDN_E2E_REF", "")

pytestmark = pytest.mark.skipif(
    not (API_KEY and E2E_REF),
    reason="set BASETEN_API_KEY and BASETEN_BDN_E2E_REF to run against a real volume",
)


@pytest.fixture(scope="module")
def volumes() -> Iterator[VolumeClient]:
    with VolumeClient(
        api_key=API_KEY, base_url_override=os.environ.get("BASETEN_BASE_URL")
    ) as client:
        yield client


@pytest.fixture(scope="module")
def ref() -> VolumeRef:
    parsed = VolumeRef.parse(E2E_REF).without_path()
    assert parsed.tag or parsed.digest, "BASETEN_BDN_E2E_REF must carry a tag or digest"
    return parsed


def test_namespace_and_volume_inventory(volumes: VolumeClient, ref: VolumeRef) -> None:
    namespaces = volumes.list()
    assert isinstance(namespaces, NamespaceListing)
    assert ref.namespace in {n.name for n in namespaces.items}

    listing = volumes.list(f"bdn:{ref.namespace}")
    assert isinstance(listing, VolumeListing)
    assert ref.volume in {v.name for v in listing.items}


def test_describe_volume_and_version_agree_with_the_manifest(
    volumes: VolumeClient, ref: VolumeRef
) -> None:
    volume = volumes.describe(f"bdn:{ref.namespace}/{ref.volume}")
    assert isinstance(volume, Volume) and volume.versions_alive >= 1

    version = volumes.describe(ref)
    assert isinstance(version, VolumeVersionDetail)
    manifest = volumes.fetch_manifest(version.version_ref)
    assert manifest.version_ref == version.version_ref

    history = volumes.list_versions(f"bdn:{ref.namespace}/{ref.volume}")
    assert version.digest in {v.digest for v in history.items}


def test_entry_listings_match_the_manifest(
    volumes: VolumeClient, ref: VolumeRef
) -> None:
    manifest = volumes.fetch_manifest(ref)
    flat = volumes.list(ref, recursive=True)
    assert isinstance(flat, VolumeEntryListing)
    assert flat.version_ref == manifest.version_ref
    assert [e.path for e in flat.items] == [e.path for e in manifest.entries]

    root = volumes.list(manifest.version_ref)
    assert isinstance(root, VolumeEntryListing)
    top = {"/" + e.path.split("/")[1] for e in manifest.entries}
    assert {e.path for e in root.items} == top

    directory = next(
        (e for e in root.items if e.kind is VolumeEntryKind.DIRECTORY), None
    )
    assert directory is not None, "BASETEN_BDN_E2E_REF must hold a directory"
    children = volumes.list(manifest.version_ref.with_path(directory.path))
    assert isinstance(children, VolumeEntryListing)
    assert all(e.path.startswith(directory.path + "/") for e in children.items)
