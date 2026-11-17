# baseten-bdn

Python SDK for working with BDN inside and outside Baseten.

:warning: SDK may change in incompatible ways between releases until the SDK
reaches 1.0.

This package is under initial development. It installs as `baseten-bdn` and
imports as `baseten.bdn`. The `baseten` package is a namespace shared with
other Baseten SDKs such as `baseten` and `baseten-loops`.

## Installation

```
pip install baseten-bdn
```

## Volumes

From any machine with a Baseten API key, `baseten.bdn.volumes` lists and
describes volumes, reads a version's manifest, and pulls a version into a local
directory. Everything it does is read-only. To read content, the client mints a
short-lived token through the Baseten API, resolves the ref, and reads the
volume's objects directly from storage with the credentials it is given,
verifying every object against its recorded digest.

```python
from baseten.bdn.volumes import VolumeClient

with VolumeClient(api_key="...") as volumes:
    result = volumes.pull("bdn:loops/sampler-abc123:step-100", "./checkpoint")

print(result.version_ref, result.file_count, result.bytes_written)
```

Refs are `bdn:<namespace>/<volume>` with an optional `:<tag>` or `@<digest>`
selector and an optional `/path` inside the version; `VolumeRef` parses and
renders them. A path on the ref, or `include=[...]`, narrows a pull to those
entries without moving them. A pull is staged next to `dest_dir` and renamed
into place once complete, so a failed pull leaves nothing behind; pass
`overwrite=True` to write into an existing directory in place. Pulling is
supported on Linux and macOS.

### Listing and describing

`list()` returns what a ref names, depending on how much of a ref it is: no ref
lists namespaces, a namespace lists its volumes, and a volume, version, or path
lists entries. Entry listings hold a directory's immediate children, or with
`recursive=True` everything beneath it, and carry the `version_ref` the head or
tag resolved to.

```python
with VolumeClient(api_key="...") as volumes:
    for namespace in volumes.list().items:
        print(namespace.name)

    for volume in volumes.list("bdn:loops").items:
        print(volume.ref, volume.head and volume.head.digest)

    listing = volumes.list("bdn:loops/sampler-abc123:step-100/adapter")
    print(listing.version_ref)  # bdn:loops/sampler-abc123@b3:…
    for entry in listing.items:
        print(entry.kind, entry.path, entry.size)
```

`describe()` returns a `Volume` for a volume ref and a `VolumeVersionDetail`
for a tag or digest ref; narrow on `kind`. Entry metadata comes from `list()`
or `fetch_manifest()`, so `describe()` refuses a path. `list_versions()` reads a
volume's history, newest first, and `include_tombstoned=True` adds deleted
versions that are still restorable.

```python
with VolumeClient(api_key="...") as volumes:
    version = volumes.describe("bdn:loops/sampler-abc123:step-100")
    print(version.kind, version.version_ref, version.total_size)

    for v in volumes.list_versions("bdn:loops/sampler-abc123").items:
        print(v.digest, v.lifecycle, v.tags)
```

## Hot Load

Inside a model pod that is opted in to BDN Hot Load, `baseten.bdn.hotload`
attaches BDN volumes below `/bdn/mounts` at runtime. An attach returns once the
directory is readable.

```python
from baseten.bdn.hotload import HotLoadClient

with HotLoadClient() as hotload:
    adapter = hotload.attach("bdn:adapters/sql-lora@b3:2547…", target="sql-lora")

load_adapter(adapter.path)  # /bdn/mounts/sql-lora
```

`AsyncHotLoadClient` offers the same methods as coroutines. Attachments belong
to the pod and stay mounted after the client is closed; call `detach` to free a
target name for reuse.

## Development

Requires [uv](https://docs.astral.sh/uv/).

```
uv sync
uv run poe lint
uv run poe typecheck
uv run poe test
```

Run `uv run poe format` to apply formatting and lint fixes.

`tests/test_volumes_e2e.py` reads a real volume and is skipped unless
`BASETEN_API_KEY` and `BASETEN_BDN_E2E_REF` are set. The ref must name a
version by tag or digest and hold at least one directory. Set
`BASETEN_BASE_URL` to test against an environment other than the public API.

## License

MIT. See [LICENSE](LICENSE).
