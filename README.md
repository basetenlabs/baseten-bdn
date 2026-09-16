# baseten-bdn

Python SDK for working with BDN inside and outside Baseten.

This package is under initial development. It installs as `baseten-bdn` and
imports as `baseten.bdn`. The `baseten` package is a namespace shared with
other Baseten SDKs such as `baseten` and `baseten-loops`.

## Installation

```
pip install baseten-bdn
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

## Volumes

From any machine with a Baseten API key, `baseten.bdn.volumes` resolves a
volume ref, reads a version's manifest, or pulls a version into a local
directory. The client mints a short-lived token through the Baseten API,
resolves the ref, and reads the volume's objects directly from storage with
the credentials it is given, verifying every object against its recorded
digest.

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

## Development

Requires [uv](https://docs.astral.sh/uv/).

```
uv sync
uv run poe lint
uv run poe typecheck
uv run poe test
```

Run `uv run poe format` to apply formatting and lint fixes.

## License

MIT. See [LICENSE](LICENSE).
