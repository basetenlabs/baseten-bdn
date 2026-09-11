# baseten-bdn

Python SDK for working with BDN inside and outside Baseten.

This package is under initial development. It installs as `baseten-bdn` and
imports as `baseten.bdn`. The `baseten` package is a namespace shared with
other Baseten SDKs such as `baseten` and `baseten-loops`.

## Installation

```
pip install baseten-bdn
```

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
