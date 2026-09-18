from __future__ import annotations

import functools
from importlib.metadata import PackageNotFoundError, version


@functools.cache
def user_agent() -> str:
    try:
        return f"baseten-bdn/{version('baseten-bdn')}"
    except PackageNotFoundError:
        return "baseten-bdn/unknown"
