from __future__ import annotations

import pickle
from typing import Any

import httpx
import pytest

from baseten.bdn.hotload import (
    AsyncHotLoadClient,
    AttachmentState,
    HotLoadAPIError,
    HotLoadAttachError,
    HotLoadClient,
    HotLoadClientOptions,
    HotLoadConnectionError,
    HotLoadError,
    HotLoadProtocolError,
    HotLoadTimeoutError,
    VolumeAttachment,
)
from tests.conftest import FakeTransport, Reply

# Shapes follow hotloadd's `VolumeAttachment` serde output: `include` and
# `exclude` are omitted when empty, `state` is SCREAMING_SNAKE_CASE, and
# `pinned_source` always carries the full digest. Values are the staging smoke
# fixture volume.
SOURCE = "bdn:hotload-smoke/fixture"
PINNED = (
    "bdn:hotload-smoke/fixture@b3:"
    "2547205401ce7b172879a2c470f0f10e31248fe02ab250623a89faf66ed5722c"
)
ATTACHMENT_ID = "vol_01K4W8ZJ7P3Q9RXH2B6D5F8GTM"
READY_ATTACHMENT: dict[str, Any] = {
    "id": ATTACHMENT_ID,
    "revision": 2,
    "source": SOURCE,
    "target": "probe",
    "path": "/bdn/mounts/probe",
    "pinned_source": PINNED,
    "state": "READY",
}
FAILED_ATTACHMENT = {**READY_ATTACHMENT, "revision": 1, "state": "FAILED"}
FILTERED_ATTACHMENT = {
    **READY_ATTACHMENT,
    "include": ["*.json"],
    "exclude": ["config.json"],
}
UNAVAILABLE = Reply(
    503,
    {
        "code": "UNAVAILABLE",
        "message": "Hot Load manager is shutting down",
        "retryable": True,
    },
)
INVALID_SOURCE = Reply(
    400,
    {"code": "INVALID_SOURCE", "message": "expected a volume ref", "retryable": False},
)
NOT_FOUND = Reply(
    404,
    {
        "code": "NOT_FOUND",
        "message": 'Hot Load attachment "vol_missing" was not found',
        "retryable": False,
    },
)


def sync_client(fake: FakeTransport, **kwargs: Any) -> HotLoadClient:
    kwargs.setdefault("retry_interval_sec", 0)
    return HotLoadClient(http_client_override=fake.sync_client(), **kwargs)


def async_client(fake: FakeTransport, **kwargs: Any) -> AsyncHotLoadClient:
    kwargs.setdefault("retry_interval_sec", 0)
    return AsyncHotLoadClient(http_client_override=fake.async_client(), **kwargs)


def test_attach_posts_the_request_and_returns_the_ready_attachment() -> None:
    fake = FakeTransport([Reply(202, READY_ATTACHMENT)])

    attachment = sync_client(fake).attach(SOURCE, target="probe")

    assert attachment == VolumeAttachment.model_validate(READY_ATTACHMENT)
    assert attachment.state is AttachmentState.READY
    assert attachment.path == "/bdn/mounts/probe"
    assert attachment.include == ()
    (request,) = fake.requests
    assert request.method == "POST"
    assert request.path == "/v1/hotload/volumes"
    assert request.body == {"source": SOURCE, "target": "probe"}
    assert len(request.headers["idempotency-key"]) == 32


def test_attach_sends_filters_and_a_caller_supplied_key() -> None:
    fake = FakeTransport([Reply(202, FILTERED_ATTACHMENT)])

    attachment = sync_client(fake).attach(
        SOURCE,
        target="probe",
        include=["*.json"],
        exclude=["config.json"],
        idempotency_key="startup-probe",
    )

    assert attachment.include == ("*.json",)
    assert attachment.exclude == ("config.json",)
    (request,) = fake.requests
    assert request.body == {
        "source": SOURCE,
        "target": "probe",
        "include": ["*.json"],
        "exclude": ["config.json"],
    }
    assert request.headers["idempotency-key"] == "startup-probe"


@pytest.mark.parametrize("key", ["", "clé", "x" * 257, "tab\there"])
def test_attach_rejects_keys_the_daemon_would_refuse(key: str) -> None:
    fake = FakeTransport([Reply(202, READY_ATTACHMENT)])

    with pytest.raises(ValueError, match="idempotency_key"):
        sync_client(fake).attach(SOURCE, target="probe", idempotency_key=key)
    assert fake.requests == []


def test_attach_raises_when_the_attachment_is_failed() -> None:
    fake = FakeTransport([Reply(202, FAILED_ATTACHMENT)])

    with pytest.raises(HotLoadAttachError, match="detach it before retrying") as raised:
        sync_client(fake).attach(SOURCE, target="probe")

    assert raised.value.attachment.state is AttachmentState.FAILED
    assert raised.value.attachment.id == ATTACHMENT_ID


def test_attach_retries_retryable_errors_with_the_same_key() -> None:
    fake = FakeTransport([UNAVAILABLE, Reply(202, READY_ATTACHMENT)])

    sync_client(fake).attach(SOURCE, target="probe")

    first, second = fake.requests
    assert first.headers["idempotency-key"] == second.headers["idempotency-key"]
    assert first.body == second.body


def test_attach_gives_up_after_max_retries() -> None:
    fake = FakeTransport([UNAVAILABLE])

    with pytest.raises(HotLoadAPIError) as raised:
        sync_client(fake, max_retries=2).attach(SOURCE, target="probe")

    assert raised.value.status_code == 503
    assert raised.value.code == "UNAVAILABLE"
    assert raised.value.retryable is True
    assert len(fake.requests) == 3


def test_attach_does_not_retry_a_rejected_request() -> None:
    fake = FakeTransport([INVALID_SOURCE])

    with pytest.raises(HotLoadAPIError) as raised:
        sync_client(fake).attach("hf://models/weights", target="probe")

    assert raised.value.code == "INVALID_SOURCE"
    assert raised.value.retryable is False
    assert len(fake.requests) == 1


def test_attach_does_not_retry_after_a_read_timeout() -> None:
    fake = FakeTransport([Reply(error=httpx.ReadTimeout("slow bind"))])

    with pytest.raises(HotLoadTimeoutError, match="holds its target until detached"):
        sync_client(fake).attach(SOURCE, target="probe")

    assert len(fake.requests) == 1


def test_attach_does_not_retry_after_a_dropped_connection() -> None:
    fake = FakeTransport(
        [Reply(error=httpx.RemoteProtocolError("server disconnected"))]
    )

    with pytest.raises(HotLoadConnectionError, match="Inspect list_volumes") as raised:
        sync_client(fake).attach(SOURCE, target="probe")

    assert not isinstance(raised.value, HotLoadTimeoutError)
    assert len(fake.requests) == 1


def test_timeout_errors_are_not_connection_errors() -> None:
    # A caller falling back on "no socket, pod not opted in" must not swallow
    # a timed-out attach that may still hold its target.
    assert not issubclass(HotLoadTimeoutError, HotLoadConnectionError)
    assert issubclass(HotLoadTimeoutError, HotLoadError)


@pytest.mark.parametrize(
    "error",
    [httpx.ConnectError("no such socket"), httpx.PoolTimeout("pool exhausted")],
    ids=["connect-error", "pool-timeout"],
)
def test_requests_the_daemon_never_saw_retry_for_every_method(
    error: httpx.HTTPError,
) -> None:
    fake = FakeTransport([Reply(error=error)])

    with pytest.raises(HotLoadConnectionError, match="could not connect") as raised:
        sync_client(fake, max_retries=1).detach(ATTACHMENT_ID)

    assert not isinstance(raised.value, HotLoadTimeoutError)
    assert len(fake.requests) == 2


def test_get_retries_after_a_dropped_response_but_delete_does_not() -> None:
    dropped = Reply(error=httpx.ReadError("connection reset"))

    listing = FakeTransport([dropped, Reply(200, {"volumes": []})])
    assert sync_client(listing).list_volumes() == []
    assert len(listing.requests) == 2

    deleting = FakeTransport([dropped])
    with pytest.raises(HotLoadConnectionError):
        sync_client(deleting).detach(ATTACHMENT_ID)
    assert len(deleting.requests) == 1


def test_non_transport_httpx_errors_are_protocol_errors() -> None:
    fake = FakeTransport([Reply(error=httpx.DecodingError("bad content-encoding"))])

    with pytest.raises(HotLoadProtocolError, match="unusable response"):
        sync_client(fake).list_volumes()
    assert len(fake.requests) == 1


def test_list_volumes_parses_the_volumes_array() -> None:
    fake = FakeTransport(
        [Reply(200, {"volumes": [READY_ATTACHMENT, FAILED_ATTACHMENT]})]
    )

    volumes = sync_client(fake).list_volumes()

    assert [volume.state for volume in volumes] == [
        AttachmentState.READY,
        AttachmentState.FAILED,
    ]
    assert fake.requests[0].method == "GET"
    assert fake.requests[0].path == "/v1/hotload/volumes"


def test_list_volumes_requires_the_volumes_key() -> None:
    fake = FakeTransport([Reply(200, {"mounts": [READY_ATTACHMENT]})])

    with pytest.raises(HotLoadProtocolError, match="volumes"):
        sync_client(fake).list_volumes()


def test_get_volume_addresses_the_attachment_and_returns_failed_as_data() -> None:
    fake = FakeTransport([Reply(200, FAILED_ATTACHMENT)])

    attachment = sync_client(fake).get_volume(ATTACHMENT_ID)

    assert attachment.state is AttachmentState.FAILED
    assert fake.requests[0].path == f"/v1/hotload/volumes/{ATTACHMENT_ID}"


def test_unknown_attachment_fields_are_ignored() -> None:
    # The daemon ships in the node image and this package in the model image,
    # so an additive daemon field must not break existing callers.
    fake = FakeTransport([Reply(200, {**READY_ATTACHMENT, "view_id": "abc"})])

    assert sync_client(fake).get_volume(ATTACHMENT_ID).id == ATTACHMENT_ID


@pytest.mark.parametrize(
    "payload",
    [
        {**READY_ATTACHMENT, "state": "PREPARING"},
        {
            key: value
            for key, value in READY_ATTACHMENT.items()
            if key != "pinned_source"
        },
        {**READY_ATTACHMENT, "revision": -1},
        [READY_ATTACHMENT],
        None,
    ],
    ids=[
        "unknown-state",
        "missing-field",
        "negative-revision",
        "not-an-object",
        "empty-body",
    ],
)
def test_attachments_off_contract_are_protocol_errors(payload: Any) -> None:
    fake = FakeTransport([Reply(200, payload)])

    with pytest.raises(HotLoadProtocolError, match="VolumeAttachment"):
        sync_client(fake).get_volume(ATTACHMENT_ID)


def test_detach_returns_none_on_204() -> None:
    fake = FakeTransport([Reply(204)])

    assert sync_client(fake).detach(ATTACHMENT_ID) is None
    (request,) = fake.requests
    assert request.method == "DELETE"
    assert request.path == f"/v1/hotload/volumes/{ATTACHMENT_ID}"


def test_detach_surfaces_not_found() -> None:
    fake = FakeTransport([NOT_FOUND])

    with pytest.raises(HotLoadAPIError) as raised:
        sync_client(fake).detach("vol_missing")

    assert raised.value.status_code == 404
    assert raised.value.code == "NOT_FOUND"


@pytest.mark.parametrize(
    "attachment_id",
    ["", "vol_a/../vol_b", "..", ".", "vol_abc?x=1", "vol_abc#f", "vol abc"],
)
def test_attachment_ids_must_be_one_plain_path_segment(attachment_id: str) -> None:
    fake = FakeTransport([Reply(204)])

    with pytest.raises(ValueError, match="attachment_id"):
        sync_client(fake).detach(attachment_id)
    assert fake.requests == []


def test_error_bodies_off_contract_are_protocol_errors() -> None:
    fake = FakeTransport([Reply(502, {"error": "bad gateway"})])

    with pytest.raises(HotLoadProtocolError, match="HTTP 502"):
        sync_client(fake).list_volumes()


def test_error_bodies_with_extra_fields_keep_their_retry_hint() -> None:
    fake = FakeTransport(
        [
            Reply(503, {**UNAVAILABLE.body, "request_id": "r1"}),
            Reply(200, {"volumes": []}),
        ]
    )

    assert sync_client(fake).list_volumes() == []
    assert len(fake.requests) == 2


def test_api_errors_survive_pickling() -> None:
    error = pickle.loads(
        pickle.dumps(HotLoadAPIError(503, "UNAVAILABLE", "draining", True))
    )
    assert (error.status_code, error.code, error.message, error.retryable) == (
        503,
        "UNAVAILABLE",
        "draining",
        True,
    )
    assert "HTTP 503 (UNAVAILABLE)" in str(error)

    attach_error = pickle.loads(
        pickle.dumps(
            HotLoadAttachError(VolumeAttachment.model_validate(FAILED_ATTACHMENT))
        )
    )
    assert attach_error.attachment.id == ATTACHMENT_ID


def test_healthy_reports_the_socket_state_with_one_short_probe() -> None:
    up = FakeTransport([Reply(204)])
    assert sync_client(up).healthy() is True
    assert up.requests[0].path == "/healthz"

    down = FakeTransport([Reply(error=httpx.ConnectError("refused"))])
    assert sync_client(down).healthy() is False
    assert len(down.requests) == 1

    draining = FakeTransport([UNAVAILABLE])
    assert sync_client(draining).healthy() is False
    assert len(draining.requests) == 1

    garbage = FakeTransport([Reply(502, {"error": "bad gateway"})])
    assert sync_client(garbage).healthy() is False


def test_reads_use_a_short_timeout_and_mutations_the_request_timeout() -> None:
    seen: list[httpx.Timeout] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(httpx.Timeout(**request.extensions["timeout"]))
        return httpx.Response(200, json={"volumes": []})

    http_client = httpx.Client(
        transport=httpx.MockTransport(handler), base_url="http://bdn.local"
    )
    client = HotLoadClient(http_client_override=http_client, request_timeout_sec=900)

    client.list_volumes()
    client.healthy()
    with pytest.raises(HotLoadProtocolError):
        client.attach(SOURCE, target="probe")

    listing, health, attach = seen
    assert listing.read == health.read == 10.0
    assert attach.read == 900.0
    assert {timeout.connect for timeout in seen} == {5.0}


@pytest.mark.parametrize(
    "kwargs",
    [{"request_timeout_sec": 0}, {"max_retries": -1}, {"retry_interval_sec": -0.5}],
)
def test_options_reject_nonsense_values(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        HotLoadClientOptions(**kwargs)


def test_default_client_targets_the_pod_socket() -> None:
    client = HotLoadClient()
    try:
        assert client.options.socket_path.as_posix() == "/bdn/hotload.sock"
        assert client.options.request_timeout_sec == 600.0
        assert client.close_http_client_on_close is True
        assert client.http_client.headers["user-agent"].startswith("baseten-bdn/")
        assert client.http_client.timeout.connect == 5.0
    finally:
        client.close()


def test_override_client_is_left_open_by_default() -> None:
    fake = FakeTransport([Reply(204)])
    http_client = fake.sync_client()

    with HotLoadClient(http_client_override=http_client) as client:
        assert client.close_http_client_on_close is False
    assert not http_client.is_closed


async def test_async_attach_and_detach() -> None:
    fake = FakeTransport([Reply(202, READY_ATTACHMENT), Reply(204)])

    async with async_client(fake) as client:
        attachment = await client.attach(SOURCE, target="probe", idempotency_key="k")
        await client.detach(attachment.id)

    assert attachment.pinned_source == PINNED
    assert [request.method for request in fake.requests] == ["POST", "DELETE"]
    assert fake.requests[0].headers["idempotency-key"] == "k"


async def test_async_attach_retries_retryable_errors_with_the_same_key() -> None:
    fake = FakeTransport([UNAVAILABLE, Reply(202, READY_ATTACHMENT)])

    await async_client(fake).attach(SOURCE, target="probe")

    first, second = fake.requests
    assert first.headers["idempotency-key"] == second.headers["idempotency-key"]


async def test_async_attach_does_not_retry_after_a_read_timeout() -> None:
    fake = FakeTransport([Reply(error=httpx.ReadTimeout("slow bind"))])

    with pytest.raises(HotLoadTimeoutError):
        await async_client(fake).attach(SOURCE, target="probe")

    assert len(fake.requests) == 1


async def test_async_list_and_health() -> None:
    fake = FakeTransport([Reply(200, {"volumes": [READY_ATTACHMENT]}), UNAVAILABLE])
    client = async_client(fake)

    assert [volume.id for volume in await client.list_volumes()] == [ATTACHMENT_ID]
    assert await client.healthy() is False
    assert len(fake.requests) == 2
    await client.close()
