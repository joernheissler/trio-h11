import h11
import pytest
import trio
import trio.testing

from trio_h11 import TrioH11Stream


async def test_roundtrip() -> None:
    client_stream, server_stream = trio.testing.memory_stream_pair()
    client = TrioH11Stream(client_stream, is_server=False)
    server = TrioH11Stream(server_stream, is_server=True)

    await client.send(h11.Request(method="GET", target="/", headers=[("Host", "x")]))
    await client.send(h11.EndOfMessage())
    request = await server.recv()
    assert isinstance(request, h11.Request)
    assert request.target == b"/"
    assert isinstance(await server.recv(), h11.EndOfMessage)

    await server.send(h11.Response(status_code=200, headers=[("Content-Length", "2")]))
    await server.send(h11.Data(data=b"ok"))
    await server.send(h11.EndOfMessage())
    events: list[h11.Event | type[h11.PAUSED]] = []
    async for event in client:
        events.append(event)
        if isinstance(event, h11.EndOfMessage):
            break

    response = events[0]
    assert isinstance(response, h11.Response)
    assert response.status_code == 200
    assert isinstance(events[1], h11.Data)
    assert isinstance(events[2], h11.EndOfMessage)

    await client.aclose()
    await server.aclose()


async def test_aclose_half_closes_stream() -> None:
    stream, peer = trio.testing.memory_stream_pair()
    adapter = TrioH11Stream(stream, is_server=False)

    await adapter.aclose()

    assert adapter.finalized
    assert await peer.receive_some(1) == b""
    await peer.aclose()


class NoHalfCloseStream(trio.abc.Stream):
    def __init__(self, inner: trio.abc.Stream) -> None:
        self.inner = inner

    async def send_all(self, data: bytes) -> None:
        await self.inner.send_all(data)

    async def wait_send_all_might_not_block(self) -> None:
        await self.inner.wait_send_all_might_not_block()

    async def receive_some(self, max_bytes: int | None = None) -> bytes | bytearray:
        return await self.inner.receive_some(max_bytes)

    async def aclose(self) -> None:
        await self.inner.aclose()


async def test_properties_and_aiter() -> None:
    stream, peer = trio.testing.memory_stream_pair()
    client = TrioH11Stream(stream, is_server=False)
    server = TrioH11Stream(peer, is_server=True)

    assert client.__aiter__() is client
    assert client.our_role is h11.CLIENT
    assert client.their_role is h11.SERVER
    assert server.our_role is h11.SERVER
    assert client.our_state is h11.IDLE
    assert client.their_state is h11.IDLE
    assert client.their_http_version is None
    assert not client.client_is_waiting_for_100_continue
    assert not client.they_are_waiting_for_100_continue
    assert client.trailing_data == (b"", False)
    assert client.states == {h11.CLIENT: h11.IDLE, h11.SERVER: h11.IDLE}
    assert not client.finalized


async def test_send_failure_marks_error() -> None:
    stream, _peer = trio.testing.memory_stream_pair()
    adapter = TrioH11Stream(stream, is_server=False)
    await stream.aclose()

    with pytest.raises(trio.ClosedResourceError):
        await adapter.send(h11.Request(method="GET", target="/", headers=[("Host", "x")]))
    assert adapter.our_state is h11.ERROR


async def test_clean_close_after_peer_close() -> None:
    stream, peer = trio.testing.memory_stream_pair()
    adapter = TrioH11Stream(stream, is_server=False)
    await adapter.send(
        h11.Request(method="GET", target="/", headers=[("Host", "x"), ("Connection", "close")])
    )
    await adapter.send(h11.EndOfMessage())
    await peer.send_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
    await peer.send_eof()
    assert isinstance(await adapter.recv(), h11.Response)
    assert isinstance(await adapter.recv(), h11.EndOfMessage)
    assert not adapter.finalized
    assert isinstance(await adapter.recv(), h11.ConnectionClosed)
    assert adapter.finalized

    with pytest.raises(Exception, match="receive"):
        await adapter.recv()
    with pytest.raises(Exception, match="send"):
        await adapter.send(h11.EndOfMessage())
    assert [event async for event in adapter] == []
    await adapter.aclose()


async def test_no_half_close_support() -> None:
    inner, peer = trio.testing.memory_stream_pair()
    adapter = TrioH11Stream(NoHalfCloseStream(inner), is_server=False)

    await adapter.aclose()

    assert adapter.finalized
    assert await peer.receive_some(1) == b""


async def test_protocol_switch() -> None:
    client_stream, server_stream = trio.testing.memory_stream_pair()
    client = TrioH11Stream(client_stream, is_server=False)
    server = TrioH11Stream(server_stream, is_server=True)

    with pytest.raises(Exception, match="SWITCHED_PROTOCOL"):
        client.finalize_switch()

    await client.send(
        h11.Request(
            method="GET",
            target="/",
            headers=[("Host", "x"), ("Connection", "upgrade"), ("Upgrade", "foo")],
        )
    )
    await client.send(h11.EndOfMessage())
    assert isinstance(await server.recv(), h11.Request)
    assert isinstance(await server.recv(), h11.EndOfMessage)
    assert await server.recv() is h11.PAUSED

    await server.send(h11.InformationalResponse(status_code=101, headers=[("Upgrade", "foo")]))
    assert isinstance(await client.recv(), h11.InformationalResponse)
    await client_stream.send_all(b"")

    stream, trailing = server.finalize_switch()
    assert stream is server_stream
    assert trailing == b""
    assert server.finalized


async def test_start_next_cycle() -> None:
    client_stream, server_stream = trio.testing.memory_stream_pair()
    client = TrioH11Stream(client_stream, is_server=False)
    server = TrioH11Stream(server_stream, is_server=True)

    await client.send(h11.Request(method="GET", target="/", headers=[("Host", "x")]))
    await client.send(h11.EndOfMessage())
    await server.recv()
    await server.recv()
    await server.send(h11.Response(status_code=204, headers=[]))
    await server.send(h11.EndOfMessage())
    await client.recv()
    await client.recv()

    client.start_next_cycle()
    server.start_next_cycle()
    assert client.our_state is h11.IDLE
    assert server.our_state is h11.IDLE
