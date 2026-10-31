import h11
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
