import h11
import trio
import trio.testing

from trio_h11 import TrioH11Connection


async def test_roundtrip() -> None:
    a, b = trio.testing.memory_stream_pair()
    client = TrioH11Connection(a, h11.CLIENT)
    server = TrioH11Connection(b, h11.SERVER)
    await client.send_event(
        h11.Request(method="GET", target="/", headers=[("Host", "x")])
    )
    await client.send_event(h11.EndOfMessage())
    req = await server.receive_event()
    assert isinstance(req, h11.Request)
    assert req.target == b"/"
    assert isinstance(await server.receive_event(), h11.EndOfMessage)
