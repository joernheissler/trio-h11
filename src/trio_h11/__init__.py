"""Glue between trio streams and h11."""

from __future__ import annotations

import h11
import trio

__version__ = "0.1.0"
__all__ = ["TrioH11Connection"]

MAX_RECV_SIZE = 64 * 1024


class TrioH11Connection:
    """Wrap a :class:`trio.abc.Stream` and exchange :mod:`h11` events.

    The same class works for clients and servers, depending on ``our_role``.
    """

    def __init__(
        self,
        stream: trio.abc.Stream,
        our_role: type[h11.CLIENT] | type[h11.SERVER],
        **h11_kwargs,
    ) -> None:
        self.stream = stream
        self.h11 = h11.Connection(our_role, **h11_kwargs)

    async def receive_event(self):
        """Return the next h11 event, reading from the stream as needed."""
        while True:
            event = self.h11.next_event()
            if event is h11.NEED_DATA:
                data = await self.stream.receive_some(MAX_RECV_SIZE)
                self.h11.receive_data(data)
                continue
            return event

    async def send_event(self, event) -> None:
        """Send an h11 event over the stream."""
        data = self.h11.send(event)
        if data is not None:
            await self.stream.send_all(data)
