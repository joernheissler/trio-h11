"""Adapt Trio byte streams to the h11 HTTP/1.1 state machine."""

from __future__ import annotations

from typing import cast

import h11
import trio

__version__ = "0.1.0"

#: Default maximum number of bytes read from the underlying stream at once.
DEFAULT_RECEIVE_SIZE = 64 * 1024
#: Default maximum size of an incomplete h11 event.
DEFAULT_INCOMPLETE_EVENT_SIZE = 16 * 1024


class TrioH11Stream(trio.abc.AsyncResource):
    """Adapt a Trio byte stream to send and receive h11 events."""

    stream: trio.abc.Stream
    recv_size: int
    _clean_shutdown: bool
    _h11: h11.Connection
    _finalized: bool

    def __init__(
        self,
        stream: trio.abc.Stream,
        is_server: bool,
        *,
        max_incomplete_event_size: int = DEFAULT_INCOMPLETE_EVENT_SIZE,
        recv_size: int = DEFAULT_RECEIVE_SIZE,
        clean_shutdown: bool = True,
    ) -> None:
        """Create an event stream around a Trio stream.

        Args:
            stream: The bidirectional byte stream to adapt.
            is_server: Whether this endpoint is the HTTP server.
            max_incomplete_event_size: Maximum size of an incomplete h11 event.
            recv_size: Maximum number of bytes read from the stream at once.
            clean_shutdown: Half-close the stream after sending the closing event,
                when the underlying stream supports it.
        """
        our_role = h11.SERVER if is_server else h11.CLIENT
        self.stream = stream
        self._h11 = h11.Connection(our_role, max_incomplete_event_size)
        self.recv_size = recv_size
        self._clean_shutdown = clean_shutdown and isinstance(
            stream, trio.abc.HalfCloseableStream
        )
        self._finalized = False

    def __aiter__(self) -> TrioH11Stream:
        """Return this object as an asynchronous iterator of h11 events."""
        return self

    async def __anext__(self) -> h11.Event | type[h11.PAUSED]:
        """Receive the next event, or stop after the stream has been finalized."""
        if self._finalized:
            raise StopAsyncIteration

        return await self.recv()

    @property
    def our_state(self) -> type[h11._util.Sentinel]:
        """Return h11's state for events sent by this endpoint."""
        return self._h11.our_state

    @property
    def their_state(self) -> type[h11._util.Sentinel]:
        """Return h11's state for events received from the peer."""
        return self._h11.their_state

    @property
    def they_are_waiting_for_100_continue(self) -> bool:
        """Whether the peer is waiting for a ``100 Continue`` response."""
        return self._h11.they_are_waiting_for_100_continue

    @property
    def trailing_data(self) -> tuple[bytes, bool]:
        """Return bytes following the current message and whether the peer closed."""
        return self._h11.trailing_data

    def start_next_cycle(self) -> None:
        """Start the next HTTP message cycle when h11 permits it."""
        self._h11.start_next_cycle()

    @property
    def finalized(self) -> bool:
        """Whether this adapter can no longer use its underlying stream."""
        return self._finalized

    async def send(self, event: h11.Event) -> None:
        """Send an h11 event to the peer.

        Args:
            event: The h11 event to serialize and send.
        """
        if self._finalized:
            raise Exception("Must not send any more events.")

        data = self._h11.send(event)
        if not data:
            return

        try:
            await self.stream.send_all(data)
        except BaseException:
            self._h11.send_failed()
            raise

    async def recv(self) -> h11.Event | type[h11.PAUSED]:
        """Receive the next h11 event, reading bytes as needed.

        Returns:
            The next h11 event, or h11's ``PAUSED`` sentinel.
        """
        if self._finalized:
            raise Exception("Can not receive any more events.")

        while (event := self._h11.next_event()) is h11.NEED_DATA:
            data = await self.stream.receive_some(self.recv_size)
            self._h11.receive_data(bytes(data))

        if self.our_state is h11.MUST_CLOSE and self.their_state is h11.CLOSED:
            await self.aclose()

        return cast(h11.Event | type[h11.PAUSED], event)

    async def aclose(self) -> None:
        """Close the adapter and its byte stream, sending a clean shutdown if possible.

        The underlying stream is left open only when ``finalize_switch`` has already
        transferred ownership to another protocol.
        """
        if self._finalized:
            return

        try:
            await self.send(h11.ConnectionClosed())
            self._finalized = True
            if self._clean_shutdown:
                await self.stream.send_eof()
        finally:
            await self.stream.aclose()

    def finalize_switch(self) -> tuple[trio.abc.Stream, bytes]:
        """Transfer the stream after a successful protocol switch.

        The adapter is finalized and will no longer close the underlying stream.
        Returns the stream and trailing bytes that the new protocol must process.
        """
        if self.our_state != h11.SWITCHED_PROTOCOL:
            raise Exception(f"State expected to be SWITCHED_PROTOCOL but it's {self.our_state}")

        self._finalized = True

        return self.stream, self.trailing_data[0]
