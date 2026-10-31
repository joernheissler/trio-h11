from __future__ import annotations

from typing import Self, cast

import h11
import trio

DEFAULT_RECEIVE_SIZE = 64 * 1024
DEFAULT_INCOMPLETE_EVENT_SIZE = 16 * 1024

class TrioH11Stream(trio.abc.AsyncResource):
    """
    Turn a trio byte Stream into an h11 Event stream.
    This can be used with both clients and servers.
    """

    # Underlying stream
    stream: trio.abc.Stream

    # Number of bytes to receive from stream on each read
    recv_size: int

    # Try to shutdown connections cleanly.
    _clean_shutdown: bool

    # Underlying h11 Connection
    _h11: h11.Connection

    # Mark this TrioH11Stream as complete. No more operations on the byte stream will occur.
    _finalized: bool = False

    def __init__(
        self,
        stream: trio.abc.Stream,
        is_server: bool,
        *,
        max_incomplete_event_size: int = DEFAULT_INCOMPLETE_EVENT_SIZE,
        recv_size: int = DEFAULT_RECEIVE_SIZE,
        clean_shutdown: bool = True,
    ) -> None:
        our_role = h11.SERVER if is_server else h11.CLIENT
        self.stream = stream
        self._h11 = h11.Connection(our_role, max_incomplete_event_size)
        self.recv_size = recv_size
        self._clean_shutdown = clean_shutdown and isinstance(stream, trio.abc.HalfCloseableStream)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> h11.Event | type[h11.PAUSED]:
        if self._finalized:
            raise StopAsyncIteration

        return await self.recv()

    @property
    def our_state(self) -> type[h11._util.Sentinel]:
        return self._h11.our_state

    @property
    def their_state(self) -> type[h11._util.Sentinel]:  # -> H11_STATES:
        return self._h11.their_state

    @property
    def they_are_waiting_for_100_continue(self) -> bool:
        return self._h11.they_are_waiting_for_100_continue

    @property
    def trailing_data(self) -> tuple[bytes, bool]:
        return self._h11.trailing_data

    def start_next_cycle(self) -> None:
        self._h11.start_next_cycle()

    @property
    def finalized(self) -> bool:
        return self._finalized

    async def send(self, event: h11.Event) -> None:
        """
        Send an h11 Event.

        Params:
            event: The event to send.
        """
        if self._finalized:
            raise Exception("Must not send any more events.")

        data = self._h11.send(event)
        if not data:
            return

        try:
            await self.stream.send_all(data)
        except Exception:
            self._h11.send_failed()
            raise

    async def recv(self) -> h11.Event | type[h11.PAUSED]:
        """
        Receive the next h11 Event. Keeps receiving data from the stream if needed.

        Returns:
            An h11 Event or PAUSED
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
        """
        Close the event stream and the underlying byte stream, unless it was protected
        by finalize_switch.
        """
        if self._finalized:
            return

        try:
            await self.send(h11.ConnectionClosed())
            self._finalized = True
            if self.clean_shutdown:
                await self.stream.send_eof()
        finally:
            await self.stream.aclose()

    def finalize_switch(self) -> tuple[trio.abc.Stream, bytes]:
        """
        This can be called after having successfully switched the protocol.
        It causes a subsequent aclose call to not close the stream.

        Returns:
            Underlying stream and any trailing data that should be processed by the next
            protocol.
        """
        if self.our_state != h11.SWITCHED_PROTOCOL:
            raise Exception(f"State expected to be SWITCHED_PROTOCOL but it's {self.our_state}")

        self._finalized = True

        return self.stream, self.trailing_data[0]
