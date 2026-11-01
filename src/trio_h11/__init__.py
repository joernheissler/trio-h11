"""
Wrap a trio byte stream into an h11 Event stream.
"""

from __future__ import annotations

from typing import Self, cast

import h11
import trio

__version__ = "0.1.0"

H11_ROLE = type[h11.CLIENT] | type[h11.SERVER]
H11_STATE = (
    type[h11.IDLE]
    | type[h11.SEND_RESPONSE]
    | type[h11.SEND_BODY]
    | type[h11.DONE]
    | type[h11.MUST_CLOSE]
    | type[h11.CLOSED]
    | type[h11.ERROR]
    | type[h11.MIGHT_SWITCH_PROTOCOL]
    | type[h11.SWITCHED_PROTOCOL]
)

#: Default maximum number of bytes read from the underlying stream at once.
DEFAULT_RECEIVE_SIZE = 64 * 1024

#: Default maximum size of an incomplete h11 Event.
DEFAULT_INCOMPLETE_EVENT_SIZE = 16 * 1024


class TrioH11Stream(trio.abc.AsyncResource):
    """
    Adapt a Trio byte stream to send and receive h11 events.
    """

    #: Underlying stream. Should not be set.
    stream: trio.abc.Stream

    #: Number of bytes to receive from stream on each read. May be modified.
    recv_size: int

    #: Try to shutdown connections cleanly by using a half-close, if supported.
    _clean_shutdown: bool

    #: Underlying h11 Connection
    _h11: h11.Connection

    #: Marks this TrioH11Stream as complete. No more operations on the byte stream will occur.
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
        """
        Create an h11 Event stream around a Trio stream.

        Args:
            stream: The bidirectional byte stream to wrap.
            is_server: Whether this endpoint is the HTTP server.
            max_incomplete_event_size: Maximum size of an incomplete h11 Event.
            recv_size: Maximum number of bytes read from the stream at once.
            clean_shutdown: Half-close the stream after sending the closing Event,
                when the underlying stream supports it.
        """
        our_role = h11.SERVER if is_server else h11.CLIENT
        self.stream = stream
        self._h11 = h11.Connection(our_role, max_incomplete_event_size)
        self.recv_size = recv_size
        self._clean_shutdown = clean_shutdown and isinstance(
            stream, trio.abc.HalfCloseableStream
        )

    def __aiter__(self) -> Self:
        """
        Return this object as an asynchronous iterator of h11 Events.
        """
        return self

    async def __anext__(self) -> h11.Event | type[h11.PAUSED]:
        """
        Receive the next Event, or stop after the stream has been finalized.
        """
        if self._finalized:
            raise StopAsyncIteration

        return await self.recv()

    async def recv(self) -> h11.Event | type[h11.PAUSED]:
        """
        Receive the next h11 Event. Keeps reading bytes from the stream as needed.

        Returns:
            The next h11 Event, or h11's ``PAUSED`` sentinel.
        """
        if self._finalized:
            raise Exception("Can not receive any more events.")

        while (event := self._h11.next_event()) is h11.NEED_DATA:
            data = await self.stream.receive_some(self.recv_size)
            self._h11.receive_data(bytes(data))

        if self.our_state is h11.MUST_CLOSE and self.their_state is h11.CLOSED:
            await self.aclose()

        return cast(h11.Event | type[h11.PAUSED], event)

    async def send(self, event: h11.Event) -> None:
        """
        Send an h11 Event to the peer.

        Args:
            event: The h11 Event to serialize and send.
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

    async def aclose(self) -> None:
        """
        Close the wrapper and its byte stream, sending a clean shutdown if possible.

        The underlying stream is left open iff ``finalize_switch`` has already transferred
        ownership to another protocol.
        """
        if self._finalized:
            return

        try:
            await self.send(h11.ConnectionClosed())
            self._finalized = True
            if self._clean_shutdown:
                stream = cast(trio.abc.HalfCloseableStream, self.stream)
                await stream.send_eof()
        finally:
            await self.stream.aclose()

    def start_next_cycle(self) -> None:
        """
        Start the next HTTP message cycle when h11 permits it.
        """
        self._h11.start_next_cycle()

    def finalize_switch(self) -> tuple[trio.abc.Stream, bytes]:
        """
        Call this method after a successful protocol switch.

        The wrapper is finalized and will no longer close the underlying stream.

        Returns:
            Underlying stream and trailing bytes that the new protocol must process.
        """
        if self.our_state != h11.SWITCHED_PROTOCOL:
            raise Exception(f"State expected to be SWITCHED_PROTOCOL but it's {self.our_state}")

        self._finalized = True

        return self.stream, self.trailing_data[0]

    @property
    def our_state(self) -> H11_STATE:
        """
        The current state of whichever role we are playing.
        """
        return self._h11.our_state

    @property
    def their_state(self) -> H11_STATE:
        """
        The current state of whichever role we are NOT playing.
        """
        return self._h11.their_state

    @property
    def their_http_version(self) -> bytes | None:
        """
        The version of HTTP that our peer claims to support.
        None if we haven’t yet received a request/response.
        """
        return self._h11.their_http_version

    @property
    def client_is_waiting_for_100_continue(self) -> bool:
        """
        True if the client sent a request with the Expect: 100-continue header,
        and is still waiting for a response.
        """
        return self._h11.client_is_waiting_for_100_continue

    @property
    def they_are_waiting_for_100_continue(self) -> bool:
        """
        True if their_role is CLIENT and client_is_waiting_for_100_continue.
        """
        return self._h11.they_are_waiting_for_100_continue

    @property
    def trailing_data(self) -> tuple[bytes, bool]:
        """
        Data that has been received, but not yet processed, represented as a tuple with two
        elements, where the first is a byte-string containing the unprocessed data itself, and
        the second is a bool that is True if the receive connection was closed.
        """
        return self._h11.trailing_data

    @property
    def our_role(self) -> H11_ROLE:
        """
        h11.CLIENT if this is a client; h11.SERVER if this is a server.
        """
        return cast(H11_ROLE, self._h11.our_role)

    @property
    def their_role(self) -> H11_ROLE:
        """
        h11.SERVER if this is a client; h11.CLIENT if this is a server.
        """
        return cast(H11_ROLE, self._h11.their_role)

    @property
    def finalized(self) -> bool:
        """
        Whether this wrapper can no longer use its underlying stream.
        """
        return self._finalized

    @property
    def states(self) -> dict[H11_ROLE, H11_STATE]:
        """
        Dictionary with client and server state.
        """
        return cast(dict[H11_ROLE, H11_STATE], self._h11.states)
