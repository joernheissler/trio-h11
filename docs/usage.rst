Usage
=====

Install with ``pip install trio-h11``. Wrap a bidirectional Trio stream in
``TrioH11Stream`` and choose the endpoint role with ``is_server``:

.. code-block:: python

   import h11
   import trio
   from trio_h11 import TrioH11Stream

   async def main():
       stream = await trio.open_tcp_stream("example.com", 80)
       conn = TrioH11Stream(stream, is_server=False)
       try:
           await conn.send(h11.Request(
               method="GET",
               target="/",
               headers=[("Host", "example.com"), ("Connection", "close")],
           ))
           await conn.send(h11.EndOfMessage())

           async for event in conn:
               print(event)
               if isinstance(event, h11.EndOfMessage):
                   break
       finally:
           await conn.aclose()

   trio.run(main)

``send`` serializes an h11 event and writes it to the byte stream; ``recv``
reads and parses bytes until the next event is available. The adapter is also
an asynchronous iterator over received events. Use the standard h11 event
sequence to stream request or response bodies with ``h11.Data`` and terminate
each message with ``h11.EndOfMessage``.

The default receive chunk is 64 KiB, and the maximum incomplete event size is
16 KiB. Both can be configured through ``recv_size`` and
``max_incomplete_event_size``. ``aclose`` closes the byte stream and sends a
half-close when the stream supports it. If h11 switches protocols, call
``finalize_switch`` after the switch; it returns the still-open byte stream and
any trailing bytes for the new protocol.

See the :doc:`api` reference for the complete interface.
