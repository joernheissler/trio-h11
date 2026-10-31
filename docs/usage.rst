Usage
=====

Install with ``pip install trio-h11``.

.. code-block:: python

   import h11
   import trio
   from trio_h11 import TrioH11Connection

   async def main():
       stream = await trio.open_tcp_stream("example.com", 80)
       conn = TrioH11Connection(stream, h11.CLIENT)
       await conn.send_event(h11.Request(
           method="GET", target="/",
           headers=[("Host", "example.com"), ("Connection", "close")]))
       await conn.send_event(h11.EndOfMessage())
       while True:
           event = await conn.receive_event()
           if isinstance(event, h11.EndOfMessage):
               break

   trio.run(main)
