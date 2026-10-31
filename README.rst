trio-h11
========

|PyPI| |Docs| |License: MIT|

Glue between `trio <https://trio.readthedocs.io/>`__ and
`h11 <https://h11.readthedocs.io/>`__. A single class wraps a trio
``Stream`` and provides coroutines to receive and send h11 events. It
works for both clients and servers.

Installation
------------

::

   pip install trio-h11

Usage
-----

.. code:: python

   import h11
   import trio
   from trio_h11 import TrioH11Connection

   async def main():
       stream = await trio.open_tcp_stream("example.com", 80)
       conn = TrioH11Connection(stream, h11.CLIENT)
       await conn.send_event(h11.Request(method="GET", target="/",
           headers=[("Host", "example.com"), ("Connection", "close")]))
       await conn.send_event(h11.EndOfMessage())
       while True:
           event = await conn.receive_event()
           print(event)
           if isinstance(event, h11.EndOfMessage):
               break

   trio.run(main)

Development
-----------

::

   pip install -e . pytest pytest-trio
   pytest

License
-------

MIT, see `LICENSE <LICENSE>`__. Compatible with trio (MIT/Apache-2.0)
and h11 (MIT).

.. |PyPI| image:: https://img.shields.io/pypi/v/trio-h11
   :target: https://pypi.org/project/trio-h11/
.. |Docs| image:: https://readthedocs.org/projects/trio-h11/badge/
   :target: https://trio-h11.1e8.de/
.. |License: MIT| image:: https://img.shields.io/badge/license-MIT-blue.svg
   :target: LICENSE
