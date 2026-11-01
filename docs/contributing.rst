Contributing
============

Docstrings start their content on a new line after the opening triple quotes.
Write API docstrings in Google style and use Sphinx Napoleon to render them.

For example:

.. code-block:: python

   def greet(name: str) -> str:
       """
       Create a greeting for a person.

       Args:
           name: The person's name.

       Returns:
           A greeting.
       """

Sphinx is configured to parse Google-style docstrings through Napoleon.
