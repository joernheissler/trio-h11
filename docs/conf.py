import ast
from pathlib import Path

project = "trio-h11"
author = "Jörn Heissler"
copyright = f"2026, {author}"
release = next(
    expr
    for expr in ast.parse(
        (Path(__file__).parent.parent / "src/trio_h11/__init__.py").read_text()
    ).body
    if isinstance(expr, ast.Assign) and expr.targets[0].id == "__version__"
).value.value

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.coverage",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_rtd_theme",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "trio": ("https://trio.readthedocs.io/en/stable/", None),
    "h11": ("https://h11.readthedocs.io/en/latest/", None),
}

html_theme = "sphinx_rtd_theme"
