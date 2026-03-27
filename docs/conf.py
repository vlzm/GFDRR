# Configuration file for the Sphinx documentation builder.
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import inspect
import os
import sys
import warnings

sys.path.insert(0, os.path.abspath(".."))

import gbp  # noqa: E402

# Suppress deprecation warnings from sphinx_autodoc_typehints (Sphinx 9 compat)
warnings.filterwarnings("ignore", message=".*RemovedInSphinx10Warning.*")

# -- Project information -----------------------------------------------------
project = "GBP"
copyright = "2025, vlzm"
author = "vlzm"
release = "0.1.0"

# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.linkcode",
    "sphinx.ext.intersphinx",
    "sphinx_autodoc_typehints",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "_autosummary", "Thumbs.db", ".DS_Store"]
suppress_warnings = ["misc.highlighting_failure"]

# -- Options for HTML output -------------------------------------------------
html_theme = "furo"
html_static_path = []
html_title = "GBP"
html_theme_options = {
    "sidebar_hide_name": False,
    "navigation_with_keys": True,
}

# -- Extension settings ------------------------------------------------------

# autodoc
add_module_names = False  # Show "RawModelData" not "gbp.core.model.RawModelData"
autodoc_member_order = "bysource"  # Follow source code order, not alphabetical
autodoc_typehints = "description"
autodoc_class_signature = "separated"
autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "show-inheritance": True,
}

# autosummary
autosummary_generate = True

# napoleon (Google-style docstrings)
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = False

# intersphinx: link to external docs
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/docs", None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "pydantic": ("https://docs.pydantic.dev/latest", None),
}

# MyST parser — allow .md source files
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# -- linkcode: generate links to GitHub source code -------------------------
_GITHUB_ROOT = "https://github.com/vlzm/GFDRR/blob/main"


def linkcode_resolve(domain: str, info: dict) -> str | None:
    """Resolve source code links to GitHub."""
    if domain != "py":
        return None
    module_name = info.get("module")
    fullname = info.get("fullname")
    if not module_name or not fullname:
        return None

    try:
        module = sys.modules.get(module_name)
        if module is None:
            return None
        obj = module
        for part in fullname.split("."):
            obj = getattr(obj, part, None)
            if obj is None:
                return None
        source_file = inspect.getfile(obj)
        source_lines, start_line = inspect.getsourcelines(obj)
        end_line = start_line + len(source_lines) - 1
    except (TypeError, OSError):
        return None

    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    try:
        rel_path = os.path.relpath(source_file, repo_root).replace("\\", "/")
    except ValueError:
        return None

    return f"{_GITHUB_ROOT}/{rel_path}#L{start_line}-L{end_line}"
