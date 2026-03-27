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
    "sphinxcontrib.mermaid",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "_autosummary", "Thumbs.db", ".DS_Store"]
suppress_warnings = ["misc.highlighting_failure"]

# -- Options for HTML output -------------------------------------------------
html_theme = "furo"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_title = "GBP"
html_theme_options = {
    "sidebar_hide_name": False,
    "navigation_with_keys": True,
    "source_repository": "https://github.com/vlzm/GFDRR",
    "source_branch": "main",
    "source_directory": "docs/",
    "light_css_variables": {
        # Brand — professional teal-blue
        "color-brand-primary": "#0d6eaa",
        "color-brand-content": "#0d6eaa",
        # Foreground
        "color-foreground-primary": "#2e3440",
        "color-foreground-secondary": "#4c566a",
        "color-foreground-muted": "#7b8894",
        "color-foreground-border": "#d8dee9",
        # Background
        "color-background-primary": "#ffffff",
        "color-background-secondary": "#f8f9fb",
        "color-background-hover": "#eef1f6",
        "color-background-hover--transparent": "#eef1f600",
        "color-background-border": "#e5e9f0",
        # Sidebar — dark slate for contrast
        "color-sidebar-background": "#1a202c",
        "color-sidebar-background-border": "#2d3748",
        "color-sidebar-brand-text": "#ffffff",
        "color-sidebar-caption-text": "#a0aec0",
        "color-sidebar-link-text": "#cbd5e0",
        "color-sidebar-link-text--top-level": "#e2e8f0",
        "color-sidebar-item-background--current": "#2d3748",
        "color-sidebar-item-background--hover": "#2d374880",
        "color-sidebar-item-expander-color": "#718096",
        "color-sidebar-item-expander-color--hover": "#a0aec0",
        "color-sidebar-search-background": "#2d3748",
        "color-sidebar-search-border": "#4a5568",
        # Code
        "color-code-background": "#f0f4f8",
        "color-code-foreground": "#2e3440",
        "color-inline-code-background": "#eef1f6",
        # Admonitions
        "color-admonition-background": "#ebf8ff",
        "color-admonition-title-background": "#bee3f8",
        "color-admonition-title": "#2c5282",
        # Links
        "color-link": "#0d6eaa",
        "color-link--hover": "#094d7a",
        "color-link-underline": "#0d6eaa40",
        "color-link-underline--hover": "#094d7a",
        # API docs
        "color-api-background": "#f7fafc",
        "color-api-overall-border": "#e2e8f0",
        "color-api-name": "#1a365d",
        # Typography
        "font-stack": (
            "'Inter', -apple-system, BlinkMacSystemFont,"
            " 'Segoe UI', Roboto, sans-serif"
        ),
        "font-stack--monospace": (
            "'JetBrains Mono', 'Fira Code', 'Consolas', monospace"
        ),
        "font-stack--headings": (
            "'Inter', -apple-system, BlinkMacSystemFont,"
            " 'Segoe UI', Roboto, sans-serif"
        ),
    },
    "dark_css_variables": {
        # Brand
        "color-brand-primary": "#63b3ed",
        "color-brand-content": "#63b3ed",
        # Foreground
        "color-foreground-primary": "#eceff4",
        "color-foreground-secondary": "#d8dee9",
        "color-foreground-muted": "#7b8894",
        "color-foreground-border": "#3b4252",
        # Background — deep blue-grey
        "color-background-primary": "#1a1e2e",
        "color-background-secondary": "#232736",
        "color-background-hover": "#2e3440",
        "color-background-hover--transparent": "#2e344000",
        "color-background-border": "#3b4252",
        # Sidebar
        "color-sidebar-background": "#171923",
        "color-sidebar-background-border": "#2d3748",
        "color-sidebar-brand-text": "#e2e8f0",
        "color-sidebar-caption-text": "#a0aec0",
        "color-sidebar-link-text": "#a0aec0",
        "color-sidebar-link-text--top-level": "#cbd5e0",
        "color-sidebar-item-background--current": "#2d3748",
        "color-sidebar-item-background--hover": "#2d374880",
        # Code
        "color-code-background": "#232736",
        "color-code-foreground": "#d8dee9",
        "color-inline-code-background": "#2e3440",
        # Admonitions
        "color-admonition-background": "#2d374880",
        # Links
        "color-link": "#63b3ed",
        "color-link--hover": "#90cdf4",
        # API
        "color-api-background": "#232736",
        "color-api-overall-border": "#3b4252",
        "color-api-name": "#90cdf4",
    },
    "footer_icons": [
        {
            "name": "GitHub",
            "url": "https://github.com/vlzm/GFDRR",
            "html": (
                '<svg stroke="currentColor" fill="currentColor" stroke-width="0"'
                ' viewBox="0 0 16 16"><path fill-rule="evenodd" d="M8 0C3.58 0 0'
                " 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38"
                " 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48"
                "-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72"
                " 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89"
                "-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0"
                " .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2"
                " .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82"
                " 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0"
                " 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016"
                ' 8c0-4.42-3.58-8-8-8z"></path></svg>'
            ),
            "class": "",
        },
    ],
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
myst_fence_as_directive = ["mermaid"]

# sphinxcontrib-mermaid
mermaid_version = "11.4.1"
mermaid_init_js = (
    "mermaid.initialize({"
    "  startOnLoad: true,"
    '  theme: "neutral",'
    "  flowchart: { useMaxWidth: true, htmlLabels: true },"
    '  securityLevel: "loose"'
    "});"
)

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
