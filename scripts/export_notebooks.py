"""Export the canonical notebooks to Hugo pages (scenario section).

Run by hand after the notebooks have been executed in Jupyter; it converts
their *saved* outputs and does not run them:

    python scripts/export_notebooks.py

Each notebook becomes a Hugo leaf bundle under
docs/site/content/docs/scenario/<slug>/index.md, with any extracted images
beside it in index_files/. `nbconvert --to markdown` writes the Markdown and
pulls out the matplotlib PNGs; the pandas HTML tables stay as raw HTML and
render because the site sets markup.goldmark.renderer.unsafe = true.

forecast_pipeline.ipynb has no saved outputs until it is run once, so its
page is code-only until then (see the T6 decision).
"""

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCENARIO = ROOT / "docs/site/content/docs/scenario"

# notebook file -> (bundle slug, page title, menu weight)
NOTEBOOKS = {
    "test_pipeline.ipynb": ("test-pipeline", "The base replay notebook", 2),
    "forecast_pipeline.ipynb": ("forecast-pipeline", "The forecast notebook", 3),
}


def export(nb_name: str, slug: str, title: str, weight: int) -> None:
    src = ROOT / "notebooks" / nb_name
    bundle = SCENARIO / slug
    # Rebuild the bundle from scratch so a re-run leaves no stale images.
    if bundle.exists():
        for p in sorted(bundle.rglob("*"), reverse=True):
            p.unlink() if p.is_file() else p.rmdir()
    bundle.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        ["jupyter", "nbconvert", "--to", "markdown",
         "--output", "index", "--output-dir", str(bundle), str(src)],
        check=True,
    )

    index = bundle / "index.md"
    body = index.read_text(encoding="utf-8")
    fm = f'---\ntitle: "{title}"\nweight: {weight}\n---\n\n'
    index.write_text(fm + body, encoding="utf-8")
    print(f"{nb_name} -> {index.relative_to(ROOT)}")


def main() -> None:
    for nb_name, (slug, title, weight) in NOTEBOOKS.items():
        export(nb_name, slug, title, weight)


if __name__ == "__main__":
    main()
