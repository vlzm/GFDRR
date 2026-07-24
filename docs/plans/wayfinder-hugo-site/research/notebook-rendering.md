# Showing Jupyter notebooks on a hugo-book site: options and costs

Resolves ticket T5. Researched 2026-07-24.

Context assumed: two notebooks (pandas tables, matplotlib + plotly), hugo-book theme on GitHub Pages, content in `docs/site/content/`, Goldmark `unsafe = true` already on, readers are recruiters who will not clone the repo.

## Option 1: `jupyter nbconvert --to markdown` into a page bundle

**Mechanics.** Convert the notebook into a Hugo leaf bundle:

```bash
mkdir -p docs/site/content/docs/test-pipeline
jupyter nbconvert notebooks/test_pipeline.ipynb \
  --to markdown \
  --output index \
  --output-dir docs/site/content/docs/test-pipeline
```

- **Images.** nbconvert extracts every `image/png` / `image/svg+xml` output into a separate file. The directory is controlled by `NbConvertApp.output_files_dir`, default `'{notebook_name}_files'` (verified in nbconvert config docs). So you get `index_files/output_12_0.png` next to `index.md`. Because `index.md` plus those files form a Hugo **leaf bundle**, Hugo publishes the resources alongside the page, and the relative link `![png](index_files/output_12_0.png)` resolves in the browser (page URL `/docs/test-pipeline/` + relative path). Note: Hugo's page-resources docs recommend image render hooks for guaranteed resolution of relative links; in a plain leaf bundle with default pretty URLs the relative path works without hooks, but that last step is convention, not something the docs state outright.
- **Front matter.** nbconvert emits no Hugo front matter. You must prepend `title`/`weight` by hand or with a small `sed`/Python step. A raw first `#` heading works but the page loses metadata.
- **Pandas tables.** `DataFrame` display output is emitted as a raw HTML `<table>` block (with a pandas `<style scoped>` block; `scoped` is ignored by browsers). With `unsafe = true` Goldmark passes the raw HTML through, confirmed by Hugo docs ("whether to render raw HTML mixed within Markdown"). The table renders, picking up whatever element-level `table` CSS hugo-book has. Two costs not fully verified: hugo-book's exact table styling of raw (non-Goldmark) tables, and overflow behavior — hugo-book's content column is narrow, so a wide DataFrame will likely need a few lines of custom CSS (`overflow-x: auto` on a wrapper, or `pd.set_option("display.max_columns", ...)` before export).
- **Plotly.** This is the weak point. Plotly's interactive output is HTML + JS that expects the notebook's script loader; plotly's own docs only promise survival of the `notebook` / `notebook_connected` renderers under **HTML** export, not markdown. In a markdown export the figure block usually renders as nothing. Realistic fixes: (a) export plotly figures as static PNG via kaleido so nbconvert extracts them like matplotlib output, or (b) `fig.write_html("static/figs/x.html")` and iframe each figure. Both are per-figure work.

**Maintenance.** Re-run one command (plus the front-matter step) whenever the notebook changes. Easy to script in a Makefile or a GitHub Actions step before `hugo`.

**Reader experience.** Native site page: hugo-book nav, dark mode, search, selectable text, KaTeX. Best-looking option once tables and plotly are tamed.

## Option 2: `nbconvert --to html`, served whole

**Mechanics.**

```bash
jupyter nbconvert notebooks/test_pipeline.ipynb --to html --embed-images \
  --output-dir docs/site/static/notebooks
```

Files in Hugo's `static/` are copied verbatim to the site root, so the page is at `/notebooks/test_pipeline.html`. `--embed-images` inlines images as base64, giving one self-contained file (verified in nbconvert usage docs). Templates: `lab` (default, full Jupyter look, `--theme dark` available), `classic`, `basic`. `--no-input` hides code cells if wanted. Then either link to it from a hand-written page, or embed with `<iframe src="/notebooks/test_pipeline.html" ...>` (allowed since `unsafe = true`).

- **Pros.** Zero fidelity loss: pandas tables styled exactly as in Jupyter, and **plotly stays interactive** — plotly docs confirm figures survive nbconvert HTML export when the `notebook`/`notebook_connected` renderer was used.
- **Cons.** The page looks like Jupyter, not like your site: no hugo-book nav, no dark-mode sync, different fonts. Iframe embedding adds height/scrolling fiddling and double scrollbars. File can be large (embedded plotly.js ~3 MB with the `notebook` renderer; `notebook_connected` pulls from CDN instead).

**Maintenance.** One command per change; trivially scriptable.

**Reader experience.** Familiar Jupyter look, fully faithful, but visually a separate artifact from the portfolio site.

## Option 3: link to GitHub's rendering or nbviewer.org

**Mechanics.** Just a markdown link. Zero build work.

- **GitHub.** GitHub docs state notebooks render as **static HTML only**: "The interactive features of the notebook, such as custom JavaScript plots, will not work." So plotly cells are blank. GitHub's renderer also intermittently fails on large notebooks ("Unable to render notebook"). Reader lands in the repo UI, not your site.
- **nbviewer.org.** GitHub's own docs recommend it for JavaScript output. It renders plotly. But it is a free community service with recurring 503 outages tied to GitHub API rate limits (multiple issues in 2025 in the jupyter/nbviewer tracker), and it caches aggressively — after you push an update, readers may see a stale version for a while. Look is generic Jupyter-classic styling.

**Maintenance.** None (nbviewer cache lag aside).

**Reader experience.** One click away from your site, inconsistent branding, and — for a recruiter clicking at an arbitrary moment — a real chance of a broken or stale render. Weakest option for a portfolio.

## Option 4: static screenshots in a hand-written page

**Mechanics.** Screenshot the key cells (or save figures with `fig.savefig` / `fig.write_image`), drop the PNGs into a page bundle, write a short narrative page around them.

- **Pros.** Total editorial control: you show the five cells that matter, with commentary, instead of 80 cells of scroll. Recruiters read curated stories better than raw notebooks.
- **Cons.** Text in screenshots is not selectable or searchable and looks blurry on some displays; tables as images are a known anti-pattern (the `dataframe_image` package exists precisely because HTML tables are awkward, but it just automates the same trade-off). No interactivity.

**Maintenance.** Highest per change: retake screenshots by hand, or wire `dataframe_image` / `savefig` into a script.

**Reader experience.** Polished if you invest in the writing; clearly not a "live" notebook.

## Option 5: Hugo-specific tooling in 2026

- **`nb2hugo`** — a notebook-to-Hugo-markdown converter that handles front matter. **Effectively dead**: no PyPI release in years, flagged inactive by Snyk. Not worth adopting.
- **`hupyter`** and similar — one-person scripts, same verdict.
- **Quarto** — the one maintained option (Posit). Its `hugo-md` format is designed exactly for this: put `index.qmd` (or point at an `.ipynb`) in a page bundle under `content/`, run `quarto render`, get `index.md` plus assets in the bundle. Quarto's Hugo docs explicitly require the two settings you already have implicitly: `unsafe = true` and `ignoreFiles = ["\\.qmd$", "\\.ipynb$", "\\.py$"]` in the Hugo config. Quarto can also freeze execution (render from stored outputs) and writes proper front matter itself. Cost: a new ~200 MB toolchain dependency and a second document format in the repo; for two notebooks it is more machinery than Option 1, but it removes Option 1's front-matter and asset glue by design. Its docs do not promise interactive plotly inside `hugo-md` output — same caveat as Option 1.

## Comparison

| Option | Setup | Per-change cost | Pandas tables | Plotly | Look on site |
|---|---|---|---|---|---|
| 1. nbconvert → md bundle | small script (convert + front matter) | re-run script | render via unsafe=true; wide ones need CSS | broken by default; PNG-ify or iframe per figure | native, best |
| 2. nbconvert → html | one command | re-run command | perfect | **interactive** | Jupyter look, off-brand |
| 3. GitHub / nbviewer link | none | none | ok | GitHub: no; nbviewer: yes but flaky | leaves your site |
| 4. screenshots + prose | manual | manual redo | image only | image only | curated, static |
| 5. Quarto hugo-md | install Quarto, config | `quarto render` | same as 1 | same as 1 | native |

## Recommendation for this portfolio

Combine 1 and 2, which is the standard pattern:

1. **Primary**: Option 1. Convert each notebook to a leaf bundle under `docs/site/content/` with a tiny script (nbconvert + prepend front matter). The two runs become real site pages with nav and search — what a recruiter actually skims. Export plotly figures as static PNG (kaleido) inside the notebook so they convert like matplotlib. Add a few lines of table-overflow CSS.
2. **Escape hatch**: also run `nbconvert --to html --embed-images` into `static/notebooks/` and put a "View the full interactive notebook" link at the top of each page. That preserves interactive plotly and full fidelity for the one reader in ten who wants it.
3. Skip nbviewer/GitHub links as the main path (reliability, no plotly on GitHub), skip nb2hugo (dead), and skip Quarto unless the front-matter glue script grows annoying — for two notebooks it is not worth the dependency.

Unverified/soft points: hugo-book's exact CSS behavior on raw pandas tables and wide-table overflow (theme docs do not cover it — test locally); relative image links in leaf bundles working without render hooks is established practice but Hugo's docs hedge toward render hooks; plotly-in-markdown breakage is inferred from plotly's docs scoping their guarantee to HTML export only.

Sources: [nbconvert usage docs](https://nbconvert.readthedocs.io/en/latest/usage.html), [nbconvert config options](https://nbconvert.readthedocs.io/en/latest/config_options.html), [Hugo page bundles](https://gohugo.io/content-management/page-bundles/), [Hugo page resources](https://gohugo.io/content-management/page-resources/), [Hugo goldmark config](https://gohugo.io/getting-started/configuration-markup/), [GitHub non-code files docs](https://docs.github.com/en/repositories/working-with-files/using-files/working-with-non-code-files), [plotly renderers docs](https://plotly.com/python/renderers/), [Quarto Hugo format](https://quarto.org/docs/output-formats/hugo.html), [nb2hugo on Snyk](https://snyk.io/advisor/python/nb2hugo), [jupyter/nbviewer issues](https://github.com/jupyter/nbviewer/issues), [hugo-book theme](https://github.com/alex-shpak/hugo-book).
