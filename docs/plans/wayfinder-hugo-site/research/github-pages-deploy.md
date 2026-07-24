# Publishing a Hugo site from `docs/site/` to GitHub Pages with GitHub Actions (verified July 2026)

Resolves ticket T8. All claims verified against primary sources on 2026-07-24.

## Summary

The current official method (Hugo docs, "Host on GitHub Pages") is: set the repository's Pages **Source** to **GitHub Actions**, and add a workflow that builds with a pinned Hugo version and deploys via `actions/upload-pages-artifact` + `actions/deploy-pages`. No `gh-pages` branch, no committed `public/` directory. For a site living in `docs/site/`, the only adaptations needed are `--source docs/site` on the build command and `path: docs/site/public` on the artifact upload.

## 1. The workflow, adapted for `docs/site/`

The official workflow is at https://gohugo.io/host-and-deploy/host-on-github-pages/ (verbatim source fetched from the `gohugoio/hugoDocs` repo). The official version pins `HUGO_VERSION: 0.164.0` and installs the **standard** edition. Three deliberate changes for this repo, each marked `# CHANGED`:

1. **Extended edition tarball** — hugo-book requires it (see §3).
2. **`--source docs/site`** — Hugo's standard flag for a project outside the working directory; equivalent to a `working-directory` default, but keeps every other step (checkout, cache) at repo root.
3. **Artifact path `docs/site/public`** — Hugo writes `public/` inside the source directory.

Also adapted: the conditional `hashFiles(...)` checks point at `docs/site/` (they will simply skip, since the theme is a vendored plain directory with no `go.mod` or `package-lock.json`). The Dart Sass step is kept as in the official workflow; it is harmless and covers any future `dartsass` transpiler use.

```yaml
# .github/workflows/hugo.yaml
name: Build and deploy
on:
  push:
    branches:
      - main
  workflow_dispatch:
permissions:
  contents: read
  pages: write
  id-token: write
concurrency:
  group: pages
  cancel-in-progress: false
defaults:
  run:
    shell: bash
jobs:
  build:
    runs-on: ubuntu-latest
    env:
      # Define tool versions
      DART_SASS_VERSION: 1.101.0
      HUGO_VERSION: 0.164.0

      # Set the build time zone
      TZ: Europe/Oslo
    steps:
      - name: Checkout
        uses: actions/checkout@v7
        with:
          submodules: recursive
          fetch-depth: 0
          lfs: false

      - name: Setup Pages
        id: pages
        uses: actions/configure-pages@v6

      - name: Create a local tools directory
        run: |
          mkdir -p "${HOME}/.local"

      - name: Install Dart Sass
        run: |
          echo "Installing Dart Sass ${DART_SASS_VERSION}..."
          curl -sfL --output-dir "${{ runner.temp }}" -O "https://github.com/sass/dart-sass/releases/download/${DART_SASS_VERSION}/dart-sass-${DART_SASS_VERSION}-linux-x64.tar.gz"
          tar -C "${HOME}/.local" -xf "${{ runner.temp }}/dart-sass-${DART_SASS_VERSION}-linux-x64.tar.gz"
          echo "${HOME}/.local/dart-sass" >> "${GITHUB_PATH}"

      - name: Install Hugo
        run: |
          echo "Installing Hugo ${HUGO_VERSION} (extended)..."
          # CHANGED: extended edition — required by the hugo-book theme
          curl -sfL --output-dir "${{ runner.temp }}" -O "https://github.com/gohugoio/hugo/releases/download/v${HUGO_VERSION}/hugo_extended_${HUGO_VERSION}_linux-amd64.tar.gz"
          mkdir "${HOME}/.local/hugo"
          tar -C "${HOME}/.local/hugo" -xf "${{ runner.temp }}/hugo_extended_${HUGO_VERSION}_linux-amd64.tar.gz"
          echo "${HOME}/.local/hugo" >> "${GITHUB_PATH}"

      - name: Log tool versions
        run: |
          echo "Logging tool versions..."
          command -v sass &> /dev/null && echo "Dart Sass: $(sass --version)" || echo "Dart Sass: not installed"
          command -v hugo &> /dev/null && echo "Hugo: $(hugo version)" || echo "Hugo: not installed"

      - name: Configure Git
        run: |
          echo "Configuring Git..."
          git config --global core.quotepath false

      - name: Fetch full Git history
        run: |
          if [[ $(git rev-parse --is-shallow-repository) == true ]]; then
            echo "Fetching full Git history..."
            git fetch --unshallow
          fi

      - name: Cache restore
        id: cache-restore
        uses: actions/cache/restore@v6
        with:
          path: ${{ runner.temp }}/.cache/hugo
          key: hugo-${{ github.run_id }}
          restore-keys: hugo-

      - name: Build
        run: |
          echo "Building the project..."
          # CHANGED: --source docs/site — the Hugo project lives in a subdirectory
          hugo build \
            --source docs/site \
            --gc \
            --minify \
            --baseURL "${{ steps.pages.outputs.base_url }}/" \
            --cacheDir "${{ runner.temp }}/.cache/hugo"

      - name: Cache save
        uses: actions/cache/save@v6
        with:
          path: ${{ runner.temp }}/.cache/hugo
          key: ${{ steps.cache-restore.outputs.cache-primary-key }}

      - name: Upload artifact
        uses: actions/upload-pages-artifact@v5
        with:
          include-hidden-files: false
          # CHANGED: Hugo writes public/ inside the source directory
          path: docs/site/public
  deploy:
    runs-on: ubuntu-latest
    needs: build
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@v5
```

Steps removed relative to the official file (Go and Node.js install, submodule init, `npm ci`): they are all conditional on files (`go.mod`, `package-lock.json`, `.gitmodules`) this repo's site does not have. Since the theme is a vendored plain directory — not a submodule, not a module — nothing extra is needed to fetch it; `actions/checkout` gets it for free. A zero-diff copy of the official workflow also works; those steps self-skip.

Two supporting notes from the same Hugo docs page:

- Do **not** commit `public/` — add `docs/site/public/` to `.gitignore`.
- The docs recommend pointing the image cache at `cacheDir` in the site config so the Actions cache step covers processed images:

  ```toml
  # docs/site/hugo.toml
  [caches.images]
  dir = ':cacheDir/images'
  ```

- Optional (not in the official file): add a `paths: ['docs/site/**', '.github/workflows/hugo.yaml']` filter under `on.push` so unrelated commits (notebooks, `gbp/`) don't trigger a rebuild. This is a repo-specific choice, not an official recommendation.

## 2. Repository Pages settings and permissions

- **Settings → Pages → Build and deployment → Source: "GitHub Actions".** Both the Hugo docs and GitHub's own docs ("Configuring a publishing source for your GitHub Pages site") state this; the change applies immediately, no save button. GitHub's doc names the same three actions the Hugo workflow uses: `actions/checkout`, `actions/upload-pages-artifact`, `actions/deploy-pages`.
- **Permissions** (verified against the `actions/deploy-pages` README): `pages: write` ("to deploy to Pages") and `id-token: write` ("to verify the deployment originates from an appropriate source") are required. `contents: read` is needed by `actions/checkout` in the build job; the official Hugo workflow sets all three at the top level, as in the YAML above.
- The deploy job must reference the `github-pages` environment (created automatically). The repo is public, so Pages is available on the free plan.

## 3. Does hugo-book need Hugo extended?

**Yes.** The hugo-book README (https://github.com/alex-shpak/hugo-book) requirements section says, verbatim: **"Hugo extended edition, v0.158 or higher."** This is why the workflow above downloads `hugo_extended_0.164.0_linux-amd64.tar.gz` instead of the standard tarball the official Hugo workflow uses. The asset name `hugo_extended_0.164.0_linux-amd64.tar.gz` exists in the v0.164.0 release. This is the one place where copying the official workflow unmodified would break the build (standard edition cannot compile the theme's SCSS with the default libsass transpiler).

## 4. `baseURL` for the project page

For a project page the final base URL is `https://vlzm.github.io/GFDRR/` — the repo name is a path prefix, and every asset/menu link must respect it.

**The official workflow handles this automatically.** The `actions/configure-pages@v6` step (id `pages`) exposes an output `base_url`; its own `action.yml` documents it as "GitHub Pages site full base URL. Examples: `https://octocat.github.io/my-repo`" — i.e. for a project page it includes the repo name, without a trailing slash. The build step passes `--baseURL "${{ steps.pages.outputs.base_url }}/"` (the workflow appends the trailing slash), and a command-line flag overrides `baseURL` in `hugo.toml`. So:

- In CI, the value in `docs/site/hugo.toml` is ignored.
- Still set `baseURL = 'https://vlzm.github.io/GFDRR/'` in `hugo.toml` so local `hugo build` output matches, and local `hugo server` keeps working (it serves from localhost regardless).
- A side benefit: if the repo is ever renamed, `configure-pages` recomputes the URL and CI builds keep working with no workflow edit.

## 5. Pinning the Hugo version

The official workflow pins the version via `HUGO_VERSION: 0.164.0` and downloads that exact release tarball — no floating "latest". As of 2026-07-24, **v0.164.0 (published 2026-07-06) is the latest stable release** (verified via the GitHub releases API), and it is also the version the Hugo docs currently pin. It satisfies hugo-book's minimum (extended v0.158+). Recommendation: pin `0.164.0` extended, bump deliberately.

## 6. Flag for the human: the URL will say "GFDRR"

The public URL will be `https://vlzm.github.io/GFDRR/`. If the repository is renamed later, verified against GitHub's official "Renaming a repository" doc:

- **Git remotes are redirected**: "all `git clone`, `git fetch`, or `git push` operations targeting the previous location will continue to function as if made on the new location."
- **The Pages URL is NOT redirected.** The doc states explicitly: "all existing information, **with the exception of project site URLs**, is automatically redirected to the new name." Anyone holding the old `https://vlzm.github.io/GFDRR/` link gets a 404 after a rename. GitHub's stated mitigation: "we recommend using a custom domain for your site. This ensures that the site's URL isn't impacted by renaming the repository."
- The site itself won't break: the next workflow run rebuilds with the new `base_url` from `configure-pages` (see §4). Only inbound links to the old URL die.

**Practical takeaway:** if the "GFDRR" name bothers you, rename the repo *before* sharing the Pages link anywhere (portfolio, resume, README badges).

## Verification notes

- Workflow YAML: fetched verbatim from the Hugo docs source (`gohugoio/hugoDocs`, `content/en/host-and-deploy/host-on-github-pages/index.md`) — not from a summary.
- One thing not verifiable against an official doc: GitHub's "Configuring a publishing source" page describes the GitHub Actions source and the three actions but does not itself list the `pages: write` / `id-token: write` permissions — those are verified from the `actions/deploy-pages` README instead (and match the Hugo workflow).
- The `--source docs/site` adaptation is the researcher's composition (the Hugo docs page shows no subdirectory example), but `--source` is a documented standard Hugo CLI flag, and the `configure-pages` `base_url` output format for project pages is confirmed from that action's `action.yml`.

Sources:

- [Hugo docs — Host on GitHub Pages](https://gohugo.io/host-and-deploy/host-on-github-pages/) (YAML fetched from the [docs source](https://raw.githubusercontent.com/gohugoio/hugoDocs/master/content/en/host-and-deploy/host-on-github-pages/index.md))
- [hugo-book README](https://github.com/alex-shpak/hugo-book)
- [GitHub docs — Configuring a publishing source](https://docs.github.com/en/pages/getting-started-with-github-pages/configuring-a-publishing-source-for-your-github-pages-site)
- [GitHub docs — Renaming a repository](https://docs.github.com/en/repositories/creating-and-managing-repositories/renaming-a-repository)
- [actions/deploy-pages README](https://github.com/actions/deploy-pages)
- [actions/configure-pages action.yml](https://raw.githubusercontent.com/actions/configure-pages/main/action.yml)
- [Hugo v0.164.0 release (GitHub API)](https://api.github.com/repos/gohugoio/hugo/releases/latest)
