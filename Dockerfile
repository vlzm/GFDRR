FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /srv

# Install dependencies from the lock file first, in their own layer.
# Code changes then reuse this layer instead of reinstalling everything.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --extra ui

# README.md is required to build the package (pyproject.toml points to it).
COPY README.md ./
COPY gbp/ gbp/
COPY app/ app/
RUN uv sync --frozen --extra ui

ENV PATH="/srv/.venv/bin:$PATH"

EXPOSE 8501
CMD ["streamlit", "run", "app/main.py", \
     "--server.port=8501", "--server.address=0.0.0.0"]
