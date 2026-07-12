"""Logging setup for gbp: structlog, configured once by the entry point.

The gbp package only emits log events (``structlog.get_logger(__name__)``);
it never configures the output itself. The entry point -- the notebook's
first cell, ``app/runner.py`` -- calls :func:`configure_logging` once to pick
the level and the output format. Without that call structlog uses its own
defaults, which also print readable lines, so the package works unconfigured.

Two conventions keep the log consistent:

- An event name states a fact in the past tense, with the words of
  Notations.md: ``sizing_run_started``, ``run_invariants_checked``.
- Context that holds over many lines (``scenario_id``, ``period_id``) is
  bound once with ``structlog.contextvars``; every line logged inside the
  bound block carries it automatically.
"""

import logging

import structlog


def configure_logging(level: int = logging.INFO) -> None:
    """Configure structlog for console output. Call once at the entry point.

    Parameters
    ----------
    level : int, optional
        Smallest level that gets printed, as a stdlib ``logging`` constant.
        ``logging.INFO`` (default) shows the stage boundaries and a progress
        line every 50 periods; ``logging.DEBUG`` adds one line per phase per
        period.
    """
    # cache_logger_on_first_use stays off on purpose: with the cache on, a
    # second configure_logging(logging.DEBUG) call in the same session (the
    # normal way to raise the level in a notebook) would have no effect,
    # because every already-used logger keeps its first configuration. The
    # simulator logs a few lines per period, so the cache buys nothing here.
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="%H:%M:%S", utc=False),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        cache_logger_on_first_use=False,
    )
