"""Logging setup for gbp: structlog, configured once by the entry point."""

import logging

import structlog


def configure_logging(level: int = logging.INFO) -> None:
    """Configure structlog for console output. Call once at the entry point."""
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
