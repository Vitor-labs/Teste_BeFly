"""Structlog configuration for the pipeline."""

import logging

import structlog


def configure_logging(level: int = logging.INFO) -> None:
	"""Configures structlog with sane defaults for pipeline runs.

	Args:
		level: Standard logging level threshold.
	"""
	structlog.configure(
		processors=[
			structlog.contextvars.merge_contextvars,
			structlog.processors.add_log_level,
			structlog.processors.TimeStamper(fmt="iso"),
			structlog.dev.ConsoleRenderer(),
		],
		wrapper_class=structlog.make_filtering_bound_logger(level),
		cache_logger_on_first_use=True,
	)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
	"""Returns a structlog logger bound to the given name.

	Args:
		name: Logger name (typically `__name__`).

	Returns:
		A configured structlog logger.
	"""
	return structlog.get_logger(name)  # type: ignore
