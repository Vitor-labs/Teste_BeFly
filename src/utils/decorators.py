"""Reusable decorators: retry and execution timing."""

import time
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

from utils.logger import get_logger

P = ParamSpec("P")
R = TypeVar("R")

_log = get_logger(__name__)


def retry(
	*,
	attempts: int = 3,
	delay_seconds: float = 1.0,
	exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[P, R]], Callable[P, R]]:
	"""Retries a callable on listed exceptions with linear backoff.

	Args:
		attempts: Maximum number of attempts (must be >= 1).
		delay_seconds: Delay between attempts.
		exceptions: Exception types that trigger a retry.

	Returns:
		A decorator wrapping the target callable with retry logic.

	Raises:
		ValueError: If `attempts` is less than 1.
	"""
	if attempts < 1:
		raise ValueError("attempts must be >= 1")

	def decorator(func: Callable[P, R]) -> Callable[P, R]:
		@wraps(func)
		def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
			last_error: Exception | None = None
			for attempt in range(1, attempts + 1):
				try:
					return func(*args, **kwargs)
				except exceptions as exc:
					last_error = exc
					_log.warning(
						"retry_attempt_failed",
						function=func.__name__,
						attempt=attempt,
						max_attempts=attempts,
						error=str(exc),
					)
					if attempt < attempts:
						time.sleep(delay_seconds)
			assert last_error is not None
			raise last_error

		return wrapper

	return decorator


def log_execution_time(func: Callable[P, R]) -> Callable[P, R]:
	"""Logs how long a function takes to execute.

	Args:
		func: Function to be wrapped.

	Returns:
		Wrapped function that emits a structured timing log.
	"""

	@wraps(func)
	def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
		start = time.perf_counter()
		result = func(*args, **kwargs)
		_log.info(
			"execution_time",
			function=func.__name__,
			elapsed_seconds=round(time.perf_counter() - start, 3),
		)
		return result

	return wrapper
