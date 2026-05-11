"""Custom exceptions for infrastructure-level failures."""

from enum import Enum


class StorageMedia(Enum):
	"""Supported storage media for read/write operations."""

	CSV = "CSV"
	PARQUET = "Parquet"


class InfraError(Exception):
	"""Base class for all infrastructure errors."""

	def __init__(self, message: str) -> None:
		"""Initializes the base infra error.

		Args:
			message: Human-readable error description.
		"""
		super().__init__(message)
		self.message = message
		self.error_type = self.__class__.__name__


class SparkSessionError(InfraError):
	"""Raised when SparkSession creation or configuration fails."""


class StorageReadError(InfraError):
	"""Raised when a read operation against a storage medium fails."""

	def __init__(self, message: str, media: StorageMedia, path: str) -> None:
		"""Initializes a storage read error.

		Args:
			message: Human-readable error description.
			media: Storage medium where the failure happened.
			path: Path that was being read.
		"""
		super().__init__(f"[{media.value}] Failed reading '{path}': {message}")
		self.media = media
		self.path = path


class StorageWriteError(InfraError):
	"""Raised when a write operation against a storage medium fails."""

	def __init__(self, message: str, media: StorageMedia, path: str) -> None:
		"""Initializes a storage write error.

		Args:
			message: Human-readable error description.
			media: Storage medium where the failure happened.
			path: Path that was being written.
		"""
		super().__init__(f"[{media.value}] Failed writing '{path}': {message}")
		self.media = media
		self.path = path
