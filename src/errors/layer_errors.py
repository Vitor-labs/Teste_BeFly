"""Custom exceptions for Bronze/Silver/Gold layer failures."""

from enum import Enum


class PipelineLayer(Enum):
	"""Logical layers of the Medallion pipeline."""

	BRONZE = "Bronze"
	SILVER = "Silver"
	GOLD = "Gold"


class LayerError(Exception):
	"""Base class for all pipeline layer errors."""

	layer: PipelineLayer

	def __init__(self, message: str) -> None:
		"""Initializes the base layer error.

		Args:
			message: Human-readable error description.
		"""
		super().__init__(f"[{self.layer.value}] {message}")
		self.message = message
		self.error_type = self.__class__.__name__


class BronzeIngestionError(LayerError):
	"""Raised when Bronze ingestion fails."""

	layer = PipelineLayer.BRONZE


class SilverTransformationError(LayerError):
	"""Raised when Silver transformations fail."""

	layer = PipelineLayer.SILVER


class GoldAggregationError(LayerError):
	"""Raised when Gold aggregations fail."""

	layer = PipelineLayer.GOLD


class SchemaValidationError(LayerError):
	"""Raised when a Pandera schema validation fails."""

	def __init__(self, message: str, layer: PipelineLayer) -> None:
		"""Initializes a schema validation error.

		Args:
			message: Validation failure description.
			layer: Layer where the validation was running.
		"""
		self.layer = layer
		super().__init__(f"Schema validation failed: {message}")
