"""Reader contract for any storage backend."""

from typing import Protocol

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


class IReader(Protocol):
	"""Protocol for components that read data into a Spark DataFrame."""

	def read(self, path: str, schema: StructType | None = None) -> DataFrame:
		"""Reads data from `path` and returns a Spark DataFrame.

		Args:
			path: Source path.
			schema: Optional explicit schema.

		Returns:
			Loaded Spark DataFrame.
		"""
		...
