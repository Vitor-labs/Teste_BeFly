"""Writer contract for any storage backend."""

from typing import Literal, Protocol

from pyspark.sql import DataFrame


class IWriter(Protocol):
	"""Protocol for components that persist a Spark DataFrame."""

	def write(
		self,
		dataframe: DataFrame,
		path: str,
		mode: Literal["overwrite", "append", "ignore", "error"] = "overwrite",
	) -> None:
		"""Persists `dataframe` to `path`.

		Args:
			dataframe: DataFrame to be written.
			path: Destination path.
			mode: Write mode.
		"""
		...
