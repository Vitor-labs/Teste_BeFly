"""Writer contract for any storage backend."""

import enum
from typing import Protocol

from pyspark.sql import DataFrame


class WriteMode(enum.Enum):
	OVERWRITE = "overwrite"
	APPEND = "append"
	IGNORE = "ignore"
	ERROR = "error"


class IWriter(Protocol):
	"""Protocol for components that persist a Spark DataFrame."""

	def write(
		self,
		dataframe: DataFrame,
		path: str,
		mode: WriteMode = WriteMode.OVERWRITE,
	) -> None:
		"""Persists `dataframe` to `path`.

		Args:
			dataframe: DataFrame to be written.
			path: Destination path.
			mode: Write mode.
		"""
		...
