"""Parquet repository for Spark-based read/write operations."""

from typing import Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from errors.infra_errors import StorageMedia, StorageReadError, StorageWriteError
from utils.decorators import log_execution_time, retry
from utils.logger import get_logger

_log = get_logger(__name__)


class ParquetRepository:
	"""Reads and writes Parquet files via Spark with Snappy compression."""

	def __init__(self, spark: SparkSession) -> None:
		"""Initializes the repository.

		Args:
			spark: Active SparkSession.
		"""
		self._spark = spark

	@retry(attempts=3, delay_seconds=2.0)
	@log_execution_time
	def read(self, path: str, schema: StructType | None = None) -> DataFrame:
		"""Reads a Parquet dataset into a DataFrame.

		Args:
			path: Parquet directory path.
			schema: Optional explicit schema (rarely needed for Parquet).

		Returns:
			Loaded DataFrame.

		Raises:
			StorageReadError: If the read operation fails.
		"""
		try:
			_log.info("parquet_read", path=path)
			reader = self._spark.read
			if schema is not None:
				reader = reader.schema(schema)
			return reader.parquet(path)
		except Exception as exc:
			raise StorageReadError(str(exc), StorageMedia.PARQUET, path) from exc

	@log_execution_time
	def write(
		self,
		dataframe: DataFrame,
		path: str,
		mode: Literal["overwrite", "append", "ignore", "error"] = "overwrite",
	) -> None:
		"""Writes a DataFrame to Parquet using Snappy compression.

		Args:
			dataframe: DataFrame to persist.
			path: Destination directory.
			mode: Write mode.

		Raises:
			StorageWriteError: If the write operation fails.
		"""
		try:
			_log.info("parquet_write", path=path, mode=mode)
			dataframe.write.mode(mode).option("compression", "snappy").parquet(path)
		except Exception as exc:
			raise StorageWriteError(str(exc), StorageMedia.PARQUET, path) from exc
