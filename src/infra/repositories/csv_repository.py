"""CSV repository for Spark-based read operations."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from errors.infra_errors import StorageMedia, StorageReadError, StorageWriteError
from infra.interfaces.i_reader import IReader
from infra.interfaces.i_writer import IWriter, WriteMode
from main.config import settings
from utils.decorators import log_execution_time, retry
from utils.logger import get_logger

_log = get_logger(__name__)


class CsvRepository(IReader, IWriter):
	"""Reads and writes CSV files via Spark."""

	def __init__(self, spark: SparkSession) -> None:
		"""Initializes the repository.

		Args:
			spark: Active SparkSession.
		"""
		self._spark = spark

	@retry(attempts=3, delay_seconds=2.0)
	@log_execution_time
	def read(self, path: str, schema: StructType | None = None) -> DataFrame:
		"""Reads a CSV file into a Spark DataFrame.

		Treats `settings.null_token` literal as NULL.

		Args:
			path: CSV file path.
			schema: Optional explicit schema. When provided, `inferSchema` is disabled.

		Returns:
			Loaded DataFrame.

		Raises:
			StorageReadError: If the read operation fails.
		"""
		try:
			reader = (
				self._spark.read.option("header", "true")
				.option("sep", ",")
				.option("nullValue", settings.null_token)
			)
			if schema is not None:
				_log.info("csv_read_with_schema", path=path)
				return reader.schema(schema).csv(path)

			_log.info("csv_read_inferred", path=path)
			return reader.option("inferSchema", "true").csv(path)
		except Exception as exc:
			raise StorageReadError(str(exc), StorageMedia.CSV, path) from exc

	@log_execution_time
	def write(
		self, dataframe: DataFrame, path: str, mode: WriteMode = WriteMode.OVERWRITE
	) -> None:
		"""Writes a DataFrame back as CSV (used for Gold KPIs if needed).

		Args:
			dataframe: DataFrame to persist.
			path: Destination path.
			mode: Write mode.

		Raises:
			StorageWriteError: If the write operation fails.
		"""
		try:
			dataframe.write.mode(mode.value).option("header", "true").csv(path)
		except Exception as exc:
			raise StorageWriteError(str(exc), StorageMedia.CSV, path) from exc
