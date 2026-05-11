"""Bronze ingestion of the country metadata reference table."""

from pyspark.sql import SparkSession

from errors.layer_errors import BronzeIngestionError
from infra.repositories.csv_repository import CsvRepository
from infra.repositories.parquet_repository import ParquetRepository
from main.config import settings
from schemas.bronze.countries_schema import countries_bronze_schema
from stages.bronze.rules import normalize_string_columns
from utils.decorators import log_execution_time
from utils.logger import get_logger

_log = get_logger(__name__)


@log_execution_time
def ingest_countries(spark: SparkSession) -> None:
	"""Reads `country_metadata.csv` and persists it as Bronze Parquet.

	Args:
		spark: Active SparkSession.

	Raises:
		BronzeIngestionError: If reading or writing fails.
	"""
	try:
		dataframe = normalize_string_columns(
			CsvRepository(spark).read(
				str(settings.raw_dir / "country_metadata.csv"),
				schema=countries_bronze_schema,
			)
		)
		_log.info("bronze_countries_loaded", row_count=dataframe.count())
		ParquetRepository(spark).write(
			dataframe, str(settings.bronze_dir / "countries"), mode="overwrite"
		)
	except Exception as exc:
		raise BronzeIngestionError(f"countries ingestion failed: {exc}") from exc
