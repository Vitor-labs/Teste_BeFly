"""Bronze ingestion of the hotel bookings dataset."""

from pandas import read_csv
from pyspark.sql import SparkSession

from errors.layer_errors import BronzeIngestionError
from infra.repositories.csv_repository import CsvRepository
from infra.repositories.parquet_repository import ParquetRepository
from main.config import settings
from schemas.bronze.bookings_schema import bookings_bronze_schema
from stages.bronze.rules import normalize_string_columns
from utils.decorators import log_execution_time
from utils.logger import get_logger

_log = get_logger(__name__)


@log_execution_time
def ingest_bookings(spark: SparkSession) -> None:
	"""Reads `hotel_bookings.csv` and persists it as Bronze Parquet.
	Downloads the file if it does not exist locally.

	Args:
		spark: Active SparkSession.

	Raises:
		BronzeIngestionError: If reading or writing fails.
	"""
	try:
		if not (csv_path := settings.raw_dir / "hotel_bookings.csv").exists():
			_log.info(
				"csv_not_found_downloading",
				url=settings.hotel_bookings_origin,
				destination=str(csv_path),
			)
			csv_path.parent.mkdir(parents=True, exist_ok=True)

			read_csv(settings.hotel_bookings_origin).to_csv(str(csv_path), index=False)
			_log.info("csv_download_complete")

		dataframe = normalize_string_columns(
			CsvRepository(spark).read(
				str(settings.raw_dir / "hotel_bookings.csv"),
				schema=bookings_bronze_schema,
			)
		)
		ParquetRepository(spark).write(
			dataframe, str(settings.bronze_dir / "bookings"), mode="overwrite"
		)
		_log.info("bronze_bookings_loaded", row_count=dataframe.count())
	except Exception as exc:
		raise BronzeIngestionError(f"bookings ingestion failed: {exc}") from exc
