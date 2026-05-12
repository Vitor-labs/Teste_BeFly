"""Builds the Silver `bookings_enriched` dataset from Bronze sources."""

from pyspark.sql import SparkSession

from errors.layer_errors import (
	PipelineLayer,
	SchemaValidationError,
	SilverTransformationError,
)
from infra.interfaces.i_writer import WriteMode
from infra.repositories.parquet_repository import ParquetRepository
from main.config import settings
from schemas.silver.bookings_enriched_schema import bookings_enriched_schema
from stages.silver.rules import (
	add_arrival_date,
	add_derived_columns,
	cast_columns,
	enrich_with_countries,
	enrich_with_hotels,
	fill_nulls,
	filter_invalid_bookings,
)
from utils.decorators import log_execution_time
from utils.logger import get_logger

_log = get_logger(__name__)


@log_execution_time
def build_bookings_enriched(spark: SparkSession) -> None:
	"""Reads Bronze datasets, applies Silver rules, validates and persists.

	Args:
		spark: Active SparkSession.

	Raises:
		SilverTransformationError: If any transformation step fails.
		SchemaValidationError: If the resulting DataFrame fails Pandera validation.
	"""
	parquet_repo = ParquetRepository(spark)
	target = str(settings.silver_dir / "bookings_enriched")

	try:
		bookings = parquet_repo.read(str(settings.bronze_dir / "bookings"))
		countries = parquet_repo.read(str(settings.bronze_dir / "countries"))
		hotels = parquet_repo.read(str(settings.bronze_dir / "hotels"))

		typed = cast_columns(bookings)
		filled = fill_nulls(typed)
		filtered, removal_counts = filter_invalid_bookings(filled)
		_log.info("silver_filters_applied", **removal_counts)

		dated = add_arrival_date(filtered)
		derived = add_derived_columns(dated)
		with_countries = enrich_with_countries(derived, countries)
		enriched = enrich_with_hotels(with_countries, hotels)

		_log.info("silver_enriched_built", row_count=enriched.count())
	except Exception as exc:
		raise SilverTransformationError(f"transformation failed: {exc}") from exc

	validated = _validate(enriched)
	parquet_repo.write(validated, target, mode=WriteMode.OVERWRITE)


def _validate(dataframe):
	"""Runs Pandera validation against the Silver schema.

	Args:
		dataframe: Enriched DataFrame to validate.

	Returns:
		The validated DataFrame.

	Raises:
		SchemaValidationError: If validation reports any errors.
	"""
	validated = bookings_enriched_schema.validate(dataframe, lazy=True)
	if errors := getattr(validated.pandera, "errors", None):
		raise SchemaValidationError(str(errors), PipelineLayer.SILVER)
	_log.info("silver_schema_validated")
	return validated
