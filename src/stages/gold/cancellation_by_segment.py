"""Gold view: cancellation analysis by market segment / channel / customer type."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum

from errors.layer_errors import GoldAggregationError
from infra.interfaces.i_writer import WriteMode
from infra.repositories.parquet_repository import ParquetRepository
from main.config import settings
from stages.gold.rules import cancellation_rate
from utils.decorators import log_execution_time
from utils.logger import get_logger

_log = get_logger(__name__)


@log_execution_time
def build_cancellation_by_segment(spark: SparkSession) -> None:
	"""Builds the `cancellation_by_segment` Gold view.

	Answers: "Which segments / channels cancel most? How early do they book?"

	Args:
		spark: Active SparkSession.

	Raises:
		GoldAggregationError: If aggregation or persistence fails.
	"""
	parquet_repo = ParquetRepository(spark)
	source = str(settings.silver_dir / "bookings_enriched")
	target = str(settings.gold_dir / "cancellation_by_segment")

	try:
		aggregated = (
			parquet_repo.read(source)
			.groupBy("market_segment", "distribution_channel", "customer_type")
			.agg(
				count("*").alias("total_bookings"),
				spark_sum("is_canceled").alias("cancelled_bookings"),
				spark_round(avg("lead_time"), 2).alias("avg_lead_time"),
				spark_round(avg("total_of_special_requests"), 2).alias(
					"avg_special_requests"
				),
			)
			.withColumn(
				"cancellation_rate",
				spark_round(
					cancellation_rate("cancelled_bookings", "total_bookings"), 4
				),
			)
			.orderBy(
				spark_round(
					cancellation_rate("cancelled_bookings", "total_bookings"), 4
				).desc()
			)
		)
		_log.info("gold_cancellation_by_segment_built", row_count=aggregated.count())
		parquet_repo.write(aggregated, target, mode=WriteMode.OVERWRITE)
	except Exception as exc:
		raise GoldAggregationError(f"cancellation_by_segment failed: {exc}") from exc
