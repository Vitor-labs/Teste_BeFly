"""Gold view: revenue and occupancy aggregated by hotel × year × month."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, when
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
def build_revenue_by_hotel_month(spark: SparkSession) -> None:
	"""Builds the `revenue_by_hotel_month` Gold view.

	Answers: "How much does each hotel earn month by month?"

	Args:
		spark: Active SparkSession.

	Raises:
		GoldAggregationError: If aggregation or persistence fails.
	"""
	parquet_repo = ParquetRepository(spark)
	source = str(settings.silver_dir / "bookings_enriched")
	target = str(settings.gold_dir / "revenue_by_hotel_month")

	try:
		aggregated = (
			parquet_repo.read(source)
			.groupBy("hotel", "arrival_date_year", "arrival_date_month_num")
			.agg(
				count("*").alias("total_bookings"),
				spark_sum(when(col("is_canceled") == 0, 1).otherwise(0)).alias(
					"effective_bookings"
				),
				spark_sum("is_canceled").alias("cancelled_bookings"),
				spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
				spark_round(avg(when(col("is_canceled") == 0, col("adr"))), 2).alias(
					"avg_adr"
				),
				spark_sum(
					when(col("is_canceled") == 0, col("total_nights")).otherwise(0)
				).alias("total_nights_sold"),
			)
			.withColumn(
				"cancellation_rate",
				spark_round(
					cancellation_rate("cancelled_bookings", "total_bookings"), 4
				),
			)
			.orderBy("hotel", "arrival_date_year", "arrival_date_month_num")
		)
		_log.info("gold_revenue_by_hotel_month_built", row_count=aggregated.count())
		parquet_repo.write(aggregated, target, mode=WriteMode.OVERWRITE)
	except Exception as exc:
		raise GoldAggregationError(f"revenue_by_hotel_month failed: {exc}") from exc
