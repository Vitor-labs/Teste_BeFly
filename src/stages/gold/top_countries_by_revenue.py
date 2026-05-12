"""Gold view: Top 20 countries ranked by total effective revenue."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum

from errors.layer_errors import GoldAggregationError
from infra.interfaces.i_writer import WriteMode
from infra.repositories.parquet_repository import ParquetRepository
from main.config import settings
from utils.decorators import log_execution_time
from utils.logger import get_logger

_TOP_N: int = 20
_log = get_logger(__name__)


@log_execution_time
def build_top_countries_by_revenue(spark: SparkSession) -> None:
	"""Builds the `top_countries_by_revenue` Gold view.

	Answers: "Which countries originate the most valuable guests?"

	Args:
		spark: Active SparkSession.

	Raises:
		GoldAggregationError: If aggregation or persistence fails.
	"""
	parquet_repo = ParquetRepository(spark)
	source = str(settings.silver_dir / "bookings_enriched")
	target = str(settings.gold_dir / "top_countries_by_revenue")

	try:
		aggregated = (
			parquet_repo.read(source)
			.filter(col("is_canceled") == 0)
			.groupBy("country", "country_name", "continent")
			.agg(
				count("*").alias("effective_bookings"),
				spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
				spark_round(avg("revenue"), 2).alias("avg_ticket"),
				spark_round(avg("lead_time"), 2).alias("avg_lead_time"),
			)
			.orderBy(col("total_revenue").desc())
			.limit(_TOP_N)
		)
		_log.info("gold_top_countries_by_revenue_built", top_n=_TOP_N)
		parquet_repo.write(aggregated, target, mode=WriteMode.OVERWRITE)
	except Exception as exc:
		raise GoldAggregationError(f"top_countries_by_revenue failed: {exc}") from exc
