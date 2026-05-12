"""Pipeline entrypoint: orchestrates Bronze → Silver → Gold."""

from errors.infra_errors import InfraError
from errors.layer_errors import LayerError
from infra.spark.session_factory import build_spark_session
from stages.bronze.ingest_bookings import ingest_bookings
from stages.bronze.ingest_countries import ingest_countries
from stages.bronze.ingest_hotels import ingest_hotels
from stages.gold.cancellation_by_segment import build_cancellation_by_segment
from stages.gold.revenue_by_hotel_month import build_revenue_by_hotel_month
from stages.gold.top_countries_by_revenue import build_top_countries_by_revenue
from stages.silver.build_bookings_enriched import build_bookings_enriched
from utils.logger import configure_logging, get_logger


def main() -> None:
	"""Runs the full Medallion pipeline end-to-end."""
	configure_logging()
	log = get_logger(__name__)
	log.info("pipeline_started")

	spark = build_spark_session()
	try:
		# Bronze
		ingest_bookings(spark)
		ingest_countries(spark)
		ingest_hotels(spark)
		# Silver
		build_bookings_enriched(spark)
		# Gold
		build_revenue_by_hotel_month(spark)
		build_cancellation_by_segment(spark)
		build_top_countries_by_revenue(spark)

		log.info("pipeline_finished")
	except (LayerError, InfraError) as exc:
		log.error("pipeline_failed", error_type=exc.error_type, message=str(exc))
		raise
	finally:
		spark.stop()
