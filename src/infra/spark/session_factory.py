"""Factory for building a SparkSession used across the pipeline."""

from pyspark.sql import SparkSession

from errors.infra_errors import SparkSessionError
from main.config import settings


def build_spark_session() -> SparkSession:
	"""Builds (or retrieves) a SparkSession for the pipeline."""
	try:
		spark = (
			SparkSession.builder.appName(settings.spark_app_name)
			.master(settings.spark_master)
			.config("spark.sql.parquet.compression.codec", "snappy")
			.config("spark.sql.legacy.timeParserPolicy", "LEGACY")
			.getOrCreate()
		)
		# Garante que aplica mesmo se a sessão já existia
		spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
		return spark
	except Exception as exc:
		raise SparkSessionError(f"Could not create SparkSession: {exc}") from exc
