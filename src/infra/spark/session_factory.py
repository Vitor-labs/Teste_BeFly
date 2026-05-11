"""Factory for building a SparkSession used across the pipeline."""

from pyspark.sql import SparkSession

from errors.infra_errors import SparkSessionError
from main.config import settings


def build_spark_session() -> SparkSession:
	"""Builds (or retrieves) a SparkSession for the pipeline.

	Returns:
		An active SparkSession.

	Raises:
		SparkSessionError: If the SparkSession cannot be created.
	"""
	try:
		return (
			SparkSession.builder.appName(settings.spark_app_name)
			.master(settings.spark_master)
			.config("spark.sql.parquet.compression.codec", "snappy")
			.getOrCreate()
		)
	except Exception as exc:
		raise SparkSessionError(f"Could not create SparkSession: {exc}") from exc
