"""Tests for Gold-layer KPI helpers."""

from pyspark.sql import SparkSession

from stages.gold.rules import cancellation_rate, percentage_of


def test_cancellation_rate_handles_zero_total(spark: SparkSession) -> None:
	df = spark.createDataFrame([(0, 0), (5, 10)], ["cancelled", "total"])
	rows = df.withColumn("rate", cancellation_rate("cancelled", "total")).collect()
	assert rows[0]["rate"] == 0.0
	assert rows[1]["rate"] == 0.5


def test_percentage_of_handles_zero_denominator(spark: SparkSession) -> None:
	df = spark.createDataFrame([(0, 0), (3, 4)], ["num", "den"])
	rows = df.withColumn("pct", percentage_of("num", "den")).collect()
	assert rows[0]["pct"] == 0.0
	assert rows[1]["pct"] == 0.75
