"""Tests for Bronze-layer rules."""

from pyspark.sql import SparkSession

from src.stages.bronze.rules import normalize_string_columns


def test_normalize_string_columns_trims_whitespace(spark: SparkSession) -> None:
	assert {
		row["hotel"]
		for row in normalize_string_columns(
			spark.createDataFrame(
				[("  Resort Hotel  ", 1), (" City Hotel", 2)], ["hotel", "id"]
			)
		).collect()
	} == {"Resort Hotel", "City Hotel"}


def test_normalize_string_columns_keeps_non_string_intact(spark: SparkSession) -> None:
	row = normalize_string_columns(
		spark.createDataFrame([(" abc ", 42)], ["name", "value"])
	).first()
	assert row is not None
	assert row["name"] == "abc"
	assert row["value"] == 42
