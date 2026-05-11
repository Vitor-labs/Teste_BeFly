"""Explicit schema for the `country_metadata.csv` reference table."""

from pyspark.sql.types import StringType, StructField, StructType

countries_bronze_schema = StructType(
	[
		StructField("country_code", StringType(), nullable=False),
		StructField("country_name", StringType(), nullable=True),
		StructField("continent", StringType(), nullable=True),
	]
)
