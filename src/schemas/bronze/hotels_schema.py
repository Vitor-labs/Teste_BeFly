"""Explicit schema for the `hotel_metadata.csv` reference table."""

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

hotels_bronze_schema = StructType(
	[
		StructField("hotel_name", StringType(), nullable=False),
		StructField("city", StringType(), nullable=True),
		StructField("country_code", StringType(), nullable=True),
		StructField("star_rating", IntegerType(), nullable=True),
		StructField("opened_year", IntegerType(), nullable=True),
	]
)
