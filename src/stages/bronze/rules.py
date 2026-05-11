"""Bronze-layer rules: light normalization that preserves raw semantics."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import trim


def normalize_string_columns(dataframe: DataFrame) -> DataFrame:
	"""Trims whitespace from every string column in the DataFrame.

	Args:
		dataframe: Input DataFrame.

	Returns:
		DataFrame with string columns trimmed.
	"""
	for column in [
		field.name
		for field in dataframe.schema.fields
		if field.dataType.simpleString() == "string"
	]:
		dataframe = dataframe.withColumn(column, trim(dataframe[column]))
	return dataframe
