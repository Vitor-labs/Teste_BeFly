"""Gold-layer business rules: KPI formulas and aggregation helpers."""

from pyspark.sql import Column
from pyspark.sql.functions import col, when


def cancellation_rate(cancelled_col: str, total_col: str) -> Column:
	"""Builds a safe cancellation-rate expression.

	Args:
		cancelled_col: Column name with cancelled bookings count.
		total_col: Column name with total bookings count.

	Returns:
		Spark column expression (0 when total = 0).
	"""
	return when(col(total_col) == 0, 0.0).otherwise(col(cancelled_col) / col(total_col))


def percentage_of(numerator_col: str, denominator_col: str) -> Column:
	"""Builds a generic safe percentage expression.

	Args:
		numerator_col: Numerator column name.
		denominator_col: Denominator column name.

	Returns:
		Spark column expression (0 when denominator = 0).
	"""
	return when(col(denominator_col) == 0, 0.0).otherwise(
		col(numerator_col) / col(denominator_col)
	)
