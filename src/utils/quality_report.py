"""Generates a JSON data quality report for a Spark DataFrame."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, isnan, when

from utils.logger import get_logger

_log = get_logger(__name__)


def generate_quality_report(
	dataframe: DataFrame,
	output_path: Path,
	*,
	dataset_name: str,
	extra_metrics: dict[str, Any] | None = None,
) -> None:
	"""Computes data-quality metrics and writes them as JSON.

	Args:
		dataframe: DataFrame to profile.
		output_path: Destination JSON path.
		dataset_name: Logical dataset name for the report header.
		extra_metrics: Optional extra metrics to merge into the report.
	"""
	total_rows = dataframe.count()
	null_counts = _null_counts_per_column(dataframe, total_rows)

	report: dict[str, Any] = {
		"dataset": dataset_name,
		"generated_at": datetime.now(timezone.utc).isoformat(),
		"total_rows": total_rows,
		"total_columns": len(dataframe.columns),
		"null_ratio_per_column": null_counts,
		"schema": [
			{"name": field.name, "type": field.dataType.simpleString()}
			for field in dataframe.schema.fields
		],
	}
	if extra_metrics:
		report["extra"] = extra_metrics

	output_path.parent.mkdir(parents=True, exist_ok=True)
	output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))
	_log.info("quality_report_written", path=str(output_path), rows=total_rows)


def _null_counts_per_column(dataframe: DataFrame, total_rows: int) -> dict[str, float]:
	"""Computes the null ratio per column.

	Args:
		dataframe: DataFrame to analyze.
		total_rows: Pre-computed row count to avoid re-scanning.

	Returns:
		Mapping of column name to null ratio (0.0 to 1.0).
	"""
	if total_rows == 0:
		return {column: 0.0 for column in dataframe.columns}

	expressions = [
		count(
			when(
				col(column).isNull()
				| (
					isnan(col(column))
					if _is_numeric(dataframe, column)
					else col(column).isNull()
				),
				column,
			)
		).alias(column)
		for column in dataframe.columns
	]
	row = dataframe.select(*expressions).first()
	assert row is not None, "isso aqui não é pra dá raise, é só pro mypy não encher"
	return {column: round(row[column] / total_rows, 4) for column in dataframe.columns}


def _is_numeric(dataframe: DataFrame, column: str) -> bool:
	"""Returns True if the column has a numeric Spark type."""
	return dict(dataframe.dtypes)[column] in {"int", "bigint", "double", "float"}
