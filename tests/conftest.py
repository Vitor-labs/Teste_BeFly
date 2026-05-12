"""Shared pytest fixtures for stage tests."""

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
	"""Provides a lightweight SparkSession for the entire test session."""
	session = (
		SparkSession.builder.master("local[1]")
		.appName("befly-tests")
		.config("spark.sql.shuffle.partitions", "2")
		.config("spark.ui.enabled", "false")
		.getOrCreate()
	)
	yield session
	session.stop()
