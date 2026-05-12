"""Tests for Silver-layer business rules."""

from datetime import date

from pyspark.sql import SparkSession

from src.stages.silver.rules import (
	add_arrival_date,
	add_derived_columns,
	enrich_with_countries,
	enrich_with_hotels,
	fill_nulls,
	filter_invalid_bookings,
)


def test_fill_nulls_replaces_children_and_country(spark: SparkSession) -> None:
	df = spark.createDataFrame([(None, None), (2, "PRT")], ["children", "country"])
	rows = fill_nulls(df).collect()
	assert rows[0]["children"] == 0
	assert rows[0]["country"] == "UNK"
	assert rows[1]["country"] == "PRT"


def test_filter_invalid_bookings_removes_zero_guests_and_negative_adr(
	spark: SparkSession,
) -> None:
	df = spark.createDataFrame(
		[
			(2, 0, 0, 100.0),  # válido
			(0, 0, 0, 100.0),  # sem hóspedes
			(1, 0, 0, -50.0),  # adr negativo
		],
		["adults", "children", "babies", "adr"],
	)
	filtered, counts = filter_invalid_bookings(df)
	assert filtered.count() == 1
	assert counts == {"no_guests_removed": 1, "negative_adr_removed": 1}


def test_add_arrival_date_builds_date_column(spark: SparkSession) -> None:
	df = spark.createDataFrame(
		[(2016, "July", 5)],
		["arrival_date_year", "arrival_date_month", "arrival_date_day_of_month"],
	)
	row = add_arrival_date(df).first()
	assert row is not None
	assert row["arrival_date_month_num"] == 7
	assert row["arrival_date"] == date(2016, 7, 5)


def test_add_derived_columns_revenue_zero_when_canceled(spark: SparkSession) -> None:
	df = spark.createDataFrame(
		[
			(1, 100.0, 2, 3, 2, 1, 0, "Canceled"),
			(0, 100.0, 2, 3, 2, 0, 0, "Check-Out"),
		],
		[
			"is_canceled",
			"adr",
			"stays_in_weekend_nights",
			"stays_in_week_nights",
			"adults",
			"children",
			"babies",
			"reservation_status",
		],
	)
	rows = add_derived_columns(df).collect()
	assert rows[0]["revenue"] == 0.0
	assert rows[0]["booking_status"] == "Canceled"
	assert rows[1]["revenue"] == 500.0
	assert rows[1]["total_nights"] == 5
	assert rows[1]["is_family"] == 1
	assert rows[1]["booking_status"] == "CheckedOut"


def test_enrich_with_countries_left_join(spark: SparkSession) -> None:
	bookings = spark.createDataFrame([("PRT",), ("ZZZ",)], ["country"])
	countries = spark.createDataFrame(
		[("PRT", "Portugal", "Europe")], ["country_code", "country_name", "continent"]
	)
	rows = enrich_with_countries(bookings, countries).collect()
	assert rows[0]["country_name"] == "Portugal"
	assert rows[1]["country_name"] is None  # LEFT join preserva linha


def test_enrich_with_hotels_brings_metadata(spark: SparkSession) -> None:
	bookings = spark.createDataFrame([("Resort Hotel",)], ["hotel"])
	hotels = spark.createDataFrame(
		[("Resort Hotel", "Algarve", "PRT", 4, 2010)],
		["hotel_name", "city", "country_code", "star_rating", "opened_year"],
	)
	row = enrich_with_hotels(bookings, hotels).first()
	assert row is not None
	assert row["hotel_city"] == "Algarve"
	assert row["hotel_star_rating"] == 4
	assert row["hotel_opened_year"] == 2010
