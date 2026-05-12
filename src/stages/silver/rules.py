"""Silver-layer business rules: cleaning, derivations and enrichment."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
	coalesce,
	col,
	concat_ws,
	lpad,
	to_date,
	when,
)
from pyspark.sql.types import IntegerType

_MONTH_NAME_TO_NUMBER: dict[str, int] = {
	"January": 1,
	"February": 2,
	"March": 3,
	"April": 4,
	"May": 5,
	"June": 6,
	"July": 7,
	"August": 8,
	"September": 9,
	"October": 10,
	"November": 11,
	"December": 12,
}

_LONG_STAY_THRESHOLD: int = 7


def cast_columns(dataframe: DataFrame) -> DataFrame:
	"""Casts numeric and date columns to their expected types.

	`reservation_status_date` is parsed tolerating both `yyyy-MM-dd`
	and `dd-MM-yy` (the format used by some public mirrors of the dataset).

	Args:
		dataframe: Bronze bookings DataFrame.

	Returns:
		DataFrame with corrected types.
	"""
	integer_columns = (
		"is_canceled",
		"lead_time",
		"arrival_date_year",
		"arrival_date_week_number",
		"arrival_date_day_of_month",
		"stays_in_weekend_nights",
		"stays_in_week_nights",
		"adults",
		"children",
		"babies",
		"is_repeated_guest",
		"previous_cancellations",
		"previous_bookings_not_canceled",
		"booking_changes",
		"days_in_waiting_list",
		"required_car_parking_spaces",
		"total_of_special_requests",
	)
	for column in integer_columns:
		dataframe = dataframe.withColumn(column, col(column).cast(IntegerType()))

	return dataframe.withColumn(
		"reservation_status_date",
		coalesce(
			to_date(col("reservation_status_date"), "yyyy-MM-dd"),
			to_date(col("reservation_status_date"), "dd-MM-yy"),
			to_date(col("reservation_status_date"), "dd/MM/yyyy"),
		),
	)


def fill_nulls(dataframe: DataFrame) -> DataFrame:
	"""Fills documented null defaults.

	- `children` → 0
	- `country`  → "UNK"

	Args:
		dataframe: DataFrame after type casting.

	Returns:
		DataFrame with nulls handled.
	"""
	return dataframe.fillna({"children": 0, "country": "UNK"})


def filter_invalid_bookings(dataframe: DataFrame) -> tuple[DataFrame, dict[str, int]]:
	"""Removes invalid bookings and returns counts per filter.

	Filters:
		- bookings without any guest (`adults + children + babies == 0`)
		- bookings with negative `adr`

	Args:
		dataframe: DataFrame to be filtered.

	Returns:
		Tuple of (filtered DataFrame, dict with removal counts).
	"""
	original_count = dataframe.count()

	with_guests = dataframe.filter(
		(col("adults") + col("children") + col("babies")) > 0
	)
	guests_removed = original_count - with_guests.count()

	valid_adr = with_guests.filter(col("adr") >= 0)
	adr_removed = with_guests.count() - valid_adr.count()

	return valid_adr, {
		"no_guests_removed": guests_removed,
		"negative_adr_removed": adr_removed,
	}


def add_arrival_date(dataframe: DataFrame) -> DataFrame:
	"""Builds `arrival_date` and `arrival_date_month_num` columns.

	Args:
		dataframe: DataFrame containing year/month-name/day columns.

	Returns:
		DataFrame enriched with arrival date columns.
	"""
	month_mapping = when(col("arrival_date_month").isNull(), None)
	for name, number in _MONTH_NAME_TO_NUMBER.items():
		month_mapping = month_mapping.when(col("arrival_date_month") == name, number)

	return dataframe.withColumn(
		"arrival_date_month_num", month_mapping.cast(IntegerType())
	).withColumn(
		"arrival_date",
		to_date(
			concat_ws(
				"-",
				col("arrival_date_year"),
				lpad(col("arrival_date_month_num").cast("string"), 2, "0"),
				lpad(col("arrival_date_day_of_month").cast("string"), 2, "0"),
			),
			"yyyy-MM-dd",
		),
	)


def add_derived_columns(dataframe: DataFrame) -> DataFrame:
	"""Creates business-derived columns.

	Adds:
		- total_nights, total_guests
		- is_family, is_long_stay
		- revenue (0 when canceled)
		- booking_status (legible status from reservation_status)

	Args:
		dataframe: DataFrame with cleaned base columns.

	Returns:
		DataFrame with derived columns.
	"""
	return (
		dataframe.withColumn(
			"total_nights",
			(col("stays_in_weekend_nights") + col("stays_in_week_nights")).cast(
				IntegerType()
			),
		)
		.withColumn(
			"total_guests",
			(col("adults") + col("children") + col("babies")).cast(IntegerType()),
		)
		.withColumn(
			"is_family",
			when((col("children") > 0) | (col("babies") > 0), 1)
			.otherwise(0)
			.cast(IntegerType()),
		)
		.withColumn(
			"is_long_stay",
			when(
				(col("stays_in_weekend_nights") + col("stays_in_week_nights"))
				> _LONG_STAY_THRESHOLD,
				1,
			)
			.otherwise(0)
			.cast(IntegerType()),
		)
		.withColumn(
			"revenue",
			when(
				col("is_canceled") == 0,
				col("adr")
				* (col("stays_in_weekend_nights") + col("stays_in_week_nights")),
			)
			.otherwise(0.0)
			.cast("double"),
		)
		.withColumn(
			"booking_status",
			when(col("reservation_status") == "Canceled", "Canceled")
			.when(col("reservation_status") == "No-Show", "NoShow")
			.when(col("reservation_status") == "Check-Out", "CheckedOut")
			.otherwise("Unknown"),
		)
	)


def enrich_with_countries(bookings: DataFrame, countries: DataFrame) -> DataFrame:
	"""Left-joins bookings with country metadata.

	Args:
		bookings: Bookings DataFrame containing `country` (ISO-3 code).
		countries: Country reference DataFrame.

	Returns:
		Enriched DataFrame with `country_name` and `continent`.
	"""
	return bookings.join(
		countries.select(
			col("country_code").alias("_country_code"),
			col("country_name"),
			col("continent"),
		),
		bookings["country"] == col("_country_code"),
		how="left",
	).drop("_country_code")


def enrich_with_hotels(bookings: DataFrame, hotels: DataFrame) -> DataFrame:
	"""Left-joins bookings with hotel metadata.

	Args:
		bookings: Bookings DataFrame containing `hotel` name.
		hotels: Hotel reference DataFrame.

	Returns:
		Enriched DataFrame with `hotel_city`, `hotel_star_rating`, `hotel_opened_year`.
	"""
	return bookings.join(
		hotels.select(
			col("hotel_name").alias("_hotel_name"),
			col("city").alias("hotel_city"),
			col("star_rating").alias("hotel_star_rating"),
			col("opened_year").alias("hotel_opened_year"),
		),
		bookings["hotel"] == col("_hotel_name"),
		how="left",
	).drop("_hotel_name")
