"""Explicit StructType schema for the raw `hotel_bookings.csv` dataset."""

from pyspark.sql.types import (
	DoubleType,
	IntegerType,
	StringType,
	StructField,
	StructType,
)

bookings_bronze_schema = StructType(
	[
		StructField("hotel", StringType(), nullable=True),
		StructField("is_canceled", IntegerType(), nullable=True),
		StructField("lead_time", IntegerType(), nullable=True),
		StructField("arrival_date_year", IntegerType(), nullable=True),
		StructField("arrival_date_month", StringType(), nullable=True),
		StructField("arrival_date_week_number", IntegerType(), nullable=True),
		StructField("arrival_date_day_of_month", IntegerType(), nullable=True),
		StructField("stays_in_weekend_nights", IntegerType(), nullable=True),
		StructField("stays_in_week_nights", IntegerType(), nullable=True),
		StructField("adults", IntegerType(), nullable=True),
		StructField("children", DoubleType(), nullable=True),
		StructField("babies", IntegerType(), nullable=True),
		StructField("meal", StringType(), nullable=True),
		StructField("country", StringType(), nullable=True),
		StructField("market_segment", StringType(), nullable=True),
		StructField("distribution_channel", StringType(), nullable=True),
		StructField("is_repeated_guest", IntegerType(), nullable=True),
		StructField("previous_cancellations", IntegerType(), nullable=True),
		StructField("previous_bookings_not_canceled", IntegerType(), nullable=True),
		StructField("reserved_room_type", StringType(), nullable=True),
		StructField("assigned_room_type", StringType(), nullable=True),
		StructField("booking_changes", IntegerType(), nullable=True),
		StructField("deposit_type", StringType(), nullable=True),
		StructField("agent", DoubleType(), nullable=True),
		StructField("company", DoubleType(), nullable=True),
		StructField("days_in_waiting_list", IntegerType(), nullable=True),
		StructField("customer_type", StringType(), nullable=True),
		StructField("adr", DoubleType(), nullable=True),
		StructField("required_car_parking_spaces", IntegerType(), nullable=True),
		StructField("total_of_special_requests", IntegerType(), nullable=True),
		StructField("reservation_status", StringType(), nullable=True),
		StructField("reservation_status_date", StringType(), nullable=True),
	]
)
