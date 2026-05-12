"""Pandera schema with validations for the Silver `bookings_enriched` dataset."""

import pandera.pyspark as pa
from pandera.pyspark import Column, DataFrameSchema
from pyspark.sql.types import (
	DateType,
	DoubleType,
	IntegerType,
	StringType,
)

bookings_enriched_schema = DataFrameSchema(
	columns={
		# Identificação do hotel
		"hotel": Column(StringType(), nullable=False),
		"hotel_city": Column(StringType(), nullable=True),
		"hotel_star_rating": Column(
			IntegerType(), pa.Check.in_range(1, 5), nullable=True
		),
		"hotel_opened_year": Column(IntegerType(), nullable=True),
		# Status da reserva
		"is_canceled": Column(IntegerType(), pa.Check.isin([0, 1])),
		"booking_status": Column(
			StringType(), pa.Check.isin(["Canceled", "NoShow", "CheckedOut"])
		),
		"reservation_status": Column(StringType(), nullable=True),
		"reservation_status_date": Column(DateType(), nullable=True),
		# Datas
		"arrival_date": Column(DateType(), nullable=False),
		"arrival_date_year": Column(IntegerType(), pa.Check.in_range(2014, 2018)),
		"arrival_date_month_num": Column(IntegerType(), pa.Check.in_range(1, 12)),
		"lead_time": Column(IntegerType(), pa.Check.ge(0)),
		# Hóspedes
		"adults": Column(IntegerType(), pa.Check.ge(0)),
		"children": Column(IntegerType(), pa.Check.ge(0)),
		"babies": Column(IntegerType(), pa.Check.ge(0)),
		"total_guests": Column(IntegerType(), pa.Check.gt(0)),
		"is_family": Column(IntegerType(), pa.Check.isin([0, 1])),
		"is_repeated_guest": Column(IntegerType(), pa.Check.isin([0, 1])),
		# Estadia
		"stays_in_weekend_nights": Column(IntegerType(), pa.Check.ge(0)),
		"stays_in_week_nights": Column(IntegerType(), pa.Check.ge(0)),
		"total_nights": Column(IntegerType(), pa.Check.ge(0)),
		"is_long_stay": Column(IntegerType(), pa.Check.isin([0, 1])),
		# Financeiro
		"adr": Column(DoubleType(), pa.Check.ge(0)),
		"revenue": Column(DoubleType(), pa.Check.ge(0)),
		# Geografia
		"country": Column(StringType(), nullable=False),
		"country_name": Column(StringType(), nullable=True),
		"continent": Column(StringType(), nullable=True),
		# Comerciais
		"market_segment": Column(StringType(), nullable=True),
		"distribution_channel": Column(StringType(), nullable=True),
		"customer_type": Column(StringType(), nullable=True),
		"meal": Column(StringType(), nullable=True),
		"deposit_type": Column(StringType(), nullable=True),
		# Operacionais
		"booking_changes": Column(IntegerType(), pa.Check.ge(0)),
		"days_in_waiting_list": Column(IntegerType(), pa.Check.ge(0)),
		"required_car_parking_spaces": Column(IntegerType(), pa.Check.ge(0)),
		"total_of_special_requests": Column(IntegerType(), pa.Check.ge(0)),
		"previous_cancellations": Column(IntegerType(), pa.Check.ge(0)),
		"previous_bookings_not_canceled": Column(IntegerType(), pa.Check.ge(0)),
	},
	strict=False,
)
