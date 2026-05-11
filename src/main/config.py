"""Centralized pipeline configuration via pydantic-settings."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
	"""Pipeline configuration loaded from environment or `.env`.

	Attributes:
		raw_dir: Directory containing the raw CSV files.
		bronze_dir: Directory for Bronze layer Parquet outputs.
		silver_dir: Directory for Silver layer Parquet outputs.
		gold_dir: Directory for Gold layer Parquet outputs.
		spark_app_name: Spark application name.
		spark_master: Spark master URL.
		null_token: String literal in raw CSVs to be treated as NULL.
	"""

	model_config = SettingsConfigDict(env_file=".env", extra="ignore")

	raw_dir: Path = Path("data/raw")
	bronze_dir: Path = Path("data/bronze")
	silver_dir: Path = Path("data/silver")
	gold_dir: Path = Path("data/gold")

	hotel_bookings_origin: str = "https://raw.githubusercontent.com/aaqibqadeer/Hotel-booking-demand/refs/heads/master/hotel_bookings.csv"

	spark_app_name: str = "befly-pipeline"
	spark_master: str = "local[*]"

	null_token: str = "NULL"


settings = Settings()
