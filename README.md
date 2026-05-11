## 🏗 Arquitetura do Projeto – BeFly Pipeline

Abaixo a estrutura refinada deste projeto pensando numa arquitetura de medalhão:

```log
projeto-befly/
├── data/                             # gitignored
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── src/
│   ├── main/
│   │   ├── __init__.py
│   │   ├── config.py                 # ⭐ Pydantic Settings (centralizado)
│   │   └── run.py           # orquestrador bronze→silver→gold
│   │
│   ├── stages/
│   │   ├── __init__.py
│   │   │
│   │   ├── bronze/
│   │   │   ├── __init__.py
│   │   │   ├── ingest_bookings.py
│   │   │   ├── ingest_countries.py
│   │   │   ├── ingest_hotels.py
│   │   │   └── rules.py              # ⭐ regras específicas Bronze
│   │   │                             #    (ex.: tratar "NULL" string,
│   │   │                             #     normalizar headers)
│   │   │
│   │   ├── silver/
│   │   │   ├── __init__.py
│   │   │   ├── build_bookings_enriched.py
│   │   │   └── rules.py              # ⭐ regras de negócio Silver
│   │   │                             #    (total_nights, revenue,
│   │   │                             #     is_family, arrival_date,
│   │   │                             #     joins de enriquecimento)
│   │   │
│   │   └── gold/
│   │       ├── __init__.py
│   │       ├── revenue_by_hotel_month.py
│   │       ├── cancellation_by_segment.py
│   │       ├── top_countries_by_revenue.py
│   │       └── rules.py              # ⭐ KPIs e fórmulas
│   │                                 #    (cancellation_rate, avg_ticket)
│   │
│   ├── schemas/
│   │   ├── __init__.py
│   │   │
│   │   ├── bronze/
│   │   │   ├── __init__.py
│   │   │   ├── bookings_schema.py    # StructType + DataFrameSchema (pandera)
│   │   │   ├── countries_schema.py
│   │   │   └── hotels_schema.py
│   │   │
│   │   └── silver/
│   │       ├── __init__.py
│   │       └── bookings_enriched_schema.py
│   │                                 # ⭐ pandera valida:
│   │                                 #   - adr >= 0
│   │                                 #   - adults+children+babies > 0
│   │                                 #   - country não nulo
│   │                                 #   - tipos corretos
│   │
│   ├── infra/
│   │   ├── __init__.py
│   │   ├── spark/
│   │   │   ├── __init__.py
│   │   │   └── session_factory.py
│   │   ├── storage/
│   │   │   ├── __init__.py
│   │   │   ├── parquet_writer.py
│   │   │   └── csv_reader.py
│   │   └── interfaces/
│   │       ├── __init__.py
│   │       ├── i_reader.py
│   │       └── i_writer.py
│   │
│   ├── errors/
│   │   ├── __init__.py
│   │   ├── infra_errors.py           # ⭐ SparkSessionError, StorageReadError,
│   │   │                             #    StorageWriteError, ConfigError
│   │   └── layer_errors.py           # ⭐ BronzeIngestionError,
│   │                                 #    SilverTransformationError,
│   │                                 #    GoldAggregationError,
│   │                                 #    SchemaValidationError
│   │
│   └── utils/
│       ├── __init__.py
│       ├── decorators.py             # @log_execution_time, @count_records
│       └── logger.py
│
├── tests/
│   ├── unit/
│   │   ├── stages/
│   │   │   ├── bronze/
│   │   │   ├── silver/
│   │   │   └── gold/
│   │   └── schemas/
│   └── integration/
│
└── notebooks/
    └── 1.0-EDA_Inicial.ipynb
```