-- Daily service telemetry (CPU core-hours, requests) landed by ingestion/telemetry/fetch_telemetry.py.
-- One row per usage date, application, provider.
select
    usage_date,
    strftime(usage_date, '%Y-%m')  as billing_period,
    application,
    provider,
    cpu_core_hours,
    requests
from read_parquet('{{ var("telemetry_path") }}', hive_partitioning = true)
