{{ config(severity = 'warn') }}
-- WARNS (does not fail the build) when a closed billing month has days with no usage at all.
-- Missing days usually mean a late or partial export: Finance should know before trusting the total.
select provider, billing_period, days_in_period, days_with_usage, missing_days
from {{ ref('mart_data_completeness') }}
where missing_days > 0
