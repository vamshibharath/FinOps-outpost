-- Weights used to split untagged (shared) cost pools across business units.
-- Each weight set is normalized to sum to 1.0 within its group:
--   telemetry_cpu      : share of CPU core-hours (observability telemetry) per provider/period
--   telemetry_requests : share of request volume (observability telemetry) per provider/period
--   usage_driver       : share of tagged compute consumption on the bill itself
--   category_cost      : share of tagged effective cost within provider/period/service_category
--   provider_cost      : share of tagged effective cost within provider/period (last fallback)
with tagged as (
    select * from {{ ref('int_charges_attributed') }}
    where is_directly_attributed and charge_category = 'Usage'
),

-- Telemetry is keyed by application; billing tags tell us which business unit owns it.
-- If an application carries several owners' tags, the owner with the most spend wins.
app_spend as (
    select provider, billing_period, application, business_unit, sum(effective_cost) as spend
    from tagged
    where application <> 'unspecified'
    group by all
),
app_owner as (
    select provider, billing_period, application, arg_max(business_unit, spend) as business_unit
    from app_spend
    group by all
),
telemetry as (
    select o.provider, o.billing_period, o.business_unit,
           sum(t.cpu_core_hours) as cpu_core_hours, sum(t.requests) as requests
    from {{ ref('stg_telemetry__service_usage') }} t
    join app_owner o using (provider, billing_period, application)
    group by all
),

telemetry_cpu as (
    select provider, billing_period, cast(null as varchar) as service_category, business_unit,
           cpu_core_hours as driver
    from telemetry where cpu_core_hours > 0
),
telemetry_requests as (
    select provider, billing_period, cast(null as varchar) as service_category, business_unit,
           cast(requests as double) as driver
    from telemetry where requests > 0
),
usage_driver as (
    select provider, billing_period, cast(null as varchar) as service_category,
           business_unit, sum(consumed_quantity) as driver
    from tagged
    where service_category = 'Compute' and consumed_quantity > 0
    group by all
),
category_cost as (
    select provider, billing_period, service_category, business_unit, sum(effective_cost) as driver
    from tagged where effective_cost > 0
    group by all
),
provider_cost as (
    select provider, billing_period, cast(null as varchar) as service_category,
           business_unit, sum(effective_cost) as driver
    from tagged where effective_cost > 0
    group by all
),

unioned as (
    select 'telemetry_cpu' as weight_set, * from telemetry_cpu
    union all select 'telemetry_requests', * from telemetry_requests
    union all select 'usage_driver', * from usage_driver
    union all select 'category_cost', * from category_cost
    union all select 'provider_cost', * from provider_cost
)

select
    weight_set, provider, billing_period, service_category, business_unit, driver,
    driver / sum(driver) over (partition by weight_set, provider, billing_period, service_category) as weight
from unioned
