-- Weights used to split untagged (shared) cost pools across business units.
-- Three weight sets, each normalized to sum to 1.0 within its group:
--   usage_driver      : share of tagged compute consumption (hours) per provider/period
--   category_cost     : share of tagged effective cost within provider/period/service_category
--   provider_cost     : share of tagged effective cost within provider/period (fallback)
with tagged as (
    select * from {{ ref('int_charges_attributed') }}
    where is_directly_attributed and charge_category = 'Usage'
),

usage_driver as (
    select provider, billing_period, cast(null as varchar) as service_category,
           business_unit, sum(consumed_quantity) as driver
    from tagged
    where service_category = 'Compute' and consumed_quantity > 0
    group by all
),

category_cost as (
    select provider, billing_period, service_category, business_unit,
           sum(effective_cost) as driver
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
    select 'usage_driver' as weight_set, * from usage_driver
    union all select 'category_cost', * from category_cost
    union all select 'provider_cost', * from provider_cost
)

select
    weight_set, provider, billing_period, service_category, business_unit, driver,
    driver / sum(driver) over (partition by weight_set, provider, billing_period, service_category) as weight
from unioned
