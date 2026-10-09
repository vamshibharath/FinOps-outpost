{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['provider', 'billing_period'],
    cluster_by = ['billing_period', 'business_unit']
  )
}}
-- The attribution fact: every dollar of source spend lands on exactly one business unit
-- (or the explicit UNALLOCATED bucket). Invariant, enforced by tests/assert_allocation_reconciles:
--   sum(allocated_effective_cost) == sum(stg effective_cost)  per provider and billing period.
--
-- Incremental by provider + billing period: a run only rebuilds months whose source data was
-- (re)loaded since the last run, e.g. a restated AWS month. Closed, unchanged months are not
-- touched. delete+insert replaces the whole month, so a restatement can never double count.
-- cluster_by applies on Snowflake (prunes micro-partitions on period/owner filters).

with changed_periods as (
    select provider, billing_period
    from {{ ref('int_charges_attributed') }}
    group by all
    {% if is_incremental() %}
    having max(loaded_at) > (select coalesce(max(processed_at), cast('1900-01-01' as timestamp)) from {{ this }})
    {% endif %}
),

charges as (
    select c.*
    from {{ ref('int_charges_attributed') }} c
    join changed_periods using (provider, billing_period)
),

direct as (
    select
        provider, billing_period, service_category, business_unit, application, environment,
        'direct' as allocation_method,
        sum(effective_cost) as allocated_effective_cost,
        sum(billed_cost)    as allocated_billed_cost,
        count(*)            as source_line_items
    from charges
    where is_directly_attributed
    group by all
),

-- Untagged spend pooled per provider/period/category. Credits, tax, adjustments and
-- purchases get their own pool: they follow each team's *spend*, not compute hours
-- (otherwise a big credit inflates the discount for whoever runs the most VMs).
pools as (
    select provider, billing_period,
           case when charge_category = 'Usage' then service_category
                else 'Credits, Tax & Adjustments' end                as service_category,
           sum(effective_cost) as pool_effective, sum(billed_cost) as pool_billed, count(*) as n
    from charges
    where not is_directly_attributed
    group by all
),

w as (select * from {{ ref('int_allocation_weights') }}),

-- Which weight set each pool uses: its governed rule if that driver exists for this
-- provider/month, else category spend share, else provider spend share, else UNALLOCATED.
resolved as (
    select p.*,
        case
            when exists (select 1 from w where w.weight_set = coalesce(r.allocation_method, 'category_cost')
                         and w.provider = p.provider and w.billing_period = p.billing_period
                         and (w.weight_set <> 'category_cost' or w.service_category = p.service_category))
                then coalesce(r.allocation_method, 'category_cost')
            when exists (select 1 from w where w.weight_set = 'category_cost'
                         and w.provider = p.provider and w.billing_period = p.billing_period
                         and w.service_category = p.service_category)
                then 'category_cost'
            when exists (select 1 from w where w.weight_set = 'provider_cost'
                         and w.provider = p.provider and w.billing_period = p.billing_period)
                then 'provider_cost'
            else 'unallocated'
        end as weight_set
    from pools p
    left join {{ ref('shared_cost_rules') }} r on lower(r.service_category) = lower(p.service_category)
),

shared as (
    select
        r.provider, r.billing_period, r.service_category,
        w.business_unit, 'shared' as application, 'shared' as environment,
        'shared_' || r.weight_set   as allocation_method,
        r.pool_effective * w.weight as allocated_effective_cost,
        r.pool_billed * w.weight    as allocated_billed_cost,
        r.n                         as source_line_items
    from resolved r
    join w on w.weight_set = r.weight_set
          and w.provider = r.provider and w.billing_period = r.billing_period
          and (r.weight_set <> 'category_cost' or w.service_category = r.service_category)
    where r.weight_set <> 'unallocated'
),

unallocated as (
    select provider, billing_period, service_category,
           'UNALLOCATED' as business_unit, 'shared' as application, 'shared' as environment,
           'unallocated' as allocation_method,
           pool_effective, pool_billed, n
    from resolved where weight_set = 'unallocated'
)

select *, current_timestamp as processed_at
from (
    select * from direct
    union all select * from shared
    union all select * from unallocated
)
