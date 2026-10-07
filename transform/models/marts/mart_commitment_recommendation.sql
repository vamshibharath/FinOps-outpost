-- Committed-use sizing: commit only to the on-demand compute floor you run every day.
-- Baseline = low percentile of daily on-demand compute spend, so the commitment stays
-- fully utilized even on quiet days (an unused commitment is pure waste).
-- Daily rather than hourly buckets: robust to sparse/sampled line items and to
-- providers that bill some compute at daily granularity.
-- Savings are a planning estimate driven by var('assumed_commitment_discount'), not a quote.
with daily as (
    select provider, billing_period, cast(charge_period_start as date) as usage_day,
           sum(effective_cost) as on_demand_cost
    from {{ ref('stg_focus__charges') }}
    where service_category = 'Compute' and charge_category = 'Usage'
      and commitment_discount_id is null and pricing_category = 'Standard'
      and effective_cost > 0   -- refunds/corrections are not commit-able usage
    group by all
),
stats as (
    select provider, billing_period,
           count(*)                                                                    as days_observed,
           sum(on_demand_cost)                                                         as on_demand_total,
           avg(on_demand_cost)                                                         as avg_daily_on_demand,
           quantile_cont(on_demand_cost, {{ var('commitment_baseline_percentile') }})  as baseline_daily_commit,
           max(on_demand_cost)                                                         as peak_daily_on_demand
    from daily group by all
)
select
    provider, billing_period, days_observed,
    round(on_demand_total, 6)                 as on_demand_total,
    round(avg_daily_on_demand, 6)             as avg_daily_on_demand,
    round(baseline_daily_commit, 6)           as recommended_daily_commitment,
    round(baseline_daily_commit / 24.0, 6)    as recommended_hourly_commitment,
    round(peak_daily_on_demand, 6)            as peak_daily_on_demand,
    round(baseline_daily_commit * days_observed / nullif(on_demand_total, 0), 4) as share_of_on_demand_covered,
    round(baseline_daily_commit * days_observed * {{ var('assumed_commitment_discount') }}, 6)
                                              as est_savings_for_period,
    {{ var('assumed_commitment_discount') }}  as assumed_discount_rate
from stats
where days_observed >= 14   -- too little history to size a commitment safely otherwise
