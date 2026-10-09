-- "Is the data complete?" per provider and billing period: how many days of the month
-- actually have usage, which days are missing, and when the month was last loaded.
-- A partial month is the most common reason a chargeback looks too low.
with periods as (
    select provider, billing_period, billing_period_start,
           min(cast(charge_period_start as date)) as first_usage_day,
           max(cast(charge_period_start as date)) as last_usage_day,
           max(loaded_at)                         as last_loaded_at,
           count(*)                               as line_items,
           -- usage billed in this period but incurred in another one = late-arriving usage
           count(*) filter (where strftime(charge_period_start, '%Y-%m') <> billing_period) as late_arriving_lines
    from {{ ref('stg_focus__charges') }}
    where charge_category = 'Usage'
    group by all
),
calendar as (
    select p.provider, p.billing_period, cast(d.day as date) as day
    from periods p,
         generate_series(cast(p.billing_period_start as timestamp),
                         cast(p.billing_period_start as timestamp) + interval 1 month - interval 1 day,
                         interval 1 day) as d(day)
),
observed as (
    select distinct provider, billing_period, cast(charge_period_start as date) as day
    from {{ ref('stg_focus__charges') }}
    where charge_category = 'Usage'
)
select
    p.provider,
    p.billing_period,
    count(c.day)                                               as days_in_period,
    count(o.day)                                               as days_with_usage,
    count(c.day) - count(o.day)                                as missing_days,
    round(count(o.day) / count(c.day), 4)                      as completeness_pct,
    p.first_usage_day,
    p.last_usage_day,
    p.line_items,
    p.late_arriving_lines,
    p.last_loaded_at
from periods p
join calendar c on c.provider = p.provider and c.billing_period = p.billing_period
left join observed o on o.provider = c.provider and o.billing_period = c.billing_period and o.day = c.day
group by p.provider, p.billing_period, p.first_usage_day, p.last_usage_day, p.line_items,
         p.late_arriving_lines, p.last_loaded_at
