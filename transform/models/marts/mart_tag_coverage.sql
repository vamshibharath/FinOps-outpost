-- Tagging governance scorecard: how much spend can be attributed without allocation rules.
-- Ranked so the accounts with the most untagged dollars (biggest wins) surface first.
with c as (
    select * from {{ ref('int_charges_attributed') }} where charge_category = 'Usage'
),
agg as (
    select
        provider,
        billing_period,
        sub_account_name,
        count(*)                                                                        as line_items,
        sum(effective_cost)                                                             as effective_cost,
        coalesce(sum(effective_cost) filter (where is_directly_attributed), 0)          as tagged_cost,
        coalesce(sum(effective_cost) filter (where not is_directly_attributed), 0)      as untagged_cost
    from c
    group by all
)
select
    provider, billing_period, sub_account_name, line_items,
    round(effective_cost, 6)                              as effective_cost,
    round(tagged_cost, 6)                                 as tagged_cost,
    round(untagged_cost, 6)                               as untagged_cost,
    round(tagged_cost / nullif(effective_cost, 0), 4)     as tag_coverage_pct,
    rank() over (partition by provider, billing_period order by untagged_cost desc) as untagged_rank
from agg
