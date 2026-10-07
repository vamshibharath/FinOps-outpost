-- What Finance bills each business unit, split into what they directly own vs. their
-- share of shared/untagged spend (so a team can see *why* their number is what it is).
select
    billing_period,
    business_unit,
    provider,
    round(sum(allocated_effective_cost) filter (where allocation_method = 'direct'), 6)       as direct_cost,
    round(sum(allocated_effective_cost) filter (where allocation_method like 'shared_%'), 6)  as shared_cost,
    round(sum(allocated_effective_cost), 6)                                                   as total_chargeback,
    round(sum(allocated_billed_cost), 6)                                                      as total_billed_basis,
    round(coalesce(sum(allocated_effective_cost) filter (where allocation_method like 'shared_%'), 0)
          / nullif(sum(allocated_effective_cost), 0), 4)                                      as shared_pct
from {{ ref('fct_cost_allocated') }}
group by all
