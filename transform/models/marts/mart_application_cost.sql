-- Unit-economics starting point: directly attributed cost per application and environment.
-- Non-prod share is a common optimization lever (schedules, rightsizing dev/test).
select
    business_unit,
    application,
    round(sum(allocated_effective_cost), 6)                                                   as total_cost,
    round(sum(allocated_effective_cost) filter (where environment = 'prod'), 6)              as prod_cost,
    round(sum(allocated_effective_cost) filter (where environment <> 'prod'), 6)             as non_prod_cost,
    round(coalesce(sum(allocated_effective_cost) filter (where environment <> 'prod'), 0)
          / nullif(sum(allocated_effective_cost), 0), 4)                                      as non_prod_pct,
    count(distinct provider)                                                                  as providers_used
from {{ ref('fct_cost_allocated') }}
where allocation_method = 'direct'
group by all
