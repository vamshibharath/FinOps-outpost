-- FAILS if any provider/billing period's allocated total drifts from source spend.
-- This is the test that lets Finance trust the chargeback: no dollar created or lost.
with src as (
    select provider, billing_period, sum(effective_cost) as src_eff, sum(billed_cost) as src_billed
    from {{ ref('stg_focus__charges') }} group by all
),
alloc as (
    select provider, billing_period,
           sum(allocated_effective_cost) as alloc_eff, sum(allocated_billed_cost) as alloc_billed
    from {{ ref('fct_cost_allocated') }} group by all
)
select s.*, a.alloc_eff, a.alloc_billed
from src s
full outer join alloc a using (provider, billing_period)
where abs(coalesce(s.src_eff, 0) - coalesce(a.alloc_eff, 0)) > {{ var('reconciliation_tolerance') }}
   or abs(coalesce(s.src_billed, 0) - coalesce(a.alloc_billed, 0)) > {{ var('reconciliation_tolerance') }}
