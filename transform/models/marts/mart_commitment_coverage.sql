-- Commitment (Savings Plan / Reservation / CUD) health per provider and period:
-- how much eligible compute is covered, and how much commitment went unused (waste).
with compute as (
    select * from {{ ref('stg_focus__charges') }}
    where service_category = 'Compute' and charge_category in ('Usage', 'Purchase')
)
select
    provider,
    billing_period,
    round(sum(effective_cost) filter (where commitment_discount_id is not null
                                        and coalesce(commitment_discount_status, 'Used') = 'Used'), 6) as committed_cost,
    round(sum(effective_cost) filter (where commitment_discount_status = 'Unused'), 6)                as unused_commitment_cost,
    round(sum(effective_cost) filter (where commitment_discount_id is null
                                        and pricing_category = 'Standard'
                                        and charge_category = 'Usage'), 6)                           as on_demand_cost,
    round(sum(list_cost) - sum(effective_cost), 6)                                                    as realized_savings_vs_list,
    round(coalesce(sum(effective_cost) filter (where commitment_discount_id is not null), 0)
          / nullif(sum(effective_cost) filter (where charge_category = 'Usage'), 0), 4)               as commitment_coverage_pct
from compute
group by all
