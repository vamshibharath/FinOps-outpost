-- FAILS if any allocation weight group doesn't sum to 1 (would create or destroy cost).
select weight_set, provider, billing_period, service_category, sum(weight) as total_weight
from {{ ref('int_allocation_weights') }}
group by all
having abs(sum(weight) - 1.0) > 1e-9
