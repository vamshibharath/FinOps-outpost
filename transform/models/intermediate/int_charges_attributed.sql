-- Every charge with its canonical owner tags (null when the line is untagged).
select
    c.*,
    t.business_unit,
    coalesce(t.application, 'unspecified')  as application,
    coalesce(t.environment, 'unspecified')  as environment,
    t.business_unit is not null             as is_directly_attributed
from {{ ref('stg_focus__charges') }} c
left join {{ ref('int_charge_tags') }} t using (charge_id)
