-- Resolve inconsistent tag keys (business_unit / org / " org" / CostCenter ...) to one
-- canonical set using the governed alias table, picking the highest-priority alias.
with kv as (
    select
        c.charge_id,
        lower(trim(k.key))                                              as raw_key,
        nullif(trim(json_extract_string(c.tags_json, '$."' || k.key || '"')), '') as tag_value
    from {{ ref('stg_focus__charges') }} c,
         unnest(json_keys(c.tags_json)) as k(key)
    where c.tags_json is not null
),

mapped as (
    select kv.charge_id, a.canonical_key, kv.tag_value, a.priority
    from kv
    join {{ ref('tag_key_aliases') }} a on kv.raw_key = lower(a.raw_key)
    where kv.tag_value is not null
)

select
    charge_id,
    arg_min(tag_value, priority) filter (where canonical_key = 'business_unit') as business_unit,
    arg_min(tag_value, priority) filter (where canonical_key = 'application')   as application,
    lower(arg_min(tag_value, priority) filter (where canonical_key = 'environment')) as environment
from mapped
group by charge_id
