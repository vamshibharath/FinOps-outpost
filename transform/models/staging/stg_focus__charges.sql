-- One row per billing line item, normalized across providers.
-- Real data quirks handled here: mixed casing ('Usage' vs 'usage', 'Usage-based' vs
-- 'Usage-Based', 'standard'), empty strings vs NULL, '{}' tags, string-typed prices.
with src as (
    select *
    from read_parquet('{{ var("lake_path") }}', hive_partitioning = true, union_by_name = true)
),

cleaned as (
    select
        md5(provider || '|' || cast(Id as varchar))                     as charge_id,
        provider,
        billing_period,
        cast(BillingPeriodStart as date)                                 as billing_period_start,
        cast(ChargePeriodStart as timestamp)                             as charge_period_start,
        cast(ChargePeriodEnd as timestamp)                               as charge_period_end,
        upper(trim(BillingCurrency))                                     as billing_currency,

        -- Canonical casing so GROUP BYs don't split one category into two.
        case lower(trim(ChargeCategory))
            when 'usage' then 'Usage' when 'purchase' then 'Purchase'
            when 'tax' then 'Tax' when 'credit' then 'Credit'
            when 'adjustment' then 'Adjustment' else 'Unknown' end       as charge_category,
        case lower(trim(ChargeFrequency))
            when 'usage-based' then 'Usage-Based' when 'one-time' then 'One-Time'
            when 'recurring' then 'Recurring' else 'Unknown' end         as charge_frequency,
        case lower(nullif(trim(PricingCategory), ''))
            when 'standard' then 'Standard' when 'committed' then 'Committed'
            when 'dynamic' then 'Dynamic' when 'other' then 'Other'
            else 'Unknown' end                                           as pricing_category,

        coalesce(nullif(trim(ServiceCategory), ''), 'Other')             as service_category,
        ServiceName                                                      as service_name,
        SubAccountId                                                     as sub_account_id,
        SubAccountName                                                   as sub_account_name,
        ResourceId                                                       as resource_id,
        RegionId                                                         as region_id,

        nullif(trim(CommitmentDiscountId), '')                           as commitment_discount_id,
        nullif(trim(CommitmentDiscountType), '')                         as commitment_discount_type,
        nullif(trim(CommitmentDiscountStatus), '')                       as commitment_discount_status,

        cast(ConsumedQuantity as double)                                 as consumed_quantity,
        ConsumedUnit                                                     as consumed_unit,

        -- Billed = what the invoice says (purchases up front).
        -- Effective = amortized: commitment fees spread across the usage they cover.
        -- Chargeback uses Effective; invoice reconciliation uses Billed.
        cast(BilledCost as double)                                       as billed_cost,
        cast(EffectiveCost as double)                                    as effective_cost,
        cast(ListCost as double)                                         as list_cost,
        cast(ContractedCost as double)                                   as contracted_cost,

        case when Tags is null or trim(Tags) in ('', '{}') then null
             else Tags end                                               as tags_json,
        _batch_id                                                        as batch_id,
        _source_file                                                     as source_file
    from src
)

select * from cleaned
