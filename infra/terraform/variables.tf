variable "region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging or prod."
  }
}

variable "landing_retention_days" {
  type    = number
  default = 400 # > 13 months so year-over-year backfills stay possible
}

variable "athena_bytes_scanned_cutoff" {
  type    = number
  default = 10737418240 # 10 GiB per query
}

variable "glue_workers" {
  type    = number
  default = 2
}

variable "db_subnet_group_name" {
  type = string
}

variable "db_security_group_ids" {
  type = list(string)
}

variable "focus_columns" {
  description = "Columns of the raw_focus external table (name => Hive type)."
  type        = map(string)
  default = {
    BilledCost               = "double"
    EffectiveCost            = "double"
    ListCost                 = "double"
    ContractedCost           = "double"
    BillingCurrency          = "string"
    BillingPeriodStart       = "timestamp"
    BillingPeriodEnd         = "timestamp"
    ChargePeriodStart        = "timestamp"
    ChargePeriodEnd          = "timestamp"
    ChargeCategory           = "string"
    ChargeFrequency          = "string"
    PricingCategory          = "string"
    ServiceCategory          = "string"
    ServiceName              = "string"
    SubAccountId             = "string"
    SubAccountName           = "string"
    ResourceId               = "string"
    RegionId                 = "string"
    CommitmentDiscountId     = "string"
    CommitmentDiscountType   = "string"
    CommitmentDiscountStatus = "string"
    ConsumedQuantity         = "double"
    Tags                     = "string"
    _batch_id                = "string"
  }
}
