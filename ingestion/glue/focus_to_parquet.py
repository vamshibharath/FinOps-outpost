"""AWS Glue (PySpark) job: the production version of ingestion/load_raw.py.

Reads raw FOCUS exports from S3, enforces the same contract (all columns read as strings
first so type drift is detected, not coerced), quarantines bad batches, and writes
provider/billing_period-partitioned Parquet for Athena and Snowflake external tables.

Job args: --JOB_NAME --source_path --lake_path --quarantine_path --batch_id
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

REQUIRED_NUMERIC = ["BilledCost", "EffectiveCost", "ListCost", "ContractedCost", "ConsumedQuantity"]
REQUIRED_COLUMNS = REQUIRED_NUMERIC + [
    "BillingCurrency", "BillingPeriodStart", "BillingPeriodEnd", "ChargePeriodStart",
    "ChargePeriodEnd", "ChargeCategory", "ChargeFrequency", "ProviderName", "ServiceCategory",
    "ServiceName", "SubAccountId", "SubAccountName", "ResourceId", "RegionId", "PricingCategory",
    "CommitmentDiscountType", "CommitmentDiscountStatus", "CommitmentDiscountId", "Tags",
]

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "lake_path", "quarantine_path", "batch_id"])
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

# Fewer shuffle partitions than the default 200: billing batches are MBs-to-low-GBs.
spark.conf.set("spark.sql.shuffle.partitions", "32")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

raw = (spark.read.option("header", True).option("inferSchema", False)
       .option("nullValue", "NULL").option("multiLine", True).option("escape", '"')
       .csv(args["source_path"]))

errors = [f"MISSING_COLUMN:{c}" for c in REQUIRED_COLUMNS if c not in raw.columns]
for c in REQUIRED_NUMERIC:
    if c in raw.columns:
        bad = raw.filter(F.col(c).isNotNull() & F.col(c).cast("double").isNull()).limit(1).count()
        if bad:
            errors.append(f"TYPE_DRIFT:{c}")

if errors:
    raw.write.mode("overwrite").option("header", True).csv(f"{args['quarantine_path']}/batch_id={args['batch_id']}")
    raise RuntimeError(f"Contract violations, batch quarantined: {errors}")

typed = raw
for c in REQUIRED_NUMERIC:
    typed = typed.withColumn(c, F.col(c).cast("double"))

# Same line item delivered twice -> keep the latest version (matches the Scala and local loaders).
from pyspark.sql.window import Window  # noqa: E402

latest_first = Window.partitionBy("ProviderName", "Id").orderBy(F.col("ChargePeriodEnd").desc_nulls_last())
typed = typed.withColumn("_rn", F.row_number().over(latest_first)).filter("_rn = 1").drop("_rn")

out = (typed
       .withColumn("billing_period", F.date_format(F.to_timestamp("BillingPeriodStart"), "yyyy-MM"))
       .withColumn("provider", F.regexp_replace(F.trim("ProviderName"), r"\s+", "_"))
       .withColumn("_batch_id", F.lit(args["batch_id"]))
       .withColumn("_loaded_at", F.current_timestamp()))

# Dynamic partition overwrite = idempotent re-runs, and a restated month replaces only itself.
# One task per partition + maxRecordsPerFile: no swarm of tiny files, no single giant file.
(out.repartition("provider", "billing_period")
    .write.mode("overwrite").partitionBy("provider", "billing_period")
    .option("maxRecordsPerFile", 1_000_000)
    .option("compression", "snappy").parquet(args["lake_path"]))

job.commit()
