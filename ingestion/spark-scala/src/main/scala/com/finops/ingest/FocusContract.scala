package com.finops.ingest

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** The same data contract as ingestion/contracts/focus_v1_contract.yaml, enforced in Spark.
  *
  * The raw file is read with every column as a string, so a value like "$1.23" in a cost
  * column is *counted* as drift instead of being silently cast to null.
  */
object FocusContract {

  val NumericColumns: Seq[String] =
    Seq("BilledCost", "EffectiveCost", "ListCost", "ContractedCost", "ConsumedQuantity")

  val TimestampColumns: Seq[String] =
    Seq("BillingPeriodStart", "BillingPeriodEnd", "ChargePeriodStart", "ChargePeriodEnd")

  val StringColumns: Seq[String] = Seq(
    "BillingCurrency", "ChargeCategory", "ChargeFrequency", "ProviderName", "ServiceCategory",
    "ServiceName", "SubAccountId", "SubAccountName", "ResourceId", "RegionId", "PricingCategory",
    "CommitmentDiscountType", "CommitmentDiscountStatus", "CommitmentDiscountId", "Tags", "Id")

  val RequiredColumns: Seq[String] = NumericColumns ++ TimestampColumns ++ StringColumns

  /** Columns that must never be null: losing these breaks reconciliation. */
  val NotNullColumns: Seq[String] = Seq("EffectiveCost", "ProviderName", "BillingPeriodStart")

  final case class Violation(code: String, detail: String) {
    override def toString: String = s"$code: $detail"
  }

  /** Lower-case and strip non-alphanumerics: "Effective_Cost" and "EffectiveCost" both -> "effectivecost". */
  private[ingest] def normalize(name: String): String = name.toLowerCase.replaceAll("[^a-z0-9]", "")

  def validate(raw: DataFrame): Seq[Violation] = {
    val actual = raw.columns.toSeq
    val unexpected = actual.filterNot(RequiredColumns.contains)
    val missing = RequiredColumns.filterNot(actual.contains)

    val structural = missing.map { col =>
      unexpected.find(u => normalize(u) == normalize(col)) match {
        case Some(found) => Violation("PROBABLE_RENAME", s"'$col' missing; found '$found'")
        case None        => Violation("MISSING_COLUMN", s"'$col' not present")
      }
    }

    // One pass over the data for all value-level checks (not one Spark job per column).
    val present = (c: String) => actual.contains(c)
    val numericChecks = NumericColumns.filter(present).map { c =>
      sum(when(col(c).isNotNull && col(c).cast("double").isNull, 1).otherwise(0)).as(s"drift_$c")
    }
    val timestampChecks = TimestampColumns.filter(present).map { c =>
      sum(when(col(c).isNotNull && to_timestamp(col(c)).isNull, 1).otherwise(0)).as(s"drift_$c")
    }
    val nullChecks = NotNullColumns.filter(present).map { c =>
      sum(when(col(c).isNull, 1).otherwise(0)).as(s"null_$c")
    }
    val checks = numericChecks ++ timestampChecks ++ nullChecks :+ count(lit(1)).as("rows")
    val row = raw.agg(checks.head, checks.tail: _*).collect().head

    val drift = (NumericColumns ++ TimestampColumns).filter(present).flatMap { c =>
      val bad = row.getAs[Long](s"drift_$c")
      if (bad > 0) Some(Violation("TYPE_DRIFT", s"$c has $bad unparseable values")) else None
    }
    val nulls = NotNullColumns.filter(present).flatMap { c =>
      val n = row.getAs[Long](s"null_$c")
      if (n > 0) Some(Violation("NULL_THRESHOLD", s"$c has $n nulls")) else None
    }
    val empty = if (row.getAs[Long]("rows") == 0L) Seq(Violation("EMPTY_BATCH", "0 rows")) else Nil

    structural ++ drift ++ nulls ++ empty
  }
}
