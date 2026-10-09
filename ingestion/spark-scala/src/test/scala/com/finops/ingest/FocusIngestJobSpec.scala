package com.finops.ingest

import java.nio.file.{Files, Path}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class FocusIngestJobSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]").appName("focus-ingest-test")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  import FocusContract._

  /** A valid all-string row, as the raw reader would produce. */
  private def row(id: String, provider: String = "AWS", billed: String = "1.5",
                  effective: String = "1.2", chargeEnd: String = "2024-09-02 01:00:00"): Map[String, String] =
    RequiredColumns.map(_ -> "x").toMap ++ Map(
      "Id" -> id, "ProviderName" -> provider, "BilledCost" -> billed, "EffectiveCost" -> effective,
      "ListCost" -> "2.0", "ContractedCost" -> "1.4", "ConsumedQuantity" -> "1",
      "BillingPeriodStart" -> "2024-09-01 00:00:00", "BillingPeriodEnd" -> "2024-10-01 00:00:00",
      "ChargePeriodStart" -> "2024-09-02 00:00:00", "ChargePeriodEnd" -> chargeEnd)

  private def df(rows: Seq[Map[String, String]], rename: Map[String, String] = Map.empty): DataFrame = {
    import spark.implicits._
    val cols = RequiredColumns
    val data = rows.map(r => cols.map(r(_)))
    spark.createDataset(data).toDF("values")
      .select(cols.indices.map(i => $"values".getItem(i).as(rename.getOrElse(cols(i), cols(i)))): _*)
  }

  test("a valid batch has no violations") {
    assert(validate(df(Seq(row("1"), row("2")))).isEmpty)
  }

  test("a renamed column is reported as a probable rename, not just missing") {
    val v = validate(df(Seq(row("1")), rename = Map("EffectiveCost" -> "Effective_Cost")))
    assert(v.exists(x => x.code == "PROBABLE_RENAME" && x.detail.contains("Effective_Cost")))
  }

  test("currency-formatted costs are type drift") {
    val v = validate(df(Seq(row("1", billed = "$1,234.56"), row("2"))))
    assert(v.exists(x => x.code == "TYPE_DRIFT" && x.detail.startsWith("BilledCost has 1")))
  }

  test("null EffectiveCost breaches the not-null threshold") {
    val v = validate(df(Seq(row("1", effective = null))))
    assert(v.exists(_.code == "NULL_THRESHOLD"))
  }

  test("duplicate line items keep the latest version") {
    val out = FocusIngestJob.transform(df(Seq(
      row("1", billed = "1.0", chargeEnd = "2024-09-02 01:00:00"),
      row("1", billed = "9.0", chargeEnd = "2024-09-03 01:00:00"),
      row("2"))), "b1")
    assert(out.count() == 2)
    assert(out.filter("Id = '1'").select("BilledCost").head().getDouble(0) == 9.0)
  }

  test("writes provider/billing_period partitions and a restatement replaces only its partition") {
    val lake: Path = Files.createTempDirectory("lake")
    val p = lake.toString
    FocusIngestJob.write(FocusIngestJob.transform(df(Seq(row("1"), row("2", provider = "Microsoft"))), "b1"), p, 1000L)
    // Restated AWS month arrives with corrected cost; Microsoft must be untouched.
    FocusIngestJob.write(FocusIngestJob.transform(df(Seq(row("1", billed = "3.0"))), "b2"), p, 1000L)

    val result = spark.read.parquet(p)
    assert(Files.exists(lake.resolve("provider=AWS/billing_period=2024-09")))
    assert(result.count() == 2)
    assert(result.filter("provider = 'AWS'").select("BilledCost").head().getDouble(0) == 3.0)
    assert(result.filter("provider = 'Microsoft'").count() == 1)
  }

  test("maxRecordsPerFile bounds file size") {
    val lake = Files.createTempDirectory("lake2").toString
    val rows = (1 to 25).map(i => row(i.toString))
    FocusIngestJob.write(FocusIngestJob.transform(df(rows), "b1"), lake, 10L)
    val files = new java.io.File(s"$lake/provider=AWS/billing_period=2024-09")
      .listFiles().filter(_.getName.endsWith(".parquet"))
    assert(files.length == 3) // 25 rows at 10 per file
  }
}
