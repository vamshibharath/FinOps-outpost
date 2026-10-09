#!/usr/bin/env bash
# Build the Scala Spark ingestion job and run it locally on a FOCUS file, then cross-check
# its output against the Python loader's lake (same rows, same cost totals per partition).
# On a cluster the same jar runs with spark-submit (EMR / EMR Serverless) — Spark is "provided".
set -euo pipefail
SRC="${1:-data/landing/focus_sample_100000.csv.gz}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROJ="$ROOT/ingestion/spark-scala"
OUT="$ROOT/data/lake/scala_focus"

( cd "$PROJ" && mvn -B -q -DskipTests package \
    && mvn -B -q dependency:build-classpath -Dmdep.includeScope=provided -Dmdep.outputFile=target/classpath.txt )

OPENS=$(sed -n 's:.*<spark.jvm.opens>\(.*\)</spark.jvm.opens>.*:\1:p' "$PROJ/pom.xml")
BATCH="scala_$(sha256sum "$SRC" | cut -c1-16)"
rm -rf "$OUT"
# shellcheck disable=SC2086
java $OPENS -Xmx2g -Dlog4j.configurationFile="$PROJ/src/main/resources/log4j2-local.properties" \
  -cp "$PROJ/target/focus-ingest-1.0.0.jar:$(cat "$PROJ/target/classpath.txt")" \
  com.finops.ingest.FocusIngestJob "$ROOT/$SRC" "$OUT" "$ROOT/data/quarantine/scala" "$BATCH" 50000

python3 - "$OUT" <<'PY'
import sys, duckdb
c = duckdb.connect()
q = "SELECT provider, billing_period, count(*), round(sum(EffectiveCost), 6) FROM read_parquet('{}/**/*.parquet', hive_partitioning=true) GROUP BY ALL ORDER BY ALL"
scala = c.execute(q.format(sys.argv[1])).fetchall()
py = c.execute(q.format("data/lake/raw_focus")).fetchall()
print("[scala-ingest] partition       rows     effective_cost")
for r in scala:
    print(f"[scala-ingest] {r[0]}/{r[1]:<9} {r[2]:>7,} {r[3]:>14}")
print("[scala-ingest] matches Python loader:", scala == py if py else "python lake not built yet")
PY
