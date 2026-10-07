# 5-minute walkthrough

Have open: terminal at the repo root, `report/finops_report.html` in a browser, `fct_cost_allocated.sql` in an editor.

## 1. Frame the problem (30 sec)

"I built this around three things from your job description: multi-cloud cost attribution,
data Finance can trust, and committed-use discount modeling. It runs on the FinOps
Foundation's FOCUS sample — anonymized real billing from AWS, Azure, Oracle and a bit of GCP."

## 2. The contract gate (60 sec) — `make drift-demo`

Show the output: the gate flags `PROBABLE_RENAME: EffectiveCost → Effective_Cost` and
`TYPE_DRIFT: BilledCost ... '$0.03'`, quarantines the file, and the lake is untouched.

"The failure I've seen most is an upstream change that the loader accepts as NULLs, so spend
looks like it dropped. Reading everything as text first means drift gets detected instead of
coerced away. Re-running a good file is a no-op because batches are fingerprinted."

## 3. Messy tags → one owner (45 sec) — `seeds/tag_key_aliases.csv`

"The real data has `business_unit`, `org`, `org` with a leading space, and `CostCenter` all
meaning the same thing. Instead of hard-coding that in SQL, it's a governed alias table — in
production it lives in Aurora so FinOps can edit it without a deploy."

## 4. Allocation (60 sec) — `fct_cost_allocated.sql`, report chart

"Tagged spend goes direct. Untagged spend is pooled and split by rule: compute by each team's
compute hours, storage and databases by spend share. Credits get their own pool and follow
spend — otherwise whoever runs the most VMs gets most of the discount. If no rule applies it
falls back, and if nothing applies it lands in an explicit UNALLOCATED bucket. Every row says
which method produced it, so a disputed number is traceable."

## 5. Trust (45 sec) — `make reconcile`, report audit table

"Three-way reconciliation: source file control totals, warehouse totals, allocated totals.
It writes an audit row every run, and a dbt test fails the build if allocation creates or
loses a cent. The Airflow DAG won't publish if reconciliation fails."

## 6. Commitments (45 sec) — report commitments section

"AWS compute here is under 1% covered by commitments. The recommendation sizes a commitment
to the 10th percentile of daily on-demand spend, so it stays fully used on quiet days. The
discount rate is a dbt variable — I'd want your actual SP and CUD rates before trusting the
dollar figure."

## 7. Close (15 sec)

"Locally it's DuckDB; the same dbt models target Snowflake, and the Glue job, Terraform and
Airflow DAG are the production shape. I'd love to hear how close this is to how your team
does it today."

---

## Questions they may ask

**Why effective cost instead of billed cost?** Effective cost amortizes commitment purchases
over the usage they covered. Billed cost is what the invoice shows, so it's used for invoice
reconciliation. Both are carried through allocation.

**What if a team disputes their shared allocation?** Every row carries `allocation_method`, and
the rules are in a governed table. The answer is "here's the rule, here's your share of the
driver" — and if the rule is wrong, change the table, not the SQL.

**How would this scale to billions of rows?** The Glue job writes provider/month partitions;
Athena uses partition projection; in Snowflake the staging layer becomes incremental on
`billing_period` with late-arriving restatements handled by reprocessing the open months
(providers revise the current and prior month).

**How would you add telemetry-driven allocation?** Replace the usage-driver weight set with
per-service metrics (e.g. Datadog request counts or CPU-seconds by service tag), landed through
the same contract gate.

**What's missing?** Real GCP volume, live pricing for savings, and incremental models.
