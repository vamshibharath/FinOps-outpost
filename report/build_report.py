"""Build a self-contained HTML cost report from the marts (the 'Finance-facing' output).

Opens in any browser, no server needed. Charts load Chart.js from cdnjs.
"""
from __future__ import annotations

import html
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ingestion.load_raw import read_manifest  # noqa: E402

WAREHOUSE = Path("data/warehouse/finops.duckdb")
BENCH = Path("data/warehouse/benchmark.json")
OUT = Path("report/finops_report.html")


def q(con, sql):
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def money(v):
    if v is None:
        return "—"
    if abs(v) < 0.005:  # avoid "-$0.00" from rounding
        return "$0.00"
    return f"−${abs(v):,.2f}" if v < 0 else f"${v:,.2f}"


def pct(v):
    return "—" if v is None else f"{v:.1%}"


def main() -> None:
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    kpi = q(con, """
        SELECT round(sum(effective_cost), 2) AS effective, round(sum(billed_cost), 2) AS billed,
               count(*) AS line_items, count(DISTINCT provider) AS providers
        FROM staging.stg_focus__charges""")[0]
    method = q(con, """
        SELECT allocation_method, round(sum(allocated_effective_cost), 2) AS cost, sum(source_line_items) AS lines
        FROM marts.fct_cost_allocated GROUP BY 1 ORDER BY 2 DESC""")
    usage_cov = q(con, """
        SELECT provider, round(sum(effective_cost), 2) AS cost,
               round(sum(tagged_cost) / nullif(sum(effective_cost), 0), 4) AS coverage,
               round(sum(untagged_cost), 2) AS untagged
        FROM marts.mart_tag_coverage GROUP BY 1 HAVING sum(effective_cost) > 0 ORDER BY cost DESC""")
    top_untagged = q(con, """
        SELECT provider, sub_account_name, round(untagged_cost, 2) AS untagged_cost, tag_coverage_pct
        FROM marts.mart_tag_coverage WHERE untagged_cost > 0 ORDER BY untagged_cost DESC LIMIT 8""")
    chargeback = q(con, """
        SELECT business_unit, round(sum(direct_cost), 2) AS direct, round(sum(shared_cost), 2) AS shared,
               round(sum(total_chargeback), 2) AS total
        FROM marts.mart_chargeback_monthly GROUP BY 1 ORDER BY total DESC LIMIT 10""")
    commit = q(con, "SELECT * FROM marts.mart_commitment_coverage ORDER BY provider")
    rec = q(con, "SELECT * FROM marts.mart_commitment_recommendation ORDER BY provider")
    recon = q(con, """SELECT check_name, expected, actual, diff, passed FROM audit.reconciliation_runs
                      WHERE run_id = (SELECT run_id FROM audit.reconciliation_runs ORDER BY run_at DESC LIMIT 1)""")
    completeness = q(con, "SELECT * FROM marts.mart_data_completeness ORDER BY provider, billing_period")
    drivers = q(con, """
        SELECT allocation_method, count(DISTINCT business_unit) AS teams,
               round(sum(allocated_effective_cost), 2) AS cost
        FROM marts.fct_cost_allocated WHERE allocation_method LIKE 'shared_%'
        GROUP BY 1 ORDER BY 1""")
    con.close()
    batches = read_manifest()
    bench = json.loads(BENCH.read_text()) if BENCH.exists() else None

    usage_total = sum(r["cost"] for r in usage_cov)
    tagged_total = sum(r["cost"] * (r["coverage"] or 0) for r in usage_cov)
    recon_ok = all(r["passed"] for r in recon)
    recon_overall = [r for r in recon if "[" not in r["check_name"]]
    recon_periods = [r for r in recon if "[" in r["check_name"]]
    recon_periods_ok = sum(1 for r in recon_periods if r["passed"])
    driver_label = {
        "shared_telemetry_cpu": "CPU core-hours (telemetry API)",
        "shared_telemetry_requests": "Request volume (telemetry API)",
        "shared_usage_driver": "Compute hours on the bill",
        "shared_category_cost": "Share of spend in that service category",
        "shared_provider_cost": "Share of total provider spend (credits, tax)",
    }

    if bench:
        sc = bench["scenarios"]
        names = {"gzip_csv_full_scan": "gzip CSV, as delivered (full scan)",
                 "parquet_column_pruning": "Parquet, only needed columns",
                 "parquet_column_and_partition_pruning": "Parquet, needed columns + AWS/2024-09 partition only"}
        brows = "".join(
            f"<tr><td>{names[k]}</td><td class='n'>{v['bytes_scanned']/1e6:,.2f} MB</td>"
            f"<td class='n'>{v['scan_reduction_vs_csv']:.1%}</td><td class='n'>{v['seconds']*1000:,.1f} ms</td></tr>"
            for k, v in sc.items())
        st = bench["storage"]
        bench_section = f"""<div class="card"><h2>Measured storage and scan savings</h2>
<p class="note">Same question ("{html.escape(bench['query'])}") against three layouts. Bytes scanned is what Athena bills
(${bench['athena_usd_per_tb']:.0f}/TB); read from Parquet footer metadata, not estimated. All layouts return identical results:
{'yes' if bench['results_match'] else '<span class=bad>NO</span>'}.</p>
<div class="scroll"><table><tr><th>Layout</th><th class="n">Bytes scanned</th><th class="n">Reduction</th><th class="n">Median latency</th></tr>
{brows}</table></div>
<p class="note">Storage: raw CSV {st['csv_uncompressed_bytes']/1e6:,.1f} MB · gzip CSV {st['csv_gzip_bytes']/1e6:,.1f} MB ·
Parquet lake {st['parquet_lake_bytes']/1e6:,.1f} MB. The sample is small, so the ratios are the result, not the dollars.</p></div>"""
    else:
        bench_section = ""

    def rows(items, cols, num_from=1, num_to=None):
        def cell(i, c, r):
            numeric = i >= num_from and (num_to is None or i < num_to)
            return f"<td class='n'>{c(r)}</td>" if numeric else f"<td>{c(r)}</td>"
        return "".join("<tr>" + "".join(cell(i, c, r) for i, c in enumerate(cols)) + "</tr>" for r in items)

    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Cloud Cost Attribution</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root{{--bg:#f7f7f5;--card:#fff;--ink:#1d1d1b;--muted:#6b6b66;--line:#e4e3de;--accent:#2f5d8a;--good:#2e7d4f;--bad:#b3261e}}
@media (prefers-color-scheme:dark){{:root{{--bg:#151515;--card:#1f1f1e;--ink:#ececea;--muted:#a3a39d;--line:#33332f;--accent:#7fa8d1;--good:#6cc08f;--bad:#f08a80}}}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 -apple-system,Segoe UI,Inter,sans-serif}}
main{{max-width:1100px;margin:0 auto;padding:32px 16px}}h1{{margin:0 0 4px;font-size:26px}}h2{{font-size:17px;margin:0 0 12px}}
.sub{{color:var(--muted);margin:0 0 24px}}.grid{{display:grid;gap:16px;grid-template-columns:repeat(auto-fit,minmax(220px,1fr))}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:18px;margin-bottom:16px}}
.kpi .v{{font-size:26px;font-weight:650}}.kpi .l{{color:var(--muted);font-size:13px}}
table{{width:100%;border-collapse:collapse;font-size:14px}}th,td{{text-align:left;padding:7px 8px;border-bottom:1px solid var(--line)}}
th{{color:var(--muted);font-weight:600}}td.n,th.n{{text-align:right;font-variant-numeric:tabular-nums}}
.ok{{color:var(--good);font-weight:600}}.bad{{color:var(--bad);font-weight:600}}.note{{color:var(--muted);font-size:13px}}
.two{{display:grid;gap:16px;grid-template-columns:repeat(auto-fit,minmax(320px,1fr))}}canvas{{max-height:300px}}
.scroll{{overflow-x:auto}}
</style></head><body><main>
<h1>Cloud Cost Attribution Report</h1>
<p class="sub">Multi-cloud FOCUS billing · report run {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC} ·
reconciliation <span class="{'ok' if recon_ok else 'bad'}">{'PASSED' if recon_ok else 'FAILED'}</span></p>

<div class="grid">
 <div class="card kpi"><div class="l">Effective (amortized) cost</div><div class="v">{money(kpi['effective'])}</div></div>
 <div class="card kpi"><div class="l">Billed (invoice) cost</div><div class="v">{money(kpi['billed'])}</div></div>
 <div class="card kpi"><div class="l">Usage spend tagged directly</div><div class="v">{pct(tagged_total / usage_total if usage_total else None)}</div></div>
 <div class="card kpi"><div class="l">Line items · providers</div><div class="v">{kpi['line_items']:,} · {kpi['providers']}</div></div>
</div>

<div class="two">
 <div class="card"><h2>How every dollar was attributed</h2><canvas id="m"></canvas>
 <p class="note">Negative shared amounts are credits, distributed in proportion to each team's spend.</p></div>
 <div class="card"><h2>Tag coverage by provider (usage spend)</h2><canvas id="c"></canvas></div>
</div>

<div class="card"><h2>Top business units — chargeback</h2><div class="scroll"><table>
<tr><th>Business unit</th><th class="n">Direct</th><th class="n">Shared allocation</th><th class="n">Total chargeback</th></tr>
{rows(chargeback, [lambda r: html.escape(str(r['business_unit'])), lambda r: money(r['direct']),
                   lambda r: money(r['shared']), lambda r: f"<b>{money(r['total'])}</b>"])}
</table></div></div>

<div class="card"><h2>Shared-cost drivers</h2><p class="note">Untagged spend is split by a governed rule per
category. Compute and networking use the usage telemetry supplied for this demonstration. Production deployment connects this step to the client's observability endpoint.</p>
<div class="scroll"><table><tr><th>Driver</th><th class="n">Teams receiving a share</th><th class="n">Cost allocated</th></tr>
{rows(drivers, [lambda r: driver_label.get(r['allocation_method'], r['allocation_method']),
                lambda r: f"{r['teams']:,}", lambda r: money(r['cost'])])}
</table></div></div>

<div class="card"><h2>Where to fix tagging first</h2><p class="note">Sub-accounts ranked by untagged usage spend — the governance backlog.</p>
<div class="scroll"><table><tr><th>Provider</th><th>Sub-account</th><th class="n">Untagged spend</th><th class="n">Coverage</th></tr>
{rows(top_untagged, [lambda r: r['provider'], lambda r: html.escape(str(r['sub_account_name'])),
                     lambda r: money(r['untagged_cost']), lambda r: pct(r['tag_coverage_pct'])], num_from=2)}
</table></div></div>

<div class="card"><h2>Commitments (Savings Plans / Reservations / CUDs)</h2><div class="scroll"><table>
<tr><th>Provider</th><th class="n">Committed</th><th class="n">On-demand</th><th class="n">Unused (waste)</th><th class="n">Coverage</th></tr>
{rows(commit, [lambda r: r['provider'] + ' ' + r['billing_period'], lambda r: money(r['committed_cost']),
               lambda r: money(r['on_demand_cost']), lambda r: money(r['unused_commitment_cost']),
               lambda r: pct(r['commitment_coverage_pct'])])}
</table></div>
<h2 style="margin-top:18px">Recommended commitment (safe floor)</h2><div class="scroll"><table>
<tr><th>Provider</th><th class="n">Daily commit</th><th class="n">Share of on-demand covered</th><th class="n">Est. savings / period</th></tr>
{rows(rec, [lambda r: r['provider'] + ' ' + r['billing_period'], lambda r: money(r['recommended_daily_commitment']),
            lambda r: pct(r['share_of_on_demand_covered']), lambda r: money(r['est_savings_for_period'])])}
</table></div><p class="note">Floor = 10th percentile of daily on-demand compute; savings assume a
{rec[0]['assumed_discount_rate'] if rec else 0.28:.0%} blended discount (planning estimate, configurable in dbt vars).</p></div>

<div class="card"><h2>Data completeness</h2><p class="note">Days with usage vs days in the billing month.
A partial month is the most common reason a chargeback looks too low. Late-arriving lines = usage incurred in an earlier month but billed in this one.</p>
<div class="scroll"><table><tr><th>Provider / month</th><th class="n">Days with usage</th><th class="n">Missing days</th>
<th class="n">Complete</th><th class="n">Late-arriving lines</th></tr>
{rows(completeness, [lambda r: f"{r['provider']} {r['billing_period']}", lambda r: f"{r['days_with_usage']} / {r['days_in_period']}",
                     lambda r: (f"<span class='bad'>{r['missing_days']}</span>" if r['missing_days'] else "0"),
                     lambda r: pct(r['completeness_pct']), lambda r: f"{r['late_arriving_lines']:,}"])}
</table></div></div>

<div class="card"><h2>Batch history and restatements</h2><p class="note">Providers re-send whole months. A newer batch
replaces only the provider/month partitions it contains; duplicate line items are dropped on load.</p>
<div class="scroll"><table><tr><th>Batch</th><th>Source file</th><th class="n">Rows</th><th class="n">Duplicates dropped</th><th>Replaced</th></tr>
{rows(batches, [lambda r: r['batch_id'], lambda r: html.escape(r['source_file']), lambda r: f"{r['rows']:,}",
                lambda r: f"{r.get('duplicates_dropped', 0):,}",
                lambda r: html.escape(', '.join(r.get('replaced_partitions', [])) or '—')], num_from=2, num_to=4)}
</table></div></div>

{bench_section}

<div class="card"><h2>Reconciliation audit (latest run)</h2>
<p class="note">{recon_periods_ok} of {len(recon_periods)} per-provider/month checks passed.</p><div class="scroll"><table>
<tr><th>Check</th><th class="n">Expected</th><th class="n">Actual</th><th class="n">Diff</th><th class="n">Status</th></tr>
{rows(recon_overall, [lambda r: r['check_name'], lambda r: f"{r['expected']:,.6f}", lambda r: f"{r['actual']:,.6f}",
              lambda r: f"{r['diff']:.8f}", lambda r: "<span class='ok'>PASS</span>" if r['passed'] else "<span class='bad'>FAIL</span>"])}
</table></div></div>

<p class="note">Data: FinOps Foundation FOCUS 1.0 sample (anonymized real billing; amounts are scaled).</p>
</main>
<script>
const css = getComputedStyle(document.documentElement);
const ink = css.getPropertyValue('--muted').trim(), accent = css.getPropertyValue('--accent').trim();
Chart.defaults.color = ink; Chart.defaults.borderColor = css.getPropertyValue('--line').trim();
new Chart(document.getElementById('m'), {{type:'bar', data:{{labels:{json.dumps([m['allocation_method'] for m in method])},
  datasets:[{{data:{json.dumps([m['cost'] for m in method])}, backgroundColor:accent}}]}},
  options:{{indexAxis:'y', plugins:{{legend:{{display:false}}}}}}}});
new Chart(document.getElementById('c'), {{type:'bar', data:{{labels:{json.dumps([r['provider'] for r in usage_cov])},
  datasets:[{{data:{json.dumps([round((r['coverage'] or 0) * 100, 1) for r in usage_cov])}, backgroundColor:accent}}]}},
  options:{{plugins:{{legend:{{display:false}}}}, scales:{{y:{{min:0,max:100,ticks:{{callback:v=>v+'%'}}}}}}}}}});
</script></body></html>"""
    OUT.write_text(page)
    print(f"[report] wrote {OUT}")


if __name__ == "__main__":
    main()
