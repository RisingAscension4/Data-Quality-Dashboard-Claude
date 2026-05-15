# Two Reports, One Truth — Radiology Revenue Cycle Demo

**Pinnacle Radiology Partners** — A data quality demonstration showing how 8 DQ defects across 6 dimensions cause two executive reports to diverge on 6 KPIs, despite querying the same underlying data.

## Core Thesis

> "Your reports don't disagree because your teams are incompetent. They disagree because your data is ambiguous — and ambiguity forces assumptions. Different assumptions produce different numbers."

If the underlying data were clean, both reporting philosophies (conservative exclusion vs. inclusive counting) would produce identical numbers. The divergence **measures** how much defective data exists.

## Repository Structure

```
rcm-pipeline/
├── README.md                          ← This file
├── Radiology Revenue Cycle - Data Model Design Document.md
│                                      ← Full design doc with 8 trust traps,
│                                         ERD, DQ rules, and talking points
├── 01_radiology_revenue_cycle_ddl     ← DDL notebook: creates 11 tables in
│                                         serverless_stable_jc9zgx_catalog.radiology_revenue_cycle
├── 02_generate_synthetic_data         ← Data generation: 284K rows with
│                                         intentionally seeded DQ defects
├── 03_report_views                    ← Report A (CFO) and Report B (RevOps)
│                                         views + divergence comparison view
└── rcm_pipeline_setup                 ← Setup/config notebook
```

### Dashboard (external to this folder)
- **Name**: "Two Reports, One Truth — Pinnacle Radiology Partners"
- **Dashboard ID**: `01f150b14a401174bcb3a17fc45a77c2`
- **Workspace Path**: `/Users/bryan.goodliffe@databricks.com/Two Reports, One Truth — Pinnacle Radiology Partners.lvdash.json`
- **Pages**: Executive Summary + Root Cause Analysis

To include the dashboard in Git, copy the `.lvdash.json` file into this folder:
```bash
cp "/Workspace/Users/bryan.goodliffe@databricks.com/Two Reports, One Truth — Pinnacle Radiology Partners.lvdash.json" \
   "/Workspace/Users/bryan.goodliffe@databricks.com/Data-Quality-Dashboard-Claude/rcm-pipeline/"
```

## How to Reproduce

### Prerequisites
- Access to `serverless_stable_jc9zgx_catalog` (or modify CATALOG variable)
- SQL warehouse or serverless compute

### Steps

1. **Create schema and tables**
   Run `01_radiology_revenue_cycle_ddl` — creates 11 empty tables with proper schemas and TBLPROPERTIES tagging each trust trap.

2. **Generate synthetic data**
   Run `02_generate_synthetic_data` — populates all tables (~284K rows) with intentionally seeded DQ defects:
   - COMPLETENESS: 20% NULL allowed on high-charge modalities (MRI/NM/IR)
   - VALIDITY: 30% of denied claims have invalid CARC codes (isolated)
   - CONSISTENCY: 3–5 payer name variants per payer
   - UNIQUENESS: 3% unflagged rebill duplicates (pre-Oct 2025)
   - TIMELINESS: Hospital AR snapshots 9 days stale
   - ACCURACY-A: 2% TC/26 component overlap
   - ACCURACY-B: PAY-011/012 allowed=charge (clearinghouse error)
   - ACCURACY-C: BCBS Q1 2026 stale fee schedule (12% below)

3. **Create report views**
   Run `03_report_views` — creates three views:
   - `v_cfo_monthly_summary` (Report A — conservative/exclusion)
   - `v_revops_performance` (Report B — inclusive/naive)
   - `v_report_divergence` (side-by-side comparison)

4. **Open the dashboard**
   The Lakeview dashboard reads from `v_report_divergence` and displays the divergence.

## KPI Divergence Results

| KPI | Report A (CFO) | Report B (RevOps) | Gap | Root Cause |
|-----|---------------|-------------------|-----|------------|
| Net Collection Rate | 82.9% | 76.4% | 6.5pp | COMPLETENESS |
| Denial Rate | 9.6% | 13.5% | 3.9pp | VALIDITY |
| Days in AR | 47 | 60 | 13 days | TIMELINESS |
| Commercial Payer Mix | 48.9% | 29.4% | 19.5pp | CONSISTENCY |
| Total Charges | $54.5M | $59.0M | $4.5M | UNIQUENESS + ACCURACY |
| Net Revenue | $23.2M | $19.1M | $4.1M | ACCURACY |

## Schema & Catalog

- **Catalog**: `serverless_stable_jc9zgx_catalog`
- **Schema**: `radiology_revenue_cycle`
- **Tables**: patient, provider, service_location, payer, encounter, claim_header, claim_line, denial, payment, ar_aging_snapshot, authorization

## Key Design Decisions

1. **Data defects drive divergence, not view logic** — both reports apply defensible business rules; the gap only exists because of defective data
2. **One legitimate view fix**: Report B's payer_mix uses an incomplete variant lookup (realistic for a team without a canonical payer reference)
3. **Seed is fixed** (`random.seed(42)`) for reproducibility
4. **No DEFAULT clauses** in DDL (Delta requirement)
5. **Dashboard avoids `$` in filter values** (stripped by query engine as parameter markers)
