# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository

https://github.com/RisingAscension4/Data-Quality-Dashboard-Claude

## Project Overview

This is the **Meridian Health System Enterprise Data Quality Dashboard** — a Databricks AI/BI (Lakeview) dashboard that visualizes data quality scores across an organization hierarchy: Organization > Domain > Catalog > Schema > Table > Column. Quality is measured across six dimensions: Completeness, Accuracy, Validity, Consistency, Uniqueness, and Timeliness.

## Key Commands

```bash
# Generate the dashboard JSON (outputs dq_dashboard.json)
python build_dashboard.py

# Deploy to Databricks (uses the Databricks CLI with the configured profile)
databricks lakeview deploy dq_dashboard.json --dashboard-name dq_dashboard_test_step3 --warehouse-id dce104510613317a --profile fe-vm-serverless-stable-jc9zgx
```

There are no tests, linters, or package dependencies beyond the Python standard library.

## Architecture

### Data Pipeline

1. **`dq_data.json`** — Raw synthetic dataset (Meridian Health System) loaded into Databricks via a Volume
2. **`ddl_and_load_notebook.sql`** — Databricks notebook that creates all Delta tables, views, and loads data from the JSON. Run this in a Databricks workspace to set up the backend.
3. **`build_dashboard.py`** — Python script that programmatically builds the Lakeview dashboard JSON definition
4. **`dq_dashboard.json`** — Generated output from `build_dashboard.py` (the Lakeview API payload)
5. **`Data Quality Dashboard.lvdash.json`** — An earlier/alternative version of the dashboard in native `.lvdash.json` format

### Database Schema (Unity Catalog)

All tables live in `serverless_stable_jc9zgx_catalog.data_quality`. The hierarchy is:

- **Dimension tables**: `organization`, `data_domain`, `catalog`, `schema`, `dq_table`, `dq_column`, `business_owner`, `quality_dimension`, `dq_rule_category`
- **Rule tables**: `dq_rule` (rule library), `column_rule_assignment` (binds rules to columns)
- **Fact tables**: `dq_run` (execution runs), `column_rule_result` (per-column-rule-run scores)
- **Alert table**: `dq_alert`
- **Views**: Score rollups at each hierarchy level (`v_org_score_latest`, `v_domain_scores_latest`, `v_catalog_scores_latest`, `v_schema_scores_latest`, `v_table_scores_latest`, `v_column_scores_latest`) plus trend views (`v_domain_score_trend`, `v_table_score_trend`) and detail views (`v_column_rule_detail`, `v_owner_scores_latest`)

### Dashboard Builder (`build_dashboard.py`)

The script builds a Lakeview JSON payload with these sections:

- **Datasets**: SQL queries defined via the `ds()` helper, referencing the views above
- **Pages/Tabs** (6 total): Organization Overview, Data Domain, Business Owner, Catalog/Database, Table Detail, Rule Library
- **Widget helpers**: `counter_widget()`, `bar_widget()`, `pie_widget()`, `table_widget()`, `line_widget()` — each returns a Lakeview widget dict
- **Config constants** at the top: `CATALOG_SCHEMA`, `WAREHOUSE_ID`, `PROFILE`, `DASHBOARD_NAME`

When adding new dashboard tabs or widgets, follow the existing pattern: define datasets with `ds()`, then build page layouts using the widget helper functions with grid positions `{"x", "y", "width", "height"}` on a 6-column grid.
