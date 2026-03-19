#!/usr/bin/env python3
"""
Build the Meridian Health System Enterprise Data Quality Dashboard
for Databricks AI/BI (Lakeview).

Catalog/Schema: serverless_stable_jc9zgx_catalog.data_quality
"""

import json
import uuid
import subprocess
import sys

CATALOG_SCHEMA = "serverless_stable_jc9zgx_catalog.data_quality"
WAREHOUSE_ID = "dce104510613317a"
PROFILE = "fe-vm-serverless-stable-jc9zgx"
DASHBOARD_NAME = "dq_dashboard_test_step3"

def uid():
    return uuid.uuid4().hex[:8]

# ─── DATASETS ──────────────────────────────────────────────────────────
datasets = []

def ds(name, display, sql):
    datasets.append({"name": name, "displayName": display, "queryLines": [sql]})

CS = CATALOG_SCHEMA

# Tab 1 - Organization Overview
ds("org_kpi_latest", "Org KPI Latest",
   f"SELECT overall_score, completeness_score, accuracy_score, validity_score, consistency_score, uniqueness_score, timeliness_score, tables_below_threshold, pct_assets_monitored, total_tables, total_domains FROM {CS}.v_org_score_latest")

ds("domain_scores_latest", "Domain Scores Latest",
   f"SELECT domain_name, overall_score, completeness_score, accuracy_score, validity_score, consistency_score, uniqueness_score, timeliness_score, tables_below_threshold FROM {CS}.v_domain_scores_latest ORDER BY overall_score ASC")

ds("org_trend_30d", "Org 30-Day Trend",
   f"SELECT dst.run_timestamp, ROUND(AVG(dst.overall_score),2) AS overall_score, ROUND(AVG(dst.completeness_score),2) AS completeness_score, ROUND(AVG(dst.accuracy_score),2) AS accuracy_score, ROUND(AVG(dst.validity_score),2) AS validity_score, ROUND(AVG(dst.consistency_score),2) AS consistency_score, ROUND(AVG(dst.uniqueness_score),2) AS uniqueness_score, ROUND(AVG(dst.timeliness_score),2) AS timeliness_score FROM {CS}.v_domain_score_trend dst GROUP BY dst.run_timestamp ORDER BY dst.run_timestamp")

ds("top10_egregious", "Top 10 Egregious Tables",
   f"SELECT table_name, domain_name, catalog_name, overall_score, egregious_count FROM {CS}.v_table_scores_latest ORDER BY overall_score ASC LIMIT 10")

ds("tables_below_thresh", "Tables Below Threshold",
   f"SELECT COUNT(*) AS cnt FROM {CS}.v_table_scores_latest WHERE overall_score < effective_threshold_min")

ds("pct_monitored", "Pct Assets Monitored",
   f"SELECT pct_assets_monitored FROM {CS}.v_org_score_latest")

# Tab 2 - Data Domain
ds("domain_detail", "Domain Detail",
   f"SELECT d.domain_name, cs.catalog_name, ROUND(AVG(ts.overall_score),2) AS overall_score, ROUND(AVG(ts.completeness_score),2) AS completeness_score, ROUND(AVG(ts.accuracy_score),2) AS accuracy_score, ROUND(AVG(ts.validity_score),2) AS validity_score, ROUND(AVG(ts.consistency_score),2) AS consistency_score, ROUND(AVG(ts.uniqueness_score),2) AS uniqueness_score, ROUND(AVG(ts.timeliness_score),2) AS timeliness_score, SUM(ts.rules_failed) AS total_failures, SUM(ts.egregious_count) AS egregious_count FROM {CS}.v_table_scores_latest ts JOIN {CS}.catalog cs ON ts.catalog_id = cs.catalog_id JOIN {CS}.data_domain d ON ts.domain_id = d.domain_id GROUP BY d.domain_name, cs.catalog_name")

ds("domain_trend", "Domain 30-Day Trend",
   f"SELECT d.domain_name, dst.run_timestamp, dst.overall_score, dst.completeness_score, dst.accuracy_score, dst.validity_score, dst.consistency_score, dst.uniqueness_score, dst.timeliness_score FROM {CS}.v_domain_score_trend dst JOIN {CS}.data_domain d ON dst.domain_id = d.domain_id ORDER BY dst.run_timestamp")

ds("domain_critical_tables", "Critical Tables at Risk",
   f"SELECT t.table_name, s.schema_name, ts.overall_score, ts.rules_failed, d.domain_name FROM {CS}.v_table_scores_latest ts JOIN {CS}.dq_table t ON ts.table_id = t.table_id JOIN {CS}.schema s ON ts.schema_id = s.schema_id JOIN {CS}.data_domain d ON ts.domain_id = d.domain_id WHERE t.is_critical = true AND ts.overall_score < ts.effective_threshold_warning ORDER BY ts.overall_score ASC")

ds("domain_rule_passfail", "Domain Rule Pass/Fail",
   f"SELECT d.domain_name, SUM(CASE WHEN crr.passed THEN 1 ELSE 0 END) AS passed_count, SUM(CASE WHEN NOT crr.passed THEN 1 ELSE 0 END) AS failed_count, COUNT(*) AS total_rules FROM {CS}.column_rule_result crr JOIN {CS}.column_rule_assignment cra ON crr.assignment_id = cra.assignment_id JOIN {CS}.dq_column dc ON cra.column_id = dc.column_id JOIN {CS}.dq_table t ON dc.table_id = t.table_id JOIN {CS}.data_domain d ON t.domain_id = d.domain_id JOIN {CS}.v_latest_run lr ON crr.run_id = lr.latest_run_id GROUP BY d.domain_name")

# Tab 3 - Business Owner
ds("owner_table_scores", "Owner Table Scores",
   f"SELECT t.table_name, d.domain_name, s.schema_name, ts.overall_score, ts.completeness_score, ts.accuracy_score, ts.validity_score, ts.consistency_score, ts.uniqueness_score, ts.timeliness_score, ts.rules_failed, ts.egregious_count, ts.effective_threshold_min, CASE WHEN ts.overall_score < ts.effective_threshold_min THEN 'Below Threshold' WHEN ts.overall_score < ts.effective_threshold_warning THEN 'Warning' ELSE 'Healthy' END AS status_band, bo.owner_name FROM {CS}.v_table_scores_latest ts JOIN {CS}.dq_table t ON ts.table_id = t.table_id JOIN {CS}.schema s ON ts.schema_id = s.schema_id JOIN {CS}.data_domain d ON ts.domain_id = d.domain_id JOIN {CS}.business_owner bo ON ts.owner_id = bo.owner_id ORDER BY ts.overall_score ASC")

ds("owner_scores", "Owner Scores",
   f"SELECT bo.owner_name, ROUND(AVG(ts.overall_score),2) AS overall_score, ROUND(AVG(ts.completeness_score),2) AS completeness_score, ROUND(AVG(ts.accuracy_score),2) AS accuracy_score, ROUND(AVG(ts.validity_score),2) AS validity_score, ROUND(AVG(ts.consistency_score),2) AS consistency_score, ROUND(AVG(ts.uniqueness_score),2) AS uniqueness_score, ROUND(AVG(ts.timeliness_score),2) AS timeliness_score, COUNT(*) AS table_count, SUM(ts.rules_failed) AS total_failures FROM {CS}.v_table_scores_latest ts JOIN {CS}.business_owner bo ON ts.owner_id = bo.owner_id GROUP BY bo.owner_name ORDER BY overall_score ASC")

ds("owner_below_threshold", "Owner Assets Below Threshold",
   f"SELECT bo.owner_name, t.table_name, d.domain_name, ts.overall_score, ts.rules_failed FROM {CS}.v_table_scores_latest ts JOIN {CS}.dq_table t ON ts.table_id = t.table_id JOIN {CS}.data_domain d ON ts.domain_id = d.domain_id JOIN {CS}.business_owner bo ON ts.owner_id = bo.owner_id WHERE ts.overall_score < ts.effective_threshold_min ORDER BY ts.overall_score ASC")

ds("owner_top_failing_cols", "Top Failing Columns by Owner",
   f"SELECT bo.owner_name, dc.column_name, t.table_name, qd.dimension_name, crr.weighted_score, crr.failure_rate, crr.passed FROM {CS}.column_rule_result crr JOIN {CS}.column_rule_assignment cra ON crr.assignment_id = cra.assignment_id JOIN {CS}.dq_column dc ON cra.column_id = dc.column_id JOIN {CS}.dq_table t ON dc.table_id = t.table_id JOIN {CS}.dq_rule dr ON cra.rule_id = dr.rule_id JOIN {CS}.quality_dimension qd ON dr.dimension_id = qd.dimension_id JOIN {CS}.business_owner bo ON t.owner_id = bo.owner_id JOIN {CS}.v_latest_run lr ON crr.run_id = lr.latest_run_id WHERE NOT crr.passed ORDER BY crr.weighted_score ASC LIMIT 50")

# Tab 4 - Catalog / Database
ds("catalog_scores", "Catalog Scores",
   f"SELECT cs.catalog_name, ROUND(AVG(ts.overall_score),2) AS overall_score, ROUND(AVG(ts.completeness_score),2) AS completeness_score, ROUND(AVG(ts.accuracy_score),2) AS accuracy_score, ROUND(AVG(ts.validity_score),2) AS validity_score, ROUND(AVG(ts.consistency_score),2) AS consistency_score, ROUND(AVG(ts.uniqueness_score),2) AS uniqueness_score, ROUND(AVG(ts.timeliness_score),2) AS timeliness_score, SUM(CASE WHEN ts.overall_score < ts.effective_threshold_min THEN 1 ELSE 0 END) AS tables_below_threshold FROM {CS}.v_table_scores_latest ts JOIN {CS}.catalog cs ON ts.catalog_id = cs.catalog_id GROUP BY cs.catalog_name ORDER BY overall_score ASC")

ds("schema_scores", "Schema Scores",
   f"SELECT cs.catalog_name, s.schema_name, ROUND(AVG(ts.overall_score),2) AS overall_score, ROUND(AVG(ts.completeness_score),2) AS completeness_score, ROUND(AVG(ts.accuracy_score),2) AS accuracy_score, ROUND(AVG(ts.validity_score),2) AS validity_score, ROUND(AVG(ts.consistency_score),2) AS consistency_score, ROUND(AVG(ts.uniqueness_score),2) AS uniqueness_score, ROUND(AVG(ts.timeliness_score),2) AS timeliness_score FROM {CS}.v_table_scores_latest ts JOIN {CS}.catalog cs ON ts.catalog_id = cs.catalog_id JOIN {CS}.schema s ON ts.schema_id = s.schema_id GROUP BY cs.catalog_name, s.schema_name ORDER BY overall_score ASC")

ds("catalog_table_grid", "Table Score Grid",
   f"SELECT t.table_name, s.schema_name, cs.catalog_name, t.is_critical, ts.overall_score, ts.completeness_score, ts.accuracy_score, ts.validity_score, ts.consistency_score, ts.uniqueness_score, ts.timeliness_score, ts.rules_failed FROM {CS}.v_table_scores_latest ts JOIN {CS}.dq_table t ON ts.table_id = t.table_id JOIN {CS}.schema s ON ts.schema_id = s.schema_id JOIN {CS}.catalog cs ON ts.catalog_id = cs.catalog_id ORDER BY ts.overall_score ASC")

ds("catalog_dim_failures", "Catalog Dimension Failures",
   f"SELECT ts.schema_name, ts.table_name, qd.dimension_name, COUNT(*) AS total_rules, SUM(CASE WHEN NOT crr.passed THEN 1 ELSE 0 END) AS failed_rules, ROUND(AVG(crr.weighted_score),2) AS avg_score, SUM(CASE WHEN crr.egregious_flag THEN 1 ELSE 0 END) AS egregious_count FROM {CS}.column_rule_result crr JOIN {CS}.column_rule_assignment cra ON crr.assignment_id = cra.assignment_id JOIN {CS}.dq_column dc ON cra.column_id = dc.column_id JOIN {CS}.dq_rule dr ON cra.rule_id = dr.rule_id JOIN {CS}.quality_dimension qd ON dr.dimension_id = qd.dimension_id JOIN {CS}.v_table_scores_latest ts ON dc.table_id = ts.table_id JOIN {CS}.v_latest_run lr ON crr.run_id = lr.latest_run_id GROUP BY ts.schema_name, ts.table_name, qd.dimension_name")

ds("completeness_failures", "Completeness Failures",
   f"SELECT dc.column_name, t.table_name, crr.failure_rate, crr.failed_records, crr.total_records, crr.weighted_score FROM {CS}.column_rule_result crr JOIN {CS}.column_rule_assignment cra ON crr.assignment_id = cra.assignment_id JOIN {CS}.dq_column dc ON cra.column_id = dc.column_id JOIN {CS}.dq_table t ON dc.table_id = t.table_id JOIN {CS}.dq_rule dr ON cra.rule_id = dr.rule_id JOIN {CS}.quality_dimension qd ON dr.dimension_id = qd.dimension_id JOIN {CS}.v_latest_run lr ON crr.run_id = lr.latest_run_id WHERE qd.dimension_name = 'Completeness' AND NOT crr.passed ORDER BY crr.weighted_score ASC")

ds("validity_failures", "Validity Failures",
   f"SELECT dc.column_name, t.table_name, dr.rule_type, crr.failed_records, crr.failure_rate, crr.executed_rule_sql, crr.weighted_score FROM {CS}.column_rule_result crr JOIN {CS}.column_rule_assignment cra ON crr.assignment_id = cra.assignment_id JOIN {CS}.dq_column dc ON cra.column_id = dc.column_id JOIN {CS}.dq_table t ON dc.table_id = t.table_id JOIN {CS}.dq_rule dr ON cra.rule_id = dr.rule_id JOIN {CS}.quality_dimension qd ON dr.dimension_id = qd.dimension_id JOIN {CS}.v_latest_run lr ON crr.run_id = lr.latest_run_id WHERE qd.dimension_name = 'Validity' AND NOT crr.passed ORDER BY crr.weighted_score ASC")

ds("uniqueness_failures_keys", "Uniqueness Failures on Key Columns",
   f"SELECT dc.column_name, t.table_name, crr.failure_rate, crr.weighted_score, dc.is_primary_key, dc.is_foreign_key FROM {CS}.column_rule_result crr JOIN {CS}.column_rule_assignment cra ON crr.assignment_id = cra.assignment_id JOIN {CS}.dq_column dc ON cra.column_id = dc.column_id JOIN {CS}.dq_table t ON dc.table_id = t.table_id JOIN {CS}.dq_rule dr ON cra.rule_id = dr.rule_id JOIN {CS}.quality_dimension qd ON dr.dimension_id = qd.dimension_id JOIN {CS}.v_latest_run lr ON crr.run_id = lr.latest_run_id WHERE qd.dimension_name = 'Uniqueness' AND NOT crr.passed AND (dc.is_primary_key = true OR dc.is_foreign_key = true) ORDER BY crr.weighted_score ASC")

# Tab 5 - Table Detail
ds("table_col_rule_detail", "Table Column Rule Detail",
   f"SELECT dc.column_name, dc.data_type, dc.is_primary_key, dc.is_foreign_key, qd.dimension_name, dr.rule_name, dr.rule_type, cra.threshold_value, crr.passed, crr.total_records, crr.failed_records, crr.failure_rate, crr.raw_score, crr.weighted_score, crr.egregious_flag, crr.executed_rule_sql, crr.result_detail, crr.evaluated_at, t.table_name FROM {CS}.column_rule_result crr JOIN {CS}.column_rule_assignment cra ON crr.assignment_id = cra.assignment_id JOIN {CS}.dq_column dc ON cra.column_id = dc.column_id JOIN {CS}.dq_table t ON dc.table_id = t.table_id JOIN {CS}.dq_rule dr ON cra.rule_id = dr.rule_id JOIN {CS}.quality_dimension qd ON dr.dimension_id = qd.dimension_id JOIN {CS}.v_latest_run lr ON crr.run_id = lr.latest_run_id ORDER BY crr.weighted_score ASC")

ds("table_trend", "Table Score Trend",
   f"SELECT t.table_name, r.run_timestamp, ROUND(AVG(crr.weighted_score),2) AS overall_score, ROUND(AVG(CASE WHEN qd.dimension_name='Completeness' THEN crr.weighted_score END),2) AS completeness_score, ROUND(AVG(CASE WHEN qd.dimension_name='Accuracy' THEN crr.weighted_score END),2) AS accuracy_score, ROUND(AVG(CASE WHEN qd.dimension_name='Validity' THEN crr.weighted_score END),2) AS validity_score, ROUND(AVG(CASE WHEN qd.dimension_name='Consistency' THEN crr.weighted_score END),2) AS consistency_score, ROUND(AVG(CASE WHEN qd.dimension_name='Uniqueness' THEN crr.weighted_score END),2) AS uniqueness_score, ROUND(AVG(CASE WHEN qd.dimension_name='Timeliness' THEN crr.weighted_score END),2) AS timeliness_score FROM {CS}.column_rule_result crr JOIN {CS}.column_rule_assignment cra ON crr.assignment_id = cra.assignment_id JOIN {CS}.dq_column dc ON cra.column_id = dc.column_id JOIN {CS}.dq_table t ON dc.table_id = t.table_id JOIN {CS}.dq_rule dr ON cra.rule_id = dr.rule_id JOIN {CS}.quality_dimension qd ON dr.dimension_id = qd.dimension_id JOIN {CS}.dq_run r ON crr.run_id = r.run_id GROUP BY t.table_name, r.run_timestamp ORDER BY r.run_timestamp")

ds("table_scores_all", "All Table Scores",
   f"SELECT t.table_name, ts.overall_score, ts.completeness_score, ts.accuracy_score, ts.validity_score, ts.consistency_score, ts.uniqueness_score, ts.timeliness_score, t.is_critical, ts.effective_threshold_min, ts.effective_threshold_warning FROM {CS}.v_table_scores_latest ts JOIN {CS}.dq_table t ON ts.table_id = t.table_id ORDER BY ts.overall_score ASC")

# Tab 6 - Rule Library
ds("rule_library", "Rule Library",
   f"SELECT dr.rule_id, dr.rule_name, dr.rule_description, dr.rule_type, dr.rule_status, dr.version, dr.is_system_rule, dr.default_severity_weight, dr.created_by, dr.rule_sql, qd.dimension_name, rc.category_name, COUNT(cra.assignment_id) AS times_deployed, ROUND(AVG(CASE WHEN crr.run_id IS NOT NULL THEN crr.failure_rate END),4) AS avg_failure_rate FROM {CS}.dq_rule dr JOIN {CS}.quality_dimension qd ON dr.dimension_id = qd.dimension_id JOIN {CS}.dq_rule_category rc ON dr.category_id = rc.category_id LEFT JOIN {CS}.column_rule_assignment cra ON dr.rule_id = cra.rule_id LEFT JOIN {CS}.column_rule_result crr ON cra.assignment_id = crr.assignment_id GROUP BY dr.rule_id, dr.rule_name, dr.rule_description, dr.rule_type, dr.rule_status, dr.version, dr.is_system_rule, dr.default_severity_weight, dr.created_by, dr.rule_sql, qd.dimension_name, rc.category_name ORDER BY times_deployed DESC")

ds("rules_by_category", "Rules by Category",
   f"SELECT rc.category_name, qd.dimension_name, COUNT(*) AS rule_count FROM {CS}.dq_rule dr JOIN {CS}.quality_dimension qd ON dr.dimension_id = qd.dimension_id JOIN {CS}.dq_rule_category rc ON dr.category_id = rc.category_id GROUP BY rc.category_name, qd.dimension_name ORDER BY rule_count DESC")

ds("rules_by_dimension", "Rules by Dimension",
   f"SELECT qd.dimension_name, COUNT(*) AS rule_count FROM {CS}.dq_rule dr JOIN {CS}.quality_dimension qd ON dr.dimension_id = qd.dimension_id GROUP BY qd.dimension_name")

ds("rules_highest_failure", "Rules with Highest Failure Rates",
   f"SELECT dr.rule_name, ROUND(AVG(crr.failure_rate),4) AS avg_failure_rate, COUNT(*) AS assignments FROM {CS}.column_rule_result crr JOIN {CS}.column_rule_assignment cra ON crr.assignment_id = cra.assignment_id JOIN {CS}.dq_rule dr ON cra.rule_id = dr.rule_id JOIN {CS}.v_latest_run lr ON crr.run_id = lr.latest_run_id GROUP BY dr.rule_name ORDER BY avg_failure_rate DESC LIMIT 10")

ds("unmonitored_tables", "Unmonitored Tables",
   f"SELECT t.table_name, s.schema_name, cs.catalog_name, bo.owner_name FROM {CS}.dq_table t JOIN {CS}.schema s ON t.schema_id = s.schema_id JOIN {CS}.catalog cs ON s.catalog_id = cs.catalog_id LEFT JOIN {CS}.business_owner bo ON t.owner_id = bo.owner_id LEFT JOIN {CS}.dq_column dc ON t.table_id = dc.table_id LEFT JOIN {CS}.column_rule_assignment cra ON dc.column_id = cra.column_id WHERE cra.assignment_id IS NULL")


# ─── HELPER FUNCTIONS FOR WIDGETS ──────────────────────────────────────

def counter_widget(dataset_name, field_expr, field_name, title, pos):
    wid = uid()
    return {
        "widget": {
            "name": wid,
            "queries": [{"name": "main_query", "query": {
                "datasetName": dataset_name,
                "fields": [{"name": field_name, "expression": field_expr}],
                "disaggregated": True
            }}],
            "spec": {
                "version": 2, "widgetType": "counter",
                "encodings": {"value": {"fieldName": field_name, "displayName": title}},
                "frame": {"showTitle": True, "title": title}
            }
        },
        "position": pos
    }

def bar_widget(dataset_name, fields, x_field, y_field, title, pos, color_field=None, horizontal=False, sort_desc=False, colors=None):
    wid = uid()
    x_scale = {"type": "categorical"}
    if sort_desc:
        x_scale["sort"] = {"by": "y-reversed"}
    enc = {
        "x": {"fieldName": x_field, "scale": x_scale, "displayName": x_field},
        "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_field},
        "label": {"show": True}
    }
    if color_field:
        enc["color"] = {"fieldName": color_field, "scale": {"type": "categorical"}, "displayName": color_field}
    spec = {
        "version": 3, "widgetType": "bar", "encodings": enc,
        "frame": {"showTitle": True, "title": title}
    }
    if colors:
        spec["mark"] = {"colors": colors}
    return {
        "widget": {
            "name": wid,
            "queries": [{"name": "main_query", "query": {
                "datasetName": dataset_name, "fields": fields, "disaggregated": False
            }}],
            "spec": spec
        },
        "position": pos
    }

def line_widget(dataset_name, fields, x_field, y_field, title, pos, color_field=None, temporal=True):
    wid = uid()
    enc = {
        "x": {"fieldName": x_field, "scale": {"type": "temporal" if temporal else "categorical"}, "displayName": x_field},
        "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_field}
    }
    if color_field:
        enc["color"] = {"fieldName": color_field, "scale": {"type": "categorical"}, "displayName": color_field}
    return {
        "widget": {
            "name": uid(),
            "queries": [{"name": "main_query", "query": {
                "datasetName": dataset_name, "fields": fields, "disaggregated": False
            }}],
            "spec": {
                "version": 3, "widgetType": "line", "encodings": enc,
                "frame": {"showTitle": True, "title": title}
            }
        },
        "position": pos
    }

def table_widget(dataset_name, columns, title, pos):
    """columns: list of (field, display_title, type)"""
    wid = uid()
    fields = [{"name": c[0], "expression": f"`{c[0]}`"} for c in columns]
    col_enc = []
    for i, (fname, dtitle, dtype) in enumerate(columns):
        da = "number" if dtype in ("float","integer") else ("datetime" if dtype == "datetime" else "string")
        align = "right" if dtype in ("float","integer") else "left"
        e = {"fieldName": fname, "type": dtype, "displayAs": da, "title": dtitle, "displayName": dtitle, "order": 100000+i, "alignContent": align}
        if dtype == "float":
            e["numberFormat"] = "0.00"
        col_enc.append(e)
    return {
        "widget": {
            "name": wid,
            "queries": [{"name": "main_query", "query": {
                "datasetName": dataset_name, "fields": fields, "disaggregated": True
            }}],
            "spec": {
                "version": 1, "widgetType": "table",
                "encodings": {"columns": col_enc},
                "frame": {"showTitle": True, "title": title}
            }
        },
        "position": pos
    }

def pie_widget(dataset_name, fields, angle_field, color_field, title, pos):
    return {
        "widget": {
            "name": uid(),
            "queries": [{"name": "main_query", "query": {
                "datasetName": dataset_name, "fields": fields, "disaggregated": False
            }}],
            "spec": {
                "version": 3, "widgetType": "pie",
                "encodings": {
                    "angle": {"fieldName": angle_field, "scale": {"type": "quantitative"}, "displayName": angle_field},
                    "color": {"fieldName": color_field, "scale": {"type": "categorical"}, "displayName": color_field}
                },
                "frame": {"showTitle": True, "title": title}
            }
        },
        "position": pos
    }

def filter_dropdown(dataset_name, field, title, pos, multi=False):
    wid = uid()
    qname = f"filter_{wid}_{field}"
    return {
        "widget": {
            "name": wid,
            "queries": [{"name": qname, "query": {
                "datasetName": dataset_name,
                "fields": [
                    {"name": field, "expression": f"`{field}`"},
                    {"name": f"{field}_assoc", "expression": "COUNT_IF(`associative_filter_predicate_group`)"}
                ],
                "disaggregated": False
            }}],
            "spec": {
                "version": 2,
                "widgetType": "filter-multi-select" if multi else "filter-single-select",
                "encodings": {"fields": [{"fieldName": field, "displayName": field, "queryName": qname}]},
                "frame": {"showTitle": True, "title": title}
            }
        },
        "position": pos
    }

def f(name, expr):
    return {"name": name, "expression": expr}


# ═══════════════════════════════════════════════════════════════════════
# PAGES
# ═══════════════════════════════════════════════════════════════════════

pages = []

# ─── TAB 1: ORGANIZATION OVERVIEW ─────────────────────────────────────
tab1 = {"name": uid(), "displayName": "Organization Overview", "pageType": "PAGE_TYPE_CANVAS", "layout": []}
y = 0
# KPI Row
tab1["layout"].append(counter_widget("org_kpi_latest", "`overall_score`", "overall_score", "Enterprise DQ Score", {"x":0,"y":y,"width":1,"height":2}))
tab1["layout"].append(counter_widget("tables_below_thresh", "`cnt`", "cnt", "Tables Below Threshold", {"x":1,"y":y,"width":1,"height":2}))
tab1["layout"].append(counter_widget("pct_monitored", "`pct_assets_monitored`", "pct_assets_monitored", "% Assets Monitored", {"x":2,"y":y,"width":1,"height":2}))
tab1["layout"].append(counter_widget("org_kpi_latest", "`total_tables`", "total_tables", "Total Tables Monitored", {"x":3,"y":y,"width":1,"height":2}))
tab1["layout"].append(counter_widget("org_kpi_latest", "`total_domains`", "total_domains", "Total Domains", {"x":4,"y":y,"width":1,"height":2}))

y = 2
# Score by Dimension - horizontal bar
tab1["layout"].append(bar_widget("org_kpi_latest",
    [f("dimension", "'Completeness'"), f("score", "`completeness_score`")],
    "dimension", "score", "Score by Dimension", {"x":0,"y":y,"width":3,"height":4},
    colors=["#00A972","#FFAB00","#FF3621","#8BCAE7","#AB4057","#99DDB4"]))
# Actually need unpivot for dimension bar. Use a table approach with UNION ALL in the dataset.
# Let me add a proper unpivot dataset
ds("org_dim_scores", "Org Dimension Scores",
   f"SELECT 'Completeness' AS dimension, completeness_score AS score FROM {CS}.v_org_score_latest UNION ALL SELECT 'Accuracy', accuracy_score FROM {CS}.v_org_score_latest UNION ALL SELECT 'Validity', validity_score FROM {CS}.v_org_score_latest UNION ALL SELECT 'Consistency', consistency_score FROM {CS}.v_org_score_latest UNION ALL SELECT 'Uniqueness', uniqueness_score FROM {CS}.v_org_score_latest UNION ALL SELECT 'Timeliness', timeliness_score FROM {CS}.v_org_score_latest")

# Replace the dimension bar with proper dataset
tab1["layout"][-1] = bar_widget("org_dim_scores",
    [f("dimension","`dimension`"), f("score","`score`")],
    "dimension", "score", "Score by Dimension", {"x":0,"y":y,"width":3,"height":4})

# Domain Score Heatmap (table with scores)
tab1["layout"].append(table_widget("domain_scores_latest", [
    ("domain_name","Domain","string"),
    ("overall_score","Overall","float"),
    ("completeness_score","Complete","float"),
    ("accuracy_score","Accuracy","float"),
    ("validity_score","Validity","float"),
    ("consistency_score","Consist","float"),
    ("uniqueness_score","Unique","float"),
    ("timeliness_score","Timely","float"),
    ("tables_below_threshold","Below Thresh","integer"),
], "Domain Score Heatmap", {"x":3,"y":y,"width":3,"height":4}))

y = 6
# 30 day trend - unpivot for multi-series line
ds("org_trend_unpivot", "Org Trend Unpivoted",
   f"SELECT run_timestamp, 'Overall' AS dimension, overall_score AS score FROM ({datasets[2]['queryLines'][0]}) UNION ALL SELECT run_timestamp, 'Completeness', completeness_score FROM ({datasets[2]['queryLines'][0]}) UNION ALL SELECT run_timestamp, 'Accuracy', accuracy_score FROM ({datasets[2]['queryLines'][0]}) UNION ALL SELECT run_timestamp, 'Validity', validity_score FROM ({datasets[2]['queryLines'][0]}) UNION ALL SELECT run_timestamp, 'Consistency', consistency_score FROM ({datasets[2]['queryLines'][0]}) UNION ALL SELECT run_timestamp, 'Uniqueness', uniqueness_score FROM ({datasets[2]['queryLines'][0]}) UNION ALL SELECT run_timestamp, 'Timeliness', timeliness_score FROM ({datasets[2]['queryLines'][0]})")

tab1["layout"].append(line_widget("org_trend_unpivot",
    [f("run_timestamp","`run_timestamp`"), f("score","`score`"), f("dimension","`dimension`")],
    "run_timestamp", "score", "Enterprise 30-Day Trend", {"x":0,"y":y,"width":6,"height":4},
    color_field="dimension"))

y = 10
# Top 10 Egregious Tables
tab1["layout"].append(table_widget("top10_egregious", [
    ("table_name","Table","string"),
    ("domain_name","Domain","string"),
    ("catalog_name","Catalog","string"),
    ("overall_score","Score","float"),
    ("egregious_count","Egregious","integer"),
], "Top 10 Egregious Tables", {"x":0,"y":y,"width":6,"height":4}))

pages.append(tab1)

# ─── TAB 2: DATA DOMAIN ───────────────────────────────────────────────
tab2 = {"name": uid(), "displayName": "Data Domain", "pageType": "PAGE_TYPE_CANVAS", "layout": []}
y = 0
# Filter
tab2["layout"].append(filter_dropdown("domain_detail", "domain_name", "Domain", {"x":0,"y":y,"width":2,"height":2}))

y = 2
# Domain score by catalog
tab2["layout"].append(bar_widget("domain_detail",
    [f("catalog_name","`catalog_name`"), f("overall_score","`overall_score`")],
    "catalog_name", "overall_score", "Catalog Score Comparison", {"x":0,"y":y,"width":3,"height":4}))

# Rule pass/fail donut
tab2["layout"].append(pie_widget("domain_rule_passfail",
    [f("status","CASE WHEN `passed_count` > 0 THEN 'Passed' ELSE 'Failed' END"), f("count","`passed_count`")],
    "count", "status", "Rule Pass/Fail Summary",
    {"x":3,"y":y,"width":3,"height":4}))
# Actually the pie needs proper passed vs failed. Let me use a simpler approach with unpivot
ds("domain_passfail_unpivot", "Domain Pass/Fail Unpivoted",
   f"SELECT d.domain_name, 'Passed' AS status, SUM(CASE WHEN crr.passed THEN 1 ELSE 0 END) AS rule_count FROM {CS}.column_rule_result crr JOIN {CS}.column_rule_assignment cra ON crr.assignment_id = cra.assignment_id JOIN {CS}.dq_column dc ON cra.column_id = dc.column_id JOIN {CS}.dq_table t ON dc.table_id = t.table_id JOIN {CS}.data_domain d ON t.domain_id = d.domain_id JOIN {CS}.v_latest_run lr ON crr.run_id = lr.latest_run_id GROUP BY d.domain_name UNION ALL SELECT d.domain_name, 'Failed' AS status, SUM(CASE WHEN NOT crr.passed THEN 1 ELSE 0 END) AS rule_count FROM {CS}.column_rule_result crr JOIN {CS}.column_rule_assignment cra ON crr.assignment_id = cra.assignment_id JOIN {CS}.dq_column dc ON cra.column_id = dc.column_id JOIN {CS}.dq_table t ON dc.table_id = t.table_id JOIN {CS}.data_domain d ON t.domain_id = d.domain_id JOIN {CS}.v_latest_run lr ON crr.run_id = lr.latest_run_id GROUP BY d.domain_name")

tab2["layout"][-1] = pie_widget("domain_passfail_unpivot",
    [f("rule_count","SUM(`rule_count`)"), f("status","`status`")],
    "rule_count", "status", "Rule Pass/Fail Summary",
    {"x":3,"y":y,"width":3,"height":4})

y = 6
# Domain trend
ds("domain_trend_unpivot", "Domain Trend Unpivoted",
   f"SELECT d.domain_name, dst.run_timestamp, 'Overall' AS dimension, dst.overall_score AS score FROM {CS}.v_domain_score_trend dst JOIN {CS}.data_domain d ON dst.domain_id = d.domain_id UNION ALL SELECT d.domain_name, dst.run_timestamp, 'Completeness', dst.completeness_score FROM {CS}.v_domain_score_trend dst JOIN {CS}.data_domain d ON dst.domain_id = d.domain_id UNION ALL SELECT d.domain_name, dst.run_timestamp, 'Accuracy', dst.accuracy_score FROM {CS}.v_domain_score_trend dst JOIN {CS}.data_domain d ON dst.domain_id = d.domain_id UNION ALL SELECT d.domain_name, dst.run_timestamp, 'Validity', dst.validity_score FROM {CS}.v_domain_score_trend dst JOIN {CS}.data_domain d ON dst.domain_id = d.domain_id UNION ALL SELECT d.domain_name, dst.run_timestamp, 'Consistency', dst.consistency_score FROM {CS}.v_domain_score_trend dst JOIN {CS}.data_domain d ON dst.domain_id = d.domain_id UNION ALL SELECT d.domain_name, dst.run_timestamp, 'Uniqueness', dst.uniqueness_score FROM {CS}.v_domain_score_trend dst JOIN {CS}.data_domain d ON dst.domain_id = d.domain_id UNION ALL SELECT d.domain_name, dst.run_timestamp, 'Timeliness', dst.timeliness_score FROM {CS}.v_domain_score_trend dst JOIN {CS}.data_domain d ON dst.domain_id = d.domain_id")

tab2["layout"].append(line_widget("domain_trend_unpivot",
    [f("run_timestamp","`run_timestamp`"), f("score","`score`"), f("dimension","`dimension`")],
    "run_timestamp", "score", "Domain 30-Day Trend by Dimension", {"x":0,"y":y,"width":6,"height":4},
    color_field="dimension"))

y = 10
# Critical tables at risk
tab2["layout"].append(table_widget("domain_critical_tables", [
    ("table_name","Table","string"),
    ("schema_name","Schema","string"),
    ("domain_name","Domain","string"),
    ("overall_score","Score","float"),
    ("rules_failed","Rules Failed","integer"),
], "Critical Tables at Risk", {"x":0,"y":y,"width":6,"height":4}))

y = 14
# Schema dimension heatmap
tab2["layout"].append(table_widget("schema_scores", [
    ("catalog_name","Catalog","string"),
    ("schema_name","Schema","string"),
    ("overall_score","Overall","float"),
    ("completeness_score","Complete","float"),
    ("accuracy_score","Accuracy","float"),
    ("validity_score","Validity","float"),
    ("consistency_score","Consist","float"),
    ("uniqueness_score","Unique","float"),
    ("timeliness_score","Timely","float"),
], "Failure Rate by Dimension (Schema Heatmap)", {"x":0,"y":y,"width":6,"height":4}))

pages.append(tab2)

# ─── TAB 3: BUSINESS OWNER ────────────────────────────────────────────
tab3 = {"name": uid(), "displayName": "Business Owner", "pageType": "PAGE_TYPE_CANVAS", "layout": []}
y = 0
tab3["layout"].append(filter_dropdown("owner_scores", "owner_name", "Business Owner", {"x":0,"y":y,"width":2,"height":2}))

y = 2
# Owner dimension scores bar
ds("owner_dim_scores", "Owner Dimension Scores",
   f"SELECT bo.owner_name, 'Completeness' AS dimension, ROUND(AVG(ts.completeness_score),2) AS score FROM {CS}.v_table_scores_latest ts JOIN {CS}.business_owner bo ON ts.owner_id = bo.owner_id GROUP BY bo.owner_name UNION ALL SELECT bo.owner_name, 'Accuracy', ROUND(AVG(ts.accuracy_score),2) FROM {CS}.v_table_scores_latest ts JOIN {CS}.business_owner bo ON ts.owner_id = bo.owner_id GROUP BY bo.owner_name UNION ALL SELECT bo.owner_name, 'Validity', ROUND(AVG(ts.validity_score),2) FROM {CS}.v_table_scores_latest ts JOIN {CS}.business_owner bo ON ts.owner_id = bo.owner_id GROUP BY bo.owner_name UNION ALL SELECT bo.owner_name, 'Consistency', ROUND(AVG(ts.consistency_score),2) FROM {CS}.v_table_scores_latest ts JOIN {CS}.business_owner bo ON ts.owner_id = bo.owner_id GROUP BY bo.owner_name UNION ALL SELECT bo.owner_name, 'Uniqueness', ROUND(AVG(ts.uniqueness_score),2) FROM {CS}.v_table_scores_latest ts JOIN {CS}.business_owner bo ON ts.owner_id = bo.owner_id GROUP BY bo.owner_name UNION ALL SELECT bo.owner_name, 'Timeliness', ROUND(AVG(ts.timeliness_score),2) FROM {CS}.v_table_scores_latest ts JOIN {CS}.business_owner bo ON ts.owner_id = bo.owner_id GROUP BY bo.owner_name")

tab3["layout"].append(bar_widget("owner_dim_scores",
    [f("dimension","`dimension`"), f("score","`score`")],
    "dimension", "score", "My Score by Dimension", {"x":0,"y":y,"width":3,"height":4}))

# Owner overall score table
tab3["layout"].append(table_widget("owner_scores", [
    ("owner_name","Owner","string"),
    ("overall_score","Overall","float"),
    ("table_count","Tables","integer"),
    ("total_failures","Failures","integer"),
], "Owner Score Summary", {"x":3,"y":y,"width":3,"height":4}))

y = 6
# Tables I Own
tab3["layout"].append(table_widget("owner_table_scores", [
    ("table_name","Table","string"),
    ("domain_name","Domain","string"),
    ("schema_name","Schema","string"),
    ("overall_score","Score","float"),
    ("rules_failed","Failed","integer"),
    ("egregious_count","Egregious","integer"),
    ("status_band","Status","string"),
    ("owner_name","Owner","string"),
], "Tables I Own - Score Summary", {"x":0,"y":y,"width":6,"height":5}))

y = 11
# Assets below threshold
tab3["layout"].append(table_widget("owner_below_threshold", [
    ("owner_name","Owner","string"),
    ("table_name","Table","string"),
    ("domain_name","Domain","string"),
    ("overall_score","Score","float"),
    ("rules_failed","Failures","integer"),
], "Assets Below Threshold", {"x":0,"y":y,"width":3,"height":4}))

# Top failing columns
tab3["layout"].append(table_widget("owner_top_failing_cols", [
    ("owner_name","Owner","string"),
    ("column_name","Column","string"),
    ("table_name","Table","string"),
    ("dimension_name","Dimension","string"),
    ("weighted_score","Score","float"),
    ("failure_rate","Failure Rate","float"),
], "Top Failing Columns I Own", {"x":3,"y":y,"width":3,"height":4}))

pages.append(tab3)

# ─── TAB 4: CATALOG / DATABASE ────────────────────────────────────────
tab4 = {"name": uid(), "displayName": "Catalog / Database", "pageType": "PAGE_TYPE_CANVAS", "layout": []}
y = 0
tab4["layout"].append(filter_dropdown("catalog_scores", "catalog_name", "Catalog", {"x":0,"y":y,"width":2,"height":2}))

y = 2
# Catalog dimension bar
ds("catalog_dim_unpivot", "Catalog Dimension Scores",
   f"SELECT cs.catalog_name, 'Completeness' AS dimension, ROUND(AVG(ts.completeness_score),2) AS score FROM {CS}.v_table_scores_latest ts JOIN {CS}.catalog cs ON ts.catalog_id = cs.catalog_id GROUP BY cs.catalog_name UNION ALL SELECT cs.catalog_name, 'Accuracy', ROUND(AVG(ts.accuracy_score),2) FROM {CS}.v_table_scores_latest ts JOIN {CS}.catalog cs ON ts.catalog_id = cs.catalog_id GROUP BY cs.catalog_name UNION ALL SELECT cs.catalog_name, 'Validity', ROUND(AVG(ts.validity_score),2) FROM {CS}.v_table_scores_latest ts JOIN {CS}.catalog cs ON ts.catalog_id = cs.catalog_id GROUP BY cs.catalog_name UNION ALL SELECT cs.catalog_name, 'Consistency', ROUND(AVG(ts.consistency_score),2) FROM {CS}.v_table_scores_latest ts JOIN {CS}.catalog cs ON ts.catalog_id = cs.catalog_id GROUP BY cs.catalog_name UNION ALL SELECT cs.catalog_name, 'Uniqueness', ROUND(AVG(ts.uniqueness_score),2) FROM {CS}.v_table_scores_latest ts JOIN {CS}.catalog cs ON ts.catalog_id = cs.catalog_id GROUP BY cs.catalog_name UNION ALL SELECT cs.catalog_name, 'Timeliness', ROUND(AVG(ts.timeliness_score),2) FROM {CS}.v_table_scores_latest ts JOIN {CS}.catalog cs ON ts.catalog_id = cs.catalog_id GROUP BY cs.catalog_name")

tab4["layout"].append(bar_widget("catalog_dim_unpivot",
    [f("dimension","`dimension`"), f("score","`score`")],
    "dimension", "score", "Score by Dimension - Catalog", {"x":0,"y":y,"width":3,"height":4}))

# Schema score comparison bar
tab4["layout"].append(bar_widget("schema_scores",
    [f("schema_name","`schema_name`"), f("overall_score","`overall_score`")],
    "schema_name", "overall_score", "Schema Score Comparison", {"x":3,"y":y,"width":3,"height":4}))

y = 6
# Table Score Grid
tab4["layout"].append(table_widget("catalog_table_grid", [
    ("table_name","Table","string"),
    ("schema_name","Schema","string"),
    ("catalog_name","Catalog","string"),
    ("is_critical","Critical","string"),
    ("overall_score","Overall","float"),
    ("completeness_score","Complete","float"),
    ("accuracy_score","Accuracy","float"),
    ("validity_score","Validity","float"),
    ("consistency_score","Consist","float"),
    ("uniqueness_score","Unique","float"),
    ("timeliness_score","Timely","float"),
    ("rules_failed","Failed","integer"),
], "Table Score Grid", {"x":0,"y":y,"width":6,"height":5}))

y = 11
# Completeness failures
tab4["layout"].append(table_widget("completeness_failures", [
    ("column_name","Column","string"),
    ("table_name","Table","string"),
    ("failure_rate","Failure Rate","float"),
    ("failed_records","Failed Records","integer"),
    ("total_records","Total Records","integer"),
    ("weighted_score","Score","float"),
], "Completeness Failures", {"x":0,"y":y,"width":3,"height":4}))

# Validity failures
tab4["layout"].append(table_widget("validity_failures", [
    ("column_name","Column","string"),
    ("table_name","Table","string"),
    ("rule_type","Rule Type","string"),
    ("failed_records","Failed Records","integer"),
    ("failure_rate","Failure Rate","float"),
    ("weighted_score","Score","float"),
], "Validity Failures", {"x":3,"y":y,"width":3,"height":4}))

y = 15
# Uniqueness failures on key columns
tab4["layout"].append(table_widget("uniqueness_failures_keys", [
    ("column_name","Column","string"),
    ("table_name","Table","string"),
    ("failure_rate","Failure Rate","float"),
    ("weighted_score","Score","float"),
    ("is_primary_key","PK","string"),
    ("is_foreign_key","FK","string"),
], "Uniqueness Failures on Key Columns", {"x":0,"y":y,"width":3,"height":4}))

# Unmonitored tables
tab4["layout"].append(table_widget("unmonitored_tables", [
    ("table_name","Table","string"),
    ("schema_name","Schema","string"),
    ("catalog_name","Catalog","string"),
    ("owner_name","Owner","string"),
], "Unmonitored Tables", {"x":3,"y":y,"width":3,"height":4}))

pages.append(tab4)

# ─── TAB 5: TABLE DETAIL ──────────────────────────────────────────────
tab5 = {"name": uid(), "displayName": "Table Detail", "pageType": "PAGE_TYPE_CANVAS", "layout": []}
y = 0
tab5["layout"].append(filter_dropdown("table_scores_all", "table_name", "Table", {"x":0,"y":y,"width":2,"height":2}))

y = 2
# Table dimension scores bar
ds("table_dim_unpivot", "Table Dimension Scores",
   f"SELECT t.table_name, 'Completeness' AS dimension, ts.completeness_score AS score FROM {CS}.v_table_scores_latest ts JOIN {CS}.dq_table t ON ts.table_id = t.table_id UNION ALL SELECT t.table_name, 'Accuracy', ts.accuracy_score FROM {CS}.v_table_scores_latest ts JOIN {CS}.dq_table t ON ts.table_id = t.table_id UNION ALL SELECT t.table_name, 'Validity', ts.validity_score FROM {CS}.v_table_scores_latest ts JOIN {CS}.dq_table t ON ts.table_id = t.table_id UNION ALL SELECT t.table_name, 'Consistency', ts.consistency_score FROM {CS}.v_table_scores_latest ts JOIN {CS}.dq_table t ON ts.table_id = t.table_id UNION ALL SELECT t.table_name, 'Uniqueness', ts.uniqueness_score FROM {CS}.v_table_scores_latest ts JOIN {CS}.dq_table t ON ts.table_id = t.table_id UNION ALL SELECT t.table_name, 'Timeliness', ts.timeliness_score FROM {CS}.v_table_scores_latest ts JOIN {CS}.dq_table t ON ts.table_id = t.table_id")

tab5["layout"].append(bar_widget("table_dim_unpivot",
    [f("dimension","`dimension`"), f("score","`score`")],
    "dimension", "score", "Score by Dimension - This Table", {"x":0,"y":y,"width":3,"height":4}))

# Table 30-day trend
ds("table_trend_unpivot", "Table Trend Unpivoted",
   f"SELECT t.table_name, r.run_timestamp, 'Overall' AS dimension, ROUND(AVG(crr.weighted_score),2) AS score FROM {CS}.column_rule_result crr JOIN {CS}.column_rule_assignment cra ON crr.assignment_id = cra.assignment_id JOIN {CS}.dq_column dc ON cra.column_id = dc.column_id JOIN {CS}.dq_table t ON dc.table_id = t.table_id JOIN {CS}.dq_rule dr ON cra.rule_id = dr.rule_id JOIN {CS}.quality_dimension qd ON dr.dimension_id = qd.dimension_id JOIN {CS}.dq_run r ON crr.run_id = r.run_id GROUP BY t.table_name, r.run_timestamp UNION ALL SELECT t.table_name, r.run_timestamp, 'Completeness', ROUND(AVG(CASE WHEN qd.dimension_name='Completeness' THEN crr.weighted_score END),2) FROM {CS}.column_rule_result crr JOIN {CS}.column_rule_assignment cra ON crr.assignment_id = cra.assignment_id JOIN {CS}.dq_column dc ON cra.column_id = dc.column_id JOIN {CS}.dq_table t ON dc.table_id = t.table_id JOIN {CS}.dq_rule dr ON cra.rule_id = dr.rule_id JOIN {CS}.quality_dimension qd ON dr.dimension_id = qd.dimension_id JOIN {CS}.dq_run r ON crr.run_id = r.run_id GROUP BY t.table_name, r.run_timestamp UNION ALL SELECT t.table_name, r.run_timestamp, 'Accuracy', ROUND(AVG(CASE WHEN qd.dimension_name='Accuracy' THEN crr.weighted_score END),2) FROM {CS}.column_rule_result crr JOIN {CS}.column_rule_assignment cra ON crr.assignment_id = cra.assignment_id JOIN {CS}.dq_column dc ON cra.column_id = dc.column_id JOIN {CS}.dq_table t ON dc.table_id = t.table_id JOIN {CS}.dq_rule dr ON cra.rule_id = dr.rule_id JOIN {CS}.quality_dimension qd ON dr.dimension_id = qd.dimension_id JOIN {CS}.dq_run r ON crr.run_id = r.run_id GROUP BY t.table_name, r.run_timestamp")

tab5["layout"].append(line_widget("table_trend_unpivot",
    [f("run_timestamp","`run_timestamp`"), f("score","`score`"), f("dimension","`dimension`")],
    "run_timestamp", "score", "Table 30-Day Trend", {"x":3,"y":y,"width":3,"height":4},
    color_field="dimension"))

y = 6
# Column Score Summary table
tab5["layout"].append(table_widget("table_col_rule_detail", [
    ("table_name","Table","string"),
    ("column_name","Column","string"),
    ("data_type","Data Type","string"),
    ("is_primary_key","PK","string"),
    ("is_foreign_key","FK","string"),
    ("dimension_name","Dimension","string"),
    ("rule_name","Rule","string"),
    ("passed","Passed","string"),
    ("weighted_score","Score","float"),
    ("failure_rate","Failure Rate","float"),
    ("egregious_flag","Egregious","string"),
], "Column Score Summary", {"x":0,"y":y,"width":6,"height":5}))

y = 11
# Rule Execution Detail
tab5["layout"].append(table_widget("table_col_rule_detail", [
    ("column_name","Column","string"),
    ("rule_name","Rule","string"),
    ("rule_type","Type","string"),
    ("dimension_name","Dimension","string"),
    ("passed","Passed","string"),
    ("total_records","Total","integer"),
    ("failed_records","Failed","integer"),
    ("failure_rate","Fail Rate","float"),
    ("weighted_score","Score","float"),
    ("egregious_flag","Egregious","string"),
    ("executed_rule_sql","SQL","string"),
], "Rule Execution Detail", {"x":0,"y":y,"width":6,"height":5}))

y = 16
# Worst columns bar (bottom 5)
ds("worst_columns", "Worst Columns",
   f"SELECT dc.column_name, t.table_name, MIN(crr.weighted_score) AS min_score FROM {CS}.column_rule_result crr JOIN {CS}.column_rule_assignment cra ON crr.assignment_id = cra.assignment_id JOIN {CS}.dq_column dc ON cra.column_id = dc.column_id JOIN {CS}.dq_table t ON dc.table_id = t.table_id JOIN {CS}.v_latest_run lr ON crr.run_id = lr.latest_run_id GROUP BY dc.column_name, t.table_name ORDER BY min_score ASC LIMIT 10")

tab5["layout"].append(bar_widget("worst_columns",
    [f("column_name","`column_name`"), f("min_score","`min_score`")],
    "column_name", "min_score", "Worst Columns (Bottom 10)", {"x":0,"y":y,"width":3,"height":4}))

# Record impact summary
tab5["layout"].append(table_widget("table_col_rule_detail", [
    ("column_name","Column","string"),
    ("rule_name","Rule","string"),
    ("failed_records","Failed Records","integer"),
    ("total_records","Total Records","integer"),
    ("failure_rate","Failure Rate","float"),
], "Record Impact Summary", {"x":3,"y":y,"width":3,"height":4}))

pages.append(tab5)

# ─── TAB 6: RULE LIBRARY ──────────────────────────────────────────────
tab6 = {"name": uid(), "displayName": "Rule Library", "pageType": "PAGE_TYPE_CANVAS", "layout": []}
y = 0
# Rule library full table
tab6["layout"].append(table_widget("rule_library", [
    ("rule_name","Rule Name","string"),
    ("category_name","Category","string"),
    ("dimension_name","Dimension","string"),
    ("rule_type","Type","string"),
    ("default_severity_weight","Severity","float"),
    ("rule_status","Status","string"),
    ("version","Version","string"),
    ("created_by","Created By","string"),
    ("times_deployed","Deployed","integer"),
    ("avg_failure_rate","Avg Fail Rate","float"),
], "Rule Library", {"x":0,"y":y,"width":6,"height":6}))

y = 6
# Rules by category bar
tab6["layout"].append(bar_widget("rules_by_category",
    [f("category_name","`category_name`"), f("rule_count","SUM(`rule_count`)")],
    "category_name", "rule_count", "Rules by Category", {"x":0,"y":y,"width":3,"height":4},
    color_field="dimension_name", sort_desc=True))

# Rules by dimension pie
tab6["layout"].append(pie_widget("rules_by_dimension",
    [f("rule_count","`rule_count`"), f("dimension_name","`dimension_name`")],
    "rule_count", "dimension_name", "Rules by Dimension",
    {"x":3,"y":y,"width":3,"height":4}))

y = 10
# Rules with highest failure rates
tab6["layout"].append(bar_widget("rules_highest_failure",
    [f("rule_name","`rule_name`"), f("avg_failure_rate","`avg_failure_rate`")],
    "rule_name", "avg_failure_rate", "Rules with Highest Failure Rates", {"x":0,"y":y,"width":3,"height":4},
    sort_desc=True))

# Unmonitored tables
tab6["layout"].append(table_widget("unmonitored_tables", [
    ("table_name","Table","string"),
    ("schema_name","Schema","string"),
    ("catalog_name","Catalog","string"),
    ("owner_name","Owner","string"),
], "Unmonitored Asset Summary", {"x":3,"y":y,"width":3,"height":4}))

pages.append(tab6)


# ═══════════════════════════════════════════════════════════════════════
# BUILD & DEPLOY
# ═══════════════════════════════════════════════════════════════════════

dashboard_payload = {
    "datasets": datasets,
    "pages": pages,
    "uiSettings": {
        "theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"},
        "applyModeEnabled": False
    }
}

# Write to file
with open("dq_dashboard.json", "w") as fh:
    json.dump(dashboard_payload, fh, indent=2)

print(f"Dashboard JSON written to dq_dashboard.json")
print(f"Datasets: {len(datasets)}")
print(f"Pages: {len(pages)}")
for p in pages:
    print(f"  {p['displayName']}: {len(p['layout'])} widgets")
