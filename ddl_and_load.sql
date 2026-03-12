-- ============================================================
-- MERIDIAN HEALTH SYSTEM — DATA QUALITY PLATFORM
-- Delta Lake DDL + Data Load Scripts
-- Databricks SQL — Unity Catalog
-- ============================================================
-- Catalog: dq_platform
-- All tables use Delta format, liquid clustering for query perf
-- ============================================================

CREATE CATALOG IF NOT EXISTS dq_platform;
USE CATALOG dq_platform;
CREATE DATABASE IF NOT EXISTS dq_core;
USE DATABASE dq_core;

-- ────────────────────────────────────────────────────────────
-- DIMENSION / REFERENCE TABLES
-- ────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE organization (
  organization_id     INT           NOT NULL,
  organization_name   STRING        NOT NULL,
  dq_threshold_min    DOUBLE        COMMENT 'Floor score — below this is unacceptable',
  dq_threshold_warning DOUBLE       COMMENT 'Amber zone lower bound',
  dq_threshold_critical DOUBLE      COMMENT 'Red zone — immediate action required',
  created_at          TIMESTAMP     NOT NULL
)
USING DELTA
COMMENT 'Single organization record for Meridian Health System'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE OR REPLACE TABLE quality_dimension (
  dimension_id        INT           NOT NULL,
  dimension_name      STRING        NOT NULL,
  dimension_description STRING
)
USING DELTA
COMMENT 'The six standard data quality dimensions';

CREATE OR REPLACE TABLE dq_rule_category (
  category_id         INT           NOT NULL,
  category_name       STRING        NOT NULL,
  category_description STRING
)
USING DELTA
COMMENT 'Rule library categories for browsing and filtering';

-- ────────────────────────────────────────────────────────────
-- PEOPLE & HIERARCHY TABLES
-- ────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE business_owner (
  owner_id            INT           NOT NULL,
  owner_name          STRING        NOT NULL,
  owner_email         STRING,
  department          STRING,
  role                STRING        COMMENT 'Executive | Domain Owner | DBA | Steward',
  created_at          TIMESTAMP     NOT NULL
)
USING DELTA
CLUSTER BY (role)
COMMENT 'Business owners and stewards accountable for data assets';

CREATE OR REPLACE TABLE data_domain (
  domain_id           INT           NOT NULL,
  organization_id     INT           NOT NULL,
  business_owner_id   INT           NOT NULL,
  domain_name         STRING        NOT NULL,
  domain_description  STRING,
  dq_threshold_min    DOUBLE,
  dq_threshold_warning DOUBLE,
  dq_threshold_critical DOUBLE,
  inherits_org_threshold BOOLEAN    DEFAULT FALSE,
  created_at          TIMESTAMP     NOT NULL,
  CONSTRAINT fk_domain_org    FOREIGN KEY (organization_id)   REFERENCES organization(organization_id),
  CONSTRAINT fk_domain_owner  FOREIGN KEY (business_owner_id) REFERENCES business_owner(owner_id)
)
USING DELTA
CLUSTER BY (organization_id)
COMMENT 'Data domains — top-level subject area groupings';

CREATE OR REPLACE TABLE catalog (
  catalog_id          INT           NOT NULL,
  domain_id           INT           NOT NULL,
  owner_id            INT           NOT NULL,
  catalog_name        STRING        NOT NULL,
  platform            STRING        COMMENT 'Databricks | Snowflake | Redshift etc.',
  environment         STRING        COMMENT 'prod | dev | staging',
  dq_threshold_min    DOUBLE,
  dq_threshold_warning DOUBLE,
  dq_threshold_critical DOUBLE,
  inherits_domain_threshold BOOLEAN DEFAULT FALSE,
  created_at          TIMESTAMP     NOT NULL,
  CONSTRAINT fk_catalog_domain FOREIGN KEY (domain_id) REFERENCES data_domain(domain_id),
  CONSTRAINT fk_catalog_owner  FOREIGN KEY (owner_id)  REFERENCES business_owner(owner_id)
)
USING DELTA
CLUSTER BY (domain_id)
COMMENT 'Catalogs / databases within each domain';

CREATE OR REPLACE TABLE schema (
  schema_id           INT           NOT NULL,
  catalog_id          INT           NOT NULL,
  owner_id            INT           NOT NULL,
  schema_name         STRING        NOT NULL,
  created_at          TIMESTAMP     NOT NULL,
  CONSTRAINT fk_schema_catalog FOREIGN KEY (catalog_id) REFERENCES catalog(catalog_id),
  CONSTRAINT fk_schema_owner   FOREIGN KEY (owner_id)   REFERENCES business_owner(owner_id)
)
USING DELTA
CLUSTER BY (catalog_id)
COMMENT 'Schemas within each catalog';

CREATE OR REPLACE TABLE dq_table (
  table_id                  INT       NOT NULL,
  schema_id                 INT       NOT NULL,
  owner_id                  INT       NOT NULL,
  table_name                STRING    NOT NULL,
  table_description         STRING,
  is_critical               BOOLEAN   DEFAULT FALSE,
  expected_refresh_cadence  STRING    COMMENT 'hourly | daily | weekly',
  last_ingested_at          TIMESTAMP,
  created_at                TIMESTAMP NOT NULL,
  CONSTRAINT fk_table_schema FOREIGN KEY (schema_id) REFERENCES schema(schema_id),
  CONSTRAINT fk_table_owner  FOREIGN KEY (owner_id)  REFERENCES business_owner(owner_id)
)
USING DELTA
CLUSTER BY (schema_id, is_critical)
COMMENT 'Tables tracked for data quality';

CREATE OR REPLACE TABLE dq_column (
  column_id           INT           NOT NULL,
  table_id            INT           NOT NULL,
  column_name         STRING        NOT NULL,
  data_type           STRING,
  is_nullable         BOOLEAN       DEFAULT TRUE,
  is_primary_key      BOOLEAN       DEFAULT FALSE,
  is_foreign_key      BOOLEAN       DEFAULT FALSE,
  created_at          TIMESTAMP     NOT NULL,
  CONSTRAINT fk_column_table FOREIGN KEY (table_id) REFERENCES dq_table(table_id)
)
USING DELTA
CLUSTER BY (table_id)
COMMENT 'Columns tracked within each monitored table';

-- ────────────────────────────────────────────────────────────
-- RULE LIBRARY TABLES
-- ────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE dq_rule (
  rule_id                 INT       NOT NULL,
  dimension_id            INT       NOT NULL,
  category_id             INT       NOT NULL,
  parent_rule_id          INT       COMMENT 'Self-ref for versioning chain',
  rule_name               STRING    NOT NULL,
  rule_description        STRING,
  rule_type               STRING    COMMENT 'null_check | row_count_check | unique_check | range_check | stddev_check | anomaly_check | pattern_check | referential_check | freshness_check',
  default_severity_weight DOUBLE    COMMENT '1.0=minor, 2.0=moderate, 3.0=egregious',
  rule_sql                STRING    COMMENT 'Parameterized SQL template for the rule',
  rule_status             STRING    DEFAULT 'active' COMMENT 'draft | active | deprecated',
  version                 INT       DEFAULT 1,
  is_system_rule          BOOLEAN   DEFAULT FALSE,
  created_by              STRING,
  created_at              TIMESTAMP NOT NULL,
  deprecated_at           TIMESTAMP,
  CONSTRAINT fk_rule_dimension FOREIGN KEY (dimension_id) REFERENCES quality_dimension(dimension_id),
  CONSTRAINT fk_rule_category  FOREIGN KEY (category_id)  REFERENCES dq_rule_category(category_id)
)
USING DELTA
CLUSTER BY (dimension_id, rule_type)
COMMENT 'Reusable data quality rule library';

CREATE OR REPLACE TABLE column_rule_assignment (
  assignment_id           INT       NOT NULL,
  column_id               INT       NOT NULL,
  rule_id                 INT       NOT NULL,
  threshold_value         DOUBLE    COMMENT 'Column-specific threshold value',
  threshold_operator      STRING    COMMENT 'lt | gt | eq | between',
  custom_severity_weight  DOUBLE    COMMENT 'Override default rule severity for this column',
  is_active               BOOLEAN   DEFAULT TRUE,
  created_at              TIMESTAMP NOT NULL,
  CONSTRAINT fk_assign_column FOREIGN KEY (column_id) REFERENCES dq_column(column_id),
  CONSTRAINT fk_assign_rule   FOREIGN KEY (rule_id)   REFERENCES dq_rule(rule_id)
)
USING DELTA
CLUSTER BY (column_id)
COMMENT 'Assigns rules to specific columns with column-level thresholds';

-- ────────────────────────────────────────────────────────────
-- EXECUTION & RESULTS TABLES
-- ────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE dq_run (
  run_id              INT           NOT NULL,
  run_timestamp       TIMESTAMP     NOT NULL,
  triggered_by        STRING        COMMENT 'scheduled | pipeline_event | manual',
  pipeline_job_id     STRING,
  run_scope           STRING        COMMENT 'full | incremental',
  status              STRING        COMMENT 'completed | failed | partial'
)
USING DELTA
CLUSTER BY (run_timestamp)
COMMENT 'One record per DQ pipeline execution run';

CREATE OR REPLACE TABLE column_rule_result (
  result_id           BIGINT        NOT NULL,
  run_id              INT           NOT NULL,
  assignment_id       INT           NOT NULL,
  passed              BOOLEAN       NOT NULL,
  total_records       BIGINT,
  failed_records      BIGINT,
  failure_rate        DOUBLE        COMMENT 'failed_records / total_records',
  raw_score           DOUBLE        COMMENT '0-100 before severity weighting',
  weighted_score      DOUBLE        COMMENT 'raw_score adjusted by severity_weight',
  egregious_flag      BOOLEAN       COMMENT 'TRUE if weighted_score < 50',
  executed_rule_sql   STRING        COMMENT 'Snapshot of actual SQL executed — immutable audit record',
  result_detail       STRING        COMMENT 'JSON: sample failures, error breakdown',
  evaluated_at        TIMESTAMP     NOT NULL,
  CONSTRAINT fk_result_run    FOREIGN KEY (run_id)        REFERENCES dq_run(run_id),
  CONSTRAINT fk_result_assign FOREIGN KEY (assignment_id) REFERENCES column_rule_assignment(assignment_id)
)
USING DELTA
CLUSTER BY (run_id, assignment_id)
COMMENT 'Core fact table — one record per column-rule-run combination'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- ────────────────────────────────────────────────────────────
-- ALERTING TABLE
-- ────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE dq_alert (
  alert_id            INT           NOT NULL,
  dimension_id        INT,
  acknowledged_by     INT           COMMENT 'FK to business_owner',
  entity_type         STRING        COMMENT 'org | domain | catalog | schema | table',
  entity_id           INT           NOT NULL,
  alert_type          STRING        COMMENT 'threshold_breach | score_regression | unmonitored_asset',
  severity            STRING        COMMENT 'info | warning | critical',
  message             STRING,
  triggered_at        TIMESTAMP     NOT NULL,
  acknowledged_at     TIMESTAMP
)
USING DELTA
CLUSTER BY (entity_type, severity)
COMMENT 'Alerts triggered by threshold breaches and regressions';

-- ============================================================
-- VIEWS — SCORE ROLLUPS FOR DASHBOARD
-- These replace pre-aggregated tables and compute dynamically
-- ============================================================

-- ── Latest run ID helper ─────────────────────────────────────
CREATE OR REPLACE VIEW v_latest_run AS
SELECT MAX(run_id) AS latest_run_id, MAX(run_timestamp) AS latest_run_ts
FROM dq_run WHERE status = 'completed';

-- ── Column scores for latest run ─────────────────────────────
CREATE OR REPLACE VIEW v_column_scores_latest AS
SELECT
  crr.assignment_id,
  crr.run_id,
  cra.column_id,
  cra.rule_id,
  dc.table_id,
  dc.column_name,
  dc.is_primary_key,
  dc.is_foreign_key,
  dr.dimension_id,
  qd.dimension_name,
  dr.rule_name,
  dr.rule_type,
  crr.passed,
  crr.total_records,
  crr.failed_records,
  crr.failure_rate,
  crr.raw_score,
  crr.weighted_score,
  crr.egregious_flag,
  crr.executed_rule_sql,
  crr.evaluated_at
FROM column_rule_result crr
JOIN column_rule_assignment cra ON crr.assignment_id = cra.assignment_id
JOIN dq_column dc               ON cra.column_id = dc.column_id
JOIN dq_rule dr                 ON cra.rule_id = dr.rule_id
JOIN quality_dimension qd       ON dr.dimension_id = qd.dimension_id
JOIN v_latest_run lr            ON crr.run_id = lr.latest_run_id;

-- ── Table scores — latest run ─────────────────────────────────
CREATE OR REPLACE VIEW v_table_scores_latest AS
SELECT
  t.table_id,
  t.table_name,
  t.is_critical,
  t.expected_refresh_cadence,
  t.owner_id,
  s.schema_id,
  s.schema_name,
  c.catalog_id,
  c.catalog_name,
  d.domain_id,
  d.domain_name,
  ROUND(AVG(cs.weighted_score), 2)                                                        AS overall_score,
  ROUND(AVG(CASE WHEN cs.dimension_name = 'Completeness' THEN cs.weighted_score END), 2) AS completeness_score,
  ROUND(AVG(CASE WHEN cs.dimension_name = 'Accuracy'     THEN cs.weighted_score END), 2) AS accuracy_score,
  ROUND(AVG(CASE WHEN cs.dimension_name = 'Validity'     THEN cs.weighted_score END), 2) AS validity_score,
  ROUND(AVG(CASE WHEN cs.dimension_name = 'Consistency'  THEN cs.weighted_score END), 2) AS consistency_score,
  ROUND(AVG(CASE WHEN cs.dimension_name = 'Uniqueness'   THEN cs.weighted_score END), 2) AS uniqueness_score,
  ROUND(AVG(CASE WHEN cs.dimension_name = 'Timeliness'   THEN cs.weighted_score END), 2) AS timeliness_score,
  COUNT(DISTINCT cs.assignment_id)                                                        AS total_rules_run,
  SUM(CASE WHEN cs.passed THEN 1 ELSE 0 END)                                             AS rules_passed,
  SUM(CASE WHEN NOT cs.passed THEN 1 ELSE 0 END)                                         AS rules_failed,
  SUM(CASE WHEN cs.egregious_flag THEN 1 ELSE 0 END)                                     AS egregious_count,
  COALESCE(d.dq_threshold_min, o.dq_threshold_min)                                       AS effective_threshold_min,
  COALESCE(d.dq_threshold_warning, o.dq_threshold_warning)                               AS effective_threshold_warning
FROM v_column_scores_latest cs
JOIN dq_table t       ON cs.table_id = t.table_id
JOIN schema s         ON t.schema_id = s.schema_id
JOIN catalog c        ON s.catalog_id = c.catalog_id
JOIN data_domain d    ON c.domain_id = d.domain_id
JOIN organization o   ON d.organization_id = o.organization_id
GROUP BY t.table_id, t.table_name, t.is_critical, t.expected_refresh_cadence, t.owner_id,
         s.schema_id, s.schema_name, c.catalog_id, c.catalog_name,
         d.domain_id, d.domain_name,
         d.dq_threshold_min, d.dq_threshold_warning, o.dq_threshold_min, o.dq_threshold_warning;

-- ── Schema scores — latest run ────────────────────────────────
CREATE OR REPLACE VIEW v_schema_scores_latest AS
SELECT
  schema_id, schema_name, catalog_id, catalog_name, domain_id, domain_name,
  ROUND(AVG(overall_score), 2)        AS overall_score,
  ROUND(AVG(completeness_score), 2)   AS completeness_score,
  ROUND(AVG(accuracy_score), 2)       AS accuracy_score,
  ROUND(AVG(validity_score), 2)       AS validity_score,
  ROUND(AVG(consistency_score), 2)    AS consistency_score,
  ROUND(AVG(uniqueness_score), 2)     AS uniqueness_score,
  ROUND(AVG(timeliness_score), 2)     AS timeliness_score,
  COUNT(*) AS total_tables,
  SUM(CASE WHEN overall_score < effective_threshold_min THEN 1 ELSE 0 END) AS tables_below_threshold
FROM v_table_scores_latest
GROUP BY schema_id, schema_name, catalog_id, catalog_name, domain_id, domain_name;

-- ── Catalog scores — latest run ───────────────────────────────
CREATE OR REPLACE VIEW v_catalog_scores_latest AS
SELECT
  catalog_id, catalog_name, domain_id, domain_name,
  ROUND(AVG(overall_score), 2)        AS overall_score,
  ROUND(AVG(completeness_score), 2)   AS completeness_score,
  ROUND(AVG(accuracy_score), 2)       AS accuracy_score,
  ROUND(AVG(validity_score), 2)       AS validity_score,
  ROUND(AVG(consistency_score), 2)    AS consistency_score,
  ROUND(AVG(uniqueness_score), 2)     AS uniqueness_score,
  ROUND(AVG(timeliness_score), 2)     AS timeliness_score,
  COUNT(DISTINCT schema_id)           AS total_schemas,
  COUNT(*)                            AS total_tables,
  SUM(CASE WHEN overall_score < effective_threshold_min THEN 1 ELSE 0 END) AS tables_below_threshold
FROM v_table_scores_latest
GROUP BY catalog_id, catalog_name, domain_id, domain_name;

-- ── Domain scores — latest run ────────────────────────────────
CREATE OR REPLACE VIEW v_domain_scores_latest AS
SELECT
  domain_id, domain_name,
  ROUND(AVG(overall_score), 2)        AS overall_score,
  ROUND(AVG(completeness_score), 2)   AS completeness_score,
  ROUND(AVG(accuracy_score), 2)       AS accuracy_score,
  ROUND(AVG(validity_score), 2)       AS validity_score,
  ROUND(AVG(consistency_score), 2)    AS consistency_score,
  ROUND(AVG(uniqueness_score), 2)     AS uniqueness_score,
  ROUND(AVG(timeliness_score), 2)     AS timeliness_score,
  COUNT(DISTINCT catalog_id)          AS total_catalogs,
  COUNT(*)                            AS total_tables,
  SUM(CASE WHEN overall_score < effective_threshold_min THEN 1 ELSE 0 END) AS tables_below_threshold
FROM v_table_scores_latest
GROUP BY domain_id, domain_name;

-- ── Org score — latest run ────────────────────────────────────
CREATE OR REPLACE VIEW v_org_score_latest AS
SELECT
  o.organization_id,
  o.organization_name,
  ROUND(AVG(ts.overall_score), 2)       AS overall_score,
  ROUND(AVG(ts.completeness_score), 2)  AS completeness_score,
  ROUND(AVG(ts.accuracy_score), 2)      AS accuracy_score,
  ROUND(AVG(ts.validity_score), 2)      AS validity_score,
  ROUND(AVG(ts.consistency_score), 2)   AS consistency_score,
  ROUND(AVG(ts.uniqueness_score), 2)    AS uniqueness_score,
  ROUND(AVG(ts.timeliness_score), 2)    AS timeliness_score,
  COUNT(DISTINCT ts.domain_id)          AS total_domains,
  COUNT(*)                              AS total_tables,
  SUM(CASE WHEN ts.overall_score < ts.effective_threshold_min THEN 1 ELSE 0 END) AS tables_below_threshold,
  ROUND(
    COUNT(DISTINCT CASE WHEN ts.total_rules_run > 0 THEN ts.table_id END) * 100.0
    / NULLIF(COUNT(DISTINCT ts.table_id), 0), 1
  )                                     AS pct_assets_monitored
FROM v_table_scores_latest ts
JOIN data_domain d  ON ts.domain_id = d.domain_id
JOIN organization o ON d.organization_id = o.organization_id
GROUP BY o.organization_id, o.organization_name;

-- ── Owner scores — latest run ─────────────────────────────────
CREATE OR REPLACE VIEW v_owner_scores_latest AS
SELECT
  bo.owner_id,
  bo.owner_name,
  bo.department,
  bo.role,
  ROUND(AVG(ts.overall_score), 2)       AS overall_score,
  ROUND(AVG(ts.completeness_score), 2)  AS completeness_score,
  ROUND(AVG(ts.accuracy_score), 2)      AS accuracy_score,
  ROUND(AVG(ts.validity_score), 2)      AS validity_score,
  ROUND(AVG(ts.consistency_score), 2)   AS consistency_score,
  ROUND(AVG(ts.uniqueness_score), 2)    AS uniqueness_score,
  ROUND(AVG(ts.timeliness_score), 2)    AS timeliness_score,
  COUNT(*)                              AS total_tables_owned,
  SUM(CASE WHEN ts.overall_score < ts.effective_threshold_min THEN 1 ELSE 0 END) AS tables_below_threshold
FROM v_table_scores_latest ts
JOIN business_owner bo ON ts.owner_id = bo.owner_id
GROUP BY bo.owner_id, bo.owner_name, bo.department, bo.role;

-- ── 30-day trend (all runs, domain level) ─────────────────────
CREATE OR REPLACE VIEW v_domain_score_trend AS
SELECT
  d.domain_id,
  d.domain_name,
  r.run_id,
  r.run_timestamp,
  ROUND(AVG(crr.weighted_score), 2)                                                         AS overall_score,
  ROUND(AVG(CASE WHEN qd.dimension_name = 'Completeness' THEN crr.weighted_score END), 2)  AS completeness_score,
  ROUND(AVG(CASE WHEN qd.dimension_name = 'Accuracy'     THEN crr.weighted_score END), 2)  AS accuracy_score,
  ROUND(AVG(CASE WHEN qd.dimension_name = 'Validity'     THEN crr.weighted_score END), 2)  AS validity_score,
  ROUND(AVG(CASE WHEN qd.dimension_name = 'Consistency'  THEN crr.weighted_score END), 2)  AS consistency_score,
  ROUND(AVG(CASE WHEN qd.dimension_name = 'Uniqueness'   THEN crr.weighted_score END), 2)  AS uniqueness_score,
  ROUND(AVG(CASE WHEN qd.dimension_name = 'Timeliness'   THEN crr.weighted_score END), 2)  AS timeliness_score
FROM column_rule_result crr
JOIN column_rule_assignment cra ON crr.assignment_id = cra.assignment_id
JOIN dq_column dc               ON cra.column_id = dc.column_id
JOIN dq_rule dr                 ON cra.rule_id = dr.rule_id
JOIN quality_dimension qd       ON dr.dimension_id = qd.dimension_id
JOIN dq_table t                 ON dc.table_id = t.table_id
JOIN schema s                   ON t.schema_id = s.schema_id
JOIN catalog c                  ON s.catalog_id = c.catalog_id
JOIN data_domain d              ON c.domain_id = d.domain_id
JOIN dq_run r                   ON crr.run_id = r.run_id
GROUP BY d.domain_id, d.domain_name, r.run_id, r.run_timestamp;

-- ── 30-day trend (table level) ────────────────────────────────
CREATE OR REPLACE VIEW v_table_score_trend AS
SELECT
  t.table_id,
  t.table_name,
  d.domain_name,
  r.run_id,
  r.run_timestamp,
  ROUND(AVG(crr.weighted_score), 2) AS overall_score
FROM column_rule_result crr
JOIN column_rule_assignment cra ON crr.assignment_id = cra.assignment_id
JOIN dq_column dc               ON cra.column_id = dc.column_id
JOIN dq_table t                 ON dc.table_id = t.table_id
JOIN schema s                   ON t.schema_id = s.schema_id
JOIN catalog c                  ON s.catalog_id = c.catalog_id
JOIN data_domain d              ON c.domain_id = d.domain_id
JOIN dq_run r                   ON crr.run_id = r.run_id
GROUP BY t.table_id, t.table_name, d.domain_name, r.run_id, r.run_timestamp;

-- ── Column-level detail for table drilldown ───────────────────
CREATE OR REPLACE VIEW v_column_rule_detail AS
SELECT
  dc.column_id,
  dc.column_name,
  dc.data_type,
  dc.is_primary_key,
  dc.is_foreign_key,
  t.table_id,
  t.table_name,
  s.schema_name,
  c.catalog_name,
  d.domain_name,
  qd.dimension_name,
  dr.rule_name,
  dr.rule_type,
  dr.default_severity_weight,
  cra.custom_severity_weight,
  cra.threshold_value,
  cra.threshold_operator,
  crr.run_id,
  crr.passed,
  crr.total_records,
  crr.failed_records,
  crr.failure_rate,
  crr.raw_score,
  crr.weighted_score,
  crr.egregious_flag,
  crr.executed_rule_sql,
  crr.result_detail,
  crr.evaluated_at
FROM column_rule_result crr
JOIN column_rule_assignment cra ON crr.assignment_id = cra.assignment_id
JOIN dq_column dc               ON cra.column_id = dc.column_id
JOIN dq_rule dr                 ON cra.rule_id = dr.rule_id
JOIN quality_dimension qd       ON dr.dimension_id = qd.dimension_id
JOIN dq_table t                 ON dc.table_id = t.table_id
JOIN schema s                   ON t.schema_id = s.schema_id
JOIN catalog c                  ON s.catalog_id = c.catalog_id
JOIN data_domain d              ON c.domain_id = d.domain_id
JOIN v_latest_run lr            ON crr.run_id = lr.latest_run_id;

-- ============================================================
-- DATA LOAD — INSERT FROM EXTERNAL JSON OR STAGING
-- (Run after uploading dq_data.json to DBFS or a volume)
-- ============================================================

-- Step 1: Load JSON into a staging table
CREATE OR REPLACE TEMP VIEW json_stage USING json
OPTIONS (path 'dbfs:/FileStore/dq_platform/dq_data.json', multiLine 'true');

-- Step 2: Load each entity (adjust field mapping if needed)
INSERT INTO organization         SELECT * FROM json_stage LATERAL VIEW EXPLODE(organization)         t AS r SELECT r.*;
INSERT INTO business_owner       SELECT * FROM json_stage LATERAL VIEW EXPLODE(business_owners)      t AS r SELECT r.*;
INSERT INTO quality_dimension    SELECT * FROM json_stage LATERAL VIEW EXPLODE(quality_dimensions)   t AS r SELECT r.*;
INSERT INTO dq_rule_category     SELECT * FROM json_stage LATERAL VIEW EXPLODE(rule_categories)      t AS r SELECT r.*;
INSERT INTO data_domain          SELECT * FROM json_stage LATERAL VIEW EXPLODE(data_domains)         t AS r SELECT r.*;
INSERT INTO catalog              SELECT * FROM json_stage LATERAL VIEW EXPLODE(catalogs)             t AS r SELECT r.*;
INSERT INTO schema               SELECT * FROM json_stage LATERAL VIEW EXPLODE(schemas)              t AS r SELECT r.*;
INSERT INTO dq_table             SELECT * FROM json_stage LATERAL VIEW EXPLODE(tables)               t AS r SELECT r.*;
INSERT INTO dq_column            SELECT * FROM json_stage LATERAL VIEW EXPLODE(columns)              t AS r SELECT r.*;
INSERT INTO dq_rule              SELECT * FROM json_stage LATERAL VIEW EXPLODE(dq_rules)             t AS r SELECT r.*;
INSERT INTO column_rule_assignment SELECT * FROM json_stage LATERAL VIEW EXPLODE(column_rule_assignments) t AS r SELECT r.*;
INSERT INTO dq_run               SELECT * FROM json_stage LATERAL VIEW EXPLODE(dq_runs)              t AS r SELECT r.*;
INSERT INTO column_rule_result   SELECT * FROM json_stage LATERAL VIEW EXPLODE(column_rule_results)  t AS r SELECT r.*;
INSERT INTO dq_alert             SELECT * FROM json_stage LATERAL VIEW EXPLODE(dq_alerts)            t AS r SELECT r.*;

-- ============================================================
-- OPTIMIZE & ANALYZE (run after load)
-- ============================================================
OPTIMIZE column_rule_result ZORDER BY (run_id, assignment_id);
OPTIMIZE column_rule_assignment ZORDER BY (column_id);
OPTIMIZE dq_column ZORDER BY (table_id);
ANALYZE TABLE column_rule_result COMPUTE STATISTICS;
ANALYZE TABLE column_rule_assignment COMPUTE STATISTICS;
