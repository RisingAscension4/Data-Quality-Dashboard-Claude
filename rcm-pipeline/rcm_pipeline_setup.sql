-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Revenue Cycle Management Pipeline
-- MAGIC Simulated Bronze → Silver → Gold pipeline with DQ score propagation.
-- MAGIC
-- MAGIC **Pipeline structure:**
-- MAGIC ```
-- MAGIC bronze_claims ──┬──▶ silver_claims_clinical  ──┬──▶ gold_clinical_outcomes
-- MAGIC                 │                               └──▶ gold_provider_performance
-- MAGIC                 └──▶ silver_claims_financial ──┬──▶ gold_revenue_analysis
-- MAGIC                                                └──▶ gold_denial_management
-- MAGIC ```

-- COMMAND ----------

USE CATALOG serverless_stable_jc9zgx_catalog;
CREATE SCHEMA IF NOT EXISTS rcm_pipeline;
USE SCHEMA rcm_pipeline;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Bronze Layer — Raw Claims Ingestion (High Quality ~96%)

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_claims (
  claim_id        STRING    NOT NULL,
  patient_id      STRING    NOT NULL,
  provider_npi    STRING    NOT NULL,
  facility_id     STRING    NOT NULL,
  service_date    DATE      NOT NULL,
  admit_date      DATE,
  discharge_date  DATE,
  diagnosis_code  STRING    NOT NULL,
  procedure_code  STRING    NOT NULL,
  claim_type      STRING    NOT NULL,
  billed_amount   DOUBLE    NOT NULL,
  allowed_amount  DOUBLE,
  paid_amount     DOUBLE,
  patient_resp    DOUBLE,
  payer_id        STRING    NOT NULL,
  payer_name      STRING,
  claim_status    STRING    NOT NULL,
  filing_date     DATE      NOT NULL,
  adjudication_date DATE,
  denial_code     STRING,
  denial_reason   STRING,
  drg_code        STRING,
  revenue_code    STRING,
  modifier_code   STRING,
  place_of_service STRING,
  referring_npi   STRING,
  authorization_num STRING,
  created_at      TIMESTAMP NOT NULL
) USING DELTA;

-- COMMAND ----------

-- Insert 200 rows of realistic RCM claims data
INSERT INTO bronze_claims
SELECT
  CONCAT('CLM-', LPAD(CAST(id AS STRING), 6, '0')) AS claim_id,
  CONCAT('PAT-', LPAD(CAST(MOD(id * 7, 150) + 1 AS STRING), 5, '0')) AS patient_id,
  CONCAT('NPI-', LPAD(CAST(MOD(id * 3, 50) + 1000000 AS STRING), 10, '0')) AS provider_npi,
  CONCAT('FAC-', LPAD(CAST(MOD(id, 10) + 1 AS STRING), 3, '0')) AS facility_id,
  DATE_ADD('2025-01-01', MOD(id * 13, 365)) AS service_date,
  CASE WHEN MOD(id, 3) = 0 THEN DATE_ADD('2025-01-01', MOD(id * 13, 365)) END AS admit_date,
  CASE WHEN MOD(id, 3) = 0 THEN DATE_ADD('2025-01-01', MOD(id * 13, 365) + MOD(id, 7) + 1) END AS discharge_date,
  CASE
    WHEN MOD(id, 5) = 0 THEN CONCAT('E11.', MOD(id, 10))
    WHEN MOD(id, 5) = 1 THEN CONCAT('I10.', MOD(id, 5))
    WHEN MOD(id, 5) = 2 THEN CONCAT('J18.', MOD(id, 9))
    WHEN MOD(id, 5) = 3 THEN CONCAT('M54.', MOD(id, 6))
    ELSE CONCAT('Z00.', MOD(id, 3))
  END AS diagnosis_code,
  CASE
    WHEN MOD(id, 4) = 0 THEN '99213'
    WHEN MOD(id, 4) = 1 THEN '99214'
    WHEN MOD(id, 4) = 2 THEN '99215'
    ELSE '99283'
  END AS procedure_code,
  CASE
    WHEN MOD(id, 3) = 0 THEN 'Inpatient'
    WHEN MOD(id, 3) = 1 THEN 'Outpatient'
    ELSE 'Professional'
  END AS claim_type,
  ROUND(50 + MOD(id * 97, 9950), 2) AS billed_amount,
  ROUND((50 + MOD(id * 97, 9950)) * (0.6 + MOD(id, 30) / 100.0), 2) AS allowed_amount,
  ROUND((50 + MOD(id * 97, 9950)) * (0.5 + MOD(id, 25) / 100.0), 2) AS paid_amount,
  ROUND((50 + MOD(id * 97, 9950)) * 0.1, 2) AS patient_resp,
  CONCAT('PYR-', LPAD(CAST(MOD(id, 8) + 1 AS STRING), 3, '0')) AS payer_id,
  CASE MOD(id, 8)
    WHEN 0 THEN 'Blue Cross Blue Shield'
    WHEN 1 THEN 'Aetna'
    WHEN 2 THEN 'UnitedHealthcare'
    WHEN 3 THEN 'Cigna'
    WHEN 4 THEN 'Humana'
    WHEN 5 THEN 'Medicare'
    WHEN 6 THEN 'Medicaid'
    ELSE 'Tricare'
  END AS payer_name,
  CASE
    WHEN MOD(id, 10) < 6 THEN 'Paid'
    WHEN MOD(id, 10) < 8 THEN 'Denied'
    WHEN MOD(id, 10) < 9 THEN 'Pending'
    ELSE 'Appealed'
  END AS claim_status,
  DATE_ADD('2025-01-01', MOD(id * 13, 365) + MOD(id, 5)) AS filing_date,
  CASE WHEN MOD(id, 10) != 8 THEN DATE_ADD('2025-01-01', MOD(id * 13, 365) + MOD(id, 5) + MOD(id, 20) + 5) END AS adjudication_date,
  CASE WHEN MOD(id, 10) IN (6, 7) THEN CONCAT('CO-', MOD(id, 50) + 1) END AS denial_code,
  CASE WHEN MOD(id, 10) IN (6, 7) THEN
    CASE MOD(id, 5)
      WHEN 0 THEN 'Missing prior authorization'
      WHEN 1 THEN 'Duplicate claim submission'
      WHEN 2 THEN 'Service not covered under plan'
      WHEN 3 THEN 'Incorrect patient information'
      ELSE 'Timely filing limit exceeded'
    END
  END AS denial_reason,
  CASE WHEN MOD(id, 3) = 0 THEN CONCAT('DRG-', LPAD(CAST(MOD(id, 500) + 1 AS STRING), 3, '0')) END AS drg_code,
  CONCAT('0', MOD(id, 9) + 1, '0', MOD(id, 9)) AS revenue_code,
  CASE WHEN MOD(id, 7) = 0 THEN '25' WHEN MOD(id, 7) = 1 THEN '59' END AS modifier_code,
  CASE MOD(id, 4)
    WHEN 0 THEN '11' WHEN 1 THEN '21' WHEN 2 THEN '23' ELSE '22'
  END AS place_of_service,
  CASE WHEN MOD(id, 5) = 0 THEN CONCAT('NPI-', LPAD(CAST(MOD(id * 11, 30) + 2000000 AS STRING), 10, '0')) END AS referring_npi,
  CASE WHEN MOD(id, 3) = 0 THEN CONCAT('AUTH-', LPAD(CAST(id * 17 AS STRING), 8, '0')) END AS authorization_num,
  TIMESTAMP(DATE_ADD('2025-01-01', MOD(id * 13, 365) + MOD(id, 3))) AS created_at
FROM (SELECT EXPLODE(SEQUENCE(1, 200)) AS id);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Silver Layer — Clinical (High Quality ~93%) and Financial (Poor Quality ~62%)

-- COMMAND ----------

-- Silver Clinical: Good quality — clean extraction of clinical fields
CREATE OR REPLACE TABLE silver_claims_clinical AS
SELECT
  claim_id,
  patient_id,
  provider_npi,
  facility_id,
  service_date,
  admit_date,
  discharge_date,
  diagnosis_code,
  procedure_code,
  claim_type,
  drg_code,
  place_of_service,
  referring_npi,
  CASE
    WHEN discharge_date IS NOT NULL AND admit_date IS NOT NULL
    THEN DATEDIFF(discharge_date, admit_date)
  END AS length_of_stay,
  CASE
    WHEN diagnosis_code LIKE 'E11%' THEN 'Endocrine'
    WHEN diagnosis_code LIKE 'I10%' THEN 'Cardiovascular'
    WHEN diagnosis_code LIKE 'J18%' THEN 'Respiratory'
    WHEN diagnosis_code LIKE 'M54%' THEN 'Musculoskeletal'
    ELSE 'Preventive'
  END AS diagnosis_category,
  created_at,
  current_timestamp() AS processed_at
FROM bronze_claims;

-- COMMAND ----------

-- Silver Financial: POOR quality — intentionally introduce data quality issues
CREATE OR REPLACE TABLE silver_claims_financial AS
SELECT
  claim_id,
  patient_id,
  payer_id,
  payer_name,
  claim_status,
  filing_date,
  adjudication_date,

  -- ISSUE 1: Billed amounts randomly nullified (completeness problem)
  CASE WHEN MOD(ABS(HASH(claim_id)), 5) = 0 THEN NULL ELSE billed_amount END AS billed_amount,

  -- ISSUE 2: Allowed > Billed (consistency problem) — swap values for some rows
  CASE WHEN MOD(ABS(HASH(claim_id)), 4) = 0 THEN billed_amount * 1.5 ELSE allowed_amount END AS allowed_amount,

  -- ISSUE 3: Negative paid amounts (validity problem)
  CASE WHEN MOD(ABS(HASH(claim_id)), 6) = 0 THEN -1 * paid_amount ELSE paid_amount END AS paid_amount,

  patient_resp,
  denial_code,
  denial_reason,
  authorization_num,

  -- ISSUE 4: Duplicate payer IDs with wrong names (accuracy problem)
  CASE WHEN MOD(ABS(HASH(claim_id)), 7) = 0 THEN 'UNKNOWN_PAYER' ELSE payer_name END AS payer_display_name,

  -- ISSUE 5: Filing dates in the future (timeliness problem)
  CASE WHEN MOD(ABS(HASH(claim_id)), 8) = 0 THEN DATE_ADD(filing_date, 1000) ELSE filing_date END AS adjusted_filing_date,

  -- ISSUE 6: Revenue code format broken (pattern problem)
  CASE WHEN MOD(ABS(HASH(claim_id)), 5) < 2 THEN CONCAT('XX-', revenue_code) ELSE revenue_code END AS revenue_code,

  created_at,
  current_timestamp() AS processed_at
FROM bronze_claims;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Gold Layer — Downstream from Silver

-- COMMAND ----------

-- Gold Clinical Outcomes (from good silver — quality stays high ~91%)
CREATE OR REPLACE TABLE gold_clinical_outcomes AS
SELECT
  diagnosis_category,
  claim_type,
  COUNT(*) AS total_encounters,
  COUNT(DISTINCT patient_id) AS unique_patients,
  AVG(length_of_stay) AS avg_length_of_stay,
  COUNT(CASE WHEN length_of_stay > 7 THEN 1 END) AS extended_stays,
  COUNT(DISTINCT diagnosis_code) AS unique_diagnoses,
  COUNT(DISTINCT facility_id) AS facilities_involved,
  MIN(service_date) AS earliest_service,
  MAX(service_date) AS latest_service,
  current_timestamp() AS refreshed_at
FROM silver_claims_clinical
GROUP BY diagnosis_category, claim_type;

-- COMMAND ----------

-- Gold Provider Performance (from good silver — quality stays high ~90%)
CREATE OR REPLACE TABLE gold_provider_performance AS
SELECT
  provider_npi,
  facility_id,
  COUNT(*) AS total_claims,
  COUNT(DISTINCT patient_id) AS unique_patients,
  COUNT(DISTINCT diagnosis_code) AS diagnosis_breadth,
  AVG(length_of_stay) AS avg_los,
  COUNT(CASE WHEN claim_type = 'Inpatient' THEN 1 END) AS inpatient_count,
  COUNT(CASE WHEN claim_type = 'Outpatient' THEN 1 END) AS outpatient_count,
  COUNT(CASE WHEN referring_npi IS NOT NULL THEN 1 END) AS referred_cases,
  current_timestamp() AS refreshed_at
FROM silver_claims_clinical
GROUP BY provider_npi, facility_id;

-- COMMAND ----------

-- Gold Revenue Analysis (from BAD silver — inherits poor quality ~58%)
CREATE OR REPLACE TABLE gold_revenue_analysis AS
SELECT
  payer_id,
  payer_display_name,
  claim_status,
  COUNT(*) AS claim_count,
  SUM(billed_amount) AS total_billed,
  SUM(allowed_amount) AS total_allowed,
  SUM(paid_amount) AS total_paid,
  SUM(patient_resp) AS total_patient_resp,
  AVG(billed_amount) AS avg_billed,
  -- This will produce garbage metrics because of upstream issues
  ROUND(SUM(paid_amount) / NULLIF(SUM(billed_amount), 0) * 100, 2) AS collection_rate,
  ROUND(SUM(allowed_amount) / NULLIF(SUM(billed_amount), 0) * 100, 2) AS allowance_rate,
  COUNT(CASE WHEN billed_amount IS NULL THEN 1 END) AS missing_billed_count,
  COUNT(CASE WHEN paid_amount < 0 THEN 1 END) AS negative_payment_count,
  current_timestamp() AS refreshed_at
FROM silver_claims_financial
GROUP BY payer_id, payer_display_name, claim_status;

-- COMMAND ----------

-- Gold Denial Management (from BAD silver — inherits poor quality ~55%)
CREATE OR REPLACE TABLE gold_denial_management AS
SELECT
  denial_code,
  denial_reason,
  payer_id,
  payer_display_name,
  COUNT(*) AS denial_count,
  SUM(billed_amount) AS denied_billed_amount,
  SUM(paid_amount) AS denied_paid_amount,
  AVG(DATEDIFF(adjudication_date, adjusted_filing_date)) AS avg_days_to_denial,
  COUNT(CASE WHEN adjusted_filing_date > current_date() THEN 1 END) AS future_filing_dates,
  COUNT(CASE WHEN revenue_code LIKE 'XX-%' THEN 1 END) AS malformed_revenue_codes,
  COUNT(CASE WHEN payer_display_name = 'UNKNOWN_PAYER' THEN 1 END) AS unknown_payer_count,
  current_timestamp() AS refreshed_at
FROM silver_claims_financial
WHERE claim_status IN ('Denied', 'Appealed')
GROUP BY denial_code, denial_reason, payer_id, payer_display_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. DQ Score View — Matches the dashboard's v_table_scores_latest format

-- COMMAND ----------

CREATE OR REPLACE VIEW v_table_scores_latest AS
SELECT * FROM (VALUES
  -- Bronze: High quality (96.2%)
  (1, 'bronze_claims', true, 'hourly', 1, 1, 'rcm_pipeline', 1, 'serverless_stable_jc9zgx_catalog',
   1, 'Revenue Cycle', 96.2, 98.5, 95.1, 97.3, 94.8, 96.0, 95.5,
   20, 19, 1, 0, 75.0, 85.0),

  -- Silver Clinical: High quality (93.1%)
  (2, 'silver_claims_clinical', true, 'hourly', 1, 1, 'rcm_pipeline', 1, 'serverless_stable_jc9zgx_catalog',
   1, 'Revenue Cycle', 93.1, 95.2, 92.8, 94.5, 91.0, 93.5, 91.6,
   18, 17, 1, 0, 75.0, 85.0),

  -- Silver Financial: POOR quality (62.4%)
  (3, 'silver_claims_financial', true, 'hourly', 1, 1, 'rcm_pipeline', 1, 'serverless_stable_jc9zgx_catalog',
   1, 'Revenue Cycle', 62.4, 58.3, 55.7, 52.1, 48.9, 82.0, 75.4,
   20, 10, 10, 5, 75.0, 85.0),

  -- Gold Clinical Outcomes: High quality (from good silver) (91.3%)
  (4, 'gold_clinical_outcomes', true, 'daily', 1, 1, 'rcm_pipeline', 1, 'serverless_stable_jc9zgx_catalog',
   1, 'Revenue Cycle', 91.3, 93.0, 90.5, 92.7, 89.8, 91.5, 90.3,
   15, 14, 1, 0, 75.0, 85.0),

  -- Gold Provider Performance: High quality (from good silver) (90.7%)
  (5, 'gold_provider_performance', true, 'daily', 1, 1, 'rcm_pipeline', 1, 'serverless_stable_jc9zgx_catalog',
   1, 'Revenue Cycle', 90.7, 92.1, 89.3, 91.8, 88.5, 92.0, 90.5,
   15, 14, 1, 0, 75.0, 85.0),

  -- Gold Revenue Analysis: Poor quality (from bad silver) (58.1%)
  (6, 'gold_revenue_analysis', true, 'daily', 1, 1, 'rcm_pipeline', 1, 'serverless_stable_jc9zgx_catalog',
   1, 'Revenue Cycle', 58.1, 52.4, 48.9, 45.3, 42.1, 80.5, 78.4,
   18, 8, 10, 6, 75.0, 85.0),

  -- Gold Denial Management: Poor quality (from bad silver) (55.2%)
  (7, 'gold_denial_management', true, 'daily', 1, 1, 'rcm_pipeline', 1, 'serverless_stable_jc9zgx_catalog',
   1, 'Revenue Cycle', 55.2, 49.1, 44.7, 41.2, 38.5, 78.8, 79.0,
   18, 7, 11, 7, 75.0, 85.0)
)
AS t(table_id, table_name, is_critical, expected_refresh_cadence, owner_id,
     schema_id, schema_name, catalog_id, catalog_name,
     domain_id, domain_name, overall_score,
     completeness_score, accuracy_score, validity_score,
     consistency_score, uniqueness_score, timeliness_score,
     total_rules_run, rules_passed, rules_failed, egregious_count,
     effective_threshold_min, effective_threshold_warning);

-- COMMAND ----------

-- Verify the pipeline
SELECT table_name, overall_score, rules_passed, rules_failed,
  CASE
    WHEN overall_score >= 85 THEN 'HEALTHY'
    WHEN overall_score >= 75 THEN 'WARNING'
    ELSE 'CRITICAL'
  END AS status
FROM v_table_scores_latest
ORDER BY table_id;

-- COMMAND ----------

-- Verify row counts
SELECT 'bronze_claims' AS tbl, COUNT(*) AS rows FROM bronze_claims
UNION ALL SELECT 'silver_claims_clinical', COUNT(*) FROM silver_claims_clinical
UNION ALL SELECT 'silver_claims_financial', COUNT(*) FROM silver_claims_financial
UNION ALL SELECT 'gold_clinical_outcomes', COUNT(*) FROM gold_clinical_outcomes
UNION ALL SELECT 'gold_provider_performance', COUNT(*) FROM gold_provider_performance
UNION ALL SELECT 'gold_revenue_analysis', COUNT(*) FROM gold_revenue_analysis
UNION ALL SELECT 'gold_denial_management', COUNT(*) FROM gold_denial_management;
