-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Extract Data Quality Rules from Source Code Files
-- MAGIC Uses `ai_query()` with a Foundation Model to semantically parse DQ rules
-- MAGIC from DLT pipelines, SQL scripts, Great Expectations suites, dbt schemas,
-- MAGIC and PySpark validation code — no brittle regex required.
-- MAGIC
-- MAGIC **Source Volume:** `/Volumes/serverless_stable_jc9zgx_catalog/data_quality/raw_files/dq_source_examples/`

-- COMMAND ----------

USE CATALOG serverless_stable_jc9zgx_catalog;
USE DATABASE data_quality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Read source files from the Volume into a table
-- MAGIC Load the raw file contents so we can pass them to `ai_query()`.

-- COMMAND ----------

CREATE OR REPLACE TABLE dq_source_files AS
SELECT
  _metadata.file_path                                       AS file_path,
  _metadata.file_name                                       AS file_name,
  regexp_extract(_metadata.file_name, '\\.([^.]+)$', 1)     AS file_type,
  cast(value AS STRING)                                      AS file_content
FROM read_files(
  '/Volumes/serverless_stable_jc9zgx_catalog/data_quality/raw_files/dq_source_examples/',
  format => 'text',
  wholeText => true
);

-- COMMAND ----------

SELECT file_name, file_type, length(file_content) AS content_length
FROM dq_source_files
ORDER BY file_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Extract DQ rules using ai_query()
-- MAGIC Each file is sent to the LLM with a structured prompt. The model returns
-- MAGIC a JSON array of extracted rules. We use `temperature = 0` for deterministic output.

-- COMMAND ----------

CREATE OR REPLACE TABLE dq_extracted_rules_raw AS
SELECT
  file_name,
  file_type,
  file_path,
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    CONCAT(
      'You are a data quality analyst. Analyze the following code file and extract every data quality rule, ',
      'expectation, constraint, check, and validation you can find.\n\n',
      'For each rule, return a JSON object with these exact fields:\n',
      '- "rule_name": a concise, snake_case name for the rule (e.g., "patient_id_not_null")\n',
      '- "rule_description": one-sentence explanation of what the rule checks\n',
      '- "rule_expression": the actual SQL expression, constraint, or check logic as written in the code\n',
      '- "rule_type": one of [null_check, unique_check, range_check, pattern_check, referential_check, ',
      'set_membership_check, consistency_check, freshness_check, custom_check]\n',
      '- "dimension": one of [completeness, accuracy, validity, consistency, uniqueness, timeliness]\n',
      '- "target_table": the table being validated (if identifiable, otherwise null)\n',
      '- "target_column": the specific column (if identifiable, otherwise null)\n',
      '- "severity": one of [critical, error, warning, info] based on the code context ',
      '(e.g., expect_or_fail = critical, expect_or_drop = error, expect = warning)\n\n',
      'Return ONLY a valid JSON array. No markdown fences, no explanation, no preamble.\n',
      'If no rules are found, return an empty array: []\n\n',
      '--- FILE: ', file_name, ' ---\n',
      file_content
    )
  ) AS extracted_json
FROM dq_source_files;

-- COMMAND ----------

-- Preview the raw LLM output
SELECT file_name, extracted_json FROM dq_extracted_rules_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Parse the JSON array into structured rows
-- MAGIC Flatten the JSON array returned by the LLM into individual rule rows.

-- COMMAND ----------

CREATE OR REPLACE TABLE dq_extracted_rules AS
WITH parsed AS (
  SELECT
    file_name,
    file_type,
    file_path,
    explode(
      from_json(
        extracted_json,
        'ARRAY<STRUCT<
          rule_name: STRING,
          rule_description: STRING,
          rule_expression: STRING,
          rule_type: STRING,
          dimension: STRING,
          target_table: STRING,
          target_column: STRING,
          severity: STRING
        >>'
      )
    ) AS rule
  FROM dq_extracted_rules_raw
)
SELECT
  monotonically_increasing_id()       AS extraction_id,
  rule.rule_name,
  rule.rule_description,
  rule.rule_expression,
  rule.rule_type,
  rule.dimension,
  rule.target_table,
  rule.target_column,
  rule.severity,
  file_name                            AS source_file,
  file_type                            AS source_type,
  file_path                            AS source_path,
  current_timestamp()                  AS extracted_at
FROM parsed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Review extracted rules

-- COMMAND ----------

SELECT * FROM dq_extracted_rules ORDER BY source_file, rule_name;

-- COMMAND ----------

-- Summary by source file
SELECT
  source_file,
  source_type,
  count(*)                                              AS total_rules,
  count_if(dimension = 'completeness')                  AS completeness,
  count_if(dimension = 'accuracy')                      AS accuracy,
  count_if(dimension = 'validity')                      AS validity,
  count_if(dimension = 'consistency')                   AS consistency,
  count_if(dimension = 'uniqueness')                    AS uniqueness,
  count_if(dimension = 'timeliness')                    AS timeliness
FROM dq_extracted_rules
GROUP BY source_file, source_type
ORDER BY source_file;

-- COMMAND ----------

-- Summary by rule type
SELECT
  rule_type,
  count(*)           AS rule_count,
  collect_set(source_file) AS found_in_files
FROM dq_extracted_rules
GROUP BY rule_type
ORDER BY rule_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Validate extraction quality
-- MAGIC Use a second LLM pass to verify the extracted rules against the original source.
-- MAGIC This catches hallucinated rules that don't actually exist in the code.

-- COMMAND ----------

CREATE OR REPLACE TABLE dq_extraction_validation AS
SELECT
  r.extraction_id,
  r.rule_name,
  r.source_file,
  r.rule_expression,
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    CONCAT(
      'You are a code reviewer. I will give you a data quality rule that was extracted from a code file. ',
      'Verify that this rule ACTUALLY EXISTS in the code. Do not infer or assume — the rule must be ',
      'explicitly present.\n\n',
      'Rule name: ', r.rule_name, '\n',
      'Rule expression: ', r.rule_expression, '\n',
      'Rule type: ', r.rule_type, '\n',
      'Target table: ', coalesce(r.target_table, 'unknown'), '\n',
      'Target column: ', coalesce(r.target_column, 'unknown'), '\n\n',
      'Source code:\n', s.file_content, '\n\n',
      'Respond with ONLY a JSON object with these fields:\n',
      '- "verified": true if the rule exists in the code, false if it was hallucinated\n',
      '- "confidence": a number from 0.0 to 1.0\n',
      '- "reason": one sentence explanation\n',
      'Return ONLY valid JSON. No markdown fences.'
    )
  ) AS validation_json
FROM dq_extracted_rules r
JOIN dq_source_files s ON r.source_file = s.file_name;

-- COMMAND ----------

-- Show validation results
SELECT
  e.extraction_id,
  e.rule_name,
  e.source_file,
  v.validation_result.verified,
  v.validation_result.confidence,
  v.validation_result.reason
FROM dq_extracted_rules e
JOIN (
  SELECT
    extraction_id,
    from_json(
      validation_json,
      'STRUCT<verified: BOOLEAN, confidence: DOUBLE, reason: STRING>'
    ) AS validation_result
  FROM dq_extraction_validation
) v ON e.extraction_id = v.extraction_id
ORDER BY v.validation_result.verified, e.source_file;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 6: Write verified rules to the final table
-- MAGIC Only keep rules that passed validation (verified = true, confidence >= 0.8).

-- COMMAND ----------

CREATE OR REPLACE TABLE dq_extracted_rules_verified AS
SELECT
  e.*,
  v.validation_result.verified,
  v.validation_result.confidence,
  v.validation_result.reason AS validation_reason
FROM dq_extracted_rules e
JOIN (
  SELECT
    extraction_id,
    from_json(
      validation_json,
      'STRUCT<verified: BOOLEAN, confidence: DOUBLE, reason: STRING>'
    ) AS validation_result
  FROM dq_extraction_validation
) v ON e.extraction_id = v.extraction_id
WHERE v.validation_result.verified = true
  AND v.validation_result.confidence >= 0.8;

-- COMMAND ----------

-- Final verified count vs total extracted
SELECT
  (SELECT count(*) FROM dq_extracted_rules)          AS total_extracted,
  (SELECT count(*) FROM dq_extracted_rules_verified) AS verified_rules,
  round(
    (SELECT count(*) FROM dq_extracted_rules_verified) * 100.0 /
    (SELECT count(*) FROM dq_extracted_rules), 1
  )                                                   AS verification_pct;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 7 (Optional): Load verified rules into the dq_rule table
-- MAGIC Maps extracted rules into the existing `dq_rule` schema used by the dashboard.

-- COMMAND ----------

-- Map dimensions to dimension_id (from the quality_dimension reference table)
-- Map rule_type strings to category_id (from dq_rule_category reference table)

INSERT INTO dq_rule (
  rule_id, dimension_id, category_id, rule_name, rule_description,
  rule_type, default_severity_weight, rule_sql, rule_status,
  version, is_system_rule, created_by, created_at
)
SELECT
  extraction_id + 10000                                  AS rule_id,
  CASE dimension
    WHEN 'completeness' THEN 1
    WHEN 'accuracy'     THEN 2
    WHEN 'validity'     THEN 3
    WHEN 'consistency'  THEN 4
    WHEN 'uniqueness'   THEN 5
    WHEN 'timeliness'   THEN 6
    ELSE 3
  END                                                     AS dimension_id,
  CASE rule_type
    WHEN 'null_check'           THEN 1
    WHEN 'unique_check'         THEN 2
    WHEN 'range_check'          THEN 3
    WHEN 'pattern_check'        THEN 4
    WHEN 'referential_check'    THEN 5
    WHEN 'set_membership_check' THEN 6
    WHEN 'consistency_check'    THEN 7
    WHEN 'freshness_check'      THEN 8
    WHEN 'custom_check'         THEN 9
    ELSE 9
  END                                                     AS category_id,
  rule_name,
  rule_description,
  rule_type,
  CASE severity
    WHEN 'critical' THEN 3.0
    WHEN 'error'    THEN 2.0
    WHEN 'warning'  THEN 1.0
    WHEN 'info'     THEN 0.5
    ELSE 1.0
  END                                                     AS default_severity_weight,
  rule_expression                                         AS rule_sql,
  'draft'                                                 AS rule_status,
  1                                                       AS version,
  false                                                   AS is_system_rule,
  'ai_extraction'                                         AS created_by,
  current_timestamp()                                     AS created_at
FROM dq_extracted_rules_verified;

-- COMMAND ----------

-- Verify the inserted rules
SELECT rule_id, rule_name, rule_type, rule_status, created_by
FROM dq_rule
WHERE created_by = 'ai_extraction'
ORDER BY rule_id;
