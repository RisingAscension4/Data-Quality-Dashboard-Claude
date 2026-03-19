-- =============================================================
-- Claims Data Quality Validation Queries
-- Run daily after the claims ETL job completes
-- =============================================================

-- 1. Completeness: Ensure no claims are missing required fields
CREATE OR REPLACE TABLE gold_claims.dq_completeness_check AS
SELECT
    claim_id,
    CASE WHEN member_id IS NULL THEN 'FAIL' ELSE 'PASS' END AS member_id_check,
    CASE WHEN provider_npi IS NULL THEN 'FAIL' ELSE 'PASS' END AS provider_npi_check,
    CASE WHEN service_date IS NULL THEN 'FAIL' ELSE 'PASS' END AS service_date_check,
    CASE WHEN diagnosis_code IS NULL THEN 'FAIL' ELSE 'PASS' END AS primary_dx_check,
    CASE WHEN billed_amount IS NULL THEN 'FAIL' ELSE 'PASS' END AS billed_amount_check,
    CASE WHEN claim_type IS NULL THEN 'FAIL' ELSE 'PASS' END AS claim_type_check
FROM silver_claims.claims
WHERE processing_date = current_date();


-- 2. Validity: Diagnosis codes must be valid ICD-10 format
CREATE OR REPLACE VIEW gold_claims.dq_icd10_validation AS
SELECT
    claim_id,
    diagnosis_code,
    CASE
        WHEN diagnosis_code RLIKE '^[A-Z][0-9]{2}(\\.[0-9A-Z]{1,4})?$' THEN 'VALID'
        ELSE 'INVALID'
    END AS icd10_format_check
FROM silver_claims.claims
WHERE diagnosis_code IS NOT NULL;


-- 3. Consistency: Billed amount should not exceed allowed amount
ALTER TABLE silver_claims.claims
ADD CONSTRAINT chk_billed_vs_allowed
CHECK (billed_amount >= allowed_amount OR allowed_amount IS NULL);


-- 4. Range check: Billed amounts must be positive and within reasonable bounds
ALTER TABLE silver_claims.claims
ADD CONSTRAINT chk_billed_amount_range
CHECK (billed_amount > 0 AND billed_amount < 10000000);


-- 5. Referential integrity: Provider NPI must exist in provider master
CREATE OR REPLACE VIEW gold_claims.dq_orphan_providers AS
SELECT c.claim_id, c.provider_npi
FROM silver_claims.claims c
LEFT JOIN reference_data.providers p ON c.provider_npi = p.npi
WHERE p.npi IS NULL
  AND c.provider_npi IS NOT NULL;


-- 6. Timeliness: Claims should be processed within 30 days of service
CREATE OR REPLACE VIEW gold_claims.dq_timeliness_check AS
SELECT
    claim_id,
    service_date,
    processing_date,
    datediff(processing_date, service_date) AS days_to_process,
    CASE
        WHEN datediff(processing_date, service_date) > 30 THEN 'LATE'
        WHEN datediff(processing_date, service_date) < 0 THEN 'INVALID'
        ELSE 'ON_TIME'
    END AS timeliness_status
FROM silver_claims.claims;


-- 7. Uniqueness: No duplicate claim submissions
CREATE OR REPLACE VIEW gold_claims.dq_duplicate_claims AS
SELECT
    member_id,
    provider_npi,
    service_date,
    diagnosis_code,
    billed_amount,
    COUNT(*) AS occurrence_count
FROM silver_claims.claims
GROUP BY member_id, provider_npi, service_date, diagnosis_code, billed_amount
HAVING COUNT(*) > 1;


-- 8. Accuracy: CPT codes must exist in the reference table
SELECT c.claim_id, c.procedure_code
FROM silver_claims.claims c
LEFT JOIN reference_data.cpt_codes ref ON c.procedure_code = ref.cpt_code
WHERE ref.cpt_code IS NULL
  AND c.procedure_code IS NOT NULL;
