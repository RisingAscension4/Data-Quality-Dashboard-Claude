# Radiology Revenue Cycle: Data Model Design
## "Two Reports, One Truth" — Pinnacle Radiology Partners

---

## The Executive Moment

The CEO of **Pinnacle Radiology Partners** opens her Monday board deck and sees:

| KPI | CFO Monthly Summary | Revenue Ops Dashboard |
|-----|--------------------|-----------------------|
| Net Collection Rate | **94.2%** | **81.7%** |
| Denial Rate | **6.1%** | **11.3%** |
| Days in AR | **42** | **51** |
| Commercial Payer Mix | **52.1%** | **38.4%** |
| Total Charges (YTD) | **$47.3M** | **$49.4M** |

*"Which one is right?"* — That question launches the demo.

---

## Why the Reports Diverge — The Core Thesis

**Neither report is wrong. The data is.**

Both teams apply reasonable, defensible business logic. Report A (Finance) takes a conservative stance: *"Only count what we can validate."* Report B (Revenue Ops) takes an inclusive stance: *"Count everything until proven invalid."* These are legitimate philosophical differences — the kind every organization has between teams with different risk tolerances.

**But here's the key insight: if the underlying data were clean, both philosophies would produce the same numbers.**

- If `allowed_amount` were never NULL on paid claims → both formulas compute the same denominator
- If all CARC codes were valid → filtering invalid codes is a no-op
- If AR snapshots were current everywhere → no locations get silently excluded
- If payer names were canonical → grouping by name = joining by ID
- If there were no unflagged rebills → deduplication finds nothing to remove
- If allowed amounts reflected actual contracts → no reconciliation needed

**The divergence is a symptom. It measures how much defective data exists in the space between two reasonable interpretations.**

This is what makes data quality a business problem, not a technical one. The fix isn't to pick the "better" report or force one team to adopt the other's logic. The fix is to resolve the underlying data defects so that assumptions become unnecessary — and both reports converge to the same truth.

**The conversation starter**: "Your reports don't disagree because your teams are incompetent. They disagree because your data is ambiguous — and ambiguity forces assumptions. Different assumptions produce different numbers. Let's eliminate the ambiguity."

---

## Provenance Trail — What Trusted Self-Service Looks Like

When a self-service analytics platform delivers an answer, **provenance** is what converts that answer into a *trusted* answer. A provenance trail means the consumer can see exactly how an answer was derived — not just the number, but the full chain of evidence behind it.

### What a Provenance Trail Contains

1. **Source Tables** — "This answer came from `claim_header`, `payment`, and `payer` tables"
2. **Business Logic** — "Net Collection Rate = SUM(payments) ÷ SUM(allowed) where allowed IS NOT NULL"
3. **Metric Definitions** — "Using the Finance-approved 'Net Collection Rate' metric view, version 3, last updated April 2026"
4. **Data Freshness** — "Source tables last updated today at 6:00 AM via pipeline run #4521"
5. **Exclusions & Flags** — "238 claims excluded due to NULL allowed_amount (8% exclusion rate)"

### Why It Matters

Without provenance, self-service analytics produces fast responses that nobody acts on — because they can't verify the reasoning. The difference is:

- **Without**: "Your collection rate is 83%" → *black box — do you trust it?*
- **With**: "Your collection rate is 83%, derived from [these tables] using [this logic] with [this freshness] and [these exclusions]" → *transparent — you can verify it*

### How It Maps to Databricks

| Provenance Component | Databricks Capability |
|---------------------|----------------------|
| Source Tables | Unity Catalog lineage (table + column level) |
| Business Logic | Metric view definitions (standardized formulas) |
| Metric Definitions | Unity Catalog metric views with versioning |
| Data Freshness | Pipeline run metadata, table `TBLPROPERTIES` timestamps |
| Exclusions & Flags | Genie trust metrics, DQ check results surfaced at query time |

### The Three-Layer Trust Model

Self-service analytics requires three layers working together:

| Layer | What It Does | Without It |
|-------|-------------|-----------|
| **Self-Service Analytics** | Anyone can ask questions of the data directly | 3–4 week wait for every answer |
| **Data Quality Foundation** | Ensures answers are correct before they're served | Fast access to wrong answers |
| **Trust Metrics (Genie)** | Communicates confidence so people act on answers | A tool nobody adopts |

- Self-service without quality = faster wrong answers
- Quality without self-service = correct answers nobody can access
- Both without trust metrics = a tool nobody adopts
- **All three together = an organization that makes decisions at the speed of questions**

---

## Organization Context

**Pinnacle Radiology Partners** — A 12-location outpatient radiology group:
- 8 freestanding imaging centers
- 3 hospital-based reading groups  
- 1 mobile MRI unit
- ~50 radiologists, 200 technologists
- Modalities: MRI, CT, X-ray, Ultrasound, Mammography, Nuclear Medicine, Interventional

Annual revenue: ~$180M | ~400K studies/year | 12 contracted payers

---

## Data Model

### Entity Relationship Diagram (Logical)

```
┌─────────────┐       ┌──────────────────┐       ┌─────────────────┐
│   patient    │       │    encounter     │       │    provider     │
│─────────────│       │──────────────────│       │─────────────────│
│ patient_id  │◄──────│ patient_id       │──────►│ provider_npi    │
│ mrn         │       │ encounter_id     │       │ provider_name   │
│ dob         │       │ provider_npi     │       │ specialty       │
│ sex         │       │ location_id      │       │ credential      │
│ zip_code    │       │ study_date       │       │ group_affil     │
│ insurance_id│       │ modality         │       └─────────────────┘
└─────────────┘       │ accession_number │
                      │ order_status     │       ┌─────────────────┐
                      └────────┬─────────┘       │service_location │
                               │                 │─────────────────│
                               ▼                 │ location_id     │
┌──────────────────────────────────────┐         │ location_name   │
│            claim_header              │────────►│ location_type   │
│──────────────────────────────────────│         │ address/state   │
│ claim_id          (PK)               │         │ pos_code        │
│ encounter_id      (FK)               │         └─────────────────┘
│ patient_id        (FK)               │
│ payer_id          (FK)               │         ┌─────────────────┐
│ payer_name        (denormalized!)    │         │      payer      │
│ rendering_npi                        │────────►│─────────────────│
│ billing_npi                          │         │ payer_id        │
│ location_id       (FK)               │         │ payer_name      │
│ claim_type (professional/facility)   │         │ payer_category  │
│ filing_date                          │         │ contract_type   │
│ total_charge_amount                  │         │ timely_filing   │
│ total_allowed_amount                 │         └─────────────────┘
│ total_paid_amount                    │
│ claim_status                         │
│ adjudication_date                    │
│ is_rebill         (flag)             │
│ original_claim_id (self-ref for rebills)│
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│             claim_line               │
│──────────────────────────────────────│
│ claim_line_id     (PK)               │
│ claim_id          (FK)               │
│ line_number                          │
│ cpt_code                             │
│ modifier_1, modifier_2               │
│ icd10_code                           │
│ units                                │
│ charge_amount                        │
│ allowed_amount                       │
│ paid_amount                          │
│ adjustment_amount                    │
│ adjustment_reason_code (CARC)        │
│ remark_code (RARC)                   │
│ place_of_service                     │
│ service_date                         │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│              denial                  │
│──────────────────────────────────────│
│ denial_id         (PK)               │
│ claim_id          (FK)               │
│ claim_line_id     (FK, nullable)     │
│ denial_date                          │
│ carc_code                            │
│ rarc_code                            │
│ denial_category                      │
│ denied_amount                        │
│ appeal_status                        │
│ appeal_date                          │
│ overturn_date                        │
│ overturn_amount                      │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│           ar_aging_snapshot          │
│──────────────────────────────────────│
│ snapshot_date     (PK part)          │
│ payer_id          (PK part)          │
│ location_id       (PK part)          │
│ aging_bucket                         │
│   (current, 31-60, 61-90, 91-120, 121+)│
│ claim_count                          │
│ outstanding_amount                   │
│ snapshot_source   (system A / system B)│
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│             payment                  │
│──────────────────────────────────────│
│ payment_id        (PK)               │
│ claim_id          (FK)               │
│ claim_line_id     (FK)               │
│ payer_id                             │
│ payment_date                         │
│ payment_amount                       │
│ adjustment_amount                    │
│ adjustment_code                      │
│ check_number                         │
│ era_id                               │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│          authorization               │
│──────────────────────────────────────│
│ auth_id           (PK)               │
│ patient_id        (FK)               │
│ payer_id          (FK)               │
│ cpt_code                             │
│ auth_number                          │
│ auth_status                          │
│ requested_date                       │
│ approved_date                        │
│ expiration_date                      │
│ authorized_units                     │
└──────────────────────────────────────┘
```

---

## The Eight Trust Traps

Each defect maps to one DQ dimension and produces a **specific, measurable divergence** between the two reports. The ACCURACY dimension includes three distinct sub-defects that compound.

---

### 1. COMPLETENESS — The Net Collections Killer

**Defect**: ~8% of paid claims have `NULL` for `allowed_amount` at the claim_line level. These are real paid claims (payment records exist in the `payment` table) but the ERA posted without an allowed amount — common with auto-adjudicated Medicare claims and some Medicaid plans.

**How it diverges**:
- **CFO Report** (Report A): Excludes rows where `allowed_amount IS NULL` from the net collections calculation. Formula: `SUM(paid_amount) / SUM(allowed_amount) WHERE allowed_amount IS NOT NULL` → **94.2%**
- **Revenue Ops Report** (Report B): Includes all paid claims, substituting `billed_amount` when allowed is NULL. Formula: `SUM(paid_amount) / SUM(COALESCE(allowed_amount, charge_amount))` → **81.7%**

**Why it's realistic**: Medicare Advantage plans and state Medicaid often post payments before the full ERA with allowed amounts arrives. The 835 file may come days later. Teams disagree on whether to wait or impute.

---

### 2. VALIDITY — The Phantom Denials

**Defect**: ~7% of denial records use CARC codes that don't exist in the X12 835 standard code set (e.g., "CO-999", "PI-300"). These were entered manually by a legacy billing system during a migration and never validated.

**How it diverges**:
- **CFO Report** (Report A): Drops any denial with an invalid CARC code (treating them as data entry errors). Denial rate: `valid_denials / total_claims` → **6.1%**
- **Revenue Ops Report** (Report B): Categorizes unknown CARC codes as "Unclassified Denials" and includes them in the denial count. Denial rate: `all_denials / total_claims` → **11.3%**

**Why it's realistic**: When practices merge or migrate billing systems, legacy denial codes often don't map cleanly. One team "cleans" them out; another treats them as actionable denials requiring follow-up.

**Invalid CARC codes to plant**:
| Code | What it looks like | Reality |
|------|-------------------|---------|
| CO-999 | Looks like a contractual obligation | Doesn't exist in X12 |
| PR-ZZZ | Looks like patient responsibility | Legacy placeholder |
| OA-500 | Looks like "other adjustment" | Fabricated during migration |
| PI-300 | Looks like "payer initiated" | Old system's internal code |

---

### 3. CONSISTENCY — The Payer Identity Crisis

**Defect**: `payer_name` on `claim_header` is a free-text denormalized field that doesn't match the canonical `payer.payer_name`. The same payer appears under 4-6 variants because of different source systems, manual entry, and legacy migrations.

**Variants planted**:
| payer_id | Canonical Name | Variants in claim_header.payer_name |
|----------|---------------|-------------------------------------|
| PAY-001 | Blue Cross Blue Shield of Illinois | "BCBS", "Blue Cross BS", "BCBS IL", "Blue Cross Blue Shield", "BlueCross BlueShield of IL" |
| PAY-002 | Aetna | "AETNA", "Aetna Inc", "AETNA HEALTH", "CVS/Aetna" |
| PAY-003 | UnitedHealthcare | "UHC", "United Healthcare", "UnitedHealth", "United HC", "UNITED HEALTHCARE" |
| PAY-004 | Medicare Part B | "MEDICARE", "Medicare B", "CMS Medicare", "MEDICARE PART B" |

**How it diverges**:
- **CFO Report** (Report A): Joins to the canonical `payer` table on `payer_id` and uses `payer.payer_category` for payer mix. Commercial = **52.1%**
- **Revenue Ops Report** (Report B): Groups by `claim_header.payer_name` directly (no join to payer ref table). Because variants fragment the categories, Commercial appears smaller at **38.4%** (with chunks showing up as "Other/Unmatched")

**Why it's realistic**: Every RCM shop has this problem. Payer names come from enrollment feeds, clearinghouse responses, and manual entry. Nobody trusts payer_name; everybody uses it anyway.

---

### 4. UNIQUENESS — The Rebill Ghost

**Defect**: When a claim is corrected and re-filed, it generates a new `claim_id` but the original is NOT voided in one of the two source paths. ~3% of claims are duplicated — same patient, same service_date, same CPT, same charge, but two different claim_ids (original + rebill).

The `is_rebill` flag and `original_claim_id` exist on the table but were only populated starting in March 2025. Earlier rebills have `is_rebill = false` and `original_claim_id = NULL`.

**How it diverges**:
- **CFO Report** (Report A): Uses a deduplication step that identifies rebills by matching on (patient_id, service_date, cpt_code, charge_amount) and keeps only the latest claim_id. Total charges: **$47.3M**
- **Revenue Ops Report** (Report B): Trusts `claim_id` as unique (no dedup logic). Counts both original and rebill. Total charges: **$49.4M** (+$2.1M phantom revenue)

**Why it's realistic**: Rebill workflows are the #1 source of claim duplication in radiology. Corrected claims, split bills, and crossover claims all create legitimate duplicate-looking records. The business rule for "which is the real claim" varies by team.

---

### 5. TIMELINESS — The Stale Snapshot

**Defect**: The `ar_aging_snapshot` table is populated by two source systems:
- **System A** (hospital-based locations): Last refreshed **May 6, 2026** (9 days stale)
- **System B** (freestanding imaging centers): Refreshed nightly (current as of today)

Both load into the same `ar_aging_snapshot` table. The `snapshot_source` column distinguishes them, but reports don't filter on it.

**How it diverges**:
- **CFO Report** (Report A): Uses `MAX(snapshot_date)` per location and only includes the latest. Since System A is stale, those locations show 9-day-old AR. Blended Days in AR: **42 days**
- **Revenue Ops Report** (Report B): Uses `snapshot_date = CURRENT_DATE` filter. System A locations return NO data for today (stale), so they're excluded entirely — the remaining locations (freestanding centers) have higher AR because they're smaller and slower-paying. Days in AR: **51 days**

**Why it's realistic**: Health systems almost always have mixed-refresh data. Hospital PMS/EHR feeds run on different schedules than billing vendor feeds. Nobody notices until the numbers don't match.

---

### 6. ACCURACY (A) — TC/26 Component Double-Count

**Defect**: ~6% of claims have a component billing conflict. The claim was filed with a **global** charge line (no modifier — meaning TC+26 combined) but the legacy system ALSO created separate Technical Component (TC) and Professional Component (26) lines on the same claim for the same CPT. This effectively triple-counts revenue on those claims.

**How it diverges**:
- **CFO Report** (Report A): Detects the modifier pattern conflict per claim (if a CPT appears with no modifier AND with modifier 26 or TC on the same claim, keep only the global line). Correctly de-duplicates. Total line charges: **$47.3M**
- **Revenue Ops Report** (Report B): Sums all claim lines naively. The triple-counted lines inflate charges by ~$2.8M.

**Why it's realistic**: Component billing errors are the #1 audit finding in radiology. They occur when hospital-based (facility bills TC) and professional (group bills 26) feeds merge into one warehouse during an acquisition, or when a PMS migration maps global charges but also carries over the split lines.

---

### 7. ACCURACY (B) — Allowed Amount Carries Billed for Certain Payers

**Defect**: For 2 payers (Medicare Advantage - Humana / PAY-011 and Molina Healthcare / PAY-012), the clearinghouse 835 feed incorrectly maps `charge_amount` into the `allowed_amount` field. So `allowed_amount = charge_amount` instead of the actual contracted rate (which is typically 40-60% of billed).

This makes net collections look artificially low for those payers since `paid / allowed` becomes `paid / billed` ≈ 40-60%.

**How it diverges**:
- **CFO Report** (Report A): Uses a payer-specific correction — for PAY-011 and PAY-012, substitutes allowed_amount with the actual payment amount from the `payment` table.
- **Revenue Ops Report** (Report B): Trusts `allowed_amount` as-is from claim_line, depressing net collections for these payers and skewing the overall rate downward.

**Why it's realistic**: Clearinghouse mapping errors are extremely common. Each payer's 835 file has slightly different field placement. The translation layer regularly gets it wrong for smaller or newer payer feeds. Teams usually discover it months later when a payer's net collection rate looks impossibly low.

---

### 8. ACCURACY (C) — Stale Contracted Rates After Mid-Year Renegotiation

**Defect**: BCBS (PAY-001, the largest commercial payer at 15% of volume) renegotiated their radiology contract effective January 1, 2026 with a 12% rate increase. Claims adjudicated in Q1 2026 still carry the **old** contracted rate in `allowed_amount` because the fee schedule update wasn't loaded into the practice management system until March 2026. The `payment` table shows the correct (higher) amounts — BCBS actually paid at the new rate.

So for ~2,800 BCBS claims filed Jan-Feb 2026: `claim_line.allowed_amount` is 12% lower than `payment.payment_amount`.

**How it diverges**:
- **CFO Report** (Report A): Reconciles against the payment table for revenue recognition. Uses actual paid amounts where they exceed allowed.
- **Revenue Ops Report** (Report B): Uses `claim_line.allowed_amount` as the contracted benchmark. Understates BCBS revenue by ~$1.2M.

**Why it's realistic**: Fee schedule lag is universal. Contract updates happen on paper months before they're loaded into the billing system. The data warehouse captures what was in the PMS at adjudication time, not the actual contractual rate.

---

## Data Quality Rules — Detection & Checks

Each trust trap maps to one or more DQ rules that the Data Quality Dashboard would flag. This section defines what each check looks like in SQL.

---

### Rule 1: Completeness — allowed_amount NULL on paid claims

| Attribute | Value |
|-----------|-------|
| **Table** | `claim_line` |
| **Column** | `allowed_amount` |
| **Dimension** | Completeness |
| **Rule Name** | `paid_claim_line_allowed_not_null` |
| **Severity** | Critical |
| **Business Impact** | Net Collections Rate diverges by 12.5 percentage points |

**Check SQL**:
```sql
SELECT 
  COUNT(*) AS total_paid_lines,
  SUM(CASE WHEN cl.allowed_amount IS NULL THEN 1 ELSE 0 END) AS failing_rows,
  ROUND(100.0 * SUM(CASE WHEN cl.allowed_amount IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_pct
FROM claim_line cl
JOIN claim_header ch ON cl.claim_id = ch.claim_id
WHERE ch.claim_status = 'paid'
```

**Expected**: `failure_pct` should be 0%. **Actual**: ~8%.

---

### Rule 2: Validity — CARC codes exist in reference code set

| Attribute | Value |
|-----------|-------|
| **Table** | `denial` |
| **Column** | `carc_code` |
| **Dimension** | Validity |
| **Rule Name** | `denial_carc_code_valid` |
| **Severity** | High |
| **Business Impact** | Denial Rate diverges by 5.2 percentage points |

**Check SQL**:
```sql
-- Assuming a reference table of valid CARC codes (or a hardcoded list)
WITH valid_carcs AS (
  SELECT explode(array(
    'CO-4','CO-16','CO-45','CO-50','CO-197','PR-1','PR-2','PR-96','OA-23','CO-42'
  )) AS valid_code
)
SELECT 
  COUNT(*) AS total_denials,
  SUM(CASE WHEN vc.valid_code IS NULL THEN 1 ELSE 0 END) AS invalid_carc_count,
  COLLECT_SET(CASE WHEN vc.valid_code IS NULL THEN d.carc_code END) AS invalid_codes_found
FROM denial d
LEFT JOIN valid_carcs vc ON d.carc_code = vc.valid_code
```

**Expected**: `invalid_carc_count` = 0. **Actual**: ~7% of denials use non-existent codes.

---

### Rule 3: Consistency — payer_name matches canonical reference

| Attribute | Value |
|-----------|-------|
| **Table** | `claim_header` |
| **Column** | `payer_name` |
| **Dimension** | Consistency |
| **Rule Name** | `claim_payer_name_matches_reference` |
| **Severity** | Medium |
| **Business Impact** | Commercial Payer Mix diverges by 13.7 percentage points |

**Check SQL**:
```sql
SELECT 
  COUNT(*) AS total_claims,
  SUM(CASE WHEN ch.payer_name != p.payer_name THEN 1 ELSE 0 END) AS mismatched_names,
  COUNT(DISTINCT ch.payer_name) AS unique_variants_in_claims,
  COUNT(DISTINCT p.payer_name) AS canonical_payer_count
FROM claim_header ch
JOIN payer p ON ch.payer_id = p.payer_id
```

**Expected**: `mismatched_names` = 0 (all names match canonical). **Actual**: Nearly 100% mismatch because variants are used everywhere.

---

### Rule 4: Uniqueness — No duplicate claims for same service

| Attribute | Value |
|-----------|-------|
| **Table** | `claim_header` + `claim_line` |
| **Column** | Composite: `patient_id`, `service_date`, `cpt_code`, `charge_amount` |
| **Dimension** | Uniqueness |
| **Rule Name** | `claim_no_duplicate_service` |
| **Severity** | Critical |
| **Business Impact** | Total Charges diverges by $2.1M |

**Check SQL**:
```sql
SELECT 
  COUNT(*) AS duplicate_groups,
  SUM(duplicate_count - 1) AS excess_claims
FROM (
  SELECT 
    ch.patient_id, cl.service_date, cl.cpt_code, cl.charge_amount,
    COUNT(DISTINCT ch.claim_id) AS duplicate_count
  FROM claim_header ch
  JOIN claim_line cl ON ch.claim_id = cl.claim_id
  WHERE ch.claim_status NOT IN ('void')
  GROUP BY ch.patient_id, cl.service_date, cl.cpt_code, cl.charge_amount
  HAVING COUNT(DISTINCT ch.claim_id) > 1
)
```

**Expected**: `duplicate_groups` = 0. **Actual**: ~3% of pre-March-2025 paid claims have undetected duplicates.

---

### Rule 5: Timeliness — AR snapshot freshness within SLA

| Attribute | Value |
|-----------|-------|
| **Table** | `ar_aging_snapshot` |
| **Column** | `snapshot_date` |
| **Dimension** | Timeliness |
| **Rule Name** | `ar_snapshot_within_24h` |
| **Severity** | High |
| **Business Impact** | Days in AR diverges by 9 days |

**Check SQL**:
```sql
SELECT 
  snapshot_source,
  MAX(snapshot_date) AS latest_snapshot,
  DATEDIFF(CURRENT_DATE(), MAX(snapshot_date)) AS days_stale,
  CASE 
    WHEN DATEDIFF(CURRENT_DATE(), MAX(snapshot_date)) > 1 THEN 'FAIL'
    ELSE 'PASS'
  END AS rule_result
FROM ar_aging_snapshot
GROUP BY snapshot_source
```

**Expected**: `days_stale` ≤ 1 for all sources. **Actual**: system_a is 9 days stale.

---

### Rule 6: Accuracy (A) — No TC/26 component overlap with global charges

| Attribute | Value |
|-----------|-------|
| **Table** | `claim_line` |
| **Column** | Composite: `cpt_code`, `modifier_1` per `claim_id` |
| **Dimension** | Accuracy |
| **Rule Name** | `claim_line_no_component_overlap` |
| **Severity** | Critical |
| **Business Impact** | Overstates line charges by ~$2.8M |

**Check SQL**:
```sql
-- Find claims where the same CPT appears with no modifier (global) 
-- AND with modifier 26 or TC (component) on the same claim
SELECT 
  COUNT(DISTINCT cl_global.claim_id) AS claims_with_overlap,
  SUM(cl_global.charge_amount) AS double_counted_dollars
FROM claim_line cl_global
JOIN claim_line cl_component 
  ON cl_global.claim_id = cl_component.claim_id
  AND cl_global.cpt_code = cl_component.cpt_code
  AND cl_global.claim_line_id != cl_component.claim_line_id
WHERE cl_global.modifier_1 IS NULL  -- Global line (no modifier)
  AND cl_component.modifier_1 IN ('26', 'TC')  -- Component line
```

**Expected**: `claims_with_overlap` = 0. **Actual**: ~6% of claims have this billing conflict.

---

### Rule 7: Accuracy (B) — allowed_amount should not equal charge_amount

| Attribute | Value |
|-----------|-------|
| **Table** | `claim_line` |
| **Column** | `allowed_amount` vs `charge_amount` |
| **Dimension** | Accuracy |
| **Rule Name** | `allowed_not_equal_to_billed` |
| **Severity** | High |
| **Business Impact** | Depresses net collections for affected payers by 20-40pp |

**Check SQL**:
```sql
SELECT 
  ch.payer_id,
  p.payer_name,
  COUNT(*) AS total_lines,
  SUM(CASE WHEN cl.allowed_amount = cl.charge_amount THEN 1 ELSE 0 END) AS allowed_equals_billed,
  ROUND(100.0 * SUM(CASE WHEN cl.allowed_amount = cl.charge_amount THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_suspicious
FROM claim_line cl
JOIN claim_header ch ON cl.claim_id = ch.claim_id
JOIN payer p ON ch.payer_id = p.payer_id
WHERE cl.allowed_amount IS NOT NULL
GROUP BY ch.payer_id, p.payer_name
HAVING SUM(CASE WHEN cl.allowed_amount = cl.charge_amount THEN 1 ELSE 0 END) > 0
ORDER BY pct_suspicious DESC
```

**Expected**: Contracted payers should NEVER have allowed = billed (contractual discount always applies). **Actual**: PAY-011 and PAY-012 show ~100% match due to clearinghouse mapping error.

---

### Rule 8: Accuracy (C) — payment_amount should not systematically exceed allowed_amount

| Attribute | Value |
|-----------|-------|
| **Table** | `claim_line` + `payment` |
| **Column** | `allowed_amount` vs `payment.payment_amount` |
| **Dimension** | Accuracy |
| **Rule Name** | `payment_not_exceeding_allowed` |
| **Severity** | Medium |
| **Business Impact** | Understates BCBS revenue by ~$1.2M |

**Check SQL**:
```sql
SELECT 
  ch.payer_id,
  p.payer_name,
  COUNT(*) AS lines_checked,
  SUM(CASE WHEN pmt.payment_amount > cl.allowed_amount * 1.05 THEN 1 ELSE 0 END) AS payment_exceeds_allowed,
  ROUND(AVG(CASE 
    WHEN pmt.payment_amount > cl.allowed_amount * 1.05 
    THEN (pmt.payment_amount - cl.allowed_amount) / cl.allowed_amount * 100 
  END), 1) AS avg_overpayment_pct
FROM claim_line cl
JOIN claim_header ch ON cl.claim_id = ch.claim_id
JOIN payer p ON ch.payer_id = p.payer_id
JOIN payment pmt ON cl.claim_line_id = pmt.claim_line_id
WHERE cl.allowed_amount IS NOT NULL AND cl.allowed_amount > 0
  AND ch.filing_date BETWEEN '2026-01-01' AND '2026-02-28'
GROUP BY ch.payer_id, p.payer_name
HAVING SUM(CASE WHEN pmt.payment_amount > cl.allowed_amount * 1.05 THEN 1 ELSE 0 END) > 10
```

**Expected**: Payments should not systematically exceed allowed (within 5% tolerance for rounding). **Actual**: PAY-001 (BCBS) shows ~12% systematic overpayment on Q1 2026 claims due to stale fee schedule in allowed_amount.

---

## The Two Reports — Detailed Specifications

### Report A: "CFO Monthly Revenue Summary"

**Audience**: Board of Directors, CFO, VP Finance  
**Source ETL**: Finance team's curated pipeline (more conservative, exclusion-based)  
**Philosophy**: "Only count what we can validate"

| KPI | Formula | Value |
|-----|---------|-------|
| Net Collection Rate | `SUM(paid) / SUM(allowed) WHERE allowed IS NOT NULL` | 94.2% |
| Denial Rate | `COUNT(valid_denials) / COUNT(DISTINCT claims)` | 6.1% |
| Days in AR | `MAX(snapshot_date) per location, weighted avg` | 42 |
| Commercial Mix | `JOIN to payer ref table, use payer_category` | 52.1% |
| Total Charges (YTD) | `Deduped by (patient, date, CPT, charge); TC/26 overlap removed` | $47.3M |
| Net Revenue | `Reconciled against payment table; payer corrections applied` | $38.1M |

### Report B: "Revenue Ops Performance Dashboard"

**Audience**: VP Revenue Cycle, Billing Managers, Ops Directors  
**Source ETL**: Operations team's pipeline (more inclusive, count-everything approach)  
**Philosophy**: "Count everything until proven invalid"

| KPI | Formula | Value |
|-----|---------|-------|
| Net Collection Rate | `SUM(paid) / SUM(COALESCE(allowed, charge_amount))` | 81.7% |
| Denial Rate | `COUNT(all_denials) / COUNT(DISTINCT claims)` | 11.3% |
| Days in AR | `WHERE snapshot_date = CURRENT_DATE (excludes stale)` | 51 |
| Commercial Mix | `GROUP BY claim_header.payer_name (fragmented)` | 38.4% |
| Total Charges (YTD) | `No dedup; all lines summed naively` | $49.4M |
| Net Revenue | `Uses allowed_amount as-is from claim_line` | $41.6M |

---

## Data Volume & Realism

| Table | Row Count | Notes |
|-------|-----------|-------|
| patient | ~5,000 | Mix of active and historical |
| provider | ~50 | Radiologists + referring MDs |
| service_location | 12 | 8 imaging centers + 3 hospital + 1 mobile |
| payer | 12 | Major commercial + govt + self-pay |
| encounter | ~40,000 | ~12 months of studies |
| claim_header | ~38,000 | Some encounters have no claim yet |
| claim_line | ~95,000 | Avg 2.5 lines per claim |
| denial | ~4,500 | ~12% initial denial rate (industry avg) |
| payment | ~85,000 | Multiple payments per claim possible |
| ar_aging_snapshot | ~8,640 | Daily snapshots × 12 locations × 5 buckets × ~30 days |
| authorization | ~15,000 | Prior auths for advanced imaging |

---

## Radiology-Specific Realism

### Modalities & CPT Codes
| Modality | Example CPTs | Avg Charge | Volume Mix |
|----------|-------------|-----------|------------|
| MRI | 70553, 72148, 73721 | $2,800 | 25% |
| CT | 74177, 70553, 72131 | $1,500 | 30% |
| X-ray | 71046, 73030, 72100 | $350 | 20% |
| Ultrasound | 76700, 76856, 93306 | $600 | 12% |
| Mammography | 77067, 77066 | $450 | 8% |
| Nuclear Med | 78452, 78816 | $3,200 | 3% |
| Interventional | 36247, 37228 | $8,500 | 2% |

### Common Radiology Denial Reasons
| CARC | Reason | % of Denials |
|------|--------|-------------|
| CO-4 | Procedure code inconsistent with modifier | 18% |
| CO-16 | Missing/incomplete information | 15% |
| CO-197 | Precertification/authorization not obtained | 22% |
| PR-96 | Non-covered charge | 8% |
| CO-45 | Charges exceed fee schedule | 12% |
| CO-50 | Non-covered service (no medical necessity) | 10% |
| OA-23 | Payment adjusted (impact of prior payer) | 8% |
| **CO-999** | **INVALID — legacy migration artifact** | **5%** |
| **PI-300** | **INVALID — old system internal code** | **2%** |

---

## Schema Name & Catalog

Target: `serverless_stable_jc9zgx_catalog.radiology_revenue_cycle`

This is separate from the existing `rcm_pipeline` schema to keep the dataset self-contained while reusing the DQ framework from the parent project.

---

## Implementation Sequence

1. **DDL**: Create all tables with proper types and constraints
2. **Data Generation**: Python/SQL to produce realistic synthetic data with planted defects
3. **Report A View**: `v_cfo_monthly_summary` — the Finance team's logic
4. **Report B View**: `v_revops_performance` — the Ops team's logic  
5. **Side-by-Side Dashboard**: Single Lakeview dashboard with both reports visible simultaneously
6. **DQ Scoring Integration**: Wire tables into the existing `data_quality` schema's DQ framework

---

## Dashboard Layout

```
┌─────────────────────────────────────────────────────────────┐
│  "Two Reports. Same Data. Which One Do You Trust?"          │
├─────────────────────────────┬───────────────────────────────┤
│  CFO MONTHLY SUMMARY        │  REVENUE OPS DASHBOARD        │
│  ─────────────────────      │  ────────────────────────     │
│  Net Collections: 94.2% ✓   │  Net Collections: 81.7% ⚠    │
│  Denial Rate: 6.1%          │  Denial Rate: 11.3%           │
│  Days in AR: 42             │  Days in AR: 51               │
│  Commercial: 52.1%          │  Commercial: 38.4%            │
│  Total Charges: $47.3M      │  Total Charges: $49.4M        │
│  Net Revenue: $38.1M        │  Net Revenue: $41.6M          │
├─────────────────────────────┴───────────────────────────────┤
│  DIVERGENCE ANALYSIS                                         │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ [Bar chart showing gap per KPI with root cause]     │    │
│  └─────────────────────────────────────────────────────┘    │
│  Net Collections: Δ 12.5pp → COMPLETENESS (NULL allowed)    │
│  Denial Rate: Δ 5.2pp → VALIDITY (invalid CARC codes)       │
│  Days in AR: Δ 9 days → TIMELINESS (stale snapshot)         │
│  Commercial Mix: Δ 13.7pp → CONSISTENCY (payer name chaos)  │
│  Total Charges: Δ $2.1M → UNIQUENESS (rebill duplicates)    │
│  Net Revenue: Δ $3.5M → ACCURACY (3 compound defects)       │
└─────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

1. **Why separate schema?** Keeps the dataset isolated. The existing `rcm_pipeline` schema already has a different data shape and volume. We want a clean, purpose-built dataset.

2. **Why denormalize payer_name on claim_header?** This IS the defect. In real life, claims arrive from clearinghouses with whatever payer name the submitter typed. The canonical payer table is maintained separately. The inconsistency between them is one of the most common real-world RCM data problems.

3. **Why include both claim_header and claim_line?** Radiology billing is inherently multi-line (technical + professional components, multiple series in one study). The line-level granularity is where the accuracy defects and allowed_amount NULL issues live.

4. **Why 40K encounters / 38K claims?** Realistic for a 12-month window of a mid-size radiology group. Small enough to generate quickly, large enough that the percentages are stable and the dollar figures feel executive-real.

5. **Why the authorization table?** Prior auth denials (CO-197) are the #1 denial reason in radiology. Having the auth table lets us show denials where an auth existed but wasn't linked — a common operational failure that both reports might handle differently in a future phase.

6. **Why three accuracy defects?** Accuracy problems in healthcare billing rarely come from a single source. The compound nature — component billing errors, clearinghouse mapping bugs, and fee schedule lag — mirrors reality and makes the diagnostic story richer. Each one individually looks minor; together they produce material financial misstatement.
