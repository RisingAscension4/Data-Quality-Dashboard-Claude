"""
PySpark Data Quality Validation: Patient Encounters
Custom validation framework applied to encounter data
after ingestion from the ADT (Admit-Discharge-Transfer) feed.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType
from datetime import datetime, timedelta


spark = SparkSession.builder.getOrCreate()


def validate_not_null(df: DataFrame, column: str, rule_name: str) -> DataFrame:
    """Check that a column has no null values."""
    return df.withColumn(
        f"dq_{rule_name}",
        F.when(F.col(column).isNull(), "FAIL").otherwise("PASS")
    )


def validate_in_set(df: DataFrame, column: str, valid_values: list, rule_name: str) -> DataFrame:
    """Check that column values are in an allowed set."""
    return df.withColumn(
        f"dq_{rule_name}",
        F.when(F.col(column).isin(valid_values), "PASS").otherwise("FAIL")
    )


def validate_range(df: DataFrame, column: str, min_val, max_val, rule_name: str) -> DataFrame:
    """Check that numeric values fall within expected range."""
    return df.withColumn(
        f"dq_{rule_name}",
        F.when(
            (F.col(column) >= min_val) & (F.col(column) <= max_val), "PASS"
        ).otherwise("FAIL")
    )


def validate_referential(df: DataFrame, column: str, ref_df: DataFrame,
                         ref_column: str, rule_name: str) -> DataFrame:
    """Check referential integrity against a reference table."""
    orphans = df.join(ref_df, df[column] == ref_df[ref_column], "left_anti")
    return df.withColumn(
        f"dq_{rule_name}",
        F.when(F.col(column).isin([r[0] for r in orphans.select(column).collect()]), "FAIL")
        .otherwise("PASS")
    )


def validate_date_order(df: DataFrame, col_before: str, col_after: str, rule_name: str) -> DataFrame:
    """Ensure one date column is before or equal to another."""
    return df.withColumn(
        f"dq_{rule_name}",
        F.when(F.col(col_after) >= F.col(col_before), "PASS").otherwise("FAIL")
    )


# ── Load data ───────────────────────────────────────────────────
encounters = spark.table("silver_clinical.encounters")
providers = spark.table("reference_data.providers")
facilities = spark.table("reference_data.facilities")
patients = spark.table("silver_clinical.patients")

# ── Apply validation rules ──────────────────────────────────────

# Completeness rules
encounters = validate_not_null(encounters, "encounter_id", "encounter_id_not_null")
encounters = validate_not_null(encounters, "patient_id", "patient_id_not_null")
encounters = validate_not_null(encounters, "admit_datetime", "admit_date_not_null")
encounters = validate_not_null(encounters, "attending_provider_npi", "provider_not_null")
encounters = validate_not_null(encounters, "facility_id", "facility_not_null")
encounters = validate_not_null(encounters, "encounter_type", "encounter_type_not_null")

# Validity rules
encounters = validate_in_set(
    encounters, "encounter_type",
    ["Inpatient", "Outpatient", "Emergency", "Observation", "Telehealth", "Home Health"],
    "valid_encounter_type"
)
encounters = validate_in_set(
    encounters, "discharge_disposition",
    ["Home", "SNF", "Rehab", "AMA", "Expired", "Transfer", "Hospice", None],
    "valid_discharge_disposition"
)
encounters = validate_in_set(
    encounters, "admit_source",
    ["ER", "Physician Referral", "Transfer", "Self-Referral", "Newborn", "Court/Law"],
    "valid_admit_source"
)

# Range checks
encounters = validate_range(encounters, "length_of_stay_days", 0, 365, "los_in_range")
encounters = validate_range(encounters, "total_charges", 0, 5000000, "charges_in_range")

# Consistency rules
encounters = validate_date_order(
    encounters, "admit_datetime", "discharge_datetime", "discharge_after_admit"
)

# Custom: If discharged, discharge date must not be null
encounters = encounters.withColumn(
    "dq_discharged_has_date",
    F.when(
        (F.col("encounter_status") == "Discharged") & F.col("discharge_datetime").isNull(),
        "FAIL"
    ).otherwise("PASS")
)

# Custom: Encounter should not be longer than 30 days without review flag
encounters = encounters.withColumn(
    "dq_extended_stay_flagged",
    F.when(
        (F.col("length_of_stay_days") > 30) & (F.col("extended_stay_review") != True),
        "FAIL"
    ).otherwise("PASS")
)

# Referential integrity
encounters = validate_referential(
    encounters, "patient_id", patients, "patient_id", "patient_exists"
)
encounters = validate_referential(
    encounters, "attending_provider_npi", providers, "npi", "provider_exists"
)
encounters = validate_referential(
    encounters, "facility_id", facilities, "facility_id", "facility_exists"
)

# ── Collect DQ results ──────────────────────────────────────────
dq_columns = [c for c in encounters.columns if c.startswith("dq_")]
dq_summary = []
for col_name in dq_columns:
    total = encounters.count()
    failures = encounters.filter(F.col(col_name) == "FAIL").count()
    dq_summary.append({
        "rule_name": col_name.replace("dq_", ""),
        "total_records": total,
        "failures": failures,
        "pass_rate": round((total - failures) / total * 100, 2),
        "run_timestamp": datetime.now().isoformat()
    })

dq_results = spark.createDataFrame(dq_summary)
dq_results.write.mode("append").saveAsTable("gold_clinical.encounter_dq_results")
