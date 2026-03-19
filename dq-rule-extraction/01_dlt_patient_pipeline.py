"""
DLT Pipeline: Patient Data Ingestion
Ingests raw patient records from the EHR landing zone,
applies quality checks, and writes to the silver layer.
"""
import dlt
from pyspark.sql.functions import col, when, regexp_extract, length, current_timestamp


@dlt.table(
    name="bronze_patients",
    comment="Raw patient records from EHR system"
)
def bronze_patients():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Volumes/health_data/ehr/landing/patients/")
    )


@dlt.table(
    name="silver_patients",
    comment="Cleansed patient records with quality expectations applied"
)
@dlt.expect("patient_id_not_null", "patient_id IS NOT NULL")
@dlt.expect("valid_date_of_birth", "date_of_birth IS NOT NULL AND date_of_birth < current_date()")
@dlt.expect_or_drop(
    "valid_email_format",
    "email IS NULL OR email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'"
)
@dlt.expect_or_fail("mrn_is_unique", "mrn_count = 1")
@dlt.expect(
    "valid_phone_number",
    "phone IS NULL OR length(regexp_replace(phone, '[^0-9]', '')) = 10"
)
@dlt.expect("gender_in_valid_set", "gender IN ('M', 'F', 'NB', 'O', 'U', 'Unknown')")
@dlt.expect(
    "zip_code_valid",
    "zip_code IS NULL OR (length(zip_code) = 5 AND zip_code RLIKE '^[0-9]{5}$')"
)
@dlt.expect("last_name_not_empty", "last_name IS NOT NULL AND trim(last_name) != ''")
@dlt.expect("first_name_not_empty", "first_name IS NOT NULL AND trim(first_name) != ''")
@dlt.expect(
    "ssn_format_valid",
    "ssn IS NULL OR ssn RLIKE '^[0-9]{3}-[0-9]{2}-[0-9]{4}$'"
)
def silver_patients():
    mrn_counts = (
        dlt.read_stream("bronze_patients")
        .groupBy("mrn")
        .count()
        .withColumnRenamed("count", "mrn_count")
    )
    return (
        dlt.read_stream("bronze_patients")
        .join(mrn_counts, "mrn", "left")
        .withColumn("ingested_at", current_timestamp())
        .withColumn(
            "age_bucket",
            when(col("age") < 18, "pediatric")
            .when(col("age") < 65, "adult")
            .otherwise("senior")
        )
    )
