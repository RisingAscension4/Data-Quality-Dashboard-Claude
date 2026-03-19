"""
Great Expectations Suite: Lab Results Validation
Validates laboratory test results flowing from the LIS system
into the clinical data warehouse.
"""
import great_expectations as gx

context = gx.get_context()

datasource = context.sources.add_or_update_spark(name="clinical_warehouse")
data_asset = datasource.add_table_asset(
    name="lab_results",
    table_name="silver_clinical.lab_results"
)

suite = context.add_or_update_expectation_suite("lab_results_quality_suite")

# Completeness checks
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="patient_id",
        meta={"dimension": "completeness", "severity": "critical"}
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="test_code",
        meta={"dimension": "completeness", "severity": "critical"}
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="result_value",
        mostly=0.98,
        meta={"dimension": "completeness", "severity": "warning",
              "notes": "Some tests may have pending results"}
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="collection_datetime",
        meta={"dimension": "completeness", "severity": "critical"}
    )
)

# Validity checks
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="result_status",
        value_set=["Final", "Preliminary", "Corrected", "Cancelled", "Pending"],
        meta={"dimension": "validity", "severity": "error"}
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToMatchRegex(
        column="test_code",
        regex=r"^[0-9]{4,5}-[0-9]$",
        mostly=0.95,
        meta={"dimension": "validity", "severity": "warning",
              "notes": "LOINC code format expected"}
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="result_numeric",
        min_value=-1000,
        max_value=100000,
        mostly=0.99,
        meta={"dimension": "accuracy", "severity": "error",
              "notes": "Catch wildly out-of-range numeric results"}
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="specimen_type",
        value_set=["Blood", "Urine", "CSF", "Tissue", "Swab",
                    "Saliva", "Stool", "Plasma", "Serum", "Other"],
        meta={"dimension": "validity", "severity": "warning"}
    )
)

# Consistency checks
suite.add_expectation(
    gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="result_datetime",
        column_B="collection_datetime",
        or_equal=True,
        meta={"dimension": "consistency", "severity": "error",
              "notes": "Result cannot be reported before collection"}
    )
)

suite.add_expectation(
    gx.expectations.ExpectCompoundColumnsToBeUnique(
        column_list=["patient_id", "test_code", "collection_datetime", "specimen_type"],
        meta={"dimension": "uniqueness", "severity": "warning",
              "notes": "Detect potential duplicate lab orders"}
    )
)

# Timeliness
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="reporting_lag_hours",
        max_value=72,
        meta={"dimension": "timeliness", "severity": "warning",
              "notes": "Results should be reported within 72 hours of collection"}
    )
)

context.save_expectation_suite(suite)

# Run validation
checkpoint = context.add_or_update_checkpoint(
    name="lab_results_checkpoint",
    validations=[{"batch_request": data_asset.build_batch_request(),
                  "expectation_suite_name": "lab_results_quality_suite"}]
)
result = checkpoint.run()
print(f"Validation success: {result.success}")
