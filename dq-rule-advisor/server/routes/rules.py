import json
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from ..llm import chat_completion
from ..sql_client import execute_query
from .catalog import get_columns, get_profile

router = APIRouter(prefix="/rules", tags=["rules"])

RULE_SCHEMA_PROMPT = """\
Return ONLY a valid JSON array of objects. No markdown fences, no explanation.
Each object must have exactly these fields:
- "rule_name": concise snake_case name (e.g., "patient_id_not_null")
- "rule_description": one-sentence explanation
- "rule_expression": a SQL boolean expression that evaluates to TRUE when data is valid \
(reference columns directly, no table prefix needed)
- "rule_type": one of [null_check, unique_check, range_check, pattern_check, \
referential_check, set_membership_check, consistency_check, freshness_check, custom_check]
- "dimension": one of [completeness, accuracy, validity, consistency, uniqueness, timeliness]
- "target_column": the column being checked (or null if multi-column)
- "severity": one of [critical, error, warning, info]
"""


class GenerateRequest(BaseModel):
    catalog: str
    schema_name: str
    table: str
    requirements: str


class AnalyzeRequest(BaseModel):
    catalog: str
    schema_name: str
    table: str


class ApproveRequest(BaseModel):
    catalog: str
    schema_name: str
    table: str
    approved_by: str
    rules: list[dict]


@router.post("/generate")
def generate_rules(req: GenerateRequest):
    """Convert natural-language requirements into structured DQ rules."""
    fqn = f"{req.catalog}.{req.schema_name}.{req.table}"
    columns = get_columns(req.catalog, req.schema_name, req.table)
    col_summary = "\n".join(
        [f"  - {c['name']} ({c['type']}){': ' + c['comment'] if c['comment'] else ''}"
         for c in columns]
    )

    messages = [
        {
            "role": "system",
            "content": (
                "You are a data quality engineer. Given a table's schema and a user's "
                "natural-language data quality requirements, produce structured DQ rules.\n\n"
                f"{RULE_SCHEMA_PROMPT}"
            ),
        },
        {
            "role": "user",
            "content": (
                f"Table: {fqn}\n\nColumns:\n{col_summary}\n\n"
                f"User requirements:\n{req.requirements}"
            ),
        },
    ]

    raw = chat_completion(messages)
    # Strip markdown fences if present
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1]
    if cleaned.endswith("```"):
        cleaned = cleaned.rsplit("```", 1)[0]
    cleaned = cleaned.strip()

    try:
        rules = json.loads(cleaned)
    except json.JSONDecodeError:
        raise HTTPException(status_code=502, detail=f"LLM returned invalid JSON: {raw[:500]}")

    for r in rules:
        r["source"] = "user_requirement"
    return rules


@router.post("/analyze")
def analyze_table(req: AnalyzeRequest):
    """Analyze a table's schema and data to recommend DQ rules automatically."""
    fqn = f"{req.catalog}.{req.schema_name}.{req.table}"
    profile = get_profile(req.catalog, req.schema_name, req.table)
    columns = profile["columns"]
    stats = profile["stats"]

    col_summary = "\n".join(
        [f"  - {c['name']} ({c['type']}){': ' + c['comment'] if c['comment'] else ''}"
         for c in columns]
    )

    # Format stats readably
    stats_lines = []
    for key, val in stats.items():
        stats_lines.append(f"  {key}: {val}")
    stats_summary = "\n".join(stats_lines)

    # Also get a sample of the data
    sample = execute_query(
        f"SELECT * FROM `{req.catalog}`.`{req.schema_name}`.`{req.table}` LIMIT 10"
    )
    sample_str = json.dumps(sample[:5], indent=2, default=str) if sample else "No data"

    messages = [
        {
            "role": "system",
            "content": (
                "You are a data quality engineer. Analyze the table schema, column statistics, "
                "and sample data below. Recommend data quality rules that should be applied.\n\n"
                "Consider:\n"
                "- Columns that should never be NULL (IDs, keys, required fields)\n"
                "- Columns with patterns (emails, phone numbers, codes, dates)\n"
                "- Numeric columns that need range checks (based on min/max/avg stats)\n"
                "- Columns that should have unique values\n"
                "- Columns with low cardinality that suggest set-membership checks\n"
                "- Date/timestamp columns that need freshness or ordering checks\n"
                "- Cross-column consistency rules (e.g., end_date > start_date)\n\n"
                f"{RULE_SCHEMA_PROMPT}"
            ),
        },
        {
            "role": "user",
            "content": (
                f"Table: {fqn}\n\n"
                f"Columns:\n{col_summary}\n\n"
                f"Column Statistics:\n{stats_summary}\n\n"
                f"Sample Data (first 5 rows):\n{sample_str}"
            ),
        },
    ]

    raw = chat_completion(messages)
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1]
    if cleaned.endswith("```"):
        cleaned = cleaned.rsplit("```", 1)[0]
    cleaned = cleaned.strip()

    try:
        rules = json.loads(cleaned)
    except json.JSONDecodeError:
        raise HTTPException(status_code=502, detail=f"LLM returned invalid JSON: {raw[:500]}")

    for r in rules:
        r["source"] = "ai_analysis"
    return rules


@router.post("/approve")
def approve_rules(req: ApproveRequest):
    """Save approved rules to the Delta table."""
    fqn = f"{req.catalog}.{req.schema_name}.{req.table}"

    if not req.rules:
        return {"saved": 0}

    values_clauses = []
    for r in req.rules:
        rule_name = r.get("rule_name", "").replace("'", "''")
        rule_desc = r.get("rule_description", "").replace("'", "''")
        rule_expr = r.get("rule_expression", "").replace("'", "''")
        rule_type = r.get("rule_type", "custom_check").replace("'", "''")
        dimension = r.get("dimension", "validity").replace("'", "''")
        target_col = r.get("target_column")
        target_col_sql = f"'{target_col}'" if target_col else "NULL"
        severity = r.get("severity", "warning").replace("'", "''")
        source = r.get("source", "manual").replace("'", "''")
        notes = r.get("notes", "").replace("'", "''")
        approved_by = req.approved_by.replace("'", "''")

        values_clauses.append(
            f"('{rule_name}', '{rule_desc}', '{rule_expr}', '{rule_type}', "
            f"'{dimension}', '{fqn}', {target_col_sql}, '{severity}', "
            f"'{source}', '{notes}', '{approved_by}', true, current_timestamp())"
        )

    sql = f"""\
INSERT INTO `{req.catalog}`.`{req.schema_name}`.dq_extracted_rules_verified
  (rule_name, rule_description, rule_expression, rule_type, dimension,
   target_table, target_column, severity, `source`, notes,
   approved_by, verified, extracted_at)
VALUES
  {','.join(values_clauses)}
"""
    execute_query(sql)
    return {"saved": len(req.rules), "table": f"{req.catalog}.{req.schema_name}.dq_extracted_rules_verified"}
