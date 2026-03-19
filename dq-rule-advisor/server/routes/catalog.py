from fastapi import APIRouter, HTTPException
from ..sql_client import execute_query
from ..config import DEFAULT_CATALOG

router = APIRouter(prefix="/catalog", tags=["catalog"])


@router.get("/catalogs")
def list_catalogs():
    rows = execute_query("SHOW CATALOGS")
    return [r["catalog"] for r in rows]


@router.get("/schemas/{catalog}")
def list_schemas(catalog: str):
    rows = execute_query(f"SHOW SCHEMAS IN `{catalog}`")
    return [r["databaseName"] for r in rows]


@router.get("/tables/{catalog}/{schema}")
def list_tables(catalog: str, schema: str):
    try:
        rows = execute_query(f"SHOW TABLES IN `{catalog}`.`{schema}`")
        return [r["tableName"] for r in rows]
    except Exception:
        return []


@router.get("/columns/{catalog}/{schema}/{table}")
def get_columns(catalog: str, schema: str, table: str):
    fqn = f"`{catalog}`.`{schema}`.`{table}`"
    rows = execute_query(f"DESCRIBE TABLE {fqn}")
    columns = []
    seen = set()
    for r in rows:
        col_name = r.get("col_name", "")
        # Stop at metadata sections (clustering info, partition info, etc.)
        if col_name.startswith("#"):
            break
        if col_name and col_name not in seen:
            seen.add(col_name)
            columns.append({
                "name": col_name,
                "type": r.get("data_type", ""),
                "comment": r.get("comment", ""),
            })
    return columns


@router.get("/sample/{catalog}/{schema}/{table}")
def get_sample(catalog: str, schema: str, table: str):
    fqn = f"`{catalog}`.`{schema}`.`{table}`"
    rows = execute_query(f"SELECT * FROM {fqn} LIMIT 20")
    return rows


@router.get("/profile/{catalog}/{schema}/{table}")
def get_profile(catalog: str, schema: str, table: str):
    """Get column-level statistics for AI analysis."""
    fqn = f"`{catalog}`.`{schema}`.`{table}`"
    columns = get_columns(catalog, schema, table)
    col_names = [c["name"] for c in columns]

    exprs = []
    for c in columns:
        name = c["name"]
        safe = name.replace("`", "``")
        exprs.append(f"COUNT(*) AS total_rows")
        exprs.append(f"COUNT(`{safe}`) AS `{safe}__non_null`")
        exprs.append(f"COUNT(DISTINCT `{safe}`) AS `{safe}__distinct`")
        if c["type"] in ("int", "bigint", "double", "float", "decimal", "long", "short"):
            exprs.append(f"MIN(`{safe}`) AS `{safe}__min`")
            exprs.append(f"MAX(`{safe}`) AS `{safe}__max`")
            exprs.append(f"AVG(`{safe}`) AS `{safe}__avg`")

    # Deduplicate total_rows
    unique_exprs = list(dict.fromkeys(exprs))
    sql = f"SELECT {', '.join(unique_exprs)} FROM {fqn}"
    rows = execute_query(sql)
    return {"columns": columns, "stats": rows[0] if rows else {}}
