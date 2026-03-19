from databricks import sql as dbsql
from .config import get_workspace_host, get_oauth_token, WAREHOUSE_ID


def _get_connection():
    host = get_workspace_host().replace("https://", "")
    token = get_oauth_token()
    return dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        access_token=token,
    )


def execute_query(statement: str) -> list[dict]:
    conn = _get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(statement)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()
