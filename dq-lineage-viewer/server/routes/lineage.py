from fastapi import APIRouter
from ..sql_client import execute_query
from ..config import DEFAULT_CATALOG, DEFAULT_SCHEMA

router = APIRouter(prefix="/lineage", tags=["lineage"])


@router.get("/catalogs")
def list_catalogs():
    rows = execute_query("SHOW CATALOGS")
    return [r["catalog"] for r in rows]


@router.get("/schemas/{catalog}")
def list_schemas(catalog: str):
    rows = execute_query(f"SHOW SCHEMAS IN `{catalog}`")
    return [r["databaseName"] for r in rows]


@router.get("/graph/{catalog}/{schema}")
def get_lineage_graph(catalog: str, schema: str):
    """Build a lineage graph with DQ scores for all tables in a schema."""

    # 1. Get all lineage edges involving this catalog/schema
    edges_sql = f"""
    SELECT DISTINCT
        source_table_full_name,
        source_table_name,
        target_table_full_name,
        target_table_name
    FROM system.access.table_lineage
    WHERE (
        (source_table_catalog = '{catalog}' AND source_table_schema = '{schema}')
        OR
        (target_table_catalog = '{catalog}' AND target_table_schema = '{schema}')
    )
    AND source_table_full_name IS NOT NULL
    AND target_table_full_name IS NOT NULL
    """
    edges_raw = execute_query(edges_sql)

    # 1b. Fallback: if no system lineage, check for v_pipeline_lineage config view
    node_type_overrides: dict[str, str] = {}
    if not edges_raw:
        try:
            fallback_sql = f"""
            SELECT
                source_table,
                target_table,
                source_type,
                target_type
            FROM `{catalog}`.`{schema}`.v_pipeline_lineage
            """
            fallback_rows = execute_query(fallback_sql)
            edges_raw = []
            for r in fallback_rows:
                src_fqn = f"{catalog}.{schema}.{r['source_table']}"
                tgt_fqn = f"{catalog}.{schema}.{r['target_table']}"
                edges_raw.append({
                    "source_table_full_name": src_fqn,
                    "source_table_name": r["source_table"],
                    "target_table_full_name": tgt_fqn,
                    "target_table_name": r["target_table"],
                })
                # Track entity types from the config
                if r.get("source_type"):
                    node_type_overrides[src_fqn] = r["source_type"]
                if r.get("target_type"):
                    node_type_overrides[tgt_fqn] = r["target_type"]
        except Exception:
            pass  # View doesn't exist, continue with empty edges

    # 2. Get DQ scores for all tables in this schema
    scores_sql = f"""
    SELECT
        table_name,
        overall_score,
        completeness_score,
        accuracy_score,
        validity_score,
        consistency_score,
        uniqueness_score,
        timeliness_score,
        total_rules_run,
        rules_passed,
        rules_failed,
        is_critical,
        domain_name,
        effective_threshold_min,
        effective_threshold_warning
    FROM `{catalog}`.`{schema}`.v_table_scores_latest
    """
    try:
        scores_raw = execute_query(scores_sql)
    except Exception:
        scores_raw = []

    scores_map = {r["table_name"]: r for r in scores_raw}

    # 3. Build unique nodes from edges
    node_names = set()
    for e in edges_raw:
        node_names.add(e["source_table_full_name"])
        node_names.add(e["target_table_full_name"])

    # 4. Determine node layers via topological position
    #    Sources = nodes that appear only as source, sinks = only as target
    sources_set = {e["source_table_full_name"] for e in edges_raw}
    targets_set = {e["target_table_full_name"] for e in edges_raw}

    # Build adjacency for BFS layering
    children: dict[str, set[str]] = {}
    parents: dict[str, set[str]] = {}
    for e in edges_raw:
        src = e["source_table_full_name"]
        tgt = e["target_table_full_name"]
        children.setdefault(src, set()).add(tgt)
        parents.setdefault(tgt, set()).add(src)
        children.setdefault(tgt, set())
        parents.setdefault(src, set())

    # BFS from root nodes (no parents) to assign layers
    roots = [n for n in node_names if not parents.get(n)]
    if not roots:
        roots = list(node_names)[:1]

    layers: dict[str, int] = {}
    queue = [(r, 0) for r in roots]
    visited = set()
    while queue:
        node, depth = queue.pop(0)
        if node in visited:
            layers[node] = max(layers.get(node, 0), depth)
            continue
        visited.add(node)
        layers[node] = max(layers.get(node, 0), depth)
        for child in children.get(node, []):
            queue.append((child, depth + 1))

    # Assign any unvisited nodes
    for n in node_names:
        if n not in layers:
            layers[n] = 0

    # 5. Position nodes in a grid layout
    layer_buckets: dict[int, list[str]] = {}
    for n, layer in layers.items():
        layer_buckets.setdefault(layer, []).append(n)

    X_SPACING = 320
    Y_SPACING = 120
    nodes = []
    for layer_idx in sorted(layer_buckets.keys()):
        bucket = sorted(layer_buckets[layer_idx])
        for i, fqn in enumerate(bucket):
            short_name = fqn.split(".")[-1] if "." in fqn else fqn
            score_data = scores_map.get(short_name, {})
            overall = score_data.get("overall_score")
            threshold_warn = score_data.get("effective_threshold_warning", 85)
            threshold_min = score_data.get("effective_threshold_min", 75)

            if overall is not None:
                if overall >= threshold_warn:
                    status = "healthy"
                elif overall >= threshold_min:
                    status = "warning"
                else:
                    status = "critical"
            else:
                status = "no_data"

            # Determine node type from overrides, naming, or default
            if fqn in node_type_overrides:
                node_type = node_type_overrides[fqn]
            elif short_name.startswith("v_"):
                node_type = "view"
            else:
                node_type = "table"

            nodes.append({
                "id": fqn,
                "label": short_name,
                "fqn": fqn,
                "type": node_type,
                "x": layer_idx * X_SPACING,
                "y": i * Y_SPACING,
                "overall_score": overall,
                "status": status,
                "scores": {
                    "completeness": score_data.get("completeness_score"),
                    "accuracy": score_data.get("accuracy_score"),
                    "validity": score_data.get("validity_score"),
                    "consistency": score_data.get("consistency_score"),
                    "uniqueness": score_data.get("uniqueness_score"),
                    "timeliness": score_data.get("timeliness_score"),
                },
                "rules_passed": score_data.get("rules_passed"),
                "rules_failed": score_data.get("rules_failed"),
                "total_rules": score_data.get("total_rules_run"),
                "domain": score_data.get("domain_name"),
                "is_critical": score_data.get("is_critical"),
            })

    edges = []
    seen_edges = set()
    for e in edges_raw:
        key = (e["source_table_full_name"], e["target_table_full_name"])
        if key not in seen_edges:
            seen_edges.add(key)
            edges.append({
                "source": e["source_table_full_name"],
                "target": e["target_table_full_name"],
            })

    return {"nodes": nodes, "edges": edges}


@router.get("/table/{catalog}/{schema}/{table}")
def get_table_lineage(catalog: str, schema: str, table: str):
    """Get lineage centered on a specific table (upstream + downstream)."""
    fqn = f"{catalog}.{schema}.{table}"

    # Upstream (what feeds into this table)
    upstream_sql = f"""
    SELECT DISTINCT source_table_full_name, source_table_name
    FROM system.access.table_lineage
    WHERE target_table_full_name = '{fqn}'
      AND source_table_full_name IS NOT NULL
    """

    # Downstream (what this table feeds)
    downstream_sql = f"""
    SELECT DISTINCT target_table_full_name, target_table_name
    FROM system.access.table_lineage
    WHERE source_table_full_name = '{fqn}'
      AND target_table_full_name IS NOT NULL
    """

    upstream = execute_query(upstream_sql)
    downstream = execute_query(downstream_sql)

    return {
        "table": fqn,
        "upstream": [r["source_table_full_name"] for r in upstream],
        "downstream": [r["target_table_full_name"] for r in downstream],
    }
