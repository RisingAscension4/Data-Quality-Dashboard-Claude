import { useCallback, useEffect, useState, useMemo } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
  MarkerType,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { GitBranch, Loader2 } from "lucide-react";
import DQNode from "./components/DQNode";
import DetailPanel from "./components/DetailPanel";

const nodeTypes = { dqNode: DQNode };

const STATUS_MINIMAP_COLORS: Record<string, string> = {
  healthy: "#22c55e",
  warning: "#f59e0b",
  critical: "#ef4444",
  no_data: "#9ca3af",
};

export default function App() {
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [catalogs, setCatalogs] = useState<string[]>([]);
  const [schemas, setSchemas] = useState<string[]>([]);
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [loading, setLoading] = useState(false);
  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Load catalogs
  useEffect(() => {
    fetch("/api/lineage/catalogs")
      .then((r) => r.json())
      .then(setCatalogs);
  }, []);

  // Load schemas when catalog changes
  useEffect(() => {
    if (!catalog) return;
    fetch(`/api/lineage/schemas/${catalog}`)
      .then((r) => r.json())
      .then(setSchemas);
  }, [catalog]);

  // Load lineage graph when schema is selected
  const loadGraph = useCallback(async () => {
    if (!catalog || !schema) return;
    setLoading(true);
    setError(null);
    setSelectedNode(null);
    try {
      const res = await fetch(`/api/lineage/graph/${catalog}/${schema}`);
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();

      const flowNodes: Node[] = data.nodes.map((n: any) => ({
        id: n.id,
        type: "dqNode",
        position: { x: n.x, y: n.y },
        data: {
          label: n.label,
          type: n.type,
          overall_score: n.overall_score,
          status: n.status,
          scores: n.scores,
          rules_passed: n.rules_passed,
          rules_failed: n.rules_failed,
          total_rules: n.total_rules,
          domain: n.domain,
          is_critical: n.is_critical,
        },
      }));

      const flowEdges: Edge[] = data.edges.map((e: any, i: number) => ({
        id: `e-${i}`,
        source: e.source,
        target: e.target,
        type: "smoothstep",
        animated: true,
        style: { stroke: "#94a3b8", strokeWidth: 2 },
        markerEnd: { type: MarkerType.ArrowClosed, color: "#94a3b8" },
      }));

      setNodes(flowNodes);
      setEdges(flowEdges);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [catalog, schema, setNodes, setEdges]);

  useEffect(() => {
    loadGraph();
  }, [loadGraph]);

  const onNodeClick = useCallback((_: any, node: Node) => {
    setSelectedNode(node);
  }, []);

  // Stats summary
  const stats = useMemo(() => {
    const scored = nodes.filter((n: Node) => (n.data as any).overall_score !== null);
    const avgScore =
      scored.length > 0
        ? scored.reduce((sum: number, n: Node) => sum + ((n.data as any).overall_score || 0), 0) / scored.length
        : null;
    const typeCounts: Record<string, number> = {};
    nodes.forEach((n: Node) => {
      const t = (n.data as any).type || "table";
      typeCounts[t] = (typeCounts[t] || 0) + 1;
    });
    return {
      total: nodes.length,
      typeCounts,
      healthy: nodes.filter((n: Node) => (n.data as any).status === "healthy").length,
      warning: nodes.filter((n: Node) => (n.data as any).status === "warning").length,
      critical: nodes.filter((n: Node) => (n.data as any).status === "critical").length,
      avgScore,
    };
  }, [nodes]);

  return (
    <div className="h-screen flex flex-col">
      {/* Header */}
      <header className="bg-white border-b shadow-sm px-6 py-3 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-3">
          <GitBranch className="w-5 h-5 text-indigo-600" />
          <div>
            <h1 className="text-lg font-bold text-gray-900">DQ Lineage Viewer</h1>
            <p className="text-xs text-gray-400">Data quality scores across the lineage graph</p>
          </div>
        </div>

        <div className="flex items-center gap-3">
          <select
            className="border rounded-lg px-3 py-1.5 text-sm"
            value={catalog}
            onChange={(e) => {
              setCatalog(e.target.value);
              setSchema("");
            }}
          >
            <option value="">Select catalog</option>
            {catalogs.map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>

          <select
            className="border rounded-lg px-3 py-1.5 text-sm"
            value={schema}
            onChange={(e) => setSchema(e.target.value)}
            disabled={!catalog}
          >
            <option value="">Select schema</option>
            {schemas.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>
      </header>

      {/* Stats bar */}
      {nodes.length > 0 && (
        <div className="bg-white border-b px-6 py-2 flex items-center gap-6 text-xs shrink-0">
          <span className="text-gray-500">
            <span className="font-semibold text-gray-700">{stats.total}</span> nodes
            ({Object.entries(stats.typeCounts).map(([t, c]) => `${c} ${t}${c > 1 ? "s" : ""}`).join(", ")})
          </span>
          <span className="text-gray-500">{edges.length} edges</span>
          <div className="flex items-center gap-3 ml-auto">
            <span className="flex items-center gap-1">
              <span className="w-2.5 h-2.5 rounded-full bg-green-500 inline-block" />
              {stats.healthy} healthy
            </span>
            <span className="flex items-center gap-1">
              <span className="w-2.5 h-2.5 rounded-full bg-amber-500 inline-block" />
              {stats.warning} warning
            </span>
            <span className="flex items-center gap-1">
              <span className="w-2.5 h-2.5 rounded-full bg-red-500 inline-block" />
              {stats.critical} critical
            </span>
            {stats.avgScore !== null && (
              <span className="text-gray-500 border-l pl-3 ml-1">
                Avg score: <span className="font-semibold text-gray-700">{stats.avgScore.toFixed(1)}%</span>
              </span>
            )}
          </div>
        </div>
      )}

      {/* Graph area */}
      <div className="flex-1 relative">
        {loading && (
          <div className="absolute inset-0 z-40 bg-white/80 flex items-center justify-center">
            <Loader2 className="w-8 h-8 text-indigo-500 animate-spin" />
            <span className="ml-3 text-gray-600">Loading lineage graph...</span>
          </div>
        )}

        {error && (
          <div className="absolute top-4 left-1/2 -translate-x-1/2 z-40 bg-red-50 border border-red-200 text-red-700 text-sm px-4 py-2 rounded-lg max-w-lg">
            {error}
          </div>
        )}

        {!catalog || !schema ? (
          <div className="flex items-center justify-center h-full text-gray-400">
            Select a catalog and schema to view the lineage graph
          </div>
        ) : (
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onNodeClick={onNodeClick}
            fitView
            fitViewOptions={{ padding: 0.2 }}
            minZoom={0.3}
            maxZoom={2}
          >
            <Background gap={20} size={1} color="#e5e7eb" />
            <Controls />
            <MiniMap
              nodeColor={(n) => STATUS_MINIMAP_COLORS[(n.data as any)?.status] || "#9ca3af"}
              maskColor="rgba(255,255,255,0.8)"
              style={{ border: "1px solid #e5e7eb" }}
            />
          </ReactFlow>
        )}

        {/* Detail panel */}
        {selectedNode && (
          <DetailPanel node={selectedNode} onClose={() => setSelectedNode(null)} />
        )}
      </div>
    </div>
  );
}
