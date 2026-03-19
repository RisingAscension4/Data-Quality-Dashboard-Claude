import { Handle, Position } from "@xyflow/react";
import {
  Database, Eye, AlertTriangle, CheckCircle2, XCircle, HelpCircle,
  LayoutDashboard, Brain, Radio, AppWindow,
} from "lucide-react";

interface DQNodeData {
  label: string;
  type: string;
  overall_score: number | null;
  status: "healthy" | "warning" | "critical" | "no_data";
  scores: Record<string, number | null>;
  rules_passed: number | null;
  rules_failed: number | null;
  total_rules: number | null;
  domain: string | null;
  is_critical: boolean | null;
  selected?: boolean;
}

const STATUS_STYLES: Record<string, { border: string; bg: string; icon: any }> = {
  healthy: { border: "border-green-400", bg: "bg-green-50", icon: CheckCircle2 },
  warning: { border: "border-amber-400", bg: "bg-amber-50", icon: AlertTriangle },
  critical: { border: "border-red-400", bg: "bg-red-50", icon: XCircle },
  no_data: { border: "border-gray-300", bg: "bg-gray-50", icon: HelpCircle },
};

const NODE_TYPE_CONFIG: Record<string, { icon: any; color: string; label: string }> = {
  table:            { icon: Database,        color: "text-blue-500",    label: "Table" },
  view:             { icon: Eye,             color: "text-indigo-500",  label: "View" },
  dashboard:        { icon: LayoutDashboard, color: "text-violet-500",  label: "Dashboard" },
  model:            { icon: Brain,           color: "text-fuchsia-500", label: "Model" },
  serving_endpoint: { icon: Radio,           color: "text-orange-500",  label: "Endpoint" },
  app:              { icon: AppWindow,       color: "text-cyan-600",    label: "App" },
};

const DIMENSION_LABELS: Record<string, string> = {
  completeness: "COM",
  accuracy: "ACC",
  validity: "VAL",
  consistency: "CON",
  uniqueness: "UNQ",
  timeliness: "TML",
};

function scoreColor(score: number | null): string {
  if (score === null) return "bg-gray-200";
  if (score >= 90) return "bg-green-400";
  if (score >= 80) return "bg-amber-400";
  return "bg-red-400";
}

export default function DQNode({ data }: { data: DQNodeData }) {
  const style = STATUS_STYLES[data.status];
  const StatusIcon = style.icon;
  const nodeConfig = NODE_TYPE_CONFIG[data.type] || NODE_TYPE_CONFIG.table;
  const TypeIcon = nodeConfig.icon;

  return (
    <div
      className={`rounded-xl border-2 ${style.border} ${style.bg} shadow-md min-w-[220px] ${
        data.selected ? "ring-2 ring-blue-500 ring-offset-2" : ""
      }`}
    >
      <Handle type="target" position={Position.Left} className="!bg-gray-400 !w-2.5 !h-2.5" />

      {/* Header */}
      <div className="flex items-center gap-2 px-3 py-2 border-b border-gray-200/50">
        <TypeIcon className={`w-3.5 h-3.5 ${nodeConfig.color} shrink-0`} />
        <span className="font-mono text-xs font-semibold text-gray-800 truncate">
          {data.label}
        </span>
        <span className={`text-[9px] px-1.5 py-0.5 rounded font-medium ml-auto shrink-0 ${
          data.type !== "table" && data.type !== "view"
            ? "bg-gray-200 text-gray-600"
            : "hidden"
        }`}>
          {nodeConfig.label.toUpperCase()}
        </span>
      </div>

      {/* Score */}
      <div className="px-3 py-2">
        {data.overall_score !== null ? (
          <>
            <div className="flex items-center justify-between mb-1.5">
              <div className="flex items-center gap-1.5">
                <StatusIcon className="w-4 h-4" />
                <span className="text-lg font-bold">
                  {data.overall_score.toFixed(1)}%
                </span>
              </div>
              {data.total_rules !== null && (
                <span className="text-[10px] text-gray-500">
                  {data.rules_passed}/{data.total_rules} rules
                </span>
              )}
            </div>

            {/* Dimension mini-bars */}
            <div className="grid grid-cols-6 gap-0.5">
              {Object.entries(DIMENSION_LABELS).map(([key, label]) => {
                const score = data.scores[key];
                return (
                  <div key={key} className="text-center">
                    <div
                      className={`h-1.5 rounded-full ${scoreColor(score)}`}
                      title={`${label}: ${score !== null ? score.toFixed(1) + "%" : "N/A"}`}
                    />
                    <span className="text-[8px] text-gray-400">{label}</span>
                  </div>
                );
              })}
            </div>
          </>
        ) : (
          <div className="flex items-center gap-1.5 text-xs text-gray-400">
            <HelpCircle className="w-3.5 h-3.5" />
            No DQ scores
          </div>
        )}
      </div>

      {/* Domain tag */}
      {data.domain && (
        <div className="px-3 pb-2">
          <span className="text-[9px] bg-white/70 text-gray-500 px-2 py-0.5 rounded-full border">
            {data.domain}
          </span>
        </div>
      )}

      <Handle type="source" position={Position.Right} className="!bg-gray-400 !w-2.5 !h-2.5" />
    </div>
  );
}
