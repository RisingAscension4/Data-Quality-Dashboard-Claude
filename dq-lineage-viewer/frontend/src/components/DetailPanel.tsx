import {
  X, Database, Eye, CheckCircle2, XCircle,
  LayoutDashboard, Brain, Radio, AppWindow,
} from "lucide-react";

const NODE_TYPE_CONFIG: Record<string, { icon: any; color: string; label: string }> = {
  table:            { icon: Database,        color: "text-blue-500",    label: "Table" },
  view:             { icon: Eye,             color: "text-indigo-500",  label: "View" },
  dashboard:        { icon: LayoutDashboard, color: "text-violet-500",  label: "Dashboard" },
  model:            { icon: Brain,           color: "text-fuchsia-500", label: "Model" },
  serving_endpoint: { icon: Radio,           color: "text-orange-500",  label: "Serving Endpoint" },
  app:              { icon: AppWindow,       color: "text-cyan-600",    label: "App" },
};

interface DetailPanelProps {
  node: any;
  onClose: () => void;
}

const DIMENSION_NAMES: Record<string, string> = {
  completeness: "Completeness",
  accuracy: "Accuracy",
  validity: "Validity",
  consistency: "Consistency",
  uniqueness: "Uniqueness",
  timeliness: "Timeliness",
};

function ScoreBar({ label, score }: { label: string; score: number | null }) {
  if (score === null) {
    return (
      <div className="flex items-center justify-between text-xs text-gray-400 py-1">
        <span>{label}</span>
        <span>N/A</span>
      </div>
    );
  }
  const color = score >= 90 ? "bg-green-500" : score >= 80 ? "bg-amber-500" : "bg-red-500";
  return (
    <div className="py-1">
      <div className="flex items-center justify-between text-xs mb-0.5">
        <span className="text-gray-600">{label}</span>
        <span className="font-semibold">{score.toFixed(1)}%</span>
      </div>
      <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
        <div className={`h-full ${color} rounded-full transition-all`} style={{ width: `${score}%` }} />
      </div>
    </div>
  );
}

export default function DetailPanel({ node, onClose }: DetailPanelProps) {
  const d = node.data;

  return (
    <div className="absolute top-0 right-0 w-80 h-full bg-white border-l shadow-xl z-50 overflow-y-auto">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b bg-gray-50">
        <div className="flex items-center gap-2">
          {(() => {
            const cfg = NODE_TYPE_CONFIG[d.type] || NODE_TYPE_CONFIG.table;
            const Icon = cfg.icon;
            return <Icon className={`w-4 h-4 ${cfg.color}`} />;
          })()}
          <span className="font-mono text-sm font-semibold">{d.label}</span>
        </div>
        <button onClick={onClose} className="p-1 hover:bg-gray-200 rounded">
          <X className="w-4 h-4" />
        </button>
      </div>

      <div className="p-4 space-y-4">
        {/* Overall score */}
        {d.overall_score !== null ? (
          <div className="text-center py-3">
            <div
              className={`text-4xl font-bold ${
                d.status === "healthy"
                  ? "text-green-600"
                  : d.status === "warning"
                  ? "text-amber-600"
                  : "text-red-600"
              }`}
            >
              {d.overall_score.toFixed(1)}%
            </div>
            <div className="text-xs text-gray-500 mt-1">Overall Data Quality Score</div>
          </div>
        ) : (
          <div className="text-center py-3 text-gray-400 text-sm">
            No DQ scores available for this {d.type}
          </div>
        )}

        {/* Rules summary */}
        {d.total_rules !== null && (
          <div className="flex gap-3 justify-center">
            <div className="flex items-center gap-1 text-sm">
              <CheckCircle2 className="w-4 h-4 text-green-500" />
              <span className="font-semibold text-green-700">{d.rules_passed}</span>
              <span className="text-gray-400">passed</span>
            </div>
            <div className="flex items-center gap-1 text-sm">
              <XCircle className="w-4 h-4 text-red-500" />
              <span className="font-semibold text-red-700">{d.rules_failed}</span>
              <span className="text-gray-400">failed</span>
            </div>
          </div>
        )}

        {/* Dimension scores */}
        {d.overall_score !== null && (
          <div>
            <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
              Quality Dimensions
            </h3>
            <div className="space-y-0.5">
              {Object.entries(DIMENSION_NAMES).map(([key, label]) => (
                <ScoreBar key={key} label={label} score={d.scores[key]} />
              ))}
            </div>
          </div>
        )}

        {/* Metadata */}
        <div>
          <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
            Details
          </h3>
          <dl className="text-xs space-y-1">
            <div className="flex justify-between">
              <dt className="text-gray-400">Full Name</dt>
              <dd className="font-mono text-gray-600 text-right max-w-[180px] truncate" title={node.id}>
                {node.id}
              </dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-gray-400">Type</dt>
              <dd className="text-gray-600">{(NODE_TYPE_CONFIG[d.type] || NODE_TYPE_CONFIG.table).label}</dd>
            </div>
            {d.domain && (
              <div className="flex justify-between">
                <dt className="text-gray-400">Domain</dt>
                <dd className="text-gray-600">{d.domain}</dd>
              </div>
            )}
            {d.is_critical && (
              <div className="flex justify-between">
                <dt className="text-gray-400">Priority</dt>
                <dd className="text-red-600 font-medium">Critical</dd>
              </div>
            )}
          </dl>
        </div>
      </div>
    </div>
  );
}
