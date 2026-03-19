import { ArrowLeft, Check, X, Pencil, ChevronRight } from "lucide-react";
import { useStore } from "../store";
import type { DQRule } from "../types";
import { useState } from "react";

const SEVERITY_COLORS: Record<string, string> = {
  critical: "bg-red-100 text-red-800",
  error: "bg-orange-100 text-orange-800",
  warning: "bg-yellow-100 text-yellow-800",
  info: "bg-blue-100 text-blue-800",
};

const DIMENSION_COLORS: Record<string, string> = {
  completeness: "bg-purple-100 text-purple-700",
  accuracy: "bg-teal-100 text-teal-700",
  validity: "bg-indigo-100 text-indigo-700",
  consistency: "bg-pink-100 text-pink-700",
  uniqueness: "bg-cyan-100 text-cyan-700",
  timeliness: "bg-amber-100 text-amber-700",
};

function RuleRow({ rule }: { rule: DQRule }) {
  const updateRule = useStore((s) => s.updateRule);
  const [editing, setEditing] = useState(false);
  const [editExpr, setEditExpr] = useState(rule.rule_expression);
  const [editNotes, setEditNotes] = useState(rule.notes);

  const save = () => {
    updateRule(rule.id, { rule_expression: editExpr, notes: editNotes });
    setEditing(false);
  };

  return (
    <tr className={`border-b ${
      rule.status === "approved" ? "bg-green-50" :
      rule.status === "rejected" ? "bg-red-50 opacity-60" : "bg-white"
    }`}>
      <td className="px-4 py-3">
        <div className="font-mono text-sm font-medium text-gray-800">{rule.rule_name}</div>
        <div className="text-xs text-gray-500 mt-0.5">{rule.rule_description}</div>
      </td>
      <td className="px-4 py-3">
        {editing ? (
          <textarea
            className="w-full border rounded px-2 py-1 text-xs font-mono h-16"
            value={editExpr}
            onChange={(e) => setEditExpr(e.target.value)}
          />
        ) : (
          <code className="text-xs bg-gray-100 px-2 py-1 rounded block">{rule.rule_expression}</code>
        )}
      </td>
      <td className="px-4 py-3 text-center">
        <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${DIMENSION_COLORS[rule.dimension] || "bg-gray-100"}`}>
          {rule.dimension}
        </span>
      </td>
      <td className="px-4 py-3 text-center">
        <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${SEVERITY_COLORS[rule.severity] || "bg-gray-100"}`}>
          {rule.severity}
        </span>
      </td>
      <td className="px-4 py-3 text-center">
        <span className="text-xs text-gray-500">
          {rule.source === "user_requirement" ? "From requirements" : "AI recommended"}
        </span>
      </td>
      <td className="px-4 py-3">
        {editing ? (
          <div className="space-y-1">
            <input
              className="w-full border rounded px-2 py-1 text-xs"
              placeholder="Notes..."
              value={editNotes}
              onChange={(e) => setEditNotes(e.target.value)}
            />
            <button onClick={save} className="text-xs text-blue-600 hover:underline">
              Save
            </button>
          </div>
        ) : (
          <span className="text-xs text-gray-400">{rule.notes || "—"}</span>
        )}
      </td>
      <td className="px-4 py-3">
        <div className="flex items-center gap-1">
          <button
            onClick={() => updateRule(rule.id, { status: "approved" })}
            className={`p-1.5 rounded-lg transition ${
              rule.status === "approved"
                ? "bg-green-600 text-white"
                : "bg-gray-100 text-gray-400 hover:bg-green-100 hover:text-green-600"
            }`}
            title="Approve"
          >
            <Check className="w-4 h-4" />
          </button>
          <button
            onClick={() => updateRule(rule.id, { status: "rejected" })}
            className={`p-1.5 rounded-lg transition ${
              rule.status === "rejected"
                ? "bg-red-600 text-white"
                : "bg-gray-100 text-gray-400 hover:bg-red-100 hover:text-red-600"
            }`}
            title="Reject"
          >
            <X className="w-4 h-4" />
          </button>
          <button
            onClick={() => setEditing(!editing)}
            className="p-1.5 rounded-lg bg-gray-100 text-gray-400 hover:bg-blue-100 hover:text-blue-600 transition"
            title="Edit"
          >
            <Pencil className="w-4 h-4" />
          </button>
        </div>
      </td>
    </tr>
  );
}

export default function RuleReview() {
  const { rules, setStep } = useStore();

  const approved = rules.filter((r) => r.status === "approved").length;
  const rejected = rules.filter((r) => r.status === "rejected").length;
  const pending = rules.filter((r) => r.status === "pending").length;
  const updateRule = useStore((s) => s.updateRule);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="text-lg font-semibold text-gray-800">
          Review Rules
        </div>
        <div className="flex items-center gap-4">
          <button
            onClick={() => setStep("define")}
            className="flex items-center gap-1 text-sm text-gray-500 hover:text-gray-700"
          >
            <ArrowLeft className="w-4 h-4" /> Add more rules
          </button>
        </div>
      </div>

      {/* Summary bar */}
      <div className="flex gap-4 text-sm">
        <span className="bg-gray-100 px-3 py-1 rounded-full">{rules.length} total</span>
        <span className="bg-green-100 text-green-700 px-3 py-1 rounded-full">{approved} approved</span>
        <span className="bg-red-100 text-red-700 px-3 py-1 rounded-full">{rejected} rejected</span>
        <span className="bg-yellow-100 text-yellow-700 px-3 py-1 rounded-full">{pending} pending</span>

        <button
          onClick={() => rules.forEach((r) => updateRule(r.id, { status: "approved" }))}
          className="ml-auto text-xs text-green-600 hover:underline"
        >
          Approve all
        </button>
        <button
          onClick={() => rules.forEach((r) => updateRule(r.id, { status: "pending" }))}
          className="text-xs text-gray-500 hover:underline"
        >
          Reset all
        </button>
      </div>

      {/* Rules table */}
      <div className="overflow-x-auto border rounded-lg">
        <table className="w-full text-left">
          <thead className="bg-gray-100 text-xs uppercase text-gray-500">
            <tr>
              <th className="px-4 py-3">Rule</th>
              <th className="px-4 py-3">Expression</th>
              <th className="px-4 py-3 text-center">Dimension</th>
              <th className="px-4 py-3 text-center">Severity</th>
              <th className="px-4 py-3 text-center">Source</th>
              <th className="px-4 py-3">Notes</th>
              <th className="px-4 py-3">Action</th>
            </tr>
          </thead>
          <tbody>
            {rules.map((r) => (
              <RuleRow key={r.id} rule={r} />
            ))}
          </tbody>
        </table>
      </div>

      {approved > 0 && pending === 0 && (
        <button
          onClick={() => setStep("done")}
          className="flex items-center gap-2 bg-green-600 text-white px-6 py-2.5 rounded-lg hover:bg-green-700 transition font-medium"
        >
          Sign Off on {approved} Rules
          <ChevronRight className="w-4 h-4" />
        </button>
      )}
      {pending > 0 && (
        <p className="text-xs text-gray-400">
          Review all pending rules before signing off.
        </p>
      )}
    </div>
  );
}
