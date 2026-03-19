import { useState } from "react";
import { ShieldCheck, Loader2, CheckCircle2, ArrowLeft } from "lucide-react";
import { useStore } from "../store";

export default function SignOff() {
  const {
    catalog, schema, table, rules,
    approvedBy, setApprovedBy, setStep,
  } = useStore();
  const [saving, setSaving] = useState(false);
  const [result, setResult] = useState<{ saved: number; table: string } | null>(null);
  const [error, setError] = useState<string | null>(null);

  const approved = rules.filter((r) => r.status === "approved");

  const handleSave = async () => {
    if (!approvedBy.trim()) return;
    setSaving(true);
    setError(null);
    try {
      const res = await fetch("/api/rules/approve", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          catalog,
          schema_name: schema,
          table,
          approved_by: approvedBy,
          rules: approved.map((r) => ({
            rule_name: r.rule_name,
            rule_description: r.rule_description,
            rule_expression: r.rule_expression,
            rule_type: r.rule_type,
            dimension: r.dimension,
            target_column: r.target_column,
            severity: r.severity,
            source: r.source,
            notes: r.notes,
          })),
        }),
      });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      setResult(data);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  };

  if (result) {
    return (
      <div className="text-center py-12 space-y-4">
        <CheckCircle2 className="w-16 h-16 text-green-500 mx-auto" />
        <h2 className="text-2xl font-bold text-gray-800">Rules Saved Successfully</h2>
        <p className="text-gray-500">
          <span className="font-semibold text-green-600">{result.saved}</span> rules
          approved by <span className="font-semibold">{approvedBy}</span> and saved to
        </p>
        <code className="block text-sm bg-gray-100 px-4 py-2 rounded-lg mx-auto max-w-xl">
          {result.table}
        </code>
        <button
          onClick={() => {
            useStore.getState().clearRules();
            setResult(null);
            setStep("select");
          }}
          className="mt-6 bg-blue-600 text-white px-6 py-2.5 rounded-lg hover:bg-blue-700 transition font-medium"
        >
          Start Over with Another Table
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-lg font-semibold text-gray-800">
          <ShieldCheck className="w-5 h-5 text-green-600" />
          Sign Off
        </div>
        <button
          onClick={() => setStep("review")}
          className="flex items-center gap-1 text-sm text-gray-500 hover:text-gray-700"
        >
          <ArrowLeft className="w-4 h-4" /> Back to review
        </button>
      </div>

      <div className="bg-green-50 border border-green-200 rounded-lg p-6 space-y-4">
        <p className="text-sm text-gray-700">
          You are about to save <span className="font-bold text-green-700">{approved.length}</span>{" "}
          approved data quality rules for{" "}
          <span className="font-mono font-bold">{catalog}.{schema}.{table}</span>.
        </p>

        {/* Summary by dimension */}
        <div className="flex flex-wrap gap-2">
          {["completeness", "accuracy", "validity", "consistency", "uniqueness", "timeliness"].map((dim) => {
            const count = approved.filter((r) => r.dimension === dim).length;
            if (!count) return null;
            return (
              <span key={dim} className="text-xs bg-white border rounded-full px-3 py-1">
                {dim}: {count}
              </span>
            );
          })}
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-600 mb-1">Your Name</label>
          <input
            className="border border-gray-300 rounded-lg px-4 py-2 text-sm w-72 focus:ring-2 focus:ring-green-500 focus:border-green-500"
            placeholder="e.g., Jane Smith"
            value={approvedBy}
            onChange={(e) => setApprovedBy(e.target.value)}
          />
        </div>

        {error && (
          <div className="bg-red-50 text-red-700 text-sm px-4 py-2 rounded">{error}</div>
        )}

        <button
          onClick={handleSave}
          disabled={!approvedBy.trim() || saving}
          className="flex items-center gap-2 bg-green-600 text-white px-6 py-2.5 rounded-lg hover:bg-green-700 transition font-medium disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <ShieldCheck className="w-4 h-4" />}
          Confirm & Save Rules
        </button>
      </div>
    </div>
  );
}
