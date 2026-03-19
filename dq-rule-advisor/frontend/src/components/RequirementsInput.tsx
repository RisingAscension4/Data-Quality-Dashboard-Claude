import { useState } from "react";
import { Sparkles, ScanSearch, ChevronRight, ArrowLeft, Loader2 } from "lucide-react";
import { useStore } from "../store";

export default function RequirementsInput() {
  const {
    catalog, schema, table,
    addRules, setStep, loading, setLoading, setError,
  } = useStore();
  const [requirements, setRequirements] = useState("");

  const handleGenerate = async () => {
    if (!requirements.trim()) return;
    setLoading("Generating rules from your requirements...");
    setError(null);
    try {
      const res = await fetch("/api/rules/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          catalog,
          schema_name: schema,
          table,
          requirements,
        }),
      });
      if (!res.ok) throw new Error(await res.text());
      const rules = await res.json();
      addRules(rules);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(null);
    }
  };

  const handleAnalyze = async () => {
    setLoading("Analyzing table schema and data...");
    setError(null);
    try {
      const res = await fetch("/api/rules/analyze", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          catalog,
          schema_name: schema,
          table,
        }),
      });
      if (!res.ok) throw new Error(await res.text());
      const rules = await res.json();
      addRules(rules);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(null);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="text-lg font-semibold text-gray-800">
          Define Requirements for{" "}
          <span className="font-mono text-blue-600">
            {catalog}.{schema}.{table}
          </span>
        </div>
        <button
          onClick={() => setStep("select")}
          className="flex items-center gap-1 text-sm text-gray-500 hover:text-gray-700"
        >
          <ArrowLeft className="w-4 h-4" /> Change table
        </button>
      </div>

      {/* Natural language input */}
      <div>
        <label className="block text-sm font-medium text-gray-600 mb-2">
          Describe your data quality requirements in plain English
        </label>
        <textarea
          className="w-full border border-gray-300 rounded-lg px-4 py-3 text-sm h-40 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 resize-y"
          placeholder={`Examples:\n- Patient ID should never be null\n- Age must be between 0 and 150\n- Email addresses must be in valid format\n- Discharge date must be after admission date\n- All diagnosis codes must follow ICD-10 format`}
          value={requirements}
          onChange={(e) => setRequirements(e.target.value)}
          disabled={!!loading}
        />
      </div>

      <div className="flex gap-3">
        <button
          onClick={handleGenerate}
          disabled={!requirements.trim() || !!loading}
          className="flex items-center gap-2 bg-blue-600 text-white px-5 py-2.5 rounded-lg hover:bg-blue-700 transition font-medium disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {loading?.includes("requirements") ? (
            <Loader2 className="w-4 h-4 animate-spin" />
          ) : (
            <Sparkles className="w-4 h-4" />
          )}
          Generate Rules from Requirements
        </button>

        <button
          onClick={handleAnalyze}
          disabled={!!loading}
          className="flex items-center gap-2 bg-emerald-600 text-white px-5 py-2.5 rounded-lg hover:bg-emerald-700 transition font-medium disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {loading?.includes("Analyzing") ? (
            <Loader2 className="w-4 h-4 animate-spin" />
          ) : (
            <ScanSearch className="w-4 h-4" />
          )}
          Auto-Analyze Table
        </button>
      </div>

      {loading && (
        <div className="flex items-center gap-2 text-sm text-blue-600 bg-blue-50 rounded-lg px-4 py-3">
          <Loader2 className="w-4 h-4 animate-spin" />
          {loading}
        </div>
      )}

      {useStore.getState().rules.length > 0 && (
        <button
          onClick={() => setStep("review")}
          className="flex items-center gap-2 bg-gray-800 text-white px-6 py-2.5 rounded-lg hover:bg-gray-900 transition font-medium"
        >
          Review {useStore.getState().rules.length} Rules
          <ChevronRight className="w-4 h-4" />
        </button>
      )}
    </div>
  );
}
