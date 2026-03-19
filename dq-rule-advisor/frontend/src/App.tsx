import { useStore } from "./store";
import TableSelector from "./components/TableSelector";
import RequirementsInput from "./components/RequirementsInput";
import RuleReview from "./components/RuleReview";
import SignOff from "./components/SignOff";
import { AlertCircle } from "lucide-react";

const STEPS = [
  { key: "select", label: "Select Table" },
  { key: "define", label: "Define Requirements" },
  { key: "review", label: "Review Rules" },
  { key: "done", label: "Sign Off" },
] as const;

export default function App() {
  const { step, error, setError } = useStore();

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b shadow-sm">
        <div className="max-w-6xl mx-auto px-6 py-4 flex items-center justify-between">
          <div>
            <h1 className="text-xl font-bold text-gray-900">DQ Rule Advisor</h1>
            <p className="text-xs text-gray-400">AI-powered data quality rule definition</p>
          </div>

          {/* Step indicator */}
          <div className="flex items-center gap-2">
            {STEPS.map((s, i) => (
              <div key={s.key} className="flex items-center gap-2">
                <div
                  className={`flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium ${
                    step === s.key
                      ? "bg-blue-600 text-white"
                      : STEPS.findIndex((x) => x.key === step) > i
                      ? "bg-blue-100 text-blue-700"
                      : "bg-gray-100 text-gray-400"
                  }`}
                >
                  <span className="w-4 h-4 flex items-center justify-center rounded-full bg-white/20 text-[10px]">
                    {i + 1}
                  </span>
                  {s.label}
                </div>
                {i < STEPS.length - 1 && (
                  <div className="w-6 h-px bg-gray-300" />
                )}
              </div>
            ))}
          </div>
        </div>
      </header>

      {/* Error banner */}
      {error && (
        <div className="max-w-6xl mx-auto px-6 mt-4">
          <div className="flex items-center gap-2 bg-red-50 border border-red-200 text-red-700 text-sm px-4 py-3 rounded-lg">
            <AlertCircle className="w-4 h-4 shrink-0" />
            <span className="flex-1">{error}</span>
            <button onClick={() => setError(null)} className="text-red-400 hover:text-red-600 text-xs">
              Dismiss
            </button>
          </div>
        </div>
      )}

      {/* Main content */}
      <main className="max-w-6xl mx-auto px-6 py-8">
        <div className="bg-white rounded-xl shadow-sm border p-8">
          {step === "select" && <TableSelector />}
          {step === "define" && <RequirementsInput />}
          {step === "review" && <RuleReview />}
          {step === "done" && <SignOff />}
        </div>
      </main>
    </div>
  );
}
