import { create } from "zustand";
import type { DQRule, ColumnInfo } from "./types";

interface AppState {
  // Table selection
  catalog: string;
  schema: string;
  table: string;
  columns: ColumnInfo[];
  setCatalog: (v: string) => void;
  setSchema: (v: string) => void;
  setTable: (v: string) => void;
  setColumns: (v: ColumnInfo[]) => void;

  // Rules
  rules: DQRule[];
  addRules: (rules: DQRule[]) => void;
  updateRule: (id: string, updates: Partial<DQRule>) => void;
  clearRules: () => void;

  // UI state
  step: "select" | "define" | "review" | "done";
  setStep: (s: AppState["step"]) => void;
  loading: string | null;
  setLoading: (v: string | null) => void;
  error: string | null;
  setError: (v: string | null) => void;

  // Approval
  approvedBy: string;
  setApprovedBy: (v: string) => void;
}

let ruleIdCounter = 0;

export const useStore = create<AppState>((set) => ({
  catalog: "",
  schema: "",
  table: "",
  columns: [],
  setCatalog: (v) => set({ catalog: v, schema: "", table: "", columns: [], rules: [] }),
  setSchema: (v) => set({ schema: v, table: "", columns: [], rules: [] }),
  setTable: (v) => set({ table: v }),
  setColumns: (v) => set({ columns: v }),

  rules: [],
  addRules: (newRules) =>
    set((state) => ({
      rules: [
        ...state.rules,
        ...newRules.map((r) => ({
          ...r,
          id: `rule-${++ruleIdCounter}`,
          status: "pending" as const,
          notes: r.notes || "",
        })),
      ],
    })),
  updateRule: (id, updates) =>
    set((state) => ({
      rules: state.rules.map((r) => (r.id === id ? { ...r, ...updates } : r)),
    })),
  clearRules: () => set({ rules: [] }),

  step: "select",
  setStep: (s) => set({ step: s }),
  loading: null,
  setLoading: (v) => set({ loading: v }),
  error: null,
  setError: (v) => set({ error: v }),

  approvedBy: "",
  setApprovedBy: (v) => set({ approvedBy: v }),
}));
