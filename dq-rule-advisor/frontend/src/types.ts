export interface DQRule {
  id: string;
  rule_name: string;
  rule_description: string;
  rule_expression: string;
  rule_type: string;
  dimension: string;
  target_column: string | null;
  severity: string;
  source: "user_requirement" | "ai_analysis" | "manual";
  status: "pending" | "approved" | "rejected";
  notes: string;
}

export interface ColumnInfo {
  name: string;
  type: string;
  comment: string;
}
