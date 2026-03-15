export type ReviewItemType = "mappings" | "transformations" | "bq_validations" | "quality_audit";

export interface WorkspaceInfo {
  workspace_path: string;
  tenant_id: string;
  client_id: string;
  thread_id: string;
}

export interface ProfileSample {
  column: string;
  values: string[];
}

export interface ProfileSummary {
  file_name: string | null;
  roster_type_detected: string | null;
  column_count: number;
  sample_size: number;
  profiling_mode: string | null;
  rows_profiled: number;
  rows_total: number;
  samples: ProfileSample[];
  semantic_evidence: unknown[];
  sheet_drift?: Record<string, unknown>;
}

export interface ReviewSectionSummary {
  total: number;
  unchecked: number;
}

export interface ConfidenceSummary {
  high: number;
  medium: number;
  low: number;
}

export interface ReviewSummary {
  total: number;
  unchecked: number;
  sections: {
    mappings: ReviewSectionSummary;
    transformations: ReviewSectionSummary;
    bq_validations: ReviewSectionSummary;
    quality_audit: ReviewSectionSummary;
  };
  confidence: {
    mappings: ConfidenceSummary;
    transformations: ConfidenceSummary;
    bq_validations: ConfidenceSummary;
    quality_audit: ConfidenceSummary;
  };
}

export interface MappingItem {
  id: string;
  source_column?: string;
  target_field?: string;
  approved?: boolean;
  confidence_band?: string;
  [key: string]: unknown;
}

export interface TransformationItem {
  id: string;
  name?: string;
  source_columns?: string[];
  target_fields?: string[];
  approved?: boolean;
  [key: string]: unknown;
}

export interface ValidationItem {
  id: string;
  name?: string;
  message?: string;
  sql_expression?: string;
  approved?: boolean;
  [key: string]: unknown;
}

export interface QualityAuditSuggestedFix {
  action?: string;
  description?: string;
  transform?: string;
  params?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface QualityAuditEvidence {
  [key: string]: unknown;
}

export interface QualityAuditItem {
  id: string;
  category?: string;
  rule_type?: string;
  severity?: "error" | "warning" | "info" | string;
  title?: string;
  message?: string;
  source_column?: string;
  target_field?: string;
  affected_rows?: number;
  affected_pct?: number;
  confidence?: number;
  confidence_band?: string;
  sample_values?: string[];
  evidence?: QualityAuditEvidence;
  suggested_fix?: QualityAuditSuggestedFix;
  approved?: boolean;
  column_key?: string;
  action_group?: string;
  client_impact?: "high" | "medium" | "low" | string;
  column_rank_score?: number;
  [key: string]: unknown;
}

export interface ColumnAuditSummaryRow {
  column_key: string;
  column_label?: string;
  sample_values: string[];
  mapped: boolean;
  profiled: boolean;
  severity_counts: {
    error: number;
    warning: number;
    info: number;
  };
  finding_count: number;
  affected_rows: number;
  affected_pct: number;
  linked_item_ids: string[];
  linked_findings: Array<{
    id: string;
    severity: string;
    title: string;
    message: string;
    action_group: string;
    affected_rows: number;
    affected_pct: number;
  }>;
  recommended_action: string;
  unchecked_count: number;
  column_rank_score: number;
  impact_tier: "high" | "medium" | "low" | string;
}

export interface ColumnAuditSummary {
  generated_at?: string;
  rows_profiled?: number;
  rows_total?: number;
  columns: ColumnAuditSummaryRow[];
  totals?: {
    column_count?: number;
    mapped_count?: number;
    unmapped_count?: number;
    findings_count?: number;
    affected_rows?: number;
  };
}

export interface StandardizationWorkstream {
  id: string;
  title: string;
  narrative?: string;
  column_count: number;
  estimated_rows_impacted: number;
  actions: Array<{
    column_key: string;
    action: string;
    reason: string;
    linked_item_ids: string[];
  }>;
}

export interface StandardizationPlan {
  generated_at?: string;
  workstreams: StandardizationWorkstream[];
  action_counts?: Record<string, number>;
  impact_counts?: Record<string, number>;
  top_priority_columns?: Array<{
    column_key: string;
    recommended_action: string;
    finding_count: number;
    impact_tier: string;
    column_rank_score: number;
  }>;
}

export interface ClientSummary {
  generated_at?: string;
  headline?: string;
  kpis: Record<string, number>;
  top_priority_columns: Array<{
    column_key: string;
    recommended_action: string;
    finding_count: number;
    impact_tier: string;
    column_rank_score: number;
  }>;
  why_it_improves_data_quality: string[];
}

export interface OperationProgress {
  phase?: string;
  message?: string;
  percent?: number;
  elapsed_ms?: number;
}

export interface OperationRecord {
  id: string;
  workspace_id: string;
  kind: string;
  status: string;
  created_at?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  input: Record<string, unknown>;
  result: Record<string, unknown>;
  error: Record<string, unknown>;
  progress?: OperationProgress;
  logs: Array<Record<string, unknown>>;
  request_id?: string;
  parent_operation_id?: string | null;
  cancel_requested?: boolean;
}

export interface PendingRosterChoice {
  id?: string;
  source?: string;
  name?: string;
  path?: string;
  roster_type?: string;
  [key: string]: unknown;
}

export interface FrontendConfig {
  enable_async_operations: boolean;
  enable_sse_progress: boolean;
  enable_web_debug_drawer: boolean;
  poll_interval_ms: number;
  ui_build_id?: string;
}

export interface WorkspaceSnapshot {
  session_id: string;
  workspace_id: string;
  workspace: WorkspaceInfo;
  status: Record<string, unknown>;
  stage: string;
  next_actions: string[];
  profile_summary: ProfileSummary;
  review_summary: ReviewSummary;
  mappings: MappingItem[];
  transformations: TransformationItem[];
  bq_validations: ValidationItem[];
  quality_audit: QualityAuditItem[];
  column_audit_summary?: ColumnAuditSummary;
  standardization_plan?: StandardizationPlan;
  client_summary?: ClientSummary;
  chat_history: Array<{ role: string; content: string }>;
  instructions_context: Record<string, unknown>;
  run_results: unknown[];
  pending_roster_choices: PendingRosterChoice[];
  pending_custom_action: Record<string, unknown>;
  pending_rationale: Record<string, unknown>;
  pending_selected_roster: Record<string, unknown>;
  active_operation_id: string | null;
  operations: OperationRecord[];
  operation_events: Array<Record<string, unknown>>;
  frontend_config: FrontendConfig;
}

export interface ChatResponse {
  type: string;
  message?: string;
  operation?: OperationRecord;
  operation_id?: string | null;
  selected_choice?: PendingRosterChoice;
  analysis_required?: boolean;
  [key: string]: unknown;
}

export type CockpitTab = "schema" | "mappings" | "suggestions" | "transformations" | "bq_validations" | "quality_audit";
