import type {
  ChatResponse,
  ClientSummary,
  ColumnAuditSummary,
  FrontendConfig,
  MappingItem,
  OperationRecord,
  PendingRosterChoice,
  ProfileSummary,
  ReviewSummary,
  StandardizationPlan,
  TransformationItem,
  ValidationItem,
  QualityAuditItem,
  WorkspaceSnapshot,
} from "./types";

function asRecord(input: unknown): Record<string, unknown> {
  return input && typeof input === "object" ? (input as Record<string, unknown>) : {};
}

function asString(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function asNumber(value: unknown, fallback = 0): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function asBoolean(value: unknown, fallback = false): boolean {
  return typeof value === "boolean" ? value : fallback;
}

function asStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === "string") : [];
}

function normalizeProfileSummary(input: unknown): ProfileSummary {
  const data = asRecord(input);
  const samplesRaw = Array.isArray(data.samples) ? data.samples : [];

  return {
    file_name: data.file_name === null ? null : asString(data.file_name, ""),
    roster_type_detected: data.roster_type_detected === null ? null : asString(data.roster_type_detected, ""),
    column_count: asNumber(data.column_count),
    sample_size: asNumber(data.sample_size),
    profiling_mode: data.profiling_mode === null ? null : asString(data.profiling_mode, ""),
    rows_profiled: asNumber(data.rows_profiled),
    rows_total: asNumber(data.rows_total),
    samples: samplesRaw.map((sample) => {
      const row = asRecord(sample);
      return {
        column: asString(row.column),
        values: asStringArray(row.values),
      };
    }),
    semantic_evidence: Array.isArray(data.semantic_evidence) ? data.semantic_evidence : [],
    sheet_drift: asRecord(data.sheet_drift),
  };
}

function normalizeReviewSummary(input: unknown): ReviewSummary {
  const data = asRecord(input);
  const sections = asRecord(data.sections);
  const confidence = asRecord(data.confidence);

  const sectionSummary = (key: string) => {
    const row = asRecord(sections[key]);
    return {
      total: asNumber(row.total),
      unchecked: asNumber(row.unchecked),
    };
  };

  const confidenceSummary = (key: string) => {
    const row = asRecord(confidence[key]);
    return {
      high: asNumber(row.high),
      medium: asNumber(row.medium),
      low: asNumber(row.low),
    };
  };

  return {
    total: asNumber(data.total),
    unchecked: asNumber(data.unchecked),
    sections: {
      mappings: sectionSummary("mappings"),
      transformations: sectionSummary("transformations"),
      bq_validations: sectionSummary("bq_validations"),
      quality_audit: sectionSummary("quality_audit"),
    },
    confidence: {
      mappings: confidenceSummary("mappings"),
      transformations: confidenceSummary("transformations"),
      bq_validations: confidenceSummary("bq_validations"),
      quality_audit: confidenceSummary("quality_audit"),
    },
  };
}

function normalizeMappings(input: unknown): MappingItem[] {
  if (!Array.isArray(input)) {
    return [];
  }
  return input.map((item) => {
    const row = asRecord(item);
    return {
      ...row,
      id: asString(row.id),
      source_column: asString(row.source_column),
      target_field: asString(row.target_field),
      approved: typeof row.approved === "boolean" ? row.approved : true,
      confidence_band: asString(row.confidence_band),
    };
  });
}

function normalizeTransformations(input: unknown): TransformationItem[] {
  if (!Array.isArray(input)) {
    return [];
  }
  return input.map((item) => {
    const row = asRecord(item);
    return {
      ...row,
      id: asString(row.id),
      name: asString(row.name),
      source_columns: asStringArray(row.source_columns),
      target_fields: asStringArray(row.target_fields),
      approved: typeof row.approved === "boolean" ? row.approved : true,
    };
  });
}

function normalizeValidations(input: unknown): ValidationItem[] {
  if (!Array.isArray(input)) {
    return [];
  }
  return input.map((item) => {
    const row = asRecord(item);
    return {
      ...row,
      id: asString(row.id),
      name: asString(row.name),
      message: asString(row.message),
      sql_expression: asString(row.sql_expression),
      approved: typeof row.approved === "boolean" ? row.approved : true,
    };
  });
}

function normalizeQualityAudit(input: unknown): QualityAuditItem[] {
  if (!Array.isArray(input)) {
    return [];
  }
  return input.map((item) => {
    const row = asRecord(item);
    return {
      ...row,
      id: asString(row.id),
      category: asString(row.category),
      rule_type: asString(row.rule_type),
      severity: asString(row.severity),
      title: asString(row.title),
      message: asString(row.message),
      source_column: asString(row.source_column),
      target_field: asString(row.target_field),
      affected_rows: asNumber(row.affected_rows),
      affected_pct: asNumber(row.affected_pct),
      confidence: asNumber(row.confidence),
      confidence_band: asString(row.confidence_band),
      sample_values: asStringArray(row.sample_values),
      evidence: asRecord(row.evidence),
      suggested_fix: asRecord(row.suggested_fix),
      approved: typeof row.approved === "boolean" ? row.approved : true,
      column_key: asString(row.column_key),
      action_group: asString(row.action_group),
      client_impact: asString(row.client_impact),
      column_rank_score: asNumber(row.column_rank_score),
    };
  });
}

function normalizeColumnAuditSummary(input: unknown): ColumnAuditSummary {
  const row = asRecord(input);
  const columnsRaw = Array.isArray(row.columns) ? row.columns : [];
  const totals = asRecord(row.totals);
  return {
    generated_at: asString(row.generated_at),
    rows_profiled: asNumber(row.rows_profiled),
    rows_total: asNumber(row.rows_total),
    columns: columnsRaw.map((entry) => {
      const col = asRecord(entry);
      const severity = asRecord(col.severity_counts);
      const linkedFindingsRaw = Array.isArray(col.linked_findings) ? col.linked_findings : [];
      return {
        column_key: asString(col.column_key),
        column_label: asString(col.column_label),
        sample_values: asStringArray(col.sample_values),
        mapped: asBoolean(col.mapped),
        profiled: asBoolean(col.profiled),
        severity_counts: {
          error: asNumber(severity.error),
          warning: asNumber(severity.warning),
          info: asNumber(severity.info),
        },
        finding_count: asNumber(col.finding_count),
        affected_rows: asNumber(col.affected_rows),
        affected_pct: asNumber(col.affected_pct),
        linked_item_ids: asStringArray(col.linked_item_ids),
        linked_findings: linkedFindingsRaw.map((finding) => {
          const f = asRecord(finding);
          return {
            id: asString(f.id),
            severity: asString(f.severity),
            title: asString(f.title),
            message: asString(f.message),
            action_group: asString(f.action_group),
            affected_rows: asNumber(f.affected_rows),
            affected_pct: asNumber(f.affected_pct),
          };
        }),
        recommended_action: asString(col.recommended_action),
        unchecked_count: asNumber(col.unchecked_count),
        column_rank_score: asNumber(col.column_rank_score),
        impact_tier: asString(col.impact_tier),
      };
    }),
    totals: {
      column_count: asNumber(totals.column_count),
      mapped_count: asNumber(totals.mapped_count),
      unmapped_count: asNumber(totals.unmapped_count),
      findings_count: asNumber(totals.findings_count),
      affected_rows: asNumber(totals.affected_rows),
    },
  };
}

function normalizeStandardizationPlan(input: unknown): StandardizationPlan {
  const row = asRecord(input);
  const workstreamsRaw = Array.isArray(row.workstreams) ? row.workstreams : [];
  const topColumnsRaw = Array.isArray(row.top_priority_columns) ? row.top_priority_columns : [];
  const actionCountsRaw = asRecord(row.action_counts);
  const impactCountsRaw = asRecord(row.impact_counts);
  const action_counts: Record<string, number> = {};
  const impact_counts: Record<string, number> = {};
  for (const [key, value] of Object.entries(actionCountsRaw)) {
    action_counts[key] = asNumber(value);
  }
  for (const [key, value] of Object.entries(impactCountsRaw)) {
    impact_counts[key] = asNumber(value);
  }
  return {
    generated_at: asString(row.generated_at),
    workstreams: workstreamsRaw.map((entry) => {
      const ws = asRecord(entry);
      const actionsRaw = Array.isArray(ws.actions) ? ws.actions : [];
      return {
        id: asString(ws.id),
        title: asString(ws.title),
        narrative: asString(ws.narrative),
        column_count: asNumber(ws.column_count),
        estimated_rows_impacted: asNumber(ws.estimated_rows_impacted),
        actions: actionsRaw.map((action) => {
          const item = asRecord(action);
          return {
            column_key: asString(item.column_key),
            action: asString(item.action),
            reason: asString(item.reason),
            linked_item_ids: asStringArray(item.linked_item_ids),
          };
        }),
      };
    }),
    action_counts,
    impact_counts,
    top_priority_columns: topColumnsRaw.map((entry) => {
      const item = asRecord(entry);
      return {
        column_key: asString(item.column_key),
        recommended_action: asString(item.recommended_action),
        finding_count: asNumber(item.finding_count),
        impact_tier: asString(item.impact_tier),
        column_rank_score: asNumber(item.column_rank_score),
      };
    }),
  };
}

function normalizeClientSummary(input: unknown): ClientSummary {
  const row = asRecord(input);
  const topColumnsRaw = Array.isArray(row.top_priority_columns) ? row.top_priority_columns : [];
  const kpisRaw = asRecord(row.kpis);
  const kpis: Record<string, number> = {};
  for (const [key, value] of Object.entries(kpisRaw)) {
    kpis[key] = asNumber(value);
  }
  return {
    generated_at: asString(row.generated_at),
    headline: asString(row.headline),
    kpis,
    top_priority_columns: topColumnsRaw.map((entry) => {
      const item = asRecord(entry);
      return {
        column_key: asString(item.column_key),
        recommended_action: asString(item.recommended_action),
        finding_count: asNumber(item.finding_count),
        impact_tier: asString(item.impact_tier),
        column_rank_score: asNumber(item.column_rank_score),
      };
    }),
    why_it_improves_data_quality: asStringArray(row.why_it_improves_data_quality),
  };
}

export function normalizeOperationRecord(input: unknown): OperationRecord {
  const row = asRecord(input);
  return {
    id: asString(row.id),
    workspace_id: asString(row.workspace_id),
    kind: asString(row.kind),
    status: asString(row.status, "queued"),
    created_at: asString(row.created_at, "") || null,
    started_at: asString(row.started_at, "") || null,
    finished_at: asString(row.finished_at, "") || null,
    input: asRecord(row.input),
    result: asRecord(row.result),
    error: asRecord(row.error),
    progress: asRecord(row.progress),
    logs: Array.isArray(row.logs) ? row.logs.map((entry) => asRecord(entry)) : [],
    request_id: asString(row.request_id),
    parent_operation_id: asString(row.parent_operation_id, "") || null,
    cancel_requested: asBoolean(row.cancel_requested),
  };
}

function normalizeFrontendConfig(input: unknown): FrontendConfig {
  const row = asRecord(input);
  return {
    enable_async_operations: asBoolean(row.enable_async_operations, true),
    enable_sse_progress: asBoolean(row.enable_sse_progress, true),
    enable_web_debug_drawer: asBoolean(row.enable_web_debug_drawer, true),
    poll_interval_ms: Math.max(250, asNumber(row.poll_interval_ms, 1500)),
    ui_build_id: asString(row.ui_build_id),
  };
}

function normalizePendingChoices(input: unknown): PendingRosterChoice[] {
  if (!Array.isArray(input)) {
    return [];
  }
  return input.map((item) => {
    const row = asRecord(item);
    return {
      ...row,
      id: asString(row.id),
      source: asString(row.source),
      name: asString(row.name),
      path: asString(row.path),
      roster_type: asString(row.roster_type),
    };
  });
}

export function normalizeWorkspaceSnapshot(input: unknown): WorkspaceSnapshot {
  const row = asRecord(input);
  const workspace = asRecord(row.workspace);

  return {
    session_id: asString(row.session_id),
    workspace_id: asString(row.workspace_id),
    workspace: {
      workspace_path: asString(workspace.workspace_path),
      tenant_id: asString(workspace.tenant_id),
      client_id: asString(workspace.client_id),
      thread_id: asString(workspace.thread_id),
    },
    status: asRecord(row.status),
    stage: asString(row.stage),
    next_actions: asStringArray(row.next_actions),
    profile_summary: normalizeProfileSummary(row.profile_summary),
    review_summary: normalizeReviewSummary(row.review_summary),
    mappings: normalizeMappings(row.mappings),
    transformations: normalizeTransformations(row.transformations),
    bq_validations: normalizeValidations(row.bq_validations),
    quality_audit: normalizeQualityAudit(row.quality_audit),
    column_audit_summary: normalizeColumnAuditSummary(row.column_audit_summary),
    standardization_plan: normalizeStandardizationPlan(row.standardization_plan),
    client_summary: normalizeClientSummary(row.client_summary),
    chat_history: Array.isArray(row.chat_history)
      ? row.chat_history.map((entry) => {
          const item = asRecord(entry);
          return {
            role: asString(item.role),
            content: asString(item.content),
          };
        })
      : [],
    instructions_context: asRecord(row.instructions_context),
    run_results: Array.isArray(row.run_results) ? row.run_results : [],
    pending_roster_choices: normalizePendingChoices(row.pending_roster_choices),
    pending_custom_action: asRecord(row.pending_custom_action),
    pending_rationale: asRecord(row.pending_rationale),
    pending_selected_roster: asRecord(row.pending_selected_roster),
    active_operation_id: asString(row.active_operation_id, "") || null,
    operations: Array.isArray(row.operations) ? row.operations.map((op) => normalizeOperationRecord(op)) : [],
    operation_events: Array.isArray(row.operation_events)
      ? row.operation_events.map((event) => asRecord(event))
      : [],
    frontend_config: normalizeFrontendConfig(row.frontend_config),
  };
}

export function normalizeChatResponse(input: unknown): ChatResponse {
  const row = asRecord(input);
  const operation = row.operation ? normalizeOperationRecord(row.operation) : undefined;
  return {
    ...row,
    type: asString(row.type),
    message: asString(row.message),
    operation,
    operation_id: asString(row.operation_id, "") || null,
    selected_choice: row.selected_choice ? (asRecord(row.selected_choice) as PendingRosterChoice) : undefined,
    analysis_required: asBoolean(row.analysis_required),
  };
}
