import { describe, expect, it } from "vitest";
import { normalizeWorkspaceSnapshot } from "./adapters";

describe("normalizeWorkspaceSnapshot", () => {
  it("normalizes contract sections with defaults", () => {
    const snapshot = normalizeWorkspaceSnapshot({
      session_id: "w-1",
      workspace_id: "w-1",
      workspace: {
        workspace_path: "/tmp/work",
        tenant_id: "tenant",
        client_id: "client",
        thread_id: "thread",
      },
      profile_summary: {
        file_name: "roster.csv",
        roster_type_detected: "practitioner",
        column_count: 20,
        sample_size: 50,
        profiling_mode: "sample",
        rows_profiled: 50,
        rows_total: 120,
        samples: [{ column: "npi", values: ["123"] }],
        semantic_evidence: [],
      },
      review_summary: {
        total: 3,
        unchecked: 1,
        sections: {
          mappings: { total: 1, unchecked: 0 },
          transformations: { total: 1, unchecked: 1 },
          bq_validations: { total: 1, unchecked: 0 },
          quality_audit: { total: 1, unchecked: 0 },
        },
        confidence: {
          mappings: { high: 1, medium: 0, low: 0 },
          transformations: { high: 0, medium: 1, low: 0 },
          bq_validations: { high: 0, medium: 0, low: 1 },
          quality_audit: { high: 0, medium: 1, low: 0 },
        },
      },
      mappings: [{ id: "m-1", source_column: "src", target_field: "dst", approved: true }],
      transformations: [{ id: "t-1", name: "trim", approved: false }],
      bq_validations: [{ id: "v-1", name: "required", approved: true }],
      quality_audit: [{ id: "qa-1", severity: "warning", suggested_fix: { action: "review" }, evidence: { rule: "x" }, sample_values: ["a"] }],
      chat_history: [{ role: "assistant", content: "hello" }],
      pending_roster_choices: [{ id: "c-1", path: "__browser_upload__0" }],
      operations: [
        {
          id: "op-1",
          workspace_id: "w-1",
          kind: "suggest",
          status: "running",
          input: {},
          result: {},
          error: {},
          progress: { phase: "analysis", message: "Running", percent: 50 },
          logs: [],
        },
      ],
      operation_events: [],
      frontend_config: {
        enable_async_operations: true,
        enable_sse_progress: true,
        enable_web_debug_drawer: true,
        poll_interval_ms: 1500,
        ui_build_id: "build",
      },
    });

    expect(snapshot.workspace_id).toBe("w-1");
    expect(snapshot.mappings).toHaveLength(1);
    expect(snapshot.transformations[0].approved).toBe(false);
    expect(snapshot.bq_validations[0].id).toBe("v-1");
    expect(snapshot.review_summary.sections.mappings.total).toBe(1);
    expect(snapshot.quality_audit[0].suggested_fix?.action).toBe("review");
    expect(snapshot.quality_audit[0].sample_values).toEqual(["a"]);
    expect(snapshot.pending_roster_choices[0].path).toBe("__browser_upload__0");
    expect(snapshot.operations[0].progress?.percent).toBe(50);
    expect(snapshot.frontend_config.poll_interval_ms).toBe(1500);
    expect(snapshot.column_audit_summary?.columns).toEqual([]);
    expect(snapshot.standardization_plan?.workstreams).toEqual([]);
    expect(snapshot.client_summary?.kpis).toEqual({});
  });

  it("normalizes additive audit structures", () => {
    const snapshot = normalizeWorkspaceSnapshot({
      session_id: "w-3",
      workspace_id: "w-3",
      workspace: {},
      column_audit_summary: {
        columns: [
          {
            column_key: "Email",
            column_label: "Email",
            sample_values: ["a@example.com"],
            mapped: true,
            profiled: true,
            severity_counts: { error: 1, warning: 0, info: 0 },
            finding_count: 1,
            affected_rows: 2,
            affected_pct: 0.2,
            linked_item_ids: ["qa::email"],
            linked_findings: [
              {
                id: "qa::email",
                severity: "error",
                title: "Email format",
                message: "Invalid email",
                action_group: "transform",
                affected_rows: 2,
                affected_pct: 0.2,
              },
            ],
            recommended_action: "transform",
            unchecked_count: 0,
            column_rank_score: 21.5,
            impact_tier: "high",
          },
        ],
      },
      standardization_plan: {
        workstreams: [
          {
            id: "format_normalization",
            title: "Format normalization",
            column_count: 1,
            estimated_rows_impacted: 2,
            actions: [
              {
                column_key: "Email",
                action: "transform",
                reason: "Fix invalid format",
                linked_item_ids: ["qa::email"],
              },
            ],
          },
        ],
      },
      client_summary: {
        kpis: { columns_profiled: 1, readiness_score: 82 },
        top_priority_columns: [
          {
            column_key: "Email",
            recommended_action: "transform",
            finding_count: 1,
            impact_tier: "high",
            column_rank_score: 21.5,
          },
        ],
        why_it_improves_data_quality: ["Linked actions are explicit"],
      },
      frontend_config: {},
    });

    expect(snapshot.column_audit_summary?.columns[0].column_key).toBe("Email");
    expect(snapshot.standardization_plan?.workstreams[0].id).toBe("format_normalization");
    expect(snapshot.client_summary?.kpis.readiness_score).toBe(82);
  });

  it("fills defaults for missing optional contract fields", () => {
    const snapshot = normalizeWorkspaceSnapshot({
      session_id: "w-2",
      workspace_id: "w-2",
      workspace: {},
      frontend_config: {},
    });

    expect(snapshot.mappings).toEqual([]);
    expect(snapshot.transformations).toEqual([]);
    expect(snapshot.bq_validations).toEqual([]);
    expect(snapshot.review_summary.total).toBe(0);
    expect(snapshot.frontend_config.poll_interval_ms).toBeGreaterThanOrEqual(250);
  });
});
