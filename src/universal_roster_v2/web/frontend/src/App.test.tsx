import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";

const workspacePayload = {
  session_id: "w-1",
  workspace_id: "w-1",
  workspace: { workspace_path: "", tenant_id: "", client_id: "", thread_id: "" },
  status: {},
  stage: "in_review",
  next_actions: [],
  profile_summary: {
    file_name: "providers.csv",
    roster_type_detected: "practitioner",
    column_count: 2,
    sample_size: 2,
    profiling_mode: "sample",
    rows_profiled: 2,
    rows_total: 2,
    samples: [],
    semantic_evidence: [],
  },
  review_summary: {
    total: 1,
    unchecked: 1,
    sections: {
      mappings: { total: 1, unchecked: 1 },
      transformations: { total: 0, unchecked: 0 },
      bq_validations: { total: 0, unchecked: 0 },
      quality_audit: { total: 1, unchecked: 0 },
    },
    confidence: {
      mappings: { high: 1, medium: 0, low: 0 },
      transformations: { high: 0, medium: 0, low: 0 },
      bq_validations: { high: 0, medium: 0, low: 0 },
      quality_audit: { high: 0, medium: 1, low: 0 },
    },
  },
  mappings: [{ id: "m-1", source_column: "src", target_field: "dst", approved: true, confidence_band: "high" }],
  transformations: [],
  bq_validations: [],
  quality_audit: [
    {
      id: "qa-1",
      severity: "warning",
      source_column: "Email",
      target_field: "primaryEmail",
      suggested_fix: { action: "review", description: "Review" },
      evidence: { rule: "x" },
      sample_values: ["a"],
      approved: true,
    },
  ],
  column_audit_summary: {
    columns: [
      {
        column_key: "Email",
        column_label: "Email",
        sample_values: ["a@example.com"],
        mapped: true,
        profiled: true,
        severity_counts: { error: 0, warning: 1, info: 0 },
        finding_count: 1,
        affected_rows: 1,
        affected_pct: 0.5,
        linked_item_ids: ["qa-1"],
        linked_findings: [
          {
            id: "qa-1",
            severity: "warning",
            title: "Email finding",
            message: "Needs review",
            action_group: "review",
            affected_rows: 1,
            affected_pct: 0.5,
          },
        ],
        recommended_action: "review",
        unchecked_count: 0,
        column_rank_score: 12,
        impact_tier: "medium",
      },
    ],
  },
  standardization_plan: {
    workstreams: [
      {
        id: "dedupe_identity",
        title: "Deduplication and identity",
        column_count: 1,
        estimated_rows_impacted: 1,
        actions: [{ column_key: "Email", action: "review", reason: "Needs review", linked_item_ids: ["qa-1"] }],
      },
    ],
  },
  client_summary: {
    kpis: { readiness_score: 80, columns_profiled: 1 },
    top_priority_columns: [
      {
        column_key: "Email",
        recommended_action: "review",
        finding_count: 1,
        impact_tier: "medium",
        column_rank_score: 12,
      },
    ],
    why_it_improves_data_quality: ["Example"],
  },
  chat_history: [{ role: "assistant", content: "hello" }],
  instructions_context: {},
  run_results: [],
  pending_roster_choices: [],
  pending_custom_action: {},
  pending_rationale: {},
  pending_selected_roster: {},
  active_operation_id: null,
  operations: [],
  operation_events: [],
  frontend_config: {
    enable_async_operations: true,
    enable_sse_progress: false,
    enable_web_debug_drawer: true,
    poll_interval_ms: 1500,
    ui_build_id: "test-build",
  },
};

class FakeEventSource {
  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;
  constructor(_url: string) {}
  addEventListener() {}
  close() {}
}

describe("App integration", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    vi.stubGlobal("EventSource", FakeEventSource);
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      if (path.endsWith("/workspaces") && init?.method === "POST") {
        return {
          ok: true,
          json: async () => ({ workspace_id: "w-1" }),
        } as Response;
      }
      if (path.includes("/workspaces/w-1") && (!init || init.method === "GET")) {
        return {
          ok: true,
          json: async () => workspacePayload,
        } as Response;
      }
      if (path.endsWith("/toggle")) {
        return {
          ok: true,
          json: async () => ({ ok: true }),
        } as Response;
      }
      return {
        ok: true,
        json: async () => ({}),
      } as Response;
    });
    vi.stubGlobal("fetch", fetchMock);
    Object.defineProperty(window, "localStorage", {
      value: {
        _store: {} as Record<string, string>,
        getItem(key: string) {
          return this._store[key] ?? null;
        },
        setItem(key: string, value: string) {
          this._store[key] = value;
        },
        removeItem(key: string) {
          delete this._store[key];
        },
        clear() {
          this._store = {};
        },
      },
      configurable: true,
    });
    window.localStorage.clear();
  });

  it("renders cockpit tabs and toggles mapping checkbox", async () => {
    render(<App />);

    await waitFor(() => expect(screen.getByText(/Workspace: w-1/)).toBeInTheDocument());
    expect(screen.getByRole("button", { name: "Mappings" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Mappings" }));
    const checkbox = await screen.findByRole("checkbox");
    fireEvent.click(checkbox);

    await waitFor(() => {
      const fetchCalls = (globalThis.fetch as unknown as ReturnType<typeof vi.fn>).mock.calls;
      const hasToggleCall = fetchCalls.some((call) => String(call[0]).endsWith("/toggle"));
      expect(hasToggleCall).toBe(true);
    });
  });
});
