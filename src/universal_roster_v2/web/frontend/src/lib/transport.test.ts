import { beforeEach, describe, expect, it, vi } from "vitest";
import { WorkspaceTransportManager } from "./transport";

vi.mock("./api", () => ({
  fetchOperation: vi.fn(async () => ({ id: "op-1", status: "running", kind: "suggest" })),
  getWorkspace: vi.fn(async () => ({
    workspace_id: "w-1",
    session_id: "w-1",
    workspace: { workspace_path: "", tenant_id: "", client_id: "", thread_id: "" },
    status: {},
    stage: "in_review",
    next_actions: [],
    profile_summary: {
      file_name: "roster.csv",
      roster_type_detected: "practitioner",
      column_count: 1,
      sample_size: 1,
      profiling_mode: "sample",
      rows_profiled: 1,
      rows_total: 1,
      samples: [],
      semantic_evidence: [],
    },
    review_summary: {
      total: 0,
      unchecked: 0,
      sections: {
        mappings: { total: 0, unchecked: 0 },
        transformations: { total: 0, unchecked: 0 },
        bq_validations: { total: 0, unchecked: 0 },
      },
      confidence: {
        mappings: { high: 0, medium: 0, low: 0 },
        transformations: { high: 0, medium: 0, low: 0 },
        bq_validations: { high: 0, medium: 0, low: 0 },
      },
    },
    mappings: [],
    transformations: [],
    bq_validations: [],
    chat_history: [],
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
      enable_sse_progress: true,
      enable_web_debug_drawer: true,
      poll_interval_ms: 1500,
      ui_build_id: "build",
    },
  })),
}));

class FakeEventSource {
  static instances: FakeEventSource[] = [];

  url: string;
  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;
  listeners = new Map<string, Array<(event: MessageEvent) => void>>();

  constructor(url: string) {
    this.url = url;
    FakeEventSource.instances.push(this);
  }

  addEventListener(type: string, callback: (event: MessageEvent) => void) {
    const existing = this.listeners.get(type) || [];
    existing.push(callback);
    this.listeners.set(type, existing);
  }

  emit(type: string, payload: unknown) {
    const callbacks = this.listeners.get(type) || [];
    const event = { data: JSON.stringify(payload) } as MessageEvent;
    callbacks.forEach((callback) => callback(event));
  }

  close() {
    return undefined;
  }
}

describe("WorkspaceTransportManager", () => {
  beforeEach(() => {
    FakeEventSource.instances = [];
    (globalThis as unknown as { EventSource: typeof FakeEventSource }).EventSource = FakeEventSource;
    vi.useFakeTimers();
  });

  it("falls back to polling when SSE errors", async () => {
    const onModeChange = vi.fn();
    const manager = new WorkspaceTransportManager({
      workspaceId: "w-1",
      pollIntervalMs: 500,
      sseEnabled: true,
      handlers: {
        onModeChange,
        onWorkspace: vi.fn(),
        onOperation: vi.fn(),
        onEvent: vi.fn(),
        onError: vi.fn(),
      },
    });

    manager.start();

    expect(FakeEventSource.instances).toHaveLength(1);
    const instance = FakeEventSource.instances[0];
    instance.onopen?.();
    expect(onModeChange).toHaveBeenCalledWith("sse");

    instance.onerror?.();
    expect(onModeChange).toHaveBeenCalledWith("polling");

    vi.advanceTimersByTime(1000);
    manager.dispose();
  });
});
