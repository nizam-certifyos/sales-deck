import { fetchOperation, getWorkspace } from "./api";
import type { OperationRecord, WorkspaceSnapshot } from "./types";

export type TransportMode = "connecting" | "sse" | "polling";

export interface TransportHandlers {
  onModeChange: (mode: TransportMode) => void;
  onWorkspace: (snapshot: WorkspaceSnapshot) => void;
  onOperation: (operation: OperationRecord) => void;
  onEvent: (payload: Record<string, unknown>) => void;
  onError: (error: Error) => void;
}

export interface TransportConfig {
  workspaceId: string;
  pollIntervalMs: number;
  sseEnabled: boolean;
  handlers: TransportHandlers;
}

export class WorkspaceTransportManager {
  private workspaceId: string;
  private handlers: TransportHandlers;
  private pollIntervalMs: number;
  private sseEnabled: boolean;
  private source: EventSource | null = null;
  private pollTimer: number | null = null;
  private currentMode: TransportMode = "connecting";
  private activeOperationId: string | null = null;
  private disposed = false;

  constructor(config: TransportConfig) {
    this.workspaceId = config.workspaceId;
    this.handlers = config.handlers;
    this.pollIntervalMs = config.pollIntervalMs;
    this.sseEnabled = config.sseEnabled;
  }

  start() {
    if (this.disposed) {
      return;
    }
    if (!this.sseEnabled) {
      this.setMode("polling");
      this.startPolling();
      return;
    }
    this.connectSSE();
  }

  updateConfig(pollIntervalMs: number, sseEnabled: boolean) {
    this.pollIntervalMs = Math.max(250, pollIntervalMs);
    if (this.sseEnabled === sseEnabled) {
      if (!this.sseEnabled) {
        this.startPolling();
      }
      return;
    }
    this.sseEnabled = sseEnabled;
    if (this.sseEnabled) {
      this.stopPolling();
      this.connectSSE();
    } else {
      this.closeSSE();
      this.setMode("polling");
      this.startPolling();
    }
  }

  setActiveOperation(operationId: string | null) {
    this.activeOperationId = operationId;
  }

  dispose() {
    this.disposed = true;
    this.closeSSE();
    this.stopPolling();
  }

  private setMode(mode: TransportMode) {
    if (this.currentMode === mode) {
      return;
    }
    this.currentMode = mode;
    this.handlers.onModeChange(mode);
  }

  private connectSSE() {
    this.closeSSE();
    this.setMode("connecting");

    const source = new EventSource(`/workspaces/${this.workspaceId}/events`);
    this.source = source;

    source.onopen = () => {
      this.setMode("sse");
      this.stopPolling();
    };

    source.onerror = () => {
      this.closeSSE();
      this.setMode("polling");
      this.startPolling();
    };

    source.addEventListener("operation", async (event: MessageEvent) => {
      try {
        const payload = JSON.parse(event.data) as Record<string, unknown>;
        this.handlers.onEvent(payload);

        const operationId = String(payload.operation_id || "");
        if (operationId) {
          const operation = await fetchOperation(this.workspaceId, operationId);
          this.handlers.onOperation(operation);
        }

        const snapshot = await getWorkspace(this.workspaceId);
        this.handlers.onWorkspace(snapshot);
      } catch (error) {
        this.handlers.onError(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  private closeSSE() {
    if (this.source) {
      this.source.close();
      this.source = null;
    }
  }

  private stopPolling() {
    if (this.pollTimer !== null) {
      window.clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
  }

  private startPolling() {
    this.stopPolling();
    this.pollTimer = window.setInterval(async () => {
      try {
        if (this.activeOperationId) {
          const operation = await fetchOperation(this.workspaceId, this.activeOperationId);
          this.handlers.onOperation(operation);
        }
        const snapshot = await getWorkspace(this.workspaceId);
        this.handlers.onWorkspace(snapshot);
      } catch (error) {
        this.handlers.onError(error instanceof Error ? error : new Error(String(error)));
      }
    }, this.pollIntervalMs);
  }
}
