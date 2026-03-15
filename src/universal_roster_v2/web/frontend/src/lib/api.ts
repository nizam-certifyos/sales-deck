import { normalizeChatResponse, normalizeOperationRecord, normalizeWorkspaceSnapshot } from "./adapters";
import type { ChatResponse, OperationRecord, WorkspaceSnapshot } from "./types";

async function request<T>(path: string, method: string, body?: unknown): Promise<T> {
  const response = await fetch(path, {
    method,
    headers: {
      "Content-Type": "application/json",
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload?.detail || JSON.stringify(payload));
  }
  return payload as T;
}

export async function createWorkspace(): Promise<{ workspace_id: string }> {
  return request<{ workspace_id: string }>("/workspaces", "POST", {});
}

export async function getWorkspace(workspaceId: string): Promise<WorkspaceSnapshot> {
  const payload = await request<unknown>(`/workspaces/${encodeURIComponent(workspaceId)}`, "GET");
  return normalizeWorkspaceSnapshot(payload);
}

export async function sendChat(workspaceId: string, message: string): Promise<ChatResponse> {
  const payload = await request<unknown>(`/workspaces/${workspaceId}/chat`, "POST", { message });
  return normalizeChatResponse(payload);
}

export async function sendSystemChat(workspaceId: string, message: string): Promise<ChatResponse> {
  const payload = await request<unknown>(`/workspaces/${workspaceId}/chat/system`, "POST", { message });
  return normalizeChatResponse(payload);
}

export async function uploadFile(
  workspaceId: string,
  file: File,
  options: { roster_type?: string; profile_full_roster_learning?: boolean; profile_max_rows?: number } = {}
): Promise<Record<string, unknown>> {
  const formData = new FormData();
  formData.append("file", file);
  if (options.roster_type) {
    formData.append("roster_type", options.roster_type);
  }
  if (typeof options.profile_full_roster_learning === "boolean") {
    formData.append("profile_full_roster_learning", String(options.profile_full_roster_learning));
  }
  if (typeof options.profile_max_rows === "number") {
    formData.append("profile_max_rows", String(options.profile_max_rows));
  }

  const response = await fetch(`/workspaces/${workspaceId}/upload`, {
    method: "POST",
    body: formData,
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload?.detail || JSON.stringify(payload));
  }
  return payload as Record<string, unknown>;
}

export async function uploadNote(workspaceId: string, file: File): Promise<Record<string, unknown>> {
  const formData = new FormData();
  formData.append("file", file);
  const response = await fetch(`/workspaces/${workspaceId}/notes/upload`, {
    method: "POST",
    body: formData,
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload?.detail || JSON.stringify(payload));
  }
  return payload as Record<string, unknown>;
}

export async function toggleReviewItem(
  workspaceId: string,
  itemType: "mappings" | "transformations" | "bq_validations" | "quality_audit",
  itemId: string,
  approved: boolean
): Promise<Record<string, unknown>> {
  return request<Record<string, unknown>>(`/workspaces/${workspaceId}/toggle`, "POST", {
    item_type: itemType,
    item_id: itemId,
    approved,
  });
}

export async function startOperation(
  workspaceId: string,
  kind: string,
  input: Record<string, unknown>,
  parent_operation_id: string | null = null
): Promise<{ operation: OperationRecord; operation_id: string; request_id: string }> {
  const payload = await request<unknown>(`/workspaces/${workspaceId}/operations`, "POST", {
    kind,
    input,
    parent_operation_id,
  });
  const row = payload as Record<string, unknown>;
  return {
    operation: normalizeOperationRecord(row.operation),
    operation_id: String(row.operation_id || ""),
    request_id: String(row.request_id || ""),
  };
}

export async function fetchOperation(workspaceId: string, operationId: string): Promise<OperationRecord> {
  const payload = await request<unknown>(`/workspaces/${workspaceId}/operations/${operationId}`, "GET");
  return normalizeOperationRecord(payload);
}

export async function cancelOperation(workspaceId: string, operationId: string): Promise<OperationRecord> {
  const payload = await request<unknown>(`/workspaces/${workspaceId}/operations/${operationId}/cancel`, "POST", {});
  return normalizeOperationRecord(payload);
}

export async function exportCsv(workspaceId: string): Promise<"downloaded" | "processing"> {
  const response = await fetch(`/workspaces/${encodeURIComponent(workspaceId)}/export/csv`, { method: "GET" });
  if (response.status === 202) {
    return "processing";
  }
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error((payload as Record<string, unknown>)?.detail as string || "Export failed");
  }
  const blob = await response.blob();
  const disposition = response.headers.get("Content-Disposition") || "";
  const match = disposition.match(/filename="?([^"]+)"?/);
  const filename = match?.[1] || "processed_roster.csv";
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
  return "downloaded";
}

export async function retryOperation(
  workspaceId: string,
  operationId: string
): Promise<{ operation: OperationRecord; operation_id: string; request_id: string }> {
  const payload = await request<Record<string, unknown>>(
    `/workspaces/${workspaceId}/operations/${operationId}/retry`,
    "POST",
    {}
  );
  return {
    operation: normalizeOperationRecord(payload.operation),
    operation_id: String(payload.operation_id || ""),
    request_id: String(payload.request_id || ""),
  };
}
