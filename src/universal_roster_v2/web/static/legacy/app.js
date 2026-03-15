const PENDING_ROSTER_CONTROL_PREFIX = "__set_pending_roster_choices__";
const ROSTER_EXTENSIONS = new Set([".csv", ".xlsx", ".xls"]);
const NOTE_EXTENSIONS = new Set([".txt", ".md", ".json", ".csv"]);

const appState = {
  workspaceId: null,
  workspace: null,
  pendingRosterUploads: new Map(),
  isSending: false,
  isUploading: false,
  activeOperationId: null,
  recentOperationId: null,
  localActivity: null,
  operationsById: new Map(),
  sse: null,
  sseConnected: false,
  transportMode: "connecting",
  pollTimer: null,
  pollIntervalMs: 1500,
  currentReviewTab: "schema",
  debugEvents: [],
  debugRequests: [],
};

const refs = {
  chatShell: document.querySelector(".chat-shell"),
  workspaceLabel: document.getElementById("workspaceLabel"),
  transportBadge: document.getElementById("transportBadge"),
  operationCard: document.getElementById("operationCard"),
  operationKind: document.getElementById("operationKind"),
  operationPhase: document.getElementById("operationPhase"),
  operationStatus: document.getElementById("operationStatus"),
  operationTrack: document.getElementById("operationTrack"),
  operationBarFill: document.getElementById("operationBarFill"),
  cancelOperationButton: document.getElementById("cancelOperationButton"),
  retryOperationButton: document.getElementById("retryOperationButton"),
  pendingChoicePanel: document.getElementById("pendingChoicePanel"),
  pendingChoiceButtons: document.getElementById("pendingChoiceButtons"),
  chatMessages: document.getElementById("chatMessages"),
  chatInput: document.getElementById("chatInput"),
  sendButton: document.getElementById("sendButton"),
  uploadInput: document.getElementById("uploadInput"),
  uploadButton: document.getElementById("uploadButton"),
  reviewCockpit: document.getElementById("reviewCockpit"),
  reviewStatus: document.getElementById("reviewStatus"),
  reviewTabSchema: document.getElementById("reviewTabSchema"),
  reviewTabTransformations: document.getElementById("reviewTabTransformations"),
  reviewTabValidations: document.getElementById("reviewTabValidations"),
  reviewPanelSchema: document.getElementById("reviewPanelSchema"),
  reviewPanelTransformations: document.getElementById("reviewPanelTransformations"),
  reviewPanelValidations: document.getElementById("reviewPanelValidations"),
  reviewSchemaContent: document.getElementById("reviewSchemaContent"),
  reviewTransformationsContent: document.getElementById("reviewTransformationsContent"),
  reviewValidationsContent: document.getElementById("reviewValidationsContent"),
  debugToggle: document.getElementById("debugToggle"),
  debugDrawer: document.getElementById("debugDrawer"),
  debugClose: document.getElementById("debugClose"),
  debugContent: document.getElementById("debugContent"),
};

function setUploadButtonDisabled(disabled) {
  refs.uploadButton?.setAttribute("aria-disabled", disabled ? "true" : "false");
  refs.uploadButton?.setAttribute("tabindex", disabled ? "-1" : "0");
}

function currentOperation() {
  if (!appState.activeOperationId) {
    return null;
  }
  return appState.operationsById.get(appState.activeOperationId) || null;
}

function escapeHtml(input) {
  return String(input)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function pushDebug(kind, payload) {
  appState.debugEvents.push({ at: new Date().toISOString(), kind, payload });
  if (appState.debugEvents.length > 200) {
    appState.debugEvents.splice(0, appState.debugEvents.length - 200);
  }
  renderDebugDrawer();
}

function syncUiState() {
  const ready = Boolean(appState.workspaceId);
  const busy = Boolean(currentOperation());
  refs.chatInput.disabled = !ready;
  refs.sendButton.disabled = !ready || appState.isSending || appState.isUploading;
  refs.uploadInput.disabled = !ready || appState.isUploading;
  refs.sendButton.classList.toggle("is-loading", appState.isSending);
  setUploadButtonDisabled(!ready || appState.isUploading);
  refs.uploadButton?.classList.toggle("is-loading", appState.isUploading);
  refs.chatShell?.setAttribute("aria-busy", busy || appState.isSending || appState.isUploading ? "true" : "false");
}

function setWorkspace(id) {
  appState.workspaceId = id;
  refs.workspaceLabel.textContent = id ? `Workspace: ${id}` : "Connecting workspace…";
  syncUiState();
}

async function api(path, method = "GET", body = null) {
  const request = { path, method, body };
  appState.debugRequests.push({ at: new Date().toISOString(), ...request });
  if (appState.debugRequests.length > 80) {
    appState.debugRequests.splice(0, appState.debugRequests.length - 80);
  }
  renderDebugDrawer();

  const opts = { method, headers: { "Content-Type": "application/json" } };
  if (body !== null) {
    opts.body = JSON.stringify(body);
  }
  const response = await fetch(path, opts);
  const data = await response.json();
  pushDebug("http", { path, method, status: response.status, data });
  if (!response.ok) {
    throw new Error(data.detail || JSON.stringify(data));
  }
  return data;
}

async function uploadApi(path, file, extra = {}) {
  const formData = new FormData();
  formData.append("file", file);
  for (const [key, value] of Object.entries(extra)) {
    if (value !== null && value !== undefined && String(value).trim() !== "") {
      formData.append(key, value);
    }
  }
  const response = await fetch(path, { method: "POST", body: formData });
  const data = await response.json();
  pushDebug("upload", { path, status: response.status, filename: file?.name, data });
  if (!response.ok) {
    throw new Error(data.detail || JSON.stringify(data));
  }
  return data;
}

function appendChatMessage(role, content) {
  const messageEl = document.createElement("div");
  messageEl.className = `chat-message ${role}`;
  const bubble = document.createElement("div");
  bubble.className = "chat-bubble";
  bubble.textContent = content || "";
  messageEl.appendChild(bubble);
  refs.chatMessages.appendChild(messageEl);
  refs.chatMessages.scrollTop = refs.chatMessages.scrollHeight;
}

function renderChat(history) {
  refs.chatMessages.innerHTML = "";
  for (const message of history || []) {
    appendChatMessage(message.role === "user" ? "user" : "assistant", message.content || "");
  }
}

function renderOperationCard() {
  const operation = currentOperation() || (appState.recentOperationId ? appState.operationsById.get(appState.recentOperationId) : null);
  const local = appState.localActivity;
  if (!operation && !local) {
    refs.operationCard.hidden = true;
    return;
  }

  refs.operationCard.hidden = false;

  if (operation) {
    refs.operationKind.textContent = `${operation.kind || "operation"} · ${operation.id}`;
    refs.operationPhase.textContent = operation.progress?.message || operation.progress?.phase || "Waiting…";
    refs.operationStatus.textContent = operation.status || "queued";
    const percent = Number.isFinite(operation.progress?.percent) ? Math.max(0, Math.min(100, operation.progress.percent)) : 0;
    refs.operationBarFill.style.width = `${percent}%`;
    refs.operationTrack.setAttribute("aria-valuenow", String(Math.round(percent)));
    refs.cancelOperationButton.disabled = !(operation.status === "queued" || operation.status === "running");
    refs.retryOperationButton.disabled = !(operation.status === "failed" || operation.status === "canceled");
    return;
  }

  refs.operationKind.textContent = `ui_activity · ${local.id || "pending"}`;
  refs.operationPhase.textContent = local.message || "Working…";
  refs.operationStatus.textContent = local.status || "running";
  const percent = Number.isFinite(local.percent) ? Math.max(0, Math.min(100, local.percent)) : 20;
  refs.operationBarFill.style.width = `${percent}%`;
  refs.operationTrack.setAttribute("aria-valuenow", String(Math.round(percent)));
  refs.cancelOperationButton.disabled = true;
  refs.retryOperationButton.disabled = true;
}

function renderPendingRosterChoices() {
  const choices = Array.isArray(appState.workspace?.pending_roster_choices) ? appState.workspace.pending_roster_choices : [];
  refs.pendingChoiceButtons.innerHTML = "";
  refs.pendingChoicePanel.hidden = choices.length === 0;
  choices.forEach((choice, index) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "secondary-button";
    button.textContent = `${index + 1}. ${choice.name || choice.path || `candidate-${index + 1}`}`;
    button.addEventListener("click", async () => {
      refs.chatInput.value = String(index + 1);
      await sendChat();
    });
    refs.pendingChoiceButtons.appendChild(button);
  });
}

function renderDebugDrawer() {
  const enabled = Boolean(appState.workspace?.frontend_config?.enable_web_debug_drawer);
  refs.debugToggle.hidden = !enabled;
  if (!enabled) {
    refs.debugDrawer.hidden = true;
    return;
  }

  const payload = {
    transport: appState.transportMode,
    activeOperationId: appState.activeOperationId,
    recentOperationId: appState.recentOperationId,
    recentRequests: appState.debugRequests.slice(-20),
    recentEvents: appState.debugEvents.slice(-40),
    activeOperation: currentOperation(),
  };
  refs.debugContent.textContent = JSON.stringify(payload, null, 2);
}

function setLocalActivity(message, percent = 20) {
  appState.localActivity = {
    id: `local-${Date.now()}`,
    message: String(message || "Working…"),
    status: "running",
    percent,
  };
  renderOperationCard();
  renderDebugDrawer();
}

function clearLocalActivity() {
  if (!appState.localActivity) return;
  appState.localActivity = null;
  renderOperationCard();
  renderDebugDrawer();
}

function updateTransportBadge() {
  refs.transportBadge.textContent = `transport: ${appState.transportMode}`;
}

function ensureReviewCockpitVisible() {
  if (!refs.reviewCockpit) return;
  refs.reviewCockpit.hidden = false;
  refs.reviewCockpit.style.display = "block";
}

function fileExtension(name) {
  const lower = String(name || "").toLowerCase();
  const idx = lower.lastIndexOf(".");
  return idx < 0 ? "" : lower.slice(idx);
}

function setReviewTab(tab) {
  const normalized = ["schema", "transformations", "bq_validations"].includes(tab) ? tab : "schema";
  appState.currentReviewTab = normalized;

  const tabs = [
    [refs.reviewTabSchema, "schema"],
    [refs.reviewTabTransformations, "transformations"],
    [refs.reviewTabValidations, "bq_validations"],
  ];
  for (const [el, key] of tabs) {
    if (!el) continue;
    const active = key === normalized;
    el.classList.toggle("is-active", active);
    el.setAttribute("aria-selected", active ? "true" : "false");
  }

  const panels = [
    [refs.reviewPanelSchema, "schema"],
    [refs.reviewPanelTransformations, "transformations"],
    [refs.reviewPanelValidations, "bq_validations"],
  ];
  for (const [panel, key] of panels) {
    if (!panel) continue;
    panel.hidden = key !== normalized;
  }
}

function reviewToggleLabel(approved) {
  return approved === false ? "unchecked" : "checked";
}

async function toggleReviewItem(itemType, itemId, approved) {
  await api(`/workspaces/${appState.workspaceId}/toggle`, "POST", {
    item_type: itemType,
    item_id: itemId,
    approved,
  });
  await refreshState();
}

function renderReviewItemList(container, items, itemType) {
  if (!container) return;
  container.innerHTML = "";
  if (!Array.isArray(items) || !items.length) {
    const empty = document.createElement("p");
    empty.className = "review-empty";
    empty.textContent = "No items available yet.";
    container.appendChild(empty);
    return;
  }

  const list = document.createElement("ul");
  list.className = "review-list";
  for (const item of items) {
    const li = document.createElement("li");
    li.className = "review-list-item";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.className = "review-item-toggle";
    checkbox.checked = item?.approved !== false;
    checkbox.disabled = !appState.workspaceId;
    checkbox.setAttribute("aria-label", `Toggle ${item?.id || "item"}`);
    checkbox.addEventListener("change", async () => {
      checkbox.disabled = true;
      try {
        await toggleReviewItem(itemType, String(item?.id || ""), checkbox.checked);
      } catch (error) {
        alert(error.message);
      } finally {
        checkbox.disabled = false;
      }
    });

    const copy = document.createElement("div");
    copy.className = "review-item-copy";
    const title = document.createElement("p");
    title.className = "review-item-title";
    if (itemType === "transformations") {
      title.textContent = `${item?.name || item?.id || "transformation"} (${reviewToggleLabel(item?.approved)})`;
    } else {
      title.textContent = `${item?.name || item?.id || "validation"} (${reviewToggleLabel(item?.approved)})`;
    }

    const subtitle = document.createElement("p");
    subtitle.className = "review-item-subtitle";
    if (itemType === "transformations") {
      subtitle.textContent = `${item?.source_columns?.join(", ") || "no source column"} → ${item?.target_fields?.join(", ") || "n/a"}`;
    } else {
      subtitle.textContent = `${item?.message || item?.sql_expression || "no details"}`;
    }

    copy.appendChild(title);
    copy.appendChild(subtitle);
    li.appendChild(checkbox);
    li.appendChild(copy);
    list.appendChild(li);
  }

  container.appendChild(list);
}

function renderSchemaPanel() {
  const container = refs.reviewSchemaContent;
  if (!container) return;
  container.innerHTML = "";
  const summary = appState.workspace?.profile_summary || {};
  if (!summary?.file_name) {
    const empty = document.createElement("p");
    empty.className = "review-empty";
    empty.textContent = "Upload a roster to view schema details.";
    container.appendChild(empty);
    return;
  }

  const metrics = document.createElement("div");
  metrics.className = "review-schema-grid";
  const metricRows = [
    ["File", summary.file_name || "n/a"],
    ["Roster type", summary.roster_type_detected || "unknown"],
    ["Columns", String(summary.column_count || 0)],
    ["Rows profiled", String(summary.rows_profiled || 0)],
    ["Rows total", String(summary.rows_total || 0)],
    ["Mode", summary.profiling_mode || "sample"],
  ];
  for (const [label, value] of metricRows) {
    const row = document.createElement("p");
    row.className = "review-schema-metric";
    row.innerHTML = `<strong>${escapeHtml(label)}</strong><span>${escapeHtml(value)}</span>`;
    metrics.appendChild(row);
  }
  container.appendChild(metrics);

  const samples = Array.isArray(summary.samples) ? summary.samples : [];
  if (!samples.length) {
    const empty = document.createElement("p");
    empty.className = "review-empty";
    empty.textContent = "No sample values available yet.";
    container.appendChild(empty);
    return;
  }

  const list = document.createElement("ul");
  list.className = "review-schema-samples";
  for (const sample of samples) {
    const item = document.createElement("li");
    item.className = "review-schema-sample";
    const column = document.createElement("p");
    column.className = "review-schema-column";
    column.textContent = sample?.column || "column";
    const values = document.createElement("p");
    values.className = "review-schema-values";
    const preview = Array.isArray(sample?.values) && sample.values.length ? sample.values.join(", ") : "(no sample values)";
    values.textContent = preview;
    item.appendChild(column);
    item.appendChild(values);
    list.appendChild(item);
  }
  container.appendChild(list);
}

function renderReviewCockpit() {
  ensureReviewCockpitVisible();
  const reviewSummary = appState.workspace?.review_summary || {};
  const total = Number(reviewSummary?.total || 0);
  const unchecked = Number(reviewSummary?.unchecked || 0);
  const hasProfile = Boolean(appState.workspace?.profile_summary?.file_name);

  if (refs.reviewStatus) {
    if (!hasProfile) {
      refs.reviewStatus.textContent = "Waiting for roster profile…";
    } else {
      refs.reviewStatus.textContent = `${total} review item(s) · ${unchecked} unchecked`;
    }
  }

  renderSchemaPanel();
  renderReviewItemList(refs.reviewTransformationsContent, appState.workspace?.transformations || [], "transformations");
  renderReviewItemList(refs.reviewValidationsContent, appState.workspace?.bq_validations || [], "bq_validations");
  setReviewTab(appState.currentReviewTab || "schema");
}

function classifyFiles(fileList) {
  const rosterCandidates = [];
  const noteCandidates = [];
  const ignored = [];
  for (const file of fileList) {
    const ext = fileExtension(file.name);
    if (ROSTER_EXTENSIONS.has(ext)) {
      rosterCandidates.push(file);
    } else if (NOTE_EXTENSIONS.has(ext)) {
      noteCandidates.push(file);
    } else {
      ignored.push(file);
    }
  }
  return { rosterCandidates, noteCandidates, ignored };
}

function detectRosterType(fileName) {
  const lower = String(fileName || "").toLowerCase();
  if (lower.includes("practitioner") || lower.includes("provider") || lower.includes("npi")) return "practitioner";
  if (lower.includes("facility") || lower.includes("location") || lower.includes("site")) return "facility";
  return "";
}

function pendingRosterControlMessage(choices) {
  return `${PENDING_ROSTER_CONTROL_PREFIX} ${JSON.stringify(choices)}`;
}

async function ensureWorkspaceReady() {
  if (appState.workspaceId) return appState.workspaceId;
  const cachedId = localStorage.getItem("ur2_workspace_id");
  if (cachedId) {
    try {
      await api(`/workspaces/${encodeURIComponent(cachedId)}`);
      setWorkspace(cachedId);
      return cachedId;
    } catch (_error) {
      localStorage.removeItem("ur2_workspace_id");
    }
  }
  const created = await api("/workspaces", "POST", {});
  localStorage.setItem("ur2_workspace_id", created.workspace_id);
  setWorkspace(created.workspace_id);
  return created.workspace_id;
}

function adoptOperations(items) {
  for (const operation of items || []) {
    if (operation?.id) {
      appState.operationsById.set(operation.id, operation);
    }
  }
}

function chooseActiveOperation(payload) {
  const explicit = payload?.active_operation_id;
  if (explicit && appState.operationsById.has(explicit)) {
    appState.activeOperationId = explicit;
    appState.recentOperationId = explicit;
    return;
  }
  const live = (payload?.operations || []).find((item) => item.status === "running" || item.status === "queued");
  if (live?.id) {
    appState.activeOperationId = live.id;
    appState.recentOperationId = live.id;
    return;
  }
  const recent = (payload?.operations || [])[0] || null;
  appState.activeOperationId = null;
  appState.recentOperationId = recent?.id || appState.recentOperationId;
}

async function refreshState() {
  if (!appState.workspaceId) return;
  const payload = await api(`/workspaces/${appState.workspaceId}`);
  appState.workspace = payload;
  appState.pollIntervalMs = payload?.frontend_config?.poll_interval_ms || 1500;
  adoptOperations(payload.operations || []);
  chooseActiveOperation(payload);
  if (appState.activeOperationId) {
    clearLocalActivity();
  }
  renderChat(payload.chat_history || []);
  renderPendingRosterChoices();
  renderReviewCockpit();
  renderOperationCard();
  renderDebugDrawer();

  const pendingChoices = Array.isArray(payload?.pending_roster_choices) ? payload.pending_roster_choices : [];
  const missingBrowserFiles = pendingChoices.some((choice) => {
    const path = String(choice?.path || "");
    return path.startsWith("__browser_upload__") && !appState.pendingRosterUploads.has(path);
  });

  if (missingBrowserFiles) {
    appendChatMessage(
      "assistant",
      "I still have a pending roster selection from before refresh, but I no longer have access to those local files. Please upload the roster files again."
    );
    await api(`/workspaces/${appState.workspaceId}/chat/system`, "POST", { message: pendingRosterControlMessage([]) });
    appState.pendingRosterUploads = new Map();
  }
}

async function fetchOperation(operationId) {
  const payload = await api(`/workspaces/${appState.workspaceId}/operations/${operationId}`);
  appState.operationsById.set(operationId, payload);
  appState.recentOperationId = operationId;
  appState.activeOperationId = payload.status === "running" || payload.status === "queued" ? operationId : appState.activeOperationId === operationId ? null : appState.activeOperationId;
  renderOperationCard();
  renderDebugDrawer();
  return payload;
}

function connectEvents() {
  if (!appState.workspaceId) return;
  if (!appState.workspace?.frontend_config?.enable_sse_progress) {
    appState.sseConnected = false;
    appState.transportMode = "polling";
    updateTransportBadge();
    startPolling();
    return;
  }
  if (appState.sse) {
    appState.sse.close();
  }
  const source = new EventSource(`/workspaces/${appState.workspaceId}/events`);
  appState.sse = source;
  appState.transportMode = "connecting";
  updateTransportBadge();
  source.onopen = () => {
    appState.sseConnected = true;
    appState.transportMode = "sse";
    updateTransportBadge();
    stopPolling();
  };
  source.onerror = () => {
    appState.sseConnected = false;
    appState.transportMode = "polling";
    updateTransportBadge();
    startPolling();
  };
  source.addEventListener("operation", async (event) => {
    try {
      const payload = JSON.parse(event.data);
      pushDebug("sse", payload);
      const operationId = payload?.operation_id;
      if (operationId) {
        await fetchOperation(operationId);
        await refreshState();
      }
    } catch (error) {
      pushDebug("sse_error", { message: error.message });
    }
  });
}

function stopPolling() {
  if (appState.pollTimer) {
    clearInterval(appState.pollTimer);
    appState.pollTimer = null;
  }
}

function startPolling() {
  stopPolling();
  appState.transportMode = "polling";
  updateTransportBadge();
  appState.pollTimer = setInterval(async () => {
    if (!appState.workspaceId) return;
    if (appState.activeOperationId) {
      await fetchOperation(appState.activeOperationId);
    }
    await refreshState();
  }, appState.pollIntervalMs);
}

async function startOperation(kind, input = {}, parentOperationId = null) {
  const response = await api(`/workspaces/${appState.workspaceId}/operations`, "POST", {
    kind,
    input,
    parent_operation_id: parentOperationId,
  });
  if (response.operation) {
    appState.operationsById.set(response.operation.id, response.operation);
    appState.activeOperationId = response.operation.id;
    appState.recentOperationId = response.operation.id;
  }
  renderOperationCard();
  renderDebugDrawer();
  return response.operation;
}

async function cancelActiveOperation() {
  const operation = currentOperation();
  if (!operation) return;
  const payload = await api(`/workspaces/${appState.workspaceId}/operations/${operation.id}/cancel`, "POST", {});
  appState.operationsById.set(operation.id, payload);
  await refreshState();
}

async function retryActiveOperation() {
  const operation = currentOperation();
  if (!operation) return;
  const payload = await api(`/workspaces/${appState.workspaceId}/operations/${operation.id}/retry`, "POST", {});
  if (payload.operation) {
    appState.operationsById.set(payload.operation.id, payload.operation);
    appState.activeOperationId = payload.operation.id;
  }
  await refreshState();
}

async function maybeUploadChosenRoster(response) {
  if (!response || response.type !== "pending_roster_choice_selected") {
    return false;
  }
  const selected = response.selected_choice || {};
  const placeholder = String(selected.path || "");
  if (!placeholder.startsWith("__browser_upload__")) {
    return false;
  }

  setLocalActivity("Uploading selected roster…", 30);
  const selectedFile = appState.pendingRosterUploads.get(placeholder);
  if (!selectedFile) {
    clearLocalActivity();
    appendChatMessage("assistant", "I can’t access that roster file after refresh. Please re-upload the roster files so I can continue.");
    await api(`/workspaces/${appState.workspaceId}/chat/system`, "POST", { message: pendingRosterControlMessage([]) });
    appState.pendingRosterUploads = new Map();
    await refreshState();
    return true;
  }
  const uploadResult = await uploadSingleRoster(selectedFile, { triggerAnalysis: false });
  appState.pendingRosterUploads.delete(placeholder);
  const selectedRoster = {
    ...(selected || {}),
    path: uploadResult?.uploaded_path || selected.path,
    name: selectedFile.name || selected.name,
    roster_type: selected?.roster_type || detectRosterType(selectedFile.name) || "",
  };
  setLocalActivity("Starting roster analysis…", 55);
  await startOperation("analyze_selected_roster", { selected_roster: selectedRoster });
  clearLocalActivity();
  await refreshState();
  return true;
}

async function sendChat() {
  if (!appState.workspaceId || appState.isSending || appState.isUploading) return;
  const message = refs.chatInput.value.trim();
  if (!message) return;

  appState.isSending = true;
  setLocalActivity("Sending message…", 15);
  syncUiState();
  try {
    const response = await api(`/workspaces/${appState.workspaceId}/chat`, "POST", { message });
    refs.chatInput.value = "";
    if (response.operation) {
      appState.operationsById.set(response.operation.id, response.operation);
      appState.activeOperationId = response.operation.id;
      appState.recentOperationId = response.operation.id;
      clearLocalActivity();
    } else {
      setLocalActivity("Processing response…", 45);
    }
    const handled = await maybeUploadChosenRoster(response);
    if (!handled) {
      await refreshState();
    }
    if (!appState.activeOperationId) {
      setTimeout(() => {
        if (!appState.activeOperationId && !appState.isSending && !appState.isUploading) {
          clearLocalActivity();
        }
      }, 1200);
    }
  } catch (error) {
    clearLocalActivity();
    alert(error.message);
  } finally {
    appState.isSending = false;
    syncUiState();
  }
}

async function uploadSingleRoster(file, { triggerAnalysis = true } = {}) {
  setLocalActivity(`Uploading ${file?.name || "roster"}…`, 25);
  const uploadResult = await uploadApi(`/workspaces/${appState.workspaceId}/upload`, file, {
    roster_type: detectRosterType(file.name) || "",
  });
  if (triggerAnalysis) {
    const selectedRoster = {
      name: file.name,
      path: uploadResult?.uploaded_path || file.name,
      roster_type: detectRosterType(file.name) || "",
    };
    if (selectedRoster.path) {
      setLocalActivity("Starting roster analysis…", 50);
      await startOperation("analyze_selected_roster", { selected_roster: selectedRoster });
      clearLocalActivity();
    }
  }
  return uploadResult;
}

async function uploadNotes(noteCandidates) {
  for (const file of noteCandidates) {
    await uploadApi(`/workspaces/${appState.workspaceId}/notes/upload`, file);
  }
}

async function handleUploadSelection() {
  if (!appState.workspaceId || appState.isUploading) return;
  const files = Array.from(refs.uploadInput.files || []);
  refs.uploadInput.value = "";
  if (!files.length) return;

  const { rosterCandidates, noteCandidates, ignored } = classifyFiles(files);
  if (!rosterCandidates.length && !noteCandidates.length) {
    alert("No supported files selected. Use CSV/XLS/XLSX for roster or TXT/MD/JSON/CSV for notes.");
    return;
  }

  appState.isUploading = true;
  syncUiState();
  try {
    if (ignored.length) {
      appendChatMessage("assistant", `Skipped unsupported files: ${ignored.slice(0, 10).map((f) => f.name).join(", ")}`);
    }
    await uploadNotes(noteCandidates);

    if (rosterCandidates.length === 1) {
      await uploadSingleRoster(rosterCandidates[0]);
      await refreshState();
      return;
    }

    appState.pendingRosterUploads = new Map();
    const choices = rosterCandidates.map((file, index) => {
      const placeholder = `__browser_upload__${index}`;
      appState.pendingRosterUploads.set(placeholder, file);
      return {
        id: `roster-${index + 1}`,
        source: "roster",
        name: file.name,
        path: placeholder,
        roster_type: detectRosterType(file.name) || "",
      };
    });

    await api(`/workspaces/${appState.workspaceId}/chat/system`, "POST", { message: pendingRosterControlMessage(choices) });
    await refreshState();
    appendChatMessage("assistant", "Choose which roster file to analyze using the buttons above.");
  } catch (error) {
    alert(error.message);
  } finally {
    appState.isUploading = false;
    syncUiState();
  }
}

function registerEvents() {
  refs.sendButton.addEventListener("click", sendChat);
  refs.chatInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      sendChat();
    }
  });
  refs.uploadInput.addEventListener("change", handleUploadSelection);
  refs.uploadButton?.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    event.preventDefault();
    if (!refs.uploadInput.disabled) refs.uploadInput.click();
  });
  refs.cancelOperationButton?.addEventListener("click", cancelActiveOperation);
  refs.retryOperationButton?.addEventListener("click", retryActiveOperation);
  refs.reviewTabSchema?.addEventListener("click", () => setReviewTab("schema"));
  refs.reviewTabTransformations?.addEventListener("click", () => setReviewTab("transformations"));
  refs.reviewTabValidations?.addEventListener("click", () => setReviewTab("bq_validations"));
  refs.debugToggle?.addEventListener("click", () => {
    refs.debugDrawer.hidden = !refs.debugDrawer.hidden;
  });
  refs.debugClose?.addEventListener("click", () => {
    refs.debugDrawer.hidden = true;
  });
}

async function initialize() {
  setWorkspace(null);
  registerEvents();
  ensureReviewCockpitVisible();
  try {
    await ensureWorkspaceReady();
    await refreshState();
    connectEvents();
    setReviewTab("schema");
    updateTransportBadge();
    refs.chatInput.focus();
  } catch (error) {
    refs.workspaceLabel.textContent = `Workspace error: ${escapeHtml(error.message)}`;
    alert(error.message);
  }
}

initialize();
