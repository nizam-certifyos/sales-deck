import { useRef, useState } from "react";
import type { TransportMode } from "../lib/transport";
import type { CockpitTab, MappingItem, OperationRecord, PendingRosterChoice, TransformationItem, ValidationItem, WorkspaceSnapshot } from "../lib/types";
import "../styles-new.css";
import { DiagnosticsPanel } from "./DiagnosticsPanel";

type View = "upload" | "mappings" | "transforms" | "validations";

interface AppNewLayoutProps {
  workspaceId: string | null;
  snapshot: WorkspaceSnapshot | null;
  message: string;
  activeTab: CockpitTab;
  sending: boolean;
  uploading: boolean;
  transportMode: TransportMode;
  diagnosticsOpen: boolean;
  diagnosticsEnabled: boolean;
  error: string | null;
  currentOperation: OperationRecord | null;
  operations: OperationRecord[];
  localActivity: { message: string; status: string; percent: number } | null;
  pendingChoices: PendingRosterChoice[];
  diagnosticsPayload: Record<string, unknown>;
  onMessageChange: (value: string) => void;
  onSend: () => void;
  onFilesSelected: (files: File[]) => void;
  onTabChange: (tab: CockpitTab) => void;
  onToggle: (
    itemType: "mappings" | "transformations" | "bq_validations" | "quality_audit",
    itemId: string,
    approved: boolean
  ) => void;
  onRunQualityAudit: () => void;
  onCancelOperation: () => void;
  onRetryOperation: () => void;
  onSelectPendingChoice: (index: number) => void;
  onDiagnosticsToggle: () => void;
  onDiagnosticsClose: () => void;
  onErrorDismiss: () => void;
  onSubmitMessage: (text: string) => void;
}

function fmt(n: number): number {
  return typeof n === "number" && Number.isFinite(n) ? Math.max(0, Math.min(100, n)) : 0;
}

function confClass(band: string | undefined): string {
  const b = (band || "").toLowerCase();
  if (b === "high") return "v3-conf v3-conf-high";
  if (b === "medium" || b === "med") return "v3-conf v3-conf-medium";
  if (b === "low") return "v3-conf v3-conf-low";
  return "v3-conf v3-conf-none";
}

function confLabel(band: string | undefined): string {
  const b = (band || "").toLowerCase();
  if (b === "high") return "high";
  if (b === "medium" || b === "med") return "med";
  if (b === "low") return "low";
  return "—";
}

/* ── Upload view ── */
function UploadView({
  snapshot,
  uploading,
  sending,
  onFilesSelected,
  onGoToMappings,
}: {
  snapshot: WorkspaceSnapshot | null;
  uploading: boolean;
  sending: boolean;
  onFilesSelected: (files: File[]) => void;
  onGoToMappings: () => void;
}) {
  const fileRef = useRef<HTMLInputElement>(null);
  const [dragOver, setDragOver] = useState(false);
  const profile = snapshot?.profile_summary;
  const hasFile = Boolean(profile?.file_name);
  const hasMappings = (snapshot?.mappings?.length ?? 0) > 0;

  function handleDrop(e: React.DragEvent) {
    e.preventDefault();
    setDragOver(false);
    const files = Array.from(e.dataTransfer.files);
    if (files.length) onFilesSelected(files);
  }

  return (
    <div className="v3-view">
      <div className="v3-upload">
        <h1 className="v3-view-title">Upload Roster File</h1>
        <p className="v3-view-sub">CSV or XLSX — AI will map columns and suggest transforms automatically.</p>

        <div
          className={`v3-dropzone${dragOver ? " v3-dropzone--over" : ""}`}
          onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
          onDragLeave={() => setDragOver(false)}
          onDrop={handleDrop}
          onClick={() => fileRef.current?.click()}
          role="button"
          tabIndex={0}
          onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") fileRef.current?.click(); }}
          aria-label="Upload roster file"
        >
          <input
            ref={fileRef}
            type="file"
            accept=".csv,.xlsx,.xls"
            hidden
            onChange={(e) => {
              const files = Array.from(e.target.files || []);
              e.currentTarget.value = "";
              if (files.length) onFilesSelected(files);
            }}
          />
          <div className="v3-dropzone-icon">
            {uploading || sending ? "⟳" : "↑"}
          </div>
          <p className="v3-dropzone-primary">
            {uploading || sending
              ? "Processing…"
              : <>Drop file here or <span className="v3-dropzone-link">browse</span></>}
          </p>
          <p className="v3-dropzone-hint">CSV · XLSX · XLS</p>
        </div>

        {hasFile && profile && (
          <div className="v3-file-preview">
            <div className="v3-file-row">
              <div className="v3-file-icon">▤</div>
              <div style={{ flex: 1 }}>
                <div className="v3-file-name">{profile.file_name}</div>
                <div className="v3-file-size">
                  {profile.rows_total ? `${profile.rows_total.toLocaleString()} rows` : ""}
                  {profile.rows_total && profile.column_count ? " · " : ""}
                  {profile.column_count ? `${profile.column_count} columns` : ""}
                  {profile.roster_type_detected ? ` · ${profile.roster_type_detected}` : ""}
                </div>
              </div>
            </div>

            {profile.samples && profile.samples.length > 0 && (
              <div style={{ overflowX: "auto" }}>
                <table className="v3-table" style={{ minWidth: "100%" }}>
                  <thead>
                    <tr>
                      {profile.samples.slice(0, 5).map((s) => (
                        <th key={s.column} className="v3-code">{s.column}</th>
                      ))}
                      {profile.samples.length > 5 && (
                        <th style={{ color: "var(--accent)", fontFamily: "var(--font)", textTransform: "none", letterSpacing: 0 }}>
                          +{profile.samples.length - 5} more
                        </th>
                      )}
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      {profile.samples.slice(0, 5).map((s) => (
                        <td key={s.column} style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--text-2)" }}>
                          {s.values.slice(0, 2).join(", ") || "—"}
                        </td>
                      ))}
                      {profile.samples.length > 5 && <td />}
                    </tr>
                  </tbody>
                </table>
              </div>
            )}

            <div className="v3-file-preview-footer">
              {hasMappings ? (
                <button className="v3-btn v3-btn-primary" onClick={onGoToMappings}>
                  View Results →
                </button>
              ) : (
                <span style={{ fontSize: 12, color: "var(--text-3)" }}>
                  {sending || uploading ? "Analyzing…" : "Waiting for analysis…"}
                </span>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Mappings view ── */
function MappingsView({
  mappings,
  sending,
  onToggle,
  onSubmitMessage,
  onNext,
  onPrev,
}: {
  mappings: MappingItem[];
  sending: boolean;
  onToggle: (id: string, approved: boolean) => void;
  onSubmitMessage: (text: string) => void;
  onNext: () => void;
  onPrev: () => void;
}) {
  const [nlText, setNlText] = useState("");
  const [filter, setFilter] = useState<"all" | "mapped" | "review">("all");

  const totalActive = mappings.filter((m) => m.approved !== false).length;

  const filtered = mappings.filter((m) => {
    if (filter === "mapped") return Boolean(m.target_field);
    if (filter === "review") return (m.confidence_band || "").toLowerCase() !== "high";
    return true;
  });

  function submitNl() {
    const t = nlText.trim();
    if (!t) return;
    onSubmitMessage(t);
    setNlText("");
  }

  if (!mappings.length) {
    return (
      <div className="v3-view">
        <div className="v3-review">
          <h1 className="v3-view-title">Column Mappings</h1>
          <p className="v3-view-sub">Upload a roster file to see AI-generated column mappings.</p>
          <div className="v3-empty">No mappings yet. Upload a file to get started.</div>
          <div className="v3-footer">
            <button className="v3-btn v3-btn-ghost" onClick={onPrev}>← Upload</button>
            <div />
          </div>
        </div>
      </div>
    );
  }

  const highCount = mappings.filter((m) => (m.confidence_band || "").toLowerCase() === "high").length;
  const medCount = mappings.filter((m) => ["medium", "med"].includes((m.confidence_band || "").toLowerCase())).length;
  const lowCount = mappings.filter((m) => (m.confidence_band || "").toLowerCase() === "low").length;
  const reviewCount = mappings.filter((m) => !["high"].includes((m.confidence_band || "").toLowerCase())).length;

  return (
    <div className="v3-view">
      <div className="v3-review">
        <div className="v3-review-header">
          <div className="v3-review-left">
            <h1 className="v3-view-title">Column Mappings</h1>
            <div className="v3-review-stats">
              {highCount > 0 && <span className="v3-chip v3-chip-green">{highCount} high</span>}
              {medCount > 0 && <span className="v3-chip v3-chip-amber">{medCount} medium</span>}
              {lowCount > 0 && <span className="v3-chip v3-chip-red">{lowCount} low</span>}
            </div>
          </div>
          <div className="v3-review-actions">
            <button className="v3-btn v3-btn-ghost v3-btn-sm"
              onClick={() => mappings.forEach((m) => onToggle(m.id, true))}>
              Approve All
            </button>
          </div>
        </div>

        <div className="v3-filterbar">
          <button className={`v3-pill${filter === "all" ? " v3-pill--active" : ""}`} onClick={() => setFilter("all")}>All ({mappings.length})</button>
          <button className={`v3-pill${filter === "mapped" ? " v3-pill--active" : ""}`} onClick={() => setFilter("mapped")}>Has target ({mappings.filter((m) => m.target_field).length})</button>
          {reviewCount > 0 && (
            <button className={`v3-pill v3-pill--amber${filter === "review" ? " v3-pill--active" : ""}`} onClick={() => setFilter("review")}>Needs review ({reviewCount})</button>
          )}
        </div>

        <div className="v3-table-wrap">
          <table className="v3-table">
            <thead>
              <tr>
                <th className="tc-check"></th>
                <th className="tc-source">Source Column</th>
                <th className="tc-arrow"></th>
                <th className="tc-target">Target Field</th>
                <th className="tc-conf">Confidence</th>
                <th className="tc-sample">Sample Values</th>
              </tr>
            </thead>
            <tbody>
              {filtered.length === 0 ? (
                <tr><td colSpan={6} className="v3-empty">No mappings match this filter.</td></tr>
              ) : filtered.map((m) => {
                const isOff = m.approved === false;
                const band = (m.confidence_band || "").toLowerCase();
                const isWarn = band === "medium" || band === "med" || band === "low";
                return (
                  <tr
                    key={m.id}
                    className={`${isOff ? "v3-row--off" : ""}${isWarn && !isOff ? " v3-row--warn" : ""}`}
                  >
                    <td className="tc-check">
                      <input
                        type="checkbox"
                        className="v3-cb"
                        checked={m.approved !== false}
                        onChange={(e) => onToggle(m.id, e.target.checked)}
                      />
                    </td>
                    <td className="tc-source">
                      <span className="v3-code">{m.source_column || m.id}</span>
                    </td>
                    <td className="tc-arrow">→</td>
                    <td className="tc-target">
                      <span style={{ fontSize: 12, color: "var(--text-2)" }}>
                        {m.target_field || <span style={{ color: "var(--text-3)" }}>— unmapped —</span>}
                      </span>
                    </td>
                    <td className="tc-conf">
                      <span className={confClass(m.confidence_band)}>{confLabel(m.confidence_band)}</span>
                    </td>
                    <td className="tc-sample">
                      <div className="v3-sample">
                        {(m as unknown as { sample_values?: string[] }).sample_values?.slice(0, 3).map((v, i) => (
                          <span key={i} className={`v3-sv${isWarn ? " v3-sv-warn" : ""}`}>{v}</span>
                        ))}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Natural language custom mapping input */}
        <div className="v3-nl">
          <span className="v3-nl-icon">✦</span>
          <input
            className="v3-nl-input"
            type="text"
            placeholder='Add a mapping — e.g. "Map ADDR_LINE1 to practice_address"'
            value={nlText}
            onChange={(e) => setNlText(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter") submitNl(); }}
          />
          <button className="v3-btn v3-btn-ghost v3-btn-sm" disabled={!nlText.trim() || sending} onClick={submitNl}>
            Add
          </button>
        </div>

        <div className="v3-footer">
          <button className="v3-btn v3-btn-ghost" onClick={onPrev}>← Upload</button>
          <div className="v3-footer-right">
            <span className="v3-footer-summary">{totalActive} of {mappings.length} approved</span>
            <button className="v3-btn v3-btn-primary" onClick={onNext}>Transformations →</button>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Transforms view ── */
function TransformsView({
  transforms,
  sending,
  onToggle,
  onSubmitMessage,
  onNext,
  onPrev,
}: {
  transforms: TransformationItem[];
  sending: boolean;
  onToggle: (id: string, approved: boolean) => void;
  onSubmitMessage: (text: string) => void;
  onNext: () => void;
  onPrev: () => void;
}) {
  const [nlText, setNlText] = useState("");

  const totalActive = transforms.filter((t) => t.approved !== false).length;

  function submitNl() {
    const text = nlText.trim();
    if (!text) return;
    onSubmitMessage(text);
    setNlText("");
  }

  return (
    <div className="v3-view">
      <div className="v3-review">
        <div className="v3-review-header">
          <div className="v3-review-left">
            <h1 className="v3-view-title">Transformations</h1>
            <p className="v3-view-sub">Data cleaning and normalization rules. Toggle to apply.</p>
          </div>
          <div className="v3-review-actions">
            {transforms.length > 0 && (
              <>
                <button className="v3-btn v3-btn-ghost v3-btn-sm" onClick={() => transforms.forEach((t) => onToggle(t.id, true))}>Enable All</button>
                <button className="v3-btn v3-btn-ghost v3-btn-sm" onClick={() => transforms.forEach((t) => onToggle(t.id, false))}>Disable All</button>
              </>
            )}
          </div>
        </div>

        {transforms.length === 0 ? (
          <div className="v3-table-wrap"><div className="v3-empty">No transformations yet. Add one below using plain English.</div></div>
        ) : (
          <div className="v3-table-wrap">
            <table className="v3-table">
              <thead>
                <tr>
                  <th className="tc-check"></th>
                  <th className="tc-name">Rule</th>
                  <th className="tc-field">Applies To</th>
                  <th className="tc-ex">Before → After</th>
                  <th className="tc-type">Type</th>
                </tr>
              </thead>
              <tbody>
                {transforms.map((t) => {
                  const isOff = t.approved === false;
                  const sources = (t.source_columns || []).join(", ");
                  const targets = (t.target_fields || []).join(", ");
                  return (
                    <tr key={t.id} className={isOff ? "v3-row--off" : ""}>
                      <td className="tc-check">
                        <input
                          type="checkbox"
                          className="v3-cb"
                          checked={t.approved !== false}
                          onChange={(e) => onToggle(t.id, e.target.checked)}
                        />
                      </td>
                      <td className="tc-name">
                        <span className="v3-rule-name">{t.name || t.id}</span>
                        {t.description ? <span className="v3-rule-desc">{String(t.description)}</span> : null}
                      </td>
                      <td className="tc-field">
                        {sources ? (
                          <span className="v3-code" style={{ fontSize: 11 }}>{sources}</span>
                        ) : (
                          <span className="v3-tag">all fields</span>
                        )}
                      </td>
                      <td className="tc-ex">
                        {sources && targets ? (
                          <div className="v3-transform">
                            <span className="v3-t-before">{sources}</span>
                            <span className="v3-t-arrow">→</span>
                            <span className="v3-t-after">{targets}</span>
                          </div>
                        ) : null}
                      </td>
                      <td className="tc-type">
                        <span className={`v3-badge${t.kind ? ` v3-badge-${String(t.kind)}` : ""}`}>
                          {t.kind ? String(t.kind) : "rule"}
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        <div className="v3-nl">
          <span className="v3-nl-icon">✦</span>
          <input
            className="v3-nl-input"
            type="text"
            placeholder='Describe a transform — e.g. "Normalize phone numbers to (###) ###-####"'
            value={nlText}
            onChange={(e) => setNlText(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter") submitNl(); }}
          />
          <button className="v3-btn v3-btn-ghost v3-btn-sm" disabled={!nlText.trim() || sending} onClick={submitNl}>
            Add
          </button>
        </div>

        <div className="v3-footer">
          <button className="v3-btn v3-btn-ghost" onClick={onPrev}>← Mappings</button>
          <div className="v3-footer-right">
            {transforms.length > 0 && (
              <span className="v3-footer-summary">{totalActive} of {transforms.length} active</span>
            )}
            <button className="v3-btn v3-btn-primary" onClick={onNext}>BQ Validations →</button>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Validations view ── */
function ValidationsView({
  validations,
  sending,
  onToggle,
  onSubmitMessage,
  onPrev,
}: {
  validations: ValidationItem[];
  sending: boolean;
  onToggle: (id: string, approved: boolean) => void;
  onSubmitMessage: (text: string) => void;
  onPrev: () => void;
}) {
  const [nlText, setNlText] = useState("");
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  function toggleExpand(id: string) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  const totalActive = validations.filter((v) => v.approved !== false).length;

  function submitNl() {
    const text = nlText.trim();
    if (!text) return;
    onSubmitMessage(text);
    setNlText("");
  }

  return (
    <div className="v3-view">
      <div className="v3-review">
        <div className="v3-review-header">
          <div className="v3-review-left">
            <h1 className="v3-view-title">BQ Validations</h1>
            <p className="v3-view-sub">SQL checks run after transformation. Failing rows are flagged, not dropped.</p>
          </div>
          <div className="v3-review-actions">
            {validations.length > 0 && (
              <button className="v3-btn v3-btn-ghost v3-btn-sm" onClick={() => validations.forEach((v) => onToggle(v.id, true))}>Enable All</button>
            )}
          </div>
        </div>

        {validations.length === 0 ? (
          <div className="v3-table-wrap"><div className="v3-empty">No validations yet. Add one below in plain English.</div></div>
        ) : (
          <div className="v3-table-wrap">
            <table className="v3-table">
              <thead>
                <tr>
                  <th className="tc-check"></th>
                  <th className="tc-name">Validation</th>
                  <th className="tc-field">Field(s)</th>
                  <th className="tc-sev">Severity</th>
                  <th className="tc-res">Result</th>
                </tr>
              </thead>
              <tbody>
                {validations.map((v) => {
                  const isOff = v.approved === false;
                  const isExpanded = expanded.has(v.id);
                  const hasSql = Boolean(v.sql_expression);
                  return (
                    <>
                      <tr key={v.id} className={isOff ? "v3-row--off" : ""}>
                        <td className="tc-check">
                          <input
                            type="checkbox"
                            className="v3-cb"
                            checked={v.approved !== false}
                            onChange={(e) => onToggle(v.id, e.target.checked)}
                          />
                        </td>
                        <td className="tc-name">
                          {hasSql && (
                            <button
                              className={`v3-sql-toggle${isExpanded ? " v3-sql-toggle--open" : ""}`}
                              onClick={() => toggleExpand(v.id)}
                              title={isExpanded ? "Hide SQL" : "Show SQL"}
                            >
                              ▶
                            </button>
                          )}
                          <span className="v3-rule-name">{v.name || v.id}</span>
                          {v.message && <span className="v3-rule-desc">{v.message}</span>}
                        </td>
                        <td className="tc-field">
                          <span className="v3-code" style={{ fontSize: 11 }}>
                            {(v as unknown as { field?: string }).field || "—"}
                          </span>
                        </td>
                        <td className="tc-sev">
                          {v.severity ? (
                            <span className={`v3-badge v3-badge-${String(v.severity).toLowerCase()}`}>
                              {String(v.severity)}
                            </span>
                          ) : <span className="v3-badge">check</span>}
                        </td>
                        <td className="tc-res">
                          <span className="v3-val-na">—</span>
                        </td>
                      </tr>
                      {hasSql && isExpanded && (
                        <tr key={`${v.id}-sql`} className="v3-sql-row">
                          <td colSpan={5} className="v3-sql-cell">
                            <pre className="v3-sql-block">{v.sql_expression}</pre>
                          </td>
                        </tr>
                      )}
                    </>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        <div className="v3-nl">
          <span className="v3-nl-icon">✦</span>
          <input
            className="v3-nl-input"
            type="text"
            placeholder='Add a validation — e.g. "Flag rows where NPI is not exactly 10 digits"'
            value={nlText}
            onChange={(e) => setNlText(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter") submitNl(); }}
          />
          <button className="v3-btn v3-btn-ghost v3-btn-sm" disabled={!nlText.trim() || sending} onClick={submitNl}>
            Add
          </button>
        </div>

        <div className="v3-footer">
          <button className="v3-btn v3-btn-ghost" onClick={onPrev}>← Transforms</button>
          <div className="v3-footer-right">
            {validations.length > 0 && (
              <span className="v3-footer-summary">{totalActive} of {validations.length} active</span>
            )}
            <button className="v3-btn v3-btn-primary" disabled>Export CSV ↓ (coming soon)</button>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Main layout ── */
export function AppNewLayout(props: AppNewLayoutProps) {
  const {
    workspaceId,
    snapshot,
    sending,
    uploading,
    transportMode,
    diagnosticsOpen,
    diagnosticsEnabled,
    error,
    currentOperation,
    localActivity,
    pendingChoices,
    diagnosticsPayload,
    onFilesSelected,
    onToggle,
    onCancelOperation,
    onRetryOperation,
    onDiagnosticsToggle,
    onDiagnosticsClose,
    onErrorDismiss,
    onSubmitMessage,
  } = props;

  const hasFile = Boolean(snapshot?.profile_summary?.file_name);
  const hasMappings = (snapshot?.mappings?.length ?? 0) > 0;

  const [view, setView] = useState<View>(() =>
    hasMappings ? "mappings" : "upload"
  );

  // Sync view when mappings appear for first time
  const mappings = snapshot?.mappings || [];
  const transforms = snapshot?.transformations || [];
  const validations = snapshot?.bq_validations || [];

  const percent = currentOperation
    ? fmt(currentOperation.progress?.percent ?? 0)
    : fmt(localActivity?.percent ?? 0);

  const opPhase = currentOperation?.progress?.message || currentOperation?.progress?.phase || localActivity?.message;
  const opStatus = currentOperation?.status || localActivity?.status || "";
  const isOpActive = Boolean(
    (currentOperation && (currentOperation.status === "queued" || currentOperation.status === "running")) ||
    localActivity
  );

  const profile = snapshot?.profile_summary;
  const chatHistory = snapshot?.chat_history || [];

  const [aiOpen, setAiOpen] = useState(false);
  const [aiInput, setAiInput] = useState("");

  function stepState(v: View): "active" | "done" | "idle" {
    const order: View[] = ["upload", "mappings", "transforms", "validations"];
    const cur = order.indexOf(view);
    const target = order.indexOf(v);
    if (cur === target) return "active";
    if (cur > target) return "done";
    return "idle";
  }

  function stepCount(v: View): number | null {
    if (v === "mappings") return mappings.length || null;
    if (v === "transforms") return transforms.length || null;
    if (v === "validations") return validations.length || null;
    return null;
  }

  const steps: { id: View; label: string }[] = [
    { id: "upload", label: "Upload" },
    { id: "mappings", label: "Mappings" },
    { id: "transforms", label: "Transforms" },
    { id: "validations", label: "BQ Validations" },
  ];

  function submitAi() {
    const t = aiInput.trim();
    if (!t) return;
    onSubmitMessage(t);
    setAiInput("");
  }

  return (
    <div className="v3" aria-label="Roster AI workspace">

      {/* ── Header ── */}
      <header className="v3-header">
        <div className="v3-brand">
          <span className="v3-brand-dot" />
          Roster AI
        </div>

        <div className="v3-header-file">
          {profile?.file_name ? (
            <>
              <span className="v3-file-chip" title={profile.file_name}>
                ▤ {profile.file_name}
              </span>
              {(profile.rows_total || profile.column_count) && (
                <span className="v3-file-chip v3-file-chip-meta">
                  {[
                    profile.rows_total && `${profile.rows_total.toLocaleString()} rows`,
                    profile.column_count && `${profile.column_count} cols`,
                  ].filter(Boolean).join(" · ")}
                </span>
              )}
            </>
          ) : null}

          {pendingChoices.length > 0 && (
            <span style={{ fontSize: 12, color: "var(--amber)", fontWeight: 500, marginLeft: 8 }}>
              Choose roster:&nbsp;
              {pendingChoices.map((c, i) => (
                <button
                  key={i}
                  className="v3-btn v3-btn-ghost v3-btn-sm"
                  style={{ marginLeft: 4 }}
                  onClick={() => props.onSelectPendingChoice(i)}
                >
                  {c.name || `File ${i + 1}`}
                </button>
              ))}
            </span>
          )}
        </div>

        <div className="v3-header-end">
          {workspaceId ? (
            <span className="v3-ws-badge" title={workspaceId}>
              ws: {workspaceId.slice(0, 8)}…
            </span>
          ) : (
            <span className="v3-ws-badge">{error ? "error" : "connecting…"}</span>
          )}
          <span className="v3-conn-dot" data-mode={transportMode} title={`Transport: ${transportMode}`} />
          {diagnosticsEnabled && (
            <button className="v3-btn v3-btn-ghost v3-btn-sm" onClick={onDiagnosticsToggle}>
              Debug
            </button>
          )}
        </div>
      </header>

      {/* ── Step nav ── */}
      <nav className="v3-stepnav" aria-label="Workflow steps">
        {steps.map((s) => {
          const state = stepState(s.id);
          const count = stepCount(s.id);
          return (
            <button
              key={s.id}
              className="v3-step"
              data-state={state}
              onClick={() => setView(s.id)}
              disabled={s.id !== "upload" && !hasFile}
              aria-current={state === "active" ? "step" : undefined}
            >
              <span className="v3-step-num">
                {state === "done" ? "✓" : steps.findIndex((x) => x.id === s.id) + 1}
              </span>
              {s.label}
              {count !== null && (
                <span className="v3-step-count">{count}</span>
              )}
            </button>
          );
        })}
      </nav>

      {/* ── Error banner ── */}
      {error && (
        <div className="v3-errbar" role="alert">
          <span>{error}</span>
          <button className="v3-errbar-dismiss" onClick={onErrorDismiss} aria-label="Dismiss">×</button>
        </div>
      )}

      {/* ── Operation progress bar ── */}
      <div className="v3-opbar" hidden={!isOpActive}>
        <div className="v3-opbar-row">
          <span className="v3-opbar-phase">{opPhase || "Processing…"}</span>
          <span className="v3-opbar-status" data-s={opStatus}>{opStatus || "running"}</span>
        </div>
        <div className="v3-opbar-track">
          <div className="v3-opbar-fill" style={{ width: `${percent}%` }} />
        </div>
        <div className="v3-opbar-actions">
          <button
            className="v3-btn v3-btn-danger v3-btn-sm"
            disabled={!currentOperation || !["queued","running"].includes(currentOperation.status)}
            onClick={onCancelOperation}
          >
            Cancel
          </button>
          <button
            className="v3-btn v3-btn-ghost v3-btn-sm"
            disabled={!currentOperation || !["failed","canceled"].includes(currentOperation.status)}
            onClick={onRetryOperation}
          >
            Retry
          </button>
        </div>
      </div>

      {/* ── Main view area ── */}
      <div className="v3-main">
        {view === "upload" && (
          <UploadView
            snapshot={snapshot}
            uploading={uploading}
            sending={sending}
            onFilesSelected={onFilesSelected}
            onGoToMappings={() => setView("mappings")}
          />
        )}
        {view === "mappings" && (
          <MappingsView
            mappings={mappings}
            sending={sending}
            onToggle={(id, approved) => onToggle("mappings", id, approved)}
            onSubmitMessage={onSubmitMessage}
            onNext={() => setView("transforms")}
            onPrev={() => setView("upload")}
          />
        )}
        {view === "transforms" && (
          <TransformsView
            transforms={transforms}
            sending={sending}
            onToggle={(id, approved) => onToggle("transformations", id, approved)}
            onSubmitMessage={onSubmitMessage}
            onNext={() => setView("validations")}
            onPrev={() => setView("mappings")}
          />
        )}
        {view === "validations" && (
          <ValidationsView
            validations={validations}
            sending={sending}
            onToggle={(id, approved) => onToggle("bq_validations", id, approved)}
            onSubmitMessage={onSubmitMessage}
            onPrev={() => setView("transforms")}
          />
        )}
      </div>

      {/* ── AI log tray (secondary) ── */}
      <div className="v3-ai-tray">
        <button className="v3-ai-tray-toggle" onClick={() => setAiOpen((o) => !o)}>
          <span className="v3-ai-tray-icon">✦</span>
          AI Log
          {chatHistory.length > 0 && ` · ${chatHistory.length} messages`}
          {isOpActive && <span style={{ color: "var(--amber)", marginLeft: 6 }}>⟳ processing</span>}
          <span className="v3-ai-tray-chevron">{aiOpen ? "▼" : "▲"}</span>
        </button>

        <div className="v3-ai-log" hidden={!aiOpen}>
          {chatHistory.length === 0 ? (
            <div style={{ padding: "12px 20px", fontSize: 11, color: "var(--text-3)" }}>
              No AI activity yet. Upload a file or add a custom rule below.
            </div>
          ) : (
            chatHistory.slice(-20).map((m, i) => (
              <div key={i} className="v3-ai-log-entry">
                <span className={`v3-ai-log-role${m.role === "user" ? " v3-ai-log-role-user" : ""}`}>
                  {m.role === "user" ? "You" : "AI"}
                </span>
                <span className="v3-ai-log-text">
                  {m.content.length > 200 ? m.content.slice(0, 200) + "…" : m.content}
                </span>
              </div>
            ))
          )}
        </div>

        {aiOpen && (
          <div className="v3-ai-input-row">
            <span className="v3-sr">Ask AI</span>
            <input
              className="v3-ai-input"
              type="text"
              placeholder="Ask the AI anything about this roster…"
              value={aiInput}
              disabled={sending || !workspaceId}
              onChange={(e) => setAiInput(e.target.value)}
              onKeyDown={(e) => { if (e.key === "Enter") submitAi(); }}
            />
            <button
              className="v3-btn v3-btn-primary v3-btn-sm"
              disabled={!aiInput.trim() || sending || !workspaceId}
              onClick={submitAi}
            >
              Send
            </button>
          </div>
        )}
      </div>

      {/* Diagnostics */}
      <DiagnosticsPanel
        enabled={diagnosticsEnabled}
        open={diagnosticsOpen}
        onClose={onDiagnosticsClose}
        payload={diagnosticsPayload}
      />
    </div>
  );
}
