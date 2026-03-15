import { useCallback, useEffect, useRef, useState } from "react";
import {
  createWorkspace,
  exportCsv,
  getWorkspace,
  startOperation,
  uploadFile,
  fetchOperation,
} from "../lib/api";
import type { MappingItem, OperationRecord, QualityAuditItem, TransformationItem, ValidationItem, WorkspaceSnapshot } from "../lib/types";
import "../styles-demo.css";
import { WorkspaceTransportManager } from "../lib/transport";

type Phase = "upload" | "processing" | "results";
type DownloadState = "preparing" | "ready" | "downloading";

function fmt(n: number | undefined | null): number {
  if (typeof n !== "number" || !Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(100, n));
}

/* -- Collapsible panel -- */
function Panel({
  icon, iconClass, title, subtitle, count, defaultOpen, children,
}: {
  icon: string;
  iconClass: string;
  title: string;
  subtitle: string;
  count?: number;
  defaultOpen?: boolean;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen ?? true);
  return (
    <div className="demo-panel">
      <div className="demo-panel-header" onClick={() => setOpen((o) => !o)} role="button" aria-expanded={open}>
        <div className={`demo-panel-icon ${iconClass}`}>{icon}</div>
        <div style={{ flex: 1 }}>
          <div className="demo-panel-title">{title}</div>
          <div className="demo-panel-count">{subtitle}</div>
        </div>
        {typeof count === "number" && (
          <span className="demo-panel-count-pill">{count}</span>
        )}
        <div className={`demo-panel-chevron${open ? " demo-panel-chevron--open" : ""}`}>&#9660;</div>
      </div>
      <div className="demo-panel-body" hidden={!open}>
        {children}
      </div>
    </div>
  );
}

/* ----------------------------------------------------------------
   SALES INTELLIGENCE: Filter what matters for the demo
   ---------------------------------------------------------------- */

/** Only show mappings we're confident about */
function getSalesMappings(mappings: MappingItem[]) {
  return mappings.filter((m) => {
    if (!m.target_field) return false;
    const band = (m.confidence_band || "").toLowerCase();
    if (band === "low") return false;
    return true;
  });
}

/** Real issues a client cares about */
function getSalesIssues(qualityAudit: QualityAuditItem[]) {
  return qualityAudit.filter((q) => {
    if (q.approved === false) return false;
    const msg = String(q.message || q.finding || q.description || "").toLowerCase();
    const title = String(q.title || q.column || q.check_type || "").toLowerCase();
    const cat = String(q.category || "").toLowerCase();

    if (msg.includes("date") && (msg.includes("format") || msg.includes("pattern") || msg.includes("yyyy"))) return false;
    if (title.includes("date") && title.includes("format")) return false;
    if ((msg.includes("zip") || title.includes("zip")) && (msg.includes("format") || msg.includes("pattern") || msg.includes("digit"))) return false;
    if ((msg.includes("ssn") || title.includes("ssn")) && (msg.includes("format") || msg.includes("dash") || msg.includes("pattern"))) return false;
    if ((msg.includes("phone") || title.includes("phone") || msg.includes("fax") || title.includes("fax")) && (msg.includes("format") || msg.includes("digit") || msg.includes("pattern"))) return false;
    if (msg.includes("bq") || msg.includes("bigquery")) return false;
    if (msg.includes("sql") && !msg.includes("provider") && !msg.includes("npi")) return false;
    if (msg.includes("required") && (msg.includes("optional") || msg.includes("when provided"))) return false;
    if (msg.includes("incorrect target") || msg.includes("incorrect mapping")) return false;
    if (title.includes("anti-pattern") || msg.includes("anti-pattern")) return false;
    if ((msg.includes("non-phone") || msg.includes("non-date") || msg.includes("non-numeric")) && msg.includes("mapped")) return false;
    if (cat === "format" && (msg.includes("mapped to") || msg.includes("target field") || msg.includes("wrong field"))) return false;
    if (msg.includes("non-phone number values in") || msg.includes("non-date values in")) return false;
    if (msg.includes("unexpected values") && msg.includes("mapped")) return false;
    if (cat === "format" && (msg.includes("date") || msg.includes("phone") || msg.includes("zip") || msg.includes("fax"))) return false;
    return true;
  });
}

/** Transformations we'll apply -- framed as value we provide */
function getSalesTransforms(transforms: TransformationItem[], mappings: MappingItem[]) {
  const items: { name: string; description: string; icon: string }[] = [];

  const hasDateMappings = mappings.some((m) =>
    (m.target_field || "").toLowerCase().includes("date") ||
    (m.target_field || "").toLowerCase().includes("dob")
  );
  if (hasDateMappings) {
    items.push({
      name: "Date Standardization",
      description: "Converting all date formats to ISO standard (YYYY-MM-DD) for consistent processing",
      icon: "\uD83D\uDCC5",
    });
  }

  const hasPhone = mappings.some((m) =>
    (m.target_field || "").toLowerCase().includes("phone") ||
    (m.target_field || "").toLowerCase().includes("fax")
  );
  if (hasPhone) {
    items.push({
      name: "Phone & Fax Normalization",
      description: "Standardizing to 10-digit format, removing dashes, parentheses, and spaces",
      icon: "\uD83D\uDCDE",
    });
  }

  const hasIds = mappings.some((m) => {
    const tf = (m.target_field || "").toLowerCase();
    return tf.includes("npi") || tf.includes("tin") || tf.includes("ssn") || tf.includes("caqh");
  });
  if (hasIds) {
    items.push({
      name: "ID Validation & Formatting",
      description: "Verifying NPI check digits, standardizing TIN and SSN to required digit counts",
      icon: "\uD83D\uDD22",
    });
  }

  const hasZip = mappings.some((m) => (m.target_field || "").toLowerCase().includes("zip"));
  if (hasZip) {
    items.push({
      name: "ZIP Code Normalization",
      description: "Standardizing ZIP codes -- handling ZIP+4 format, padding, and validation",
      icon: "\uD83D\uDCCD",
    });
  }

  const hasState = mappings.some((m) => (m.target_field || "").toLowerCase().includes("state"));
  if (hasState) {
    items.push({
      name: "State Code Standardization",
      description: "Converting full state names to 2-letter USPS codes",
      icon: "\uD83D\uDDFA",
    });
  }

  const hasGender = mappings.some((m) => (m.target_field || "").toLowerCase().includes("gender"));
  if (hasGender) {
    items.push({
      name: "Gender Value Normalization",
      description: "Standardizing to M/F/U codes accepted by health plan systems",
      icon: "\uD83D\uDC64",
    });
  }

  items.push({
    name: "Data Cleanup",
    description: "Removing empty rows, normalizing null values (N/A, None, TBD), trimming whitespace",
    icon: "\uD83E\uDDF9",
  });

  for (const t of transforms) {
    if (t.approved === false) continue;
    const name = (t.name || "").toLowerCase();
    if (name.includes("date") || name.includes("phone") || name.includes("npi") || name.includes("tin") ||
        name.includes("ssn") || name.includes("zip") || name.includes("state") || name.includes("gender") ||
        name.includes("null") || name.includes("whitespace") || name.includes("duplicate") || name.includes("sparse")) continue;
    items.push({
      name: t.name || t.id || "Transform",
      description: t.description || (t.source_columns || []).join(", "),
      icon: "\u26A1",
    });
  }

  return items;
}

/* -- Main component -- */
export function SalesDemo() {
  const [phase, setPhase] = useState<Phase>("upload");
  const [workspaceId, setWorkspaceId] = useState<string | null>(null);
  const [snapshot, setSnapshot] = useState<WorkspaceSnapshot | null>(null);
  const [currentOp, setCurrentOp] = useState<OperationRecord | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [downloadState, setDownloadState] = useState<DownloadState>("preparing");
  const [dragOver, setDragOver] = useState(false);
  const [fileName, setFileName] = useState("");
  const [startTime, setStartTime] = useState<number>(0);
  const [elapsed, setElapsed] = useState<number>(0);
  const transportRef = useRef<WorkspaceTransportManager | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);
  const preprocessPollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    return () => {
      transportRef.current?.dispose();
      transportRef.current = null;
      if (preprocessPollRef.current) clearInterval(preprocessPollRef.current);
    };
  }, []);

  // Timer for processing phase
  useEffect(() => {
    if (phase !== "processing" || !startTime) return;
    const interval = setInterval(() => {
      setElapsed(Math.round((Date.now() - startTime) / 1000));
    }, 1000);
    return () => clearInterval(interval);
  }, [phase, startTime]);

  // Poll for preprocessing completion when in results phase
  const pollPreprocessing = useCallback((wsId: string) => {
    if (preprocessPollRef.current) clearInterval(preprocessPollRef.current);

    preprocessPollRef.current = setInterval(async () => {
      try {
        const ws = await getWorkspace(wsId);
        const ops = ws.operations || [];
        const preprocessOp = ops.find(
          (o) => o.kind === "preprocess_roster"
        );

        if (preprocessOp) {
          if (preprocessOp.status === "completed") {
            setDownloadState("ready");
            if (preprocessPollRef.current) {
              clearInterval(preprocessPollRef.current);
              preprocessPollRef.current = null;
            }
          } else if (preprocessOp.status === "failed" || preprocessOp.status === "canceled") {
            // Preprocessing failed, still allow download of basic export
            setDownloadState("ready");
            if (preprocessPollRef.current) {
              clearInterval(preprocessPollRef.current);
              preprocessPollRef.current = null;
            }
          }
        } else {
          // No preprocess op found - might not be in demo mode or it hasn't started yet
          // Give it a few seconds, then default to ready
          setDownloadState("ready");
          if (preprocessPollRef.current) {
            clearInterval(preprocessPollRef.current);
            preprocessPollRef.current = null;
          }
        }
      } catch {
        // Ignore polling errors
      }
    }, 2000);
  }, []);

  async function handleFiles(files: File[]) {
    const roster = files.find((f) => /\.(csv|xlsx|xls)$/i.test(f.name));
    if (!roster) {
      setError("Please upload a CSV or XLSX file.");
      return;
    }

    setError(null);
    setFileName(roster.name);
    setPhase("processing");
    setStartTime(Date.now());
    setDownloadState("preparing");

    try {
      const { workspace_id } = await createWorkspace();
      setWorkspaceId(workspace_id);
      const uploadResult = await uploadFile(workspace_id, roster);
      const { operation } = await startOperation(workspace_id, "analyze_selected_roster", {
        selected_roster: {
          name: roster.name,
          path: String(uploadResult.uploaded_path || roster.name),
          roster_type: "",
        },
      });
      setCurrentOp(operation);
      transportRef.current?.dispose();
      const ws = await getWorkspace(workspace_id);
      setSnapshot(ws);

      transportRef.current = new WorkspaceTransportManager({
        workspaceId: workspace_id,
        pollIntervalMs: ws.frontend_config?.poll_interval_ms ?? 1500,
        sseEnabled: ws.frontend_config?.enable_sse_progress ?? false,
        handlers: {
          onModeChange: () => {},
          onWorkspace: (next) => {
            setSnapshot(next);
            const active = next.operations?.find(
              (o) => (o.status === "queued" || o.status === "running") && o.kind !== "preprocess_roster"
            );
            if (!active && (next.mappings?.length || next.quality_audit?.length || next.transformations?.length)) {
              setPhase("results");
              transportRef.current?.dispose();
              // Start polling for preprocessing
              pollPreprocessing(workspace_id);
            }
          },
          onOperation: (op) => {
            setCurrentOp(op);
            if (op.kind === "preprocess_roster") {
              if (op.status === "completed") {
                setDownloadState("ready");
              }
              return;
            }
            if (op.status === "completed" || op.status === "failed" || op.status === "canceled") {
              if (op.status !== "failed") {
                setTimeout(async () => {
                  const latest = await getWorkspace(workspace_id);
                  setSnapshot(latest);
                  setElapsed(Math.round((Date.now() - startTime) / 1000));
                  setPhase("results");
                  transportRef.current?.dispose();
                  // Start polling for preprocessing
                  pollPreprocessing(workspace_id);
                }, 800);
              } else {
                setError(String(op.error?.message || "Analysis failed. Please try again."));
                setPhase("upload");
                transportRef.current?.dispose();
              }
            }
          },
          onEvent: () => {},
          onError: (e) => setError(e.message),
        },
      });

      transportRef.current.setActiveOperation(operation.id);
      transportRef.current.start();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setPhase("upload");
    }
  }

  async function handleExport() {
    if (!workspaceId) return;
    setDownloadState("downloading");
    setError(null);
    try {
      const result = await exportCsv(workspaceId);
      if (result === "processing") {
        setDownloadState("preparing");
        // Resume polling
        pollPreprocessing(workspaceId);
      } else {
        setDownloadState("ready");
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setDownloadState("ready");
    }
  }

  function resetDemo() {
    transportRef.current?.dispose();
    transportRef.current = null;
    if (preprocessPollRef.current) {
      clearInterval(preprocessPollRef.current);
      preprocessPollRef.current = null;
    }
    setPhase("upload");
    setWorkspaceId(null);
    setSnapshot(null);
    setCurrentOp(null);
    setError(null);
    setFileName("");
    setStartTime(0);
    setElapsed(0);
    setDownloadState("preparing");
  }

  const percent = fmt(currentOp?.progress?.percent);
  const colCount = snapshot?.profile_summary?.column_count || 0;

  const allMappings = snapshot?.mappings || [];
  const allTransforms = snapshot?.transformations || [];
  const allQA = snapshot?.quality_audit || [];

  const salesMappings = getSalesMappings(allMappings);
  const salesIssues = getSalesIssues(allQA);
  const salesTransforms = getSalesTransforms(allTransforms, allMappings);
  const rowCount = snapshot?.profile_summary?.rows_total || 0;

  const downloadButtonLabel =
    downloadState === "preparing"
      ? "Preparing clean file..."
      : downloadState === "downloading"
      ? "Generating..."
      : "\u2193 Download Clean Roster";

  const downloadButtonDisabled = downloadState === "preparing" || downloadState === "downloading";
  const downloadButtonClass =
    downloadState === "ready"
      ? "d-btn d-btn-green d-btn-lg"
      : "d-btn d-btn-disabled d-btn-lg";

  return (
    <div className="demo">
      {/* Nav */}
      <nav className="demo-nav">
        <div className="demo-nav-brand">
          <span className="demo-nav-dot" />
          CertifyOS
        </div>
        <span className="demo-nav-tag">Roster Intelligence</span>
        <div className="demo-nav-spacer" />
        {phase === "results" && (
          <button className="d-btn d-btn-ghost d-btn-sm" onClick={resetDemo}>
            &larr; New Analysis
          </button>
        )}
      </nav>

      {/* Error banner */}
      {error && (
        <div className="demo-errbar" role="alert">
          <span>{error}</span>
          <button className="demo-errbar-dismiss" onClick={() => setError(null)}>&times;</button>
        </div>
      )}

      {/* -- UPLOAD PHASE -- */}
      {phase === "upload" && (
        <div className="demo-upload-wrap">
          <div className="demo-hero-text">
            <div className="demo-hero-badge">&#10022; Live Demo</div>
            <h1 className="demo-hero-h1">
              See what's really in<br />
              <span>your provider roster.</span>
            </h1>
            <p className="demo-hero-sub">
              Drop any provider roster file. Our AI reads every column, validates every NPI,
              catches data quality issues, and shows you exactly what needs
              attention &mdash; in under a minute.
            </p>
          </div>

          <div
            className={`demo-dropzone${dragOver ? " demo-dropzone--over" : ""}`}
            onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
            onDragLeave={() => setDragOver(false)}
            onDrop={(e) => { e.preventDefault(); setDragOver(false); handleFiles(Array.from(e.dataTransfer.files)); }}
            onClick={() => fileRef.current?.click()}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") fileRef.current?.click(); }}
          >
            <input
              ref={fileRef}
              type="file"
              accept=".csv,.xlsx,.xls"
              hidden
              onChange={(e) => {
                const files = Array.from(e.target.files || []);
                e.currentTarget.value = "";
                handleFiles(files);
              }}
            />
            <div className="demo-dropzone-icon">&uarr;</div>
            <p className="demo-dropzone-primary">
              Drop your roster here or{" "}
              <span className="demo-dropzone-link">browse</span>
            </p>
            <p className="demo-dropzone-hint">CSV or XLSX &mdash; practitioner or facility rosters</p>
          </div>
        </div>
      )}

      {/* -- PROCESSING PHASE -- */}
      {phase === "processing" && (
        <div className="demo-processing">
          <div className="demo-processing-card">
            <div className="demo-spinner" />
            <div className="demo-processing-title">Analyzing your roster</div>
            <div className="demo-processing-steps">
              <div className={`demo-step ${percent >= 10 ? "demo-step--done" : percent > 0 ? "demo-step--active" : ""}`}>
                <span className="demo-step-dot" />Reading file structure
              </div>
              <div className={`demo-step ${percent >= 45 ? "demo-step--done" : percent >= 20 ? "demo-step--active" : ""}`}>
                <span className="demo-step-dot" />Mapping {colCount || ""} columns to schema
              </div>
              <div className={`demo-step ${percent >= 75 ? "demo-step--done" : percent >= 50 ? "demo-step--active" : ""}`}>
                <span className="demo-step-dot" />Detecting data quality issues
              </div>
              <div className={`demo-step ${percent >= 90 ? "demo-step--done" : percent >= 75 ? "demo-step--active" : ""}`}>
                <span className="demo-step-dot" />Analyzing provider credentials
              </div>
            </div>
            <div className="demo-proc-bar">
              <div className="demo-proc-fill" style={{ width: `${Math.max(percent, 5)}%` }} />
            </div>
            <div className="demo-processing-timer">{elapsed}s</div>
          </div>
          {fileName && (
            <div className="demo-file-pill">&#9636; {fileName}</div>
          )}
        </div>
      )}

      {/* -- RESULTS PHASE -- */}
      {phase === "results" && (
        <div className="demo-results">
          {/* Header */}
          <div className="demo-results-header">
            <div>
              <div className="demo-results-title">Analysis Complete</div>
              <div className="demo-results-sub">
                <strong style={{ color: "var(--d-text)", fontFamily: "var(--d-mono)", fontSize: 12 }}>{fileName}</strong>
                {rowCount ? ` \u2014 ${rowCount.toLocaleString()} providers` : ""}
                {elapsed > 0 && elapsed < 600 ? ` analyzed in ${elapsed}s` : ""}
              </div>
            </div>
            <div className="demo-results-actions">
              <button className="d-btn d-btn-ghost" onClick={resetDemo}>Analyze Another</button>
              <button
                className={downloadButtonClass}
                disabled={downloadButtonDisabled}
                onClick={handleExport}
              >
                {downloadState === "preparing" && <span className="demo-btn-spinner" />}
                {downloadButtonLabel}
              </button>
            </div>
          </div>

          {/* Score cards */}
          <div className="demo-score-row">
            <div className="demo-score-card">
              <div className="demo-score-card-label">Providers</div>
              <div className="demo-score-card-value demo-score-card-value--blue">{rowCount || "\u2014"}</div>
              <div className="demo-score-card-sub">rows analyzed</div>
            </div>
            <div className="demo-score-card">
              <div className="demo-score-card-label">Fields Mapped</div>
              <div className="demo-score-card-value demo-score-card-value--green">{salesMappings.length}</div>
              <div className="demo-score-card-sub">of {colCount} columns recognized</div>
            </div>
            <div className="demo-score-card">
              <div className="demo-score-card-label">Issues Found</div>
              <div className={`demo-score-card-value ${salesIssues.length > 0 ? "demo-score-card-value--red" : "demo-score-card-value--green"}`}>
                {salesIssues.length}
              </div>
              <div className="demo-score-card-sub">{salesIssues.length === 0 ? "clean data" : "need attention"}</div>
            </div>
            <div className="demo-score-card">
              <div className="demo-score-card-label">Auto-Corrections</div>
              <div className="demo-score-card-value demo-score-card-value--amber">{salesTransforms.length}</div>
              <div className="demo-score-card-sub">applied automatically</div>
            </div>
          </div>

          <div className="demo-sections">
            {/* -- Section 1: Intelligent Field Mapping -- */}
            <Panel
              icon="&#9678;"
              iconClass="demo-panel-icon--blue"
              title="Intelligent Field Mapping"
              subtitle={`AI matched ${salesMappings.length} of your columns to our standard schema`}
              count={salesMappings.length}
              defaultOpen
            >
              <div className="demo-map-grid-header">
                <span>Your Column</span>
                <span></span>
                <span>Mapped To</span>
              </div>
              {salesMappings.map((m) => (
                <div key={m.id} className="demo-map-row">
                  <span className="demo-map-source">{m.source_column || m.id}</span>
                  <span className="demo-map-arrow">&rarr;</span>
                  <span className="demo-map-target">{m.target_field}</span>
                </div>
              ))}
            </Panel>

            {/* -- Section 2: Data Quality Issues -- */}
            {salesIssues.length > 0 ? (
              <Panel
                icon="!"
                iconClass="demo-panel-icon--red"
                title="Data Quality Issues"
                subtitle={`${salesIssues.length} issue${salesIssues.length !== 1 ? "s" : ""} that need attention before ingestion`}
                count={salesIssues.length}
                defaultOpen
              >
                {salesIssues.map((q) => {
                  const sev = String(q.severity || "warning").toLowerCase();
                  const isError = sev === "error" || sev === "critical";
                  const isInfo = sev === "info";
                  const dotClass = isError ? "demo-sev-dot--error" : isInfo ? "demo-sev-dot--info" : "demo-sev-dot--warn";
                  return (
                    <div key={q.id} className="demo-item-row">
                      <div className="demo-item-sev">
                        <div className={`demo-sev-dot ${dotClass}`} />
                      </div>
                      <div className="demo-item-body">
                        <p className="demo-item-title">{String(q.title || q.column || q.check_type || q.id || "Issue")}</p>
                        <p className="demo-item-detail">{String(q.message || q.finding || q.description || "")}</p>
                        {q.sample_values && q.sample_values.length > 0 && (
                          <p className="demo-item-samples">
                            Examples: {q.sample_values.slice(0, 3).map((v: string) => `"${v}"`).join(", ")}
                          </p>
                        )}
                      </div>
                      <div className="demo-item-badge">
                        <span className={isError ? "demo-badge demo-badge-error" : isInfo ? "demo-badge demo-badge-info" : "demo-badge demo-badge-warn"}>
                          {isError ? "error" : isInfo ? "info" : "warning"}
                        </span>
                      </div>
                    </div>
                  );
                })}
              </Panel>
            ) : (
              <Panel
                icon="&#10003;"
                iconClass="demo-panel-icon--green"
                title="Data Quality"
                subtitle="No critical issues found -- your data looks clean"
                defaultOpen={false}
              >
                <div className="demo-empty">
                  All NPI numbers validated. No critical data quality issues detected.
                </div>
              </Panel>
            )}

            {/* -- Section 3: Automated Corrections -- */}
            <Panel
              icon="&#9889;"
              iconClass="demo-panel-icon--amber"
              title="Automated Corrections"
              subtitle={`${salesTransforms.length} normalization rules applied to prepare your data`}
              count={salesTransforms.length}
              defaultOpen
            >
              {salesTransforms.map((t, i) => (
                <div key={i} className="demo-transform-row">
                  <div className="demo-transform-icon">{t.icon}</div>
                  <div className="demo-transform-body">
                    <p className="demo-transform-name">{t.name}</p>
                    <p className="demo-transform-desc">{t.description}</p>
                  </div>
                </div>
              ))}
            </Panel>
          </div>

          {/* Export CTA */}
          <div className="demo-export-section">
            <div className="demo-export-left">
              <div className="demo-export-title">
                {downloadState === "ready" ? "Your clean roster is ready." : "Building your clean roster..."}
              </div>
              <div className="demo-export-sub">
                All {salesTransforms.length} corrections applied. {salesMappings.length} fields mapped to standard schema.
                {salesIssues.length > 0 ? ` ${salesIssues.length} flagged rows included with error annotations.` : ""}
              </div>
            </div>
            <div className="demo-export-actions">
              <button
                className={downloadButtonClass}
                disabled={downloadButtonDisabled}
                onClick={handleExport}
              >
                {downloadState === "preparing" && <span className="demo-btn-spinner" />}
                {downloadButtonLabel}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
