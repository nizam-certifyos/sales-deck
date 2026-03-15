import { useEffect, useMemo, useRef, useState } from "react";
import type {
  ClientSummary,
  ColumnAuditSummary,
  OperationRecord,
  PendingRosterChoice,
  QualityAuditItem,
  StandardizationPlan,
} from "../lib/types";
import { AuditClientSummary } from "./AuditClientSummary";
import { AuditColumnDrawer } from "./AuditColumnDrawer";
import { AuditColumnMatrix } from "./AuditColumnMatrix";
import { AuditStandardizationPlan } from "./AuditStandardizationPlan";
import { OperationPanel } from "./OperationPanel";
import { QualityAuditList } from "./QualityAuditList";

interface AuditPageProps {
  workspaceId: string | null;
  uploading: boolean;
  qualityAudit: QualityAuditItem[];
  columnAuditSummary?: ColumnAuditSummary;
  standardizationPlan?: StandardizationPlan;
  clientSummary?: ClientSummary;
  pendingChoices: PendingRosterChoice[];
  canRunQualityAudit: boolean;
  currentOperation: OperationRecord | null;
  operations: OperationRecord[];
  localActivity: { message: string; status: string; percent: number } | null;
  onFilesSelected: (files: File[]) => void;
  onRunQualityAudit: () => void;
  onToggleQualityAudit: (itemId: string, approved: boolean) => void;
  onCancelOperation: () => void;
  onRetryOperation: () => void;
  onSelectPendingChoice: (index: number) => void;
}

export function AuditPage({
  workspaceId,
  uploading,
  qualityAudit,
  columnAuditSummary,
  standardizationPlan,
  clientSummary,
  pendingChoices,
  canRunQualityAudit,
  currentOperation,
  operations,
  localActivity,
  onFilesSelected,
  onRunQualityAudit,
  onToggleQualityAudit,
  onCancelOperation,
  onRetryOperation,
  onSelectPendingChoice,
}: AuditPageProps) {
  const [isDragOver, setIsDragOver] = useState(false);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const selectedColumnFallback = columnAuditSummary?.columns?.[0]?.column_key || "";
  const [selectedColumnKey, setSelectedColumnKey] = useState<string>(selectedColumnFallback);

  useEffect(() => {
    if (!columnAuditSummary?.columns?.length) {
      return;
    }
    if (!selectedColumnKey || !columnAuditSummary.columns.some((item) => item.column_key === selectedColumnKey)) {
      setSelectedColumnKey(columnAuditSummary.columns[0].column_key);
    }
  }, [columnAuditSummary, selectedColumnKey]);

  const kpis = useMemo(() => {
    let critical = 0;
    let warning = 0;
    let info = 0;
    for (const item of qualityAudit) {
      const severity = String(item.severity || "info").toLowerCase();
      if (severity === "error") {
        critical += 1;
      } else if (severity === "warning") {
        warning += 1;
      } else {
        info += 1;
      }
    }
    return { critical, warning, info, total: qualityAudit.length };
  }, [qualityAudit]);

  function applyFiles(fileList: FileList | null) {
    const files = Array.from(fileList || []);
    if (files.length) {
      onFilesSelected(files);
    }
  }

  return (
    <main className="audit-shell" aria-label="Dedicated quality audit page">
      <header className="audit-hero audit-animate-in">
        <div>
          <p className="audit-eyebrow">Client-ready data quality cockpit</p>
          <h1>Roster Standardization Sales Console</h1>
          <p className="audit-subtitle">
            Show clients exactly what is wrong, what will be standardized per column, and why that drives trustable downstream data.
          </p>
          <div className="audit-hero-points">
            <span>Column-level impact clarity</span>
            <span>Actionable standardization plan</span>
            <span>Executive-ready KPI narrative</span>
          </div>
        </div>
        <div className="audit-hero-actions">
          <p className="audit-workspace-badge">{workspaceId ? `Workspace ${workspaceId}` : "Connecting workspace…"}</p>
          <a className="secondary-button audit-back-link" href="/">
            Back to main workspace
          </a>
        </div>
      </header>

      <section className="audit-kpi-strip audit-animate-in" aria-label="Audit KPI strip">
        <article className="audit-kpi-card is-critical">
          <p className="audit-kpi-label">Critical</p>
          <p className="audit-kpi-value">{kpis.critical}</p>
        </article>
        <article className="audit-kpi-card is-warning">
          <p className="audit-kpi-label">Warning</p>
          <p className="audit-kpi-value">{kpis.warning}</p>
        </article>
        <article className="audit-kpi-card is-neutral">
          <p className="audit-kpi-label">Info</p>
          <p className="audit-kpi-value">{kpis.info}</p>
        </article>
        <article className="audit-kpi-card is-total">
          <p className="audit-kpi-label">Total findings</p>
          <p className="audit-kpi-value">{kpis.total}</p>
        </article>
      </section>

      <div className="audit-layout">
        <section className="audit-upload-card audit-animate-in" aria-label="Upload files for audit">
          <h2>Upload roster or notes</h2>
          <p className="audit-upload-copy">Drop files here or use the picker. Supported: CSV, XLS, XLSX, TXT, MD, JSON.</p>
          <div
            className={`audit-dropzone ${isDragOver ? "is-drag-over" : ""}`}
            role="button"
            tabIndex={uploading ? -1 : 0}
            onClick={() => {
              if (!uploading) {
                inputRef.current?.click();
              }
            }}
            onKeyDown={(event) => {
              if ((event.key === "Enter" || event.key === " ") && !uploading) {
                event.preventDefault();
                inputRef.current?.click();
              }
            }}
            onDragOver={(event) => {
              event.preventDefault();
              if (!uploading) {
                setIsDragOver(true);
              }
            }}
            onDragEnter={(event) => {
              event.preventDefault();
              if (!uploading) {
                setIsDragOver(true);
              }
            }}
            onDragLeave={(event) => {
              event.preventDefault();
              if (!event.currentTarget.contains(event.relatedTarget as Node | null)) {
                setIsDragOver(false);
              }
            }}
            onDrop={(event) => {
              event.preventDefault();
              setIsDragOver(false);
              if (!uploading) {
                applyFiles(event.dataTransfer?.files || null);
              }
            }}
            aria-disabled={uploading}
          >
            <p className="audit-dropzone-title">Drag and drop files here</p>
            <p className="audit-dropzone-subtitle">or click to browse from your device</p>
            <input
              ref={inputRef}
              type="file"
              accept=".csv,.xlsx,.xls,.txt,.md,.json"
              multiple
              hidden
              disabled={uploading}
              onChange={(event) => {
                applyFiles(event.target.files);
                event.currentTarget.value = "";
              }}
            />
          </div>

          <div className="audit-actions-row">
            <button type="button" className="primary-button" disabled={!canRunQualityAudit} onClick={onRunQualityAudit}>
              Run deep quality audit
            </button>
          </div>
        </section>

        {pendingChoices.length > 0 ? (
          <section className="pending-choice-panel audit-animate-in" aria-label="Pending roster choices">
            <p className="pending-choice-title">Choose a roster to analyze</p>
            <div className="pending-choice-buttons">
              {pendingChoices.map((choice, index) => (
                <button
                  key={`${choice.id || choice.name || choice.path || index}`}
                  type="button"
                  className="secondary-button"
                  onClick={() => onSelectPendingChoice(index)}
                >
                  {index + 1}. {choice.name || choice.path || `candidate-${index + 1}`}
                </button>
              ))}
            </div>
          </section>
        ) : null}

        <OperationPanel
          operation={currentOperation}
          localActivity={localActivity}
          operations={operations}
          onCancel={onCancelOperation}
          onRetry={onRetryOperation}
        />

        <AuditClientSummary
          summary={
            clientSummary || {
              headline: "Executive summary pending run",
              kpis: {
                readiness_score: 0,
                columns_profiled: columnAuditSummary?.columns?.length || 0,
                columns_with_findings: 0,
                high_impact_columns: 0,
                estimated_rows_impacted: 0,
              },
              top_priority_columns: [],
              why_it_improves_data_quality: [
                "Run deep quality audit to auto-populate client KPI narrative.",
                "The matrix and drawer below will translate technical findings into client-facing standardization actions.",
              ],
            }
          }
        />

        <div className="audit-matrix-layout">
          <AuditColumnMatrix summary={columnAuditSummary || { columns: [] }} selectedColumnKey={selectedColumnKey} onSelectColumn={setSelectedColumnKey} />
          <AuditColumnDrawer
            summary={columnAuditSummary || { columns: [] }}
            qualityAudit={qualityAudit}
            selectedColumnKey={selectedColumnKey}
            onToggleQualityAudit={onToggleQualityAudit}
          />
        </div>

        <AuditStandardizationPlan plan={standardizationPlan || { workstreams: [] }} />

        <section className="audit-findings-panel audit-animate-in" aria-label="Quality audit fallback findings">
          <div className="audit-findings-header">
            <h2>Raw findings fallback</h2>
            <p>{qualityAudit.length} item(s)</p>
          </div>
          <QualityAuditList items={qualityAudit} onToggle={onToggleQualityAudit} />
        </section>
      </div>
    </main>
  );
}
