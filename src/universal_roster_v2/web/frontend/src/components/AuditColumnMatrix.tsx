import { useMemo } from "react";
import type { ColumnAuditSummary, ColumnAuditSummaryRow } from "../lib/types";

interface AuditColumnMatrixProps {
  summary: ColumnAuditSummary;
  selectedColumnKey: string;
  onSelectColumn: (columnKey: string) => void;
}

const severityOrder: Record<string, number> = { error: 0, warning: 1, info: 2 };

function impactClass(tier: string) {
  const key = String(tier || "low").toLowerCase();
  if (key === "high") {
    return "is-high";
  }
  if (key === "medium") {
    return "is-medium";
  }
  return "is-low";
}

export function AuditColumnMatrix({ summary, selectedColumnKey, onSelectColumn }: AuditColumnMatrixProps) {
  const rows = useMemo(() => {
    return [...(summary.columns || [])].sort((a, b) => {
      const scoreDelta = Number(b.column_rank_score || 0) - Number(a.column_rank_score || 0);
      if (scoreDelta !== 0) {
        return scoreDelta;
      }
      const aSeverity = Object.entries(a.severity_counts || {}).sort((x, y) => (severityOrder[x[0]] ?? 9) - (severityOrder[y[0]] ?? 9))[0]?.[0] || "info";
      const bSeverity = Object.entries(b.severity_counts || {}).sort((x, y) => (severityOrder[x[0]] ?? 9) - (severityOrder[y[0]] ?? 9))[0]?.[0] || "info";
      const severityDelta = (severityOrder[aSeverity] ?? 9) - (severityOrder[bSeverity] ?? 9);
      if (severityDelta !== 0) {
        return severityDelta;
      }
      return String(a.column_key || "").localeCompare(String(b.column_key || ""));
    });
  }, [summary.columns]);

  return (
    <section className="audit-matrix-panel audit-animate-in" aria-label="Column audit matrix">
      <div className="audit-panel-header">
        <div>
          <p className="audit-panel-eyebrow">Column matrix</p>
          <h2>What exists, what we standardize, why it matters</h2>
        </div>
        <p>{rows.length} columns</p>
      </div>

      <div className="audit-column-table" role="table" aria-label="Column audit matrix table">
        <div className="audit-column-header" role="row">
          <span role="columnheader">Column</span>
          <span role="columnheader">Status</span>
          <span role="columnheader">Findings</span>
          <span role="columnheader">Impact</span>
          <span role="columnheader">Recommended action</span>
        </div>
        {!rows.length ? (
          <div className="audit-column-empty" role="row">
            <p>No column audit data yet. Run deep quality audit to populate this matrix.</p>
          </div>
        ) : null}
        {rows.map((row: ColumnAuditSummaryRow) => {
          const selected = row.column_key === selectedColumnKey;
          return (
            <button
              type="button"
              key={row.column_key}
              className={`audit-column-row ${selected ? "is-selected" : ""}`}
              onClick={() => onSelectColumn(row.column_key)}
            >
              <span>
                <strong>{row.column_label || row.column_key}</strong>
                <small>{row.sample_values?.slice(0, 2).join(" · ") || "No sample preview"}</small>
              </span>
              <span>{row.mapped ? "Mapped" : "Needs mapping"}</span>
              <span>{row.finding_count}</span>
              <span>
                <span className={`audit-impact-pill ${impactClass(String(row.impact_tier || "low"))}`}>{row.impact_tier || "low"}</span>
              </span>
              <span>{row.recommended_action || "review"}</span>
            </button>
          );
        })}
      </div>
    </section>
  );
}
