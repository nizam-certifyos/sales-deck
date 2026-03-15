import { useMemo } from "react";
import { QualityAuditList } from "./QualityAuditList";
import type { ColumnAuditSummary, ColumnAuditSummaryRow, QualityAuditItem } from "../lib/types";

interface AuditColumnDrawerProps {
  summary: ColumnAuditSummary;
  qualityAudit: QualityAuditItem[];
  selectedColumnKey: string;
  onToggleQualityAudit: (itemId: string, approved: boolean) => void;
}

function issueMatchesColumn(issue: QualityAuditItem, row: ColumnAuditSummaryRow) {
  const issueKeys = new Set([String(issue.column_key || ""), String(issue.source_column || ""), String(issue.target_field || "")]);
  return issueKeys.has(String(row.column_key || "")) || issueKeys.has(String(row.column_label || ""));
}

export function AuditColumnDrawer({ summary, qualityAudit, selectedColumnKey, onToggleQualityAudit }: AuditColumnDrawerProps) {
  const row = useMemo(() => summary.columns.find((item) => item.column_key === selectedColumnKey), [selectedColumnKey, summary.columns]);

  const linkedIssues = useMemo(() => {
    if (!row) {
      return [];
    }
    return qualityAudit.filter((issue) => issueMatchesColumn(issue, row));
  }, [qualityAudit, row]);

  if (!row) {
    return (
      <section className="audit-drawer-panel audit-animate-in" aria-label="Column details">
        <div className="audit-panel-header">
          <div>
            <p className="audit-panel-eyebrow">Column detail</p>
            <h2>No column selected yet</h2>
          </div>
        </div>
        <p className="empty">Select a column from the matrix to open linked findings, IDs, and proposed actions.</p>
      </section>
    );
  }

  return (
    <section className="audit-drawer-panel audit-animate-in" aria-label="Column details">
      <div className="audit-panel-header">
        <div>
          <p className="audit-panel-eyebrow">Column detail</p>
          <h2>{row.column_label || row.column_key}</h2>
        </div>
        <p>Rank score {row.column_rank_score.toFixed(2)}</p>
      </div>

      <ul className="audit-detail-metadata">
        <li>
          <strong>Recommended action</strong>
          <span>{row.recommended_action}</span>
        </li>
        <li>
          <strong>Impact tier</strong>
          <span>{row.impact_tier}</span>
        </li>
        <li>
          <strong>Affected rows</strong>
          <span>{row.affected_rows}</span>
        </li>
        <li>
          <strong>Severity mix</strong>
          <span>
            E:{row.severity_counts.error} · W:{row.severity_counts.warning} · I:{row.severity_counts.info}
          </span>
        </li>
      </ul>

      <div className="audit-linked-items">
        <h3>Linked item IDs</h3>
        <div className="audit-chip-row">
          {(row.linked_item_ids || []).map((itemId) => (
            <span className="qa-chip" key={itemId}>
              {itemId}
            </span>
          ))}
        </div>
      </div>

      <div className="audit-linked-items">
        <h3>Column findings</h3>
        <QualityAuditList items={linkedIssues} onToggle={onToggleQualityAudit} />
      </div>
    </section>
  );
}
