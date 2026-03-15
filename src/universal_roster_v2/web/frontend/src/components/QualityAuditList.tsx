import { useMemo, useState } from "react";
import type { QualityAuditItem } from "../lib/types";

function reviewToggleLabel(approved: boolean | undefined) {
  return approved === false ? "unchecked" : "checked";
}

function formatPercent(value: number | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "0%";
  }
  return `${Math.round(Math.max(0, value) * 100)}%`;
}

function severityClass(severity: string | undefined): string {
  const key = String(severity || "info").toLowerCase();
  if (key === "error") {
    return "qa-severity-error";
  }
  if (key === "warning") {
    return "qa-severity-warning";
  }
  return "qa-severity-info";
}

interface QualityAuditListProps {
  items: QualityAuditItem[];
  onToggle: (itemId: string, approved: boolean) => void;
}

export function QualityAuditList({ items, onToggle }: QualityAuditListProps) {
  const [expandedId, setExpandedId] = useState<string>("");

  const sortedItems = useMemo(() => {
    const order = { error: 0, warning: 1, info: 2 } as const;
    return [...items].sort((a, b) => {
      const sa = String(a.severity || "info").toLowerCase();
      const sb = String(b.severity || "info").toLowerCase();
      const oa = order[sa as keyof typeof order] ?? 3;
      const ob = order[sb as keyof typeof order] ?? 3;
      if (oa !== ob) {
        return oa - ob;
      }
      return String(a.id || "").localeCompare(String(b.id || ""));
    });
  }, [items]);

  if (!sortedItems.length) {
    return <p className="empty">No quality audit findings yet.</p>;
  }

  return (
    <ul className="item-list qa-list-grid">
      {sortedItems.map((item) => {
        const severity = (item.severity || "info").toLowerCase();
        const scopeParts = [
          item.source_column ? `source=${item.source_column}` : "",
          item.target_field ? `target=${item.target_field}` : "",
          typeof item.affected_rows === "number" ? `rows=${item.affected_rows}` : "",
          typeof item.affected_pct === "number" ? `pct=${formatPercent(item.affected_pct)}` : "",
        ].filter(Boolean);

        const fix = item.suggested_fix || {};
        const fixAction = typeof fix.action === "string" ? fix.action : "review";
        const fixDescription = typeof fix.description === "string" ? fix.description : "Review and correct source values.";
        const fixParams = fix.params && typeof fix.params === "object" ? (fix.params as Record<string, unknown>) : {};
        const evidence = item.evidence && typeof item.evidence === "object" ? (item.evidence as Record<string, unknown>) : {};
        const expanded = expandedId === item.id;

        return (
          <li className="item-row qa-item-row qa-finding-card" key={item.id}>
            <input
              type="checkbox"
              checked={item.approved !== false}
              onChange={(event) => onToggle(item.id, event.target.checked)}
              aria-label={`Toggle ${item.id}`}
            />
            <div className="item-copy qa-card-content">
              <p className="item-title qa-title-row">
                <span>{(item.title || item.id || "quality finding") + " (" + reviewToggleLabel(item.approved) + ")"}</span>
                <span className={`qa-severity-badge ${severityClass(severity)}`}>{severity}</span>
              </p>
              <p className="item-subtitle">{(item.category || item.rule_type || "quality") + " · " + (item.message || "no details")}</p>
              {!!scopeParts.length && <p className="qa-meta">{scopeParts.join(" · ")}</p>}

              <div className="qa-card-footer-row">
                <div className="qa-card-chips">
                  <span className="qa-chip">confidence {(Number(item.confidence || 0) * 100).toFixed(0)}%</span>
                  {item.confidence_band ? <span className="qa-chip">{String(item.confidence_band)}</span> : null}
                </div>
                <button
                  type="button"
                  className="secondary-button qa-expand-button"
                  onClick={() => setExpandedId(expanded ? "" : item.id)}
                >
                  {expanded ? "Hide details" : "Show details"}
                </button>
              </div>

              <div className={`qa-expand-region ${expanded ? "is-open" : ""}`} hidden={!expanded}>
                <div className="qa-structured-block">
                  <p className="qa-label">Suggested fix</p>
                  <p className="qa-value">{`${fixAction}: ${fixDescription}`}</p>
                  {Object.keys(fixParams).length > 0 && <pre className="qa-json">{JSON.stringify(fixParams, null, 2)}</pre>}
                </div>

                {(Object.keys(evidence).length > 0 || (item.sample_values || []).length > 0) && (
                  <div className="qa-structured-block">
                    <p className="qa-label">Evidence</p>
                    {Object.keys(evidence).length > 0 && <pre className="qa-json">{JSON.stringify(evidence, null, 2)}</pre>}
                    {(item.sample_values || []).length > 0 && <p className="qa-value">Samples: {(item.sample_values || []).slice(0, 6).join(", ")}</p>}
                  </div>
                )}
              </div>
            </div>
          </li>
        );
      })}
    </ul>
  );
}
