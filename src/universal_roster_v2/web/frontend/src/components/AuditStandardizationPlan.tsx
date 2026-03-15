import type { StandardizationPlan } from "../lib/types";

interface AuditStandardizationPlanProps {
  plan: StandardizationPlan;
}

export function AuditStandardizationPlan({ plan }: AuditStandardizationPlanProps) {
  const workstreams = plan.workstreams || [];
  if (!workstreams.length) {
    return (
      <section className="audit-plan-panel audit-animate-in" aria-label="Standardization plan">
        <div className="audit-panel-header">
          <div>
            <p className="audit-panel-eyebrow">Standardization plan</p>
            <h2>Client-facing workstreams</h2>
          </div>
        </div>
        <p className="empty">No workstreams yet. Run deep quality audit to generate format, remediation, dedupe, and validation tracks.</p>
      </section>
    );
  }

  return (
    <section className="audit-plan-panel audit-animate-in" aria-label="Standardization plan">
      <div className="audit-panel-header">
        <div>
          <p className="audit-panel-eyebrow">Standardization plan</p>
          <h2>Client-facing workstreams</h2>
        </div>
      </div>
      <div className="audit-workstream-grid">
        {workstreams.map((workstream) => (
          <article key={workstream.id} className="audit-workstream-card">
            <h3>{workstream.title}</h3>
            <p>{workstream.narrative}</p>
            <p className="audit-workstream-meta">
              {workstream.column_count} column(s) · {workstream.estimated_rows_impacted} rows impacted
            </p>
            <ul>
              {(workstream.actions || []).slice(0, 5).map((action) => (
                <li key={`${workstream.id}-${action.column_key}-${action.action}`}>
                  <strong>{action.column_key}</strong>: {action.action} — {action.reason}
                </li>
              ))}
            </ul>
          </article>
        ))}
      </div>
    </section>
  );
}
