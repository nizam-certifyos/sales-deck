import type { ClientSummary } from "../lib/types";

interface AuditClientSummaryProps {
  summary: ClientSummary;
}

function metric(summary: ClientSummary, key: string) {
  return Number(summary.kpis?.[key] ?? 0);
}

export function AuditClientSummary({ summary }: AuditClientSummaryProps) {
  const readiness = metric(summary, "readiness_score");
  return (
    <section className="audit-client-summary audit-animate-in" aria-label="Client audit summary">
      <div className="audit-client-summary-header">
        <div>
          <p className="audit-panel-eyebrow">Client summary</p>
          <h2>{summary.headline || "Column-level audit summary"}</h2>
        </div>
        <p className="audit-readiness-score">Readiness score: {readiness}</p>
      </div>

      <div className="audit-client-kpis">
        <article className="audit-client-kpi-card">
          <p>Columns profiled</p>
          <strong>{metric(summary, "columns_profiled")}</strong>
        </article>
        <article className="audit-client-kpi-card">
          <p>Columns with findings</p>
          <strong>{metric(summary, "columns_with_findings")}</strong>
        </article>
        <article className="audit-client-kpi-card is-critical">
          <p>High-impact columns</p>
          <strong>{metric(summary, "high_impact_columns")}</strong>
        </article>
        <article className="audit-client-kpi-card">
          <p>Estimated rows impacted</p>
          <strong>{metric(summary, "estimated_rows_impacted")}</strong>
        </article>
      </div>

      <div className="audit-client-why">
        <h3>Why this improves data quality</h3>
        <ul>
          {(summary.why_it_improves_data_quality || []).map((line, index) => (
            <li key={`${line}-${index}`}>{line}</li>
          ))}
        </ul>
      </div>
    </section>
  );
}
