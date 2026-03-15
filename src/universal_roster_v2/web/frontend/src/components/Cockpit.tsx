import type {
  CockpitTab,
  MappingItem,
  ProfileSummary,
  ReviewSummary,
  TransformationItem,
  ValidationItem,
  QualityAuditItem,
} from "../lib/types";
import { QualityAuditList } from "./QualityAuditList";

interface CockpitProps {
  activeTab: CockpitTab;
  profileSummary: ProfileSummary;
  reviewSummary: ReviewSummary;
  mappings: MappingItem[];
  transformations: TransformationItem[];
  validations: ValidationItem[];
  qualityAudit: QualityAuditItem[];
  onTabChange: (tab: CockpitTab) => void;
  onToggle: (itemType: "mappings" | "transformations" | "bq_validations" | "quality_audit", itemId: string, approved: boolean) => void;
}

function reviewToggleLabel(approved: boolean | undefined) {
  return approved === false ? "unchecked" : "checked";
}

function EmptyState({ label }: { label: string }) {
  return <p className="empty">{label}</p>;
}

function SchemaPanel({ summary }: { summary: ProfileSummary }) {
  if (!summary.file_name) {
    return <EmptyState label="Upload a roster to view schema details." />;
  }

  const metrics = [
    ["File", summary.file_name || "n/a"],
    ["Roster type", summary.roster_type_detected || "unknown"],
    ["Columns", String(summary.column_count || 0)],
    ["Rows profiled", String(summary.rows_profiled || 0)],
    ["Rows total", String(summary.rows_total || 0)],
    ["Mode", summary.profiling_mode || "sample"],
  ];

  return (
    <>
      <ul className="kv-grid">
        {metrics.map(([label, value]) => (
          <li key={label}>
            <strong>{label}</strong>
            <span>{value}</span>
          </li>
        ))}
      </ul>

      {!summary.samples.length ? (
        <div style={{ marginTop: 10 }}>
          <EmptyState label="No sample values available yet." />
        </div>
      ) : (
        <ul className="item-list" style={{ marginTop: 10 }}>
          {summary.samples.map((sample) => (
            <li key={sample.column} className="item-row">
              <div className="item-copy">
                <p className="item-title">{sample.column || "column"}</p>
                <p className="item-subtitle">{sample.values.length ? sample.values.join(", ") : "(no sample values)"}</p>
              </div>
            </li>
          ))}
        </ul>
      )}
    </>
  );
}

function SuggestionsPanel({ summary }: { summary: ReviewSummary }) {
  if (!summary.total) {
    return <EmptyState label="Suggestions appear after roster analysis." />;
  }

  const sectionCard = (
    title: string,
    key: "mappings" | "transformations" | "bq_validations" | "quality_audit"
  ) => {
    const section = summary.sections[key];
    const confidence = summary.confidence[key];
    return (
      <article className="suggestion-card" key={key}>
        <h4>{title}</h4>
        <p>
          Total {section.total} · Unchecked {section.unchecked}
        </p>
        <ul>
          <li>High confidence: {confidence.high}</li>
          <li>Medium confidence: {confidence.medium}</li>
          <li>Low confidence: {confidence.low}</li>
        </ul>
      </article>
    );
  };

  return (
    <div className="suggestion-grid">
      {sectionCard("Mappings", "mappings")}
      {sectionCard("Transformations", "transformations")}
      {sectionCard("BQ Validations", "bq_validations")}
      {sectionCard("Quality Audit", "quality_audit")}
    </div>
  );
}

function MappingList({
  items,
  onToggle,
}: {
  items: MappingItem[];
  onToggle: (itemId: string, approved: boolean) => void;
}) {
  if (!items.length) {
    return <EmptyState label="No mappings available yet." />;
  }
  return (
    <ul className="item-list">
      {items.map((item) => (
        <li className="item-row" key={item.id}>
          <input
            type="checkbox"
            checked={item.approved !== false}
            onChange={(event) => onToggle(item.id, event.target.checked)}
          />
          <div className="item-copy">
            <p className="item-title">
              {(item.source_column || item.id || "mapping") + " → " + (item.target_field || "?")} ({reviewToggleLabel(item.approved)})
            </p>
            <p className="item-subtitle">
              Confidence: {item.confidence_band || "unknown"} · target={item.target_field || "n/a"}
            </p>
          </div>
        </li>
      ))}
    </ul>
  );
}

function TransformationList({
  items,
  onToggle,
}: {
  items: TransformationItem[];
  onToggle: (itemId: string, approved: boolean) => void;
}) {
  if (!items.length) {
    return <EmptyState label="No transformations available yet." />;
  }
  return (
    <ul className="item-list">
      {items.map((item) => (
        <li className="item-row" key={item.id}>
          <input
            type="checkbox"
            checked={item.approved !== false}
            onChange={(event) => onToggle(item.id, event.target.checked)}
          />
          <div className="item-copy">
            <p className="item-title">
              {(item.name || item.id || "transformation") + " (" + reviewToggleLabel(item.approved) + ")"}
            </p>
            <p className="item-subtitle">
              {(item.source_columns || []).join(", ") || "no source"} → {(item.target_fields || []).join(", ") || "n/a"}
            </p>
          </div>
        </li>
      ))}
    </ul>
  );
}

function ValidationList({
  items,
  onToggle,
}: {
  items: ValidationItem[];
  onToggle: (itemId: string, approved: boolean) => void;
}) {
  if (!items.length) {
    return <EmptyState label="No validations available yet." />;
  }
  return (
    <ul className="item-list">
      {items.map((item) => (
        <li className="item-row" key={item.id}>
          <input
            type="checkbox"
            checked={item.approved !== false}
            onChange={(event) => onToggle(item.id, event.target.checked)}
          />
          <div className="item-copy">
            <p className="item-title">{(item.name || item.id || "validation") + " (" + reviewToggleLabel(item.approved) + ")"}</p>
            <p className="item-subtitle">{item.message || item.sql_expression || "no details"}</p>
          </div>
        </li>
      ))}
    </ul>
  );
}

const tabOrder: Array<{ key: CockpitTab; label: string }> = [
  { key: "schema", label: "Schema" },
  { key: "mappings", label: "Mappings" },
  { key: "suggestions", label: "Suggestions" },
  { key: "transformations", label: "Transformations" },
  { key: "bq_validations", label: "BQ Validations" },
  { key: "quality_audit", label: "Quality Audit" },
];

export function Cockpit(props: CockpitProps) {
  const { activeTab, profileSummary, reviewSummary, mappings, transformations, validations, qualityAudit, onTabChange, onToggle } = props;
  const reviewStatus = profileSummary.file_name
    ? `${reviewSummary.total} review item(s) · ${reviewSummary.unchecked} unchecked`
    : "Waiting for roster profile…";

  return (
    <section className="cockpit" aria-label="Review cockpit">
      <div className="cockpit-header">
        <p className="cockpit-title">Review cockpit</p>
        <p className="cockpit-status">{reviewStatus}</p>
      </div>
      <div className="cockpit-tabs" role="tablist" aria-label="Review sections">
        {tabOrder.map((tab) => (
          <button
            key={tab.key}
            type="button"
            className={`cockpit-tab ${activeTab === tab.key ? "is-active" : ""}`}
            aria-selected={activeTab === tab.key}
            onClick={() => onTabChange(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      <div className="cockpit-panel" hidden={activeTab !== "schema"}>
        <SchemaPanel summary={profileSummary} />
      </div>
      <div className="cockpit-panel" hidden={activeTab !== "mappings"}>
        <MappingList items={mappings} onToggle={(itemId, approved) => onToggle("mappings", itemId, approved)} />
      </div>
      <div className="cockpit-panel" hidden={activeTab !== "suggestions"}>
        <SuggestionsPanel summary={reviewSummary} />
      </div>
      <div className="cockpit-panel" hidden={activeTab !== "transformations"}>
        <TransformationList
          items={transformations}
          onToggle={(itemId, approved) => onToggle("transformations", itemId, approved)}
        />
      </div>
      <div className="cockpit-panel" hidden={activeTab !== "bq_validations"}>
        <ValidationList
          items={validations}
          onToggle={(itemId, approved) => onToggle("bq_validations", itemId, approved)}
        />
      </div>
      <div className="cockpit-panel" hidden={activeTab !== "quality_audit"}>
        <QualityAuditList
          items={qualityAudit}
          onToggle={(itemId, approved) => onToggle("quality_audit", itemId, approved)}
        />
      </div>
    </section>
  );
}
