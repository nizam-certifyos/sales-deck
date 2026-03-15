import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { Cockpit } from "./Cockpit";

const baseProfile = {
  file_name: "roster.csv",
  roster_type_detected: "practitioner",
  column_count: 10,
  sample_size: 20,
  profiling_mode: "sample",
  rows_profiled: 20,
  rows_total: 100,
  samples: [{ column: "npi", values: ["111"] }],
  semantic_evidence: [],
  sheet_drift: {},
};

const baseReview = {
  total: 3,
  unchecked: 1,
  sections: {
    mappings: { total: 1, unchecked: 0 },
    transformations: { total: 1, unchecked: 1 },
    bq_validations: { total: 1, unchecked: 0 },
    quality_audit: { total: 1, unchecked: 0 },
  },
  confidence: {
    mappings: { high: 1, medium: 0, low: 0 },
    transformations: { high: 0, medium: 1, low: 0 },
    bq_validations: { high: 0, medium: 0, low: 1 },
    quality_audit: { high: 0, medium: 1, low: 0 },
  },
};

describe("Cockpit", () => {
  it("renders all required cockpit tabs including mappings", () => {
    render(
      <Cockpit
        activeTab="schema"
        profileSummary={baseProfile}
        reviewSummary={baseReview}
        mappings={[]}
        transformations={[]}
        validations={[]}
        qualityAudit={[]}
        onTabChange={vi.fn()}
        onToggle={vi.fn()}
      />
    );

    expect(screen.getByRole("button", { name: "Schema" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Mappings" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Suggestions" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Transformations" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "BQ Validations" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Quality Audit" })).toBeInTheDocument();
  });

  it("triggers tab change and toggle callbacks", () => {
    const onTabChange = vi.fn();
    const onToggle = vi.fn();

    render(
      <Cockpit
        activeTab="mappings"
        profileSummary={baseProfile}
        reviewSummary={baseReview}
        mappings={[{ id: "m-1", source_column: "src", target_field: "dst", approved: true }]}
        transformations={[]}
        validations={[]}
        qualityAudit={[]}
        onTabChange={onTabChange}
        onToggle={onToggle}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Suggestions" }));
    expect(onTabChange).toHaveBeenCalledWith("suggestions");

    fireEvent.click(screen.getByRole("checkbox"));
    expect(onToggle).toHaveBeenCalledWith("mappings", "m-1", false);
  });

  it("renders quality details and emits quality toggle", () => {
    const onToggle = vi.fn();
    render(
      <Cockpit
        activeTab="quality_audit"
        profileSummary={baseProfile}
        reviewSummary={baseReview}
        mappings={[]}
        transformations={[]}
        validations={[]}
        qualityAudit={[
          {
            id: "qa::state_zip",
            title: "State/ZIP mismatch",
            message: "Mismatch detected",
            severity: "warning",
            category: "consistency",
            source_column: "State",
            target_field: "addressState",
            affected_rows: 3,
            affected_pct: 0.25,
            sample_values: ["CA/10001"],
            evidence: { pairs_evaluated: 10 },
            suggested_fix: {
              action: "review",
              description: "Check state and ZIP alignment",
              params: { state_column: "State", zip_column: "Zip" },
            },
            approved: true,
          },
        ]}
        onTabChange={vi.fn()}
        onToggle={onToggle}
      />
    );

    expect(screen.getByText(/State\/ZIP mismatch/i)).toBeInTheDocument();
    expect(screen.getByText(/Suggested fix/i)).toBeInTheDocument();
    expect(screen.getByText(/Evidence/i)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("checkbox"));
    expect(onToggle).toHaveBeenCalledWith("quality_audit", "qa::state_zip", false);
  });
});
