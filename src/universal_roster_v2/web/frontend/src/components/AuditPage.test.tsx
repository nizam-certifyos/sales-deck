import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { AuditPage } from "./AuditPage";

describe("AuditPage", () => {
  it("renders matrix, drawer, and standardization plan when additive payloads exist", () => {
    const onToggle = vi.fn();

    render(
      <AuditPage
        workspaceId="w-1"
        uploading={false}
        qualityAudit={[
          {
            id: "qa::email",
            severity: "warning",
            source_column: "Email",
            target_field: "primaryEmail",
            message: "Invalid format",
            title: "Email format",
            suggested_fix: { action: "transform", description: "Normalize email" },
            approved: true,
          },
        ]}
        columnAuditSummary={{
          columns: [
            {
              column_key: "Email",
              column_label: "Email",
              sample_values: ["a@example.com"],
              mapped: true,
              profiled: true,
              severity_counts: { error: 0, warning: 1, info: 0 },
              finding_count: 1,
              affected_rows: 1,
              affected_pct: 0.2,
              linked_item_ids: ["qa::email"],
              linked_findings: [
                {
                  id: "qa::email",
                  severity: "warning",
                  title: "Email format",
                  message: "Invalid format",
                  action_group: "transform",
                  affected_rows: 1,
                  affected_pct: 0.2,
                },
              ],
              recommended_action: "transform",
              unchecked_count: 0,
              column_rank_score: 9,
              impact_tier: "medium",
            },
          ],
        }}
        standardizationPlan={{
          workstreams: [
            {
              id: "format_normalization",
              title: "Format normalization",
              narrative: "Normalize values",
              column_count: 1,
              estimated_rows_impacted: 1,
              actions: [{ column_key: "Email", action: "transform", reason: "Fix format", linked_item_ids: ["qa::email"] }],
            },
          ],
        }}
        clientSummary={{
          headline: "Column-level standardization plan ready",
          kpis: { readiness_score: 85, columns_profiled: 1, columns_with_findings: 1, high_impact_columns: 0, estimated_rows_impacted: 1 },
          top_priority_columns: [
            {
              column_key: "Email",
              recommended_action: "transform",
              finding_count: 1,
              impact_tier: "medium",
              column_rank_score: 9,
            },
          ],
          why_it_improves_data_quality: ["Actions are linked to findings"],
        }}
        pendingChoices={[]}
        canRunQualityAudit
        currentOperation={null}
        operations={[]}
        localActivity={null}
        onFilesSelected={vi.fn()}
        onRunQualityAudit={vi.fn()}
        onToggleQualityAudit={onToggle}
        onCancelOperation={vi.fn()}
        onRetryOperation={vi.fn()}
        onSelectPendingChoice={vi.fn()}
      />
    );

    expect(screen.getByText(/Client summary/i)).toBeInTheDocument();
    expect(screen.getByText(/Column matrix/i)).toBeInTheDocument();
    expect(screen.getByText(/Column detail/i)).toBeInTheDocument();
    expect(screen.getByText(/Standardization plan/i)).toBeInTheDocument();

    const checkbox = screen.getByRole("checkbox", { name: /toggle qa::email/i });
    fireEvent.click(checkbox);
    expect(onToggle).toHaveBeenCalledWith("qa::email", false);
  });
});
