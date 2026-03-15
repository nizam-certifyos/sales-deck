import type { OperationRecord } from "../lib/types";

interface OperationPanelProps {
  operation: OperationRecord | null;
  localActivity: { message: string; status: string; percent: number } | null;
  operations: OperationRecord[];
  onCancel: () => void;
  onRetry: () => void;
}

function formatPercent(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.max(0, Math.min(100, value));
  }
  return 0;
}

export function OperationPanel({ operation, localActivity, operations, onCancel, onRetry }: OperationPanelProps) {
  const percent = operation ? formatPercent(operation.progress?.percent) : formatPercent(localActivity?.percent ?? 0);
  const kind = operation ? `${operation.kind || "operation"} · ${operation.id}` : "Operation";
  const phase = operation?.progress?.message || operation?.progress?.phase || localActivity?.message || "Waiting for activity…";
  const status = operation?.status || localActivity?.status || "idle";

  const cancelDisabled = !operation || !(operation.status === "queued" || operation.status === "running");
  const retryDisabled = !operation || !(operation.status === "failed" || operation.status === "canceled");

  return (
    <section className="operation-panel" aria-live="polite">
      <div className="operation-main">
        <div>
          <p className="operation-kind">{kind}</p>
          <p className="operation-phase">{phase}</p>
        </div>
        <p className="operation-status">{status}</p>
      </div>

      <div className="progress-track" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(percent)}>
        <div className="progress-fill" style={{ width: `${percent}%` }} />
      </div>

      <div className="operation-meta">
        <p className="operation-percent">{Math.round(percent)}%</p>
        <div className="operation-buttons">
          <button type="button" className="danger-button" disabled={cancelDisabled} onClick={onCancel}>
            Cancel
          </button>
          <button type="button" className="secondary-button" disabled={retryDisabled} onClick={onRetry}>
            Retry
          </button>
        </div>
      </div>

      <div>
        <p className="operation-history-title">Recent operations</p>
        <ul className="operation-history">
          {operations.length === 0 ? (
            <li>No operations yet.</li>
          ) : (
            operations.slice(0, 6).map((item) => (
              <li key={item.id}>
                {item.kind || "operation"} · {item.status || "queued"} · {item.id}
              </li>
            ))
          )}
        </ul>
      </div>
    </section>
  );
}
