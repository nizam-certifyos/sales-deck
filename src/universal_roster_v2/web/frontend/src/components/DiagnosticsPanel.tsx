interface DiagnosticsPanelProps {
  enabled: boolean;
  open: boolean;
  onClose: () => void;
  payload: Record<string, unknown>;
}

export function DiagnosticsPanel({ enabled, open, onClose, payload }: DiagnosticsPanelProps) {
  if (!enabled || !open) {
    return null;
  }

  return (
    <section className="diagnostics">
      <div className="diagnostics-header">
        <p className="diagnostics-title">Runtime diagnostics</p>
        <button id="diagnosticsClose" type="button" className="secondary-button" onClick={onClose}>
          Close
        </button>
      </div>
      <pre className="diagnostics-content">{JSON.stringify(payload, null, 2)}</pre>
    </section>
  );
}
