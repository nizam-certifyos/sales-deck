interface ChatTimelineProps {
  history: Array<{ role: string; content: string }>;
}

export function ChatTimeline({ history }: ChatTimelineProps) {
  return (
    <section className="chat-timeline" role="log" aria-live="polite" aria-relevant="additions text">
      {history.length === 0 ? (
        <div className="chat-empty">Start by uploading a roster or asking for an analysis.</div>
      ) : (
        history.map((message, index) => (
          <div className={`chat-message ${message.role === "user" ? "user" : "assistant"}`} key={`${message.role}-${index}`}>
            <div className="bubble">{message.content || ""}</div>
          </div>
        ))
      )}
    </section>
  );
}
