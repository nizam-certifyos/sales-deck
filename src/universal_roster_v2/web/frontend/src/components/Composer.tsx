import { useRef } from "react";

interface ComposerProps {
  disabled: boolean;
  sending: boolean;
  uploading: boolean;
  message: string;
  onMessageChange: (value: string) => void;
  onSend: () => void;
  onFilesSelected: (files: File[]) => void;
}

export function Composer(props: ComposerProps) {
  const { disabled, sending, uploading, message, onMessageChange, onSend, onFilesSelected } = props;
  const inputRef = useRef<HTMLInputElement | null>(null);

  return (
    <footer className="composer">
      <div className="composer-actions">
        <label
          className="upload-button"
          role="button"
          aria-disabled={disabled || uploading}
          tabIndex={disabled || uploading ? -1 : 0}
          onKeyDown={(event) => {
            if ((event.key === "Enter" || event.key === " ") && !disabled && !uploading) {
              event.preventDefault();
              inputRef.current?.click();
            }
          }}
        >
          Upload file or folder
          <input
            ref={inputRef}
            type="file"
            accept=".csv,.xlsx,.xls,.txt,.md,.json"
            multiple
            hidden
            disabled={disabled || uploading}
            onChange={(event) => {
              const files = Array.from(event.target.files || []);
              event.currentTarget.value = "";
              onFilesSelected(files);
            }}
          />
        </label>

        <button type="button" className="primary-button" disabled={disabled || sending || uploading} onClick={onSend}>
          Send
        </button>
      </div>

      <label className="visually-hidden" htmlFor="chat-input">
        Message
      </label>
      <textarea
        id="chat-input"
        rows={3}
        placeholder="Ask for analysis, review a tab, or approve/reject items…"
        value={message}
        disabled={disabled}
        onChange={(event) => onMessageChange(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === "Enter" && !event.shiftKey) {
            event.preventDefault();
            onSend();
          }
        }}
      />
    </footer>
  );
}
