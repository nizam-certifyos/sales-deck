"""Persistent folder-first conversation memory for workspace chat flows."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from universal_roster_v2.config import Settings, get_settings


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slug(value: str, fallback: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback
    text = re.sub(r"[^a-z0-9._-]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or fallback


@dataclass(frozen=True)
class ConversationScope:
    workspace_path: str
    tenant_id: str
    client_id: str
    thread_id: str


def scope_signature(scope: ConversationScope) -> str:
    key = f"{scope.workspace_path}|{scope.tenant_id}|{scope.client_id}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


class WorkspaceConversationStore:
    """JSON-backed workspace conversation store keyed by folder + tenant + client (+ thread)."""

    VERSION = 4

    def __init__(self, base_dir: str | Path | None = None, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        default_dir = self.settings.workspace_dir / "workspace_memory"
        self.base_dir = Path(base_dir or default_dir).expanduser().resolve()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.index_path = self.base_dir / "index.json"

    def normalize_scope(
        self,
        workspace_path: str | Path,
        tenant_id: str,
        client_id: str,
        thread_id: Optional[str] = None,
    ) -> ConversationScope:
        workspace = Path(workspace_path).expanduser().resolve()
        workspace.mkdir(parents=True, exist_ok=True)
        tenant = _slug(tenant_id, "default-tenant")
        client = _slug(client_id, "default-client")
        thread = _slug(thread_id or "default", "default")
        return ConversationScope(
            workspace_path=str(workspace),
            tenant_id=tenant,
            client_id=client,
            thread_id=thread,
        )

    @staticmethod
    def _scope_digest(scope: ConversationScope) -> str:
        key = f"{scope.workspace_path}|{scope.tenant_id}|{scope.client_id}|{scope.thread_id}"
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    @staticmethod
    def _workspace_signature(scope: ConversationScope) -> str:
        return scope_signature(scope)

    def workspace_id(self, scope: ConversationScope) -> str:
        return self._scope_digest(scope)[:20]

    def _conversation_path(self, scope: ConversationScope) -> Path:
        workspace_sig = self._workspace_signature(scope)
        folder = self.base_dir / workspace_sig
        folder.mkdir(parents=True, exist_ok=True)
        filename = f"{scope.tenant_id}__{scope.client_id}__{scope.thread_id}.json"
        return folder / filename

    def _base_payload(self, scope: ConversationScope) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "workspace_id": self.workspace_id(scope),
            "workspace_signature": self._workspace_signature(scope),
            "workspace_path": scope.workspace_path,
            "tenant_id": scope.tenant_id,
            "client_id": scope.client_id,
            "thread_id": scope.thread_id,
            "created_at": _utc_now_iso(),
            "updated_at": _utc_now_iso(),
            "chat_history": [],
            "plan_snapshots": [],
            "decisions": [],
            "decision_events": [],
            "run_outputs": [],
            "rationales": [],
            "operations": [],
            "operation_events": [],
            "active_operation_id": None,
            "pending_roster_choices": [],
            "pending_custom_action": None,
            "pending_rationale": None,
            "pending_selected_roster": None,
            "instructions_context": {
                "free_text_notes": [],
                "client_rules": [],
                "schema_caveats": [],
                "exceptions": [],
                "attachment_hints": [],
                "attachments": [],
            },
            "latest_profile": None,
            "latest_plan_path": None,
        }

    def _load_index(self) -> Dict[str, Any]:
        if not self.index_path.exists():
            return {"version": 1, "updated_at": _utc_now_iso(), "workspaces": {}}
        try:
            payload = json.loads(self.index_path.read_text(encoding="utf-8"))
        except Exception:
            return {"version": 1, "updated_at": _utc_now_iso(), "workspaces": {}}

        if not isinstance(payload, dict):
            return {"version": 1, "updated_at": _utc_now_iso(), "workspaces": {}}

        workspaces = payload.get("workspaces")
        if not isinstance(workspaces, dict):
            workspaces = {}
        return {
            "version": 1,
            "updated_at": str(payload.get("updated_at") or _utc_now_iso()),
            "workspaces": workspaces,
        }

    def _save_index(self, payload: Dict[str, Any]) -> None:
        out = {
            "version": 1,
            "updated_at": _utc_now_iso(),
            "workspaces": payload.get("workspaces", {}),
        }
        self.index_path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")

    def _upsert_index(self, scope: ConversationScope, path: Path, updated_at: Optional[str] = None) -> None:
        index = self._load_index()
        workspace_id = self.workspace_id(scope)
        index["workspaces"][workspace_id] = {
            "workspace_id": workspace_id,
            "workspace_signature": self._workspace_signature(scope),
            "workspace_path": scope.workspace_path,
            "tenant_id": scope.tenant_id,
            "client_id": scope.client_id,
            "thread_id": scope.thread_id,
            "conversation_file": str(path),
            "updated_at": updated_at or _utc_now_iso(),
        }
        self._save_index(index)

    def resolve_scope(self, workspace_id: str) -> ConversationScope:
        index = self._load_index()
        item = (index.get("workspaces") or {}).get(workspace_id)
        if not isinstance(item, dict):
            raise KeyError(f"Unknown workspace: {workspace_id}")
        return self.normalize_scope(
            workspace_path=str(item.get("workspace_path") or ""),
            tenant_id=str(item.get("tenant_id") or "default-tenant"),
            client_id=str(item.get("client_id") or "default-client"),
            thread_id=str(item.get("thread_id") or "default"),
        )

    def load(self, scope: ConversationScope) -> Dict[str, Any]:
        path = self._conversation_path(scope)
        if not path.exists():
            payload = self._base_payload(scope)
            self.save(scope, payload)
            return payload

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            payload = self._base_payload(scope)

        if not isinstance(payload, dict):
            payload = self._base_payload(scope)

        base = self._base_payload(scope)
        base.update({k: v for k, v in payload.items() if k in base})

        for key in [
            "chat_history",
            "plan_snapshots",
            "decisions",
            "decision_events",
            "run_outputs",
            "rationales",
            "operations",
            "operation_events",
            "pending_roster_choices",
        ]:
            if not isinstance(base.get(key), list):
                base[key] = []

        instructions = base.get("instructions_context")
        if not isinstance(instructions, dict):
            instructions = self._base_payload(scope)["instructions_context"]
        defaults = self._base_payload(scope)["instructions_context"]
        for k, v in defaults.items():
            if k not in instructions or not isinstance(instructions.get(k), list):
                instructions[k] = list(v)
        base["instructions_context"] = instructions

        for key in ["active_operation_id", "pending_custom_action", "pending_rationale", "pending_selected_roster"]:
            value = base.get(key)
            if value in [None, ""]:
                base[key] = None
            elif key == "active_operation_id":
                base[key] = str(value)
            elif not isinstance(value, dict):
                base[key] = None

        base["workspace_id"] = self.workspace_id(scope)
        base["workspace_signature"] = self._workspace_signature(scope)
        base["workspace_path"] = scope.workspace_path
        base["tenant_id"] = scope.tenant_id
        base["client_id"] = scope.client_id
        base["thread_id"] = scope.thread_id

        self._upsert_index(scope, path, updated_at=str(base.get("updated_at") or _utc_now_iso()))
        return base

    def save(self, scope: ConversationScope, payload: Dict[str, Any]) -> Path:
        path = self._conversation_path(scope)
        out = self._base_payload(scope)
        out.update({k: v for k, v in payload.items() if k in out})
        for key in ["active_operation_id", "pending_custom_action", "pending_rationale", "pending_selected_roster"]:
            value = out.get(key)
            if value in [None, ""]:
                out[key] = None
            elif key == "active_operation_id":
                out[key] = str(value)
            elif not isinstance(value, dict):
                out[key] = None

        out["workspace_id"] = self.workspace_id(scope)
        out["workspace_signature"] = self._workspace_signature(scope)
        out["workspace_path"] = scope.workspace_path
        out["tenant_id"] = scope.tenant_id
        out["client_id"] = scope.client_id
        out["thread_id"] = scope.thread_id
        out["updated_at"] = _utc_now_iso()

        instructions = out.get("instructions_context")
        defaults = self._base_payload(scope)["instructions_context"]
        if not isinstance(instructions, dict):
            instructions = defaults
        else:
            for key, default_value in defaults.items():
                if key not in instructions or not isinstance(instructions.get(key), list):
                    instructions[key] = list(default_value)
        out["instructions_context"] = instructions

        path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
        self._upsert_index(scope, path, updated_at=out["updated_at"])
        return path

    def append_chat_message(
        self,
        scope: ConversationScope,
        role: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload = self.load(scope)
        history = payload.setdefault("chat_history", [])
        history.append(
            {
                "timestamp": _utc_now_iso(),
                "role": str(role or "assistant"),
                "content": str(content or ""),
                "metadata": metadata or {},
            }
        )
        payload["chat_history"] = history[-5000:]
        self.save(scope, payload)
        return payload

    def append_plan_snapshot(self, scope: ConversationScope, plan: Dict[str, Any]) -> Dict[str, Any]:
        payload = self.load(scope)
        snapshots = payload.setdefault("plan_snapshots", [])
        snapshots.append(
            {
                "timestamp": _utc_now_iso(),
                "roster_type": plan.get("roster_type"),
                "counts": {
                    "mappings": len(plan.get("mappings") or []),
                    "transformations": len(plan.get("transformations") or []),
                    "bq_validations": len(plan.get("bq_validations") or []),
                },
                "plan": plan,
            }
        )
        payload["plan_snapshots"] = snapshots[-100:]
        payload["latest_plan_path"] = plan.get("_plan_path")
        self.save(scope, payload)
        return payload

    @staticmethod
    def _clean_rationale_record(rationale: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(rationale or {})
        tags = payload.get("rationale_tags")
        if isinstance(tags, list):
            payload["rationale_tags"] = [str(tag).strip().lower() for tag in tags if str(tag).strip()]
        else:
            payload["rationale_tags"] = []
        payload.setdefault("schema_version", 2)
        payload.setdefault("event", "")
        payload.setdefault("item_type", "")
        payload.setdefault("item_id", "")
        payload.setdefault("section", "")
        payload.setdefault("approved", None)
        payload.setdefault("rationale_text", "")
        payload.setdefault("workspace_scope", {})
        payload.setdefault("workspace_signature", "")
        payload.setdefault("tenant_id", "")
        payload.setdefault("client_id", "")
        payload.setdefault("thread_id", "")
        payload.setdefault("item_context", {})
        payload.setdefault("decision", {})
        payload.setdefault("followup", {})
        payload.setdefault("supervisor", {})
        payload.setdefault("provenance", "native_capture")
        payload.setdefault("timestamp", _utc_now_iso())
        return payload

    def append_decision(self, scope: ConversationScope, decision: Dict[str, Any]) -> Dict[str, Any]:
        payload = self.load(scope)
        decisions = payload.setdefault("decisions", [])
        decision_record = dict(decision)
        decision_record.setdefault("timestamp", _utc_now_iso())
        if isinstance(decision_record.get("rationale"), dict):
            decision_record["rationale"] = self._clean_rationale_record(decision_record.get("rationale") or {})
        decisions.append(decision_record)
        payload["decisions"] = decisions[-5000:]
        self.save(scope, payload)
        return payload

    def append_rationale(self, scope: ConversationScope, rationale: Dict[str, Any]) -> Dict[str, Any]:
        payload = self.load(scope)
        rationales = payload.setdefault("rationales", [])
        clean = self._clean_rationale_record(rationale)
        rationales.append(clean)
        payload["rationales"] = rationales[-5000:]
        decision_events = payload.setdefault("decision_events", [])
        decision_events.append(clean)
        payload["decision_events"] = decision_events[-5000:]
        self.save(scope, payload)
        return payload

    def append_decision_event(self, scope: ConversationScope, event: Dict[str, Any]) -> Dict[str, Any]:
        payload = self.load(scope)
        decision_events = payload.setdefault("decision_events", [])
        decision_events.append(self._clean_rationale_record(event))
        payload["decision_events"] = decision_events[-5000:]
        self.save(scope, payload)
        return payload

    def append_run_output(self, scope: ConversationScope, run_output: Dict[str, Any]) -> Dict[str, Any]:
        payload = self.load(scope)
        outputs = payload.setdefault("run_outputs", [])
        output_record = dict(run_output)
        output_record.setdefault("timestamp", _utc_now_iso())
        outputs.append(output_record)
        payload["run_outputs"] = outputs[-500:]
        self.save(scope, payload)
        return payload

    def update_instructions_context(self, scope: ConversationScope, context_updates: Dict[str, Any]) -> Dict[str, Any]:
        payload = self.load(scope)
        instructions = payload.setdefault("instructions_context", self._base_payload(scope)["instructions_context"])
        for key in ["free_text_notes", "client_rules", "schema_caveats", "exceptions", "attachment_hints", "attachments"]:
            incoming = context_updates.get(key)
            if incoming is None:
                continue
            if not isinstance(incoming, list):
                incoming = [incoming]
            cleaned = [item for item in incoming if item not in [None, ""]]
            if not cleaned:
                continue
            bucket = instructions.setdefault(key, [])
            for item in cleaned:
                if item not in bucket:
                    bucket.append(item)

        payload["instructions_context"] = instructions
        self.save(scope, payload)
        return payload

    def update_latest_profile(self, scope: ConversationScope, profile: Dict[str, Any]) -> Dict[str, Any]:
        payload = self.load(scope)
        payload["latest_profile"] = profile
        self.save(scope, payload)
        return payload

    def list_workspaces(self, limit: int = 200) -> List[Dict[str, Any]]:
        index = self._load_index()
        items = []
        for workspace_id, entry in (index.get("workspaces") or {}).items():
            if not isinstance(entry, dict):
                continue
            row = dict(entry)
            row["workspace_id"] = workspace_id
            items.append(row)
        items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        return items[: max(1, limit)]
