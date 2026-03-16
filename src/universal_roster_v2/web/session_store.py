"""Folder-first workspace store and chat command dispatcher."""

from __future__ import annotations

import csv
import json
import re
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Callable, Dict, List, Optional, Sequence

from universal_roster_v2.config import get_settings
from universal_roster_v2.core.chat_custom_actions import (
    is_cancel_message,
    is_confirm_message,
    parse_custom_chat_action,
)
from universal_roster_v2.core.conversation_store import ConversationScope, WorkspaceConversationStore, scope_signature
from universal_roster_v2.core.profile import sample_values_by_column
from universal_roster_v2.core.quality_audit import suggest_quality_audit
from universal_roster_v2.core.session import UniversalRosterSession

_ALLOWED_UPLOAD_EXTENSIONS = {".csv", ".xlsx", ".xls"}
_ALLOWED_NOTE_EXTENSIONS = {".txt", ".md", ".json", ".csv"}
_PROFILE_SAMPLE_COLUMN_LIMIT = 8
_PROFILE_SAMPLE_VALUE_LIMIT = 4
_PENDING_ROSTER_CONTROL_PREFIX = "__set_pending_roster_choices__"
_OPERATION_TERMINAL_STATUSES = {"completed", "failed", "canceled"}
_OPERATION_ACTIVE_STATUSES = {"queued", "running"}
_EVENT_QUEUE_MAXSIZE = 500
_COLUMN_ACTION_PRIORITY = {"source remediation": 0, "transform": 1, "validate": 2, "review": 3}
_SEVERITY_WEIGHT = {"error": 5.0, "warning": 3.0, "info": 1.0}
_IMPACT_WEIGHT = {"high": 3.0, "medium": 2.0, "low": 1.0}


@dataclass
class OperationRecord:
    id: str
    workspace_id: str
    kind: str
    status: str = "queued"
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    input: Dict[str, Any] = field(default_factory=dict)
    result: Dict[str, Any] = field(default_factory=dict)
    error: Dict[str, Any] = field(default_factory=dict)
    progress: Dict[str, Any] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    request_id: str = ""
    parent_operation_id: Optional[str] = None
    cancel_requested: bool = False


@dataclass
class WebSession:
    workspace_id: str
    scope: ConversationScope
    session: UniversalRosterSession
    pending_roster_choices: List[Dict[str, Any]] = field(default_factory=list)
    pending_custom_action: Optional[Dict[str, Any]] = None
    pending_rationale: Optional[Dict[str, Any]] = None


class SessionStore:
    _SUPERVISOR_DECISION_TYPES = {"toggle", "custom_action_request", "rationale_followup"}
    _SUPERVISOR_RESPONSE_TYPES = {"approve", "reject", "skip", "clarify", "none"}

    def __init__(self, workspace_root: Optional[str | Path] = None):
        self.workspace_root = Path(workspace_root or (Path.cwd() / "web_sessions")).expanduser().resolve()
        self.workspace_root.mkdir(parents=True, exist_ok=True)
        self.conversation_store = WorkspaceConversationStore(base_dir=self.workspace_root / "_conversation_memory")
        self._sessions: Dict[str, WebSession] = {}
        self._operation_lock = threading.RLock()
        self._workspace_locks: Dict[str, threading.Lock] = {}
        self._operations: Dict[str, Dict[str, OperationRecord]] = {}
        self._operation_order: Dict[str, List[str]] = {}
        self._operation_events: Dict[str, List[Dict[str, Any]]] = {}
        self._event_queues: Dict[str, Queue] = {}
        self._active_futures: Dict[str, Future] = {}
        settings = get_settings()
        max_workers = max(2, int(getattr(settings, "max_concurrent_operations_per_workspace", 1)) * 4)
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="ur2-op")

    def new_session(self) -> str:
        return self.create_workspace()

    def create_workspace(
        self,
        workspace_path: Optional[str] = None,
        tenant_id: str = "default-tenant",
        client_id: str = "default-client",
        thread_id: Optional[str] = None,
    ) -> str:
        default_workspace = self.workspace_root / "workspaces" / uuid.uuid4().hex
        scope = self.conversation_store.normalize_scope(
            workspace_path=workspace_path or str(default_workspace),
            tenant_id=tenant_id,
            client_id=client_id,
            thread_id=thread_id or "default",
        )

        workspace_id = self.conversation_store.workspace_id(scope)
        web_session = self._build_web_session(scope)
        self._sessions[workspace_id] = web_session

        conversation = self.conversation_store.load(scope)
        if not conversation.get("chat_history"):
            self._append(
                web_session,
                role="assistant",
                content=(
                    "I’m ready. Upload a roster file (or a folder) and I’ll analyze it, suggest mappings, "
                    "transformations, and BigQuery validations."
                ),
            )

        return workspace_id

    def list_workspaces(self, limit: int = 200) -> List[Dict[str, Any]]:
        return self.conversation_store.list_workspaces(limit=limit)

    def get(self, session_id: str) -> WebSession:
        if session_id not in self._sessions:
            self._bootstrap_workspace(session_id)
        if session_id not in self._sessions:
            raise KeyError(f"Unknown workspace: {session_id}")
        return self._sessions[session_id]

    def _bootstrap_workspace(self, workspace_id: str) -> None:
        try:
            scope = self.conversation_store.resolve_scope(workspace_id)
        except Exception:
            return
        self._sessions[workspace_id] = self._build_web_session(scope)

    def _build_web_session(self, scope: ConversationScope) -> WebSession:
        conversation = self.conversation_store.load(scope)
        workspace_id = self.conversation_store.workspace_id(scope)

        session = UniversalRosterSession(
            workspace_dir=scope.workspace_path,
            workspace_scope={
                "workspace_signature": str(conversation.get("workspace_signature") or ""),
                "tenant_id": scope.tenant_id,
                "client_id": scope.client_id,
                "thread_id": scope.thread_id,
                "workspace_path": scope.workspace_path,
            },
        )

        instructions_context = conversation.get("instructions_context") or {}
        if isinstance(instructions_context, dict):
            session.set_instructions_context(instructions_context)

        latest_profile = conversation.get("latest_profile")
        if isinstance(latest_profile, dict):
            session.state.profile = latest_profile

        latest_plan_path = str(conversation.get("latest_plan_path") or "").strip()
        if latest_plan_path:
            plan_path = Path(latest_plan_path).expanduser().resolve()
            if plan_path.exists():
                try:
                    session.load_plan(str(plan_path))
                except Exception:
                    pass

        pending_roster_choices = [
            item for item in (conversation.get("pending_roster_choices") or []) if isinstance(item, dict)
        ]
        pending_custom_action = conversation.get("pending_custom_action")
        if not isinstance(pending_custom_action, dict):
            pending_custom_action = None
        pending_rationale = conversation.get("pending_rationale")
        if not isinstance(pending_rationale, dict):
            pending_rationale = None

        web_session = WebSession(
            workspace_id=workspace_id,
            scope=scope,
            session=session,
            pending_roster_choices=pending_roster_choices,
            pending_custom_action=pending_custom_action,
            pending_rationale=pending_rationale,
        )
        self._load_operations_from_conversation(web_session, conversation)
        return web_session

    def state_payload(self, session_id: str) -> Dict[str, Any]:
        web_session = self.get(session_id)
        session = web_session.session
        conversation = self.conversation_store.load(web_session.scope)
        plan = session.state.plan or {}

        stage = self._session_stage(session)
        review_summary = self._review_summary(session, plan)
        profile_summary = self._profile_summary(session)
        plan_views = self._plan_views(session, plan)
        column_bundle = self._build_column_audit_bundle(
            profile=session.state.profile or {},
            mappings=plan_views["mappings"],
            transformations=plan_views["transformations"],
            bq_validations=plan_views["bq_validations"],
            quality_audit=plan_views["quality_audit"],
            review_summary=review_summary,
        )

        return {
            "session_id": session_id,
            "workspace_id": session_id,
            "workspace": {
                "workspace_path": web_session.scope.workspace_path,
                "tenant_id": web_session.scope.tenant_id,
                "client_id": web_session.scope.client_id,
                "thread_id": web_session.scope.thread_id,
            },
            "status": session.status(),
            "stage": stage,
            "next_actions": self._next_actions(stage),
            "profile_summary": profile_summary,
            "review_summary": review_summary,
            "mappings": plan_views["mappings"],
            "transformations": plan_views["transformations"],
            "bq_validations": plan_views["bq_validations"],
            "quality_audit": column_bundle["quality_audit"],
            "column_audit_summary": column_bundle["column_audit_summary"],
            "standardization_plan": column_bundle["standardization_plan"],
            "client_summary": column_bundle["client_summary"],
            "chat_history": conversation.get("chat_history", []),
            "instructions_context": conversation.get("instructions_context", {}),
            "run_results": conversation.get("run_outputs", []),
            "pending_roster_choices": list(web_session.pending_roster_choices),
            "pending_custom_action": dict(web_session.pending_custom_action or {}),
            "pending_rationale": dict(web_session.pending_rationale or {}),
            "pending_selected_roster": dict((conversation.get("pending_selected_roster") or {})),
            "active_operation_id": conversation.get("active_operation_id"),
            "operations": self.list_operations(session_id),
            "operation_events": self.list_operation_events(session_id),
            "frontend_config": {
                "enable_async_operations": bool(getattr(session.settings, "enable_async_operations", True)),
                "enable_sse_progress": bool(getattr(session.settings, "enable_sse_progress", True)),
                "enable_web_debug_drawer": bool(getattr(session.settings, "enable_web_debug_drawer", True)),
                "poll_interval_ms": int(getattr(session.settings, "web_operation_poll_interval_ms", 1500)),
                "ui_build_id": "ur2-ui-20260304-react-overhaul",
            },
        }

    def load_file(
        self,
        session_id: str,
        file_path: str,
        roster_type: Optional[str] = None,
        profile_full_roster_learning: Optional[bool] = None,
        profile_max_rows: Optional[int] = None,
    ) -> Dict[str, Any]:
        web_session = self.get(session_id)
        profile = self._do_load_file(
            web_session,
            file_path=file_path,
            roster_type=roster_type,
            profile_full_roster_learning=profile_full_roster_learning,
            profile_max_rows=profile_max_rows,
        )
        self.conversation_store.update_latest_profile(web_session.scope, profile)

        web_session.session.record_chat_outcome(
            {
                "event": "load_file",
                "result": "ok",
                "workspace_id": session_id,
                "file_name": profile.get("file_name"),
                "roster_type": profile.get("roster_type_detected"),
            }
        )
        return profile

    def upload_file(
        self,
        session_id: str,
        filename: str,
        content: bytes,
        roster_type: Optional[str] = None,
        profile_full_roster_learning: Optional[bool] = None,
        profile_max_rows: Optional[int] = None,
    ) -> Dict[str, Any]:
        web_session = self.get(session_id)
        upload_path = self._save_uploaded_file(session_id=session_id, filename=filename, content=content)
        profile = self._do_load_file(
            web_session,
            file_path=str(upload_path),
            roster_type=roster_type,
            profile_full_roster_learning=profile_full_roster_learning,
            profile_max_rows=profile_max_rows,
        )
        self.conversation_store.update_latest_profile(web_session.scope, profile)

        web_session.session.record_chat_outcome(
            {
                "event": "upload_file",
                "result": "ok",
                "workspace_id": session_id,
                "file_name": profile.get("file_name"),
                "stored_path": str(upload_path),
                "roster_type": profile.get("roster_type_detected"),
            }
        )

        stage = self._session_stage(web_session.session)
        return {
            "uploaded_path": str(upload_path),
            "profile": profile,
            "stage": stage,
            "next_actions": self._next_actions(stage),
            "profile_summary": self._profile_summary(web_session.session),
        }

    def add_instruction_context(
        self,
        session_id: str,
        free_text: Optional[str] = None,
        client_rules: Optional[List[str]] = None,
        schema_caveats: Optional[List[str]] = None,
        exceptions: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        web_session = self.get(session_id)
        updates: Dict[str, Any] = {}
        if free_text and str(free_text).strip():
            updates["free_text_notes"] = [str(free_text).strip()]
        if client_rules:
            updates["client_rules"] = [str(item).strip() for item in client_rules if str(item).strip()]
        if schema_caveats:
            updates["schema_caveats"] = [str(item).strip() for item in schema_caveats if str(item).strip()]
        if exceptions:
            updates["exceptions"] = [str(item).strip() for item in exceptions if str(item).strip()]

        if not updates:
            raise ValueError("No instruction context provided")

        payload = self.conversation_store.update_instructions_context(web_session.scope, updates)
        web_session.session.set_instructions_context(payload.get("instructions_context", {}))

        self._append(web_session, role="assistant", content="Saved that note to workspace memory.")
        web_session.session.record_chat_outcome(
            {
                "event": "instruction_context_updated",
                "result": "ok",
                "workspace_id": session_id,
                "keys": sorted(updates.keys()),
            }
        )
        return payload.get("instructions_context", {})

    def upload_note_attachment(self, session_id: str, filename: str, content: bytes) -> Dict[str, Any]:
        web_session = self.get(session_id)
        note_path = self._save_note_attachment(session_id=session_id, filename=filename, content=content)
        ingested = self._ingest_note_attachment(note_path)

        attachment_record = {
            "filename": note_path.name,
            "stored_path": str(note_path),
            "suffix": note_path.suffix.lower(),
            "preview": ingested.get("preview", ""),
        }

        payload = self.conversation_store.update_instructions_context(
            web_session.scope,
            {
                "attachments": [attachment_record],
                "attachment_hints": ingested.get("hints", []),
            },
        )
        web_session.session.set_instructions_context(payload.get("instructions_context", {}))

        self._append(web_session, role="assistant", content=f"Attached {note_path.name} as context.")
        web_session.session.record_chat_outcome(
            {
                "event": "note_attachment_uploaded",
                "result": "ok",
                "workspace_id": session_id,
                "file_name": note_path.name,
                "stored_path": str(note_path),
                "hint_count": len(ingested.get("hints", [])),
            }
        )

        return {
            "attachment": attachment_record,
            "hints": ingested.get("hints", []),
            "instructions_context": payload.get("instructions_context", {}),
        }

    def suggest(self, session_id: str) -> Dict[str, Any]:
        web_session = self.get(session_id)
        if not web_session.session.state.profile:
            raise ValueError("No profile loaded. Upload a file first or use /load, then run suggest.")

        plan = self._do_suggest(web_session)
        self._sync_plan_audit_views(web_session, plan)
        self.conversation_store.append_plan_snapshot(web_session.scope, plan)
        unchecked = web_session.session.plan_manager.unchecked_counts(plan)

        web_session.session.record_chat_outcome(
            {
                "event": "suggest",
                "result": "ok",
                "workspace_id": session_id,
                "unchecked": unchecked,
                "llm_trace": plan.get("llm_trace", {}),
            }
        )
        return plan

    def toggle_item(self, session_id: str, item_type: str, item_id: str, approved: bool) -> Dict[str, Any]:
        web_session = self.get(session_id)
        if not web_session.session.state.plan:
            raise ValueError("No active review plan. Run suggest first.")

        ok = web_session.session.set_item_approval(item_type=item_type, item_id=item_id, approved=approved)
        if not ok:
            raise ValueError(f"Unable to update {item_type} item {item_id}")

        self.conversation_store.append_decision(
            web_session.scope,
            {
                "event": "toggle_item",
                "item_type": item_type,
                "item_id": item_id,
                "approved": approved,
            },
        )

        self._sync_plan_audit_views(web_session, web_session.session.state.plan or {})
        self.conversation_store.append_plan_snapshot(web_session.scope, web_session.session.state.plan or {})

        web_session.session.record_chat_outcome(
            {
                "event": "toggle_item",
                "result": "ok",
                "workspace_id": session_id,
                "item_type": item_type,
                "item_id": item_id,
                "approved": approved,
            }
        )
        response = {"updated": True, "item_type": item_type, "item_id": item_id, "approved": approved}
        if approved is False:
            followup = self._queue_rationale_followup(
                web_session,
                event="toggle_item",
                item_type=item_type,
                item_id=item_id,
                approved=approved,
                source="uncheck",
            )
            if followup is not None:
                response["rationale_prompt"] = followup
        return response

    def generate(self, session_id: str, mode: str, output_dir: Optional[str] = None, pipeline_name: str = "UniversalRosterPipeline") -> Dict[str, Any]:
        web_session = self.get(session_id)
        if not web_session.session.state.plan:
            raise ValueError("No active suggestion plan. Run suggest first, then generate artifacts.")
        if not web_session.session.state.profile:
            raise ValueError("No active profile loaded. Upload or load a file first.")

        result = self._do_generate(web_session, mode=mode, output_dir=output_dir, pipeline_name=pipeline_name)

        self.conversation_store.append_run_output(
            web_session.scope,
            {
                "event": "generate",
                "mode": mode,
                "output_dir": result.get("output_dir"),
                "files": result.get("files", []),
                "unchecked": result.get("unchecked", {}),
            },
        )

        web_session.session.record_chat_outcome(
            {
                "event": "generate",
                "result": "ok",
                "workspace_id": session_id,
                "mode": mode,
                "output_dir": result.get("output_dir"),
                "files": result.get("files", []),
            }
        )
        return result

    def export_training(self, session_id: str, output_dir: Optional[str] = None) -> Dict[str, Any]:
        web_session = self.get(session_id)
        result = self._do_export_training(web_session, output_dir=output_dir)
        web_session.session.record_chat_outcome(
            {
                "event": "training_export",
                "result": result.get("status"),
                "workspace_id": session_id,
                "output_dir": result.get("output_dir") or output_dir,
                "counts": result.get("counts", {}),
            }
        )
        return result

    def run_training(self, session_id: str, output_dir: Optional[str] = None, extra_args: Optional[List[str]] = None) -> Dict[str, Any]:
        web_session = self.get(session_id)
        result = self._do_run_training(web_session, output_dir=output_dir, extra_args=extra_args)
        web_session.session.record_chat_outcome(
            {
                "event": "training_run",
                "result": result.get("status"),
                "workspace_id": session_id,
                "run_id": result.get("run_id"),
                "artifact_dir": result.get("artifact_dir"),
            }
        )
        return result

    def run_generated(
        self,
        session_id: str,
        input_file: str,
        generated_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        table_id: str = "project.dataset.staging",
    ) -> Dict[str, Any]:
        web_session = self.get(session_id)
        result = self._do_run_generated(
            web_session,
            input_file=input_file,
            generated_dir=generated_dir,
            output_dir=output_dir,
            table_id=table_id,
        )

        self.conversation_store.append_run_output(
            web_session.scope,
            {
                "event": "run_generated",
                **result,
            },
        )

        web_session.session.record_chat_outcome(
            {
                "event": "run_generated",
                "result": "ok" if result.get("success") else "error",
                "workspace_id": session_id,
                "return_code": result.get("return_code"),
            }
        )
        return result

    def handle_chat(
        self,
        session_id: str,
        message: str,
        output_dir: Optional[str] = None,
        *,
        skip_user_append: bool = False,
        request_id: str = "",
    ) -> Dict[str, Any]:
        web_session = self.get(session_id)
        text = message.strip()
        lower = " ".join(text.lower().split())

        if text.startswith(_PENDING_ROSTER_CONTROL_PREFIX):
            payload = text[len(_PENDING_ROSTER_CONTROL_PREFIX) :].strip()
            try:
                options = json.loads(payload) if payload else []
            except Exception as exc:
                raise ValueError("Invalid pending roster choice payload") from exc
            if not isinstance(options, list):
                raise ValueError("Pending roster choices payload must be a list")
            web_session.pending_roster_choices = [item for item in options if isinstance(item, dict)]
            conversation = self.conversation_store.load(web_session.scope)
            conversation["pending_selected_roster"] = None
            self.conversation_store.save(web_session.scope, conversation)
            self._persist_operation_state(web_session)
            return {"type": "pending_roster_choices_set", "count": len(web_session.pending_roster_choices)}

        if not skip_user_append:
            self._append(web_session, role="user", content=message)

        pending_choice = self._consume_pending_roster_choice(web_session, text, lower)
        if pending_choice is not None:
            return pending_choice

        pending_rationale = self._consume_pending_rationale(web_session, text)
        if pending_rationale is not None:
            return pending_rationale

        pending_custom = self._consume_pending_custom_action(web_session, text)
        if pending_custom is not None:
            return pending_custom

        supervisor_trace: Dict[str, Any] = {}
        custom_supervisor_wants_parse = False
        if web_session.session.settings.enable_claude_chat_supervisor:
            supervisor_trace = self._ask_supervisor(
                web_session,
                decision_type="toggle",
                text=text,
                context={"message_lower": lower},
            )
            custom_supervisor_wants_parse = str(supervisor_trace.get("response_type") or "") in {"approve", "clarify"}

        if self._is_custom_action_intent(lower) or custom_supervisor_wants_parse:
            parsed_custom = self._parse_custom_action_request(web_session, text)
            if parsed_custom is not None:
                return parsed_custom

        if self._is_note_intent(text, lower):
            note_text = self._extract_note_text(text)
            if not note_text:
                return self._guidance(
                    web_session,
                    event="note_blocked",
                    message="Tell me what note you want saved, and I’ll store it as workspace context.",
                )
            context = self.add_instruction_context(session_id=session_id, free_text=note_text)
            assistant_message = "Got it. I saved that note and will use it when I analyze this roster."
            self._append(web_session, role="assistant", content=assistant_message)
            return {"type": "note", "saved": True, "instructions_context": context, "message": assistant_message}

        if self._is_suggest_intent(lower):
            if not web_session.session.state.profile:
                conversation = self.conversation_store.load(web_session.scope)
                pending_selected_roster = conversation.get("pending_selected_roster")
                if isinstance(pending_selected_roster, dict) and pending_selected_roster.get("path"):
                    pending_path = str(pending_selected_roster.get("path") or "")
                    if pending_path.startswith("__browser_upload__"):
                        assistant_message = (
                            "A roster choice is selected but still needs browser upload before analysis. "
                            "Please finish the roster selection upload flow first."
                        )
                        self._append(web_session, role="assistant", content=assistant_message)
                        return {
                            "type": "pending_roster_choice_blocked",
                            "message": assistant_message,
                            "choices": list(web_session.pending_roster_choices),
                        }
                    operation = self.start_operation(
                        session_id,
                        kind="analyze_selected_roster",
                        input_payload={"selected_roster": pending_selected_roster},
                        request_id=request_id,
                    )
                    assistant_message = (
                        "Started analysis for the selected roster. "
                        f"Track progress with operation {operation.get('id')}."
                    )
                    self._append(web_session, role="assistant", content=assistant_message)
                    return {
                        "type": "operation_started",
                        "operation": operation,
                        "operation_id": operation.get("id"),
                        "message": assistant_message,
                    }
                return self._guidance(
                    web_session,
                    event="suggest_blocked",
                    message="Upload a roster file first, then I can analyze the schema, suggest mappings, recommend transformations, and draft BigQuery validations.",
                )

            plan = self.suggest(session_id)
            review_summary = self._review_summary(web_session.session, plan)
            counts = {
                "mappings": len(plan.get("mappings", [])),
                "transformations": len(plan.get("transformations", [])),
                "bq_validations": len(plan.get("bq_validations", [])),
                "quality_audit": len(plan.get("quality_audit", [])),
            }
            assistant_message = self._suggestions_message(counts, review_summary)
            self._append(web_session, role="assistant", content=assistant_message)
            return {
                "type": "suggestions",
                "counts": counts,
                "review_summary": review_summary,
                "message": assistant_message,
            }

        generate_mode = self._generate_mode_from_message(lower)
        if generate_mode:
            if not web_session.session.state.plan:
                return self._guidance(
                    web_session,
                    event="generate_blocked",
                    message="I can generate artifacts after I’ve analyzed the file and built suggestions. Ask me to analyze the roster first.",
                )
            operation = self.start_operation(
                session_id,
                kind="generate",
                input_payload={
                    "mode": generate_mode,
                    "output_dir": output_dir,
                    "pipeline_name": "UniversalRosterPipeline",
                },
                request_id=request_id,
            )
            assistant_message = f"Started {generate_mode} generation. Operation: {operation.get('id')}"
            self._append(web_session, role="assistant", content=assistant_message)
            return {
                "type": "operation_started",
                "operation": operation,
                "operation_id": operation.get("id"),
                "message": assistant_message,
            }

        run_input_file = self._extract_run_generated_input_file(text, lower)
        if run_input_file is not None:
            if not run_input_file:
                return self._guidance(
                    web_session,
                    event="run_blocked",
                    message="Tell me which input file to run, for example: run generated /path/to/input.csv",
                )
            operation = self.start_operation(
                session_id,
                kind="run_generated",
                input_payload={
                    "input_file": run_input_file,
                    "output_dir": output_dir,
                    "table_id": "project.dataset.staging",
                },
                request_id=request_id,
            )
            assistant_message = f"Started generated pipeline run. Operation: {operation.get('id')}"
            self._append(web_session, role="assistant", content=assistant_message)
            return {
                "type": "operation_started",
                "operation": operation,
                "operation_id": operation.get("id"),
                "message": assistant_message,
            }

        if self._is_show_unchecked_intent(lower):
            if not web_session.session.state.plan:
                return self._guidance(
                    web_session,
                    event="show_unchecked_blocked",
                    message="I don’t have review items yet. Ask me to analyze the roster first.",
                )

            unchecked = web_session.session.list_unchecked()
            scope = self._unchecked_scope(lower)
            if scope == "mappings":
                payload: Any = unchecked.get("mappings", [])
            elif scope == "transformations":
                payload = unchecked.get("transformations", [])
            elif scope == "bq_validations":
                payload = unchecked.get("bq_validations", [])
            elif scope == "quality_audit":
                payload = unchecked.get("quality_audit", [])
            else:
                payload = unchecked

            assistant_message = self._unchecked_message(scope, payload)
            self._append(web_session, role="assistant", content=assistant_message)
            web_session.session.record_chat_outcome(
                {
                    "event": "show_unchecked",
                    "result": "ok",
                    "workspace_id": session_id,
                    "scope": scope,
                    "count": len(payload) if isinstance(payload, list) else None,
                }
            )
            return {"type": "unchecked", "scope": scope, "items": payload, "message": assistant_message}

        check_match = re.match(r"^(check|uncheck)\s+(.+)$", lower)
        if check_match:
            if not web_session.session.state.plan:
                return self._guidance(
                    web_session,
                    event="toggle_blocked",
                    message="I don’t have anything to approve or reject yet. Ask me to analyze the roster first.",
                )

            action, _ = check_match.groups()
            item_id = text[len(action) :].strip()
            item_type = self._infer_item_type(item_id)
            approved_value: Optional[bool] = action == "check"
            trace_approved = supervisor_trace.get("approved")
            if isinstance(trace_approved, bool):
                approved_value = trace_approved
            if approved_value is None:
                approved_value = action == "check"
            result = self.toggle_item(session_id, item_type=item_type, item_id=item_id, approved=approved_value)
            assistant_message = self._toggle_message(result)
            self._append(web_session, role="assistant", content=assistant_message)
            response = {"type": "toggle", "result": result, "message": assistant_message}
            followup = result.get("rationale_prompt") if isinstance(result, dict) else None
            if followup is not None:
                response["rationale_prompt"] = followup
            return response

        add_validation_match = re.match(r"^add\s+validation\s+for\s+(.+)$", text, flags=re.IGNORECASE)
        if add_validation_match:
            if not web_session.session.state.plan:
                return self._guidance(
                    web_session,
                    event="add_validation_blocked",
                    message="I need an analyzed plan before I can add a validation. Ask me to analyze the roster first.",
                )

            target_field = add_validation_match.group(1).strip()
            item = self._add_required_validation_for_field(
                web_session.session,
                target_field,
                learning_scope=web_session.session.state.workspace_scope,
                capture_episode=False,
            )
            assistant_message = f"I added a required-field validation for {target_field}."
            self._append(web_session, role="assistant", content=assistant_message)
            web_session.session.record_chat_outcome(
                {
                    "event": "add_validation",
                    "result": "ok",
                    "workspace_id": session_id,
                    "target_field": target_field,
                    "item_id": item.get("id"),
                }
            )
            return {"type": "add_validation", "item": item, "message": assistant_message}

        review_sections = self._review_sections_from_message(lower)
        if review_sections:
            if not web_session.session.state.profile:
                return self._guidance(
                    web_session,
                    event="summary_blocked",
                    message="Upload a roster file first, then I can summarize the current schema and profile.",
                )
            payload: Dict[str, Any] = {
                "type": "review_sections",
                "sections": review_sections,
            }
            message_parts: List[str] = []

            if "schema" in review_sections:
                summary = self._profile_summary(web_session.session)
                payload["profile_summary"] = summary
                message_parts.append(self._profile_summary_message(summary))

            if any(section in review_sections for section in ["transformations", "bq_validations", "quality_audit", "mappings"]):
                if not web_session.session.state.plan:
                    return self._guidance(
                        web_session,
                        event="review_sections_blocked",
                        message="I need to analyze the roster first before I can show mappings, transformations, or validations.",
                    )

            if "mappings" in review_sections:
                mapping_items = web_session.session.plan_manager.combined_items(web_session.session.state.plan, "mappings")
                payload["mappings"] = mapping_items
                message_parts.append(self._section_summary_message("mappings", mapping_items))

            if "transformations" in review_sections:
                tx_items = web_session.session.plan_manager.combined_items(web_session.session.state.plan, "transformations")
                payload["transformations"] = tx_items
                message_parts.append(self._section_summary_message("transformations", tx_items))

            if "bq_validations" in review_sections:
                val_items = web_session.session.plan_manager.combined_items(web_session.session.state.plan, "bq_validations")
                payload["bq_validations"] = val_items
                message_parts.append(self._section_summary_message("validations", val_items))

            if "quality_audit" in review_sections:
                qa_items = web_session.session.plan_manager.combined_items(web_session.session.state.plan, "quality_audit")
                payload["quality_audit"] = qa_items
                message_parts.append(self._section_summary_message("quality audit", qa_items))

            assistant_message = "\n".join(part for part in message_parts if str(part).strip())
            self._append(web_session, role="assistant", content=assistant_message)
            payload["message"] = assistant_message
            return payload

        if self._is_summary_intent(lower):
            if not web_session.session.state.profile:
                return self._guidance(
                    web_session,
                    event="summary_blocked",
                    message="Upload a roster file first, then I can summarize the current schema and profile.",
                )
            summary = self._profile_summary(web_session.session)
            assistant_message = self._profile_summary_message(summary)
            self._append(web_session, role="assistant", content=assistant_message)
            return {"type": "profile_summary", "profile_summary": summary, "message": assistant_message}

        if self._is_explain_transformations_intent(lower):
            if not web_session.session.state.plan:
                return self._guidance(
                    web_session,
                    event="transformations_explain_blocked",
                    message="I need to analyze the roster first before I can explain the proposed transformations.",
                )
            items = web_session.session.plan_manager.combined_items(web_session.session.state.plan, "transformations")
            assistant_message = self._section_summary_message("transformations", items)
            self._append(web_session, role="assistant", content=assistant_message)
            return {"type": "transformations_summary", "items": items, "message": assistant_message}

        if self._is_explain_validations_intent(lower):
            if not web_session.session.state.plan:
                return self._guidance(
                    web_session,
                    event="validations_explain_blocked",
                    message="I need to analyze the roster first before I can explain the proposed validations.",
                )
            items = web_session.session.plan_manager.combined_items(web_session.session.state.plan, "bq_validations")
            assistant_message = self._section_summary_message("validations", items)
            self._append(web_session, role="assistant", content=assistant_message)
            return {"type": "validations_summary", "items": items, "message": assistant_message}

        if self._is_explain_quality_audit_intent(lower):
            if not web_session.session.state.plan:
                return self._guidance(
                    web_session,
                    event="quality_audit_explain_blocked",
                    message="I need to analyze the roster first before I can explain the quality audit findings.",
                )
            items = web_session.session.plan_manager.combined_items(web_session.session.state.plan, "quality_audit")
            assistant_message = self._section_summary_message("quality audit", items)
            self._append(web_session, role="assistant", content=assistant_message)
            return {"type": "quality_audit_summary", "items": items, "message": assistant_message}

        if self._is_mappings_intent(lower):
            if not web_session.session.state.plan:
                return self._guidance(
                    web_session,
                    event="mappings_blocked",
                    message="I need to analyze the roster first before I can summarize the mappings.",
                )
            items = web_session.session.plan_manager.combined_items(web_session.session.state.plan, "mappings")
            assistant_message = self._section_summary_message("mappings", items)
            self._append(web_session, role="assistant", content=assistant_message)
            return {"type": "mappings_summary", "items": items, "message": assistant_message}

        if self._is_status_intent(lower):
            status = web_session.session.status()
            stage = self._session_stage(web_session.session)
            next_actions = self._next_actions(stage)
            profile_summary = self._profile_summary(web_session.session)
            review_summary = self._review_summary(web_session.session, web_session.session.state.plan or {})
            assistant_message = self._status_message(stage, profile_summary, review_summary, next_actions)
            self._append(web_session, role="assistant", content=assistant_message)
            web_session.session.record_chat_outcome(
                {
                    "event": "status",
                    "result": "ok",
                    "workspace_id": session_id,
                    "stage": stage,
                }
            )
            return {
                "type": "status",
                "status": status,
                "stage": stage,
                "next_actions": next_actions,
                "profile_summary": profile_summary,
                "review_summary": review_summary,
                "message": assistant_message,
            }

        help_text = (
            "You can talk to me normally. I can analyze the roster, summarize the schema, explain mappings, transformations, quality audit findings, and validations, generate artifacts, run the generated pipeline, save notes, and tell you what to do next."
        )
        stage = self._session_stage(web_session.session)
        next_actions = self._next_actions(stage)
        assistant_message = help_text + "\n\nNext actions:\n- " + "\n- ".join(next_actions)
        self._append(web_session, role="assistant", content=assistant_message)
        web_session.session.record_chat_outcome(
            {
                "event": "help",
                "result": "unknown_command",
                "workspace_id": session_id,
                "input": text,
                "stage": stage,
            }
        )
        return {"type": "help", "message": assistant_message, "stage": stage, "next_actions": next_actions}

    @staticmethod
    def _now_ts() -> float:
        return time.time()

    @staticmethod
    def _iso_from_ts(ts: Optional[float]) -> Optional[str]:
        if ts is None:
            return None
        from datetime import datetime, timezone

        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    @staticmethod
    def _serialize_operation(operation: OperationRecord) -> Dict[str, Any]:
        data = asdict(operation)
        data["created_at"] = SessionStore._iso_from_ts(operation.created_at)
        data["started_at"] = SessionStore._iso_from_ts(operation.started_at)
        data["finished_at"] = SessionStore._iso_from_ts(operation.finished_at)
        return data

    @staticmethod
    def _deserialize_operation(payload: Dict[str, Any]) -> OperationRecord:
        def _ts(value: Any) -> Optional[float]:
            if value in [None, ""]:
                return None
            if isinstance(value, (int, float)):
                return float(value)
            text = str(value)
            try:
                return float(text)
            except Exception:
                pass
            try:
                from datetime import datetime

                normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
                return datetime.fromisoformat(normalized).timestamp()
            except Exception:
                return None

        operation = OperationRecord(
            id=str(payload.get("id") or uuid.uuid4().hex),
            workspace_id=str(payload.get("workspace_id") or ""),
            kind=str(payload.get("kind") or "unknown"),
            status=str(payload.get("status") or "queued"),
            created_at=_ts(payload.get("created_at")) or SessionStore._now_ts(),
            started_at=_ts(payload.get("started_at")),
            finished_at=_ts(payload.get("finished_at")),
            input=dict(payload.get("input") or {}),
            result=dict(payload.get("result") or {}),
            error=dict(payload.get("error") or {}),
            progress=dict(payload.get("progress") or {}),
            logs=[item for item in (payload.get("logs") or []) if isinstance(item, dict)],
            request_id=str(payload.get("request_id") or ""),
            parent_operation_id=(
                str(payload.get("parent_operation_id") or "") or None
            ),
            cancel_requested=bool(payload.get("cancel_requested")),
        )
        return operation

    def _workspace_lock(self, workspace_id: str) -> threading.Lock:
        with self._operation_lock:
            lock = self._workspace_locks.get(workspace_id)
            if lock is None:
                lock = threading.Lock()
                self._workspace_locks[workspace_id] = lock
            return lock

    def _event_queue(self, workspace_id: str) -> Queue:
        with self._operation_lock:
            queue = self._event_queues.get(workspace_id)
            if queue is None:
                queue = Queue(maxsize=_EVENT_QUEUE_MAXSIZE)
                self._event_queues[workspace_id] = queue
            return queue

    def _load_operations_from_conversation(self, web_session: WebSession, conversation: Optional[Dict[str, Any]] = None) -> None:
        payload = conversation if isinstance(conversation, dict) else self.conversation_store.load(web_session.scope)
        workspace_id = web_session.workspace_id

        with self._workspace_lock(workspace_id):
            if workspace_id in self._operations:
                return
            ops_payload = payload.get("operations") or []
            operations: Dict[str, OperationRecord] = {}
            order: List[str] = []
            for item in ops_payload:
                if not isinstance(item, dict):
                    continue
                op = self._deserialize_operation(item)
                op.workspace_id = workspace_id
                operations[op.id] = op
                order.append(op.id)

            events = [item for item in (payload.get("operation_events") or []) if isinstance(item, dict)]
            self._operations[workspace_id] = operations
            self._operation_order[workspace_id] = order
            self._operation_events[workspace_id] = events
            self._event_queue(workspace_id)

    def _persist_operation_state(self, web_session: WebSession, *, active_operation_id: Optional[str] = None) -> None:
        conversation = self.conversation_store.load(web_session.scope)
        workspace_id = web_session.workspace_id
        operation_ids = self._operation_order.get(workspace_id, [])
        registry = self._operations.get(workspace_id, {})
        serialized_ops = [
            self._serialize_operation(registry[operation_id])
            for operation_id in operation_ids
            if operation_id in registry
        ]
        conversation["operations"] = serialized_ops

        events = self._operation_events.get(workspace_id, [])
        max_events = max(100, int(getattr(web_session.session.settings, "operation_events_retention", 1000)))
        conversation["operation_events"] = events[-max_events:]

        selected_active = active_operation_id
        if selected_active is None:
            selected_active = conversation.get("active_operation_id")

        if selected_active:
            selected_operation = registry.get(str(selected_active))
            if selected_operation and selected_operation.status in _OPERATION_ACTIVE_STATUSES:
                conversation["active_operation_id"] = selected_operation.id
            else:
                conversation["active_operation_id"] = None
        else:
            conversation["active_operation_id"] = None

        conversation["pending_roster_choices"] = list(web_session.pending_roster_choices)
        conversation["pending_custom_action"] = dict(web_session.pending_custom_action) if isinstance(web_session.pending_custom_action, dict) else None
        conversation["pending_rationale"] = dict(web_session.pending_rationale) if isinstance(web_session.pending_rationale, dict) else None
        self.conversation_store.save(web_session.scope, conversation)

    def _append_operation_event(
        self,
        web_session: WebSession,
        operation: OperationRecord,
        *,
        event_type: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        event = {
            "id": uuid.uuid4().hex,
            "workspace_id": web_session.workspace_id,
            "operation_id": operation.id,
            "kind": operation.kind,
            "status": operation.status,
            "type": str(event_type or "update"),
            "timestamp": self._iso_from_ts(self._now_ts()),
            "request_id": operation.request_id,
            "payload": dict(payload or {}),
        }

        events = self._operation_events.setdefault(web_session.workspace_id, [])
        events.append(event)
        max_events = max(100, int(getattr(web_session.session.settings, "operation_events_retention", 1000)))
        if len(events) > max_events:
            del events[:-max_events]

        queue = self._event_queue(web_session.workspace_id)
        try:
            queue.put_nowait(event)
        except Exception:
            try:
                queue.get_nowait()
            except Empty:
                pass
            try:
                queue.put_nowait(event)
            except Exception:
                pass

        return event

    def _append_operation_log(self, web_session: WebSession, operation: OperationRecord, level: str, message: str) -> None:
        timestamp = self._iso_from_ts(self._now_ts())
        entry = {
            "timestamp": timestamp,
            "level": str(level or "info"),
            "message": str(message or ""),
            "request_id": operation.request_id,
        }
        operation.logs.append(entry)
        max_logs = max(20, int(getattr(web_session.session.settings, "operation_log_retention", 300)))
        if len(operation.logs) > max_logs:
            operation.logs[:] = operation.logs[-max_logs:]

        self._append_operation_event(
            web_session,
            operation,
            event_type="log",
            payload={"log": entry},
        )

    def _update_operation_progress(
        self,
        web_session: WebSession,
        operation: OperationRecord,
        *,
        phase: str,
        message: str,
        percent: Optional[float] = None,
    ) -> None:
        now = self._now_ts()
        elapsed_ms = int(max(0.0, now - operation.created_at) * 1000)
        progress = {
            "phase": str(phase or "running"),
            "message": str(message or ""),
            "percent": None if percent is None else max(0.0, min(100.0, float(percent))),
            "elapsed_ms": elapsed_ms,
        }
        operation.progress = progress
        self._append_operation_event(
            web_session,
            operation,
            event_type="progress",
            payload={"progress": progress},
        )

    def _new_operation(
        self,
        web_session: WebSession,
        *,
        kind: str,
        input_payload: Optional[Dict[str, Any]] = None,
        request_id: str = "",
        parent_operation_id: Optional[str] = None,
    ) -> OperationRecord:
        workspace_id = web_session.workspace_id
        with self._workspace_lock(workspace_id):
            registry = self._operations.setdefault(workspace_id, {})
            order = self._operation_order.setdefault(workspace_id, [])
            active_count = sum(1 for op in registry.values() if op.status in _OPERATION_ACTIVE_STATUSES)
            max_active = max(1, int(getattr(web_session.session.settings, "max_concurrent_operations_per_workspace", 1)))
            if active_count >= max_active:
                raise RuntimeError("Another operation is already running for this workspace")
            operation = OperationRecord(
                id=uuid.uuid4().hex,
                workspace_id=workspace_id,
                kind=str(kind or "unknown"),
                input=dict(input_payload or {}),
                request_id=str(request_id or ""),
                parent_operation_id=str(parent_operation_id or "") or None,
            )
            registry[operation.id] = operation
            order.append(operation.id)
            max_ops = max(50, int(getattr(web_session.session.settings, "operation_retention_max_records", 2000)))
            if len(order) > max_ops:
                removed = order[:-max_ops]
                del order[:-max_ops]
                for op_id in removed:
                    registry.pop(op_id, None)
            self._append_operation_event(web_session, operation, event_type="created", payload={"input": operation.input})
            self._persist_operation_state(web_session, active_operation_id=operation.id)
            return operation

    def _set_operation_running(self, web_session: WebSession, operation: OperationRecord) -> None:
        operation.status = "running"
        operation.started_at = self._now_ts()
        self._append_operation_event(web_session, operation, event_type="running")
        self._persist_operation_state(web_session, active_operation_id=operation.id)

    def _set_operation_completed(self, web_session: WebSession, operation: OperationRecord, result: Dict[str, Any]) -> None:
        operation.status = "completed"
        clean_result = {k: v for k, v in (result or {}).items() if not k.startswith("_")}
        operation.result = clean_result
        operation.finished_at = self._now_ts()
        self._append_operation_event(web_session, operation, event_type="completed", payload={"result": operation.result})
        self._persist_operation_state(web_session, active_operation_id=None)

    def _set_operation_failed(self, web_session: WebSession, operation: OperationRecord, error: Exception) -> None:
        operation.status = "failed"
        operation.error = {
            "message": str(error),
            "type": error.__class__.__name__,
        }
        operation.finished_at = self._now_ts()
        self._append_operation_log(web_session, operation, "error", str(error))
        self._append_operation_event(web_session, operation, event_type="failed", payload={"error": operation.error})
        self._persist_operation_state(web_session, active_operation_id=None)

    def _set_operation_canceled(self, web_session: WebSession, operation: OperationRecord, *, reason: str = "Canceled") -> None:
        operation.status = "canceled"
        operation.error = {"message": str(reason), "type": "OperationCanceled"}
        operation.finished_at = self._now_ts()
        self._append_operation_log(web_session, operation, "warning", str(reason))
        self._append_operation_event(web_session, operation, event_type="canceled", payload={"reason": reason})
        self._persist_operation_state(web_session, active_operation_id=None)

    def _check_operation_canceled(self, operation: OperationRecord) -> None:
        if operation.cancel_requested:
            raise RuntimeError("Operation canceled by user")

    def list_operations(self, session_id: str) -> List[Dict[str, Any]]:
        web_session = self.get(session_id)
        workspace_id = web_session.workspace_id
        with self._workspace_lock(workspace_id):
            registry = self._operations.get(workspace_id, {})
            order = self._operation_order.get(workspace_id, [])
            return [
                self._serialize_operation(registry[operation_id])
                for operation_id in reversed(order)
                if operation_id in registry
            ]

    def get_operation(self, session_id: str, operation_id: str) -> Dict[str, Any]:
        web_session = self.get(session_id)
        with self._workspace_lock(web_session.workspace_id):
            operation = (self._operations.get(web_session.workspace_id, {}) or {}).get(operation_id)
            if not operation:
                raise KeyError(f"Unknown operation: {operation_id}")
            return self._serialize_operation(operation)

    def list_operation_logs(self, session_id: str, operation_id: str) -> Dict[str, Any]:
        web_session = self.get(session_id)
        with self._workspace_lock(web_session.workspace_id):
            operation = (self._operations.get(web_session.workspace_id, {}) or {}).get(operation_id)
            if not operation:
                raise KeyError(f"Unknown operation: {operation_id}")
            return {
                "workspace_id": web_session.workspace_id,
                "operation_id": operation.id,
                "kind": operation.kind,
                "status": operation.status,
                "logs": list(operation.logs),
            }

    def list_operation_events(self, session_id: str, operation_id: Optional[str] = None) -> List[Dict[str, Any]]:
        web_session = self.get(session_id)
        events = list(self._operation_events.get(web_session.workspace_id, []))
        if operation_id:
            events = [event for event in events if str(event.get("operation_id") or "") == operation_id]
        return events

    def next_event(self, session_id: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
        web_session = self.get(session_id)
        queue = self._event_queue(web_session.workspace_id)
        try:
            return queue.get(timeout=max(0.1, timeout))
        except Empty:
            return None

    def cancel_operation(self, session_id: str, operation_id: str) -> Dict[str, Any]:
        web_session = self.get(session_id)
        with self._workspace_lock(web_session.workspace_id):
            operation = (self._operations.get(web_session.workspace_id, {}) or {}).get(operation_id)
            if not operation:
                raise KeyError(f"Unknown operation: {operation_id}")
            operation.cancel_requested = True
            if operation.status == "queued":
                self._set_operation_canceled(web_session, operation, reason="Canceled before start")
            else:
                self._append_operation_event(web_session, operation, event_type="cancel_requested")
                self._persist_operation_state(web_session, active_operation_id=operation.id)
            return self._serialize_operation(operation)

    def retry_operation(self, session_id: str, operation_id: str, *, request_id: str = "") -> Dict[str, Any]:
        web_session = self.get(session_id)
        with self._workspace_lock(web_session.workspace_id):
            source = (self._operations.get(web_session.workspace_id, {}) or {}).get(operation_id)
            if not source:
                raise KeyError(f"Unknown operation: {operation_id}")
            kind = source.kind
            input_payload = dict(source.input)

        operation = self.start_operation(
            session_id=session_id,
            kind=kind,
            input_payload=input_payload,
            request_id=request_id,
            parent_operation_id=operation_id,
        )
        return operation

    def start_operation(
        self,
        session_id: str,
        *,
        kind: str,
        input_payload: Optional[Dict[str, Any]] = None,
        request_id: str = "",
        parent_operation_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        web_session = self.get(session_id)
        if not getattr(web_session.session.settings, "enable_async_operations", True):
            raise RuntimeError("Async operations are disabled")
        operation = self._new_operation(
            web_session,
            kind=kind,
            input_payload=input_payload,
            request_id=request_id,
            parent_operation_id=parent_operation_id,
        )

        future = self._executor.submit(self._run_operation_worker, web_session, operation.id)
        with self._workspace_lock(web_session.workspace_id):
            self._active_futures[operation.id] = future
        return self._serialize_operation(operation)

    def _run_operation_worker(self, web_session: WebSession, operation_id: str) -> None:
        workspace_id = web_session.workspace_id
        with self._workspace_lock(workspace_id):
            operation = (self._operations.get(workspace_id, {}) or {}).get(operation_id)
            if not operation:
                return
            if operation.cancel_requested:
                self._set_operation_canceled(web_session, operation)
                return
            self._set_operation_running(web_session, operation)
            self._append_operation_log(web_session, operation, "info", f"Starting operation: {operation.kind}")

        try:
            result = self._dispatch_operation(web_session, operation)
            with self._workspace_lock(workspace_id):
                operation = (self._operations.get(workspace_id, {}) or {}).get(operation_id)
                if not operation:
                    return
                if operation.cancel_requested:
                    self._set_operation_canceled(web_session, operation)
                    return
                self._set_operation_completed(web_session, operation, result)
                self._append_operation_log(web_session, operation, "info", "Operation completed")

            # Auto-start preprocessing in demo mode after analyze completes
            if isinstance(result, dict) and result.get("_start_preprocess"):
                try:
                    preprocess_record = self._new_operation(
                        web_session,
                        kind="preprocess_roster",
                        input_payload={"source_file": result.get("_preprocess_source_file", "")},
                        request_id=str(result.get("_preprocess_request_id", "")),
                        parent_operation_id=str(result.get("_preprocess_parent_op_id", "")),
                    )
                    pp_id = preprocess_record.id
                    future = self._executor.submit(self._run_operation_worker, web_session, pp_id)
                    with self._workspace_lock(workspace_id):
                        self._active_futures[pp_id] = future
                except Exception:
                    pass  # Preprocessing failure should not block analysis results

        except Exception as exc:
            with self._workspace_lock(workspace_id):
                operation = (self._operations.get(workspace_id, {}) or {}).get(operation_id)
                if not operation:
                    return
                if operation.cancel_requested:
                    self._set_operation_canceled(web_session, operation)
                    return
                self._set_operation_failed(web_session, operation, exc)
        finally:
            with self._workspace_lock(workspace_id):
                self._active_futures.pop(operation_id, None)

    def _dispatch_operation(self, web_session: WebSession, operation: OperationRecord) -> Dict[str, Any]:
        def progress(phase: str, message: str, percent: Optional[float] = None, level: str = "info") -> None:
            with self._workspace_lock(web_session.workspace_id):
                live = (self._operations.get(web_session.workspace_id, {}) or {}).get(operation.id)
                if not live:
                    return
                self._check_operation_canceled(live)
                self._update_operation_progress(web_session, live, phase=phase, message=message, percent=percent)
                self._append_operation_log(web_session, live, level, message)
                self._persist_operation_state(web_session, active_operation_id=live.id)

        kind = operation.kind
        payload = dict(operation.input or {})
        progress("started", f"Running {kind}", 0)

        if kind == "suggest":
            plan = self._do_suggest(web_session, progress=progress)
            bundle = self._sync_plan_audit_views(web_session, plan)
            self.conversation_store.append_plan_snapshot(web_session.scope, plan)
            result = {
                "plan": plan,
                "profile_summary": self._profile_summary(web_session.session),
                "review_summary": self._review_summary(web_session.session, plan),
                "quality_audit": bundle["quality_audit"],
                "column_audit_summary": bundle["column_audit_summary"],
                "standardization_plan": bundle["standardization_plan"],
                "client_summary": bundle["client_summary"],
                "counts": {
                    "mappings": len(plan.get("mappings", [])),
                    "transformations": len(plan.get("transformations", [])),
                    "bq_validations": len(plan.get("bq_validations", [])),
                    "quality_audit": len(bundle["quality_audit"]),
                },
            }
        elif kind == "generate":
            result = self._do_generate(
                web_session,
                mode=str(payload.get("mode") or "processor"),
                output_dir=payload.get("output_dir"),
                pipeline_name=str(payload.get("pipeline_name") or "UniversalRosterPipeline"),
                progress=progress,
            )
        elif kind == "export_training":
            result = self._do_export_training(web_session, output_dir=payload.get("output_dir"), progress=progress)
        elif kind == "run_training":
            result = self._do_run_training(
                web_session,
                output_dir=payload.get("output_dir"),
                extra_args=payload.get("extra_args"),
                progress=progress,
            )
        elif kind == "run_generated":
            result = self._do_run_generated(
                web_session,
                input_file=str(payload.get("input_file") or ""),
                generated_dir=payload.get("generated_dir"),
                output_dir=payload.get("output_dir"),
                table_id=str(payload.get("table_id") or "project.dataset.staging"),
                progress=progress,
            )
        elif kind == "analyze_selected_roster":
            selected = payload.get("selected_roster") if isinstance(payload.get("selected_roster"), dict) else {}
            file_path = str(selected.get("path") or "")
            roster_type = str(selected.get("roster_type") or "").strip() or None
            if not file_path or file_path.startswith("__browser_upload__"):
                raise ValueError("Selected roster path is not uploaded yet")
            self._do_load_file(web_session, file_path=file_path, roster_type=roster_type, progress=progress)
            plan = self._do_suggest(web_session, progress=progress)
            bundle = self._sync_plan_audit_views(web_session, plan)
            self.conversation_store.append_plan_snapshot(web_session.scope, plan)
            review_summary = self._review_summary(web_session.session, plan)
            result = {
                "profile_summary": self._profile_summary(web_session.session),
                "counts": {
                    "mappings": len(plan.get("mappings", [])),
                    "transformations": len(plan.get("transformations", [])),
                    "bq_validations": len(plan.get("bq_validations", [])),
                    "quality_audit": len(bundle["quality_audit"]),
                },
                "review_summary": review_summary,
                "quality_audit": bundle["quality_audit"],
                "column_audit_summary": bundle["column_audit_summary"],
                "standardization_plan": bundle["standardization_plan"],
                "client_summary": bundle["client_summary"],
                "plan": plan,
            }
            conversation = self.conversation_store.load(web_session.scope)
            conversation["pending_selected_roster"] = None
            self.conversation_store.save(web_session.scope, conversation)

            # In demo mode, flag that preprocessing should start after this operation completes
            if getattr(web_session.session.settings, "demo_mode", False):
                result["_start_preprocess"] = True
                result["_preprocess_source_file"] = file_path
                result["_preprocess_parent_op_id"] = operation.id
                result["_preprocess_request_id"] = operation.request_id

        elif kind == "preprocess_roster":
            result = self._do_preprocess_roster(web_session, payload=payload, progress=progress)
        elif kind == "quality_audit":
            if not web_session.session.state.plan:
                raise ValueError("No active review plan. Run suggest first.")
            if not web_session.session.state.profile:
                raise ValueError("No active profile loaded. Upload or load a file first.")

            progress("quality_audit_refresh_started", "Refreshing quality audit findings", 20)
            profile = web_session.session.state.profile or {}
            plan = web_session.session.state.plan or {}
            mappings = list(web_session.session.plan_manager.combined_items(plan, "mappings"))
            instructions_context = dict(web_session.session.state.instructions_context or {})

            quality_audit_router = web_session.session.router_factory.for_task("quality_audit")
            verifier_router = web_session.session.router_factory.for_task("verifier")
            quality_result = suggest_quality_audit(
                profile=profile,
                mappings=mappings,
                instructions_context=instructions_context,
                settings=web_session.session.settings,
                learning_kb=web_session.session.learning_kb,
                learning_retrieval=web_session.session.learning_retrieval,
                learning_scope=web_session.session.state.workspace_scope,
                roster_type=str(profile.get("roster_type_detected", "practitioner") or "practitioner"),
                primary_router=quality_audit_router,
                verifier_router=verifier_router,
                collaboration_mode=web_session.session.settings.collaboration_mode,
            )
            quality_items = list(quality_result.get("quality_audit", []))

            plan["quality_audit"] = quality_items
            confidence_summary = dict(plan.get("confidence_summary") or {})
            confidence_summary["quality_audit"] = web_session.session.plan_manager._confidence_summary(quality_items)
            plan["confidence_summary"] = confidence_summary

            auto_summary = dict(plan.get("auto_approval_summary") or {})
            auto_summary["quality_audit"] = web_session.session.plan_manager._auto_approved_count(quality_items)
            plan["auto_approval_summary"] = auto_summary

            llm_trace = dict(plan.get("llm_trace") or {})
            llm_trace["quality_audit"] = quality_result.get("llm_trace", {})
            plan["llm_trace"] = llm_trace
            web_session.session.state.plan = plan

            bundle = self._sync_plan_audit_views(web_session, plan)
            self.conversation_store.append_plan_snapshot(web_session.scope, plan)
            review_summary = self._review_summary(web_session.session, plan)
            result = {
                "profile_summary": self._profile_summary(web_session.session),
                "counts": {
                    "quality_audit": len(bundle["quality_audit"]),
                },
                "review_summary": review_summary,
                "quality_audit": bundle["quality_audit"],
                "column_audit_summary": bundle["column_audit_summary"],
                "standardization_plan": bundle["standardization_plan"],
                "client_summary": bundle["client_summary"],
                "llm_trace": quality_result.get("llm_trace", {}),
            }
            progress("quality_audit_refresh_completed", "Quality audit findings refreshed", 95)
        else:
            raise ValueError(f"Unsupported operation kind: {kind}")

        progress("completed", f"Finished {kind}", 100)
        return result

    def _do_load_file(
        self,
        web_session: WebSession,
        *,
        file_path: str,
        roster_type: Optional[str] = None,
        profile_full_roster_learning: Optional[bool] = None,
        profile_max_rows: Optional[int] = None,
        progress: Optional[Callable[..., None]] = None,
    ) -> Dict[str, Any]:
        if progress:
            progress("profiling_started", "Profiling roster", 5)
        profile = web_session.session.load_file(
            file_path=file_path,
            roster_type=roster_type,
            profile_full_roster_learning=profile_full_roster_learning,
            profile_max_rows=profile_max_rows,
            progress_callback=progress,
        )
        self.conversation_store.update_latest_profile(web_session.scope, profile)
        if progress:
            progress("profiling_completed", "Roster profile ready", 20)
        return profile

    def _do_suggest(
        self,
        web_session: WebSession,
        *,
        progress: Optional[Callable[..., None]] = None,
    ) -> Dict[str, Any]:
        conversation = self.conversation_store.load(web_session.scope)
        if isinstance(conversation.get("instructions_context"), dict):
            web_session.session.set_instructions_context(conversation["instructions_context"])

        if progress:
            progress("analysis_started", "Analyzing mappings and rules", 25)
        plan = web_session.session.suggest(use_llm_for_unresolved=True, progress_callback=progress)
        if web_session.session.state.plan_path:
            plan["_plan_path"] = str(web_session.session.state.plan_path)

        if progress:
            progress("analysis_completed", "Analysis complete", 95)
        return plan

    def _do_generate(
        self,
        web_session: WebSession,
        *,
        mode: str,
        output_dir: Optional[str],
        pipeline_name: str,
        progress: Optional[Callable[..., None]] = None,
    ) -> Dict[str, Any]:
        return web_session.session.generate(
            mode=mode,
            output_dir=output_dir,
            pipeline_name=pipeline_name,
            progress_callback=progress,
        )

    def _do_export_training(
        self,
        web_session: WebSession,
        *,
        output_dir: Optional[str],
        progress: Optional[Callable[..., None]] = None,
    ) -> Dict[str, Any]:
        return web_session.session.export_training_datasets(output_dir=output_dir, progress_callback=progress)

    def _do_run_training(
        self,
        web_session: WebSession,
        *,
        output_dir: Optional[str],
        extra_args: Optional[List[str]],
        progress: Optional[Callable[..., None]] = None,
    ) -> Dict[str, Any]:
        return web_session.session.run_trainer(export_dir=output_dir, extra_args=extra_args, progress_callback=progress)

    def _do_run_generated(
        self,
        web_session: WebSession,
        *,
        input_file: str,
        generated_dir: Optional[str],
        output_dir: Optional[str],
        table_id: str,
        progress: Optional[Callable[..., None]] = None,
    ) -> Dict[str, Any]:
        return web_session.session.run_generated_pipeline(
            input_file=input_file,
            generated_dir=generated_dir,
            output_dir=output_dir,
            table_id=table_id,
            progress_callback=progress,
        )

    def _do_preprocess_roster(
        self,
        web_session: WebSession,
        *,
        payload: Dict[str, Any],
        progress: Optional[Callable[..., None]] = None,
    ) -> Dict[str, Any]:
        import io
        import pandas as pd

        if progress:
            progress("preprocess_started", "Starting roster preprocessing", 5)

        session = web_session.session
        plan = session.state.plan or {}
        profile = session.state.profile or {}

        # Find uploaded file
        upload_dir = self.workspace_root / web_session.workspace_id / "uploads"
        upload_files = list(upload_dir.glob("*")) if upload_dir.exists() else []
        if not upload_files:
            source_file = str(payload.get("source_file") or "")
            if not source_file:
                raise ValueError("No uploaded file found for preprocessing")
        else:
            source_path = sorted(upload_files, key=lambda p: p.stat().st_mtime)[-1]
            source_file = str(source_path)

        if progress:
            progress("preprocess_loading", "Loading roster data", 10)

        # Load full DataFrame
        from pathlib import Path as _Path
        source_path = _Path(source_file)
        suffix = source_path.suffix.lower()
        if suffix in (".xlsx", ".xls"):
            df = pd.read_excel(source_path, dtype=str)
        else:
            df = pd.read_csv(source_path, dtype=str)
        df = df.fillna("")

        if progress:
            progress("preprocess_mapping", "Applying column mappings", 20)

        # Get approved mappings
        from universal_roster_v2.core.plan import PlanManager
        pm = PlanManager()
        raw_mappings = pm.combined_items(plan, "mappings") if plan else []
        approved_mappings = [m for m in raw_mappings if m.get("approved") is not False and m.get("target_field")]

        # Get BQ NPPES cache from quality audit enrichment trace
        nppes_cache: Dict[str, Any] = {}
        qa_trace = (plan.get("llm_trace") or {}).get("quality_audit") or {}
        enrichment_trace = qa_trace.get("enrichment") or {}
        # The BQ cache is populated during enrichment; extract from reference client if possible
        try:
            from universal_roster_v2.core.reference_clients import ReferenceClientFactory
            factory = ReferenceClientFactory(session.settings)
            bq = factory.bq()
            if bq and hasattr(bq, "_cache"):
                for key, entry in bq._cache.items():
                    if key.startswith("bq_npi::"):
                        npi = key.split("::")[-1]
                        nppes_cache[npi] = entry.value
        except Exception:
            pass

        # Phase 2: ONE combined LLM verification call (replaces 6 separate calls)
        import logging as _pp_log
        import json as _pp_json
        if progress:
            progress("preprocess_llm_verify", "AI verifying analysis results (single batch call)", 25)

        try:
            from universal_roster_v2.core.profile import sample_values_by_column
            from universal_roster_v2.core.plan import PlanManager as _PM2
            _pm2 = _PM2()

            samples = sample_values_by_column(profile)
            roster_type = str(profile.get("roster_type_detected", "practitioner"))

            # Gather Phase 1 candidates
            transforms_list = list(_pm2.combined_items(plan, "transformations")) if plan else []
            validations_list = list(_pm2.combined_items(plan, "bq_validations")) if plan else []
            qa_list = list(plan.get("quality_audit", []))

            # Only call LLM if there are candidates to verify
            has_candidates = bool(transforms_list or validations_list or qa_list)
            if has_candidates:
                # Build combined verification prompt
                tx_summary = [
                    {"id": t.get("id", ""), "name": t.get("name", ""), "source_columns": t.get("source_columns", []),
                     "target_fields": t.get("target_fields", []), "confidence": t.get("confidence", 0)}
                    for t in transforms_list[:30]
                ]
                val_summary = [
                    {"id": v.get("id", ""), "name": v.get("name", ""), "rule_type": v.get("rule_type", ""),
                     "source_column": v.get("source_column", ""), "target_field": v.get("target_field", ""),
                     "severity": v.get("severity", ""), "confidence": v.get("confidence", 0)}
                    for v in validations_list[:40]
                ]
                qa_summary = [
                    {"id": q.get("id", ""), "category": q.get("category", ""), "severity": q.get("severity", ""),
                     "title": q.get("title", ""), "affected_rows": q.get("affected_rows", 0),
                     "confidence": q.get("confidence", 0)}
                    for q in qa_list[:30]
                ]
                # Limit sample values to mapped columns only
                mapped_cols = {m.get("source_column", "") for m in approved_mappings}
                sample_hint = {col: vals[:6] for col, vals in samples.items() if col in mapped_cols}

                verify_prompt = f"""You are a healthcare roster data verification expert.
Review the Phase 1 analysis candidates below and decide which to keep or reject.
Roster type: {roster_type}

## TRANSFORM CANDIDATES
{_pp_json.dumps(tx_summary, indent=1)}

## VALIDATION CANDIDATES
{_pp_json.dumps(val_summary, indent=1)}

## QUALITY AUDIT FINDINGS
{_pp_json.dumps(qa_summary, indent=1)}

## SAMPLE DATA (mapped columns)
{_pp_json.dumps(sample_hint, indent=1)}

Return JSON with three arrays. For each candidate, decide "keep" or "reject":
{{
  "transform_decisions": [{{"id":"tx::...", "action":"keep"}}],
  "validation_decisions": [{{"id":"bq::...", "action":"keep"}}],
  "quality_decisions": [{{"id":"qa::...", "action":"keep"}}]
}}
Only reject candidates that are clearly wrong. When in doubt, keep."""

                from universal_roster_v2.llm.router import LLMRouterFactory
                router_factory = LLMRouterFactory(settings=session.settings)
                verify_router = router_factory.for_task("quality_audit")

                _pp_log.warning(f"PREPROCESS: Running combined verification call ({len(tx_summary)} transforms, {len(val_summary)} validations, {len(qa_summary)} QA)")
                routed = verify_router.generate(prompt=verify_prompt, task_type="quality_audit")
                from universal_roster_v2.core.mapping import extract_json_object
                decisions = extract_json_object(routed.response.text)

                # Apply rejection decisions
                reject_ids = set()
                for section_key in ("transform_decisions", "validation_decisions", "quality_decisions"):
                    for d in decisions.get(section_key, []):
                        if isinstance(d, dict) and str(d.get("action", "")).lower() == "reject":
                            reject_ids.add(str(d.get("id", "")))

                if reject_ids:
                    _pp_log.warning(f"PREPROCESS: LLM rejected {len(reject_ids)} candidates: {reject_ids}")
                else:
                    _pp_log.warning("PREPROCESS: LLM kept all candidates")

                if progress:
                    progress("preprocess_llm_done", f"AI verification complete ({len(reject_ids)} rejected)", 45)
            else:
                _pp_log.warning("PREPROCESS: No candidates to verify, skipping LLM call")
                if progress:
                    progress("preprocess_llm_done", "No candidates to verify", 45)

        except Exception as exc:
            _pp_log.warning(f"PREPROCESS: Combined verification failed (non-fatal): {exc}")
            if progress:
                progress("preprocess_llm_done", "Using Phase 1 results (verification skipped)", 45)

        if progress:
            progress("preprocess_transforms", "Applying data transforms", 50)

        # Run preprocessing pipeline
        from universal_roster_v2.core.preprocessing_pipeline import PreprocessingPipeline
        pipeline = PreprocessingPipeline(
            mappings=approved_mappings,
            nppes_cache=nppes_cache,
        )
        output_df = pipeline.run(df)

        if progress:
            progress("preprocess_saving", "Saving processed file", 80)

        # Save to workspace
        workspace_dir = self.workspace_root / web_session.workspace_id
        workspace_dir.mkdir(parents=True, exist_ok=True)
        file_name = profile.get("file_name") or "roster"
        stem = _Path(str(file_name)).stem
        output_path = workspace_dir / f"{stem}_preprocessed.csv"
        output_df.to_csv(output_path, index=False)

        if progress:
            progress("preprocess_completed", "Preprocessing complete", 95)

        return {
            "preprocessed_path": str(output_path),
            "row_count": len(output_df),
            "column_count": len(output_df.columns),
            "warnings_count": int((output_df.get("Warnings", pd.Series(dtype=str)).astype(str).str.strip() != "").sum()),
            "validations_count": int((output_df.get("Business_Validations", pd.Series(dtype=str)).astype(str).str.strip() != "").sum()),
        }

    def _append(self, web_session: WebSession, role: str, content: str) -> None:
        self.conversation_store.append_chat_message(web_session.scope, role=role, content=content)

    def _guidance(self, web_session: WebSession, event: str, message: str) -> Dict[str, Any]:
        stage = self._session_stage(web_session.session)
        next_actions = self._next_actions(stage)
        assistant_message = message + "\n\nNext actions:\n- " + "\n- ".join(next_actions)
        self._append(web_session, role="assistant", content=assistant_message)
        web_session.session.record_chat_outcome(
            {
                "event": event,
                "result": "blocked",
                "workspace_id": web_session.workspace_id,
                "stage": stage,
            }
        )
        return {
            "type": "guidance",
            "message": assistant_message,
            "stage": stage,
            "next_actions": next_actions,
        }

    @staticmethod
    def _session_stage(session: UniversalRosterSession) -> str:
        if not session.state.profile:
            return "new_workspace"
        if not session.state.plan:
            return "profile_ready"
        unchecked = session.plan_manager.unchecked_counts(session.state.plan)
        return "in_review" if sum(unchecked.values()) > 0 else "ready_to_generate"

    @staticmethod
    def _next_actions(stage: str) -> List[str]:
        if stage == "new_workspace":
            return [
                "Upload a roster file (CSV/XLS/XLSX).",
                "Optionally upload notes/context files.",
                "Ask me to analyze the roster when upload finishes.",
            ]
        if stage == "profile_ready":
            return [
                "Ask for schema and profile summary.",
                "Ask me to analyze suggestions (mappings, transformations, quality audit, validations).",
                "Review suggested items and decide what to keep.",
            ]
        if stage == "in_review":
            return [
                "Review or change mapping/transformation/quality-audit/validation decisions.",
                "Ask me to explain any transformation, quality audit finding, or validation.",
                "Generate artifacts when you are ready.",
            ]
        return [
            "Generate schema, processor, or main artifacts.",
            "Run the generated pipeline with an input file.",
            "Adjust approvals and regenerate if needed.",
        ]

    @staticmethod
    def _review_summary(session: UniversalRosterSession, plan: Dict[str, Any]) -> Dict[str, Any]:
        if not plan:
            return {
                "total": 0,
                "unchecked": 0,
                "sections": {
                    "mappings": {"total": 0, "unchecked": 0},
                    "transformations": {"total": 0, "unchecked": 0},
                    "bq_validations": {"total": 0, "unchecked": 0},
                    "quality_audit": {"total": 0, "unchecked": 0},
                },
                "confidence": {
                    "mappings": {"high": 0, "medium": 0, "low": 0},
                    "transformations": {"high": 0, "medium": 0, "low": 0},
                    "bq_validations": {"high": 0, "medium": 0, "low": 0},
                    "quality_audit": {"high": 0, "medium": 0, "low": 0},
                },
            }

        def confidence_counts(items: List[Dict[str, Any]]) -> Dict[str, int]:
            out = {"high": 0, "medium": 0, "low": 0}
            for item in items:
                band = str(item.get("confidence_band", "") or "").strip().lower()
                if band in out:
                    out[band] += 1
            return out

        mappings = session.plan_manager.combined_items(plan, "mappings")
        transformations = session.plan_manager.combined_items(plan, "transformations")
        bq_validations = session.plan_manager.combined_items(plan, "bq_validations")
        quality_audit = session.plan_manager.combined_items(plan, "quality_audit")

        total_counts = {
            "mappings": len(mappings),
            "transformations": len(transformations),
            "bq_validations": len(bq_validations),
            "quality_audit": len(quality_audit),
        }
        unchecked_counts = session.plan_manager.unchecked_counts(plan)
        return {
            "total": sum(total_counts.values()),
            "unchecked": sum(unchecked_counts.values()),
            "sections": {
                "mappings": {"total": total_counts["mappings"], "unchecked": unchecked_counts["mappings"]},
                "transformations": {
                    "total": total_counts["transformations"],
                    "unchecked": unchecked_counts["transformations"],
                },
                "bq_validations": {
                    "total": total_counts["bq_validations"],
                    "unchecked": unchecked_counts["bq_validations"],
                },
                "quality_audit": {
                    "total": total_counts["quality_audit"],
                    "unchecked": unchecked_counts["quality_audit"],
                },
            },
            "confidence": {
                "mappings": confidence_counts(mappings),
                "transformations": confidence_counts(transformations),
                "bq_validations": confidence_counts(bq_validations),
                "quality_audit": confidence_counts(quality_audit),
            },
        }

    @staticmethod
    def _profile_summary(session: UniversalRosterSession) -> Dict[str, Any]:
        profile = session.state.profile or {}
        if not profile:
            return {
                "file_name": None,
                "roster_type_detected": None,
                "column_count": 0,
                "sample_size": 0,
                "profiling_mode": None,
                "rows_profiled": 0,
                "rows_total": 0,
                "samples": [],
                "semantic_evidence": [],
            }

        samples_by_column = sample_values_by_column(profile, max_per_column=_PROFILE_SAMPLE_VALUE_LIMIT)
        columns = [str(col) for col in (profile.get("columns") or [])]
        sample_preview = [
            {"column": column, "values": samples_by_column.get(column, [])}
            for column in columns[:_PROFILE_SAMPLE_COLUMN_LIMIT]
        ]

        # Include actual sample rows for provider extraction (credentialing/monitoring tabs)
        raw_sample_rows = profile.get("sample_rows") or []
        sample_rows_for_frontend = []
        if isinstance(raw_sample_rows, list):
            for row in raw_sample_rows[:50]:  # Limit to 50 rows
                if isinstance(row, dict):
                    sample_rows_for_frontend.append({str(k): str(v).strip() for k, v in row.items()})

        semantic_profile = profile.get("semantic_profile") or {}
        semantic_by_column = semantic_profile.get("column_semantics") or {}
        semantic_evidence = []
        for column in columns[:_PROFILE_SAMPLE_COLUMN_LIMIT]:
            col_sem = semantic_by_column.get(column) or {}
            if not isinstance(col_sem, dict):
                continue
            semantic_evidence.append(
                {
                    "column": column,
                    "type_likelihoods": col_sem.get("aggregate_type_likelihoods", {}),
                    "top_values": col_sem.get("top_values", []),
                    "null_pct_avg": col_sem.get("null_pct_avg", 0.0),
                }
            )

        return {
            "file_name": profile.get("file_name"),
            "roster_type_detected": profile.get("roster_type_detected"),
            "column_count": len(columns),
            "sample_size": int(profile.get("row_sample_size", 0) or 0),
            "profiling_mode": profile.get("profiling_mode") or "sample",
            "rows_profiled": int(profile.get("rows_profiled", profile.get("row_sample_size", 0)) or 0),
            "rows_total": int(profile.get("rows_total", 0) or 0),
            "samples": sample_preview,
            "semantic_evidence": semantic_evidence,
            "sheet_drift": (semantic_profile.get("sheet_drift") or {}),
            "sample_rows": sample_rows_for_frontend,
        }

    @staticmethod
    def _plan_views(session: UniversalRosterSession, plan: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        if not plan:
            return {
                "mappings": [],
                "transformations": [],
                "bq_validations": [],
                "quality_audit": [],
            }
        return {
            "mappings": session.plan_manager.combined_items(plan, "mappings"),
            "transformations": session.plan_manager.combined_items(plan, "transformations"),
            "bq_validations": session.plan_manager.combined_items(plan, "bq_validations"),
            "quality_audit": session.plan_manager.combined_items(plan, "quality_audit"),
        }

    @staticmethod
    def _issue_impact_tier(affected_pct: float) -> str:
        if affected_pct >= 0.35:
            return "high"
        if affected_pct >= 0.12:
            return "medium"
        return "low"

    @staticmethod
    def _sorted_linked_ids(items: Sequence[str]) -> List[str]:
        return sorted({str(item).strip() for item in items if str(item).strip()})

    @staticmethod
    def _preferred_action(actions: Sequence[str], *, has_unmapped: bool) -> str:
        normalized = [str(action or "").strip().lower() for action in actions if str(action or "").strip()]
        if has_unmapped:
            normalized.append("source remediation")
        if not normalized:
            return "review"
        return sorted(normalized, key=lambda item: (_COLUMN_ACTION_PRIORITY.get(item, 99), item))[0]

    def _build_column_audit_bundle(
        self,
        *,
        profile: Dict[str, Any],
        mappings: List[Dict[str, Any]],
        transformations: List[Dict[str, Any]],
        bq_validations: List[Dict[str, Any]],
        quality_audit: List[Dict[str, Any]],
        review_summary: Dict[str, Any],
    ) -> Dict[str, Any]:
        rows_profiled = int(profile.get("rows_profiled", profile.get("row_sample_size", 0)) or 0)
        rows_total = int(profile.get("rows_total", 0) or 0)
        denominator = max(rows_profiled, rows_total, 1)
        columns = [str(col) for col in (profile.get("columns") or []) if str(col).strip()]
        columns_set = set(columns)
        samples_by_column = sample_values_by_column(profile, max_per_column=_PROFILE_SAMPLE_VALUE_LIMIT)

        rows: Dict[str, Dict[str, Any]] = {}
        for column in columns:
            rows[column] = {
                "column_key": column,
                "column_label": column,
                "sample_values": list(samples_by_column.get(column, []))[:4],
                "mapped": False,
                "profiled": True,
                "severity_counts": {"error": 0, "warning": 0, "info": 0},
                "finding_count": 0,
                "affected_rows": 0,
                "affected_pct": 0.0,
                "linked_item_ids": [],
                "linked_findings": [],
                "recommended_action": "review",
                "unchecked_count": 0,
                "column_rank_score": 0.0,
                "impact_tier": "low",
            }

        for item in mappings:
            source = str(item.get("source_column") or "").strip()
            target = str(item.get("target_field") or "").strip()
            item_id = str(item.get("id") or "").strip()
            approved = bool(item.get("approved", True))
            for key in [source, target]:
                if not key:
                    continue
                rows.setdefault(
                    key,
                    {
                        "column_key": key,
                        "column_label": key,
                        "sample_values": list(samples_by_column.get(key, []))[:4],
                        "mapped": False,
                        "profiled": key in columns_set,
                        "severity_counts": {"error": 0, "warning": 0, "info": 0},
                        "finding_count": 0,
                        "affected_rows": 0,
                        "affected_pct": 0.0,
                        "linked_item_ids": [],
                        "linked_findings": [],
                        "recommended_action": "review",
                        "unchecked_count": 0,
                        "column_rank_score": 0.0,
                        "impact_tier": "low",
                    },
                )
                rows[key]["mapped"] = rows[key]["mapped"] or bool(source and target)
                if item_id:
                    rows[key]["linked_item_ids"].append(item_id)
                if not approved:
                    rows[key]["unchecked_count"] += 1

        for item in transformations:
            item_id = str(item.get("id") or "").strip()
            approved = bool(item.get("approved", True))
            linked_columns = [str(value).strip() for value in (item.get("source_columns") or []) if str(value).strip()]
            linked_columns += [str(value).strip() for value in (item.get("target_fields") or []) if str(value).strip()]
            for key in linked_columns:
                rows.setdefault(
                    key,
                    {
                        "column_key": key,
                        "column_label": key,
                        "sample_values": list(samples_by_column.get(key, []))[:4],
                        "mapped": False,
                        "profiled": key in columns_set,
                        "severity_counts": {"error": 0, "warning": 0, "info": 0},
                        "finding_count": 0,
                        "affected_rows": 0,
                        "affected_pct": 0.0,
                        "linked_item_ids": [],
                        "linked_findings": [],
                        "recommended_action": "review",
                        "unchecked_count": 0,
                        "column_rank_score": 0.0,
                        "impact_tier": "low",
                    },
                )
                if item_id:
                    rows[key]["linked_item_ids"].append(item_id)
                if not approved:
                    rows[key]["unchecked_count"] += 1

        for item in bq_validations:
            source = str(item.get("source_column") or "").strip()
            target = str(item.get("target_field") or "").strip()
            item_id = str(item.get("id") or "").strip()
            approved = bool(item.get("approved", True))
            for key in [source, target]:
                if not key:
                    continue
                rows.setdefault(
                    key,
                    {
                        "column_key": key,
                        "column_label": key,
                        "sample_values": list(samples_by_column.get(key, []))[:4],
                        "mapped": False,
                        "profiled": key in columns_set,
                        "severity_counts": {"error": 0, "warning": 0, "info": 0},
                        "finding_count": 0,
                        "affected_rows": 0,
                        "affected_pct": 0.0,
                        "linked_item_ids": [],
                        "linked_findings": [],
                        "recommended_action": "review",
                        "unchecked_count": 0,
                        "column_rank_score": 0.0,
                        "impact_tier": "low",
                    },
                )
                if item_id:
                    rows[key]["linked_item_ids"].append(item_id)
                if not approved:
                    rows[key]["unchecked_count"] += 1

        issue_rank: Dict[str, Dict[str, Any]] = {}
        for issue in quality_audit:
            issue_id = str(issue.get("id") or "").strip()
            severity = str(issue.get("severity") or "info").strip().lower() or "info"
            if severity not in {"error", "warning", "info"}:
                severity = "info"
            affected_rows = max(0, int(issue.get("affected_rows", 0) or 0))
            affected_pct = float(issue.get("affected_pct", 0.0) or 0.0)
            if affected_pct <= 0.0 and denominator > 0:
                affected_pct = round(affected_rows / denominator, 4)

            source = str(issue.get("source_column") or "").strip()
            target = str(issue.get("target_field") or "").strip()
            keys = [value for value in [source, target] if value]
            if not keys:
                keys = list(columns)

            fix = issue.get("suggested_fix") if isinstance(issue.get("suggested_fix"), dict) else {}
            issue_action = str(fix.get("action") or "review").strip().lower() or "review"
            if issue_action == "source_remediation":
                issue_action = "source remediation"
            if issue_action not in {"transform", "validate", "review", "source remediation"}:
                issue_action = "review"

            for key in keys:
                rows.setdefault(
                    key,
                    {
                        "column_key": key,
                        "column_label": key,
                        "sample_values": list(samples_by_column.get(key, []))[:4],
                        "mapped": False,
                        "profiled": key in columns_set,
                        "severity_counts": {"error": 0, "warning": 0, "info": 0},
                        "finding_count": 0,
                        "affected_rows": 0,
                        "affected_pct": 0.0,
                        "linked_item_ids": [],
                        "linked_findings": [],
                        "recommended_action": "review",
                        "unchecked_count": 0,
                        "column_rank_score": 0.0,
                        "impact_tier": "low",
                    },
                )
                rows[key]["severity_counts"][severity] += 1
                rows[key]["finding_count"] += 1
                rows[key]["affected_rows"] = max(rows[key]["affected_rows"], affected_rows)
                rows[key]["affected_pct"] = max(rows[key]["affected_pct"], round(affected_pct, 4))
                if issue_id:
                    rows[key]["linked_item_ids"].append(issue_id)
                rows[key]["linked_findings"].append(
                    {
                        "id": issue_id,
                        "severity": severity,
                        "title": str(issue.get("title") or issue.get("message") or issue_id),
                        "message": str(issue.get("message") or ""),
                        "action_group": issue_action,
                        "affected_rows": affected_rows,
                        "affected_pct": round(affected_pct, 4),
                    }
                )

        for key, row in rows.items():
            row["linked_item_ids"] = self._sorted_linked_ids(row.get("linked_item_ids") or [])
            row["linked_findings"] = sorted(
                list(row.get("linked_findings") or []),
                key=lambda item: (
                    {"error": 0, "warning": 1, "info": 2}.get(str(item.get("severity") or "info"), 3),
                    str(item.get("id") or ""),
                ),
            )
            finding_actions = [str(item.get("action_group") or "review") for item in row["linked_findings"]]
            row["recommended_action"] = self._preferred_action(finding_actions, has_unmapped=not bool(row.get("mapped")))
            row["impact_tier"] = self._issue_impact_tier(float(row.get("affected_pct", 0.0) or 0.0))
            sev_counts = row.get("severity_counts") or {}
            severity_score = (
                float(sev_counts.get("error", 0) or 0) * _SEVERITY_WEIGHT["error"]
                + float(sev_counts.get("warning", 0) or 0) * _SEVERITY_WEIGHT["warning"]
                + float(sev_counts.get("info", 0) or 0) * _SEVERITY_WEIGHT["info"]
            )
            impact_score = _IMPACT_WEIGHT.get(str(row.get("impact_tier") or "low"), 1.0)
            row["column_rank_score"] = round(
                (severity_score * 8.0)
                + (float(row.get("affected_pct", 0.0) or 0.0) * 100.0 * 2.0)
                + (float(row.get("unchecked_count", 0) or 0) * 4.0)
                + (6.0 if not bool(row.get("mapped")) else 0.0)
                + impact_score,
                2,
            )

            for finding in row["linked_findings"]:
                if finding.get("id"):
                    issue_rank[str(finding["id"])] = {
                        "column_key": key,
                        "action_group": str(finding.get("action_group") or row["recommended_action"]),
                        "client_impact": str(row.get("impact_tier") or "low"),
                        "column_rank_score": float(row.get("column_rank_score") or 0.0),
                    }

        column_rows = sorted(
            rows.values(),
            key=lambda row: (-float(row.get("column_rank_score", 0.0) or 0.0), str(row.get("column_key") or "")),
        )

        action_counts: Dict[str, int] = defaultdict(int)
        impact_counts: Dict[str, int] = defaultdict(int)
        total_affected_rows = 0
        for row in column_rows:
            action_counts[str(row.get("recommended_action") or "review")] += 1
            impact_counts[str(row.get("impact_tier") or "low")] += 1
            total_affected_rows += int(row.get("affected_rows", 0) or 0)

        top_priority_columns = [
            {
                "column_key": str(row.get("column_key") or ""),
                "recommended_action": str(row.get("recommended_action") or "review"),
                "finding_count": int(row.get("finding_count", 0) or 0),
                "impact_tier": str(row.get("impact_tier") or "low"),
                "column_rank_score": float(row.get("column_rank_score", 0.0) or 0.0),
            }
            for row in column_rows[:6]
        ]

        workstream_defs = [
            ("format_normalization", "Format normalization", {"transform"}, "Standardize formats and canonical value shapes."),
            (
                "completeness_remediation",
                "Completeness remediation",
                {"source remediation"},
                "Address null-heavy or incomplete source fields before downstream use.",
            ),
            ("dedupe_identity", "Deduplication and identity", {"review"}, "Review duplicate and conflicting identities before load."),
            (
                "external_quality_checks",
                "External quality checks",
                {"validate"},
                "Enforce validation rules and external reference checks for trustable output.",
            ),
        ]

        workstreams: List[Dict[str, Any]] = []
        for workstream_id, title, action_set, narrative in workstream_defs:
            scoped_rows = [row for row in column_rows if str(row.get("recommended_action") or "") in action_set]
            if not scoped_rows:
                continue
            actions = []
            for row in scoped_rows:
                actions.append(
                    {
                        "column_key": str(row.get("column_key") or ""),
                        "action": str(row.get("recommended_action") or "review"),
                        "reason": f"{int(row.get('finding_count', 0) or 0)} finding(s), impact {row.get('impact_tier')}",
                        "linked_item_ids": list(row.get("linked_item_ids") or []),
                    }
                )
            workstreams.append(
                {
                    "id": workstream_id,
                    "title": title,
                    "narrative": narrative,
                    "column_count": len(scoped_rows),
                    "estimated_rows_impacted": sum(int(row.get("affected_rows", 0) or 0) for row in scoped_rows),
                    "actions": actions,
                }
            )

        column_audit_summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "rows_profiled": rows_profiled,
            "rows_total": rows_total,
            "columns": column_rows,
            "totals": {
                "column_count": len(column_rows),
                "mapped_count": len([row for row in column_rows if bool(row.get("mapped"))]),
                "unmapped_count": len([row for row in column_rows if not bool(row.get("mapped"))]),
                "findings_count": len(quality_audit),
                "affected_rows": total_affected_rows,
            },
        }

        standardization_plan = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "workstreams": workstreams,
            "action_counts": dict(sorted(action_counts.items())),
            "impact_counts": dict(sorted(impact_counts.items())),
            "top_priority_columns": top_priority_columns,
        }

        high_impact_columns = len([row for row in column_rows if row.get("impact_tier") == "high"])
        findings_total = int((review_summary.get("sections") or {}).get("quality_audit", {}).get("total", len(quality_audit)) or 0)
        unchecked_total = int(review_summary.get("unchecked", 0) or 0)
        readiness_score = 100 - min(95, (high_impact_columns * 12) + (unchecked_total * 3) + (len([row for row in column_rows if not row.get("mapped")]) * 6))

        client_summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "headline": "Column-level standardization plan ready",
            "kpis": {
                "columns_profiled": len(column_rows),
                "columns_with_findings": len([row for row in column_rows if int(row.get("finding_count", 0) or 0) > 0]),
                "high_impact_columns": high_impact_columns,
                "total_findings": findings_total,
                "pending_review_items": unchecked_total,
                "estimated_rows_impacted": total_affected_rows,
                "readiness_score": max(5, int(readiness_score)),
            },
            "top_priority_columns": top_priority_columns,
            "why_it_improves_data_quality": [
                "Every source column is mapped to a clear action path (transform, validate, review, or source remediation).",
                "Priority ordering is tied to severity and row impact so teams fix the highest-value issues first.",
                "Linked IDs provide traceability from client summary to technical findings and approvals.",
            ],
        }

        enriched_quality_audit = []
        for issue in quality_audit:
            if not isinstance(issue, dict):
                continue
            entry = dict(issue)
            meta = issue_rank.get(str(issue.get("id") or ""), {})
            entry["column_key"] = str(meta.get("column_key") or issue.get("source_column") or issue.get("target_field") or "")
            entry["action_group"] = str(meta.get("action_group") or "review")
            entry["client_impact"] = str(meta.get("client_impact") or "low")
            entry["column_rank_score"] = float(meta.get("column_rank_score") or 0.0)
            enriched_quality_audit.append(entry)

        return {
            "column_audit_summary": column_audit_summary,
            "standardization_plan": standardization_plan,
            "client_summary": client_summary,
            "quality_audit": enriched_quality_audit,
        }

    def _sync_plan_audit_views(self, web_session: WebSession, plan: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(plan, dict):
            return {
                "column_audit_summary": {"columns": [], "totals": {}},
                "standardization_plan": {"workstreams": [], "top_priority_columns": []},
                "client_summary": {"kpis": {}, "top_priority_columns": []},
                "quality_audit": [],
            }

        plan_views = self._plan_views(web_session.session, plan)
        review_summary = self._review_summary(web_session.session, plan)
        bundle = self._build_column_audit_bundle(
            profile=web_session.session.state.profile or {},
            mappings=plan_views["mappings"],
            transformations=plan_views["transformations"],
            bq_validations=plan_views["bq_validations"],
            quality_audit=plan_views["quality_audit"],
            review_summary=review_summary,
        )

        plan["quality_audit"] = list(bundle["quality_audit"])
        plan["column_audit_summary"] = dict(bundle["column_audit_summary"])
        plan["standardization_plan"] = dict(bundle["standardization_plan"])
        plan["client_summary"] = dict(bundle["client_summary"])
        web_session.session.state.plan = plan

        if web_session.session.state.plan_path:
            web_session.session.plan_manager.save_plan(plan, web_session.session.state.plan_path)

        return bundle

    def _consume_pending_roster_choice(self, web_session: WebSession, text: str, lower: str) -> Optional[Dict[str, Any]]:
        if not web_session.pending_roster_choices:
            return None

        choice = self._resolve_roster_choice(web_session.pending_roster_choices, text, lower)
        if choice is None:
            prompt = self._pending_roster_choice_prompt(web_session.pending_roster_choices)
            normalized = " ".join(str(text or "").strip().split())
            if normalized and self._is_unrelated_pending_roster_text(text, lower):
                assistant_message = (
                    "A roster choice is still pending. Please reply with only the option number "
                    "or exact file name before asking for other actions.\n\n"
                    f"{prompt}"
                )
                self._append(web_session, role="assistant", content=assistant_message)
                return {
                    "type": "pending_roster_choice_blocked",
                    "choices": list(web_session.pending_roster_choices),
                    "message": assistant_message,
                }
            if normalized:
                prompt = (
                    f"I couldn’t match '{normalized}' to one unique roster option.\n"
                    "Reply with just the option number (for example: 1 or 2), or paste the exact file name.\n\n"
                    f"{prompt}"
                )
            self._append(web_session, role="assistant", content=prompt)
            return {
                "type": "pending_roster_choice",
                "choices": list(web_session.pending_roster_choices),
                "message": prompt,
            }

        web_session.pending_roster_choices = []
        self._persist_operation_state(web_session)
        source = str(choice.get("source", "roster") or "roster")
        file_path = str(choice.get("path", "") or "").strip()
        if not file_path:
            assistant_message = f"Selected {choice.get('name') or 'candidate'}."
            self._append(web_session, role="assistant", content=assistant_message)
            conversation = self.conversation_store.load(web_session.scope)
            conversation["pending_selected_roster"] = dict(choice)
            self.conversation_store.save(web_session.scope, conversation)
            return {
                "type": "pending_roster_choice_selected",
                "selected_choice": dict(choice),
                "message": assistant_message,
            }

        if file_path.startswith("__browser_upload__"):
            assistant_message = f"Selected {choice.get('name') or 'candidate'}. Ready to upload and analyze when you confirm."
            self._append(web_session, role="assistant", content=assistant_message)
            conversation = self.conversation_store.load(web_session.scope)
            conversation["pending_selected_roster"] = dict(choice)
            self.conversation_store.save(web_session.scope, conversation)
            return {
                "type": "pending_roster_choice_selected",
                "selected_choice": dict(choice),
                "message": assistant_message,
                "analysis_required": True,
            }

        if source == "note":
            note_path = Path(file_path).expanduser().resolve()
            content = note_path.read_bytes()
            result = self.upload_note_attachment(
                session_id=web_session.workspace_id,
                filename=str(choice.get("name") or note_path.name),
                content=content,
            )
            assistant_message = f"Attached {note_path.name} as context."
            self._append(web_session, role="assistant", content=assistant_message)
            return {
                "type": "note_upload",
                "attachment": result.get("attachment"),
                "hints": result.get("hints", []),
                "message": assistant_message,
            }

        selected_name = str(choice.get("name") or Path(file_path).name or "selected roster").strip()
        assistant_message = (
            f"Selected {selected_name}. I’ve set it as the active roster choice. "
            "Start analysis to run profiling and suggestions."
        )
        self._append(web_session, role="assistant", content=assistant_message)
        conversation = self.conversation_store.load(web_session.scope)
        conversation["pending_selected_roster"] = {
            **dict(choice),
            "path": file_path,
            "name": selected_name,
        }
        self.conversation_store.save(web_session.scope, conversation)
        return {
            "type": "pending_roster_choice_selected",
            "selected_choice": {
                **dict(choice),
                "path": file_path,
                "name": selected_name,
            },
            "message": assistant_message,
            "analysis_required": True,
        }

    @staticmethod
    def _is_unrelated_pending_roster_text(text: str, lower: str) -> bool:
        normalized = str(lower or "").strip()
        raw = str(text or "").strip()
        if not normalized:
            return False
        if re.match(r"^(?:option\s*)?\d{1,3}(?:\b.*)?$", normalized):
            return False
        if len(raw) <= 16:
            return False
        allowed_fragments = [
            "option",
            "candidate",
            "choose",
            "select",
            ".csv",
            ".xls",
            ".xlsx",
            "roster",
        ]
        return not any(fragment in normalized for fragment in allowed_fragments)

    @staticmethod
    def _resolve_roster_choice(choices: Sequence[Dict[str, Any]], text: str, lower: str) -> Optional[Dict[str, Any]]:
        stripped = text.strip()
        if not stripped:
            return None

        index_match = re.match(r"^(?:option\s*)?(\d{1,3})(?:\b.*)?$", lower)
        if index_match:
            index = int(index_match.group(1)) - 1
            if 0 <= index < len(choices):
                return choices[index]

        for idx, option in enumerate(choices, start=1):
            name = str(option.get("name") or "").strip()
            path = str(option.get("path") or "").strip()
            if not name and not path:
                continue
            if stripped == name or stripped == path:
                return option
            if lower == name.lower() or lower == path.lower():
                return option
            if lower in {f"{idx}", f"option {idx}"}:
                return option

        substring_matches: List[Dict[str, Any]] = []
        for option in choices:
            name = str(option.get("name") or "").strip().lower()
            path = str(option.get("path") or "").strip().lower()
            if name and name in lower:
                substring_matches.append(option)
                continue
            if path and path in lower:
                substring_matches.append(option)

        if len(substring_matches) == 1:
            return substring_matches[0]
        return None

    @staticmethod
    def _pending_roster_choice_prompt(choices: Sequence[Dict[str, Any]]) -> str:
        if not choices:
            return "I found multiple roster candidates. Tell me which file to use."
        lines = [
            "I found multiple roster candidates. Reply with the option number or file name to choose one:",
        ]
        for idx, option in enumerate(choices, start=1):
            name = str(option.get("name") or option.get("path") or "candidate")
            roster_type = str(option.get("roster_type") or "unknown")
            source = str(option.get("source") or "roster")
            lines.append(f"{idx}. {name} ({source}, detected={roster_type})")
        return "\n".join(lines)

    @staticmethod
    def _scope_payload(scope: ConversationScope) -> Dict[str, Any]:
        return {
            "workspace_signature": scope_signature(scope),
            "tenant_id": scope.tenant_id,
            "client_id": scope.client_id,
            "thread_id": scope.thread_id,
            "workspace_path": scope.workspace_path,
        }

    def _ask_supervisor(
        self,
        web_session: WebSession,
        *,
        decision_type: str,
        text: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if decision_type not in self._SUPERVISOR_DECISION_TYPES:
            return {
                "status": "unsupported_decision_type",
                "provider": None,
                "model": None,
                "attempts": [],
                "response_type": "none",
                "question_text": "",
                "reason": "",
                "approved": None,
                "confidence": 0.0,
            }
        payload = web_session.session.chat_supervisor_trace(
            decision_type=decision_type,
            text=text,
            context=context or {},
        )
        response_type = str(payload.get("response_type") or "none").strip().lower()
        if response_type not in self._SUPERVISOR_RESPONSE_TYPES:
            payload["response_type"] = "none"
        return payload

    def _dynamic_rationale_prompt(
        self,
        web_session: WebSession,
        *,
        event: str,
        item_type: str,
        item_id: str,
        approved: Optional[bool],
        source: str,
    ) -> tuple[str, Dict[str, Any]]:
        item = web_session.session._item_for_id(item_type=item_type, item_id=item_id) or {}
        context = {
            "event": event,
            "item_type": item_type,
            "item_id": item_id,
            "approved": approved,
            "source": source,
            "item": item,
        }
        supervisor = self._ask_supervisor(
            web_session,
            decision_type="rationale_followup",
            text=f"Generate a concise contextual follow-up question for decision rationale capture. source={source}",
            context=context,
        )
        if web_session.session.settings.enable_dynamic_rationale_questions:
            question = str(supervisor.get("question_text") or "").strip()
            if question:
                return question, supervisor
        return "What drove that decision? (one short reason, or say 'skip')", supervisor

    def _parse_custom_action_request(self, web_session: WebSession, text: str) -> Optional[Dict[str, Any]]:
        session = web_session.session
        if not session.state.plan:
            return None

        parser_router = None
        try:
            parser_router = session.router_factory.for_task("analysis")
        except Exception:
            parser_router = None

        supervisor_trace = self._ask_supervisor(
            web_session,
            decision_type="custom_action_request",
            text=text,
            context={
                "roster_type": session._plan_roster_type(),
                "has_plan": bool(session.state.plan),
            },
        )

        parsed = parse_custom_chat_action(
            text,
            roster_type=session._plan_roster_type(),
            schema_registry=session.schema_registry,
            parser_router=parser_router,
        )

        action_type = str(parsed.get("action_type") or "none")
        if action_type == "none":
            return None

        if action_type == "clarify":
            assistant_message = str(
                supervisor_trace.get("question_text")
                or parsed.get("preview_text")
                or "I need a bit more detail before I can add that custom action."
            )
            self._append(web_session, role="assistant", content=assistant_message)
            session.record_chat_outcome(
                {
                    "event": "custom_action_parse_clarify",
                    "result": "clarify",
                    "workspace_id": web_session.workspace_id,
                    "input": text,
                    "parsed": parsed,
                }
            )
            return {
                "type": "custom_action_clarify",
                "message": assistant_message,
                "parsed": parsed,
            }

        if action_type not in {"custom_transformation", "custom_validation"}:
            return None

        confidence_raw = parsed.get("confidence")
        try:
            confidence = float(confidence_raw)
        except Exception:
            confidence = 0.0

        pending = {
            "action_type": action_type,
            "preview_text": str(parsed.get("preview_text") or "I parsed a custom action."),
            "confidence": confidence,
            "apply_payload": dict(parsed.get("apply_payload") or {}),
            "source_text": text,
            "supervisor_trace": supervisor_trace,
        }
        web_session.pending_custom_action = pending
        self._persist_operation_state(web_session)

        assistant_message = (
            f"Preview: {pending['preview_text']}\n"
            "Reply 'apply' to confirm or 'cancel' to discard."
        )
        self._append(web_session, role="assistant", content=assistant_message)
        session.record_chat_outcome(
            {
                "event": "custom_action_preview",
                "result": "pending_confirmation",
                "workspace_id": web_session.workspace_id,
                "action_type": action_type,
                "confidence": pending["confidence"],
                "apply_payload": pending["apply_payload"],
            }
        )
        return {
            "type": "custom_action_preview",
            "action_type": action_type,
            "preview": pending,
            "message": assistant_message,
        }

    def _queue_rationale_followup(
        self,
        web_session: WebSession,
        *,
        event: str,
        item_type: str,
        item_id: str,
        approved: Optional[bool],
        source: str,
    ) -> Optional[str]:
        if not self._should_capture_rationale(web_session):
            return None
        if source == "uncheck" and approved is not False:
            return None
        if source not in {"uncheck", "custom_action"}:
            return None

        prompt, supervisor_trace = self._dynamic_rationale_prompt(
            web_session,
            event=event,
            item_type=item_type,
            item_id=item_id,
            approved=approved,
            source=source,
        )
        web_session.pending_rationale = {
            "event": event,
            "item_type": item_type,
            "item_id": item_id,
            "approved": approved,
            "source": source,
            "question": prompt,
            "supervisor_trace": supervisor_trace,
        }
        self._persist_operation_state(web_session)
        self._append(web_session, role="assistant", content=prompt)
        return prompt

    @staticmethod
    def _is_skip_rationale_message(text: str) -> bool:
        normalized = " ".join(str(text or "").strip().lower().split())
        return normalized in {"", "skip", "n/a", "na", "none", "no reason", "prefer not to say"}

    def _should_capture_rationale(self, web_session: WebSession) -> bool:
        settings = web_session.session.settings
        return bool(
            settings.enable_rationale_capture
            and settings.enable_rationale_followup_question
        )

    def _consume_pending_rationale(self, web_session: WebSession, text: str) -> Optional[Dict[str, Any]]:
        pending = web_session.pending_rationale
        if not isinstance(pending, dict):
            return None

        web_session.pending_rationale = None
        self._persist_operation_state(web_session)
        if self._is_skip_rationale_message(text):
            skipped_rationale = web_session.session.normalize_rationale_payload(
                event=str(pending.get("event") or ""),
                item_type=str(pending.get("item_type") or ""),
                item_id=str(pending.get("item_id") or ""),
                approved=pending.get("approved"),
                rationale_text="",
                workspace_scope=self._scope_payload(web_session.scope),
                question_text=str(pending.get("question") or ""),
                response_type="skip",
                followup_status="skipped",
                source=str(pending.get("source") or "chat"),
                supervisor_trace=pending.get("supervisor_trace") if isinstance(pending.get("supervisor_trace"), dict) else {},
            )
            web_session.session._record_rationale_if_enabled(skipped_rationale)
            self.conversation_store.append_rationale(web_session.scope, skipped_rationale)

            assistant_message = "Got it — skipping rationale for this one."
            self._append(web_session, role="assistant", content=assistant_message)
            web_session.session.record_chat_outcome(
                {
                    "event": "rationale_capture",
                    "result": "skipped",
                    "workspace_id": web_session.workspace_id,
                    "item_type": pending.get("item_type"),
                    "item_id": pending.get("item_id"),
                    "source": pending.get("source"),
                }
            )
            return {
                "type": "rationale_skipped",
                "message": assistant_message,
                "context": pending,
            }

        response_type = "skip" if self._is_skip_rationale_message(text) else "answered"
        rationale = web_session.session.normalize_rationale_payload(
            event=str(pending.get("event") or ""),
            item_type=str(pending.get("item_type") or ""),
            item_id=str(pending.get("item_id") or ""),
            approved=pending.get("approved"),
            rationale_text=text,
            workspace_scope=self._scope_payload(web_session.scope),
            question_text=str(pending.get("question") or ""),
            response_type=response_type,
            followup_status="answered",
            source=str(pending.get("source") or "chat"),
            supervisor_trace=pending.get("supervisor_trace") if isinstance(pending.get("supervisor_trace"), dict) else {},
            impact_scope="item",
        )

        self.conversation_store.append_rationale(web_session.scope, rationale)
        self.conversation_store.append_decision(
            web_session.scope,
            {
                "event": "rationale_capture",
                "item_type": rationale.get("item_type"),
                "item_id": rationale.get("item_id"),
                "approved": rationale.get("approved"),
                "rationale": rationale,
            },
        )
        web_session.session.record_chat_outcome(
            {
                "event": "rationale_capture",
                "result": "ok",
                "workspace_id": web_session.workspace_id,
                "item_type": rationale.get("item_type"),
                "item_id": rationale.get("item_id"),
                "source": pending.get("source"),
            }
        )

        web_session.session._record_rationale_if_enabled(rationale)
        source = str(pending.get("source") or "")
        if source in {"uncheck", "custom_action"}:
            web_session.session._finalize_learning_episodes(
                item_type=str(rationale.get("item_type") or ""),
                item_id=str(rationale.get("item_id") or ""),
                approved=bool(rationale.get("approved")),
                rationale=rationale,
                rationale_only=True,
            )

        assistant_message = "Thanks — I saved that rationale for future suggestions."
        self._append(web_session, role="assistant", content=assistant_message)
        return {
            "type": "rationale_captured",
            "message": assistant_message,
            "rationale": rationale,
        }

    def _consume_pending_custom_action(self, web_session: WebSession, text: str) -> Optional[Dict[str, Any]]:
        pending = web_session.pending_custom_action
        if not isinstance(pending, dict):
            return None

        if is_cancel_message(text):
            web_session.pending_custom_action = None
            self._persist_operation_state(web_session)
            assistant_message = "Okay, I canceled that pending custom action."
            self._append(web_session, role="assistant", content=assistant_message)
            web_session.session.record_chat_outcome(
                {
                    "event": "custom_action_cancel",
                    "result": "canceled",
                    "workspace_id": web_session.workspace_id,
                    "action_type": pending.get("action_type"),
                    "apply_payload": pending.get("apply_payload"),
                }
            )
            return {
                "type": "custom_action_canceled",
                "message": assistant_message,
            }

        if not is_confirm_message(text):
            assistant_message = "You still have a pending custom action. Reply 'apply' to confirm or 'cancel' to discard."
            self._append(web_session, role="assistant", content=assistant_message)
            return {
                "type": "custom_action_pending",
                "pending": dict(pending),
                "message": assistant_message,
            }

        try:
            applied = self._apply_pending_custom_action(web_session)
        except Exception as exc:
            web_session.pending_custom_action = None
            self._persist_operation_state(web_session)
            assistant_message = f"I couldn't apply that custom action: {exc}"
            self._append(web_session, role="assistant", content=assistant_message)
            web_session.session.record_chat_outcome(
                {
                    "event": "custom_action_apply",
                    "result": "error",
                    "workspace_id": web_session.workspace_id,
                    "action_type": pending.get("action_type"),
                    "error": str(exc),
                }
            )
            return {
                "type": "custom_action_error",
                "message": assistant_message,
                "error": str(exc),
            }

        web_session.pending_custom_action = None
        self._persist_operation_state(web_session)
        item = applied.get("item") or {}
        item_type = "transformations"
        if str(applied.get("action_type") or "") == "custom_validation":
            item_type = "bq_validations"
        assistant_message = f"Applied. Added {applied.get('action_type')} item {item.get('id') or '(new item)'}."
        self._append(web_session, role="assistant", content=assistant_message)
        web_session.session.record_chat_outcome(
            {
                "event": "custom_action_apply",
                "result": "ok",
                "workspace_id": web_session.workspace_id,
                "action_type": applied.get("action_type"),
                "item_id": item.get("id"),
                "apply_payload": dict(pending.get("apply_payload") or {}),
            }
        )
        response = {
            "type": "custom_action_applied",
            "action_type": applied.get("action_type"),
            "item": item,
            "message": assistant_message,
        }
        followup = self._queue_rationale_followup(
            web_session,
            event="custom_action_apply",
            item_type=item_type,
            item_id=str(item.get("id") or ""),
            approved=True,
            source="custom_action",
        )
        if followup is not None:
            response["rationale_prompt"] = followup
        return response

    def _apply_pending_custom_action(self, web_session: WebSession) -> Dict[str, Any]:
        pending = web_session.pending_custom_action or {}
        payload = pending.get("apply_payload") if isinstance(pending.get("apply_payload"), dict) else {}
        action_type = str(pending.get("action_type") or "")
        session = web_session.session

        if action_type == "custom_transformation":
            name = str(payload.get("name") or "").strip()
            source_columns = [str(value).strip() for value in (payload.get("source_columns") or []) if str(value).strip()]
            target_fields = [str(value).strip() for value in (payload.get("target_fields") or []) if str(value).strip()]
            params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
            reason = str(payload.get("reason") or "NL custom transformation")
            if not name:
                raise ValueError("Missing transformation name")
            if not source_columns:
                raise ValueError("Missing transformation source column")

            item = session.add_transformation(
                name=name,
                source_columns=source_columns,
                target_fields=target_fields,
                params=params,
                reason=reason,
                learning_scope=None,
                capture_episode=True,
            )
            return {"action_type": action_type, "item": item}

        if action_type == "custom_validation":
            kind = str(payload.get("kind") or "").strip().lower()
            if kind == "required_field":
                target_field = str(payload.get("target_field") or "").strip()
                if not target_field:
                    raise ValueError("Missing target field for required validation")
                item = self._add_required_validation_for_field(session, target_field, learning_scope=None, capture_episode=True)
                return {"action_type": action_type, "item": item}

            if kind == "enum_values":
                target_field = str(payload.get("target_field") or "").strip()
                values = [str(value).strip() for value in (payload.get("values") or []) if str(value).strip()]
                if not target_field:
                    raise ValueError("Missing target field for enum validation")
                if not values:
                    raise ValueError("Missing allowed values for enum validation")

                item = self._add_enum_validation_for_field(
                    session,
                    target_field,
                    values,
                    learning_scope=None,
                    capture_episode=True,
                )
                return {"action_type": action_type, "item": item}

            raise ValueError("Unsupported custom validation payload")

        raise ValueError("Unsupported pending custom action type")

    @staticmethod
    def _suggestions_message(counts: Dict[str, int], review_summary: Dict[str, Any]) -> str:
        unchecked = int(review_summary.get("unchecked", 0) or 0)
        return (
            "Analysis complete. "
            f"I found {counts.get('mappings', 0)} mappings, {counts.get('transformations', 0)} transformations, "
            f"{counts.get('quality_audit', 0)} quality audit findings, and {counts.get('bq_validations', 0)} BigQuery validations. "
            f"{unchecked} item(s) still need review."
        )

    @staticmethod
    def _generation_message(mode: str, result: Dict[str, Any]) -> str:
        files = result.get("files") or []
        out_dir = result.get("output_dir") or "(unknown output dir)"
        if files:
            return f"Done. I generated {mode} artifacts in {out_dir}. Files: {', '.join(str(f) for f in files)}"
        return f"Done. I generated {mode} artifacts in {out_dir}."

    @staticmethod
    def _run_message(result: Dict[str, Any]) -> str:
        success = bool(result.get("success"))
        return_code = result.get("return_code")
        log_path = result.get("log_path")
        error_log_path = result.get("error_log_path")
        if success:
            return f"Pipeline run completed successfully (return code {return_code}). Log: {log_path}"
        return (
            f"Pipeline run failed (return code {return_code}). "
            f"Check logs: stdout={log_path}, stderr={error_log_path}"
        )

    @staticmethod
    def _unchecked_message(scope: str, payload: Any) -> str:
        if isinstance(payload, list):
            if not payload:
                return f"No unchecked {scope} items right now."
            ids = [str(item.get("id") or "(no id)") for item in payload[:12] if isinstance(item, dict)]
            extra = "" if len(payload) <= 12 else f" (+{len(payload) - 12} more)"
            return f"Unchecked {scope}: {', '.join(ids)}{extra}"
        if isinstance(payload, dict):
            parts = []
            for key in ["mappings", "transformations", "bq_validations", "quality_audit"]:
                values = payload.get(key) or []
                parts.append(f"{key}={len(values)}")
            return "Unchecked review items: " + ", ".join(parts)
        return "I couldn’t determine unchecked items from the current state."

    @staticmethod
    def _toggle_message(result: Dict[str, Any]) -> str:
        item_id = str(result.get("item_id") or "item")
        approved = bool(result.get("approved"))
        return f"Updated {item_id}: {'approved' if approved else 'rejected'}."

    @staticmethod
    def _profile_summary_message(summary: Dict[str, Any]) -> str:
        file_name = summary.get("file_name") or "(no file loaded)"
        roster = summary.get("roster_type_detected") or "unknown"
        column_count = int(summary.get("column_count") or 0)
        sample_size = int(summary.get("sample_size") or 0)
        return (
            f"Current roster profile: file={file_name}, type={roster}, "
            f"columns={column_count}, sampled_rows={sample_size}."
        )

    @staticmethod
    def _section_summary_message(section: str, items: Sequence[Dict[str, Any]]) -> str:
        if not items:
            return f"I don’t have any {section} suggestions yet."
        preview = []
        for item in list(items)[:5]:
            if section == "mappings":
                left = str(item.get("source_column") or "?")
                right = str(item.get("target_field") or "?")
                preview.append(f"{left}→{right}")
            elif section == "transformations":
                preview.append(str(item.get("name") or item.get("id") or "(unnamed)"))
            else:
                preview.append(str(item.get("name") or item.get("id") or "(unnamed)"))
        tail = "" if len(items) <= 5 else f" (+{len(items) - 5} more)"
        return f"{section.capitalize()} ({len(items)}): {', '.join(preview)}{tail}"

    @staticmethod
    def _status_message(
        stage: str,
        profile_summary: Dict[str, Any],
        review_summary: Dict[str, Any],
        next_actions: Sequence[str],
    ) -> str:
        file_name = profile_summary.get("file_name") or "none"
        total = int(review_summary.get("total") or 0)
        unchecked = int(review_summary.get("unchecked") or 0)
        return (
            f"Status: stage={stage}, file={file_name}, review_items={total}, unchecked={unchecked}.\n"
            "Next actions:\n- "
            + "\n- ".join(next_actions)
        )

    @staticmethod
    def _is_suggest_intent(lower: str) -> bool:
        if lower == "suggest" or lower.startswith("suggest "):
            return True
        if any(
            phrase in lower
            for phrase in [
                "run suggest",
                "run suggestions",
                "show suggestions",
                "suggest mappings",
                "recommend mappings",
                "what do you suggest",
            ]
        ):
            return True
        return any(word in lower for word in ["suggest", "recommend", "analyze", "analysis"]) and any(
            word in lower for word in ["mapping", "transform", "validation", "file", "plan", "roster", "schema"]
        )

    @staticmethod
    def _generate_mode_from_message(lower: str) -> Optional[str]:
        if lower.startswith("generate schema"):
            return "schema"
        if lower.startswith("generate processor"):
            return "processor"
        if lower.startswith("generate main"):
            return "main"

        if not any(verb in lower for verb in ["generate", "create", "build", "make"]):
            return None

        if "schema" in lower:
            return "schema"
        if "processor" in lower:
            return "processor"
        if "main.py" in lower or re.search(r"\bmain\b", lower):
            return "main"
        return None

    @staticmethod
    def _is_show_unchecked_intent(lower: str) -> bool:
        return lower.startswith("show unchecked") or "unchecked" in lower or "unreviewed" in lower

    @staticmethod
    def _unchecked_scope(lower: str) -> str:
        if "mapping" in lower:
            return "mappings"
        if "transform" in lower:
            return "transformations"
        if "quality audit" in lower or "quality" in lower or "audit" in lower:
            return "quality_audit"
        if "validation" in lower or "bq" in lower:
            return "bq_validations"
        return "all"

    @staticmethod
    def _is_status_intent(lower: str) -> bool:
        if lower == "status":
            return True
        return lower in {"state", "what's next", "whats next", "where am i"} or "session status" in lower or "workspace status" in lower

    @staticmethod
    def _review_sections_from_message(lower: str) -> List[str]:
        text = str(lower or "")
        wants_schema = "schema" in text or "profile" in text
        wants_mappings = "mapping" in text
        wants_transformations = "transform" in text or "transformation" in text
        wants_validations = "validation" in text or "bq" in text
        wants_quality = "quality" in text or "audit" in text
        cue = any(token in text for token in ["show", "summar", "what", "current", "list", "explain", "review", "and", "+", ","])

        sections: List[str] = []
        if wants_schema and cue:
            sections.append("schema")
        if wants_mappings and cue:
            sections.append("mappings")
        if wants_transformations and cue:
            sections.append("transformations")
        if wants_validations and cue:
            sections.append("bq_validations")
        if wants_quality and cue:
            sections.append("quality_audit")
        if len(sections) < 2:
            return []
        return sections

    @staticmethod
    def _is_summary_intent(lower: str) -> bool:
        if any(phrase in lower for phrase in ["schema summary", "profile summary", "show schema", "show profile"]):
            return True
        return ("schema" in lower or "profile" in lower) and any(
            word in lower for word in ["show", "summar", "what", "current", "view", "describe"]
        )

    @staticmethod
    def _is_explain_transformations_intent(lower: str) -> bool:
        if "transformation" not in lower and "transform" not in lower:
            return False
        return any(word in lower for word in ["explain", "show", "list", "why", "what"])

    @staticmethod
    def _is_explain_validations_intent(lower: str) -> bool:
        if "validation" not in lower and "bq" not in lower:
            return False
        return any(word in lower for word in ["explain", "show", "list", "why", "what"])

    @staticmethod
    def _is_explain_quality_audit_intent(lower: str) -> bool:
        if "quality" not in lower and "audit" not in lower:
            return False
        return any(word in lower for word in ["explain", "show", "list", "why", "what"])

    @staticmethod
    def _is_mappings_intent(lower: str) -> bool:
        if "mapping" not in lower:
            return False
        return any(word in lower for word in ["show", "list", "explain", "what", "why"])

    @staticmethod
    def _is_run_intent(lower: str) -> bool:
        return "run" in lower and "generated" in lower

    @staticmethod
    def _extract_run_generated_input_file(text: str, lower: str) -> Optional[str]:
        if not SessionStore._is_run_intent(lower):
            return None

        run_generated_match = re.search(r"\brun\b\s+\bgenerated\b", text, flags=re.IGNORECASE)
        if not run_generated_match:
            return SessionStore._input_file_from_message(text)

        remainder = text[run_generated_match.end() :].strip()
        if not remainder:
            return ""

        remainder = re.sub(r"^(?:with|using)\b\s*", "", remainder, flags=re.IGNORECASE).strip()
        if not remainder:
            return ""

        return SessionStore._input_file_from_message(remainder)

    @staticmethod
    def _input_file_from_message(text: str) -> str:
        stripped = text.strip()
        if not stripped:
            return ""

        quoted = re.search(r"['\"]([^'\"]+)['\"]", stripped)
        if quoted:
            return quoted.group(1).strip()

        path_like = re.search(
            r"((?:/|~/|\./|\.\./)[^\s'\"]+\.(?:csv|xlsx|xls|json|txt|md))",
            stripped,
            flags=re.IGNORECASE,
        )
        if path_like:
            return path_like.group(1).strip()
        return ""

    @staticmethod
    def _is_custom_action_intent(lower: str) -> bool:
        if not lower:
            return False
        actionable_patterns = [
            r"\badd\s+validation\b",
            r"\b(?:field|column)\s+[A-Za-z0-9_`\"'\.\-]+\s+(?:is\s+)?required\b",
            r"\bmust\s+be\s+one\s+of\b",
            r"\b(?:convert|map|change|replace|normalize|transform|transfrom)\b.+\b(?:to|into|as)\b",
        ]
        return any(re.search(pattern, lower, flags=re.IGNORECASE) for pattern in actionable_patterns)

    @staticmethod
    def _is_note_intent(text: str, lower: str) -> bool:
        return lower.startswith("note:") or lower.startswith("add note ") or lower.startswith("note ")

    @staticmethod
    def _extract_note_text(text: str) -> str:
        stripped = text.strip()
        if ":" in stripped and stripped.lower().startswith("note"):
            return stripped.split(":", 1)[1].strip()
        if stripped.lower().startswith("add note "):
            return stripped[len("add note ") :].strip()
        if stripped.lower().startswith("note "):
            return stripped[len("note ") :].strip()
        return ""

    @staticmethod
    def _infer_item_type(item_id: str) -> str:
        if item_id.startswith("map::"):
            return "mappings"
        if item_id.startswith("tx::"):
            return "transformations"
        if item_id.startswith("bq::"):
            return "bq_validations"
        if item_id.startswith("qa::"):
            return "quality_audit"
        raise ValueError("Item id must start with map::, tx::, bq::, or qa::")

    def _save_uploaded_file(self, session_id: str, filename: str, content: bytes) -> Path:
        clean_name = Path(filename or "").name.strip()
        if not clean_name:
            raise ValueError("Uploaded file must include a filename")

        suffix = Path(clean_name).suffix.lower()
        if suffix not in _ALLOWED_UPLOAD_EXTENSIONS:
            allowed = ", ".join(sorted(_ALLOWED_UPLOAD_EXTENSIONS))
            raise ValueError(f"Unsupported upload format: {suffix}. Allowed formats: {allowed}")
        if not content:
            raise ValueError("Uploaded file is empty")

        upload_dir = self.workspace_root / session_id / "uploads"
        upload_dir.mkdir(parents=True, exist_ok=True)

        target = upload_dir / clean_name
        if target.exists():
            target = upload_dir / f"{target.stem}_{uuid.uuid4().hex[:8]}{suffix}"
        target.write_bytes(content)
        return target

    def _save_note_attachment(self, session_id: str, filename: str, content: bytes) -> Path:
        clean_name = Path(filename or "").name.strip()
        if not clean_name:
            raise ValueError("Attachment must include a filename")

        suffix = Path(clean_name).suffix.lower()
        if suffix not in _ALLOWED_NOTE_EXTENSIONS:
            allowed = ", ".join(sorted(_ALLOWED_NOTE_EXTENSIONS))
            raise ValueError(f"Unsupported note attachment format: {suffix}. Allowed formats: {allowed}")
        if not content:
            raise ValueError("Attachment is empty")

        note_dir = self.workspace_root / session_id / "notes"
        note_dir.mkdir(parents=True, exist_ok=True)

        target = note_dir / clean_name
        if target.exists():
            target = note_dir / f"{target.stem}_{uuid.uuid4().hex[:8]}{suffix}"
        target.write_bytes(content)
        return target

    @staticmethod
    def _ingest_note_attachment(path: Path) -> Dict[str, Any]:
        suffix = path.suffix.lower()

        if suffix in {".txt", ".md"}:
            text = path.read_text(encoding="utf-8", errors="ignore")
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            preview = "\n".join(lines[:12])
            hints = [f"{path.name}: {line}" for line in lines[:8]]
            return {"preview": preview[:4000], "hints": hints}

        if suffix == ".json":
            raw = path.read_text(encoding="utf-8", errors="ignore")
            try:
                data = json.loads(raw)
            except Exception:
                return {"preview": raw[:4000], "hints": [f"{path.name}: invalid JSON; treated as plain text"]}

            if isinstance(data, dict):
                keys = sorted([str(key) for key in data.keys()])
                hints = [f"{path.name}: json keys={', '.join(keys[:12])}"]
                for key in keys[:8]:
                    value = data.get(key)
                    hints.append(f"{path.name}: {key}={str(value)[:120]}")
                return {"preview": json.dumps(data, indent=2, ensure_ascii=False)[:4000], "hints": hints}

            if isinstance(data, list):
                hints = [f"{path.name}: json list length={len(data)}"]
                if data:
                    hints.append(f"{path.name}: first_item={str(data[0])[:140]}")
                return {"preview": json.dumps(data[:10], indent=2, ensure_ascii=False)[:4000], "hints": hints}

            return {"preview": str(data)[:4000], "hints": [f"{path.name}: scalar json value ingested"]}

        if suffix == ".csv":
            hints: List[str] = []
            preview_rows: List[Dict[str, str]] = []
            with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
                reader = csv.DictReader(handle)
                fieldnames = list(reader.fieldnames or [])
                if fieldnames:
                    hints.append(f"{path.name}: columns={', '.join(fieldnames[:20])}")
                for idx, row in enumerate(reader):
                    if idx >= 8:
                        break
                    compact = {str(k): str(v)[:120] for k, v in row.items() if str(v).strip()}
                    preview_rows.append(compact)
                    if compact:
                        sample_parts = [f"{k}={v}" for k, v in list(compact.items())[:4]]
                        hints.append(f"{path.name}: row{idx + 1} {' | '.join(sample_parts)}")
            preview = json.dumps(preview_rows, indent=2, ensure_ascii=False)
            return {"preview": preview[:4000], "hints": hints[:12]}

        raw = path.read_text(encoding="utf-8", errors="ignore")
        return {"preview": raw[:4000], "hints": [f"{path.name}: attachment ingested as text"]}

    @staticmethod
    def _add_required_validation_for_field(
        session: UniversalRosterSession,
        target_field: str,
        *,
        learning_scope: Any = None,
        capture_episode: bool = False,
    ) -> Dict[str, Any]:
        if not session.state.plan:
            raise ValueError("No active plan")

        all_mappings = session.plan_manager.combined_items(session.state.plan, "mappings")
        selected = None
        for item in all_mappings:
            if str(item.get("target_field", "") or "").strip().lower() == target_field.lower():
                selected = item
                break

        if not selected:
            raise ValueError(f"No mapped field found for {target_field}")

        source = str(selected.get("source_column", "") or "").strip()
        name = f"required_{target_field}"
        sql = f"IFNULL(TRIM(CAST(`{source}` AS STRING)), '') = ''"
        message = f"{target_field} is required"
        item = session.add_bq_validation(
            name=name,
            sql_expression=sql,
            message=message,
            severity="error",
            source_column=source,
            target_field=target_field,
            runtime={"kind": "required"},
            learning_scope=learning_scope,
            capture_episode=capture_episode,
        )
        return item

    @staticmethod
    def _add_enum_validation_for_field(
        session: UniversalRosterSession,
        target_field: str,
        values: Sequence[str],
        *,
        learning_scope: Any = None,
        capture_episode: bool = False,
    ) -> Dict[str, Any]:
        if not session.state.plan:
            raise ValueError("No active plan")

        all_mappings = session.plan_manager.combined_items(session.state.plan, "mappings")
        selected = None
        for item in all_mappings:
            if str(item.get("target_field", "") or "").strip().lower() == target_field.lower():
                selected = item
                break

        if not selected:
            raise ValueError(f"No mapped field found for {target_field}")

        source = str(selected.get("source_column", "") or "").strip()
        cleaned = [str(value).strip() for value in values if str(value).strip()]
        if not cleaned:
            raise ValueError("Enum validation requires at least one allowed value")

        quoted = ", ".join(["'" + value.replace("'", "\\'") + "'" for value in cleaned])
        name = f"enum_{target_field}"
        sql = (
            f"IFNULL(TRIM(CAST(`{source}` AS STRING)), '') != '' "
            f"AND CAST(`{source}` AS STRING) NOT IN ({quoted})"
        )
        item = session.add_bq_validation(
            name=name,
            sql_expression=sql,
            message=f"{target_field} must be one of allowed values",
            severity="error",
            source_column=source,
            target_field=target_field,
            runtime={"kind": "enum", "values": cleaned},
            learning_scope=learning_scope,
            capture_episode=capture_episode,
        )
        return item
