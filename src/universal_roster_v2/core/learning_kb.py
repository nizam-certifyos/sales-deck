"""Lightweight persistent learning KB for approvals, rejections, and chat outcomes."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from universal_roster_v2.config import Settings, get_settings


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _scope_prefix(scope: Optional[Dict[str, Any]]) -> str:
    if not scope:
        return ""
    workspace_signature = str(scope.get("workspace_signature", "") or "").strip().lower()
    tenant_id = str(scope.get("tenant_id", "") or "").strip().lower()
    client_id = str(scope.get("client_id", "") or "").strip().lower()
    if not workspace_signature and not tenant_id and not client_id:
        return ""
    return f"{workspace_signature}|{tenant_id}|{client_id}|"


class LearningKB:
    """JSON-backed KB for mapping/transform/validation feedback and chat outcomes."""

    VERSION = 5

    def __init__(self, path: str | Path | None = None, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        default_path = self.settings.learning_kb_path
        self.path = Path(path or default_path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _base_state() -> Dict[str, Any]:
        return {
            "version": LearningKB.VERSION,
            "updated_at": _utc_now_iso(),
            "mapping_feedback": {},
            "transformation_feedback": {},
            "validation_feedback": {},
            "quality_audit_feedback": {},
            "chat_outcomes": [],
            "rationales": [],
            "decision_events": [],
        }

    @staticmethod
    def _safe_int(value: Any) -> int:
        try:
            return int(value)
        except Exception:
            return 0

    @staticmethod
    def _normalize_counter_map(raw: Any) -> Dict[str, int]:
        if not isinstance(raw, dict):
            return {"approved": 0, "rejected": 0, "added": 0}
        return {
            "approved": LearningKB._safe_int(raw.get("approved", 0)),
            "rejected": LearningKB._safe_int(raw.get("rejected", 0)),
            "added": LearningKB._safe_int(raw.get("added", 0)),
        }

    @staticmethod
    def _normalize_rationale_record(raw: Any) -> Dict[str, Any]:
        if not isinstance(raw, dict):
            raw = {}
        tags = raw.get("rationale_tags") if isinstance(raw.get("rationale_tags"), list) else []
        followup = raw.get("followup") if isinstance(raw.get("followup"), dict) else {}
        decision = raw.get("decision") if isinstance(raw.get("decision"), dict) else {}
        supervisor = raw.get("supervisor") if isinstance(raw.get("supervisor"), dict) else {}
        workspace_scope = raw.get("workspace_scope") if isinstance(raw.get("workspace_scope"), dict) else {}
        return {
            "schema_version": max(3, LearningKB._safe_int(raw.get("schema_version", 3))),
            "event": str(raw.get("event") or ""),
            "item_type": str(raw.get("item_type") or ""),
            "item_id": str(raw.get("item_id") or ""),
            "section": str(raw.get("section") or ""),
            "approved": raw.get("approved"),
            "rationale_text": str(raw.get("rationale_text") or "").strip(),
            "rationale_tags": [str(tag).strip().lower() for tag in tags if str(tag).strip()],
            "workspace_scope": workspace_scope,
            "workspace_signature": str(raw.get("workspace_signature") or workspace_scope.get("workspace_signature") or ""),
            "tenant_id": str(raw.get("tenant_id") or workspace_scope.get("tenant_id") or ""),
            "client_id": str(raw.get("client_id") or workspace_scope.get("client_id") or ""),
            "thread_id": str(raw.get("thread_id") or workspace_scope.get("thread_id") or ""),
            "item_context": raw.get("item_context") if isinstance(raw.get("item_context"), dict) else {},
            "decision": {
                "approved": decision.get("approved", raw.get("approved")),
                "source": str(decision.get("source") or raw.get("source") or ""),
                "confidence": float(decision.get("confidence", 0.0) or 0.0),
            },
            "followup": {
                "question_text": str(followup.get("question_text") or raw.get("question_text") or ""),
                "response_type": str(followup.get("response_type") or raw.get("response_type") or ""),
                "status": str(followup.get("status") or raw.get("followup_status") or ""),
            },
            "supervisor": {
                "provider": str(supervisor.get("provider") or ""),
                "model": str(supervisor.get("model") or ""),
                "status": str(supervisor.get("status") or ""),
                "attempts": supervisor.get("attempts") if isinstance(supervisor.get("attempts"), list) else [],
            },
            "reason_category": str(raw.get("reason_category") or ""),
            "confidence_before": float(raw.get("confidence_before", 0.0) or 0.0),
            "confidence_after": float(raw.get("confidence_after", 0.0) or 0.0),
            "confidence_delta": float(raw.get("confidence_after", 0.0) or 0.0) - float(raw.get("confidence_before", 0.0) or 0.0),
            "impact_scope": str(raw.get("impact_scope") or "item"),
            "suggested_rule_change": raw.get("suggested_rule_change") if isinstance(raw.get("suggested_rule_change"), dict) else {},
            "provenance": str(raw.get("provenance") or "native_capture"),
            "timestamp": str(raw.get("timestamp") or _utc_now_iso()),
            "recorded_at": str(raw.get("recorded_at") or _utc_now_iso()),
        }

    @staticmethod
    def _legacy_to_scoped_migration(state: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(state, dict):
            return LearningKB._base_state()

        migrated = LearningKB._base_state()
        migrated["version"] = max(5, LearningKB._safe_int(state.get("version", 0)))
        migrated["updated_at"] = str(state.get("updated_at", _utc_now_iso()) or _utc_now_iso())

        for bucket_key in ["mapping_feedback", "transformation_feedback", "validation_feedback", "quality_audit_feedback"]:
            raw_bucket = state.get(bucket_key, {})
            if not isinstance(raw_bucket, dict):
                continue
            out_bucket: Dict[str, Dict[str, int]] = {}
            for key, counters in raw_bucket.items():
                key_str = str(key or "").strip()
                if not key_str:
                    continue
                scoped_key = key_str if key_str.count("|") >= 5 else f"global|||{key_str}"
                out_bucket[scoped_key] = LearningKB._normalize_counter_map(counters)
            migrated[bucket_key] = out_bucket

        chat_outcomes = state.get("chat_outcomes", [])
        if isinstance(chat_outcomes, list):
            migrated["chat_outcomes"] = [item for item in chat_outcomes if isinstance(item, dict)][-5000:]

        rationales = state.get("rationales", [])
        if isinstance(rationales, list):
            migrated["rationales"] = [
                LearningKB._normalize_rationale_record(item)
                for item in rationales
                if isinstance(item, dict)
            ][-5000:]

        decision_events = state.get("decision_events", [])
        if isinstance(decision_events, list):
            migrated["decision_events"] = [
                LearningKB._normalize_rationale_record(item)
                for item in decision_events
                if isinstance(item, dict)
            ][-5000:]
        elif migrated.get("rationales"):
            migrated["decision_events"] = list(migrated.get("rationales") or [])[-5000:]

        return migrated

    def load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return self._base_state()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return self._base_state()

        payload = self._legacy_to_scoped_migration(payload)

        out = self._base_state()
        out["version"] = self._safe_int(payload.get("version", self.VERSION)) or self.VERSION
        out["updated_at"] = str(payload.get("updated_at", _utc_now_iso()) or _utc_now_iso())

        mapping_feedback = payload.get("mapping_feedback", {}) if isinstance(payload, dict) else {}
        transform_feedback = payload.get("transformation_feedback", {}) if isinstance(payload, dict) else {}
        validation_feedback = payload.get("validation_feedback", {}) if isinstance(payload, dict) else {}
        quality_audit_feedback = payload.get("quality_audit_feedback", {}) if isinstance(payload, dict) else {}
        chat_outcomes = payload.get("chat_outcomes", []) if isinstance(payload, dict) else []
        rationales = payload.get("rationales", []) if isinstance(payload, dict) else []
        decision_events = payload.get("decision_events", []) if isinstance(payload, dict) else []

        if isinstance(mapping_feedback, dict):
            out["mapping_feedback"] = {
                str(k): self._normalize_counter_map(v)
                for k, v in mapping_feedback.items()
                if str(k).strip()
            }
        if isinstance(transform_feedback, dict):
            out["transformation_feedback"] = {
                str(k): self._normalize_counter_map(v)
                for k, v in transform_feedback.items()
                if str(k).strip()
            }
        if isinstance(validation_feedback, dict):
            out["validation_feedback"] = {
                str(k): self._normalize_counter_map(v)
                for k, v in validation_feedback.items()
                if str(k).strip()
            }
        if isinstance(quality_audit_feedback, dict):
            out["quality_audit_feedback"] = {
                str(k): self._normalize_counter_map(v)
                for k, v in quality_audit_feedback.items()
                if str(k).strip()
            }
        if isinstance(chat_outcomes, list):
            out["chat_outcomes"] = [item for item in chat_outcomes if isinstance(item, dict)][-5000:]
        if isinstance(rationales, list):
            out["rationales"] = [self._normalize_rationale_record(item) for item in rationales if isinstance(item, dict)][-5000:]
        if isinstance(decision_events, list):
            out["decision_events"] = [
                self._normalize_rationale_record(item)
                for item in decision_events
                if isinstance(item, dict)
            ][-5000:]
        elif out.get("rationales"):
            out["decision_events"] = list(out.get("rationales") or [])[-5000:]

        return out

    def save(self, payload: Dict[str, Any]) -> None:
        state = self._base_state()
        state["version"] = max(self.VERSION, self._safe_int(payload.get("version", self.VERSION)) or self.VERSION)
        state["updated_at"] = _utc_now_iso()
        state["mapping_feedback"] = {
            str(k): self._normalize_counter_map(v)
            for k, v in (payload.get("mapping_feedback", {}) or {}).items()
            if str(k).strip()
        }
        state["transformation_feedback"] = {
            str(k): self._normalize_counter_map(v)
            for k, v in (payload.get("transformation_feedback", {}) or {}).items()
            if str(k).strip()
        }
        state["validation_feedback"] = {
            str(k): self._normalize_counter_map(v)
            for k, v in (payload.get("validation_feedback", {}) or {}).items()
            if str(k).strip()
        }
        state["quality_audit_feedback"] = {
            str(k): self._normalize_counter_map(v)
            for k, v in (payload.get("quality_audit_feedback", {}) or {}).items()
            if str(k).strip()
        }
        state["chat_outcomes"] = [
            item for item in (payload.get("chat_outcomes", []) or []) if isinstance(item, dict)
        ][-5000:]
        state["rationales"] = [
            self._normalize_rationale_record(item)
            for item in (payload.get("rationales", []) or [])
            if isinstance(item, dict)
        ][-5000:]
        state["decision_events"] = [
            self._normalize_rationale_record(item)
            for item in (payload.get("decision_events", state.get("rationales", [])) or [])
            if isinstance(item, dict)
        ][-5000:]
        self.path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")

    @staticmethod
    def _mapping_key(roster_type: str, source_column: str, target_field: str, scope: Optional[Dict[str, Any]] = None) -> str:
        prefix = _scope_prefix(scope)
        return (
            f"{prefix}{str(roster_type).strip().lower()}|{str(source_column).strip().lower()}|"
            f"{str(target_field).strip()}"
        )

    @staticmethod
    def _transformation_key(
        roster_type: str,
        transform_name: str,
        source_column: str,
        target_field: str,
        scope: Optional[Dict[str, Any]] = None,
    ) -> str:
        prefix = _scope_prefix(scope)
        return (
            f"{prefix}{str(roster_type).strip().lower()}|{str(transform_name).strip().lower()}|"
            f"{str(source_column).strip().lower()}|{str(target_field).strip()}"
        )

    @staticmethod
    def _validation_key(
        roster_type: str,
        rule_type: str,
        source_column: str,
        target_field: str,
        scope: Optional[Dict[str, Any]] = None,
    ) -> str:
        prefix = _scope_prefix(scope)
        return (
            f"{prefix}{str(roster_type).strip().lower()}|{str(rule_type).strip().lower()}|"
            f"{str(source_column).strip().lower()}|{str(target_field).strip()}"
        )

    @staticmethod
    def _increment_counter(store: Dict[str, Any], key: str, action: str) -> None:
        if action not in {"approved", "rejected", "added"}:
            return
        bucket = store.setdefault(key, {"approved": 0, "rejected": 0, "added": 0})
        bucket[action] = int(bucket.get(action, 0) or 0) + 1

    def _lookup_with_fallback(self, store: Dict[str, Any], scoped_key: str, legacy_key: str) -> Dict[str, int]:
        if scoped_key in store:
            return self._normalize_counter_map(store.get(scoped_key))
        if legacy_key in store:
            return self._normalize_counter_map(store.get(legacy_key))
        global_legacy = f"global|||{legacy_key}"
        if global_legacy in store:
            return self._normalize_counter_map(store.get(global_legacy))
        return {"approved": 0, "rejected": 0, "added": 0}

    def get_mapping_feedback(
        self,
        roster_type: str,
        source_column: str,
        target_field: str,
        scope: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, int]:
        state = self.load()
        scoped_key = self._mapping_key(roster_type, source_column, target_field, scope=scope)
        legacy_key = self._mapping_key(roster_type, source_column, target_field, scope=None)
        return self._lookup_with_fallback(state.get("mapping_feedback", {}) or {}, scoped_key, legacy_key)

    def get_transformation_feedback(
        self,
        roster_type: str,
        transform_name: str,
        source_column: str,
        target_field: str,
        scope: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, int]:
        state = self.load()
        scoped_key = self._transformation_key(roster_type, transform_name, source_column, target_field, scope=scope)
        legacy_key = self._transformation_key(roster_type, transform_name, source_column, target_field, scope=None)
        return self._lookup_with_fallback(state.get("transformation_feedback", {}) or {}, scoped_key, legacy_key)

    def get_validation_feedback(
        self,
        roster_type: str,
        rule_type: str,
        source_column: str,
        target_field: str,
        scope: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, int]:
        state = self.load()
        scoped_key = self._validation_key(roster_type, rule_type, source_column, target_field, scope=scope)
        legacy_key = self._validation_key(roster_type, rule_type, source_column, target_field, scope=None)
        return self._lookup_with_fallback(state.get("validation_feedback", {}) or {}, scoped_key, legacy_key)

    def record_mapping_feedback(
        self,
        roster_type: str,
        source_column: str,
        target_field: str,
        action: str,
        scope: Optional[Dict[str, Any]] = None,
    ) -> None:
        state = self.load()
        key = self._mapping_key(roster_type, source_column, target_field, scope=scope)
        self._increment_counter(state.setdefault("mapping_feedback", {}), key, action)
        self.save(state)

    def record_transformation_feedback(
        self,
        roster_type: str,
        transform_name: str,
        source_column: str,
        target_field: str,
        action: str,
        scope: Optional[Dict[str, Any]] = None,
    ) -> None:
        state = self.load()
        key = self._transformation_key(roster_type, transform_name, source_column, target_field, scope=scope)
        self._increment_counter(state.setdefault("transformation_feedback", {}), key, action)
        self.save(state)

    def record_validation_feedback(
        self,
        roster_type: str,
        rule_type: str,
        source_column: str,
        target_field: str,
        action: str,
        scope: Optional[Dict[str, Any]] = None,
    ) -> None:
        state = self.load()
        key = self._validation_key(roster_type, rule_type, source_column, target_field, scope=scope)
        self._increment_counter(state.setdefault("validation_feedback", {}), key, action)
        self.save(state)

    def get_quality_audit_feedback(
        self,
        roster_type: str,
        rule_type: str,
        source_column: str,
        target_field: str,
        scope: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, int]:
        state = self.load()
        scoped_key = self._validation_key(roster_type, rule_type, source_column, target_field, scope=scope)
        legacy_key = self._validation_key(roster_type, rule_type, source_column, target_field, scope=None)
        quality_store = state.get("quality_audit_feedback", {}) or {}
        if not isinstance(quality_store, dict):
            quality_store = {}
        if quality_store:
            return self._lookup_with_fallback(quality_store, scoped_key, legacy_key)
        return self._lookup_with_fallback(state.get("validation_feedback", {}) or {}, scoped_key, legacy_key)

    def record_quality_audit_feedback(
        self,
        roster_type: str,
        rule_type: str,
        source_column: str,
        target_field: str,
        action: str,
        scope: Optional[Dict[str, Any]] = None,
    ) -> None:
        state = self.load()
        key = self._validation_key(roster_type, rule_type, source_column, target_field, scope=scope)
        self._increment_counter(state.setdefault("quality_audit_feedback", {}), key, action)
        self.save(state)

    def append_chat_outcome(self, record: Dict[str, Any]) -> None:
        payload = dict(record)
        payload.setdefault("timestamp", _utc_now_iso())
        state = self.load()
        outcomes = state.setdefault("chat_outcomes", [])
        outcomes.append(payload)
        state["chat_outcomes"] = outcomes[-5000:]
        self.save(state)

    def append_rationale(self, record: Dict[str, Any]) -> None:
        payload = self._normalize_rationale_record(record)
        state = self.load()
        rationales = state.setdefault("rationales", [])
        rationales.append(payload)
        state["rationales"] = [self._normalize_rationale_record(item) for item in rationales if isinstance(item, dict)][-5000:]
        decision_events = state.setdefault("decision_events", [])
        decision_events.append(payload)
        state["decision_events"] = [
            self._normalize_rationale_record(item) for item in decision_events if isinstance(item, dict)
        ][-5000:]
        self.save(state)

    def append_decision_event(self, record: Dict[str, Any]) -> None:
        payload = self._normalize_rationale_record(record)
        state = self.load()
        decision_events = state.setdefault("decision_events", [])
        decision_events.append(payload)
        state["decision_events"] = [
            self._normalize_rationale_record(item) for item in decision_events if isinstance(item, dict)
        ][-5000:]
        self.save(state)

    def get_chat_outcomes(self, limit: int = 200) -> List[Dict[str, Any]]:
        state = self.load()
        outcomes = [item for item in (state.get("chat_outcomes") or []) if isinstance(item, dict)]
        try:
            safe_limit = int(limit)
        except Exception:
            safe_limit = 200
        if safe_limit <= 0:
            return []
        recent = outcomes[-safe_limit:]
        recent.reverse()
        return recent

    def get_rationales(self, limit: int = 200) -> List[Dict[str, Any]]:
        state = self.load()
        rationales = [self._normalize_rationale_record(item) for item in (state.get("rationales") or []) if isinstance(item, dict)]
        try:
            safe_limit = int(limit)
        except Exception:
            safe_limit = 200
        if safe_limit <= 0:
            return []
        recent = rationales[-safe_limit:]
        recent.reverse()
        return recent

    def get_decision_events(self, limit: int = 200) -> List[Dict[str, Any]]:
        state = self.load()
        events = [
            self._normalize_rationale_record(item)
            for item in (state.get("decision_events") or state.get("rationales") or [])
            if isinstance(item, dict)
        ]
        try:
            safe_limit = int(limit)
        except Exception:
            safe_limit = 200
        if safe_limit <= 0:
            return []
        recent = events[-safe_limit:]
        recent.reverse()
        return recent
