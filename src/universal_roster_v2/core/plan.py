"""Canonical plan model and approval mechanics for standalone workflow."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_plan_section(item_type: str) -> str:
    normalized = str(item_type or "").strip().lower()
    if normalized in {"mapping", "mappings"}:
        return "mappings"
    if normalized in {"transform", "transformation", "transformations"}:
        return "transformations"
    if normalized in {"bq", "validation", "bq_validation", "bq_validations", "validations"}:
        return "bq_validations"
    if normalized in {"quality_audit", "quality_audits", "quality", "audit"}:
        return "quality_audit"
    raise ValueError("Unknown item_type")


@dataclass
class PlanValidationResult:
    ok: bool
    errors: List[str]


class PlanManager:
    """Create, mutate, validate, and persist chat-session plan artifacts."""

    def create_plan(
        self,
        source_profile: Dict[str, Any],
        roster_type: str,
        mappings: List[Dict[str, Any]],
        transformations: List[Dict[str, Any]],
        bq_validations: List[Dict[str, Any]],
        quality_audit: Optional[List[Dict[str, Any]]] = None,
        instructions_context: Optional[Dict[str, Any]] = None,
        workspace_scope: Optional[Dict[str, Any]] = None,
        collaboration_metadata: Optional[Dict[str, Any]] = None,
        column_audit_summary: Optional[Dict[str, Any]] = None,
        standardization_plan: Optional[Dict[str, Any]] = None,
        client_summary: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        quality_items = list(quality_audit or [])
        auto_approval = {
            "mappings": self._auto_approved_count(mappings),
            "transformations": self._auto_approved_count(transformations),
            "bq_validations": self._auto_approved_count(bq_validations),
            "quality_audit": self._auto_approved_count(quality_items),
        }

        return {
            "version": "2.1",
            "roster_type": roster_type,
            "input_fingerprint": deepcopy(source_profile.get("input_fingerprint", {})),
            "source_profile": {
                "file_name": source_profile.get("file_name", ""),
                "file_path": source_profile.get("file_path", ""),
                "file_type": source_profile.get("file_type", ""),
                "sheets": deepcopy(source_profile.get("sheets", [])),
                "columns": deepcopy(source_profile.get("columns", [])),
                "row_sample_size": int(source_profile.get("row_sample_size", 0) or 0),
                "profiling_mode": str(source_profile.get("profiling_mode", "sample") or "sample"),
                "rows_profiled": int(source_profile.get("rows_profiled", source_profile.get("row_sample_size", 0)) or 0),
                "rows_total": int(source_profile.get("rows_total", 0) or 0),
                "profile_max_rows": int(source_profile.get("profile_max_rows", 0) or 0),
                "semantic_profile": deepcopy(source_profile.get("semantic_profile", {})),
            },
            "workspace_scope": deepcopy(workspace_scope or {}),
            "instructions_context": deepcopy(instructions_context or {}),
            "collaboration": deepcopy(collaboration_metadata or {}),
            "mappings": deepcopy(mappings),
            "transformations": deepcopy(transformations),
            "bq_validations": deepcopy(bq_validations),
            "quality_audit": deepcopy(quality_items),
            "column_audit_summary": deepcopy(column_audit_summary or {}),
            "standardization_plan": deepcopy(standardization_plan or {}),
            "client_summary": deepcopy(client_summary or {}),
            "custom_user_items": {
                "mappings": [],
                "transformations": [],
                "bq_validations": [],
                "quality_audit": [],
            },
            "confidence_summary": {
                "mappings": self._confidence_summary(mappings),
                "transformations": self._confidence_summary(transformations),
                "bq_validations": self._confidence_summary(bq_validations),
                "quality_audit": self._confidence_summary(quality_items),
            },
            "auto_approval_summary": auto_approval,
            "audit_log": [
                {
                    "timestamp": _utc_now_iso(),
                    "action": "plan_created",
                    "details": {
                        "mapping_count": len(mappings),
                        "transformation_count": len(transformations),
                        "bq_validation_count": len(bq_validations),
                        "quality_audit_count": len(quality_items),
                        "auto_approval": auto_approval,
                    },
                }
            ],
            "approved": False,
        }

    @staticmethod
    def _auto_approved_count(items: List[Dict[str, Any]]) -> int:
        return len([item for item in items if isinstance(item, dict) and bool(item.get("approved", False))])

    @staticmethod
    def _confidence_summary(items: List[Dict[str, Any]]) -> Dict[str, Any]:
        summary = {"high": 0, "medium": 0, "low": 0, "unknown": 0}
        for item in items:
            if not isinstance(item, dict):
                continue
            band = str(item.get("confidence_band", "") or "").strip().lower()
            if band == "high":
                summary["high"] += 1
            elif band == "medium":
                summary["medium"] += 1
            elif band == "low":
                summary["low"] += 1
            else:
                summary["unknown"] += 1
        summary["total"] = sum(summary.values())
        return summary

    def validate_plan(self, plan: Dict[str, Any]) -> PlanValidationResult:
        errors: List[str] = []
        if not isinstance(plan, dict):
            return PlanValidationResult(ok=False, errors=["Plan must be an object"])

        if not str(plan.get("version", "")).strip():
            errors.append("Missing plan version")

        roster_type = str(plan.get("roster_type", "")).strip().lower()
        if roster_type not in {"practitioner", "facility"}:
            errors.append("roster_type must be practitioner or facility")

        fingerprint = plan.get("input_fingerprint") or {}
        if not isinstance(fingerprint, dict):
            errors.append("input_fingerprint must be an object")
        else:
            if not str(fingerprint.get("signature", "")).strip():
                errors.append("input_fingerprint.signature is required")

        for key in ["mappings", "transformations", "bq_validations", "quality_audit"]:
            if not isinstance(plan.get(key, []), list):
                errors.append(f"{key} must be a list")

        for key in ["column_audit_summary", "standardization_plan", "client_summary"]:
            value = plan.get(key, {})
            if value is not None and not isinstance(value, dict):
                errors.append(f"{key} must be an object")

        custom = plan.get("custom_user_items") or {}
        if not isinstance(custom, dict):
            errors.append("custom_user_items must be an object")

        workspace_scope = plan.get("workspace_scope") or {}
        if workspace_scope and not isinstance(workspace_scope, dict):
            errors.append("workspace_scope must be an object")

        return PlanValidationResult(ok=not errors, errors=errors)

    def ensure_fingerprint_match(self, plan: Dict[str, Any], profile: Dict[str, Any]) -> None:
        expected = str(((plan.get("input_fingerprint") or {}).get("signature", "")) or "").strip()
        actual = str(((profile.get("input_fingerprint") or {}).get("signature", "")) or "").strip()
        if not expected or not actual or expected != actual:
            raise ValueError("Input fingerprint mismatch: plan does not match current input profile")

    def _append_audit(self, plan: Dict[str, Any], action: str, details: Dict[str, Any]) -> None:
        log = plan.setdefault("audit_log", [])
        if not isinstance(log, list):
            plan["audit_log"] = []
            log = plan["audit_log"]
        log.append({"timestamp": _utc_now_iso(), "action": action, "details": details})

    def _section_keys(self, item_type: str) -> tuple[str, str]:
        section_key = normalize_plan_section(item_type)
        return section_key, section_key

    def set_item_approval(self, plan: Dict[str, Any], item_type: str, item_id: str, approved: bool) -> bool:
        base_key, custom_key = self._section_keys(item_type)

        updated = False
        for item in plan.get(base_key, []) or []:
            if isinstance(item, dict) and str(item.get("id", "")) == item_id:
                item["approved"] = bool(approved)
                updated = True
                break

        if not updated:
            for item in ((plan.get("custom_user_items") or {}).get(custom_key, []) or []):
                if isinstance(item, dict) and str(item.get("id", "")) == item_id:
                    item["approved"] = bool(approved)
                    updated = True
                    break

        if updated:
            self._append_audit(
                plan,
                "item_approval_updated",
                {"item_type": base_key, "item_id": item_id, "approved": bool(approved)},
            )

        return updated

    def add_custom_mapping(self, plan: Dict[str, Any], source_column: str, target_field: str, reason: str = "") -> Dict[str, Any]:
        item = {
            "id": f"map::custom::{source_column}",
            "source_column": source_column,
            "target_field": target_field,
            "confidence": 1.0,
            "confidence_band": "High",
            "approved": True,
            "reason": reason or "User-added mapping",
            "reason_evidence": {
                "matched_tokens": [source_column, target_field],
                "sample_pattern_evidence": {},
                "schema_metadata": {"target_field": target_field},
                "note_directives": [],
            },
            "suggested_by": "user",
            "schema_valid": True,
        }
        plan.setdefault("custom_user_items", {}).setdefault("mappings", []).append(item)
        self._append_audit(plan, "custom_mapping_added", {"source_column": source_column, "target_field": target_field})
        return item

    def add_custom_transformation(
        self,
        plan: Dict[str, Any],
        name: str,
        source_columns: List[str],
        target_fields: Optional[List[str]] = None,
        params: Optional[Dict[str, Any]] = None,
        reason: str = "",
    ) -> Dict[str, Any]:
        item = {
            "id": f"tx::custom::{name}",
            "name": name,
            "source_columns": source_columns,
            "target_fields": target_fields or [],
            "params": params or {},
            "approved": True,
            "confidence": 1.0,
            "confidence_band": "High",
            "reason": reason or "User-added transformation",
            "reason_evidence": {
                "matched_tokens": [name],
                "sample_pattern_evidence": {},
                "schema_metadata": {},
                "note_directives": [],
            },
            "priority": "high",
        }
        plan.setdefault("custom_user_items", {}).setdefault("transformations", []).append(item)
        self._append_audit(plan, "custom_transformation_added", {"name": name})
        return item

    def add_custom_bq_validation(
        self,
        plan: Dict[str, Any],
        name: str,
        sql_expression: str,
        message: str,
        severity: str = "error",
        source_column: str = "",
        target_field: str = "",
    ) -> Dict[str, Any]:
        item = {
            "id": f"bq::custom::{name}",
            "name": name,
            "source_column": source_column,
            "target_field": target_field,
            "severity": severity,
            "approved": True,
            "rule_type": "custom",
            "sql_expression": sql_expression,
            "message": message,
            "confidence": 1.0,
            "confidence_band": "High",
            "reason_evidence": {
                "matched_tokens": [name],
                "sample_pattern_evidence": {},
                "schema_metadata": {"target_field": target_field},
                "note_directives": [],
            },
        }
        plan.setdefault("custom_user_items", {}).setdefault("bq_validations", []).append(item)
        self._append_audit(plan, "custom_bq_validation_added", {"name": name, "severity": severity})
        return item

    def combined_items(self, plan: Dict[str, Any], item_type: str) -> List[Dict[str, Any]]:
        base_key, custom_key = self._section_keys(item_type)
        base = list(plan.get(base_key, []) or [])
        custom = list(((plan.get("custom_user_items") or {}).get(custom_key, []) or []))
        return [item for item in base + custom if isinstance(item, dict)]

    def approved_subset(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        out = deepcopy(plan)
        out["mappings"] = [i for i in self.combined_items(plan, "mappings") if bool(i.get("approved", False))]
        out["transformations"] = [i for i in self.combined_items(plan, "transformations") if bool(i.get("approved", False))]
        out["bq_validations"] = [i for i in self.combined_items(plan, "bq_validations") if bool(i.get("approved", False))]
        out["quality_audit"] = [i for i in self.combined_items(plan, "quality_audit") if bool(i.get("approved", False))]
        out["approved"] = bool(plan.get("approved", False))
        out["confidence_summary"] = {
            "mappings": self._confidence_summary(out.get("mappings", [])),
            "transformations": self._confidence_summary(out.get("transformations", [])),
            "bq_validations": self._confidence_summary(out.get("bq_validations", [])),
            "quality_audit": self._confidence_summary(out.get("quality_audit", [])),
        }
        return out

    def unchecked_counts(self, plan: Dict[str, Any]) -> Dict[str, int]:
        return {
            "mappings": len([i for i in self.combined_items(plan, "mappings") if not bool(i.get("approved", False))]),
            "transformations": len([i for i in self.combined_items(plan, "transformations") if not bool(i.get("approved", False))]),
            "bq_validations": len([i for i in self.combined_items(plan, "bq_validations") if not bool(i.get("approved", False))]),
            "quality_audit": len([i for i in self.combined_items(plan, "quality_audit") if not bool(i.get("approved", False))]),
        }

    def save_plan(self, plan: Dict[str, Any], path: str | Path) -> Path:
        target = Path(path).expanduser().resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8")
        return target

    def load_plan(self, path: str | Path) -> Dict[str, Any]:
        target = Path(path).expanduser().resolve()
        return json.loads(target.read_text(encoding="utf-8"))
