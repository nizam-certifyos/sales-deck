"""Stateful workspace-session orchestrator for standalone Universal Roster V2."""

from __future__ import annotations

import json
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from universal_roster_v2.config import Settings, get_settings
from universal_roster_v2.core.conversation_store import ConversationScope, scope_signature
from universal_roster_v2.core.codegen import CodeGenerator
from universal_roster_v2.core.learning_episodes import LearningEpisodeStore
from universal_roster_v2.core.learning_kb import LearningKB
from universal_roster_v2.core.learning_retrieval import LearningRetrieval
from universal_roster_v2.core.mapping import MappingEngine
from universal_roster_v2.core.plan import PlanManager, normalize_plan_section
from universal_roster_v2.core.profile import profile_input, sample_values_by_column
from universal_roster_v2.core.schema import SchemaRegistry
from universal_roster_v2.core.trainer import TrainerOrchestrator
from universal_roster_v2.core.training_export import TrainingExportService
from universal_roster_v2.core.transforms import suggest_transformations
from universal_roster_v2.core.validations import suggest_bq_validations
from universal_roster_v2.core.quality_audit import suggest_quality_audit
from universal_roster_v2.llm.router import LLMRouterFactory


_DEFAULT_LEARNING_SCOPE = object()


class SessionState:
    def __init__(self):
        self.profile: Optional[Dict[str, Any]] = None
        self.plan: Optional[Dict[str, Any]] = None
        self.plan_path: Optional[Path] = None
        self.instructions_context: Dict[str, Any] = {
            "free_text_notes": [],
            "client_rules": [],
            "schema_caveats": [],
            "exceptions": [],
            "attachment_hints": [],
            "attachments": [],
        }
        self.workspace_scope: Dict[str, Any] = {
            "workspace_signature": "",
            "tenant_id": "default-tenant",
            "client_id": "default-client",
            "thread_id": "default",
            "workspace_path": "",
        }


class UniversalRosterSession:
    """Orchestrates profiling, suggestions, review toggles, and generation."""

    def __init__(
        self,
        workspace_dir: str | Path | None = None,
        settings: Optional[Settings] = None,
        workspace_scope: Optional[Dict[str, Any]] = None,
    ):
        self.settings = settings or get_settings()
        self.workspace_dir = Path(workspace_dir or self.settings.workspace_dir).expanduser().resolve()
        self.workspace_dir.mkdir(parents=True, exist_ok=True)

        self.state = SessionState()
        self.schema_registry = SchemaRegistry()
        self.learning_kb = LearningKB(settings=self.settings)
        self.learning_episodes = LearningEpisodeStore(settings=self.settings)
        self.learning_retrieval = LearningRetrieval(
            episode_store=self.learning_episodes,
            learning_kb=self.learning_kb,
            settings=self.settings,
        )
        self.mapping_engine = MappingEngine(
            schema_registry=self.schema_registry,
            settings=self.settings,
            learning_kb=self.learning_kb,
            learning_retrieval=self.learning_retrieval,
        )
        self.training_exporter = TrainingExportService(
            episode_store=self.learning_episodes,
            settings=self.settings,
        )
        self.trainer = TrainerOrchestrator(settings=self.settings)
        self.plan_manager = PlanManager()
        self.codegen = CodeGenerator(plan_manager=self.plan_manager)
        self.router_factory = LLMRouterFactory(settings=self.settings)

        self.state.workspace_scope = {
            "workspace_signature": str((workspace_scope or {}).get("workspace_signature", "") or ""),
            "tenant_id": str((workspace_scope or {}).get("tenant_id", "default-tenant") or "default-tenant"),
            "client_id": str((workspace_scope or {}).get("client_id", "default-client") or "default-client"),
            "thread_id": str((workspace_scope or {}).get("thread_id", "default") or "default"),
            "workspace_path": str((workspace_scope or {}).get("workspace_path", str(self.workspace_dir)) or str(self.workspace_dir)),
        }
        self.state.workspace_scope["workspace_signature"] = self._workspace_signature(self.state.workspace_scope)
        self._last_quality_gate_metrics: Dict[str, Dict[str, float]] = {}
        self._last_quality_gate_stage: Dict[str, str] = {}

    @staticmethod
    def _workspace_signature(scope: Dict[str, Any]) -> str:
        tenant = str((scope or {}).get("tenant_id") or "").strip().lower()
        client = str((scope or {}).get("client_id") or "").strip().lower()
        workspace_path = str((scope or {}).get("workspace_path") or "").strip().lower()
        return "|".join([tenant, client, workspace_path])

    @staticmethod
    def _utc_now_iso() -> str:
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _normalize_yes_no(value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        text = str(value or "").strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n"}:
            return False
        return None

    @staticmethod
    def _scope_signature_from_mapping(scope_mapping: Optional[Dict[str, Any]]) -> str:
        scope_mapping = scope_mapping or {}
        scope = ConversationScope(
            workspace_path=str(scope_mapping.get("workspace_path") or ""),
            tenant_id=str(scope_mapping.get("tenant_id") or ""),
            client_id=str(scope_mapping.get("client_id") or ""),
            thread_id=str(scope_mapping.get("thread_id") or ""),
        )
        return scope_signature(scope)

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return default

    def _item_for_id(self, item_type: str, item_id: str) -> Optional[Dict[str, Any]]:
        if not self.state.plan:
            return None
        try:
            section = normalize_plan_section(item_type)
        except Exception:
            section = str(item_type or "")
        for item in self.plan_manager.combined_items(self.state.plan, section):
            if str(item.get("id") or "") == str(item_id or ""):
                return item
        return None

    def _normalize_supervisor_trace(self, payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        trace = payload if isinstance(payload, dict) else {}
        provider = str(trace.get("provider") or "").strip().lower()
        model = str(trace.get("model") or "").strip()
        status = str(trace.get("status") or "").strip().lower()
        attempts = trace.get("attempts") if isinstance(trace.get("attempts"), list) else []
        return {
            "provider": provider,
            "model": model,
            "status": status,
            "attempts": [str(item).strip() for item in attempts if str(item).strip()],
        }

    def _safe_rate(self, numerator: float, denominator: float) -> float:
        if denominator <= 0:
            return 0.0
        return max(0.0, min(1.0, float(numerator) / float(denominator)))

    def _section_stage_policy(self, section: str, metrics: Dict[str, float]) -> Dict[str, Any]:
        key = self.settings.normalize_section_key(section)
        accepted = float(metrics.get("accepted", 0.0) or 0.0)
        reviewed = float(metrics.get("reviewed", 0.0) or 0.0)
        acceptance_rate = self._safe_rate(accepted, reviewed)

        gate = self.settings.qwen_quality_gate_for_section(key)
        min_accepted = float(gate.get("min_accepted", 0.0))
        min_rate = float(gate.get("min_acceptance_rate", 0.0))

        contradiction_rate = self._safe_rate(
            float(metrics.get("contradictions", 0.0) or 0.0),
            reviewed,
        )
        clarification_rate = self._safe_rate(
            float(metrics.get("clarification_unresolved", 0.0) or 0.0),
            reviewed,
        )

        rollback_reasons: List[str] = []
        if accepted < min_accepted:
            rollback_reasons.append("accepted_below_gate")
        if acceptance_rate < min_rate:
            rollback_reasons.append("acceptance_rate_below_gate")
        if contradiction_rate > 0.25:
            rollback_reasons.append("contradiction_rate_high")
        if clarification_rate > 0.2:
            rollback_reasons.append("clarification_spike")

        stage = "supervised"
        if self.settings.enable_qwen_stage_promotion:
            if accepted >= min_accepted and acceptance_rate >= min_rate:
                stage = "mixed"
            if (
                stage == "mixed"
                and accepted >= (min_accepted * 1.5)
                and acceptance_rate >= max(min_rate, 0.85)
                and contradiction_rate <= 0.1
                and clarification_rate <= 0.1
            ):
                stage = "qwen_only_candidate"

        if rollback_reasons:
            stage = "supervised"

        return {
            "stage": stage,
            "accepted": accepted,
            "reviewed": reviewed,
            "acceptance_rate": acceptance_rate,
            "min_accepted": min_accepted,
            "min_acceptance_rate": min_rate,
            "contradiction_rate": contradiction_rate,
            "clarification_rate": clarification_rate,
            "rollback": bool(rollback_reasons),
            "rollback_reasons": rollback_reasons,
        }

    def set_workspace_scope(self, scope: Dict[str, Any]) -> None:
        merged = dict(self.state.workspace_scope)
        merged.update({k: v for k, v in (scope or {}).items() if v not in [None, ""]})
        merged["workspace_signature"] = self._workspace_signature(merged)
        self.state.workspace_scope = merged

    def set_instructions_context(self, context: Dict[str, Any]) -> None:
        if not isinstance(context, dict):
            return
        defaults = {
            "free_text_notes": [],
            "client_rules": [],
            "schema_caveats": [],
            "exceptions": [],
            "attachment_hints": [],
            "attachments": [],
        }
        merged = dict(defaults)
        merged.update(self.state.instructions_context)
        for key in defaults.keys():
            value = context.get(key)
            if value is None:
                continue
            if not isinstance(value, list):
                value = [value]
            cleaned = [item for item in value if item not in [None, ""]]
            if cleaned:
                merged[key] = cleaned
        self.state.instructions_context = merged

    def update_instructions_context(self, context_updates: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(self.state.instructions_context)
        for key in ["free_text_notes", "client_rules", "schema_caveats", "exceptions", "attachment_hints", "attachments"]:
            incoming = context_updates.get(key)
            if incoming is None:
                continue
            if not isinstance(incoming, list):
                incoming = [incoming]
            cleaned = [item for item in incoming if item not in [None, ""]]
            if not cleaned:
                continue
            bucket = merged.setdefault(key, [])
            for item in cleaned:
                if item not in bucket:
                    bucket.append(item)
        self.state.instructions_context = merged
        return merged

    def load_file(
        self,
        file_path: str,
        roster_type: Optional[str] = None,
        profile_full_roster_learning: Optional[bool] = None,
        profile_max_rows: Optional[int] = None,
        progress_callback: Optional[Callable[..., None]] = None,
    ) -> Dict[str, Any]:
        full_learning = (
            self.settings.profile_full_roster_learning
            if profile_full_roster_learning is None
            else bool(profile_full_roster_learning)
        )
        max_rows = self.settings.profile_max_rows if profile_max_rows is None else max(0, int(profile_max_rows or 0))

        if progress_callback:
            progress_callback("profiling_started", "Profiling input roster", 10)
        profile = profile_input(
            file_path,
            sample_rows=500,
            full_roster_learning=full_learning,
            profile_max_rows=max_rows,
        )
        if roster_type:
            profile["roster_type_detected"] = roster_type
        self.state.profile = profile
        self.state.plan = None
        if progress_callback:
            progress_callback("profiling_completed", "Profiling complete", 20)
        return profile

    def suggest(
        self,
        use_llm_for_unresolved: bool = True,
        progress_callback: Optional[Callable[..., None]] = None,
    ) -> Dict[str, Any]:
        if not self.state.profile:
            raise ValueError("Load a file first")

        self._sync_router_quality_gate_metrics()
        profile = self.state.profile
        roster_type = str(profile.get("roster_type_detected", "practitioner"))
        samples = sample_values_by_column(profile)
        semantic_profile = profile.get("semantic_profile") if self.settings.advanced_profile_inference else {}

        mapping_router = self.router_factory.for_task("mappings")
        transform_router = self.router_factory.for_task("transformations")
        validation_router = self.router_factory.for_task("validations")
        quality_audit_router = self.router_factory.for_task("quality_audit")
        verifier_router = self.router_factory.for_task("verifier")

        self.mapping_engine.primary_router = mapping_router
        self.mapping_engine.verifier_router = verifier_router

        if progress_callback:
            progress_callback("mappings_started", "Generating mapping suggestions", 30)
        mapping_result = self.mapping_engine.suggest_mappings(
            columns=list(profile.get("columns", [])),
            sample_values=samples,
            roster_type=roster_type,
            use_llm_for_unresolved=use_llm_for_unresolved,
            instructions_context=self.state.instructions_context,
            semantic_profile=semantic_profile,
            learning_scope=self.state.workspace_scope,
        )
        mappings = mapping_result["mappings"]

        if progress_callback:
            progress_callback("mappings_completed", f"Generated {len(mappings)} mappings", 45)
            progress_callback("parallel_started", "Generating transforms, validations & quality audit in parallel", 50)

        # Run transforms, validations, and quality audit in PARALLEL
        # (they all depend on mappings but NOT on each other)
        transform_result = {}
        validation_result = {}
        quality_audit_result = {}

        _demo = bool(getattr(self.settings, "demo_mode", False))

        def _run_transforms():
            return suggest_transformations(
                mappings=mappings,
                schema_registry=self.schema_registry,
                roster_type=roster_type,
                sample_values=samples,
                learning_kb=self.learning_kb,
                learning_retrieval=self.learning_retrieval,
                instructions_context=self.state.instructions_context,
                learning_scope=self.state.workspace_scope,
                settings=self.settings,
                primary_router=transform_router,
                verifier_router=verifier_router,
                collaboration_mode=self.settings.collaboration_mode,
                demo_mode=_demo,
            )

        def _run_validations():
            return suggest_bq_validations(
                mappings=mappings,
                schema_registry=self.schema_registry,
                roster_type=roster_type,
                sample_values=samples,
                learning_kb=self.learning_kb,
                learning_retrieval=self.learning_retrieval,
                instructions_context=self.state.instructions_context,
                learning_scope=self.state.workspace_scope,
                settings=self.settings,
                primary_router=validation_router,
                verifier_router=verifier_router,
                collaboration_mode=self.settings.collaboration_mode,
                demo_mode=_demo,
            )

        def _run_quality_audit():
            return suggest_quality_audit(
                profile=profile,
                mappings=mappings,
                instructions_context=self.state.instructions_context,
                settings=self.settings,
                learning_kb=self.learning_kb,
                learning_retrieval=self.learning_retrieval,
                learning_scope=self.state.workspace_scope,
                roster_type=roster_type,
                primary_router=quality_audit_router,
                verifier_router=verifier_router,
                collaboration_mode=self.settings.collaboration_mode,
                demo_mode=_demo,
            )

        with ThreadPoolExecutor(max_workers=3) as executor:
            future_transforms = executor.submit(_run_transforms)
            future_validations = executor.submit(_run_validations)
            future_quality_audit = executor.submit(_run_quality_audit)

            for future in as_completed([future_transforms, future_validations, future_quality_audit]):
                try:
                    result = future.result()
                    if future is future_transforms:
                        transform_result = result
                        transformations_count = len(result.get("transformations", []))
                        if progress_callback:
                            progress_callback("transformations_completed", f"Generated {transformations_count} transformations", 65)
                    elif future is future_validations:
                        validation_result = result
                        validations_count = len(result.get("bq_validations", []))
                        if progress_callback:
                            progress_callback("validations_completed", f"Generated {validations_count} validations", 75)
                    elif future is future_quality_audit:
                        quality_audit_result = result
                        qa_count = len(result.get("quality_audit", []))
                        if progress_callback:
                            progress_callback("quality_audit_completed", f"Generated {qa_count} quality audit findings", 85)
                except Exception as exc:
                    if future is future_transforms:
                        transform_result = {"transformations": [], "llm_trace": {"error": str(exc)}}
                    elif future is future_validations:
                        validation_result = {"bq_validations": [], "llm_trace": {"error": str(exc)}}
                    elif future is future_quality_audit:
                        quality_audit_result = {"quality_audit": [], "llm_trace": {"error": str(exc)}}

        transformations = transform_result.get("transformations", [])
        bq_validations = validation_result.get("bq_validations", [])
        quality_audit = quality_audit_result.get("quality_audit", [])

        llm_trace = {
            "policy": {
                "mode": self.settings.collaboration_mode,
                "strict": self.settings.is_strict_collaboration(),
                "require_claude_verifier": {
                    "mappings": self.settings.require_claude_verifier_for_section("mappings"),
                    "transformations": self.settings.require_claude_verifier_for_section("transformations"),
                    "validations": self.settings.require_claude_verifier_for_section("validations"),
                    "quality_audit": self.settings.require_claude_verifier_for_section("quality_audit"),
                },
            },
            "mappings": mapping_result.get("llm_trace", {}),
            "transformations": transform_result.get("llm_trace", {}),
            "validations": validation_result.get("llm_trace", {}),
            "quality_audit": quality_audit_result.get("llm_trace", {}),
        }

        self._enforce_section_policy(section="mappings", trace=llm_trace.get("mappings", {}))
        self._enforce_section_policy(section="transformations", trace=llm_trace.get("transformations", {}))
        self._enforce_section_policy(section="validations", trace=llm_trace.get("validations", {}))
        self._enforce_section_policy(section="quality_audit", trace=llm_trace.get("quality_audit", {}))

        if not _demo:
            self._capture_learning_episodes(
                section="mappings",
                items=mappings,
                llm_trace=llm_trace.get("mappings", {}),
                roster_type=roster_type,
                fingerprint=str((profile.get("input_fingerprint") or {}).get("signature") or ""),
            )
            self._capture_learning_episodes(
                section="transformations",
                items=transformations,
                llm_trace=llm_trace.get("transformations", {}),
                roster_type=roster_type,
                fingerprint=str((profile.get("input_fingerprint") or {}).get("signature") or ""),
            )
            self._capture_learning_episodes(
                section="validations",
                items=bq_validations,
                llm_trace=llm_trace.get("validations", {}),
                roster_type=roster_type,
                fingerprint=str((profile.get("input_fingerprint") or {}).get("signature") or ""),
            )
            self._capture_learning_episodes(
                section="quality_audit",
                items=quality_audit,
                llm_trace=llm_trace.get("quality_audit", {}),
                roster_type=roster_type,
                fingerprint=str((profile.get("input_fingerprint") or {}).get("signature") or ""),
            )

        plan = self.plan_manager.create_plan(
            source_profile=profile,
            roster_type=roster_type,
            mappings=mappings,
            transformations=transformations,
            bq_validations=bq_validations,
            quality_audit=quality_audit,
            instructions_context=self.state.instructions_context,
            workspace_scope=self.state.workspace_scope,
            collaboration_metadata={
                "policy": llm_trace.get("policy", {}),
                "providers": {
                    "mappings": mapping_router.provider_names(),
                    "transformations": transform_router.provider_names(),
                    "validations": validation_router.provider_names(),
                    "quality_audit": quality_audit_router.provider_names(),
                    "verifier": verifier_router.provider_names(),
                },
            },
        )
        plan["llm_trace"] = llm_trace

        self.state.plan = plan
        default_name = Path(str(profile.get("file_name", "roster"))).stem + "_universal_plan.json"
        self.state.plan_path = self.workspace_dir / default_name
        plan["_plan_path"] = str(self.state.plan_path)
        self.plan_manager.save_plan(plan, self.state.plan_path)
        if progress_callback:
            progress_callback("analysis_completed", "Suggestion plan ready", 100)
        return plan

    def save_plan(self, path: Optional[str] = None) -> Path:
        if not self.state.plan:
            raise ValueError("No active plan")
        target = Path(path).expanduser().resolve() if path else (self.state.plan_path or (self.workspace_dir / "plan.json"))
        self.state.plan_path = target
        return self.plan_manager.save_plan(self.state.plan, target)

    def load_plan(self, path: str) -> Dict[str, Any]:
        plan = self.plan_manager.load_plan(path)
        validation = self.plan_manager.validate_plan(plan)
        if not validation.ok:
            raise ValueError("Invalid plan: " + "; ".join(validation.errors))
        self.state.plan = plan
        self.state.plan_path = Path(path).expanduser().resolve()
        if isinstance(plan.get("instructions_context"), dict):
            self.set_instructions_context(plan["instructions_context"])
        if isinstance(plan.get("workspace_scope"), dict):
            self.set_workspace_scope(plan["workspace_scope"])
        return plan

    def approve_plan(self, approved: bool = True) -> Dict[str, Any]:
        if not self.state.plan:
            raise ValueError("No active plan")
        self.state.plan["approved"] = bool(approved)
        self.plan_manager._append_audit(self.state.plan, "plan_approval_updated", {"approved": bool(approved)})
        self.save_plan()
        return self.state.plan

    def set_item_approval(
        self,
        item_type: str,
        item_id: str,
        approved: bool,
        rationale: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if not self.state.plan:
            raise ValueError("No active plan")
        ok = self.plan_manager.set_item_approval(self.state.plan, item_type, item_id, approved)
        if ok:
            self._record_item_feedback(item_type=item_type, item_id=item_id, approved=approved, rationale=rationale)
            self._finalize_learning_episodes(item_type=item_type, item_id=item_id, approved=approved, rationale=rationale)
            self.save_plan()
        return ok

    def add_mapping(self, source_column: str, target_field: str, reason: str = "") -> Dict[str, Any]:
        if not self.state.plan:
            raise ValueError("No active plan")
        roster_type = str(self.state.plan.get("roster_type", "practitioner"))
        if not self.schema_registry.is_valid_field(target_field, roster_type):
            raise ValueError(f"Invalid schema target for {roster_type}: {target_field}")
        item = self.plan_manager.add_custom_mapping(self.state.plan, source_column, target_field, reason)
        self._record_mapping_addition(source_column=source_column, target_field=target_field)
        self.save_plan()
        return item

    def add_transformation(
        self,
        name: str,
        source_columns: List[str],
        target_fields: Optional[List[str]] = None,
        params: Optional[Dict[str, Any]] = None,
        reason: str = "",
        *,
        learning_scope: Any = _DEFAULT_LEARNING_SCOPE,
        capture_episode: bool = False,
        rationale: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not self.state.plan:
            raise ValueError("No active plan")
        item = self.plan_manager.add_custom_transformation(
            self.state.plan,
            name=name,
            source_columns=source_columns,
            target_fields=target_fields,
            params=params,
            reason=reason,
        )
        resolved_scope = self._resolve_learning_scope(learning_scope)
        self._record_transformation_addition(item, scope=resolved_scope, rationale=rationale)
        if capture_episode:
            self._capture_custom_add_episode("transformations", item, rationale=rationale)
        self.save_plan()
        return item

    def add_bq_validation(
        self,
        name: str,
        sql_expression: str,
        message: str,
        severity: str = "error",
        source_column: str = "",
        target_field: str = "",
        runtime: Optional[Dict[str, Any]] = None,
        *,
        learning_scope: Any = _DEFAULT_LEARNING_SCOPE,
        capture_episode: bool = False,
        rationale: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not self.state.plan:
            raise ValueError("No active plan")
        item = self.plan_manager.add_custom_bq_validation(
            self.state.plan,
            name=name,
            sql_expression=sql_expression,
            message=message,
            severity=severity,
            source_column=source_column,
            target_field=target_field,
        )
        if runtime:
            item["runtime"] = runtime
        resolved_scope = self._resolve_learning_scope(learning_scope)
        self._record_validation_addition(item, scope=resolved_scope, rationale=rationale)
        if capture_episode:
            self._capture_custom_add_episode("validations", item, rationale=rationale)
        self.save_plan()
        return item

    def explain_item(self, item_id: str) -> Optional[Dict[str, Any]]:
        if not self.state.plan:
            raise ValueError("No active plan")
        for section in ["mappings", "transformations", "bq_validations", "quality_audit"]:
            for item in self.plan_manager.combined_items(self.state.plan, section):
                if str(item.get("id", "")) == item_id:
                    return {"section": section, "item": item}
        return None

    def list_unchecked(self) -> Dict[str, List[Dict[str, Any]]]:
        if not self.state.plan:
            raise ValueError("No active plan")
        return {
            section: [item for item in self.plan_manager.combined_items(self.state.plan, section) if not bool(item.get("approved", False))]
            for section in ["mappings", "transformations", "bq_validations", "quality_audit"]
        }

    def generate(
        self,
        mode: str = "processor",
        output_dir: Optional[str] = None,
        pipeline_name: str = "UniversalRosterPipeline",
        progress_callback: Optional[Callable[..., None]] = None,
    ) -> Dict[str, Any]:
        if not self.state.plan:
            raise ValueError("No active plan")
        if not self.state.profile:
            raise ValueError("No active profile loaded")

        self.plan_manager.ensure_fingerprint_match(self.state.plan, self.state.profile)

        if progress_callback:
            progress_callback("generation_started", f"Generating {mode} artifacts", 25)
        generated = self.codegen.generate(self.state.plan, mode=mode, pipeline_name=pipeline_name)
        out_dir = Path(output_dir).expanduser().resolve() if output_dir else (self.workspace_dir / "generated")
        if progress_callback:
            progress_callback("generation_writing", "Writing generated files", 70)
        written = self.codegen.write_outputs(generated, out_dir)

        metadata = {
            "workspace_scope": self.state.workspace_scope,
            "instructions_context": self.state.instructions_context,
            "generated_at": str(Path(out_dir).resolve()),
        }

        if progress_callback:
            progress_callback("generation_completed", f"Generated {len(written)} files", 100)

        return {
            "mode": mode,
            "output_dir": str(out_dir),
            "files": [str(path) for path in written],
            "unchecked": self.plan_manager.unchecked_counts(self.state.plan),
            "metadata": metadata,
        }

    def run_generated_pipeline(
        self,
        input_file: str,
        generated_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        table_id: str = "project.dataset.staging",
        progress_callback: Optional[Callable[..., None]] = None,
    ) -> Dict[str, Any]:
        if progress_callback:
            progress_callback("pipeline_run_started", "Starting generated pipeline", 15)
        base_dir = Path(generated_dir).expanduser().resolve() if generated_dir else (self.workspace_dir / "generated")
        main_py = base_dir / "main.py"
        if not main_py.exists():
            raise FileNotFoundError(f"Generated main.py not found at {main_py}")

        run_output_dir = Path(output_dir).expanduser().resolve() if output_dir else (base_dir / "run_outputs")
        run_output_dir.mkdir(parents=True, exist_ok=True)

        command = [
            "python",
            str(main_py),
            str(Path(input_file).expanduser().resolve()),
            "--output-dir",
            str(run_output_dir),
            "--table-id",
            table_id,
        ]

        if progress_callback:
            progress_callback("pipeline_run_executing", "Running generated main.py", 45)

        proc = subprocess.run(
            command,
            cwd=str(base_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )

        log_path = run_output_dir / "run.log"
        err_path = run_output_dir / "run.err.log"
        log_path.write_text(proc.stdout or "", encoding="utf-8")
        err_path.write_text(proc.stderr or "", encoding="utf-8")

        result = {
            "command": command,
            "return_code": int(proc.returncode),
            "log_path": str(log_path),
            "error_log_path": str(err_path),
            "output_dir": str(run_output_dir),
            "success": proc.returncode == 0,
        }
        if progress_callback:
            progress_callback(
                "pipeline_run_completed" if result["success"] else "pipeline_run_failed",
                "Generated pipeline run completed" if result["success"] else "Generated pipeline run failed",
                100,
            )
        return result

    def schema_summary(self) -> Dict[str, Any]:
        if not self.state.plan:
            raise ValueError("No active plan")
        roster_type = str(self.state.plan.get("roster_type", "practitioner"))
        approved = self.plan_manager.approved_subset(self.state.plan)
        mapped_fields = [str(m.get("target_field", "") or "").strip() for m in approved.get("mappings", [])]
        return self.schema_registry.template_summary(roster_type, mapped_fields)

    def status(self) -> Dict[str, Any]:
        plan = self.state.plan or {}
        profile = self.state.profile or {}

        counts = {
            "mappings": len(self.plan_manager.combined_items(plan, "mappings")) if plan else 0,
            "transformations": len(self.plan_manager.combined_items(plan, "transformations")) if plan else 0,
            "bq_validations": len(self.plan_manager.combined_items(plan, "bq_validations")) if plan else 0,
            "quality_audit": len(self.plan_manager.combined_items(plan, "quality_audit")) if plan else 0,
        }

        unchecked = self.plan_manager.unchecked_counts(plan) if plan else {"mappings": 0, "transformations": 0, "bq_validations": 0, "quality_audit": 0}

        learning_rows = self.learning_episodes.list_episodes()
        accepted = sum(1 for row in learning_rows if str(row.get("outcome") or "") == "accepted")
        rejected = sum(1 for row in learning_rows if str(row.get("outcome") or "") == "rejected")

        return {
            "loaded_file": profile.get("file_name"),
            "roster_type": profile.get("roster_type_detected") or plan.get("roster_type"),
            "plan_path": str(self.state.plan_path) if self.state.plan_path else None,
            "plan_approved": bool(plan.get("approved", False)),
            "counts": counts,
            "unchecked": unchecked,
            "llm_trace": plan.get("llm_trace", {}),
            "workspace_scope": self.state.workspace_scope,
            "instructions_context": self.state.instructions_context,
            "learning": {
                "episodes_total": len(learning_rows),
                "accepted": accepted,
                "rejected": rejected,
            },
            "qwen_quality_gates": self._qwen_quality_gate_metrics(),
        }

    def _enforce_section_policy(self, section: str, trace: Dict[str, Any]) -> None:
        normalized = str(section or "").strip().lower()
        requires = self.settings.is_strict_collaboration() or self.settings.require_claude_verifier_for_section(normalized)
        if not requires:
            return

        if not self.settings.strict_fail_closed():
            return

        verifier = (trace or {}).get("verifier") if isinstance(trace, dict) else {}
        verifier_status = str((verifier or {}).get("status", "") or "").strip().lower()
        verifier_provider = str((verifier or {}).get("provider", "") or "").strip().lower()

        if verifier_status != "ok":
            raise RuntimeError(
                f"Strict collaboration policy requires a successful Claude verifier for {normalized}, but verifier status is '{verifier_status or 'missing'}'"
            )
        if not self.settings.is_claude_provider(verifier_provider):
            raise RuntimeError(
                f"Strict collaboration policy requires Claude verifier provider for {normalized}, got '{verifier_provider or 'missing'}'"
            )

    @staticmethod
    def _section_key_for_episode(section: str) -> str:
        key = str(section or "").strip().lower()
        if key in {"mappings", "mapping"}:
            return "mappings"
        if key in {"transformations", "transformation", "transform"}:
            return "transformations"
        if key in {"validations", "validation", "bq_validations", "bq_validation"}:
            return "validations"
        if key in {"quality_audit", "quality_audits", "quality", "audit"}:
            return "quality_audit"
        return key

    @staticmethod
    def _episode_candidate_key(section: str, item: Dict[str, Any]) -> Dict[str, Any]:
        key = UniversalRosterSession._section_key_for_episode(section)
        if key == "mappings":
            return {
                "source_column": str(item.get("source_column") or "").strip(),
                "target_field": str(item.get("target_field") or "").strip(),
                "rule_type": "",
                "transform_name": "",
            }
        if key == "transformations":
            return {
                "source_columns": [str(v).strip() for v in (item.get("source_columns") or []) if str(v).strip()],
                "target_fields": [str(v).strip() for v in (item.get("target_fields") or []) if str(v).strip()],
                "transform_name": str(item.get("name") or "").strip(),
                "rule_type": "",
            }
        if key == "quality_audit":
            return {
                "source_column": str(item.get("source_column") or "").strip(),
                "target_field": str(item.get("target_field") or "").strip(),
                "rule_type": str(item.get("rule_type") or "").strip(),
                "category": str(item.get("category") or "").strip(),
                "severity": str(item.get("severity") or "").strip(),
                "title": str(item.get("title") or "").strip(),
                "transform_name": "",
            }
        return {
            "source_column": str(item.get("source_column") or "").strip(),
            "target_field": str(item.get("target_field") or "").strip(),
            "rule_type": str(item.get("rule_type") or "").strip(),
            "transform_name": "",
        }

    def _capture_learning_episodes(
        self,
        *,
        section: str,
        items: List[Dict[str, Any]],
        llm_trace: Dict[str, Any],
        roster_type: str,
        fingerprint: str,
    ) -> None:
        if not self.learning_episodes.is_enabled():
            return

        section_key = self._section_key_for_episode(section)
        primary = (llm_trace or {}).get("primary") if isinstance(llm_trace, dict) else {}
        verifier = (llm_trace or {}).get("verifier") if isinstance(llm_trace, dict) else {}
        policy = (llm_trace or {}).get("policy") if isinstance(llm_trace, dict) else {}
        retrieval_trace = (llm_trace or {}).get("retrieval") if isinstance(llm_trace, dict) else {}

        for item in items or []:
            if not isinstance(item, dict):
                continue
            item_id = str(item.get("id") or "").strip()
            if not item_id:
                continue
            run_metadata = {
                "workspace_scope": self.state.workspace_scope,
                "section": section_key,
                "retrieval": retrieval_trace,
            }
            self.learning_episodes.capture_episode(
                section=section_key,
                item_id=item_id,
                candidate_key=self._episode_candidate_key(section_key, item),
                workspace_scope=self.state.workspace_scope,
                roster_type=roster_type,
                fingerprint=fingerprint,
                run_metadata=run_metadata,
                primary_stage={
                    "provider": (primary or {}).get("provider"),
                    "model": (primary or {}).get("model"),
                    "status": (primary or {}).get("status") or ("ok" if (primary or {}).get("attempts") else "skipped"),
                    "task_type": (primary or {}).get("task_type"),
                    "attempts": (primary or {}).get("attempts") or [],
                },
                verifier_stage={
                    "provider": (verifier or {}).get("provider"),
                    "model": (verifier or {}).get("model"),
                    "status": (verifier or {}).get("status") or ("ok" if (verifier or {}).get("attempts") else "skipped"),
                    "decision": (verifier or {}).get("decision") or "",
                    "task_type": (verifier or {}).get("task_type"),
                    "attempts": (verifier or {}).get("attempts") or [],
                },
                final_candidate=item,
                policy=policy or {},
            )

    def _finalize_learning_episodes(
        self,
        item_type: str,
        item_id: str,
        approved: bool,
        rationale: Optional[Dict[str, Any]] = None,
        rationale_only: bool = False,
    ) -> None:
        if not self.learning_episodes.is_enabled():
            return
        try:
            section = normalize_plan_section(item_type)
        except Exception:
            section = item_type

        fingerprint = ""
        if self.state.profile:
            fingerprint = str((self.state.profile.get("input_fingerprint") or {}).get("signature") or "")

        section_key = self._section_key_for_episode(section)
        if rationale_only:
            updated = self.learning_episodes.attach_rationale_to_item(
                item_id=item_id,
                section=section_key,
                fingerprint=fingerprint or None,
                rationale=rationale,
            )
        else:
            updated = self.learning_episodes.finalize_item_outcome(
                item_id=item_id,
                approved=approved,
                section=section_key,
                fingerprint=fingerprint or None,
                rationale=rationale,
            )
        if rationale_only or not approved:
            return
        for episode in updated:
            quality_flags = episode.get("quality_flags") or {}
            if all(bool(value) for value in quality_flags.values()):
                continue
            episode_id = str(episode.get("episode_id") or "").strip()
            if not episode_id:
                continue
            failed = [key for key, value in quality_flags.items() if not bool(value)]
            reason = "quality_gate_failed" + (":" + ",".join(failed) if failed else "")
            self.learning_episodes.quarantine_episode(episode_id=episode_id, reason=reason)

    def _plan_roster_type(self) -> str:
        if self.state.plan:
            return str(self.state.plan.get("roster_type", "practitioner") or "practitioner")
        if self.state.profile:
            return str(self.state.profile.get("roster_type_detected", "practitioner") or "practitioner")
        return "practitioner"

    def _record_item_feedback(
        self,
        item_type: str,
        item_id: str,
        approved: bool,
        rationale: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.state.plan:
            return

        roster_type = self._plan_roster_type()

        try:
            section = normalize_plan_section(item_type)
        except ValueError:
            return

        if section == "mappings" and item_id.startswith("map::"):
            for item in self.plan_manager.combined_items(self.state.plan, "mappings"):
                if str(item.get("id", "")) != item_id:
                    continue
                source = str(item.get("source_column", "") or "").strip()
                target = str(item.get("target_field", "") or "").strip()
                if source and target:
                    self.learning_kb.record_mapping_feedback(
                        roster_type=roster_type,
                        source_column=source,
                        target_field=target,
                        action="approved" if approved else "rejected",
                        scope=self.state.workspace_scope,
                    )
                    self._record_rationale_if_enabled(rationale)
                return

        if section == "transformations" and item_id.startswith("tx::"):
            for item in self.plan_manager.combined_items(self.state.plan, "transformations"):
                if str(item.get("id", "")) != item_id:
                    continue
                name = str(item.get("name", "") or "").strip()
                source_columns = [str(s).strip() for s in (item.get("source_columns") or []) if str(s).strip()]
                target_fields = [str(t).strip() for t in (item.get("target_fields") or []) if str(t).strip()]
                source = source_columns[0] if source_columns else ""
                target = target_fields[0] if target_fields else ""
                if name and source:
                    self.learning_kb.record_transformation_feedback(
                        roster_type=roster_type,
                        transform_name=name,
                        source_column=source,
                        target_field=target,
                        action="approved" if approved else "rejected",
                        scope=self.state.workspace_scope,
                    )
                    self._record_rationale_if_enabled(rationale)
                return

        if section == "bq_validations" and item_id.startswith("bq::"):
            for item in self.plan_manager.combined_items(self.state.plan, "bq_validations"):
                if str(item.get("id", "")) != item_id:
                    continue
                rule_type = str(item.get("rule_type", "") or "").strip() or "custom"
                source = str(item.get("source_column", "") or "").strip()
                target = str(item.get("target_field", "") or "").strip()
                if source or target:
                    self.learning_kb.record_validation_feedback(
                        roster_type=roster_type,
                        rule_type=rule_type,
                        source_column=source,
                        target_field=target,
                        action="approved" if approved else "rejected",
                        scope=self.state.workspace_scope,
                    )
                    self._record_rationale_if_enabled(rationale)
                return

        if section == "quality_audit" and item_id.startswith("qa::"):
            for item in self.plan_manager.combined_items(self.state.plan, "quality_audit"):
                if str(item.get("id", "")) != item_id:
                    continue
                rule_type = str(item.get("rule_type", "") or "").strip() or "quality_audit"
                source = str(item.get("source_column", "") or "").strip()
                target = str(item.get("target_field", "") or "").strip()
                self.learning_kb.record_quality_audit_feedback(
                    roster_type=roster_type,
                    rule_type=rule_type,
                    source_column=source,
                    target_field=target,
                    action="approved" if approved else "rejected",
                    scope=self.state.workspace_scope,
                )
                self._record_rationale_if_enabled(rationale)
                return

    def _qwen_quality_gate_metrics(self) -> Dict[str, Dict[str, float]]:
        rows = self.learning_episodes.list_episodes()
        metrics: Dict[str, Dict[str, float]] = {}
        for section in ["mappings", "transformations", "validations", "quality_audit"]:
            section_rows = [
                row
                for row in rows
                if self._section_key_for_episode(str(row.get("section") or "")) == section
                and str(row.get("outcome") or "") in {"accepted", "rejected"}
            ]
            accepted = float(sum(1 for row in section_rows if str(row.get("outcome") or "") == "accepted"))
            reviewed = float(len(section_rows))
            acceptance_rate = self._safe_rate(accepted, reviewed)
            contradiction_count = float(
                sum(
                    1
                    for row in section_rows
                    if any(
                        str((entry or {}).get("decision") or "") != str(row.get("outcome") or "")
                        for entry in (row.get("review_history") or [])
                        if isinstance(entry, dict) and str((entry or {}).get("decision") or "") in {"accepted", "rejected"}
                    )
                )
            )
            clarification_unresolved = float(
                sum(
                    1
                    for row in section_rows
                    if any(
                        isinstance((entry or {}).get("rationale"), dict)
                        and str(
                            ((((entry or {}).get("rationale") or {}).get("followup") or {}).get("response_type")
                            or ((entry or {}).get("rationale") or {}).get("response_type")
                            or "")
                        ).strip().lower() in {"skip", "skipped"}
                        for entry in (row.get("review_history") or [])
                        if isinstance(entry, dict)
                    )
                )
            )
            gate = self.settings.qwen_quality_gate_for_section(section)
            metrics[section] = {
                "accepted": accepted,
                "reviewed": reviewed,
                "acceptance_rate": acceptance_rate,
                "contradictions": contradiction_count,
                "clarification_unresolved": clarification_unresolved,
                "min_accepted": float(gate.get("min_accepted", 0.0)),
                "min_acceptance_rate": float(gate.get("min_acceptance_rate", 0.0)),
                "meets_gate": float(accepted >= float(gate.get("min_accepted", 0.0)) and acceptance_rate >= float(gate.get("min_acceptance_rate", 0.0))),
            }
        return metrics

    def _sync_router_quality_gate_metrics(self) -> None:
        metrics = self._qwen_quality_gate_metrics()
        self._last_quality_gate_metrics = metrics
        for section, values in metrics.items():
            stage_policy = self._section_stage_policy(section, values)
            self._last_quality_gate_stage[section] = str(stage_policy.get("stage") or "supervised")
            self.router_factory.set_quality_gate_metrics(
                section,
                {
                    "accepted": float(values.get("accepted", 0.0)),
                    "reviewed": float(values.get("reviewed", 0.0)),
                    "acceptance_rate": float(values.get("acceptance_rate", 0.0)),
                    "contradictions": float(values.get("contradictions", 0.0)),
                    "clarification_unresolved": float(values.get("clarification_unresolved", 0.0)),
                    "stage": str(stage_policy.get("stage") or "supervised"),
                    "rollback": bool(stage_policy.get("rollback")),
                    "rollback_reasons": stage_policy.get("rollback_reasons") or [],
                },
            )

    def _resolve_learning_scope(self, learning_scope: Any) -> Optional[Dict[str, Any]]:
        if learning_scope is _DEFAULT_LEARNING_SCOPE:
            return self.state.workspace_scope
        if learning_scope is None:
            return None
        if isinstance(learning_scope, dict):
            return learning_scope
        return self.state.workspace_scope

    def _record_mapping_addition(
        self,
        source_column: str,
        target_field: str,
        scope: Any = _DEFAULT_LEARNING_SCOPE,
        rationale: Optional[Dict[str, Any]] = None,
    ) -> None:
        resolved_scope = self._resolve_learning_scope(scope)
        self.learning_kb.record_mapping_feedback(
            roster_type=self._plan_roster_type(),
            source_column=source_column,
            target_field=target_field,
            action="added",
            scope=resolved_scope,
        )
        self._record_rationale_if_enabled(rationale)

    def _record_transformation_addition(
        self,
        item: Dict[str, Any],
        scope: Any = _DEFAULT_LEARNING_SCOPE,
        rationale: Optional[Dict[str, Any]] = None,
    ) -> None:
        resolved_scope = self._resolve_learning_scope(scope)
        name = str(item.get("name", "") or "").strip()
        source_columns = [str(s).strip() for s in (item.get("source_columns") or []) if str(s).strip()]
        target_fields = [str(t).strip() for t in (item.get("target_fields") or []) if str(t).strip()]
        source = source_columns[0] if source_columns else ""
        target = target_fields[0] if target_fields else ""
        if name and source:
            self.learning_kb.record_transformation_feedback(
                roster_type=self._plan_roster_type(),
                transform_name=name,
                source_column=source,
                target_field=target,
                action="added",
                scope=resolved_scope,
            )
            self._record_rationale_if_enabled(rationale)

    def _record_validation_addition(
        self,
        item: Dict[str, Any],
        scope: Any = _DEFAULT_LEARNING_SCOPE,
        rationale: Optional[Dict[str, Any]] = None,
    ) -> None:
        resolved_scope = self._resolve_learning_scope(scope)
        rule_type = str(item.get("rule_type", "") or "").strip() or "custom"
        source = str(item.get("source_column", "") or "").strip()
        target = str(item.get("target_field", "") or "").strip()
        if source or target:
            self.learning_kb.record_validation_feedback(
                roster_type=self._plan_roster_type(),
                rule_type=rule_type,
                source_column=source,
                target_field=target,
                action="added",
                scope=resolved_scope,
            )
            self._record_rationale_if_enabled(rationale)

    def _capture_custom_add_episode(
        self,
        section: str,
        item: Dict[str, Any],
        rationale: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.learning_episodes.is_enabled():
            return
        section_key = self._section_key_for_episode(section)
        item_id = str(item.get("id") or "").strip()
        if not item_id:
            return

        fingerprint = ""
        if self.state.profile:
            fingerprint = str((self.state.profile.get("input_fingerprint") or {}).get("signature") or "")

        run_metadata = {
            "workspace_scope": None,
            "section": section_key,
            "origin": "chat_custom_action",
            "scope_mode": "global",
        }
        self.learning_episodes.capture_episode(
            section=section_key,
            item_id=item_id,
            candidate_key=self._episode_candidate_key(section_key, item),
            workspace_scope={},
            roster_type=self._plan_roster_type(),
            fingerprint=fingerprint,
            run_metadata=run_metadata,
            primary_stage={
                "provider": "chat_custom_action",
                "model": "deterministic",
                "status": "ok",
                "task_type": "custom_add",
                "attempts": ["chat_custom_action:ok"],
            },
            verifier_stage={
                "provider": "user_confirmed",
                "model": "n/a",
                "status": "ok",
                "decision": "accepted",
                "task_type": "custom_add",
                "attempts": ["user_confirmed:ok"],
            },
            final_candidate=item,
            policy={"status": "satisfied"},
        )
        self.learning_episodes.finalize_item_outcome(
            item_id=item_id,
            approved=True,
            section=section_key,
            fingerprint=fingerprint or None,
            rationale=rationale,
        )

    @staticmethod
    def _rationale_tags_from_text(text: str) -> List[str]:
        lowered = str(text or "").strip().lower()
        if not lowered:
            return []
        tags: List[str] = []
        for token in [
            "duplicate",
            "incorrect",
            "format",
            "overfit",
            "false_positive",
            "false_negative",
            "schema",
            "business_rule",
            "required",
            "enum",
            "normalization",
            "edge_case",
            "manual_override",
            "missing_rule",
            "too_strict",
            "too_loose",
            "client_specific",
            "regulatory",
            "cross_field",
        ]:
            if token.replace("_", " ") in lowered or token in lowered:
                tags.append(token)
        return tags[:10]

    @staticmethod
    def _infer_reason_category(text: str, tags: List[str]) -> str:
        lowered = str(text or "").strip().lower()
        inferred = set(tags or [])
        if "false_positive" in inferred or "too_strict" in inferred:
            return "false_positive"
        if "false_negative" in inferred or "too_loose" in inferred or "missing_rule" in inferred:
            return "false_negative"
        if "schema" in inferred or "format" in inferred:
            return "schema_mismatch"
        if "business_rule" in inferred or "client_specific" in inferred or "regulatory" in inferred:
            return "business_rule"
        if "manual_override" in inferred:
            return "manual_override"
        if "edge_case" in inferred or "cross_field" in inferred:
            return "edge_case"
        if "incorrect" in inferred or "duplicate" in inferred:
            return "incorrect_suggestion"
        if lowered:
            return "other"
        return "unknown"

    def normalize_rationale_payload(
        self,
        *,
        event: str,
        item_type: str,
        item_id: str,
        approved: Optional[bool],
        rationale_text: str,
        workspace_scope: Optional[Dict[str, Any]] = None,
        rationale_tags: Optional[List[str]] = None,
        timestamp: Optional[str] = None,
        question_text: Optional[str] = None,
        response_type: Optional[str] = None,
        followup_status: Optional[str] = None,
        source: Optional[str] = None,
        decision_confidence: Optional[float] = None,
        supervisor_trace: Optional[Dict[str, Any]] = None,
        provenance: Optional[str] = None,
        reason_category: Optional[str] = None,
        confidence_before: Optional[float] = None,
        confidence_after: Optional[float] = None,
        impact_scope: Optional[str] = None,
        suggested_rule_change: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        tags = [str(tag).strip().lower() for tag in (rationale_tags or []) if str(tag).strip()]
        text_value = str(rationale_text or "").strip()
        if not tags and text_value:
            tags = self._rationale_tags_from_text(text_value)

        scope_payload = dict(workspace_scope) if isinstance(workspace_scope, dict) else dict(self.state.workspace_scope)
        scope_payload["workspace_signature"] = self._scope_signature_from_mapping(scope_payload)

        normalized_approved = self._normalize_yes_no(approved)
        if normalized_approved is None:
            normalized_approved = approved if isinstance(approved, bool) else None

        clean_question = str(question_text or "").strip()
        clean_response_type = str(response_type or "").strip().lower()
        if not clean_response_type:
            clean_response_type = "answered" if text_value else "unknown"

        clean_followup_status = str(followup_status or "").strip().lower()
        if not clean_followup_status:
            clean_followup_status = "answered" if clean_response_type == "answered" else clean_response_type or "unknown"

        item_section = self.settings.normalize_section_key(str(item_type or ""))
        item_context = self._item_for_id(item_type=item_type, item_id=item_id) or {}

        supervisor_payload = self._normalize_supervisor_trace(supervisor_trace)
        decision_source = str(source or "").strip().lower() or "chat"
        decision_conf = max(0.0, min(1.0, self._safe_float(decision_confidence, default=0.0)))

        generated_timestamp = str(timestamp or "").strip() or self._utc_now_iso()
        reason_value = str(reason_category or "").strip().lower() or self._infer_reason_category(text_value, tags)
        before_conf = max(0.0, min(1.0, self._safe_float(confidence_before, default=0.0)))
        after_conf = max(0.0, min(1.0, self._safe_float(confidence_after, default=0.0)))
        impact_value = str(impact_scope or "").strip().lower() or "item"
        rule_change_payload = dict(suggested_rule_change) if isinstance(suggested_rule_change, dict) else {}

        return {
            "schema_version": 3,
            "event": str(event or ""),
            "item_type": str(item_type or ""),
            "item_id": str(item_id or ""),
            "section": item_section,
            "approved": normalized_approved,
            "rationale_text": text_value,
            "rationale_tags": tags,
            "workspace_scope": scope_payload,
            "workspace_signature": str(scope_payload.get("workspace_signature") or ""),
            "tenant_id": str(scope_payload.get("tenant_id") or ""),
            "client_id": str(scope_payload.get("client_id") or ""),
            "thread_id": str(scope_payload.get("thread_id") or ""),
            "timestamp": generated_timestamp,
            "recorded_at": self._utc_now_iso(),
            "item_context": {
                "item_type": str(item_type or ""),
                "item_id": str(item_id or ""),
                "section": item_section,
                "source_column": item_context.get("source_column"),
                "target_field": item_context.get("target_field"),
                "name": item_context.get("name"),
                "rule_type": item_context.get("rule_type"),
            },
            "decision": {
                "approved": normalized_approved,
                "source": decision_source,
                "confidence": round(decision_conf, 4),
            },
            "followup": {
                "question_text": clean_question,
                "response_type": clean_response_type,
                "status": clean_followup_status,
            },
            "supervisor": supervisor_payload,
            "reason_category": reason_value,
            "confidence_before": round(before_conf, 4),
            "confidence_after": round(after_conf, 4),
            "impact_scope": impact_value,
            "suggested_rule_change": rule_change_payload,
            "provenance": str(provenance or "native_capture"),
        }

    def _record_rationale_if_enabled(self, rationale: Optional[Dict[str, Any]]) -> None:
        if not self.settings.enable_rationale_capture:
            return
        if not isinstance(rationale, dict):
            return
        text = str(rationale.get("rationale_text") or "").strip()
        response_type = str(rationale.get("followup", {}).get("response_type") or rationale.get("response_type") or "").strip().lower()
        if not text and response_type not in {"skip", "skipped"}:
            return
        payload = dict(rationale)
        payload.setdefault("workspace_scope", self.state.workspace_scope)

        if self.settings.enable_structured_decision_events:
            self.learning_kb.append_decision_event(payload)
        else:
            self.learning_kb.append_rationale(payload)

    def record_chat_outcome(self, outcome: Dict[str, Any]) -> None:
        payload = dict(outcome)
        payload.setdefault("roster_type", self._plan_roster_type())
        payload.setdefault("workspace_scope", self.state.workspace_scope)
        self.learning_kb.append_chat_outcome(payload)

    def chat_supervisor_trace(self, *, decision_type: str, text: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not self.settings.enable_claude_chat_supervisor:
            return {
                "status": "disabled",
                "provider": None,
                "model": None,
                "attempts": [],
                "response_type": "none",
                "question_text": "",
                "reason": "",
                "approved": None,
                "confidence": 0.0,
            }

        prompt = json.dumps(
            {
                "task": "chat_supervisor",
                "decision_type": str(decision_type or ""),
                "message": str(text or ""),
                "context": context or {},
                "instructions": {
                    "return_json_only": True,
                    "response_type": "approve|reject|skip|clarify|none",
                },
            },
            ensure_ascii=False,
        )

        try:
            router = self.router_factory.for_task("chat_supervisor")
            result = router.generate(prompt=prompt, task_type="chat_supervisor")
            parsed = json.loads(str(result.response.text or "{}").strip())
            if not isinstance(parsed, dict):
                parsed = {}
        except Exception:
            return {
                "status": "error",
                "provider": None,
                "model": None,
                "attempts": [],
                "response_type": "none",
                "question_text": "",
                "reason": "",
                "approved": None,
                "confidence": 0.0,
            }

        response_type = str(parsed.get("response_type") or "none").strip().lower()
        if response_type not in {"approve", "reject", "skip", "clarify", "none"}:
            response_type = "none"
        try:
            confidence = float(parsed.get("confidence", 0.0) or 0.0)
        except Exception:
            confidence = 0.0
        approved = parsed.get("approved")
        if not isinstance(approved, bool):
            approved = None

        return {
            "status": "ok",
            "provider": result.response.provider,
            "model": result.response.model,
            "attempts": result.attempts,
            "response_type": response_type,
            "question_text": str(parsed.get("question_text") or "").strip(),
            "reason": str(parsed.get("reason") or "").strip(),
            "approved": approved,
            "confidence": max(0.0, min(1.0, confidence)),
        }

    def export_training_datasets(
        self,
        output_dir: Optional[str | Path] = None,
        progress_callback: Optional[Callable[..., None]] = None,
    ) -> Dict[str, Any]:
        try:
            if progress_callback:
                progress_callback("trainer_export_started", "Exporting training datasets", 20)
            result = self.training_exporter.export(output_dir=output_dir)
            if progress_callback:
                progress_callback("trainer_export_completed", "Training dataset export complete", 100)
            return result
        except Exception as exc:
            if progress_callback:
                progress_callback("trainer_export_failed", f"Training dataset export failed: {exc}", 100)
            return {
                "status": "failed",
                "error": str(exc),
                "fail_open": True,
            }

    def run_trainer(
        self,
        *,
        export_dir: Optional[str | Path] = None,
        extra_args: Optional[List[str]] = None,
        env_overrides: Optional[Dict[str, str]] = None,
        progress_callback: Optional[Callable[..., None]] = None,
    ) -> Dict[str, Any]:
        try:
            if progress_callback:
                progress_callback("trainer_run_started", "Starting trainer run", 10)
            result = self.trainer.run(
                export_dir=export_dir,
                extra_args=extra_args,
                env_overrides=env_overrides,
            )
            if progress_callback:
                progress_callback("trainer_run_completed", "Trainer run complete", 100)
            return result
        except Exception as exc:
            if progress_callback:
                progress_callback("trainer_run_failed", f"Trainer run failed: {exc}", 100)
            return {
                "status": "failed",
                "error": str(exc),
                "fail_open": True,
            }

    def export_status_json(self) -> str:
        return json.dumps(self.status(), indent=2, ensure_ascii=False)
