"""Standalone configuration for Universal Roster V2."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Dict, List


PACKAGE_ROOT = Path(__file__).resolve().parent
SCHEMAS_DIR = PACKAGE_ROOT / "schemas"
TEMPLATES_DIR = PACKAGE_ROOT / "templates"


def _env_csv(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default)
    return [part.strip() for part in raw.split(",") if part.strip()]


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except Exception:
        return default


def _env_choice(name: str, default: str, allowed: set[str]) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in allowed:
        return value
    return default


def _env_path(name: str, default: Path) -> Path:
    raw = os.getenv(name)
    if raw is None:
        return default.expanduser().resolve()
    value = raw.strip()
    if not value:
        return default.expanduser().resolve()
    return Path(value).expanduser().resolve()


@dataclass(frozen=True)
class Settings:
    ollama_base_url: str
    ollama_analysis_model: str
    ollama_analysis_fallbacks: List[str]
    ollama_generation_model: str
    ollama_generation_fallbacks: List[str]
    claude_fallback_model: str
    enable_claude_verifier: bool
    claude_verifier_model: str
    enable_claude_primary_fallback: bool
    enable_claude_api_fallback: bool
    anthropic_api_key: str
    workspace_dir: Path
    default_output_dir: Path
    learning_kb_path: Path
    claude_cli_command: str
    claude_cli_args: List[str]
    claude_cli_model: str
    claude_cli_timeout_seconds: int
    claude_cli_retries: int
    llm_analysis_provider_order: List[str]
    llm_verifier_provider_order: List[str]
    llm_generation_provider_order: List[str]
    advanced_profile_inference: bool
    enable_transcript_ingestion: bool
    enable_auto_approval_policy: bool
    auto_approval_mapping_threshold: float
    auto_approval_transformation_threshold: float
    auto_approval_validation_threshold: float
    workspace_mode_default: bool
    collaboration_mode: str
    require_claude_verifier_mappings: bool
    require_claude_verifier_transformations: bool
    require_claude_verifier_validations: bool
    profile_full_roster_learning: bool
    profile_max_rows: int
    require_claude_verifier_quality_audit: bool = False
    enable_async_operations: bool = True
    enable_sse_progress: bool = True
    enable_web_debug_drawer: bool = True
    web_operation_poll_interval_ms: int = 1500
    operation_retention_max_records: int = 2000
    operation_log_retention: int = 300
    operation_events_retention: int = 2000
    max_concurrent_operations_per_workspace: int = 1

    enable_learning_episode_capture: bool = True
    capture_llm_payloads: bool = False
    learning_episodes_path: Path = Path("./learning_episodes.jsonl")
    learning_episode_payload_max_chars: int = 4000
    learning_episode_max_records: int = 50000
    learning_episode_max_age_days: int = 180
    learning_episode_quarantine_on_conflict: bool = True

    enable_rag_retrieval: bool = False
    rag_max_examples_per_item: int = 3
    rag_min_score: float = 0.55
    rag_use_workspace_scope_weight: bool = True
    rag_use_roster_type_weight: bool = True
    rag_chat_outcome_hint_weight: float = 0.05

    training_export_dir: Path = Path("./training_exports")
    training_export_max_records: int = 50000
    training_min_accepted_episodes: int = 25

    trainer_command: List[str] = field(default_factory=list)
    trainer_working_dir: Path = Path(".")
    trainer_timeout_seconds: int = 3600

    enable_shadow_trained_primary: bool = False
    shadow_model_name: str = ""
    shadow_sections: List[str] = field(default_factory=lambda: ["mappings", "transformations", "validations", "quality_audit"])

    enable_rationale_capture: bool = False
    enable_rationale_followup_question: bool = False
    enable_rationale_retrieval_influence: bool = False
    enable_rationale_training_export: bool = False

    enable_claude_chat_supervisor: bool = False
    enable_dynamic_rationale_questions: bool = False
    enable_structured_decision_events: bool = False
    enable_episode_finalize_idempotency: bool = False
    enable_qwen_stage_promotion: bool = False
    llm_supervisor_provider_order: List[str] = field(default_factory=lambda: ["claude_cli", "claude_api", "local_ollama"])

    qwen_quality_gate_mappings_min_accepted: int = 25
    qwen_quality_gate_mappings_min_acceptance_rate: float = 0.8
    qwen_quality_gate_transformations_min_accepted: int = 25
    qwen_quality_gate_transformations_min_acceptance_rate: float = 0.8
    qwen_quality_gate_validations_min_accepted: int = 25
    qwen_quality_gate_validations_min_acceptance_rate: float = 0.8
    qwen_quality_gate_quality_audit_min_accepted: int = 25
    qwen_quality_gate_quality_audit_min_acceptance_rate: float = 0.8

    enable_gateway_claude: bool = False
    gateway_base_url: str = "https://api-gateway-yfec.onrender.com/v1"
    gateway_api_key: str = ""
    gateway_model: str = "claude-opus-4.6"
    gateway_timeout_seconds: int = 120
    gateway_requests_per_minute: int = 8
    gateway_max_wait_seconds: float = 20.0

    quality_audit_enrichment_enabled: bool = False
    quality_audit_enrichment_timeout_seconds: float = 2.0
    quality_audit_enrichment_cache_ttl_seconds: int = 900
    quality_audit_nppes_enabled: bool = False
    quality_audit_nppes_endpoint: str = ""
    quality_audit_nppes_api_key: str = ""
    quality_audit_nucc_enabled: bool = False
    quality_audit_nucc_endpoint: str = ""
    quality_audit_nucc_api_key: str = ""
    quality_audit_client_refs_enabled: bool = False
    quality_audit_client_refs_endpoint: str = ""
    quality_audit_client_refs_api_key: str = ""
    quality_audit_service_account_key_path: str = ""
    quality_audit_bq_enabled: bool = False
    quality_audit_bq_project_id: str = ""
    quality_audit_bq_dataset: str = ""

    demo_mode: bool = False

    enable_gemini: bool = False
    gemini_pro_model: str = "gemini-2.5-pro"
    gemini_flash_model: str = "gemini-2.5-flash"
    gemini_api_key: str = ""
    gemini_service_account_key_path: str = ""
    gemini_location: str = "us-central1"
    gemini_project_id: str = ""

    def ollama_candidates(self, task_type: str) -> List[str]:
        task_key = (task_type or "analysis").strip().lower()
        if task_key == "analysis":
            models = [self.ollama_analysis_model, *self.ollama_analysis_fallbacks]
        else:
            models = [self.ollama_generation_model, *self.ollama_generation_fallbacks]

        out: List[str] = []
        seen = set()
        for model in models:
            key = model.strip()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(key)
        return out

    def provider_order(self, task_type: str) -> List[str]:
        task_key = (task_type or "analysis").strip().lower()
        if task_key in {"supervisor", "chat_supervisor", "chat_review"}:
            providers = list(self.llm_supervisor_provider_order)
        elif task_key in {"verifier", "verification"}:
            providers = list(self.llm_verifier_provider_order)
        elif task_key == "generation":
            providers = list(self.llm_generation_provider_order)
        elif task_key in {"quality_audit", "quality_audits", "quality", "audit"}:
            providers = list(self.llm_verifier_provider_order)
        else:
            providers = list(self.llm_analysis_provider_order)

        if task_key in {"analysis", "verifier", "verification"} and self.enable_claude_primary_fallback:
            providers.append("claude_api")
        if self.enable_claude_api_fallback:
            providers.append("claude_api")

        aliases = {
            "qwen_local": "local_ollama",
            "qwen": "local_ollama",
            "ollama": "local_ollama",
            "claude": "claude_cli",
            "claude_gateway": "gateway_claude",
            "gateway": "gateway_claude",
            "gemini": "gemini_vertex",
            "gemini_pro": "gemini_pro",
            "gemini_flash": "gemini_flash",
            "vertex": "gemini_vertex",
            "vertex_ai": "gemini_vertex",
        }

        out: List[str] = []
        seen = set()
        for provider in providers:
            key = aliases.get(provider.strip().lower(), provider.strip().lower())
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(key)
        return out

    def is_strict_collaboration(self) -> bool:
        return self.collaboration_mode in {"strict_fail_open", "strict_fail_closed"}

    def strict_fail_closed(self) -> bool:
        return self.collaboration_mode == "strict_fail_closed"

    @staticmethod
    def normalize_section_key(section: str) -> str:
        key = (section or "").strip().lower()
        if key in {"mapping", "mappings"}:
            return "mappings"
        if key in {"transformation", "transformations", "transform"}:
            return "transformations"
        if key in {"validation", "validations", "bq_validation", "bq_validations"}:
            return "validations"
        if key in {"quality", "audit", "quality_audit", "quality_audits"}:
            return "quality_audit"
        return key

    def require_claude_verifier_for_section(self, section: str) -> bool:
        key = self.normalize_section_key(section)
        if key == "mappings":
            return bool(self.require_claude_verifier_mappings)
        if key == "transformations":
            return bool(self.require_claude_verifier_transformations)
        if key == "validations":
            return bool(self.require_claude_verifier_validations)
        if key == "quality_audit":
            return bool(self.require_claude_verifier_quality_audit)
        return False

    def qwen_quality_gate_for_section(self, section: str) -> Dict[str, float]:
        key = self.normalize_section_key(section)
        if key == "mappings":
            return {
                "min_accepted": float(max(0, int(self.qwen_quality_gate_mappings_min_accepted))),
                "min_acceptance_rate": float(max(0.0, min(1.0, self.qwen_quality_gate_mappings_min_acceptance_rate))),
            }
        if key == "transformations":
            return {
                "min_accepted": float(max(0, int(self.qwen_quality_gate_transformations_min_accepted))),
                "min_acceptance_rate": float(max(0.0, min(1.0, self.qwen_quality_gate_transformations_min_acceptance_rate))),
            }
        if key == "validations":
            return {
                "min_accepted": float(max(0, int(self.qwen_quality_gate_validations_min_accepted))),
                "min_acceptance_rate": float(max(0.0, min(1.0, self.qwen_quality_gate_validations_min_acceptance_rate))),
            }
        if key == "quality_audit":
            return {
                "min_accepted": float(max(0, int(self.qwen_quality_gate_quality_audit_min_accepted))),
                "min_acceptance_rate": float(max(0.0, min(1.0, self.qwen_quality_gate_quality_audit_min_acceptance_rate))),
            }
        return {
            "min_accepted": 0.0,
            "min_acceptance_rate": 0.0,
        }

    @staticmethod
    def is_claude_provider(provider_name: str) -> bool:
        return str(provider_name or "").strip().lower() in {"claude_cli", "claude_api", "gateway_claude"}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    workspace_dir = Path(os.getenv("UR2_WORKSPACE_DIR", "./workspace")).expanduser().resolve()
    output_dir = Path(os.getenv("UR2_DEFAULT_OUTPUT_DIR", "./generated")).expanduser().resolve()
    learning_kb_default = workspace_dir / "learning_kb.json"
    learning_kb_path = Path(os.getenv("UR2_LEARNING_KB_PATH", str(learning_kb_default))).expanduser().resolve()

    workspace_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    learning_kb_path.parent.mkdir(parents=True, exist_ok=True)

    learning_episodes_path = _env_path("UR2_LEARNING_EPISODES_PATH", workspace_dir / "learning_episodes.jsonl")
    learning_episodes_path.parent.mkdir(parents=True, exist_ok=True)

    training_export_dir = _env_path("UR2_TRAINING_EXPORT_DIR", workspace_dir / "training_exports")
    training_export_dir.mkdir(parents=True, exist_ok=True)

    trainer_working_dir = _env_path("UR2_TRAINER_WORKING_DIR", workspace_dir)

    claude_fallback_model = os.getenv("UR2_CLAUDE_FALLBACK_MODEL", "claude-opus-4-6").strip() or "claude-opus-4-6"
    claude_verifier_model = os.getenv("UR2_CLAUDE_VERIFIER_MODEL", claude_fallback_model).strip() or claude_fallback_model
    claude_cli_model = os.getenv("UR2_CLAUDE_CLI_MODEL", claude_verifier_model).strip() or claude_verifier_model

    analysis_default = "gemini_pro,local_ollama,claude_cli"
    verifier_default = "gemini_pro,claude_cli,local_ollama"
    generation_default = "gemini_flash,local_ollama,claude_cli"
    supervisor_default = "gemini_pro,claude_cli,claude_api,local_ollama"

    collaboration_mode = _env_choice(
        "UR2_COLLABORATION_MODE",
        "advisory",
        {"advisory", "strict_fail_open", "strict_fail_closed"},
    )

    return Settings(
        ollama_base_url=os.getenv("UR2_OLLAMA_BASE_URL", "http://localhost:11434").strip() or "http://localhost:11434",
        ollama_analysis_model=os.getenv("UR2_OLLAMA_ANALYSIS_MODEL", "certifyos-analyst").strip() or "certifyos-analyst",
        ollama_analysis_fallbacks=_env_csv("UR2_OLLAMA_ANALYSIS_FALLBACKS", "qwen2.5-coder:7b,qwen2.5:7b"),
        ollama_generation_model=os.getenv("UR2_OLLAMA_GENERATION_MODEL", "certifyos-coder").strip() or "certifyos-coder",
        ollama_generation_fallbacks=_env_csv("UR2_OLLAMA_GENERATION_FALLBACKS", "qwen2.5-coder:7b,qwen2.5:7b"),
        claude_fallback_model=claude_fallback_model,
        enable_claude_verifier=_env_bool("UR2_ENABLE_CLAUDE_VERIFIER", True),
        claude_verifier_model=claude_verifier_model,
        enable_claude_primary_fallback=_env_bool("UR2_ENABLE_CLAUDE_PRIMARY_FALLBACK", False),
        enable_claude_api_fallback=_env_bool("UR2_ENABLE_CLAUDE_API_FALLBACK", False),
        anthropic_api_key=os.getenv("ANTHROPIC_API_KEY", "").strip(),
        enable_gateway_claude=_env_bool("UR2_ENABLE_GATEWAY_CLAUDE", False),
        gateway_base_url=os.getenv("UR2_GATEWAY_BASE_URL", "https://api-gateway-yfec.onrender.com/v1").strip() or "https://api-gateway-yfec.onrender.com/v1",
        gateway_api_key=os.getenv("UR2_GATEWAY_API_KEY", "").strip(),
        gateway_model=os.getenv("UR2_GATEWAY_MODEL", "claude-opus-4.6").strip() or "claude-opus-4.6",
        gateway_timeout_seconds=max(5, _env_int("UR2_GATEWAY_TIMEOUT_SECONDS", 120)),
        gateway_requests_per_minute=max(1, _env_int("UR2_GATEWAY_REQUESTS_PER_MINUTE", 8)),
        gateway_max_wait_seconds=max(0.1, _env_float("UR2_GATEWAY_MAX_WAIT_SECONDS", 20.0)),
        workspace_dir=workspace_dir,
        default_output_dir=output_dir,
        learning_kb_path=learning_kb_path,
        claude_cli_command=os.getenv("UR2_CLAUDE_CLI_COMMAND", "claude").strip() or "claude",
        claude_cli_args=_env_csv("UR2_CLAUDE_CLI_ARGS", "-p"),
        claude_cli_model=claude_cli_model,
        claude_cli_timeout_seconds=max(5, _env_int("UR2_CLAUDE_CLI_TIMEOUT_SECONDS", 120)),
        claude_cli_retries=max(0, _env_int("UR2_CLAUDE_CLI_RETRIES", 1)),
        llm_analysis_provider_order=_env_csv("UR2_LLM_ANALYSIS_PROVIDER_ORDER", analysis_default),
        llm_verifier_provider_order=_env_csv("UR2_LLM_VERIFIER_PROVIDER_ORDER", verifier_default),
        llm_generation_provider_order=_env_csv("UR2_LLM_GENERATION_PROVIDER_ORDER", generation_default),
        llm_supervisor_provider_order=_env_csv("UR2_LLM_SUPERVISOR_PROVIDER_ORDER", supervisor_default),
        advanced_profile_inference=_env_bool("UR2_ADVANCED_PROFILE_INFERENCE", True),
        enable_transcript_ingestion=_env_bool("UR2_ENABLE_TRANSCRIPT_INGESTION", False),
        enable_auto_approval_policy=_env_bool("UR2_ENABLE_AUTO_APPROVAL_POLICY", True),
        auto_approval_mapping_threshold=max(0.0, min(1.0, _env_float("UR2_AUTO_APPROVAL_MAPPING_THRESHOLD", 0.74))),
        auto_approval_transformation_threshold=max(
            0.0,
            min(1.0, _env_float("UR2_AUTO_APPROVAL_TRANSFORMATION_THRESHOLD", 0.72)),
        ),
        auto_approval_validation_threshold=max(0.0, min(1.0, _env_float("UR2_AUTO_APPROVAL_VALIDATION_THRESHOLD", 0.78))),
        workspace_mode_default=_env_bool("UR2_WORKSPACE_MODE_DEFAULT", True),
        collaboration_mode=collaboration_mode,
        require_claude_verifier_mappings=_env_bool("UR2_REQUIRE_CLAUDE_VERIFIER_MAPPINGS", False),
        require_claude_verifier_transformations=_env_bool("UR2_REQUIRE_CLAUDE_VERIFIER_TRANSFORMATIONS", False),
        require_claude_verifier_validations=_env_bool("UR2_REQUIRE_CLAUDE_VERIFIER_VALIDATIONS", False),
        require_claude_verifier_quality_audit=_env_bool("UR2_REQUIRE_CLAUDE_VERIFIER_QUALITY_AUDIT", False),
        profile_full_roster_learning=_env_bool("UR2_PROFILE_FULL_ROSTER_LEARNING", False),
        profile_max_rows=max(0, _env_int("UR2_PROFILE_MAX_ROWS", 0)),
        enable_async_operations=_env_bool("UR2_ENABLE_ASYNC_OPERATIONS", True),
        enable_sse_progress=_env_bool("UR2_ENABLE_SSE_PROGRESS", True),
        enable_web_debug_drawer=_env_bool("UR2_ENABLE_WEB_DEBUG_DRAWER", True),
        web_operation_poll_interval_ms=max(250, _env_int("UR2_WEB_OPERATION_POLL_INTERVAL_MS", 1500)),
        operation_retention_max_records=max(50, _env_int("UR2_OPERATION_RETENTION_MAX_RECORDS", 2000)),
        operation_log_retention=max(20, _env_int("UR2_OPERATION_LOG_RETENTION", 300)),
        operation_events_retention=max(100, _env_int("UR2_OPERATION_EVENTS_RETENTION", 2000)),
        max_concurrent_operations_per_workspace=max(1, _env_int("UR2_MAX_CONCURRENT_OPERATIONS_PER_WORKSPACE", 1)),

        enable_learning_episode_capture=_env_bool("UR2_ENABLE_LEARNING_EPISODE_CAPTURE", True),
        capture_llm_payloads=_env_bool("UR2_CAPTURE_LLM_PAYLOADS", False),
        learning_episodes_path=learning_episodes_path,
        learning_episode_payload_max_chars=max(500, _env_int("UR2_LEARNING_EPISODE_PAYLOAD_MAX_CHARS", 4000)),
        learning_episode_max_records=max(100, _env_int("UR2_LEARNING_EPISODE_MAX_RECORDS", 50000)),
        learning_episode_max_age_days=max(1, _env_int("UR2_LEARNING_EPISODE_MAX_AGE_DAYS", 180)),
        learning_episode_quarantine_on_conflict=_env_bool("UR2_LEARNING_EPISODE_QUARANTINE_ON_CONFLICT", True),

        enable_rag_retrieval=_env_bool("UR2_ENABLE_RAG_RETRIEVAL", False),
        rag_max_examples_per_item=max(1, _env_int("UR2_RAG_MAX_EXAMPLES_PER_ITEM", 3)),
        rag_min_score=max(0.0, min(1.0, _env_float("UR2_RAG_MIN_SCORE", 0.55))),
        rag_use_workspace_scope_weight=_env_bool("UR2_RAG_USE_WORKSPACE_SCOPE_WEIGHT", True),
        rag_use_roster_type_weight=_env_bool("UR2_RAG_USE_ROSTER_TYPE_WEIGHT", True),
        rag_chat_outcome_hint_weight=max(0.0, min(1.0, _env_float("UR2_RAG_CHAT_OUTCOME_HINT_WEIGHT", 0.05))),

        training_export_dir=training_export_dir,
        training_export_max_records=max(100, _env_int("UR2_TRAINING_EXPORT_MAX_RECORDS", 50000)),
        training_min_accepted_episodes=max(1, _env_int("UR2_TRAINING_MIN_ACCEPTED_EPISODES", 25)),

        trainer_command=[part for part in os.getenv("UR2_TRAINER_COMMAND", "").strip().split() if part],
        trainer_working_dir=trainer_working_dir,
        trainer_timeout_seconds=max(30, _env_int("UR2_TRAINER_TIMEOUT_SECONDS", 3600)),

        enable_shadow_trained_primary=_env_bool("UR2_ENABLE_SHADOW_TRAINED_PRIMARY", False),
        shadow_model_name=os.getenv("UR2_SHADOW_MODEL_NAME", "").strip(),
        shadow_sections=[
            token.strip().lower()
            for token in os.getenv("UR2_SHADOW_SECTIONS", "mappings,transformations,validations,quality_audit").split(",")
            if token.strip()
        ],

        enable_rationale_capture=_env_bool("UR2_ENABLE_RATIONALE_CAPTURE", False),
        enable_rationale_followup_question=_env_bool("UR2_ENABLE_RATIONALE_FOLLOWUP_QUESTION", False),
        enable_rationale_retrieval_influence=_env_bool("UR2_ENABLE_RATIONALE_RETRIEVAL_INFLUENCE", False),
        enable_rationale_training_export=_env_bool("UR2_ENABLE_RATIONALE_TRAINING_EXPORT", False),

        enable_claude_chat_supervisor=_env_bool("UR2_ENABLE_CLAUDE_CHAT_SUPERVISOR", False),
        enable_dynamic_rationale_questions=_env_bool("UR2_ENABLE_DYNAMIC_RATIONALE_QUESTIONS", False),
        enable_structured_decision_events=_env_bool("UR2_ENABLE_STRUCTURED_DECISION_EVENTS", False),
        enable_episode_finalize_idempotency=_env_bool("UR2_ENABLE_EPISODE_FINALIZE_IDEMPOTENCY", False),
        enable_qwen_stage_promotion=_env_bool("UR2_ENABLE_QWEN_STAGE_PROMOTION", False),

        qwen_quality_gate_mappings_min_accepted=max(0, _env_int("UR2_QWEN_GATE_MAPPINGS_MIN_ACCEPTED", 25)),
        qwen_quality_gate_mappings_min_acceptance_rate=max(
            0.0,
            min(1.0, _env_float("UR2_QWEN_GATE_MAPPINGS_MIN_ACCEPTANCE_RATE", 0.8)),
        ),
        qwen_quality_gate_transformations_min_accepted=max(0, _env_int("UR2_QWEN_GATE_TRANSFORMATIONS_MIN_ACCEPTED", 25)),
        qwen_quality_gate_transformations_min_acceptance_rate=max(
            0.0,
            min(1.0, _env_float("UR2_QWEN_GATE_TRANSFORMATIONS_MIN_ACCEPTANCE_RATE", 0.8)),
        ),
        qwen_quality_gate_validations_min_accepted=max(0, _env_int("UR2_QWEN_GATE_VALIDATIONS_MIN_ACCEPTED", 25)),
        qwen_quality_gate_validations_min_acceptance_rate=max(
            0.0,
            min(1.0, _env_float("UR2_QWEN_GATE_VALIDATIONS_MIN_ACCEPTANCE_RATE", 0.8)),
        ),
        qwen_quality_gate_quality_audit_min_accepted=max(0, _env_int("UR2_QWEN_GATE_QUALITY_AUDIT_MIN_ACCEPTED", 25)),
        qwen_quality_gate_quality_audit_min_acceptance_rate=max(
            0.0,
            min(1.0, _env_float("UR2_QWEN_GATE_QUALITY_AUDIT_MIN_ACCEPTANCE_RATE", 0.8)),
        ),

        quality_audit_enrichment_enabled=_env_bool("UR2_QUALITY_AUDIT_ENRICHMENT_ENABLED", False),
        quality_audit_enrichment_timeout_seconds=max(0.1, _env_float("UR2_QUALITY_AUDIT_ENRICHMENT_TIMEOUT_SECONDS", 2.0)),
        quality_audit_enrichment_cache_ttl_seconds=max(0, _env_int("UR2_QUALITY_AUDIT_ENRICHMENT_CACHE_TTL_SECONDS", 900)),
        quality_audit_nppes_enabled=_env_bool("UR2_QUALITY_AUDIT_NPPES_ENABLED", False),
        quality_audit_nppes_endpoint=os.getenv("UR2_QUALITY_AUDIT_NPPES_ENDPOINT", "").strip(),
        quality_audit_nppes_api_key=os.getenv("UR2_QUALITY_AUDIT_NPPES_API_KEY", "").strip(),
        quality_audit_nucc_enabled=_env_bool("UR2_QUALITY_AUDIT_NUCC_ENABLED", False),
        quality_audit_nucc_endpoint=os.getenv("UR2_QUALITY_AUDIT_NUCC_ENDPOINT", "").strip(),
        quality_audit_nucc_api_key=os.getenv("UR2_QUALITY_AUDIT_NUCC_API_KEY", "").strip(),
        quality_audit_client_refs_enabled=_env_bool("UR2_QUALITY_AUDIT_CLIENT_REFS_ENABLED", False),
        quality_audit_client_refs_endpoint=os.getenv("UR2_QUALITY_AUDIT_CLIENT_REFS_ENDPOINT", "").strip(),
        quality_audit_client_refs_api_key=os.getenv("UR2_QUALITY_AUDIT_CLIENT_REFS_API_KEY", "").strip(),
        quality_audit_service_account_key_path=os.getenv("UR2_QUALITY_AUDIT_SERVICE_ACCOUNT_KEY_PATH", "").strip(),
        quality_audit_bq_enabled=_env_bool("UR2_QUALITY_AUDIT_BQ_ENABLED", False),
        quality_audit_bq_project_id=os.getenv("UR2_QUALITY_AUDIT_BQ_PROJECT_ID", "").strip(),
        quality_audit_bq_dataset=os.getenv("UR2_QUALITY_AUDIT_BQ_DATASET", "").strip(),

        demo_mode=_env_bool("UR2_DEMO_MODE", False),

        enable_gemini=_env_bool("UR2_ENABLE_GEMINI", False),
        gemini_pro_model=os.getenv("UR2_GEMINI_PRO_MODEL", "gemini-2.5-pro").strip() or "gemini-2.5-pro",
        gemini_flash_model=os.getenv("UR2_GEMINI_FLASH_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash",
        gemini_api_key=os.getenv("UR2_GEMINI_API_KEY", "").strip(),
        gemini_service_account_key_path=os.getenv("UR2_GEMINI_SERVICE_ACCOUNT_KEY_PATH", "").strip(),
        gemini_location=os.getenv("UR2_GEMINI_LOCATION", "us-central1").strip() or "us-central1",
        gemini_project_id=os.getenv("UR2_GEMINI_PROJECT_ID", "").strip(),
    )
