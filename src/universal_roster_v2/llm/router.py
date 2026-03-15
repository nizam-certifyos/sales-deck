"""LLM routing policy with configurable provider order."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from universal_roster_v2.config import Settings, get_settings
from universal_roster_v2.llm.providers import (
    BaseLLMProvider,
    ClaudeAPIProvider,
    ClaudeCLIProvider,
    GatewayClaudeProvider,
    LLMResponse,
    LocalOllamaProvider,
    ShadowLocalOllamaProvider,
)
from universal_roster_v2.llm.gemini_provider import (
    GeminiFlashProvider,
    GeminiProProvider,
    GeminiVertexProvider,
)


@dataclass
class RouterResult:
    response: LLMResponse
    attempts: List[str]


_SECTION_TASK_KEYS = {
    "mappings": {"mapping", "mappings"},
    "transformations": {"transformation", "transformations", "transform"},
    "validations": {"validation", "validations", "bq_validation", "bq_validations"},
    "quality_audit": {"quality_audit", "quality_audits", "quality", "audit"},
}


@dataclass
class ProviderPromotionGate:
    task: str
    section: Optional[str]
    allowed: bool
    metrics: Dict[str, float]
    reasons: List[str]
    stage: str = "supervised"


class LLMRouter:
    """Try providers in order with trace capture for chat session visibility."""

    def __init__(
        self,
        providers: Optional[List[BaseLLMProvider]] = None,
        settings: Optional[Settings] = None,
        task_type: str = "analysis",
    ):
        self.settings = settings or get_settings()
        self.task_type = task_type
        self.providers = providers or self._providers_for_task(task_type)

    @staticmethod
    def _safe_rate(numerator: float, denominator: float) -> float:
        if denominator <= 0:
            return 0.0
        return max(0.0, min(1.0, float(numerator) / float(denominator)))

    def _section_for_task(self, task_key: str) -> Optional[str]:
        normalized = (task_key or "").strip().lower()
        for section, aliases in _SECTION_TASK_KEYS.items():
            if normalized in aliases:
                return section
        return None

    def _evaluate_quality_gate(self, task_key: str) -> ProviderPromotionGate:
        section = self._section_for_task(task_key)
        reasons: List[str] = []
        metrics: Dict[str, Any] = {
            "accepted": 0.0,
            "reviewed": 0.0,
            "acceptance_rate": 0.0,
            "min_accepted": 0.0,
            "min_acceptance_rate": 0.0,
            "stage": "supervised",
            "rollback": False,
            "rollback_reasons": [],
        }

        if not self.settings.enable_shadow_trained_primary:
            reasons.append("shadow_disabled")
        if not str(self.settings.shadow_model_name or "").strip():
            reasons.append("shadow_model_missing")

        normalized_shadow_sections = {
            self.settings.normalize_section_key(str(name or ""))
            for name in (self.settings.shadow_sections or [])
            if str(name or "").strip()
        }
        if section and section not in normalized_shadow_sections:
            reasons.append("section_not_enabled")

        if section is not None:
            gate = self.settings.qwen_quality_gate_for_section(section)
            min_accepted = float(gate.get("min_accepted", 0.0))
            min_rate = float(gate.get("min_acceptance_rate", 0.0))
            metrics["min_accepted"] = min_accepted
            metrics["min_acceptance_rate"] = min_rate

            accepted = 0.0
            reviewed = 0.0
            acceptance_rate = self._safe_rate(accepted, reviewed)
            accepted = float(metrics.get("accepted", accepted))
            reviewed = float(metrics.get("reviewed", reviewed))
            acceptance_rate = self._safe_rate(accepted, reviewed)
            metrics["accepted"] = accepted
            metrics["reviewed"] = reviewed
            metrics["acceptance_rate"] = acceptance_rate

            stage = str(metrics.get("stage") or "").strip().lower()
            if stage not in {"supervised", "mixed", "qwen_only_candidate"}:
                stage = "supervised"
            metrics["stage"] = stage

            if accepted < min_accepted or acceptance_rate < min_rate:
                reasons.append("quality_gate_unmet")
            if bool(metrics.get("rollback")):
                reasons.append("rollback_active")
            if stage == "supervised":
                reasons.append("stage_supervised")

        allowed = not reasons
        return ProviderPromotionGate(
            task=task_key,
            section=section,
            allowed=allowed,
            metrics=metrics,
            reasons=reasons,
            stage=str(metrics.get("stage") or "supervised"),
        )

    def _providers_for_task(self, task_type: str) -> List[BaseLLMProvider]:
        task_key = (task_type or "analysis").strip().lower()
        provider_names = self.settings.provider_order(task_key)

        gate = self._evaluate_quality_gate(task_key)
        if gate.allowed and task_key in {"analysis", "mapping", "mappings", "transformations", "validations", "quality_audit"}:
            provider_names = ["shadow_local_ollama", *provider_names]
        elif task_key in {"analysis", "mapping", "mappings", "transformations", "validations", "quality_audit"}:
            provider_names = [name for name in provider_names if name != "shadow_local_ollama"]

        if task_key in {"verifier", "verification"} and self.settings.is_strict_collaboration():
            provider_names = [
                name
                for name in provider_names
                if self.settings.is_claude_provider(name)
            ]

        providers: List[BaseLLMProvider] = []
        for name in provider_names:
            provider = self._provider_from_name(name)
            if provider is not None:
                providers.append(provider)

        if providers:
            return providers

        if task_key in {"verifier", "verification"} and self.settings.is_strict_collaboration():
            return [
                GatewayClaudeProvider(settings=self.settings),
                ClaudeCLIProvider(settings=self.settings),
                ClaudeAPIProvider(settings=self.settings),
            ]

        fallback: List[BaseLLMProvider] = []
        if self.settings.enable_shadow_trained_primary and task_key in set(self.settings.shadow_sections or []):
            fallback.append(ShadowLocalOllamaProvider(settings=self.settings))
        fallback.extend([
            LocalOllamaProvider(settings=self.settings),
            GatewayClaudeProvider(settings=self.settings),
            ClaudeCLIProvider(settings=self.settings),
            ClaudeAPIProvider(settings=self.settings),
        ])
        return fallback

    def _provider_from_name(self, name: str) -> Optional[BaseLLMProvider]:
        key = (name or "").strip().lower()
        if key == "shadow_local_ollama":
            return ShadowLocalOllamaProvider(settings=self.settings)
        if key == "local_ollama":
            return LocalOllamaProvider(settings=self.settings)
        if key == "claude_cli":
            return ClaudeCLIProvider(settings=self.settings)
        if key == "claude_api":
            return ClaudeAPIProvider(settings=self.settings)
        if key == "gateway_claude":
            return GatewayClaudeProvider(settings=self.settings)
        if key == "gemini_vertex":
            return GeminiVertexProvider(settings=self.settings)
        if key == "gemini_pro":
            return GeminiProProvider(settings=self.settings)
        if key == "gemini_flash":
            return GeminiFlashProvider(settings=self.settings)
        return None

    def provider_names(self) -> List[str]:
        return [provider.name for provider in self.providers]

    def generate(self, prompt: str, task_type: str = "analysis") -> RouterResult:
        attempts: List[str] = []
        last_error = None

        for provider in self.providers:
            if not provider.is_available():
                attempts.append(f"{provider.name}:unavailable")
                continue

            try:
                response = provider.generate(prompt=prompt, task_type=task_type)
                attempts.append(f"{provider.name}:ok:{response.model}")
                return RouterResult(response=response, attempts=attempts)
            except Exception as exc:
                attempts.append(f"{provider.name}:error")
                last_error = exc

        if last_error is not None:
            raise RuntimeError(f"All LLM providers failed. Attempts={attempts}. Last error={last_error}")
        raise RuntimeError(f"No available LLM providers. Attempts={attempts}")


class LLMRouterFactory:
    """Build task-specific routers from settings."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self._cache: Dict[str, LLMRouter] = {}
        self._quality_gate_overrides: Dict[str, Dict[str, Any]] = {}

    def set_quality_gate_metrics(self, section: str, metrics: Optional[Dict[str, float]]) -> None:
        key = self.settings.normalize_section_key(section)
        if not key:
            return
        if not isinstance(metrics, dict):
            self._quality_gate_overrides.pop(key, None)
        else:
            self._quality_gate_overrides[key] = {
                "accepted": float(metrics.get("accepted", 0.0)),
                "reviewed": float(metrics.get("reviewed", 0.0)),
                "acceptance_rate": float(metrics.get("acceptance_rate", 0.0)),
                "contradictions": float(metrics.get("contradictions", 0.0)),
                "clarification_unresolved": float(metrics.get("clarification_unresolved", 0.0)),
                "stage": str(metrics.get("stage") or "supervised"),
                "rollback": bool(metrics.get("rollback", False)),
                "rollback_reasons": metrics.get("rollback_reasons") or [],
            }
        self._cache = {}

    def for_task(self, task_type: str) -> LLMRouter:
        key = (task_type or "analysis").strip().lower()
        if key not in self._cache:
            router = LLMRouter(settings=self.settings, task_type=key)
            section = router._section_for_task(key)
            if section and section in self._quality_gate_overrides:
                metrics = self._quality_gate_overrides.get(section, {})
                gate = router._evaluate_quality_gate(key)
                gate.metrics["accepted"] = float(metrics.get("accepted", 0.0))
                gate.metrics["reviewed"] = float(metrics.get("reviewed", 0.0))
                gate.metrics["acceptance_rate"] = float(
                    metrics.get("acceptance_rate", router._safe_rate(gate.metrics["accepted"], gate.metrics["reviewed"]))
                )
                gate.metrics["stage"] = str(metrics.get("stage") or gate.metrics.get("stage") or "supervised")
                gate.metrics["rollback"] = bool(metrics.get("rollback", False))
                gate.metrics["rollback_reasons"] = metrics.get("rollback_reasons") or []

                unmet = (
                    gate.metrics["accepted"] < gate.metrics["min_accepted"]
                    or gate.metrics["acceptance_rate"] < gate.metrics["min_acceptance_rate"]
                )
                if unmet and "quality_gate_unmet" not in gate.reasons:
                    gate.reasons.append("quality_gate_unmet")
                if not unmet and "quality_gate_unmet" in gate.reasons:
                    gate.reasons = [reason for reason in gate.reasons if reason != "quality_gate_unmet"]

                stage = str(gate.metrics.get("stage") or "supervised").strip().lower()
                if stage == "supervised" and "stage_supervised" not in gate.reasons:
                    gate.reasons.append("stage_supervised")
                if stage != "supervised" and "stage_supervised" in gate.reasons:
                    gate.reasons = [reason for reason in gate.reasons if reason != "stage_supervised"]

                if bool(gate.metrics.get("rollback")) and "rollback_active" not in gate.reasons:
                    gate.reasons.append("rollback_active")
                if not bool(gate.metrics.get("rollback")) and "rollback_active" in gate.reasons:
                    gate.reasons = [reason for reason in gate.reasons if reason != "rollback_active"]

                gate.allowed = not gate.reasons
                gate.stage = stage
                if gate.allowed and "shadow_local_ollama" not in router.provider_names():
                    router.providers = [ShadowLocalOllamaProvider(settings=self.settings), *router.providers]
                if not gate.allowed:
                    router.providers = [provider for provider in router.providers if provider.name != "shadow_local_ollama"]
            self._cache[key] = router
        return self._cache[key]
