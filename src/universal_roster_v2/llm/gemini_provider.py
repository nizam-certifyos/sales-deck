"""Gemini LLM provider via Vertex AI with context caching for speed."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

from universal_roster_v2.config import Settings, get_settings
from universal_roster_v2.llm.providers import BaseLLMProvider, LLMResponse
from universal_roster_v2.llm.knowledge_loader import (
    build_codegen_system_prompt,
    build_mapping_system_prompt,
    build_quality_audit_system_prompt,
    build_transform_system_prompt,
    build_validation_system_prompt,
)

_TASK_SYSTEM_PROMPTS = {
    "analysis": build_mapping_system_prompt,
    "mapping": build_mapping_system_prompt,
    "mappings": build_mapping_system_prompt,
    "verifier": build_mapping_system_prompt,
    "verification": build_mapping_system_prompt,
    "transformation": build_transform_system_prompt,
    "transformations": build_transform_system_prompt,
    "transform": build_transform_system_prompt,
    "validation": build_validation_system_prompt,
    "validations": build_validation_system_prompt,
    "bq_validation": build_validation_system_prompt,
    "quality_audit": build_quality_audit_system_prompt,
    "quality_audits": build_quality_audit_system_prompt,
    "quality": build_quality_audit_system_prompt,
    "audit": build_quality_audit_system_prompt,
    "generation": build_codegen_system_prompt,
    "codegen": build_codegen_system_prompt,
    "supervisor": build_quality_audit_system_prompt,
    "chat_supervisor": build_quality_audit_system_prompt,
    "chat_review": build_quality_audit_system_prompt,
}

# Module-level cache: maps (model, prompt_hash) -> cache resource name
_context_cache: Dict[str, str] = {}
_cache_warmup_done = False


def _get_system_prompt(task_type: str) -> str:
    key = (task_type or "analysis").strip().lower()
    builder = _TASK_SYSTEM_PROMPTS.get(key, build_mapping_system_prompt)
    return builder()


def _select_model(task_type: str, settings: Settings) -> str:
    """Use Flash by default for speed. Pro only for verification tasks where quality is critical."""
    key = (task_type or "analysis").strip().lower()
    if key in {"verifier", "verification"}:
        return settings.gemini_pro_model
    return settings.gemini_flash_model


class GeminiVertexProvider(BaseLLMProvider):
    """Gemini provider with optional context caching for system prompts."""

    name = "gemini_vertex"

    def __init__(self, settings: Optional[Settings] = None, model: Optional[str] = None):
        self.settings = settings or get_settings()
        self._model_override = model
        self._client = None

    def _get_client(self):
        if self._client is not None:
            return self._client
        try:
            from google import genai
            from google.oauth2 import service_account

            sa_path = self.settings.gemini_service_account_key_path
            logging.warning(f"GEMINI INIT: sa_path={sa_path!r}, exists={os.path.isfile(sa_path) if sa_path else False}, enable={self.settings.enable_gemini}")
            if sa_path and os.path.isfile(sa_path):
                sa_info = json.load(open(sa_path))
                project_id = self.settings.gemini_project_id or sa_info.get("project_id", "")
                creds = service_account.Credentials.from_service_account_info(
                    sa_info,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
                self._client = genai.Client(
                    vertexai=True,
                    project=project_id,
                    location=self.settings.gemini_location,
                    credentials=creds,
                )
            else:
                api_key = self.settings.gemini_api_key
                if api_key:
                    self._client = genai.Client(api_key=api_key)
                else:
                    self._client = genai.Client(
                        vertexai=True,
                        project=self.settings.gemini_project_id or os.getenv("GOOGLE_CLOUD_PROJECT", "certifyos-development"),
                        location=self.settings.gemini_location,
                    )

            return self._client
        except Exception as exc:
            raise RuntimeError(f"Failed to initialize Gemini client: {exc}")

    def is_available(self) -> bool:
        if not self.settings.enable_gemini:
            logging.warning("GEMINI: enable_gemini is False")
            return False
        sa_path = self.settings.gemini_service_account_key_path
        api_key = self.settings.gemini_api_key
        if sa_path and os.path.isfile(sa_path):
            return True
        if api_key:
            return True
        try:
            from google.auth import default as google_auth_default
            creds, project = google_auth_default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            logging.warning(f"GEMINI: ADC available, project={project}, creds_type={type(creds).__name__}")
            return True
        except Exception as e:
            logging.error(f"GEMINI: ADC failed: {e}")
            return False

    def warmup_cache(self) -> None:
        """Pre-create context caches for all task types at startup. Non-blocking."""
        global _cache_warmup_done
        if _cache_warmup_done:
            return
        _cache_warmup_done = True

        try:
            client = self._get_client()
            model = self.settings.gemini_flash_model
            # Only cache the mapping prompt (largest, most frequently used)
            system_prompt = build_mapping_system_prompt()
            self._create_cache(client, model, system_prompt)
            logging.warning("GEMINI WARMUP: mapping cache ready")
        except Exception as e:
            logging.warning(f"GEMINI WARMUP failed (non-fatal): {e}")

    def _cache_key(self, model_name: str, system_prompt: str) -> str:
        prompt_hash = hashlib.sha256(system_prompt.encode()).hexdigest()[:16]
        return f"{model_name}:{prompt_hash}"

    def _create_cache(self, client, model_name: str, system_prompt: str) -> Optional[str]:
        """Create a context cache. Returns cache name or None."""
        global _context_cache
        from google.genai import types

        key = self._cache_key(model_name, system_prompt)
        if key in _context_cache:
            return _context_cache[key]

        try:
            cached = client.caches.create(
                model=model_name,
                config=types.CreateCachedContentConfig(
                    system_instruction=system_prompt,
                    display_name=f"ur2-{key}",
                    ttl="3600s",
                ),
            )
            _context_cache[key] = cached.name
            logging.warning(f"GEMINI CACHE CREATED: {cached.name}")
            return cached.name
        except Exception as e:
            logging.warning(f"GEMINI CACHE failed: {e}")
            return None

    def _get_cache(self, model_name: str, system_prompt: str) -> Optional[str]:
        """Get existing cache name or None (no API calls if not cached)."""
        key = self._cache_key(model_name, system_prompt)
        return _context_cache.get(key)

    def generate(self, prompt: str, task_type: str = "analysis") -> LLMResponse:
        if not self.is_available():
            raise RuntimeError("Gemini provider not available (missing credentials or disabled)")

        from google.genai import types
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

        client = self._get_client()
        model_name = self._model_override or _select_model(task_type, self.settings)
        system_prompt = _get_system_prompt(task_type)
        use_json = task_type not in {"generation", "codegen"}

        # Check for pre-warmed cache (no API call — just memory lookup)
        cache_name = self._get_cache(model_name, system_prompt)

        def _build_config(use_cache_name: Optional[str] = None):
            if use_cache_name:
                return types.GenerateContentConfig(
                    cached_content=use_cache_name,
                    temperature=0.1,
                    max_output_tokens=16384,
                    response_mime_type="application/json" if use_json else "text/plain",
                )
            return types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=0.1,
                max_output_tokens=16384,
                response_mime_type="application/json" if use_json else "text/plain",
            )

        config = _build_config(cache_name)

        logging.warning(
            f"GEMINI CALL: model={model_name}, task={task_type}, "
            f"prompt_chars={len(prompt)}, system_chars={len(system_prompt)}, "
            f"cached={'yes' if cache_name else 'no'}, json={use_json}"
        )
        t0 = time.time()

        _TIMEOUT_SECONDS = 120

        def _call(cfg):
            return client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=cfg,
            )

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_call, config)
            try:
                response = future.result(timeout=_TIMEOUT_SECONDS)
            except FuturesTimeoutError:
                elapsed = time.time() - t0
                logging.error(f"GEMINI TIMEOUT: {elapsed:.1f}s > {_TIMEOUT_SECONDS}s for task={task_type}, model={model_name}")
                raise RuntimeError(f"Gemini call timed out after {_TIMEOUT_SECONDS}s for task={task_type}")
            except Exception as exc:
                # If cached call failed (e.g. cache expired), retry WITHOUT cache
                if cache_name:
                    elapsed = time.time() - t0
                    logging.warning(f"GEMINI CACHE EXPIRED: {elapsed:.1f}s, retrying without cache for task={task_type}")
                    # Remove stale cache from registry
                    key = self._cache_key(model_name, system_prompt)
                    _context_cache.pop(key, None)
                    # Recreate cache for next time (async-ish, best effort)
                    try:
                        self._create_cache(client, model_name, system_prompt)
                    except Exception:
                        pass
                    # Retry without cache
                    config = _build_config(None)
                    t0 = time.time()
                    future2 = executor.submit(_call, config)
                    try:
                        response = future2.result(timeout=_TIMEOUT_SECONDS)
                    except FuturesTimeoutError:
                        elapsed = time.time() - t0
                        logging.error(f"GEMINI TIMEOUT (retry): {elapsed:.1f}s")
                        raise RuntimeError(f"Gemini call timed out after {_TIMEOUT_SECONDS}s for task={task_type}")
                    except Exception as exc2:
                        elapsed = time.time() - t0
                        logging.error(f"GEMINI ERROR (retry): {elapsed:.1f}s, error={exc2}")
                        raise
                else:
                    elapsed = time.time() - t0
                    logging.error(f"GEMINI ERROR: {elapsed:.1f}s, task={task_type}, model={model_name}, error={exc}")
                    raise

        elapsed = time.time() - t0
        text = response.text.strip() if response.text else ""
        logging.warning(
            f"GEMINI OK: {elapsed:.1f}s, task={task_type}, model={model_name}, "
            f"cached={'yes' if cache_name else 'no'}, response_chars={len(text)}"
        )

        if not text:
            raise RuntimeError("Gemini returned empty response")

        return LLMResponse(
            text=text,
            provider=self.name,
            model=model_name,
            metadata={
                "task_type": task_type,
                "transport": "vertex_ai",
                "system_prompt_chars": len(system_prompt),
                "elapsed_seconds": round(elapsed, 1),
                "cached": bool(cache_name),
            },
        )


class GeminiFlashProvider(BaseLLMProvider):
    """Dedicated Gemini Flash provider for fast tasks."""

    name = "gemini_flash"

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self._inner = GeminiVertexProvider(settings=self.settings)

    def is_available(self) -> bool:
        return self._inner.is_available()

    def generate(self, prompt: str, task_type: str = "analysis") -> LLMResponse:
        self._inner._model_override = self.settings.gemini_flash_model
        resp = self._inner.generate(prompt, task_type)
        resp.provider = self.name
        return resp


class GeminiProProvider(BaseLLMProvider):
    """Dedicated Gemini Pro provider for high-quality tasks."""

    name = "gemini_pro"

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self._inner = GeminiVertexProvider(settings=self.settings)

    def is_available(self) -> bool:
        return self._inner.is_available()

    def generate(self, prompt: str, task_type: str = "analysis") -> LLMResponse:
        self._inner._model_override = self.settings.gemini_pro_model
        resp = self._inner.generate(prompt, task_type)
        resp.provider = self.name
        return resp
