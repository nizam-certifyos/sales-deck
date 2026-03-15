"""LLM providers for standalone Universal Roster V2."""

from __future__ import annotations

import json
import shutil
import subprocess
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from universal_roster_v2.config import Settings, get_settings
from universal_roster_v2.llm.rate_limit import limiter_for


@dataclass
class LLMResponse:
    text: str
    provider: str
    model: str
    metadata: Dict[str, Any]


class BaseLLMProvider:
    name: str = "base"

    def is_available(self) -> bool:
        raise NotImplementedError

    def generate(self, prompt: str, task_type: str = "analysis") -> LLMResponse:
        raise NotImplementedError


class ShadowLocalOllamaProvider(BaseLLMProvider):
    """Optional shadow trained model provider (Qwen variant)."""

    name = "shadow_local_ollama"

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()

    def is_available(self) -> bool:
        if not self.settings.enable_shadow_trained_primary:
            return False
        if not str(self.settings.shadow_model_name or "").strip():
            return False
        url = f"{self.settings.ollama_base_url.rstrip('/')}/api/tags"
        req = urllib.request.Request(url=url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=3) as resp:
                _ = resp.read()
            return True
        except Exception:
            return False

    def generate(self, prompt: str, task_type: str = "analysis") -> LLMResponse:
        model_name = str(self.settings.shadow_model_name or "").strip()
        if not model_name:
            raise RuntimeError("Shadow model name not configured")

        url = f"{self.settings.ollama_base_url.rstrip('/')}/api/generate"
        payload = {
            "model": model_name,
            "prompt": prompt,
            "stream": False,
        }
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        text = str(data.get("response", "") or "").strip()
        return LLMResponse(
            text=text,
            provider=self.name,
            model=model_name,
            metadata={"task_type": task_type, "shadow": True},
        )


class LocalOllamaProvider(BaseLLMProvider):
    """Qwen-first Ollama provider using HTTP API."""

    name = "local_ollama"

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()

    def is_available(self) -> bool:
        url = f"{self.settings.ollama_base_url.rstrip('/')}/api/tags"
        req = urllib.request.Request(url=url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=3) as resp:
                _ = resp.read()
            return True
        except Exception:
            return False

    def _call_generate(self, model: str, prompt: str) -> str:
        url = f"{self.settings.ollama_base_url.rstrip('/')}/api/generate"
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
        }
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return str(data.get("response", "") or "").strip()

    def generate(self, prompt: str, task_type: str = "analysis") -> LLMResponse:
        errors: List[str] = []
        for model in self.settings.ollama_candidates(task_type):
            try:
                text = self._call_generate(model=model, prompt=prompt)
                return LLMResponse(
                    text=text,
                    provider=self.name,
                    model=model,
                    metadata={"task_type": task_type, "errors": errors},
                )
            except Exception as exc:
                errors.append(f"{model}: {exc}")

        raise RuntimeError(f"All Ollama models failed for {task_type}: {errors}")


class ClaudeCLIProvider(BaseLLMProvider):
    """Claude CLI provider with local command execution and retries."""

    name = "claude_cli"

    def __init__(self, settings: Optional[Settings] = None, model: Optional[str] = None):
        self.settings = settings or get_settings()
        self.model = (model or self.settings.claude_cli_model).strip() or self.settings.claude_cli_model

    def is_available(self) -> bool:
        return shutil.which(self.settings.claude_cli_command) is not None

    def _build_command(self) -> List[str]:
        args = [self.settings.claude_cli_command]
        cli_args = list(self.settings.claude_cli_args)
        if not cli_args:
            cli_args = ["-p"]
        args.extend(cli_args)
        if self.model:
            args.extend(["--model", self.model])
        return args

    def _try_parse_structured_output(self, output: str) -> Dict[str, Any]:
        text = str(output or "").strip()
        if not text:
            return {}
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return data
        except Exception:
            pass

        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                data = json.loads(text[start : end + 1])
                if isinstance(data, dict):
                    return data
            except Exception:
                return {}
        return {}

    def generate(self, prompt: str, task_type: str = "analysis") -> LLMResponse:
        if not self.is_available():
            raise RuntimeError("Claude CLI is not available on PATH")

        command = self._build_command()
        retries = max(0, int(self.settings.claude_cli_retries))
        timeout = max(5, int(self.settings.claude_cli_timeout_seconds))

        errors: List[str] = []
        for attempt in range(retries + 1):
            try:
                proc = subprocess.run(
                    command,
                    input=prompt,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=timeout,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                errors.append(f"attempt_{attempt}:timeout")
                continue
            except Exception as exc:
                errors.append(f"attempt_{attempt}:exec_error:{exc}")
                continue

            if proc.returncode != 0:
                err_text = (proc.stderr or "").strip() or (proc.stdout or "").strip()
                errors.append(f"attempt_{attempt}:exit_{proc.returncode}:{err_text[:240]}")
                continue

            text = (proc.stdout or "").strip()
            if not text:
                errors.append(f"attempt_{attempt}:empty_output")
                continue

            parsed = self._try_parse_structured_output(text)
            final_text = str(parsed.get("text") or text)

            return LLMResponse(
                text=final_text,
                provider=self.name,
                model=self.model,
                metadata={
                    "task_type": task_type,
                    "command": command,
                    "attempt": attempt,
                    "errors": errors,
                    "stderr": (proc.stderr or "").strip()[:500],
                },
            )

        raise RuntimeError(f"Claude CLI failed after retries: {errors}")


class GatewayClaudeProvider(BaseLLMProvider):
    """Company gateway Claude provider (OpenAI-compatible chat completions)."""

    name = "gateway_claude"

    def __init__(self, settings: Optional[Settings] = None, model: Optional[str] = None):
        self.settings = settings or get_settings()
        self.model = (model or self.settings.gateway_model).strip() or self.settings.gateway_model

    def is_available(self) -> bool:
        return bool(self.settings.enable_gateway_claude and self.settings.gateway_api_key)

    def generate(self, prompt: str, task_type: str = "analysis") -> LLMResponse:
        if not self.is_available():
            raise RuntimeError("Gateway Claude provider disabled or missing UR2_GATEWAY_API_KEY")

        limiter = limiter_for(
            self.name,
            requests_per_minute=int(self.settings.gateway_requests_per_minute),
            max_wait_seconds=float(self.settings.gateway_max_wait_seconds),
        )
        wait_meta = limiter.acquire()

        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
        }
        body = json.dumps(payload).encode("utf-8")
        base = str(self.settings.gateway_base_url or "").rstrip("/")
        url = f"{base}/chat/completions"

        req = urllib.request.Request(
            url=url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.settings.gateway_api_key}",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=int(self.settings.gateway_timeout_seconds)) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Gateway Claude HTTP error {exc.code}: {detail}")

        choices = data.get("choices") if isinstance(data, dict) else []
        message = choices[0].get("message", {}) if isinstance(choices, list) and choices else {}
        text = str(message.get("content", "") or "").strip()
        if not text:
            raise RuntimeError("Gateway Claude returned empty content")

        return LLMResponse(
            text=text,
            provider=self.name,
            model=self.model,
            metadata={
                "task_type": task_type,
                "transport": "gateway_http",
                "waited_seconds": round(float(wait_meta.waited_seconds), 4),
                "queue_depth": int(wait_meta.queue_depth),
                "gateway_base_url": base,
            },
        )


class ClaudeAPIProvider(BaseLLMProvider):
    """Claude API provider."""

    name = "claude_api"

    def __init__(self, settings: Optional[Settings] = None, model: Optional[str] = None):
        self.settings = settings or get_settings()
        default_model = self.settings.claude_fallback_model
        self.model = (model or default_model).strip() or default_model

    def is_available(self) -> bool:
        return bool(self.settings.anthropic_api_key)

    def _generate_with_sdk(self, prompt: str, task_type: str) -> Optional[LLMResponse]:
        if not self.is_available():
            return None
        try:
            import anthropic  # type: ignore

            client = anthropic.Anthropic(api_key=self.settings.anthropic_api_key)
            max_tokens = 2200 if task_type in {"analysis", "verifier", "verification"} else 3200
            response = client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            blocks = []
            for block in getattr(response, "content", []) or []:
                if getattr(block, "type", "") == "text":
                    blocks.append(getattr(block, "text", ""))
            text = "\n".join([b for b in blocks if b]).strip()
            return LLMResponse(
                text=text,
                provider=self.name,
                model=self.model,
                metadata={"task_type": task_type, "transport": "sdk"},
            )
        except Exception:
            return None

    def _generate_with_http(self, prompt: str, task_type: str) -> LLMResponse:
        if not self.is_available():
            raise RuntimeError("Missing ANTHROPIC_API_KEY")

        url = "https://api.anthropic.com/v1/messages"
        payload = {
            "model": self.model,
            "max_tokens": 2200 if task_type in {"analysis", "verifier", "verification"} else 3200,
            "messages": [{"role": "user", "content": prompt}],
        }

        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "x-api-key": self.settings.anthropic_api_key,
                "anthropic-version": "2023-06-01",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=90) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Claude API HTTP error {exc.code}: {detail}")

        parts = []
        for block in data.get("content") or []:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(str(block.get("text", "")))

        return LLMResponse(
            text="\n".join([p for p in parts if p]).strip(),
            provider=self.name,
            model=self.model,
            metadata={"task_type": task_type, "transport": "http"},
        )

    def generate(self, prompt: str, task_type: str = "analysis") -> LLMResponse:
        sdk_response = self._generate_with_sdk(prompt, task_type)
        if sdk_response is not None:
            return sdk_response
        return self._generate_with_http(prompt, task_type)
