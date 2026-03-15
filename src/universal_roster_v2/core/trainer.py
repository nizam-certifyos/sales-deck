from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from universal_roster_v2.config import Settings, get_settings


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TrainerOrchestrator:
    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()

    def _artifact_dir(self) -> Path:
        path = self.settings.training_export_dir / "trainer_runs"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _base_command(self) -> List[str]:
        return list(self.settings.trainer_command or [])

    def run(
        self,
        *,
        export_dir: Optional[str | Path] = None,
        extra_args: Optional[List[str]] = None,
        env_overrides: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        command = self._base_command()
        if not command:
            return {
                "status": "skipped",
                "reason": "trainer_command_not_configured",
                "fail_open": True,
            }

        if extra_args:
            command.extend([str(arg) for arg in extra_args])

        run_id = datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S_%f")
        artifact_dir = self._artifact_dir() / run_id
        artifact_dir.mkdir(parents=True, exist_ok=True)

        export_root = Path(export_dir or self.settings.training_export_dir).expanduser().resolve()

        env = dict(os.environ)
        env["UR2_TRAINING_EXPORT_DIR"] = str(export_root)
        if env_overrides:
            for key, value in env_overrides.items():
                env[str(key)] = str(value)

        timeout = max(30, int(self.settings.trainer_timeout_seconds or 30))
        cwd = Path(self.settings.trainer_working_dir).expanduser().resolve()

        try:
            proc = subprocess.run(
                command,
                cwd=str(cwd),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout,
                check=False,
            )
            status = "ok" if proc.returncode == 0 else "failed"
            timed_out = False
            stdout = proc.stdout or ""
            stderr = proc.stderr or ""
            return_code = int(proc.returncode)
        except subprocess.TimeoutExpired as exc:
            status = "failed"
            timed_out = True
            stdout = (exc.stdout or "") if isinstance(exc.stdout, str) else ""
            stderr = (exc.stderr or "") if isinstance(exc.stderr, str) else ""
            return_code = -1

        stdout_path = artifact_dir / "stdout.log"
        stderr_path = artifact_dir / "stderr.log"
        metadata_path = artifact_dir / "metadata.json"

        stdout_path.write_text(stdout, encoding="utf-8")
        stderr_path.write_text(stderr, encoding="utf-8")

        metadata = {
            "run_id": run_id,
            "started_at": _utc_now_iso(),
            "command": command,
            "cwd": str(cwd),
            "timeout_seconds": timeout,
            "return_code": return_code,
            "status": status,
            "timed_out": timed_out,
            "shadow": {
                "enabled": bool(self.settings.enable_shadow_trained_primary),
                "model_name": self.settings.shadow_model_name,
                "sections": list(self.settings.shadow_sections or []),
            },
        }
        metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")

        response = {
            "status": status,
            "run_id": run_id,
            "artifact_dir": str(artifact_dir),
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "metadata_path": str(metadata_path),
            "return_code": return_code,
            "timed_out": timed_out,
        }
        if status != "ok":
            response["fail_open"] = True
        return response
