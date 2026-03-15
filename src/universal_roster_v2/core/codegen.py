"""Code generation for schema summary, processor.py, and main.py outputs."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from pprint import pformat
from typing import Any, Dict, List

from universal_roster_v2.core.plan import PlanManager
from universal_roster_v2.core.validations import compile_bq_validation_sql


def _read_template(name: str) -> str:
    template_path = Path(__file__).resolve().parents[1] / "templates" / name
    return template_path.read_text(encoding="utf-8")


def _render_simple(template: str, context: Dict[str, Any]) -> str:
    def repl(match: re.Match) -> str:
        key = match.group(1).strip()
        return str(context.get(key, ""))

    return re.sub(r"\{\{\s*(.*?)\s*\}\}", repl, template)


def _ensure_python(code: str) -> None:
    ast.parse(code)


class CodeGenerator:
    """Generate standalone output artifacts from approved plan subset."""

    def __init__(self, plan_manager: PlanManager | None = None):
        self.plan_manager = plan_manager or PlanManager()

    def _approved_payload(self, plan: Dict[str, Any], pipeline_name: str) -> Dict[str, Any]:
        approved = self.plan_manager.approved_subset(plan)
        mapping_dict = {
            item.get("source_column"): item.get("target_field")
            for item in approved.get("mappings", [])
            if item.get("source_column") and item.get("target_field")
        }
        return {
            "approved": approved,
            "pipeline_name": pipeline_name,
            "mapping_dict": mapping_dict,
            "transformations": approved.get("transformations", []),
            "bq_validations": approved.get("bq_validations", []),
            "workspace_scope": approved.get("workspace_scope", {}),
            "instructions_context": approved.get("instructions_context", {}),
        }

    def generate(self, plan: Dict[str, Any], mode: str = "processor", pipeline_name: str = "UniversalRosterPipeline") -> Dict[str, str]:
        mode_key = (mode or "processor").strip().lower()
        if mode_key not in {"schema", "processor", "main", "full"}:
            raise ValueError("mode must be one of: schema, processor, main, full")

        payload = self._approved_payload(plan=plan, pipeline_name=pipeline_name)
        approved = payload["approved"]

        outputs: Dict[str, str] = {}

        if mode_key in {"schema", "full"}:
            summary = {
                "pipeline_name": pipeline_name,
                "roster_type": approved.get("roster_type", "practitioner"),
                "approved_counts": {
                    "mappings": len(approved.get("mappings", [])),
                    "transformations": len(approved.get("transformations", [])),
                    "bq_validations": len(approved.get("bq_validations", [])),
                },
                "confidence_summary": approved.get("confidence_summary", {}),
                "workspace_scope": approved.get("workspace_scope", {}),
                "instructions_context": approved.get("instructions_context", {}),
                "target_fields": sorted(
                    {
                        str(item.get("target_field", "") or "").strip()
                        for item in approved.get("mappings", [])
                        if str(item.get("target_field", "") or "").strip()
                    }
                ),
            }
            outputs["schema_summary.json"] = json.dumps(summary, indent=2, ensure_ascii=False)

        if mode_key in {"processor", "full"}:
            template = _read_template("processor.py.j2")
            rendered = _render_simple(
                template,
                {
                    "pipeline_name": repr(pipeline_name),
                    "roster_type": repr(approved.get("roster_type", "practitioner")),
                    "workspace_scope_json": pformat(payload["workspace_scope"], width=120, sort_dicts=False),
                    "instructions_context_json": pformat(payload["instructions_context"], width=120, sort_dicts=False),
                    "mappings_json": pformat(payload["mapping_dict"], width=120, sort_dicts=False),
                    "transformations_json": pformat(payload["transformations"], width=120, sort_dicts=False),
                    "bq_validations_json": pformat(payload["bq_validations"], width=120, sort_dicts=False),
                },
            )
            _ensure_python(rendered)
            outputs["processor.py"] = rendered

            bq_sql = compile_bq_validation_sql(payload["bq_validations"])
            outputs["bq_validations.sql"] = bq_sql

        if mode_key in {"main", "full"}:
            template = _read_template("main.py.j2")
            rendered = _render_simple(template, {})
            _ensure_python(rendered)
            outputs["main.py"] = rendered

        return outputs

    def write_outputs(self, generated: Dict[str, str], output_dir: str | Path) -> List[Path]:
        target_dir = Path(output_dir).expanduser().resolve()
        target_dir.mkdir(parents=True, exist_ok=True)
        written: List[Path] = []
        for name, content in generated.items():
            path = target_dir / name
            path.write_text(content, encoding="utf-8")
            written.append(path)
        return written
