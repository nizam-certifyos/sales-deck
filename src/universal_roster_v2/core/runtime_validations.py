"""Runtime validation execution for generated processor output."""

from __future__ import annotations

import re
from typing import Dict, List, Tuple

import pandas as pd


def _is_blank(value) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    return text == "" or text.lower() == "nan"


def _check_runtime_rule(value, runtime_meta: Dict) -> bool:
    kind = str(runtime_meta.get("kind", "") or "").lower()
    if kind == "required":
        return _is_blank(value)

    if _is_blank(value):
        return False

    text = str(value).strip()

    if kind == "pattern":
        pattern = str(runtime_meta.get("pattern", "") or "")
        if not pattern:
            return False
        try:
            return re.fullmatch(pattern, text) is None
        except re.error:
            return False

    if kind == "enum":
        values = {str(v).strip().lower() for v in (runtime_meta.get("values") or []) if str(v).strip()}
        if not values:
            return False
        return text.lower() not in values

    if kind == "format":
        fmt = str(runtime_meta.get("format", "") or "").lower()
        if fmt == "date":
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
                return False
            return True
        if fmt == "email":
            return re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", text) is None

    return False


def run_runtime_validations(output_df: pd.DataFrame, validations: List[Dict]) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    failed_rows: List[Dict] = []
    failed_indices = set()

    for idx, row in output_df.iterrows():
        messages: List[str] = []
        warnings: List[str] = []

        for rule in validations:
            if not bool(rule.get("approved", False)):
                continue
            target_field = str(rule.get("target_field", "") or "").strip()
            if not target_field or target_field not in output_df.columns:
                continue

            runtime_meta = rule.get("runtime") or {}
            if _check_runtime_rule(row.get(target_field), runtime_meta):
                message = str(rule.get("message", "Validation failed") or "Validation failed")
                severity = str(rule.get("severity", "error") or "error").lower()
                if severity == "warning":
                    warnings.append(message)
                else:
                    messages.append(message)

        if messages or warnings:
            payload = row.to_dict()
            payload["Business_Validations"] = ", ".join(messages) if messages else ""
            payload["Warning"] = ", ".join(warnings) if warnings else ""
            failed_rows.append(payload)
            if messages:
                failed_indices.add(idx)

    errors_df = pd.DataFrame(failed_rows) if failed_rows else pd.DataFrame(columns=list(output_df.columns) + ["Business_Validations", "Warning"])
    clean_df = output_df.drop(index=list(failed_indices)).copy() if failed_indices else output_df.copy()

    stats = {
        "total_records": int(len(output_df)),
        "clean_records": int(len(clean_df)),
        "error_records": int(len(errors_df)),
    }

    return clean_df, errors_df, stats
