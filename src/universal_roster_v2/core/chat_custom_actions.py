"""Natural-language parser for preview-first custom chat actions."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

from universal_roster_v2.core.mapping import extract_json_object
from universal_roster_v2.core.schema import SchemaRegistry
from universal_roster_v2.llm.router import LLMRouter


_CONFIRM_WORDS = {
    "apply",
    "confirm",
    "yes",
    "y",
    "ok",
    "okay",
    "go ahead",
    "do it",
    "proceed",
}

_CANCEL_WORDS = {
    "cancel",
    "no",
    "n",
    "stop",
    "never mind",
    "nevermind",
    "dont apply",
    "don't apply",
}


def normalize_identifier(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    text = text.strip("`\"' ")
    text = re.sub(r"\s+", "", text)
    return text


def _quoted_literals(text: str) -> List[str]:
    values: List[str] = []
    for match in re.finditer(r"(['\"])(.*?)\1", text):
        value = str(match.group(2) or "").strip()
        if value:
            values.append(value)
    return values


def _split_value_list(raw: str) -> List[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    text = re.sub(r"\b(and|or)\b", ",", text, flags=re.IGNORECASE)
    parts = [part.strip(" \t\n\r\"'") for part in re.split(r"[,/;|]", text)]
    return [part for part in parts if part]


def _normalize_values(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        key = str(value or "").strip()
        if not key:
            continue
        if key.lower() in seen:
            continue
        seen.add(key.lower())
        out.append(key)
    return out


def _match_schema_field(schema_registry: SchemaRegistry, roster_type: str, requested: str) -> Optional[str]:
    candidate = normalize_identifier(requested)
    if not candidate:
        return None
    if schema_registry.is_valid_field(candidate, roster_type):
        return candidate

    # case-insensitive exact fallback
    candidate_l = candidate.lower()
    for field in schema_registry.list_fields(roster_type):
        if str(field).lower() == candidate_l:
            return str(field)

    return None


def _parse_required_validation(text: str, schema_registry: SchemaRegistry, roster_type: str) -> Optional[Dict[str, Any]]:
    match = re.search(
        r"(?:add\s+validation[:\-]?\s*)?(?:for\s+)?(?:field|column)?\s*([A-Za-z0-9_`\"'\.\-]+)\s*(?:is\s+)?required\b",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    target_candidate = normalize_identifier(match.group(1))
    target_field = _match_schema_field(schema_registry, roster_type, target_candidate)
    if not target_field:
        return {
            "action_type": "clarify",
            "confidence": 0.45,
            "preview_text": "I can add that required validation, but I couldn’t match the field name. Tell me the exact target field.",
            "apply_payload": {
                "kind": "required_field",
                "target_field": target_candidate,
                "reason": "NL custom required validation",
            },
            "clarification": {
                "reason": "unknown_target_field",
                "target_field": target_candidate,
            },
        }

    return {
        "action_type": "custom_validation",
        "confidence": 0.96,
        "preview_text": f"Add required validation for {target_field}.",
        "apply_payload": {
            "kind": "required_field",
            "target_field": target_field,
            "reason": "NL custom required validation",
        },
    }


def _parse_enum_validation(text: str, schema_registry: SchemaRegistry, roster_type: str) -> Optional[Dict[str, Any]]:
    match = re.search(
        (
            r"(?:add\s+validation[:\-]?\s*)?(?:for\s+)?(?:field|column)?\s*"
            r"([A-Za-z0-9_`\"'\.\-]+)\s*(?:must\s+be\s+one\s+of|in)\s+(.+)$"
        ),
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    target_candidate = normalize_identifier(match.group(1))
    values_text = str(match.group(2) or "").strip().rstrip(".")

    values = _quoted_literals(values_text) or _split_value_list(values_text)
    values = _normalize_values(values)
    if not values:
        return {
            "action_type": "clarify",
            "confidence": 0.42,
            "preview_text": "I can add the enum validation, but I couldn’t parse the allowed values. Please list them comma-separated.",
            "apply_payload": {
                "kind": "enum_values",
                "target_field": target_candidate,
                "values": [],
                "reason": "NL custom enum validation",
            },
            "clarification": {
                "reason": "missing_enum_values",
                "target_field": target_candidate,
            },
        }

    target_field = _match_schema_field(schema_registry, roster_type, target_candidate)
    if not target_field:
        return {
            "action_type": "clarify",
            "confidence": 0.45,
            "preview_text": "I can add that enum validation, but I couldn’t match the field name. Tell me the exact target field.",
            "apply_payload": {
                "kind": "enum_values",
                "target_field": target_candidate,
                "values": values,
                "reason": "NL custom enum validation",
            },
            "clarification": {
                "reason": "unknown_target_field",
                "target_field": target_candidate,
            },
        }

    value_preview = ", ".join(values[:6]) + (" ..." if len(values) > 6 else "")
    return {
        "action_type": "custom_validation",
        "confidence": 0.93,
        "preview_text": f"Add enum validation for {target_field}: allowed values [{value_preview}].",
        "apply_payload": {
            "kind": "enum_values",
            "target_field": target_field,
            "values": values,
            "reason": "NL custom enum validation",
        },
    }


def _parse_value_map_transformation(text: str) -> Optional[Dict[str, Any]]:
    patterns = [
        (
            r"(?:for\s+)?(?:column|field)\s*([A-Za-z0-9_`\"'\.\-]+)\s*"
            r"(?:convert|map|change|replace|normalize|transform|transfrom)\s*"
            r"(.+?)\s*(?:to|into|as)\s*(['\"]?[^'\"]+['\"]?)\s*$"
        ),
        (
            r"(?:convert|map|change|replace|normalize|transform|transfrom)\s*"
            r"(?:value[s]?\s+)?(.+?)\s*(?:to|into|as)\s*(['\"]?[^'\"]+['\"]?)\s*"
            r"(?:for|in)\s*(?:column|field)\s*([A-Za-z0-9_`\"'\.\-]+)\s*$"
        ),
    ]

    for idx, pattern in enumerate(patterns):
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue

        if idx == 0:
            source_column = normalize_identifier(match.group(1))
            from_values_text = str(match.group(2) or "")
            target_value_raw = str(match.group(3) or "")
        else:
            from_values_text = str(match.group(1) or "")
            target_value_raw = str(match.group(2) or "")
            source_column = normalize_identifier(match.group(3))

        target_value = str(target_value_raw).strip(" \t\n\r\"'")
        if not source_column or not target_value:
            return None

        from_values = _quoted_literals(from_values_text) or _split_value_list(from_values_text)
        from_values = _normalize_values(from_values)
        if not from_values:
            return None

        value_map = {value: target_value for value in from_values}
        source_preview = ", ".join(from_values[:6]) + (" ..." if len(from_values) > 6 else "")

        return {
            "action_type": "custom_transformation",
            "confidence": 0.95,
            "preview_text": (
                f"Add transformation on {source_column}: map [{source_preview}] -> {target_value} "
                "using normalize_enum value_map."
            ),
            "apply_payload": {
                "kind": "value_map",
                "name": "normalize_enum",
                "source_columns": [source_column],
                "target_fields": [],
                "params": {
                    "value_map": value_map,
                    "strip": True,
                },
                "reason": "NL custom value conversion",
            },
        }

    return None


def _parse_date_normalization(text: str) -> Optional[Dict[str, Any]]:
    match = re.search(
        r"(?:normalize|convert|format)\s*(?:column|field)?\s*([A-Za-z0-9_`\"'\.\-]+)\s*(?:to|as)\s*(?:yyyy-mm-dd|%y-%m-%d|%Y-%m-%d)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    source_column = normalize_identifier(match.group(1))
    if not source_column:
        return None

    return {
        "action_type": "custom_transformation",
        "confidence": 0.9,
        "preview_text": f"Add date normalization for {source_column} to YYYY-MM-DD.",
        "apply_payload": {
            "kind": "normalize_date",
            "name": "normalize_date",
            "source_columns": [source_column],
            "target_fields": [],
            "params": {"target_format": "%Y-%m-%d"},
            "reason": "NL custom date normalization",
        },
    }


def _apply_llm_fallback(
    text: str,
    *,
    roster_type: str,
    schema_registry: SchemaRegistry,
    parser_router: Optional[LLMRouter],
) -> Optional[Dict[str, Any]]:
    if parser_router is None:
        return None

    prompt = f"""
You parse chat instructions into strict JSON for roster custom actions.
Return JSON only with keys: action_type, confidence, preview_text, apply_payload.

Allowed action_type values:
- custom_transformation
- custom_validation
- clarify
- none

Constraints:
- Never return free-form SQL from user text.
- For custom_transformation, allowed names: normalize_enum, normalize_date.
- Use normalize_enum with params.value_map for value conversion requests.
- For custom_validation, allowed payload kinds: required_field, enum_values.
- If unclear, return action_type=clarify.
- If unrelated, return action_type=none.

Roster type: {roster_type}
Known schema fields (subset): {json.dumps(schema_registry.list_fields(roster_type)[:400], ensure_ascii=False)}
User message: {text}

Return shape example:
{{
  "action_type": "custom_validation",
  "confidence": 0.9,
  "preview_text": "Add required validation for practitionerNpi.",
  "apply_payload": {{"kind": "required_field", "target_field": "practitionerNpi", "reason": "..."}}
}}
""".strip()

    try:
        routed = parser_router.generate(prompt=prompt, task_type="analysis")
    except Exception:
        return None

    payload = extract_json_object(routed.response.text)
    if not isinstance(payload, dict):
        return None

    action_type = str(payload.get("action_type") or "none").strip().lower()
    if action_type not in {"custom_transformation", "custom_validation", "clarify", "none"}:
        return None

    apply_payload = payload.get("apply_payload") if isinstance(payload.get("apply_payload"), dict) else {}
    confidence_raw = payload.get("confidence")
    try:
        confidence = float(confidence_raw)
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    preview_text = str(payload.get("preview_text") or "").strip()
    if action_type != "none" and not preview_text:
        preview_text = "I parsed a custom action."

    return {
        "action_type": action_type,
        "confidence": confidence,
        "preview_text": preview_text,
        "apply_payload": apply_payload,
    }


def parse_custom_chat_action(
    text: str,
    *,
    roster_type: str,
    schema_registry: SchemaRegistry,
    parser_router: Optional[LLMRouter] = None,
) -> Dict[str, Any]:
    message = str(text or "").strip()
    if not message:
        return {
            "action_type": "none",
            "confidence": 0.0,
            "preview_text": "",
            "apply_payload": {},
        }

    deterministic_checks = [
        lambda: _parse_value_map_transformation(message),
        lambda: _parse_date_normalization(message),
        lambda: _parse_required_validation(message, schema_registry=schema_registry, roster_type=roster_type),
        lambda: _parse_enum_validation(message, schema_registry=schema_registry, roster_type=roster_type),
    ]
    for check in deterministic_checks:
        parsed = check()
        if parsed is not None:
            return parsed

    llm_parsed = _apply_llm_fallback(
        message,
        roster_type=roster_type,
        schema_registry=schema_registry,
        parser_router=parser_router,
    )
    if llm_parsed is not None:
        return llm_parsed

    return {
        "action_type": "none",
        "confidence": 0.0,
        "preview_text": "",
        "apply_payload": {},
    }


def is_confirm_message(text: str) -> bool:
    normalized = " ".join(str(text or "").strip().lower().split())
    if not normalized:
        return False
    return normalized in _CONFIRM_WORDS


def is_cancel_message(text: str) -> bool:
    normalized = " ".join(str(text or "").strip().lower().split())
    if not normalized:
        return False
    return normalized in _CANCEL_WORDS
