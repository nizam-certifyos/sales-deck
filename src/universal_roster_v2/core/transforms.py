"""Schema-driven transformation suggestion engine."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

from universal_roster_v2.config import Settings, get_settings
from universal_roster_v2.core.learning_kb import LearningKB
from universal_roster_v2.core.learning_retrieval import LearningRetrieval
from universal_roster_v2.core.mapping import clean_note_snippets, confidence_band, extract_json_object
from universal_roster_v2.core.schema import SchemaRegistry
from universal_roster_v2.llm.router import LLMRouter, LLMRouterFactory


TRANSFORM_CATALOG: Dict[str, Dict[str, Any]] = {
    "normalize_date": {"description": "Convert values to YYYY-MM-DD", "priority": "high"},
    "normalize_npi": {"description": "Normalize NPI values to 10 digits", "priority": "high"},
    "normalize_tin": {"description": "Normalize TIN values to 9 digits", "priority": "high"},
    "normalize_ssn": {"description": "Normalize SSN values to 9 digits", "priority": "high"},
    "normalize_phone": {"description": "Normalize phone/fax to 10 digits", "priority": "medium"},
    "normalize_zip": {"description": "Normalize ZIP to 5 or 9 digits", "priority": "medium"},
    "normalize_state": {"description": "Normalize state to 2-letter code", "priority": "medium"},
    "normalize_enum": {"description": "Normalize source values into schema enum domain", "priority": "medium"},
    "split_hours": {"description": "Split office-hour range values", "priority": "high"},
    "split_multivalue": {"description": "Split delimited lists", "priority": "high"},
    "review": {"description": "Manual review required", "priority": "high"},
}


def _infer_transform(target_field: str, source_column: str, meta: Dict[str, Any], samples: List[str]) -> str:
    target = (target_field or "").lower()
    source = (source_column or "").lower()

    if str(meta.get("format") or "").lower() == "date":
        return "normalize_date"
    if str(meta.get("format") or "").lower() == "email":
        return "normalize_enum"

    pattern = str(meta.get("pattern") or "")
    if "\\d{10}" in pattern and "npi" in target:
        return "normalize_npi"
    if "\\d{9}" in pattern and "tin" in target:
        return "normalize_tin"
    if "\\d{9}" in pattern and "ssn" in target:
        return "normalize_ssn"

    if "npi" in target:
        return "normalize_npi"
    if "tin" in target:
        return "normalize_tin"
    if "ssn" in target:
        return "normalize_ssn"
    if "phone" in target or "fax" in target:
        return "normalize_phone"
    if "zip" in target:
        return "normalize_zip"
    if "state" in target:
        return "normalize_state"
    if any(token in source for token in ["hours", "open", "close"]) and any(token in target for token in ["open", "close"]):
        return "split_hours"

    joined = "|".join(samples[:4]).lower()
    if joined and any(delim in joined for delim in [";", "|", ","]):
        if meta.get("value_array_grouping") or meta.get("object_array_grouping"):
            return "split_multivalue"

    if meta.get("enum"):
        return "normalize_enum"

    if str(meta.get("pattern") or ""):
        return "normalize_enum"

    return "review"


def _example_params(transform_name: str, meta: Dict[str, Any]) -> Dict[str, Any]:
    if transform_name == "normalize_date":
        return {"target_format": "%Y-%m-%d"}
    if transform_name in {"normalize_npi", "normalize_tin", "normalize_ssn", "normalize_phone", "normalize_zip"}:
        digits = 10 if transform_name in {"normalize_npi", "normalize_phone"} else 9
        return {"digits": digits}
    if transform_name == "normalize_state":
        return {"upper": True, "length": 2}
    if transform_name == "normalize_enum":
        return {"enum_values": list(meta.get("enum") or [])[:100]}
    if transform_name == "split_multivalue":
        return {"delimiters": ["|", ";", ","]}
    if transform_name == "split_hours":
        return {"separator_patterns": ["-", "to"]}
    return {}


def _kb_get_transformation_feedback(
    learning_kb: Optional[LearningKB],
    roster_type: str,
    transform_name: str,
    source_column: str,
    target_field: str,
    scope: Optional[Dict[str, Any]],
) -> Dict[str, int]:
    if learning_kb is None:
        return {"approved": 0, "rejected": 0, "added": 0}

    getter = getattr(learning_kb, "get_transformation_feedback", None)
    if getter is None:
        return {"approved": 0, "rejected": 0, "added": 0}

    try:
        return getter(
            roster_type=roster_type,
            transform_name=transform_name,
            source_column=source_column,
            target_field=target_field,
            scope=scope,
        )
    except TypeError:
        return getter(
            roster_type=roster_type,
            transform_name=transform_name,
            source_column=source_column,
            target_field=target_field,
        )
    except Exception:
        return {"approved": 0, "rejected": 0, "added": 0}


def suggest_transformations(
    mappings: List[Dict[str, Any]],
    schema_registry: SchemaRegistry,
    roster_type: str,
    sample_values: Dict[str, List[str]],
    learning_kb: Optional[LearningKB] = None,
    learning_retrieval: Optional[LearningRetrieval] = None,
    instructions_context: Optional[Dict[str, Any]] = None,
    learning_scope: Optional[Dict[str, Any]] = None,
    settings: Optional[Settings] = None,
    primary_router: Optional[LLMRouter] = None,
    verifier_router: Optional[LLMRouter] = None,
    collaboration_mode: Optional[str] = None,
    demo_mode: bool = False,
) -> Dict[str, Any]:
    settings = settings or get_settings()
    mode = str(collaboration_mode or settings.collaboration_mode or "advisory").strip().lower() or "advisory"
    strict_mode = mode in {"strict_fail_open", "strict_fail_closed"}
    fail_closed = mode == "strict_fail_closed"

    router_factory = LLMRouterFactory(settings=settings)
    primary_router = primary_router or router_factory.for_task("transformations")
    verifier_router = verifier_router or router_factory.for_task("verifier")

    items: List[Dict[str, Any]] = []
    seen = set()
    note_snippets = clean_note_snippets(instructions_context)

    for mapping in mappings:
        source = str(mapping.get("source_column", "") or "").strip()
        target = str(mapping.get("target_field", "") or "").strip()

        if not source:
            continue

        if not target:
            transform_name = "review"
            key = (transform_name, source)
            if key in seen:
                continue
            seen.add(key)
            item = {
                "id": f"tx::{transform_name}::{source}",
                "name": transform_name,
                "source_columns": [source],
                "target_fields": [],
                "params": {},
                "approved": False,
                "confidence": 0.3,
                "confidence_band": confidence_band(0.3),
                "reason": "No mapped target field",
                "reason_evidence": {
                    "matched_tokens": [],
                    "sample_pattern_evidence": {"samples": sample_values.get(source, [])[:6]},
                    "schema_metadata": {},
                    "note_directives": note_snippets[:4],
                },
                "priority": "high",
                "suggested_by": "deterministic",
            }
            items.append(item)
            continue

        meta = schema_registry.field_metadata(target, roster_type) or {}
        samples = sample_values.get(source, [])
        transform_name = _infer_transform(target_field=target, source_column=source, meta=meta, samples=samples)

        key = (transform_name, source)
        if key in seen:
            continue
        seen.add(key)

        catalog = TRANSFORM_CATALOG.get(transform_name, TRANSFORM_CATALOG["review"])

        confidence = 0.84 if transform_name != "review" else 0.35
        if transform_name == "normalize_enum" and meta.get("enum"):
            enum_values = {str(v).strip().lower() for v in (meta.get("enum") or []) if str(v).strip()}
            sample_match = 0.0
            if samples and enum_values:
                matches = sum(1 for value in samples if str(value).strip().lower() in enum_values)
                sample_match = matches / len(samples)
            confidence = max(0.6, min(0.98, 0.65 + (sample_match * 0.3)))

        reason = catalog.get("description", "Schema-driven suggestion")
        if meta.get("pattern"):
            reason = f"{reason}; pattern={meta.get('pattern')}"
        elif meta.get("format"):
            reason = f"{reason}; format={meta.get('format')}"
        elif meta.get("enum"):
            reason = f"{reason}; enum_count={len(meta.get('enum') or [])}"

        if note_snippets:
            reason = f"{reason}; notes considered"

        item = {
            "id": f"tx::{transform_name}::{source}",
            "name": transform_name,
            "source_columns": [source],
            "target_fields": [target],
            "params": _example_params(transform_name, meta),
            "approved": transform_name != "review",
            "confidence": round(confidence, 4),
            "confidence_band": confidence_band(confidence),
            "reason": reason,
            "reason_evidence": {
                "matched_tokens": [token for token in [transform_name, source, target] if token],
                "sample_pattern_evidence": {
                    "samples": samples[:6],
                    "enum_sample_match": None,
                },
                "schema_metadata": {
                    "target_field": target,
                    "format": meta.get("format"),
                    "pattern": bool(meta.get("pattern")),
                    "enum_count": len(meta.get("enum") or []),
                },
                "note_directives": note_snippets[:4],
            },
            "priority": catalog.get("priority", "medium"),
            "suggested_by": "deterministic",
        }

        if transform_name == "normalize_enum" and meta.get("enum"):
            enum_values = {str(v).strip().lower() for v in (meta.get("enum") or []) if str(v).strip()}
            sample_match = 0.0
            if samples and enum_values:
                matches = sum(1 for value in samples if str(value).strip().lower() in enum_values)
                sample_match = matches / len(samples)
            item["reason_evidence"]["sample_pattern_evidence"]["enum_sample_match"] = round(sample_match, 4)

        if learning_kb is not None:
            stats = _kb_get_transformation_feedback(
                learning_kb=learning_kb,
                roster_type=roster_type,
                transform_name=transform_name,
                source_column=source,
                target_field=target,
                scope=learning_scope,
            )
            approved_count = int(stats.get("approved", 0) or 0)
            rejected_count = int(stats.get("rejected", 0) or 0)
            added_count = int(stats.get("added", 0) or 0)
            adjustment = min(0.18, 0.02 * (approved_count + added_count)) - min(0.24, 0.03 * rejected_count)
            item["confidence"] = round(max(0.0, min(1.0, float(item["confidence"]) + adjustment)), 4)
            item["confidence_band"] = confidence_band(item["confidence"])
            item["reason_evidence"]["kb_prior"] = {
                "approved": approved_count,
                "rejected": rejected_count,
                "added": added_count,
                "adjustment": round(adjustment, 4),
            }
            if rejected_count >= approved_count + added_count + 2:
                item["approved"] = False
            elif approved_count + added_count >= rejected_count + 3 and item["confidence"] >= 0.58:
                item["approved"] = True

        if settings.enable_auto_approval_policy:
            threshold = float(settings.auto_approval_transformation_threshold)
            item["approved"] = bool(float(item.get("confidence", 0.0) or 0.0) >= threshold and item.get("name") != "review")

        items.append(item)

    # Demo mode: return deterministic items only, skip LLM
    if demo_mode:
        return {
            "transformations": items,
            "llm_trace": {
                "used": False,
                "demo_mode": True,
                "primary": {"status": "skipped_demo", "provider": None, "model": None, "attempts": []},
                "verifier": {"status": "skipped_demo", "provider": None, "model": None, "attempts": []},
                "merge": {"kept": len(items), "refined": 0, "rejected": 0, "added": 0},
            },
        }

    policy_requires_claude_verifier = bool(strict_mode or settings.require_claude_verifier_for_section("transformations"))

    llm_trace: Dict[str, Any] = {
        "used": False,
        "provider_order": {
            "analysis": primary_router.provider_names(),
            "verifier": verifier_router.provider_names(),
        },
        "retrieval": {"enabled": bool(learning_retrieval and learning_retrieval.is_enabled()), "count": 0, "sources": [], "scores": []},
        "primary": {
            "enabled": True,
            "status": "skipped",
            "provider": None,
            "model": None,
            "attempts": [],
        },
        "verifier": {
            "enabled": True,
            "status": "skipped",
            "provider": None,
            "model": None,
            "attempts": [],
        },
        "merge": {"kept": 0, "refined": 0, "rejected": 0, "added": 0},
        "policy": {
            "mode": mode,
            "requires_claude_verifier": policy_requires_claude_verifier,
            "status": "not_required" if not policy_requires_claude_verifier else "pending",
            "fail_closed": bool(fail_closed),
            "failure_reason": None,
        },
    }

    if items:
        llm_trace["used"] = True
        base_for_prompt = _transform_candidates_for_prompt(items, schema_registry=schema_registry, roster_type=roster_type)

        rag_examples: Dict[str, List[Dict[str, Any]]] = {}
        retrieval_hits: List[Dict[str, Any]] = []
        if learning_retrieval is not None and learning_retrieval.is_enabled():
            for candidate in base_for_prompt:
                item_id = str(candidate.get("id") or "")
                retrieval_result = learning_retrieval.retrieve(
                    section="transformations",
                    item_key={
                        "item_id": item_id,
                        "transform_name": candidate.get("name"),
                        "source_columns": candidate.get("source_columns") or [],
                        "target_fields": candidate.get("target_fields") or [],
                    },
                    roster_type=roster_type,
                    workspace_scope=learning_scope,
                )
                examples = learning_retrieval.format_examples_for_prompt("transformations", retrieval_result)
                if examples:
                    rag_examples[item_id] = examples
                    top = examples[0]
                    retrieval_hits.append(
                        {
                            "source": "learning_episode",
                            "item_id": top.get("item_id") or item_id,
                            "score": top.get("score"),
                            "candidate_key": top.get("candidate_key") or {},
                            "final_candidate": top.get("final_candidate") or {},
                        }
                    )

        llm_trace["retrieval"] = {
            "enabled": bool(learning_retrieval and learning_retrieval.is_enabled()),
            "count": len(retrieval_hits),
            "sources": sorted({str(hit.get("source") or "") for hit in retrieval_hits if str(hit.get("source") or "")}),
            "scores": [hit.get("score") for hit in retrieval_hits],
            "hits": retrieval_hits,
        }

        primary_raw, primary_trace = _transform_primary_review(
            candidates=base_for_prompt,
            roster_type=roster_type,
            sample_values=sample_values,
            notes=note_snippets,
            router=primary_router,
            rag_examples=rag_examples,
        )
        llm_trace["primary"].update(primary_trace)

        primary_candidates: List[Dict[str, Any]] = []
        for candidate in primary_raw:
            normalized = _normalize_transform_candidate(candidate, schema_registry=schema_registry, roster_type=roster_type)
            if normalized is None:
                continue
            normalized["suggested_by"] = primary_trace.get("provider") or "analysis"
            rag_payload = rag_examples.get(str(normalized.get("id") or ""), [])
            if rag_payload:
                evidence = normalized.setdefault("reason_evidence", {})
                evidence["rag_examples"] = rag_payload[:2]
            primary_candidates.append(normalized)

        verifier_input = primary_candidates if primary_candidates else items
        verifier_prompt_candidates = _transform_candidates_for_prompt(
            verifier_input,
            schema_registry=schema_registry,
            roster_type=roster_type,
        )
        verifier_raw, verifier_trace = _transform_verifier_review(
            candidates=verifier_prompt_candidates,
            roster_type=roster_type,
            sample_values=sample_values,
            notes=note_snippets,
            router=verifier_router,
        )
        llm_trace["verifier"].update(verifier_trace)

        verifier_decisions: List[Dict[str, Any]] = []
        for decision in verifier_raw:
            action = str(decision.get("action", "") or "").strip().lower()
            candidate_id = str(decision.get("id", "") or "").strip()
            if action not in {"keep", "refine", "reject", "add"} or not candidate_id:
                continue

            if action in {"refine", "add"}:
                payload = decision.get("candidate") if isinstance(decision.get("candidate"), dict) else {}
                payload = dict(payload)
                payload["id"] = candidate_id
                normalized = _normalize_transform_candidate(payload, schema_registry=schema_registry, roster_type=roster_type)
                if normalized is None:
                    continue
                normalized["suggested_by"] = verifier_trace.get("provider") or "verifier"
                verifier_decisions.append({"action": action, "id": candidate_id, "candidate": normalized})
            else:
                verifier_decisions.append({"action": action, "id": candidate_id})

        merged_items, merge_stats = _merge_transform_candidates(
            base_candidates=items,
            primary_candidates=primary_candidates,
            verifier_decisions=verifier_decisions,
            threshold=float(settings.auto_approval_transformation_threshold),
        )
        llm_trace["merge"] = merge_stats

        policy_status = _enforce_transform_collaboration_policy(
            settings=settings,
            policy_trace=llm_trace.get("policy", {}),
            verifier_trace=llm_trace.get("verifier", {}),
            merged_items=merged_items,
        )
        llm_trace["policy"] = policy_status

        if policy_status.get("affected_ids"):
            affected_ids = {
                str(item_id).strip()
                for item_id in (policy_status.get("affected_ids") or [])
                if str(item_id).strip()
            }
            failure_reason = str(policy_status.get("failure_reason") or "collaboration verifier unavailable")
            for item in merged_items:
                item_id = str(item.get("id") or "")
                if item_id not in affected_ids:
                    continue
                item["approved"] = False
                reason_prefix = str(item.get("reason") or "").strip()
                item["reason"] = f"{reason_prefix}; unverified ({failure_reason})" if reason_prefix else f"unverified ({failure_reason})"
                evidence = item.setdefault("reason_evidence", {})
                evidence["collaboration_policy"] = {
                    "mode": mode,
                    "requires_claude_verifier": bool(policy_status.get("requires_claude_verifier")),
                    "status": policy_status.get("status"),
                    "failure_reason": policy_status.get("failure_reason"),
                }

        items = merged_items

    return {
        "transformations": items,
        "llm_trace": llm_trace,
    }


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        raw = float(value)
    except Exception:
        raw = default
    return max(0.0, min(1.0, raw))


def _transform_candidates_for_prompt(
    candidates: List[Dict[str, Any]],
    schema_registry: SchemaRegistry,
    roster_type: str,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in candidates:
        source_columns = [str(col).strip() for col in (item.get("source_columns") or []) if str(col).strip()]
        target_fields = [str(field).strip() for field in (item.get("target_fields") or []) if str(field).strip()]
        target_meta: Dict[str, Any] = {}
        if target_fields:
            target_meta = schema_registry.field_metadata(target_fields[0], roster_type) or {}

        out.append(
            {
                "id": item.get("id"),
                "name": item.get("name"),
                "source_columns": source_columns,
                "target_fields": target_fields,
                "params": item.get("params") or {},
                "reason": item.get("reason") or "",
                "confidence": _safe_float(item.get("confidence", 0.0), default=0.0),
                "schema_metadata": {
                    "target_field": target_fields[0] if target_fields else "",
                    "format": target_meta.get("format"),
                    "pattern": bool(target_meta.get("pattern")),
                    "enum_count": len(target_meta.get("enum") or []),
                },
            }
        )
    return out


def _normalize_transform_candidate(
    candidate: Dict[str, Any],
    schema_registry: SchemaRegistry,
    roster_type: str,
) -> Optional[Dict[str, Any]]:
    if not isinstance(candidate, dict):
        return None
    name = str(candidate.get("name", "") or "").strip()
    if name not in TRANSFORM_CATALOG:
        return None

    source_columns = [str(col).strip() for col in (candidate.get("source_columns") or []) if str(col).strip()]
    if not source_columns:
        return None

    target_fields = [str(field).strip() for field in (candidate.get("target_fields") or []) if str(field).strip()]
    valid_targets = [field for field in target_fields if schema_registry.is_valid_field(field, roster_type)]

    params = candidate.get("params") or {}
    if not isinstance(params, dict):
        params = {}

    reason = str(candidate.get("reason", "LLM collaboration suggestion") or "LLM collaboration suggestion")
    confidence = _safe_float(candidate.get("confidence", 0.0), default=0.0)

    source_key = "::".join(source_columns)
    return {
        "id": str(candidate.get("id") or f"tx::{name}::{source_key}"),
        "name": name,
        "source_columns": source_columns,
        "target_fields": valid_targets,
        "params": params,
        "approved": bool(name != "review"),
        "confidence": round(confidence, 4),
        "confidence_band": confidence_band(confidence),
        "reason": reason,
        "reason_evidence": {
            "matched_tokens": [name, *source_columns, *valid_targets],
            "sample_pattern_evidence": {},
            "schema_metadata": {
                "target_field": valid_targets[0] if valid_targets else "",
                "enum_count": len((schema_registry.field_metadata(valid_targets[0], roster_type) or {}).get("enum") or [])
                if valid_targets
                else 0,
            },
            "note_directives": [],
        },
        "priority": TRANSFORM_CATALOG.get(name, {}).get("priority", "medium"),
    }


def _merge_transform_candidates(
    base_candidates: List[Dict[str, Any]],
    primary_candidates: List[Dict[str, Any]],
    verifier_decisions: List[Dict[str, Any]],
    threshold: float,
) -> tuple[List[Dict[str, Any]], Dict[str, int]]:
    by_id: Dict[str, Dict[str, Any]] = {
        str(item.get("id") or ""): dict(item)
        for item in base_candidates
        if str(item.get("id") or "")
    }

    for candidate in primary_candidates:
        candidate_id = str(candidate.get("id") or "")
        if not candidate_id:
            continue
        by_id[candidate_id] = dict(candidate)

    stats = {"kept": 0, "refined": 0, "rejected": 0, "added": 0}
    explicit = set()

    for decision in verifier_decisions:
        action = str(decision.get("action", "") or "").strip().lower()
        candidate_id = str(decision.get("id", "") or "").strip()
        if action not in {"keep", "refine", "reject", "add"} or not candidate_id:
            continue

        if action == "keep":
            if candidate_id in by_id:
                explicit.add(candidate_id)
            continue

        if action == "reject":
            if candidate_id in by_id:
                by_id.pop(candidate_id, None)
                stats["rejected"] += 1
                explicit.add(candidate_id)
            continue

        payload = decision.get("candidate") if isinstance(decision.get("candidate"), dict) else None
        if action in {"refine", "add"} and payload is None:
            continue

        if action == "refine":
            if candidate_id not in by_id:
                continue
            merged = dict(by_id[candidate_id])
            merged.update({k: v for k, v in payload.items() if k not in {"id"}})
            merged["id"] = candidate_id
            confidence = _safe_float(merged.get("confidence", 0.0), default=0.0)
            merged["confidence"] = round(confidence, 4)
            merged["confidence_band"] = confidence_band(confidence)
            merged["approved"] = bool(confidence >= threshold and merged.get("name") != "review")
            by_id[candidate_id] = merged
            stats["refined"] += 1
            explicit.add(candidate_id)
            continue

        if action == "add":
            if candidate_id in by_id:
                continue
            confidence = _safe_float(payload.get("confidence", 0.0), default=0.0)
            added = dict(payload)
            added["id"] = candidate_id
            added["confidence"] = round(confidence, 4)
            added["confidence_band"] = confidence_band(confidence)
            added["approved"] = bool(confidence >= threshold and added.get("name") != "review")
            by_id[candidate_id] = added
            stats["added"] += 1
            explicit.add(candidate_id)

    for candidate_id in list(by_id.keys()):
        if candidate_id not in explicit:
            stats["kept"] += 1

    merged = list(by_id.values())
    return merged, stats


def _transform_primary_review(
    candidates: List[Dict[str, Any]],
    roster_type: str,
    sample_values: Dict[str, List[str]],
    notes: List[str],
    router: LLMRouter,
    rag_examples: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not candidates:
        return [], {"status": "skipped", "provider": None, "model": None, "attempts": []}

    sample_hint: Dict[str, List[str]] = {}
    for candidate in candidates:
        for source in candidate.get("source_columns") or []:
            col = str(source).strip()
            if not col:
                continue
            sample_hint[col] = (sample_values.get(col) or [])[:6]

    prompt = f"""
You are reviewing roster transformation candidates.
Return JSON only with key 'candidates'.

Roster type: {roster_type}
Candidate transformations:
{json.dumps(candidates, ensure_ascii=False, indent=2)}

Sample values by source column:
{json.dumps(sample_hint, ensure_ascii=False, indent=2)}

Notes and directives:
{json.dumps(notes[:8], ensure_ascii=False, indent=2)}

Retrieved accepted exemplars:
{json.dumps(rag_examples or {}, ensure_ascii=False, indent=2)}

Rules:
- Keep to known transformation names from provided candidates.
- You may adjust params, confidence, reason, target_fields.
- Keep IDs stable for existing candidates.
- Output shape:
{{
  "candidates": [
    {{
      "id": "tx::...",
      "name": "normalize_npi|normalize_tin|normalize_ssn|normalize_phone|normalize_zip|normalize_state|normalize_enum|normalize_date|split_hours|split_multivalue|review",
      "source_columns": ["..."],
      "target_fields": ["..."],
      "params": {{}},
      "confidence": 0.0,
      "reason": "..."
    }}
  ]
}}
""".strip()

    try:
        routed = router.generate(prompt=prompt, task_type="analysis")
    except Exception as exc:
        return [], {"status": "error", "provider": None, "model": None, "attempts": [f"error:{exc}"]}

    parsed = extract_json_object(routed.response.text)
    raw_candidates = parsed.get("candidates") if isinstance(parsed.get("candidates"), list) else []
    updates: List[Dict[str, Any]] = []
    by_id = {str(item.get("id") or ""): item for item in candidates if str(item.get("id") or "")}

    for candidate in raw_candidates:
        if not isinstance(candidate, dict):
            continue
        candidate_id = str(candidate.get("id", "") or "").strip()
        if not candidate_id or candidate_id not in by_id:
            continue
        merged = dict(by_id[candidate_id])
        merged.update({k: v for k, v in candidate.items() if k not in {"id"}})
        merged["id"] = candidate_id
        updates.append(merged)

    trace = {
        "status": "ok",
        "provider": routed.response.provider,
        "model": routed.response.model,
        "attempts": routed.attempts,
        "task_type": "analysis",
        "suggestion_count": len(updates),
    }
    return updates, trace


def _transform_verifier_review(
    candidates: List[Dict[str, Any]],
    roster_type: str,
    sample_values: Dict[str, List[str]],
    notes: List[str],
    router: LLMRouter,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not candidates:
        return [], {"status": "skipped", "provider": None, "model": None, "attempts": []}

    sample_hint: Dict[str, List[str]] = {}
    for candidate in candidates:
        for source in candidate.get("source_columns") or []:
            col = str(source).strip()
            if not col:
                continue
            sample_hint[col] = (sample_values.get(col) or [])[:6]

    prompt = f"""
You are the verifier for transformation candidates. Return JSON only.

Roster type: {roster_type}
Candidates to verify:
{json.dumps(candidates, ensure_ascii=False, indent=2)}

Sample values:
{json.dumps(sample_hint, ensure_ascii=False, indent=2)}

Notes and directives:
{json.dumps(notes[:8], ensure_ascii=False, indent=2)}

Decision rules:
- action=keep: keep candidate as-is
- action=refine: modify candidate fields in candidate payload
- action=reject: remove candidate
- action=add: add new candidate with full payload
- Candidate name must be one of:
  normalize_npi, normalize_tin, normalize_ssn, normalize_phone, normalize_zip,
  normalize_state, normalize_enum, normalize_date, split_hours, split_multivalue, review
- source_columns must not be empty
- target_fields should remain schema-valid when provided

Output:
{{
  "decisions": [
    {{
      "action": "keep|refine|reject|add",
      "id": "tx::...",
      "candidate": {{"id": "tx::...", "name": "...", "source_columns": ["..."], "target_fields": ["..."], "params": {{}}, "confidence": 0.0, "reason": "..."}}
    }}
  ]
}}
""".strip()

    try:
        routed = router.generate(prompt=prompt, task_type="verifier")
    except Exception as exc:
        return [], {"status": "error", "provider": None, "model": None, "attempts": [f"error:{exc}"]}

    parsed = extract_json_object(routed.response.text)
    raw_decisions = parsed.get("decisions") if isinstance(parsed.get("decisions"), list) else []

    decisions: List[Dict[str, Any]] = []
    candidate_ids = {str(item.get("id") or "") for item in candidates if str(item.get("id") or "")}

    for decision in raw_decisions:
        if not isinstance(decision, dict):
            continue
        action = str(decision.get("action", "") or "").strip().lower()
        candidate_id = str(decision.get("id", "") or "").strip()
        if action not in {"keep", "refine", "reject", "add"} or not candidate_id:
            continue
        if action in {"keep", "refine", "reject"} and candidate_id not in candidate_ids:
            continue

        payload = decision.get("candidate") if isinstance(decision.get("candidate"), dict) else None
        if action in {"refine", "add"} and not isinstance(payload, dict):
            continue

        decisions.append(
            {
                "action": action,
                "id": candidate_id,
                "candidate": payload,
                "suggested_by": routed.response.provider,
            }
        )

    trace = {
        "status": "ok",
        "provider": routed.response.provider,
        "model": routed.response.model,
        "attempts": routed.attempts,
        "task_type": "verifier",
        "decision_count": len(decisions),
    }
    return decisions, trace


def _enforce_transform_collaboration_policy(
    *,
    settings: Settings,
    policy_trace: Dict[str, Any],
    verifier_trace: Dict[str, Any],
    merged_items: List[Dict[str, Any]],
) -> Dict[str, Any]:
    policy = dict(policy_trace or {})
    policy["affected_ids"] = []

    requires = bool(policy.get("requires_claude_verifier"))
    if not requires:
        policy["status"] = "not_required"
        policy["failure_reason"] = None
        return policy

    if not merged_items:
        policy["status"] = "satisfied"
        policy["failure_reason"] = None
        return policy

    verifier_status = str(verifier_trace.get("status", "") or "").strip().lower()
    verifier_provider = str(verifier_trace.get("provider", "") or "").strip().lower()

    if verifier_status != "ok":
        policy["status"] = "failed"
        policy["failure_reason"] = "verifier_unavailable"
        policy["affected_ids"] = [str(item.get("id") or "") for item in merged_items if str(item.get("id") or "")]
        if settings.strict_fail_closed():
            raise RuntimeError("Strict collaboration policy requires Claude verifier for transformations")
        return policy

    if not settings.is_claude_provider(verifier_provider):
        policy["status"] = "failed"
        policy["failure_reason"] = "verifier_not_claude"
        policy["affected_ids"] = [str(item.get("id") or "") for item in merged_items if str(item.get("id") or "")]
        if settings.strict_fail_closed():
            raise RuntimeError("Strict collaboration policy requires Claude verifier provider for transformations")
        return policy

    policy["status"] = "satisfied"
    policy["failure_reason"] = None
    return policy


def apply_transformation(series, name: str, params: Dict[str, Any]):
    import pandas as pd

    if name == "normalize_date":
        fmt = str(params.get("target_format", "%Y-%m-%d"))
        return pd.to_datetime(series, errors="coerce").dt.strftime(fmt)
    if name in {"normalize_npi", "normalize_tin", "normalize_ssn", "normalize_phone", "normalize_zip"}:
        digits = int(params.get("digits", 10))
        return series.astype(str).apply(lambda v: "".join(ch for ch in v if ch.isdigit())[:digits])
    if name == "normalize_state":
        length = int(params.get("length", 2))
        return series.astype(str).str.strip().str.upper().str[:length]
    if name == "normalize_enum":
        strip_values = bool(params.get("strip", True))
        if strip_values:
            out = series.astype(str).str.strip()
        else:
            out = series.astype(str)

        value_map = params.get("value_map") or {}
        if isinstance(value_map, dict) and value_map:
            normalized_map: Dict[str, Any] = {str(k): v for k, v in value_map.items()}
            out = out.replace(normalized_map)
        return out
    if name == "split_multivalue":
        delimiters = params.get("delimiters") or ["|", ";", ","]
        pattern = "|".join(re.escape(d) for d in delimiters)
        return series.astype(str).str.replace(pattern, "|", regex=True)
    return series
