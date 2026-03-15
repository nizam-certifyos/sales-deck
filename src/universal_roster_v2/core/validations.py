"""Schema-first validation suggestion and SQL compilation."""

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


def _sanitize_identifier(name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_]", "_", str(name or "").strip())
    safe = re.sub(r"_+", "_", safe).strip("_")
    if not safe:
        return "col"
    if safe[0].isdigit():
        return f"col_{safe}"
    return safe


def _required_rule(source: str, target: str, alias: str) -> Dict[str, Any]:
    return {
        "id": f"bq::{alias}::required",
        "name": f"{alias}_required",
        "source_column": source,
        "target_field": target,
        "severity": "error",
        "approved": True,
        "rule_type": "required",
        "runtime": {"kind": "required"},
        "sql_expression": f"IFNULL(TRIM(CAST(`{alias}` AS STRING)), '') = ''",
        "message": f"{source} is required",
        "confidence": 0.95,
    }


def _pattern_rule(source: str, target: str, alias: str, pattern: str) -> Dict[str, Any]:
    escaped = pattern.replace("\\", "\\\\").replace("'", "\\'")
    return {
        "id": f"bq::{alias}::pattern",
        "name": f"{alias}_pattern",
        "source_column": source,
        "target_field": target,
        "severity": "error",
        "approved": True,
        "rule_type": "pattern",
        "runtime": {"kind": "pattern", "pattern": pattern},
        "sql_expression": (
            f"IFNULL(TRIM(CAST(`{alias}` AS STRING)), '') != '' "
            f"AND NOT REGEXP_CONTAINS(CAST(`{alias}` AS STRING), r'{escaped}')"
        ),
        "message": f"{source} does not match required format",
        "confidence": 0.9,
    }


def _enum_rule(source: str, target: str, alias: str, enum_values: List[str]) -> Dict[str, Any]:
    cleaned_values = [str(v).strip() for v in enum_values if str(v).strip()]
    quoted = ", ".join(["'" + value.replace("'", "\\'") + "'" for value in cleaned_values])
    return {
        "id": f"bq::{alias}::enum",
        "name": f"{alias}_enum",
        "source_column": source,
        "target_field": target,
        "severity": "error",
        "approved": True,
        "rule_type": "enum",
        "runtime": {"kind": "enum", "values": cleaned_values},
        "sql_expression": (
            f"IFNULL(TRIM(CAST(`{alias}` AS STRING)), '') != '' "
            f"AND CAST(`{alias}` AS STRING) NOT IN ({quoted})"
            if quoted
            else "FALSE"
        ),
        "message": f"{source} must be one of allowed enum values",
        "confidence": 0.86,
    }


def _format_rule(source: str, target: str, alias: str, fmt: str) -> Dict[str, Any]:
    fmt_key = (fmt or "").lower()
    if fmt_key == "date":
        expr = (
            f"IFNULL(TRIM(CAST(`{alias}` AS STRING)), '') != '' "
            f"AND SAFE.PARSE_DATE('%Y-%m-%d', CAST(`{alias}` AS STRING)) IS NULL"
        )
        msg = f"{source} must be YYYY-MM-DD"
    elif fmt_key == "email":
        expr = (
            f"IFNULL(TRIM(CAST(`{alias}` AS STRING)), '') != '' "
            f"AND NOT REGEXP_CONTAINS(CAST(`{alias}` AS STRING), r'^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$')"
        )
        msg = f"{source} must be a valid email"
    else:
        expr = "FALSE"
        msg = f"{source} has invalid format"

    return {
        "id": f"bq::{alias}::format::{fmt_key or 'unknown'}",
        "name": f"{alias}_{fmt_key or 'format'}",
        "source_column": source,
        "target_field": target,
        "severity": "error",
        "approved": True,
        "rule_type": "format",
        "runtime": {"kind": "format", "format": fmt_key},
        "sql_expression": expr,
        "message": msg,
        "confidence": 0.88,
    }


def _kb_get_validation_feedback(
    learning_kb: Optional[LearningKB],
    roster_type: str,
    rule_type: str,
    source_column: str,
    target_field: str,
    scope: Optional[Dict[str, Any]],
) -> Dict[str, int]:
    if learning_kb is None:
        return {"approved": 0, "rejected": 0, "added": 0}

    getter = getattr(learning_kb, "get_validation_feedback", None)
    if getter is None:
        return {"approved": 0, "rejected": 0, "added": 0}

    try:
        return getter(
            roster_type=roster_type,
            rule_type=rule_type,
            source_column=source_column,
            target_field=target_field,
            scope=scope,
        )
    except TypeError:
        return getter(
            roster_type=roster_type,
            rule_type=rule_type,
            source_column=source_column,
            target_field=target_field,
        )
    except Exception:
        return {"approved": 0, "rejected": 0, "added": 0}


def suggest_bq_validations(
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
    primary_router = primary_router or router_factory.for_task("validations")
    verifier_router = verifier_router or router_factory.for_task("verifier")

    items: List[Dict[str, Any]] = []
    seen = set()
    note_snippets = clean_note_snippets(instructions_context)

    def apply_kb_prior(item: Dict[str, Any], source: str, target: str) -> Dict[str, Any]:
        if learning_kb is None:
            item["confidence_band"] = confidence_band(item.get("confidence", 0.0))
            return item
        rule_type = str(item.get("rule_type", "") or "custom")
        stats = _kb_get_validation_feedback(
            learning_kb=learning_kb,
            roster_type=roster_type,
            rule_type=rule_type,
            source_column=source,
            target_field=target,
            scope=learning_scope,
        )
        approved_count = int(stats.get("approved", 0) or 0)
        rejected_count = int(stats.get("rejected", 0) or 0)
        added_count = int(stats.get("added", 0) or 0)
        adjustment = min(0.16, 0.018 * (approved_count + added_count)) - min(0.22, 0.03 * rejected_count)
        base_conf = float(item.get("confidence", 0.0) or 0.0)
        item["confidence"] = round(max(0.0, min(1.0, base_conf + adjustment)), 4)
        item["confidence_band"] = confidence_band(item["confidence"])
        evidence = item.setdefault("reason_evidence", {})
        evidence["kb_prior"] = {
            "approved": approved_count,
            "rejected": rejected_count,
            "added": added_count,
            "adjustment": round(adjustment, 4),
        }
        if rejected_count >= approved_count + added_count + 2:
            item["approved"] = False
        elif approved_count + added_count >= rejected_count + 3 and item["confidence"] >= 0.6:
            item["approved"] = True
        return item

    for mapping in mappings:
        source = str(mapping.get("source_column", "") or "").strip()
        target = str(mapping.get("target_field", "") or "").strip()
        if not source or not target:
            continue

        alias = _sanitize_identifier(source)
        meta = schema_registry.field_metadata(target, roster_type) or {}
        samples = sample_values.get(source, [])

        def enrich_rule(rule: Dict[str, Any]) -> Dict[str, Any]:
            rule["confidence_band"] = confidence_band(rule.get("confidence", 0.0))
            rule["reason_evidence"] = {
                "matched_tokens": [source, target, str(rule.get("rule_type", ""))],
                "sample_pattern_evidence": {"samples": samples[:6]},
                "schema_metadata": {
                    "target_field": target,
                    "required": bool(meta.get("required")),
                    "format": meta.get("format"),
                    "pattern": bool(meta.get("pattern")),
                    "enum_count": len(meta.get("enum") or []),
                },
                "note_directives": note_snippets[:4],
            }
            if note_snippets:
                existing_msg = str(rule.get("message", "") or "")
                if existing_msg:
                    rule["message"] = f"{existing_msg} (notes-aware)"
            rule["suggested_by"] = "deterministic"
            return rule

        if meta.get("required"):
            rule = enrich_rule(_required_rule(source, target, alias))
            if rule["id"] not in seen:
                items.append(apply_kb_prior(rule, source, target))
                seen.add(rule["id"])

        pattern = str(meta.get("pattern") or "")
        if pattern:
            rule = enrich_rule(_pattern_rule(source, target, alias, pattern))
            if rule["id"] not in seen:
                if samples:
                    try:
                        compiled = re.compile(pattern)
                        mismatch = sum(1 for value in samples if compiled.fullmatch(str(value).strip() or "") is None)
                        rule["confidence"] = round(max(0.65, 0.95 - (mismatch / max(len(samples), 1)) * 0.2), 4)
                        rule["reason_evidence"]["sample_pattern_evidence"]["mismatch_ratio"] = round(
                            mismatch / max(len(samples), 1), 4
                        )
                    except re.error:
                        pass
                items.append(apply_kb_prior(rule, source, target))
                seen.add(rule["id"])

        enum_values = [str(v) for v in (meta.get("enum") or []) if str(v).strip()]
        if enum_values:
            rule = enrich_rule(_enum_rule(source, target, alias, enum_values[:1000]))
            if rule["id"] not in seen:
                if samples:
                    enum_lookup = {v.lower() for v in enum_values}
                    unmatched = sum(1 for value in samples if str(value).strip().lower() not in enum_lookup)
                    rule["confidence"] = round(max(0.6, 0.9 - (unmatched / max(len(samples), 1)) * 0.25), 4)
                    rule["reason_evidence"]["sample_pattern_evidence"]["unmatched_ratio"] = round(
                        unmatched / max(len(samples), 1), 4
                    )
                items.append(apply_kb_prior(rule, source, target))
                seen.add(rule["id"])

        fmt = str(meta.get("format") or "")
        if fmt:
            rule = enrich_rule(_format_rule(source, target, alias, fmt))
            if rule["id"] not in seen:
                items.append(apply_kb_prior(rule, source, target))
                seen.add(rule["id"])

    if settings.enable_auto_approval_policy:
        threshold = float(settings.auto_approval_validation_threshold)
        for item in items:
            item["approved"] = bool(float(item.get("confidence", 0.0) or 0.0) >= threshold)

    # Demo mode: return deterministic items only, skip LLM
    if demo_mode:
        return {
            "bq_validations": items,
            "llm_trace": {
                "used": False,
                "demo_mode": True,
                "primary": {"status": "skipped_demo", "provider": None, "model": None, "attempts": []},
                "verifier": {"status": "skipped_demo", "provider": None, "model": None, "attempts": []},
                "merge": {"kept": len(items), "refined": 0, "rejected": 0, "added": 0},
            },
        }

    policy_requires_claude_verifier = bool(strict_mode or settings.require_claude_verifier_for_section("validations"))

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

        prompt_candidates: List[Dict[str, Any]] = []
        for item in items:
            prompt_candidates.append(
                {
                    "id": item.get("id"),
                    "name": item.get("name"),
                    "source_column": item.get("source_column"),
                    "target_field": item.get("target_field"),
                    "rule_type": item.get("rule_type"),
                    "severity": item.get("severity"),
                    "sql_expression": item.get("sql_expression"),
                    "message": item.get("message"),
                    "runtime": item.get("runtime") or {},
                    "confidence": item.get("confidence"),
                }
            )

        sample_hint = {str(item.get("source_column") or ""): (sample_values.get(str(item.get("source_column") or "")) or [])[:6] for item in prompt_candidates}

        rag_examples: Dict[str, List[Dict[str, Any]]] = {}
        retrieval_hits: List[Dict[str, Any]] = []
        if learning_retrieval is not None and learning_retrieval.is_enabled():
            for candidate in prompt_candidates:
                item_id = str(candidate.get("id") or "")
                retrieval_result = learning_retrieval.retrieve(
                    section="validations",
                    item_key={
                        "item_id": item_id,
                        "rule_type": candidate.get("rule_type"),
                        "source_column": candidate.get("source_column"),
                        "target_field": candidate.get("target_field"),
                    },
                    roster_type=roster_type,
                    workspace_scope=learning_scope,
                )
                examples = learning_retrieval.format_examples_for_prompt("validations", retrieval_result)
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

        primary_prompt = f"""
You are reviewing BigQuery validation rule candidates.
Return JSON only with key 'rules'.

Roster type: {roster_type}
Rule candidates:
{json.dumps(prompt_candidates, ensure_ascii=False, indent=2)}

Sample values:
{json.dumps(sample_hint, ensure_ascii=False, indent=2)}

Notes and directives:
{json.dumps(note_snippets[:8], ensure_ascii=False, indent=2)}

Retrieved accepted exemplars:
{json.dumps(rag_examples, ensure_ascii=False, indent=2)}

Allowed rule_type values: required, pattern, enum, format, custom
Keep IDs stable for existing rules.
Output:
{{
  "rules": [
    {{
      "id": "bq::...",
      "name": "...",
      "source_column": "...",
      "target_field": "...",
      "severity": "error|warning",
      "rule_type": "required|pattern|enum|format|custom",
      "runtime": {{}},
      "sql_expression": "...",
      "message": "...",
      "confidence": 0.0
    }}
  ]
}}
""".strip()

        try:
            primary_routed = primary_router.generate(prompt=primary_prompt, task_type="analysis")
            llm_trace["primary"].update(
                {
                    "status": "ok",
                    "provider": primary_routed.response.provider,
                    "model": primary_routed.response.model,
                    "attempts": primary_routed.attempts,
                    "task_type": "analysis",
                }
            )
            primary_parsed = extract_json_object(primary_routed.response.text)
            primary_rules = primary_parsed.get("rules") if isinstance(primary_parsed.get("rules"), list) else []
        except Exception as exc:
            primary_rules = []
            llm_trace["primary"].update(
                {
                    "status": "error",
                    "provider": None,
                    "model": None,
                    "attempts": [f"error:{exc}"],
                    "task_type": "analysis",
                }
            )

        by_id: Dict[str, Dict[str, Any]] = {
            str(item.get("id") or ""): dict(item)
            for item in items
            if str(item.get("id") or "")
        }

        allowed_rule_types = {"required", "pattern", "enum", "format", "custom"}

        def normalize_rule(rule: Dict[str, Any], default_suggested_by: str) -> Optional[Dict[str, Any]]:
            if not isinstance(rule, dict):
                return None
            rule_id = str(rule.get("id", "") or "").strip()
            source = str(rule.get("source_column", "") or "").strip()
            target = str(rule.get("target_field", "") or "").strip()
            rule_type = str(rule.get("rule_type", "") or "").strip().lower()
            sql_expression = str(rule.get("sql_expression", "") or "").strip()
            message = str(rule.get("message", "") or "").strip()
            if not rule_id or not source or not target:
                return None
            if not schema_registry.is_valid_field(target, roster_type):
                return None
            if rule_type not in allowed_rule_types:
                return None
            if not sql_expression or not message:
                return None

            normalized = dict(rule)
            normalized["id"] = rule_id
            normalized["source_column"] = source
            normalized["target_field"] = target
            normalized["rule_type"] = rule_type
            normalized["sql_expression"] = sql_expression
            normalized["message"] = message
            normalized["confidence"] = round(max(0.0, min(1.0, float(rule.get("confidence", 0.0) or 0.0))), 4)
            normalized["confidence_band"] = confidence_band(normalized["confidence"])
            normalized["approved"] = bool(normalized["confidence"] >= float(settings.auto_approval_validation_threshold))
            normalized["suggested_by"] = default_suggested_by
            normalized.setdefault("severity", "error")
            normalized.setdefault("runtime", {"kind": rule_type})
            evidence = normalized.setdefault("reason_evidence", {})
            rag_payload = rag_examples.get(rule_id, [])
            if rag_payload:
                evidence["rag_examples"] = rag_payload[:2]
            return normalized

        primary_updates: List[Dict[str, Any]] = []
        for rule in primary_rules:
            normalized = normalize_rule(rule, llm_trace["primary"].get("provider") or "analysis")
            if normalized is None:
                continue
            primary_updates.append(normalized)

        for rule in primary_updates:
            by_id[str(rule.get("id") or "")] = rule

        verifier_candidates = list(by_id.values())

        verifier_prompt = f"""
You are the verifier for BigQuery validation rules. Return JSON only.

Roster type: {roster_type}
Candidates:
{json.dumps(verifier_candidates, ensure_ascii=False, indent=2)}

Sample values:
{json.dumps(sample_hint, ensure_ascii=False, indent=2)}

Notes and directives:
{json.dumps(note_snippets[:8], ensure_ascii=False, indent=2)}

Decision rules:
- action=keep: keep candidate
- action=refine: replace candidate payload
- action=reject: remove candidate
- action=add: add new candidate payload
- target_field must be schema-valid
- rule_type must be one of required|pattern|enum|format|custom
- sql_expression and message must be non-empty for approved rules

Output:
{{
  "decisions": [
    {{
      "action": "keep|refine|reject|add",
      "id": "bq::...",
      "rule": {{... full rule payload ...}}
    }}
  ]
}}
""".strip()

        try:
            verifier_routed = verifier_router.generate(prompt=verifier_prompt, task_type="verifier")
            llm_trace["verifier"].update(
                {
                    "status": "ok",
                    "provider": verifier_routed.response.provider,
                    "model": verifier_routed.response.model,
                    "attempts": verifier_routed.attempts,
                    "task_type": "verifier",
                }
            )
            verifier_parsed = extract_json_object(verifier_routed.response.text)
            verifier_decisions = verifier_parsed.get("decisions") if isinstance(verifier_parsed.get("decisions"), list) else []
        except Exception as exc:
            verifier_decisions = []
            llm_trace["verifier"].update(
                {
                    "status": "error",
                    "provider": None,
                    "model": None,
                    "attempts": [f"error:{exc}"],
                    "task_type": "verifier",
                }
            )

        merge_stats = {"kept": 0, "refined": 0, "rejected": 0, "added": 0}
        explicit_ids = set()

        for decision in verifier_decisions:
            if not isinstance(decision, dict):
                continue
            action = str(decision.get("action", "") or "").strip().lower()
            rule_id = str(decision.get("id", "") or "").strip()
            if action not in {"keep", "refine", "reject", "add"} or not rule_id:
                continue

            if action == "keep":
                if rule_id in by_id:
                    explicit_ids.add(rule_id)
                continue

            if action == "reject":
                if rule_id in by_id:
                    by_id.pop(rule_id, None)
                    merge_stats["rejected"] += 1
                    explicit_ids.add(rule_id)
                continue

            payload = decision.get("rule") if isinstance(decision.get("rule"), dict) else None
            if payload is None:
                continue
            payload = dict(payload)
            payload["id"] = rule_id
            normalized = normalize_rule(payload, llm_trace["verifier"].get("provider") or "verifier")
            if normalized is None:
                continue

            if action == "refine":
                if rule_id not in by_id:
                    continue
                by_id[rule_id] = normalized
                merge_stats["refined"] += 1
                explicit_ids.add(rule_id)
                continue

            if action == "add":
                if rule_id in by_id:
                    continue
                by_id[rule_id] = normalized
                merge_stats["added"] += 1
                explicit_ids.add(rule_id)

        for rule_id in list(by_id.keys()):
            if rule_id not in explicit_ids:
                merge_stats["kept"] += 1

        merged_items = list(by_id.values())
        llm_trace["merge"] = merge_stats

        policy = dict(llm_trace.get("policy", {}))
        policy["affected_ids"] = []
        if not policy.get("requires_claude_verifier"):
            policy["status"] = "not_required"
            policy["failure_reason"] = None
        elif not merged_items:
            policy["status"] = "satisfied"
            policy["failure_reason"] = None
        else:
            verifier_status = str(llm_trace.get("verifier", {}).get("status", "") or "").strip().lower()
            verifier_provider = str(llm_trace.get("verifier", {}).get("provider", "") or "").strip().lower()
            if verifier_status != "ok":
                policy["status"] = "failed"
                policy["failure_reason"] = "verifier_unavailable"
                policy["affected_ids"] = [str(item.get("id") or "") for item in merged_items if str(item.get("id") or "")]
                if fail_closed:
                    raise RuntimeError("Strict collaboration policy requires Claude verifier for validations")
            elif not settings.is_claude_provider(verifier_provider):
                policy["status"] = "failed"
                policy["failure_reason"] = "verifier_not_claude"
                policy["affected_ids"] = [str(item.get("id") or "") for item in merged_items if str(item.get("id") or "")]
                if fail_closed:
                    raise RuntimeError("Strict collaboration policy requires Claude verifier provider for validations")
            else:
                policy["status"] = "satisfied"
                policy["failure_reason"] = None

        llm_trace["policy"] = policy

        if policy.get("affected_ids"):
            affected_ids = {
                str(item_id).strip()
                for item_id in (policy.get("affected_ids") or [])
                if str(item_id).strip()
            }
            failure_reason = str(policy.get("failure_reason") or "collaboration verifier unavailable")
            for item in merged_items:
                item_id = str(item.get("id") or "")
                if item_id not in affected_ids:
                    continue
                item["approved"] = False
                message_prefix = str(item.get("message") or "").strip()
                if message_prefix:
                    item["message"] = f"{message_prefix} (unverified: {failure_reason})"
                evidence = item.setdefault("reason_evidence", {})
                evidence["collaboration_policy"] = {
                    "mode": mode,
                    "requires_claude_verifier": bool(policy.get("requires_claude_verifier")),
                    "status": policy.get("status"),
                    "failure_reason": policy.get("failure_reason"),
                }

        items = merged_items

    return {
        "bq_validations": items,
        "llm_trace": llm_trace,
    }


def compile_bq_validation_sql(validations: List[Dict[str, Any]], table_id: str = "project.dataset.staging") -> str:
    approved = [v for v in validations if isinstance(v, dict) and bool(v.get("approved", False))]
    if not approved:
        return f"SELECT * FROM `{table_id}`;"

    error_exprs: List[str] = []
    warning_exprs: List[str] = []

    for item in approved:
        expr = str(item.get("sql_expression", "") or "").strip()
        if not expr:
            continue
        msg = str(item.get("message", "Validation failed") or "Validation failed").replace("'", "\\'")
        wrapped = f"IF({expr}, '{msg}', NULL)"
        severity = str(item.get("severity", "error") or "error").lower()
        if severity == "warning":
            warning_exprs.append(wrapped)
        else:
            error_exprs.append(wrapped)

    error_concat = "CONCAT_WS(', ', " + ", ".join(error_exprs) + ")" if error_exprs else "NULL"
    warning_concat = "CONCAT_WS(', ', " + ", ".join(warning_exprs) + ")" if warning_exprs else "NULL"

    return (
        "SELECT t.*,\n"
        f"       {error_concat} AS Business_Validations,\n"
        f"       {warning_concat} AS Warning\n"
        f"FROM `{table_id}` t;"
    )
