"""Schema-first mapping engine with local-first LLM routing and verifier merge."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional

from universal_roster_v2.config import Settings, get_settings
from universal_roster_v2.core.learning_kb import LearningKB
from universal_roster_v2.core.learning_retrieval import LearningRetrieval
from universal_roster_v2.core.schema import SchemaRegistry
from universal_roster_v2.llm.router import LLMRouter, LLMRouterFactory


@dataclass
class MappingSuggestion:
    source_column: str
    target_field: str
    confidence: float
    reason: str
    suggested_by: str = "deterministic"


def _normalize_header(value: str) -> str:
    text = str(value or "").strip()
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    text = re.sub(r"[_\-/:()]+", " ", text)
    text = re.sub(r"\s+", " ", text).lower().strip()
    return text


def _tokens(value: str) -> List[str]:
    return [p for p in _normalize_header(value).split(" ") if p]


def _token_similarity(source: str, candidate: str) -> float:
    s_tokens = set(_tokens(source))
    c_tokens = set(_tokens(candidate))
    if not s_tokens or not c_tokens:
        return 0.0

    overlap = len(s_tokens & c_tokens)
    coverage = overlap / max(len(c_tokens), 1)
    precision = overlap / max(len(s_tokens), 1)
    seq = SequenceMatcher(None, " ".join(sorted(s_tokens)), " ".join(sorted(c_tokens))).ratio()
    return (coverage * 0.5) + (precision * 0.2) + (seq * 0.3)


def _best_field_by_name(source_column: str, fields: List[str]) -> tuple[str, float, Dict[str, Any]]:
    best_field = ""
    best_score = 0.0
    source_norm = _normalize_header(source_column)
    source_tokens = set(_tokens(source_norm))
    evidence: Dict[str, Any] = {"source_tokens": sorted(source_tokens), "candidate": "", "matched_tokens": []}

    for field in fields:
        field_norm = _normalize_header(field)
        if field_norm == source_norm:
            return field, 0.99, {"source_tokens": sorted(source_tokens), "candidate": field, "matched_tokens": sorted(source_tokens)}

        score = _token_similarity(source_norm, field_norm)
        if score > best_score:
            best_score = score
            best_field = field
            candidate_tokens = set(_tokens(field_norm))
            evidence = {
                "source_tokens": sorted(source_tokens),
                "candidate": field,
                "matched_tokens": sorted(source_tokens & candidate_tokens),
            }

    return best_field, best_score, evidence


def _validate_by_samples(meta: Dict[str, Any], samples: List[str]) -> float:
    if not samples:
        return 0.0

    usable = [s for s in samples if str(s).strip()]
    if not usable:
        return 0.0

    if meta.get("enum"):
        enum_values = {str(v).strip().lower() for v in (meta.get("enum") or []) if str(v).strip()}
        if enum_values:
            matched = sum(1 for value in usable if str(value).strip().lower() in enum_values)
            return matched / len(usable)

    pattern = meta.get("pattern")
    if pattern:
        try:
            compiled = re.compile(pattern)
        except re.error:
            compiled = None
        if compiled is not None:
            matched = sum(1 for value in usable if compiled.fullmatch(str(value).strip()) is not None)
            return matched / len(usable)

    fmt = str(meta.get("format") or "").lower()
    if fmt == "email":
        matched = sum(1 for value in usable if re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", str(value).strip()))
        return matched / len(usable)
    if fmt == "date":
        matched = sum(1 for value in usable if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(value).strip()))
        return matched / len(usable)

    return 0.0


def _semantic_type_score(meta: Dict[str, Any], semantic_profile: Dict[str, Any]) -> float:
    if not semantic_profile:
        return 0.0
    likelihoods = semantic_profile.get("aggregate_type_likelihoods") or {}
    if not isinstance(likelihoods, dict):
        return 0.0

    fmt = str(meta.get("format") or "").lower()
    field_name = str(meta.get("name") or "").lower()

    if fmt == "email":
        return float(likelihoods.get("email", 0.0) or 0.0)
    if fmt == "date":
        return float(likelihoods.get("date", 0.0) or 0.0)

    if "phone" in field_name or "fax" in field_name:
        return float(likelihoods.get("phone", 0.0) or 0.0)
    if any(token in field_name for token in ["npi", "tin", "ssn", "id"]):
        return float(likelihoods.get("identifier", 0.0) or 0.0)

    if meta.get("enum"):
        return max(0.2, float(likelihoods.get("text", 0.0) or 0.0))

    return 0.0


def _safe_confidence(value: Any, default: float = 0.0) -> float:
    try:
        raw = float(value)
    except Exception:
        raw = default
    return max(0.0, min(1.0, raw))


def confidence_band(confidence: Any) -> str:
    value = _safe_confidence(confidence)
    if value >= 0.85:
        return "High"
    if value >= 0.65:
        return "Medium"
    return "Low"


_NOTE_SNIPPET_KEYS = ("free_text_notes", "client_rules", "schema_caveats", "exceptions", "attachment_hints")


def clean_note_snippets(instructions_context: Optional[Dict[str, Any]], limit: int = 8) -> List[str]:
    if not isinstance(instructions_context, dict):
        return []
    snippets: List[str] = []
    for key in _NOTE_SNIPPET_KEYS:
        for item in instructions_context.get(key, []) or []:
            text = str(item).strip()
            if not text:
                continue
            snippets.append(text)
            if len(snippets) >= limit:
                return snippets
    return snippets


def _repair_truncated_json(text: str) -> Optional[Any]:
    """Attempt to repair truncated JSON by finding the last complete array element."""
    # Find the last complete object in a JSON array (ends with })
    # Then close the array and any outer object
    last_complete = text.rfind("}")
    if last_complete == -1:
        return None

    # Walk backwards to find the last position where we have a complete array element
    for end_pos in range(last_complete, max(0, last_complete - 5000), -1):
        if text[end_pos] != "}":
            continue
        # Try closing with ]} or just ]
        for suffix in ["]}", "]", "]}}", "]}]}}"]:
            candidate = text[: end_pos + 1] + suffix
            try:
                data = json.loads(candidate)
                return data
            except Exception:
                pass
    return None


def extract_json_object(raw: str) -> Dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}

    def _wrap_array(arr: list) -> Dict[str, Any]:
        """Wrap a bare JSON array into a dict with the right key based on content."""
        if not arr:
            return {}
        first = arr[0] if isinstance(arr[0], dict) else {}
        if "source_column" in first or "target_field" in first:
            return {"mappings": arr}
        if "rule_type" in first or "sql_expression" in first:
            return {"rules": arr}
        if "category" in first or "severity" in first:
            return {"issues": arr}
        if "action" in first:
            return {"decisions": arr}
        if "name" in first and ("source_columns" in first or "params" in first):
            return {"candidates": arr}
        return {"mappings": arr}

    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            return _wrap_array(data)
    except Exception:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = text[start : end + 1]
        try:
            data = json.loads(candidate)
            if isinstance(data, dict):
                return data
        except Exception:
            pass

    start = text.find("[")
    end = text.rfind("]")
    if start != -1 and end != -1 and end > start:
        try:
            data = json.loads(text[start : end + 1])
            if isinstance(data, list):
                return _wrap_array(data)
        except Exception:
            pass

    # Attempt to repair truncated JSON (e.g. from token limit cutoff)
    repaired = _repair_truncated_json(text)
    if repaired is not None:
        if isinstance(repaired, dict):
            return repaired
        if isinstance(repaired, list):
            return _wrap_array(repaired)

    return {}


class MappingEngine:
    """Combine deterministic mapping with schema-validated LLM primary+verifier flow."""

    def __init__(
        self,
        schema_registry: SchemaRegistry,
        llm_router: Optional[LLMRouter] = None,
        settings: Optional[Settings] = None,
        learning_kb: Optional[LearningKB] = None,
        verifier_router: Optional[LLMRouter] = None,
        learning_retrieval: Optional[LearningRetrieval] = None,
    ):
        self.schema_registry = schema_registry
        self.settings = settings or get_settings()
        self.learning_kb = learning_kb
        self.learning_retrieval = learning_retrieval
        self.router_factory = LLMRouterFactory(settings=self.settings)

        if llm_router is not None:
            self.primary_router = llm_router
        else:
            self.primary_router = self.router_factory.for_task("mappings")

        if verifier_router is not None:
            self.verifier_router = verifier_router
        else:
            self.verifier_router = self.router_factory.for_task("verifier")

        self.llm_router = self.primary_router

    def suggest_mappings(
        self,
        columns: List[str],
        sample_values: Dict[str, List[str]],
        roster_type: str,
        use_llm_for_unresolved: bool = True,
        instructions_context: Optional[Dict[str, Any]] = None,
        semantic_profile: Optional[Dict[str, Any]] = None,
        learning_scope: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        fields = self.schema_registry.list_fields(roster_type)
        items: List[Dict[str, Any]] = []
        unresolved: List[str] = []

        semantic_by_column = (semantic_profile or {}).get("column_semantics", {}) if isinstance(semantic_profile, dict) else {}

        for column in columns:
            candidate_field, name_score, token_evidence = _best_field_by_name(column, fields)
            candidate_meta = self.schema_registry.field_metadata(candidate_field, roster_type) if candidate_field else None
            sample_score = _validate_by_samples(candidate_meta or {}, sample_values.get(column, [])) if candidate_meta else 0.0
            semantic_score = _semantic_type_score(candidate_meta or {}, semantic_by_column.get(column) or {}) if candidate_meta else 0.0

            target = candidate_field if name_score >= 0.62 else ""
            confidence = min(1.0, (name_score * 0.6) + (sample_score * 0.25) + (semantic_score * 0.15)) if target else 0.0

            threshold = float(self.settings.auto_approval_mapping_threshold)
            approved = bool(target and confidence >= threshold) if self.settings.enable_auto_approval_policy else bool(target and confidence >= 0.74)

            reason = "Schema/token similarity"
            if sample_score > 0:
                reason += "+sample validation"
            if semantic_score > 0:
                reason += "+semantic profile"

            item = {
                "id": f"map::{column}",
                "source_column": column,
                "target_field": target,
                "confidence": round(confidence, 4),
                "confidence_band": confidence_band(confidence),
                "approved": approved,
                "reason": reason if target else "No strong deterministic match",
                "reason_evidence": {
                    "matched_tokens": token_evidence.get("matched_tokens", []),
                    "token_source": token_evidence.get("source_tokens", []),
                    "schema_metadata": {
                        "target_field": candidate_field,
                        "format": (candidate_meta or {}).get("format"),
                        "pattern": bool((candidate_meta or {}).get("pattern")),
                        "enum_count": len((candidate_meta or {}).get("enum") or []),
                    },
                    "sample_pattern_evidence": {
                        "sample_score": round(sample_score, 4),
                        "sample_values": sample_values.get(column, [])[:6],
                    },
                    "semantic_profile_evidence": {
                        "semantic_score": round(semantic_score, 4),
                        "type_likelihoods": ((semantic_by_column.get(column) or {}).get("aggregate_type_likelihoods") or {}),
                    },
                    "note_directives": [],
                },
                "suggested_by": "deterministic",
                "schema_valid": bool(target),
            }
            if not target or confidence < 0.70:
                # Skip empty/unnamed columns — no point sending to LLM
                col_stripped = column.strip()
                if col_stripped and not col_stripped.startswith("Unnamed:") and not col_stripped.startswith("Unnamed "):
                    # Only send columns that actually have data
                    col_has_data = any(str(v).strip() for v in sample_values.get(column, []))
                    if col_has_data or col_stripped:
                        unresolved.append(column)
            items.append(item)

        policy_requires_claude_verifier = bool(
            self.settings.is_strict_collaboration()
            or self.settings.require_claude_verifier_for_section("mappings")
        )

        llm_trace: Dict[str, Any] = {
            "used": False,
            "provider_order": {
                "analysis": self.primary_router.provider_names(),
                "verifier": self.verifier_router.provider_names(),
            },
            "primary": {
                "enabled": bool(use_llm_for_unresolved),
                "status": "skipped",
                "provider": None,
                "model": None,
                "attempts": [],
            },
            "verifier": {
                "enabled": bool(self.settings.enable_claude_verifier),
                "status": "skipped",
                "provider": None,
                "model": None,
                "attempts": [],
            },
            "merge": {"kept": 0, "refined": 0, "rejected": 0, "added": 0},
            "policy": {
                "mode": self.settings.collaboration_mode,
                "requires_claude_verifier": policy_requires_claude_verifier,
                "status": "not_required" if not policy_requires_claude_verifier else "pending",
                "fail_closed": bool(self.settings.strict_fail_closed()),
                "failure_reason": None,
            },
        }

        retrieval_by_source: Dict[str, Dict[str, Any]] = {}
        llm_trace["retrieval"] = {"enabled": bool(self.settings.enable_rag_retrieval), "count": 0, "sources": [], "scores": []}

        logging.warning(f"MAPPING DEBUG: unresolved={len(unresolved)} columns, use_llm={use_llm_for_unresolved}")

        if use_llm_for_unresolved and unresolved:
            llm_trace["used"] = True
            primary_updates, primary_trace, retrieval_summary = self._llm_primary_suggest_unresolved(
                unresolved=unresolved,
                sample_values=sample_values,
                roster_type=roster_type,
                instructions_context=instructions_context,
                semantic_profile=semantic_profile,
                learning_scope=learning_scope,
            )
            llm_trace["primary"].update(primary_trace)
            llm_trace["retrieval"] = retrieval_summary
            retrieval_by_source = {
                str(hit.get("candidate_key", {}).get("source_column") or ""): hit
                for hit in (retrieval_summary.get("hits") or [])
                if str(hit.get("candidate_key", {}).get("source_column") or "")
            }

            verifier_decisions: List[Dict[str, Any]] = []
            verifier_trace: Dict[str, Any] = {
                "enabled": bool(self.settings.enable_claude_verifier),
                "status": "skipped",
                "attempts": [],
            }
            if self.settings.enable_claude_verifier:
                verifier_decisions, verifier_trace = self._llm_verify_primary_suggestions(
                    unresolved=unresolved,
                    primary_updates=primary_updates,
                    sample_values=sample_values,
                    roster_type=roster_type,
                    instructions_context=instructions_context,
                    semantic_profile=semantic_profile,
                )
            llm_trace["verifier"].update(verifier_trace)

            merged_updates, merge_stats = self._merge_primary_and_verifier(
                unresolved=unresolved,
                primary_updates=primary_updates,
                verifier_decisions=verifier_decisions,
                roster_type=roster_type,
            )
            llm_trace["merge"] = merge_stats
            logging.warning(f"MAPPING DEBUG: primary_updates={len(primary_updates)}, merged={len(merged_updates)}, merge_stats={merge_stats}")

            policy_status = self._enforce_mapping_collaboration_policy(
                unresolved=unresolved,
                primary_updates=primary_updates,
                merged_updates=merged_updates,
                verifier_trace=llm_trace.get("verifier", {}),
                policy_trace=llm_trace.get("policy", {}),
            )
            llm_trace["policy"] = policy_status

            if merged_updates:
                by_source = {item["source_column"]: item for item in items}
                for update in merged_updates:
                    source = update.get("source_column")
                    if source not in by_source:
                        continue
                    existing = by_source[source]
                    existing_conf = float(existing.get("confidence", 0.0) or 0.0)
                    update_conf = float(update.get("confidence", 0.0) or 0.0)
                    # Override if deterministic had no target OR if Gemini has higher confidence
                    if not existing.get("target_field") or update_conf > existing_conf:
                        by_source[source].update(update)
                        if source in retrieval_by_source:
                            by_source[source]["reason_evidence"] = by_source[source].get("reason_evidence", {})
                            by_source[source]["reason_evidence"]["rag_examples"] = retrieval_by_source[source]

            if policy_status.get("affected_sources"):
                by_source = {item["source_column"]: item for item in items}
                affected_sources = {str(src).strip() for src in (policy_status.get("affected_sources") or []) if str(src).strip()}
                for source in affected_sources:
                    target_item = by_source.get(source)
                    if not target_item or not target_item.get("target_field"):
                        continue
                    target_item["approved"] = False
                    reason_prefix = str(target_item.get("reason", "") or "").strip()
                    failure_reason = str(policy_status.get("failure_reason") or "collaboration verifier unavailable")
                    target_item["reason"] = (
                        f"{reason_prefix}; unverified ({failure_reason})"
                        if reason_prefix
                        else f"unverified ({failure_reason})"
                    )
                    evidence = target_item.setdefault("reason_evidence", {})
                    evidence["collaboration_policy"] = {
                        "mode": self.settings.collaboration_mode,
                        "requires_claude_verifier": bool(policy_status.get("requires_claude_verifier")),
                        "status": policy_status.get("status"),
                        "failure_reason": policy_status.get("failure_reason"),
                    }

        note_snippets = clean_note_snippets(instructions_context)
        for item in items:
            self._apply_mapping_prior(item=item, roster_type=roster_type, scope=learning_scope)
            item["confidence_band"] = confidence_band(item.get("confidence", 0.0))
            evidence = item.setdefault("reason_evidence", {})
            if note_snippets:
                evidence["note_directives"] = note_snippets[:4]
                if item.get("target_field"):
                    item["reason"] = f"{item.get('reason', '')}; notes considered"

            if self.settings.enable_auto_approval_policy and item.get("target_field"):
                if (
                    self.settings.is_strict_collaboration()
                    and str((llm_trace.get("policy") or {}).get("status", "") or "").strip().lower() == "failed"
                    and str(item.get("source_column", "") or "").strip()
                    in {
                        str(src).strip()
                        for src in ((llm_trace.get("policy") or {}).get("affected_sources") or [])
                        if str(src).strip()
                    }
                ):
                    item["approved"] = False
                else:
                    item["approved"] = bool(
                        float(item.get("confidence", 0.0) or 0.0)
                        >= float(self.settings.auto_approval_mapping_threshold)
                    )

        valid, invalid = self.schema_registry.validate_mapping_targets(items, roster_type)
        return {
            "mappings": items,
            "valid_count": len(valid),
            "invalid_count": len(invalid),
            "llm_trace": llm_trace,
        }

    def _llm_primary_suggest_unresolved(
        self,
        unresolved: List[str],
        sample_values: Dict[str, List[str]],
        roster_type: str,
        instructions_context: Optional[Dict[str, Any]] = None,
        semantic_profile: Optional[Dict[str, Any]] = None,
        learning_scope: Optional[Dict[str, Any]] = None,
    ) -> tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
        retrieval_summary: Dict[str, Any] = {
            "enabled": bool(self.learning_retrieval and self.learning_retrieval.is_enabled()),
            "count": 0,
            "sources": [],
            "scores": [],
            "hits": [],
        }
        if not unresolved:
            return [], {"status": "skipped", "provider": None, "model": None, "attempts": []}, retrieval_summary

        # Batch unresolved columns if too many (>80 causes token truncation)
        BATCH_SIZE = 80
        if len(unresolved) > BATCH_SIZE:
            all_updates: List[Dict[str, Any]] = []
            all_attempts: List[str] = []
            for batch_start in range(0, len(unresolved), BATCH_SIZE):
                batch = unresolved[batch_start:batch_start + BATCH_SIZE]
                batch_updates, batch_trace, _ = self._llm_primary_suggest_unresolved(
                    unresolved=batch,
                    sample_values=sample_values,
                    roster_type=roster_type,
                    instructions_context=instructions_context,
                    semantic_profile=semantic_profile,
                    learning_scope=learning_scope,
                )
                all_updates.extend(batch_updates)
                all_attempts.extend(batch_trace.get("attempts", []))
            trace = {"status": "ok", "provider": "gemini_vertex", "model": None, "attempts": all_attempts, "batches": len(range(0, len(unresolved), BATCH_SIZE))}
            return all_updates, trace, retrieval_summary

        field_block = self.schema_registry.fields_prompt_block(roster_type=roster_type, max_items=700)
        sample_hint = {col: sample_values.get(col, [])[:8] for col in unresolved}

        semantic_hint: Dict[str, Any] = {}
        if isinstance(semantic_profile, dict):
            by_column = semantic_profile.get("column_semantics") or {}
            if isinstance(by_column, dict):
                for col in unresolved:
                    if col in by_column:
                        semantic_hint[col] = by_column.get(col)

        note_snippets = clean_note_snippets(instructions_context)

        rag_examples: Dict[str, List[Dict[str, Any]]] = {}
        retrieval_hits: List[Dict[str, Any]] = []
        if self.learning_retrieval is not None and self.learning_retrieval.is_enabled():
            for source in unresolved:
                retrieval_result = self.learning_retrieval.retrieve(
                    section="mappings",
                    item_key={"item_id": f"map::{source}", "source_column": source, "target_field": ""},
                    roster_type=roster_type,
                    workspace_scope=learning_scope,
                )
                examples = self.learning_retrieval.format_examples_for_prompt("mappings", retrieval_result)
                if not examples:
                    continue
                rag_examples[source] = examples
                top = examples[0]
                retrieval_hits.append(
                    {
                        "source": "learning_episode",
                        "candidate_key": top.get("candidate_key") or {"source_column": source},
                        "final_candidate": top.get("final_candidate") or {},
                        "score": top.get("score"),
                        "item_id": top.get("item_id") or f"map::{source}",
                    }
                )

        retrieval_summary = {
            "enabled": bool(self.learning_retrieval and self.learning_retrieval.is_enabled()),
            "count": len(retrieval_hits),
            "sources": sorted({str(hit.get("source") or "") for hit in retrieval_hits if str(hit.get("source") or "")}),
            "scores": [hit.get("score") for hit in retrieval_hits],
            "hits": retrieval_hits,
        }

        prompt = f"""
You map healthcare roster columns to a strict schema.
Return JSON only with key 'mappings'.

Roster type: {roster_type}
Unresolved columns:
{json.dumps(unresolved, ensure_ascii=False, indent=2)}

Sample values:
{json.dumps(sample_hint, ensure_ascii=False, indent=2)}

Semantic profile hints:
{json.dumps(semantic_hint, ensure_ascii=False, indent=2)}

Retrieved accepted exemplars:
{json.dumps(rag_examples, ensure_ascii=False, indent=2)}

Notes and directives:
{json.dumps(note_snippets, ensure_ascii=False, indent=2)}

Allowed target fields:
{field_block}

IMPORTANT: Return MINIMAL JSON to save tokens. Only these 3 fields per mapping:
{{
  "mappings": [
    {{"source_column":"...", "target_field":"...", "confidence":0.0}}
  ]
}}
Do NOT include reason, reason_evidence, or any other fields.
""".strip()

        try:
            routed = self.primary_router.generate(prompt=prompt, task_type="analysis")
        except Exception as exc:
            logging.error(f"MAPPING LLM CALL FAILED: {type(exc).__name__}: {exc}")
            return [], {"status": "error", "provider": None, "model": None, "attempts": [f"error:{exc}"]}, retrieval_summary

        parsed = extract_json_object(routed.response.text)
        updates: List[Dict[str, Any]] = []
        logging.warning(f"MAPPING DEBUG INNER: raw_text_len={len(routed.response.text)}, parsed_keys={list(parsed.keys())}, mappings_count={len(parsed.get('mappings', []))}")

        # Build a normalized lookup for fuzzy matching LLM source_column to actual column names
        _norm_to_actual: Dict[str, str] = {}
        for col in unresolved:
            normalized = re.sub(r"[^a-z0-9]", "", col.lower())
            _norm_to_actual[normalized] = col
            _norm_to_actual[col] = col  # exact match too

        for item in parsed.get("mappings", []):
            if not isinstance(item, dict):
                continue
            raw_source = str(item.get("source_column", "") or "").strip()
            target = str(item.get("target_field", "") or "").strip()
            if not raw_source or not target:
                continue
            # Try exact match first, then normalized match
            source = _norm_to_actual.get(raw_source)
            if source is None:
                norm_key = re.sub(r"[^a-z0-9]", "", raw_source.lower())
                source = _norm_to_actual.get(norm_key)
            if source is None:
                continue
            if not self.schema_registry.is_valid_field(target, roster_type):
                continue

            confidence = _safe_confidence(item.get("confidence", 0.0), default=0.0)
            reason = str(item.get("reason", "LLM primary suggestion") or "LLM primary suggestion")
            reason_evidence = item.get("reason_evidence") if isinstance(item.get("reason_evidence"), dict) else {}
            updates.append(
                {
                    "source_column": source,
                    "target_field": target,
                    "confidence": confidence,
                    "confidence_band": confidence_band(confidence),
                    "approved": confidence >= float(self.settings.auto_approval_mapping_threshold),
                    "reason": reason,
                    "reason_evidence": {
                        "matched_tokens": reason_evidence.get("matched_tokens", []),
                        "sample_pattern_evidence": reason_evidence.get("sample_pattern_evidence", {}),
                        "schema_metadata": reason_evidence.get("schema_metadata", {}),
                        "note_directives": reason_evidence.get("note_directives", note_snippets[:4]),
                        "rag_examples": rag_examples.get(source, [])[:2],
                    },
                    "suggested_by": routed.response.provider,
                    "schema_valid": True,
                }
            )

        trace = {
            "status": "ok",
            "provider": routed.response.provider,
            "model": routed.response.model,
            "attempts": routed.attempts,
            "suggestion_count": len(updates),
            "retrieval_example_count": len(retrieval_hits),
            "task_type": "analysis",
        }
        return updates, trace, retrieval_summary

    def _llm_verify_primary_suggestions(
        self,
        unresolved: List[str],
        primary_updates: List[Dict[str, Any]],
        sample_values: Dict[str, List[str]],
        roster_type: str,
        instructions_context: Optional[Dict[str, Any]] = None,
        semantic_profile: Optional[Dict[str, Any]] = None,
    ) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        if not unresolved:
            return [], {"status": "skipped", "provider": None, "model": None, "attempts": []}

        field_block = self.schema_registry.fields_prompt_block(roster_type=roster_type, max_items=700)
        sample_hint = {col: sample_values.get(col, [])[:8] for col in unresolved}

        semantic_hint: Dict[str, Any] = {}
        if isinstance(semantic_profile, dict):
            by_column = semantic_profile.get("column_semantics") or {}
            if isinstance(by_column, dict):
                for col in unresolved:
                    if col in by_column:
                        semantic_hint[col] = by_column.get(col)

        note_snippets = clean_note_snippets(instructions_context)

        prompt = f"""
You are verifying unresolved-to-schema mappings for healthcare roster ingestion.
Primary suggestions came from a local model. Review and return JSON only.

Roster type: {roster_type}
Unresolved columns:
{json.dumps(unresolved, ensure_ascii=False, indent=2)}

Sample values:
{json.dumps(sample_hint, ensure_ascii=False, indent=2)}

Semantic profile hints:
{json.dumps(semantic_hint, ensure_ascii=False, indent=2)}

Notes and directives:
{json.dumps(note_snippets, ensure_ascii=False, indent=2)}

Primary suggestions:
{json.dumps(primary_updates, ensure_ascii=False, indent=2)}

Allowed target fields:
{field_block}

Decision rules:
- action=keep: keep a primary suggestion as-is.
- action=refine: same source_column, but change target_field and/or confidence/reason.
- action=reject: remove primary suggestion.
- action=add: propose mapping for unresolved source that primary missed.

Output schema:
{{
  "decisions": [
    {{
      "action": "keep|refine|reject|add",
      "source_column": "...",
      "target_field": "... optional for keep/reject",
      "confidence": 0.0,
      "reason": "..."
    }}
  ]
}}
""".strip()

        try:
            routed = self.verifier_router.generate(prompt=prompt, task_type="verifier")
        except Exception as exc:
            return [], {"status": "error", "provider": None, "model": None, "attempts": [f"error:{exc}"]}

        parsed = extract_json_object(routed.response.text)
        raw_decisions = parsed.get("decisions")
        if not isinstance(raw_decisions, list):
            raw_decisions = parsed.get("reviews") if isinstance(parsed.get("reviews"), list) else []

        primary_sources = {str(item.get("source_column", "") or "").strip() for item in primary_updates}
        unresolved_set = {str(col or "").strip() for col in unresolved}
        decisions: List[Dict[str, Any]] = []

        for decision in raw_decisions:
            if not isinstance(decision, dict):
                continue
            action = str(decision.get("action", "") or "").strip().lower()
            source = str(decision.get("source_column", "") or "").strip()
            target = str(decision.get("target_field", "") or "").strip()
            reason = str(decision.get("reason", "Verifier review") or "Verifier review")
            confidence = _safe_confidence(decision.get("confidence", 0.0), default=0.0)

            if action not in {"keep", "refine", "reject", "add"}:
                continue
            if not source:
                continue

            if action in {"keep", "refine", "reject"} and source not in primary_sources:
                continue
            if action == "add" and source not in unresolved_set:
                continue

            if action in {"refine", "add"}:
                if not target or not self.schema_registry.is_valid_field(target, roster_type):
                    continue

            decisions.append(
                {
                    "action": action,
                    "source_column": source,
                    "target_field": target,
                    "confidence": confidence,
                    "reason": reason,
                    "suggested_by": routed.response.provider,
                }
            )

        trace = {
            "status": "ok",
            "provider": routed.response.provider,
            "model": routed.response.model,
            "attempts": routed.attempts,
            "decision_count": len(decisions),
            "task_type": "verifier",
        }
        return decisions, trace

    def _merge_primary_and_verifier(
        self,
        unresolved: List[str],
        primary_updates: List[Dict[str, Any]],
        verifier_decisions: List[Dict[str, Any]],
        roster_type: str,
    ) -> tuple[List[Dict[str, Any]], Dict[str, int]]:
        unresolved_order = [str(col or "").strip() for col in unresolved if str(col or "").strip()]
        unresolved_set = set(unresolved_order)

        merged_by_source: Dict[str, Dict[str, Any]] = {
            str(item.get("source_column", "") or "").strip(): dict(item)
            for item in primary_updates
            if str(item.get("source_column", "") or "").strip() in unresolved_set
        }

        explicit_actions: set[str] = set()
        merge_stats = {"kept": 0, "refined": 0, "rejected": 0, "added": 0}

        for decision in verifier_decisions:
            action = str(decision.get("action", "") or "").strip().lower()
            source = str(decision.get("source_column", "") or "").strip()
            reason = str(decision.get("reason", "") or "").strip()
            target = str(decision.get("target_field", "") or "").strip()
            confidence = _safe_confidence(decision.get("confidence", 0.0), default=0.0)
            suggested_by = str(decision.get("suggested_by", "claude_cli") or "claude_cli")

            if not source or source not in unresolved_set:
                continue

            if action == "keep":
                if source in merged_by_source:
                    explicit_actions.add(source)
                    merge_stats["kept"] += 1
                    if reason:
                        merged_by_source[source]["reason"] = reason
                continue

            if action == "reject":
                if source in merged_by_source:
                    explicit_actions.add(source)
                    merge_stats["rejected"] += 1
                    merged_by_source.pop(source, None)
                continue

            if action == "refine":
                if source not in merged_by_source:
                    continue
                if not target or not self.schema_registry.is_valid_field(target, roster_type):
                    continue
                explicit_actions.add(source)
                merge_stats["refined"] += 1
                refined = merged_by_source[source]
                refined["target_field"] = target
                refined["confidence"] = confidence
                refined["confidence_band"] = confidence_band(confidence)
                refined["approved"] = confidence >= float(self.settings.auto_approval_mapping_threshold)
                refined["reason"] = reason or "Verifier refinement"
                refined["suggested_by"] = suggested_by
                refined["schema_valid"] = True
                continue

            if action == "add":
                if source in merged_by_source:
                    continue
                if not target or not self.schema_registry.is_valid_field(target, roster_type):
                    continue
                explicit_actions.add(source)
                merge_stats["added"] += 1
                merged_by_source[source] = {
                    "source_column": source,
                    "target_field": target,
                    "confidence": confidence,
                    "confidence_band": confidence_band(confidence),
                    "approved": confidence >= float(self.settings.auto_approval_mapping_threshold),
                    "reason": reason or "Verifier-added suggestion",
                    "reason_evidence": {
                        "matched_tokens": [],
                        "sample_pattern_evidence": {},
                        "schema_metadata": {"target_field": target},
                        "note_directives": [],
                    },
                    "suggested_by": suggested_by,
                    "schema_valid": True,
                }

        for source in unresolved_order:
            if source in merged_by_source and source not in explicit_actions:
                merge_stats["kept"] += 1

        merged_updates = [
            merged_by_source[source]
            for source in unresolved_order
            if source in merged_by_source and merged_by_source[source].get("target_field")
        ]
        return merged_updates, merge_stats

    def _enforce_mapping_collaboration_policy(
        self,
        unresolved: List[str],
        primary_updates: List[Dict[str, Any]],
        merged_updates: List[Dict[str, Any]],
        verifier_trace: Dict[str, Any],
        policy_trace: Dict[str, Any],
    ) -> Dict[str, Any]:
        policy = dict(policy_trace or {})
        policy.setdefault("mode", self.settings.collaboration_mode)
        policy.setdefault(
            "requires_claude_verifier",
            bool(self.settings.is_strict_collaboration() or self.settings.require_claude_verifier_for_section("mappings")),
        )
        policy.setdefault("fail_closed", bool(self.settings.strict_fail_closed()))
        policy["affected_sources"] = []

        requires_verifier = bool(policy.get("requires_claude_verifier"))
        if not requires_verifier:
            policy["status"] = "not_required"
            policy["failure_reason"] = None
            return policy

        unresolved_set = {str(col).strip() for col in unresolved if str(col).strip()}
        primary_sources = {
            str(item.get("source_column", "") or "").strip()
            for item in primary_updates
            if str(item.get("source_column", "") or "").strip() in unresolved_set
        }
        merged_sources = {
            str(item.get("source_column", "") or "").strip()
            for item in merged_updates
            if str(item.get("source_column", "") or "").strip() in unresolved_set
        }

        if not primary_sources and not merged_sources:
            policy["status"] = "satisfied"
            policy["failure_reason"] = None
            return policy

        verifier_status = str(verifier_trace.get("status", "") or "").strip().lower()
        verifier_provider = str(verifier_trace.get("provider", "") or "").strip().lower()

        if verifier_status != "ok":
            policy["status"] = "failed"
            policy["failure_reason"] = "verifier_unavailable"
            policy["affected_sources"] = sorted(merged_sources)
            if self.settings.strict_fail_closed():
                raise RuntimeError("Strict collaboration policy requires Claude verifier, but verifier stage failed")
            return policy

        if not self.settings.is_claude_provider(verifier_provider):
            policy["status"] = "failed"
            policy["failure_reason"] = "verifier_not_claude"
            policy["affected_sources"] = sorted(merged_sources)
            if self.settings.strict_fail_closed():
                raise RuntimeError("Strict collaboration policy requires Claude verifier provider (claude_cli/claude_api)")
            return policy

        policy["status"] = "satisfied"
        policy["failure_reason"] = None
        return policy

    def _kb_get_mapping_feedback(
        self,
        roster_type: str,
        source_column: str,
        target_field: str,
        scope: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, int]:
        if self.learning_kb is None:
            return {"approved": 0, "rejected": 0, "added": 0}

        getter = getattr(self.learning_kb, "get_mapping_feedback", None)
        if getter is None:
            return {"approved": 0, "rejected": 0, "added": 0}

        try:
            return getter(
                roster_type=roster_type,
                source_column=source_column,
                target_field=target_field,
                scope=scope,
            )
        except TypeError:
            return getter(roster_type=roster_type, source_column=source_column, target_field=target_field)
        except Exception:
            return {"approved": 0, "rejected": 0, "added": 0}

    def _apply_mapping_prior(self, item: Dict[str, Any], roster_type: str, scope: Optional[Dict[str, Any]] = None) -> None:
        if self.learning_kb is None:
            return

        source = str(item.get("source_column", "") or "").strip()
        target = str(item.get("target_field", "") or "").strip()
        if not source or not target:
            return

        stats = self._kb_get_mapping_feedback(roster_type=roster_type, source_column=source, target_field=target, scope=scope)
        adjustment = self._feedback_adjustment(stats)
        confidence = _safe_confidence(item.get("confidence", 0.0), default=0.0)
        item["confidence"] = round(_safe_confidence(confidence + adjustment, default=confidence), 4)
        item["confidence_band"] = confidence_band(item.get("confidence", 0.0))

        approved = int(stats.get("approved", 0) or 0)
        rejected = int(stats.get("rejected", 0) or 0)
        if rejected >= approved + 3:
            item["approved"] = False
        elif approved >= rejected + 3 and item["confidence"] >= 0.62:
            item["approved"] = True

        evidence = item.setdefault("reason_evidence", {})
        evidence["kb_prior"] = {
            "approved": approved,
            "rejected": rejected,
            "added": int(stats.get("added", 0) or 0),
            "adjustment": round(adjustment, 4),
        }

    @staticmethod
    def _feedback_adjustment(stats: Dict[str, Any]) -> float:
        approved = int(stats.get("approved", 0) or 0)
        rejected = int(stats.get("rejected", 0) or 0)
        added = int(stats.get("added", 0) or 0)
        net = (approved + added) - rejected
        if net > 0:
            return min(0.2, 0.02 * net)
        if net < 0:
            return max(-0.25, 0.025 * net)
        return 0.0

