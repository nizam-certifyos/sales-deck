"""Quality-audit suggestion engine with LLM-primary generation and deterministic fallback."""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from universal_roster_v2.config import Settings, get_settings
from universal_roster_v2.core.learning_kb import LearningKB
from universal_roster_v2.core.learning_retrieval import LearningRetrieval
from universal_roster_v2.core.mapping import confidence_band, extract_json_object
from universal_roster_v2.core.quality_audit_enrichment import enrich_quality_audit
from universal_roster_v2.llm.router import LLMRouter, LLMRouterFactory


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_PHONE_RE = re.compile(r"^\+?[0-9().\-\s]{7,}$")
_STATE_RE = re.compile(r"^[A-Z]{2}$")
_ZIP_RE = re.compile(r"^\d{5}(?:-\d{4})?$")
_NPI_RE = re.compile(r"^\d{10}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _clean(v: Any) -> str:
    return str(v or "").strip()


def _norm(v: Any) -> str:
    return _clean(v).lower()


def _stable_id(*parts: Any) -> str:
    token = "::".join(_norm(p).replace(" ", "_") for p in parts if _clean(p))
    token = re.sub(r"[^a-z0-9_:.-]", "_", token)
    token = re.sub(r"_+", "_", token).strip("_")
    return token or "quality_rule"


def _field_severity(conf: float, hard_fail: bool = False) -> str:
    if hard_fail:
        return "error"
    if conf >= 0.8:
        return "error"
    if conf >= 0.5:
        return "warning"
    return "info"


def _make_issue(
    *,
    category: str,
    rule_type: str,
    source_column: str,
    target_field: str,
    title: str,
    message: str,
    confidence: float,
    affected_rows: int,
    rows_profiled: int,
    sample_values: Optional[List[str]] = None,
    evidence: Optional[Dict[str, Any]] = None,
    suggested_fix: Optional[Dict[str, Any]] = None,
    severity: Optional[str] = None,
) -> Dict[str, Any]:
    conf = max(0.0, min(1.0, float(confidence or 0.0)))
    total = max(0, int(rows_profiled or 0))
    affected = max(0, int(affected_rows or 0))
    pct = round((affected / total), 4) if total > 0 else 0.0
    sev = severity or _field_severity(conf)
    issue_id = f"qa::{_stable_id(category, rule_type, source_column or target_field)}"
    return {
        "id": issue_id,
        "category": category,
        "rule_type": rule_type,
        "severity": sev,
        "title": title,
        "message": message,
        "source_column": source_column,
        "target_field": target_field,
        "affected_rows": affected,
        "affected_pct": pct,
        "sample_values": list(sample_values or [])[:8],
        "evidence": evidence or {},
        "suggested_fix": suggested_fix or {"action": "review", "description": "Review and correct source values."},
        "confidence": round(conf, 4),
        "confidence_band": confidence_band(conf),
        "approved": conf >= 0.72,
        "suggested_by": "deterministic_quality_audit",
        "schema_valid": True,
        "column_key": source_column or target_field or "",
        "action_group": str((suggested_fix or {}).get("action") or "review"),
        "client_impact": "low",
        "column_rank_score": 0.0,
    }


_STATE_ZIP_PREFIX_RANGES: Dict[str, Tuple[int, int]] = {
    "CT": (6, 6), "MA": (1, 2), "ME": (3, 4), "NH": (3, 3), "NJ": (7, 8), "NY": (10, 14), "PA": (15, 19), "RI": (2, 2), "VT": (5, 5),
    "DE": (19, 19), "DC": (20, 20), "MD": (20, 21), "VA": (22, 24), "WV": (24, 26), "NC": (27, 28), "SC": (29, 29),
    "AL": (35, 36), "FL": (32, 34), "GA": (30, 31), "MS": (38, 39), "TN": (37, 38),
    "IN": (46, 47), "KY": (40, 42), "MI": (48, 49), "OH": (43, 45),
    "IL": (60, 62), "IA": (50, 52), "KS": (66, 67), "MN": (55, 56), "MO": (63, 65), "NE": (68, 69), "ND": (58, 58), "SD": (57, 57), "WI": (53, 54),
    "AR": (71, 72), "LA": (70, 71), "OK": (73, 74), "TX": (75, 79),
    "AZ": (85, 86), "CO": (80, 81), "ID": (83, 83), "MT": (59, 59), "NV": (88, 89), "NM": (87, 88), "UT": (84, 84), "WY": (82, 83),
    "AK": (99, 99), "CA": (90, 96), "HI": (96, 96), "OR": (97, 97), "WA": (98, 99),
}


def _parse_iso_date(value: str) -> Optional[datetime]:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except Exception:
        return None


def _zip_prefix_pair(value: str) -> Optional[int]:
    if not _ZIP_RE.fullmatch(value):
        return None
    digits = re.sub(r"[^0-9]", "", value)
    if len(digits) < 2:
        return None
    try:
        return int(digits[:2])
    except Exception:
        return None


def _summary_profile_for_prompt(profile: Dict[str, Any], mappings: List[Dict[str, Any]]) -> Dict[str, Any]:
    sample_values_by_col = profile.get("sample_values_by_column") if isinstance(profile.get("sample_values_by_column"), dict) else {}
    summary_map: Dict[str, List[str]] = {}
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        source = _clean(mapping.get("source_column"))
        if not source:
            continue
        vals = sample_values_by_col.get(source) if isinstance(sample_values_by_col, dict) else None
        if isinstance(vals, list):
            summary_map[source] = [_clean(v) for v in vals if _clean(v)][:6]

    if not summary_map and isinstance(profile.get("sample_rows"), list):
        for row in profile.get("sample_rows")[:20]:
            if not isinstance(row, dict):
                continue
            for key, value in row.items():
                name = str(key)
                summary_map.setdefault(name, [])
                if len(summary_map[name]) < 6:
                    clean_value = _clean(value)
                    if clean_value:
                        summary_map[name].append(clean_value)

    return {
        "rows_profiled": int(profile.get("rows_profiled", profile.get("row_sample_size", 0)) or 0),
        "rows_total": int(profile.get("rows_total", 0) or 0),
        "columns": list(profile.get("columns") or []),
        "sample_values": summary_map,
        "semantic_profile": profile.get("semantic_profile") if isinstance(profile.get("semantic_profile"), dict) else {},
    }


def _normalize_issue_candidate(candidate: Dict[str, Any], rows_profiled: int, suggested_by: str) -> Optional[Dict[str, Any]]:
    if not isinstance(candidate, dict):
        return None
    category = _clean(candidate.get("category") or "quality")
    rule_type = _clean(candidate.get("rule_type") or "custom_quality_rule")
    source_column = _clean(candidate.get("source_column"))
    target_field = _clean(candidate.get("target_field"))
    title = _clean(candidate.get("title") or f"Quality check: {rule_type}")
    message = _clean(candidate.get("message") or "Review potential data quality issue.")

    if not title or not message:
        return None

    try:
        confidence = float(candidate.get("confidence", 0.0) or 0.0)
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    try:
        affected_rows = int(candidate.get("affected_rows", 0) or 0)
    except Exception:
        affected_rows = 0
    affected_rows = max(0, affected_rows)

    sample_values = candidate.get("sample_values") if isinstance(candidate.get("sample_values"), list) else []
    evidence = candidate.get("evidence") if isinstance(candidate.get("evidence"), dict) else {}
    suggested_fix = candidate.get("suggested_fix") if isinstance(candidate.get("suggested_fix"), dict) else {}
    severity = _clean(candidate.get("severity") or _field_severity(confidence)).lower()
    if severity not in {"error", "warning", "info"}:
        severity = _field_severity(confidence)

    issue = _make_issue(
        category=category,
        rule_type=rule_type,
        source_column=source_column,
        target_field=target_field,
        title=title,
        message=message,
        confidence=confidence,
        affected_rows=affected_rows,
        rows_profiled=max(rows_profiled, affected_rows),
        sample_values=[_clean(v) for v in sample_values if _clean(v)][:8],
        evidence=evidence,
        suggested_fix=suggested_fix or None,
        severity=severity,
    )
    if _clean(candidate.get("id")):
        issue["id"] = _clean(candidate.get("id"))
    issue["suggested_by"] = suggested_by
    fix = issue.get("suggested_fix") if isinstance(issue.get("suggested_fix"), dict) else {}
    issue["column_key"] = source_column or target_field or ""
    issue["action_group"] = str(fix.get("action") or "review")
    issue["client_impact"] = "low"
    issue["column_rank_score"] = 0.0
    return issue


def _kb_feedback_for_issue(
    issue: Dict[str, Any],
    *,
    learning_kb: Optional[LearningKB],
    roster_type: str,
    learning_scope: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if learning_kb is None:
        return {"approved": 0, "rejected": 0, "added": 0, "adjustment": 0.0}

    getter = getattr(learning_kb, "get_quality_audit_feedback", None)
    if getter is None:
        return {"approved": 0, "rejected": 0, "added": 0, "adjustment": 0.0}

    try:
        stats = getter(
            roster_type=roster_type,
            rule_type=str(issue.get("rule_type", "") or "quality_audit"),
            source_column=str(issue.get("source_column", "") or ""),
            target_field=str(issue.get("target_field", "") or ""),
            scope=learning_scope,
        )
    except TypeError:
        stats = getter(
            roster_type=roster_type,
            rule_type=str(issue.get("rule_type", "") or "quality_audit"),
            source_column=str(issue.get("source_column", "") or ""),
            target_field=str(issue.get("target_field", "") or ""),
        )
    except Exception:
        return {"approved": 0, "rejected": 0, "added": 0, "adjustment": 0.0}

    if not isinstance(stats, dict):
        return {"approved": 0, "rejected": 0, "added": 0, "adjustment": 0.0}

    approved = int(stats.get("approved", 0) or 0)
    rejected = int(stats.get("rejected", 0) or 0)
    added = int(stats.get("added", 0) or 0)
    adjustment = min(0.18, 0.02 * (approved + added)) - min(0.24, 0.03 * rejected)
    return {"approved": approved, "rejected": rejected, "added": added, "adjustment": round(adjustment, 4)}


def _apply_learning_adjustments(
    issues: List[Dict[str, Any]],
    *,
    learning_kb: Optional[LearningKB],
    roster_type: str,
    learning_scope: Optional[Dict[str, Any]],
) -> None:
    for issue in issues:
        if not isinstance(issue, dict):
            continue
        stats = _kb_feedback_for_issue(
            issue,
            learning_kb=learning_kb,
            roster_type=roster_type,
            learning_scope=learning_scope,
        )
        base_conf = float(issue.get("confidence", 0.0) or 0.0)
        conf = max(0.0, min(1.0, base_conf + float(stats.get("adjustment", 0.0) or 0.0)))
        issue["confidence"] = round(conf, 4)
        issue["confidence_band"] = confidence_band(conf)
        issue["approved"] = bool(conf >= 0.72)
        evidence = issue.setdefault("evidence", {})
        if isinstance(evidence, dict):
            evidence["kb_prior"] = stats


def _apply_retrieval_adjustments(
    issues: List[Dict[str, Any]],
    *,
    learning_retrieval: Optional[LearningRetrieval],
    roster_type: str,
    learning_scope: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if learning_retrieval is None or not learning_retrieval.is_enabled() or not issues:
        return {
            "enabled": bool(learning_retrieval and learning_retrieval.is_enabled()),
            "hits": 0,
            "scores": [],
            "sample_item_ids": [],
            "confidence_boost": 0.0,
        }

    hit_count = 0
    scores: List[float] = []
    sample_item_ids: List[str] = []

    for issue in issues:
        if not isinstance(issue, dict):
            continue
        retrieval = learning_retrieval.retrieve(
            section="quality_audit",
            item_key={
                "section": "quality_audit",
                "item_id": str(issue.get("id") or ""),
                "source_column": str(issue.get("source_column") or ""),
                "target_field": str(issue.get("target_field") or ""),
                "rule_type": str(issue.get("rule_type") or ""),
                "severity": str(issue.get("severity") or ""),
                "category": str(issue.get("category") or ""),
            },
            roster_type=roster_type,
            workspace_scope=learning_scope,
        )
        hits = retrieval.get("hits") if isinstance(retrieval, dict) else []
        if not isinstance(hits, list) or not hits:
            continue

        hit_count += len(hits)
        local_scores = [float(hit.get("score", 0.0) or 0.0) for hit in hits if isinstance(hit, dict)]
        scores.extend(local_scores)
        for hit in hits[:2]:
            if not isinstance(hit, dict):
                continue
            item_id = _clean(hit.get("item_id"))
            if item_id and item_id not in sample_item_ids and len(sample_item_ids) < 8:
                sample_item_ids.append(item_id)

        avg_local = sum(local_scores) / max(1.0, float(len(local_scores))) if local_scores else 0.0
        boost = min(0.12, max(0.0, avg_local * 0.1))
        base_conf = float(issue.get("confidence", 0.0) or 0.0)
        conf = max(0.0, min(1.0, base_conf + boost))
        issue["confidence"] = round(conf, 4)
        issue["confidence_band"] = confidence_band(conf)
        issue["approved"] = bool(conf >= 0.72)
        evidence = issue.setdefault("evidence", {})
        if isinstance(evidence, dict):
            evidence["retrieval"] = {
                "hits": len(hits),
                "top_score": round(max(local_scores) if local_scores else 0.0, 4),
                "avg_score": round(avg_local, 4),
            }

    avg_score = (sum(scores) / float(len(scores))) if scores else 0.0
    confidence_boost = min(0.12, max(0.0, avg_score * 0.1))
    return {
        "enabled": True,
        "hits": hit_count,
        "scores": [round(score, 4) for score in scores],
        "sample_item_ids": sample_item_ids,
        "avg_score": round(avg_score, 4),
        "confidence_boost": round(confidence_boost, 4),
    }


def _deterministic_quality_audit(
    *,
    profile: Dict[str, Any],
    mappings: List[Dict[str, Any]],
    instructions_context: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    rows_profiled = int(profile.get("rows_profiled", profile.get("row_sample_size", 0)) or 0)
    sample_values_by_col = profile.get("sample_values_by_column") if isinstance(profile.get("sample_values_by_column"), dict) else {}
    if not sample_values_by_col and isinstance(profile.get("sample_rows"), list):
        derived: Dict[str, List[str]] = {}
        for row in profile.get("sample_rows") or []:
            if not isinstance(row, dict):
                continue
            for k, v in row.items():
                derived.setdefault(str(k), []).append(_clean(v))
        sample_values_by_col = derived

    issues: List[Dict[str, Any]] = []
    seen = set()

    source_to_target: Dict[str, str] = {}
    for mapping in mappings or []:
        if not isinstance(mapping, dict):
            continue
        source = _clean(mapping.get("source_column"))
        target = _clean(mapping.get("target_field"))
        if source and target:
            source_to_target[source] = target

    sample_rows: List[Dict[str, Any]] = []
    if isinstance(profile.get("sample_rows"), list):
        sample_rows = [row for row in (profile.get("sample_rows") or []) if isinstance(row, dict)]

    row_source_columns = [
        source
        for source in source_to_target.keys()
        if source in {str(k) for row in sample_rows for k in row.keys()}
    ]

    state_columns = [c for c, t in source_to_target.items() if "state" in t.lower() or "state" in c.lower()]
    zip_columns = [c for c, t in source_to_target.items() if "zip" in t.lower() or "postal" in t.lower() or "zip" in c.lower() or "postal" in c.lower()]

    for state_col in state_columns:
        for zip_col in zip_columns:
            if state_col == zip_col:
                continue
            mismatches = 0
            mismatch_samples: List[str] = []
            pair_rows = 0
            for row in sample_rows:
                state_val = _clean(row.get(state_col)).upper()
                zip_val = _clean(row.get(zip_col))
                if not state_val or not zip_val:
                    continue
                if not _STATE_RE.fullmatch(state_val):
                    continue
                prefix = _zip_prefix_pair(zip_val)
                if prefix is None:
                    continue
                range_pair = _STATE_ZIP_PREFIX_RANGES.get(state_val)
                if range_pair is None:
                    continue
                pair_rows += 1
                lo, hi = range_pair
                if prefix < lo or prefix > hi:
                    mismatches += 1
                    if len(mismatch_samples) < 6:
                        mismatch_samples.append(f"{state_val}/{zip_val}")
            if pair_rows >= 3 and mismatches > 0:
                issue = _make_issue(
                    category="consistency",
                    rule_type="state_zip_mismatch",
                    source_column=state_col,
                    target_field=source_to_target.get(state_col) or source_to_target.get(zip_col) or "",
                    title=f"State/ZIP inconsistency between {state_col} and {zip_col}",
                    message=f"Detected {mismatches} sampled rows where state and ZIP prefix appear inconsistent.",
                    confidence=0.84,
                    affected_rows=mismatches,
                    rows_profiled=max(pair_rows, rows_profiled),
                    sample_values=mismatch_samples,
                    evidence={"state_column": state_col, "zip_column": zip_col, "pairs_evaluated": pair_rows},
                    suggested_fix={
                        "action": "review",
                        "description": "Validate state and ZIP alignment, then correct mismatched addresses.",
                        "params": {"state_column": state_col, "zip_column": zip_col},
                    },
                    severity="warning",
                )
                if issue["id"] not in seen:
                    issues.append(issue)
                    seen.add(issue["id"])

    date_columns = [c for c, t in source_to_target.items() if any(token in (t.lower() + " " + c.lower()) for token in ["date", "dob", "birth", "effective", "start", "end"])]
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    for date_col in date_columns:
        bad_count = 0
        bad_samples: List[str] = []
        seen_dates = [_clean(row.get(date_col)) for row in sample_rows if _clean(row.get(date_col))]
        for raw in seen_dates:
            parsed = _parse_iso_date(raw)
            if parsed is None:
                continue
            too_old = parsed.year < 1900
            too_future = parsed > now_utc
            if too_old or too_future:
                bad_count += 1
                if len(bad_samples) < 6:
                    bad_samples.append(raw)
        if bad_count > 0:
            issue = _make_issue(
                category="consistency",
                rule_type="date_sanity",
                source_column=date_col,
                target_field=source_to_target.get(date_col) or "",
                title=f"Date sanity issue in {date_col}",
                message=f"Detected {bad_count} sampled date values that are implausible (before 1900 or in the future).",
                confidence=0.82,
                affected_rows=bad_count,
                rows_profiled=max(len(seen_dates), rows_profiled),
                sample_values=bad_samples,
                evidence={"column": date_col, "rule": "year>=1900 && date<=today"},
                suggested_fix={
                    "action": "transform",
                    "description": "Normalize and bound-check date values.",
                    "transform": "normalize_date",
                    "params": {"min_year": 1900, "max_date": now_utc.strftime("%Y-%m-%d")},
                },
                severity="warning",
            )
            if issue["id"] not in seen:
                issues.append(issue)
                seen.add(issue["id"])

    if sample_rows and row_source_columns:
        signature_counts: Counter[Tuple[str, ...]] = Counter()
        for row in sample_rows:
            signature = tuple(_clean(row.get(col)).lower() for col in row_source_columns)
            if any(part for part in signature):
                signature_counts[signature] += 1
        duplicate_occurrences = sum(count - 1 for count in signature_counts.values() if count > 1)
        if duplicate_occurrences > 0:
            top_signature = next((sig for sig, count in signature_counts.items() if count > 1), tuple())
            signature_preview = " | ".join([part for part in top_signature if part][:4])
            issue = _make_issue(
                category="uniqueness",
                rule_type="duplicate_row_signature",
                source_column=",".join(row_source_columns[:4]),
                target_field="multiple",
                title="Potential duplicate rows by mapped signature",
                message=f"Detected {duplicate_occurrences} duplicate sampled row occurrences using mapped-column signatures.",
                confidence=0.79,
                affected_rows=duplicate_occurrences,
                rows_profiled=max(len(sample_rows), rows_profiled),
                sample_values=[signature_preview] if signature_preview else [],
                evidence={"signature_columns": row_source_columns, "duplicate_occurrences": duplicate_occurrences},
                suggested_fix={
                    "action": "review",
                    "description": "Review row-level duplicates and deduplicate upstream before transformation.",
                    "params": {"signature_columns": row_source_columns},
                },
                severity="warning",
            )
            if issue["id"] not in seen:
                issues.append(issue)
                seen.add(issue["id"])

    for mapping in mappings or []:
        if not isinstance(mapping, dict):
            continue
        source = _clean(mapping.get("source_column"))
        target = _clean(mapping.get("target_field"))
        if not source:
            continue

        values = [_clean(v) for v in (sample_values_by_col.get(source) or []) if _clean(v)]
        if not values:
            continue

        non_empty = [v for v in values if v]
        null_like = len(values) - len(non_empty)
        null_ratio = (null_like / len(values)) if values else 0.0

        if null_ratio >= 0.35:
            issue = _make_issue(
                category="completeness",
                rule_type="high_null_ratio",
                source_column=source,
                target_field=target,
                title=f"High null ratio in {source}",
                message=f"{source} has {round(null_ratio * 100, 1)}% null/blank values in profiled rows.",
                confidence=0.85,
                affected_rows=null_like,
                rows_profiled=len(values),
                sample_values=values[:6],
                evidence={"null_ratio": round(null_ratio, 4)},
                suggested_fix={
                    "action": "transform",
                    "description": "Add required/default handling for missing values.",
                    "transform": "fill_missing",
                    "params": {"strategy": "default_or_drop"},
                },
                severity="error",
            )
            if issue["id"] not in seen:
                issues.append(issue)
                seen.add(issue["id"])

        source_l = source.lower()
        target_l = target.lower()
        key = f"{source_l} {target_l}"

        def _format_mismatch(pattern: re.Pattern[str]) -> int:
            return sum(1 for v in non_empty if not pattern.fullmatch(v))

        if "email" in key:
            bad = _format_mismatch(_EMAIL_RE)
            if bad:
                issue = _make_issue(
                    category="format",
                    rule_type="email_format",
                    source_column=source,
                    target_field=target,
                    title=f"Invalid email format in {source}",
                    message=f"Detected {bad} malformed email values.",
                    confidence=0.9,
                    affected_rows=bad,
                    rows_profiled=len(values),
                    sample_values=[v for v in non_empty if not _EMAIL_RE.fullmatch(v)][:6],
                    evidence={"pattern": "email"},
                    suggested_fix={"action": "transform", "description": "Normalize/validate email format.", "transform": "normalize_email"},
                    severity="error",
                )
                if issue["id"] not in seen:
                    issues.append(issue)
                    seen.add(issue["id"])

        if "phone" in key or "tel" in key:
            bad = _format_mismatch(_PHONE_RE)
            if bad:
                issue = _make_issue(
                    category="format",
                    rule_type="phone_format",
                    source_column=source,
                    target_field=target,
                    title=f"Invalid phone format in {source}",
                    message=f"Detected {bad} malformed phone values.",
                    confidence=0.78,
                    affected_rows=bad,
                    rows_profiled=len(values),
                    sample_values=[v for v in non_empty if not _PHONE_RE.fullmatch(v)][:6],
                    evidence={"pattern": "phone"},
                    suggested_fix={"action": "transform", "description": "Normalize phone numbers.", "transform": "normalize_phone"},
                )
                if issue["id"] not in seen:
                    issues.append(issue)
                    seen.add(issue["id"])

        if "state" in key:
            bad = _format_mismatch(_STATE_RE)
            if bad:
                issue = _make_issue(
                    category="format",
                    rule_type="state_format",
                    source_column=source,
                    target_field=target,
                    title=f"Invalid state values in {source}",
                    message=f"Detected {bad} state values not matching 2-letter code.",
                    confidence=0.74,
                    affected_rows=bad,
                    rows_profiled=len(values),
                    sample_values=[v for v in non_empty if not _STATE_RE.fullmatch(v)][:6],
                    evidence={"pattern": "state_code"},
                    suggested_fix={"action": "transform", "description": "Uppercase and map states.", "transform": "normalize_state"},
                )
                if issue["id"] not in seen:
                    issues.append(issue)
                    seen.add(issue["id"])

        if "zip" in key or "postal" in key:
            bad = _format_mismatch(_ZIP_RE)
            if bad:
                issue = _make_issue(
                    category="format",
                    rule_type="zip_format",
                    source_column=source,
                    target_field=target,
                    title=f"Invalid ZIP/postal format in {source}",
                    message=f"Detected {bad} ZIP/postal values with invalid format.",
                    confidence=0.76,
                    affected_rows=bad,
                    rows_profiled=len(values),
                    sample_values=[v for v in non_empty if not _ZIP_RE.fullmatch(v)][:6],
                    evidence={"pattern": "zip"},
                    suggested_fix={"action": "transform", "description": "Normalize ZIP/postal values.", "transform": "normalize_zip"},
                )
                if issue["id"] not in seen:
                    issues.append(issue)
                    seen.add(issue["id"])

        if "npi" in key:
            bad = _format_mismatch(_NPI_RE)
            if bad:
                issue = _make_issue(
                    category="format",
                    rule_type="npi_format",
                    source_column=source,
                    target_field=target,
                    title=f"Invalid NPI format in {source}",
                    message=f"Detected {bad} NPI values not matching 10-digit structure.",
                    confidence=0.92,
                    affected_rows=bad,
                    rows_profiled=len(values),
                    sample_values=[v for v in non_empty if not _NPI_RE.fullmatch(v)][:6],
                    evidence={"pattern": "npi_10_digit"},
                    suggested_fix={"action": "transform", "description": "Strip non-digits and validate NPI.", "transform": "normalize_npi"},
                    severity="error",
                )
                if issue["id"] not in seen:
                    issues.append(issue)
                    seen.add(issue["id"])

        if "date" in key or "dob" in key:
            bad = _format_mismatch(_DATE_RE)
            if bad:
                issue = _make_issue(
                    category="format",
                    rule_type="date_format",
                    source_column=source,
                    target_field=target,
                    title=f"Invalid date format in {source}",
                    message=f"Detected {bad} date values not matching YYYY-MM-DD.",
                    confidence=0.8,
                    affected_rows=bad,
                    rows_profiled=len(values),
                    sample_values=[v for v in non_empty if not _DATE_RE.fullmatch(v)][:6],
                    evidence={"pattern": "yyyy-mm-dd"},
                    suggested_fix={"action": "transform", "description": "Normalize date formats.", "transform": "normalize_date"},
                )
                if issue["id"] not in seen:
                    issues.append(issue)
                    seen.add(issue["id"])

        if any(token in key for token in ["npi", "id", "identifier"]):
            dup = len(non_empty) - len(set(non_empty))
            if dup > 0:
                issue = _make_issue(
                    category="uniqueness",
                    rule_type="duplicate_values",
                    source_column=source,
                    target_field=target,
                    title=f"Duplicate values in {source}",
                    message=f"Detected {dup} duplicate value occurrences in profiled rows.",
                    confidence=0.86,
                    affected_rows=dup,
                    rows_profiled=len(values),
                    sample_values=non_empty[:6],
                    evidence={"duplicates": dup},
                    suggested_fix={"action": "review", "description": "Review duplicate identifiers and deduplicate upstream."},
                    severity="error",
                )
                if issue["id"] not in seen:
                    issues.append(issue)
                    seen.add(issue["id"])

        counts = Counter(non_empty)
        if counts:
            top_value, top_count = counts.most_common(1)[0]
            dom_ratio = top_count / max(1, len(non_empty))
            if dom_ratio >= 0.9 and len(non_empty) >= 10:
                issue = _make_issue(
                    category="distribution",
                    rule_type="one_value_dominance",
                    source_column=source,
                    target_field=target,
                    title=f"Suspicious value dominance in {source}",
                    message=f"Top value '{top_value}' represents {round(dom_ratio*100, 1)}% of sampled non-empty rows.",
                    confidence=0.62,
                    affected_rows=top_count,
                    rows_profiled=len(values),
                    sample_values=[top_value],
                    evidence={"top_value": top_value, "top_ratio": round(dom_ratio, 4)},
                    suggested_fix={"action": "review", "description": "Verify this concentration is expected for the client/tenant."},
                )
                if issue["id"] not in seen:
                    issues.append(issue)
                    seen.add(issue["id"])

    notes = []
    if isinstance(instructions_context, dict):
        notes.extend([_clean(v) for v in (instructions_context.get("free_text_notes") or [])])
        notes.extend([_clean(v) for v in (instructions_context.get("client_rules") or [])])
    for note in [n for n in notes if n][:6]:
        if "must" in note.lower() or "required" in note.lower() or "always" in note.lower():
            issue = _make_issue(
                category="business_rule",
                rule_type="instruction_guardrail",
                source_column="",
                target_field="",
                title="Instruction-derived guardrail",
                message=note,
                confidence=0.58,
                affected_rows=0,
                rows_profiled=rows_profiled,
                sample_values=[],
                evidence={"instruction_note": note},
                suggested_fix={"action": "review", "description": "Ensure generated plan enforces this business instruction."},
                severity="warning",
            )
            if issue["id"] not in seen:
                issues.append(issue)
                seen.add(issue["id"])

    # ── Sales-critical deterministic checks ──────────────────────────────
    # These run on the full sample_rows and catch real data quality issues
    # that impress clients during demos.

    rows_total = int(profile.get("rows_total", 0) or 0)
    total_for_display = rows_total or rows_profiled

    # (A) Empty/blank rows — rows where all critical fields are empty
    critical_source_cols = [
        src for src, tgt in source_to_target.items()
        if any(k in tgt.lower() for k in ("npi", "lastname", "firstname", "name", "tin"))
    ]
    if sample_rows and critical_source_cols:
        blank_count = 0
        for row in sample_rows:
            if all(not _clean(row.get(c)) for c in critical_source_cols):
                blank_count += 1
        if blank_count > 0:
            scaled = int(blank_count * (total_for_display / max(len(sample_rows), 1))) if total_for_display > len(sample_rows) else blank_count
            issue = _make_issue(
                category="completeness",
                rule_type="blank_rows",
                source_column="",
                target_field="",
                title="Empty Rows Detected",
                message=f"{scaled} rows have no data in critical fields (NPI, Name, TIN). These appear to be blank or padding rows that should be removed before ingestion.",
                confidence=0.95,
                affected_rows=scaled,
                rows_profiled=total_for_display,
                severity="warning",
                suggested_fix={"action": "transform", "description": "Remove rows where all critical identifier fields are empty."},
            )
            if issue["id"] not in seen:
                issues.append(issue)
                seen.add(issue["id"])

    npi_cols = [src for src, tgt in source_to_target.items() if "npi" in tgt.lower() and "group" not in tgt.lower()]

    # (C) Expired credentials — board certs, licenses, DEA past due
    import datetime as _dt
    today = _dt.date.today()
    date_cols = [
        (src, tgt) for src, tgt in source_to_target.items()
        if any(k in tgt.lower() for k in ("expir", "expiration"))
    ]
    for date_src, date_tgt in date_cols:
        expired_count = 0
        expired_samples: List[str] = []
        for row in sample_rows:
            val = _clean(row.get(date_src))
            if not val:
                continue
            parsed_date = None
            for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                try:
                    parsed_date = _dt.datetime.strptime(val, fmt).date()
                    break
                except ValueError:
                    continue
            if parsed_date and parsed_date < today:
                expired_count += 1
                if len(expired_samples) < 3:
                    expired_samples.append(val)
        if expired_count > 2:
            friendly = date_tgt.replace("ExpirationDate", "").replace("Expiration", "").replace("Expir", "")
            friendly = re.sub(r'([a-z])([A-Z])', r'\1 \2', friendly).strip()
            scaled = int(expired_count * (total_for_display / max(len(sample_rows), 1))) if total_for_display > len(sample_rows) else expired_count
            issue = _make_issue(
                category="business_rule",
                rule_type="expired_credential",
                source_column=date_src,
                target_field=date_tgt,
                title=f"Expired {friendly}",
                message=f"{scaled} providers have expired {friendly.lower()} dates (before {today.strftime('%m/%d/%Y')}). These may need to be renewed before credentialing can proceed.",
                confidence=0.95,
                affected_rows=scaled,
                rows_profiled=total_for_display,
                sample_values=expired_samples,
                severity="warning",
                suggested_fix={"action": "review", "description": f"Verify {friendly.lower()} dates and request updated documentation for expired records."},
            )
            if issue["id"] not in seen:
                issues.append(issue)
                seen.add(issue["id"])

    # (D) Invalid DEA format
    dea_cols = [src for src, tgt in source_to_target.items() if "dea" in tgt.lower() and "expir" not in tgt.lower() and "date" not in tgt.lower()]
    dea_re = re.compile(r'^[A-Za-z]{1,2}\d{7,8}$')
    for dea_col in dea_cols:
        bad_count = 0
        bad_samples: List[str] = []
        for row in sample_rows:
            val = _clean(row.get(dea_col))
            if not val:
                continue
            if val.upper() in ("N/A", "NA", "NONE", "PENDING", "TBD", "EXEMPT"):
                continue
            if not dea_re.match(val):
                bad_count += 1
                if len(bad_samples) < 3:
                    bad_samples.append(val)
        if bad_count > 0:
            scaled = int(bad_count * (total_for_display / max(len(sample_rows), 1))) if total_for_display > len(sample_rows) else bad_count
            issue = _make_issue(
                category="format",
                rule_type="invalid_dea_format",
                source_column=dea_col,
                target_field=source_to_target.get(dea_col, ""),
                title="Invalid DEA Numbers",
                message=f"{scaled} DEA numbers don't match expected format (1-2 letters + 7 digits). These will fail DEA verification.",
                confidence=0.90,
                affected_rows=scaled,
                rows_profiled=total_for_display,
                sample_values=bad_samples,
                severity="error",
                suggested_fix={"action": "review", "description": "Request corrected DEA numbers from the provider group."},
            )
            if issue["id"] not in seen:
                issues.append(issue)
                seen.add(issue["id"])

    # (E) Missing NPI (critical field)
    for npi_col in npi_cols:
        missing_count = sum(1 for row in sample_rows if not _clean(row.get(npi_col)))
        if missing_count > 0:
            scaled = int(missing_count * (total_for_display / max(len(sample_rows), 1))) if total_for_display > len(sample_rows) else missing_count
            issue = _make_issue(
                category="completeness",
                rule_type="missing_npi",
                source_column=npi_col,
                target_field=source_to_target.get(npi_col, ""),
                title="Missing NPI Numbers",
                message=f"{scaled} rows are missing Individual NPI. NPI is required for provider identification and NPPES validation.",
                confidence=0.98,
                affected_rows=scaled,
                rows_profiled=total_for_display,
                severity="error",
                suggested_fix={"action": "review", "description": "Obtain NPI numbers from the provider group or verify if these rows should be removed."},
            )
            if issue["id"] not in seen:
                issues.append(issue)
                seen.add(issue["id"])

    return issues


def _llm_primary_quality_audit(
    *,
    profile: Dict[str, Any],
    mappings: List[Dict[str, Any]],
    deterministic_issues: List[Dict[str, Any]],
    instructions_context: Optional[Dict[str, Any]],
    primary_router: LLMRouter,
    verifier_router: LLMRouter,
    settings: Settings,
    collaboration_mode: str,
) -> Dict[str, Any]:
    rows_profiled = int(profile.get("rows_profiled", profile.get("row_sample_size", 0)) or 0)
    mode = str(collaboration_mode or settings.collaboration_mode or "advisory").strip().lower() or "advisory"
    strict_mode = mode in {"strict_fail_open", "strict_fail_closed"}
    fail_closed = mode == "strict_fail_closed"
    require_claude_verifier = bool(strict_mode or settings.require_claude_verifier_for_section("quality_audit"))

    llm_trace: Dict[str, Any] = {
        "used": False,
        "engine": "quality_audit_llm",
        "provider_order": {
            "analysis": primary_router.provider_names(),
            "verifier": verifier_router.provider_names(),
        },
        "primary": {
            "enabled": True,
            "status": "skipped",
            "provider": None,
            "model": None,
            "attempts": [],
            "task_type": "quality_audit",
        },
        "verifier": {
            "enabled": True,
            "status": "skipped",
            "provider": None,
            "model": None,
            "attempts": [],
            "task_type": "verifier",
        },
        "merge": {"kept": 0, "refined": 0, "rejected": 0, "added": 0},
        "policy": {
            "mode": mode,
            "requires_claude_verifier": require_claude_verifier,
            "status": "not_required" if not require_claude_verifier else "pending",
            "fail_closed": bool(fail_closed),
            "failure_reason": None,
            "affected_ids": [],
        },
        "fallback_reason": None,
    }

    base_candidates = [
        {
            "id": str(issue.get("id") or ""),
            "category": str(issue.get("category") or "quality"),
            "rule_type": str(issue.get("rule_type") or "custom_quality_rule"),
            "severity": str(issue.get("severity") or "info"),
            "title": str(issue.get("title") or ""),
            "message": str(issue.get("message") or ""),
            "source_column": str(issue.get("source_column") or ""),
            "target_field": str(issue.get("target_field") or ""),
            "affected_rows": int(issue.get("affected_rows", 0) or 0),
            "affected_pct": float(issue.get("affected_pct", 0.0) or 0.0),
            "sample_values": issue.get("sample_values") if isinstance(issue.get("sample_values"), list) else [],
            "evidence": issue.get("evidence") if isinstance(issue.get("evidence"), dict) else {},
            "suggested_fix": issue.get("suggested_fix") if isinstance(issue.get("suggested_fix"), dict) else {},
            "confidence": float(issue.get("confidence", 0.0) or 0.0),
        }
        for issue in deterministic_issues
        if isinstance(issue, dict)
    ]

    prompt = f"""
You are a senior healthcare roster quality auditor.
Return JSON only with key 'issues'.

Task:
- Refine and expand quality-audit findings from deterministic candidates.
- Keep IDs stable for existing findings when possible.
- Prioritize actionable findings with strong evidence.

Constraints:
- Output only valid JSON object.
- Each issue must include: id, category, rule_type, severity, title, message, source_column, target_field, affected_rows, confidence.
- severity must be one of: error, warning, info.
- confidence must be between 0.0 and 1.0.
- suggested_fix should include action and description.

Profile summary:
{json.dumps(_summary_profile_for_prompt(profile, mappings), ensure_ascii=False, indent=2)}

Mappings:
{json.dumps(mappings, ensure_ascii=False, indent=2)}

Deterministic candidates:
{json.dumps(base_candidates, ensure_ascii=False, indent=2)}

Instruction context:
{json.dumps(instructions_context or {}, ensure_ascii=False, indent=2)}

Output:
{{
  "issues": [
    {{
      "id": "qa::...",
      "category": "format|consistency|completeness|uniqueness|distribution|business_rule|external_reference|quality",
      "rule_type": "...",
      "severity": "error|warning|info",
      "title": "...",
      "message": "...",
      "source_column": "...",
      "target_field": "...",
      "affected_rows": 0,
      "sample_values": ["..."],
      "evidence": {{}},
      "suggested_fix": {{"action": "review|transform|validate", "description": "...", "params": {{}}}},
      "confidence": 0.0
    }}
  ]
}}
""".strip()

    try:
        primary_routed = primary_router.generate(prompt=prompt, task_type="quality_audit")
        llm_trace["primary"].update(
            {
                "status": "ok",
                "provider": primary_routed.response.provider,
                "model": primary_routed.response.model,
                "attempts": primary_routed.attempts,
                "task_type": "quality_audit",
            }
        )
        primary_parsed = extract_json_object(primary_routed.response.text)
        primary_issues_raw = primary_parsed.get("issues") if isinstance(primary_parsed.get("issues"), list) else []
    except Exception as exc:
        llm_trace["primary"].update(
            {
                "status": "error",
                "provider": None,
                "model": None,
                "attempts": [f"error:{exc}"],
                "task_type": "quality_audit",
            }
        )
        llm_trace["fallback_reason"] = "llm_primary_failed"
        return {"quality_audit": list(deterministic_issues), "llm_trace": llm_trace}

    by_id: Dict[str, Dict[str, Any]] = {
        str(item.get("id") or ""): dict(item)
        for item in deterministic_issues
        if isinstance(item, dict) and str(item.get("id") or "")
    }

    normalized_primary: List[Dict[str, Any]] = []
    for raw in primary_issues_raw:
        normalized = _normalize_issue_candidate(raw, rows_profiled, llm_trace["primary"].get("provider") or "quality_audit_llm")
        if normalized is None:
            continue
        normalized_primary.append(normalized)

    if not normalized_primary and deterministic_issues:
        llm_trace["fallback_reason"] = "llm_primary_parse_failed"
        return {"quality_audit": list(deterministic_issues), "llm_trace": llm_trace}

    for issue in normalized_primary:
        by_id[str(issue.get("id") or "")] = issue

    verifier_prompt = f"""
You are the verifier for quality audit findings.
Return JSON only with key 'decisions'.

Candidates:
{json.dumps(list(by_id.values()), ensure_ascii=False, indent=2)}

Decision rules:
- action=keep: keep issue as-is.
- action=refine: replace with provided issue payload.
- action=reject: remove issue.
- action=add: add a new issue payload.
- Keep schema compatible with existing quality audit UI fields.

Output:
{{
  "decisions": [
    {{
      "action": "keep|refine|reject|add",
      "id": "qa::...",
      "issue": {{ ...full issue payload... }}
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
        decisions = verifier_parsed.get("decisions") if isinstance(verifier_parsed.get("decisions"), list) else []
    except Exception as exc:
        llm_trace["verifier"].update(
            {
                "status": "error",
                "provider": None,
                "model": None,
                "attempts": [f"error:{exc}"],
                "task_type": "verifier",
            }
        )
        decisions = []

    merge = {"kept": 0, "refined": 0, "rejected": 0, "added": 0}
    explicit_ids = set()

    for decision in decisions:
        if not isinstance(decision, dict):
            continue
        action = _clean(decision.get("action")).lower()
        issue_id = _clean(decision.get("id"))
        if action not in {"keep", "refine", "reject", "add"} or not issue_id:
            continue

        if action == "keep":
            if issue_id in by_id:
                explicit_ids.add(issue_id)
            continue

        if action == "reject":
            if issue_id in by_id:
                by_id.pop(issue_id, None)
                merge["rejected"] += 1
                explicit_ids.add(issue_id)
            continue

        payload = decision.get("issue") if isinstance(decision.get("issue"), dict) else None
        if payload is None:
            continue

        payload = dict(payload)
        payload["id"] = issue_id
        normalized = _normalize_issue_candidate(
            payload,
            rows_profiled,
            llm_trace["verifier"].get("provider") or "quality_audit_verifier",
        )
        if normalized is None:
            continue

        if action == "refine":
            if issue_id not in by_id:
                continue
            by_id[issue_id] = normalized
            merge["refined"] += 1
            explicit_ids.add(issue_id)
            continue

        if action == "add":
            if issue_id in by_id:
                continue
            by_id[issue_id] = normalized
            merge["added"] += 1
            explicit_ids.add(issue_id)

    for issue_id in list(by_id.keys()):
        if issue_id not in explicit_ids:
            merge["kept"] += 1

    merged_items = list(by_id.values())

    policy = dict(llm_trace.get("policy") or {})
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
        elif not settings.is_claude_provider(verifier_provider):
            policy["status"] = "failed"
            policy["failure_reason"] = "verifier_not_claude"
        else:
            policy["status"] = "satisfied"
            policy["failure_reason"] = None

        if policy.get("status") == "failed":
            policy["affected_ids"] = [str(item.get("id") or "") for item in merged_items if str(item.get("id") or "")]
            if fail_closed:
                raise RuntimeError("Strict collaboration policy requires Claude verifier for quality audit")

    llm_trace["policy"] = policy
    llm_trace["merge"] = merge

    if policy.get("affected_ids"):
        affected_ids = {str(item_id).strip() for item_id in (policy.get("affected_ids") or []) if str(item_id).strip()}
        failure_reason = str(policy.get("failure_reason") or "collaboration verifier unavailable")
        for item in merged_items:
            item_id = str(item.get("id") or "")
            if item_id not in affected_ids:
                continue
            item["approved"] = False
            message_prefix = str(item.get("message") or "").strip()
            item["message"] = f"{message_prefix} (unverified: {failure_reason})" if message_prefix else f"unverified: {failure_reason}"
            evidence = item.setdefault("evidence", {})
            if isinstance(evidence, dict):
                evidence["collaboration_policy"] = {
                    "mode": mode,
                    "requires_claude_verifier": bool(policy.get("requires_claude_verifier")),
                    "status": policy.get("status"),
                    "failure_reason": policy.get("failure_reason"),
                }

    llm_trace["used"] = True
    llm_trace["rule_count"] = len(merged_items)
    llm_trace["categories"] = sorted({str(i.get("category") or "") for i in merged_items if str(i.get("category") or "")})
    if not merged_items and deterministic_issues:
        llm_trace["fallback_reason"] = "llm_merge_empty"
        return {"quality_audit": list(deterministic_issues), "llm_trace": llm_trace}
    return {"quality_audit": merged_items, "llm_trace": llm_trace}


def suggest_quality_audit(
    *,
    profile: Dict[str, Any],
    mappings: List[Dict[str, Any]],
    instructions_context: Optional[Dict[str, Any]] = None,
    settings: Optional[Settings] = None,
    learning_kb: Optional[LearningKB] = None,
    learning_retrieval: Optional[LearningRetrieval] = None,
    learning_scope: Optional[Dict[str, Any]] = None,
    roster_type: str = "practitioner",
    primary_router: Optional[LLMRouter] = None,
    verifier_router: Optional[LLMRouter] = None,
    collaboration_mode: Optional[str] = None,
    demo_mode: bool = False,
) -> Dict[str, Any]:
    runtime_settings = settings or get_settings()
    router_factory = LLMRouterFactory(settings=runtime_settings)
    quality_router = primary_router or router_factory.for_task("quality_audit")
    verifier = verifier_router or router_factory.for_task("verifier")

    deterministic_issues = _deterministic_quality_audit(
        profile=profile,
        mappings=mappings,
        instructions_context=instructions_context,
    )

    if demo_mode:
        # Demo mode: use preprocessing pipeline's schema-driven validation for UI consistency
        # This ensures the UI shows the SAME issues as the Business_Validations column
        pipeline_issues: List[Dict[str, Any]] = []
        try:
            import pandas as pd
            from universal_roster_v2.core.preprocessing_pipeline import PreprocessingPipeline

            # Load the file from profile
            sample_rows = profile.get("sample_rows", [])
            if sample_rows:
                df = pd.DataFrame(sample_rows).fillna("")
            else:
                # Try loading from full file path
                file_path = str(profile.get("file_path") or profile.get("input_file") or "")
                if file_path:
                    from pathlib import Path as _P
                    p = _P(file_path)
                    if p.exists():
                        df = pd.read_csv(p, dtype=str, encoding="utf-8-sig").fillna("") if p.suffix == ".csv" else pd.read_excel(p, dtype=str).fillna("")
                    else:
                        df = pd.DataFrame()
                else:
                    df = pd.DataFrame()

            if not df.empty:
                # Get BQ NPPES cache
                nppes_cache = {}
                try:
                    from universal_roster_v2.core.reference_clients import ReferenceClientFactory
                    factory = ReferenceClientFactory(runtime_settings)
                    bq = factory.bq()
                    if bq and hasattr(bq, "bulk_lookup_npis"):
                        npi_col = None
                        for m in mappings:
                            if "npi" in str(m.get("target_field", "")).lower() and "group" not in str(m.get("target_field", "")).lower():
                                npi_col = str(m.get("source_column", "")).strip()
                                break
                        if npi_col and npi_col in df.columns:
                            unique_npis = list(set(df[npi_col].dropna().astype(str).str.strip().tolist()))
                            unique_npis = [n for n in unique_npis if n]
                            nppes_cache = bq.bulk_lookup_npis(unique_npis)
                except Exception:
                    pass

                pipeline = PreprocessingPipeline(mappings=mappings, nppes_cache=nppes_cache)
                pipeline_issues = pipeline.summarize_for_ui(df)
        except Exception:
            pass

        # Merge: use pipeline issues as primary, add any deterministic issues not already covered
        seen_ids = {i["id"] for i in pipeline_issues}
        issues = list(pipeline_issues)
        for di in deterministic_issues:
            if di.get("id") not in seen_ids:
                issues.append(di)

        llm_trace: Dict[str, Any] = {
            "used": False,
            "demo_mode": True,
            "engine": "preprocessing_pipeline_schema_driven",
            "primary": {"status": "skipped_demo", "provider": None, "model": None, "attempts": []},
            "verifier": {"status": "skipped_demo", "provider": None, "model": None, "attempts": []},
        }
    else:
        llm_result: Dict[str, Any]
        fallback_reason: Optional[str] = None
        try:
            llm_result = _llm_primary_quality_audit(
                profile=profile,
                mappings=mappings,
                deterministic_issues=deterministic_issues,
                instructions_context=instructions_context,
                primary_router=quality_router,
                verifier_router=verifier,
                settings=runtime_settings,
                collaboration_mode=str(collaboration_mode or runtime_settings.collaboration_mode or "advisory"),
            )
        except Exception:
            llm_result = {
                "quality_audit": list(deterministic_issues),
                "llm_trace": {
                    "used": False,
                    "engine": "deterministic_quality_audit",
                    "provider_order": {
                        "analysis": quality_router.provider_names(),
                        "verifier": verifier.provider_names(),
                    },
                    "primary": {"status": "error", "attempts": ["error:quality_audit_llm_exception"]},
                    "verifier": {"status": "skipped", "attempts": []},
                    "fallback_reason": "llm_exception",
                },
            }
            fallback_reason = "llm_exception"

        issues = list(llm_result.get("quality_audit") or [])
        llm_trace = dict(llm_result.get("llm_trace") or {})

        if fallback_reason and not llm_trace.get("fallback_reason"):
            llm_trace["fallback_reason"] = fallback_reason

        _apply_learning_adjustments(
            issues,
            learning_kb=learning_kb,
            roster_type=str(roster_type or "practitioner"),
            learning_scope=learning_scope,
        )

        retrieval_trace = _apply_retrieval_adjustments(
            issues,
            learning_retrieval=learning_retrieval,
            roster_type=str(roster_type or "practitioner"),
            learning_scope=learning_scope,
        )
        llm_trace["retrieval"] = retrieval_trace

    enrichment = enrich_quality_audit(
        profile=profile,
        mappings=mappings,
        base_issues=issues,
        settings=runtime_settings,
    )
    seen = {str(issue.get("id") or "") for issue in issues if isinstance(issue, dict)}
    enriched_issues = enrichment.get("issues", []) if isinstance(enrichment, dict) else []
    if isinstance(enriched_issues, list):
        for issue in enriched_issues:
            if not isinstance(issue, dict):
                continue
            issue_id = str(issue.get("id") or "")
            if issue_id and issue_id not in seen:
                issues.append(issue)
                seen.add(issue_id)

    llm_trace.setdefault("engine", "quality_audit_llm")
    llm_trace["rule_count"] = len(issues)
    llm_trace["categories"] = sorted({str(i.get("category") or "") for i in issues if str(i.get("category") or "")})
    llm_trace["enrichment"] = enrichment.get("trace", {}) if isinstance(enrichment, dict) else {}

    if not llm_trace.get("used") and not llm_trace.get("fallback_reason"):
        llm_trace["fallback_reason"] = "deterministic_only"

    return {"quality_audit": issues, "llm_trace": llm_trace}
