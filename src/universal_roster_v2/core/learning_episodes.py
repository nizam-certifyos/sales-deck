from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from universal_roster_v2.config import Settings, get_settings


_STATUS_PENDING = "pending"
_STATUS_ACCEPTED = "accepted"
_STATUS_REJECTED = "rejected"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _parse_dt(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _json_clone(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


class LearningEpisodeStore:
    VERSION = 1

    def __init__(self, path: str | Path | None = None, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self.path = Path(path or self.settings.learning_episodes_path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def is_enabled(self) -> bool:
        return bool(self.settings.enable_learning_episode_capture)

    def _normalize_text(self, value: Any) -> str:
        text = str(value or "")
        limit = max(200, int(self.settings.learning_episode_payload_max_chars or 0))
        if len(text) <= limit:
            return text
        return text[:limit] + "…"

    def _sanitize_payload(self, value: Any) -> Any:
        if not self.settings.capture_llm_payloads:
            return None
        if value is None:
            return None
        if isinstance(value, str):
            return self._normalize_text(value)
        if isinstance(value, (int, float, bool)):
            return value
        if isinstance(value, list):
            return [self._sanitize_payload(item) for item in value][:50]
        if isinstance(value, dict):
            out: Dict[str, Any] = {}
            for key, item in value.items():
                out[str(key)] = self._sanitize_payload(item)
            return out
        return self._normalize_text(value)

    def _eligible_quality_flags(self, episode: Dict[str, Any]) -> Dict[str, bool]:
        final_candidate = episode.get("final_candidate") or {}
        policy = episode.get("policy") or {}
        verifier = episode.get("verifier_stage") or {}
        schema_valid = bool(final_candidate.get("schema_valid", True))
        policy_ok = str(policy.get("status") or "ok").lower() in {"ok", "accepted", "not_applicable", "satisfied", "not_required"}
        verifier_ok = str(verifier.get("status") or "ok").lower() not in {"failed", "error"}
        quarantined = bool(episode.get("quarantined"))
        no_rejection = str(episode.get("outcome") or _STATUS_PENDING) != _STATUS_REJECTED
        return {
            "schema_valid": schema_valid,
            "policy_ok": policy_ok,
            "verifier_ok": verifier_ok,
            "no_rejection": no_rejection,
            "not_quarantined": not quarantined,
        }

    def _is_quality_eligible(self, episode: Dict[str, Any]) -> bool:
        flags = self._eligible_quality_flags(episode)
        return all(flags.values())

    def _read_lines(self) -> List[Dict[str, Any]]:
        if not self.path.exists():
            return []
        rows: List[Dict[str, Any]] = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except Exception:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
        return rows

    def _write_lines(self, rows: Iterable[Dict[str, Any]]) -> None:
        encoded = [json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows]
        suffix = "\n" if encoded else ""
        self.path.write_text("\n".join(encoded) + suffix, encoding="utf-8")

    def _prune_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        max_age_days = max(1, int(self.settings.learning_episode_max_age_days or 0))
        cutoff = _utc_now() - timedelta(days=max_age_days)
        kept: List[Dict[str, Any]] = []
        for row in rows:
            created_at = _parse_dt(row.get("created_at")) or _parse_dt(row.get("updated_at"))
            if created_at and created_at < cutoff:
                continue
            kept.append(row)
        max_records = max(1, int(self.settings.learning_episode_max_records or 0))
        if len(kept) > max_records:
            kept = kept[-max_records:]
        return kept

    def _write_rows_with_retention(self, rows: List[Dict[str, Any]]) -> None:
        self._write_lines(self._prune_rows(rows))

    def _episode_id(self, section: str, item_id: str, fingerprint: str, stage_status: str) -> str:
        safe_section = str(section or "unknown").strip().lower()
        safe_item = str(item_id or "item").strip()
        safe_fp = str(fingerprint or "none").strip()
        safe_status = str(stage_status or "unknown").strip().lower()
        return f"{safe_section}::{safe_item}::{safe_fp}::{safe_status}"

    def capture_episode(
        self,
        *,
        section: str,
        item_id: str,
        candidate_key: Dict[str, Any],
        workspace_scope: Dict[str, Any],
        roster_type: str,
        fingerprint: str,
        run_metadata: Optional[Dict[str, Any]] = None,
        primary_stage: Optional[Dict[str, Any]] = None,
        verifier_stage: Optional[Dict[str, Any]] = None,
        final_candidate: Optional[Dict[str, Any]] = None,
        policy: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if not self.is_enabled():
            return None

        now = _utc_now_iso()
        stage_status = str((verifier_stage or {}).get("status") or (primary_stage or {}).get("status") or "ok")
        episode = {
            "version": self.VERSION,
            "episode_id": self._episode_id(section, item_id, fingerprint, stage_status),
            "section": str(section or "").strip().lower(),
            "item_id": str(item_id or "").strip(),
            "candidate_key": _json_clone(candidate_key or {}),
            "workspace_scope": _json_clone(workspace_scope or {}),
            "roster_type": str(roster_type or "").strip().lower(),
            "fingerprint": str(fingerprint or "").strip(),
            "run_metadata": _json_clone(run_metadata or {}),
            "primary_stage": self._sanitize_stage(primary_stage),
            "verifier_stage": self._sanitize_stage(verifier_stage),
            "final_candidate": _json_clone(final_candidate or {}),
            "policy": _json_clone(policy or {}),
            "outcome": _STATUS_PENDING,
            "eligible_for_rag": False,
            "eligible_for_training": False,
            "quarantined": False,
            "quarantine_reasons": [],
            "created_at": now,
            "updated_at": now,
            "review_history": [],
        }
        quality_flags = self._eligible_quality_flags(episode)
        episode["quality_flags"] = quality_flags
        rows = self._read_lines()
        rows.append(episode)
        self._write_rows_with_retention(rows)
        return episode

    def _sanitize_stage(self, stage: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        payload = dict(stage or {})
        if not payload:
            return {}
        if "prompt" in payload:
            payload["prompt"] = self._sanitize_payload(payload.get("prompt"))
        if "raw_output" in payload:
            payload["raw_output"] = self._sanitize_payload(payload.get("raw_output"))
        return _json_clone(payload)

    def list_episodes(self) -> List[Dict[str, Any]]:
        return self._read_lines()

    def lookup_candidates(self, *, section: str, item_id: str, fingerprint: Optional[str] = None) -> List[Dict[str, Any]]:
        matches: List[Dict[str, Any]] = []
        for row in self._read_lines():
            if str(row.get("section") or "") != str(section or ""):
                continue
            if str(row.get("item_id") or "") != str(item_id or ""):
                continue
            if fingerprint and str(row.get("fingerprint") or "") != str(fingerprint):
                continue
            matches.append(row)
        return matches

    def lookup_by_item(self, item_id: str) -> List[Dict[str, Any]]:
        return [row for row in self._read_lines() if str(row.get("item_id") or "") == str(item_id or "")]

    @staticmethod
    def _rationale_review_payload(rationale: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not isinstance(rationale, dict):
            return None
        followup = rationale.get("followup") if isinstance(rationale.get("followup"), dict) else {}
        decision = rationale.get("decision") if isinstance(rationale.get("decision"), dict) else {}
        supervisor = rationale.get("supervisor") if isinstance(rationale.get("supervisor"), dict) else {}
        return {
            "schema_version": rationale.get("schema_version", 3),
            "event": rationale.get("event"),
            "item_type": rationale.get("item_type"),
            "item_id": rationale.get("item_id"),
            "section": rationale.get("section"),
            "approved": rationale.get("approved"),
            "rationale_text": rationale.get("rationale_text"),
            "rationale_tags": rationale.get("rationale_tags") or [],
            "workspace_scope": rationale.get("workspace_scope") or {},
            "workspace_signature": rationale.get("workspace_signature") or "",
            "tenant_id": rationale.get("tenant_id") or "",
            "client_id": rationale.get("client_id") or "",
            "thread_id": rationale.get("thread_id") or "",
            "item_context": rationale.get("item_context") if isinstance(rationale.get("item_context"), dict) else {},
            "decision": {
                "approved": decision.get("approved", rationale.get("approved")),
                "source": decision.get("source") or rationale.get("source") or "",
                "confidence": decision.get("confidence", 0.0),
            },
            "followup": {
                "question_text": followup.get("question_text") or rationale.get("question_text") or "",
                "response_type": followup.get("response_type") or rationale.get("response_type") or "",
                "status": followup.get("status") or rationale.get("followup_status") or "",
            },
            "supervisor": {
                "provider": supervisor.get("provider") or "",
                "model": supervisor.get("model") or "",
                "status": supervisor.get("status") or "",
                "attempts": supervisor.get("attempts") if isinstance(supervisor.get("attempts"), list) else [],
            },
            "reason_category": rationale.get("reason_category") or "",
            "confidence_before": rationale.get("confidence_before", 0.0),
            "confidence_after": rationale.get("confidence_after", 0.0),
            "confidence_delta": float(rationale.get("confidence_after", 0.0) or 0.0) - float(rationale.get("confidence_before", 0.0) or 0.0),
            "impact_scope": rationale.get("impact_scope") or "item",
            "suggested_rule_change": rationale.get("suggested_rule_change") if isinstance(rationale.get("suggested_rule_change"), dict) else {},
            "provenance": rationale.get("provenance") or "native_capture",
            "timestamp": rationale.get("timestamp"),
            "recorded_at": rationale.get("recorded_at"),
        }

    def _iter_matching_rows(
        self,
        rows: List[Dict[str, Any]],
        *,
        item_id: str,
        section: Optional[str] = None,
        fingerprint: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        matches: List[Dict[str, Any]] = []
        for row in rows:
            if str(row.get("item_id") or "") != str(item_id or ""):
                continue
            if section and str(row.get("section") or "") != str(section or ""):
                continue
            if fingerprint and str(row.get("fingerprint") or "") != str(fingerprint or ""):
                continue
            matches.append(row)
        return matches

    def finalize_item_outcome(
        self,
        *,
        item_id: str,
        approved: bool,
        section: Optional[str] = None,
        fingerprint: Optional[str] = None,
        rationale: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        rows = self._read_lines()
        updated: List[Dict[str, Any]] = []
        decision = _STATUS_ACCEPTED if approved else _STATUS_REJECTED
        rationale_payload = self._rationale_review_payload(rationale)
        idempotent = bool(getattr(self.settings, "enable_episode_finalize_idempotency", False))

        for row in self._iter_matching_rows(rows, item_id=item_id, section=section, fingerprint=fingerprint):
            existing_outcome = str(row.get("outcome") or _STATUS_PENDING)
            review_history = row.setdefault("review_history", [])
            if not isinstance(review_history, list):
                review_history = []
                row["review_history"] = review_history

            last_review = review_history[-1] if review_history else {}
            last_rationale = last_review.get("rationale") if isinstance(last_review, dict) else None
            same_decision = existing_outcome == decision
            same_rationale = (
                isinstance(last_rationale, dict)
                and isinstance(rationale_payload, dict)
                and str(last_rationale.get("rationale_text") or "").strip()
                == str(rationale_payload.get("rationale_text") or "").strip()
            )

            if not (idempotent and same_decision and (rationale_payload is None or same_rationale)):
                row["outcome"] = decision
                review_entry: Dict[str, Any] = {
                    "timestamp": _utc_now_iso(),
                    "decision": row["outcome"],
                    "source": "user_approval",
                }
                if rationale_payload is not None:
                    review_entry["rationale"] = rationale_payload
                review_history.append(review_entry)

            if not approved and self.settings.learning_episode_quarantine_on_conflict:
                row["quarantined"] = True
                reasons = list(row.get("quarantine_reasons") or [])
                if "user_rejected" not in reasons:
                    reasons.append("user_rejected")
                row["quarantine_reasons"] = reasons
            quality_flags = self._eligible_quality_flags(row)
            row["quality_flags"] = quality_flags
            quality_eligible = self._is_quality_eligible(row)
            row["eligible_for_rag"] = bool(approved and quality_eligible)
            row["eligible_for_training"] = bool(approved and quality_eligible)
            row["updated_at"] = _utc_now_iso()
            updated.append(_json_clone(row))
        self._write_rows_with_retention(rows)
        return updated

    def attach_rationale_to_item(
        self,
        *,
        item_id: str,
        rationale: Optional[Dict[str, Any]],
        section: Optional[str] = None,
        fingerprint: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        rows = self._read_lines()
        updated: List[Dict[str, Any]] = []
        rationale_payload = self._rationale_review_payload(rationale)
        if rationale_payload is None:
            return updated

        for row in self._iter_matching_rows(rows, item_id=item_id, section=section, fingerprint=fingerprint):
            review_history = row.setdefault("review_history", [])
            if not isinstance(review_history, list):
                review_history = []
                row["review_history"] = review_history

            attached = False
            if review_history and isinstance(review_history[-1], dict):
                last = review_history[-1]
                if not isinstance(last.get("rationale"), dict):
                    last["rationale"] = rationale_payload
                    attached = True

            if not attached:
                review_history.append(
                    {
                        "timestamp": _utc_now_iso(),
                        "decision": str(row.get("outcome") or _STATUS_PENDING),
                        "source": "rationale_attachment",
                        "rationale": rationale_payload,
                    }
                )

            row["updated_at"] = _utc_now_iso()
            updated.append(_json_clone(row))

        self._write_rows_with_retention(rows)
        return updated

    def quarantine_episode(self, episode_id: str, reason: str) -> Optional[Dict[str, Any]]:
        rows = self._read_lines()
        updated: Optional[Dict[str, Any]] = None
        for row in rows:
            if str(row.get("episode_id") or "") != str(episode_id or ""):
                continue
            row["quarantined"] = True
            reasons = list(row.get("quarantine_reasons") or [])
            clean_reason = str(reason or "quarantined").strip() or "quarantined"
            if clean_reason not in reasons:
                reasons.append(clean_reason)
            row["quarantine_reasons"] = reasons
            row["eligible_for_rag"] = False
            row["eligible_for_training"] = False
            row["quality_flags"] = self._eligible_quality_flags(row)
            row["updated_at"] = _utc_now_iso()
            updated = _json_clone(row)
            break
        self._write_rows_with_retention(rows)
        return updated

    def accepted_episodes(self, section: Optional[str] = None) -> List[Dict[str, Any]]:
        rows = []
        for row in self._read_lines():
            if section and str(row.get("section") or "") != str(section or ""):
                continue
            if not row.get("eligible_for_rag"):
                continue
            if row.get("quarantined"):
                continue
            if not self._is_quality_eligible(row):
                continue
            rows.append(row)
        return rows

    def training_eligible_episodes(self, section: Optional[str] = None) -> List[Dict[str, Any]]:
        rows = []
        for row in self._read_lines():
            if section and str(row.get("section") or "") != str(section or ""):
                continue
            if not row.get("eligible_for_training"):
                continue
            if row.get("quarantined"):
                continue
            if not self._is_quality_eligible(row):
                continue
            rows.append(row)
        return rows
