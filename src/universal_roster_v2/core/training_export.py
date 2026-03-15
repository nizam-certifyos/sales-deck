from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from universal_roster_v2.config import Settings, get_settings
from universal_roster_v2.core.learning_episodes import LearningEpisodeStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_section(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"mapping", "mappings"}:
        return "mappings"
    if text in {"transform", "transformation", "transformations"}:
        return "transformations"
    if text in {"validation", "validations", "bq_validations", "bq_validation"}:
        return "validations"
    if text in {"quality_audit", "quality_audits", "quality", "audit"}:
        return "quality_audit"
    return text or "unknown"


class TrainingExportService:
    _REDACTION_PATTERNS = [
        (r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", "[REDACTED_EMAIL]"),
        (r"\b\d{3}[-.\s]?\d{2}[-.\s]?\d{4}\b", "[REDACTED_SSN]"),
        (r"\b\+?\d[\d\s().-]{7,}\d\b", "[REDACTED_PHONE]"),
    ]

    def __init__(
        self,
        *,
        episode_store: Optional[LearningEpisodeStore] = None,
        settings: Optional[Settings] = None,
    ):
        self.settings = settings or get_settings()
        self.episode_store = episode_store or LearningEpisodeStore(settings=self.settings)

    def _redact_text(self, text: Any) -> str:
        value = str(text or "")
        for pattern, replacement in self._REDACTION_PATTERNS:
            value = re.sub(pattern, replacement, value, flags=re.IGNORECASE)
        return value

    def _extract_latest_rationale(self, episode: Dict[str, Any]) -> Dict[str, Any]:
        review_history = episode.get("review_history") or []
        if not isinstance(review_history, list):
            return {
                "text": "",
                "tags": [],
                "question_text": "",
                "response_type": "",
                "status": "",
                "decision_source": "",
                "decision_confidence": 0.0,
                "supervisor": {},
            }
        for entry in reversed(review_history):
            if not isinstance(entry, dict):
                continue
            rationale = entry.get("rationale")
            if not isinstance(rationale, dict):
                continue
            tags = [str(tag).strip().lower() for tag in (rationale.get("rationale_tags") or []) if str(tag).strip()]
            followup = rationale.get("followup") if isinstance(rationale.get("followup"), dict) else {}
            decision = rationale.get("decision") if isinstance(rationale.get("decision"), dict) else {}
            supervisor = rationale.get("supervisor") if isinstance(rationale.get("supervisor"), dict) else {}
            return {
                "text": self._redact_text(rationale.get("rationale_text")),
                "tags": tags,
                "question_text": self._redact_text(followup.get("question_text")),
                "response_type": str(followup.get("response_type") or ""),
                "status": str(followup.get("status") or ""),
                "decision_source": str(decision.get("source") or ""),
                "decision_confidence": float(decision.get("confidence", 0.0) or 0.0),
                "supervisor": {
                    "provider": str(supervisor.get("provider") or ""),
                    "model": str(supervisor.get("model") or ""),
                    "status": str(supervisor.get("status") or ""),
                    "attempts": supervisor.get("attempts") if isinstance(supervisor.get("attempts"), list) else [],
                },
            }
        return {
            "text": "",
            "tags": [],
            "question_text": "",
            "response_type": "",
            "status": "",
            "decision_source": "",
            "decision_confidence": 0.0,
            "supervisor": {},
        }

    def _prompt_context(self, episode: Dict[str, Any]) -> Dict[str, Any]:
        rationale = self._extract_latest_rationale(episode)
        review_history = episode.get("review_history") if isinstance(episode.get("review_history"), list) else []
        latest_review = review_history[-1] if review_history else {}
        return {
            "section": _safe_section(episode.get("section")),
            "item_id": episode.get("item_id"),
            "candidate_key": episode.get("candidate_key") or {},
            "workspace_scope": episode.get("workspace_scope") or {},
            "roster_type": episode.get("roster_type"),
            "fingerprint": episode.get("fingerprint"),
            "run_metadata": episode.get("run_metadata") or {},
            "rationale_text": rationale.get("text") or "",
            "rationale_tags": rationale.get("tags") or [],
            "followup_question_text": rationale.get("question_text") or "",
            "followup_response_type": rationale.get("response_type") or "",
            "followup_status": rationale.get("status") or "",
            "decision_source": rationale.get("decision_source") or "",
            "decision_confidence": rationale.get("decision_confidence") or 0.0,
            "reason_category": str((latest_review.get("rationale") or {}).get("reason_category") or ""),
            "impact_scope": str((latest_review.get("rationale") or {}).get("impact_scope") or "item"),
            "confidence_before": float((latest_review.get("rationale") or {}).get("confidence_before", 0.0) or 0.0),
            "confidence_after": float((latest_review.get("rationale") or {}).get("confidence_after", 0.0) or 0.0),
            "confidence_delta": float((latest_review.get("rationale") or {}).get("confidence_after", 0.0) or 0.0)
            - float((latest_review.get("rationale") or {}).get("confidence_before", 0.0) or 0.0),
            "suggested_rule_change": (latest_review.get("rationale") or {}).get("suggested_rule_change")
            if isinstance((latest_review.get("rationale") or {}).get("suggested_rule_change"), dict)
            else {},
            "supervisor": rationale.get("supervisor") or {},
        }

    def _make_sft_record(self, episode: Dict[str, Any]) -> Dict[str, Any]:
        rationale = self._extract_latest_rationale(episode)
        review_history = episode.get("review_history") if isinstance(episode.get("review_history"), list) else []
        latest_review = review_history[-1] if review_history else {}
        review_rationale = latest_review.get("rationale") if isinstance(latest_review, dict) and isinstance(latest_review.get("rationale"), dict) else {}
        return {
            "episode_id": episode.get("episode_id"),
            "section": _safe_section(episode.get("section")),
            "input": self._prompt_context(episode),
            "output": episode.get("final_candidate") or {},
            "quality_flags": episode.get("quality_flags") or {},
            "rationale": {
                "text": rationale.get("text") or "",
                "tags": rationale.get("tags") or [],
                "question_text": rationale.get("question_text") or "",
                "response_type": rationale.get("response_type") or "",
                "status": rationale.get("status") or "",
                "decision_source": rationale.get("decision_source") or "",
                "decision_confidence": rationale.get("decision_confidence") or 0.0,
                "reason_category": str(review_rationale.get("reason_category") or ""),
                "impact_scope": str(review_rationale.get("impact_scope") or "item"),
                "confidence_before": float(review_rationale.get("confidence_before", 0.0) or 0.0),
                "confidence_after": float(review_rationale.get("confidence_after", 0.0) or 0.0),
                "confidence_delta": float(review_rationale.get("confidence_after", 0.0) or 0.0)
                - float(review_rationale.get("confidence_before", 0.0) or 0.0),
                "suggested_rule_change": review_rationale.get("suggested_rule_change") if isinstance(review_rationale.get("suggested_rule_change"), dict) else {},
                "supervisor": rationale.get("supervisor") or {},
            },
            "metadata": {
                "created_at": episode.get("created_at"),
                "updated_at": episode.get("updated_at"),
                "outcome": episode.get("outcome"),
            },
        }

    def _make_preference_record(self, episode: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        accepted = episode.get("final_candidate") or {}
        primary_stage = episode.get("primary_stage") or {}
        verifier_stage = episode.get("verifier_stage") or {}
        rationale = self._extract_latest_rationale(episode)

        rejected_alternative: Dict[str, Any] = {}
        if isinstance(primary_stage.get("raw_output"), dict):
            rejected_alternative = primary_stage.get("raw_output") or {}
        elif isinstance(verifier_stage.get("raw_output"), dict):
            rejected_alternative = verifier_stage.get("raw_output") or {}

        if not rejected_alternative:
            rejected_alternative = {
                "candidate_key": episode.get("candidate_key") or {},
                "status": "primary_or_verifier_alternative_unavailable",
            }

        review_history = episode.get("review_history") if isinstance(episode.get("review_history"), list) else []
        latest_review = review_history[-1] if review_history else {}
        review_rationale = latest_review.get("rationale") if isinstance(latest_review, dict) and isinstance(latest_review.get("rationale"), dict) else {}
        return {
            "episode_id": episode.get("episode_id"),
            "section": _safe_section(episode.get("section")),
            "prompt": self._prompt_context(episode),
            "chosen": accepted,
            "rejected": rejected_alternative,
            "rationale": {
                "text": rationale.get("text") or "",
                "tags": rationale.get("tags") or [],
                "question_text": rationale.get("question_text") or "",
                "response_type": rationale.get("response_type") or "",
                "status": rationale.get("status") or "",
                "decision_source": rationale.get("decision_source") or "",
                "decision_confidence": rationale.get("decision_confidence") or 0.0,
                "reason_category": str(review_rationale.get("reason_category") or ""),
                "impact_scope": str(review_rationale.get("impact_scope") or "item"),
                "confidence_before": float(review_rationale.get("confidence_before", 0.0) or 0.0),
                "confidence_after": float(review_rationale.get("confidence_after", 0.0) or 0.0),
                "confidence_delta": float(review_rationale.get("confidence_after", 0.0) or 0.0)
                - float(review_rationale.get("confidence_before", 0.0) or 0.0),
                "suggested_rule_change": review_rationale.get("suggested_rule_change") if isinstance(review_rationale.get("suggested_rule_change"), dict) else {},
                "supervisor": rationale.get("supervisor") or {},
            },
            "metadata": {
                "created_at": episode.get("created_at"),
                "updated_at": episode.get("updated_at"),
                "outcome": episode.get("outcome"),
                "supervisor_trace": rationale.get("supervisor") or {},
            },
        }

    @staticmethod
    def _write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
        encoded = [json.dumps(record, ensure_ascii=False, sort_keys=True) for record in records]
        suffix = "\n" if encoded else ""
        path.write_text("\n".join(encoded) + suffix, encoding="utf-8")

    def _build_manifest(
        self,
        *,
        all_training_eligible: List[Dict[str, Any]],
        exported_sft: List[Dict[str, Any]],
        exported_preference: List[Dict[str, Any]],
        exclusions: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        by_section_total = Counter(_safe_section(item.get("section")) for item in all_training_eligible)
        by_section_sft = Counter(_safe_section(item.get("section")) for item in exported_sft)
        by_section_pref = Counter(_safe_section(item.get("section")) for item in exported_preference)

        accepted_count = len(all_training_eligible)
        rejected_count = len([item for item in self.episode_store.list_episodes() if str(item.get("outcome") or "") == "rejected"])
        total_reviewed = accepted_count + rejected_count
        acceptance_rate = (accepted_count / total_reviewed) if total_reviewed else 0.0
        rationale_rich_sft = len([item for item in exported_sft if str(((item.get("rationale") or {}).get("text") or "")).strip()])
        rationale_rich_pref = len([item for item in exported_preference if str(((item.get("rationale") or {}).get("text") or "")).strip()])

        return {
            "generated_at": _utc_now_iso(),
            "source_path": str(self.episode_store.path),
            "eligible_episode_count": accepted_count,
            "records": {
                "sft": len(exported_sft),
                "preference": len(exported_preference),
                "rationale_rich_sft": rationale_rich_sft,
                "rationale_rich_preference": rationale_rich_pref,
            },
            "sections": {
                "eligible": dict(by_section_total),
                "sft": dict(by_section_sft),
                "preference": dict(by_section_pref),
            },
            "quality": {
                "accepted": accepted_count,
                "rejected": rejected_count,
                "acceptance_rate": round(acceptance_rate, 4),
            },
            "rationale_export_enabled": bool(self.settings.enable_rationale_training_export),
            "exclusions": exclusions,
        }

    def export(self, output_dir: Optional[str | Path] = None) -> Dict[str, Any]:
        if not self.episode_store.is_enabled():
            return {
                "status": "skipped",
                "reason": "learning_episode_capture_disabled",
            }

        export_root = Path(output_dir or self.settings.training_export_dir).expanduser().resolve()
        export_root.mkdir(parents=True, exist_ok=True)

        eligible = self.episode_store.training_eligible_episodes()
        min_required = max(1, int(self.settings.training_min_accepted_episodes or 1))
        if len(eligible) < min_required:
            return {
                "status": "skipped",
                "reason": "insufficient_accepted_episodes",
                "eligible_count": len(eligible),
                "min_required": min_required,
            }

        reviewed_by_section = Counter(
            _safe_section(item.get("section"))
            for item in self.episode_store.list_episodes()
            if str(item.get("outcome") or "") in {"accepted", "rejected"}
        )
        accepted_by_section = Counter(_safe_section(item.get("section")) for item in eligible)

        max_records = max(1, int(self.settings.training_export_max_records or 1))
        eligible = eligible[-max_records:]

        sft_records: List[Dict[str, Any]] = []
        pref_records: List[Dict[str, Any]] = []
        exclusions: List[Dict[str, Any]] = []

        include_rationale = bool(self.settings.enable_rationale_training_export)

        for episode in eligible:
            episode_id = str(episode.get("episode_id") or "")
            section = _safe_section(episode.get("section"))
            quality_flags = episode.get("quality_flags") or {}
            if not all(bool(value) for value in quality_flags.values()):
                exclusions.append(
                    {
                        "episode_id": episode_id,
                        "section": section,
                        "reason": "quality_gate_failed",
                        "flags": quality_flags,
                    }
                )
                continue

            if section == "quality_audit":
                gate = self.settings.qwen_quality_gate_for_section(section)
                section_min_accepted = int(float(gate.get("min_accepted", 0.0) or 0.0))
                section_min_rate = float(gate.get("min_acceptance_rate", 0.0) or 0.0)
                section_accepted = int(accepted_by_section.get(section, 0))
                section_reviewed = int(reviewed_by_section.get(section, 0))
                section_rate = (float(section_accepted) / float(section_reviewed)) if section_reviewed > 0 else 0.0

                if section_accepted < section_min_accepted or section_rate < section_min_rate:
                    exclusions.append(
                        {
                            "episode_id": episode_id,
                            "section": section,
                            "reason": "section_gate_failed",
                            "gate": {
                                "accepted": section_accepted,
                                "reviewed": section_reviewed,
                                "acceptance_rate": round(section_rate, 4),
                                "min_accepted": section_min_accepted,
                                "min_acceptance_rate": round(section_min_rate, 4),
                            },
                        }
                    )
                    continue

            sft_record = self._make_sft_record(episode)
            if not include_rationale:
                sft_record.pop("rationale", None)
                if isinstance(sft_record.get("input"), dict):
                    sft_record["input"].pop("rationale_text", None)
                    sft_record["input"].pop("rationale_tags", None)
            sft_records.append(sft_record)

            pref_record = self._make_preference_record(episode)
            if pref_record is None:
                exclusions.append(
                    {
                        "episode_id": episode_id,
                        "reason": "preference_pair_unavailable",
                    }
                )
            else:
                if not include_rationale:
                    pref_record.pop("rationale", None)
                    if isinstance(pref_record.get("prompt"), dict):
                        pref_record["prompt"].pop("rationale_text", None)
                        pref_record["prompt"].pop("rationale_tags", None)
                pref_records.append(pref_record)

        sft_path = export_root / "sft.jsonl"
        pref_path = export_root / "preference.jsonl"
        manifest_path = export_root / "manifest.json"

        self._write_jsonl(sft_path, sft_records)
        self._write_jsonl(pref_path, pref_records)

        manifest = self._build_manifest(
            all_training_eligible=eligible,
            exported_sft=sft_records,
            exported_preference=pref_records,
            exclusions=exclusions,
        )
        manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

        return {
            "status": "ok",
            "output_dir": str(export_root),
            "sft_path": str(sft_path),
            "preference_path": str(pref_path),
            "manifest_path": str(manifest_path),
            "counts": {
                "eligible": len(eligible),
                "sft": len(sft_records),
                "preference": len(pref_records),
            },
        }
