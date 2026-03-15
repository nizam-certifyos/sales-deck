from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from universal_roster_v2.config import Settings, get_settings
from universal_roster_v2.core.learning_episodes import LearningEpisodeStore
from universal_roster_v2.core.learning_kb import LearningKB


def _safe_lower(value: Any) -> str:
    return str(value or "").strip().lower()


def _list_norm(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    return [_safe_lower(item) for item in values if _safe_lower(item)]


def _first_list_item(values: Any) -> str:
    normalized = _list_norm(values)
    return normalized[0] if normalized else ""


def _tokenize(value: Any) -> List[str]:
    text = _safe_lower(value)
    if not text:
        return []
    out: List[str] = []
    current = []
    for ch in text:
        if ch.isalnum() or ch in {"_", "-"}:
            current.append(ch)
            continue
        if current:
            token = "".join(current).strip("_-")
            if token:
                out.append(token)
            current = []
    if current:
        token = "".join(current).strip("_-")
        if token:
            out.append(token)
    return out


def _parse_iso(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


class LearningRetrieval:
    _RATIONALE_MATCH_CAP = 0.2

    def __init__(
        self,
        *,
        episode_store: Optional[LearningEpisodeStore] = None,
        learning_kb: Optional[LearningKB] = None,
        settings: Optional[Settings] = None,
    ):
        self.settings = settings or get_settings()
        self.episode_store = episode_store or LearningEpisodeStore(settings=self.settings)
        self.learning_kb = learning_kb

    def is_enabled(self) -> bool:
        return bool(self.settings.enable_rag_retrieval)

    def _score_scope(self, *, episode: Dict[str, Any], roster_type: str, workspace_scope: Optional[Dict[str, Any]]) -> float:
        score = 0.0
        episode_scope = episode.get("workspace_scope") or {}
        workspace_scope = workspace_scope or {}

        if self.settings.rag_use_roster_type_weight:
            if _safe_lower(episode.get("roster_type")) == _safe_lower(roster_type):
                score += 0.2

        if self.settings.rag_use_workspace_scope_weight:
            ep_sig = _safe_lower(episode_scope.get("workspace_signature"))
            cur_sig = _safe_lower(workspace_scope.get("workspace_signature"))
            if ep_sig and cur_sig and ep_sig == cur_sig:
                score += 0.15

            ep_tenant = _safe_lower(episode_scope.get("tenant_id"))
            cur_tenant = _safe_lower(workspace_scope.get("tenant_id"))
            if ep_tenant and cur_tenant and ep_tenant == cur_tenant:
                score += 0.08

            ep_client = _safe_lower(episode_scope.get("client_id"))
            cur_client = _safe_lower(workspace_scope.get("client_id"))
            if ep_client and cur_client and ep_client == cur_client:
                score += 0.08

        return score

    def _score_mapping(self, episode: Dict[str, Any], item_key: Dict[str, Any]) -> float:
        key = episode.get("candidate_key") or {}
        score = 0.0

        key_source = _safe_lower(key.get("source_column")) or _first_list_item(key.get("source_columns"))
        item_source = _safe_lower(item_key.get("source_column")) or _first_list_item(item_key.get("source_columns"))
        key_target = _safe_lower(key.get("target_field")) or _first_list_item(key.get("target_fields"))
        item_target = _safe_lower(item_key.get("target_field")) or _first_list_item(item_key.get("target_fields"))

        src_match = bool(key_source and item_source and key_source == item_source)
        tgt_match = bool(key_target and item_target and key_target == item_target)
        if src_match:
            score += 0.45
        if tgt_match:
            score += 0.35
        if src_match and tgt_match:
            score += 0.15

        key_rule = _safe_lower(key.get("rule_type"))
        item_rule = _safe_lower(item_key.get("rule_type"))
        if key_rule and key_rule == item_rule:
            score += 0.1

        key_transform = _safe_lower(key.get("transform_name"))
        item_transform = _safe_lower(item_key.get("transform_name"))
        if key_transform and key_transform == item_transform:
            score += 0.1

        section_key = _safe_lower(item_key.get("section") or episode.get("section"))
        if section_key in {"quality_audit", "quality", "audit"}:
            key_category = _safe_lower(key.get("category"))
            item_category = _safe_lower(item_key.get("category"))
            if key_category and key_category == item_category:
                score += 0.18

            key_severity = _safe_lower(key.get("severity"))
            item_severity = _safe_lower(item_key.get("severity"))
            if key_severity and key_severity == item_severity:
                score += 0.12

            if key_rule and item_rule and key_rule == item_rule:
                score += 0.1

            if src_match and key_rule and key_severity:
                score += 0.05

        return min(1.0, score)

    def _score_freshness(self, episode: Dict[str, Any]) -> float:
        updated_at = _parse_iso(episode.get("updated_at")) or _parse_iso(episode.get("created_at"))
        if updated_at is None:
            return 0.0
        age_days = max(0.0, (datetime.now(timezone.utc) - updated_at).total_seconds() / 86400.0)
        capped = min(age_days, 60.0)
        return max(0.0, (60.0 - capped) / 60.0) * 0.08

    def _chat_hint_boost(self, section: str, item_key: Dict[str, Any], roster_type: str, workspace_scope: Optional[Dict[str, Any]]) -> float:
        if self.learning_kb is None:
            return 0.0
        weight = float(self.settings.rag_chat_outcome_hint_weight or 0.0)
        if weight <= 0:
            return 0.0

        try:
            outcomes = self.learning_kb.get_chat_outcomes(limit=200)
        except Exception:
            return 0.0
        if not outcomes:
            return 0.0

        key_text = " ".join(
            [
                _safe_lower(item_key.get("item_id")),
                _safe_lower(item_key.get("source_column")) or _first_list_item(item_key.get("source_columns")),
                _safe_lower(item_key.get("target_field")) or _first_list_item(item_key.get("target_fields")),
                _safe_lower(item_key.get("transform_name")),
                _safe_lower(item_key.get("rule_type")),
            ]
        )

        scope_sig = _safe_lower((workspace_scope or {}).get("workspace_signature"))
        tenant = _safe_lower((workspace_scope or {}).get("tenant_id"))
        client = _safe_lower((workspace_scope or {}).get("client_id"))

        hits = 0
        for outcome in outcomes:
            if not isinstance(outcome, dict):
                continue
            if _safe_lower(outcome.get("roster_type")) and _safe_lower(outcome.get("roster_type")) != _safe_lower(roster_type):
                continue

            out_scope = outcome.get("workspace_scope") or {}
            if scope_sig and _safe_lower(out_scope.get("workspace_signature")) and _safe_lower(out_scope.get("workspace_signature")) != scope_sig:
                continue
            if tenant and _safe_lower(out_scope.get("tenant_id")) and _safe_lower(out_scope.get("tenant_id")) != tenant:
                continue
            if client and _safe_lower(out_scope.get("client_id")) and _safe_lower(out_scope.get("client_id")) != client:
                continue

            as_text = _safe_lower(outcome.get("event")) + " " + _safe_lower(outcome.get("result")) + " " + _safe_lower(outcome)
            if section in as_text and key_text and any(token for token in key_text.split(" ") if token and token in as_text):
                hits += 1
                if hits >= 3:
                    break

        return min(weight, hits * (weight / 3.0))

    @staticmethod
    def _scope_match(scope_a: Optional[Dict[str, Any]], scope_b: Optional[Dict[str, Any]]) -> bool:
        a = scope_a or {}
        b = scope_b or {}
        for key in ["workspace_signature", "tenant_id", "client_id"]:
            av = _safe_lower(a.get(key))
            bv = _safe_lower(b.get(key))
            if av and bv and av != bv:
                return False
        return True

    def _rationale_boost(
        self,
        *,
        section: str,
        item_key: Dict[str, Any],
        workspace_scope: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if self.learning_kb is None or not self.settings.enable_rationale_retrieval_influence:
            return {"score": 0.0, "snippets": []}

        try:
            if hasattr(self.learning_kb, "get_decision_events"):
                rationales = self.learning_kb.get_decision_events(limit=500)
            else:
                rationales = self.learning_kb.get_rationales(limit=500)
        except Exception:
            return {"score": 0.0, "snippets": []}
        if not rationales:
            return {"score": 0.0, "snippets": []}

        section_key = self.settings.normalize_section_key(section)
        key_tokens = set(
            _tokenize(item_key.get("item_id"))
            + _tokenize(item_key.get("source_column"))
            + _tokenize(item_key.get("target_field"))
            + _tokenize(item_key.get("transform_name"))
            + _tokenize(item_key.get("rule_type"))
        )
        if not key_tokens:
            key_tokens = set(_tokenize(item_key))

        matches: List[Dict[str, Any]] = []
        for rationale in rationales:
            if not isinstance(rationale, dict):
                continue
            item_type = self.settings.normalize_section_key(str(rationale.get("item_type") or rationale.get("section") or ""))
            if item_type and section_key and item_type != section_key:
                continue
            if not self._scope_match(rationale.get("workspace_scope"), workspace_scope):
                continue

            text = str(rationale.get("rationale_text") or "").strip()
            tags = [str(tag).strip().lower() for tag in (rationale.get("rationale_tags") or []) if str(tag).strip()]
            tokens = set(_tokenize(text)) | set(tags)
            overlap = tokens.intersection(key_tokens)
            if not overlap and not text:
                continue

            decision = rationale.get("decision") if isinstance(rationale.get("decision"), dict) else {}
            followup = rationale.get("followup") if isinstance(rationale.get("followup"), dict) else {}
            supervisor = rationale.get("supervisor") if isinstance(rationale.get("supervisor"), dict) else {}

            lexical = min(1.0, len(overlap) / max(2.0, len(key_tokens))) if overlap else 0.0
            confidence = float(decision.get("confidence", 0.0) or 0.0)
            quality = 0.3 + (0.7 * confidence)

            response_type = _safe_lower(followup.get("response_type"))
            status = _safe_lower(followup.get("status"))
            if response_type in {"skip", "skipped"} or status in {"skip", "skipped"}:
                quality *= 0.4
            if len(text) < 8:
                quality *= 0.6

            provider = _safe_lower(supervisor.get("provider"))
            if provider.startswith("claude"):
                quality *= 1.05

            score = min(1.0, max(lexical, 0.15) * quality)
            approved_value = decision.get("approved", rationale.get("approved"))
            if _safe_lower(approved_value) in {"false", "0", "no"}:
                score *= 0.95

            matches.append(
                {
                    "score": score,
                    "text": text,
                    "tags": tags,
                    "item_id": rationale.get("item_id"),
                    "timestamp": rationale.get("timestamp"),
                    "decision_confidence": round(confidence, 4),
                    "response_type": response_type,
                    "provider": provider,
                }
            )

        if not matches:
            return {"score": 0.0, "snippets": []}

        matches.sort(key=lambda row: float(row.get("score", 0.0)), reverse=True)
        top = matches[:4]
        avg = sum(float(row.get("score", 0.0)) for row in top) / float(len(top))
        capped = min(self._RATIONALE_MATCH_CAP, avg * self._RATIONALE_MATCH_CAP)
        snippets = [
            {
                "item_id": row.get("item_id"),
                "text": row.get("text"),
                "tags": row.get("tags"),
                "score": round(float(row.get("score", 0.0)), 4),
                "decision_confidence": row.get("decision_confidence"),
                "response_type": row.get("response_type"),
                "provider": row.get("provider"),
            }
            for row in top
        ]
        return {
            "score": round(capped, 4),
            "snippets": snippets,
        }

    def retrieve(
        self,
        *,
        section: str,
        item_key: Dict[str, Any],
        roster_type: str,
        workspace_scope: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not self.is_enabled():
            return {
                "enabled": False,
                "hits": [],
                "count": 0,
                "min_score": float(self.settings.rag_min_score),
                "sources": [],
            }

        episodes = self.episode_store.accepted_episodes(section=section)
        if not episodes:
            return {
                "enabled": True,
                "hits": [],
                "count": 0,
                "min_score": float(self.settings.rag_min_score),
                "sources": [],
            }

        rationale_signal = self._rationale_boost(
            section=section,
            item_key=item_key,
            workspace_scope=workspace_scope,
        )
        rationale_boost = float(rationale_signal.get("score", 0.0) or 0.0)
        rationale_snippets = rationale_signal.get("snippets") or []

        scored: List[Dict[str, Any]] = []
        for episode in episodes:
            base = self._score_mapping(episode, item_key)
            if base <= 0:
                continue
            scope_score = self._score_scope(episode=episode, roster_type=roster_type, workspace_scope=workspace_scope)
            freshness = self._score_freshness(episode)
            chat_boost = self._chat_hint_boost(section, item_key, roster_type, workspace_scope)
            total = min(1.0, base + scope_score + freshness + chat_boost + rationale_boost)
            if total < float(self.settings.rag_min_score):
                continue
            scored.append(
                {
                    "score": round(total, 4),
                    "episode_id": episode.get("episode_id"),
                    "source": "learning_episode",
                    "section": episode.get("section"),
                    "item_id": episode.get("item_id"),
                    "candidate_key": episode.get("candidate_key") or {},
                    "final_candidate": episode.get("final_candidate") or {},
                    "outcome": episode.get("outcome"),
                    "updated_at": episode.get("updated_at"),
                    "rationale_snippets": rationale_snippets,
                    "influence_trace": {
                        "why_used": [
                            snippet.get("text")
                            for snippet in rationale_snippets
                            if str(snippet.get("text") or "").strip()
                        ][:2],
                        "rationale_signal": round(rationale_boost, 4),
                    },
                    "score_breakdown": {
                        "base": round(base, 4),
                        "scope": round(scope_score, 4),
                        "freshness": round(freshness, 4),
                        "chat_hint": round(chat_boost, 4),
                        "rationale": round(rationale_boost, 4),
                    },
                }
            )

        scored.sort(key=lambda row: (float(row.get("score", 0.0)), _parse_iso(row.get("updated_at")) or datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
        top_k = max(1, int(self.settings.rag_max_examples_per_item or 1))
        hits = scored[:top_k]

        return {
            "enabled": True,
            "hits": hits,
            "count": len(hits),
            "min_score": float(self.settings.rag_min_score),
            "sources": sorted({str(hit.get("source") or "") for hit in hits if str(hit.get("source") or "")}),
            "scores": [hit.get("score") for hit in hits],
            "rationale_signal": {
                "enabled": bool(self.settings.enable_rationale_retrieval_influence),
                "boost": rationale_boost,
                "snippets": rationale_snippets,
            },
        }

    @staticmethod
    def format_examples_for_prompt(section: str, retrieval_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        hits = retrieval_result.get("hits") or []
        examples: List[Dict[str, Any]] = []
        for idx, hit in enumerate(hits, start=1):
            final_candidate = hit.get("final_candidate") or {}
            candidate_key = hit.get("candidate_key") or {}
            example = {
                "rank": idx,
                "score": hit.get("score"),
                "item_id": hit.get("item_id"),
                "candidate_key": candidate_key,
                "final_candidate": final_candidate,
                "rationale_snippets": hit.get("rationale_snippets") or [],
            }
            if section == "mappings":
                example["summary"] = {
                    "source_column": candidate_key.get("source_column") or final_candidate.get("source_column"),
                    "target_field": candidate_key.get("target_field") or final_candidate.get("target_field"),
                    "confidence": final_candidate.get("confidence"),
                    "reason": final_candidate.get("reason"),
                }
            elif section == "transformations":
                example["summary"] = {
                    "name": candidate_key.get("transform_name") or final_candidate.get("name"),
                    "source_columns": final_candidate.get("source_columns") or candidate_key.get("source_columns"),
                    "target_fields": final_candidate.get("target_fields") or candidate_key.get("target_fields"),
                    "params": final_candidate.get("params") or {},
                }
            elif section == "quality_audit":
                example["summary"] = {
                    "category": final_candidate.get("category") or candidate_key.get("category"),
                    "rule_type": candidate_key.get("rule_type") or final_candidate.get("rule_type"),
                    "severity": final_candidate.get("severity") or candidate_key.get("severity"),
                    "source_column": candidate_key.get("source_column") or final_candidate.get("source_column"),
                    "target_field": candidate_key.get("target_field") or final_candidate.get("target_field"),
                    "message": final_candidate.get("message"),
                }
            else:
                example["summary"] = {
                    "rule_type": candidate_key.get("rule_type") or final_candidate.get("rule_type"),
                    "source_column": candidate_key.get("source_column") or final_candidate.get("source_column"),
                    "target_field": candidate_key.get("target_field") or final_candidate.get("target_field"),
                    "sql_expression": final_candidate.get("sql_expression"),
                    "message": final_candidate.get("message"),
                }
            examples.append(example)
        return examples
