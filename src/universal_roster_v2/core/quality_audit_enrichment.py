from __future__ import annotations

from typing import Any, Dict, List, Optional

import re

from universal_roster_v2.config import Settings
from universal_roster_v2.core.mapping import confidence_band
from universal_roster_v2.core.reference_clients import ReferenceClientFactory


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _sample_values_for_column(profile: Dict[str, Any], column: str) -> List[str]:
    if not column:
        return []
    by_col = profile.get("sample_values_by_column") if isinstance(profile.get("sample_values_by_column"), dict) else {}
    values = by_col.get(column) if isinstance(by_col, dict) else None
    if isinstance(values, list):
        return [str(v).strip() for v in values if str(v).strip()][:20]

    from_rows: List[str] = []
    for row in (profile.get("sample_rows") or []):
        if not isinstance(row, dict):
            continue
        value = str(row.get(column) or "").strip()
        if value:
            from_rows.append(value)
        if len(from_rows) >= 20:
            break
    return from_rows


def _stable_id(*parts: Any) -> str:
    token = "::".join(str(part or "").strip().lower().replace(" ", "_") for part in parts if str(part or "").strip())
    token = re.sub(r"[^a-z0-9_:.-]", "_", token)
    token = re.sub(r"_+", "_", token).strip("_")
    return token or "quality_rule"


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
    sev = str(severity or ("error" if conf >= 0.8 else "warning" if conf >= 0.5 else "info"))
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
        "suggested_by": "quality_audit_enrichment",
        "schema_valid": True,
    }


def _mapped_source(mappings: List[Dict[str, Any]], tokens: List[str]) -> str:
    token_set = [str(token).lower() for token in tokens if str(token).strip()]
    for mapping in mappings or []:
        if not isinstance(mapping, dict):
            continue
        source = _clean(mapping.get("source_column"))
        target = _clean(mapping.get("target_field")).lower()
        key = f"{source.lower()} {target}"
        if any(token in key for token in token_set):
            return source
    return ""


def _source_with_profile_fallback(profile: Dict[str, Any], mappings: List[Dict[str, Any]], tokens: List[str]) -> str:
    source = _mapped_source(mappings, tokens)
    if source:
        return source

    token_set = [str(token).lower() for token in tokens if str(token).strip()]
    for column in (profile.get("columns") or []):
        col = _clean(column)
        if not col:
            continue
        key = col.lower().replace("_", " ").replace("-", " ")
        if any(token in key for token in token_set):
            return col
    return ""


def _split_person_name(raw: str) -> tuple[str, str]:
    value = _clean(raw)
    if not value:
        return "", ""
    value = re.sub(r"\s+", " ", value).strip()

    if "," in value:
        parts = [part.strip() for part in value.split(",") if part.strip()]
        if len(parts) >= 2:
            last = parts[0].split(" ")[0].strip().lower()
            first = parts[1].split(" ")[0].strip().lower()
            return first, last

    parts = [part for part in value.split(" ") if part]
    if len(parts) == 1:
        token = parts[0].strip().lower()
        return token, token

    first = parts[0].strip().lower()
    last = parts[-1].strip().lower()
    return first, last


def enrich_quality_audit(
    *,
    profile: Dict[str, Any],
    mappings: List[Dict[str, Any]],
    base_issues: List[Dict[str, Any]],
    settings: Settings,
) -> Dict[str, Any]:
    if not bool(getattr(settings, "quality_audit_enrichment_enabled", False)):
        return {"issues": [], "trace": {"enabled": False, "errors": [], "sources": []}}

    rows_profiled = int(profile.get("rows_profiled", profile.get("row_sample_size", 0)) or 0)
    factory = ReferenceClientFactory(settings)
    nppes = factory.nppes()
    nucc = factory.nucc()
    client_refs = factory.client_refs()
    bq = factory.bq()

    issues: List[Dict[str, Any]] = []
    seen_ids = {str(issue.get("id") or "") for issue in (base_issues or []) if isinstance(issue, dict)}
    errors: List[str] = []
    sources: List[str] = []

    npi_source = _source_with_profile_fallback(profile, mappings, ["npi"])
    taxonomy_source = _source_with_profile_fallback(profile, mappings, ["taxonomy", "specialty"])
    name_source = _source_with_profile_fallback(profile, mappings, ["name", "provider_name", "full_name"])
    first_name_source = _source_with_profile_fallback(profile, mappings, ["first name", "firstname", "given name"])
    last_name_source = _source_with_profile_fallback(profile, mappings, ["last name", "lastname", "surname", "family name"])

    def add_issue(issue: Dict[str, Any]) -> None:
        issue_id = str(issue.get("id") or "")
        if issue_id and issue_id not in seen_ids:
            issues.append(issue)
            seen_ids.add(issue_id)

    npi_values = _sample_values_for_column(profile, npi_source) if npi_source else []

    # ── Fast path: BQ bulk lookup first, NPPES API only for BQ failures ──
    if bq and npi_source and npi_values and hasattr(bq, "bulk_lookup_npis"):
        try:
            unique_npis = list(set(npi_values))
            bq_results = bq.bulk_lookup_npis(unique_npis)

            bq_missing = [npi for npi, r in bq_results.items() if r is None]
            bq_found = {npi: r for npi, r in bq_results.items() if r is not None}

            # Name mismatch check from BQ
            expected_first_names = [v.lower() for v in _sample_values_for_column(profile, first_name_source)] if first_name_source else []
            expected_last_names = [v.lower() for v in _sample_values_for_column(profile, last_name_source)] if last_name_source else []

            name_mismatch_count = 0
            name_mismatch_samples: List[str] = []
            for npi, result in bq_found.items():
                if not expected_first_names or not expected_last_names:
                    break
                bq_first = _clean(result.get("first_name")).lower()
                bq_last = _clean(result.get("last_name")).lower()
                if not bq_first or not bq_last:
                    continue
                has_match = False
                for idx, roster_first in enumerate(expected_first_names):
                    roster_last = expected_last_names[idx] if idx < len(expected_last_names) else ""
                    if roster_first.split()[0] == bq_first.split()[0] and roster_last.split()[0] == bq_last.split()[0]:
                        has_match = True
                        break
                if not has_match:
                    name_mismatch_count += 1
                    if len(name_mismatch_samples) < 6:
                        name_mismatch_samples.append(f"{npi}: roster vs NPPES={bq_first} {bq_last}")

            # For BQ-missing NPIs, try NPPES API (only a few, so fast)
            api_missing = 0
            api_missing_samples: List[str] = []
            if nppes and bq_missing:
                for npi in bq_missing[:20]:  # Cap API calls
                    try:
                        result = nppes.lookup_npi(npi)
                        if not result:
                            api_missing += 1
                            if len(api_missing_samples) < 6:
                                api_missing_samples.append(npi)
                    except Exception:
                        api_missing += 1
                        if len(api_missing_samples) < 6:
                            api_missing_samples.append(npi)

            total_missing = len(bq_missing)
            if api_missing > 0 or total_missing > 0:
                confirmed_missing = api_missing  # Only truly missing = failed both BQ and API
                if confirmed_missing > 0:
                    add_issue(
                        _make_issue(
                            category="external_reference",
                            rule_type="npi_not_found_in_nppes",
                            source_column=npi_source,
                            target_field="npi",
                            title="NPI Not Found in NPPES",
                            message=f"{confirmed_missing} NPI numbers were not found in the national NPPES registry. These providers may have invalid or deactivated NPIs.",
                            confidence=0.90,
                            affected_rows=confirmed_missing,
                            rows_profiled=len(unique_npis),
                            sample_values=api_missing_samples,
                            evidence={"provider": "bq+nppes", "check": "existence", "bq_missing": len(bq_missing), "api_confirmed": api_missing},
                            suggested_fix={"action": "review", "description": "Verify NPI numbers and request corrections from the provider group."},
                            severity="error",
                        )
                    )

            if name_mismatch_count > 0:
                add_issue(
                    _make_issue(
                        category="external_reference",
                        rule_type="npi_first_last_name_mismatch",
                        source_column=first_name_source or last_name_source or npi_source,
                        target_field="providerName",
                        title="Provider Name Mismatch vs NPPES",
                        message=f"{name_mismatch_count} providers have names that don't match the NPPES registry for their NPI. This may indicate data entry errors or name changes.",
                        confidence=0.82,
                        affected_rows=name_mismatch_count,
                        rows_profiled=len(unique_npis),
                        sample_values=name_mismatch_samples,
                        evidence={"provider": "bq_nppes_raw", "check": "name_match"},
                        suggested_fix={"action": "review", "description": "Confirm provider names match NPPES records. Update roster or request name correction."},
                        severity="warning",
                    )
                )

            sources.append("bq_nppes_bulk")
            if nppes and bq_missing:
                sources.append("nppes_api_fallback")

        except Exception as exc:
            errors.append(f"bq_bulk_nppes:{exc}")

    elif nppes and npi_source:
        try:
            missing_npi = 0
            name_mismatch = 0
            first_last_mismatch = 0
            taxonomy_mismatch = 0
            npi_samples: List[str] = []
            taxonomy_samples: List[str] = []
            name_samples: List[str] = []
            first_last_samples: List[str] = []

            expected_names = set(v.lower() for v in _sample_values_for_column(profile, name_source)) if name_source else set()
            expected_taxonomies = set(v.lower() for v in _sample_values_for_column(profile, taxonomy_source)) if taxonomy_source else set()
            expected_first_names = [v.lower() for v in _sample_values_for_column(profile, first_name_source)] if first_name_source else []
            expected_last_names = [v.lower() for v in _sample_values_for_column(profile, last_name_source)] if last_name_source else []

            for npi in npi_values:
                result = nppes.lookup_npi(npi)
                if not result:
                    missing_npi += 1
                    if len(npi_samples) < 6:
                        npi_samples.append(npi)
                    continue

                remote_name = _clean(result.get("provider_name") or result.get("name") or result.get("basic_name"))
                if expected_names and remote_name and remote_name.lower() not in expected_names:
                    name_mismatch += 1
                    if len(name_samples) < 6:
                        name_samples.append(f"{npi}:{remote_name}")

                if remote_name and expected_first_names and expected_last_names:
                    remote_first, remote_last = _split_person_name(remote_name)
                    has_pair_match = False
                    for idx, first in enumerate(expected_first_names):
                        last = expected_last_names[idx] if idx < len(expected_last_names) else ""
                        if not first or not last:
                            continue
                        first_key = _split_person_name(first)[0] if " " in first else first.split(" ")[0].strip().lower()
                        last_key = _split_person_name(last)[1] if " " in last else last.split(" ")[0].strip().lower()
                        if remote_first and remote_last and first_key == remote_first and last_key == remote_last:
                            has_pair_match = True
                            break
                    if not has_pair_match:
                        first_last_mismatch += 1
                        if len(first_last_samples) < 6:
                            first_example = expected_first_names[0] if expected_first_names else ""
                            last_example = expected_last_names[0] if expected_last_names else ""
                            first_last_samples.append(f"{npi}:remote={remote_name}:roster={first_example} {last_example}".strip())

                remote_taxonomy = _clean(result.get("taxonomy") or result.get("primary_taxonomy") or result.get("taxonomy_code"))
                if expected_taxonomies and remote_taxonomy and remote_taxonomy.lower() not in expected_taxonomies:
                    taxonomy_mismatch += 1
                    if len(taxonomy_samples) < 6:
                        taxonomy_samples.append(f"{npi}:{remote_taxonomy}")

            if missing_npi > 0:
                add_issue(
                    _make_issue(
                        category="external_reference",
                        rule_type="npi_not_found_in_nppes",
                        source_column=npi_source,
                        target_field="npi",
                        title="NPI not found in NPPES",
                        message=f"{missing_npi} sampled NPI values were not found in NPPES.",
                        confidence=0.83,
                        affected_rows=missing_npi,
                        rows_profiled=max(len(npi_values), rows_profiled),
                        sample_values=npi_samples,
                        evidence={"provider": "nppes", "check": "existence_only"},
                        suggested_fix={
                            "action": "review",
                            "description": "Validate NPI values and correct invalid NPIs before export.",
                        },
                        severity="warning",
                    )
                )

            if name_mismatch > 0:
                add_issue(
                    _make_issue(
                        category="external_reference",
                        rule_type="npi_name_mismatch",
                        source_column=name_source or npi_source,
                        target_field="providerName",
                        title="Provider name mismatch vs NPPES",
                        message=f"{name_mismatch} sampled rows had provider names that differ from NPPES response.",
                        confidence=0.75,
                        affected_rows=name_mismatch,
                        rows_profiled=max(len(npi_values), rows_profiled),
                        sample_values=name_samples,
                        evidence={"provider": "nppes", "check": "name_match"},
                        suggested_fix={
                            "action": "review",
                            "description": "Confirm legal/provider names and align records with NPPES where appropriate.",
                        },
                        severity="info",
                    )
                )

            if taxonomy_mismatch > 0:
                add_issue(
                    _make_issue(
                        category="external_reference",
                        rule_type="taxonomy_mismatch_nppes",
                        source_column=taxonomy_source or npi_source,
                        target_field="taxonomy",
                        title="Taxonomy mismatch vs NPPES",
                        message=f"{taxonomy_mismatch} sampled taxonomy values differ from NPPES.",
                        confidence=0.77,
                        affected_rows=taxonomy_mismatch,
                        rows_profiled=max(len(npi_values), rows_profiled),
                        sample_values=taxonomy_samples,
                        evidence={"provider": "nppes", "check": "taxonomy_match"},
                        suggested_fix={
                            "action": "review",
                            "description": "Verify taxonomy coding against NPPES and resolve inconsistencies.",
                        },
                        severity="warning",
                    )
                )

            if first_last_mismatch > 0:
                add_issue(
                    _make_issue(
                        category="external_reference",
                        rule_type="npi_first_last_name_mismatch",
                        source_column=first_name_source or last_name_source or name_source or npi_source,
                        target_field="providerName",
                        title="First/last name mismatch vs NPPES",
                        message=f"{first_last_mismatch} sampled rows had first/last name combinations that differ from NPPES.",
                        confidence=0.79,
                        affected_rows=first_last_mismatch,
                        rows_profiled=max(len(npi_values), rows_profiled),
                        sample_values=first_last_samples,
                        evidence={
                            "provider": "nppes",
                            "check": "first_last_pair_match",
                            "first_name_column": first_name_source,
                            "last_name_column": last_name_source,
                        },
                        suggested_fix={
                            "action": "review",
                            "description": "Review first/last name pair values and align with NPPES provider identity.",
                        },
                        severity="warning",
                    )
                )

            sources.append("nppes")
        except Exception as exc:
            errors.append(f"nppes:{exc}")

    if nucc and taxonomy_source:
        try:
            taxonomy_values = _sample_values_for_column(profile, taxonomy_source)
            invalid_taxonomy = 0
            invalid_samples: List[str] = []
            for code in taxonomy_values:
                result = nucc.lookup_taxonomy(code)
                if not result:
                    invalid_taxonomy += 1
                    if len(invalid_samples) < 6:
                        invalid_samples.append(code)

            if invalid_taxonomy > 0:
                add_issue(
                    _make_issue(
                        category="external_reference",
                        rule_type="taxonomy_not_valid_nucc",
                        source_column=taxonomy_source,
                        target_field="taxonomy",
                        title="Taxonomy not found in NUCC",
                        message=f"{invalid_taxonomy} sampled taxonomy values were not found in NUCC reference.",
                        confidence=0.82,
                        affected_rows=invalid_taxonomy,
                        rows_profiled=max(len(taxonomy_values), rows_profiled),
                        sample_values=invalid_samples,
                        evidence={"provider": "nucc", "check": "existence_only"},
                        suggested_fix={
                            "action": "review",
                            "description": "Validate taxonomy codes against NUCC and correct invalid codes.",
                        },
                        severity="warning",
                    )
                )

            sources.append("nucc")
        except Exception as exc:
            errors.append(f"nucc:{exc}")

    if client_refs and npi_source:
        try:
            mismatches = 0
            mismatch_samples: List[str] = []
            for npi in npi_values:
                result = client_refs.lookup_npi(npi)
                if not result:
                    continue
                client_name = _clean(result.get("provider_name") or result.get("name"))
                if not client_name:
                    continue
                if name_source:
                    expected_names = {v.lower() for v in _sample_values_for_column(profile, name_source)}
                    if expected_names and client_name.lower() not in expected_names:
                        mismatches += 1
                        if len(mismatch_samples) < 6:
                            mismatch_samples.append(f"{npi}:{client_name}")

            if mismatches > 0:
                add_issue(
                    _make_issue(
                        category="external_reference",
                        rule_type="client_ref_name_mismatch",
                        source_column=name_source or npi_source,
                        target_field="providerName",
                        title="Provider mismatch vs client reference",
                        message=f"{mismatches} sampled providers differ from client reference data.",
                        confidence=0.72,
                        affected_rows=mismatches,
                        rows_profiled=max(len(npi_values), rows_profiled),
                        sample_values=mismatch_samples,
                        evidence={"provider": "client_refs", "check": "name_match"},
                        suggested_fix={
                            "action": "review",
                            "description": "Confirm provider naming against approved client reference records.",
                        },
                        severity="info",
                    )
                )

            sources.append("client_refs")
        except Exception as exc:
            errors.append(f"client_refs:{exc}")

    if bq and npi_source:
        try:
            missing_in_bq = 0
            mismatch_in_bq = 0
            missing_samples: List[str] = []
            mismatch_samples: List[str] = []
            expected_taxonomies = {v.lower() for v in _sample_values_for_column(profile, taxonomy_source)} if taxonomy_source else set()

            for npi in npi_values:
                result = bq.lookup_npi(npi)
                if not result:
                    missing_in_bq += 1
                    if len(missing_samples) < 6:
                        missing_samples.append(npi)
                    continue
                remote_taxonomy = _clean(result.get("taxonomy") or result.get("primary_taxonomy"))
                if expected_taxonomies and remote_taxonomy and remote_taxonomy.lower() not in expected_taxonomies:
                    mismatch_in_bq += 1
                    if len(mismatch_samples) < 6:
                        mismatch_samples.append(f"{npi}:{remote_taxonomy}")

            if missing_in_bq > 0:
                add_issue(
                    _make_issue(
                        category="external_reference",
                        rule_type="npi_not_found_in_bq",
                        source_column=npi_source,
                        target_field="npi",
                        title="NPI missing in BigQuery reference",
                        message=f"{missing_in_bq} sampled NPIs were not found in configured BigQuery reference dataset.",
                        confidence=0.78,
                        affected_rows=missing_in_bq,
                        rows_profiled=max(len(npi_values), rows_profiled),
                        sample_values=missing_samples,
                        evidence={"provider": "bq", "check": "existence_only"},
                        suggested_fix={
                            "action": "review",
                            "description": "Validate NPIs and sync missing records into reference dataset if needed.",
                        },
                        severity="warning",
                    )
                )

            if mismatch_in_bq > 0:
                add_issue(
                    _make_issue(
                        category="external_reference",
                        rule_type="taxonomy_mismatch_bq",
                        source_column=taxonomy_source or npi_source,
                        target_field="taxonomy",
                        title="Taxonomy mismatch vs BigQuery reference",
                        message=f"{mismatch_in_bq} sampled taxonomy values differ from BigQuery reference.",
                        confidence=0.76,
                        affected_rows=mismatch_in_bq,
                        rows_profiled=max(len(npi_values), rows_profiled),
                        sample_values=mismatch_samples,
                        evidence={"provider": "bq", "check": "taxonomy_match"},
                        suggested_fix={
                            "action": "review",
                            "description": "Review taxonomy discrepancies between roster and BigQuery reference.",
                        },
                        severity="warning",
                    )
                )

            sources.append("bq")
        except Exception as exc:
            errors.append(f"bq:{exc}")

    trace = {
        "enabled": True,
        "sources": sorted(set(sources)),
        "errors": errors,
        "issue_count": len(issues),
        "fail_open": True,
    }
    return {"issues": issues, "trace": trace}
