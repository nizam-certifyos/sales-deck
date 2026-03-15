"""Load and compile domain knowledge into system prompts for Gemini."""

from __future__ import annotations

import csv
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional


_KB_DIR = Path(__file__).resolve().parent.parent.parent.parent / "knowledge_base"


def _kb_path(name: str) -> Path:
    override = os.getenv("UR2_KNOWLEDGE_BASE_DIR", "").strip()
    base = Path(override) if override else _KB_DIR
    return base / name


def _read_csv(name: str) -> List[Dict[str, str]]:
    p = _kb_path(name)
    if not p.exists():
        return []
    with open(p, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _read_json(name: str) -> dict:
    p = _kb_path(name)
    if not p.exists():
        return {}
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def _read_text(name: str) -> str:
    p = _kb_path(name)
    if not p.exists():
        return ""
    return p.read_text(encoding="utf-8")


@lru_cache(maxsize=1)
def load_field_ontology() -> str:
    rows = _read_csv("01_field_ontology.csv")
    if not rows:
        return ""
    lines = ["## FIELD ONTOLOGY (ALL VALID TARGET FIELDS)\n"]
    for r in rows:
        tf = r.get("target_field", "")
        scope = r.get("entity_scope", "")
        pos = r.get("positive_cues", "")
        neg = r.get("negative_cues", "")
        # Slim: field name + scope + cues only (no business_definition to save tokens)
        parts = [f"- **{tf}** ({scope})"]
        if pos:
            parts.append(f" +[{pos}]")
        if neg:
            parts.append(f" -[{neg}]")
        lines.append("".join(parts))
    return "\n".join(lines)


@lru_cache(maxsize=1)
def load_header_synonyms() -> str:
    rows = _read_csv("02_header_synonyms.csv")
    if not rows:
        return ""
    by_target: Dict[str, List[str]] = {}
    for r in rows:
        tf = r.get("target_field", "")
        hv = r.get("header_variant", "")
        if tf and hv:
            by_target.setdefault(tf, []).append(hv)
    lines = ["## HEADER SYNONYM DICTIONARY\n"]
    for tf, variants in sorted(by_target.items()):
        # Limit to 3 variants per field to reduce prompt size
        shown = variants[:3]
        lines.append(f"- **{tf}**: {', '.join(repr(v) for v in shown)}")
    return "\n".join(lines)


@lru_cache(maxsize=1)
def load_disambiguation_rules() -> str:
    rows = _read_csv("03_disambiguation_rules.csv")
    if not rows:
        return ""
    lines = ["## DISAMBIGUATION RULES (117 tie-break rules)\n"]
    for r in rows:
        rid = r.get("rule_id", "")
        trigger = r.get("trigger_pattern", "")
        candidates = r.get("candidate_set", "")
        select = r.get("select_target_if", "")
        reject = r.get("reject_target_if", "")
        pri = r.get("priority", "")
        lines.append(f"- **{rid}** (priority={pri}): trigger=\"{trigger}\"")
        lines.append(f"  candidates: {candidates}")
        lines.append(f"  SELECT: {select}")
        if reject:
            lines.append(f"  REJECT: {reject}")
    return "\n".join(lines)


@lru_cache(maxsize=1)
def load_anti_patterns() -> str:
    rows = _read_csv("05_anti_patterns.csv")
    if not rows:
        return ""
    lines = ["## ANTI-PATTERNS (FORBIDDEN MAPPINGS)\n"]
    for r in rows:
        src = r.get("source_pattern", "")
        forbid = r.get("forbid_target", "")
        reason = r.get("reason", "")
        lines.append(f"- \"{src}\" must NEVER map to **{forbid}**: {reason}")
    return "\n".join(lines)


@lru_cache(maxsize=1)
def load_must_not_mis_map() -> str:
    rows = _read_csv("06_must_not_mis_map.csv")
    if not rows:
        return ""
    lines = ["## CRITICAL FIELDS — MUST NOT MIS-MAP\n"]
    for r in rows:
        tf = r.get("target_field", "")
        crit = r.get("criticality", "")
        val = r.get("validation_expectation", "")
        lines.append(f"- **{tf}** [{crit}]: {val}")
    return "\n".join(lines)


@lru_cache(maxsize=1)
def load_transforms_catalog() -> str:
    data = _read_json("transforms_catalog.json")
    if not data:
        return ""
    return "## TRANSFORMS CATALOG\n\n" + json.dumps(data, indent=2)


@lru_cache(maxsize=1)
def load_bq_validations_catalog() -> str:
    data = _read_json("bq_validations_catalog.json")
    if not data:
        return ""
    return "## BQ VALIDATIONS CATALOG\n\n" + json.dumps(data, indent=2)


@lru_cache(maxsize=1)
def load_schema_field_rules() -> str:
    data = _read_json("schema_field_rules.json")
    if not data:
        return ""
    return "## SCHEMA FIELD RULES\n\n" + json.dumps(data, indent=2)


@lru_cache(maxsize=1)
def load_action_type_rules() -> str:
    data = _read_json("action_type_validation_rules.json")
    if not data:
        return ""
    return "## ACTION TYPE VALIDATION RULES\n\n" + json.dumps(data, indent=2)


@lru_cache(maxsize=1)
def load_transaction_type_logic() -> str:
    data = _read_json("transaction_type_business_logic.json")
    if not data:
        return ""
    return "## TRANSACTION TYPE BUSINESS LOGIC\n\n" + json.dumps(data, indent=2)


@lru_cache(maxsize=1)
def load_platform_accepted_values() -> str:
    data = _read_json("platform_accepted_values.json")
    if not data:
        return ""
    return "## PLATFORM ACCEPTED VALUES\n\n" + json.dumps(data, indent=2)


@lru_cache(maxsize=1)
def load_healthcare_domain() -> str:
    return _read_text("healthcare_domain_knowledge.md")


@lru_cache(maxsize=1)
def load_system_business_logic() -> str:
    return _read_text("system_business_logic.md")


@lru_cache(maxsize=1)
def build_mapping_system_prompt() -> str:
    """Deep system prompt for mapping tasks — includes full ontology and synonym dictionary."""
    parts = [
        "# CertifyOS Healthcare Roster Mapping Expert\n",
        "You are a senior healthcare credentialing data analyst specializing in provider roster column mapping.",
        "You have deep expertise in CertifyOS schema, HIPAA identifiers, and healthcare data engineering.",
        "You MUST map roster column headers to the EXACT target fields listed in the ontology below.",
        "Return ONLY valid JSON. Never invent target fields not in the ontology.\n",
        load_field_ontology(),
        "\n",
        load_header_synonyms(),
        "\n",
        load_disambiguation_rules(),
        "\n",
        load_anti_patterns(),
        "\n",
        load_must_not_mis_map(),
        "\n",
        "## KEY RULES:\n"
        "1. Strip vendor prefixes (mpac_, prov_, sys_, pdm_) before matching.\n"
        "2. Expand abbreviations: nm=name, dt=date, addr=address, lic=license, cert=certificate.\n"
        "3. Context matters: practitioner roster → practitioner fields, facility roster → facility fields.\n"
        "4. Phone/Fax NEVER cross-map. Practice ≠ Billing ≠ Mailing addresses.\n"
        "5. providerType (MD/DO) ≠ practitionerRole (PCP/Specialist).\n"
        "6. State prefix in header (e.g. 'DC LICENSE') indicates the ISSUING STATE.\n"
        "7. Always check disambiguation rules for ambiguous headers.\n"
        "8. Check anti-patterns before committing a mapping.\n"
    ]
    return "\n".join(parts)


@lru_cache(maxsize=1)
def build_transform_system_prompt() -> str:
    """Deep system prompt for transformation suggestion tasks — focused on transform catalog."""
    parts = [
        "# CertifyOS Transformation Expert\n",
        "You are a senior data engineer specializing in healthcare roster data cleaning and transformation.",
        "You suggest data transformations to normalize, clean, and validate provider roster data for CertifyOS ingestion.",
        "Return ONLY valid JSON.\n",
        load_transforms_catalog(),
        "\n",
        "## KEY RULES:\n"
        "1. Always apply universal transforms (null normalization, date cleaning, whitespace).\n"
        "2. Detect date formats and apply clean_date_series to normalize to YYYY-MM-DD.\n"
        "3. NPI must be 10 digits, TIN 9 digits, Phone 10 digits.\n"
        "4. State names → 2-letter codes. Gender → M/F/U.\n"
        "5. Excel scientific notation (1.23E+10) must be detected and fixed.\n"
        "6. Valid transform names: normalize_npi, normalize_tin, normalize_ssn, normalize_phone, normalize_zip, normalize_state, normalize_enum, normalize_date, split_hours, split_multivalue, review.\n"
    ]
    return "\n".join(parts)


@lru_cache(maxsize=1)
def build_validation_system_prompt() -> str:
    """Deep system prompt for BQ validation rule generation — focused on validation catalog."""
    parts = [
        "# CertifyOS BigQuery Validation Expert\n",
        "You generate production-ready BigQuery SQL validation rules for healthcare roster data.",
        "Return ONLY valid JSON.\n",
        load_bq_validations_catalog(),
        "\n",
        load_action_type_rules(),
        "\n",
        "## KEY RULES:\n"
        "1. Each validation returns NULL (pass) or error message string (fail).\n"
        "2. NPI must be exactly 10 digits. TIN exactly 9 digits.\n"
        "3. Dates validated via SAFE_CAST to DATE, range >= 1901-01-01.\n"
        "4. NPPES validation: LEFT JOIN nppes_raw ON NPI.\n"
        "5. Required fields differ by transaction type (see action_type_validation_rules).\n"
        "6. Allowed rule_type values: required, pattern, enum, format, custom.\n"
        "7. severity must be one of: error, warning.\n"
    ]
    return "\n".join(parts)


@lru_cache(maxsize=1)
def build_quality_audit_system_prompt() -> str:
    """Deep system prompt for quality audit tasks — focused on rules, not full domain."""
    parts = [
        "# CertifyOS Quality Audit Expert\n",
        "You are a senior healthcare roster quality auditor.",
        "You detect data quality issues, compliance violations, and business rule breaches in provider roster data.",
        "Return ONLY valid JSON.\n",
        load_anti_patterns(),
        "\n",
        load_must_not_mis_map(),
        "\n",
        load_action_type_rules(),
        "\n",
        load_transaction_type_logic(),
        "\n",
        "## KEY RULES:\n"
        "1. NPI: 10 digits, Luhn check digit. Validate via NPPES.\n"
        "2. TIN: 9 digits. SSN: 9 digits (never 000, 666, 900+).\n"
        "3. Phone/Fax cross-mapping is a BLOCKING error.\n"
        "4. Missing required fields per transaction type = error.\n"
        "5. Date range violations (before 1901, after 2099) = warning.\n"
        "6. Duplicate rows within file = warning.\n"
        "7. providerType (MD/DO) ≠ practitionerRole (PCP/Specialist).\n"
    ]
    return "\n".join(parts)


@lru_cache(maxsize=1)
def build_codegen_system_prompt() -> str:
    """Deep system prompt for code generation tasks."""
    parts = [
        "# CertifyOS Code Generator\n",
        "You generate production-ready Python and SQL code for healthcare roster data processing pipelines.",
        "You output clean, well-documented code with proper error handling.\n",
        load_transforms_catalog(),
        "\n",
        load_bq_validations_catalog(),
        "\n",
        load_system_business_logic(),
        "\n",
        "## KEY RULES:\n"
        "1. Use pandas for data processing. BigQuery client for validation.\n"
        "2. Apply transforms in correct order (null → clean → normalize → validate).\n"
        "3. Generate idempotent SQL (MERGE/UPSERT patterns).\n"
        "4. Include logging and error tracking in all generated code.\n"
    ]
    return "\n".join(parts)
