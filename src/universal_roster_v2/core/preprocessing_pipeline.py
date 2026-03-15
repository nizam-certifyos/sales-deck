"""Full preprocessing pipeline for sales demo — SCHEMA-DRIVEN using schema_field_rules.json.

All transforms and validations are driven by the schema field rules, not by
hard-coded field-type classification. When a source column is mapped to a target
schema field, we look up the rules for that field and apply:
  - transforms from `transforms_needed`
  - validations from `validations_needed`, `pattern`, `enum_values`, `format`
  - column naming from `title`
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# ── Schema field rules cache ──
_SCHEMA_RULES_CACHE: Optional[Dict[str, Any]] = None
_FIELD_LOOKUP_CACHE: Optional[Dict[str, Dict[str, Any]]] = None


def _load_schema_rules() -> Dict[str, Any]:
    """Load schema_field_rules.json and return the raw dict."""
    global _SCHEMA_RULES_CACHE
    if _SCHEMA_RULES_CACHE is not None:
        return _SCHEMA_RULES_CACHE
    kb_dir = os.getenv("UR2_KNOWLEDGE_BASE_DIR", "").strip()
    if not kb_dir:
        kb_dir = str(Path(__file__).resolve().parent.parent.parent.parent / "knowledge_base")
    p = Path(kb_dir) / "schema_field_rules.json"
    if p.exists():
        with open(p, encoding="utf-8") as f:
            _SCHEMA_RULES_CACHE = json.load(f)
    else:
        _SCHEMA_RULES_CACHE = {}
    return _SCHEMA_RULES_CACHE


def _build_field_lookup() -> Dict[str, Dict[str, Any]]:
    """Build a flat lookup: schema_field_name -> field_rules_dict.

    Merges fields from all top-level sections (practitioner_fields, facility_fields, etc.)
    """
    global _FIELD_LOOKUP_CACHE
    if _FIELD_LOOKUP_CACHE is not None:
        return _FIELD_LOOKUP_CACHE
    raw = _load_schema_rules()
    lookup: Dict[str, Dict[str, Any]] = {}
    for key, val in raw.items():
        if key.startswith("_"):
            continue
        if isinstance(val, dict):
            # Determine if this is a section (containing field definitions) or a single field.
            # A section is a dict where most/all values are themselves dicts with 'title'.
            sub_dicts_with_title = sum(
                1 for v in val.values()
                if isinstance(v, dict) and "title" in v
            )
            if sub_dicts_with_title > 5:
                # This is a section containing many field definitions
                for field_name, field_def in val.items():
                    if isinstance(field_def, dict) and "title" in field_def:
                        if field_name not in lookup:
                            lookup[field_name] = field_def
            elif "title" in val and "type" in val:
                # This is a standalone field definition
                lookup[key] = val
    _FIELD_LOOKUP_CACHE = lookup
    return _FIELD_LOOKUP_CACHE


# ── Null tokens ──
NULL_TOKENS = frozenset({
    "", "nan", "n/a", "na", "none", "#n/a", "null", "empty", "n.a.", "-",
    "pending", "not provided", "nat", "tbd", "not applicable", "unknown",
})

# ── Scientific notation regex ──
_SCI_RE = re.compile(r"^[+-]?\d+(?:\.\d+)?[eE][+-]?\d+$")
_TRAILING_ZERO_RE = re.compile(r"^\d+\.0+$")

# ── Validation patterns ──
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# ── Gender map ──
GENDER_MAP = {
    "male": "M", "female": "F", "unknown": "U", "m": "M", "f": "F", "u": "U",
    "not listed": "U", "not listed/unknown": "U", "other": "U",
}

# ── State map ──
US_STATE_MAP = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI",
    "wyoming": "WY", "district of columbia": "DC", "puerto rico": "PR", "guam": "GU",
    "virgin islands": "VI", "american samoa": "AS",
}

# ── Accepts new patients normalization map ──
_ACCEPTS_PATIENTS_MAP = {
    "y": "Accepting New",
    "yes": "Accepting New",
    "true": "Accepting New",
    "1": "Accepting New",
    "accepting": "Accepting New",
    "accepting new": "Accepting New",
    "n": "Closed",
    "no": "Closed",
    "false": "Closed",
    "0": "Closed",
    "closed": "Closed",
    "existing": "Existing Patients Only",
    "existing patients only": "Existing Patients Only",
    "existing only": "Existing Patients Only",
    "telemedicine": "Telemedicine",
    "urgent care only": "Urgent Care Only",
}

# ── Office hours day parsing ──
_DAY_ABBREVS = {
    "mon": "Mon", "tue": "Tue", "wed": "Wed", "thu": "Thu",
    "fri": "Fri", "sat": "Sat", "sun": "Sun",
    "monday": "Mon", "tuesday": "Tue", "wednesday": "Wed",
    "thursday": "Thu", "friday": "Fri", "saturday": "Sat", "sunday": "Sun",
}

_DAY_SCHEMA_FIELDS = {
    "Mon": ("practitionerOpenTimeMon", "practitionerCloseTimeMon"),
    "Tue": ("practitionerOpenTimeTue", "practitionerCloseTimeTue"),
    "Wed": ("practitionerOpenTimeWed", "practitionerCloseTimeWed"),
    "Thu": ("practitionerOpenTimeThu", "practitionerCloseTimeThu"),
    "Fri": ("practitionerOpenTimeFri", "practitionerCloseTimeFri"),
    "Sat": ("practitionerOpenTimeSat", "practitionerCloseTimeSat"),
    "Sun": ("practitionerOpenTimeSun", "practitionerCloseTimeSun"),
}

_ORDERED_DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _is_null(val: Any) -> bool:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return True
    return str(val).strip().lower() in NULL_TOKENS


def _clean_null(val: Any) -> str:
    if _is_null(val):
        return ""
    return str(val).strip()


def _fix_scientific(text: str) -> str:
    if not text or not _SCI_RE.match(text):
        return text
    try:
        dec = Decimal(text)
        if dec % 1 == 0:
            return format(dec.quantize(Decimal("1")), "f")
        return format(dec, "f").rstrip("0").rstrip(".")
    except InvalidOperation:
        return text


def _strip_trailing_decimal(text: str) -> str:
    if _TRAILING_ZERO_RE.match(text):
        return text.split(".")[0]
    return text


# ── Transform functions ──

def _normalize_date(text: str) -> Tuple[str, str]:
    """Returns (normalized_value, warning_or_empty)."""
    if not text:
        return "", ""
    try:
        dt = pd.to_datetime(text, errors="coerce", format="mixed", dayfirst=False)
        if pd.isna(dt):
            return text, ""
        if dt.year < 1901 or dt.year > 2099:
            return "", f"Date out of range: {text}"
        result = dt.strftime("%Y-%m-%d")
        if result != text:
            return result, f"{text} -> {result}"
        return result, ""
    except Exception:
        return text, ""


def _normalize_digits(text: str, length: int) -> Tuple[str, str]:
    if not text:
        return "", ""
    digits = re.sub(r"\D", "", text)
    if not digits:
        return "", ""
    if len(digits) < length:
        padded = digits.zfill(length)
        if len(padded) == length:
            return padded, f"{text} -> {padded} (zero-padded)"
        return "", f"Invalid length for {text}"
    if len(digits) > length:
        truncated = digits[:length]
        return truncated, f"{text} -> {truncated} (truncated)"
    if digits != text:
        return digits, f"{text} -> {digits}"
    return digits, ""


def _normalize_phone(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""
    digits = re.sub(r"\D", "", text)
    if digits.startswith("1") and len(digits) == 11:
        digits = digits[1:]
    if len(digits) == 10:
        if digits != text:
            return digits, f"{text} -> {digits}"
        return digits, ""
    return text, ""


def _normalize_state(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""
    if len(text) == 2 and text.upper().isalpha():
        upper = text.upper()
        if upper != text:
            return upper, f"{text} -> {upper}"
        return upper, ""
    mapped = US_STATE_MAP.get(text.lower())
    if mapped:
        return mapped, f"{text} -> {mapped}"
    return text, ""


def _normalize_gender(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""
    mapped = GENDER_MAP.get(text.lower())
    if mapped:
        if mapped != text:
            return mapped, f"Gender {text} -> {mapped}"
        return mapped, ""
    return text, ""


def _normalize_zip(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""
    digits = re.sub(r"\D", "", text)
    if len(digits) == 5:
        if digits != text:
            return digits, f"{text} -> {digits}"
        return digits, ""
    if len(digits) == 9:
        if digits != text:
            return digits, f"{text} -> {digits}"
        return digits, ""
    if len(digits) == 4:
        padded = "0" + digits
        return padded, f"{text} -> {padded} (zero-padded)"
    return text, ""


def _parse_time(text: str) -> Optional[str]:
    """Parse a time string like '8am', '5pm', '08:00', '5:00 PM' into HH:MM format."""
    if not text:
        return None
    text = text.strip().lower()
    # Already HH:MM
    m = re.match(r"^(\d{1,2}):(\d{2})\s*(am|pm)?$", text)
    if m:
        h, mi, ampm = int(m.group(1)), int(m.group(2)), m.group(3)
        if ampm == "pm" and h < 12:
            h += 12
        elif ampm == "am" and h == 12:
            h = 0
        return f"{h:02d}:{mi:02d}"
    # Just hour + am/pm: "8am", "5pm"
    m = re.match(r"^(\d{1,2})\s*(am|pm)$", text)
    if m:
        h, ampm = int(m.group(1)), m.group(2)
        if ampm == "pm" and h < 12:
            h += 12
        elif ampm == "am" and h == 12:
            h = 0
        return f"{h:02d}:00"
    return None


def _parse_day_range(text: str) -> List[str]:
    """Parse day specification like 'Mon-Fri', 'Monday - Friday', 'Mon,Tue,Wed' into list of day abbreviations."""
    if not text:
        return []
    text = text.strip().lower()
    # Handle range like "mon-fri"
    m = re.match(r"^(\w+)\s*[-–]\s*(\w+)$", text)
    if m:
        start_raw, end_raw = m.group(1), m.group(2)
        start = _DAY_ABBREVS.get(start_raw)
        end = _DAY_ABBREVS.get(end_raw)
        if start and end:
            si = _ORDERED_DAYS.index(start)
            ei = _ORDERED_DAYS.index(end)
            if si <= ei:
                return _ORDERED_DAYS[si:ei + 1]
            else:
                # wrap around (e.g. Fri-Mon)
                return _ORDERED_DAYS[si:] + _ORDERED_DAYS[:ei + 1]
    # Handle comma-separated
    parts = re.split(r"[,;/\s]+", text)
    days = []
    for p in parts:
        p = p.strip()
        d = _DAY_ABBREVS.get(p)
        if d:
            days.append(d)
    return days


def _split_malpractice_amounts(text: str) -> Tuple[str, str, str]:
    """Split '$1,000,000   $3,000,000' into (individual, aggregate, warning).

    Returns (individual_amount, aggregate_amount, warning).
    """
    if not text:
        return "", "", ""
    # First try splitting on multiple spaces
    parts = re.split(r"\s{2,}", text.strip())
    if len(parts) < 2:
        # Try splitting on $ signs: "$1,000,000 $3,000,000" or "$1,000,000/$3,000,000"
        parts = re.split(r"(?<=\d)\s*[/]\s*(?=\$?)|(?<=\d)\s+(?=\$)", text.strip())
    if len(parts) < 2:
        # Try finding all dollar amounts
        amounts = re.findall(r"\$?[\d,]+(?:\.\d+)?", text)
        if len(amounts) >= 2:
            parts = amounts

    cleaned_parts = []
    for part in parts:
        part = part.strip()
        if part:
            cleaned = re.sub(r"[$,]", "", part).strip()
            if cleaned and re.match(r"^\d+(?:\.\d+)?$", cleaned):
                cleaned_parts.append(cleaned)
    if len(cleaned_parts) >= 2:
        return cleaned_parts[0], cleaned_parts[1], f"Malpractice split: {text} -> individual={cleaned_parts[0]}, aggregate={cleaned_parts[1]}"
    elif len(cleaned_parts) == 1:
        return "", cleaned_parts[0], ""
    return "", "", ""


def _apply_schema_transform(val: str, field_name: str, rules: Dict[str, Any]) -> Tuple[str, str]:
    """Apply transforms based on schema rules. Returns (new_val, warning)."""
    if not val:
        return "", ""

    transforms = rules.get("transforms_needed", [])
    fmt = rules.get("format")

    # Date transform
    if fmt == "date" or "clean_date_series" in transforms:
        return _normalize_date(val)

    # Digit normalization: normalize_id_digits(N)
    for t in transforms:
        m = re.match(r"normalize_id_digits\((\d+)\)", t)
        if m:
            length = int(m.group(1))
            return _normalize_digits(val, length)

    # State normalization
    if "normalize_state" in transforms:
        # Don't normalize state for license numbers (they are free text)
        if "licenseNumber" in field_name or field_name == "stateLicenseNumber":
            return val, ""
        return _normalize_state(val)

    # Gender normalization
    if "normalize_gender" in transforms:
        return _normalize_gender(val)

    # ── Fallback transforms by field name when schema doesn't specify ──
    fn_lower = field_name.lower()

    # Date fallback
    if not transforms and not fmt:
        if any(kw in fn_lower for kw in ("date", "dob", "expir", "effective", "appointment")):
            return _normalize_date(val)

    # Phone/Fax fallback — strip to 10 digits
    if not transforms:
        if any(kw in fn_lower for kw in ("phone", "fax", "tty", "telephone")):
            return _normalize_phone(val)

    # TIN fallback — strip to 9 digits
    if not transforms:
        if "tin" in fn_lower or "taxid" in fn_lower or "taxidentification" in fn_lower:
            return _normalize_digits(val, 9)

    # NPI fallback — strip to 10 digits
    if not transforms:
        if "npi" in fn_lower and "group" not in fn_lower:
            return _normalize_digits(val, 10)

    # ZIP fallback
    if not transforms:
        if "zip" in fn_lower or "postal" in fn_lower:
            return _normalize_zip(val)

    # State fallback
    if not transforms:
        if "state" in fn_lower and "license" not in fn_lower and "number" not in fn_lower:
            return _normalize_state(val)

    # Gender fallback
    if not transforms:
        if fn_lower == "gender":
            return _normalize_gender(val)

    return val, ""


def _nppes_api_bulk_lookup(npis: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
    """Fallback: look up NPIs via the public NPPES API. Fast for small batches."""
    import urllib.request
    import json as _json

    results: Dict[str, Optional[Dict[str, Any]]] = {}
    for npi in npis[:50]:  # Cap at 50 to keep it fast
        try:
            url = f"https://npiregistry.cms.hhs.gov/api/?number={npi}&version=2.1&limit=1"
            req = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = _json.loads(resp.read().decode("utf-8"))
            if data.get("result_count", 0) > 0 and data.get("results"):
                r = data["results"][0]
                basic = r.get("basic", {})
                results[npi] = {
                    "npi": npi,
                    "first_name": str(basic.get("first_name") or "").strip(),
                    "last_name": str(basic.get("last_name") or basic.get("organization_name") or "").strip(),
                    "provider_name": f"{basic.get('first_name', '')} {basic.get('last_name', '')}".strip(),
                    "source": "nppes_api",
                }
            else:
                results[npi] = None
        except Exception:
            results[npi] = None  # API failure = unknown
    return results


class PreprocessingPipeline:
    """Full preprocessing pipeline — SCHEMA-DRIVEN.

    Takes the raw DataFrame, approved mappings, and optional NPPES cache.
    Returns the processed DataFrame with Warnings and Business_Validations columns.
    """

    def __init__(
        self,
        *,
        mappings: List[Dict[str, Any]],
        nppes_cache: Optional[Dict[str, Optional[Dict[str, Any]]]] = None,
    ):
        self.mappings = mappings
        self.nppes_cache = nppes_cache or {}
        self._field_lookup = _build_field_lookup()

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Run the full pipeline. Returns processed DataFrame."""
        df = df.copy()
        df.columns = [c.strip() for c in df.columns]

        # Build source->target map from approved mappings
        mapping_dict: Dict[str, str] = {}
        for m in self.mappings:
            if m.get("approved") is False:
                continue
            src = str(m.get("source_column") or "").strip()
            tgt = str(m.get("target_field") or "").strip()
            if src and tgt and src in df.columns:
                mapping_dict[src] = tgt

        # Auto-populate NPPES cache via API if empty
        if not self.nppes_cache:
            npi_col = None
            for src, tgt in mapping_dict.items():
                if "npi" in tgt.lower() and "group" not in tgt.lower():
                    npi_col = src
                    break
            if npi_col and npi_col in df.columns:
                unique_npis = list(set(v.strip() for v in df[npi_col].dropna().astype(str) if v.strip()))
                if unique_npis:
                    self.nppes_cache = _nppes_api_bulk_lookup(unique_npis)

        # Stage 1: Universal Cleanup
        df = self._stage1_cleanup(df)

        # Initialize per-row tracking
        n = len(df)
        warnings: List[List[str]] = [[] for _ in range(n)]
        validations: List[List[str]] = [[] for _ in range(n)]

        # Stage 2: Schema-driven transforms
        df = self._stage2_transforms(df, mapping_dict, warnings)

        # Stage 3: Special splits (office hours, malpractice)
        extra_cols = self._stage3_special_splits(df, mapping_dict, warnings)

        # Stage 4: Schema-driven validations
        self._stage4_validations(df, mapping_dict, validations, warnings, extra_cols)

        # Build output using schema titles as column names
        output = self._build_output(df, mapping_dict, extra_cols, warnings, validations)

        return output

    def _stage1_cleanup(self, df: pd.DataFrame) -> pd.DataFrame:
        """Stage 1: Universal cleanup — remove empty cols/rows, null normalization, whitespace.

        Vectorized implementation using pandas string operations.
        """
        df = df.copy()

        # Remove unnamed empty columns
        cols_to_drop = [
            c for c in df.columns
            if (not c.strip() or c.strip().startswith("Unnamed:"))
            and not df[c].astype(str).str.strip().any()
        ]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)

        # Vectorized null normalization + whitespace cleanup
        for col in df.columns:
            s = df[col].astype(str).str.strip()
            # Null normalization — replace known null tokens with empty
            s = s.where(~s.str.lower().isin(NULL_TOKENS), "")
            # Whitespace cleanup
            s = s.str.replace(r"[\r\n\t]+", " ", regex=True)
            s = s.str.replace(r" +", " ", regex=True).str.strip()
            # Scientific notation fix (vectorized for common case)
            sci_mask = s.str.match(r"^[+-]?\d+(?:\.\d+)?[eE][+-]?\d+$", na=False)
            if sci_mask.any():
                s = s.where(~sci_mask, s[sci_mask].apply(_fix_scientific))
            # Trailing decimal fix (e.g. "123.0" -> "123")
            trail_mask = s.str.match(r"^\d+\.0+$", na=False)
            if trail_mask.any():
                s = s.where(~trail_mask, s[trail_mask].str.split(".").str[0])
            df[col] = s

        # Remove completely empty rows
        non_empty = df.apply(lambda row: row.astype(str).str.strip().any(), axis=1)
        df = df[non_empty].reset_index(drop=True)

        return df

    def _stage2_transforms(
        self, df: pd.DataFrame, mapping_dict: Dict[str, str], warnings: List[List[str]]
    ) -> pd.DataFrame:
        """Stage 2: Schema-driven field transforms (vectorized where possible)."""
        df = df.copy()

        for src, tgt in mapping_dict.items():
            if src not in df.columns or tgt == "officeHours_days":
                continue
            rules = self._field_lookup.get(tgt, {})
            transforms = rules.get("transforms_needed", [])
            fmt = rules.get("format")
            is_enum_field = bool(rules.get("enum_values"))
            enum_values = rules.get("enum_values", [])
            fn_lower = tgt.lower()

            col = df[src].astype(str).str.strip()
            # Clean nulls vectorized
            null_mask = col.str.lower().isin(NULL_TOKENS)
            col = col.where(~null_mask, "")
            original = col.copy()
            non_empty = col != ""

            # ── Date transform (vectorized) ──
            is_date = (fmt == "date" or "clean_date_series" in transforms
                       or any(kw in fn_lower for kw in ("date", "dob", "expir", "effective", "appointment")))
            if is_date:
                date_mask = non_empty
                if date_mask.any():
                    parsed = pd.to_datetime(col[date_mask], errors="coerce", format="mixed", dayfirst=False)
                    valid = ~parsed.isna()
                    formatted = parsed.dt.strftime("%Y-%m-%d")
                    new_vals = col.copy()
                    new_vals.loc[date_mask] = formatted.where(valid, col[date_mask])
                    # Track warnings
                    changed = (new_vals != original) & non_empty
                    for idx in changed[changed].index:
                        if idx < len(warnings):
                            warnings[idx].append(f"{src}: {original[idx]} -> {new_vals[idx]}")
                    df[src] = new_vals
                continue

            # ── Phone/fax normalization (vectorized) ──
            is_phone = (any(kw in fn_lower for kw in ("phone", "fax", "tty", "telephone"))
                        or any("normalize_id_digits(10)" in t for t in transforms))
            if is_phone:
                phone_mask = non_empty
                if phone_mask.any():
                    digits = col.str.replace(r"\D", "", regex=True)
                    # Strip country code
                    has_country = digits.str.startswith("1") & (digits.str.len() == 11)
                    digits = digits.where(~has_country, digits.str[1:])
                    valid = digits.str.len() == 10
                    new_vals = digits.where(valid & non_empty, col)
                    changed = (new_vals != original) & non_empty
                    for idx in changed[changed].index:
                        if idx < len(warnings):
                            warnings[idx].append(f"{src}: {original[idx]} -> {new_vals[idx]}")
                    df[src] = new_vals
                continue

            # ── TIN normalization (vectorized) ──
            if "tin" in fn_lower or "taxid" in fn_lower or "taxidentification" in fn_lower:
                tin_mask = non_empty
                if tin_mask.any():
                    digits = col.str.replace(r"\D", "", regex=True)
                    valid = digits.str.len() == 9
                    new_vals = digits.where(valid & (digits != ""), col)
                    changed = (new_vals != original) & non_empty
                    for idx in changed[changed].index:
                        if idx < len(warnings):
                            warnings[idx].append(f"{src}: {original[idx]} -> {new_vals[idx]}")
                    df[src] = new_vals
                continue

            # ── NPI normalization (vectorized) ──
            if "npi" in fn_lower and "group" not in fn_lower:
                npi_mask = non_empty
                if npi_mask.any():
                    digits = col.str.replace(r"\D", "", regex=True)
                    padded = digits.str.zfill(10)
                    valid = padded.str.len() == 10
                    new_vals = padded.where(valid & (digits != ""), col)
                    changed = (new_vals != original) & non_empty
                    for idx in changed[changed].index:
                        if idx < len(warnings):
                            warnings[idx].append(f"{src}: {original[idx]} -> {new_vals[idx]}")
                    df[src] = new_vals
                continue

            # ── Gender (vectorized) ──
            if fn_lower == "gender" or "normalize_gender" in transforms:
                gender_map_series = col.str.lower().map(GENDER_MAP)
                new_vals = gender_map_series.where(gender_map_series.notna(), col)
                changed = (new_vals != original) & non_empty
                for idx in changed[changed].index:
                    if idx < len(warnings):
                        warnings[idx].append(f"{src}: Gender {original[idx]} -> {new_vals[idx]}")
                df[src] = new_vals
                continue

            # ── State normalization (vectorized) ──
            if ("normalize_state" in transforms
                or ("state" in fn_lower and "license" not in fn_lower and "number" not in fn_lower)):
                if "licenseNumber" not in tgt and tgt != "stateLicenseNumber":
                    # 2-letter uppercase
                    is_2letter = col.str.len().eq(2) & col.str.isalpha()
                    upper_vals = col.str.upper()
                    # Full state name lookup
                    state_mapped = col.str.lower().map(US_STATE_MAP)
                    new_vals = col.copy()
                    new_vals = new_vals.where(~is_2letter, upper_vals)
                    has_map = state_mapped.notna() & ~is_2letter
                    new_vals = new_vals.where(~has_map, state_mapped)
                    changed = (new_vals != original) & non_empty
                    for idx in changed[changed].index:
                        if idx < len(warnings):
                            warnings[idx].append(f"{src}: {original[idx]} -> {new_vals[idx]}")
                    df[src] = new_vals
                continue

            # ── ZIP normalization (vectorized) ──
            if "zip" in fn_lower or "postal" in fn_lower:
                if not transforms and fmt != "date":
                    digits = col.str.replace(r"\D", "", regex=True)
                    is_5 = digits.str.len() == 5
                    is_9 = digits.str.len() == 9
                    is_4 = digits.str.len() == 4
                    new_vals = col.copy()
                    new_vals = new_vals.where(~is_5, digits)
                    new_vals = new_vals.where(~is_9, digits)
                    new_vals = new_vals.where(~is_4, "0" + digits)
                    changed = (new_vals != original) & non_empty
                    for idx in changed[changed].index:
                        if idx < len(warnings):
                            warnings[idx].append(f"{src}: {original[idx]} -> {new_vals[idx]}")
                    df[src] = new_vals
                continue

            # ── acceptsNewPatients (vectorized) ──
            if tgt == "acceptsNewPatients" and is_enum_field:
                mapped = col.str.lower().str.strip().map(_ACCEPTS_PATIENTS_MAP)
                new_vals = mapped.where(mapped.notna(), col)
                # Also try case-insensitive enum match for unmapped
                if enum_values:
                    lower_enum = {str(ev).lower(): str(ev) for ev in enum_values}
                    still_unmapped = mapped.isna() & non_empty
                    if still_unmapped.any():
                        enum_mapped = col[still_unmapped].str.lower().map(lower_enum)
                        new_vals = new_vals.where(~still_unmapped, enum_mapped.where(enum_mapped.notna(), col))
                changed = (new_vals != original) & non_empty
                for idx in changed[changed].index:
                    if idx < len(warnings):
                        warnings[idx].append(f"{src}: acceptsNewPatients: '{original[idx]}' normalized to '{new_vals[idx]}'")
                df[src] = new_vals
                continue

            # ── Transaction type normalization (vectorized) ──
            if tgt == "pdmTransactionType" and is_enum_field:
                tx_map = {
                    "term": "Terminate from Network", "terminate": "Terminate from Network",
                    "termination": "Terminate from Network", "term from network": "Terminate from Network",
                    "term from group": "Terminate from Group", "term from location": "Terminate from Location",
                    "add": "Add to Network", "new": "Add to Network",
                    "add to network": "Add to Network", "add to group": "Add to Group",
                    "add to plan": "Add to Plan", "update": "Update Information",
                    "change": "Update Information", "update information": "Update Information",
                }
                mapped_tx = col.str.lower().str.strip().map(tx_map)
                new_vals = mapped_tx.where(mapped_tx.notna(), col)
                changed = (new_vals != original) & non_empty
                for idx in changed[changed].index:
                    if idx < len(warnings):
                        warnings[idx].append(f"{src}: Transaction type: '{original[idx]}' normalized to '{new_vals[idx]}'")
                df[src] = new_vals
                continue

            # ── Location type normalization (vectorized) ──
            if tgt == "groupPractitionerLocationType":
                loc_map = {"primary": "PRI", "secondary": "PRA", "pri": "PRI", "pra": "PRA"}
                mapped_loc = col.str.lower().str.strip().map(loc_map)
                new_vals = mapped_loc.where(mapped_loc.notna(), col)
                changed = (new_vals != original) & non_empty
                for idx in changed[changed].index:
                    if idx < len(warnings):
                        warnings[idx].append(f"{src}: Location type: '{original[idx]}' normalized to '{new_vals[idx]}'")
                df[src] = new_vals
                continue

            # ── Gender restrictions (vectorized) ──
            if tgt == "practitionerLocationGenderAccepted":
                gr_map = {"y": "Both", "n": "Both", "yes": "Both", "no": "Both",
                          "male": "Male", "female": "Female", "both": "Both",
                          "m": "Male", "f": "Female"}
                mapped_gr = col.str.lower().str.strip().map(gr_map)
                new_vals = mapped_gr.where(mapped_gr.notna(), col)
                changed = (new_vals != original) & non_empty
                for idx in changed[changed].index:
                    if idx < len(warnings):
                        warnings[idx].append(f"{src}: Gender restrictions: '{original[idx]}' normalized to '{new_vals[idx]}'")
                df[src] = new_vals
                continue

            # ── Digit normalization by schema rule (vectorized) ──
            digit_match = None
            for t in transforms:
                m = re.match(r"normalize_id_digits\((\d+)\)", t)
                if m:
                    digit_match = int(m.group(1))
                    break
            if digit_match is not None:
                length = digit_match
                digits = col.str.replace(r"\D", "", regex=True)
                padded = digits.str.zfill(length)
                valid = padded.str.len() == length
                new_vals = padded.where(valid & (digits != ""), col)
                changed = (new_vals != original) & non_empty
                for idx in changed[changed].index:
                    if idx < len(warnings):
                        warnings[idx].append(f"{src}: {original[idx]} -> {new_vals[idx]}")
                df[src] = new_vals
                continue

            # ── Fallback: cell-by-cell for remaining fields with schema transforms ──
            if transforms or fmt:
                col_pos = list(df.columns).index(src)
                for idx in range(len(df)):
                    val = col.iat[idx]
                    if not val:
                        continue
                    new_val, warn = _apply_schema_transform(val, tgt, rules)
                    if new_val != val:
                        df.iat[idx, col_pos] = new_val
                    if warn and idx < len(warnings):
                        warnings[idx].append(f"{src}: {warn}")

        return df

    def _stage3_special_splits(
        self, df: pd.DataFrame, mapping_dict: Dict[str, str], warnings: List[List[str]]
    ) -> Dict[str, pd.Series]:
        """Stage 3: Handle office hours split and malpractice amount split.

        Returns a dict of extra column name -> Series to add to output.
        """
        extra_cols: Dict[str, pd.Series] = {}
        n = len(df)
        col_positions = {col: i for i, col in enumerate(df.columns)}

        # ── Office Hours Split ──
        # Detect hours column by: mapping target OR source column name pattern
        hours_src = None
        days_src = None
        for src, tgt in mapping_dict.items():
            if tgt == "officeHours" or tgt == "officeHours_days":
                if "hour" in src.lower() and "day" not in src.lower():
                    hours_src = src
                elif "day" in src.lower():
                    days_src = src
        # Also scan ALL source columns for hours/days patterns (even if not mapped to officeHours)
        if not hours_src:
            for col in df.columns:
                cl = col.lower()
                if ("practice hour" in cl or "office hour" in cl or cl == "hours") and col in col_positions:
                    hours_src = col
                    break
        if not days_src:
            for col in df.columns:
                cl = col.lower()
                if ("days" in cl and "hour" in cl) or ("days" in cl and "effective" in cl):
                    if col in col_positions:
                        days_src = col
                        break

        if hours_src and hours_src in col_positions:
            hours_pos = col_positions[hours_src]
            days_pos = col_positions.get(days_src) if days_src else None

            # Prepare 14 columns
            day_cols: Dict[str, List[str]] = {}
            for day in _ORDERED_DAYS:
                open_field, close_field = _DAY_SCHEMA_FIELDS[day]
                open_rules = self._field_lookup.get(open_field, {})
                close_rules = self._field_lookup.get(close_field, {})
                open_title = open_rules.get("title", open_field)
                close_title = close_rules.get("title", close_field)
                day_cols[f"open_{day}"] = [""] * n
                day_cols[f"close_{day}"] = [""] * n
                # Store title mapping
                day_cols[f"_title_open_{day}"] = [open_title]
                day_cols[f"_title_close_{day}"] = [close_title]

            for idx in range(n):
                hours_val = str(df.iat[idx, hours_pos]).strip()
                days_val = str(df.iat[idx, days_pos]).strip() if days_pos is not None else ""

                if not hours_val:
                    continue

                # Parse time range: "8am-5pm", "8:00am - 5:00pm", "08:00-17:00"
                time_match = re.match(r"(.+?)\s*[-–]\s*(.+)", hours_val)
                open_time = None
                close_time = None
                if time_match:
                    open_time = _parse_time(time_match.group(1))
                    close_time = _parse_time(time_match.group(2))

                if not open_time or not close_time:
                    continue

                # Parse days
                days = _parse_day_range(days_val) if days_val else _ORDERED_DAYS[:5]  # default Mon-Fri

                for day in days:
                    if day in _DAY_SCHEMA_FIELDS:
                        day_cols[f"open_{day}"][idx] = open_time
                        day_cols[f"close_{day}"][idx] = close_time

                warnings[idx].append(
                    f"Office hours split from '{hours_val}"
                    + (f" {days_val}" if days_val else "")
                    + f"' into daily schedule"
                )

            # Add to extra_cols with schema titles
            for day in _ORDERED_DAYS:
                open_field, close_field = _DAY_SCHEMA_FIELDS[day]
                open_rules = self._field_lookup.get(open_field, {})
                close_rules = self._field_lookup.get(close_field, {})
                open_title = open_rules.get("title", open_field)
                close_title = close_rules.get("title", close_field)
                extra_cols[open_title] = pd.Series(day_cols[f"open_{day}"], index=df.index)
                extra_cols[close_title] = pd.Series(day_cols[f"close_{day}"], index=df.index)

        # ── Malpractice Amount Split ──
        malp_src = None
        for src, tgt in mapping_dict.items():
            if tgt == "practitionerMalpracticeAggregateCoverageAmount":
                malp_src = src
                break

        if malp_src and malp_src in col_positions:
            malp_pos = col_positions[malp_src]
            indiv_rules = self._field_lookup.get("practitionerMalpracticeOccuranceCoverageAmount", {})
            agg_rules = self._field_lookup.get("practitionerMalpracticeAggregateCoverageAmount", {})
            indiv_title = indiv_rules.get("title", "Practitioner Malpractice Individual Coverage Amount")
            agg_title = agg_rules.get("title", "Practitioner Malpractice Aggregate Coverage Amount")

            indiv_vals = [""] * n
            agg_vals = [""] * n

            for idx in range(n):
                val = str(df.iat[idx, malp_pos]).strip()
                if not val:
                    continue

                individual, aggregate, warn = _split_malpractice_amounts(val)
                if individual or aggregate:
                    indiv_vals[idx] = individual
                    agg_vals[idx] = aggregate
                    df.iat[idx, malp_pos] = aggregate  # Update the original to just aggregate
                    if warn:
                        warnings[idx].append(warn)
                else:
                    # Single value, just clean
                    cleaned = re.sub(r"[$,]", "", val).strip()
                    agg_vals[idx] = cleaned
                    if cleaned != val:
                        df.iat[idx, malp_pos] = cleaned
                        warnings[idx].append(f"Malpractice amount cleaned: {val} -> {cleaned}")

            extra_cols[indiv_title] = pd.Series(indiv_vals, index=df.index)
            # The aggregate column will be handled via the normal mapping output

        return extra_cols

    def _stage4_validations(
        self,
        df: pd.DataFrame,
        mapping_dict: Dict[str, str],
        validations: List[List[str]],
        warnings: List[List[str]],
        extra_cols: Dict[str, pd.Series],
    ) -> None:
        """Stage 4: Schema-driven validations."""
        col_positions = {col: i for i, col in enumerate(df.columns)}
        n = len(df)

        # Build reverse lookup
        tgt_to_src = {tgt: src for src, tgt in mapping_dict.items()}

        # ── Empty row detection ──
        npi_col = tgt_to_src.get("practitionerNpi")
        lastname_col = tgt_to_src.get("lastName")
        firstname_col = tgt_to_src.get("firstName")

        for idx in range(n):
            npi_val = str(df.iat[idx, col_positions[npi_col]]).strip() if npi_col and npi_col in col_positions else ""
            ln_val = str(df.iat[idx, col_positions[lastname_col]]).strip() if lastname_col and lastname_col in col_positions else ""
            fn_val = str(df.iat[idx, col_positions[firstname_col]]).strip() if firstname_col and firstname_col in col_positions else ""

            if not npi_val and not fn_val and not ln_val:
                validations[idx].append("Row has no provider data — missing NPI, Name")

        # ── NPPES name match ──
        for idx in range(n):
            npi_val = str(df.iat[idx, col_positions[npi_col]]).strip() if npi_col and npi_col in col_positions else ""
            if npi_val and self.nppes_cache and npi_val in self.nppes_cache:
                nppes_record = self.nppes_cache[npi_val]
                if nppes_record and isinstance(nppes_record, dict):
                    nppes_last = str(nppes_record.get("last_name") or "").strip().upper()
                    nppes_first = str(nppes_record.get("first_name") or "").strip().upper()
                    ln_val = str(df.iat[idx, col_positions[lastname_col]]).strip() if lastname_col and lastname_col in col_positions else ""
                    fn_val = str(df.iat[idx, col_positions[firstname_col]]).strip() if firstname_col and firstname_col in col_positions else ""
                    roster_last = ln_val.upper()
                    roster_first = fn_val.upper()
                    if (nppes_last and roster_last and nppes_last != roster_last) or \
                       (nppes_first and roster_first and nppes_first != roster_first):
                        warnings[idx].append(
                            f"Provider name does not match NPPES: roster={fn_val} {ln_val}, NPPES={nppes_first} {nppes_last}"
                        )

        # ── Per-field validations ──
        today = datetime.now()

        for src, tgt in mapping_dict.items():
            if src not in col_positions or tgt == "officeHours_days":
                continue
            col_pos = col_positions[src]
            rules = self._field_lookup.get(tgt, {})
            pattern = rules.get("pattern")
            enum_values = rules.get("enum_values", [])
            vneeded = rules.get("validations_needed", [])
            fmt = rules.get("format")
            system_required = rules.get("system_required", False)
            field_title = src  # Use source column name — what the client recognizes

            # Build case-insensitive enum lookup
            enum_lower_map: Dict[str, str] = {}
            if enum_values:
                for ev in enum_values:
                    enum_lower_map[str(ev).lower()] = str(ev)

            for idx in range(n):
                val = str(df.iat[idx, col_pos] or "").strip()

                # Required field empty check
                if not val:
                    if system_required and "required_field_empty" in vneeded:
                        validations[idx].append(f"{field_title}: required field is empty")
                    continue

                # Pattern validation
                if pattern:
                    try:
                        if not re.fullmatch(pattern, val):
                            error_msg = (rules.get("error_messages", {}).get("pattern") or
                                         f"Value '{val}' does not match pattern {pattern}")
                            validations[idx].append(f"{field_title}: {error_msg}")
                    except re.error:
                        pass

                # Enum validation (case-insensitive)
                # Skip enum check for: free-text fields, fields where value looks like
                # a sentence/description, and fields not explicitly marked for enum validation
                if enum_values and "enum_reference" in vneeded:
                    val_lower = val.lower()
                    if val_lower not in enum_lower_map:
                        # Skip if value looks like free text (>40 chars or has multiple words with >3 words)
                        word_count = len(val.split())
                        if len(val) > 50 or word_count > 4:
                            pass  # Free text, skip enum validation
                        else:
                            validations[idx].append(
                                f"{field_title}: value '{val}' is not in accepted values"
                            )

                # Email validation
                if fmt == "email" or "email_format" in vneeded:
                    if not _EMAIL_RE.fullmatch(val):
                        validations[idx].append(f"{field_title}: invalid email format '{val}'")

                # Date validation — only for values that look like they could be dates
                # Skip pure text values (e.g., "Term", "Add") in date fields
                if fmt == "date" or "date_format" in vneeded:
                    looks_like_date = bool(re.search(r"\d", val))  # Has at least one digit
                    if not looks_like_date:
                        pass  # Pure text in a date field — not a date validation error
                    elif _DATE_RE.fullmatch(val):
                        try:
                            dt = datetime.strptime(val, "%Y-%m-%d")
                            if dt.year < 1901 or dt.year > 2099:
                                validations[idx].append(f"{field_title}: date {val} out of range")
                        except ValueError:
                            validations[idx].append(f"{field_title}: date {val} not parseable")
                    else:
                        try:
                            dt = pd.to_datetime(val, errors="coerce", format="mixed")
                            if pd.isna(dt):
                                validations[idx].append(f"{field_title}: date {val} not parseable")
                        except Exception:
                            pass

                # ── Expired credential check ──
                if "expir" in tgt.lower():
                    if _DATE_RE.fullmatch(val):
                        try:
                            exp_dt = datetime.strptime(val, "%Y-%m-%d")
                            if exp_dt < today:
                                validations[idx].append(f"{field_title} expired ({val})")
                        except ValueError:
                            pass

    def _build_output(
        self,
        df: pd.DataFrame,
        mapping_dict: Dict[str, str],
        extra_cols: Dict[str, pd.Series],
        warnings: List[List[str]],
        validations: List[List[str]],
    ) -> pd.DataFrame:
        """Build output DataFrame using ORIGINAL SOURCE column names (what the client recognizes)."""
        output = pd.DataFrame(index=df.index)

        # Track which source columns we've already added
        added_sources = set()

        for src, tgt in mapping_dict.items():
            if tgt == "officeHours_days":
                continue  # Skip virtual mapping, handled by office hours split
            if tgt == "officeHours":
                continue  # Skip — replaced by daily columns

            # Use the ORIGINAL source column name — client recognizes their own headers
            col_name = src

            if src in df.columns:
                output[col_name] = df[src]
            else:
                output[col_name] = ""
            added_sources.add(src)

        # Add extra columns (office hours, malpractice individual)
        for col_name, series in extra_cols.items():
            if col_name not in output.columns:
                output[col_name] = series

        # Add unmapped columns (pass-through with original names)
        for col in df.columns:
            if col not in added_sources and col not in output.columns:
                output[col] = df[col]

        output["Warnings"] = ["; ".join(w) if w else "" for w in warnings]
        output["Business_Validations"] = ["; ".join(v) if v else "" for v in validations]

        return output

    def summarize_for_ui(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Run transforms FIRST, then validate the POST-TRANSFORM values.

        Only issues that persist AFTER normalization are shown to the client.
        e.g., "Male" → "M" via normalize_gender is NOT an error.
        But "XYZ" → still "XYZ" (not in enum) IS an error.
        """
        df = df.copy()
        df.columns = [c.strip() for c in df.columns]

        mapping_dict: Dict[str, str] = {}
        for m in self.mappings:
            if m.get("approved") is False:
                continue
            src = str(m.get("source_column") or "").strip()
            tgt = str(m.get("target_field") or "").strip()
            if src and tgt and src in df.columns:
                mapping_dict[src] = tgt

        # Apply cleanup + transforms first so validations see post-transform values
        df = self._stage1_cleanup(df)
        dummy_warnings: List[List[str]] = [[] for _ in range(len(df))]
        df = self._stage2_transforms(df, mapping_dict, dummy_warnings)

        n = len(df)
        col_positions = {col: i for i, col in enumerate(df.columns)}
        tgt_to_src = {tgt: src for src, tgt in mapping_dict.items()}
        today = datetime.now()

        # Collect issue counts by type
        issue_counts: Dict[str, Dict[str, Any]] = {}

        def _add(key: str, title: str, message: str, severity: str, category: str, sample: str = ""):
            if key not in issue_counts:
                issue_counts[key] = {
                    "title": title, "message": message, "severity": severity,
                    "category": category, "count": 0, "samples": [],
                }
            issue_counts[key]["count"] += 1
            if sample and len(issue_counts[key]["samples"]) < 5:
                issue_counts[key]["samples"].append(sample)

        # Empty rows
        npi_col = tgt_to_src.get("practitionerNpi")
        ln_col = tgt_to_src.get("lastName")
        fn_col = tgt_to_src.get("firstName")
        for idx in range(n):
            npi = str(df.iat[idx, col_positions[npi_col]]).strip() if npi_col and npi_col in col_positions else ""
            ln = str(df.iat[idx, col_positions[ln_col]]).strip() if ln_col and ln_col in col_positions else ""
            fn = str(df.iat[idx, col_positions[fn_col]]).strip() if fn_col and fn_col in col_positions else ""
            if not npi and not fn and not ln:
                _add("empty_rows", "Empty Rows", "", "warning", "completeness")

        # NPPES checks
        if npi_col and npi_col in col_positions:
            for idx in range(n):
                npi = str(df.iat[idx, col_positions[npi_col]]).strip()
                if npi and self.nppes_cache and npi in self.nppes_cache:
                    rec = self.nppes_cache[npi]
                    if rec is None:
                        _add("nppes_missing", "NPI Not Found in NPPES", "", "error", "external_reference", npi)
                    elif isinstance(rec, dict):
                        nppes_last = str(rec.get("last_name", "")).strip().upper()
                        nppes_first = str(rec.get("first_name", "")).strip().upper()
                        ln = str(df.iat[idx, col_positions[ln_col]]).strip().upper() if ln_col and ln_col in col_positions else ""
                        fn = str(df.iat[idx, col_positions[fn_col]]).strip().upper() if fn_col and fn_col in col_positions else ""
                        if (nppes_last and ln and nppes_last != ln) or (nppes_first and fn and nppes_first != fn):
                            _add("nppes_name", "Provider Name Mismatch vs NPPES", "", "warning",
                                 "external_reference", f"{fn} {ln} vs NPPES: {nppes_first} {nppes_last}")

        # Per-field validations
        for src, tgt in mapping_dict.items():
            if src not in col_positions or tgt == "officeHours_days":
                continue
            col_pos = col_positions[src]
            rules = self._field_lookup.get(tgt, {})
            pattern = rules.get("pattern")
            enum_values = rules.get("enum_values", [])
            vneeded = rules.get("validations_needed", [])
            fmt = rules.get("format")
            system_required = rules.get("system_required", False)
            field_title = src  # Use source column name — what the client recognizes

            enum_lower = {str(ev).lower(): ev for ev in enum_values} if enum_values else {}

            for idx in range(n):
                val = str(df.iat[idx, col_pos] or "").strip()
                if not val:
                    if system_required and "required_field_empty" in vneeded:
                        _add(f"required_{tgt}", f"Missing {field_title}", "", "error", "completeness")
                    continue

                if pattern:
                    try:
                        if not re.fullmatch(pattern, val):
                            _add(f"pattern_{tgt}", f"Invalid {field_title} Format", "", "error", "format", val)
                    except re.error:
                        pass

                if enum_values and "enum_reference" in vneeded and val.lower() not in enum_lower:
                    # Skip free-text values
                    if len(val) <= 50 and len(val.split()) <= 4:
                        _add(f"enum_{tgt}", f"Invalid {field_title} Value", "", "warning", "business_rule", val)

                if "expir" in tgt.lower() and fmt == "date":
                    try:
                        dt = pd.to_datetime(val, errors="coerce", format="mixed")
                        if not pd.isna(dt) and dt < pd.Timestamp(today):
                            _add(f"expired_{tgt}", f"Expired {field_title}", "", "warning", "business_rule", val)
                    except Exception:
                        pass

        # Build quality_audit-style items
        issues: List[Dict[str, Any]] = []
        for key, info in issue_counts.items():
            count = info["count"]
            title = info["title"]
            category = info["category"]
            severity = info["severity"]
            samples = info["samples"]

            msg = f"{count} rows affected"
            if key == "empty_rows":
                msg = f"{count} rows have no provider data — missing NPI, Name"
            elif key == "nppes_missing":
                msg = f"{count} NPI numbers not found in the NPPES registry"
            elif key == "nppes_name":
                msg = f"{count} providers have names that don't match NPPES records"
            elif key.startswith("required_"):
                msg = f"{count} rows missing required field"
            elif key.startswith("pattern_"):
                msg = f"{count} values don't match expected format"
            elif key.startswith("enum_"):
                msg = f"{count} values not in accepted values list"
            elif key.startswith("expired_"):
                msg = f"{count} credentials are expired"

            issues.append({
                "id": f"qa::{key}",
                "category": category,
                "rule_type": key,
                "severity": severity,
                "title": title,
                "message": msg,
                "source_column": "",
                "target_field": "",
                "affected_rows": count,
                "affected_pct": round(count / max(n, 1), 4),
                "sample_values": samples,
                "evidence": {},
                "suggested_fix": {"action": "review", "description": "Review affected rows"},
                "confidence": 0.90,
                "confidence_band": "High",
                "approved": True,
                "suggested_by": "preprocessing_pipeline",
                "schema_valid": True,
            })

        return sorted(issues, key=lambda x: (0 if x["severity"] == "error" else 1, -x["affected_rows"]))
