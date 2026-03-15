"""Input profiling utilities for Universal Roster V2 standalone."""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def _sample_column(series: pd.Series, max_samples: int = 8) -> List[str]:
    samples: List[str] = []
    for value in series.dropna().astype(str).tolist():
        text = value.strip()
        if text:
            samples.append(text)
        if len(samples) >= max_samples:
            break
    return samples


def _normalize_column_name(name: Any) -> str:
    return str(name).strip()


def _file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(chunk_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _detect_roster_type_from_columns(columns: List[str]) -> str:
    lowered = [c.lower() for c in columns]

    practitioner_signals = [
        "first name",
        "last name",
        "dob",
        "date of birth",
        "taxonomy",
        "specialty",
        "dea",
        "caqh",
        "provider",
    ]
    facility_signals = [
        "facility",
        "organization",
        "facility type",
        "line of business",
        "group npi",
        "facility npi",
    ]

    p_score = sum(1 for signal in practitioner_signals if any(signal in col for col in lowered))
    f_score = sum(1 for signal in facility_signals if any(signal in col for col in lowered))
    return "practitioner" if p_score >= f_score else "facility"


def _normalize_scalar(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def _type_likelihoods(values: List[str]) -> Dict[str, float]:
    non_blank = [v for v in values if v]
    total = len(non_blank)
    if total == 0:
        return {"identifier": 0.0, "date": 0.0, "email": 0.0, "phone": 0.0, "numeric": 0.0, "text": 1.0}

    date_re = re.compile(r"^(\d{4}-\d{2}-\d{2}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})$")
    email_re = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    phone_re = re.compile(r"^(\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$")
    identifier_re = re.compile(r"^[A-Za-z0-9_-]{5,}$")

    date_hits = sum(1 for v in non_blank if date_re.fullmatch(v))
    email_hits = sum(1 for v in non_blank if email_re.fullmatch(v))
    phone_hits = sum(1 for v in non_blank if phone_re.fullmatch(v))
    numeric_hits = sum(1 for v in non_blank if re.fullmatch(r"^-?\d+(\.\d+)?$", v) is not None)
    identifier_hits = sum(1 for v in non_blank if identifier_re.fullmatch(v) and any(ch.isdigit() for ch in v))

    text_hits = total - max(date_hits, email_hits, phone_hits, numeric_hits)

    return {
        "identifier": round(identifier_hits / total, 4),
        "date": round(date_hits / total, 4),
        "email": round(email_hits / total, 4),
        "phone": round(phone_hits / total, 4),
        "numeric": round(numeric_hits / total, 4),
        "text": round(max(0, text_hits) / total, 4),
    }


def _regex_pattern_hits(values: List[str]) -> Dict[str, float]:
    non_blank = [v for v in values if v]
    total = len(non_blank)
    if total == 0:
        return {}

    patterns = {
        "digits_only": re.compile(r"^\d+$"),
        "alnum_mixed": re.compile(r"^[A-Za-z0-9]+$"),
        "contains_dash": re.compile(r"-"),
        "contains_slash": re.compile(r"/"),
        "contains_at": re.compile(r"@"),
        "upper_code": re.compile(r"^[A-Z]{2,}$"),
    }
    out: Dict[str, float] = {}
    for key, pattern in patterns.items():
        hits = 0
        for value in non_blank:
            if key in {"contains_dash", "contains_slash", "contains_at"}:
                matched = pattern.search(value) is not None
            else:
                matched = pattern.fullmatch(value) is not None
            if matched:
                hits += 1
        out[key] = round(hits / total, 4)
    return out


def _top_k_distribution(values: List[str], k: int = 10) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = {}
    for value in values:
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    rows = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:k]
    return [{"value": value, "count": count} for value, count in rows]


def _stratified_sample(values: List[str], max_samples: int = 12) -> List[str]:
    non_blank = [v for v in values if v]
    if len(non_blank) <= max_samples:
        return non_blank

    sample: List[str] = []
    by_length: Dict[int, List[str]] = {}
    for value in non_blank:
        by_length.setdefault(len(value), []).append(value)

    lengths = sorted(by_length.keys())
    if not lengths:
        return non_blank[:max_samples]

    per_bucket = max(1, math.ceil(max_samples / max(1, len(lengths))))
    for length in lengths:
        for value in by_length[length][:per_bucket]:
            if value not in sample:
                sample.append(value)
            if len(sample) >= max_samples:
                return sample

    for value in non_blank:
        if value not in sample:
            sample.append(value)
        if len(sample) >= max_samples:
            break
    return sample[:max_samples]


def _column_stat(sheet_name: str, column: str, series: pd.Series, max_samples: int = 12, top_k: int = 10) -> Dict[str, Any]:
    raw_values = [_normalize_scalar(value) for value in series.astype(str).tolist()]
    total_count = len(raw_values)
    non_blank = [v for v in raw_values if v]
    non_null_count = len(non_blank)
    null_count = total_count - non_null_count
    null_pct = round((null_count / total_count), 4) if total_count else 0.0

    distinct_values = sorted(set(non_blank))
    cardinality = len(distinct_values)

    sampled = _stratified_sample(non_blank, max_samples=max_samples)
    type_likelihoods = _type_likelihoods(non_blank)
    regex_hits = _regex_pattern_hits(non_blank)
    distribution = _top_k_distribution(non_blank, k=top_k)

    return {
        "sheet_name": sheet_name,
        "column": column,
        "non_null": non_null_count,
        "null": null_count,
        "null_pct": null_pct,
        "cardinality": cardinality,
        "distinct_ratio": round((cardinality / non_null_count), 4) if non_null_count else 0.0,
        "sample_values": sampled[: max_samples // 2],
        "stratified_samples": sampled,
        "top_values": distribution,
        "type_likelihoods": type_likelihoods,
        "pattern_hits": regex_hits,
    }


def _sheet_drift_summary(sheet_stats: Dict[str, Dict[str, Dict[str, Any]]]) -> Dict[str, Any]:
    sheet_names = sorted(sheet_stats.keys())
    if len(sheet_names) <= 1:
        return {"sheets": sheet_names, "pairwise": []}

    pairwise = []
    for i in range(len(sheet_names)):
        for j in range(i + 1, len(sheet_names)):
            left = sheet_names[i]
            right = sheet_names[j]
            left_cols = set(sheet_stats[left].keys())
            right_cols = set(sheet_stats[right].keys())
            overlap = sorted(left_cols & right_cols)
            only_left = sorted(left_cols - right_cols)
            only_right = sorted(right_cols - left_cols)

            overlap_scores = []
            for column in overlap:
                left_card = int(sheet_stats[left][column].get("cardinality", 0) or 0)
                right_card = int(sheet_stats[right][column].get("cardinality", 0) or 0)
                denominator = max(left_card, right_card, 1)
                overlap_scores.append(round(min(left_card, right_card) / denominator, 4))

            pairwise.append(
                {
                    "left_sheet": left,
                    "right_sheet": right,
                    "shared_columns": overlap,
                    "left_only_columns": only_left,
                    "right_only_columns": only_right,
                    "shared_column_count": len(overlap),
                    "shared_value_overlap_score": round(sum(overlap_scores) / len(overlap_scores), 4)
                    if overlap_scores
                    else 0.0,
                }
            )

    return {"sheets": sheet_names, "pairwise": pairwise}


def read_input_file(
    file_path: str | Path,
    sample_rows: int = 200,
    full_roster_learning: bool = False,
    profile_max_rows: int = 0,
    sheet_name: Optional[str] = None,
) -> pd.DataFrame:
    path = Path(file_path).expanduser().resolve()
    suffix = path.suffix.lower()

    if full_roster_learning:
        nrows = None
        if profile_max_rows and profile_max_rows > 0:
            nrows = int(profile_max_rows)
    else:
        nrows = max(1, int(sample_rows or 1))

    if suffix == ".csv":
        return pd.read_csv(path, dtype=str, nrows=nrows)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet_name, dtype=str, nrows=nrows)
    raise ValueError(f"Unsupported input format: {suffix}")


def profile_input(
    file_path: str | Path,
    sample_rows: int = 500,
    full_roster_learning: bool = False,
    profile_max_rows: int = 0,
) -> Dict[str, Any]:
    path = Path(file_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    suffix = path.suffix.lower()
    if suffix not in {".csv", ".xlsx", ".xls"}:
        raise ValueError(f"Unsupported input format: {suffix}")

    profiling_mode = "full_roster" if full_roster_learning else "sample"

    profile: Dict[str, Any] = {
        "file_name": path.name,
        "file_path": str(path),
        "file_type": suffix,
        "sheets": [],
        "columns": [],
        "column_stats": [],
        "row_sample_size": 0,
        "profiling_mode": profiling_mode,
        "rows_profiled": 0,
        "rows_total": 0,
        "profile_max_rows": int(profile_max_rows or 0),
        "semantic_profile": {
            "column_semantics": {},
            "sheet_drift": {},
            "profiling_mode": "standard",
        },
    }

    all_columns: List[str] = []
    sampled_rows = 0
    total_rows = 0
    sheet_stats: Dict[str, Dict[str, Dict[str, Any]]] = {}

    if suffix == ".csv":
        if full_roster_learning and (profile_max_rows <= 0):
            df = read_input_file(
                path,
                sample_rows=sample_rows,
                full_roster_learning=True,
                profile_max_rows=0,
            )
            total_sheet_rows = int(len(df))
        else:
            df = read_input_file(
                path,
                sample_rows=sample_rows,
                full_roster_learning=full_roster_learning,
                profile_max_rows=profile_max_rows,
            )
            total_df = pd.read_csv(path, dtype=str)
            total_sheet_rows = int(len(total_df))
        profiled_sheet_rows = int(len(df))
        columns = [_normalize_column_name(c) for c in df.columns]
        profile["sheets"].append(
            {
                "sheet_name": "__csv__",
                "columns": columns,
                "row_sample_size": profiled_sheet_rows,
                "rows_total": total_sheet_rows,
                "profiling_mode": profiling_mode,
            }
        )
        sheet_stats["__csv__"] = {}
        for col in columns:
            series = df[col] if col in df.columns else pd.Series(dtype=str)
            all_columns.append(col)
            stat = _column_stat("__csv__", col, series)
            profile["column_stats"].append(stat)
            sheet_stats["__csv__"][col] = stat
        sampled_rows += profiled_sheet_rows
        total_rows += total_sheet_rows
    else:
        workbook = pd.ExcelFile(path)
        for sheet_name in workbook.sheet_names:
            if full_roster_learning and (profile_max_rows <= 0):
                df = read_input_file(
                    path,
                    sample_rows=sample_rows,
                    full_roster_learning=True,
                    profile_max_rows=0,
                    sheet_name=sheet_name,
                )
                total_sheet_rows = int(len(df))
            else:
                total_df = pd.read_excel(path, sheet_name=sheet_name, dtype=str)
                total_sheet_rows = int(len(total_df))
                df = read_input_file(
                    path,
                    sample_rows=sample_rows,
                    full_roster_learning=full_roster_learning,
                    profile_max_rows=profile_max_rows,
                    sheet_name=sheet_name,
                )
            profiled_sheet_rows = int(len(df))
            columns = [_normalize_column_name(c) for c in df.columns]
            profile["sheets"].append(
                {
                    "sheet_name": sheet_name,
                    "columns": columns,
                    "row_sample_size": profiled_sheet_rows,
                    "rows_total": total_sheet_rows,
                    "profiling_mode": profiling_mode,
                }
            )
            sheet_stats[sheet_name] = {}
            for col in columns:
                series = df[col] if col in df.columns else pd.Series(dtype=str)
                all_columns.append(col)
                stat = _column_stat(sheet_name, col, series)
                profile["column_stats"].append(stat)
                sheet_stats[sheet_name][col] = stat
            sampled_rows += profiled_sheet_rows
            total_rows += total_sheet_rows

    deduped: List[str] = []
    seen = set()
    for col in all_columns:
        if col and col not in seen:
            seen.add(col)
            deduped.append(col)

    profile["columns"] = deduped
    profile["row_sample_size"] = sampled_rows
    profile["rows_profiled"] = sampled_rows
    profile["rows_total"] = total_rows
    profile["roster_type_detected"] = _detect_roster_type_from_columns(deduped)

    column_semantics: Dict[str, Dict[str, Any]] = {}
    for stat in profile["column_stats"]:
        column = str(stat.get("column", "") or "").strip()
        if not column:
            continue
        bucket = column_semantics.setdefault(
            column,
            {
                "column": column,
                "sheets": [],
                "aggregate_type_likelihoods": {"identifier": 0.0, "date": 0.0, "email": 0.0, "phone": 0.0, "numeric": 0.0, "text": 0.0},
                "top_values": [],
                "null_pct_avg": 0.0,
            },
        )
        bucket["sheets"].append(stat.get("sheet_name"))
        bucket["null_pct_avg"] += float(stat.get("null_pct", 0.0) or 0.0)

        for key, value in (stat.get("type_likelihoods") or {}).items():
            bucket["aggregate_type_likelihoods"][key] = bucket["aggregate_type_likelihoods"].get(key, 0.0) + float(value or 0.0)

        for top in (stat.get("top_values") or [])[:3]:
            if isinstance(top, dict):
                bucket["top_values"].append(top)

    for column, info in column_semantics.items():
        sheet_count = max(1, len(info["sheets"]))
        info["null_pct_avg"] = round(info["null_pct_avg"] / sheet_count, 4)
        for key in list(info["aggregate_type_likelihoods"].keys()):
            info["aggregate_type_likelihoods"][key] = round(info["aggregate_type_likelihoods"][key] / sheet_count, 4)

        deduped_top = []
        seen_values = set()
        for item in info["top_values"]:
            value = str(item.get("value", "") or "")
            if not value or value in seen_values:
                continue
            seen_values.add(value)
            deduped_top.append(item)
            if len(deduped_top) >= 5:
                break
        info["top_values"] = deduped_top

    profile["semantic_profile"] = {
        "column_semantics": column_semantics,
        "sheet_drift": _sheet_drift_summary(sheet_stats),
        "profiling_mode": "advanced",
        "source_rows_profiled": sampled_rows,
        "source_rows_total": total_rows,
        "sampling_mode": profiling_mode,
    }

    # Build sample_values_by_column from column_stats for quality audit
    profile["sample_values_by_column"] = sample_values_by_column(profile, max_per_column=20)

    # Build sample_rows (list of row dicts) for deterministic quality checks
    # Re-read the sampled dataframe to get actual rows
    _sample_rows_list: List[Dict[str, Any]] = []
    try:
        _sample_df = read_input_file(path, sample_rows=sample_rows, full_roster_learning=False, profile_max_rows=0)
        _sample_df.columns = [_normalize_column_name(c) for c in _sample_df.columns]
        _sample_rows_list = _sample_df.fillna("").astype(str).to_dict(orient="records")
    except Exception:
        pass
    profile["sample_rows"] = _sample_rows_list

    fingerprint_payload = {
        "file_name": profile["file_name"],
        "columns": profile["columns"],
        "sheets": profile["sheets"],
        "sha256": _file_sha256(path),
    }
    signature_base = json.dumps(fingerprint_payload, ensure_ascii=False, sort_keys=True)
    profile["input_fingerprint"] = {
        "sha256": fingerprint_payload["sha256"],
        "signature": hashlib.sha256(signature_base.encode("utf-8")).hexdigest(),
    }

    return profile


def sample_values_by_column(profile: Dict[str, Any], max_per_column: int = 8) -> Dict[str, List[str]]:
    samples: Dict[str, List[str]] = {}
    for stat in profile.get("column_stats", []):
        col = str(stat.get("column", "") or "").strip()
        if not col:
            continue
        combined_source = []
        combined_source.extend(stat.get("stratified_samples") or [])
        combined_source.extend(stat.get("sample_values") or [])

        values = [str(v).strip() for v in combined_source if str(v).strip()]
        if not values:
            continue
        existing = samples.setdefault(col, [])
        for value in values:
            if value not in existing:
                existing.append(value)
            if len(existing) >= max_per_column:
                break
    return samples
