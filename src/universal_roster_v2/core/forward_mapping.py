"""Forward mapping utilities for target output construction."""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List


def mapping_dict(mappings: List[Dict]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in mappings:
        source = str(item.get("source_column", "") or "").strip()
        target = str(item.get("target_field", "") or "").strip()
        if source and target:
            out[source] = target
    return out


def grouped_target_families(mappings: List[Dict]) -> Dict[str, List[Dict]]:
    groups = defaultdict(list)
    for item in mappings:
        target = str(item.get("target_field", "") or "").strip()
        if not target:
            continue
        family = target
        for sep in ["Address", "Language", "Specialty", "Taxonomy", "HospitalAffiliation"]:
            if sep in target:
                family = target.split(sep)[0] + sep
                break
        groups[family].append(item)

    return dict(groups)
