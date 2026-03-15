"""Build output dataframes from approved mappings and transformed source data."""

from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd

from universal_roster_v2.core.forward_mapping import mapping_dict


def build_target_output(transformed_df: pd.DataFrame, approved_mappings: List[Dict]) -> Tuple[pd.DataFrame, Dict[str, str]]:
    mapping = mapping_dict(approved_mappings)
    output = pd.DataFrame(index=transformed_df.index)

    for source, target in mapping.items():
        if source not in transformed_df.columns:
            output[target] = ""
            continue
        output[target] = transformed_df[source]

    return output, mapping
