"""Business rule pack entrypoints.

Layer A (universal baseline) applies no extra business rules.
Layer B can register EVRY-specific rule modules here.
"""

from __future__ import annotations

from typing import Dict

import pandas as pd


def apply_business_rules(df: pd.DataFrame, context: Dict) -> pd.DataFrame:
    """Apply optional business rules by template family.

    For baseline delivery this is intentionally pass-through.
    """

    _ = context
    return df
