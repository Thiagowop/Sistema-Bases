"""
Campaign splitter.
Splits data into groups based on campaign patterns.
"""
from __future__ import annotations

import pandas as pd

from ..core.base import BaseSplitter, SplitResult
from ..core.schemas import SplitterConfig


class CampaignSplitter(BaseSplitter):
    """Splits records into groups based on campaign patterns."""

    @property
    def name(self) -> str:
        return "campaign"

    def split(self, df: pd.DataFrame) -> SplitResult:
        if not self.enabled or df.empty:
            return SplitResult(splits={"default": df})

        column = self.params.get("column", "CAMPANHA")
        rules = self.params.get("rules", [])
        default_group = self.params.get("default_group", "outros")

        if column not in df.columns:
            return SplitResult(splits={"default": df})

        if not rules:
            return SplitResult(splits={"default": df})

        values = df[column].astype(str).str.strip().str.upper()
        splits: dict[str, pd.DataFrame] = {}
        remaining_mask = pd.Series(True, index=df.index)

        # Process each rule in order
        for rule in rules:
            group_name = rule.get("name", "unknown")
            patterns = rule.get("patterns", [])

            if not patterns:
                continue

            # Match any of the patterns
            match_mask = pd.Series(False, index=df.index)
            for pattern in patterns:
                pattern_upper = str(pattern).upper()
                match_mask = match_mask | values.str.contains(
                    pattern_upper, na=False, regex=False
                )

            # Only consider records not already assigned
            group_mask = remaining_mask & match_mask
            if group_mask.any():
                splits[group_name] = df[group_mask].copy()
                remaining_mask = remaining_mask & ~group_mask

        # Add remaining to default group
        if remaining_mask.any():
            splits[default_group] = df[remaining_mask].copy()

        return SplitResult(splits=splits)


def create_campaign_splitter(config: SplitterConfig) -> CampaignSplitter:
    """Factory function to create a CampaignSplitter."""
    return CampaignSplitter(config)
