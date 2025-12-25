"""
Campaign validator.
Filters records based on campaign name patterns.
"""
from __future__ import annotations

import pandas as pd

from ..core.base import BaseValidator, ValidationResult
from ..core.schemas import ValidatorConfig


class CampaignValidator(BaseValidator):
    """Filters records based on campaign patterns."""

    @property
    def name(self) -> str:
        return "campaign"

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        if not self.enabled or df.empty:
            return ValidationResult(valid=df, invalid=pd.DataFrame(), errors=[])

        column = self.params.get("column", "CAMPANHA")
        include_patterns = self.params.get("include", [])
        exclude_patterns = self.params.get("exclude", [])

        if column not in df.columns:
            return ValidationResult(
                valid=df,
                invalid=pd.DataFrame(),
                errors=[f"Campaign column '{column}' not found"],
            )

        values = df[column].astype(str).str.strip().str.upper()
        valid_mask = pd.Series(True, index=df.index)
        errors = []

        # Apply include filter (must match at least one pattern)
        if include_patterns:
            include_mask = pd.Series(False, index=df.index)
            for pattern in include_patterns:
                pattern_upper = str(pattern).upper()
                include_mask = include_mask | values.str.contains(
                    pattern_upper, na=False, regex=False
                )
            valid_mask = valid_mask & include_mask
            excluded = (~include_mask).sum()
            if excluded > 0:
                errors.append(
                    f"{excluded} records excluded (campaign not in: {include_patterns})"
                )

        # Apply exclude filter
        if exclude_patterns:
            exclude_mask = pd.Series(False, index=df.index)
            for pattern in exclude_patterns:
                pattern_upper = str(pattern).upper()
                exclude_mask = exclude_mask | values.str.contains(
                    pattern_upper, na=False, regex=False
                )
            valid_mask = valid_mask & ~exclude_mask
            excluded = exclude_mask.sum()
            if excluded > 0:
                errors.append(
                    f"{excluded} records excluded (campaign in: {exclude_patterns})"
                )

        return ValidationResult(
            valid=df[valid_mask].copy(),
            invalid=df[~valid_mask].copy(),
            errors=errors,
        )


def create_campaign_validator(config: ValidatorConfig) -> CampaignValidator:
    """Factory function to create a CampaignValidator."""
    return CampaignValidator(config)
