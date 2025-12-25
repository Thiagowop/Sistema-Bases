"""
Status validator.
Filters records based on status field values.
Used by VIC to filter STATUS_TITULO = "EM ABERTO".
"""
from __future__ import annotations

import pandas as pd

from ..core.base import BaseValidator, ValidationResult
from ..core.schemas import ValidatorConfig


class StatusValidator(BaseValidator):
    """Filters records based on status field values."""

    @property
    def name(self) -> str:
        return "status"

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        if not self.enabled or df.empty:
            return ValidationResult(valid=df, invalid=pd.DataFrame(), errors=[])

        column = self.params.get("column", "STATUS_TITULO")
        include = self.params.get("include", [])
        exclude = self.params.get("exclude", [])
        case_sensitive = self.params.get("case_sensitive", False)

        if column not in df.columns:
            return ValidationResult(
                valid=df,
                invalid=pd.DataFrame(),
                errors=[f"Status column '{column}' not found"],
            )

        # Normalize values for comparison
        if case_sensitive:
            values = df[column].astype(str).str.strip()
        else:
            values = df[column].astype(str).str.strip().str.upper()

        valid_mask = pd.Series(True, index=df.index)
        errors = []

        # Apply include filter (must match at least one)
        if include:
            if not case_sensitive:
                include = [str(v).upper() for v in include]
            include_mask = values.isin(include)
            excluded = (~include_mask).sum()
            if excluded > 0:
                errors.append(
                    f"{excluded} records excluded (status not in: {include})"
                )
            valid_mask = valid_mask & include_mask

        # Apply exclude filter
        if exclude:
            if not case_sensitive:
                exclude = [str(v).upper() for v in exclude]
            exclude_mask = values.isin(exclude)
            excluded = exclude_mask.sum()
            if excluded > 0:
                errors.append(
                    f"{excluded} records excluded (status in: {exclude})"
                )
            valid_mask = valid_mask & ~exclude_mask

        return ValidationResult(
            valid=df[valid_mask].copy(),
            invalid=df[~valid_mask].copy(),
            errors=errors,
        )


def create_status_validator(config: ValidatorConfig) -> StatusValidator:
    """Factory function to create a StatusValidator."""
    return StatusValidator(config)
