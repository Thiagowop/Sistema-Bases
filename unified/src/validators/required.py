"""
Required fields validator.
Validates that specified columns have non-null, non-empty values.
"""
from __future__ import annotations

import pandas as pd

from ..core.base import BaseValidator, ValidationResult
from ..core.schemas import ValidatorConfig


class RequiredValidator(BaseValidator):
    """Validates that required columns are present and non-empty."""

    @property
    def name(self) -> str:
        return "required"

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        if not self.enabled or df.empty:
            return ValidationResult(valid=df, invalid=pd.DataFrame(), errors=[])

        columns = self.params.get("columns", [])
        if not columns:
            return ValidationResult(valid=df, invalid=pd.DataFrame(), errors=[])

        # Check which columns exist
        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            return ValidationResult(
                valid=pd.DataFrame(),
                invalid=df,
                errors=[f"Missing required columns: {missing_cols}"],
            )

        # Build mask for valid rows (all required columns have values)
        valid_mask = pd.Series(True, index=df.index)
        errors = []

        for col in columns:
            col_valid = df[col].notna() & (df[col].astype(str).str.strip() != "")
            invalid_count = (~col_valid).sum()
            if invalid_count > 0:
                errors.append(f"Column '{col}' has {invalid_count} empty/null values")
            valid_mask = valid_mask & col_valid

        return ValidationResult(
            valid=df[valid_mask].copy(),
            invalid=df[~valid_mask].copy(),
            errors=errors,
        )


def create_required_validator(config: ValidatorConfig) -> RequiredValidator:
    """Factory function to create a RequiredValidator."""
    return RequiredValidator(config)
