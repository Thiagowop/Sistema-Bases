"""
Type filter validator.
Filters records based on type/category field values.
Used by VIC (TIPO_PARCELA) and Emccamp (TIPO_PAGTO).
"""
from __future__ import annotations

import pandas as pd

from ..core.base import BaseValidator, ValidationResult
from ..core.schemas import ValidatorConfig


class TypeFilterValidator(BaseValidator):
    """Filters records based on type/category field values."""

    @property
    def name(self) -> str:
        return "type_filter"

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        if not self.enabled or df.empty:
            return ValidationResult(valid=df, invalid=pd.DataFrame(), errors=[])

        column = self.params.get("column", "TIPO_PARCELA")
        include = self.params.get("include", [])
        exclude = self.params.get("exclude", [])
        case_sensitive = self.params.get("case_sensitive", False)
        match_mode = self.params.get("match_mode", "exact")  # exact, contains, startswith

        if column not in df.columns:
            return ValidationResult(
                valid=df,
                invalid=pd.DataFrame(),
                errors=[f"Type column '{column}' not found"],
            )

        # Normalize values for comparison
        if case_sensitive:
            values = df[column].astype(str).str.strip()
        else:
            values = df[column].astype(str).str.strip().str.upper()

        valid_mask = pd.Series(True, index=df.index)
        errors = []

        # Apply include filter
        if include:
            if not case_sensitive:
                include = [str(v).upper() for v in include]

            if match_mode == "exact":
                include_mask = values.isin(include)
            elif match_mode == "contains":
                include_mask = pd.Series(False, index=df.index)
                for pattern in include:
                    include_mask = include_mask | values.str.contains(pattern, na=False, regex=False)
            elif match_mode == "startswith":
                include_mask = pd.Series(False, index=df.index)
                for pattern in include:
                    include_mask = include_mask | values.str.startswith(pattern, na=False)
            else:
                include_mask = values.isin(include)

            excluded = (~include_mask).sum()
            if excluded > 0:
                errors.append(f"{excluded} records excluded (type not in allowed list)")
            valid_mask = valid_mask & include_mask

        # Apply exclude filter
        if exclude:
            if not case_sensitive:
                exclude = [str(v).upper() for v in exclude]

            if match_mode == "exact":
                exclude_mask = values.isin(exclude)
            elif match_mode == "contains":
                exclude_mask = pd.Series(False, index=df.index)
                for pattern in exclude:
                    exclude_mask = exclude_mask | values.str.contains(pattern, na=False, regex=False)
            elif match_mode == "startswith":
                exclude_mask = pd.Series(False, index=df.index)
                for pattern in exclude:
                    exclude_mask = exclude_mask | values.str.startswith(pattern, na=False)
            else:
                exclude_mask = values.isin(exclude)

            excluded = exclude_mask.sum()
            if excluded > 0:
                errors.append(f"{excluded} records excluded (type in exclusion list)")
            valid_mask = valid_mask & ~exclude_mask

        return ValidationResult(
            valid=df[valid_mask].copy(),
            invalid=df[~valid_mask].copy(),
            errors=errors,
        )


def create_type_filter_validator(config: ValidatorConfig) -> TypeFilterValidator:
    """Factory function to create a TypeFilterValidator."""
    return TypeFilterValidator(config)
