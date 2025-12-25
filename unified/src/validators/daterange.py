"""
Date range validator.
Validates that date fields are within acceptable ranges.
Used by Tabelionato to reject dates before 1900.
"""
from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from ..core.base import BaseValidator, ValidationResult
from ..core.schemas import ValidatorConfig


class DateRangeValidator(BaseValidator):
    """Validates that date columns are within acceptable ranges."""

    @property
    def name(self) -> str:
        return "daterange"

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        if not self.enabled or df.empty:
            return ValidationResult(valid=df, invalid=pd.DataFrame(), errors=[])

        column = self.params.get("column", "VENCIMENTO")
        min_year = self.params.get("min_year", 1900)
        max_year = self.params.get("max_year", 2100)
        min_date = self.params.get("min_date")  # format: YYYY-MM-DD
        max_date = self.params.get("max_date")  # format: YYYY-MM-DD
        null_action = self.params.get("null_action", "include")  # include, exclude

        if column not in df.columns:
            return ValidationResult(
                valid=df,
                invalid=pd.DataFrame(),
                errors=[f"Date column '{column}' not found"],
            )

        # Parse dates
        date_series = pd.to_datetime(df[column], errors="coerce", dayfirst=True)

        valid_mask = pd.Series(True, index=df.index)
        errors = []

        # Handle null dates
        null_mask = date_series.isna()
        if null_mask.any():
            if null_action == "exclude":
                valid_mask = valid_mask & ~null_mask
                errors.append(f"{null_mask.sum()} records with null dates excluded")

        # Build min/max date thresholds
        min_threshold = None
        max_threshold = None

        if min_date:
            try:
                min_threshold = pd.Timestamp(min_date)
            except Exception:
                min_threshold = pd.Timestamp(f"{min_year}-01-01")
        else:
            min_threshold = pd.Timestamp(f"{min_year}-01-01")

        if max_date:
            try:
                max_threshold = pd.Timestamp(max_date)
            except Exception:
                max_threshold = pd.Timestamp(f"{max_year}-12-31")
        else:
            max_threshold = pd.Timestamp(f"{max_year}-12-31")

        # Apply date range filters (only on non-null dates)
        non_null_mask = ~null_mask

        if min_threshold:
            before_min = non_null_mask & (date_series < min_threshold)
            if before_min.any():
                valid_mask = valid_mask & ~before_min
                errors.append(
                    f"{before_min.sum()} records before {min_threshold.date()} excluded"
                )

        if max_threshold:
            after_max = non_null_mask & (date_series > max_threshold)
            if after_max.any():
                valid_mask = valid_mask & ~after_max
                errors.append(
                    f"{after_max.sum()} records after {max_threshold.date()} excluded"
                )

        return ValidationResult(
            valid=df[valid_mask].copy(),
            invalid=df[~valid_mask].copy(),
            errors=errors,
        )


def create_daterange_validator(config: ValidatorConfig) -> DateRangeValidator:
    """Factory function to create a DateRangeValidator."""
    return DateRangeValidator(config)
