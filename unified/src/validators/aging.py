"""
Aging validator.
Validates records based on date field (e.g., vencimento).
Filters out records older/newer than specified thresholds.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd

from ..core.base import BaseValidator, ValidationResult
from ..core.schemas import ValidatorConfig


class AgingValidator(BaseValidator):
    """Validates records based on date thresholds."""

    @property
    def name(self) -> str:
        return "aging"

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        if not self.enabled or df.empty:
            return ValidationResult(valid=df, invalid=pd.DataFrame(), errors=[])

        date_column = self.params.get("date_column", "VENCIMENTO")
        if date_column not in df.columns:
            return ValidationResult(
                valid=df,
                invalid=pd.DataFrame(),
                errors=[f"Date column '{date_column}' not found, skipping aging validation"],
            )

        # Parse date thresholds
        min_date = self._parse_date(self.params.get("min_date"))
        max_date = self._parse_date(self.params.get("max_date"))
        min_age_days = self.params.get("min_age_days")
        max_age_days = self.params.get("max_age_days")

        # Convert to date thresholds if using age_days
        today = date.today()
        if min_age_days is not None:
            max_date = today - timedelta(days=min_age_days)
        if max_age_days is not None:
            min_date = today - timedelta(days=max_age_days)

        # Convert date column to datetime
        df_copy = df.copy()
        date_series = pd.to_datetime(df_copy[date_column], errors="coerce", dayfirst=True)

        # Build mask
        valid_mask = pd.Series(True, index=df.index)
        errors = []

        # Handle null dates
        null_dates = date_series.isna()
        if null_dates.any():
            null_action = self.params.get("null_action", "include")
            if null_action == "exclude":
                valid_mask = valid_mask & ~null_dates
                errors.append(f"{null_dates.sum()} records with null dates excluded")
            elif null_action == "include":
                pass  # Keep null dates as valid

        # Apply date range filters (only on non-null dates)
        non_null_mask = ~null_dates

        if min_date is not None:
            min_dt = pd.Timestamp(min_date)
            too_old = non_null_mask & (date_series < min_dt)
            if too_old.any():
                valid_mask = valid_mask & ~too_old
                errors.append(f"{too_old.sum()} records before {min_date} excluded")

        if max_date is not None:
            max_dt = pd.Timestamp(max_date)
            too_new = non_null_mask & (date_series > max_dt)
            if too_new.any():
                valid_mask = valid_mask & ~too_new
                errors.append(f"{too_new.sum()} records after {max_date} excluded")

        return ValidationResult(
            valid=df[valid_mask].copy(),
            invalid=df[~valid_mask].copy(),
            errors=errors,
        )

    def _parse_date(self, value: str | date | None) -> date | None:
        """Parse a date value from string or date object."""
        if value is None:
            return None
        if isinstance(value, date):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            if value.upper() == "TODAY":
                return date.today()
            try:
                return datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                try:
                    return datetime.strptime(value, "%d/%m/%Y").date()
                except ValueError:
                    return None
        return None


def create_aging_validator(config: ValidatorConfig) -> AgingValidator:
    """Factory function to create an AgingValidator."""
    return AgingValidator(config)
