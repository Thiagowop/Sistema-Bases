"""
Line break validator.
Detects and filters records with internal line breaks in text fields.
Used by Tabelionato to identify malformed records.
"""
from __future__ import annotations

import pandas as pd

from ..core.base import BaseValidator, ValidationResult
from ..core.schemas import ValidatorConfig


class LineBreakValidator(BaseValidator):
    """Detects records with internal line breaks in specified columns."""

    @property
    def name(self) -> str:
        return "linebreak"

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        if not self.enabled or df.empty:
            return ValidationResult(valid=df, invalid=pd.DataFrame(), errors=[])

        # Columns to check for line breaks
        columns = self.params.get("columns", [])
        check_all = self.params.get("check_all", False)
        action = self.params.get("action", "exclude")  # exclude, flag, clean

        if check_all:
            # Check all string columns
            columns = [col for col in df.columns if df[col].dtype == object]

        if not columns:
            return ValidationResult(valid=df, invalid=pd.DataFrame(), errors=[])

        # Build mask for records with line breaks
        has_linebreak = pd.Series(False, index=df.index)

        for col in columns:
            if col not in df.columns:
                continue
            # Check for various line break characters
            col_has_break = (
                df[col].astype(str).str.contains(r'[\n\r]', na=False, regex=True)
            )
            has_linebreak = has_linebreak | col_has_break

        affected_count = has_linebreak.sum()
        errors = []

        if affected_count > 0:
            errors.append(f"{affected_count} records have internal line breaks")

        if action == "exclude":
            return ValidationResult(
                valid=df[~has_linebreak].copy(),
                invalid=df[has_linebreak].copy(),
                errors=errors,
            )
        elif action == "flag":
            # Add flag column but keep all records
            df = df.copy()
            df["_HAS_LINEBREAK"] = has_linebreak
            return ValidationResult(
                valid=df,
                invalid=pd.DataFrame(),
                errors=errors,
            )
        elif action == "clean":
            # Clean line breaks from specified columns
            df = df.copy()
            for col in columns:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.replace(r'[\n\r]+', ' ', regex=True)
            return ValidationResult(
                valid=df,
                invalid=pd.DataFrame(),
                errors=[f"Cleaned line breaks from {affected_count} records"],
            )
        else:
            return ValidationResult(
                valid=df[~has_linebreak].copy(),
                invalid=df[has_linebreak].copy(),
                errors=errors,
            )


def create_linebreak_validator(config: ValidatorConfig) -> LineBreakValidator:
    """Factory function to create a LineBreakValidator."""
    return LineBreakValidator(config)
