"""
Regex validator.
Validates column values against regular expression patterns.
"""
from __future__ import annotations

import re

import pandas as pd

from ..core.base import BaseValidator, ValidationResult
from ..core.schemas import ValidatorConfig


class RegexValidator(BaseValidator):
    """Validates column values against regex patterns."""

    @property
    def name(self) -> str:
        return "regex"

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        if not self.enabled or df.empty:
            return ValidationResult(valid=df, invalid=pd.DataFrame(), errors=[])

        column = self.params.get("column")
        pattern = self.params.get("pattern")
        mode = self.params.get("mode", "match")  # match, fullmatch, search

        if not column or not pattern:
            return ValidationResult(
                valid=df,
                invalid=pd.DataFrame(),
                errors=["Regex validator requires 'column' and 'pattern' params"],
            )

        if column not in df.columns:
            return ValidationResult(
                valid=df,
                invalid=pd.DataFrame(),
                errors=[f"Column '{column}' not found"],
            )

        try:
            compiled = re.compile(pattern)
        except re.error as e:
            return ValidationResult(
                valid=df,
                invalid=pd.DataFrame(),
                errors=[f"Invalid regex pattern: {e}"],
            )

        # Apply regex validation
        values = df[column].astype(str).fillna("")

        if mode == "fullmatch":
            valid_mask = values.apply(lambda x: bool(compiled.fullmatch(x)))
        elif mode == "search":
            valid_mask = values.apply(lambda x: bool(compiled.search(x)))
        else:  # match (default)
            valid_mask = values.apply(lambda x: bool(compiled.match(x)))

        invalid_count = (~valid_mask).sum()
        errors = []
        if invalid_count > 0:
            errors.append(
                f"{invalid_count} records in '{column}' don't match pattern '{pattern}'"
            )

        return ValidationResult(
            valid=df[valid_mask].copy(),
            invalid=df[~valid_mask].copy(),
            errors=errors,
        )


def create_regex_validator(config: ValidatorConfig) -> RegexValidator:
    """Factory function to create a RegexValidator."""
    return RegexValidator(config)
