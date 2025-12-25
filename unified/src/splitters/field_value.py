"""
Field value splitter.
Splits data into groups based on field values.
"""
from __future__ import annotations

import pandas as pd

from ..core.base import BaseSplitter, SplitResult
from ..core.schemas import SplitterConfig


class FieldValueSplitter(BaseSplitter):
    """Splits records into groups based on field values."""

    @property
    def name(self) -> str:
        return "field_value"

    def split(self, df: pd.DataFrame) -> SplitResult:
        if not self.enabled or df.empty:
            return SplitResult(splits={"default": df})

        column = self.params.get("column")
        mode = self.params.get("mode", "exact")  # exact, contains, prefix, suffix
        normalize = self.params.get("normalize", True)
        mappings = self.params.get("mappings", {})
        default_group = self.params.get("default_group", "outros")

        if not column or column not in df.columns:
            return SplitResult(splits={"default": df})

        # Get values to split on
        if normalize:
            values = df[column].astype(str).str.strip().str.upper()
        else:
            values = df[column].astype(str)

        splits: dict[str, pd.DataFrame] = {}
        remaining_mask = pd.Series(True, index=df.index)

        # Process each mapping
        for group_name, match_values in mappings.items():
            if not isinstance(match_values, list):
                match_values = [match_values]

            match_mask = pd.Series(False, index=df.index)
            for match_value in match_values:
                if normalize:
                    match_str = str(match_value).strip().upper()
                else:
                    match_str = str(match_value)

                if mode == "exact":
                    match_mask = match_mask | (values == match_str)
                elif mode == "contains":
                    match_mask = match_mask | values.str.contains(
                        match_str, na=False, regex=False
                    )
                elif mode == "prefix":
                    match_mask = match_mask | values.str.startswith(match_str, na=False)
                elif mode == "suffix":
                    match_mask = match_mask | values.str.endswith(match_str, na=False)

            # Only consider records not already assigned
            group_mask = remaining_mask & match_mask
            if group_mask.any():
                splits[group_name] = df[group_mask].copy()
                remaining_mask = remaining_mask & ~group_mask

        # Add remaining to default group
        if remaining_mask.any():
            splits[default_group] = df[remaining_mask].copy()

        return SplitResult(splits=splits)


class UniqueValueSplitter(BaseSplitter):
    """Splits records into groups based on unique values of a column."""

    @property
    def name(self) -> str:
        return "unique_value"

    def split(self, df: pd.DataFrame) -> SplitResult:
        if not self.enabled or df.empty:
            return SplitResult(splits={"default": df})

        column = self.params.get("column")
        normalize = self.params.get("normalize", True)
        prefix = self.params.get("prefix", "")
        max_groups = self.params.get("max_groups", 100)

        if not column or column not in df.columns:
            return SplitResult(splits={"default": df})

        # Get unique values
        if normalize:
            df_copy = df.copy()
            df_copy["_split_key"] = df[column].astype(str).str.strip().str.upper()
        else:
            df_copy = df.copy()
            df_copy["_split_key"] = df[column].astype(str)

        unique_values = df_copy["_split_key"].unique()

        # Limit number of groups
        if len(unique_values) > max_groups:
            unique_values = unique_values[:max_groups]

        splits: dict[str, pd.DataFrame] = {}
        for value in unique_values:
            group_name = f"{prefix}{value}" if prefix else str(value)
            mask = df_copy["_split_key"] == value
            if mask.any():
                splits[group_name] = df[mask].copy()

        return SplitResult(splits=splits)


def create_field_value_splitter(config: SplitterConfig) -> FieldValueSplitter:
    """Factory function to create a FieldValueSplitter."""
    return FieldValueSplitter(config)


def create_unique_value_splitter(config: SplitterConfig) -> UniqueValueSplitter:
    """Factory function to create a UniqueValueSplitter."""
    return UniqueValueSplitter(config)
