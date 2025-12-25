"""
Validators package.
Provides data validation components for the pipeline.
"""
from __future__ import annotations

from typing import Callable

from ..core.base import BaseValidator, ValidationResult
from ..core.schemas import ValidatorConfig, ValidatorType

from .required import RequiredValidator, create_required_validator
from .aging import AgingValidator, create_aging_validator
from .blacklist import BlacklistValidator, create_blacklist_validator
from .regex import RegexValidator, create_regex_validator
from .campaign import CampaignValidator, create_campaign_validator
from .status import StatusValidator, create_status_validator
from .type_filter import TypeFilterValidator, create_type_filter_validator
from .linebreak import LineBreakValidator, create_linebreak_validator
from .daterange import DateRangeValidator, create_daterange_validator


# Registry of validator factories
_VALIDATOR_REGISTRY: dict[ValidatorType, Callable[[ValidatorConfig], BaseValidator]] = {
    ValidatorType.REQUIRED: create_required_validator,
    ValidatorType.AGING: create_aging_validator,
    ValidatorType.BLACKLIST: create_blacklist_validator,
    ValidatorType.REGEX: create_regex_validator,
    ValidatorType.CAMPAIGN: create_campaign_validator,
    ValidatorType.STATUS: create_status_validator,
    ValidatorType.TYPE_FILTER: create_type_filter_validator,
    ValidatorType.LINEBREAK: create_linebreak_validator,
    ValidatorType.DATERANGE: create_daterange_validator,
}


def create_validator(config: ValidatorConfig) -> BaseValidator:
    """
    Factory function to create a validator based on configuration.

    Args:
        config: Validator configuration

    Returns:
        Configured validator instance

    Raises:
        ValueError: If validator type is not registered
    """
    factory = _VALIDATOR_REGISTRY.get(config.type)
    if factory is None:
        raise ValueError(f"Unknown validator type: {config.type}")
    return factory(config)


def register_validator(
    validator_type: ValidatorType,
    factory: Callable[[ValidatorConfig], BaseValidator],
) -> None:
    """
    Register a custom validator factory.

    Args:
        validator_type: The validator type to register
        factory: Factory function that creates the validator
    """
    _VALIDATOR_REGISTRY[validator_type] = factory


__all__ = [
    "BaseValidator",
    "ValidationResult",
    "ValidatorConfig",
    "ValidatorType",
    "RequiredValidator",
    "AgingValidator",
    "BlacklistValidator",
    "RegexValidator",
    "CampaignValidator",
    "StatusValidator",
    "TypeFilterValidator",
    "LineBreakValidator",
    "DateRangeValidator",
    "create_validator",
    "register_validator",
]
