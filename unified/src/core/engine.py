"""
Pipeline engine (orchestrator).
Coordinates the execution of the entire pipeline for a client.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .base import BaseClientExtension, ProcessorResult
from .config import ConfigLoader
from .keys import create_key_generator
from .schemas import ClientConfig, ProcessorType

from ..loaders import create_loader
from ..validators import create_validator
from ..splitters import create_splitter


logger = logging.getLogger(__name__)


@dataclass
class PipelineContext:
    """Context passed through the pipeline."""
    client_config: ClientConfig
    client_data: pd.DataFrame = field(default_factory=pd.DataFrame)
    max_data: pd.DataFrame = field(default_factory=pd.DataFrame)
    start_time: datetime = field(default_factory=datetime.now)
    output_dir: Path = field(default_factory=lambda: Path.cwd())
    metadata: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    outputs: dict[str, Path] = field(default_factory=dict)

    def add_error(self, error: str) -> None:
        """Add an error message to the context."""
        self.errors.append(error)
        logger.error(error)

    def add_output(self, name: str, path: Path) -> None:
        """Register an output file."""
        self.outputs[name] = path


@dataclass
class PipelineResult:
    """Final result of pipeline execution."""
    success: bool
    context: PipelineContext
    duration_seconds: float
    summary: dict[str, Any]


class PipelineEngine:
    """
    Main pipeline orchestrator.
    Loads config, executes pipeline stages, and produces outputs.
    """

    def __init__(
        self,
        config_dir: Path | str | None = None,
        output_dir: Path | str | None = None,
    ):
        self.config_loader = ConfigLoader(config_dir)
        self.output_dir = Path(output_dir) if output_dir else Path.cwd() / "output"
        self._extensions: dict[str, type[BaseClientExtension]] = {}
        self._processors: dict[ProcessorType, type] = {}

    def register_extension(self, name: str, extension_class: type[BaseClientExtension]) -> None:
        """Register a client extension class."""
        self._extensions[name] = extension_class

    def register_processor(self, processor_type: ProcessorType, processor_class: type) -> None:
        """Register a processor class."""
        self._processors[processor_type] = processor_class

    def run(self, client_name: str) -> PipelineResult:
        """
        Run the complete pipeline for a client.

        Args:
            client_name: Name of the client (matches config file)

        Returns:
            PipelineResult with execution details
        """
        start_time = datetime.now()

        # Load configuration
        try:
            config = self.config_loader.load(client_name)
        except Exception as e:
            return PipelineResult(
                success=False,
                context=PipelineContext(
                    client_config=ClientConfig(name=client_name),
                    errors=[f"Failed to load config: {e}"],
                ),
                duration_seconds=0,
                summary={"error": str(e)},
            )

        # Initialize context
        client_output_dir = self.output_dir / client_name / start_time.strftime("%Y%m%d_%H%M%S")
        client_output_dir.mkdir(parents=True, exist_ok=True)

        context = PipelineContext(
            client_config=config,
            start_time=start_time,
            output_dir=client_output_dir,
        )

        # Get extension if specified
        extension = self._get_extension(config)

        try:
            # Stage 1: Load data
            self._load_data(context, extension)

            # Stage 2: Pre-process (extension hook)
            if extension:
                context.client_data = extension.pre_process(context.client_data, "client")
                context.max_data = extension.pre_process(context.max_data, "max")

            # Stage 3: Generate keys
            self._generate_keys(context)

            # Stage 4: Apply validators
            self._apply_validators(context)

            # Stage 5: Run pipeline processors
            self._run_processors(context, extension)

            # Stage 6: Post-process (extension hook)
            if extension:
                context.client_data = extension.post_process(context.client_data, "client")
                context.max_data = extension.post_process(context.max_data, "max")

            success = len(context.errors) == 0

        except Exception as e:
            context.add_error(f"Pipeline failed: {e}")
            if extension:
                extension.on_error(e, "pipeline")
            success = False

        # Calculate duration
        duration = (datetime.now() - start_time).total_seconds()

        # Build summary
        summary = {
            "client": client_name,
            "success": success,
            "duration_seconds": duration,
            "client_records": len(context.client_data),
            "max_records": len(context.max_data),
            "errors": len(context.errors),
            "outputs": {k: str(v) for k, v in context.outputs.items()},
        }

        return PipelineResult(
            success=success,
            context=context,
            duration_seconds=duration,
            summary=summary,
        )

    def run_from_config(self, config: ClientConfig) -> PipelineResult:
        """Run pipeline from an already loaded config."""
        start_time = datetime.now()

        # Initialize context
        client_output_dir = self.output_dir / config.name / start_time.strftime("%Y%m%d_%H%M%S")
        client_output_dir.mkdir(parents=True, exist_ok=True)

        context = PipelineContext(
            client_config=config,
            start_time=start_time,
            output_dir=client_output_dir,
        )

        extension = self._get_extension(config)

        try:
            self._load_data(context, extension)

            if extension:
                context.client_data = extension.pre_process(context.client_data, "client")
                context.max_data = extension.pre_process(context.max_data, "max")

            self._generate_keys(context)
            self._apply_validators(context)
            self._run_processors(context, extension)

            if extension:
                context.client_data = extension.post_process(context.client_data, "client")
                context.max_data = extension.post_process(context.max_data, "max")

            success = len(context.errors) == 0

        except Exception as e:
            context.add_error(f"Pipeline failed: {e}")
            if extension:
                extension.on_error(e, "pipeline")
            success = False

        duration = (datetime.now() - start_time).total_seconds()

        return PipelineResult(
            success=success,
            context=context,
            duration_seconds=duration,
            summary={
                "client": config.name,
                "success": success,
                "duration_seconds": duration,
                "client_records": len(context.client_data),
                "max_records": len(context.max_data),
                "errors": len(context.errors),
            },
        )

    def _get_extension(self, config: ClientConfig) -> BaseClientExtension | None:
        """Get extension instance for client."""
        if config.extension_class and config.extension_class in self._extensions:
            extension_class = self._extensions[config.extension_class]
            return extension_class(config)
        return None

    def _load_data(self, context: PipelineContext, extension: BaseClientExtension | None) -> None:
        """Load data from configured sources."""
        config = context.client_config

        # Load client data
        if config.client_source:
            loader = create_loader(config.client_source.loader, config)
            result = loader.load()
            if "error" in result.metadata:
                context.add_error(f"Client data load error: {result.metadata['error']}")
            context.client_data = result.data
            context.metadata["client_source"] = result.metadata
            logger.info(f"Loaded {len(result.data)} client records")

        # Load MAX data
        if config.max_source:
            loader = create_loader(config.max_source.loader, config)
            result = loader.load()
            if "error" in result.metadata:
                context.add_error(f"MAX data load error: {result.metadata['error']}")
            context.max_data = result.data
            context.metadata["max_source"] = result.metadata
            logger.info(f"Loaded {len(result.data)} MAX records")

    def _generate_keys(self, context: PipelineContext) -> None:
        """Generate CHAVE keys for loaded data."""
        config = context.client_config

        # Generate client keys
        if config.client_source and not context.client_data.empty:
            key_gen = create_key_generator(config.client_source.key)
            context.client_data = key_gen.generate(context.client_data)
            logger.info(f"Generated client keys in column: {key_gen.output_column}")

        # Generate MAX keys
        if config.max_source and not context.max_data.empty:
            key_gen = create_key_generator(config.max_source.key)
            context.max_data = key_gen.generate(context.max_data)
            logger.info(f"Generated MAX keys in column: {key_gen.output_column}")

    def _apply_validators(self, context: PipelineContext) -> None:
        """Apply validators to client data."""
        config = context.client_config

        if not config.client_source or context.client_data.empty:
            return

        for validator_config in config.client_source.validators:
            if not validator_config.enabled:
                continue

            validator = create_validator(validator_config)
            result = validator.validate(context.client_data)

            # Log errors
            for error in result.errors:
                logger.warning(f"Validator {validator.name}: {error}")

            # Keep only valid records (could be configurable)
            context.client_data = result.valid
            logger.info(
                f"Validator {validator.name}: {result.total_valid} valid, "
                f"{result.total_invalid} invalid"
            )

    def _run_processors(
        self, context: PipelineContext, extension: BaseClientExtension | None
    ) -> None:
        """Run configured pipeline processors."""
        config = context.client_config

        for proc_config in config.pipeline.processors:
            if not proc_config.enabled:
                continue

            processor_class = self._processors.get(proc_config.type)
            if not processor_class:
                context.add_error(f"Processor not registered: {proc_config.type}")
                continue

            processor = processor_class(config, proc_config.params)

            try:
                result = processor.process(
                    context.client_data,
                    context.max_data,
                    {"output_dir": context.output_dir, **context.metadata},
                )

                # Update context with results
                context.client_data = result.data
                for error in result.errors:
                    context.add_error(error)
                for path in result.output_files:
                    context.add_output(path.stem, path)

                logger.info(f"Processor {processor.name} completed")

            except Exception as e:
                context.add_error(f"Processor {proc_config.type} failed: {e}")
                if extension:
                    extension.on_error(e, f"processor:{proc_config.type}")
