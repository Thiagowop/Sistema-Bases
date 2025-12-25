"""
REST API for the Unified Pipeline System.
Provides HTTP endpoints for n8n and external integrations.
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request

from ..core import (
    ConfigLoader,
    PipelineEngine,
    ProcessorType,
)
from ..processors import (
    TratamentoProcessor,
    BatimentoProcessor,
    BaixaProcessor,
    DevolucaoProcessor,
    EnriquecimentoProcessor,
)


# Initialize Flask app
app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global configuration
CONFIG_DIR = Path("./configs/clients")
OUTPUT_DIR = Path("./output")


def get_engine() -> PipelineEngine:
    """Create and configure a pipeline engine."""
    engine = PipelineEngine(config_dir=CONFIG_DIR, output_dir=OUTPUT_DIR)

    # Register processors
    engine.register_processor(ProcessorType.TRATAMENTO, TratamentoProcessor)
    engine.register_processor(ProcessorType.BATIMENTO, BatimentoProcessor)
    engine.register_processor(ProcessorType.BAIXA, BaixaProcessor)
    engine.register_processor(ProcessorType.DEVOLUCAO, DevolucaoProcessor)
    engine.register_processor(ProcessorType.ENRIQUECIMENTO, EnriquecimentoProcessor)

    return engine


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0",
    })


@app.route("/clients", methods=["GET"])
def list_clients():
    """List available client configurations."""
    try:
        config_files = list(CONFIG_DIR.glob("*.yaml")) + list(CONFIG_DIR.glob("*.yml"))
        loader = ConfigLoader(CONFIG_DIR)

        clients = []
        for config_path in sorted(config_files):
            try:
                config = loader.load_from_file(config_path)
                clients.append({
                    "name": config.name,
                    "version": config.version,
                    "description": config.description,
                })
            except Exception as e:
                clients.append({
                    "name": config_path.stem,
                    "error": str(e),
                })

        return jsonify({
            "success": True,
            "clients": clients,
            "count": len(clients),
        })

    except Exception as e:
        logger.error(f"Error listing clients: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
        }), 500


@app.route("/clients/<client_name>", methods=["GET"])
def get_client_config(client_name: str):
    """Get client configuration details."""
    try:
        loader = ConfigLoader(CONFIG_DIR)
        config = loader.load(client_name)

        return jsonify({
            "success": True,
            "client": {
                "name": config.name,
                "version": config.version,
                "description": config.description,
                "has_client_source": config.client_source is not None,
                "has_max_source": config.max_source is not None,
                "processors": [
                    {
                        "type": p.type.value,
                        "enabled": p.enabled,
                    }
                    for p in config.pipeline.processors
                ],
            },
        })

    except Exception as e:
        logger.error(f"Error getting client config: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
        }), 404


@app.route("/run/<client_name>", methods=["POST"])
def run_pipeline(client_name: str):
    """
    Run pipeline for a specific client.

    Request body (optional):
    {
        "processors": ["tratamento", "batimento"],  // optional: specific processors
        "output_format": "zip",  // optional: output format
    }
    """
    try:
        # Get request parameters
        params = request.get_json(silent=True) or {}

        logger.info(f"Starting pipeline for client: {client_name}")

        # Create engine and run
        engine = get_engine()
        result = engine.run(client_name)

        # Build response
        response = {
            "success": result.success,
            "client": client_name,
            "duration_seconds": result.duration_seconds,
            "summary": {
                "client_records": result.summary.get("client_records", 0),
                "max_records": result.summary.get("max_records", 0),
                "error_count": len(result.context.errors),
            },
            "outputs": {k: str(v) for k, v in result.context.outputs.items()},
            "errors": result.context.errors if not result.success else [],
        }

        status_code = 200 if result.success else 500
        return jsonify(response), status_code

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
        }), 500


@app.route("/run/<client_name>/async", methods=["POST"])
def run_pipeline_async(client_name: str):
    """
    Start pipeline execution asynchronously.
    Returns a job ID that can be used to check status.

    Note: This is a placeholder for async execution.
    In production, use Celery or similar task queue.
    """
    try:
        # For now, just validate the client exists
        loader = ConfigLoader(CONFIG_DIR)
        config = loader.load(client_name)

        # Generate job ID
        job_id = f"{client_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # TODO: In production, queue the job with Celery/RQ
        logger.info(f"Async job created: {job_id}")

        return jsonify({
            "success": True,
            "job_id": job_id,
            "client": client_name,
            "status": "queued",
            "message": "Pipeline execution queued. Use /jobs/{job_id} to check status.",
        }), 202

    except Exception as e:
        logger.error(f"Failed to queue pipeline: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
        }), 500


@app.route("/jobs/<job_id>", methods=["GET"])
def get_job_status(job_id: str):
    """
    Get status of an async job.

    Note: Placeholder for job status tracking.
    """
    return jsonify({
        "job_id": job_id,
        "status": "not_implemented",
        "message": "Async job tracking not yet implemented",
    }), 501


@app.route("/validate/<client_name>", methods=["POST"])
def validate_config(client_name: str):
    """Validate a client configuration."""
    try:
        loader = ConfigLoader(CONFIG_DIR)
        config = loader.load(client_name)

        validation_results = {
            "name_valid": bool(config.name),
            "has_client_source": config.client_source is not None,
            "has_max_source": config.max_source is not None,
            "has_processors": len(config.pipeline.processors) > 0,
        }

        issues = []
        if not config.client_source:
            issues.append("No client data source configured")
        if not config.max_source:
            issues.append("No MAX data source configured")
        if not config.pipeline.processors:
            issues.append("No processors configured")

        return jsonify({
            "success": True,
            "client": client_name,
            "valid": len(issues) == 0,
            "validation": validation_results,
            "issues": issues,
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "client": client_name,
            "valid": False,
            "error": str(e),
        }), 400


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors."""
    return jsonify({
        "success": False,
        "error": "Resource not found",
    }), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    return jsonify({
        "success": False,
        "error": "Internal server error",
    }), 500


def create_app(config_dir: str = None, output_dir: str = None) -> Flask:
    """
    Application factory for creating configured Flask app.

    Args:
        config_dir: Directory containing client configs
        output_dir: Directory for pipeline outputs

    Returns:
        Configured Flask application
    """
    global CONFIG_DIR, OUTPUT_DIR

    if config_dir:
        CONFIG_DIR = Path(config_dir)
    if output_dir:
        OUTPUT_DIR = Path(output_dir)

    return app


if __name__ == "__main__":
    # Run development server
    app.run(host="0.0.0.0", port=5000, debug=True)
