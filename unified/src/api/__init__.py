"""
API package.
Provides REST API for external integrations (n8n, etc).
"""
from .app import app, create_app

__all__ = ["app", "create_app"]
