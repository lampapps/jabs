"""Blueprint registration for app.routes."""

from .dashboard import dashboard_bp
from .config import config_bp
from .jobs import jobs_bp
from .api import api_bp
from .logs import logs_bp

def register_blueprints(app):
    """Register all blueprints with the Flask app."""
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(config_bp)
    app.register_blueprint(jobs_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(logs_bp)
    