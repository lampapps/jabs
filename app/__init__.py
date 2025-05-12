import os
import logging
from flask import Flask, render_template
from app.settings import TEMPLATE_DIR, STATIC_DIR, LOG_DIR
from app.routes import register_blueprints
from app.utils.logger import ensure_dir

def create_app():
    app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)
    app.secret_key = os.environ.get("JABS_SECRET_KEY", "dev-secret-key")
    register_blueprints(app)

    # Ensure log directory exists
    ensure_dir(LOG_DIR)

    # Set up Werkzeug (HTTP server) logging to file in LOG_DIR/server.log
    server_log_path = os.path.join(LOG_DIR, 'server.log')
    werkzeug_logger = logging.getLogger('werkzeug')

    # Remove any existing FileHandlers to avoid duplicate logs
    for handler in list(werkzeug_logger.handlers):
        if isinstance(handler, logging.FileHandler):
            werkzeug_logger.removeHandler(handler)

    # Attach a plain FileHandler (no formatter) for raw HTTP logs
    file_handler = logging.FileHandler(server_log_path)
    werkzeug_logger.addHandler(file_handler)
    werkzeug_logger.setLevel(logging.INFO)
    werkzeug_logger.propagate = False  # Prevent double logging

    @app.errorhandler(404)
    def page_not_found(e):
        return render_template('404.html'), 404

    return app