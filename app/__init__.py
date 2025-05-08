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

    # Set up Flask app logging to file
    file_handler = logging.FileHandler(os.path.join(LOG_DIR, 'server.log'))
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
    file_handler.setFormatter(formatter)

    app.logger.setLevel(logging.INFO)
    if not any(isinstance(h, logging.FileHandler) for h in app.logger.handlers):
        app.logger.addHandler(file_handler)

    # Add this for Werkzeug access logs
    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.setLevel(logging.INFO)
    if not any(isinstance(h, logging.FileHandler) for h in werkzeug_logger.handlers):
        werkzeug_logger.addHandler(file_handler)

    @app.errorhandler(404)
    def page_not_found(e):
        return render_template('404.html'), 404

    return app