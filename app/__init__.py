import os
import logging
from flask import Flask, render_template
from app.settings import TEMPLATE_DIR, STATIC_DIR, LOG_DIR, VERSION
from app.routes import register_blueprints
from app.utils.logger import ensure_dir

def create_app():
    app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)
    app.secret_key = os.environ.get("JABS_SECRET_KEY", "dev-secret-key")
    register_blueprints(app)

    @app.errorhandler(404)
    def page_not_found(e):
        return render_template('404.html'), 404

    @app.context_processor
    def inject_version():
        return dict(VERSION=VERSION)

    return app