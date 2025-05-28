"""Flask application factory and configuration."""

import os
from flask import Flask, render_template, send_from_directory
from app.settings import TEMPLATE_DIR, STATIC_DIR, VERSION
from app.routes import register_blueprints

def create_app():
    """Create and configure the Flask app."""
    app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)
    app.secret_key = os.environ.get("JABS_SECRET_KEY", "dev-secret-key")
    app.config['APP_ENV'] = os.getenv('APP_ENV', 'production')
    register_blueprints(app)

    @app.errorhandler(404)
    def page_not_found(_):
        return render_template('404.html'), 404

    @app.context_processor
    def inject_version():
        return {"VERSION": VERSION}

    @app.route('/favicon.ico')
    def favicon():
        return send_from_directory(
            os.path.join(app.root_path, 'static'),
            'favicon.ico',
            mimetype='image/vnd.microsoft.icon'
        )

    return app
