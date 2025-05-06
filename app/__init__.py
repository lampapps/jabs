import os
from flask import Flask
from .settings import TEMPLATE_DIR, STATIC_DIR
from .routes import register_blueprints

def create_app():
    app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)
    app.secret_key = os.environ.get("JABS_SECRET_KEY", "dev-secret-key")
    register_blueprints(app)
    return app