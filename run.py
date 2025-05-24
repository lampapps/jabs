import os
import secrets
import logging
from dotenv import load_dotenv
from app.settings import LOG_DIR
from app.utils.logger import ensure_dir  # Use your existing util

env_path = os.path.join(os.path.dirname(__file__), ".env")
if not os.path.exists(env_path):
    with open(env_path, "w") as f:
        pass  # Create an empty .env file

# Load .env file (by default, looks in current directory)
load_dotenv(env_path)

# Get the passphrase
PASSPHRASE = os.getenv("JABS_ENCRYPT_PASSPHRASE")

# Generate a random secret key if not set
if "JABS_SECRET_KEY" not in os.environ:
    os.environ["JABS_SECRET_KEY"] = secrets.token_urlsafe(32)

from app import create_app

app = create_app()

class AccessLogMiddleware:
    def __init__(self, app):
        self.app = app
        self.logger = logging.getLogger("waitress.access")

    def __call__(self, environ, start_response):
        def custom_start_response(status, headers, exc_info=None):
            self.status = status
            return start_response(status, headers, exc_info)
        result = self.app(environ, custom_start_response)
        # Log after response is generated
        self.logger.info(
            '%s - - "%s %s" %s',
            environ.get("REMOTE_ADDR", "-"),
            environ.get("REQUEST_METHOD", "-"),
            environ.get("PATH_INFO", "-"),
            self.status.split()[0] if hasattr(self, "status") else "-"
        )
        return result

if __name__ == "__main__":
    try:
        from waitress import serve

        # Ensure log directory exists
        ensure_dir(LOG_DIR)
        server_log_path = os.path.join(LOG_DIR, "server.log")

        # Configure logging for Waitress and the app
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s: %(message)s",
            handlers=[
                logging.FileHandler(server_log_path),
                logging.StreamHandler()
            ]
        )

        # Wrap your app with the access log middleware
        app_with_access_log = AccessLogMiddleware(app)

        serve(app_with_access_log, host="0.0.0.0", port=5000)
    except ImportError:
        print("Waitress is not installed. Falling back to Flask's built-in server.")
        app.run(host="0.0.0.0", port=5000, debug=True)