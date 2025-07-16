"""Entry point for running the JABS web application."""

import os
import secrets
import logging
import socket
from dotenv import load_dotenv
from app.settings import LOG_DIR
from app.utils.logger import ensure_dir
from app import create_app
from app.models.db_core import init_db

env_path = os.path.join(os.path.dirname(__file__), ".env")
if not os.path.exists(env_path):
    with open(env_path, "w", encoding="utf-8") as f:
        pass  # Create an empty .env file

# Load .env file (by default, looks in current directory)
load_dotenv(env_path)

# Get the passphrase
PASSPHRASE = os.getenv("JABS_ENCRYPT_PASSPHRASE")

# Generate a random secret key if not set
if "JABS_SECRET_KEY" not in os.environ:
    os.environ["JABS_SECRET_KEY"] = secrets.token_urlsafe(32)

app = create_app()

init_db()

class AccessLogMiddleware:
    """WSGI middleware for logging HTTP access logs in Waitress style."""
    def __init__(self, wsgi_app):
        self.app = wsgi_app
        self.logger = logging.getLogger("waitress.access")
        self.status = "-"

    def __call__(self, environ, start_response):
        def custom_start_response(status, headers, exc_info=None):
            self.status = status
            return start_response(status, headers, exc_info)
        result = self.app(environ, custom_start_response)
        self.logger.info(
            '%s - - "%s %s" %s',
            environ.get("REMOTE_ADDR", "-"),
            environ.get("REQUEST_METHOD", "-"),
            environ.get("PATH_INFO", "-"),
            self.status.split()[0] if hasattr(self, "status") else "-"
        )
        return result

def get_local_ip():
    """Get the primary local IP address of the machine."""
    try:
        # This doesn't have to be reachable, just a valid IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

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

        # Get local IP addresses
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        primary_ip = get_local_ip()
        print("\n" + "="*60)
        print("JABS server is starting!")
        print(f"Open your browser and go to: http://{local_ip}:5000")
        if primary_ip != local_ip:
            print(f"Or try: http://{primary_ip}:5000")
        print("\nTo stop the server, press Ctrl+C in this terminal.")
        print("="*60 + "\n")

        # Wrap your app with the access log middleware
        app_with_access_log = AccessLogMiddleware(app)

        serve(app_with_access_log, host="0.0.0.0", port=5000)
    except ImportError:
        print("Waitress is not installed. Falling back to Flask's built-in server.")

        # Get local IP addresses
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        primary_ip = get_local_ip()
        print("\n" + "="*60)
        print("JABS server is starting!")
        print(f"Open your browser and go to: http://{local_ip}:5000")
        if primary_ip != local_ip:
            print(f"Or try: http://{primary_ip}:5000")
        print("\nTo stop the server, press Ctrl+C in this terminal.")
        print("="*60 + "\n")

        app.run(host="0.0.0.0", port=5000, debug=True)
