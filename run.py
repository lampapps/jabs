import os
import secrets

# Generate a random secret key if not set
if "JABS_SECRET_KEY" not in os.environ:
    os.environ["JABS_SECRET_KEY"] = secrets.token_urlsafe(32)

from app import create_app

app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)