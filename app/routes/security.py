"""Routes for security management in JABS."""

import os
import socket
from flask import Blueprint, render_template, request, redirect, url_for, flash
from dotenv import set_key, load_dotenv
from app.settings import ENV_PATH, ENV_MODE

security_bp = Blueprint('security', __name__)

@security_bp.route("/security", endpoint="security")
def show_security():
    """Display the security settings page."""
    load_dotenv(ENV_PATH)
    current_passphrase = bool(os.environ.get("JABS_ENCRYPT_PASSPHRASE"))
    current_smtp_password = bool(os.environ.get("JABS_SMTP_PASSWORD"))
    current_smtp_username = os.environ.get("JABS_SMTP_USERNAME", "")
    return render_template(
        "security.html",
        current_passphrase=current_passphrase,
        current_smtp_password=current_smtp_password,
        current_smtp_username=current_smtp_username,
        env_mode=ENV_MODE,
        hostname=socket.gethostname()
    )

@security_bp.route("/security/set_passphrase", methods=["POST"])
def set_passphrase():
    """Set the encryption passphrase in the .env file."""
    passphrase = request.form.get("passphrase", "").strip()
    if not passphrase:
        flash("Passphrase cannot be empty.", "danger")
        return redirect(url_for("security.security"))
    load_dotenv(ENV_PATH)
    set_key(ENV_PATH, "JABS_ENCRYPT_PASSPHRASE", passphrase)
    flash("Encryption passphrase updated.", "success")
    return redirect(url_for("security.security"))

@security_bp.route("/security/set_smtp_credentials", methods=["POST"])
def set_smtp_credentials():
    """Set the SMTP username and password in the .env file."""
    smtp_username = request.form.get("smtp_username", "").strip()
    smtp_password = request.form.get("smtp_password", "").strip()
    if not smtp_username or not smtp_password:
        flash("SMTP username and password cannot be empty.", "danger")
        return redirect(url_for("security.security"))
    load_dotenv(ENV_PATH)
    set_key(ENV_PATH, "JABS_SMTP_USERNAME", smtp_username)
    set_key(ENV_PATH, "JABS_SMTP_PASSWORD", smtp_password)
    flash("SMTP credentials updated.", "success")
    return redirect(url_for("security.security"))
