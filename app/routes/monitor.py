from flask import Blueprint, render_template, jsonify
from app.utils.poll_targets import poll_targets
import yaml

monitor_bp = Blueprint('monitor', __name__)

@monitor_bp.route("/monitor")
def monitor():
    # Load targets from config
    with open("config/monitor.yaml") as f:
        config = yaml.safe_load(f)
    targets = config.get("monitored_targets", [])
    monitors = poll_targets(targets)
    return render_template("monitor.html", monitors=monitors)