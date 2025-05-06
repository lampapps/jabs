import os
from flask import Blueprint, render_template, current_app
from app.settings import LOG_DIR

logs_bp = Blueprint('logs', __name__)

def get_log_stats(content):
    lines = content.splitlines()
    total = len(lines)
    info = sum(1 for l in lines if 'INFO' in l)
    warning = sum(1 for l in lines if 'WARNING' in l)
    error = sum(1 for l in lines if 'ERROR' in l)
    other = total - info - warning - error
    return {'total': total, 'info': info, 'warning': warning, 'error': error, 'other': other}

@logs_bp.route("/logs")
def logs():
    logs = []
    for fname in sorted(os.listdir(LOG_DIR)):
        if fname.endswith(".log"):
            fpath = os.path.join(LOG_DIR, fname)
            try:
                with open(fpath) as f:
                    content = f.read()
                stats = get_log_stats(content)
                logs.append((fname, content, stats))
            except Exception:
                logs.append((fname, "Could not read log.", {'total': 0, 'info': 0, 'warning': 0, 'error': 0, 'other': 0}))
    return render_template("logs.html", logs=logs)