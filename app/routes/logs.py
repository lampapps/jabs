"""Flask routes for viewing and summarizing log files in JABS."""

import os
import re
import socket
from collections import Counter
from flask import Blueprint, render_template
from app.settings import LOG_DIR, MAX_LOG_LINES

logs_bp = Blueprint('logs', __name__)

def get_log_stats(content):
    """Return a dict with counts of INFO, WARNING, ERROR, and other lines in the log content."""
    lines = content.splitlines()
    total = len(lines)
    info = sum(1 for l in lines if 'INFO' in l)
    warning = sum(1 for l in lines if 'WARNING' in l)
    error = sum(1 for l in lines if 'ERROR' in l)
    other = total - info - warning - error
    return {'total': total, 'info': info, 'warning': warning, 'error': error, 'other': other}

def parse_response_codes(log_path):
    """Parse HTTP response codes from a log file and return their counts."""
    code_re = re.compile(r'"\s*(\d{3})\b')
    codes = []
    with open(log_path, encoding="utf-8") as f:
        for line in f:
            match = code_re.search(line)
            if match:
                codes.append(match.group(1))
    return dict(Counter(codes))

@logs_bp.route("/logs")
def logs_view():
    """Display available logs and their summaries."""
    logs_list = []
    for fname in sorted(os.listdir(LOG_DIR)):
        if fname.endswith(".log"):
            fpath = os.path.join(LOG_DIR, fname)
            try:
                with open(fpath, encoding="utf-8") as f:
                    content = f.read()
                stats = get_log_stats(content)
                response_codes = parse_response_codes(fpath) if fname == "server.log" else None

                lines = content.splitlines()
                trimmed_content = "\n".join(lines[-20:]) if len(lines) > 20 else content

                # Pass both trimmed and full content
                logs_list.append((fname, trimmed_content, stats, response_codes, content))
            except OSError:
                logs_list.append(
                    (fname, "Could not read log.",
                     {'total': 0, 'info': 0, 'warning': 0, 'error': 0, 'other': 0},
                     None, "")
                )
    return render_template(
        "logs.html",
        logs=logs_list,
        MAX_LOG_LINES=MAX_LOG_LINES,
        hostname=socket.gethostname()
    )
