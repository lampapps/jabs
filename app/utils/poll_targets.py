"""Utility for polling remote JABS targets and collecting heartbeat status."""

import requests

def poll_targets(targets):
    """
    Poll a list of targets for their heartbeat status.

    Args:
        targets (list): List of dicts with 'name' and 'url' keys.

    Returns:
        list: List of dicts with polling results for each target.
    """
    results = []
    for t in targets:
        url = t['url']
        base_url = url
        # Append /api/heartbeat if not already present
        if not url.rstrip('/').endswith('/api/heartbeat'):
            url = url.rstrip('/') + '/api/heartbeat'
        try:
            resp = requests.get(url, timeout=5)
            ok = False
            details = {}
            # Try to parse JABS heartbeat JSON
            try:
                data = resp.json()
                ok = data.get('status') == 'ok'
                details = data
            except (ValueError, requests.exceptions.JSONDecodeError):
                ok = False
        except (requests.exceptions.RequestException, Exception) as e:
            ok = False
            details = {"error": str(e)}
        results.append({
            "name": t['name'],
            "url": url,
            "base_url": base_url,
            "ok": ok,
            "details": details
        })
    return results
