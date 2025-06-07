import requests

def poll_targets(targets):
    results = []
    for t in targets:
        url = t['url']
        base_url = url
        # Append /api/heartbeat if not already present
        if not url.rstrip('/').endswith('/api/heartbeat'):
            url = url.rstrip('/') + '/api/heartbeat'
        try:
            resp = requests.get(url, timeout=5)
            status_code = resp.status_code
            ok = False
            details = {}
            # Try to parse JABS heartbeat JSON
            try:
                data = resp.json()
                ok = data.get('status') == 'ok'
                details = data
            except Exception:
                ok = False
        except Exception as e:
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