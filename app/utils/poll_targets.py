import requests

def poll_targets(targets):
    results = []
    for t in targets:
        try:
            resp = requests.get(t['url'], timeout=5)
            status_code = resp.status_code
            ok = False
            details = {}
            if t['type'] == 'jabs':
                # Try to parse JABS heartbeat JSON
                try:
                    data = resp.json()
                    ok = data.get('status') == 'ok'
                    details = data
                except Exception:
                    ok = False
            else:  # type == 'web'
                ok = status_code == 200 and t.get('expect_text', '') in resp.text
                details = {
                    "status_code": status_code,
                    "found_text": t.get('expect_text', '') in resp.text
                }
        except Exception as e:
            ok = False
            details = {"error": str(e)}
        results.append({
            "name": t['name'],
            "url": t['url'],
            "type": t['type'],
            "ok": ok,
            "details": details
        })
    return results