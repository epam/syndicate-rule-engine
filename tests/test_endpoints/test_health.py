

def test_health_live(sre_client):
    resp = sre_client.request('/health/live')
    assert resp.status_int == 200
    assert resp.json == {'data': {'details': {}, 'id': 'live', 'status': 'OK'}}
