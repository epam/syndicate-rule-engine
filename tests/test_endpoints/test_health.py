

def test_health_live(wsgi_test_app):
    resp = wsgi_test_app.get('/caas/health/live')
    assert resp.status_int == 200
    assert resp.json == {'data': {'details': {}, 'id': 'live', 'status': 'OK'}}
