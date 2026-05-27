def test_health_live(sre_client):
    resp = sre_client.request('/health/live')
    assert resp.status_int == 200
    assert resp.json == {'data': {'details': {}, 'id': 'live', 'status': 'OK'}}


def test_health_buckets_exist_fail(sre_client, mocked_s3_client):
    mocked_s3_client.delete_bucket(Bucket='reports')
    resp = sre_client.request('/health/buckets_exist')
    assert resp.status_int == 503
    assert resp.json == {'data': {
        'details': {'reports': False, 'rulesets': True,
                    'statistics': True}, 'id': 'buckets_exist',
        'impact': 'Depending on missing buckets some features may not work',
        'remediation': 'Set bucket names to .env and execute `main.py create_buckets`. For saas deploy the buckets',
        'status': 'NOT_OK'}}


def test_health_buckets_exist_pass(sre_client, s3_buckets):
    resp = sre_client.request('/health/buckets_exist')
    assert resp.status_int == 200
    assert resp.json == {'data': {'details': {}, 'id': 'buckets_exist', 'status': 'OK'}}
