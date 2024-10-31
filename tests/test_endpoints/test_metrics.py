def test_metrics_update_denied(sre_client):
    resp = sre_client.request('/metrics/update', 'POST')
    assert resp.status_int == 401
    assert resp.json == {'message': 'Unauthorized'}


def test_metrics_update(sre_client, system_user_token, s3_buckets):
    resp = sre_client.request('/metrics/update', 'POST',
                              auth=system_user_token)
