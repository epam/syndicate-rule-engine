def test_signup(wsgi_test_app):
    resp = wsgi_test_app.post_json('/caas/signup', {
        'username': 'example',
        'password': 'Qwerty12345=',
        'customer_name': 'example',
        'customer_display_name': 'example customer',
        'customer_admins': ['example@gmail.com']
    })
    assert resp.status_code == 201
    assert resp.content_type == 'application/json'
    assert 'message' in resp.json

    resp = wsgi_test_app.post_json('/caas/signup', {
        'username': 'example',
        'password': 'Qwerty12345=',
        'customer_name': 'example',
        'customer_display_name': 'example customer',
        'customer_admins': ['example@gmail.com']
    }, expect_errors=True)
    assert resp.status_code == 409
    assert 'message' in resp.json


def test_signup_invalid_password(wsgi_test_app):
    resp = wsgi_test_app.post_json('/caas/signup', {
        'username': 'example',
        'password': '12345',
        'customer_name': 'example',
        'customer_display_name': 'example customer',
        'customer_admins': ['example@gmail.com']
    }, expect_errors=True)
    assert resp.status_code == 400
    assert resp.json == {'errors': [{
        'description': 'Value error, must have uppercase characters, must have lowercase characters, must have at least one symbol, valid min length for password: 8',
        'location': ['password']
    }]}
