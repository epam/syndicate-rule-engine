from helpers.constants import CAASEnv
from ..commons import valid_isoformat


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


def test_system_whoami(system_user: str, wsgi_test_app):
    resp = wsgi_test_app.get('/caas/users/whoami', expect_errors=True)
    assert resp.status_code == 401
    assert resp.json == {'message': 'Unauthorized'}

    resp = wsgi_test_app.post_json('/caas/signin', {
        'username': system_user[0],
        'password': system_user[1]
    })
    assert resp.status_code == 200
    token = resp.json['access_token']

    resp = wsgi_test_app.get('/caas/users/whoami', headers={
        'Authorization': token
    })
    assert resp.status_code == 200
    data = resp.json
    assert valid_isoformat(data['data']['created_at'])
    assert valid_isoformat(data['data']['latest_login'])
    assert data['data']['customer'] == CAASEnv.SYSTEM_CUSTOMER_NAME.get()
    assert data['data']['role'] == 'system'
    assert data['data']['username'] == 'system'


def test_refresh_token(system_user: str, wsgi_test_app):
    resp = wsgi_test_app.post_json('/caas/signin', {
        'username': system_user[0],
        'password': system_user[1]
    })
    assert resp.status_code == 200
    at = resp.json['access_token']
    rt = resp.json['refresh_token']
    expires_in = resp.json['expires_in']
    assert expires_in <= 3600, 'Too high expiration for access token'

    assert wsgi_test_app.get('/caas/users/whoami', headers={'Authorization': at}).status_code == 200

    assert wsgi_test_app.post_json('/caas/refresh', {'refresh_token': 'junk'}, expect_errors=True).status_code == 401

    resp = wsgi_test_app.post_json('/caas/refresh', {'refresh_token': rt})
    assert resp.status_code == 200

    new_at = resp.json['access_token']
    assert resp.json['refresh_token']
    assert resp.json['expires_in']

    assert wsgi_test_app.get('/caas/users/whoami', headers={'Authorization': new_at}).status_code == 200

    assert wsgi_test_app.post_json('/caas/refresh', {'refresh_token': rt}, expect_errors=True).status_code == 401
