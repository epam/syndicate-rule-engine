from helpers.constants import Env
from ..commons import valid_isoformat


def test_signup(sre_client):
    resp = sre_client.request('/signup', 'POST', data={
        'username': 'example',
        'password': 'Qwerty12345=',
        'customer_name': 'example',
        'customer_display_name': 'example customer',
        'customer_admins': ['example@gmail.com']
    })
    assert resp.status_code == 201
    assert resp.content_type == 'application/json'
    assert 'message' in resp.json

    resp = sre_client.request('/signup', 'POST', data={
        'username': 'example',
        'password': 'Qwerty12345=',
        'customer_name': 'example',
        'customer_display_name': 'example customer',
        'customer_admins': ['example@gmail.com']
    })
    assert resp.status_code == 409
    assert 'message' in resp.json


def test_signup_invalid_password(sre_client):
    resp = sre_client.request('/signup', 'POST', data={
        'username': 'example',
        'password': '12345',
        'customer_name': 'example',
        'customer_display_name': 'example customer',
        'customer_admins': ['example@gmail.com']
    })
    assert resp.status_code == 400
    assert resp.json == {'errors': [{
        'description': 'Value error, must have uppercase characters, must have lowercase characters, must have at least one symbol, valid min length for password: 8',
        'location': ['password']
    }]}


def test_signin(system_user: str, sre_client):
    resp = sre_client.request('/signin', 'POST', data={
        'username': system_user[0],
        'password': system_user[1]
    })
    assert resp.status_code == 200
    assert resp.json['access_token']
    assert resp.json['refresh_token']
    assert resp.json['expires_in']

    resp = sre_client.request('/signin', 'POST', data={
        'username': system_user[0],
        'password': 'invalid password'
    })
    assert resp.status_code == 401
    assert resp.json == {'message': 'Incorrect username and/or password'}

    resp = sre_client.request('/signin', 'POST', data={
        'username': 'invalid username',
        'password': system_user[1]
    })
    assert resp.status_code == 401
    assert resp.json == {'message': 'Incorrect username and/or password'}


def test_system_whoami(system_user: str, sre_client):
    resp = sre_client.request('/users/whoami', 'GET')
    assert resp.status_code == 401
    assert resp.json == {'message': 'Unauthorized'}

    resp = sre_client.request('/signin', 'POST', data={
        'username': system_user[0],
        'password': system_user[1]
    })
    assert resp.status_code == 200
    token = resp.json['access_token']

    resp = sre_client.request('/users/whoami', 'GET', auth=token)
    assert resp.status_code == 200
    data = resp.json
    assert valid_isoformat(data['data']['created_at'])
    assert valid_isoformat(data['data']['latest_login'])
    assert data['data']['customer'] == Env.SYSTEM_CUSTOMER_NAME.get()
    assert data['data']['role'] == 'system'
    assert data['data']['username'] == 'system'


def test_refresh_token(system_user: str, sre_client):
    resp = sre_client.request('/signin', 'POST', data={
        'username': system_user[0],
        'password': system_user[1]
    })
    assert resp.status_code == 200
    at = resp.json['access_token']
    rt = resp.json['refresh_token']
    expires_in = resp.json['expires_in']
    assert expires_in <= 3600, 'Too high expiration for access token'

    assert sre_client.request('/users/whoami', 'GET', auth=at).status_code == 200

    assert sre_client.request('/refresh', 'POST', data={'refresh_token': 'junk'}).status_code == 401

    resp = sre_client.request('/refresh', 'POST', data={'refresh_token': rt})
    assert resp.status_code == 200

    new_at = resp.json['access_token']
    assert resp.json['refresh_token']
    assert resp.json['expires_in']

    assert sre_client.request('/users/whoami', 'GET', auth=new_at).status_code == 200

    assert sre_client.request('/refresh', 'POST', data={'refresh_token': rt}).status_code == 401


def test_reset_password(system_user: str, sre_client):
    resp = sre_client.request('/signin', 'POST', data={
        'username': system_user[0],
        'password': system_user[1]
    })
    at = resp.json['access_token']

    resp = sre_client.request('/users/reset-password', 'POST', auth=at, data={'new_password': '12345'})
    assert resp.status_code == 400

    resp = sre_client.request('/users/reset-password', 'POST', auth=at, data={'new_password': 'Qwerty12345='})
    assert resp.status_code == 204

    assert sre_client.request('/signin', 'POST', data={
        'username': system_user[0],
        'password': system_user[1]
    }).status_code == 401

    assert sre_client.request('/signin', 'POST', data={
        'username': system_user[0],
        'password': 'Qwerty12345='
    }).status_code == 200


