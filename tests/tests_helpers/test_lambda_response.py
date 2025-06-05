from http import HTTPStatus

import pytest
from helpers.__version__ import __version__

from helpers.lambda_response import LambdaResponse, CustodianException, \
    JsonLambdaResponse, ResponseFactory, build_response
from services import SP


def test_ok_lambda_response():
    SP.tls.aws_request_id = 'mock'
    resp = LambdaResponse(
        code=HTTPStatus.OK,
        content='<h1>Hello, world!</h1>',
        headers={'Content-Type': 'text/html'}
    )
    assert resp.code == HTTPStatus.OK
    assert resp.ok
    assert resp.build() == {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
            'Accept-Version': __version__,
            'Content-Type': 'text/html',
            'Lambda-Invocation-Trace-Id': 'mock'
        },
        'isBase64Encoded': False,
        'body': '<h1>Hello, world!</h1>'
    }


def test_not_ok_lambda_response():
    resp = LambdaResponse(code=HTTPStatus.NOT_FOUND)
    assert not resp.ok
    with pytest.raises(CustodianException):
        raise resp.exc()


def test_too_large_lambda_response():
    resp = JsonLambdaResponse(
        code=HTTPStatus.OK,
        content={'data': (b'a' * (6291456 - 11)).decode()}
    )  # 11 for {"data":""}
    with pytest.raises(CustodianException):
        resp.build()
    resp = JsonLambdaResponse(
        code=HTTPStatus.OK,
        content={'data': (b'a' * (6291456 - 12)).decode()}
    )
    resp.build()


def test_json_lambda_response():
    SP.tls.aws_request_id = 'mock'
    resp = JsonLambdaResponse(
        code=HTTPStatus.OK,
        content={'str': 'value', 'list': [1, 2, 3]},
    )
    assert resp.build() == {
        'statusCode': 200,
        'headers': {
            'Accept-Version': __version__,
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': '*',
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json',
            'Lambda-Invocation-Trace-Id': 'mock'
        },
        'isBase64Encoded': False,
        'body': b'{"list":[1,2,3],"str":"value"}',
    }


class TestResponseFactory:
    def test_build_items(self):
        data = ResponseFactory(HTTPStatus.OK).items([1, 2, 3]).build()
        assert data['body'] == b'{"items":[1,2,3]}'
        data = ResponseFactory(HTTPStatus.OK).items(range(5)).build()
        assert data['body'] == b'{"items":[0,1,2,3,4]}'

    def test_build_data(self):
        data = ResponseFactory(HTTPStatus.OK).data({'key': 'value'}).build()
        assert data['body'] == b'{"data":{"key":"value"}}'

    def test_build_message(self):
        data = ResponseFactory(HTTPStatus.OK).message('hello world').build()
        assert data['body'] == b'{"message":"hello world"}'

    def test_build_errors(self):
        data = ResponseFactory(HTTPStatus.OK).errors(
            [{'key': 'value'}]).build()
        assert data['body'] == b'{"errors":[{"key":"value"}]}'

    def test_build_raw(self):
        data = ResponseFactory(HTTPStatus.OK).raw({'token': '123'}).build()
        assert data['body'] == b'{"token":"123"}'

    def test_build_default(self):
        data = ResponseFactory(
            HTTPStatus.INSUFFICIENT_STORAGE).default().build()
        assert data['body'] == b'{"message":"Insufficient Storage"}'


def test_build_response():
    assert build_response('hello')['body'] == b'{"message":"hello"}'
    assert (build_response({'key': 'value'})['body'] ==
            b'{"data":{"key":"value"}}')
    assert (build_response([{'key': 'value'}])['body'] ==
            b'{"items":[{"key":"value"}]}')

    def gen():
        yield {'k1': 'v1'}
        yield {'k2': 'v2'}
        yield {'k3': 'v3'}

    assert (build_response(gen())['body'] ==
            b'{"items":[{"k1":"v1"},{"k2":"v2"},{"k3":"v3"}]}')

    with pytest.raises(CustodianException):
        build_response(code=HTTPStatus.NOT_FOUND)
