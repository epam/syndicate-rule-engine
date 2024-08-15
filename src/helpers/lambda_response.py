import base64
import os
from http import HTTPStatus
from typing import Iterable, TypedDict, TypeVar, Final, Any

import msgspec
from helpers.__version__ import __version__
from helpers.constants import JSON_CONTENT_TYPE, \
    LAMBDA_URL_HEADER_CONTENT_TYPE_UPPER, CAASEnv
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


Content = dict | list | str | Iterable | None

# https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html
# https://zaccharles.medium.com/deep-dive-lambdas-response-payload-size-limit-8aedba9530ed
PAYLOAD_SIZE_LIMIT: Final[int] = (2 << 19) * 6  # 6mb


class LambdaOutput(TypedDict):
    statusCode: int
    headers: dict[str, str]
    body: str
    isBase64Encoded: bool


class LambdaForceExit(Exception):
    """
    Can be used not only for 400 or 500. It actually can be very convenient
    to make 200 exit anywhere in the code. Still... don't do that
    """
    __slots__ = ('_response',)

    def __init__(self, response: 'LambdaResponse'):
        self._response = response

    @property
    def response(self) -> 'LambdaResponse':
        return self._response

    def __str__(self):
        return f'<{self._response.code}:{str(self._response.content)}>'

    __repr__ = __str__

    def build(self) -> LambdaOutput:
        return self._response.build()


class CustodianException(LambdaForceExit):
    ...


class MetricsUpdateException(LambdaForceExit):
    ...


class ReportNotSendException(LambdaForceExit):
    ...


TE = TypeVar('TE', bound=LambdaForceExit)


class LambdaResponse:
    __slots__ = ('_code', '_content', '_headers')

    def __init__(self, code: HTTPStatus = HTTPStatus.OK,
                 content: Any = '',
                 headers: dict[str, str] | None = None):
        self._code = code
        self._content = content
        self._headers = headers or {}

    @property
    def code(self) -> HTTPStatus:
        return self._code

    @property
    def content(self) -> Any:
        return self._content

    @property
    def ok(self) -> bool:
        return 200 <= self._code <= 206 or 301 <= self._code <= 308

    def _common_headers(self) -> dict[str, str]:
        headers = {
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
            'Accept-Version': __version__,  # TODO API think about header name
        }
        if trace_id := CAASEnv.INVOCATION_REQUEST_ID.get(None):
            headers['Lambda-Invocation-Trace-Id'] = trace_id
        if not self.ok:
            headers['x-amzn-ErrorType'] = str(self._code.value)
        headers.update(self._headers)
        return headers

    def build(self) -> LambdaOutput:
        """
        Must return a format which is expected by Lambda Url/Api gateway
        :return:
        """
        return {
            'statusCode': self._code.value,
            'headers': self._common_headers(),
            'isBase64Encoded': False,
            'body': self._content,
        }

    def exc(self, exc_type: type[TE] = CustodianException) -> TE:
        return exc_type(response=self)


class BinaryResponse(LambdaResponse):
    """
    Returns binary data
    """
    def __init__(self, code: HTTPStatus = HTTPStatus.OK,
                 content: bytes = b'',
                 content_type: str | None = None):
        super().__init__(
            code=code,
            content=content,
            headers={'Content-Type': content_type} if content_type else {}
        )

    def build(self) -> LambdaOutput:
        return {
            'headers': self._common_headers(),
            'body': base64.b64encode(self._content).decode(),
            'isBase64Encoded': True,
            'statusCode': self._code.value
        }


class JsonLambdaResponse(LambdaResponse):
    def __init__(self, code: HTTPStatus = HTTPStatus.OK,
                 content: Content = None,
                 headers: dict[str, str] | None = None):
        headers = headers or {}
        headers.update({
            LAMBDA_URL_HEADER_CONTENT_TYPE_UPPER: JSON_CONTENT_TYPE
        })
        super().__init__(
            code=code,
            content=content,
            headers=headers,
        )

    @staticmethod
    def _default(obj):
        """
        Default hook for json serializer
        :param obj:
        :return:
        """
        if hasattr(obj, '__json__'):
            return obj.__json__()
        if isinstance(obj, bytes):
            return obj.decode()
        if isinstance(obj, Iterable):
            return list(obj)
        raise TypeError

    encoder = msgspec.json.Encoder(enc_hook=_default, order='sorted')

    def build(self) -> LambdaOutput:
        _LOG.debug('Dumping output to Json')
        body = self.encoder.encode(self._content)
        _LOG.debug('Output was dumped')
        if len(body) >= PAYLOAD_SIZE_LIMIT:
            _LOG.warning('Output is too large to be returned from lambda')
            raise ResponseFactory(HTTPStatus.REQUEST_ENTITY_TOO_LARGE).message(
                'Entity is too large. Use href=true query param or '
                'connect support'
            ).exc()
        return {
            'headers': self._common_headers(),
            'body': body.decode(),
            'isBase64Encoded': False,
            'statusCode': self._code.value
        }


class ResponseFactory:
    """
    Builds some common JSON responses
    >>> response = ResponseFactory(HTTPStatus.OK).items()
    >>> raise ResponseFactory(HTTPStatus.BAD_REQUEST).default().exc()
    """
    __slots__ = ('_code',)

    def __init__(self, code: HTTPStatus | int = HTTPStatus.OK):
        self._code = HTTPStatus(code) if isinstance(code, int) else code

    def items(self, it: Iterable, next_token: Any = None
              ) -> JsonLambdaResponse:
        content = {'items': it}
        if next_token:
            content['next_token'] = next_token
        return self.raw(content)

    def data(self, data: dict) -> JsonLambdaResponse:
        return self.raw({'data': data})

    def message(self, message: str) -> JsonLambdaResponse:
        return self.raw({'message': message})

    def errors(self, errors: list[dict]) -> JsonLambdaResponse:
        return self.raw({'errors': errors})

    def raw(self, raw: Content) -> JsonLambdaResponse:
        return JsonLambdaResponse(code=self._code, content=raw)

    def default(self) -> JsonLambdaResponse:
        return self.message(message=self._code.phrase)


def build_response(content: Content = None,
                   code: HTTPStatus | int = HTTPStatus.OK) -> LambdaOutput:
    """
    Auxiliary function. Use ResponseFactory in case you want your own response
    format
    """
    f = ResponseFactory(code)
    match content:
        case str():
            resp = f.message(content)
        case dict():
            resp = f.data(content)
        case list():
            resp = f.items(content)
        case None:
            resp = f.default()
        case _:  # generator / iterator
            resp = f.items(list(content))
    if not resp.ok:
        raise resp.exc()
        # return
    return resp.build()
