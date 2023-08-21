import json
from helpers.constants import PARAM_MESSAGE, PARAM_TRACE_ID


class CustodianException(Exception):

    def __init__(self, code, content):
        self._code = code
        self._content = content

    @property
    def code(self) -> str:
        return self._code

    @code.setter
    def code(self, value):
        self._code = value

    @property
    def content(self) -> str:
        return self._content

    @content.setter
    def content(self, value):
        self._content = value

    def __str__(self):
        return f'{self._code}:{self._content}'

    def __repr__(self):
        return self.__str__()

    def response(self):
        from helpers import _import_request_context
        context = _import_request_context()
        return {
            'statusCode': self._code,
            'headers': {
                'Content-Type': 'application/json',
                'x-amzn-ErrorType': self._code,
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*'
            },
            'isBase64Encoded': False,
            'body': json.dumps({
                PARAM_MESSAGE: self._content,
                PARAM_TRACE_ID: context.aws_request_id
            }, sort_keys=True, separators=(",", ":"))
        }


class MetricsUpdateException(CustodianException):
    ...
