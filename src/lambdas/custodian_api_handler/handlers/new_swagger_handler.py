from http import HTTPStatus
from typing import TYPE_CHECKING

from helpers import urljoin
from helpers.__version__ import __version__
from helpers.constants import CustodianEndpoint, HTTPMethod
from helpers.lambda_response import LambdaResponse, ResponseFactory
from helpers.log_helper import get_logger
from handlers import AbstractHandler, Mapping
from services import SP
from services.abs_lambda import ProcessedEvent
from services.openapi_spec_generator import OpenApiGenerator
from validators.registry import iter_all
from urllib3.util import parse_url, Url

if TYPE_CHECKING:
    from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)

SWAGGER_HTML = \
"""
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="description" content="SwaggerUI" />
    <title>SwaggerUI</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@{version}/swagger-ui.css" />
  </head>
  <body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@{version}/swagger-ui-bundle.js" crossorigin></script>
  <script src="https://unpkg.com/swagger-ui-dist@{version}/swagger-ui-standalone-preset.js" crossorigin></script>
  <script>
    window.onload = () => {{
      window.ui = SwaggerUIBundle({{
        url: '{url}',
        dom_id: '#swagger-ui',
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        layout: "StandaloneLayout",
      }});
    }};
  </script>
  </body>
</html>
"""


class SwaggerHandler(AbstractHandler):
    def __init__(self, environment_service: 'EnvironmentService'):
        self._env = environment_service

    @classmethod
    def build(cls) -> 'SwaggerHandler':
        return cls(environment_service=SP.environment_service)

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.DOC: {
                HTTPMethod.GET: self.get
            },
            CustodianEndpoint.DOC_SWAGGER_JSON: {
                HTTPMethod.GET: self.get_spec
            }
        }

    @staticmethod
    def _resolve_urls(headers: dict) -> list[str]:
        scheme = headers.get('X-Forwarded-Proto')
        host = headers.get('Host')
        if scheme:
            return [f'{scheme.lower()}://{host}']
        parsed: Url = parse_url(host)
        port = parsed.port or 443
        scheme = parsed.scheme or ('https' if port == 443 else 'http')
        return [f'{scheme}://{host}']

    def get(self, event: dict):
        out = SWAGGER_HTML.format(
            version='latest',  # get from env?
            url=urljoin(
                '/',
                self._env.api_gateway_stage(),
                CustodianEndpoint.DOC_SWAGGER_JSON.value
            )
        )
        return LambdaResponse(
            code=HTTPStatus.OK,
            content=out,
            headers={'Content-Type': 'text/html'}
        ).build()

    def get_spec(self, event: dict, _pe: ProcessedEvent):
        _LOG.debug('Returning openapi spec')
        spec = OpenApiGenerator(
            title='Rule Engine - OpenAPI 3.0',
            description='Rule engine rest api',
            url=self._resolve_urls(_pe['headers']),
            stages=self._env.api_gateway_stage(),
            version=__version__,
            endpoints=iter_all()
        ).generate()
        _LOG.debug('Open api spec was generated')
        return ResponseFactory().raw(spec).build()
