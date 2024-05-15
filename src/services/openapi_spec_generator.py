import re
from http import HTTPStatus
from typing import Iterable
from enum import Enum

from pydantic import BaseModel
from typing_extensions import TypedDict, NotRequired

from helpers import urljoin, dereference_json
from helpers.constants import HTTPMethod, Permission


class OpenApiInfoLicense(TypedDict):
    name: str
    url: str


class OpenApiInfo(TypedDict):
    title: str
    description: str
    version: str
    license: OpenApiInfoLicense


class OpenApiServer(TypedDict):
    url: str
    description: str
    variables: dict[str, dict]


class OpenApiTag(TypedDict):
    name: str
    description: NotRequired[str]


class OpenApiComponents(TypedDict):
    schemas: dict[str, dict]
    securitySchemes: dict[str, dict]


class OpenApiV3(TypedDict):
    openapi: str
    info: OpenApiInfo
    servers: list[OpenApiServer]
    paths: dict[str, dict]
    tags: list[OpenApiTag]
    components: OpenApiComponents


class EndpointInfo:
    """
    Just container for data
    """
    __slots__ = ('path', 'method', 'summary', 'description', 'request_model',
                 'responses', 'auth', 'tags', 'permission')

    def __init__(self, path: str | Enum, method: HTTPMethod,
                 summary: str | None = None,
                 description: str | None = None,
                 request_model: type[BaseModel] | None = None,
                 responses: list[tuple[HTTPStatus, type[BaseModel] | None, str | None]] | None = None,
                 auth: bool = True, tags: list[str] | None = None,
                 permission: Permission | None = None):
        """

        :param path:
        :param method:
        :param summary:
        :param description:
        :param request_model:
        :param responses: list of tuples: (202, JobCreatedModel, 'description')
        :param auth:
        :param tags:
        :param permission: permission that is necessary for this endpoint
        """
        self.path: str = path.value if isinstance(path, Enum) else path
        self.method: HTTPMethod = method
        self.summary: str | None = summary
        self.description: str | None = description
        self.request_model: type[BaseModel] | None = request_model
        self.responses: list = responses or []
        self.auth: bool = auth
        self.tags: list = tags or []
        self.permission: Permission | None = permission


class OpenApiGenerator:
    dynamic_resource_regex = re.compile(r'([^{/]+)(?=})')

    def __init__(self, title: str, description: str, url: list[str] | str,
                 stages: list[str] | str, version: str,
                 endpoints: Iterable[EndpointInfo], auto_tags: bool = True):
        """
        :param title:
        :param description:
        :param url:
        :param stages:
        :param version:
        :param endpoints:
        :param auto_tags: resolves tags from path prefix. Only for endpoints
        that don't have tags provided
        """
        self._title = title
        self._description = description
        if isinstance(url, str):
            self._urls = [url]
        else:
            self._urls = url

        assert stages, 'stages cannot be empty'
        self._stages: list[str] | str = stages
        self._version = version
        self._endpoints = endpoints
        self._auto_tags = auto_tags

    def _generate_base(self) -> OpenApiV3:
        match self._stages:
            case str():
                stages = {'default': self._stages, 'description': 'Main stage'}
            case _:  # list()
                stages = {
                    'default': self._stages[0],
                    'description': 'Api stage',
                    'enum': self._stages
                }
        return {
            'openapi': '3.0.3',
            'info': {
                'title': self._title,
                'description': self._description,
                'version': self._version,
                'license': {
                    'name': 'Apache 2.0',
                    'url': 'http://www.apache.org/licenses/LICENSE-2.0.html'
                }
            },
            'servers': [{
                'url': urljoin(url, '{stage}'),
                'description': 'Main url',
                'variables': {'stage': stages}
            } for url in self._urls],
            'paths': {},
            'tags': [],
            'components': {
                'schemas': {},
                'securitySchemes': {
                    'access_token': {
                        'type': 'apiKey',
                        'description': 'Simple token authentication. The same as AWS Cognito and AWS Api gateway integration has',
                        'name': 'Authorization',
                        'in': 'header',
                    }
                }
            }
        }

    @staticmethod
    def _model_schema(model: type[BaseModel]) -> dict:
        sch = model.model_json_schema()
        dereference_json(sch)
        sch.pop('$defs', None)
        return sch

    def _model_to_parameters(self, model: type[BaseModel]) -> list[dict]:
        parameters = []
        sch = self._model_schema(model)
        for name, field in model.model_fields.items():
            param = {
                'name': name,
                'in': 'query',
                'required': field.is_required(),
            }
            if d := field.description:
                param['description'] = d
            if e := field.examples:
                param['example'] = e[0]
            if s := sch.get('properties', {}).get(name):
                param['schema'] = s
            parameters.append(param)
        return parameters

    def generate(self) -> OpenApiV3:
        base = self._generate_base()
        paths = base['paths']
        schemas = base['components']['schemas']
        tags = set()  # todo allow to provide tags with description
        for endpoint in self._endpoints:
            data = paths.setdefault(endpoint.path, {}).setdefault(
                endpoint.method.lower(), {})
            # tags
            if self._auto_tags and not endpoint.tags:
                tag = endpoint.path.strip('/').split('/')[0].title()
                data['tags'] = [tag]
                tags.add(tag)
            else:
                data['tags'] = endpoint.tags
                tags.update(endpoint.tags)

            # path params
            for slot in re.finditer(self.dynamic_resource_regex,
                                    endpoint.path):
                data.setdefault('parameters', []).append({
                    'name': slot.group(),
                    'in': 'path',
                    'required': True,
                    'schema': {'type': 'string'}
                })

            # summary
            if summary := endpoint.summary:
                data['summary'] = summary

            # description
            if description := endpoint.description:
                data['description'] = description

            # request model / query params
            if model := endpoint.request_model:
                match endpoint.method:
                    case HTTPMethod.GET:
                        data.setdefault('parameters', []).extend(
                            self._model_to_parameters(model)
                        )
                    case _:
                        name = model.__name__
                        data['requestBody'] = {
                            'content': {
                                'application/json': {
                                    'schema': {
                                        '$ref': f'#/components/schemas/{name}'}
                                }
                            },
                            'required': True
                        }
                        schemas[name] = self._model_schema(model)

            # auth
            if endpoint.auth:
                data['security'] = [{'access_token': []}]

            # responses
            responses = data.setdefault('responses', {})
            for code, model, description in endpoint.responses:
                resp = {}
                if description:
                    resp['description'] = description
                if model:
                    name = model.__name__
                    resp['content'] = {
                        'application/json': {'schema': {
                            '$ref': f'#/components/schemas/{name}'
                        }}
                    }
                    schemas[name] = self._model_schema(model)
                responses[str(code.value)] = resp

        base['tags'].extend({'name': name} for name in sorted(tags))

        return base
