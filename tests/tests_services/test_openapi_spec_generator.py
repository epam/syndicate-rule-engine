from http import HTTPStatus

from pydantic import BaseModel, Field

from helpers.constants import HTTPMethod
from services.openapi_spec_generator import OpenApiGenerator, EndpointInfo


def test_generate():
    class User(BaseModel):
        name: str
        age: int = Field(ge=0)

    class Users(BaseModel):
        items: list[User]
        next_token: str | None

    class UsersList(BaseModel):
        limit: int = Field(None, ge=1, le=50,
                           description='Limit number of returned users',
                           examples=[49, 50])
        next_token: str = Field(None)

    generator = OpenApiGenerator(
        title='title',
        description='description',
        url=['http://127.0.0.1:8080'],
        stages='dev',
        version='1.2.3',
        auto_tags=True,
        endpoints=[
            EndpointInfo(
                path='/users/{id}',
                method=HTTPMethod.GET,
                lambda_name='caas-api-handler',
                summary='Get a specific user',
                description='long description',
                responses=[(HTTPStatus.OK, User, 'User model'),
                           (HTTPStatus.NOT_FOUND, None, 'User not found')],
                auth=False
            ),
            EndpointInfo(
                path='/users',
                method=HTTPMethod.GET,
                lambda_name='caas-api-handler',
                summary='List all users',
                description='long description',
                request_model=UsersList,
                responses=[(HTTPStatus.OK, Users, 'Many users')],
                auth=False
            ),
            EndpointInfo(
                path='/users',
                method=HTTPMethod.POST,
                lambda_name='caas-api-handler',
                summary='user-summary',
                description='user-description',
                request_model=User,
                responses=[(HTTPStatus.CREATED, User, 'User model')],
                tags=['Admin api', 'Users']
            ),
        ]
    )
    res = generator.generate()
    assert res == {
        'openapi': '3.0.3',
        'info': {
            'title': 'title',
            'description': 'description',
            'version': '1.2.3',
            'license': {
                'name': 'Apache 2.0',
                'url': 'http://www.apache.org/licenses/LICENSE-2.0.html'
            }
        },
        'servers': [
            {
                'url': 'http://127.0.0.1:8080/{stage}',
                'description': 'Main url',
                'variables': {
                    'stage': {
                        'default': 'dev',
                        'description': 'Main stage'
                    }
                }
            }
        ],
        'paths': {
            '/users/{id}': {
                'get': {
                    'tags': [
                        'Users'
                    ],
                    'parameters': [
                        {
                            'name': 'id',
                            'in': 'path',
                            'required': True,
                            'schema': {
                                'type': 'string'
                            }
                        }
                    ],
                    'summary': 'Get a specific user',
                    'description': 'long description',
                    'responses': {
                        '200': {
                            'description': 'User model',
                            'content': {
                                'application/json': {
                                    'schema': {
                                        '$ref': '#/components/schemas/User'
                                    }
                                }
                            }
                        },
                        '404': {
                            'description': 'User not found'
                        }
                    }
                }
            },
            '/users': {
                'get': {
                    'tags': [
                        'Users'
                    ],
                    'summary': 'List all users',
                    'description': 'long description',
                    'parameters': [
                        {
                            'name': 'limit',
                            'in': 'query',
                            'required': False,
                            'description': 'Limit number of returned users',
                            'example': 49,
                            'schema': {
                                'default': None,
                                'description': 'Limit number of returned users',
                                'examples': [
                                    49,
                                    50
                                ],
                                'maximum': 50,
                                'minimum': 1,
                                'title': 'Limit',
                                'type': 'integer'
                            }
                        },
                        {
                            'name': 'next_token',
                            'in': 'query',
                            'required': False,
                            'schema': {
                                'default': None,
                                'title': 'Next Token',
                                'type': 'string'
                            }
                        }
                    ],
                    'responses': {
                        '200': {
                            'description': 'Many users',
                            'content': {
                                'application/json': {
                                    'schema': {
                                        '$ref': '#/components/schemas/Users'
                                    }
                                }
                            }
                        }
                    }
                },
                'post': {
                    'tags': [
                        'Admin api',
                        'Users'
                    ],
                    'summary': 'user-summary',
                    'description': 'user-description',
                    'requestBody': {
                        'content': {
                            'application/json': {
                                'schema': {
                                    '$ref': '#/components/schemas/User'
                                }
                            }
                        },
                        'required': True
                    },
                    'security': [
                        {
                            'access_token': []
                        }
                    ],
                    'responses': {
                        '201': {
                            'description': 'User model',
                            'content': {
                                'application/json': {
                                    'schema': {
                                        '$ref': '#/components/schemas/User'
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        'tags': [
            {
                'name': 'Admin api'
            },
            {
                'name': 'Users'
            }
        ],
        'components': {
            'schemas': {
                'User': {
                    'properties': {
                        'name': {
                            'title': 'Name',
                            'type': 'string'
                        },
                        'age': {
                            'minimum': 0,
                            'title': 'Age',
                            'type': 'integer'
                        }
                    },
                    'required': [
                        'name',
                        'age'
                    ],
                    'title': 'User',
                    'type': 'object'
                },
                'Users': {
                    'properties': {
                        'items': {
                            'items': {
                                'properties': {
                                    'name': {
                                        'title': 'Name',
                                        'type': 'string'
                                    },
                                    'age': {
                                        'minimum': 0,
                                        'title': 'Age',
                                        'type': 'integer'
                                    }
                                },
                                'required': [
                                    'name',
                                    'age'
                                ],
                                'title': 'User',
                                'type': 'object'
                            },
                            'title': 'Items',
                            'type': 'array'
                        },
                        'next_token': {
                            'anyOf': [
                                {
                                    'type': 'string'
                                },
                                {
                                    'type': 'null'
                                }
                            ],
                            'title': 'Next Token'
                        }
                    },
                    'required': [
                        'items',
                        'next_token'
                    ],
                    'title': 'Users',
                    'type': 'object'
                }
            },
            'securitySchemes': {
                'access_token': {
                    'type': 'apiKey',
                    'description': 'Simple token authentication. The same as AWS Cognito and AWS Api gateway integration has',
                    'name': 'Authorization',
                    'in': 'header'
                }
            }
        }
    }
