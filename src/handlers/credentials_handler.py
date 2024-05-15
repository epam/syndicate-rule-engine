"""
Maestro applications with credentials and CUSTODIAN_ACCESS parent
"""

from functools import cached_property
from http import HTTPStatus
from itertools import chain
from typing import Iterable, Literal

from modular_sdk.commons.constants import ApplicationType, ParentType
from modular_sdk.models.application import Application
from modular_sdk.models.parent import Parent
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.parent_service import ParentService

from handlers import AbstractHandler, Mapping
from helpers import NextToken
from helpers.constants import Cloud, CustodianEndpoint, HTTPMethod
from helpers.lambda_response import ResponseFactory, build_response
from services import SP
from services.abs_lambda import ProcessedEvent
from services.modular_helpers import (
    ResolveParentsPayload,
    build_parents,
    get_activation_dto,
    split_into_to_keep_to_delete,
)
from validators.swagger_request_models import (
    BaseModel,
    CredentialsBindModel,
    CredentialsQueryModel,
)
from helpers.log_helper import get_logger
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class CredentialsHandler(AbstractHandler):
    __slots__ = '_aps', '_ps'

    cloud_to_app_types = {
        Cloud.AWS: (ApplicationType.AWS_ROLE, 
                    ApplicationType.AWS_CREDENTIALS),
        Cloud.AZURE: (ApplicationType.AZURE_CREDENTIALS, 
                      ApplicationType.AZURE_CERTIFICATE),
        Cloud.GOOGLE: (ApplicationType.GCP_COMPUTE_ACCOUNT, 
                       ApplicationType.GCP_SERVICE_ACCOUNT)
    }
    all_types = set(chain.from_iterable(cloud_to_app_types.values()))

    def __init__(self, application_service: ApplicationService,
                 parent_service: ParentService):
        self._aps = application_service
        self._ps = parent_service

    @classmethod
    def build(cls):
        return cls(
            application_service=SP.modular_client.application_service(),
            parent_service=SP.modular_client.parent_service()
        )

    @cached_property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.CREDENTIALS: {
                HTTPMethod.GET: self.query
            },
            CustodianEndpoint.CREDENTIALS_ID: {
                HTTPMethod.GET: self.get
            },
            CustodianEndpoint.CREDENTIALS_ID_BINDING: {
                HTTPMethod.PUT: self.bind,
                HTTPMethod.DELETE: self.unbind,
                HTTPMethod.GET: self.get_binding
            },
        }

    def _get_app(self, aid: str, customer_id: str) -> Application | None:
        app = self._aps.get_application_by_id(aid)
        if (not app or app.is_deleted or app.customer_id != customer_id
                or app.type not in self.all_types):
            return
        return app

    @staticmethod
    def _app_cloud(app: Application) -> Literal['AWS', 'AZURE', 'GOOGLE']:
        """
        As long as maestro does not change their types, this function 
        should work as expected
        """
        _LOG.info(f'Application type: {app.type}')
        cl = app.type.split('_', maxsplit=1)[0]
        _LOG.debug(f'Application cloud from type: {cl}')
        if cl == 'GCP':
            _LOG.debug('Changing cloud to GOOGLE instead of GCP')
            cl = 'GOOGLE'
        assert cl in ('AWS', 'AZURE', 'GOOGLE'), 'A bug found'
        return cl

    @staticmethod
    def get_dto(app: Application) -> dict:
        return {
            'id': app.application_id,
            'type': app.type,
            'description': app.description,
            'has_secret': not not app.secret,
            'credentials': app.meta.as_dict()
        }

    @validate_kwargs
    def get(self, event: BaseModel, id: str):
        app = self._get_app(id, event.customer_id)
        if not app:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'Credentials item not found'
            ).exc()
        return build_response(self.get_dto(app))

    @validate_kwargs
    def query(self, event: CredentialsQueryModel):
        cloud = event.cloud
        if cloud == 'GOOGLE':
            # I doubt Maestro will change their application types names.
            # so we can do this hack and use prefix as range key condition
            cloud = 'GCP'
        cursor = self._aps.query_by_customer(
            customer=event.customer,
            range_key_condition=Application.type.startswith(cloud),
            filter_condition=(Application.is_deleted == False),
            limit=event.limit,
            last_evaluated_key=NextToken.deserialize(event.next_token).value,
        )
        items = tuple(cursor)
        return ResponseFactory().items(
            it=map(self.get_dto, items),
            next_token=NextToken(cursor.last_evaluated_key)
        ).build()

    def get_all_activations(self, application_id: str,
                            customer: str | None = None) -> Iterable[Parent]:
        it = self._ps.i_list_application_parents(
            application_id=application_id,
            type_=ParentType.CUSTODIAN_ACCESS,
            rate_limit=3
        )
        if customer:
            it = filter(lambda p: p.customer_id == customer, it)
        return it

    @validate_kwargs
    def bind(self, event: CredentialsBindModel, id: str, _pe: ProcessedEvent):
        app = self._get_app(id, event.customer_id)
        _LOG.info(f'Application received: {app}')

        if not app:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'Credentials item not found'
            ).exc()
        if event.all_tenants:
            _LOG.info('Resolving application clouds')
            clouds = {self._app_cloud(app)}
        else:
            clouds = set()
        payload = ResolveParentsPayload(
            parents=list(self.get_all_activations(app.application_id, 
                                                  event.customer)),
            tenant_names=event.tenant_names,
            exclude_tenants=event.exclude_tenants,
            clouds=clouds,
            all_tenants=event.all_tenants
        )
        to_keep, to_delete = split_into_to_keep_to_delete(payload)
        for parent in to_delete:
            self._ps.force_delete(parent)
        to_create = build_parents(
            payload=payload,
            parent_service=self._ps,
            application_id=app.application_id,
            customer_id=event.customer_id,
            type_=ParentType.CUSTODIAN_ACCESS,
            created_by=_pe['cognito_user_id'],
        )
        for parent in to_create:
            self._ps.save(parent)
        return build_response(
            content=get_activation_dto(chain(to_keep, to_create))
        )

    @validate_kwargs
    def unbind(self, event: BaseModel, id: str):
        app = self._get_app(id, event.customer_id)
        if not app:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'Credentials item not found'
            ).exc()
        activations = self.get_all_activations(
            application_id=app.application_id,
            customer=event.customer_id
        )
        for parent in activations:
            self._ps.force_delete(parent)
        return build_response(code=HTTPStatus.NO_CONTENT)

    @validate_kwargs
    def get_binding(self, event: BaseModel, id: str):
        app = self._get_app(id, event.customer_id)
        if not app:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'Credentials item not found'
            ).exc()
        activations = self.get_all_activations(app.application_id, 
                                               event.customer_id)
        return build_response(content=get_activation_dto(activations))

