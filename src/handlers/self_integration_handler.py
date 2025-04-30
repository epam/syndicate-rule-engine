from http import HTTPStatus
from itertools import chain
from typing import Iterable

from modular_sdk.commons.constants import ApplicationType, ParentScope, ParentType
from modular_sdk.models.application import Application
from modular_sdk.models.parent import Parent
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.impl.maestro_credentials_service import (
    CustodianApplicationMeta,
)
from modular_sdk.services.parent_service import ParentService
from modular_sdk.services.ssm_service import AbstractSSMClient

from handlers import AbstractHandler, Mapping
from helpers.constants import CustodianEndpoint, HTTPMethod
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from services import SP
from services.abs_lambda import ProcessedEvent
from services.clients.cognito import BaseAuthClient
from services.environment_service import EnvironmentService
from services.modular_helpers import (
    ResolveParentsPayload,
    build_parents,
    get_activation_dto,
    get_main_scope,
    split_into_to_keep_to_delete,
)
from validators.swagger_request_models import (
    BaseModel,
    SelfIntegrationPatchModel,
    SelfIntegrationPutModel,
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class SelfIntegrationHandler(AbstractHandler):
    """
    Self integration means that we create an application with type CUSTODIAN
    that contains access to custodian instance so that other users of
    modular sdk (maestro) could use it. One integration per customer is allowed
    """

    def __init__(self, application_service: ApplicationService,
                 parent_service: ParentService,
                 users_client: BaseAuthClient,
                 environment_service: EnvironmentService,
                 ssm_service: AbstractSSMClient):
        self._aps = application_service
        self._ps = parent_service
        self._uc = users_client
        self._env = environment_service
        self._ssm = ssm_service

    @classmethod
    def build(cls) -> 'SelfIntegrationHandler':
        return cls(
            application_service=SP.modular_client.application_service(),
            parent_service=SP.modular_client.parent_service(),
            users_client=SP.users_client,
            environment_service=SP.environment_service,
            ssm_service=SP.modular_client.assume_role_ssm_service()
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.INTEGRATIONS_SELF: {
                HTTPMethod.PUT: self.put,
                HTTPMethod.GET: self.get,
                HTTPMethod.DELETE: self.delete,
                HTTPMethod.PATCH: self.patch
            },
        }

    def validate_username(self, username: str, customer: str):
        """
        May raise CustodianException
        :param customer:
        :param username:
        :return:
        """
        user = self._uc.get_user_by_username(username)
        if user and user.customer == customer:
            return
        raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
            f'User {username} not found inside {customer} customer'
        ).exc()

    def set_user_password(self, application: Application, password: str):
        """
        Modifies the incoming application with secret
        :param application:
        :param password:
        """
        secret_name = application.secret
        if not secret_name:
            secret_name = self._ssm.safe_name(
                name=application.customer_id,
                prefix='m3.custodian.application',
                date=False
            )
        _LOG.debug('Saving password to SSM')
        secret = self._ssm.put_parameter(
            name=secret_name,
            value=password
        )
        if not secret:
            _LOG.warning('Something went wrong trying to same password '
                         'to ssm. Keeping application.secret empty')
        _LOG.debug('Password was saved to SSM')
        application.secret = secret

    def get_all_activations(self, application_id: str,
                            customer: str | None = None) -> Iterable[Parent]:
        it = self._ps.i_list_application_parents(
            application_id=application_id,
            type_=ParentType.CUSTODIAN,
            rate_limit=3
        )
        if customer:
            it = filter(lambda p: p.customer_id == customer, it)
        return it

    @validate_kwargs
    def put(self, event: SelfIntegrationPutModel, _pe: ProcessedEvent):
        customer = event.customer
        application = next(self._aps.list(
            customer=customer,
            _type=ApplicationType.CUSTODIAN.value,
            limit=1,
            deleted=False
        ), None)
        if not application:
            application = self._aps.build(
                customer_id=event.customer,
                created_by=_pe['cognito_user_id'],
                type=ApplicationType.CUSTODIAN.value,
                description=event.description,
                is_deleted=False,
            )

        self.validate_username(event.username, customer)
        meta = CustodianApplicationMeta.from_dict({})
        if event.auto_resolve_access:
            meta.update_host(
                host=SP.tls.__dict__.get('host'),
                stage=SP.tls.__dict__.get('stage')
            )
        else:  # url
            url = event.url
            meta.update_host(
                host=url.host,
                port=int(url.port) if url.port else None,
                protocol=url.scheme,
                stage=url.path
            )
        if event.results_storage:
            meta.results_storage = event.results_storage
        meta.username = event.username
        application.meta = meta.dict()
        self.set_user_password(application, event.password)

        self._aps.save(application)

        # creating parents
        payload = ResolveParentsPayload(
            parents=list(self.get_all_activations(application.application_id,
                                                  event.customer)),
            tenant_names=event.tenant_names,
            exclude_tenants=event.exclude_tenants,
            clouds=event.clouds,
            all_tenants=event.all_tenants
        )
        to_keep, to_delete = split_into_to_keep_to_delete(payload)
        for parent in to_delete:
            self._ps.force_delete(parent)
        to_create = build_parents(
            payload=payload,
            parent_service=self._ps,
            application_id=application.application_id,
            customer_id=event.customer,
            type_=ParentType.CUSTODIAN,
            created_by=_pe['cognito_user_id'],
        )
        for parent in to_create:
            self._ps.save(parent)

        return build_response(content=self.get_dto(application,
                                                   chain(to_create, to_keep)),
                              code=HTTPStatus.CREATED)

    @validate_kwargs
    def get(self, event: BaseModel):
        customer = event.customer
        existing = next(self._aps.list(
            customer=customer,
            _type=ApplicationType.CUSTODIAN.value,
            limit=1,
            deleted=False
        ), None)
        if not existing:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                f'Self integration was not created for customer {customer}'
            ).exc()
        parents = self.get_all_activations(existing.application_id)
        return build_response(self.get_dto(existing, parents))

    @validate_kwargs
    def delete(self, event: BaseModel):
        customer = event.customer
        existing = next(self._aps.list(
            customer=customer,
            _type=ApplicationType.CUSTODIAN.value,
            limit=1,
            deleted=False
        ), None)
        if not existing:
            return build_response(code=HTTPStatus.NO_CONTENT)
        self._aps.mark_deleted(existing)
        if name := existing.secret:
            self._ssm.delete_parameter(name)
        for parent in self.get_all_activations(existing.application_id):
            self._ps.force_delete(parent)
        return build_response(code=HTTPStatus.NO_CONTENT)

    @validate_kwargs
    def patch(self, event: SelfIntegrationPatchModel, _pe: ProcessedEvent):
        customer = event.customer
        existing = next(self._aps.list(
            customer=customer,
            _type=ApplicationType.CUSTODIAN.value,
            limit=1,
            deleted=False
        ), None)
        if not existing:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                f'Self integration was not created for customer {customer}'
            ).exc()
        parents = list(self.get_all_activations(existing.application_id))
        payload = ResolveParentsPayload.from_parents_list(parents)
        match get_main_scope(parents):
            case ParentScope.SPECIFIC:
                payload.tenant_names.update(event.add_tenants)
                payload.tenant_names.difference_update(event.remove_tenants)
            case ParentScope.ALL:
                payload.exclude_tenants.difference_update(event.add_tenants)
                payload.exclude_tenants.update(event.remove_tenants)
        to_keep, to_delete = split_into_to_keep_to_delete(payload)

        for parent in to_delete:
            self._ps.force_delete(parent)
        to_create = build_parents(
            payload=payload,
            parent_service=self._ps,
            application_id=existing.application_id,
            customer_id=event.customer,
            type_=ParentType.CUSTODIAN,
            created_by=_pe['cognito_user_id'],
        )
        for parent in to_create:
            self._ps.save(parent)

        return build_response(content=self.get_dto(existing,
                                                   chain(to_create, to_keep)),
                              code=HTTPStatus.OK)

    @staticmethod
    def get_dto(application: Application, parents: Iterable[Parent]) -> dict:
        meta = CustodianApplicationMeta.from_dict(application.meta.as_dict())
        return {
            **get_activation_dto(parents),
            'customer_name': application.customer_id,
            'description': application.description,
            **meta.dict()
        }
