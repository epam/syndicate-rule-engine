from functools import partial
from http import HTTPStatus
from itertools import chain
from typing import Iterable

from modular_sdk.commons.constants import ApplicationType, ParentType, \
    ParentScope
from modular_sdk.models.parent import Parent
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.parent_service import ParentService

from handlers import AbstractHandler, Mapping
from helpers.constants import (
    CustodianEndpoint,
    HTTPMethod,
)
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from onprem.tasks import sync_license
from services import SP
from services.abs_lambda import ProcessedEvent
from services.clients.lambda_func import LambdaClient
from services.license_manager_service import LicenseManagerService
from services.license_service import LicenseService
from services.modular_helpers import (
    ResolveParentsPayload,
    build_parents,
    get_activation_dto,
    split_into_to_keep_to_delete,
    get_main_scope
)
from services.ruleset_service import RulesetService
from validators.swagger_request_models import (
    BaseModel,
    LicenseActivationPutModel,
    LicensePostModel,
    LicenseActivationPatchModel
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class LicenseHandler(AbstractHandler):
    """
    Manage License API
    """

    def __init__(self,
                 self_service: LicenseService,
                 ruleset_service: RulesetService,
                 license_manager_service: LicenseManagerService,
                 lambda_client: LambdaClient,
                 application_service: ApplicationService,
                 parent_service: ParentService):
        self.service = self_service
        self.ruleset_service = ruleset_service
        self.lambda_client = lambda_client
        self.license_manager_service = license_manager_service
        self.aps = application_service
        self.ps = parent_service

    @classmethod
    def build(cls) -> 'LicenseHandler':
        return cls(
            self_service=SP.license_service,
            ruleset_service=SP.ruleset_service,
            license_manager_service=SP.license_manager_service,
            lambda_client=SP.lambda_client,
            application_service=SP.modular_client.application_service(),
            parent_service=SP.modular_client.parent_service()
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.LICENSES: {
                HTTPMethod.POST: self.post_license,
                HTTPMethod.GET: self.query_licenses
            },
            CustodianEndpoint.LICENSES_LICENSE_KEY: {
                HTTPMethod.GET: self.get_license,
                HTTPMethod.DELETE: self.delete_license
            },
            CustodianEndpoint.LICENSE_LICENSE_KEY_ACTIVATION: {
                HTTPMethod.PUT: self.activate_license,
                HTTPMethod.PATCH: self.update_activation,
                HTTPMethod.DELETE: self.deactivate_license,
                HTTPMethod.GET: self.get_activation
            },
            CustodianEndpoint.LICENSES_LICENSE_KEY_SYNC: {
                HTTPMethod.POST: self.license_sync
            },
        }

    def get_all_activations(self, license_key: str,
                            customer: str | None = None) -> Iterable[Parent]:
        it = self.ps.i_list_application_parents(
            application_id=license_key,
            type_=ParentType.CUSTODIAN_LICENSES,
            rate_limit=3
        )
        if customer:
            it = filter(lambda p: p.customer_id == customer, it)
        return it

    @validate_kwargs
    def activate_license(self, event: LicenseActivationPutModel,
                         license_key: str, _pe: ProcessedEvent):
        lic = self.service.get_nullable(license_key)
        if not lic:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                f'License {license_key} not found'
            ).exc()
        # either ALL & [cloud] & [exclude] or tenant_names
        # Should not be many
        payload = ResolveParentsPayload(
            parents=list(
                self.get_all_activations(license_key, event.customer)),
            tenant_names=event.tenant_names,
            exclude_tenants=event.exclude_tenants,
            clouds=event.clouds,
            all_tenants=event.all_tenants
        )
        to_keep, to_delete = split_into_to_keep_to_delete(payload)
        for parent in to_delete:
            self.ps.force_delete(parent)
        to_create = build_parents(
            payload=payload,
            parent_service=self.ps,
            application_id=lic.license_key,
            customer_id=event.customer,
            type_=ParentType.CUSTODIAN_LICENSES,
            created_by=_pe['cognito_user_id'],
        )
        for parent in to_create:
            self.ps.save(parent)

        return build_response(content=get_activation_dto(
            chain(to_keep, to_create)
        ))

    @validate_kwargs
    def update_activation(self, event: LicenseActivationPatchModel,
                          license_key: str, _pe: ProcessedEvent):
        lic = self.service.get_nullable(license_key)
        if not lic:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                f'License {license_key} not found'
            ).exc()
        parents = list(self.get_all_activations(license_key, event.customer))
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
            self.ps.force_delete(parent)
        to_create = build_parents(
            payload=payload,
            parent_service=self.ps,
            application_id=lic.license_key,
            customer_id=event.customer,
            type_=ParentType.CUSTODIAN_LICENSES,
            created_by=_pe['cognito_user_id'],
        )
        for parent in to_create:
            self.ps.save(parent)
        return build_response(content=get_activation_dto(
            chain(to_keep, to_create)
        ))

    @validate_kwargs
    def get_activation(self, event: BaseModel, license_key: str):
        lic = self.service.get_nullable(license_key)
        if not lic:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                f'License {license_key} not found'
            ).exc()
        activations = self.get_all_activations(license_key, event.customer)
        return build_response(content=get_activation_dto(activations))

    @validate_kwargs
    def deactivate_license(self, event: BaseModel, license_key: str):
        lic = self.service.get_nullable(license_key)
        if not lic:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                f'License {license_key} not found'
            ).exc()
        activations = self.get_all_activations(
            license_key=lic.license_key,
            customer=event.customer
        )
        for parent in activations:
            self.ps.force_delete(parent)
        return build_response(code=HTTPStatus.NO_CONTENT)

    @validate_kwargs
    def get_license(self, event: BaseModel, license_key: str):
        obj = self.service.get_nullable(license_key)
        if not obj or event.customer not in obj.customers:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'License not found').exc()
        return build_response(self.service.dto(obj))

    @validate_kwargs
    def query_licenses(self, event: BaseModel):
        # TODO, pagination
        applications = self.aps.i_get_application_by_customer(
            customer_id=event.customer,
            application_type=ApplicationType.CUSTODIAN_LICENSES.value,
            deleted=False
        )
        licenses = self.service.to_licenses(applications)
        if event.customer:
            licenses = filter(lambda x: event.customer in x.customers,
                              licenses)
        return build_response(content=map(self.service.dto, licenses))

    @validate_kwargs
    def post_license(self, event: LicensePostModel, _pe: ProcessedEvent):
        license_key = self.activate_license_lm(event.tenant_license_key,
                                               event.customer)
        license_obj = self.service.get_nullable(license_key)
        if license_obj:
            _LOG.info(f'License object {license_key} already exists')
        else:
            _LOG.info('License object does not exist. Creating')
            license_obj = self.service.create(
                license_key=license_key,
                customer=event.customer,
                created_by=_pe['cognito_user_id'],
            )
        self.service.save(license_obj)
        sync_license.apply_async(([license_key],), countdown=3)

        return build_response(
            code=HTTPStatus.ACCEPTED,
            content=self.service.dto(license_obj)
        )

    @validate_kwargs
    def delete_license(self, event: BaseModel, license_key: str):
        _success = partial(
            build_response,
            code=HTTPStatus.NO_CONTENT
        )
        lic = self.service.get_nullable(license_key)
        if not lic or event.customer and event.customer != lic.customer:
            return _success()
        self.service.delete(lic)
        self.service.remove_rulesets_for_license(lic)
        activations = self.get_all_activations(
            license_key=lic.license_key,
            customer=event.customer
        )
        for parent in activations:
            self.ps.force_delete(parent)
        return _success()

    @validate_kwargs
    def license_sync(self, event: BaseModel, license_key: str):
        """
        Returns a response from an asynchronously invoked
        sync-concerned lambda, `license-updater`.
        :return:Dict[code=202]
        """
        sync_license.delay([license_key])
        return build_response(
            code=HTTPStatus.ACCEPTED,
            content='License is being synchronized'
        )

    def activate_license_lm(self, tenant_license_key: str,
                            customer: str) -> str:
        forbid = ResponseFactory(HTTPStatus.FORBIDDEN).message(
            'License manager does not allow to active the license'
        )
        response = self.license_manager_service.cl.activate_customer(
            customer, tenant_license_key
        )
        if not response:
            _LOG.info('Not successful response from LM')
            raise forbid.exc()
        return response[0]
