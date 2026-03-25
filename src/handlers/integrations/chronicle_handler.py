from http import HTTPStatus
from typing import Iterable

from modular_sdk.commons.constants import ApplicationType, ParentType
from modular_sdk.models.parent import Parent
from modular_sdk.models.tenant import Tenant
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.parent_service import ParentService

from handlers import AbstractHandler, Mapping
from helpers import NextToken
from helpers.constants import Endpoint, HTTPMethod
from helpers.lambda_response import ResponseFactory, build_response
from services import SP
from services.abs_lambda import ProcessedEvent
from services.chronicle_service import ChronicleInstanceService, \
    ChronicleParentMeta
from services.modular_helpers import ResolveParentsPayload, \
    build_parents, get_activation_dto, iter_tenants_by_names
from validators.swagger_request_models import (
    BaseModel,
    BasePaginationModel,
    ChroniclePostModel,
    ChronicleActivationPutModel
)
from validators.utils import validate_kwargs


class ChronicleHandler(AbstractHandler):
    def __init__(self, chronicle_instance_service: ChronicleInstanceService,
                 application_service: ApplicationService,
                 parent_service: ParentService):
        self._chr = chronicle_instance_service
        self._aps = application_service
        self._ps = parent_service

    @classmethod
    def build(cls) -> 'ChronicleHandler':
        return cls(
            chronicle_instance_service=SP.chronicle_instance_service,
            application_service=SP.modular_client.application_service(),
            parent_service=SP.modular_client.parent_service()
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.INTEGRATIONS_CHRONICLE: {
                HTTPMethod.POST: self.post,
                HTTPMethod.GET: self.query
            },
            Endpoint.INTEGRATIONS_CHRONICLE_ID: {
                HTTPMethod.GET: self.get,
                HTTPMethod.DELETE: self.delete
            },
            Endpoint.INTEGRATIONS_CHRONICLE_ID_ACTIVATION: {
                HTTPMethod.PUT: self.put_activation,
                HTTPMethod.DELETE: self.delete_activation,
                HTTPMethod.GET: self.get_activation
            }
        }

    @validate_kwargs
    def post(self, event: ChroniclePostModel, _pe: ProcessedEvent):
        creds_app = self._aps.get_application_by_id(event.credentials_application_id)
        if not creds_app or creds_app.is_deleted or creds_app.type not in (
                ApplicationType.GCP_COMPUTE_ACCOUNT, ApplicationType.GCP_SERVICE_ACCOUNT):
            raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                f'Application with google credentials and '
                f'id {event.credentials_application_id} not found'
            ).exc()
        item = self._chr.create(
            description=event.description,
            customer=event.customer_id,
            created_by=_pe['cognito_user_id'],
            credentials_application_id=event.credentials_application_id,
            endpoint=event.baseurl,
            instance_customer_id=event.instance_customer_id
        )
        self._chr.save(item)
        return build_response(self._chr.dto(item))

    @validate_kwargs
    def query(self, event: BasePaginationModel):
        cursor = self._aps.list(
            customer=event.customer_id,
            _type=ApplicationType.GCP_CHRONICLE_INSTANCE.value,
            deleted=False,
            limit=event.limit,
            last_evaluated_key=NextToken.deserialize(event.next_token).value,
        )
        items = tuple(cursor)
        return ResponseFactory().items(
            it=map(self._chr.dto, self._chr.to_chronicle_instances(items)),
            next_token=NextToken(cursor.last_evaluated_key)
        ).build()

    @validate_kwargs
    def get(self, event: BaseModel, id: str):
        item = self._chr.get_nullable(id)
        if not item or item.is_deleted or event.customer and item.customer != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._chr.not_found_message(id)
            ).exc()
        return build_response(content=self._chr.dto(item))

    @validate_kwargs
    def delete(self, event: BaseModel, id: str):
        item = self._chr.get_nullable(id)
        if not item or item.is_deleted or event.customer and item.customer != event.customer:
            return build_response(code=HTTPStatus.NO_CONTENT)
        self._chr.delete(item)
        for parent in self.get_all_activations(item.id, event.customer):
            self._ps.force_delete(parent)
        return build_response(code=HTTPStatus.NO_CONTENT)

    def get_all_activations(self, chronicle_id: str,
                            customer: str | None = None) -> Iterable[Parent]:
        it = self._ps.i_list_application_parents(
            application_id=chronicle_id,
            type_=ParentType.GCP_CHRONICLE_INSTANCE,
            rate_limit=3
        )
        if customer:
            it = filter(lambda p: p.customer_id == customer, it)
        return it

    @staticmethod
    def get_dto(parents: Iterable[Parent],
                meta: ChronicleParentMeta | dict) -> dict:
        base = get_activation_dto(parents)
        base.update(meta if isinstance(meta, dict) else meta.dto())
        return base

    @validate_kwargs
    def put_activation(self, event: ChronicleActivationPutModel, id: str,
                       _pe: ProcessedEvent):
        # todo these: put_activation, delete_activation, get_activation is
        #  duplicates a lot, maybe we should make a separate func
        item = self._chr.get_nullable(id)
        if not item or item.is_deleted or event.customer and item.customer != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._chr.not_found_message(id)
            ).exc()
        if event.tenant_names:
            it = iter_tenants_by_names(
                tenant_service=self._ps.tenant_service,
                customer=event.customer_id,
                names=event.tenant_names,
                attributes_to_get=(Tenant.name, )
            )
            tenants = {tenant.name for tenant in it}
            if missing := event.tenant_names - tenants:
                raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                    f'Active tenant(s) {", ".join(missing)} not found'
                ).exc()
        for parent in self.get_all_activations(item.id, event.customer):
            self._ps.force_delete(parent)

        meta = ChronicleParentMeta(send_after_job=event.send_after_job,
                                   converter_type=event.convert_to)
        to_create = build_parents(
            payload=ResolveParentsPayload(
                parents=list(),
                tenant_names=event.tenant_names,
                exclude_tenants=event.exclude_tenants,
                clouds=event.clouds,
                all_tenants=event.all_tenants
            ),
            parent_service=self._ps,
            application_id=item.id,
            customer_id=event.customer,
            type_=ParentType.GCP_CHRONICLE_INSTANCE,
            created_by=_pe['cognito_user_id'],
            meta=meta.dict()
        )
        for parent in to_create:
            self._ps.save(parent)
        return build_response(content=self.get_dto(to_create, meta))

    @validate_kwargs
    def delete_activation(self, event: BaseModel, id: str):
        item = self._chr.get_nullable(id)
        if not item or item.is_deleted or event.customer and item.customer != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._chr.not_found_message(id)
            ).exc()
        for parent in self.get_all_activations(item.id, event.customer):
            self._ps.force_delete(parent)
        return build_response(code=HTTPStatus.NO_CONTENT)

    @validate_kwargs
    def get_activation(self, event: BaseModel, id: str):
        item = self._chr.get_nullable(id)
        if not item or item.is_deleted or event.customer and item.customer != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._chr.not_found_message(id)
            ).exc()
        parents = list(self.get_all_activations(item.id, event.customer))
        if not parents:
            return build_response(self.get_dto([], {}))
        # first because they are all equal
        meta = ChronicleParentMeta.from_dict(parents[0].meta.as_dict())
        return build_response(self.get_dto(parents, meta))
