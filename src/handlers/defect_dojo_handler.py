from functools import cached_property
from http import HTTPStatus
from typing import Iterable

from modular_sdk.commons.constants import ApplicationType, ParentType
from modular_sdk.models.parent import Parent
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.parent_service import ParentService

from handlers import AbstractHandler, Mapping
from helpers.constants import CustodianEndpoint, HTTPMethod
from helpers.lambda_response import ResponseFactory, build_response
from services import SP
from services.abs_lambda import ProcessedEvent
from services.defect_dojo_service import DefectDojoParentMeta, DefectDojoService
from services.modular_helpers import (
    ResolveParentsPayload,
    build_parents,
    get_activation_dto,
)
from validators.swagger_request_models import (
    BaseModel,
    DefectDojoActivationPutModel,
    DefectDojoPostModel,
    DefectDojoQueryModel,
)
from validators.utils import validate_kwargs


class DefectDojoHandler(AbstractHandler):
    def __init__(self, defect_dojo_service: DefectDojoService,
                 application_service: ApplicationService,
                 parent_service: ParentService):
        self._dds = defect_dojo_service
        self._aps = application_service
        self._ps = parent_service

    @classmethod
    def build(cls) -> 'DefectDojoHandler':
        return cls(
            defect_dojo_service=SP.defect_dojo_service,
            application_service=SP.modular_client.application_service(),
            parent_service=SP.modular_client.parent_service()
        )

    @cached_property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO: {
                HTTPMethod.POST: self.post,
                HTTPMethod.GET: self.query
            },
            CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO_ID: {
                HTTPMethod.GET: self.get,
                HTTPMethod.DELETE: self.delete
            },
            CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO_ID_ACTIVATION: {
                HTTPMethod.PUT: self.put_activation,
                HTTPMethod.DELETE: self.delete_activation,
                HTTPMethod.GET: self.get_activation
            }
        }

    @validate_kwargs
    def post(self, event: DefectDojoPostModel, _pe: ProcessedEvent):
        dojo = self._dds.create(
            customer=event.customer,
            description=event.description,
            created_by=_pe['cognito_user_id']
        )
        url = event.url
        dojo.update_host(
            host=url.host,
            port=int(url.port) if url.port else None,
            protocol=url.scheme,
            stage=url.path
        )
        self._dds.set_dojo_api_key(dojo, event.api_key)
        # todo check connect
        self._dds.save(dojo)
        return build_response(content=self._dds.dto(dojo),
                              code=HTTPStatus.CREATED)

    @validate_kwargs
    def query(self, event: DefectDojoQueryModel):
        cursor = self._aps.i_get_application_by_customer(
            customer_id=event.customer,
            application_type=ApplicationType.DEFECT_DOJO.value
        )
        cursor = self._dds.to_dojos(cursor)
        return build_response(content=map(self._dds.dto, cursor))

    @validate_kwargs
    def get(self, event: BaseModel, id: str):
        item = self._dds.get_nullable(id)
        if not item or event.customer and item.customer != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._dds.not_found_message(id)
            ).exc()
        return build_response(content=self._dds.dto(item))

    @validate_kwargs
    def delete(self, event: BaseModel, id: str):
        item = self._dds.get_nullable(id)
        if not item or event.customer and item.customer != event.customer:
            return build_response(code=HTTPStatus.NO_CONTENT)
        self._dds.delete(item)
        for parent in self.get_all_activations(item.id, event.customer):
            self._ps.force_delete(parent)
        return build_response(code=HTTPStatus.NO_CONTENT)

    def get_all_activations(self, dojo_id: str,
                            customer: str | None = None) -> Iterable[Parent]:
        it = self._ps.i_list_application_parents(
            application_id=dojo_id,
            type_=ParentType.CUSTODIAN_SIEM_DEFECT_DOJO,
            rate_limit=3
        )
        if customer:
            it = filter(lambda p: p.customer_id == customer, it)
        return it

    @staticmethod
    def get_dto(parents: Iterable[Parent],
                meta: DefectDojoParentMeta | dict) -> dict:
        base = get_activation_dto(parents)
        base.update(meta if isinstance(meta, dict) else meta.dto())
        return base

    @validate_kwargs
    def put_activation(self, event: DefectDojoActivationPutModel, id: str, 
                       _pe: ProcessedEvent):
        item = self._dds.get_nullable(id)
        if not item or event.customer and item.customer != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._dds.not_found_message(id)
            ).exc()
        for parent in self.get_all_activations(item.id, event.customer):
            self._ps.force_delete(parent)

        meta = DefectDojoParentMeta(
            scan_type=event.scan_type,
            product_type=event.product_type,
            product=event.product,
            engagement=event.engagement,
            test=event.test,
            send_after_job=event.send_after_job,
            attachment=event.attachment
        )
        to_create = build_parents(
            payload=ResolveParentsPayload(
                parents=list(),
                tenant_names=event.tenant_names,
                exclude_tenants=event.exclude_tenants,
                clouds=event.clouds,
                all_tenants=event.all_tenants
            ),
            parent_service=self._ps,
            application_id=id,
            customer_id=event.customer,
            type_=ParentType.CUSTODIAN_SIEM_DEFECT_DOJO,
            created_by=_pe['cognito_user_id'],
            meta=meta.dict()
        )
        for parent in to_create:
            self._ps.save(parent)
        return build_response(content=self.get_dto(to_create, meta))

    @validate_kwargs
    def delete_activation(self, event: BaseModel, id: str):
        item = self._dds.get_nullable(id)
        if not item or event.customer and item.customer != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._dds.not_found_message(id)
            ).exc()
        for parent in self.get_all_activations(item.id, event.customer):
            self._ps.force_delete(parent)
        return build_response(code=HTTPStatus.NO_CONTENT)

    @validate_kwargs
    def get_activation(self, event: BaseModel, id: str):
        item = self._dds.get_nullable(id)
        if not item or event.customer and item.customer != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._dds.not_found_message(id)
            ).exc()
        parents = list(self.get_all_activations(item.id, event.customer))
        if not parents:
            return build_response(self.get_dto([], {}))
        # first because they are all equal
        meta = DefectDojoParentMeta.from_dict(parents[0].meta.as_dict())
        return build_response(self.get_dto(parents, meta))
