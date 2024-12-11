from http import HTTPStatus
from typing import Iterable

from handlers import AbstractHandler, Mapping
from helpers import NextToken
from helpers.constants import CustodianEndpoint, HTTPMethod
from helpers.lambda_response import ResponseFactory, build_response
from helpers.time_helper import utc_iso
from services import SP
from services.rbac_service import PolicyService, RoleService
from validators.swagger_request_models import (
    BaseModel,
    BasePaginationModel,
    RolePatchModel,
    RolePostModel,
)
from validators.utils import validate_kwargs


class RoleHandler(AbstractHandler):
    """
    Manage Role API
    """

    def __init__(self, role_service: RoleService, 
                 policy_service: PolicyService):
        self._role_service = role_service
        self._policy_service = policy_service

    @classmethod
    def build(cls):
        return cls(role_service=SP.role_service, 
                   policy_service=SP.policy_service)

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.ROLES: {
                HTTPMethod.POST: self.create_role,
                HTTPMethod.GET: self.list_roles
            },
            CustodianEndpoint.ROLES_NAME: {
                HTTPMethod.GET: self.get_role,
                HTTPMethod.DELETE: self.delete_role,
                HTTPMethod.PATCH: self.update_role
            }
        }

    @validate_kwargs
    def get_role(self, event: BaseModel, name: str):
        item = self._role_service.get_nullable(event.customer_id, name)
        if not item:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._role_service.not_found_message(name)
            ).exc()
        return build_response(self._role_service.dto(item))

    @validate_kwargs
    def list_roles(self, event: BasePaginationModel):
        cursor = self._role_service.query(
            customer=event.customer_id,
            limit=event.limit,
            last_evaluated_key=NextToken.deserialize(event.next_token).value
        )
        items = tuple(cursor)
        return ResponseFactory().items(
            it=map(self._role_service.dto, items),
            next_token=NextToken(cursor.last_evaluated_key)
        ).build()

    @validate_kwargs
    def create_role(self, event: RolePostModel):
        name = event.name
        customer = event.customer_id
        existing = self._role_service.get_nullable(customer, name)
        if existing:
            raise ResponseFactory(HTTPStatus.CONFLICT).message(
                f'Role with name {name} already exists'
            ).exc()
        self.ensure_policies_exist(customer, event.policies)
        role = self._role_service.create(
            customer=customer,
            name=name,
            policies=tuple(event.policies),
            expiration=event.expiration,
            description=event.description
        )
        self._role_service.save(role)
        return build_response(content=self._role_service.dto(role),
                              code=HTTPStatus.CREATED)

    def ensure_policies_exist(self, customer: str, policies: Iterable[str]):
        for name in policies:
            item = self._policy_service.get_nullable(customer, name)
            if not item:
                raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                    f'Policy {name} not found'
                ).exc()

    @validate_kwargs
    def update_role(self, event: RolePatchModel, name: str):
        customer = event.customer_id

        role = self._role_service.get_nullable(customer, name)
        if not role:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                f'Role with name {name} already not found'
            ).exc()
        self.ensure_policies_exist(customer, event.policies_to_attach)
        policies = set(role.policies or [])
        policies -= event.policies_to_attach
        policies |= event.policies_to_detach
        role.policies = list(policies)

        if event.expiration:
            role.expiration = utc_iso(event.expiration)
        if event.description:
            role.description = event.description

        self._role_service.save(role)

        return build_response(
            code=HTTPStatus.OK,
            content=self._role_service.dto(role)
        )

    @validate_kwargs
    def delete_role(self, event: BaseModel, name: str):
        role = self._role_service.get_nullable(event.customer_id, name)
        if role:
            self._role_service.delete(role)
        return build_response(code=HTTPStatus.NO_CONTENT)

    # @validate_kwargs
    # def delete_role_cache(self, event: RoleCacheDeleteModel):
    #     name = event.name
    #     customer = event.customer
    #     self._iam_service.clean_role_cache(customer=customer, name=name)
    #     return build_response(code=HTTPStatus.NO_CONTENT)
