from http import HTTPStatus

from handlers import AbstractHandler, Mapping
from helpers import NextToken
from helpers.constants import Endpoint, HTTPMethod
from helpers.lambda_response import ResponseFactory, build_response
from services import SP
from services.rbac_service import PolicyService
from validators.swagger_request_models import (
    BaseModel,
    BasePaginationModel,
    PolicyPatchModel,
    PolicyPostModel,
)
from validators.utils import validate_kwargs


class PolicyHandler(AbstractHandler):
    """
    Manage Policy API
    """

    def __init__(self, policy_service: PolicyService):
        self._policy_service = policy_service

    @classmethod
    def build(cls):
        return cls(policy_service=SP.policy_service)

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.POLICIES: {
                HTTPMethod.POST: self.create_policy,
                HTTPMethod.GET: self.list_policies
            },
            Endpoint.POLICIES_NAME: {
                HTTPMethod.GET: self.get_policy,
                HTTPMethod.PATCH: self.update_policy,
                HTTPMethod.DELETE: self.delete_policy,
            }
        }

    @validate_kwargs
    def get_policy(self, event: BaseModel, name: str):
        item = self._policy_service.get_nullable(event.customer_id, name)
        if not item:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._policy_service.not_found_message(name)
            ).exc()
        return build_response(content=self._policy_service.dto(item))

    @validate_kwargs
    def list_policies(self, event: BasePaginationModel):
        cursor = self._policy_service.query(
            customer=event.customer_id,
            limit=event.limit,
            last_evaluated_key=NextToken.deserialize(event.next_token).value
        )
        items = tuple(cursor)
        return ResponseFactory().items(
            it=map(self._policy_service.dto, items),
            next_token=NextToken(cursor.last_evaluated_key)
        ).build()

    @validate_kwargs
    def create_policy(self, event: PolicyPostModel):
        customer = event.customer_id
        name = event.name
        existing = self._policy_service.get_nullable(customer, name)
        if existing:
            return build_response(
                code=HTTPStatus.CONFLICT,
                content=f'Policy with name {name} already exists'
            )
        policy = self._policy_service.create(
            customer=customer,
            name=name,
            description=event.description,
            permissions=[p.value for p in event.permissions],
            tenants=tuple(event.tenants),
            effect=event.effect
        )
        self._policy_service.save(policy)
        return build_response(content=self._policy_service.dto(policy),
                              code=HTTPStatus.CREATED)

    @validate_kwargs
    def update_policy(self, event: PolicyPatchModel, name: str):
        customer = event.customer_id
        policy = self._policy_service.get_nullable(customer, name)
        if not policy:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Policy with name {name} already not found'
            )
        to_attach = {p.value for p in event.permissions_to_attach}
        to_detach = {p.value for p in event.permissions_to_detach}
        permission = set(policy.permissions or ())
        permission -= to_detach
        permission |= to_attach
        policy.permissions = sorted(permission)

        tenants = set(policy.tenants or ())
        tenants -= event.tenants_to_remove
        tenants |= event.tenants_to_add
        policy.tenants = sorted(tenants)

        if event.effect:
            policy.effect = event.effect.value
        if event.description:
            policy.description = event.description

        self._policy_service.save(policy)

        return build_response(
            code=HTTPStatus.OK,
            content=self._policy_service.dto(policy)
        )

    @validate_kwargs
    def delete_policy(self, event: BaseModel, name: str):
        policy = self._policy_service.get_nullable(event.customer_id, name)
        if policy:
            self._policy_service.delete(policy)
        return build_response(code=HTTPStatus.NO_CONTENT)

    # @validate_kwargs
    # def delete_policy_cache(self, event: PolicyCacheDeleteModel):
    #     name = event.name
    #     customer = event.customer
    #     self._iam_service.clean_policy_cache(customer=customer, name=name)
    #     return build_response(code=HTTPStatus.NO_CONTENT)
