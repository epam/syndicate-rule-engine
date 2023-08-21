from datetime import datetime
from typing import Iterable
from helpers import build_response, \
    RESPONSE_RESOURCE_NOT_FOUND_CODE, RESPONSE_OK_CODE, RESPONSE_CONFLICT
from helpers.constants import CUSTOMER_ATTR, NAME_ATTR, EXPIRATION_ATTR, \
    POLICIES_ATTR, POLICIES_TO_ATTACH, POLICIES_TO_DETACH, \
    GET_METHOD, POST_METHOD, PATCH_METHOD, DELETE_METHOD
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from helpers.time_helper import utc_iso
from services.rbac.iam_cache_service import CachedIamService

_LOG = get_logger(__name__)


class RoleHandler:
    """
    Manage Role API
    """

    def __init__(self,cached_iam_service: CachedIamService):
        self._iam_service = cached_iam_service

    def define_action_mapping(self):
        return {
            '/roles': {
                GET_METHOD: self.get_role,
                POST_METHOD: self.create_role,
                PATCH_METHOD: self.update_role,
                DELETE_METHOD: self.delete_role,
            },
            '/roles/cache': {
                DELETE_METHOD: self.delete_role_cache
            },
        }

    def get_role(self, event):
        name = event.get(NAME_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        it = self._iam_service.list_roles(
            customer=customer, name=name
        )
        return build_response(content=(
            self._iam_service.get_dto(entity) for entity in it
        ))

    def create_role(self, event):
        name = event.get(NAME_ATTR)
        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        policies: set = event.get(POLICIES_ATTR)
        expiration: datetime = event.get(EXPIRATION_ATTR)

        existing = self._iam_service.get_role(customer, name)
        if existing:
            return build_response(
                code=RESPONSE_CONFLICT,
                content=f'Role with name {name} already exists'
            )
        self.ensure_policies_exist(customer, policies)
        role = self._iam_service.create_role({
            'name': name,
            'customer': customer,
            'policies': list(policies),
            'expiration': utc_iso(expiration)
        })
        self._iam_service.save(role)
        return build_response(content=self._iam_service.get_dto(role))

    def ensure_policies_exist(self, customer: str, policies: Iterable[str]):
        for name in policies:
            item = self._iam_service.get_policy(customer, name)
            if not item:
                return build_response(
                    code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                    content=f'Policy \'{name}\' not found'
                )

    def update_role(self, event):
        name = event.get(NAME_ATTR)
        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        # todo validate to_attach
        to_attach: set = event.get(POLICIES_TO_ATTACH)
        to_detach: set = event.get(POLICIES_TO_DETACH)
        expiration: datetime = event.get(EXPIRATION_ATTR)

        role = self._iam_service.get_role(customer, name)
        if not role:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Role with name {name} already not found'
            )
        self.ensure_policies_exist(customer, to_attach)
        policies = set(role.policies or [])
        policies -= to_detach
        policies |= to_attach
        role.policies = list(policies)

        if expiration:
            role.expiration = utc_iso(expiration)

        self._iam_service.save(role)

        return build_response(
            code=RESPONSE_OK_CODE,
            content=self._iam_service.get_dto(role)
        )

    def delete_role(self, event):
        name = event.get(NAME_ATTR)
        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        role = self._iam_service.get_role(customer, name)
        if role:
            self._iam_service.delete(role)
        return build_response(
            content=f'No traces of role \'{name}\' left in customer {customer}'
        )

    def delete_role_cache(self, event):
        name = event.get(NAME_ATTR)
        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        self._iam_service.clean_role_cache(customer=customer, name=name)
        return build_response(content='Cleaned')
