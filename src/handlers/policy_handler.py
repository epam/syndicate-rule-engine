from helpers import RESPONSE_RESOURCE_NOT_FOUND_CODE, \
    build_response, RESPONSE_OK_CODE, \
    RESPONSE_CONFLICT, RESPONSE_BAD_REQUEST_CODE
from helpers.constants import CUSTOMER_ATTR, NAME_ATTR, \
    PERMISSIONS_ATTR, DELETE_METHOD, POST_METHOD, \
    GET_METHOD, PATCH_METHOD, PERMISSIONS_TO_ATTACH, PERMISSIONS_TO_DETACH, \
    PARAM_USER_CUSTOMER
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from services.rbac.iam_cache_service import CachedIamService
from services.rbac.access_control_service import AccessControlService
from typing import Iterable

_LOG = get_logger(__name__)


class PolicyHandler:
    """
    Manage Policy API
    """

    def __init__(self, cached_iam_service: CachedIamService,
                 access_control_service: AccessControlService):
        self._iam_service = cached_iam_service
        self._access_control_service = access_control_service

    def define_action_mapping(self):
        return {
            '/policies': {
                GET_METHOD: self.get_policy,
                POST_METHOD: self.create_policy,
                PATCH_METHOD: self.update_policy,
                DELETE_METHOD: self.delete_policy,
            },
            '/policies/cache': {
                DELETE_METHOD: self.delete_policy_cache
            },
        }

    def get_policy(self, event: dict):
        name = event.get(NAME_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        it = self._iam_service.list_policies(
            customer=customer, name=name
        )
        return build_response(content=(
            self._iam_service.get_dto(entity) for entity in it
        ))

    def create_policy(self, event):
        name = event.get(NAME_ATTR)
        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        permissions: set = event.get(PERMISSIONS_ATTR)
        self.ensure_permissions_allowed(event.get(PARAM_USER_CUSTOMER),
                                        permissions)
        existing = self._iam_service.get_policy(customer, name)
        if existing:
            return build_response(
                code=RESPONSE_CONFLICT,
                content=f'Policy with name {name} already exists'
            )
        policy = self._iam_service.create_policy({
            'name': name,
            'customer': customer,
            'permissions': list(permissions)
        })
        self._iam_service.save(policy)
        return build_response(content=self._iam_service.get_dto(policy))

    def ensure_permissions_allowed(self, user_customer: str,
                                   permissions: Iterable[str]):
        not_allowed = []
        for permission in permissions:
            if not self._access_control_service.is_permission_allowed(
                    customer=user_customer, permission=permission):
                not_allowed.append(permission)
        if not_allowed:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Such permissions not allowed: {", ".join(not_allowed)}'
            )

    def update_policy(self, event: dict):
        name = event.get(NAME_ATTR)
        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        to_attach: set = event.get(PERMISSIONS_TO_ATTACH)
        to_detach: set = event.get(PERMISSIONS_TO_DETACH)
        self.ensure_permissions_allowed(event.get(PARAM_USER_CUSTOMER),
                                        to_attach)
        policy = self._iam_service.get_policy(customer, name)
        if not policy:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Policy with name {name} already not found'
            )
        permission = set(policy.permissions or [])
        permission -= to_detach
        permission |= to_attach
        policy.permissions = list(permission)
        self._iam_service.save(policy)

        return build_response(
            code=RESPONSE_OK_CODE,
            content=self._iam_service.get_dto(policy)
        )

    def delete_policy(self, event):
        name = event.get(NAME_ATTR)
        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        policy = self._iam_service.get_policy(customer, name)
        if policy:
            self._iam_service.delete(policy)
        return build_response(
            content=f'Policy:{name!r} was successfully deleted'
        )

    def delete_policy_cache(self, event):
        name = event.get(NAME_ATTR)
        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        self._iam_service.clean_policy_cache(customer=customer, name=name)
        return build_response(content='Cleaned')
