from functools import cached_property
from itertools import chain
from typing import Optional, Dict, Set

from botocore.exceptions import ClientError
from modular_sdk.commons.constants import CUSTODIAN_TYPE, \
    CUSTODIAN_LICENSES_TYPE, SIEM_DEFECT_DOJO_TYPE, DEFECT_DOJO_TYPE, \
    CUSTODIAN_ACCESS_TYPE
from modular_sdk.models.application import Application
from modular_sdk.models.tenant import Tenant

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response, RESPONSE_BAD_REQUEST_CODE, \
    RESPONSE_RESOURCE_NOT_FOUND_CODE, RESPONSE_NO_CONTENT, \
    RESPONSE_FORBIDDEN_CODE
from helpers.constants import CUSTOMER_ATTR, APPLICATION_ID_ATTR, \
    DESCRIPTION_ATTR, CLOUDS_ATTR, SCOPE_ATTR, PARENT_ID_ATTR, \
    DELETE_METHOD, RULES_TO_EXCLUDE_ATTR, PATCH_METHOD, \
    RULES_TO_INCLUDE_ATTR, TENANT_ATTR, SPECIFIC_TENANT_SCOPE, \
    CUSTODIAN_LICENSES_TYPE, TYPE_ATTR, POST_METHOD, GET_METHOD, \
    CLOUD_TO_APP_TYPE
from helpers.enums import ParentType
from helpers.log_helper import get_logger
from models.modular.parents import Parent, ParentMeta, ScopeParentMeta
from services import SERVICE_PROVIDER
from services.modular_service import ModularService
from services.rule_meta_service import RuleService
from helpers import adjust_cloud

_LOG = get_logger(__name__)


class ParentsHandler(AbstractHandler):
    def __init__(self, modular_service: ModularService,
                 rule_service: RuleService):
        self._modular_service = modular_service
        self._rule_service = rule_service
        self._content = ''  # buffer for response

    @classmethod
    def build(cls) -> 'ParentsHandler':
        return cls(
            modular_service=SERVICE_PROVIDER.modular_service(),
            rule_service=SERVICE_PROVIDER.rule_service()
        )

    def define_action_mapping(self) -> dict:
        return {
            '/parents': {
                POST_METHOD: self.post,
                GET_METHOD: self.list
            },
            '/parents/{parent_id}': {
                GET_METHOD: self.get,
                DELETE_METHOD: self.delete,
                PATCH_METHOD: self.patch
            },
            '/parents/tenant-link': {
                POST_METHOD: self.post_tenant_link,
                DELETE_METHOD: self.delete_tenant_link
            }
        }

    def post_tenant_link(self, event: dict) -> dict:
        parent_id = event.get(PARENT_ID_ATTR)
        tenant = event.get(TENANT_ATTR)
        # type_ = event.get(TYPE_ATTR)  # type for parent_map
        customer = event.get(CUSTOMER_ATTR)
        parent = self.get_parent(parent_id, customer)
        if not parent:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Parent with id '
                        f'{parent_id} within your customer not found'
            )
        tenant_item = self._modular_service.get_tenant(tenant)
        if not self._modular_service.is_tenant_valid(tenant_item, customer):
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Tenant {tenant} not found'
            )
        self._assert_allowed_to_link(tenant_item, parent, parent.type)
        try:
            self._modular_service.modular_client.tenant_service().add_to_parent_map(
                tenant=tenant_item, parent=parent, type_=parent.type
            )  # we catch ModularException in abstract_api_handler_lambda
        except ClientError:  # we used to have no rights to write to Tenants...
            return build_response(
                code=RESPONSE_FORBIDDEN_CODE,
                content='You are not allowed to change the tenant'
            )
        return build_response(
            content=f'Tenant {tenant} was successfully linked '
                    f'to parent {parent_id}'
        )

    def _is_allowed_to_link_custodian(self, tenant: Tenant, parent: Parent,
                                      application: Application) -> bool:
        if parent.type != CUSTODIAN_TYPE:
            self._content = f'parent type must be {CUSTODIAN_TYPE} ' \
                            f'for {CUSTODIAN_TYPE} linkage'
            return False
        if application.type != CUSTODIAN_TYPE:
            self._content = f'application type must be {CUSTODIAN_TYPE} ' \
                            f'for {CUSTODIAN_TYPE} linkage'
            return False
        meta = ParentMeta.from_dict(parent.meta.as_dict())
        if meta.scope != SPECIFIC_TENANT_SCOPE:
            self._content = f'parent scope must be {SPECIFIC_TENANT_SCOPE}'
            return False
        if tenant.cloud not in meta.clouds:
            self._content = 'tenant cloud mu be in a list of parent clouds'
            return False
        return True

    def _is_allowed_to_link_custodian_licenses(self, tenant: Tenant,
                                               parent: Parent,
                                               application: Application
                                               ) -> bool:
        if parent.type != CUSTODIAN_LICENSES_TYPE:
            self._content = f'parent type must be {CUSTODIAN_LICENSES_TYPE} ' \
                            f'for {CUSTODIAN_LICENSES_TYPE} linkage'
            return False
        if application.type != CUSTODIAN_LICENSES_TYPE:
            self._content = f'application type must be ' \
                            f'{CUSTODIAN_LICENSES_TYPE} ' \
                            f'for {CUSTODIAN_LICENSES_TYPE} linkage'
            return False
        meta = ParentMeta.from_dict(parent.meta.as_dict())
        if meta.scope != SPECIFIC_TENANT_SCOPE:
            self._content = f'parent scope must be {SPECIFIC_TENANT_SCOPE}'
            return False
        if tenant.cloud not in meta.clouds:
            self._content = 'tenant cloud mu be in a list of parent clouds'
            return False
        return True

    def _is_allowed_to_link_siem_defect_dojo(self, tenant: Tenant,
                                             parent: Parent,
                                             application: Application
                                             ) -> bool:
        if parent.type != SIEM_DEFECT_DOJO_TYPE:
            self._content = f'parent type must be {SIEM_DEFECT_DOJO_TYPE} ' \
                            f'for {SIEM_DEFECT_DOJO_TYPE} linkage'
            return False
        if application.type != DEFECT_DOJO_TYPE:
            self._content = f'application type must be ' \
                            f'{SIEM_DEFECT_DOJO_TYPE} ' \
                            f'for {SIEM_DEFECT_DOJO_TYPE} linkage'
            return False
        meta = ParentMeta.from_dict(parent.meta.as_dict())
        if meta.scope != SPECIFIC_TENANT_SCOPE:
            self._content = f'parent scope must be {SPECIFIC_TENANT_SCOPE}'
            return False
        if tenant.cloud not in meta.clouds:
            self._content = 'tenant cloud mu be in a list of parent clouds'
            return False
        return True

    def _is_allowed_to_link_custodian_access(self, tenant: Tenant,
                                             parent: Parent,
                                             application: Application
                                             ) -> bool:
        if parent.type != CUSTODIAN_ACCESS_TYPE:
            self._content = f'parent type must be {CUSTODIAN_ACCESS_TYPE} ' \
                            f'for {CUSTODIAN_ACCESS_TYPE} linkage'
            return False
        if application.type not in CLOUD_TO_APP_TYPE.get(tenant.cloud):
            self._content = f'application must provide credentials ' \
                            f'for {tenant.cloud} cloud'
            return False
        return True

    def _assert_allowed_to_link(self, tenant: Tenant, parent: Parent,
                                type_: str):
        """
        Tells whether it's allowed to set the parent's ID to the
        tenant's parent_map by key type_
        Tenant.parent_map  -> Parent.type        -> Application.type
        CUSTODIAN          -> CUSTODIAN          -> CUSTODIAN
        CUSTODIAN_LICENSES -> CUSTODIAN_LICENSES -> CUSTODIAN_LICENSES
        SIEM_DEFECT_DOJO   -> SIEM_DEFECT_DOJO   -> DEFECT_DOJO
        CUSTODIAN_ACCESS   -> CUSTODIAN_ACCESS   -> [creds application for tenant's cloud]
        For the first three rows scope must be SPECIFIC_TENANT, cloud be valid
        :param tenant:
        :param parent:
        :param type_:
        :return:
        """
        application = self._modular_service.get_parent_application(parent)
        if not application:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='Parent`s application not found'
            )
        type_method = {
            CUSTODIAN_TYPE: self._is_allowed_to_link_custodian,
            CUSTODIAN_LICENSES_TYPE: self._is_allowed_to_link_custodian_licenses,
            SIEM_DEFECT_DOJO_TYPE: self._is_allowed_to_link_siem_defect_dojo,
            CUSTODIAN_ACCESS_TYPE: self._is_allowed_to_link_custodian_access
        }
        method = type_method[type_]  # type_ is validated before
        if not method(tenant, parent, application):
            _LOG.warning(f'Cannot link: {self._content}')
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Cannot link: {self._content}'
            )

    def delete_tenant_link(self, event: dict) -> dict:
        tenant = event.get(TENANT_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        type_ = event.get(TYPE_ATTR)
        tenant_item = self._modular_service.get_tenant(tenant)
        if not self._modular_service.is_tenant_valid(tenant_item, customer):
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Tenant {tenant} not found'
            )
        try:
            self._modular_service.modular_client.tenant_service().remove_from_parent_map(
                tenant=tenant_item, type_=type_
            )
        except ClientError:  # we used to have no rights to write to Tenants...
            return build_response(
                code=RESPONSE_FORBIDDEN_CODE,
                content='You are not allowed to change tenant'
            )
        return build_response(code=RESPONSE_NO_CONTENT)

    def list(self, event: dict) -> dict:
        customer = event.get(CUSTOMER_ATTR)
        items = self._modular_service.get_customer_bound_parents(
            customer=customer,
            parent_type=ParentType.list(),
            is_deleted=False
        )
        return build_response(
            content=(self._modular_service.get_dto(parent) for parent in items)
        )

    def get(self, event: dict):
        customer = event.get(CUSTOMER_ATTR)
        parent_id = event.get(PARENT_ID_ATTR)
        item = self.get_parent(parent_id, customer)
        if not item:
            return build_response(content=[])
        return build_response(content=self._modular_service.get_dto(item))

    def delete(self, event: dict):
        customer = event.get(CUSTOMER_ATTR)
        parent_id = event.get(PARENT_ID_ATTR)
        item = self.get_parent(parent_id, customer)
        if not item:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Parent with id '
                        f'{parent_id} within your customer not found'
            )
        erased = self._modular_service.delete(item)
        if erased:  # Modular sdk does not remove the parent, just sets is_deleted
            self._modular_service.save(item)
            return build_response(code=RESPONSE_NO_CONTENT)
        return build_response(
            code=RESPONSE_BAD_REQUEST_CODE,
            content='Could not remove the parent. '
                    'Probably it\'s used by some tenants.'
        )

    def get_parent(self, parent_id: str,
                   customer: Optional[str]) -> Optional[Parent]:
        item = self._modular_service.get_parent(parent_id)
        if not item or not ParentType.has(
                item.type) or customer and item.customer_id != customer:
            return
        return item

    @cached_property
    def parent_type_to_application_type_map(self) -> Dict[str, Set[str]]:
        return {
            SIEM_DEFECT_DOJO_TYPE: {DEFECT_DOJO_TYPE},
            CUSTODIAN_LICENSES_TYPE: {CUSTODIAN_LICENSES_TYPE},
            CUSTODIAN_TYPE: {CUSTODIAN_TYPE},
            CUSTODIAN_ACCESS_TYPE: set(
                chain.from_iterable(CLOUD_TO_APP_TYPE.values()))
            # any creds application
        }

    def post(self, event: dict) -> dict:
        customer = event.get(CUSTOMER_ATTR)
        application_id = event.get(APPLICATION_ID_ATTR)
        description: str = event.get(DESCRIPTION_ATTR)
        clouds: set = event.get(CLOUDS_ATTR)
        scope: str = event.get(SCOPE_ATTR)
        rules_to_exclude: set = event.get(RULES_TO_EXCLUDE_ATTR)
        type_ = event.get(TYPE_ATTR)

        app = self._modular_service.get_application(application_id)
        _required_types = self.parent_type_to_application_type_map.get(type_)
        if not app or app.is_deleted or app.customer_id != customer or app.type not in _required_types:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Application {application_id} with type: '
                        f'{", ".join(_required_types)} not found in '
                        f'customer {customer}'
            )

        meta = {}
        if type_ == CUSTODIAN_LICENSES_TYPE:
            _LOG.debug(f'Parent is {CUSTODIAN_LICENSES_TYPE}. '
                       f'Resolving rule ids')
            meta = ParentMeta(
                scope=scope,
                clouds=list(clouds),
                rules_to_exclude=list(
                    self._rule_service.resolved_names(
                        names=rules_to_exclude,
                        clouds=set(adjust_cloud(cl) for cl in clouds)
                    )
                )
            ).dict()
        elif type_ != CUSTODIAN_ACCESS_TYPE:
            _LOG.debug(f'Parent is {type_}. Setting scope and clouds')
            meta = ScopeParentMeta(scope=scope, clouds=list(clouds)).dict()
        parent = self._modular_service.create_parent(
            customer_id=customer,
            parent_type=type_,
            application_id=application_id,
            description=description,
            meta=meta
        )
        self._modular_service.save(parent)

        return build_response(
            content=self._modular_service.get_dto(parent)
        )

    def patch(self, event: dict) -> dict:
        customer = event.get(CUSTOMER_ATTR)
        parent_id = event.get(PARENT_ID_ATTR)
        application_id = event.get(APPLICATION_ID_ATTR)
        description: str = event.get(DESCRIPTION_ATTR)
        clouds: set = event.get(CLOUDS_ATTR)
        scope: str = event.get(SCOPE_ATTR)
        rules_to_exclude: set = event.get(RULES_TO_EXCLUDE_ATTR)
        rules_to_include: set = event.get(RULES_TO_INCLUDE_ATTR)

        parent = self.get_parent(parent_id, customer)
        if not parent:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Parent with id {parent_id} not found'
            )
        if description:
            parent.description = description
        if application_id:
            app = self._modular_service.get_application(application_id)
            _required_types = self.parent_type_to_application_type_map.get(
                parent.type)
            if not app or app.is_deleted or app.customer_id != customer or app.type not in _required_types:
                return build_response(
                    code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                    content=f'Application {application_id} with type: '
                            f'{", ".join(_required_types)} not found in '
                            f'customer {customer}'
                )
            parent.application_id = application_id
        if parent.type == CUSTODIAN_ACCESS_TYPE:
            _LOG.debug('Custodian access parent has no meta. All possible '
                       'updates are done. Saving')
            self._modular_service.save(parent)
            return build_response(
                content=self._modular_service.get_dto(parent)
            )

        # updating meta for CUSTODIAN, CUSTODIAN_LICENSES, SIEM_DEFECT_DOJO
        if parent.type == CUSTODIAN_LICENSES_TYPE:
            meta = ParentMeta.from_dict(parent.meta.as_dict())
            resolved_to_exclude = set(self._rule_service.resolved_names(
                names=rules_to_exclude,
                clouds=set(adjust_cloud(cl) for cl in clouds or meta.clouds)
            ))
            existing_rules_to_exclude = set(meta.rules_to_exclude)
            existing_rules_to_exclude -= rules_to_include
            existing_rules_to_exclude |= resolved_to_exclude
            meta.rules_to_exclude = list(existing_rules_to_exclude)

        else:  # CUSTODIAN or SIEM_DEFECT_DOJO
            meta = ScopeParentMeta.from_dict(parent.meta.as_dict())
        if clouds:
            meta.clouds = list(clouds)
        if scope:
            meta.scope = scope

        parent.meta = meta.dict()
        self._modular_service.save(parent)
        return build_response(
            content=self._modular_service.get_dto(parent)
        )
