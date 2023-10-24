from functools import cached_property
from http import HTTPStatus
from itertools import chain
from typing import Optional, Dict, Set

from modular_sdk.commons.constants import ApplicationType, ParentType, \
    ParentScope
from modular_sdk.models.application import Application
from modular_sdk.models.tenant import Tenant
from modular_sdk.services.parent_service import ParentService

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import adjust_cloud
from helpers import build_response
from helpers.constants import CUSTOMER_ATTR, PARENT_ID_ATTR, \
    SPECIFIC_TENANT_SCOPE, \
    CLOUD_TO_APP_TYPE, HTTPMethod
from helpers.enums import ParentType
from helpers.log_helper import get_logger
from models.modular.parents import Parent, ParentMeta
from services import SERVICE_PROVIDER
from services.modular_service import ModularService
from services.rule_meta_service import RuleService
from validators.request_validation import ParentPostModel, ParentPatchModel
from validators.utils import validate_kwargs

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

    @property
    def ps(self) -> ParentService:
        return self._modular_service.modular_client.parent_service()

    def define_action_mapping(self) -> dict:
        return {
            '/parents': {
                HTTPMethod.POST: self.post,
                HTTPMethod.GET: self.list
            },
            '/parents/{parent_id}': {
                HTTPMethod.GET: self.get,
                HTTPMethod.DELETE: self.delete,
                HTTPMethod.PATCH: self.patch
            },
        }

    def _is_allowed_to_link_custodian(self, tenant: Tenant, parent: Parent,
                                      application: Application) -> bool:
        if parent.type != ParentType.CUSTODIAN:
            self._content = f'parent type must be {ParentType.CUSTODIAN} ' \
                            f'for {ParentType.CUSTODIAN} linkage'
            return False
        if application.type != ApplicationType.CUSTODIAN:
            self._content = f'application type must be ' \
                            f'{ApplicationType.CUSTODIAN} ' \
                            f'for {ParentType.CUSTODIAN} linkage'
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
        if parent.type != ParentType.CUSTODIAN_LICENSES:
            self._content = f'parent type must be ' \
                            f'{ParentType.CUSTODIAN_LICENSES} ' \
                            f'for {ParentType.CUSTODIAN_LICENSES} linkage'
            return False
        if application.type != ApplicationType.CUSTODIAN_LICENSES:
            self._content = f'application type must be ' \
                            f'{ApplicationType.CUSTODIAN_LICENSES} ' \
                            f'for {ApplicationType.CUSTODIAN_LICENSES} linkage'
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
        if parent.type != ParentType.SIEM_DEFECT_DOJO:
            self._content = f'parent type must be ' \
                            f'{ParentType.SIEM_DEFECT_DOJO} ' \
                            f'for {ParentType.SIEM_DEFECT_DOJO} linkage'
            return False
        if application.type != ApplicationType.DEFECT_DOJO:
            self._content = f'application type must be ' \
                            f'{ApplicationType.DEFECT_DOJO} ' \
                            f'for {ParentType.SIEM_DEFECT_DOJO} linkage'
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
        if parent.type != ParentType.CUSTODIAN_ACCESS:
            self._content = f'parent type must be ' \
                            f'{ParentType.CUSTODIAN_ACCESS} ' \
                            f'for {ParentType.CUSTODIAN_ACCESS} linkage'
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
        CUSTODIAN_ACCESS   -> CUSTODIAN_ACCESS   -> [creds application for tenant's cloud]  # noqa
        For the first three rows scope must be SPECIFIC_TENANT, cloud be valid
        :param tenant:
        :param parent:
        :param type_:
        :return:
        """
        application = self._modular_service.get_parent_application(parent)
        if not application:
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content='Parent`s application not found'
            )
        type_method = {
            ParentType.CUSTODIAN.value: self._is_allowed_to_link_custodian,
            ParentType.CUSTODIAN_LICENSES.value: self._is_allowed_to_link_custodian_licenses,
            # noqa
            ParentType.SIEM_DEFECT_DOJO.value: self._is_allowed_to_link_siem_defect_dojo,
            # noqa
            ParentType.CUSTODIAN_ACCESS.value: self._is_allowed_to_link_custodian_access
            # noqa
        }
        method = type_method[type_]  # type_ is validated before
        if not method(tenant, parent, application):
            _LOG.warning(f'Cannot link: {self._content}')
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content=f'Cannot link: {self._content}'
            )

    def list(self, event: dict) -> dict:
        customer = event.get(CUSTOMER_ATTR)
        items = self._modular_service.get_customer_bound_parents(
            customer=customer,
            parent_type=ParentType.list(),
            is_deleted=False
        )
        return build_response(
            content=(self.ps.get_dto(parent) for parent in items)
        )

    def get(self, event: dict):
        customer = event.get(CUSTOMER_ATTR)
        parent_id = event.get(PARENT_ID_ATTR)
        item = self.get_parent(parent_id, customer)
        if not item:
            return build_response(content=[])
        return build_response(content=self.ps.get_dto(item))

    def delete(self, event: dict):
        customer = event.get(CUSTOMER_ATTR)
        parent_id = event.get(PARENT_ID_ATTR)
        item = self.get_parent(parent_id, customer)
        if not item:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Parent with id '
                        f'{parent_id} within your customer not found'
            )
        self._modular_service.modular_client.parent_service().mark_deleted(
            item)
        return build_response(code=HTTPStatus.NO_CONTENT)

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
            ParentType.SIEM_DEFECT_DOJO.value: {
                ApplicationType.DEFECT_DOJO.value},
            ParentType.CUSTODIAN_LICENSES.value: {
                ApplicationType.CUSTODIAN_LICENSES.value},
            ParentType.CUSTODIAN.value: {ApplicationType.CUSTODIAN.value},
            ParentType.CUSTODIAN_ACCESS.value: set(
                chain.from_iterable(CLOUD_TO_APP_TYPE.values())),
            # any creds application
        }

    @validate_kwargs
    def post(self, event: ParentPostModel) -> dict:
        app = self._modular_service.get_application(event.application_id)
        _required_types = self.parent_type_to_application_type_map.get(
            event.type)
        if not app or app.is_deleted or app.customer_id != event.customer or \
                app.type not in _required_types:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Application {event.application_id} with type: '
                        f'{", ".join(_required_types)} not found in '
                        f'customer {event.customer}'
            )
        tenant = None
        if event.tenant_name:
            tenant = self._modular_service.get_tenant(event.tenant_name)
            self._modular_service.assert_tenant_valid(tenant, event.customer)

        meta = {}
        if event.type == ParentType.CUSTODIAN_LICENSES:
            _LOG.debug(f'Parent is {event.type}. Resolving rule ids')
            clouds = set()
            if tenant:
                clouds.add(adjust_cloud(tenant.cloud))
            elif event.cloud:  # scope all
                clouds.add(adjust_cloud(event.cloud))
            meta = ParentMeta(
                rules_to_exclude=list(
                    self._rule_service.resolved_names(
                        names=event.rules_to_exclude,
                        clouds=clouds
                    )
                )
            ).dict()
        if event.scope == ParentScope.ALL:
            parent = self.ps.create_all_scope(
                application_id=event.application_id,
                customer_id=event.customer,
                type_=event.type,
                description=event.description,
                meta=meta,
                cloud=event.cloud
            )
        else:
            parent = self.ps.create_tenant_scope(
                application_id=event.application_id,
                customer_id=event.customer,
                type_=event.type,
                tenant_name=event.tenant_name,
                disabled=event.scope == ParentScope.DISABLED,
                description=event.description,
                meta=meta
            )
        self._modular_service.save(parent)

        return build_response(
            content=self.ps.get_dto(parent)
        )

    @validate_kwargs
    def patch(self, event: ParentPatchModel) -> dict:

        parent = self.get_parent(event.parent_id, event.customer)
        if not parent:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Parent with id {event.parent_id} not found'
            )
        actions = []
        if event.description:
            actions.append(Parent.description.set(event.description))
        if event.application_id:
            app = self._modular_service.get_application(event.application_id)
            _required_types = self.parent_type_to_application_type_map.get(
                parent.type)
            if not app or app.is_deleted or app.customer_id != event.customer or app.type not in _required_types:  # noqa
                return build_response(
                    code=HTTPStatus.NOT_FOUND,
                    content=f'Application {event.application_id} with type: '
                            f'{", ".join(_required_types)} not found in '
                            f'customer {event.customer}'
                )
            actions.append(Parent.application_id.set(event.application_id))
        if parent.type == ParentType.CUSTODIAN_ACCESS.value:
            _LOG.debug('Custodian access parent has no meta. All possible '
                       'updates are done. Saving')
            parent.update(actions)
            return build_response(
                content=self.ps.get_dto(parent)
            )

        # updating meta for CUSTODIAN, CUSTODIAN_LICENSES, SIEM_DEFECT_DOJO
        if parent.type == ParentType.CUSTODIAN_LICENSES.value:
            meta = ParentMeta.from_dict(parent.meta.as_dict())
            resolved_to_exclude = set(self._rule_service.resolved_names(
                names=event.rules_to_exclude,
            ))
            existing_rules_to_exclude = set(meta.rules_to_exclude)
            existing_rules_to_exclude -= event.rules_to_include
            existing_rules_to_exclude |= resolved_to_exclude
            meta.rules_to_exclude = list(existing_rules_to_exclude)
            actions.append(Parent.meta.set(meta.dict()))
        parent.update(actions)
        return build_response(
            content=self._modular_service.get_dto(parent)
        )
