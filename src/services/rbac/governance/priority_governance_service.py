from services.modular_service import ModularService, Tenant, Customer, \
    Complemented, TenantSettings
from helpers.constants import ATTACHMENT_MODEL_ATTR, \
    LICENSE_KEYS_ATTR, ACCOUNT_ATTR, PERMITTED_ATTR, \
    ALLOWED_ATTACHMENT_ATTRS, PROHIBITED_ATTR

from services.rbac.governance.abstract_governance_service import \
    AbstractGovernanceService, MANAGEMENT_ATTR, GOVERNANCE_ATTR, \
    DELEGATION_ATTR

from helpers import generate_id
from helpers.log_helper import get_logger

from typing import List, Callable, Union, Optional, Dict, Type, Any

ACCOUNTS_ATTR = ACCOUNT_ATTR + 's'

_LOG = get_logger(__name__)

MANAGEMENT_ID_ATTR = f'{MANAGEMENT_ATTR}_id'
DELEGATION_ID_ATTR = f'{DELEGATION_ATTR}_id'


class PriorityGovernanceService(AbstractGovernanceService):
    """
    Governance Service which mediates priority management, which is expressed
    by the following data model.
    - entity: Complemented[Tenant, Customer]
    - resource `management` of priorities: Dict
     * $(management-id or mid)(s): str
      * $management-data: Any
    - party `delegation` of accounts: Dict
     * $(delegation-id or did)(s): str
      * $delegation-data: Dict of attachment-model: str, accounts: List[str]
    - interaction `governance`: Dict
     * $(governance-id or gid): str
      * $governance-data: Dict of
       * attachment-model: str
    """

    def __init__(self, modular_service: ModularService):
        self._modular_service = modular_service
        self._entity_type: Optional[Type[Union[Tenant, Customer]]] = None
        self._governance_type: Optional[str] = None

        self._managed_attr: str = 'priorities'
        self._delegated_attr: str = 'delegation'

    @staticmethod
    def _allowed_entity_types():
        return Tenant, Customer

    @property
    def managed_attr(self):
        return self._managed_attr

    @managed_attr.setter
    def managed_attr(self, other: str):
        self._managed_attr = other

    @property
    def delegated_attr(self):
        return self._delegated_attr

    @delegated_attr.setter
    def delegated_attr(self, other: str):
        self._delegated_attr = other

    @property
    def entity_type(self):
        return self._entity_type

    @entity_type.setter
    def entity_type(self, other: Type[Union[Tenant, Customer]]):
        assert other in (*self._allowed_entity_types(), None)
        self._entity_type = other

    @property
    def entity_fetcher_map(self):
        return {
            Customer: self._modular_service.get_complemented_customer,
            Tenant: self._modular_service.get_complemented_tenant
        }

    @property
    def governance_type(self):
        return self._governance_type

    @governance_type.setter
    def governance_type(self, other: str):
        self._governance_type = other

    @property
    def _attribute_dto_map(self):
        return {
            MANAGEMENT_ATTR: self.get_management_dto
        }

    def get_entity(self, entity: Union[str, Tenant, Customer]):
        reference = self.entity_fetcher_map
        entity_type = self.entity_type
        governance_type: str = self.governance_type
        fetcher: Callable[[str, str], Optional[Complemented]] = reference.get(
            entity_type
        )

        if not entity_type:
            _LOG.error('\'entity_type\' attribute has not been set.')
            return None
        elif not fetcher:
            _LOG.error(f'{entity_type} \'entity_type\' is not allowed.')
            return None
        elif not governance_type:
            _LOG.error('\'governance_type\' attribute has not been set.')
            return None
        entity = fetcher(entity, governance_type)
        return entity

    def get_management_dto(self, mid: str, data: Dict[str, List]):
        """
        Produces a single priority management data-transfer-object,
        adhering to the attached `managed` data type.
        :return: Dict[str, List[str]]
        """
        return {
            MANAGEMENT_ID_ATTR: mid, self.managed_attr: data
        }

    def get_management(
        self, entity: Complemented, mid: str
    ):
        """
        Retrieves id-driven ruleset priority-management data, within a
        Ruleset-License-Priority TenantSetting bound to tenant name,
        denoted with `entity`.
        :parameter entity: str, TenantSetting.tenant_name
        :parameter mid: str, priority identifier
        :return: Optional[Dict]
        """
        return self._get_attr_data(
            attr=MANAGEMENT_ATTR, entity=entity, subject=mid
        )

    def i_get_management(self, entity: Complemented):
        return self._i_get_attr_data(attr=MANAGEMENT_ATTR, entity=entity)

    def get_delegation(
        self, entity: Complemented, did: str
    ):
        """
        Retrieves id-driven ruleset priority-management data, within a
        Ruleset-License-Priority TenantSetting bound to tenant name,
        denoted with `entity`.
        :parameter entity: str, TenantSetting.tenant_name
        :parameter did: str, delegation identifier
        :return: Optional[Dict]
        """
        return self._get_attr_data(
            attr=DELEGATION_ATTR, entity=entity, subject=did
        )

    def i_get_delegation(self, entity: Complemented):
        return self._i_get_attr_data(attr=DELEGATION_ATTR, entity=entity)

    def get_governance(self, entity: Complemented, scope: str):
        """
        Retrieves scope-driven ruleset-license governance data, within a
        Ruleset-License-Priority TenantSetting.
        :parameter entity: str, TenantSetting.tenant_name
        :parameter scope: str, Account identifier or 'All'
        :return: Optional[Dict]
        """
        return self._get_attr_data(
            attr=GOVERNANCE_ATTR, entity=entity, subject=scope
        )

    def i_get_governance(self, entity: Complemented):
        return self._i_get_attr_data(attr=MANAGEMENT_ATTR, entity=entity)

    def _get_attr_data(self, attr: str, entity: Complemented, subject: str):
        """
        Mediates data-transfer object derivation of ruleset-license priority
        content of either:
         * attr: `management`, subject: priority-identifier `$mid`
         * attr: `governance`, subject: `$scope`
        :parameter attr: str, either `management` or `scope`
        :parameter entity: Complemented
        :parameter subject: str, either `mid` or `scope` identifier
        :return: Dict[str, List[str]]
        """

        output = None

        attr_data = getattr(entity, attr, None) or dict()

        _get_dto: Callable = self._attribute_dto_map.get(attr, None)

        if attr_data and subject in attr_data and _get_dto:
            output = _get_dto(subject, attr_data.get(subject))

        return output

    def _i_get_attr_data(self, attr: str, entity: Complemented):
        """
        Mediates data-transfer object derivation of ruleset-license priority
        content of either:
         * attr: `management`
         * attr: `governance`
        :parameter attr: str, either `management` or `scope`
        :parameter entity: Union[str, Tenant], Tenant of TenantSettings
        :return: Dict[str, List[str]]
        """

        attr_data = getattr(entity, attr, None) or dict()

        _get_dto: Callable = self._attribute_dto_map.get(attr, None)

        if _get_dto:
            for subject, data in attr_data.items():
                yield _get_dto(mid=subject, data=data)

    def create_management(
        self, entity: Complemented, data: Dict[str, List[str]]
    ) -> Optional[str]:
        management = entity.management or dict()
        mid = generate_id()
        management[mid] = data
        entity.management = management
        return mid

    def delete_management(self, entity: Complemented, mid: str):
        head = f'{self.governance_type}:\'{entity.name}\''

        mgmt = entity.management
        mgmt = (mgmt if isinstance(mgmt, dict) else None) or dict()
        if mid not in mgmt:
            _LOG.warning(head + f'\'{mid}\' {MANAGEMENT_ATTR} does not exist.')
            return None
        else:
            mgmt.pop(mid)
            entity.management = mgmt
            _LOG.info(head + f'\'{mid}\' {MANAGEMENT_ATTR} has been removed.')

        gvc: Dict[str, Dict] = entity.governance
        gvc = (gvc if isinstance(gvc, dict) else None) or dict()

        # Currently obsolete.
        gvc_items = list(gvc.items())
        updated_gvc = {}
        for did, gvc_data in gvc_items:
            attachment_data = gvc_data

            if self.is_subject_applicable(
                subject=mid, attachment=gvc_data, attached_attr=MANAGEMENT_ATTR
            ):
                _to_log = f' going to exclude \'{mid}\' {MANAGEMENT_ATTR} ' \
                          f'out of \'{did}\' {GOVERNANCE_ATTR}.'

                _LOG.info(head + _to_log)
                # Update attachment.
                _head = head + f' - \'{mid}\' {GOVERNANCE_ATTR}:'
                data = self.derive_attachment_update_entity_data(
                    entity=gvc_data, head_attr=_head,
                    attached_attr=MANAGEMENT_ATTR,
                    attachment_model=PROHIBITED_ATTR,
                    to_attach=[mid]
                )
                if not data:
                    continue

                attached = data.get(MANAGEMENT_ATTR)
                if not attached:
                    # Detached of all management-id(s)
                    _LOG.info(head + f' removing \'{did}\' {GOVERNANCE_ATTR}'
                                     f' due to absence of {MANAGEMENT_ATTR}.')
                    continue
                else:
                    attachment_data = data

            updated_gvc[did] = attachment_data

        if updated_gvc:
            entity.governance = updated_gvc
        return entity

    @staticmethod
    def is_subject_applicable(
        subject: str, attached_attr: str, attachment: dict
    ):
        atm = attachment.get(ATTACHMENT_MODEL_ATTR)
        if atm not in ALLOWED_ATTACHMENT_ATTRS:
            return False

        attached = attachment.get(attached_attr, None)
        if not isinstance(attached, list):
            return False

        permit = atm == PERMITTED_ATTR
        retained = subject in attached
        _all = not attached

        return (
            (permit and (retained or _all)) or
            (not permit and not(retained or _all))
        )

    @staticmethod
    def derive_attachment_update_entity_data(
        entity: dict, head_attr: str, attached_attr: str,
        attachment_model: str, to_attach: List[str]
    ):
        """
        Designated retrieve a patched attachment model of a given entity data,
        based on a requested model and payload to attach, commencing diff or
        merge actions.
        :parameter entity: Dict, attachment data of an entity
        :parameter head_attr: str, entity log-header
        :parameter attached_attr: str, attribute referencing attached data
        :parameter attachment_model: str, model of payload to attach
        :parameter to_attach: List[str], payload to attach
        :return: Optional[Dict]
        """

        head = head_attr
        atm = attachment_model

        # Entity data.
        e_atm: str = entity.get(ATTACHMENT_MODEL_ATTR)
        e_attached: set = set(entity.get(attached_attr, []))

        if e_atm not in ALLOWED_ATTACHMENT_ATTRS:
            _LOG.error(head + f' maintains improper {ATTACHMENT_MODEL_ATTR}.')
            return None

        if attachment_model not in ALLOWED_ATTACHMENT_ATTRS:
            _LOG.warning(head + f' issued {ATTACHMENT_MODEL_ATTR} - invalid.')
            return None

        # Explicitly empty tenants - apply to all.
        to_attach = set(to_attach)

        _apply_model = f' explicitly applying \'{atm}\' model to'
        _update_model = f' updating \'{e_atm}\' to'
        _switch_model = f' switching to \'{atm}\' model and'

        attr = f'{attached_attr}(s)'

        if not to_attach:
            scope = f' all {attached_attr}(s).'
            _LOG.info(head + _apply_model + scope)
            e_attached = to_attach
            e_atm = atm

        elif e_atm == atm:
            scope = f' merge {to_attach} with {e_attached} {attr}.'
            _LOG.info(head + _update_model + scope)
            e_attached |= to_attach

        else:
            scope = f' diff {to_attach} out of {e_attached} {attr}.'
            _LOG.info(head + _update_model + scope)
            remainder = to_attach - e_attached
            e_attached -= to_attach
            if not e_attached and remainder:
                scope = f' attaching {remainder} remaining {attr}.'
                _LOG.info(head + _switch_model + scope)
                e_attached = remainder
                e_atm = atm

        entity[attached_attr] = list(e_attached)
        entity[ATTACHMENT_MODEL_ATTR] = e_atm
        state = f' attachment-model:\'{e_atm}\' is bound to ' \
                f'{", ".join(e_attached)} {attr}.'
        _LOG.info(head + state)

        return entity

