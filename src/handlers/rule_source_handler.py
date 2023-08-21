from typing import Optional, Any, Iterable, List

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response, RESPONSE_RESOURCE_NOT_FOUND_CODE, \
    RESPONSE_OK_CODE, RESPONSE_UNAUTHORIZED, \
    RESPONSE_CONFLICT, RESPONSE_NO_CONTENT
from helpers.constants import CUSTOMER_ATTR, GET_METHOD, \
    POST_METHOD, PATCH_METHOD, DELETE_METHOD, \
    RULE_SOURCE_REQUIRED_ATTRS, GIT_PROJECT_ID_ATTR, \
    ID_ATTR, ALLOWED_FOR_ATTR, TENANTS_ATTR, TENANT_RESTRICTION, \
    TENANT_ALLOWANCE, DESCRIPTION_ATTR, GIT_URL_ATTR, GIT_REF_ATTR, \
    GIT_RULES_PREFIX_ATTR
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from models.rule_source import RuleSource
from services.modular_service import ModularService
from services.rbac.restriction_service import RestrictionService
from services.rule_meta_service import RuleService
from services.rule_source_service import RuleSourceService

_LOG = get_logger(__name__)
RULE_SOURCES_NOT_FOUND_MESSAGE = 'No rule sources found matching given query'
CONCRETE_RULE_SOURCE_NOT_FOUND_MESSAGE = \
    'Customer \'{customer}\' does not have a rule-source ' \
    'with project id \'{rule_source_id}\''
RULE_SOURCE_FOUND_MESSAGE = \
    'Customer \'{customer}\' already has a rule-source ' \
    'with project id \'{rule_source_id}\''


class RuleSourceHandler(AbstractHandler):
    def __init__(self, rule_source_service: RuleSourceService,
                 modular_service: ModularService,
                 rule_service: RuleService,
                 restriction_service: RestrictionService):
        self.rule_source_service = rule_source_service
        self.modular_service = modular_service
        self.rule_service = rule_service
        self.restriction_service = restriction_service

    def define_action_mapping(self):
        return {
            '/rule-sources': {
                GET_METHOD: self.get_rule_source,
                POST_METHOD: self.create_rule_source,
                PATCH_METHOD: self.update_rule_source,
                DELETE_METHOD: self.delete_rule_source
            }
        }

    def get_rule_source(self, event):
        _LOG.debug(f'Describe rule source event: {event}')
        customer = event.get(CUSTOMER_ATTR)
        tenants = event.get(TENANTS_ATTR)

        rule_source_id = event.get(ID_ATTR)
        service = self.rule_source_service
        if rule_source_id:
            entity = self._attain_rule_source(
                rule_source_id=rule_source_id,
                customer=customer, tenants=tenants
            )
            if not entity:
                return build_response(
                    content=f'Rule-source:{rule_source_id!r} does not exist.',
                    code=RESPONSE_RESOURCE_NOT_FOUND_CODE
                )
            rule_sources = [entity]
        else:
            git_project_id = event.get(GIT_PROJECT_ID_ATTR)
            params = dict(customer=customer, git_project_id=git_project_id)
            _list_rule_sources = self.rule_source_service.list_rule_sources
            rule_sources = self.rule_source_service.filter_by_tenants(
                _list_rule_sources(**params)
            )
        # DO NOT remove the commented code. It's a disabled feature
        # Note: first be must filter by tenants and second - expand
        # with SYSTEM's (in case inherit=True of course) and NOT vice versa.
        # That is why, we cannot use `derive_from_system` decorator
        # if self.modular_service.customer_inherit(customer):
        #     rule_sources = self.rule_source_service.expand_systems(
        #         _list_rule_sources(**self.rule_source_service.system_payload(
        #             params)), rule_sources
        #     )

        _LOG.debug(f'Rule-sources to return: {rule_sources}')
        return build_response(
            code=RESPONSE_OK_CODE,
            content=[
                service.get_rule_source_dto(rule_source=rule_source)
                for rule_source in rule_sources
            ]
        )

    def create_rule_source(self, event):
        """
        Handles POST request, by either:
         * creating a new rule-source
         * or updating tenants & git-access-credentials
          of the rule-source
        :param event: dict
        :return: dict
        """
        _LOG.debug(f'Create rule source event: {event}')
        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        git_url = event.get(GIT_URL_ATTR)
        git_project_id = event.get(GIT_PROJECT_ID_ATTR)
        git_ref = event.get(GIT_REF_ATTR)
        git_rules_prefix = event.get(GIT_RULES_PREFIX_ATTR)
        description = event.get(DESCRIPTION_ATTR)

        repo_conf = {
            attr: event.get(attr) for attr in
            RULE_SOURCE_REQUIRED_ATTRS
        }
        repo_conf[CUSTOMER_ATTR] = customer

        # Given no explicit tenants to allow access for - refer to user's
        allowed_tenants = (event.get(TENANT_ALLOWANCE) or event.get(TENANTS_ATTR))

        service = self.rule_source_service

        errors = self._check_tenants(allowed_tenants)
        if errors:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content='\n'.join(errors)
            )
        rsid = service.derive_rule_source_id(
            customer=customer,
            git_url=git_url,
            git_project_id=git_project_id,
            git_ref=git_ref,
            git_rules_prefix=git_rules_prefix
        )

        title = f'Rule-source:{rsid!r}'
        # Obtaining just an entity.
        entity = self._attain_rule_source(
            rule_source_id=rsid,
            customer=None,
            tenants=None
        )

        if entity:
            _LOG.info(f'{title} - exists, checking for accessibility.')
            is_subject_applicable = service.is_subject_applicable
            is_applicable = is_subject_applicable(
                rule_source=entity, customer=customer,
                tenants=allowed_tenants
            )

            if not is_applicable:
                return self._respond_with_unauthorized(entity=entity)

            _LOG.info(f'{title} - checking for new tenants to update for.')
            derive_updated_tenants = service.derive_updated_tenants
            updated = derive_updated_tenants(
                rule_source=entity,
                tenants=allowed_tenants,
                restrict=False
            )
            if not updated:
                return self._respond_with_already_exists(entity=entity)

            repo_conf[ALLOWED_FOR_ATTR] = allowed_tenants
            _LOG.info(f'{title} updating, based on - {repo_conf}.')
            entity = service.update_rule_source(
                rule_source=entity,
                rule_source_data=repo_conf
            )

        else:
            repo_conf[ALLOWED_FOR_ATTR] = allowed_tenants
            _LOG.info(f'{title} does not exist, creating one, based on - {repo_conf}.')
            entity = service.create_rule_source(rule_source_data=repo_conf)

        entity.description = description
        service.save_rule_source(entity)

        return build_response(
            code=RESPONSE_OK_CODE,
            content=service.get_rule_source_dto(rule_source=entity)
        )

    def update_rule_source(self, event):
        _LOG.debug(f'Update rule source event: {event}')
        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        tenants = event.get(TENANTS_ATTR) or []  # Authorization-claim
        description = event.get(DESCRIPTION_ATTR)
        rsid = event.get(ID_ATTR)  # noqa rule_source_id
        title = f'Rule-Source:{rsid!r}'

        service = self.rule_source_service

        entity = self._attain_rule_source(
            rule_source_id=rsid,
            customer=customer,
            tenants=tenants  # Checks for user-tenant access.
        )

        if not entity:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'{title} does not exist.'
            )

        repo_conf = {
            attr: event.get(attr) for attr in
            RULE_SOURCE_REQUIRED_ATTRS
        }

        to_allow = event.get(TENANT_ALLOWANCE)
        to_restrict = event.get(TENANT_RESTRICTION)
        errors = self._check_tenants(
            tenants=(to_allow or []) + (to_restrict or [])
        )
        if errors:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content='\n'.join(errors)
            )

        action_to_update_with_map = zip((False, True), (to_allow, to_restrict))
        allowed_for = entity.allowed_for or []
        for action, update_with in action_to_update_with_map:
            if update_with is not None:
                updated = service.derive_updated_tenants(
                    rule_source=entity,
                    tenants=update_with,
                    restrict=action
                )  # Updates iff attached-tenants have changed.
                allowed_for = updated or allowed_for

        # If allowed_for := [] i.e., allows ALL - check user authorization.
        if not allowed_for and tenants:
            return self._respond_with_unauthorized(entity=entity)

        repo_conf[ALLOWED_FOR_ATTR] = allowed_for

        _LOG.debug(f'{title} - being updated with {repo_conf}.')
        entity = service.update_rule_source(
            rule_source=entity,
            rule_source_data=repo_conf
        )
        _LOG.debug(f'{title} - saving data.')
        if description:
            entity.description = description
        service.save_rule_source(rule_source=entity)
        return build_response(
            code=RESPONSE_OK_CODE,
            content=service.get_rule_source_dto(rule_source=entity)
        )

    def delete_rule_source(self, event):
        _LOG.debug(f'Delete rule source event: {event}')

        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        tenants = event.get(TENANTS_ATTR) or []  # Authorization-claim
        rsid = event.get(ID_ATTR)  # noqa rule_source_id
        title = f'Rule-Source:{rsid!r}'
        scope = ', '.join(map("'{}'".format, tenants)) + ' tenants'

        service = self.rule_source_service

        entity = self._attain_rule_source(
            rule_source_id=rsid,
            customer=customer,
            tenants=tenants  # Checks for user-tenant access.
        )

        if not entity:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'{title} does not exist.'
            )

        _LOG.info(f'{title} - restricting access for {scope}.')
        allowed_for = service.derive_updated_tenants(
            rule_source=entity,
            tenants=tenants,
            restrict=True
        )
        # Removes iff auth-claim affects to all/attached tenants.
        to_remove = not tenants
        to_remove |= not allowed_for and allowed_for is not None

        if to_remove:
            _LOG.info(f'{title} - removing all related rules.')
            rules = self.rule_service.get_by_location_index(
                customer=customer,
                project=entity.git_project_id
            )
            self.rule_service.batch_delete(rules)
            _LOG.info(f'{title} - being removed.')
            service.delete_rule_source(rule_source=entity)
        elif allowed_for:
            _LOG.info(f'{title} removing access of {scope}.')
            entity.allowed_for = allowed_for
            _LOG.info(f'{title} - being persisted.')
            service.save_rule_source(rule_source=entity)
        else:
            # Error within ._attain_rule_source, failed to assert either:
            # * user-tenants == []
            # * set(user-tenants) intersects set(rule_source.allowed_for)
            raise RuntimeError(f'{title} tenant-access has not changed.')

        return build_response(code=RESPONSE_NO_CONTENT)

    def _attain_rule_source(
        self, rule_source_id: str,
        customer: Optional[str] = None,
        tenants: Optional[List[str]] = None
    ):
        """
        Returns a rule-source entity, on a given identifier,
        provided no customer, tenant conflict is present.
        Note: rule-source is attainable, given any tenant
        is allowed access.

        :param rule_source_id: str
        :param customer: str
        :param tenants: List[str]
        :return: Optional[RuleSource]
        """
        log_head = f'Rule-source:{rule_source_id!r}'
        service = self.rule_source_service
        _LOG.info(f'{log_head} - obtaining entity.')
        is_subject_applicable = service.is_subject_applicable
        entity = service.get(rule_source_id=rule_source_id)
        if not entity:
            _LOG.warning(f'{log_head} - does not exist.')
        elif not is_subject_applicable(rule_source=entity, customer=customer, tenants=tenants):
            entity = None
        return entity

    def _check_tenants(self, tenants: Iterable[str]) -> List[str]:
        errors = []
        _LOG.info(f'Verifying whether access to {", ".join(tenants)} tenants.')
        for tenant_name in tenants:
            if not self.restriction_service.is_allowed_tenant(tenant_name):
                errors.append(f'Tenant {tenant_name} not found')
        return errors

    @classmethod
    def _respond_with_already_exists(cls, entity: RuleSource):
        title = cls._get_response_title(entity=entity)
        message = f'{title} - already exists'
        return build_response(
            code=RESPONSE_CONFLICT,
            content=message
        )

    @classmethod
    def _respond_with_unauthorized(cls, entity: RuleSource):
        title = cls._get_response_title(entity=entity)
        message = f'{title} access could not be authorized.'
        return build_response(
            code=RESPONSE_UNAUTHORIZED,
            content=message
        )

    @staticmethod
    def _get_response_title(entity: RuleSource):
        return f'Rule-source:{entity.id!r}'
