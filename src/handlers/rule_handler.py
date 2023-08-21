from modular_sdk.models.pynamodb_extension.base_model import \
    LastEvaluatedKey as Lek

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response, RESPONSE_NO_CONTENT, \
    RESPONSE_RESOURCE_NOT_FOUND_CODE
from helpers.constants import GET_METHOD, LIMIT_ATTR, DELETE_METHOD, \
    CLOUD_ATTR, NEXT_TOKEN_ATTR, RULE_ATTR, CUSTOMER_ATTR, GIT_REF_ATTR, \
    GIT_PROJECT_ID_ATTR
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from services.modular_service import ModularService
from services.rule_meta_service import RuleService
from services.rule_source_service import RuleSourceService

_LOG = get_logger(__name__)


class RuleHandler(AbstractHandler):
    """
    Manage Rule API
    """

    def __init__(self, modular_service: ModularService,
                 rule_service: RuleService,
                 rule_source_service: RuleSourceService):
        self.rule_service = rule_service
        self.rule_source_service = rule_source_service
        self.modular_service = modular_service

    def define_action_mapping(self):
        return {
            '/rules': {
                GET_METHOD: self.get_rule,
                DELETE_METHOD: self.delete_rule
            }
        }

    def get_rule(self, event: dict) -> dict:
        _LOG.debug(f'Get rules action')
        rule_name = event.get(RULE_ATTR)
        git_project_id = event.get(GIT_PROJECT_ID_ATTR)
        git_ref = event.get(GIT_REF_ATTR)
        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        cloud = event.get(CLOUD_ATTR)
        limit = event.get(LIMIT_ATTR)
        lek = Lek.deserialize(event.get(NEXT_TOKEN_ATTR) or None)
        if rule_name:
            # TODO split to get and list endpoints
            _LOG.debug('Rule id was given. Trying to resolve one rule')
            rule = self.rule_service.resolve_rule(
                customer=customer,
                name_prefix=rule_name,
                cloud=cloud
            )
            if rule:
                return build_response(
                    content=self.rule_service.dto(rule)
                )
            return build_response(content=[])
        _LOG.debug('Going to list rules')
        if git_project_id:
            cursor = self.rule_service.get_by(
                customer=customer,
                project=git_project_id,
                ref=git_ref,
                cloud=cloud,
                limit=limit,
                last_evaluated_key=lek.value
            )
        else:  # no git_project_id & git_ref
            cursor = self.rule_service.get_by_id_index(
                customer=customer,
                cloud=cloud,
                limit=limit,
                last_evaluated_key=lek.value
            )
        dto = list(self.rule_service.dto(rule) for rule in cursor)
        lek.value = cursor.last_evaluated_key
        return build_response(
            content=dto,
            meta={NEXT_TOKEN_ATTR: lek.serialize()} if lek else None
        )

    def delete_rule(self, event):
        _LOG.debug(f'Delete rule action')
        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        rule_name = event.get(RULE_ATTR)
        cloud = event.get(CLOUD_ATTR)
        git_project_id = event.get(GIT_PROJECT_ID_ATTR)
        git_ref = event.get(GIT_REF_ATTR)

        if rule_name:
            _LOG.debug('Rule name given. Removing one rule')
            item = self.rule_service.get_latest_rule(
                customer=customer,
                name=rule_name
            )
            if not item:
                return build_response(
                    code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                    content=self.rule_service.not_found_message(rule_name)
                )
            if item:
                self.rule_service.delete(item)
            return build_response(code=RESPONSE_NO_CONTENT)

        _LOG.debug('Going to list rules')
        if git_project_id:
            cursor = self.rule_service.get_by(
                customer=customer,
                project=git_project_id,
                ref=git_ref,
                cloud=cloud,
            )
        else:  # no git_project_id & git_ref
            cursor = self.rule_service.get_by_id_index(
                customer=customer,
                cloud=cloud,
            )
        self.rule_service.batch_delete(cursor)
        return build_response(code=RESPONSE_NO_CONTENT)
