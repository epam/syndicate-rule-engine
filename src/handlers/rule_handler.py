from http import HTTPStatus

from modular_sdk.models.pynamodb_extension.base_model import \
    LastEvaluatedKey as Lek

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response
from helpers.constants import HTTPMethod, NEXT_TOKEN_ATTR
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from services.modular_service import ModularService
from services.rule_meta_service import RuleService
from services.rule_source_service import RuleSourceService
from validators.request_validation import RuleGetModel, RuleDeleteModel
from validators.utils import validate_kwargs

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
                HTTPMethod.GET: self.get_rule,
                HTTPMethod.DELETE: self.delete_rule
            }
        }

    @validate_kwargs
    def get_rule(self, event: RuleGetModel) -> dict:
        _LOG.debug(f'Get rules action')
        customer = event.customer or SYSTEM_CUSTOMER
        lek = Lek.deserialize(event.next_token or None)
        if event.rule:
            # TODO split to get and list endpoints
            _LOG.debug('Rule id was given. Trying to resolve one rule')
            rule = self.rule_service.resolve_rule(
                customer=customer,
                name_prefix=event.rule,
                cloud=event.cloud
            )
            if rule:
                return build_response(
                    content=self.rule_service.dto(rule)
                )
            return build_response(content=[])
        _LOG.debug('Going to list rules')
        if event.git_project_id:
            cursor = self.rule_service.get_by(
                customer=customer,
                project=event.git_project_id,
                ref=event.git_ref,
                cloud=event.cloud,
                limit=event.limit,
                last_evaluated_key=lek.value
            )
        else:  # no git_project_id & git_ref
            cursor = self.rule_service.get_by_id_index(
                customer=customer,
                cloud=event.cloud,
                limit=event.limit,
                last_evaluated_key=lek.value
            )
        dto = list(self.rule_service.dto(rule) for rule in cursor)
        lek.value = cursor.last_evaluated_key
        return build_response(
            content=dto,
            meta={NEXT_TOKEN_ATTR: lek.serialize()} if lek else None
        )

    @validate_kwargs
    def delete_rule(self, event: RuleDeleteModel):
        _LOG.debug(f'Delete rule action')
        customer = event.customer or SYSTEM_CUSTOMER

        if event.rule:
            _LOG.debug('Rule name given. Removing one rule')
            item = self.rule_service.get_latest_rule(
                customer=customer,
                name=event.rule
            )
            if not item:
                return build_response(
                    code=HTTPStatus.NOT_FOUND,
                    content=self.rule_service.not_found_message(event.rule)
                )
            if item:
                self.rule_service.delete(item)
            return build_response(code=HTTPStatus.NO_CONTENT)

        _LOG.debug('Going to list rules')
        if event.git_project_id:
            cursor = self.rule_service.get_by(
                customer=customer,
                project=event.git_project_id,
                ref=event.git_ref,
                cloud=event.cloud,
            )
        else:  # no git_project_id & git_ref
            cursor = self.rule_service.get_by_id_index(
                customer=customer,
                cloud=event.cloud,
            )
        self.rule_service.batch_delete(cursor)
        return build_response(code=HTTPStatus.NO_CONTENT)
