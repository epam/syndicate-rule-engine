from http import HTTPStatus

from handlers import AbstractHandler, Mapping
from helpers.constants import CustodianEndpoint, HTTPMethod
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from helpers import NextToken
from helpers.system_customer import SYSTEM_CUSTOMER
from services import SP
from services.rule_meta_service import RuleService
from services.rule_source_service import RuleSourceService
from validators.swagger_request_models import RuleDeleteModel, RuleGetModel
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class RuleHandler(AbstractHandler):
    """
    Manage Rule API
    """

    def __init__(self, rule_service: RuleService,
                 rule_source_service: RuleSourceService):
        self.rule_service = rule_service
        self.rule_source_service = rule_source_service

    @classmethod
    def build(cls):
        return cls(
            rule_service=SP.rule_service,
            rule_source_service=SP.rule_source_service
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.RULES: {
                HTTPMethod.GET: self.get_rule,
                HTTPMethod.DELETE: self.delete_rule
            }
        }

    @validate_kwargs
    def get_rule(self, event: RuleGetModel):
        _LOG.debug(f'Get rules action')
        customer = event.customer or SYSTEM_CUSTOMER
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
        if event.rule_source_id:
            rs = self.rule_source_service.get_nullable(event.rule_source_id)
            if not rs or rs.customer != customer:
                raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                    self.rule_source_service.not_found_message()
                ).exc()
            cursor = self.rule_service.get_by_rule_source(
                rule_source=rs,
                cloud=event.cloud,
                limit=event.limit,
                last_evaluated_key=NextToken.deserialize(event.next_token).value
            )
        elif event.git_project_id:
            cursor = self.rule_service.get_by(
                customer=customer,
                project=event.git_project_id,
                ref=event.git_ref,
                cloud=event.cloud,
                limit=event.limit,
                last_evaluated_key=NextToken.deserialize(event.next_token).value
            )
        else:  # no rule_source_id, git_project_id, git_ref
            cursor = self.rule_service.get_by_id_index(
                customer=customer,
                cloud=event.cloud,
                limit=event.limit,
                last_evaluated_key=NextToken.deserialize(event.next_token).value
            )
        dto = list(self.rule_service.dto(rule) for rule in cursor)

        return ResponseFactory().items(
            it=dto,
            next_token=NextToken(cursor.last_evaluated_key)
        ).build()

    @validate_kwargs
    def delete_rule(self, event: RuleDeleteModel):
        _LOG.debug('Delete rule action')
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
