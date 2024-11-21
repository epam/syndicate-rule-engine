from http import HTTPStatus

from handlers import AbstractHandler, Mapping
from helpers import NextToken
from helpers.constants import (
    CustodianEndpoint,
    HTTPMethod,
    RULE_META_UPDATER_LAMBDA_NAME,
)
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from models.rule_source import RuleSource
from services import SP
from services.clients.lambda_func import LambdaClient
from services.rule_meta_service import RuleService
from services.rule_source_service import RuleSourceService
from validators.swagger_request_models import (
    BaseModel,
    RuleSourceDeleteModel,
    RuleSourcePatchModel,
    RuleSourcePostModel,
    RuleSourcesListModel,
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class RuleSourceHandler(AbstractHandler):
    def __init__(self, rule_source_service: RuleSourceService,
                 rule_service: RuleService,
                 lambda_client: LambdaClient):
        self._rule_source_service = rule_source_service
        self._rule_service = rule_service
        self._lambda_client = lambda_client

    @classmethod
    def build(cls):
        return cls(
            rule_source_service=SP.rule_source_service,
            rule_service=SP.rule_service,
            lambda_client=SP.lambda_client
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.RULE_SOURCES: {
                HTTPMethod.GET: self.list_rule_sources,
                HTTPMethod.POST: self.create_rule_source,
            },
            CustodianEndpoint.RULE_SOURCES_ID: {
                HTTPMethod.GET: self.get_rule_source,
                HTTPMethod.DELETE: self.delete_rule_source,
                HTTPMethod.PATCH: self.update_rule_source,
            },
            CustodianEndpoint.RULE_SOURCES_ID_SYNC: {
                HTTPMethod.POST: self.sync_rule_source
            }
        }

    def _ensure_rule_source(self, id: str, customer: str | None) -> RuleSource:
        """
        Raises 404 if rule_source not found
        :param id:
        :param customer:
        :return:
        """
        item = self._rule_source_service.get_nullable(id)
        if not item or customer and item.customer != customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._rule_source_service.not_found_message()
            ).exc()
        return item

    @validate_kwargs
    def get_rule_source(self, event: BaseModel, id: str):
        # Todo maybe support project id
        item = self._ensure_rule_source(id, event.customer or SYSTEM_CUSTOMER)
        return build_response(self._rule_source_service.dto(item))

    @validate_kwargs
    def list_rule_sources(self, event: RuleSourcesListModel):
        cursor = self._rule_source_service.query(
            customer=event.customer or SYSTEM_CUSTOMER,
            has_secret=event.has_secret,
            limit=event.limit,
            last_evaluated_key=NextToken.deserialize(event.next_token).value
        )
        items = tuple(cursor)
        return ResponseFactory().items(
            it=map(self._rule_source_service.dto, items),
            next_token=NextToken(cursor.last_evaluated_key)
        ).build()

    @validate_kwargs
    def delete_rule_source(self, event: RuleSourceDeleteModel, id: str):
        customer = event.customer or SYSTEM_CUSTOMER
        item = self._ensure_rule_source(id, customer)
        if not item:
            return build_response(code=HTTPStatus.NO_CONTENT)
        _LOG.debug(f'Removing rule source with id {id}')
        self._rule_source_service.delete(item)

        if event.delete_rules:
            _LOG.debug('Removing rule source rules')
            rules = self._rule_service.get_by_rule_source_id(
                rule_source_id=item.id,
                customer=event.customer_id,
            )
            self._rule_service.batch_delete(rules)
        return build_response(code=HTTPStatus.NO_CONTENT)

    @validate_kwargs
    def create_rule_source(self, event: RuleSourcePostModel):
        customer = event.customer or SYSTEM_CUSTOMER

        identifier = self._rule_source_service.generate_id(
            customer=customer,
            git_project_id=event.git_project_id,
            type_=event.type,
            git_url=event.baseurl,
            git_ref=event.git_ref,
            git_rules_prefix=event.git_rules_prefix
        )
        item = self._rule_source_service.get_nullable(identifier)
        if item:
            raise ResponseFactory(HTTPStatus.CONFLICT).message(
                'Rule source with such parameters already exists'
            ).exc()

        entity = self._rule_source_service.create(
            git_project_id=event.git_project_id,
            type_=event.type,
            git_url=event.baseurl,
            git_ref=event.git_ref,
            git_rules_prefix=event.git_rules_prefix,
            customer=customer,
            description=event.description
        )

        self._rule_source_service.validate_git_access_data(
            item=entity, secret=event.git_access_secret
        )

        if event.git_access_secret:
            self._rule_source_service.set_secret(entity, event.git_access_secret)
        self._rule_source_service.save(entity)

        return build_response(
            code=HTTPStatus.CREATED,
            content=self._rule_source_service.dto(entity)
        )

    @validate_kwargs
    def update_rule_source(self, event: RuleSourcePatchModel, id: str):
        customer = event.customer or SYSTEM_CUSTOMER
        entity = self._ensure_rule_source(id, customer)
        if event.git_access_secret:
            self._rule_source_service.validate_git_access_data(
                item=entity,
                secret=event.git_access_secret
            )
            self._rule_source_service.set_secret(entity,
                                                 event.git_access_secret)
        if event.description:
            entity.description = event.description
        self._rule_source_service.save(entity)
        return build_response(
            code=HTTPStatus.OK,
            content=self._rule_source_service.dto(entity)
        )

    @validate_kwargs
    def sync_rule_source(self, event: BaseModel, id: str):
        customer = event.customer or SYSTEM_CUSTOMER
        entity = self._ensure_rule_source(id, customer)

        if not self._rule_source_service.is_allowed_to_sync(entity):
            raise ResponseFactory(HTTPStatus.CONFLICT).message(
                'Rule source is already being synced'
            ).exc()

        # todo handle error?
        self._lambda_client.invoke_function_async(
            RULE_META_UPDATER_LAMBDA_NAME,
            event={'rule_source_ids': [entity.id]}
        )
        return build_response(
            code=HTTPStatus.ACCEPTED,
            content=f'Rule source {id} is being synced'
        )
