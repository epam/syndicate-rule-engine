from functools import cached_property
from http import HTTPStatus

from handlers import AbstractHandler, Mapping
from helpers.constants import CustodianEndpoint, HTTPMethod
from helpers.lambda_response import ResponseFactory, build_response
from helpers.system_customer import SYSTEM_CUSTOMER
from models.rule_source import RuleSource
from services import SP
from services.rule_meta_service import RuleService
from services.rule_source_service import RuleSourceService
from validators.swagger_request_models import (
    RuleSourceDeleteModel,
    RuleSourceGetModel,
    RuleSourcePatchModel,
    RuleSourcePostModel,
)
from validators.utils import validate_kwargs


class RuleSourceHandler(AbstractHandler):
    def __init__(self, rule_source_service: RuleSourceService,
                 rule_service: RuleService):
        self.rule_source_service = rule_source_service
        self.rule_service = rule_service

    @cached_property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.RULE_SOURCES: {
                HTTPMethod.GET: self.get_rule_source,
                HTTPMethod.POST: self.create_rule_source,
                HTTPMethod.PATCH: self.update_rule_source,
                HTTPMethod.DELETE: self.delete_rule_source
            }
        }

    @classmethod
    def build(cls):
        return cls(
            rule_source_service=SP.rule_source_service,
            rule_service=SP.rule_service,
        )

    @validate_kwargs
    def get_rule_source(self, event: RuleSourceGetModel):
        customer = event.customer or SYSTEM_CUSTOMER
        rule_source_id = event.id
        service = self.rule_source_service
        if rule_source_id:
            entity = self._obtain_rule_source(
                rule_source_id=rule_source_id,
                customer=customer,
            )
            if not entity:
                return build_response(
                    content=f'Rule-source:{rule_source_id} does not exist.',
                    code=HTTPStatus.NOT_FOUND
                )
            rule_sources = [entity]
        else:
            git_project_id = event.git_project_id
            rule_sources = self.rule_source_service.list_rule_sources(
                customer=customer,
                git_project_id=git_project_id
            )

        return build_response(
            code=HTTPStatus.OK,
            content=[
                service.get_rule_source_dto(rule_source=rule_source)
                for rule_source in rule_sources
            ]
        )

    @validate_kwargs
    def create_rule_source(self, event: RuleSourcePostModel):
        customer = event.customer or SYSTEM_CUSTOMER
        git_url = event.git_url
        git_project_id = event.git_project_id
        git_ref = event.git_ref
        git_rules_prefix = event.git_rules_prefix

        rsid = self.rule_source_service.derive_rule_source_id(
            customer=customer,
            git_url=git_url,
            git_project_id=git_project_id,
            git_ref=git_ref,
            git_rules_prefix=git_rules_prefix
        )

        # Obtaining just an entity.
        entity = self._obtain_rule_source(
            rule_source_id=rsid,
            customer=None,
        )

        if entity:
            raise ResponseFactory(HTTPStatus.CONFLICT).message(
                'Such rule source already exists within a customer'
            ).exc()
        self.rule_source_service.validate_git_access_data(
            git_project_id=git_project_id,
            git_url=git_url,
            git_access_secret=event.git_access_secret
        )
        entity = self.rule_source_service.create_rule_source(
            git_project_id=git_project_id,
            git_url=git_url,
            git_ref=git_ref,
            git_rules_prefix=git_rules_prefix,
            git_access_type=event.git_access_type,
            customer=customer,
            description=event.description,
            git_access_secret=event.git_access_secret
        )
        self.rule_source_service.save_rule_source(entity)

        return build_response(
            code=HTTPStatus.CREATED,
            content=self.rule_source_service.get_rule_source_dto(rule_source=entity)
        )

    @validate_kwargs
    def update_rule_source(self, event: RuleSourcePatchModel):
        customer = event.customer or SYSTEM_CUSTOMER

        entity = self._obtain_rule_source(
            rule_source_id=event.id,
            customer=customer,
        )

        if not entity:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='Rule source not found'
            )

        if event.git_access_secret:
            self.rule_source_service.validate_git_access_data(
                git_project_id=entity.git_project_id,
                git_url=entity.git_url,
                git_access_secret=event.git_access_secret
            )
            self.rule_source_service.set_secret(entity,
                                                event.git_access_secret)
        if event.description:
            entity.description = event.description
        self.rule_source_service.save_rule_source(rule_source=entity)
        return build_response(
            code=HTTPStatus.OK,
            content=self.rule_source_service.get_rule_source_dto(rule_source=entity)
        )

    @validate_kwargs
    def delete_rule_source(self, event: RuleSourceDeleteModel):

        customer = event.customer or SYSTEM_CUSTOMER

        service = self.rule_source_service

        entity = self._obtain_rule_source(
            rule_source_id=event.id,
            customer=customer,
        )
        if not entity:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Rule source {event.id} does not exist.'
            )

        rules = self.rule_service.get_by_location_index(
            customer=customer,
            project=entity.git_project_id
        )
        self.rule_service.batch_delete(rules)
        service.delete_rule_source(rule_source=entity)
        return build_response(code=HTTPStatus.NO_CONTENT)

    def _obtain_rule_source(self, rule_source_id: str, 
                            customer: str | None = None) -> RuleSource | None:
        service = self.rule_source_service
        entity = service.get(rule_source_id=rule_source_id)
        if not entity or customer and entity.customer != customer:
            return
        return entity
