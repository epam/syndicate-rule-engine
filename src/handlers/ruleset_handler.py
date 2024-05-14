from functools import cached_property
from http import HTTPStatus
from itertools import chain
from typing import Generator, Optional
from modular_sdk.commons.constants import ApplicationType

from handlers import AbstractHandler, Mapping
from helpers.constants import (
    CustodianEndpoint,
    ED_AWS_RULESET_NAME,
    ED_AZURE_RULESET_NAME,
    ED_GOOGLE_RULESET_NAME,
    ED_KUBERNETES_RULESET_NAME,
    EVENT_DRIVEN_ATTR,
    HTTPMethod,
    ID_ATTR,
    RULES_ATTR,
    RuleDomain,
    S3_PATH_ATTR,
)
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from helpers.reports import Standard
from helpers.system_customer import SYSTEM_CUSTOMER
from helpers.time_helper import utc_iso
from models.rule import Rule
from models.ruleset import Ruleset
from services import SERVICE_PROVIDER
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.license_service import LicenseService
from services.mappings_collector import LazyLoadedMappingsCollector
from services.rule_meta_service import RuleService
from services.rule_source_service import RuleSourceService
from services.ruleset_service import RulesetService
from services.setting_service import SettingsService
from validators.swagger_request_models import (
    EventDrivenRulesetDeleteModel,
    EventDrivenRulesetGetModel,
    EventDrivenRulesetPostModel,
    RulesetContentGetModel,
    RulesetDeleteModel,
    RulesetGetModel,
    RulesetPatchModel,
    RulesetPostModel,
)
from modular_sdk.services.application_service import ApplicationService
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)

CONCRETE_RULESET_NOT_FOUND_MESSAGE = \
    'The ruleset \'{name}\' version \'{version}\' ' \
    'in the customer \'{customer}\' does not exist'
RULESET_FOUND_MESSAGE = \
    'The ruleset \'{name}\' version \'{version}\' for ' \
    'in the customer \'{customer}\' already exists'


class RulesetHandler(AbstractHandler):

    def __init__(self, ruleset_service: RulesetService,
                 application_service: ApplicationService,
                 rule_service: RuleService,
                 s3_client: S3Client,
                 environment_service: EnvironmentService,
                 rule_source_service: RuleSourceService,
                 license_service: LicenseService,
                 settings_service: SettingsService,
                 mappings_collector: LazyLoadedMappingsCollector):
        self.ruleset_service = ruleset_service
        self.application_service = application_service
        self.rule_service = rule_service
        self.s3_client = s3_client
        self.environment_service = environment_service
        self.rule_source_service = rule_source_service
        self.license_service = license_service
        self.settings_service = settings_service
        self.mappings_collector = mappings_collector

    @classmethod
    def build(cls) -> 'RulesetHandler':
        return cls(
            ruleset_service=SERVICE_PROVIDER.ruleset_service,
            application_service=SERVICE_PROVIDER.modular_client.application_service(),
            rule_service=SERVICE_PROVIDER.rule_service,
            s3_client=SERVICE_PROVIDER.s3,
            environment_service=SERVICE_PROVIDER.environment_service,
            rule_source_service=SERVICE_PROVIDER.rule_source_service,
            license_service=SERVICE_PROVIDER.license_service,
            settings_service=SERVICE_PROVIDER.settings_service,
            mappings_collector=SERVICE_PROVIDER.mappings_collector
        )

    @cached_property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.RULESETS: {
                HTTPMethod.GET: self.get_ruleset,
                HTTPMethod.POST: self.create_ruleset,
                HTTPMethod.PATCH: self.update_ruleset,
                HTTPMethod.DELETE: self.delete_ruleset
            },
            CustodianEndpoint.RULESETS_CONTENT: {
                HTTPMethod.GET: self.pull_ruleset_content
            },
            CustodianEndpoint.ED_RULESETS: {
                HTTPMethod.GET: self.get_event_driven_ruleset,
                HTTPMethod.POST: self.post_event_driven_ruleset,
                HTTPMethod.DELETE: self.delete_event_driven_ruleset
            }
        }

    @cached_property
    def cloud_to_ed_ruleset_name(self) -> dict:
        return {
            RuleDomain.AWS.value: ED_AWS_RULESET_NAME,
            RuleDomain.AZURE.value: ED_AZURE_RULESET_NAME,
            RuleDomain.GCP.value: ED_GOOGLE_RULESET_NAME,
            RuleDomain.KUBERNETES.value: ED_KUBERNETES_RULESET_NAME
        }

    @validate_kwargs
    def get_event_driven_ruleset(self, event: EventDrivenRulesetGetModel):
        _LOG.debug('Get event-driven rulesets')
        items = self.ruleset_service.iter_standard(
            customer=SYSTEM_CUSTOMER,
            event_driven=True,
            cloud=event.cloud,
        )
        params_to_exclude = {S3_PATH_ATTR, ID_ATTR}
        if not event.get_rules:
            params_to_exclude.add(RULES_ATTR)
        return build_response(
            code=HTTPStatus.OK,
            content=(self.ruleset_service.dto(
                item, params_to_exclude) for item in items)
        )

    @validate_kwargs
    def post_event_driven_ruleset(self, event: EventDrivenRulesetPostModel):
        _LOG.debug('Create event-driven rulesets')
        name = self.cloud_to_ed_ruleset_name[event.cloud]
        version = str(event.version)

        maybe_ruleset = self.ruleset_service.get_standard(
            customer=SYSTEM_CUSTOMER, name=name, version=version,
        )
        if maybe_ruleset:
            raise ResponseFactory(HTTPStatus.CONFLICT).message(
                f'Ruleset {name}:{version} already exists'
            ).exc()
        if event.rules:
            rules = (
                self.rule_service.get_latest_rule(SYSTEM_CUSTOMER,
                                                  event.cloud, name)
                for name in event.rules
            )
        else:
            rules = self.rule_service.get_by_id_index(
                customer=SYSTEM_CUSTOMER, cloud=event.cloud
            )
        rules = list(self.rule_service.without_duplicates(rules=rules))

        if not rules:
            _LOG.warning('No rules by given parameters were found')
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='No rules found'
            )
        ruleset = self.ruleset_service.create(
            customer=SYSTEM_CUSTOMER,
            name=name,
            version=version,
            cloud=event.cloud,
            rules=[rule.name for rule in rules],
            active=True,
            event_driven=True,
            status={
                "code": "READY_TO_SCAN",
                "last_update_time": utc_iso(),
                "reason": "Assembled successfully"
            },
            licensed=False,
        )
        self.upload_ruleset(ruleset, self.build_policy(rules))
        self.ruleset_service.save(ruleset)
        return build_response(self.ruleset_service.dto(
            ruleset, params_to_exclude={RULES_ATTR, S3_PATH_ATTR}
        ), code=HTTPStatus.CREATED)

    @validate_kwargs
    def delete_event_driven_ruleset(self, event: EventDrivenRulesetDeleteModel):
        _LOG.debug('Delete event-driven rulesets')
        cloud = event.cloud
        name = self.cloud_to_ed_ruleset_name[cloud]
        version = str(event.version)
        item = self.ruleset_service.get_standard(
            customer=SYSTEM_CUSTOMER,
            name=name,
            version=version
        )
        if not item or not item.event_driven:
            return build_response(
                code=HTTPStatus.NO_CONTENT,
            )
        self.ruleset_service.delete(item)
        return build_response(
            code=HTTPStatus.NO_CONTENT,
        )

    def yield_standard_rulesets(self, customer: str,
                                name: Optional[str] = None,
                                version: Optional[str] = None,
                                cloud: Optional[str] = None,
                                active: Optional[bool] = None
                                ) -> Generator[Ruleset, None, None]:
        yield from self.ruleset_service.iter_standard(
            customer=customer,
            name=name,
            version=version,
            cloud=cloud,
            active=active
        )

    def yield_licensed_rulesets(
            self, customer: Optional[str] = None, name: Optional[str] = None,
            version: Optional[str] = None, cloud: Optional[str] = None,
            active: Optional[bool] = None) -> Generator[Ruleset, None, None]:

        def _check(ruleset: Ruleset) -> bool:
            """
            We currently don't have names and version for licensed
            rule-sets. Just their id
            :param ruleset:
            :return:
            """
            if name and ruleset.name != name:
                return False
            if version and ruleset.version != version:
                return False
            if cloud and ruleset.cloud != cloud.upper():
                return False
            if isinstance(active, bool) and ruleset.active != active:
                return False
            return True

        if not customer:  # SYSTEM
            # TODO probably remove
            yield from self.ruleset_service.iter_licensed(
                name=name,
                version=version,
                cloud=cloud,
                active=active
            )
            return
        applications = self.application_service.list(
            customer=customer,
            _type=ApplicationType.CUSTODIAN_LICENSES.value,
            deleted=False
        )

        licenses = tuple(self.license_service.to_licenses(applications))
        license_keys = {_license.license_key for _license in licenses}

        ids = chain.from_iterable(
            _license.ruleset_ids for _license in licenses
        )
        source = self.ruleset_service.iter_by_lm_id(ids)
        # source contains rule-sets from applications, now we
        # just filter them by input params
        for rs in filter(_check, source):
            rs.license_keys = list(set(rs.license_keys) & license_keys)
            yield rs

    @validate_kwargs
    def get_ruleset(self, event: RulesetGetModel):
        # maybe filter licensed rule-sets by tenants.

        params = dict(
            customer=event.customer or SYSTEM_CUSTOMER,
            name=event.name,
            version=event.version,
            cloud=event.cloud,
            active=event.active
        )
        _standard = self.yield_standard_rulesets(**params)
        _licensed = self.yield_licensed_rulesets(**params)
        # generators, by here they are not executed
        if not isinstance(event.licensed, bool):
            items = chain(_standard, _licensed)
        elif event.licensed:  # True
            items = chain(_licensed)
        else:  # False
            items = chain(_standard)

        params_to_exclude = {S3_PATH_ATTR, EVENT_DRIVEN_ATTR}
        if not event.get_rules:
            params_to_exclude.add(RULES_ATTR)
        return build_response(
            content=(self.ruleset_service.dto(
                item, params_to_exclude) for item in items)
        )

    def _filtered_rules(self, rules: list[Rule], severity: Optional[str],
                        service_section: Optional[str], standard: set,
                        mitre: set) -> Generator[Rule, None, None]:
        mappings = self.mappings_collector
        for rule in rules:
            name = rule.name
            if severity and mappings.severity.get(name) != severity:
                _LOG.debug(f'Skipping rule {name}. Severity does not match')
                continue
            if service_section and \
                    (mappings.service_section.get(name) != service_section):
                _LOG.debug(f'Skipping rule {name}. Service '
                           f'section does not match')
                continue
            if standard:  # list
                st = mappings.standard.get(name) or {}
                available = (Standard.deserialize_to_strs(st) | st.keys())
                if not all(item in available for item in standard):
                    _LOG.debug(f'Skipping rule {name}. '
                               f'Standard does not match')
                    continue
            if mitre:  # list
                available = mappings.mitre.get(name) or {}
                if not all(item in available for item in mitre):
                    _LOG.debug(f'Skipping rule: {name}. Mitre does not match')
                    continue
            yield rule

    @validate_kwargs
    def create_ruleset(self, event: RulesetPostModel):
        """
        Filtering the rules by standards, severity, mitre and service
        section is performed in python after the rules were queried from DB.
        (Also, we can implement filtering by resource if needed).
        But first, we must query some rules. We have four criteria to query by:
        - cloud: str          [required] ---------+
                                                  |---> Customer Rule Id GSI
        - rules: list         [optional] ---------+
        - git_project_id: str [optional] ---------+
                                                  |---> Customer Location GSI
        - git_ref: str        [optional] ---------+
        So, if concrete rules are provided, we just query each of them using
        the first index and then filter by git_project_id (and maybe git_ref)
        locally. If concrete names are not provided, we query by
        git_project_id & git_ref or by cloud.
        It seems efficient.
        :param event:
        :return:
        """
        customer = event.customer or SYSTEM_CUSTOMER

        ruleset = self.ruleset_service.get_standard(
            customer=customer, name=event.name, version=event.version,
        )
        if ruleset:
            raise ResponseFactory(HTTPStatus.CONFLICT).message(
                f'Ruleset {event.name}:{event.version} already exists'
            ).exc()
        # here we must collect a list of Rule items using incoming params
        if event.rules:
            _LOG.info('Concrete rules were provided. '
                      'Assembling the ruleset using them')
            rules = []
            for rule_name in event.rules:
                rule = self.rule_service.resolve_rule(
                    customer=customer,
                    name_prefix=rule_name,
                    cloud=event.cloud
                )
                if not rule:
                    raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                        self.rule_service.not_found_message(rule_name)
                    ).exc()
                rules.append(rule)
            rules = self.rule_service.filter_by(
                rules=rules,
                git_project=event.git_project_id,
                ref=event.git_ref
            )
        elif event.git_project_id:
            _LOG.debug('Git project id is provided. Querying rules by it')
            rules = self.rule_service.get_by(
                customer=customer,
                project=event.git_project_id,
                ref=event.git_ref,
                cloud=event.cloud
            )
        else:
            _LOG.debug('Concrete names and project id are not provided. '
                       'Querying all the rules for cloud')
            rules = self.rule_service.get_by_id_index(customer, event.cloud)
        _LOG.info('Removing rules duplicates')
        rules = list(self.rule_service.without_duplicates(rules=rules))

        # here we filter by standards, service_section, mitre and severity
        _LOG.debug('Filtering rules by mappings')
        rules = list(self._filtered_rules(
            rules=rules,
            severity=event.severity,
            service_section=event.service_section,
            standard=event.standard,
            mitre=event.mitre
        ))

        if not rules:
            _LOG.warning('No rules found by filters')
            raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                'No rules left after filtering'
            ).exc()

        ruleset = self.ruleset_service.create(
            customer=customer,
            name=event.name,
            version=event.version,
            cloud=event.cloud,
            rules=[rule.name for rule in rules],
            active=event.active,
            event_driven=False,
            status={
                "code": "READY_TO_SCAN",
                "last_update_time": utc_iso(),
                "reason": "Assembled successfully"
            },
            allowed_for=[],
            licensed=False,
        )
        self.upload_ruleset(ruleset, self.build_policy(rules))
        self.ruleset_service.save(ruleset)
        return build_response(self.ruleset_service.dto(
            ruleset, params_to_exclude={RULES_ATTR, S3_PATH_ATTR}
        ), code=HTTPStatus.CREATED)

    @staticmethod
    def build_policy(rules: list[Rule]) -> dict:
        return {'policies': [
            rule.build_policy() for rule in rules
        ]}

    def upload_ruleset(self, ruleset: Ruleset, content: dict) -> None:
        """
        Uploads content to s3 and sets s3_path to ruleset item
        :param ruleset:
        :param content:
        :return:
        """
        bucket = self.environment_service.get_rulesets_bucket_name()
        key = self.ruleset_service.build_s3_key(ruleset)
        self.s3_client.gz_put_json(
            bucket=bucket,
            key=key,
            obj=content
        )
        self.ruleset_service.set_s3_path(ruleset, bucket=bucket,
                                         key=key)

    @validate_kwargs
    def update_ruleset(self, event: RulesetPatchModel):
        customer = event.customer or SYSTEM_CUSTOMER

        ruleset_to_update = self.ruleset_service.get_standard(
            customer=customer,
            name=event.name,
            version=event.version
        )
        if not ruleset_to_update:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                f'Ruleset {event.name}:{event.version} not found'
            ).exc()
        s3_path = ruleset_to_update.s3_path.as_dict()
        if not s3_path:
            return build_response(code=HTTPStatus.BAD_REQUEST,
                                  content='Cannot update empty ruleset')

        if event.rules_to_attach or event.rules_to_detach:
            content = self.s3_client.gz_get_json(
                bucket=s3_path.get('bucket_name'),
                key=s3_path.get('path')
            )
            name_body = self.rule_name_to_body(content)
            for to_detach in event.rules_to_detach:
                name_body.pop(to_detach, None)

            for rule in event.rules_to_attach:
                item = self.rule_service.get_latest_rule(
                    customer, ruleset_to_update.cloud, rule
                )
                if not item:
                    return build_response(
                        code=HTTPStatus.NOT_FOUND,
                        content=self.rule_service.not_found_message(rule)
                    )
                name_body[item.name] = item.build_policy()
            ruleset_to_update.rules = list(name_body.keys())
            self.upload_ruleset(ruleset_to_update,
                                {'policies': list(name_body.values())})

        was_active = ruleset_to_update.active
        if isinstance(event.active, bool):
            ruleset_to_update.active = event.active
        if not was_active and ruleset_to_update.active:
            _LOG.debug('Ruleset to update became active. Deactivation '
                       'previous rulesets')
            previous = self.ruleset_service.get_previous_ruleset(
                ruleset_to_update)
            for item in previous:
                item.active = False
                self.ruleset_service.save(item)
        self.ruleset_service.set_ruleset_status(ruleset_to_update)
        self.ruleset_service.save(ruleset_to_update)

        return build_response(
            code=HTTPStatus.OK,
            content=self.ruleset_service.dto(
                ruleset_to_update,
                {S3_PATH_ATTR, RULES_ATTR, EVENT_DRIVEN_ATTR}
            )
        )

    @staticmethod
    def rule_name_to_body(content: dict) -> dict:
        res = {}
        for item in content.get('policies', []):
            res[item.get('name')] = item
        return res

    @validate_kwargs
    def delete_ruleset(self, event: RulesetDeleteModel):
        customer = event.customer or SYSTEM_CUSTOMER

        ruleset = self.ruleset_service.get_standard(
            customer=customer,
            name=event.name,
            version=event.version
        )
        if not ruleset:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                f'Ruleset {event.name}:{event.version} not found'
            ).exc()
        previous = next(
            self.ruleset_service.get_previous_ruleset(ruleset, limit=1), None
        )
        if previous:
            _LOG.info('Previous version of ruleset is found. Making it active')
            if not previous.active:
                previous.active = True
            self.ruleset_service.save(previous)

        self.ruleset_service.delete(ruleset)
        return build_response(code=HTTPStatus.NO_CONTENT)

    @validate_kwargs
    def pull_ruleset_content(self, event: RulesetContentGetModel):

        customer = event.customer or SYSTEM_CUSTOMER
        name = event.name
        version = event.version

        ruleset = self.ruleset_service.get_standard(
            customer=customer, name=name, version=version
        )
        if not ruleset:
            _LOG.info(f'No rulesets with the name \'{name}\', version '
                      f'{version} in the customer {customer}')
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'The ruleset \'{name}\' version {version} '
                        f'in the customer {customer} does not exist'
            )
        s3_path = ruleset.s3_path.as_dict()
        if not s3_path:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'The ruleset \'{name}\' version {version} '
                        f'in the customer {customer} has not yet been '
                        f'successfully assembled'
            )
        # by default all new files have this header in metadata. But some
        # old gzipped files can have octet-stream type, so
        # I forcefully set the right encoding for response
        return build_response(
            code=HTTPStatus.OK,
            content=self.ruleset_service.download_url(ruleset)
        )
