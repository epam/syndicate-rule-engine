import json
from functools import cached_property
from itertools import chain
from typing import Optional, Any, Iterable, List, Generator

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response, RESPONSE_RESOURCE_NOT_FOUND_CODE, \
    RESPONSE_OK_CODE, RESPONSE_BAD_REQUEST_CODE, RESPONSE_NO_CONTENT, \
    RESPONSE_FORBIDDEN_CODE
from helpers.constants import ID_ATTR, ACTIVE_ATTR, \
    EVENT_DRIVEN_ATTR, NAME_ATTR, VERSION_ATTR, CUSTOMER_ATTR, RULES_ATTR, \
    GET_METHOD, POST_METHOD, PATCH_METHOD, DELETE_METHOD, CLOUD_ATTR, \
    RULE_VERSION_ATTR, RULES_TO_DETACH, \
    RULES_TO_ATTACH, LICENSED_ATTR, GET_RULES_ATTR, \
    S3_PATH_ATTR, TENANTS_ATTR, \
    TENANT_ALLOWANCE, TENANT_RESTRICTION, \
    CUSTODIAN_LICENSES_TYPE, AWS_CLOUD_ATTR, AZURE_CLOUD_ATTR, \
    GCP_CLOUD_ATTR, ED_AWS_RULESET_NAME, ED_AZURE_RULESET_NAME, \
    ED_GOOGLE_RULESET_NAME, SEVERITY_ATTR, SERVICE_SECTION, STANDARD, MITRE, \
    GIT_REF_ATTR,  GIT_PROJECT_ID_ATTR
from helpers.log_helper import get_logger
from helpers.reports import Standard
from helpers.system_customer import SYSTEM_CUSTOMER
from helpers.time_helper import utc_iso
from models.modular.application import CustodianLicensesApplicationMeta
from models.rule import Rule
from models.ruleset import Ruleset
from services import SERVICE_PROVIDER
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.license_service import LicenseService
from services.modular_service import ModularService
from services.rbac.restriction_service import RestrictionService
from services.rule_meta_service import RuleService, LazyLoadedMappingsCollector
from services.rule_source_service import RuleSourceService
from services.ruleset_service import RulesetService
from services.setting_service import SettingsService

_LOG = get_logger(__name__)
RULESET_COMPILER_LAMBDA_NAME = 'caas-ruleset-compiler'

RULESETS_NOT_FOUND_MESSAGE = 'No rulesets found matching given query'
CONCRETE_RULESET_NOT_FOUND_MESSAGE = \
    'The ruleset \'{name}\' version \'{version}\' ' \
    'in the customer \'{customer}\' does not exist'
RULESET_FOUND_MESSAGE = \
    'The ruleset \'{name}\' version \'{version}\' for ' \
    'in the customer \'{customer}\' already exists'


class RulesetHandler(AbstractHandler):

    def __init__(self, ruleset_service: RulesetService,
                 modular_service: ModularService,
                 rule_service: RuleService,
                 s3_client: S3Client,
                 environment_service: EnvironmentService,
                 restriction_service: RestrictionService,
                 rule_source_service: RuleSourceService,
                 license_service: LicenseService,
                 settings_service: SettingsService,
                 mappings_collector: LazyLoadedMappingsCollector):
        self.ruleset_service = ruleset_service
        self.modular_service = modular_service
        self.rule_service = rule_service
        self.s3_client = s3_client
        self.environment_service = environment_service
        self.restriction_service = restriction_service
        self.rule_source_service = rule_source_service
        self.license_service = license_service
        self.settings_service = settings_service
        self.mappings_collector = mappings_collector

    @classmethod
    def build(cls) -> 'RulesetHandler':
        return cls(
            ruleset_service=SERVICE_PROVIDER.ruleset_service(),
            modular_service=SERVICE_PROVIDER.modular_service(),
            rule_service=SERVICE_PROVIDER.rule_service(),
            s3_client=SERVICE_PROVIDER.s3(),
            environment_service=SERVICE_PROVIDER.environment_service(),
            restriction_service=SERVICE_PROVIDER.restriction_service(),
            rule_source_service=SERVICE_PROVIDER.rule_source_service(),
            license_service=SERVICE_PROVIDER.license_service(),
            settings_service=SERVICE_PROVIDER.settings_service(),
            mappings_collector=SERVICE_PROVIDER.mappings_collector()
        )

    def define_action_mapping(self):
        return {
            '/rulesets': {
                GET_METHOD: self.get_ruleset,
                POST_METHOD: self.create_ruleset,
                PATCH_METHOD: self.update_ruleset,
                DELETE_METHOD: self.delete_ruleset
            },
            '/rulesets/content': {
                GET_METHOD: self.pull_ruleset_content
            },
            '/rulesets/event-driven': {
                GET_METHOD: self.get_event_driven_ruleset,
                POST_METHOD: self.post_event_driven_ruleset,
                DELETE_METHOD: self.delete_event_driven_ruleset
            }
        }

    @cached_property
    def cloud_to_ed_ruleset_name(self) -> dict:
        return {
            AWS_CLOUD_ATTR: ED_AWS_RULESET_NAME,
            AZURE_CLOUD_ATTR: ED_AZURE_RULESET_NAME,
            GCP_CLOUD_ATTR: ED_GOOGLE_RULESET_NAME
        }

    @staticmethod
    def _only_for_system(event: dict):
        if event.get(CUSTOMER_ATTR) and event.get(
                CUSTOMER_ATTR) != SYSTEM_CUSTOMER:
            return build_response(code=RESPONSE_FORBIDDEN_CODE,
                                  content='Not allowed')

    def get_event_driven_ruleset(self, event: dict) -> dict:
        self._only_for_system(event)
        _LOG.debug('Get event-driven rulesets')
        cloud = event.get(CLOUD_ATTR)
        get_rules = event.get(GET_RULES_ATTR)
        items = self.ruleset_service.iter_standard(
            customer=SYSTEM_CUSTOMER,
            event_driven=True,
            cloud=cloud,
        )
        params_to_exclude = {S3_PATH_ATTR, ID_ATTR}
        if not get_rules:
            params_to_exclude.add(RULES_ATTR)
        return build_response(
            code=RESPONSE_OK_CODE,
            content=(self.ruleset_service.dto(
                item, params_to_exclude) for item in items)
        )

    def post_event_driven_ruleset(self, event: dict) -> dict:
        self._only_for_system(event)
        _LOG.debug('Create event-driven rulesets')
        cloud = event[CLOUD_ATTR]
        name = self.cloud_to_ed_ruleset_name[cloud]
        version = str(event[VERSION_ATTR])
        rule_names = event.get(RULES_ATTR)
        rule_version = event.get(RULE_VERSION_ATTR)

        maybe_ruleset = self.ruleset_service.get_standard(
            customer=SYSTEM_CUSTOMER, name=name, version=version,
        )
        self._assert_does_not_exist(maybe_ruleset, name=name,
                                    version=version, customer=SYSTEM_CUSTOMER)
        if rule_names:
            rules = (
                self.rule_service.get_latest_rule(SYSTEM_CUSTOMER, cloud, name)
                for name in rule_names
            )
        else:
            rules = self.rule_service.get_by_id_index(
                customer=SYSTEM_CUSTOMER, cloud=cloud
            )
        rules = list(self.rule_service.without_duplicates(
            rules=rules, rules_version=rule_version
        ))

        if not rules:
            _LOG.warning('No rules by given parameters were found')
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content='No rules found'
            )
        ruleset = self.ruleset_service.create(
            customer=SYSTEM_CUSTOMER,
            name=name,
            version=version,
            cloud=cloud,
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
        ))

    def delete_event_driven_ruleset(self, event: dict) -> dict:
        self._only_for_system(event)
        _LOG.debug('Delete event-driven rulesets')
        cloud = event[CLOUD_ATTR]
        name = self.cloud_to_ed_ruleset_name[cloud]
        version = str(event.get(VERSION_ATTR))
        item = self.ruleset_service.get_standard(
            customer=SYSTEM_CUSTOMER,
            name=name,
            version=version
        )
        if not item or not item.event_driven:
            return build_response(
                code=RESPONSE_NO_CONTENT,
            )
        self.ruleset_service.delete(item)
        return build_response(
            code=RESPONSE_NO_CONTENT,
        )

    def yield_standard_rulesets(self, customer: Optional[str] = None,
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
            source = self.ruleset_service.iter_licensed(
                name=name,
                version=version,
                cloud=cloud,
                active=active
            )
        else:
            license_keys = set()
            applications = self.modular_service.get_applications(
                customer=customer,
                _type=CUSTODIAN_LICENSES_TYPE
            )
            for application in applications:
                meta = CustodianLicensesApplicationMeta(
                    **application.meta.as_dict())
                license_keys.update(
                    key for key in meta.cloud_to_license_key().values() if key
                )
            licenses = (
                self.license_service.get_license(lk) for lk in license_keys
            )
            ids = chain.from_iterable(
                _license.ruleset_ids for _license in licenses if _license
            )
            source = self.ruleset_service.iter_by_lm_id(ids)
            # source contains rule-sets from applications, now we
            # just filter them by input params
            source = filter(_check, source)
        yield from source

    def get_ruleset(self, event: dict):
        _LOG.debug(f'Get ruleset event: {event}')
        # maybe filter licensed rule-sets by tenants.

        get_rules = event.get(GET_RULES_ATTR)
        licensed = event.get(LICENSED_ATTR)
        params = dict(
            customer=event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER,
            name=event.get(NAME_ATTR),
            version=event.get(VERSION_ATTR),
            cloud=event.get(CLOUD_ATTR),
            active=event.get(ACTIVE_ATTR)
        )
        _standard = self.yield_standard_rulesets(**params)
        _licensed = self.yield_licensed_rulesets(**params)
        # generators, by here they are not executed
        if not isinstance(licensed, bool):  # None, both licensed and standard
            items = chain(_standard, _licensed)
        elif licensed:  # True
            items = chain(_licensed)
        else:  # False
            items = chain(_standard)

        params_to_exclude = {S3_PATH_ATTR, EVENT_DRIVEN_ATTR}
        if not get_rules:
            params_to_exclude.add(RULES_ATTR)
        return build_response(
            content=(self.ruleset_service.dto(
                item, params_to_exclude) for item in items)
        )

    def _check_tenants(self, tenants: Iterable[str]) -> List[str]:
        errors = []
        for tenant_name in tenants:
            if not self.restriction_service.is_allowed_tenant(tenant_name):
                errors.append(f'Tenant {tenant_name} not found')
        return errors

    def _filtered_rules(self, rules: List[Rule], severity: Optional[str],
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
                available = (Standard.deserialize(st, return_strings=True) |
                             st.keys())
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

    def create_ruleset(self, event: dict) -> dict:
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
        _LOG.debug(f'Create ruleset event: {event}')
        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        name, version = event.get(NAME_ATTR), event.get(VERSION_ATTR)
        cloud = event.get(CLOUD_ATTR)
        active = event.get(ACTIVE_ATTR)
        rule_names: set = event.get(RULES_ATTR)
        git_project_id = event.get(GIT_PROJECT_ID_ATTR)
        git_ref = event.get(GIT_REF_ATTR)

        allowed_tenants = (event.get(TENANT_ALLOWANCE)
                           or event.get(TENANTS_ATTR))
        errors = self._check_tenants(allowed_tenants)
        if errors:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content='\n'.join(errors)
            )

        ruleset = self.ruleset_service.get_standard(
            customer=customer, name=name, version=version,
        )
        self._assert_does_not_exist(ruleset, name=name,
                                    version=version, customer=customer)
        # here we must collect a list of Rule items using incoming params
        if rule_names:
            _LOG.info('Concrete rules were provided. '
                      'Assembling the ruleset using them')
            rules = []
            for rule_name in rule_names:
                rule = self.rule_service.resolve_rule(
                    customer=customer,
                    name_prefix=rule_name,
                    cloud=cloud
                )
                if not rule:
                    return build_response(
                        code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                        content=self.rule_service.not_found_message(rule_name)
                    )
                rules.append(rule)
            rules = self.rule_service.filter_by(
                rules=rules,
                git_project=git_project_id,
                ref=git_ref
            )
        elif git_project_id:
            _LOG.debug('Git project id is provided. Querying rules by it')
            rules = self.rule_service.get_by(
                customer=customer,
                project=git_project_id,
                ref=git_ref,
                cloud=cloud
            )
        else:
            _LOG.debug('Concrete names and project id are not provided. '
                       'Querying all the rules for cloud')
            rules = self.rule_service.get_by_id_index(customer, cloud)
        _LOG.info('Removing rules duplicates')
        rules = list(self.rule_service.without_duplicates(rules=rules))

        # here we filter by standards, service_section, mitre and severity
        _LOG.debug('Filtering rules by mappings')
        rules = list(self._filtered_rules(
            rules=rules,
            severity=event.get(SEVERITY_ATTR),
            service_section=event.get(SERVICE_SECTION),
            standard=event.get(STANDARD),
            mitre=event.get(MITRE)
        ))

        if not rules:
            _LOG.warning('No rules found by filters')
            return build_response(code=RESPONSE_BAD_REQUEST_CODE,
                                  content='No rules left after filtering')
        ruleset = self.ruleset_service.create(
            customer=customer,
            name=name,
            version=version,
            cloud=cloud,
            rules=[rule.name for rule in rules],
            active=active,
            event_driven=False,
            status={
                "code": "READY_TO_SCAN",
                "last_update_time": utc_iso(),
                "reason": "Assembled successfully"
            },
            allowed_for=allowed_tenants,
            licensed=False,
        )
        self.upload_ruleset(ruleset, self.build_policy(rules))
        self.ruleset_service.save(ruleset)
        return build_response(self.ruleset_service.dto(
            ruleset, params_to_exclude={RULES_ATTR, S3_PATH_ATTR}
        ))

    @staticmethod
    def build_policy(rules: List[Rule]) -> dict:
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
        self.s3_client.put_object(
            bucket_name=bucket,
            object_name=key,
            body=json.dumps(content, separators=(",", ":"))
        )
        self.ruleset_service.set_s3_path(ruleset, bucket=bucket,
                                         key=key)

    def update_ruleset(self, event):
        _LOG.debug(f'Update ruleset event: {event}')

        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        name = event.get(NAME_ATTR)
        version = event.get(VERSION_ATTR)

        rules_to_attach = event.get(RULES_TO_ATTACH)
        rules_to_detach = event.get(RULES_TO_DETACH)
        active = event.get(ACTIVE_ATTR)
        tenant_allowance = set(event.get(TENANT_ALLOWANCE))
        tenant_restriction = set(event.get(TENANT_RESTRICTION))

        ruleset_to_update = self.ruleset_service. \
            get_ruleset_filtered_by_tenant(customer=customer, name=name,
                                           version=version)
        self._assert_exists(ruleset_to_update, name=name, version=version,
                            customer=customer)
        s3_path = ruleset_to_update.s3_path.as_dict()
        if not s3_path:
            return build_response(code=RESPONSE_BAD_REQUEST_CODE,
                                  content='Cannot update empty ruleset')

        if rules_to_attach or rules_to_detach:
            content = self.s3_client.get_json_file_content(
                bucket_name=s3_path.get('bucket_name'),
                full_file_name=s3_path.get('path')
            )
            name_body = self.rule_name_to_body(content)
            for to_detach in rules_to_detach:
                name_body.pop(to_detach, None)

            for rule in rules_to_attach:
                item = self.rule_service.get_latest_rule(
                    customer, ruleset_to_update.cloud, rule
                )
                if not item:
                    return build_response(
                        code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                        content=self.rule_service.not_found_message(rule)
                    )
                name_body[item.name] = item.build_policy()
            ruleset_to_update.rules = list(name_body.keys())
            self.upload_ruleset(ruleset_to_update,
                                {'policies': list(name_body.values())})

        # tenant restriction
        existing_allowed_tenants = set(ruleset_to_update.allowed_for or [])
        to_check = tenant_allowance - existing_allowed_tenants
        errors = self._check_tenants(to_check)
        if errors:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content='\n'.join(errors)
            )
        existing_allowed_tenants -= tenant_restriction
        existing_allowed_tenants.update(to_check)
        ruleset_to_update.allowed_for = list(existing_allowed_tenants)
        # end tenant restriction

        was_active = ruleset_to_update.active
        if isinstance(active, bool):
            ruleset_to_update.active = active
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
            code=RESPONSE_OK_CODE,
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

    def delete_ruleset(self, event):
        _LOG.debug(f'Delete ruleset event: {event}')

        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        name = event.get(NAME_ATTR)
        version = event.get(VERSION_ATTR)

        ruleset = self.ruleset_service.get_ruleset_filtered_by_tenant(
            customer=customer, name=name, version=version
        )
        self._assert_exists(ruleset, CONCRETE_RULESET_NOT_FOUND_MESSAGE,
                            customer=customer, name=name, version=version)
        previous = next(
            self.ruleset_service.get_previous_ruleset(ruleset, limit=1), None
        )
        if previous:
            _LOG.info('Previous version of ruleset is found. Making it active')
            if not previous.active:
                previous.active = True
            self.ruleset_service.save(previous)

        self.ruleset_service.delete(ruleset)
        return build_response(code=RESPONSE_NO_CONTENT)

    def pull_ruleset_content(self, event):
        _LOG.debug(f'Pull rulesets\' content event: {event}')

        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER
        name = event.get(NAME_ATTR)
        version = event.get(VERSION_ATTR)

        ruleset = self.ruleset_service.get_standard(
            customer=customer, name=name, version=version
        )
        if not ruleset:
            _LOG.info(f'No rulesets with the name \'{name}\', version '
                      f'{version} in the customer {customer}')
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'The ruleset \'{name}\' version {version} '
                        f'in the customer {customer} does not exist'
            )
        s3_path = ruleset.s3_path.as_dict()
        if not s3_path:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'The ruleset \'{name}\' version {version} '
                        f'in the customer {customer} has not yet been '
                        f'successfully assembled'
            )
        _LOG.debug(f'S3 path: {s3_path}')
        if not self.s3_client.file_exists(s3_path.get('bucket_name'),
                                          s3_path.get('path')):
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'The content of ruleset \'{name}\' version {version} '
                        f'in the customer {customer} does not exist'
            )
        url = self.s3_client.generate_presigned_url(
            bucket_name=s3_path.get('bucket_name'),
            full_file_name=s3_path.get('path'),
            expires_in_sec=300
        )
        return build_response(
            code=RESPONSE_OK_CODE,
            content=url
        )

    def _assert_exists(self, entity: Optional[Any] = None,
                       message: str = None, **kwargs) -> None:
        super()._assert_exists(
            entity, message or CONCRETE_RULESET_NOT_FOUND_MESSAGE, **kwargs)

    def _assert_does_not_exist(self, entity: Optional[Any] = None,
                               message: str = None, **kwargs) -> None:
        super()._assert_does_not_exist(
            entity, message or RULESET_FOUND_MESSAGE, **kwargs)
