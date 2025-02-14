import operator
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus
from itertools import chain
from typing import Generator, Optional, cast

from modular_sdk.commons.constants import ApplicationType
from modular_sdk.services.application_service import ApplicationService

from handlers import AbstractHandler, Mapping
from helpers import Version
from helpers.constants import (
    EVENT_DRIVEN_ATTR,
    RULES_ATTR,
    S3_PATH_ATTR,
    CustodianEndpoint,
    HTTPMethod,
    RuleSourceType,
)
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from helpers.time_helper import utc_iso
from models.rule import Rule, RuleIndex
from models.rule_source import RuleSource
from models.ruleset import EMPTY_VERSION, Ruleset
from services import SERVICE_PROVIDER
from services.clients.lm_client import LMClientAfter3p0
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.license_manager_service import LicenseManagerService
from services.license_service import LicenseService
from services.rule_meta_service import RuleNamesResolver, RuleService
from services.rule_source_service import RuleSourceService
from services.ruleset_service import RulesetService
from validators.swagger_request_models import (
    EventDrivenRulesetDeleteModel,
    EventDrivenRulesetGetModel,
    EventDrivenRulesetPostModel,
    RulesetDeleteModel,
    RulesetGetModel,
    RulesetPatchModel,
    RulesetPostModel,
    RulesetReleasePostModel,
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class RulesetHandler(AbstractHandler):
    def __init__(
        self,
        ruleset_service: RulesetService,
        application_service: ApplicationService,
        rule_service: RuleService,
        s3_client: S3Client,
        environment_service: EnvironmentService,
        rule_source_service: RuleSourceService,
        license_service: LicenseService,
        license_manager_service: LicenseManagerService,
    ):
        self.ruleset_service = ruleset_service
        self.application_service = application_service
        self.rule_service = rule_service
        self.s3_client = s3_client
        self.environment_service = environment_service
        self.rule_source_service = rule_source_service
        self.license_service = license_service
        self.license_manager_service = license_manager_service

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
            license_manager_service=SERVICE_PROVIDER.license_manager_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.RULESETS: {
                HTTPMethod.GET: self.get_ruleset,
                HTTPMethod.POST: self.create_ruleset,
                HTTPMethod.PATCH: self.update_ruleset,
                HTTPMethod.DELETE: self.delete_ruleset,
            },
            CustodianEndpoint.ED_RULESETS: {
                HTTPMethod.GET: self.get_event_driven_ruleset,
                HTTPMethod.POST: self.post_event_driven_ruleset,
                HTTPMethod.DELETE: self.delete_event_driven_ruleset,
            },
            CustodianEndpoint.RULESETS_RELEASE: {
                HTTPMethod.POST: self.release_ruleset
            },
        }

    @validate_kwargs
    def get_event_driven_ruleset(self, event: EventDrivenRulesetGetModel):
        _LOG.debug('Get event-driven rulesets')
        items = self.ruleset_service.iter_standard(
            customer=SYSTEM_CUSTOMER, event_driven=True, cloud=event.cloud
        )
        params_to_exclude = {EVENT_DRIVEN_ATTR}
        if not event.get_rules:
            params_to_exclude.add(RULES_ATTR)
        return build_response(
            code=HTTPStatus.OK,
            content=(
                self.ruleset_service.dto(item, params_to_exclude)
                for item in items
            ),
        )

    @validate_kwargs
    def post_event_driven_ruleset(self, event: EventDrivenRulesetPostModel):
        _LOG.debug('Create event-driven rulesets')
        customer = event.customer or SYSTEM_CUSTOMER

        rs: RuleSource | None = None
        desired_version: Version
        if event.rule_source_id:
            rs = self.rule_source_service.get_nullable(event.rule_source_id)
            if not rs or rs.customer != customer:
                raise (
                    ResponseFactory(HTTPStatus.NOT_FOUND)
                    .message(
                        self.rule_source_service.not_found_message(
                            event.rule_source_id
                        )
                    )
                    .exc()
                )

        if event.version:
            _LOG.debug(
                'User specified the version of ruleset '
                'he wants to create. Checking whether we can'
            )
            desired_version = Version(event.version)  # validated
            ruleset = self.ruleset_service.get_event_driven(
                cloud=event.cloud, version=desired_version.to_str()
            )
            if ruleset:
                raise (
                    ResponseFactory(HTTPStatus.CONFLICT)
                    .message(
                        f'Event driven ruleset {desired_version} already exists'
                    )
                    .exc()
                )
        else:
            _LOG.debug('User did not provide the version. ')
            release_version = None
            if rs and rs.type == RuleSourceType.GITHUB_RELEASE:
                try:
                    release_version = Version(rs.latest_sync.release_tag or '')
                except ValueError:
                    pass
            if release_version:
                ruleset = self.ruleset_service.get_event_driven(
                    cloud=event.cloud, version=release_version.to_str()
                )
                if ruleset:
                    raise (
                        ResponseFactory(HTTPStatus.CONFLICT)
                        .message(
                            f'Event driven ruleset for rules release '
                            f'{release_version} already exists'
                        )
                        .exc()
                    )
                desired_version = release_version
            else:
                latest = self.ruleset_service.get_latest_event_driven(
                    cloud=event.cloud
                )
                if latest:
                    _LOG.debug(
                        'The previous ruleset found. Creating the '
                        'next version'
                    )
                    desired_version = Version(latest.version).next_major()
                else:
                    _LOG.debug(
                        'The previous ruleset not found. Creating the '
                        'first version'
                    )
                    desired_version = Version.first_version()

        if event.rule_source_id:
            _LOG.debug('Querying rules by rule source')
            rs = cast(RuleSource, rs)
            rules = self.rule_service.get_by_rule_source(
                rule_source=rs, cloud=event.cloud
            )
        else:
            _LOG.debug('Querying all the rules for cloud')
            rules = self.rule_service.get_by_id_index(customer, event.cloud)
        rules = list(self.rule_service.without_duplicates(rules=rules))
        if not rules:
            _LOG.warning('No rules by given parameters were found')
            return build_response(
                code=HTTPStatus.NOT_FOUND, content='No rules found'
            )
        ruleset = self.ruleset_service.create_event_driven(
            version=desired_version.to_str(),
            cloud=event.cloud,
            rules=[rule.name for rule in rules],
        )
        self.upload_ruleset(ruleset, self.build_policy(rules))
        self.ruleset_service.save(ruleset)
        return build_response(
            self.ruleset_service.dto(ruleset, params_to_exclude={RULES_ATTR}),
            code=HTTPStatus.CREATED,
        )

    @validate_kwargs
    def delete_event_driven_ruleset(
        self, event: EventDrivenRulesetDeleteModel
    ):
        _LOG.debug('Delete event-driven rulesets')
        if event.is_all_versions:
            _LOG.debug('Removing all versions of a specific ruleset')
            items = self.ruleset_service.iter_event_driven(cloud=event.cloud)
            with ThreadPoolExecutor() as ex:
                ex.map(self.ruleset_service.delete, items)
        else:
            _LOG.debug('Removing a specific version of a ruleset')
            item = self.ruleset_service.get_event_driven(
                cloud=event.cloud, version=Version(event.version).to_str()
            )
            if item:
                self.ruleset_service.delete(item)
        return build_response(code=HTTPStatus.NO_CONTENT)

    def yield_standard_rulesets(
        self,
        customer: str,
        name: Optional[str] = None,
        version: Optional[str] = None,
        cloud: Optional[str] = None,
    ) -> Generator[Ruleset, None, None]:
        """
        Not to be saved after this method
        :param customer:
        :param name:
        :param version:
        :param cloud:
        :return:
        """
        mapping = {}
        it = self.ruleset_service.iter_standard(
            customer=customer,
            name=name,
            version=version,
            cloud=cloud,
            ascending=False,
            event_driven=False,
        )
        for rs in it:
            if rs.name not in mapping:
                if v := rs.version:
                    rs.versions.append(v)
                rs.version = EMPTY_VERSION
                mapping[rs.name] = rs
            else:
                if v := rs.version:
                    mapping[rs.name].versions.append(v)
        yield from mapping.values()

    def yield_licensed_rulesets(
        self,
        customer: Optional[str] = None,
        name: Optional[str] = None,
        version: Optional[str] = None,
        cloud: Optional[str] = None,
    ) -> Generator[Ruleset, None, None]:
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
            return True

        if not customer:  # SYSTEM
            # TODO probably remove
            yield from self.ruleset_service.iter_licensed(
                name=name, version=version, cloud=cloud
            )
            return
        applications = self.application_service.list(
            customer=customer,
            _type=ApplicationType.CUSTODIAN_LICENSES.value,
            deleted=False,
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
            version=Version(event.version).to_str() if event.version else None,
            cloud=event.cloud,
        )
        _standard = self.yield_standard_rulesets(**params)
        _licensed = self.yield_licensed_rulesets(**params)
        # generators, by here they are not executed
        if not isinstance(event.licensed, bool):
            items = chain(_licensed, _standard)
        elif event.licensed:  # True
            items = _licensed
        else:  # False
            items = _standard

        params_to_exclude = {EVENT_DRIVEN_ATTR}
        if not event.get_rules:
            params_to_exclude.add(RULES_ATTR)
        return build_response(
            content=(
                self.ruleset_service.dto(item, params_to_exclude)
                for item in items
            )
        )

    @staticmethod
    def _filtered_rules(
        rules: list[Rule],
        platforms: set[str],
        categories: set[str],
        service_sections: set[str],
        sources: set[str],
    ) -> Generator[Rule, None, None]:
        for rule in rules:
            comment = RuleIndex(rule.comment)
            name = rule.name
            if platforms and (
                not comment.raw_platform
                or comment.raw_platform.lower() not in platforms
            ):
                _LOG.debug(f'Skipping rule {name}. Platform does not match')
                continue
            if categories and (
                not comment.category
                or comment.category.lower() not in categories
            ):
                _LOG.debug(f'Skipping rule {name}. Category does not match')
                continue
            if service_sections and (
                not comment.service_section
                or comment.service_section.lower() not in service_sections
            ):
                _LOG.debug(
                    f'Skipping rule {name}. Service section does not match'
                )
                continue
            if sources and (
                not comment.source or comment.source.lower() not in sources
            ):
                _LOG.debug(f'Skipping rule {name}. Source does not match')
                continue
            yield rule

    @validate_kwargs
    def create_ruleset(self, event: RulesetPostModel):
        customer = event.customer or SYSTEM_CUSTOMER

        rs: RuleSource | None = None
        desired_version: Version
        if event.rule_source_id:
            rs = self.rule_source_service.get_nullable(event.rule_source_id)
            if not rs or rs.customer != customer:
                raise (
                    ResponseFactory(HTTPStatus.NOT_FOUND)
                    .message(
                        self.rule_source_service.not_found_message(
                            event.rule_source_id
                        )
                    )
                    .exc()
                )

        if event.version:
            _LOG.debug(
                'User specified the version of ruleset '
                'he wants to create. Checking whether we can'
            )
            desired_version = Version(event.version)  # validated
            ruleset = self.ruleset_service.get_standard(
                customer=customer,
                name=event.name,
                version=desired_version.to_str(),
            )
            if ruleset:
                raise (
                    ResponseFactory(HTTPStatus.CONFLICT)
                    .message(
                        f'Ruleset {event.name} {desired_version} already exists'
                    )
                    .exc()
                )
        else:
            _LOG.debug('User did not provide the version.')
            release_version: Version | None = None
            if rs:
                release_version = self.rule_source_service.get_ruleset_version(
                    rs
                )

            if release_version:
                ruleset = self.ruleset_service.get_standard(
                    customer=customer,
                    name=event.name,
                    version=release_version.to_str(),
                )
                if ruleset:
                    raise (
                        ResponseFactory(HTTPStatus.CONFLICT)
                        .message(
                            f'Ruleset {event.name} for rules release '
                            f'{release_version} already exists'
                        )
                        .exc()
                    )
                desired_version = release_version
            else:
                raise (
                    ResponseFactory(HTTPStatus.BAD_REQUEST)
                    .message(
                        'Cannot resolve new ruleset version. Please specify one in format: major.minor.patch'
                    )
                    .exc()
                )
        # The logic above feels a little congested. It just resolves the
        # version for the next ruleset and checks whether this version is
        # allowed

        _LOG.info(f'Resolved version for the next ruleset: {desired_version}')
        _LOG.info('Going to check ruleset cloud')
        latest = self.ruleset_service.get_latest(
            customer=customer, name=event.name
        )
        if latest:
            if latest.cloud != event.cloud:
                raise (
                    ResponseFactory(HTTPStatus.BAD_REQUEST)
                    .message(
                        f'Cannot create a new version of ruleset {event.name} for a different cloud'
                    )
                    .exc()
                )

        _LOG.debug('Collecting the list of rules based on incoming params')
        if event.rules:
            _LOG.info(
                'Concrete rules were provided. '
                'Assembling the ruleset using them'
            )
            rules = []
            for rule_name in event.rules:
                rule = self.rule_service.resolve_rule(
                    customer=customer, name_prefix=rule_name, cloud=event.cloud
                )
                if not rule:
                    raise (
                        ResponseFactory(HTTPStatus.NOT_FOUND)
                        .message(
                            self.rule_service.not_found_message(rule_name)
                        )
                        .exc()
                    )
                rules.append(rule)
            rules = self.rule_service.filter_by(
                rules=rules,
                git_project=event.git_project_id,
                rule_source_id=event.rule_source_id,
                ref=event.git_ref,
            )
        elif event.rule_source_id:
            _LOG.debug('Querying rules by rule source')
            rs = cast(RuleSource, rs)
            rules = self.rule_service.get_by_rule_source(
                rule_source=rs, cloud=event.cloud
            )
        elif event.git_project_id:
            _LOG.debug('Querying rules by location index')
            rules = self.rule_service.get_by(
                customer=customer,
                project=event.git_project_id,
                ref=event.git_ref,
                cloud=event.cloud,
            )
        else:
            _LOG.debug('Querying all the rules for cloud')
            rules = self.rule_service.get_by_id_index(customer, event.cloud)
        _LOG.debug('Removing duplicates')
        rules = list(self.rule_service.without_duplicates(rules=rules))
        if event.excluded_rules:
            _LOG.debug('Removing excluded rules')
            resolver = RuleNamesResolver(
                resolve_from=map(operator.attrgetter('name'), rules)
            )
            resolved = set(resolver.resolved_names(event.excluded_rules))
            rules = [rule for rule in rules if rule.name not in resolved]

        _LOG.debug('Filtering rules by mappings')
        rules = list(
            self._filtered_rules(
                rules=rules,
                platforms=event.platforms,
                categories=event.categories,
                service_sections=event.service_sections,
                sources=event.sources,
            )
        )
        if not rules:
            _LOG.warning('No rules found by filters')
            raise (
                ResponseFactory(HTTPStatus.BAD_REQUEST)
                .message('No rules left after filtering')
                .exc()
            )

        ruleset = self.ruleset_service.create(
            customer=customer,
            name=event.name,
            version=desired_version.to_str(),
            cloud=event.cloud,
            rules=[rule.name for rule in rules],
            event_driven=False,
            licensed=False,
        )
        # TODO: add changelog or metadata from release
        self.upload_ruleset(ruleset, self.build_policy(rules))
        self.ruleset_service.save(ruleset)
        return build_response(
            self.ruleset_service.dto(
                ruleset, params_to_exclude={RULES_ATTR, S3_PATH_ATTR}
            ),
            code=HTTPStatus.CREATED,
        )

    @staticmethod
    def build_policy(rules: list[Rule]) -> dict:
        return {'policies': [rule.build_policy() for rule in rules]}

    def upload_ruleset(self, ruleset: Ruleset, content: dict) -> None:
        """
        Uploads content to s3 and sets s3_path to ruleset item
        :param ruleset:
        :param content:
        :return:
        """
        bucket = self.environment_service.get_rulesets_bucket_name()
        key = self.ruleset_service.build_s3_key(ruleset)
        self.s3_client.gz_put_json(bucket=bucket, key=key, obj=content)
        self.ruleset_service.set_s3_path(ruleset, bucket=bucket, key=key)

    @validate_kwargs
    def update_ruleset(self, event: RulesetPatchModel):
        customer = event.customer or SYSTEM_CUSTOMER
        if not event.force and self.ruleset_service.get_standard(
            customer=customer, name=event.name, version=event.new_version
        ):
            raise (
                ResponseFactory(HTTPStatus.CONFLICT)
                .message(
                    f'Ruleset {event.name} with version {event.new_version} already exists'
                )
                .exc()
            )

        if not event.version:
            _LOG.debug(
                'Ruleset version was not specified. Updating the latest one'
            )
            ruleset = self.ruleset_service.get_latest(
                customer=customer, name=event.name
            )
            if not ruleset:
                raise (
                    ResponseFactory(HTTPStatus.NOT_FOUND)
                    .message(f'There not any rulesets with name {event.name}')
                    .exc()
                )
        else:
            _LOG.debug('Ruleset version was provided. Trying to resolve')
            ruleset = self.ruleset_service.get_standard(
                customer=customer,
                name=event.name,
                version=Version(event.version).to_str(),
            )
            if not ruleset:
                raise (
                    ResponseFactory(HTTPStatus.NOT_FOUND)
                    .message(
                        f'Ruleset {event.name} with version '
                        f'{event.version} not found'
                    )
                    .exc()
                )

        # by here we have the ruleset we want to update
        s3_path = ruleset.s3_path.as_dict()
        if not s3_path:
            # should never happen
            raise (
                ResponseFactory(HTTPStatus.BAD_REQUEST)
                .message('Cannot update an empty ruleset')
                .exc()
            )

        content = self.s3_client.gz_get_json(
            bucket=s3_path['bucket_name'], key=s3_path['path']
        )
        name_body = self._rule_name_to_body(cast(dict, content))
        hash_before = self.ruleset_service.hash_from_name_to_body(name_body)

        # updated versions of needed rules must be present in a new ruleset
        needed_rules = set(ruleset.rules)
        new_name_body = {}

        if event.rules_to_detach:
            resolved = RuleNamesResolver(name_body.keys()).resolved_names(
                event.rules_to_detach
            )
            for to_detach in resolved:
                needed_rules.discard(to_detach)

        # TODO: can be optimized for only one rules query
        for rule in event.rules_to_attach:
            item = self.rule_service.resolve_rule(
                customer=customer, name_prefix=rule, cloud=ruleset.cloud
            )
            if not item:
                raise (
                    ResponseFactory(HTTPStatus.NOT_FOUND)
                    .message(self.rule_service.not_found_message(rule))
                    .exc()
                )
            new_name_body[item.name] = item.build_policy()
            needed_rules.discard(item.name)

        _LOG.debug(f'Querying all the rules for cloud {ruleset.cloud}')
        for rule_item in self.rule_service.get_by_id_index(
            customer, ruleset.cloud
        ):
            if rule_item.name in needed_rules:
                new_name_body[rule_item.name] = rule_item.build_policy()
                needed_rules.discard(rule_item.name)
        if needed_rules:
            _LOG.warning(
                'Some missing rules has left. It means that these rules '
                'were probably removed from the rulesource so the one '
                'ruleset does not contain them after being updated'
            )

        hash_after = self.ruleset_service.hash_from_name_to_body(new_name_body)
        if not event.force and hash_before == hash_after:
            raise (
                ResponseFactory(HTTPStatus.CONFLICT)
                .message(
                    'No changes detected in rules. Use force update if you still want to create a new version'
                )
                .exc()
            )

        ruleset.version = Version(event.new_version).to_str()
        ruleset.rules = list(new_name_body)
        ruleset.created_at = utc_iso()

        self.upload_ruleset(ruleset, {'policies': list(new_name_body.values())})

        self.ruleset_service.save(ruleset)

        return build_response(
            code=HTTPStatus.OK,
            content=self.ruleset_service.dto(
                ruleset, {S3_PATH_ATTR, RULES_ATTR, EVENT_DRIVEN_ATTR}
            ),
        )

    @staticmethod
    def _rule_name_to_body(content: dict) -> dict:
        return {p['name']: p for p in content.get('policies') or ()}

    @validate_kwargs
    def delete_ruleset(self, event: RulesetDeleteModel):
        customer = event.customer or SYSTEM_CUSTOMER

        if event.is_all_versions:
            _LOG.debug('Removing all versions of a specific ruleset')
            items = self.ruleset_service.iter_standard(
                customer=customer, name=event.name
            )
            with ThreadPoolExecutor() as ex:
                ex.map(self.ruleset_service.delete, items)
        else:
            _LOG.debug('Removing a specific version of a ruleset')
            item = self.ruleset_service.get_standard(
                customer=customer,
                name=event.name,
                version=Version(event.version).to_str(),
            )
            if item:
                self.ruleset_service.delete(item)
        return build_response(code=HTTPStatus.NO_CONTENT)

    @validate_kwargs
    def release_ruleset(self, event: RulesetReleasePostModel):
        customer = event.customer or SYSTEM_CUSTOMER
        client = self.license_manager_service.cl
        if not isinstance(client, LMClientAfter3p0):
            raise (
                ResponseFactory(HTTPStatus.NOT_IMPLEMENTED)
                .message(
                    'The linked License Manager does not support this operation'
                )
                .exc()
            )
        if event.is_all_versions:
            rulesets = list(
                self.ruleset_service.iter_standard(
                    customer=customer, name=event.name
                )
            )
        elif event.version:
            ruleset = self.ruleset_service.get_standard(
                customer=customer,
                name=event.name,
                version=Version(event.version).to_str(),
            )
            if not ruleset:
                raise (
                    ResponseFactory(HTTPStatus.NOT_FOUND)
                    .message(
                        f'Ruleset with name {event.name} and '
                        f'version {event.version} not found'
                    )
                    .exc()
                )
            rulesets = [ruleset]
        else:
            ruleset = self.ruleset_service.get_latest(
                customer=customer, name=event.name
            )
            if not ruleset:
                raise (
                    ResponseFactory(HTTPStatus.NOT_FOUND)
                    .message(f'No rulesets with name {event.name} exist')
                    .exc()
                )
            rulesets = [ruleset]
        responses = []
        for rs in rulesets:
            result = client.post_ruleset(
                name=rs.name,
                version=rs.version,
                cloud=rs.cloud,
                description=event.description,
                display_name=event.display_name,
                download_url=self.ruleset_service.download_url(rs),
                rules=rs.rules,
            )
            if not result:
                code = HTTPStatus.SERVICE_UNAVAILABLE
                released = False
                message = 'Problem with the License Manager occurred'
            else:
                code, message = result
                released = code is HTTPStatus.CREATED
            responses.append(
                dict(
                    name=rs.name,
                    version=rs.version,
                    released=released,
                    message=message,
                    code=code,
                )
            )
        if all([item['released'] for item in responses]):
            result_code = HTTPStatus.CREATED
        else:
            result_code = HTTPStatus.MULTI_STATUS
        return build_response(code=result_code, content=responses)
