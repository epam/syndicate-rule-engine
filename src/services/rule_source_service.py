from hashlib import md5
from http import HTTPStatus
from typing import List, Optional, Generator, Iterable, Tuple

from helpers import build_response, list_update
from helpers.constants import GIT_ACCESS_SECRET_ATTR, CUSTOMER_ATTR, \
    ID_ATTR, GIT_PROJECT_ID_ATTR, GIT_RULES_PREFIX_ATTR, STATUS_SYNCING, \
    STATUS_SYNCED, GIT_ACCESS_TYPE_ATTR, LATEST_SYNC_ATTR, \
    RESTRICT_FROM_ATTR, COMMIT_HASH_ATTR, COMMIT_TIME_ATTR, STATUS_ATTR, \
    ALL_ATTR, ALLOWED_FOR_ATTR, RULE_SOURCE_REQUIRED_ATTRS, GIT_REF_ATTR, \
    GIT_URL_ATTR, RULE_SOURCE_ID_ATTR, TYPE_ATTR, STATUS_SYNCING_FAILED
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from models.rule_source import RuleSource
from services.abstract_rule_service import AbstractRuleService
from services.clients.git_service_clients import GitLabClient, GitHubClient
from services.rbac.restriction_service import RestrictionService
from services.ssm_service import SSMService

SSM_SECRET_NAME_TEMPLATE = 'caas.{rule_source_id}.{timestamp}.repo_secret'
STATUS_MESSAGE_UPDATE_EVENT_SUBMITTED = 'Rule update event has been submitted'
STATUS_MESSAGE_UPDATE_EVENT_FORBIDDEN = \
    'Rule source is currently being updated. ' \
    'Rule update event has not been submitted'

RULE_SOURCE_ATTRS_FOR_ID = (
    CUSTOMER_ATTR, GIT_URL_ATTR, GIT_PROJECT_ID_ATTR,
    GIT_REF_ATTR, GIT_RULES_PREFIX_ATTR
)

_LOG = get_logger(__name__)

RuleSourceGenerator = Generator[RuleSource, None, None]


class RuleSourceService(AbstractRuleService):

    def __init__(self, ssm_service: SSMService,
                 restriction_service: RestrictionService):
        super().__init__(restriction_service)
        self.ssm_service = ssm_service

    def create_rule_source(self, rule_source_data: dict) -> RuleSource:
        self._validate_git_access_data(rule_source_data)
        rule_source_id = self.derive_rule_source_id(**rule_source_data)
        rule_source_data[ID_ATTR] = rule_source_id
        git_access_secret = rule_source_data.get(GIT_ACCESS_SECRET_ATTR)
        if git_access_secret:
            _LOG.debug('Access secret was provided. Saving to ssm')
            ssm_secret_name = self._save_ssm_secret(
                rule_source_id=rule_source_id,
                git_access_secret=git_access_secret
            )
            rule_source_data[GIT_ACCESS_SECRET_ATTR] = ssm_secret_name
        return RuleSource(**rule_source_data)

    def update_rule_source(
            self, rule_source: RuleSource,
            rule_source_data: dict
    ) -> RuleSource:

        secret_name = rule_source.git_access_secret
        if not rule_source_data.get(GIT_ACCESS_SECRET_ATTR):
            rule_source.git_access_secret = self.ssm_service.get_secret_value(
                secret_name
            )

        for key, value in rule_source_data.items():
            if not value and key != ALLOWED_FOR_ATTR:
                continue
            setattr(rule_source, key, value)

        _attrs = RULE_SOURCE_REQUIRED_ATTRS.copy()  # pre v4.2.0
        _attrs.remove(GIT_PROJECT_ID_ATTR)
        # _attrs: tuple = RULE_SOURCE_TO_UPDATE_ATTRS.copy()
        if any(bool(rule_source_data.get(key)) for key in _attrs):
            _LOG.debug('Some rule source access attrs changed. '
                       'Validating git access data')
            self._validate_git_access_data(rule_source.get_json())

        if rule_source_data.get(GIT_ACCESS_SECRET_ATTR):
            self.ssm_service.delete_secret(
                secret_name=secret_name
            )
            ssm_secret_name = self._save_ssm_secret(
                rule_source_id=rule_source.id,
                git_access_secret=rule_source.git_access_secret
            )
            rule_source.git_access_secret = ssm_secret_name
        else:
            rule_source.git_access_secret = secret_name
        return rule_source

    @classmethod
    def list_rule_sources(cls, customer: str = None,
                          git_project_id: str = None) -> list:
        if customer:
            iterable = cls.i_query_by_customer(
                customer=customer, git_project_id=git_project_id
            )
        elif git_project_id:
            cnd = RuleSource.git_project_id == git_project_id
            iterable = RuleSource.scan(filter_condition=cnd)
        else:
            iterable = RuleSource.scan()
        return list(iterable)

    @staticmethod
    def get(rule_source_id: str) -> Optional[RuleSource]:
        return RuleSource.get_nullable(hash_key=rule_source_id)

    def iter_by_ids(self, ids: Iterable[str]
                    ) -> Generator[Tuple[RuleSource, Optional[str]], None, None]:
        """
        Iterates over pairs: rule-source, secret
        :param ids:
        :return:
        """
        for _id in ids:
            item = self.get(_id)
            if not item:
                continue
            secret = None
            if item.git_access_secret:
                secret = self.ssm_service.get_secret_value(
                    item.git_access_secret
                )
            yield item, secret

    @classmethod
    def get_rule_source_by_customer(cls, customer: str,
                                    git_project_id: str) -> RuleSource:
        query = cls.i_query_by_customer(customer=customer,
                                        git_project_id=git_project_id)
        return next(query, None)

    @staticmethod
    def i_query_by_customer(
            customer: str, git_project_id: Optional[str] = None,
            limit: Optional[int] = None,
            last_evaluated_key: Optional[str] = None
    ) -> RuleSourceGenerator:
        gpid_attr = RuleSource.git_project_id
        rk = gpid_attr == git_project_id if git_project_id else None
        index = RuleSource.customer_git_project_id_index
        return index.query(
            hash_key=customer, range_key_condition=rk,
            limit=limit, last_evaluated_key=last_evaluated_key
        )

    @staticmethod
    def save_rule_source(rule_source: RuleSource):
        return rule_source.save()

    @staticmethod
    def derive_rule_source_id(**kwargs):
        delimiter = ':'
        encoding = 'utf-8'
        missing_template = "RuleSource identifier missing - '{}'."
        _LOG.info(f'Deriving RuleSource identifier based on {kwargs}.')
        ordered = []
        for attr in RULE_SOURCE_ATTRS_FOR_ID:
            if attr not in kwargs:
                raise ValueError(missing_template.format(attr))
            ordered.append(kwargs[attr])
        string_to_hash = delimiter.join(ordered)
        bytes_to_hash = string_to_hash.encode(encoding)
        return md5(bytes_to_hash).hexdigest()

    @staticmethod
    def derive_updated_tenants(rule_source: RuleSource, tenants: List[str],
                               restrict: bool):
        """
        >= v4.2.0
            Returns updated-only tenants,
            based on pending `allowed_for` ones.
            :param rule_source: RuleSource
            :param tenants: List[str]
            :param restrict: bool
            :return: Optional[List[str]]
        """
        log_head = f'Rule-source:{rule_source.id!r}'
        tenants = set(tenants)
        allowed_for = set(rule_source.allowed_for or [])
        op = allowed_for.difference if restrict else allowed_for.union
        updated = op(tenants)
        if sorted(updated) == sorted(allowed_for):
            _LOG.warning(
                f'{log_head} - `allowed_for`({allowed_for}) has not been updated with {tenants}.')
            return
        else:
            _LOG.info(
                f'{log_head} - `allowed_for` has been updated: {updated}.')
        return list(updated)

    @classmethod
    def is_subject_applicable(cls, rule_source: RuleSource,
                              customer: Optional[str], tenants: List[str]):
        """
        Verifies whether rule-source is applicable to
        subject-scope of a customer and given tenants.
        :param rule_source: RuleSource
        :param customer: Optional[str]
        :param tenants: List[str]
        :return: bool
        """
        is_applicable = True
        log_head = f'Rule-source:{rule_source.id!r}'
        if customer and rule_source.customer != customer:
            related = rule_source.customer
            _LOG.warning(
                f'{log_head} - {customer!r} customer must be {related!r}.')
            is_applicable = False
        elif tenants:
            is_applicable = cls.is_tenant_applicable(rule_source=rule_source,
                                                     tenants=tenants)
        return is_applicable

    @staticmethod
    def is_tenant_applicable(rule_source: RuleSource,
                             tenants: List[str]) -> bool:
        is_applicable = True
        log_head = f'Rule-source:{rule_source.id!r}'
        intersection = set(rule_source.allowed_for) | set(
            tenants) if tenants else {}
        grounds = f"{', '.join(tenants)} tenants"
        if (tenants and intersection) or not tenants:
            _LOG.info(f'{log_head} - is accessible based on given {grounds}.')
        elif not intersection:
            _LOG.warning(f'{log_head} - is not accessible for {grounds}.')
            is_applicable = False

        return is_applicable

    @staticmethod
    def is_allowed_to_sync(rule_source: RuleSource) -> bool:
        _LOG.debug(f'Checking the status of rule_source with with customer '
                   f'\'{rule_source.customer}\' and project_id '
                   f'\'{rule_source.id}\'')
        return rule_source.latest_sync.as_dict().get('current_status') != \
            STATUS_SYNCING

    @staticmethod
    def build_update_event_response(rule_source: RuleSource,
                                    forbidden: bool = False) -> dict:
        message = STATUS_MESSAGE_UPDATE_EVENT_FORBIDDEN if forbidden \
            else STATUS_MESSAGE_UPDATE_EVENT_SUBMITTED
        return {
            RULE_SOURCE_ID_ATTR: rule_source.id,
            CUSTOMER_ATTR: rule_source.customer,
            GIT_PROJECT_ID_ATTR: rule_source.git_project_id,
            STATUS_ATTR: message
        }

    def delete_rule_source(self, rule_source: RuleSource):
        secret_name = rule_source.git_access_secret
        if secret_name:
            self.ssm_service.delete_secret(secret_name=secret_name)
        return rule_source.delete()

    def get_rule_source_dto(self, rule_source: RuleSource) -> dict:
        tenants = self._restriction_service.user_tenants

        rule_source_json = rule_source.get_json()
        rule_source_json.pop(GIT_ACCESS_SECRET_ATTR, None)
        rule_source_json.pop(GIT_ACCESS_TYPE_ATTR, None)
        rule_source_json.pop(RESTRICT_FROM_ATTR, None)
        (rule_source_json.get(LATEST_SYNC_ATTR) or {}).pop(COMMIT_HASH_ATTR,
                                                           None)
        (rule_source_json.get(LATEST_SYNC_ATTR) or {}).pop(COMMIT_TIME_ATTR,
                                                           None)
        rule_source_json[ALLOWED_FOR_ATTR] = [
                                                 tenant for tenant in
                                                 rule_source_json.get(
                                                     ALLOWED_FOR_ATTR) or []
                                                 if
                                                 not tenants or tenant in tenants
                                             ] or ALL_ATTR.upper()
        rule_source_json[TYPE_ATTR] = rule_source.type
        rule_source_json['has_secret'] = rule_source.has_secret
        return rule_source_json

    @staticmethod
    def update_latest_sync(rule_source: RuleSource,
                           current_status: Optional[str] = None,
                           sync_date: Optional[str] = None,
                           commit_hash: Optional[str] = None,
                           commit_time: Optional[str] = None):
        actions = []
        if current_status:
            assert current_status in {STATUS_SYNCING, STATUS_SYNCED,
                                      STATUS_SYNCING_FAILED}
            actions.append(RuleSource.latest_sync.current_status.set(
                current_status))
        if sync_date:
            actions.append(RuleSource.latest_sync.sync_date.set(sync_date))
        if commit_hash:
            actions.append(RuleSource.latest_sync.commit_hash.set(commit_hash))
        if commit_time:
            actions.append(RuleSource.latest_sync.commit_time.set(commit_time))
        if actions:
            rule_source.update(actions=actions)

    def _save_ssm_secret(self, rule_source_id, git_access_secret):
        timestamp = int(utc_datetime().timestamp())
        secret_name = SSM_SECRET_NAME_TEMPLATE.format(
            rule_source_id=rule_source_id,
            timestamp=timestamp
        )
        self.ssm_service.create_secret_value(
            secret_name=secret_name,
            secret_value=git_access_secret
        )
        return secret_name

    @staticmethod
    def _validate_git_access_data(git_access_data):
        project = git_access_data.get(GIT_PROJECT_ID_ATTR)
        url = git_access_data.get(GIT_URL_ATTR)
        secret = git_access_data.get(GIT_ACCESS_SECRET_ATTR)
        is_gitlab = str(project).isdigit()
        if is_gitlab:
            client = GitLabClient(url=url, private_token=secret)
        else:
            client = GitHubClient(url=url)
        pr = client.get_project(project)
        if not pr:
            _LOG.warning(f'Cannot access project: {project}')
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content=f'Cannot access {"GitLab" if is_gitlab else "GitHub"} '
                        f'project {project}'
            )

    @staticmethod
    def expand_systems(system_entities: List[RuleSource],
                       customer_entities: List[RuleSource]
                       ) -> List[RuleSource]:
        """Updates by `id` attribute"""
        return list_update(system_entities, customer_entities, ('id',))
