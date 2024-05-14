from hashlib import md5
from http import HTTPStatus
from typing import Generator, Iterable, Optional

from pynamodb.pagination import ResultIterator

from helpers.constants import (
    COMMIT_HASH_ATTR,
    COMMIT_TIME_ATTR,
    CUSTOMER_ATTR,
    GIT_ACCESS_SECRET_ATTR,
    GIT_ACCESS_TYPE_ATTR,
    GIT_PROJECT_ID_ATTR,
    LATEST_SYNC_ATTR,
    RESTRICT_FROM_ATTR,
    RULE_SOURCE_ID_ATTR,
    STATUS_ATTR,
    STATUS_SYNCED,
    STATUS_SYNCING,
    STATUS_SYNCING_FAILED,
    TYPE_ATTR,
)
from helpers.lambda_response import ResponseFactory
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from models.rule_source import RuleSource
from services.clients.git_service_clients import GitHubClient, GitLabClient
from services.ssm_service import SSMService

SSM_SECRET_NAME_TEMPLATE = 'caas.{rule_source_id}.{timestamp}.repo_secret'
STATUS_MESSAGE_UPDATE_EVENT_SUBMITTED = 'Rule update event has been submitted'
STATUS_MESSAGE_UPDATE_EVENT_FORBIDDEN = \
    'Rule source is currently being updated. ' \
    'Rule update event has not been submitted'


_LOG = get_logger(__name__)



class RuleSourceService:

    def __init__(self, ssm_service: SSMService):
        self.ssm_service = ssm_service

    def create_rule_source(self, git_project_id: str, git_url: str,
                           git_ref: str, git_rules_prefix: str,
                           git_access_type: str, customer: str,
                           description: str | None = None,
                           git_access_secret: str | None = None) -> RuleSource:
        rule_source_id = self.derive_rule_source_id(
            customer=customer,
            git_url=git_url,
            git_project_id=git_project_id,
            git_ref=git_ref,
            git_rules_prefix=git_rules_prefix
        )
        item = RuleSource(
            id=rule_source_id,
            customer=customer,
            git_project_id=git_project_id,
            git_url=git_url,
            git_access_type=git_access_type,
            git_rules_prefix=git_rules_prefix,
            git_ref=git_ref,
            description=description
        )
        if git_access_secret:
            _LOG.debug('Access secret was provided. Saving to ssm')
            self.set_secret(item, git_access_secret)
        return item

    def set_secret(self, rule_source: RuleSource, git_access_secret: str):
        if rule_source.git_access_secret:
            self.ssm_service.delete_secret(rule_source.git_access_secret)
        ssm_secret_name = self._save_ssm_secret(
            rule_source_id=rule_source.id,
            git_access_secret=git_access_secret
        )
        rule_source.git_access_secret = ssm_secret_name

    @classmethod
    def list_rule_sources(cls, customer: str | None = None,
                          git_project_id: str | None = None) -> list:
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
                    ) -> Generator[tuple[RuleSource, Optional[str]], None, None]:
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

    @staticmethod
    def i_query_by_customer(
            customer: str, git_project_id: Optional[str] = None,
            limit: Optional[int] = None,
            last_evaluated_key: Optional[str] = None
    ) -> ResultIterator[RuleSource]:
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
    def derive_rule_source_id(customer: str, git_url: str,
                              git_project_id: str, git_ref: str,
                              git_rules_prefix: str) -> str:
        string_to_hash = ':'.join((
            customer, git_url, git_project_id, git_ref, git_rules_prefix
        ))
        return md5(string_to_hash.encode('utf-8')).hexdigest()

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

    @staticmethod
    def get_rule_source_dto(rule_source: RuleSource) -> dict:
        rule_source_json = rule_source.get_json()
        rule_source_json.pop(GIT_ACCESS_SECRET_ATTR, None)
        rule_source_json.pop(GIT_ACCESS_TYPE_ATTR, None)
        rule_source_json.pop(RESTRICT_FROM_ATTR, None)
        (rule_source_json.get(LATEST_SYNC_ATTR) or {}).pop(COMMIT_HASH_ATTR,
                                                           None)
        (rule_source_json.get(LATEST_SYNC_ATTR) or {}).pop(COMMIT_TIME_ATTR,
                                                           None)
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
    def validate_git_access_data(git_project_id: str, git_url: str,
                                 git_access_secret: str | None = None):
        is_gitlab = str(git_project_id).isdigit()
        if is_gitlab:
            client = GitLabClient(url=git_url, private_token=git_access_secret)
        else:
            client = GitHubClient(url=git_url)
        pr = client.get_project(git_project_id)
        if not pr:
            _LOG.warning(f'Cannot access project: {git_project_id}')
            raise ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE).message(
                f'Cannot access {"GitLab" if is_gitlab else "GitHub"} '
                f'project {git_project_id}'
            ).exc()
