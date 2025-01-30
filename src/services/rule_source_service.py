import hashlib
import uuid
from http import HTTPStatus
from typing import Any, Generator, Iterable, cast

from modular_sdk.models.pynamongo.convertors import instance_as_dict
from pynamodb.pagination import ResultIterator

from helpers.constants import (
    COMMIT_HASH_ATTR,
    COMMIT_TIME_ATTR,
    GIT_ACCESS_SECRET_ATTR,
    GIT_ACCESS_TYPE_ATTR,
    LATEST_SYNC_ATTR,
    TYPE_ATTR,
    RuleSourceSyncingStatus,
    RuleSourceType,
)
from helpers import Version
from helpers.lambda_response import ResponseFactory
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from models.rule_source import RuleSource
from services.base_data_service import BaseDataService
from services.clients.git_service_clients import GitHubClient, GitLabClient
from services.clients.ssm import AbstractSSMClient

_LOG = get_logger(__name__)


class RuleSourceService(BaseDataService[RuleSource]):
    def __init__(self, ssm: AbstractSSMClient):
        super().__init__()
        self._ssm = ssm

    def get_nullable(self, id: str) -> RuleSource | None:
        return super().get_nullable(hash_key=id)

    def dto(self, item: RuleSource) -> dict[str, Any]:
        data = instance_as_dict(item)
        data.pop('type_', None)
        data.pop('restrict_from', None)
        data.pop('allowed_for', None)
        data.pop(GIT_ACCESS_SECRET_ATTR, None)
        data.pop(GIT_ACCESS_TYPE_ATTR, None)
        (data.get(LATEST_SYNC_ATTR) or {}).pop(COMMIT_HASH_ATTR, None)
        (data.get(LATEST_SYNC_ATTR) or {}).pop(COMMIT_TIME_ATTR, None)
        (data.get(LATEST_SYNC_ATTR) or {}).pop('cc_version', None)
        data[TYPE_ATTR] = item.type
        data['has_secret'] = item.has_secret
        return data

    def query(
        self,
        customer: str,
        project_id: str | None = None,
        limit: int | None = None,
        last_evaluated_key: dict | None = None,
        has_secret: bool | None = None,
    ) -> ResultIterator[RuleSource]:
        rkc = None
        if project_id:
            rkc = RuleSource.git_project_id == project_id
        fc = None
        if isinstance(has_secret, bool):
            if has_secret:
                fc = RuleSource.git_access_secret.exists()
            else:
                fc = RuleSource.git_access_secret.does_not_exist()
        return RuleSource.customer_git_project_id_index.query(
            hash_key=customer,
            range_key_condition=rkc,
            filter_condition=fc,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            scan_index_forward=True,
        )

    def delete(self, item: RuleSource):
        name = item.git_access_secret
        if name:
            self._ssm.delete_parameter(secret_name=name)
        return super().delete(item)

    def iter_by_ids(
        self, ids: Iterable[str]
    ) -> Generator[RuleSource, None, None]:
        """
        Iterates over pairs: rule-source, secret
        :param ids:
        :return:
        """
        for _id in ids:
            item = self.get_nullable(_id)
            if not item:
                continue
            yield item

    def get_secret(self, item: RuleSource) -> str | None:
        name = item.git_access_secret
        if not name:
            return
        return self._ssm.get_secret_value(name)

    @staticmethod
    def build_ssm_secret_name(item: RuleSource) -> str:
        ts = int(utc_datetime().timestamp())
        return f'caas.{item.id}.{ts}.repo_secret'

    def set_secret(self, item: RuleSource, secret: str):
        if item.git_access_secret:
            self._ssm.delete_parameter(item.git_access_secret)

        name = self.build_ssm_secret_name(item)
        self._ssm.create_secret(secret_name=name, secret_value=secret)
        item.git_access_secret = name

    def iter_by_ids_with_secrets(
        self, ids: Iterable[str]
    ) -> Generator[tuple[RuleSource, str | None], None, None]:
        for item in self.iter_by_ids(ids):
            yield item, self.get_secret(item)

    @staticmethod
    def generate_id(
        customer: str,
        git_project_id: str,
        type_: RuleSourceType,
        git_url: str,
        git_ref: str,
        git_rules_prefix: str,
    ) -> str:
        """
        Generates deterministic uuid based on rule source attributes in order
        to eliminate duplicates
        :param customer:
        :param git_project_id:
        :param type_:
        :param git_url: domain with schema, validated using pydantic
        :param git_ref:
        :param git_rules_prefix:
        :return:
        """
        s = '#'.join(
            (
                customer.strip(),
                git_project_id.strip().strip('/'),
                type_.value,
                git_url.removeprefix('http://')
                .removeprefix('https://')
                .strip()
                .strip('/'),
                git_ref.strip(),
                git_rules_prefix.strip().strip('/'),
            )
        )
        return str(uuid.UUID(hashlib.md5(s.encode('utf-8')).hexdigest()))

    def set_id(self, item: RuleSource) -> None:
        item.id = self.generate_id(
            customer=item.customer,
            git_project_id=item.git_project_id,
            type_=item.type,
            git_url=item.git_url,
            git_ref=item.git_ref,
            git_rules_prefix=item.git_rules_prefix,
        )

    def create(
        self,
        git_project_id: str,
        type_: RuleSourceType,
        git_url: str,
        git_ref: str,
        git_rules_prefix: str,
        customer: str,
        description: str,
    ) -> RuleSource:
        item = RuleSource(
            customer=customer,
            git_project_id=git_project_id,
            git_url=git_url,
            git_ref=git_ref,
            description=description,
            type_=type_.value,
            git_rules_prefix=git_rules_prefix,
        )
        self.set_id(item)
        return item

    def validate_git_access_data(
        self, item: RuleSource, secret: str | None = None
    ):
        project_id = item.git_project_id
        client = self.derive_git_client(item, secret)
        pr = client.get_project(project_id)
        if not pr:
            _LOG.warning(f'Cannot access project: {project_id}')
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(
                    f'Cannot access {"GitLab" if item.type is RuleSourceType.GITLAB else "GitHub"} '
                    f'project {project_id}'
                )
                .exc()
            )

    @staticmethod
    def is_allowed_to_sync(item: RuleSource) -> bool:
        return (
            item.latest_sync.as_dict().get('current_status')
            != RuleSourceSyncingStatus.SYNCING
        )

    @staticmethod
    def update_latest_sync(
        item: RuleSource,
        current_status: RuleSourceSyncingStatus | None = None,
        sync_date: str | None = None,
        commit_hash: str | None = None,
        commit_time: str | None = None,
        release_tag: str | None = None,
        version: str | None = None,
        cc_version: str | None = None,
    ):
        actions = []
        if current_status:
            actions.append(
                RuleSource.latest_sync.current_status.set(current_status.value)
            )
        if sync_date:
            actions.append(RuleSource.latest_sync.sync_date.set(sync_date))
        if commit_hash:
            actions.append(RuleSource.latest_sync.commit_hash.set(commit_hash))
        if commit_time:
            actions.append(RuleSource.latest_sync.commit_time.set(commit_time))
        if release_tag:
            actions.append(RuleSource.latest_sync.release_tag.set(release_tag))
        if version:
            actions.append(RuleSource.latest_sync.version.set(version))
        if cc_version:
            actions.append(RuleSource.latest_sync.cc_version.set(cc_version))
        if actions:
            item.update(actions=actions)

    @staticmethod
    def derive_git_client(
        item: RuleSource, secret: str | None
    ) -> GitHubClient | GitLabClient | None:
        match item.type:
            case RuleSourceType.GITLAB:
                return GitLabClient(url=item.git_url, private_token=secret)
            case RuleSourceType.GITHUB | RuleSourceType.GITHUB_RELEASE:
                return GitHubClient(url=item.git_url, private_token=secret)

    @staticmethod
    def get_ruleset_version(item: RuleSource) -> Version | None:
        """
        All GITHUB_RELEASE rulesources will have release tag. Other
        rulesources may have version and cannot have release tag
        """

        version = item.release_tag or item.version or None
        if version:
            try:
                return Version(version)
            except ValueError:
                _LOG.exception(
                    'Cannot parse release tag or version of rulesource'
                )
