from concurrent.futures import ThreadPoolExecutor
import dataclasses
from functools import cached_property
import os
from pathlib import Path
import tempfile
from typing import Generator, Iterable, TypedDict, cast

from modular_sdk.commons import DataclassBase
from pydantic import ValidationError
from ruamel.yaml import YAML, YAMLError, __with_libyaml__

from helpers import RequestContext
from helpers.constants import RuleSourceSyncingStatus, RuleSourceType, S3SettingKey
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.rule import Rule
from models.rule_source import RuleSource
from models.setting import Setting
from services import SERVICE_PROVIDER
from services.abs_lambda import EventProcessorLambdaHandler
from services.clients.git_service_clients import GitHubClient, GitLabClient
from services.clients.s3 import S3Client, S3Url
from services.clients.xlsx_standard_parser import parse_standards
from services.mappings_collector import MappingsCollector
from services.rule_meta_service import RuleMetaModel, RuleModel, RuleService
from services.rule_source_service import RuleSourceService
from services.s3_settings_service import S3SettingsService
from services.setting_service import SettingsService
from services.clients.ssm import AbstractSSMClient

_LOG = get_logger('custodian-rule-meta-updater')

_LOG.debug(f'Using CYaml: {__with_libyaml__}')


@dataclasses.dataclass()
class MetaAccess(DataclassBase):
    url: str
    project: str
    ref: str
    secret: str | None


class StandardsSetting(TypedDict):
    source_s3: str
    notation_s3: str
    value: str


class RuleMetaUpdaterLambdaHandler(EventProcessorLambdaHandler):
    processors = ()

    def __init__(self, rule_service: RuleService,
                 rule_source_service: RuleSourceService,
                 settings_service: SettingsService,
                 s3_settings_service: S3SettingsService,
                 ssm: AbstractSSMClient,
                 s3_client: S3Client):
        self._rule_service = rule_service
        self._rule_source_service = rule_source_service
        self._settings_service = settings_service
        self._s3_settings_service = s3_settings_service
        self._ssm = ssm
        self._s3_client = s3_client

    @classmethod
    def build(cls):
        return cls(
            rule_service=SERVICE_PROVIDER.rule_service,
            rule_source_service=SERVICE_PROVIDER.rule_source_service,
            settings_service=SERVICE_PROVIDER.settings_service,
            s3_settings_service=SERVICE_PROVIDER.s3_settings_service,
            ssm=SERVICE_PROVIDER.ssm,
            s3_client=SERVICE_PROVIDER.s3
        )

    def parse_standards(self, setting: Setting) -> dict:
        value: StandardsSetting = setting.value or {}
        source_s3 = value.get('source_s3')
        notation_s3 = value.get('notation_s3')
        if not all((source_s3, notation_s3)):
            _LOG.warning(f'Source and notation are not set. '
                         f'Cannot parse standards for {setting.name}')
            return {}
        source_s3, notation_s3 = S3Url(source_s3), S3Url(notation_s3)
        notation = self._s3_client.get_json(
            bucket=notation_s3.bucket, key=notation_s3.key
        )
        if not notation:
            _LOG.warning(f'Notation not found by path: {notation_s3.url}')
            return {}
        source = self._s3_client.get_object(
            bucket=source_s3.bucket, key=source_s3.key
        )
        if not source:
            _LOG.warning(f'Source not found by path: {source_s3.url}')
            return {}
        return parse_standards(source, cast(dict, notation))

    def update_standards(self):
        _LOG.info('Generating standards')
        aws = self._settings_service.aws_standards_coverage()
        if aws:
            _LOG.debug('Updating standards for AWS')
            res = self.parse_standards(aws)
            self._s3_settings_service.set(
                key=S3SettingKey.AWS_STANDARDS_COVERAGE,
                data=res
            )
        azure = self._settings_service.azure_standards_coverage()
        if azure:
            _LOG.debug('Updating standards for AZURE')
            self._s3_settings_service.set(
                key=S3SettingKey.AZURE_STANDARDS_COVERAGE,
                data=self.parse_standards(azure)
            )
        google = self._settings_service.google_standards_coverage()
        if google:
            _LOG.debug('Updating standards for GOOGLE')
            self._s3_settings_service.set(
                key=S3SettingKey.GOOGLE_STANDARDS_COVERAGE,
                data=self.parse_standards(google)
            )

    def save_mappings(self, collector: MappingsCollector):
        _LOG.debug('Saving mappings to settings')
        # TODO set in threads?
        self._s3_settings_service.set(
            key=S3SettingKey.RULES_TO_STANDARDS,
            data=collector.standard
        )
        self._s3_settings_service.set(
            key=S3SettingKey.RULES_TO_SEVERITY,
            data=collector.severity
        )
        self._s3_settings_service.set(
            key=S3SettingKey.RULES_TO_MITRE,
            data=collector.mitre
        )
        self._s3_settings_service.set(
            key=S3SettingKey.RULES_TO_SERVICE_SECTION,
            data=collector.service_section
        )
        self._s3_settings_service.set(
            key=S3SettingKey.CLOUD_TO_RULES,
            data=collector.cloud_rules
        )
        self._s3_settings_service.set(
            key=S3SettingKey.AWS_EVENTS,
            data=collector.aws_events
        )
        self._s3_settings_service.set(
            key=S3SettingKey.AZURE_EVENTS,
            data=collector.azure_events
        )
        self._s3_settings_service.set(
            key=S3SettingKey.GOOGLE_EVENTS,
            data=collector.google_events
        )
        self._s3_settings_service.set(
            key=S3SettingKey.HUMAN_DATA,
            data=collector.human_data
        )
        self._s3_settings_service.set(
            key=S3SettingKey.RULES_TO_SERVICE,
            data=collector.service
        )
        self._s3_settings_service.set(
            key=S3SettingKey.RULES_TO_CATEGORY,
            data=collector.category
        )
        _LOG.debug('Mappings were saved')

    @staticmethod
    def is_yaml(filename: str) -> bool:
        exception_names = ['.gitlab-ci.yml']
        if filename in exception_names:
            return False
        return filename.endswith('.yaml') or filename.endswith('.yml')

    @staticmethod
    def to_rule_name(filepath: str) -> str:
        """
        To get the rule name from metadata file we should adhere to such
        a protocol:
        filename = path/to/rule/[rule name]_metadata.yml
        >>> RuleMetaUpdaterLambdaHandler.to_rule_name('path/name_metadata.yml')
        name
        >>> RuleMetaUpdaterLambdaHandler.to_rule_name('name_metadata.yml')
        name
        >>> RuleMetaUpdaterLambdaHandler.to_rule_name('name_metadata.yaml')
        name
        >>> RuleMetaUpdaterLambdaHandler.to_rule_name('name.yml')
        name
        >>> RuleMetaUpdaterLambdaHandler.to_rule_name('name.yaml')
        name
        >>> RuleMetaUpdaterLambdaHandler.to_rule_name('name')
        name
        :param filepath:
        :return:
        """
        suffix_to_remove = ['_metadata', '_meta']
        filename = os.path.basename(filepath)
        filename = os.path.splitext(filename)[0]
        for suffix in suffix_to_remove:
            if filename.endswith(suffix):
                filename = filename[:-len(suffix)]
        return filename

    @cached_property
    def metadata_key(self) -> str:
        return 'metadata'

    @cached_property
    def policies_key(self) -> str:
        return 'policies'

    def iter_files(self, root: Path
                   ) -> Generator[tuple[Path, dict], None, None]:
        """
        Walks through the given root folder, looks for yaml files, loads
        them and yields JSONs
        :param root:
        :return: Generator
        """
        yaml = YAML(typ='safe', pure=False)
        # yaml.default_flow_style = False  # for saving, but we only load
        for folder, _, files in os.walk(root):
            for file in files:
                if not self.is_yaml(file):
                    _LOG.debug(f'Skipping: {file} because not yaml')
                    continue
                try:
                    path = Path(folder, file)
                    with open(path, 'rb') as fp:
                        yield path, yaml.load(fp)
                except YAMLError:
                    _LOG.warning(f'Failed to load rule \'{file}\' '
                                 f'content, skipping')

    def get_metadata_data(self) -> list[MetaAccess]:
        # maybe get from another place. It's a temp solution
        secret_name = self._settings_service.rules_metadata_repo_access_data()
        secret_value = self._ssm.get_secret_value(secret_name)
        if not secret_value:
            _LOG.warning('No metas found')
            return []
        if isinstance(secret_value, dict):
            secret_value = [secret_value]
        return [MetaAccess.from_dict(item) for item in secret_value]

    def pull_meta(self):
        _LOG.debug('Pulling rules meta')
        metas = self.get_metadata_data()
        collector = MappingsCollector()
        for meta in metas:
            _LOG.info(f'Pulling meta from {meta.project}{meta.ref}')
            client = GitLabClient(
                url=meta.url,
                private_token=meta.secret
            )
            with tempfile.TemporaryDirectory() as folder:
                root = client.clone_project(
                    meta.project, Path(folder), meta.ref,
                )
                if not root:
                    continue
                for filename, content in self.iter_files(root):
                    try:
                        rule_name = self.to_rule_name(str(filename))
                        metadata = content.get(self.metadata_key, {})
                        item = RuleMetaModel(
                            name=rule_name,
                            **metadata
                        )
                        _LOG.debug(
                            f'Adding meta {item.name}:{item.version}')
                        collector.add_meta(item)
                    except ValidationError as e:
                        _LOG.warning(f'Invalid meta: {content}, {e}')
                        continue
        self.save_mappings(collector)

    def _load_rules(self, rule_source: RuleSource, root: Path
                    ) -> Generator[Rule, None, None]:
        """
        Iterates over the local folder with rules and loads them to models
        skipping invalid ones
        """
        to_look_up = root / (rule_source.git_rules_prefix or '').strip('/')
        for filepath, content in self.iter_files(to_look_up):
            for policy in (content.get(self.policies_key) or []):
                try:
                    item = RuleModel(**policy)
                except ValidationError as e:
                    _LOG.warning(f'Invalid rule: {content}, {e}')
                    continue
                yield self._rule_service.create(
                    customer=rule_source.customer,
                    rule_source_id=rule_source.id,
                    cloud=item.cloud.value,
                    path=str(filepath.relative_to(root)),
                    git_project=rule_source.git_project_id,
                    ref=rule_source.latest_sync.release_tag or rule_source.git_ref,
                    **item.model_dump()
                )

    def _download_rule_source(self, item: RuleSource,
                              client: GitLabClient | GitHubClient,
                              buffer: str) -> Path | None:
        """
        Downloads the repository for the given rule source item using buffer
        as a temp directory. Returns the path to repo root
        """
        match item.type:
            case RuleSourceType.GITLAB:
                root = client.clone_project(
                    project=item.git_project_id,
                    to=Path(buffer),
                    ref=item.git_ref
                )
            case RuleSourceType.GITHUB:
                root = client.clone_project(
                    project=item.git_project_id,
                    to=Path(buffer),
                    ref=item.git_ref
                )
            case RuleSourceType.GITHUB_RELEASE:
                client = cast(GitHubClient, client)
                release = client.get_latest_release(item.git_project_id)
                if not release:
                    _LOG.warning(f'Cannot find latest release for rs {item}')
                    return
                self._rule_source_service.update_latest_sync(
                    item=item,
                    release_tag=release['tag_name']
                )
                root = client.download_tarball(url=release['tarball_url'],
                                               to=Path(buffer))
            case _:
                root = None
        return root

    def pull_rules(self, ids: list[str]):
        for rule_source, secret in self._rule_source_service.iter_by_ids_with_secrets(ids):
            self._rule_source_service.update_latest_sync(
                rule_source, RuleSourceSyncingStatus.SYNCING
            )
            client = self._rule_source_service.derive_git_client(rule_source, secret)
            if not client:
                _LOG.warning(f'Cannot derive git client from '
                             f'rule source: {rule_source}')
                self._rule_source_service.update_latest_sync(
                    rule_source, RuleSourceSyncingStatus.FAILED
                )
                continue

            with tempfile.TemporaryDirectory() as folder:
                root = self._download_rule_source(
                    item=rule_source,
                    client=client,
                    buffer=folder
                )
                if not root:
                    _LOG.warning('Could not clone repo')
                    self._rule_source_service.update_latest_sync(
                        rule_source, RuleSourceSyncingStatus.FAILED
                    )
                    continue
                rules = list(self._load_rules(rule_source, root))

            # because otherwise we cannot detect whether some rules were
            # removed from GitHub
            _LOG.debug('Removing old versions of rules')
            cursor = self._rule_service.get_by_rule_source(rule_source)
            self._rule_service.batch_delete(cursor)

            try:
                _LOG.info('Going to query git blame for rules')
                self.expand_with_commit_hash(rules, client)
                _LOG.info(
                    f'Saving: {len(rules)} for rule-souce: {rule_source.id}')
                self._rule_service.batch_save(rules)
            except Exception as e:
                _LOG.error(f'Unexpected error occurred trying '
                           f'to save rules: {e}')
                self._rule_source_service.update_latest_sync(
                    rule_source, RuleSourceSyncingStatus.FAILED
                )
            else:
                self._rule_source_service.update_latest_sync(
                    rule_source, RuleSourceSyncingStatus.SYNCED, utc_iso()
                )

    @staticmethod
    def _gh_add_commit_hash(rule: Rule, client: GitHubClient) -> None:
        """
        Changes the given rule instance
        :param rule:
        :param client:
        :return:
        """
        if not client.has_token:
            return  # we can receive git blame only by graphql with token
        _LOG.debug(f'Requesting GitHub blame for file: {rule.path}')
        blames = client.get_file_blame(
            project=rule.git_project,
            filepath=rule.path,
            ref=rule.ref
        )
        if not blames:
            # at least one blame must be for each file. No blame
            # possible only in case filename is invalid, but still
            return
        recent = client.most_reset_blame(blames)
        rule.commit_hash = recent['commit']['oid']
        rule.updated_date = recent['commit']['committedDate']

    @staticmethod
    def _gl_add_commit_hash(rule: Rule, client: GitLabClient) -> None:
        """
        Changes the given rule instance
        :param rule:
        :param client:
        :return:
        """
        _LOG.debug(f'Requesting GitLab blame for file: {rule.path}')
        meta = client.get_file_meta(
            project=rule.git_project,
            filepath=rule.path,
            ref=rule.ref
        )
        if not meta:
            return
        rule.commit_hash = meta['last_commit_id']

    def expand_with_commit_hash(self, rules: Iterable[Rule],
                                client: GitLabClient | GitHubClient):
        """
        Fetches commit info and set to rules
        :param client:
        :param rules:
        :return:
        """
        method = self._gl_add_commit_hash \
            if isinstance(client, GitLabClient) else self._gh_add_commit_hash
        with ThreadPoolExecutor() as executor:
            for rule in rules:
                executor.submit(method, rule, client)

    def handle_request(self, event: dict, context: RequestContext):
        """
        Receives an event from EventBridge cron rule. By default -> the event
        indicates that rules meta should be updated.
        If list of rule-source ids, pull them. If
        :param event:
        :param context:
        :return:
        """
        ids = event.get('rule_source_ids') or []
        action = event.get('action')
        if action == 'standards':
            self.update_standards()
        elif action == 'mappings':
            self.pull_meta()
        elif ids:
            _LOG.debug(f'Pulling rules for ids: {ids}')
            self.pull_rules(ids)
        else:
            _LOG.debug('Pulling mappings - default')
            self.pull_meta()

        return build_response()


HANDLER = RuleMetaUpdaterLambdaHandler.build()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
