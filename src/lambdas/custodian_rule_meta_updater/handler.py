import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Generator, Iterable, cast

from pydantic import ValidationError
from ruamel.yaml import YAML, YAMLError, __with_libyaml__

from helpers import RequestContext
from helpers.constants import RuleSourceSyncingStatus, RuleSourceType
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.rule import Rule
from models.rule_source import RuleSource
from services import SERVICE_PROVIDER
from services.abs_lambda import EventProcessorLambdaHandler
from services.clients.git_service_clients import GitHubClient, GitLabClient
from services.rule_meta_service import RuleModel, RuleService
from services.rule_source_service import RuleSourceService

_LOG = get_logger(__name__)

_LOG.debug(f'Using CYaml: {__with_libyaml__}')


class RuleMetaUpdaterLambdaHandler(EventProcessorLambdaHandler):
    processors = ()
    policies_key = 'policies'

    def __init__(
        self, rule_service: RuleService, rule_source_service: RuleSourceService
    ):
        self._rule_service = rule_service
        self._rule_source_service = rule_source_service

    @classmethod
    def build(cls):
        return cls(
            rule_service=SERVICE_PROVIDER.rule_service,
            rule_source_service=SERVICE_PROVIDER.rule_source_service,
        )

    @staticmethod
    def is_yaml(filename: str) -> bool:
        exception_names = ['.gitlab-ci.yml']
        if filename in exception_names:
            return False
        return filename.endswith('.yaml') or filename.endswith('.yml')

    def iter_files(
        self, root: Path
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
                    _LOG.warning(
                        f"Failed to load rule '{file}' " f'content, skipping'
                    )

    def _load_rules(
        self, rule_source: RuleSource, root: Path
    ) -> Generator[Rule, None, None]:
        """
        Iterates over the local folder with rules and loads them to models
        skipping invalid ones
        """
        to_look_up = root / (rule_source.git_rules_prefix or '').strip('/')
        for filepath, content in self.iter_files(to_look_up):
            for policy in content.get(self.policies_key) or []:
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
                    ref=rule_source.latest_sync.release_tag
                    or rule_source.git_ref,
                    **item.model_dump(),
                )

    def _download_rule_source(
        self,
        item: RuleSource,
        client: GitLabClient | GitHubClient,
        buffer: str,
    ) -> Path | None:
        """
        Downloads the repository for the given rule source item using buffer
        as a temp directory. Returns the path to repo root
        """
        match item.type:
            case RuleSourceType.GITLAB:
                root = client.clone_project(
                    project=item.git_project_id,
                    to=Path(buffer),
                    ref=item.git_ref,
                )
            case RuleSourceType.GITHUB:
                root = client.clone_project(
                    project=item.git_project_id,
                    to=Path(buffer),
                    ref=item.git_ref,
                )
            case RuleSourceType.GITHUB_RELEASE:
                client = cast(GitHubClient, client)
                release = client.get_latest_release(item.git_project_id)
                if not release:
                    _LOG.warning(f'Cannot find latest release for rs {item}')
                    return
                self._rule_source_service.update_latest_sync(
                    item=item, release_tag=release['tag_name']
                )
                root = client.download_tarball(
                    url=release['tarball_url'], to=Path(buffer)
                )
        return root

    def pull_rules(self, ids: list[str]):
        for (
            rule_source,
            secret,
        ) in self._rule_source_service.iter_by_ids_with_secrets(ids):
            self._rule_source_service.update_latest_sync(
                rule_source, RuleSourceSyncingStatus.SYNCING
            )
            client = self._rule_source_service.derive_git_client(
                rule_source, secret
            )
            if not client:
                _LOG.warning(
                    f'Cannot derive git client from '
                    f'rule source: {rule_source}'
                )
                self._rule_source_service.update_latest_sync(
                    rule_source, RuleSourceSyncingStatus.FAILED
                )
                continue

            with tempfile.TemporaryDirectory() as folder:
                root = self._download_rule_source(
                    item=rule_source, client=client, buffer=folder
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
                    f'Saving: {len(rules)} for rule-souce: {rule_source.id}'
                )
                self._rule_service.batch_save(rules)
            except Exception as e:
                _LOG.error(
                    f'Unexpected error occurred trying ' f'to save rules: {e}'
                )
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
            project=rule.git_project, filepath=rule.path, ref=rule.ref
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
            project=rule.git_project, filepath=rule.path, ref=rule.ref
        )
        if not meta:
            return
        rule.commit_hash = meta['last_commit_id']

    def expand_with_commit_hash(
        self, rules: Iterable[Rule], client: GitLabClient | GitHubClient
    ):
        """
        Fetches commit info and set to rules
        :param client:
        :param rules:
        :return:
        """
        method = (
            self._gl_add_commit_hash
            if isinstance(client, GitLabClient)
            else self._gh_add_commit_hash
        )
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
        self.pull_rules(ids)
        return build_response()


HANDLER = RuleMetaUpdaterLambdaHandler.build()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
