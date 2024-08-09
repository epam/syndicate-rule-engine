import hashlib
import tempfile
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from pathlib import Path
from typing import Iterable, TypedDict, Generator

import msgspec

from executor.services.environment_service import BatchEnvironmentService
from helpers import download_url
from helpers.constants import Cloud
from helpers.log_helper import get_logger
from models.ruleset import Ruleset
from services.ruleset_service import RulesetService, RulesetName
from models.job import Job

_LOG = get_logger(__name__)


class PolicyDict(TypedDict):
    name: str
    resource: str
    filters: list
    description: str | None
    comment: str | None


class PoliciesService:
    __slots__ = '_ruleset_service', '_environment_service'

    def __init__(self, ruleset_service: RulesetService,
                 environment_service: BatchEnvironmentService):
        self._ruleset_service = ruleset_service
        self._environment_service = environment_service

    def get_standard_rulesets(self, job: Job) -> Generator[Ruleset, None, None]:
        for r in map(RulesetName, job.rulesets):
            if r.license_key:
                continue
            item = self._ruleset_service.get_standard(
                customer=job.customer_name,
                name=r.name,
                version=r.version.to_str()
            )
            if not item:
                continue
            yield item

    @staticmethod
    def iter_excluding(policies: Iterable[PolicyDict], exclude: set[str]
                       ) -> Iterable[PolicyDict]:
        def check(p: PolicyDict):
            return p['name'] not in exclude

        return filter(check, policies)

    @staticmethod
    def iter_keeping(policies: Iterable[PolicyDict], keep: set[str]
                     ) -> Iterable[PolicyDict]:
        def check(p: PolicyDict):
            if not keep:
                return True
            return p['name'] in keep

        return filter(check, policies)

    @staticmethod
    def without_duplicates(policies: Iterable[PolicyDict]
                           ) -> Iterable[PolicyDict]:
        duplicated = set()
        for p in policies:
            name = p['name']
            if name in duplicated:
                _LOG.debug(f'Duplicated policy found {name}. Skipping')
                continue
            duplicated.add(name)
            yield p

    def get_policies(self, urls: Iterable[str], keep: set[str] | None = None,
                     exclude: set[str] | None = None) -> list[PolicyDict]:
        """
        Downloads multiple files with policies and merges them into one tuple
        of policies.
        :param urls:
        :param keep:
        :param exclude:
        :return:
        """
        lists = []
        decoder = msgspec.json.Decoder(type=dict)
        with ThreadPoolExecutor() as ex:
            for fp in ex.map(download_url, urls):
                lists.append(decoder.decode(fp.getvalue()).get('policies') or ())
        policies = chain.from_iterable(lists)
        if exclude:
            policies = self.iter_excluding(policies, exclude)
        if keep:
            policies = self.iter_keeping(policies, keep)
        policies = self.without_duplicates(policies)
        return list(policies)

    def ensure_event_driven_ruleset(self, cloud: Cloud) -> Path:
        """
        For event-driven scans we use full system event-driven rulesets
        created beforehand. But only rules that are allowed for tenant
        are will be loaded.
        Returns local path to event-driven ruleset loading it in case
        it has not been loaded yet
        """
        fn = hashlib.sha1(cloud.value.encode()).hexdigest()
        path = Path(tempfile.gettempdir(), f'{fn}.json')
        if path.exists():
            _LOG.info(f'Event-driven ruleset for cloud {cloud} has already '
                      f'been downloaded. Returning path to it.')
            return path

        ruleset = self._ruleset_service.get_latest_event_driven(cloud)
        if ruleset:
            with open(path, 'wb') as file:
                self._ruleset_service.download(ruleset, file)
        else:
            _LOG.warning(f'Event-driven ruleset item for cloud {cloud} not '
                         f'found in DB. Creating an empty one')
            with open(path, 'wb') as file:
                file.write(b'{"policies": []}')
        return path

    def separate_ruleset(self, from_: Path,
                         keep: set[str] | None = None,
                         exclude: set[str] | None = None) -> list[PolicyDict]:
        """
        Creates new ruleset file in work_dir filtering the ruleset
        in `from_` variable (keeping and excluding specific rules).
        This is done in order to reduce the size of rule-sets for event-driven
        scans before they are loaded by Custom-Core.
        """
        with open(from_, 'rb') as file:
            policies = msgspec.json.decode(file.read()).get('policies') or ()
        if exclude:
            policies = self.iter_excluding(policies, exclude)
        if keep:
            policies = self.iter_keeping(policies, keep)
        policies = self.without_duplicates(policies)
        return list(policies)
