from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from typing import Iterable, TypedDict, Generator

import msgspec

from executor.services.environment_service import BatchEnvironmentService
from helpers import download_url
from helpers.log_helper import get_logger
from models.job import Job
from models.ruleset import Ruleset
from services.ruleset_service import RulesetService, RulesetName

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

    def get_standard_rulesets(self, job: Job) -> Generator[
        Ruleset, None, None]:
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
                lists.append(
                    decoder.decode(fp.getvalue()).get('policies') or ())
        policies = chain.from_iterable(lists)
        if exclude:
            policies = self.iter_excluding(policies, exclude)
        if keep:
            policies = self.iter_keeping(policies, keep)
        policies = self.without_duplicates(policies)
        return list(policies)
