from itertools import chain
from typing import Iterable, TypedDict

from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


class PolicyDict(TypedDict):
    name: str
    resource: str
    filters: list
    description: str | None
    comment: str | None


class PoliciesService:
    __slots__ = ()

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

    def get_policies(self, lists: Iterable[list[dict]],
                     keep: set[str] | None = None,
                     exclude: set[str] | None = None) -> list[PolicyDict]:
        """
        Downloads multiple files with policies and merges them into one tuple
        of policies.
        :param lists:
        :param keep:
        :param exclude:
        :return:
        """
        policies = chain.from_iterable(lists)
        if exclude:
            policies = self.iter_excluding(policies, exclude)
        if keep:
            policies = self.iter_keeping(policies, keep)
        policies = self.without_duplicates(policies)
        return list(policies)
