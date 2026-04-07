"""
Cloud Custodian policy loader. Loads and prepares policy objects for execution.
"""

import os
from collections import defaultdict
from itertools import chain
from pathlib import Path
from typing import Callable, Generator

from botocore.exceptions import ClientError
from c7n.config import Config
from c7n.exceptions import PolicyValidationError
from c7n.policy import Policy, PolicyCollection
from c7n.provider import clouds
from c7n.resources import load_resources
from c7n_kube.query import DescribeSource, sources

from executor.helpers.constants import AWS_DEFAULT_REGION
from executor.job.types import PolicyDict
from helpers.constants import Cloud, GLOBAL_REGION
from helpers.log_helper import get_logger
from helpers.regions import AWS_REGIONS
from models.rule import RuleIndex

from modular_sdk.commons.constants import ENV_KUBECONFIG

_LOG = get_logger(__name__)


class PoliciesLoader:
    __slots__ = (
        '_cloud',
        '_output_dir',
        '_regions',
        '_cache',
        '_cache_period',
        '_load_global',
    )

    def __init__(
        self,
        cloud: Cloud,
        output_dir: Path | None = None,
        regions: set[str] | None = None,
        cache: str | None = 'memory',
        cache_period: int = 30,
    ):
        """
        :param cloud:
        :param output_dir:
        :param regions:
        :param cache:
        :param cache_period:
        """
        self._cloud = cloud
        self._output_dir = output_dir
        self._regions = regions or set()
        if self._cloud != Cloud.AWS and self._regions:
            _LOG.warning(
                f'Given regions will be ignored because the cloud is '
                f'{self._cloud}'
            )
        self._cache = cache
        self._cache_period = cache_period
        self._load_global = not self._regions or GLOBAL_REGION in self._regions

    @staticmethod
    def cc_provider_name(cloud: Cloud) -> str:
        match cloud:
            case Cloud.GOOGLE | Cloud.GCP:
                return 'gcp'
            case Cloud.KUBERNETES | Cloud.K8S:
                return 'k8s'
            case _:
                return cloud.value.lower()

    def set_global_output(self, policy: Policy) -> None:
        if self._output_dir:
            policy.options.output_dir = str(
                (self._output_dir / GLOBAL_REGION).resolve()
            )

    def set_regional_output(self, policy: Policy) -> None:
        if self._output_dir:
            policy.options.output_dir = str(
                (self._output_dir / policy.options.region).resolve()
            )

    @staticmethod
    def is_global(policy: Policy) -> bool:
        """
        This "is_global" means not whether the resource itself is global but
        rather whether it's enough for us to execute such policy only once
        independently on a region. Thus, it's always ok to execute AZURE and
        GCP policies only once and for AWS we should consider some logic
        :param policy:
        :return:
        """
        if policy.provider_name != 'aws':
            return True

        if comment := policy.data.get('comment'):
            return RuleIndex(comment).is_global
        rt = policy.resource_manager.resource_type
        return rt.global_resource or rt.service == 's3'

    @staticmethod
    def get_policy_region(policy: Policy) -> str:
        if PoliciesLoader.is_global(policy):
            return GLOBAL_REGION
        return policy.options.region

    @property
    def _kubeconfig_path(self) -> str | None:
        return os.getenv(ENV_KUBECONFIG)

    def _base_config(self) -> Config:
        match self._cloud:
            case Cloud.AWS:
                regions = ['all']
            case Cloud.AZURE:
                regions = ['AzureCloud']
            case _:
                regions = []
        return Config.empty(
            regions=regions,
            cache=self._cache,
            cache_period=self._cache_period,
            command='c7n.commands.run',
            config=None,
            configs=[],
            output_dir=str(self._output_dir) if self._output_dir else '',
            subparser='run',
            policy_filters=[],
            resource_types=[],
            verbose=None,
            quiet=False,
            debug=False,
            skip_validation=False,
            vars=None,
            log_group='null',
            tracer='default',
        )

    @staticmethod
    def _kube_session_factory(kubeconfig_path: str) -> Callable:
        """Returns a factory function that creates Session with config_file"""
        from c7n_kube.client import Session

        _LOG.debug('Creating Kubernetes session factory')

        def factory():
            return Session(config_file=kubeconfig_path)

        return factory

    def _session_factory(self) -> Callable | None:
        """Returns a session factory callable or None"""
        session_factory = None
        config_path = self._kubeconfig_path
        if self._cloud == Cloud.KUBERNETES and config_path:
            session_factory = self._kube_session_factory(config_path)

        return session_factory

    @staticmethod
    def _get_resource_types(policies: list[PolicyDict]) -> set[str]:
        res = set()
        for pol in policies:
            rtype = pol['resource']
            if isinstance(rtype, list):
                res.update(rtype)
            elif '.' not in rtype:
                rtype = f'aws.{rtype}'
            res.add(rtype)
        return res

    def prepare_policies(
        self, policies: list[Policy]
    ) -> Generator[Policy, None, None]:
        """
        - aws.account: global, loaded once
        - aws.distribution: global, loaded once
        ...
        Cloud Custodian automatically knows global resource types. Except s3
        which we treat as global - execute all s3 rules only once.
        """
        global_yielded = set()
        n_global, n_not_global = 0, 0
        for policy in policies:
            if self.is_global(policy):
                if not self._load_global:
                    continue
                if policy.name in global_yielded:
                    continue
                _LOG.debug(f'Global policy found: {policy.name}')
                self.set_global_output(policy)
                policy.options.region = AWS_DEFAULT_REGION
                policy.session_factory.region = AWS_DEFAULT_REGION
                global_yielded.add(policy.name)
                n_global += 1
            else:
                if (
                    self._regions
                    and policy.options.region not in self._regions
                ):
                    continue
                _LOG.debug(f'Not global policy found: {policy.name}')
                n_not_global += 1
            yield policy
        _LOG.debug(f'Global policies: {n_global}')
        _LOG.debug(f'Not global policies: {n_not_global}')

    @staticmethod
    def _load_provider_aws(
        policies: list['Policy'], options: Config
    ) -> 'PolicyCollection':
        provider = clouds['aws']()
        p_options = provider.initialize(options)
        try:
            return provider.initialize_policies(
                PolicyCollection(policies, p_options), p_options
            )
        except ClientError:
            _LOG.warning(
                'Error initializing policies, probably cannot describe regions. '
                'Trying again with specific regions',
                exc_info=True,
            )
            p_options.regions = sorted(AWS_REGIONS)
            return provider.initialize_policies(
                PolicyCollection(policies, p_options), p_options
            )

    @staticmethod
    def _load_provider(
        provider_name: str, policies: list['Policy'], options: Config
    ) -> 'PolicyCollection':
        provider = clouds[provider_name]()
        p_options = provider.initialize(options)
        return provider.initialize_policies(
            PolicyCollection(policies, p_options), p_options
        )

    def _load(
        self, policies: list[PolicyDict], options: Config | None = None
    ) -> list[Policy]:
        if not policies:
            return []

        if not options:
            options = self._base_config()
        options.region = ''
        load_resources(self._get_resource_types(policies))
        provider_policies = defaultdict(list)
        session_factory = self._session_factory()
        for policy in policies:
            try:
                pol = Policy(
                    data=policy,
                    options=options,
                    session_factory=session_factory,
                )
            except PolicyValidationError:
                _LOG.warning(
                    f'Cannot load policy {policy["name"]} '
                    f'dict to object. Skipping',
                    exc_info=True,
                )
                continue
            except AssertionError:
                _LOG.warning(
                    f'Cannot load {policy["name"]}. Skipping'
                )
                continue
            provider_policies[pol.provider_name].append(pol)

        if not provider_policies:
            return []

        if len(provider_policies) > 1:
            _LOG.warning(
                f'Multiple policies providers {provider_policies.keys()} '
                'are loaded but only one is expected'
            )
            p_name, p_policies = (
                self.cc_provider_name(self._cloud),
                provider_policies.get(self.cc_provider_name(self._cloud), ()),
            )
        else:
            p_name, p_policies = next(iter(provider_policies.items()))

        if not p_policies:
            return []
        _LOG.info(
            f'Loaded {len(p_policies)} policies for provider {p_name}. '
            f'Going to initialize the provider'
        )

        if p_name == 'aws':
            collection = self._load_provider_aws(p_policies, options)
        else:
            collection = self._load_provider(p_name, p_policies, options)

        result = []
        for p in collection:
            p.expand_variables(p.get_variables())
            try:
                p.validate()
            except PolicyValidationError:
                _LOG.warning(
                    f'Policy {p.name} validation failed', exc_info=True
                )
                continue
            except (ValueError, Exception):
                _LOG.warning(
                    'Unexpected error occurred validating policy',
                    exc_info=True,
                )
                continue
            result.append(p)
        return result

    def load_from_policies(self, policies: list[PolicyDict]) -> list[Policy]:
        _LOG.info('Loading policies')
        items = self._load(policies)
        match self._cloud:
            case Cloud.AWS:
                items = list(self.prepare_policies(items))
            case Cloud.KUBERNETES:
                for pol in items:
                    self.set_global_output(pol)
                    pol.data.setdefault(
                        'source',
                        self._ensure_source(pol.resource_type),
                    )
            case _:
                if self._output_dir:
                    for pol in items:
                        self.set_global_output(pol)
        _LOG.info('Policies were loaded')
        return items

    def load_from_regions_to_rules(
        self, policies: list[PolicyDict], mapping: dict[str, set[str]]
    ) -> list[Policy]:
        rules = set(chain.from_iterable(mapping.values()))
        if self._cloud != Cloud.AWS:
            items = self._load(policies)
            items = list(filter(lambda p: p.name in rules, items))
            for policy in items:
                self.set_global_output(policy)
            return items

        config = self._base_config()
        config.regions = [*mapping.keys(), AWS_DEFAULT_REGION]
        items = []
        for policy in self.prepare_policies(self._load(policies, config)):
            if self.is_global(policy) and policy.name in rules:
                items.append(policy)
            elif policy.name in (mapping.get(policy.options.region) or ()):
                items.append(policy)
        return items

    @staticmethod
    def _ensure_source(resource_type: str) -> str:
        source_name = f'describe-{resource_type.replace(".", "-")}'
        if sources.get(source_name):
            _LOG.debug(f'Source {source_name} already registered')
            return source_name

        _LOG.debug(f'Registering new source: {source_name}')
        cls = type(
            f'Describe{resource_type.title().replace(".", "")}',
            (DescribeSource,),
            {"__doc__": f"Auto source for {resource_type}"},
        )

        sources.register(source_name)(cls)

        return source_name
