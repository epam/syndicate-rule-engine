"""
Cloud Custodian is designed as a cli tool while we are using its internals
here. That is not so bad but keep your eyes open for their internal changes
that can possibly break something of ours.
Cloud Custodian declares a class wrapper over one policy that is called
(surprisingly) "Policy". A yaml file with policies is loaded to a collection
of these Policy objects, a separate Policy object per policy and region.
They have a lot of "logic and validation" there (see
c7n.commands.policy_command decorator). After being loaded each policy
object is called in a loop. Quite simple.

We do the same thing except that we use our custom code for
"loading and validation" though it mostly consists of their snippets of code.
Our PoliciesLoader class is responsible for that. Its method _load contains
their snippets. You can sort out the rest by yourself.

---------------------------------------
Memory issue
---------------------------------------
Seems like Cloud Custodian has some issue with RAM.
I scrutinized it a lot and am sure that the issue exists. It looks like a
memory leak. It seems to happen under certain conditions that I can reproduce.
But still I'm not 100% sure that the issue is a Cloud Custodian's fault. Maybe
its roots are much deeper. So, the explanation follows:

If we have a more or less big set of policies (say 100) and try to run them
against many regions of an AWS account we can notice that the memory that is
used by this running process keeps increasing. Sometimes it decreases to
normal levels, but generally it tend to become bigger with time. It looks like
when we get to run a policy against a NEW region (say, all policies are
finished for eu-central-1 and we start eu-west-1) the RAM jumps up for
approximately ~100Mb. I want to emphasize that it just looks like it has to
do something with regions.
That IS a problem because if we execute one job that is about to scan all 20
regions with all 500 policies it will be taking about 3Gb or OS's RAM in
the end. Needless to say that it will eventually cause our k8s cluster to
freeze or to kill the pod with OOM.

A plausible cause
---------------------------------------
Boto3 sessions are not thread-safe meaning that you must not pass one
session object to multiple threads and create a client from that session
inside each thread (https://github.com/boto/boto3/pull/2848). But you can
share the same client between different threads. Clients are generally
thread-safe to use. And somehow this creation of clients inside threads
cause memory to upswing. You can check it yourself comparing these two
functions and checking their RAM usage using htop, ps or some other activity
monitor:

.. code-block:: python

    def create_clients_threads():
        session = boto3.Session()

        def method(s, c):
            print('creating client without lock')
            client = s.client('s3')
            time.sleep(1)

        with ThreadPoolExecutor() as e:
            for _ in range(100):
                e.submit(method, session, cl)

    def create_clients_threads_lock():
        session = boto3.Session()
        client_lock = threading.Lock()

        def method(s, l):
            print('creating client under lock')
            with l:
                client = s.client('s3')
            time.sleep(1)

        with ThreadPoolExecutor() as e:
            for _ in range(100):
                e.submit(method, session, client_lock)


The second one consumes much less memory. You can experiment with different
number of workers, types of clients, etc.

I tried to fix it https://github.com/cloud-custodian/cloud-custodian/pull/9065.
Then they made this new c7n.credentials.CustodianSession class that was
probably supposed to completely mitigate the issue, but it was reverted:
https://github.com/cloud-custodian/cloud-custodian/pull/9569. The problem
persists.

Our solution:
---------------------------------------
After lots of different tries we just decided to allocate a separate OS process
for each region (i know, we figured out that regions are not important here,
but they serve as a splitting point). Then we just close the process after
finishing with that region. This releases its resources and all the memory
that could leak within that process. As quirky as it could be, it worked!
Processes are executed one after another, not in parallel.
Now memory always stays within 500-600mb.
It was a solution for while.

Welcome, Celery
---------------------------------------
After a while we need some tasks queue management framework to run our jobs.
I decided to use Celery since I knew it more or less, and it seems
a reasonable choice. After moving to Celery our jobs stopped working
(ERROR: celery: daemonic processes are not allowed to have children).
We use Celery prefork pool mode.
Without our separate-region-processes the worker was eventually killed by k8s.
So we needed to find a way to solve that. After a lot of pain and
research and attempts I got it working with billiard instead of
multiprocessing. I just could not make it work with other Celery pool modes
and normal memory so here it's.
Some links:
https://stackoverflow.com/questions/51485212/multiprocessing-gives-assertionerror-daemonic-processes-are-not-allowed-to-have
https://stackoverflow.com/questions/30624290/celery-daemonic-processes-are-not-allowed-to-have-children
https://github.com/celery/celery/issues/4525
https://stackoverflow.com/questions/54858326/python-multiprocessing-billiard-vs-multiprocessing
https://github.com/celery/billiard/issues/282
"""

import os
import tempfile
import time
import traceback
from abc import ABC, abstractmethod
from collections import defaultdict
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, Generator, Iterable, cast

# import multiprocessing
import billiard as multiprocessing  # allows to execute child processes from a daemon process
from azure.core.exceptions import ClientAuthenticationError
from botocore.exceptions import ClientError
from c7n.config import Config
from c7n.exceptions import PolicyValidationError
from c7n.policy import Policy, PolicyCollection
from c7n.provider import clouds
from c7n.resources import load_resources
from celery.exceptions import SoftTimeLimitExceeded
from google.auth.exceptions import GoogleAuthError
from googleapiclient.errors import HttpError
from modular_sdk.commons.constants import (
    ENV_AZURE_CLIENT_CERTIFICATE_PATH,
    ENV_GOOGLE_APPLICATION_CREDENTIALS,
    ENV_KUBECONFIG,
    ParentType,
)
from modular_sdk.models.tenant import Tenant
from modular_sdk.services.environment_service import EnvironmentContext
from msrestazure.azure_exceptions import CloudError
from typing_extensions import NotRequired, TypedDict

from executor.helpers.constants import (
    ACCESS_DENIED_ERROR_CODE,
    AWS_DEFAULT_REGION,
    CACHE_FILE,
    INVALID_CREDENTIALS_ERROR_CODES,
    ExecutorError,
)
from executor.helpers.profiling import xray_recorder as _XRAY
from executor.services import BSP
from executor.services.report_service import JobResult
from helpers.constants import (
    GLOBAL_REGION,
    TS_EXCLUDED_RULES_KEY,
    BatchJobEnv,
    Env,
    Cloud,
    JobState,
    PlatformType,
    PolicyErrorType,
    DEPRECATED_RULE_SUFFIX
)
from helpers.log_helper import get_logger
from helpers.regions import AWS_REGIONS
from helpers.time_helper import utc_datetime, utc_iso
from models.batch_results import BatchResults
from models.job import Job
from models.rule import RuleIndex
from services import SP
from services.ambiguous_job_service import AmbiguousJob
from services.chronicle_service import ChronicleConverterType
from services.clients import Boto3ClientFactory
from services.clients.chronicle import ChronicleV2Client
from services.clients.dojo_client import DojoV2Client
from services.clients.eks_client import EKSClient
from services.clients.lm_client import LMException
from services.clients.sts import StsClient, TokenGenerator
from services.job_lock import TenantSettingJobLock
from services.job_service import JobUpdater
from services.platform_service import K8STokenKubeconfig, Kubeconfig, Platform
from services.report_convertors import ShardCollectionDojoConvertor
from services.reports_bucket import (
    PlatformReportsBucketKeysBuilder,
    RulesetsBucketKeys,
    StatisticsBucketKeysBuilder,
    TenantReportsBucketKeysBuilder,
)
from services.ruleset_service import RulesetName
from services.sharding import (
    ShardsCollection,
    ShardsCollectionFactory,
    ShardsS3IO,
)
from services.udm_generator import (
    ShardCollectionUDMEntitiesConvertor,
    ShardCollectionUDMEventsConvertor,
)
from executor.plugins import register_all


class PolicyDict(TypedDict):
    name: str
    resource: str
    comment: NotRequired[str]
    description: str


if TYPE_CHECKING:
    from celery import Task  # noqa

_LOG = get_logger(__name__)


class ExecutorException(Exception):
    def __init__(self, error: ExecutorError):
        self.error = error


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
        output_dir: Path | None = None, # NOTE: output_dir should be None only if policy execution mode don't use it
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
        # s3 has one endpoint for all regions
        return rt.global_resource or rt.service == 's3'

    @staticmethod
    def get_policy_region(policy: Policy) -> str:
        if PoliciesLoader.is_global(policy):
            return GLOBAL_REGION
        return policy.options.region

    def _base_config(self) -> Config:
        match self._cloud:
            case Cloud.AWS:
                # load for all and just keep necessary. It does not provide
                # much overhead but more convenient
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
        - aws.hostedzone: global, loaded once
        - aws.iam-certificate: global, loaded once
        - aws.iam-group: global, loaded once
        - aws.iam-role: global, loaded once
        - aws.iam-user: global, loaded once
        - aws.r53domain: global, loaded once
        - aws.rrset: global, loaded once
        - aws.s3: global, loaded for each region (kind of bug)
        - aws.waf: global, loaded once
        Cloud Custodian automatically knows that all the listed resource types
        are global and loads them only once, EXCEPT s3. Technically it's not
        global because each bucket is living in its own region but the api to
        list buckets is the same for all buckets for we must execute all s3
        rules only once, and they will contain results for all regions. In
        other words treat s3 rules as global
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
                # next two lines are probably just for s3 resource types
                policy.options.region = AWS_DEFAULT_REGION
                policy.session_factory.region = AWS_DEFAULT_REGION
                global_yielded.add(policy.name)
                n_global += 1
            else:  # not global
                if (
                    self._regions
                    and policy.options.region not in self._regions
                ):
                    # here is tricky implementation: self._regions can
                    # contain "global" which is not a valid region.
                    # self._load_global is based on existence of "global" in
                    # self._regions. If we want to load only global rules
                    # the fact that self._regions contains only "global"
                    # will help because no policy will skip this if stmt.
                    # But if we want to load all regions, just keep empty
                    # self._regions
                    continue
                _LOG.debug(f'Not global policy found: {policy.name}')
                n_not_global += 1
                # self.set_regional_output(policy)  # Cloud Custodian does it
            yield policy
        _LOG.debug(f'Global policies: {n_global}')
        _LOG.debug(f'Not global policies: {n_not_global}')

    @staticmethod
    def _load_provider_aws(
        policies: list['Policy'], options: Config
    ) -> 'PolicyCollection':
        provider = clouds['aws']()
        p_options = provider.initialize(options)  # same object returned
        try:
            return provider.initialize_policies(
                PolicyCollection(policies, p_options), p_options
            )
        except ClientError:
            _LOG.warning(
                'Error initializing policies, probably cannot describe regions'
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
        # initialize providers (copied from Cloud Custodian)
        provider = clouds[provider_name]()
        p_options = provider.initialize(options)
        return provider.initialize_policies(
            PolicyCollection(policies, p_options), p_options
        )

    def _load(
        self, policies: list[PolicyDict], options: Config | None = None
    ) -> list[Policy]:
        """
        Unsafe load using internal CLoud Custodian API:
        - does not load file, we already have policies list
        - does not check duplicates, can be sure there are no them
        - we almost can be sure there are all 100% valid. We should just skip
          invalid instead of throwing SystemError
        - don't need Structure parser from Cloud Custodian
        - don't need schema validation
        - don't need filters from config and some other things
        :param policies:
        :return:
        """
        if not policies:
            return []

        if not options:
            options = self._base_config()
        options.region = ''
        load_resources(self._get_resource_types(policies))
        # here we should probably validate schema, but it's too time-consuming
        provider_policies = defaultdict(list)
        for policy in policies:
            # if policy['name'].endswith(DEPRECATED_RULE_SUFFIX):
            #     _LOG.warning(
            #         f'Policy {policy["name"]} is deprecated. '
            #         'Skipping'
            #     )
            #     continue
            try:
                pol = Policy(policy, options)
            except PolicyValidationError:
                _LOG.warning(
                    f'Cannot load policy {policy["name"]} '
                    f'dict to object. Skipping',
                    exc_info=True,
                )
                continue
            except AssertionError:
                _LOG.warning(
                    f'Cannot load {policy["name"]}. '
                    'Skipping'
                )
                continue
            provider_policies[pol.provider_name].append(pol)

        if not provider_policies:
            return []
        # provider_policies contains something
        if len(provider_policies) > 1:
            _LOG.warning(
                f'Multiple policies providers {provider_policies.keys()} are loaded but only one is expected'
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

        # Variable expansion and non schema validation
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
        """
        This functionality is already present inside Cloud Custodian but that
        is that part of private python API, besides it does more that we need.
        So, this is our small implementation which does exactly what we need
        here.
        :param policies:
        :return:
        """
        _LOG.info('Loading policies')
        items = self._load(policies)
        match self._cloud:
            case Cloud.AWS:
                items = list(self.prepare_policies(items))
            case _:
                if self._output_dir:
                    for pol in items:
                        self.set_global_output(pol)
        _LOG.info('Policies were loaded')
        return items

    def load_from_regions_to_rules(
        self, policies: list[PolicyDict], mapping: dict[str, set[str]]
    ) -> list[Policy]:
        """
        Currently, self._load_global does not impact this method
        Expected mapping:
        {
            'eu-central-1': {'epam-aws-005..', 'epam-aws-006..'},
            'eu-west-1': {'epam-aws-006..', 'epam-aws-007..'}
        }
        :param policies:
        :param mapping:
        :return:
        """
        rules = set(chain.from_iterable(mapping.values()))  # all rules
        if self._cloud != Cloud.AWS:
            # load all policies ignoring region and set global to all
            items = self._load(policies)
            items = list(filter(lambda p: p.name in rules, items))
            for policy in items:
                self.set_global_output(policy)
            return items
        # self._cloud == Cloud.AWS
        # first -> I load all the rules for regions that came + us-east-1.
        # second -> execute self.prepare_policies in order to set global
        # third -> For each region I keep only necessary rules
        config = self._base_config()
        config.regions = [*mapping.keys(), AWS_DEFAULT_REGION]
        items = []
        for policy in self.prepare_policies(self._load(policies, config)):
            if self.is_global(policy) and policy.name in rules:
                items.append(policy)
            elif policy.name in (mapping.get(policy.options.region) or ()):
                items.append(policy)
        return items


class Runner(ABC):
    cloud: Cloud

    def __init__(self, policies: list[Policy], failed: dict | None = None):
        self._policies = policies

        self.failed = failed if isinstance(failed, dict) else {}
        self.n_successful = 0

        self._err = None
        self._err_msg = None
        self._err_exc = None

    @classmethod
    def factory(
        cls, cloud: Cloud, policies: list[Policy], failed: dict | None = None
    ) -> 'Runner':
        _class = next(
            filter(lambda sub: sub.cloud == cloud, cls.__subclasses__())
        )
        return _class(policies, failed)

    def start(self):
        while self._policies:
            self._call_policy(policy=self._policies.pop())

    def _call_policy(self, policy: Policy):
        if self._err is None:
            self._handle_errors(policy)  # won't raise
            return
        _LOG.debug(
            'Some previous policy failed with error that will recur. '
            f'Skipping policy {policy.name}'
        )
        self._add_failed(
            region=PoliciesLoader.get_policy_region(policy),
            policy=policy.name,
            error_type=self._err,
            message=self._err_msg,
            exception=self._err_exc,
        )

    @staticmethod
    def add_failed(
        failed: dict,
        region: str,
        policy: str,
        error_type: PolicyErrorType,
        exception: Exception | None = None,
        message: str | None = None,
    ):
        tb = []
        if exception:
            te = traceback.TracebackException.from_exception(exception)
            tb.extend(te.format())
            if not message:
                message = ''.join(te.format_exception_only())
        failed[(region, policy)] = (error_type, message, tb)

    def _add_failed(
        self,
        region: str,
        policy: str,
        error_type: PolicyErrorType,
        exception: Exception | None = None,
        message: str | None = None,
    ):
        self.add_failed(
            self.failed, region, policy, error_type, exception, message
        )

    @abstractmethod
    def _handle_errors(self, policy: Policy): ...


class AWSRunner(Runner):
    cloud = Cloud.AWS

    def _handle_errors(self, policy: Policy):
        try:
            policy()
            self.n_successful += 1
        except ClientError as error:
            ec = error.response.get('Error', {}).get('Code')
            er = error.response.get('Error', {}).get('Message')

            if ec in ACCESS_DENIED_ERROR_CODE.get(self.cloud):
                _LOG.warning(
                    f"Policy '{policy.name}' is skipped. Reason: '{er}'"
                )
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.ACCESS,
                    message=er,
                )
            elif ec in INVALID_CREDENTIALS_ERROR_CODES.get(self.cloud, ()):
                _LOG.warning(
                    f"Policy '{policy.name}' is skipped due to invalid "
                    f'credentials. All the subsequent rules will be skipped'
                )
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.CREDENTIALS,
                    message=er,
                )
                self._err = PolicyErrorType.CREDENTIALS
                self._err_msg = er
            else:
                _LOG.warning(
                    f"Policy '{policy.name}' has failed. "
                    f'Client error occurred. '
                    f"Code: '{ec}'. "
                    f'Reason: {er}'
                )
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.CLIENT,
                    exception=error,
                )
        except Exception as error:
            _LOG.exception(
                f'Policy {policy.name} has failed with unexpected error'
            )
            self._add_failed(
                region=PoliciesLoader.get_policy_region(policy),
                policy=policy.name,
                error_type=PolicyErrorType.INTERNAL,
                exception=error,
            )


class AZURERunner(Runner):
    cloud = Cloud.AZURE

    def _handle_errors(self, policy: Policy):
        try:
            policy()
            self.n_successful += 1
        except CloudError as error:
            ec = error.error
            er = error.message.split(':')[-1].strip()
            if ec in INVALID_CREDENTIALS_ERROR_CODES.get(self.cloud, ()):
                _LOG.warning(
                    f"Policy '{policy.name}' is skipped due to invalid "
                    f'credentials. All the subsequent rules will be skipped'
                )
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.CREDENTIALS,
                    message=er,
                )
                self._err = PolicyErrorType.CREDENTIALS
                self._err_msg = er
            else:
                _LOG.warning(
                    f"Policy '{policy.name}' has failed. "
                    f'Client error occurred. '
                    f"Code: '{ec}'. "
                    f'Reason: {er}'
                )
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.CLIENT,
                    exception=error,
                )
        except Exception as error:
            _LOG.exception(
                f'Policy {policy.name} has failed with unexpected error'
            )
            self._add_failed(
                region=PoliciesLoader.get_policy_region(policy),
                policy=policy.name,
                error_type=PolicyErrorType.INTERNAL,
                exception=error,
            )
        except SystemExit as error:
            if isinstance(error.__context__, ClientAuthenticationError):
                _LOG.warning(
                    f"Policy '{policy.name}' is skipped due to invalid "
                    f'credentials. All the subsequent rules will be skipped'
                )
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.CREDENTIALS,
                    message=error.__context__.message,
                )
                self._err = PolicyErrorType.CREDENTIALS
                self._err_msg = error.__context__.message
            else:
                raise


class GCPRunner(Runner):
    cloud = Cloud.GOOGLE

    def _handle_errors(self, policy: Policy):
        try:
            policy()
            self.n_successful += 1
        except GoogleAuthError as error:
            error_reason = str(error.args[-1])
            _LOG.warning(
                f"Policy '{policy.name}' is skipped due to invalid "
                f'credentials. All the subsequent rules will be skipped'
            )
            self._add_failed(
                region=PoliciesLoader.get_policy_region(policy),
                policy=policy.name,
                error_type=PolicyErrorType.CREDENTIALS,
                message=error_reason,
            )
            self._err = PolicyErrorType.CREDENTIALS
            self._err_msg = error_reason
        except HttpError as error:
            if error.status_code == 403:
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.ACCESS,
                    message=error.reason,
                )
            else:
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.CLIENT,
                    exception=error,
                )
        except Exception as error:
            _LOG.exception(
                f'Policy {policy.name} has failed with unexpected error'
            )
            self._add_failed(
                region=PoliciesLoader.get_policy_region(policy),
                policy=policy.name,
                error_type=PolicyErrorType.INTERNAL,
                exception=error,
            )


class K8SRunner(Runner):
    cloud = Cloud.KUBERNETES

    def _handle_errors(self, policy: Policy):
        try:
            policy()
            self.n_successful += 1
        except Exception as error:
            _LOG.exception(
                f'Policy {policy.name} has failed with unexpected error'
            )
            self._add_failed(
                region=PoliciesLoader.get_policy_region(policy),
                policy=policy.name,
                error_type=PolicyErrorType.INTERNAL,
                exception=error,
            )


def post_lm_job(job: Job):
    if not job.affected_license:
        return
    rulesets = list(
        filter(lambda x: x.license_key, [RulesetName(r) for r in job.rulesets])
    )
    if not rulesets:
        return
    lk = rulesets[0].license_key
    lic = SP.license_service.get_nullable(lk)
    if not lic:
        return

    try:
        SP.license_manager_service.cl.post_job(
            job_id=job.id,
            customer=job.customer_name,
            tenant=job.tenant_name,
            ruleset_map={
                lic.tenant_license_key(job.customer_name): [
                    r.to_str() for r in rulesets
                ]
            },
        )
    except LMException as e:
        ExecutorError.LM_DID_NOT_ALLOW.reason = str(e)
        raise ExecutorException(ExecutorError.LM_DID_NOT_ALLOW)


def upload_to_siem(ctx: 'JobExecutionContext', collection: ShardsCollection):
    tenant = ctx.tenant
    job = AmbiguousJob(ctx.job)
    platform = ctx.platform
    warnings = []
    cloud = ctx.cloud()

    metadata = SP.license_service.get_customer_metadata(tenant.customer_name)
    for dojo, configuration in SP.integration_service.get_dojo_adapters(
        tenant, True
    ):
        convertor = ShardCollectionDojoConvertor.from_scan_type(
            configuration.scan_type, cloud, metadata
        )
        configuration = configuration.substitute_fields(job, platform)
        client = DojoV2Client(
            url=dojo.url, api_key=SP.defect_dojo_service.get_api_key(dojo)
        )
        try:
            client.import_scan(
                scan_type=configuration.scan_type,
                scan_date=utc_datetime(),
                product_type_name=configuration.product_type,
                product_name=configuration.product,
                engagement_name=configuration.engagement,
                test_title=configuration.test,
                data=convertor.convert(collection),
                tags=SP.integration_service.job_tags_dojo(job),
            )
        except Exception:
            _LOG.exception('Unexpected error occurred pushing to dojo')
            warnings.append(f'could not upload data to DefectDojo {dojo.id}')
    mcs = SP.modular_client.maestro_credentials_service()
    for (
        chronicle,
        configuration,
    ) in SP.integration_service.get_chronicle_adapters(tenant, True):
        _LOG.debug('Going to push data to Chronicle')
        creds = mcs.get_by_application(
            chronicle.credentials_application_id, tenant
        )
        if not creds:
            continue
        client = ChronicleV2Client(
            url=chronicle.endpoint,
            credentials=creds.GOOGLE_APPLICATION_CREDENTIALS,
            customer_id=chronicle.instance_customer_id,
        )
        match configuration.converter_type:
            case ChronicleConverterType.EVENTS:
                _LOG.debug('Converting our collection to UDM events')
                convertor = ShardCollectionUDMEventsConvertor(
                    cloud, metadata, tenant=tenant
                )
                success = client.create_udm_events(
                    events=convertor.convert(collection)
                )
            case _:  # ENTITIES
                _LOG.debug('Converting our collection to UDM entities')
                convertor = ShardCollectionUDMEntitiesConvertor(
                    cloud, metadata, tenant=tenant
                )
                success = client.create_udm_entities(
                    entities=convertor.convert(collection),
                    log_type='AWS_API_GATEWAY',  # todo use a generic log type or smt
                )
        if not success:
            warnings.append(
                f'could not upload data to Chronicle {chronicle.id}'
            )
    if warnings:
        ctx.add_warnings(*warnings)


def get_tenant_credentials(tenant: Tenant) -> dict | None:
    """
    If dict is returned it means that we should export that dict to envs
    and start the scan even if the dict is empty
    """
    mcs = SP.modular_client.maestro_credentials_service()
    credentials = None
    _LOG.info('Trying to get creds from `CUSTODIAN_ACCESS` parent')
    parent = (
        SP.modular_client.parent_service().get_linked_parent_by_tenant(
            tenant=tenant, type_=ParentType.CUSTODIAN_ACCESS
        )
    )
    if parent:
        application = (
            SP.modular_client.application_service().get_application_by_id(
                parent.application_id
            )
        )
        if application:
            _creds = mcs.get_by_application(application, tenant)
            if _creds:
                credentials = _creds.dict()
    if credentials is None and BatchJobEnv.ALLOW_MANAGEMENT_CREDS.as_bool():
        _LOG.info(
            'Trying to get creds from maestro management parent & application'
        )
        _creds = mcs.get_by_tenant(tenant=tenant)
        if _creds:  # not a dict
            credentials = _creds.dict()
    if credentials is None:
        _LOG.info('Trying to get creds from instance profile')
        match tenant.cloud:
            case Cloud.AWS:
                try:
                    aid = StsClient.build().get_caller_identity()['Account']
                    _LOG.debug('Instance profile found')
                    if aid == tenant.project:
                        _LOG.info(
                            'Instance profile credentials match to tenant id'
                        )
                        credentials = {}
                except (Exception, ClientError) as e:
                    _LOG.warning(f'No instance credentials found: {e}')
            case Cloud.AZURE:
                try:
                    from c7n_azure.session import Session

                    aid = Session().subscription_id
                    _LOG.info('subscription id found')
                    if aid == tenant.project:
                        _LOG.info('Subscription id matches to tenant id')
                        credentials = {}
                except BaseException:  # catch sys.exit(1)
                    _LOG.warning('Could not find azure subscription id')
    if credentials is not None:
        credentials = mcs.complete_credentials_dict(
            credentials=credentials, tenant=tenant
        )
    return credentials


def get_job_credentials(job: Job | BatchResults, cloud: Cloud) -> dict | None:
    _LOG.info('Trying to resolve credentials from job')
    if not job.credentials_key:
        return
    creds = BSP.credentials_service.get_credentials_from_ssm(
        job.credentials_key, remove=True
    )
    if creds is None:
        return
    if cloud is Cloud.GOOGLE:
        creds = BSP.credentials_service.google_credentials_to_file(creds)
    return creds


def get_platform_credentials(job: Job, platform: Platform) -> dict | None:
    """
    Credentials for platform (k8s) only. This should be refactored somehow.
    Raises ExecutorException if not credentials are found
    :param platform:
    :return:
    """
    token = None
    if job.credentials_key:
        token = BSP.credentials_service.get_credentials_from_ssm(
            job.credentials_key
        )

    app = SP.modular_client.application_service().get_application_by_id(
        platform.parent.application_id
    )
    kubeconfig = {}
    if app.secret:
        kubeconfig = (
            SP.modular_client.assume_role_ssm_service().get_parameter(
                app.secret
            )
            or {}
        )  # noqa

    if kubeconfig and token:
        _LOG.debug('Kubeconfig and custom token are provided. Combining both')
        config = Kubeconfig(kubeconfig)
        session = str(int(time.time()))
        user = f'user-{session}'
        context = f'context-{session}'
        cluster = next(config.cluster_names())  # always should be 1 at least

        config.add_user(user, token)
        config.add_context(context, cluster, user)
        config.current_context = context
        return {ENV_KUBECONFIG: str(config.to_temp_file())}
    elif kubeconfig:
        _LOG.debug('Only kubeconfig is provided')
        config = Kubeconfig(kubeconfig)
        return {ENV_KUBECONFIG: str(config.to_temp_file())}
    if platform.type != PlatformType.EKS:
        _LOG.warning('No kubeconfig provided and platform is not EKS')
        return
    _LOG.debug(
        'Kubeconfig and token are not provided. Using management creds for EKS'
    )
    tenant = SP.modular_client.tenant_service().get(platform.tenant_name)
    parent = SP.modular_client.parent_service().get_linked_parent_by_tenant(
        tenant=tenant, type_=ParentType.AWS_MANAGEMENT
    )
    # TODO: get tenant credentials here somehow
    if not parent:
        _LOG.warning('Parent AWS_MANAGEMENT not found')
        return
    application = (
        SP.modular_client.application_service().get_application_by_id(
            parent.application_id
        )
    )
    if not application:
        _LOG.warning('Management application is not found')
        return
    creds = SP.modular_client.maestro_credentials_service().get_by_application(
        application, tenant
    )
    if not creds:
        _LOG.warning(
            f'No credentials in application: {application.application_id}'
        )
        return
    cl = EKSClient.build()
    cl.client = Boto3ClientFactory(EKSClient.service_name).build(
        region_name=platform.region,
        aws_access_key_id=creds.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=creds.AWS_SECRET_ACCESS_KEY,
        aws_session_token=creds.AWS_SESSION_TOKEN,
    )
    cluster = cl.describe_cluster(platform.name)
    if not cluster:
        _LOG.error(
            f'No cluster with name: {platform.name} '
            f'in region: {platform.region}'
        )
        return
    sts = Boto3ClientFactory('sts').from_keys(
        aws_access_key_id=creds.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=creds.AWS_SECRET_ACCESS_KEY,
        aws_session_token=creds.AWS_SESSION_TOKEN,
        region_name=AWS_DEFAULT_REGION,
    )
    token_config = K8STokenKubeconfig(
        endpoint=cluster['endpoint'],
        ca=cluster['certificateAuthority']['data'],
        token=TokenGenerator(sts).get_token(platform.name),
    )
    return {ENV_KUBECONFIG: str(token_config.to_temp_file())}


def get_rules_to_exclude(tenant: Tenant) -> set[str]:
    """
    Takes into consideration rules that are excluded for that specific tenant
    and for its customer
    :param tenant:
    :return:
    """
    _LOG.info('Querying excluded rules')
    excluded = set()
    ts = SP.modular_client.tenant_settings_service().get(
        tenant_name=tenant.name, key=TS_EXCLUDED_RULES_KEY
    )
    if ts:
        _LOG.info('Tenant setting with excluded rules is found')
        excluded.update(ts.value.as_dict().get('rules') or ())
    cs = SP.modular_client.customer_settings_service().get_nullable(
        customer_name=tenant.customer_name, key=TS_EXCLUDED_RULES_KEY
    )
    if cs:
        _LOG.info('Customer setting with excluded rules is found')
        excluded.update(cs.value.get('rules') or ())
    return excluded


def batch_results_job(batch_results: BatchResults):
    _XRAY.put_annotation('batch_results_id', batch_results.id)

    temp_dir = tempfile.TemporaryDirectory()
    work_dir = Path(temp_dir.name)

    tenant: Tenant = SP.modular_client.tenant_service().get(
        batch_results.tenant_name
    )
    cloud = Cloud[tenant.cloud.upper()]
    credentials = get_credentials(tenant, batch_results)

    # TODO: use standard ruleset
    policies = []
    # policies = BSP.policies_service.separate_ruleset(
    #     from_=BSP.policies_service.ensure_event_driven_ruleset(cloud),
    #     exclude=get_rules_to_exclude(tenant),
    #     keep=set(
    #         chain.from_iterable(batch_results.regions_to_rules().values())
    #     ),
    # )
    loader = PoliciesLoader(
        cloud=cloud,
        output_dir=work_dir,
        regions=BSP.environment_service.target_regions(),
    )
    with EnvironmentContext(credentials, reset_all=False):
        runner = Runner.factory(
            cloud,
            loader.load_from_regions_to_rules(
                policies, batch_results.regions_to_rules()
            ),
        )
        runner.start()

    result = JobResult(work_dir, cloud)
    keys_builder = TenantReportsBucketKeysBuilder(tenant)
    collection = ShardsCollectionFactory.from_cloud(cloud)
    collection.put_parts(result.iter_shard_parts(runner.failed))
    meta = result.rules_meta()
    collection.meta = meta

    _LOG.info('Going to upload to SIEM')
    # upload_to_siem(
    #     tenant=tenant, collection=collection, job=AmbiguousJob(batch_results)
    # )

    collection.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=keys_builder.ed_job_result(batch_results),
        client=SP.s3,
    )
    _LOG.debug('Writing job report')
    collection.write_all()  # writes job report

    latest = ShardsCollectionFactory.from_cloud(cloud)
    latest.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=keys_builder.latest_key(),
        client=SP.s3,
    )
    _LOG.debug('Pulling latest state')
    latest.fetch_by_indexes(collection.shards.keys())
    latest.fetch_meta()

    difference = collection - latest

    _LOG.debug('Writing latest state')
    latest.update(collection)
    latest.update_meta(meta)
    latest.write_all()
    latest.write_meta()

    _LOG.debug('Writing difference')
    difference.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=keys_builder.ed_job_difference(batch_results),
        client=SP.s3,
    )
    difference.write_all()

    _LOG.info('Writing statistics')
    SP.s3.gz_put_json(
        bucket=SP.environment_service.get_statistics_bucket_name(),
        key=StatisticsBucketKeysBuilder.job_statistics(batch_results),
        obj=result.statistics(tenant, runner.failed),
    )
    temp_dir.cleanup()


def multi_account_event_driven_job() -> int:
    for br_uuid in BSP.environment_service.batch_results_ids():
        _LOG.info(f'Processing batch results with id {br_uuid}')
        actions = []
        batch_results = BatchResults.get_nullable(br_uuid)
        if not batch_results:
            _LOG.warning('Somehow batch results item does not exist. Skipping')
            continue
        if batch_results.status == JobState.SUCCEEDED.value:
            _LOG.info('Batch results already succeeded. Skipping')
            continue
        try:
            _LOG.info(f'Starting job for batch result')
            batch_results_job(batch_results)
            _LOG.info(f'Job for batch result {br_uuid} has finished')
            actions.append(BatchResults.status.set(JobState.SUCCEEDED.value))
        except ExecutorException as e:
            _LOG.exception(f'Executor exception {e.error} occurred')
            actions.append(BatchResults.status.set(JobState.FAILED.value))
            actions.append(BatchResults.reason.set(traceback.format_exc()))
        except SoftTimeLimitExceeded:
            _LOG.error('Job is terminated because of soft timeout')
            actions.append(BatchResults.status.set(JobState.FAILED.value))
            actions.append(
                BatchResults.reason.set(ExecutorError.TIMEOUT.with_reason())
            )
        except Exception:
            _LOG.exception('Unexpected exception occurred')
            actions.append(BatchResults.status.set(JobState.FAILED.value))
            actions.append(BatchResults.reason.set(traceback.format_exc()))
        actions.append(BatchResults.stopped_at.set(utc_iso()))
        _LOG.info('Saving batch results item')
        batch_results.update(actions=actions)
    Path(CACHE_FILE).unlink(missing_ok=True)
    return 0


def job_initializer(envs):
    _LOG.info(
        f'Initializing subprocess for a region: {multiprocessing.current_process()}'
    )
    os.environ.update(envs)
    os.environ.setdefault('AWS_DEFAULT_REGION', AWS_DEFAULT_REGION)


def process_job_concurrent(
    items: list[PolicyDict], work_dir: Path, cloud: Cloud, region: str
) -> tuple[int, dict | None]:
    """
    Cloud Custodian keeps consuming RAM for some reason. After 9th-10th region
    scanned the used memory can be more than 1GI, and it isn't freed. Not
    sure about correctness and legality of this workaround, but it
    seems to help. We execute scan for each region in a separate process
    consequently. When one process finished its memory is freed
    (any way the results is flushed to files).
    """
    if Env.ENABLE_CUSTOM_CC_PLUGINS.is_set():
        register_all()

    _LOG.debug(f'Running scan process for region {region}')
    loader = PoliciesLoader(
        cloud=cloud, output_dir=work_dir, regions={region}, cache_period=120
    )
    try:
        _LOG.debug(f'Going to load {len(items)} policies dicts')
        policies = loader.load_from_policies(items)
    except Exception:
        _LOG.exception(
            f'Unexpected error occurred trying to load policies for region {region}'
        )
        return 0, None

    _LOG.info(
        f'{len(policies)} policies instances were loaded and due '
        f'to be executed (one policy instance per available region)'
    )
    # len(policies) is the number of invocations we need to make. If at
    # least one is successful, we consider the job successful but
    # with warnings
    _LOG.info('Starting runner')
    runner = Runner.factory(cloud, policies)
    runner.start()
    _LOG.info('Runner has finished')
    return runner.n_successful, runner.failed


def resolve_standard_ruleset(
    customer_name: str, ruleset: RulesetName
) -> tuple[RulesetName, list[dict]] | None:
    rs = SP.ruleset_service
    if v := ruleset.version:
        item = rs.get_standard(
            customer=customer_name, name=ruleset.name, version=v.to_str()
        )
    else:
        item = rs.get_latest(customer=customer_name, name=ruleset.name)
    if not item:
        _LOG.warning(f'Somehow ruleset does not exist: {ruleset}')
        return
    content = rs.fetch_content(item)
    if not content:
        _LOG.warning(f'Somehow ruleset does not have content: {ruleset}')
        return
    return RulesetName(
        ruleset.name, ruleset.version.to_str() if ruleset.version else None
    ), content.get('policies') or []


def resolve_licensed_ruleset(
    customer_name: str, ruleset: RulesetName
) -> tuple[RulesetName, list[dict]] | None:
    s3 = SP.s3
    if v := ruleset.version:
        content = s3.gz_get_json(
            bucket=SP.environment_service.get_rulesets_bucket_name(),
            key=RulesetsBucketKeys.licensed_ruleset_key(
                ruleset.name, v.to_str()
            ),
        )
        if not content:
            _LOG.warning(f'Content of {ruleset} does not exist')
            return
        return ruleset, content.get('policies', [])
    # no version, resolving latest
    item = SP.ruleset_service.get_licensed(name=ruleset.name)
    if not item:
        _LOG.warning(f'Ruleset {ruleset} does not exist')
        return
    content = s3.gz_get_json(
        bucket=SP.environment_service.get_rulesets_bucket_name(),
        key=RulesetsBucketKeys.licensed_ruleset_key(
            ruleset.name, item.latest_version
        ),
    )
    if not content:
        _LOG.warning(f'Content of {ruleset} does not exist')
        return
    return RulesetName(
        ruleset.name, item.latest_version, ruleset.license_key
    ), content.get('policies') or []


def resolve_job_rulesets(
    customer_name: str, rulesets: Iterable[RulesetName]
) -> Generator[tuple[RulesetName, list[dict]], None, None]:
    for rs in rulesets:
        if rs.license_key:
            resolver = resolve_licensed_ruleset
        else:
            resolver = resolve_standard_ruleset
        result = resolver(customer_name, rs)
        if result is None:
            continue
        yield result


class JobExecutionContext:
    def __init__(
        self,
        job: Job,
        tenant: Tenant,
        platform: Platform | None = None,
        cache: str | None = 'memory',
        cache_period: int = 30,
    ):
        self.job = job
        self.tenant = tenant
        self.platform = platform
        self.cache = cache
        self.cache_period = cache_period

        self.updater = JobUpdater(job)

        self._work_dir = None
        self._exit_code = 0

    def is_platform_job(self) -> bool:
        return self.platform is not None

    def is_scheduled_job(self) -> bool:
        return self.job.scheduled_rule_name is not None

    def cloud(self) -> Cloud:
        if self.is_platform_job():
            return Cloud.KUBERNETES
        return cast(Cloud, Cloud.parse(self.tenant.cloud))  # cannot be None

    @property
    def work_dir(self) -> Path:
        if not self._work_dir:
            raise RuntimeError('can be used only within context')
        return Path(self._work_dir.name)

    def add_warnings(self, *warnings: str) -> None:
        self.updater.add_warnings(*warnings)
        self.updater.update()

    def __enter__(self):
        _LOG.info(f'Acquiring lock for job {self.job.id}')
        TenantSettingJobLock(self.tenant.name).acquire(
            job_id=self.job.id, regions=self.job.regions
        )
        _LOG.info('Setting job status to RUNNING')
        self.updater.started_at = utc_iso()
        self.updater.status = JobState.RUNNING
        self.updater.update()

        _LOG.info('Creating a working dir')
        self._work_dir = tempfile.TemporaryDirectory()

    def _cleanup_cache(self) -> None:
        if self.cache is None or self.cache == 'memory':
            return
        f = Path(self.cache)
        if f.exists():
            f.unlink(missing_ok=True)

    def _cleanup_work_dir(self) -> None:
        if not self._work_dir:
            return
        self._work_dir.cleanup()
        self._work_dir = None

    def _update_lm_job(self):
        if not self.job.affected_license or not Env.is_docker():
            return
        _LOG.info('Updating job in license manager')
        SP.license_manager_service.client.update_job(
            job_id=self.job.id,
            customer=self.job.customer_name,
            created_at=self.job.created_at,
            started_at=self.job.started_at,
            stopped_at=self.job.stopped_at,
            status=self.job.status,
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        _LOG.info('Cleaning cache after job')
        self._cleanup_cache()
        _LOG.info('Cleaning work dir')
        self._cleanup_work_dir()
        _LOG.info('Releasing job lock')
        TenantSettingJobLock(self.tenant.name).release(self.job.id)

        if exc_val is None:
            _LOG.info(
                f'Job {self.job.id} finished without exceptions. Setting SUCCEEDED status'
            )
            self.updater.status = JobState.SUCCEEDED
            self.updater.stopped_at = utc_iso()
            self.updater.update()
            self._update_lm_job()
            return

        _LOG.info(
            f'Job {self.job.id} finished with exception. Setting FAILED status'
        )
        self.updater.status = JobState.FAILED
        self.updater.stopped_at = utc_iso()
        if isinstance(exc_val, ExecutorException):
            _LOG.exception(f'Executor exception {exc_val} occurred')
            # in case the job has failed, we should update it here even if it's
            # saas installation because we cannot retrieve traceback from
            # caas-job-updater lambda
            self.updater.reason = exc_val.error.with_reason()
            if exc_val.error is ExecutorError.LM_DID_NOT_ALLOW:
                self._exit_code = 2
            else:
                self._exit_code = 1
        elif isinstance(exc_val, SoftTimeLimitExceeded):
            _LOG.error('Job is terminated because of soft timeout')
            self.updater.reason = ExecutorError.TIMEOUT.with_reason()
            self._exit_code = 1
        else:
            _LOG.exception('Unexpected error occurred')
            self.updater.reason = ExecutorError.INTERNAL.with_reason()
            self._exit_code = 1

        _LOG.info('Updating job status')
        self.updater.update()
        self._update_lm_job()
        return True


def filter_policies(
    it: Iterable[dict],
    keep: set[str] | None = None,
    exclude: set[str] | None = None,
) -> Iterable[dict]:
    if exclude:
        it = filter(lambda p: p['name'] not in exclude, it)
    if keep:
        it = filter(lambda p: p['name'] in keep, it)
    return it


def skip_duplicated_policies(
    ctx: JobExecutionContext, it: Iterable[dict]
) -> Iterable[dict]:
    emitted = set()
    duplicated = []
    for p in it:
        name = p['name']
        if name in emitted:
            _LOG.warning(f'Duplicated policy found {name}. Skipping')
            duplicated.append(name)
            continue
        emitted.add(name)
        yield p
    if duplicated:
        ctx.add_warnings(
            *[
                f'multiple policies with name {name}'
                for name in sorted(duplicated)
            ]
        )


def run_standard_job(ctx: JobExecutionContext):
    """
    Accepts a job item full of data and runs it. Does not use envs to
    get info about the job
    """
    cloud = ctx.cloud()
    job = ctx.job

    names = []
    lists = []
    for name, lst in resolve_job_rulesets(
        job.customer_name, map(RulesetName, job.rulesets)
    ):
        names.append(name)
        lists.append(lst)

    SP.job_service.update(job, rulesets=[r.to_str() for r in names])

    if job.affected_license:
        _LOG.info('The job is licensed. Making post job request to lm')
        post_lm_job(job)  # can raise ExecutorException

    if pl := ctx.platform:
        credentials = get_platform_credentials(job, pl)
        keys_builder = PlatformReportsBucketKeysBuilder(pl)
    else:
        credentials = get_job_credentials(
            job, cloud
        ) or get_tenant_credentials(ctx.tenant)
        keys_builder = TenantReportsBucketKeysBuilder(ctx.tenant)

    if credentials is None:
        raise ExecutorException(ExecutorError.NO_CREDENTIALS)
    credentials = {str(k): str(v) for k, v in credentials.items() if v}

    policies = list(
        skip_duplicated_policies(
            ctx=ctx,
            it=filter_policies(
                it=chain.from_iterable(lists),
                keep=set(job.rules_to_scan),
                exclude=get_rules_to_exclude(ctx.tenant),
            ),
        )
    )
    _LOG.info(f'Policies are collected: {len(policies)}')
    regions = set(job.regions) | {GLOBAL_REGION}

    successful = 0
    failed = {}
    warnings = []

    for region in sorted(regions):
        # NOTE: read the documentation for this module to see why we need
        # this Pool. Basically it isolates rules run for one region and
        # prevents high ram usage
        _LOG.info(f'Going to init pool for region {region}')
        with multiprocessing.Pool(
            processes=1, initializer=job_initializer, initargs=(credentials,)
        ) as pool:
            pair = pool.apply(
                process_job_concurrent, (policies, ctx.work_dir, cloud, region)
            )

        if pair[1] is None:
            _LOG.warning(
                f'Job for region {region} has failed with no policies loaded'
            )
            warnings.append(f'Could not load policies for region {region}')
            continue

        successful += pair[0]
        if pair[1]:
            if region == GLOBAL_REGION:
                w = f'{len(pair[1])}/{len(pair[1]) + pair[0]} global policies failed'
            else:
                w = f'{len(pair[1])}/{len(pair[1]) + pair[0]} policies failed in region {region}'
            warnings.append(w)
        failed.update(pair[1])

    if cloud is Cloud.GOOGLE and (
        filename := credentials.get(ENV_GOOGLE_APPLICATION_CREDENTIALS)
    ):
        Path(filename).unlink(missing_ok=True)
    if cloud is Cloud.AZURE and (
        filename := credentials.get(ENV_AZURE_CLIENT_CERTIFICATE_PATH)
    ):
        Path(filename).unlink(missing_ok=True)
    ctx.add_warnings(*warnings)
    del warnings
    del credentials

    # NOTE: here we should collect all the data about this scan, but make
    # it failed if number of successful policies is 0

    result = JobResult(ctx.work_dir, cloud)

    collection = ShardsCollectionFactory.from_cloud(cloud)
    collection.put_parts(result.iter_shard_parts(failed))
    meta = result.rules_meta()
    collection.meta = meta

    if successful:
        _LOG.info('Going to upload to SIEM')
        upload_to_siem(ctx=ctx, collection=collection)

    collection.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=keys_builder.job_result(job),
        client=SP.s3,
    )

    _LOG.debug('Writing job report')
    collection.write_all()  # writes job report

    latest = ShardsCollectionFactory.from_cloud(cloud)
    latest.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=keys_builder.latest_key(),
        client=SP.s3,
    )

    _LOG.debug('Pulling latest state')
    latest.fetch_by_indexes(collection.shards.keys())
    latest.fetch_meta()

    _LOG.debug('Writing latest state')
    latest.update(collection)
    latest.update_meta(meta)
    latest.write_all()
    latest.write_meta()

    _LOG.info('Writing statistics')
    SP.s3.gz_put_json(
        bucket=SP.environment_service.get_statistics_bucket_name(),
        key=StatisticsBucketKeysBuilder.job_statistics(job),
        obj=result.statistics(ctx.tenant, failed),
    )
    if not successful:
        raise ExecutorException(ExecutorError.NO_SUCCESSFUL_POLICIES)
    _LOG.info(f"Job '{job.id}' has ended")


# celery tasks
def task_standard_job(self: 'Task | None', job_id: str):
    """
    Runs a single job by the given id
    """
    job = SP.job_service.get_nullable(job_id)
    if not job:
        _LOG.error('Task started for not existing job')
        return

    tenant = SP.modular_client.tenant_service().get(job.tenant_name)
    if not tenant:
        _LOG.error('Task started for not existing tenant')
        return
    platform = None
    if job.platform_id:
        parent = SP.modular_client.parent_service().get_parent_by_id(
            job.platform_id
        )
        if not parent:
            _LOG.error('Task started for not existing parent')
            return
        platform = Platform(parent)
    ctx = JobExecutionContext(job=job, tenant=tenant, platform=platform)
    with ctx:
        run_standard_job(ctx)


def task_scheduled_job(self: 'Task | None', customer_name: str, name: str):
    sch_job = SP.scheduled_job_service.get_by_name(
        customer_name=customer_name, name=name
    )
    if not sch_job:
        _LOG.error('Cannot start scheduled job for not existing sch item')
        return
    tenant = SP.modular_client.tenant_service().get(sch_job.tenant_name)
    if not tenant:
        _LOG.error('Task started for not existing tenant')
        return

    _LOG.info('Building job item from scheduled')
    rulesets = sch_job.meta.as_dict().get('rulesets', [])
    licensed = [r for r in map(RulesetName, rulesets) if r.license_key]
    license_key = licensed[0].license_key if licensed else None

    job = SP.job_service.create(
        customer_name=sch_job.customer_name,
        tenant_name=sch_job.tenant_name,
        regions=sch_job.meta.as_dict().get('regions', []),
        rulesets=sch_job.meta.as_dict().get('rulesets', []),
        rules_to_scan=[],
        affected_license=license_key,
        status=JobState.STARTING,
        batch_job_id=BatchJobEnv.JOB_ID.get(),
        celery_job_id=self.request.id if self is not None else None,
        scheduled_rule_name=name,
    )
    SP.job_service.save(job)

    ctx = JobExecutionContext(job=job, tenant=tenant, platform=None)
    with ctx:
        run_standard_job(ctx)


# def main(environment: dict[str, str] | None = None) -> int:
#     env = environment or {}
#     env.setdefault(ENV_AWS_DEFAULT_REGION, AWS_DEFAULT_REGION)
#     BSP.environment_service.override_environment(env)
#
#     buffer = io.BytesIO()
#     _XRAY.configure(emitter=BytesEmitter(buffer))  # noqa
#
#     _XRAY.begin_segment('AWS Batch job')
#     sampled = _XRAY.is_sampled()
#     _LOG.info(f'Batch job is {"" if sampled else "NOT "}sampled')
#     _XRAY.put_annotation('batch_job_id', BSP.env.batch_job_id())
#
#     match BSP.environment_service.job_type():
#         case BatchJobType.EVENT_DRIVEN:
#             _LOG.info('Starting event driven job')
#             code = multi_account_event_driven_job()
#         case BatchJobType.STANDARD | BatchJobType.SCHEDULED:
#             _LOG.info('Starting standard job')
#             code = single_account_standard_job()
#         case _:  # unreachable
#             code = 1
#
#     _XRAY.end_segment()
#
#     if sampled:
#         _LOG.debug('Writing xray data to S3')
#         buffer.seek(0)
#         SP.s3.gz_put_object(
#             bucket=SP.environment_service.get_statistics_bucket_name(),
#             key=StatisticsBucketKeysBuilder.xray_log(BSP.env.batch_job_id()),
#             body=buffer,
#         )
#     _LOG.info('Finished')
#     return code
#
#
# if __name__ == '__main__':
#     sys.exit(main())
