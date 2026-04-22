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


from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

from modular_sdk.models.tenant import Tenant
from modular_sdk.services.customer_service import CustomerService
from modular_sdk.services.tenant_service import TenantService
from helpers.log_helper import get_logger
from services import SP
from services.platform_service import Platform, PlatformService
from services.reports_bucket import (
    PlatformReportsBucketKeysBuilder,
    TenantReportsBucketKeysBuilder,
    ReportsBucketKeysBuilder,
)
from services.sharding import (
    ShardsCollection,
    ShardsCollectionFactory,
    ShardsS3IO,
)
from executor.job.credentials.resolver import (
    get_job_credentials,
    get_platform_credentials,
    get_rules_to_exclude,
    get_tenant_credentials,
)
from executor.job.execution.context import JobExecutionContext
from executor.job.execution.orchestrator import run_standard_job
from executor.job.execution.region_executor import (
    RegionScanResult,
    job_initializer,
    process_job_concurrent,
)
from executor.job.job_failure import (
    JobFailure,
    JobErrorCode,
    classify_exception,
    default_message_for,
)
from executor.job.integration.dojo import upload_to_dojo
from executor.job.integration.license_manager import post_lm_job
from executor.job.integration.siem import upload_to_siem
from executor.job.policies.filter import (
    expand_results_to_aliases,
    filter_policies,
    skip_duplicated_policies,
)
from executor.job.policies.loader import PoliciesLoader
from executor.job.policies.runners import (
    AWSRunner,
    AZURERunner,
    GCPRunner,
    K8SRunner,
    Runner,
)
from executor.job.rulesets.resolver import (
    resolve_job_rulesets,
    resolve_licensed_ruleset,
    resolve_standard_ruleset,
)
from executor.job.tasks.metadata import update_metadata
from executor.job.tasks.standard import task_scheduled_job, task_standard_job
from executor.job.types import JobExecutionError, ModeDict, PolicyDict


_LOG = get_logger(__name__)



__all__ = (
    "AWSRunner",
    "AZURERunner",
    "GCPRunner",
    "JobExecutionContext",
    "JobExecutionError",
    "JobFailure",
    "JobErrorCode",
    "K8SRunner",
    "ModeDict",
    "PoliciesLoader",
    "PolicyDict",
    "Runner",
    "expand_results_to_aliases",
    "filter_policies",
    "get_job_credentials",
    "get_platform_credentials",
    "get_rules_to_exclude",
    "get_tenant_credentials",
    "RegionScanResult",
    "classify_exception",
    "default_message_for",
    "job_initializer",
    "post_lm_job",
    "process_job_concurrent",
    "resolve_job_rulesets",
    "resolve_licensed_ruleset",
    "resolve_standard_ruleset",
    "run_standard_job",
    "skip_duplicated_policies",
    "task_scheduled_job",
    "task_standard_job",
    "update_metadata",
    "upload_to_dojo",
    "upload_to_siem",
    "remove_old_shard_parts",
)


# TODO: move to a separate file
def _remove_stale_parts_from_collection(
        days: int,
        tenant: Tenant,
        keys_builder: ReportsBucketKeysBuilder,
) -> None:
    """Load the latest shards collection for a given tenant/platform,
    drop every shard part whose last successful scan timestamp is older
    than *days* days, and persist the pruned collection back to S3/MinIO bucket.

    Parts that have never been scanned successfully (no timestamp) are
    left untouched — they may still be pending their first scan.

    Args:
        days: Age threshold in days. Parts older than this are removed.
        tenant: The tenant whose shard collection is being cleaned.
        keys_builder: Provides the S3 key path for the latest shards
            file (platform-level or tenant-level).
    """
    cutoff_ts: float = (
        (datetime.now(timezone.utc) - timedelta(days=days)).timestamp()
    )

    # Build the collection bound to the tenant and point its I/O at the
    # "latest" shards object in the reports bucket.
    collection: ShardsCollection = ShardsCollectionFactory.from_tenant(tenant)
    collection.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=keys_builder.latest_key(),
        client=SP.s3,
    )
    # Download all shard data from bucket.
    collection.fetch_all()
    parts_to_drop = []
    for part in collection.iter_parts():
        # Timestamp of the most recent successful scan that updated
        # this part.  `None` means the resources was never scanned.
        timestamp: float | None = part.last_successful_timestamp()
        if timestamp is None:
            continue
        if timestamp < cutoff_ts:
            # The part has not been refreshed within the retention
            # window, meaning the corresponding rule was likely removed
            # from the ruleset.  Drop it so it doesn't linger forever.
            parts_to_drop.append(part)

    for part in parts_to_drop:
        collection.drop_part(part)
    # Persist the (possibly smaller) collection back to bucket.
    collection.write_all()


# TODO: move to a separate file
def remove_old_shard_parts(days: int) -> None:
    """Iterate over every tenant and its platforms and purge shard parts
    that have not been updated within the last *days* days.

    Background
    ----------
    When a rule is removed from a ruleset, any shard parts it produced
    remain in the "latest" shards collection indefinitely because
    nothing overwrites or deletes them.  This function implements a
    time-based eviction policy: any shard part whose last successful
    scan is older than *days* days is dropped.

    Flow
    ----
    For each customer → tenant/platform:
      1. For every **platform** linked to the tenant, clean the
         platform-level latest shards collection.
      2. Clean the **tenant-level** latest shards collection.

    Args:
        days: Retention period in days.  Parts older than this are
            permanently removed from the latest shards file.
    """
    customer_service: CustomerService = SP.modular_client.customer_service()
    tenant_service: TenantService = SP.modular_client.tenant_service()
    platform_service: PlatformService = SP.platform_service

    # Walk the full hierarchy: customer → tenant/platform.
    for customer in customer_service.i_get_customer():
        for tenant in tenant_service.i_get_tenant_by_customer(customer.name):

            platforms: Iterator[Platform] = platform_service.query_by_tenant(tenant)
            for platform in platforms:
                keys_builder = PlatformReportsBucketKeysBuilder(platform)
                _LOG.info(f"Removing stale shards from tenant {tenant.name}: "
                          f"platform {platform.name}")
                _remove_stale_parts_from_collection(days, tenant, keys_builder)

            keys_builder = TenantReportsBucketKeysBuilder(tenant)
            _LOG.info(f"Removing stale shards from tenant {tenant.name}: "
                      f"cloud {tenant.cloud}")
            _remove_stale_parts_from_collection(days, tenant, keys_builder)
