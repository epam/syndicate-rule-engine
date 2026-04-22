import c7n_kube.query as c7n_kube_query

from executor.job.execution.context import JobExecutionContext
from executor.job.execution.orchestrator import run_standard_job
from executor.job.execution.region_executor import (
    RegionScanResult,
    job_initializer,
    process_job_concurrent,
)

__all__ = (
    "JobExecutionContext",
    "RegionScanResult",
    "job_initializer",
    "process_job_concurrent",
    "run_standard_job",
)


# TODO: need fix on Cloud Custodian side
def _get_resource_query_fixed(self):
    raw = self.data.get("query")
    if not raw:
        return {}
    merged = {}
    for part in raw if isinstance(raw, list) else [raw]:
        merged.update(part)
    return merged


c7n_kube_query.QueryResourceManager.get_resource_query = _get_resource_query_fixed
