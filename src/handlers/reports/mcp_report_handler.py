from http import HTTPStatus
from typing import Any, TypedDict, Optional, Generator

from modular_sdk.models.tenant import Tenant
from modular_sdk.services.tenant_service import TenantService

from handlers import AbstractHandler, Mapping
from helpers.constants import Endpoint, HTTPMethod, MCPReportType
from helpers.lambda_response import build_response, ResponseFactory
from helpers.reports import severity_chain, remediation_complexity_chain
from helpers.time_helper import utc_datetime
from models.job import Job
from services import SP, modular_helpers
from services.job_service import JobService
from services.license_service import LicenseService
from services.metadata import Metadata
from services.platform_service import PlatformService, Platform
from services.report_service import ReportService
from services.resources import iter_rule_resource, InPlaceResourceView, \
    CloudResource
from services.sharding import ShardsCollection
from validators.swagger_request_models import MCPReportJobGetModel, \
    MCPReportCompareJobsGetModel
from validators.utils import validate_kwargs


class MCPReportBuilder:
    class MCPResourceReport(TypedDict):
        id: str
        violated_rules: list[dict]

    class MCPRulesReport(TypedDict):
        name: str
        description: str
        severity: str
        remediation: str
        article: str
        impact: str
        remediation_complexity: str
        violated_resources: list[str]

    def __init__(
        self,
        findings_iterator: Generator[tuple[str, CloudResource], None, None],
        entity: Tenant | Platform,
        metadata: Metadata,
        collection: ShardsCollection,
        top: Optional[int] = None,

    ):
        self._it = findings_iterator
        self._entity = entity
        self._meta = metadata
        self._collection = collection
        self._top = top


    def _build_rule(
        self,
        rule: str,
    ) -> dict:
        rm = self._meta.rule(rule)

        return {
            'name': rule,
            'description': self._collection.meta.get(rule, {}).get(
                'description'),
            'severity': rm.severity.value,
            'remediation': rm.remediation,
            'article': rm.article,
            'impact': rm.impact,
            'remediation_complexity': rm.remediation_complexity.value,
            }


    def build_resource_report(self) -> list[MCPResourceReport]:
        datas = {}
        for rule, res in self._it:
            datas.setdefault(res, []).append(rule)

        result = []
        view = InPlaceResourceView()
        for res, rules in datas.items():
            res_data = res.accept(view)
            result.append(
                {
                    'id': res_data.get('arn') or res_data.get('id'),
                    'violated_rules': [self._build_rule(r) for r in rules],
                }
            )

        if self._top:
            result = sorted(
                result,
                key=lambda i: self._compute_resource_sort_key(i),
                reverse=True)[:self._top]

        return result


    def build_rules_report(self) -> list[MCPRulesReport]:
        datas = {}
        for rule, res in self._it:
            datas.setdefault(rule, []).append(res)

        result = []
        view = InPlaceResourceView()
        for rule_name, resources in datas.items():
            rule = self._build_rule(rule_name)
            violated_resources = []
            for res in resources:
                res_data = res.accept(view)
                violated_resources.append(
                        res_data.get('arn') or res_data.get('id')
                )
            rule['violated_resources'] = violated_resources
            result.append(rule)

        if self._top:
            result = sorted(
                result,
                key=lambda i: self._compute_rule_sort_key(i),
                reverse=True)[:self._top]

        return result


    @staticmethod
    def _compute_resource_sort_key(
        item: dict[str, Any],
    ) -> tuple:
        """
        Compute a sort key for each item based on its violated_rules.

        Priority:
          1. More rules with higher severity → better (ranked higher)
          2. Lower remediation complexity → better (ranked higher)

        Returns a tuple that, when sorted in DESCENDING order, places the
        "worst offenders" (most severe, easiest to fix) at the top.
        """
        violated_rules = item.get("violated_rules", [])

        total_severity = sum(
            severity_chain.get(rule.get("severity", "Info"), 0)
            for rule in violated_rules
        )

        rule_count = len(violated_rules)

        max_severity = max(
            (severity_chain.get(rule.get("severity", "Info"), 0)
             for rule in violated_rules),
            default=0,
        )

        total_inverse_complexity = sum(
            (max(remediation_complexity_chain.values()) + 1)
            - remediation_complexity_chain.get(
                rule.get("remediation_complexity", "Unknown"), 5)
            for rule in violated_rules
        )

        return (total_severity, rule_count, max_severity,
                total_inverse_complexity)


    @staticmethod
    def _compute_rule_sort_key(
        item: dict[str, Any],
    ) -> tuple:
        """
        Compute a sort key for rule.

        Priority:
          1. Higher severity → ranked higher
          2. Lower remediation complexity → ranked higher

        Returns a tuple that, when sorted in DESCENDING order, places the
        "worst offenders" (most severe, easiest to fix) at the top.
        """
        violated_resource_count = len(item.get("violated_resources", []))

        severity = severity_chain.get(item.get("severity", "Info"), 0)

        inverse_complexity = (
            max(remediation_complexity_chain.values()) + 1 -
            remediation_complexity_chain.get(item.get(
                "remediation_complexity", "Unknown"), 5)
        )

        return severity, violated_resource_count, inverse_complexity


class MCPReportComparator:
    class MCPResourceComparison(TypedDict):
        id: str
        new_violated_rules: list[dict]
        remediated_rules: list[dict]
        unchanged_violated_rules: list[dict]

    class MCPRulesComparison(TypedDict):
        name: str
        new_violated_resources: list[str]
        remediated_resources: list[str]
        unchanged_violated_resources: list[str]

    def __init__(
        self,
        builder1: MCPReportBuilder,
        builder2: MCPReportBuilder,
    ):
        self._builder1 = builder1
        self._builder2 = builder2

    def compare_resource_reports(self) -> list[MCPResourceComparison]:
        report1 = self._builder1.build_resource_report()
        report2 = self._builder2.build_resource_report()
        result = []

        for res in report1:
            remediated = True
            for new_res in report2:
                if res['id'] == new_res['id']:
                    result.append(self._compare_resources(res, new_res))
                    remediated = False
                    break
            if remediated:
                result.append({
                    'id': res['id'],
                    'new_violated_rules': [],
                    'remediated_rules': [r['name'] for r in res['violated_rules']],
                    'unchanged_violated_rules': [],
                })

        return result


    @staticmethod
    def _compare_resources(
        one: dict,
        two: dict,
    ) -> MCPResourceComparison:
        violated_rules1 = {r['name'] for r in one['violated_rules']}
        violated_rules2 = {r['name'] for r in two['violated_rules']}

        remediated_rules = list(violated_rules1 - violated_rules2)
        new_violated_rules = list(violated_rules2 - violated_rules1)
        unchanged_violated_rules = list(violated_rules1 & violated_rules2)

        return {
            'id': one['id'],
            'new_violated_rules': new_violated_rules,
            'remediated_rules': remediated_rules,
            'unchanged_violated_rules': unchanged_violated_rules,
        }


    def compare_rules_reports(self) -> list[MCPRulesComparison]:
        report1 = self._builder1.build_rules_report()
        report2 = self._builder2.build_rules_report()
        result = []

        for rule in report1:
            v_r = set(rule['violated_resources'])
            remediated = True
            for new_rule in report2:
                if rule['name'] == new_rule['name']:
                    n_v_r = set(new_rule['violated_resources'])
                    result.append(
                        {
                            'name': rule['name'],
                            'new_violated_resources': list(n_v_r - v_r),
                            'remediated_resources': list(v_r - n_v_r),
                            'unchanged_violated_resources': list(v_r & n_v_r),
                        }
                    )
                    remediated = False
                    break
            if remediated:
                result.append(
                    {
                        'name': rule['name'],
                        'new_violated_resources': [],
                        'remediated_resources': rule['violated_resources'],
                        'unchanged_violated_resources': [],
                    }
                )
        return result


class MCPReportHandler(AbstractHandler):
    def __init__(
        self,
        job_service: JobService,
        tenant_service: TenantService,
        report_service: ReportService,
        platform_service: PlatformService,
        license_service: LicenseService,
    ):
        self._job_service = job_service
        self._tenant_service = tenant_service
        self._report_service = report_service
        self._platform_service = platform_service
        self._ls = license_service

    @property
    def rs(self):
        return self._report_service

    @classmethod
    def build(cls):
        return cls(
            job_service=SP.job_service,
            tenant_service=SP.modular_client.tenant_service(),
            report_service=SP.report_service,
            platform_service=SP.platform_service,
            license_service=SP.license_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.MCP_REPORTS_JOBS_JOB_ID: {
                HTTPMethod.GET: self.mcp_get_specific_job
            },
            Endpoint.MCP_REPORTS_COMPARE_JOBS: {
                HTTPMethod.GET: self.mcp_compare_jobs
            },
        }


    @validate_kwargs
    def mcp_get_specific_job(
        self,
        event: MCPReportJobGetModel,
        job_id: str,
    ):
        job = self._ensure_job(job_id)

        tenant = self._tenant_service.get(job.tenant_name)
        tenant = modular_helpers.assert_tenant_valid(tenant, event.customer)

        metadata = self._ls.get_customer_metadata(event.customer_id)

        builder = self._get_builder(job, tenant, metadata, event.top)

        if event.type == MCPReportType.RESOURCES:
            result = builder.build_resource_report()
        else:
            result = builder.build_rules_report()

        return build_response(content=result)


    @validate_kwargs
    def mcp_compare_jobs(
        self,
        event: MCPReportCompareJobsGetModel,
    ):
        job1 = self._ensure_job(event.previous_job_id)
        job2 = self._ensure_job(event.current_job_id)

        if not self._comparable_jobs(job1, job2):
            return build_response(
                content='Jobs are not comparable', code=HTTPStatus.BAD_REQUEST
            )

        tenant = self._tenant_service.get(job1.tenant_name)
        tenant = modular_helpers.assert_tenant_valid(tenant, event.customer)

        metadata = self._ls.get_customer_metadata(event.customer_id)

        builder1 = self._get_builder(job1, tenant, metadata, event.top)
        builder2 = self._get_builder(job2, tenant, metadata)

        comparator = MCPReportComparator(builder1, builder2)

        if event.type == MCPReportType.RESOURCES:
            result = comparator.compare_resource_reports()
        else:
            result = comparator.compare_rules_reports()

        return build_response(content=result)


    def _get_builder(
        self,
        job: Job,
        tenant: Tenant,
        metadata: Metadata,
        top: Optional[int] = None,
    ) -> MCPReportBuilder:

        collection = self._report_service.job_collection(tenant, job)
        collection.fetch_all()

        collection.meta = self._report_service.fetch_meta(tenant)

        findings_iterator = iter_rule_resource(
            collection=collection,
            cloud=modular_helpers.tenant_cloud(tenant),
            metadata=metadata,
            account_id=tenant.project,
        )
        return MCPReportBuilder(
            findings_iterator=findings_iterator,
            entity=tenant,
            metadata=metadata,
            collection=collection,
            top=top,
        )


    @staticmethod
    def _ensure_job(job_id: str) -> Job:
        job = next(
            SP.job_service.get_by_job_types(
                job_id=job_id,
                job_types=None,
                customer_name=None,
            ),
            None,
        )

        if not job:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                f'Job {job_id} not found'
            ).exc()

        if job.is_platform_job:
            raise ResponseFactory(HTTPStatus.NOT_IMPLEMENTED).message(
                'Platform job is not available now'
            ).exc()

        return job


    @staticmethod
    def _comparable_jobs(
        job1: Job,
        job2: Job,
    ) -> bool:

        result = [
            job1.id != job2.id,
            job1.tenant_name == job2.tenant_name,
            job1.rulesets == job2.rulesets,
            utc_datetime(job1.submitted_at) < utc_datetime(job2.stopped_at)
        ]

        return all(result)
