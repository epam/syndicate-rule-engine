import uuid
from datetime import timedelta
from http import HTTPStatus
from typing import cast

import msgspec.json
from botocore.exceptions import ClientError
from dateutil.relativedelta import relativedelta
from modular_sdk.models.tenant import Tenant
from modular_sdk.modular import Modular
from typing_extensions import NotRequired, TypedDict

from handlers import AbstractHandler, Mapping
from helpers.constants import (
    CustodianEndpoint,
    HTTPMethod,
    RabbitCommand,
    ReportType,
    Cloud,
)
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime, utc_iso
from models.metrics import ReportMetrics
from services import SP, modular_helpers
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.rabbitmq_service import RabbitMQService
from services.rbac_service import TenantsAccessPayload
from services.platform_service import PlatformService
from services.reports import ReportMetricsService, add_diff
from validators.swagger_request_models import (
    CLevelGetReportModel,
    OperationalGetReportModel,
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)

SRE_REPORTS_TYPE_TO_M3_MAPPING = {
    # Operational
    ReportType.OPERATIONAL_RESOURCES: 'CUSTODIAN_RESOURCES_REPORT',
    ReportType.OPERATIONAL_OVERVIEW: 'CUSTODIAN_OVERVIEW_REPORT',
    ReportType.OPERATIONAL_RULES: 'CUSTODIAN_RULES_REPORT',
    ReportType.OPERATIONAL_FINOPS: 'CUSTODIAN_FINOPS_REPORT',
    ReportType.OPERATIONAL_COMPLIANCE: 'CUSTODIAN_COMPLIANCE_REPORT',
    ReportType.OPERATIONAL_ATTACKS: 'CUSTODIAN_ATTACKS_REPORT',
    ReportType.OPERATIONAL_KUBERNETES: 'CUSTODIAN_K8S_CLUSTER_REPORT',
    # C-Level
    ReportType.C_LEVEL_OVERVIEW: 'CUSTODIAN_CUSTOMER_OVERVIEW_REPORT',
}


class MaestroReport(TypedDict):
    receivers: list[str] | tuple[str, ...]
    report_type: str
    customer: str
    # from
    to: str
    outdated_tenants: list
    externalData: bool
    externalDataKey: NotRequired[str]
    externalDataBucket: NotRequired[str]

    data: list | dict


class MaestroModelBuilder:
    """
    Keeps all the dirty logic of transforming our report metrics to the
    format that maestro expects. I believe this logic won't last long
    and something will be changed drastically soon so this class is kind of
    congested.
    """

    __slots__ = '_receivers', '_limit'

    @staticmethod
    def convert_to_old_rt(rt: ReportType) -> str:
        """
        Maestro still needs those
        """
        match rt:
            case ReportType.OPERATIONAL_RESOURCES:
                return 'RESOURCES'
            case ReportType.OPERATIONAL_OVERVIEW | ReportType.C_LEVEL_OVERVIEW:
                return 'OVERVIEW'
            case (
                ReportType.OPERATIONAL_COMPLIANCE
                | ReportType.C_LEVEL_COMPLIANCE
            ):
                return 'COMPLIANCE'
            case ReportType.OPERATIONAL_ATTACKS:
                return 'ATTACK_VECTOR'
            case ReportType.OPERATIONAL_RULES:
                return 'RULE'
            case ReportType.OPERATIONAL_FINOPS:
                return 'FINOPS'
            case ReportType.OPERATIONAL_KUBERNETES:
                return 'KUBERNETES'

    def __init__(self, receivers: tuple[str, ...] = (), size_limit: int = 0):
        self._receivers = receivers  # base receivers
        self._limit = size_limit  # size limit in bytes. 0 means no limit
        # TODO: implement this limit logic to send reports via s3

    @staticmethod
    def _operational_overview_custom(rep: ReportMetrics) -> dict:
        assert rep.type == ReportType.OPERATIONAL_OVERVIEW
        data = rep.data.as_dict()
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'activated_regions': data['activated_regions'],
            'last_scan_date': data['last_scan_date'],
            'data': {
                'total_scans': data['total_scans'],
                'failed_scans': data['failed_scans'],
                'succeeded_scans': data['succeeded_scans'],
                'resources_violated': data['resources_violated'],
                'regions_data': {
                    r: {'severity_data': d}
                    for r, d in data['regions_severity'].items()
                },
            },
        }

    @staticmethod
    def _operational_resources_custom(rep: ReportMetrics) -> dict:
        assert rep.type == ReportType.OPERATIONAL_RESOURCES
        data = rep.data.as_dict()
        result = []
        for item in data.get('data', []):
            rd = {}
            for region, res in item.pop('resources', {}).items():
                rd[region] = {'resources': res}
            item['regions_data'] = rd
            result.append(item)
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'activated_regions': data['activated_regions'],
            'last_scan_date': data['last_scan_date'],
            'data': result,
        }

    @staticmethod
    def _operational_rules_custom(rep: ReportMetrics) -> dict:
        assert rep.type == ReportType.OPERATIONAL_RULES
        data = rep.data.as_dict()
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'activated_regions': data['activated_regions'],
            'last_scan_date': data['last_scan_date'],
            'data': {
                'rules_data': data.get('data', []),
                'violated_resources_length': data['resources_violated'],
            },
        }

    @staticmethod
    def _operational_finops_custom(rep: ReportMetrics) -> dict:
        assert rep.type == ReportType.OPERATIONAL_FINOPS
        data = rep.data.as_dict()
        result = []
        for ss, rules in data['data'].items():
            for rule in rules:
                rule['regions_data'] = {
                    region: {'resources': resources}
                    for region, resources in rule.pop('resources', {}).items()
                }

            result.append({'service_section': ss, 'rules_data': rules})

        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'activated_regions': data['activated_regions'],
            'last_scan_date': data['last_scan_date'],
            'data': result,
        }

    @staticmethod
    def _operational_compliance_custom(rep: ReportMetrics) -> dict:
        assert rep.type == ReportType.OPERATIONAL_COMPLIANCE
        data = rep.data.as_dict()

        regions_data = [
            {
                'region': region,
                'standards_data': [
                    {'name': name, 'value': round(value * 100, 2)}
                    for name, value in standards.items()
                ],
            }
            for region, standards in data['data'].get('regions', {}).items()
        ]
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'activated_regions': data['activated_regions'],
            'last_scan_date': data['last_scan_date'],
            'data': {'regions_data': regions_data},
        }

    @staticmethod
    def _operational_attacks_custom(rep: ReportMetrics) -> dict:
        assert rep.type == ReportType.OPERATIONAL_ATTACKS
        data = rep.data.as_dict()
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'activated_regions': data['activated_regions'],
            'last_scan_date': data['last_scan_date'],
            'data': data['data'],
        }

    @staticmethod
    def _operational_k8s_custom(rep: ReportMetrics) -> dict:
        assert rep.type == ReportType.OPERATIONAL_KUBERNETES
        data = rep.data.as_dict()
        return {
            'tenant_name': data['tenant_name'],
            'last_scan_date': data['last_scan_date'],
            'cluster_id': rep.platform_id,
            'cloud': Cloud.AWS.value,  # TODO: get from tenant
            'region': data['region'],
            'data': {
                'policy_data': data['resources'],
                'mitre_data': data['mitre'],
                'compliance_data': [
                    {'name': name, 'value': round(cov * 100, 2)}
                    for name, cov in data['compliance'].items()
                ],
            },
        }

    def build_base(self, rep: ReportMetrics) -> MaestroReport:
        return {
            'receivers': self._receivers,
            'report_type': self.convert_to_old_rt(rep.type),
            'customer': rep.customer,
            'from': rep.start
            if rep.start
            else utc_iso(
                utc_datetime(rep.end) - timedelta(days=7)
            ),  # TODO: fix on maestro side. Not every report can have "from"
            'to': rep.end,
            'outdated_tenants': [],  # TODO: implement
            'externalData': False,
            'data': {},
        }

    def convert(self, rep: ReportMetrics) -> MaestroReport:
        base = self.build_base(rep)
        match rep.type:
            case ReportType.OPERATIONAL_OVERVIEW:
                custom = self._operational_overview_custom(rep)
            case ReportType.OPERATIONAL_RESOURCES:
                custom = self._operational_resources_custom(rep)
            case ReportType.OPERATIONAL_RULES:
                custom = self._operational_rules_custom(rep)
            case ReportType.OPERATIONAL_FINOPS:
                custom = self._operational_finops_custom(rep)
            case ReportType.OPERATIONAL_COMPLIANCE:
                custom = self._operational_compliance_custom(rep)
            case ReportType.OPERATIONAL_ATTACKS:
                custom = self._operational_attacks_custom(rep)
            case ReportType.OPERATIONAL_KUBERNETES:
                custom = self._operational_k8s_custom(rep)
            case _:
                raise NotImplementedError()
        base.update(custom)
        return base


class MaestroReportToS3Packer:
    """
    Holds logic how to compress some large reports to jsonl files specifically
    for Maestro
    """

    # actual Maestro RabbitMQ limit seems to be 5mb, but the data that we
    # send is converted/compressed somehow inside modular-sdk before being sent
    # to Maestro. So I put 4mb here (just in case)
    _default_size_limit = (2 << 19) * 4
    _encoder = msgspec.json.Encoder()

    __slots__ = '_s3', '_bucket', '_limit', '_mapping'

    def __init__(
        self,
        s3_client: S3Client,
        bucket: str,
        size_limit: int = _default_size_limit,
    ):
        self._s3 = s3_client
        self._bucket = bucket
        self._limit = size_limit
        self._mapping = {
            'KUBERNETES': self._pack_k8s,
            'FINOPS': self._pack_finops,
            'RESOURCES': self._pack_resources,
            'ATTACK_VECTOR': self._pack_attacks,
        }

    def _pack_k8s(self, data: dict):
        buf = bytearray()
        for line in data.get('policy_data', {}):
            resources = line.pop('resources', [])
            self._write_line(buf, line, b'policy')
            for resource in resources:
                self._write_line(buf, resource)
        for tactic in data.get('mitre_data', {}):
            techniques = tactic.pop('techniques_data', [])
            self._write_line(buf, tactic, b'tactic')
            for technique in techniques:
                resources = technique.pop('resources', [])
                self._write_line(buf, technique, b'technique')
                for resource in resources:
                    self._write_line(buf, resource)
        return buf

    def _pack_finops(self, data: list[dict]) -> bytearray:
        buf = bytearray()
        for line in data:
            rules = line.pop('rules_data', [])
            self._write_line(buf, line, b'service')
            for rule in rules:
                regions = rule.pop('regions_data', {})
                self._write_line(buf, rule, b'rule')
                for region, resources in regions.items():
                    self._write_line(buf, {'key': region}, b'region')
                    for resource in resources.get('resources', []):
                        self._write_line(buf, resource)
        return buf

    def _pack_resources(self, data: list[dict]) -> bytearray:
        buf = bytearray()
        for line in data:
            regions = line.pop('regions_data', {})
            self._write_line(buf, line, b'policy')
            for region, resources in regions.items():
                self._write_line(buf, {'key': region}, b'region')
                for resource in resources.get('resources', []):
                    self._write_line(buf, resource)
        return buf

    def _write_line(
        self, to: bytearray, data: dict | list, tag: bytes | None = None
    ) -> None:
        if tag:
            to.extend(tag)
        self._encoder.encode_into(data, to, len(to))
        to.extend(b'\n')

    def _pack_attacks(self, data: list[dict]) -> bytearray:
        buf = bytearray()
        # NOTE: maybe we should use temp file
        for tactic in data:
            techniques = tactic.pop('techniques_data', [])
            self._write_line(buf, tactic, b'tactic')

            for technique in techniques:
                regions = technique.pop('regions_data', {})
                self._write_line(buf, technique, b'technique')
                for region, resources in regions.items():
                    self._write_line(buf, {'key': region}, b'region')
                    for resource in resources.get('resources', []):
                        self._write_line(buf, resource)
        return buf

    def _is_too_big(self, data: dict | list) -> bool:
        return len(self._encoder.encode(data)) > self._limit

    def _write_to_s3(self, key: str, data: bytearray) -> None:
        self._s3.put_object(bucket=self._bucket, key=key, body=data)

    def pack(self, report: MaestroReport) -> MaestroReport:
        """
        This method has side effects: it can store some data to S3. It can
        change the given "report" dict in place and also returns it (just for
        convenience).
        """
        typ = report['report_type']
        if typ not in self._mapping:
            return report
        # typ in mapping
        data = report['data']
        if not self._is_too_big(data):
            return report
        _LOG.info(
            f'Report size is bigger than limit: {self._limit}. '
            f'Writing to S3'
        )
        customer = report['customer']
        key = f'{customer}/{str(uuid.uuid4())}.jsonl'

        buf = self._mapping[typ](data)
        try:
            self._write_to_s3(key, buf)
        except ClientError:
            _LOG.exception('Could not write packed report to s3')
            raise (
                ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE)
                .message('Could not send data to Maestro S3')
                .exc()
            )

        report['externalData'] = True
        report['externalDataKey'] = key
        report['externalDataBucket'] = self._bucket
        report['data'] = data.__class__()
        return report


class HighLevelReportsHandler(AbstractHandler):
    def __init__(
        self,
        report_metrics_service: ReportMetricsService,
        modular_client: Modular,
        environment_service: EnvironmentService,
        rabbitmq_service: RabbitMQService,
        assume_role_s3_client: S3Client,
        platform_service: PlatformService,
    ):
        self._rms = report_metrics_service
        self._mc = modular_client
        self._env = environment_service
        self._rmq = rabbitmq_service
        self._assume_role_s3 = assume_role_s3_client
        self._ps = platform_service

    @classmethod
    def build(cls) -> 'HighLevelReportsHandler':
        return cls(
            report_metrics_service=SP.report_metrics_service,
            modular_client=SP.modular_client,
            environment_service=SP.environment_service,
            rabbitmq_service=SP.rabbitmq_service,
            assume_role_s3_client=SP.assume_role_s3,
            platform_service=SP.platform_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.REPORTS_CLEVEL: {
                HTTPMethod.POST: self.post_c_level
            },
            CustodianEndpoint.REPORTS_OPERATIONAL: {
                HTTPMethod.POST: self.post_operational
            },
        }

    @validate_kwargs
    def post_c_level(self, event: CLevelGetReportModel):
        models = []
        rabbitmq = self._rmq.get_customer_rabbitmq(event.customer_id)
        if not rabbitmq:
            raise self._rmq.no_rabbitmq_response().exc()

        builder = MaestroModelBuilder(receivers=tuple(event.receivers))

        for typ in event.new_types:
            rep = self._rms.get_latest_for_customer(event.customer_id, typ)
            if not rep:
                _LOG.warning(f'Cannot find {typ} for {event.customer_id}')
                continue
            self._rms.fetch_data_from_s3(rep)

            previous = self._rms.get_latest_for_customer(
                customer=event.customer_id,
                type_=typ,
                till=utc_datetime() + relativedelta(months=-1),
            )
            if not previous:
                _LOG.info(
                    'Previous clevel report not found, diffs will be empty'
                )
                previous_data = {}
            else:
                self._rms.fetch_data_from_s3(previous)
                previous_data = previous.data.as_dict()
            current_data = rep.data.as_dict()
            for cl, data in current_data.items():
                add_diff(
                    data,
                    previous_data.get(cl, {}),
                    exclude=('total_scanned_tenants',),
                )
            base = builder.build_base(rep)
            base['data'] = current_data

            models.append(
                self._rmq.build_m3_json_model(
                    notification_type=SRE_REPORTS_TYPE_TO_M3_MAPPING[typ],
                    data=base,
                )
            )

        if not models:
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(
                    'No collected reports found to send. Update metrics first'
                )
                .exc()
            )
        code = self._rmq.send_to_m3(
            rabbitmq=rabbitmq, command=RabbitCommand.SEND_MAIL, models=models
        )
        if code != 200:
            raise (
                ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE)
                .message('Could not send message to RabbitMQ')
                .exc()
            )
        return build_response(
            code=HTTPStatus.ACCEPTED, content='Successfully sent'
        )

    @validate_kwargs
    def post_operational(
        self, event: OperationalGetReportModel, _tap: TenantsAccessPayload
    ):
        models = []
        rabbitmq = self._rmq.get_customer_rabbitmq(event.customer_id)
        if not rabbitmq:
            raise self._rmq.no_rabbitmq_response().exc()
        packer = MaestroReportToS3Packer(
            s3_client=self._assume_role_s3,
            bucket=self._env.get_recommendation_bucket(),
        )

        builder = MaestroModelBuilder(receivers=tuple(event.receivers))

        for tenant_name in event.tenant_names:
            if not _tap.is_allowed_for(tenant_name):
                raise (
                    ResponseFactory(HTTPStatus.FORBIDDEN)
                    .message(f'Action is forbidden for tenant {tenant_name}')
                    .exc()
                )
            tenant = self._mc.tenant_service().get(tenant_name)
            modular_helpers.assert_tenant_valid(tenant, event.customer)
            tenant = cast(Tenant, tenant)

            for typ in event.new_types:
                if typ is ReportType.OPERATIONAL_KUBERNETES:
                    _LOG.debug('Specific handling for k8s reports')
                    k8s_datas = []
                    for platform in self._ps.query_by_tenant(tenant):
                        rep = self._rms.get_latest_for_platform(platform, typ)
                        if not rep:
                            _LOG.warning(
                                f'Could not find data for platform {platform.id}'
                            )
                            continue
                        self._rms.fetch_data_from_s3(rep)
                        data = builder.convert(rep)
                        data = packer.pack(data)
                        k8s_datas.append(data)
                    if not k8s_datas:
                        _LOG.debug(
                            f'Could not find any {typ} for {tenant.name}'
                        )
                        raise (
                            ResponseFactory(HTTPStatus.NOT_FOUND)
                            .message(
                                f'Could not find any {typ} for {tenant.name}'
                            )
                            .exc()
                        )
                    models.extend(
                        self._rmq.build_m3_json_model(
                            notification_type=SRE_REPORTS_TYPE_TO_M3_MAPPING[
                                typ
                            ],
                            data=i,
                        )
                        for i in k8s_datas
                    )
                    continue

                _LOG.debug(f'Going to generate {typ} for {tenant.name}')
                rep = self._rms.get_latest_for_tenant(tenant=tenant, type_=typ)
                if not rep:
                    _LOG.debug(f'Could not find any {typ} for {tenant.name}')
                    raise (
                        ResponseFactory(HTTPStatus.NOT_FOUND)
                        .message(f'Could not find any {typ} for {tenant.name}')
                        .exc()
                    )
                self._rms.fetch_data_from_s3(rep)
                data = builder.convert(rep)
                data = packer.pack(data)

                models.append(
                    self._rmq.build_m3_json_model(
                        notification_type=SRE_REPORTS_TYPE_TO_M3_MAPPING[typ],
                        data=data,
                    )
                )

        if not models:
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(
                    'No collected reports found to send. Update metrics first'
                )
                .exc()
            )
        code = self._rmq.send_to_m3(
            rabbitmq=rabbitmq, command=RabbitCommand.SEND_MAIL, models=models
        )
        if code != 200:
            raise (
                ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE)
                .message('Could not send message to RabbitMQ')
                .exc()
            )
        return build_response(
            code=HTTPStatus.ACCEPTED, content='Successfully sent'
        )
