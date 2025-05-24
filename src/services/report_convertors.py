import csv
import io
from abc import ABC, abstractmethod
from base64 import b64encode
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Literal, TypedDict

import msgspec
from typing_extensions import NotRequired
from xlsxwriter.workbook import Workbook

from helpers.constants import Cloud, Severity
from services.metadata import Metadata
from services.resources import (
    CloudResource,
    InPlaceResourceView,
    iter_rule_region_resources,
    iter_rule_resource_region_resources,
)
from services.xlsx_writer import CellContent, Table, XlsxRowsWriter

if TYPE_CHECKING:
    from services.sharding import ShardsCollection


class ShardCollectionConvertor(ABC):
    def __init__(self, cloud: Cloud, metadata: Metadata) -> None:
        self.cloud = cloud
        self.meta = metadata

    @abstractmethod
    def convert(self, collection: 'ShardsCollection') -> dict | list:
        """
        Must convert the given shards collection to some other report
        :param collection:
        :return:
        """


class ShardCollectionDojoConvertor(ShardCollectionConvertor):
    """
    Subclass only for defect dojo convertors
    """

    @abstractmethod
    def convert(self, collection: 'ShardsCollection') -> dict | list: ...

    @classmethod
    def from_scan_type(
        cls, scan_type: str, cloud: Cloud, metadata: Metadata, **kwargs
    ) -> 'ShardCollectionDojoConvertor':
        """
        Returns a generic dojo convertor by default
        """
        match scan_type:
            case 'Generic Findings Import':
                return ShardsCollectionGenericDojoConvertor(
                    cloud, metadata, **kwargs
                )
            case 'Cloud Custodian Scan':
                return ShardsCollectionCloudCustodianDojoConvertor(
                    cloud, metadata, **kwargs
                )
            case _:
                return ShardsCollectionGenericDojoConvertor(
                    cloud, metadata, **kwargs
                )

    @staticmethod
    def to_dojo_severity(sev: Severity) -> Severity:
        if sev is Severity.UNKNOWN:
            return Severity.MEDIUM  # Why?.. Why not?
        return sev


# for generic dojo parser
class FindingFile(TypedDict):
    title: str
    data: str


class Finding(TypedDict):
    title: str
    date: str  # when discovered, iso
    severity: str  # Info, Low, Medium, High, Critical. Medium, if we don't know
    description: str
    mitigation: str | None
    impact: str | None
    references: str  # standards vs mitre?
    tags: list[str]
    vuln_id_from_tool: str  # rule id
    service: str  # service
    files: NotRequired[list[FindingFile]]


class Findings(TypedDict):
    findings: list[Finding]


class ShardsCollectionGenericDojoConvertor(ShardCollectionDojoConvertor):
    def __init__(
        self,
        cloud: Cloud,
        metadata: Metadata,
        attachment: Literal['json', 'xlsx', 'csv'] | None = None,
        **kwargs,
    ):
        """
        In case attachment is provided, findings data will be attached as file
        in that format. Otherwise, table will be drawn directly inside
        description
        :param attachment:
        :param kwargs:
        """
        super().__init__(cloud, metadata)
        self._attachment = attachment

    @staticmethod
    def _make_table(resources: list[CloudResource]) -> str:
        """
        In case resource have arn, we don't show id and name and namespace
        (cause arn can be really long and can break dojo description),
        otherwise -> id, name, namespace
        :param resources:
        :return:
        """
        from tabulate import tabulate

        r = resources[0]
        if getattr(r, 'arn', None):
            headers = ('arn', 'id', 'name')
        elif getattr(r, 'urn', None):
            headers = ('urn', 'id', 'name')
        elif getattr(r, 'namespace', None):
            headers = ('namespace', 'id', 'name')
        else:
            headers = ('id', 'name')

        return tabulate(
            tabular_data=[
                [getattr(res, h, None) for h in headers] for res in resources
            ],
            headers=map(str.title, headers),  # type: ignore
            tablefmt='rounded_grid',
            stralign='center',
            numalign='center',
            showindex='always',
            missingval='-',
            disable_numparse=True,
        )

    @staticmethod
    def _make_references(standards: dict) -> str:
        data = bytearray(b'#### Standards\n')
        for name in standards:
            for version in standards[name]:
                data.extend(f'* {name} **{version}**\n'.encode())
        # TODO: check and fix null version
        # TODO: add mitre here
        return data.decode('utf-8')

    @staticmethod
    def _make_json_file(resources: list[CloudResource]) -> str:
        """
        Dumps resources to json and encodes to base64 as dojo expects
        :return:
        """
        view = InPlaceResourceView(full=True)

        return b64encode(
            msgspec.json.encode([r.accept(view) for r in resources])
        ).decode()

    @staticmethod
    def _make_xlsx_file(resources: list[CloudResource]) -> str:
        """
        Dumps resources to xlsx file and encodes to base64 as dojo expects
        :param resources:
        :return:
        """

        buffer = io.BytesIO()

        with Workbook(buffer) as wb:
            bold = wb.add_format({'bold': True})
            table = Table()
            table.new_row()
            headers = ('arn', 'id', 'name', 'namespace')
            for h in ('№',) + headers:
                table.add_cells(CellContent(h.title(), bold))
            for i, r in enumerate(resources, 1):
                table.new_row()
                table.add_cells(CellContent(i))
                for h in headers:
                    table.add_cells(CellContent(getattr(r, h, None)))

            wsh = wb.add_worksheet('resources')
            XlsxRowsWriter().write(wsh, table)
        return b64encode(buffer.getvalue()).decode()

    @staticmethod
    def _make_csv_file(resources: list[CloudResource]) -> str:
        """
        Dumps resources to csv file and encodes to base64 as dojo expects
        :param resources:
        :return:
        """
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(('№', 'Arn', 'Id', 'Name', 'Namespace'))

        writer.writerows(
            (
                i,
                getattr(res, 'arn', None) or getattr(res, 'urn', None) or None,
                res.id,
                res.name,
                getattr(res, 'namespace', None),
            )
            for i, res in enumerate(resources, 1)
        )

        return b64encode(buffer.getvalue().encode()).decode()

    def convert(self, collection: 'ShardsCollection') -> Findings:
        findings = []
        meta = collection.meta

        it = iter_rule_resource_region_resources(
            collection=collection, cloud=self.cloud, metadata=self.meta
        )

        for rule, region, resources in it:
            if not resources:
                continue
            pm = meta.get(rule) or {}  # part meta
            pm2 = self.meta.rule(
                rule, comment=pm.get('comment'), resource=pm.get('resource')
            )

            # tags
            tags = [region, pm.get('resource')]
            if service_section := pm2.service_section:
                tags.append(service_section)

            match self._attachment:
                case 'xlsx':
                    extra = {
                        'description': pm2.article,
                        'files': [
                            {
                                'title': f'{rule}.xlsx',
                                'data': self._make_xlsx_file(resources),
                            }
                        ],
                    }
                case 'json':
                    extra = {
                        'description': pm2.article,
                        'files': [
                            {
                                'title': f'{rule}.json',
                                'data': self._make_json_file(resources),
                            }
                        ],
                    }
                case 'csv':
                    extra = {
                        'description': pm2.article,
                        'files': [
                            {
                                'title': f'{rule}.csv',
                                'data': self._make_csv_file(resources),
                            }
                        ],
                    }
                case _:  # None or some unexpected
                    table = self._make_table(resources)
                    extra = {'description': f'{pm2.article}\n{table}'}

            findings.append(
                {
                    'title': pm['description']
                    if 'description' in pm
                    else rule,
                    'date': datetime.fromtimestamp(
                        resources[0].sync_date, tz=timezone.utc
                    ).isoformat(),
                    'severity': self.to_dojo_severity(pm2.severity).value,
                    'mitigation': pm2.remediation,
                    'impact': pm2.impact,
                    'references': self._make_references(pm2.standard),
                    'tags': tags,
                    'vuln_id_from_tool': rule,
                    'service': pm2.service,
                    **extra,
                }
            )
        return {'findings': findings}


class ShardsCollectionCloudCustodianDojoConvertor(
    ShardCollectionDojoConvertor
):
    """
    Converts existing shards collection to the format that is accepted by
    Cloud Custodian dojo parser
    """

    class Model(TypedDict):
        """
        Parser expects a list of such items
        """

        description: str
        resources: list[dict]
        remediation: str | None
        impact: str | None
        standard: dict
        severity: str | None
        article: str | None
        service: str | None
        vuln_id_from_tool: str | None
        tags: list[str]

    def __init__(
        self,
        cloud: Cloud,
        metadata: Metadata,
        resource_per_finding: bool = False,
        **kwargs,
    ):
        super().__init__(cloud, metadata)
        self._rpf = resource_per_finding

    @staticmethod
    def _convert_standards(standards: dict) -> dict:
        res = {}
        for name in standards:
            for version in standards[name]:
                res.setdefault(name, []).append(version)
        return res

    def convert(self, collection: 'ShardsCollection') -> list[Model]:
        result = []
        meta = collection.meta
        view = InPlaceResourceView(full=False)

        it = iter_rule_resource_region_resources(
            collection=collection, cloud=self.cloud, metadata=self.meta
        )
        for rule, region, resources in it:
            if not resources:
                continue
            pm = self.meta.rule(
                rule,
                comment=meta.get(rule, {}).get('comment'),
                resource=meta.get(rule, {}).get('comment'),
            )
            base = {
                'description': meta.get(rule, {}).get('description'),
                'remediation': pm.remediation,
                'impact': pm.impact,
                'severity': self.to_dojo_severity(pm.severity.value).value,
                'standard': self._convert_standards(pm.standard),
                'article': pm.article,
                'service': pm.service,
                'vuln_id_from_tool': rule,
                'tags': [region],
            }
            if self._rpf:
                for res in resources:
                    result.append({**base, 'resources': res.accept(view)})
            else:
                base['resources'] = sorted(
                    (res.accept(view) for res in resources),
                    key=lambda r: r.get('id') or chr(123),
                )
                result.append(base)
        return result


class ShardsCollectionDigestConvertor(ShardCollectionConvertor):
    class DigestsReport(TypedDict):
        total_checks: int
        successful_checks: int
        failed_checks: dict
        violating_resources: int

    def convert(self, collection: 'ShardsCollection') -> DigestsReport:
        total = 0  # total number of rules checked
        successful = 0  # number of rules that found nothing
        by_severity = {}
        total_resources = set()

        it = iter_rule_region_resources(
            collection, self.cloud, self.meta, ''
        )
        for rule, _, resources in it:
            total += 1
            sev = self.meta.rule(rule).severity.value
            resources = tuple(resources)
            if resources:
                by_severity.setdefault(sev, 0)
                by_severity[sev] += 1
            else:
                successful += 1

            total_resources.update(resources)
        return {
            'total_checks': total,
            'successful_checks': successful,
            'failed_checks': {
                'severity': by_severity,
                'total': sum(by_severity.values()),
            },
            'violating_resources': len(total_resources),
        }


class ShardsCollectionDetailsConvertor(ShardCollectionConvertor):
    def __init__(self, cloud: Cloud) -> None:
        self.cloud = cloud

    def convert(self, collection: 'ShardsCollection') -> dict[str, list[dict]]:
        result = {}
        it = iter_rule_region_resources(collection, self.cloud)
        view = InPlaceResourceView(full=True)
        for rule, region, resources in it:
            result.setdefault(region, []).append(
                {
                    'policy': {'name': rule, **collection.meta[rule]},
                    'resources': [r.accept(view) for r in resources],
                }
            )
        return result


class ShardsCollectionFindingsConvertor(ShardCollectionConvertor):
    def __init__(self, cloud: Cloud) -> None:
        self.cloud = cloud

    def convert(self, collection: 'ShardsCollection') -> dict[str, dict]:
        """
        Can't be two parts with the same policy and region
        :param collection:
        :return:
        """
        view = InPlaceResourceView(full=True)
        result = {}
        meta = collection.meta

        it = iter_rule_region_resources(collection, self.cloud)
        for rule, region, resources in it:
            inner = result.setdefault(rule, {'resources': {}, **meta[rule]})
            inner['resources'][region] = [r.accept(view) for r in resources]
        return result
