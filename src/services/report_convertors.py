import csv
import io
from abc import ABC, abstractmethod
from base64 import b64encode
from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Literal, TypedDict

import msgspec
from typing_extensions import NotRequired
from xlsxwriter.workbook import Workbook

from helpers import filter_dict, hashable
from helpers.constants import REPORT_FIELDS
from services.metadata import Metadata
from services.xlsx_writer import CellContent, Table, XlsxRowsWriter

if TYPE_CHECKING:
    from services.sharding import ShardsCollection


class ShardCollectionConvertor(ABC):
    def __init__(self, metadata: Metadata) -> None:
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
        cls, scan_type: str, metadata: Metadata, **kwargs
    ) -> 'ShardCollectionDojoConvertor':
        """
        Returns a generic dojo convertor by default
        :param scan_type:
        :param kwargs:
        :return:
        """
        match scan_type:
            case 'Generic Findings Import':
                return ShardsCollectionGenericDojoConvertor(metadata, **kwargs)
            case 'Cloud Custodian Scan':
                return ShardsCollectionCloudCustodianDojoConvertor(
                    metadata, **kwargs
                )
            case _:
                return ShardsCollectionGenericDojoConvertor(metadata, **kwargs)


# for generic dojo parser
class FindingFile(TypedDict):
    title: str
    data: str


class Finding(TypedDict):
    title: str
    date: str  # when discovered, iso
    severity: str  # Info, Low, Medium, High, Critical. Info, if we don't know
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
        super().__init__(metadata)
        self._attachment = attachment

    @staticmethod
    def _make_table(resources: list[dict]) -> str:
        """
        In case resource have arn, we don't show id and name and namespace
        (cause arn can be really long and can break dojo description),
        otherwise -> id, name, namespace
        :param resources:
        :return:
        """
        from tabulate import tabulate

        if resources[0].get('arn'):  # can be sure IndexError won't occur
            # all resources within a table are similar
            headers = ('arn',)
        else:  # id name, namespace
            headers = ('id', 'name', 'namespace')

        return tabulate(
            tabular_data=[[res.get(h) for h in headers] for res in resources],
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
    def _make_json_file(resources: list[dict]) -> str:
        """
        Dumps resources to json and encodes to base64 as dojo expects
        :return:
        """
        return b64encode(msgspec.json.encode(resources)).decode()

    @staticmethod
    def _make_xlsx_file(resources: list[dict]) -> str:
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
                    table.add_cells(CellContent(r.get(h)))

            wsh = wb.add_worksheet('resources')
            XlsxRowsWriter().write(wsh, table)
        return b64encode(buffer.getvalue()).decode()

    @staticmethod
    def _make_csv_file(resources: list[dict]) -> str:
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
                res.get('arn'),
                res.get('id'),
                res.get('name'),
                res.get('namespace'),
            )
            for i, res in enumerate(resources, 1)
        )

        return b64encode(buffer.getvalue().encode()).decode()

    def convert(self, collection: 'ShardsCollection') -> Findings:
        findings = []
        meta = collection.meta

        for part in collection.iter_parts():
            if not part.resources:
                continue
            pm = meta.get(part.policy) or {}  # part meta
            p = part.policy
            pm2 = self.meta.rule(
                p, comment=pm.get('comment'), resource=pm.get('resource')
            )

            # tags
            tags = [part.location, pm.get('resource')]
            if service_section := pm2.service_section:
                tags.append(service_section)

            match self._attachment:
                case 'xlsx':
                    extra = {
                        'description': pm2.article,
                        'files': [
                            {
                                'title': f'{p}.xlsx',
                                'data': self._make_xlsx_file(part.resources),
                            }
                        ],
                    }
                case 'json':
                    extra = {
                        'description': pm2.article,
                        'files': [
                            {
                                'title': f'{p}.json',
                                'data': self._make_json_file(part.resources),
                            }
                        ],
                    }
                case 'csv':
                    extra = {
                        'description': pm2.article,
                        'files': [
                            {
                                'title': f'{p}.csv',
                                'data': self._make_csv_file(part.resources),
                            }
                        ],
                    }
                case _:  # None or some unexpected
                    table = self._make_table(part.resources)
                    extra = {'description': f'{pm2.article}\n{table}'}

            findings.append(
                {
                    'title': pm['description'] if 'description' in pm else p,
                    'date': datetime.fromtimestamp(
                        part.timestamp, tz=timezone.utc
                    ).isoformat(),
                    'severity': pm2.severity.value,
                    'mitigation': pm2.remediation,
                    'impact': pm2.impact,
                    'references': self._make_references(pm2.standard),
                    'tags': tags,
                    'vuln_id_from_tool': p,
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
        self, metadata: Metadata, resource_per_finding: bool = False, **kwargs
    ):
        super().__init__(metadata)
        self._rpf = resource_per_finding

    @staticmethod
    def _convert_standards(standards: dict) -> dict:
        res = {}
        for name in standards:
            for version in standards[name]:
                res.setdefault(name, []).append(version)
        return res

    @staticmethod
    def _prepare_resources(resources: list[dict]) -> list[dict]:
        """
        Keeps only report fields and sorts by
        :param resources:
        :return:
        """
        skey = 'id'
        ftr = partial(filter_dict, keys=REPORT_FIELDS)
        return sorted(
            map(ftr, resources), key=lambda r: r.get(skey) or chr(123)
        )

    def convert(self, collection: 'ShardsCollection') -> list[Model]:
        result = []
        meta = collection.meta

        for part in collection.iter_parts():
            if not part.resources:
                continue
            rule = part.policy
            pm = self.meta.rule(
                rule,
                comment=meta.get(rule, {}).get('comment'),
                resource=meta.get(rule, {}).get('comment'),
            )
            base = {
                'description': meta.get(rule, {}).get('description'),
                'remediation': pm.remediation,
                'impact': pm.impact,
                'severity': pm.severity.value,
                'standard': self._convert_standards(pm.standard),
                'article': pm.article,
                'service': pm.service,
                'vuln_id_from_tool': rule,
                'tags': [part.location],
            }
            if self._rpf:
                for res in part.resources:
                    result.append(
                        {**base, 'resources': filter_dict(res, REPORT_FIELDS)}
                    )
            else:
                base['resources'] = self._prepare_resources(part.resources)
                result.append(base)
        return result


class ShardsCollectionDigestConvertor(ShardCollectionConvertor):
    class DigestsReport(TypedDict):
        total_checks: int
        successful_checks: int
        failed_checks: dict
        violating_resources: int

    def convert(self, collection: 'ShardsCollection') -> DigestsReport:
        total_checks = 0
        successful_checks = 0
        total_resources = set()
        failed_checks = {'total': 0}
        failed_by_severity = {}
        for part in collection.iter_parts():
            total_checks += 1
            sev = self.meta.rule(part.policy).severity.value
            if part.resources:
                failed_checks['total'] += 1
                failed_by_severity.setdefault(sev, 0)
                failed_by_severity[sev] += 1
            else:
                successful_checks += 1
            keep_report_fields = partial(filter_dict, keys=REPORT_FIELDS)
            total_resources.update(
                map(hashable, map(keep_report_fields, part.resources))
            )
        failed_checks['severity'] = failed_by_severity
        return {
            'total_checks': total_checks,
            'successful_checks': successful_checks,
            'failed_checks': failed_checks,
            'violating_resources': len(total_resources),
        }


class ShardsCollectionDetailsConvertor(ShardCollectionConvertor):
    def __init__(self) -> None:
        pass

    def convert(self, collection: 'ShardsCollection') -> dict[str, list[dict]]:
        res = {}
        for part in collection.iter_parts():
            res.setdefault(part.location, []).append(
                {
                    'policy': {
                        'name': part.policy,
                        **(collection.meta.get(part.policy) or {}),
                    },
                    'resources': part.resources,
                }
            )
        return res


class ShardsCollectionFindingsConvertor(ShardCollectionConvertor):
    def __init__(self) -> None:
        pass

    def convert(self, collection: 'ShardsCollection') -> dict[str, dict]:
        """
        Can't be two parts with the same policy and region
        :param collection:
        :return:
        """
        res = {}
        meta = collection.meta
        for part in collection.iter_parts():
            inner = res.setdefault(
                part.policy, {'resources': {}, **(meta.get(part.policy) or {})}
            )
            inner['resources'][part.location] = part.resources
        return res
