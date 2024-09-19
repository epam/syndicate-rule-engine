from abc import ABC, abstractmethod
from base64 import b64encode
import csv
from datetime import datetime, timezone
from functools import partial
import io
from typing import Literal, TYPE_CHECKING, TypedDict
from typing_extensions import NotRequired

from xlsxwriter.workbook import Workbook
import msgspec

from helpers import filter_dict, hashable
from helpers.constants import REPORT_FIELDS
from helpers.reports import NONE_VERSION, Standard, Severity
from services import SP
from services.xlsx_writer import CellContent, Table, XlsxRowsWriter
from services.mappings_collector import MappingsCollector

if TYPE_CHECKING:
    from services.sharding import ShardsCollection


class ShardCollectionConvertor(ABC):
    mc = SP.mappings_collector

    @abstractmethod
    def convert(self, collection: 'ShardsCollection'):
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
    def convert(self, collection: 'ShardsCollection'):
        ...

    @classmethod
    def from_scan_type(cls, scan_type: str, **kwargs
                       ) -> 'ShardCollectionDojoConvertor':
        """
        Returns a generic dojo convertor by default
        :param scan_type:
        :param kwargs:
        :return:
        """
        match scan_type:
            case 'Generic Findings Import':
                return ShardsCollectionGenericDojoConvertor(**kwargs)
            case 'Cloud Custodian Scan':
                return ShardsCollectionCloudCustodianDojoConvertor(**kwargs)
            case _:
                return ShardsCollectionGenericDojoConvertor(**kwargs)


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

    def __init__(self, attachment: Literal['json', 'xlsx', 'csv'] | None = None,
                 **kwargs):
        """
        In case attachment is provided, findings data will be attached as file
        in that format. Otherwise, table will be drawn directly inside
        description
        :param attachment:
        :param kwargs:
        """
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
            headers = ('arn', )
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
        for st in Standard.deserialize(standards):
            if st.version == NONE_VERSION:
                data.extend(f'* {st.name}\n'.encode())
            else:
                data.extend(f'* {st.name} **{st.version}**\n'.encode())
        # todo add mitre here
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
            for h in ('№', ) + headers:
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
            (i, res.get('arn'), res.get('id'), res.get('name'), res.get('namespace'))
            for i, res in enumerate(resources, 1)
        )

        return b64encode(buffer.getvalue().encode()).decode()

    def convert(self, collection: 'ShardsCollection') -> Findings:
        findings = []
        meta = collection.meta
        mc2 = MappingsCollector.build_from_sharding_collection_meta(collection.meta)

        human_data = self.mc.human_data
        severity = self.mc.severity
        standards = self.mc.standard or mc2.standard
        service = self.mc.service
        ss = self.mc.service_section or mc2.service_section

        for part in collection.iter_parts():
            if not part.resources:
                continue
            pm = meta.get(part.policy) or {}  # part meta
            p = part.policy

            # tags
            tags = [part.location, pm.get('resource')]
            if service_section := ss.get(p):
                tags.append(service_section)

            article = human_data.get(p, {}).get('article', '')

            match self._attachment:
                case 'xlsx':
                    extra = {
                        'description': article,
                        'files': [{
                            'title': f'{p}.xlsx',
                            'data': self._make_xlsx_file(part.resources)
                        }]
                    }
                case 'json':
                    extra = {
                        'description': article,
                        'files': [{
                            'title': f'{p}.json',
                            'data': self._make_json_file(part.resources)
                        }]
                    }
                case 'csv':
                    extra = {
                        'description': article,
                        'files': [{
                            'title': f'{p}.csv',
                            'data': self._make_csv_file(part.resources)
                        }]
                    }
                case _:  # None or some unexpected
                    table = self._make_table(part.resources)
                    extra = {'description': f'{article}\n{table}'}

            findings.append({
                'title': pm['description'] if 'description' in pm else p,
                'date': datetime.fromtimestamp(part.timestamp,
                                               tz=timezone.utc).isoformat(),
                'severity': severity.get(p) or Severity.INFO.value,
                'mitigation': human_data.get(p, {}).get('remediation'),
                'impact': human_data.get(p, {}).get('impact'),
                'references': self._make_references(standards.get(p, {})),
                'tags': tags,
                'vuln_id_from_tool': p,
                'service': service.get(p),
                **extra
            })
        return {'findings': findings}


class ShardsCollectionCloudCustodianDojoConvertor(ShardCollectionDojoConvertor):
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

    def __init__(self, resource_per_finding: bool = False, **kwargs):
        self._rpf = resource_per_finding

    @staticmethod
    def _convert_standards(standards: dict | None = None) -> dict:
        if not standards:
            return {}
        res = {}
        for item in Standard.deserialize(standards):
            res.setdefault(item.name, []).append(item.version)
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
        return sorted(map(ftr, resources),
                      key=lambda r: r.get(skey) or chr(123))

    def convert(self, collection: 'ShardsCollection') -> list[Model]:
        result = []
        meta = collection.meta
        mc2 = MappingsCollector.build_from_sharding_collection_meta(meta)
        human_data = self.mc.human_data
        severity = self.mc.severity
        standards = self.mc.standard or mc2.standard
        for part in collection.iter_parts():
            if not part.resources:
                continue
            rule = part.policy
            hd = human_data.get(rule) or {}
            base = {
                'description': meta.get(rule, {}).get('description'),
                'remediation': hd.get('remediation'),
                'impact': hd.get('impact'),
                'severity': severity.get(rule),
                'standard': self._convert_standards(standards.get(rule)),
                'article': hd.get('article'),
                'service': hd.get('service') or meta.get(rule, {}).get('resource'),
                'vuln_id_from_tool': rule,
                'tags': [part.location],
            }
            if self._rpf:
                for res in part.resources:
                    result.append({
                        **base,
                        'resources': filter_dict(res, REPORT_FIELDS)
                    })
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
        failed_checks = {
            'total': 0
        }
        failed_by_severity = {}
        severity = self.mc.severity
        for part in collection.iter_parts():
            total_checks += 1
            if part.resources:
                failed_checks['total'] += 1
                failed_by_severity.setdefault(severity.get(part.policy), 0)
                failed_by_severity[severity.get(part.policy)] += 1
            else:
                successful_checks += 1
            keep_report_fields = partial(filter_dict, keys=REPORT_FIELDS)
            total_resources.update(
                map(hashable, map(keep_report_fields, part.resources))
            )
        if None in failed_by_severity:
            failed_by_severity['Unknown'] = failed_by_severity.pop(None)
        failed_checks['severity'] = failed_by_severity
        return {
            'total_checks': total_checks,
            'successful_checks': successful_checks,
            'failed_checks': failed_checks,
            'violating_resources': len(total_resources)
        }


class ShardsCollectionDetailsConvertor(ShardCollectionConvertor):

    def convert(self, collection: 'ShardsCollection') -> dict[str, list[dict]]:
        res = {}
        for part in collection.iter_parts():
            res.setdefault(part.location, []).append({
                'policy': {
                    'name': part.policy,
                    **(collection.meta.get(part.policy) or {})
                },
                'resources': part.resources
            })
        return res


class ShardsCollectionFindingsConvertor(ShardCollectionConvertor):
    def convert(self, collection: 'ShardsCollection') -> dict[str, dict]:
        """
        Can't be two parts with the same policy and region
        :param collection:
        :return:
        """
        res = {}
        meta = collection.meta
        for part in collection.iter_parts():
            inner = res.setdefault(part.policy, {
                'resources': {},
                **(meta.get(part.policy) or {}),
            })
            inner['resources'][part.location] = part.resources
        return res
