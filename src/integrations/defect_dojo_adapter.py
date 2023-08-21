import copy
import csv
import io
import itertools
import json
import uuid
from base64 import b64encode
from datetime import timedelta, datetime
from typing import List, Dict

from helpers.constants import PRODUCT_TYPE_NAME_ATTR, PRODUCT_NAME_ATTR, \
    ENGAGEMENT_NAME_ATTR, \
    TEST_TITLE_ATTR
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from integrations import AbstractAdapter
from services.clients.dojo_client import DojoClient, DOJO_CAAS_SCAN_TYPE
from services.coverage_service import Standard

_LOG = get_logger(__name__)
NB_SPACE = '&nbsp;'  # non-breaking space
SIEM_DOJO_TYPE = 'dojo'
TIME_FORMAT = "%H:%M:%S"


class CustodianToDefectDojoEntitiesMapper:
    defaults = {
        PRODUCT_TYPE_NAME_ATTR: 'Custodian scan for {customer}',
        PRODUCT_NAME_ATTR: '{tenant}',
        ENGAGEMENT_NAME_ATTR: '{day_scope}',
        TEST_TITLE_ATTR: '{job_scope}: {job_id}'
    }

    class _SkipKeyErrorDict(dict):
        def __missing__(self, key):
            return '{' + key + '}'

    def __init__(self, mapping: dict, **kwargs):
        """The kwargs are used to substitute keys from the given mapping.
        Currently, a set of possible keys to substitute is not defined and
        depends only on the kwargs which were given."""
        self.mapping = mapping.copy()
        for key, value in self.defaults.items():
            self.mapping.setdefault(key, value)
        for key in set(self.mapping) - set(self.defaults):
            self.mapping.pop(key)
        self.dictionary = self._SkipKeyErrorDict(kwargs)

    def generate(self) -> dict:
        for key, value in self.mapping.items():
            self.mapping[key] = value.format_map(self.dictionary)
        return self.mapping


class DefectDojoAdapter(AbstractAdapter):
    siem_type = SIEM_DOJO_TYPE
    request_error = f'Request error occurred while uploading ' \
                    f'findings to {SIEM_DOJO_TYPE}.'

    def __init__(self, host: str, api_key: str, entities_mapping: dict = None,
                 display_all_fields: bool = False, upload_files: bool = False,
                 resource_per_finding: bool = False):
        self.entities_mapping = entities_mapping or {}
        self.display_all_fields = display_all_fields if isinstance(
            display_all_fields, bool) else False
        self.upload_files = upload_files if isinstance(upload_files,
                                                       bool) else False
        self.resource_per_finding = resource_per_finding if isinstance(
            resource_per_finding, bool) else False
        self.client = DojoClient(
            host=host,
            api_key=api_key,
        )
        super().__init__()

    def add_entity(self, job_id, job_type, started_at, stopped_at,
                   tenant_display_name, customer_display_name,
                   policy_reports: List[Dict]):

        _LOG.info(f'Jos:\'{job_id}\' - formatting compatible policy reports.')
        formatted_findings = self.format_policy_report_list(
            policy_report_list=policy_reports
        )
        # formatted_findings = self.policy_report_to_generic(policy_reports)

        started_at_obj = utc_datetime(_from=started_at)
        stopped_at_obj = utc_datetime(_from=stopped_at) if \
            stopped_at else utc_datetime()

        job_scope = f'{started_at_obj.strftime(TIME_FORMAT)} - ' \
                    f'{stopped_at_obj.strftime(TIME_FORMAT)}'
        stopped_at_date_obj = stopped_at_obj.date()
        day_scope = \
            f'{stopped_at_date_obj.isoformat()} - ' \
            f'{(stopped_at_date_obj + timedelta(days=1)).isoformat()}'
        self._entities.append({
            'job_id': job_id,  # not for dojo but for our code
            'job_type': job_type,
            'scan_date': stopped_at_date_obj.isoformat(),
            'formatted_findings': formatted_findings,
            **CustodianToDefectDojoEntitiesMapper(
                self.entities_mapping,
                customer=customer_display_name,
                tenant=tenant_display_name,
                day_scope=day_scope,
                job_scope=job_scope,
                job_id=job_id).generate()
        })

    def policy_report_to_generic(self, policy_reports: List[Dict]) -> Dict:
        # TODO get meta from mappings
        findings = []
        for report in policy_reports:
            vuln_id = report['vuln_id_from_tool']
            _len = report.get('resources #')
            if not _len:
                _LOG.debug(f'Skipping policy {vuln_id} because '
                           f'no resources violate it')
                continue
            resources_str = self.make_markdown_table(
                report['resources'], report['report_fields']
            )
            findings.append({
                'title': report.get('description') or 'No title',
                'date': datetime.now().date().isoformat(),
                'description': f'{report["article"]}\nNumber of resources found: ' \
                               f'**{_len}**\n\n{resources_str}',
                'severity': (report.get('severity') or 'Medium').title(),
                'mitigation': report['remediation'],
                'impact': report.get('impact'),
                'references': '\n'.join(
                    sorted(st.name for st in
                           Standard.deserialize(report.get('standard') or {}))
                ),
                'service': report["service"],
                'tags': report.get('tags') or [],
                'vuln_id_from_tool': vuln_id,
            })
        return {'findings': findings}

    def upload(self, engagement_name, product_name, product_type_name,
               formatted_findings, test_title=None, job_id=None,
               scan_date=None, job_type=None, scan_type=DOJO_CAAS_SCAN_TYPE):

        _LOG.info(f'Importing \'{job_id}\' job of {job_type} type '
                  f'with \'{test_title}\' DefectDojo test')
        formatted_report_buffer = io.BytesIO(
            json.dumps(formatted_findings, sort_keys=True, separators=(",", ":")).encode()
        )
        self.client.import_scan(
            product_type_name=product_type_name,
            product_name=product_name,
            engagement_name=engagement_name,
            test_title=test_title,
            reimport=True,
            buffer=formatted_report_buffer,
            scan_date=scan_date,
            scan_type=scan_type
        )
        formatted_report_buffer.close()

    def upload_all_entities(self):
        # _LOG.info('Creating Dojo classes for entities synchronously')
        # batches = set((e.get('product_type_name'), e.get('product_name'),
        #                e.get('engagement_name'), e.get('scan_date'))
        #               for e in self._entities)
        # try:
        #     for batch in batches:
        #         ptn, prn, en, sd = batch
        #         _LOG.info(f'Creating context for {prn} of {ptn} type'
        #                   f' with engagement name of {en} of {sd} date.')
        #         self.client.create_context(product_type_name=batch[0],
        #                                    product_name=batch[1],
        #                                    engagement_name=batch[2],
        #                                    scan_date=batch[3])
        # except requests.RequestException as e:
        #     _LOG.error(f'An error occurred while creating Dojo context - {e}')
        #     return [
        #         self.result(job_id='ALL', job_type='ALL', error=str(e))
        #     ]
        # _LOG.info('Necessary Dojo classes were created. Importing findings')
        return super().upload_all_entities()

    def format_policy_report_list(
            self, policy_report_list: List[Dict]
    ) -> List[Dict]:
        """
        Returns EPAM`s Defect Dojo compatible findings-object, derived
        out of policy report(s), each of which is either resource-specific or
        accumulated - based on the one_res_per_finding.
        :param policy_report_list: List[Dict]
        :return: List[Dict]
        """
        findings = []

        one_res_per_finding = self.resource_per_finding
        helper_keys = ('report_fields',)
        for policy_report in policy_report_list:
            vuln_id = policy_report.get('vuln_id_from_tool')  # policy-name.

            helper_values = []
            # Removes helper key-value pairs.
            for key in helper_keys:
                value = None
                if key in policy_report:
                    value = policy_report.pop(key)
                helper_values.append(value)

            report_fields, *_ = helper_values
            report_fields = report_fields or []

            if not report_fields and not self.display_all_fields:
                _LOG.warning(
                    f'Policy:\'{vuln_id}\' maintains no \'report_fields\' '
                    f'- going to default reporting all keys.'
                )

            resources = []
            for _resource in policy_report.get('resources', []):
                resource = {}
                if not self.display_all_fields and report_fields:
                    for field in report_fields:
                        resource[field] = _resource.get(field)
                else:
                    resource = _resource

                if resource:
                    resources.append(resource)

            resource_type = policy_report.get('service')

            # v3.3.1 Date, Description is self set-up on the DefectDojo side.
            # todo table-markdown may not be required, as it given within
            #  `description`, which may be used, for non-multi-regional titles.

            # todo Consider 'resources': List[str of rows of a resources table]

            if one_res_per_finding:

                for resource in resources:
                    _report = copy.deepcopy(policy_report)
                    _report['resources'] = [resource]
                    _report['resources #'] = 1

                    if self.upload_files:
                        _report['files'] = {
                            'title': f'{resource_type}-{str(uuid.uuid4())[:8]}'
                                     f'.json',
                            'data': b64encode(json.dumps(resource).encode()).
                            decode()
                        }

                    if 'Arn' in resource:
                        _report["component_name"] = resource["Arn"]

                    elif 'component_name' in _report:
                        del _report["component_name"]

                    findings.append(_report)

            else:
                policy_report['resources'] = resources
                if self.upload_files:
                    policy_report['files'] = [
                        {
                            'title': f'{resource_type}-{str(uuid.uuid4())[:8]}'
                                     f'.json',
                            'data': b64encode(json.dumps(resource).encode()).
                            decode()
                        }
                        for resource in resources
                    ]

                findings.append(policy_report)

        return findings

    def make_markdown_table(self, detailed_resources, report_fields):
        from pytablewriter import MarkdownTableWriter
        display_all_fields = self.display_all_fields
        try:

            key_report = report_fields[0] if report_fields else None
            if key_report:
                detailed_resources = sorted(detailed_resources,
                                            key=lambda d: d[key_report])
        except KeyError:
            _LOG.warning(f'Invalid detailed_report.json!!! '
                         f'Fields: {report_fields}, '
                         f'resources :{detailed_resources}')
            display_all_fields = True

        report_fields = set(report_fields) if report_fields and not \
            display_all_fields else set(
            itertools.chain.from_iterable(
                resource.keys() for resource in detailed_resources
            )
        )
        resources = [
            {f'{field[0].upper() + field[1:]}{NB_SPACE}': f'{value}{NB_SPACE}'
             for field, value in resource.items() if field in report_fields
             and isinstance(value, (str, int, float))}
            for resource in detailed_resources
        ]

        try:
            fieldnames = sorted(set(itertools.chain.from_iterable(
                resource.keys() for resource in resources
            )))
            table_writer = MarkdownTableWriter()
            with io.StringIO() as buffer:
                dict_writer = csv.DictWriter(buffer, fieldnames)
                dict_writer.writeheader()
                dict_writer.writerows(resources)
                table_writer.from_csv(buffer.getvalue())
            table_str = table_writer.dumps()
        except Exception as e:
            _LOG.warning(f'Something went wrong while making resources table '
                         f'for DefectDojo\'s finding: {e}. Dumping resources '
                         f'to JSON')
            table_str = json.dumps(
                resources, sort_keys=True, indent=4).translate(
                {ord(c): None for c in '{[]}",'}
            )
        return table_str
