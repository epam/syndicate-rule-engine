import copy
import io
import json
import uuid
from base64 import b64encode
from datetime import timedelta
from typing import List, Dict

from helpers.constants import PRODUCT_TYPE_NAME_ATTR, PRODUCT_NAME_ATTR, \
    ENGAGEMENT_NAME_ATTR, TEST_TITLE_ATTR
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from integrations.abstract_adapter import AbstractAdapter
from services.clients.dojo_client import DojoClient

_LOG = get_logger(__name__)

PRODUCT_TYPE_DTO_ATTRS = ['name', 'description', 'created', 'updated']
PRODUCT_DTO_ATTRS = ['name', 'findings_count', 'description', 'created']
FINDINGS_DTO_ATTRS = ['title', 'date', 'severity', 'sla_days_remaining',
                      'mitigation', 'impact']
TEMP_FOLDER = '/tmp'
NB_SPACE = '&nbsp;'  # non-breaking space
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
    def __init__(self, host: str, api_key: str, entities_mapping: dict = None,
                 display_all_fields: bool = False, upload_files: bool = False,
                 resource_per_finding: bool = False):
        self.entities_mapping = entities_mapping or {}
        self.display_all_fields = display_all_fields if isinstance(display_all_fields, bool) else False
        self.upload_files = upload_files if isinstance(upload_files, bool) else False
        self.resource_per_finding = resource_per_finding if isinstance(resource_per_finding, bool) else False
        self.client = DojoClient(
            host=host,
            api_key=api_key,
        )
        super().__init__()

    def push_notification(self, job_id: str, started_at: str,
                          tenant_display_name: str,
                          customer_display_name: str,
                          policy_report: List[Dict]):

        _LOG.info(f'Job:\'{job_id}\' - formatting compatible policy reports.')
        formatted_findings = self.format_policy_report_list(
            policy_report_list=policy_report
        )
        started_at_obj = utc_datetime(_from=started_at)
        stopped_at_obj = utc_datetime()
        job_scope = f'{started_at_obj.strftime(TIME_FORMAT)} - ' \
                    f'{stopped_at_obj.strftime(TIME_FORMAT)}'
        day_scope = \
            f'{stopped_at_obj.date().isoformat()} - ' \
            f'{(stopped_at_obj.date() + timedelta(days=1)).isoformat()}'
        _LOG.info(f'Importing job {job_id}')
        self.upload_scan(
            formatted_findings=formatted_findings,
            **CustodianToDefectDojoEntitiesMapper(
                self.entities_mapping,
                customer=customer_display_name,
                tenant=tenant_display_name,
                day_scope=day_scope,
                job_scope=job_scope,
                job_id=job_id
            ).generate()
        )

    def upload_scan(self, test_title: str, engagement_name: str,
                    product_name: str,
                    product_type_name: str, formatted_findings):
        product = self.client.get_product_by_name(product_name)
        engagement = self.client.get_engagement(
            name=engagement_name,
            product_id=product.get('id') if product else None
        )

        if engagement and product:
            reimport = True
            _LOG.info(f'Engagement "{engagement_name}" inside product '
                      f'{product_name} already exists, reimporting...')
        else:
            reimport = False
            _LOG.info(f'Engagement "{engagement_name}" inside product '
                      f'{product_name} does not exist, importing...')

        formatted_report_buffer = io.BytesIO(
            json.dumps(formatted_findings).encode()
        )
        _LOG.debug(f'Importing test: {test_title}')
        self.client.import_scan(
            product_type_name=product_type_name,
            product_name=product_name,
            engagement_name=engagement_name,
            buffer=formatted_report_buffer,
            reimport=reimport,
            test_title=test_title
        )
        formatted_report_buffer.close()

    def format_policy_report_list(
            self, policy_report_list: List[Dict]
    ):
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

    # Pre v3.3.1 methods.

    # def deactivate_previous_engagements(self, engagement_id):
    #     engagements = self.client.list_engagements()
    #     engagements = engagements.get('results', [])
    #     engagements = [eng for eng in engagements if
    #                    eng.get('id') != engagement_id]
    #
    #     for eng in engagements:
    #         eng_tests = self.client.list_tests(engagement_id=eng.get('id'))
    #         eng_tests = eng_tests.get('results', [])
    #         for test in eng_tests:
    #             active_findings = self.client.list_findings(
    #                 test_id=test.get('id'), active=True)
    #             active_findings = active_findings.get('results', [])
    #             for finding in active_findings:
    #                 self.client.deactivate_finding(
    #                     finding=finding
    #                 )
    #         self.client.close_engagement(
    #             engagement_id=eng.get('id')
    #         )

    # def format_detailed_report(self, detailed_report):
    #     _LOG.debug('Formatting report to Dojo format')
    #     formatted_findings = []
    #     for region_name, region_policies in detailed_report.items():
    #         for policy in region_policies:
    #             policy_meta = policy.get('policy', {})
    #             resources = policy.get('resources', [])
    #
    #             mitigation = policy_meta.get('mitigation', '')
    #             mitigation = self.format_mitigation(mitigation=mitigation)
    #
    #             for resource in resources:
    #                 description = policy_meta.get('name') + '\n' + \
    #                               policy_meta.get('article')
    #                 finding_data = {
    #                     'title': policy_meta.get('description', ''),
    #                     'description': description,
    #                     'severity': policy_meta.get('severity', '').title(),
    #                     'mitigation': mitigation,
    #                     'impact': policy_meta.get('impact'),
    #                     'nb_occurences': 1,
    #                     'file_upload': resource
    #                 }
    #                 formatted_findings.append(finding_data)
    #     return formatted_findings
    #
    # @staticmethod
    # def format_mitigation(mitigation):
    #     updated_mitigation = mitigation
    #     try:
    #         points = re.findall(r'\s(\d\.){1,2}\s', updated_mitigation)
    #         for point in points:
    #             updated_mitigation = updated_mitigation.replace(
    #                 point, f'\n{point[1:]}')
    #     except Exception as e:
    #         _LOG.debug(f'Failed to format mitigation, error: {e}')
    #     return updated_mitigation
    #
    # def list_connected_tenants(self):
    #     response = self.client.list_product_types()
    #     product_types = response.get('results', [])
    #     return self._get_dojo_entities_dto(
    #         entities=product_types,
    #         dto_attrs=PRODUCT_TYPE_DTO_ATTRS
    #     )
    #
    # def list_accounts(self, tenant_name=None):
    #     response = self.client.list_products(name=tenant_name)
    #     products = response.get('results', [])
    #     return self._get_dojo_entities_dto(
    #         entities=products,
    #         dto_attrs=PRODUCT_DTO_ATTRS
    #     )
    #
    # def list_findings(self, account_name=None, severity=None, limit=None,
    #                   offset=None):
    #     response = self.client.list_findings(severity=severity, limit=limit,
    #                                          offset=offset)
    #     findings = response.get('results', [])
    #     return self._get_dojo_entities_dto(
    #         entities=findings,
    #         dto_attrs=FINDINGS_DTO_ATTRS
    #     )

    # @staticmethod
    # def _save_report(formatted_findings, account_name, target_start):
    #     file_name = f'{account_name}_detailed_report_{target_start}.json'
    #     file_path = os.path.join(TEMP_FOLDER, file_name)
    #     with open(file_path, 'w') as f:
    #         json.dump(formatted_findings, f)
    #     return file_path
    #
    # @staticmethod
    # def _delete_report(file_path):
    #     try:
    #         os.remove(file_path)
    #     except OSError:
    #         pass
    #
    # @staticmethod
    # def _get_dojo_entities_dto(entities: list, dto_attrs: list):
    #     result = []
    #     for entity in entities:
    #         entity_dto = {}
    #         for key, value in entity.items():
    #             if key not in dto_attrs:
    #                 continue
    #             value = value if value else ''
    #             entity_dto[key] = value
    #         result.append(entity_dto)
    #     return result

    # def _pre_v3_3_0_format_detailed_report(self, detailed_report,
    #                               one_res_per_finding=True,
    #                               finding_date: str = None):
    #    """Intended to transform CaaS detailed report to a format which can
    #    be understood and parsed by EPAM DefectDojo's fork with custom CaaS
    #    parser"""
    #    findings = []
    #    for region, vulnerabilities in detailed_report.items():
    #        for vulnerability in vulnerabilities:
    #            policy = vulnerability.get('policy')
    #            resources = vulnerability.get('resources', [])
    #            if len(resources) == 0:
    #                continue
    #            report_fields = policy.get('report_fields', [])
    #            references = []
    #            for standard, versions in policy.get(
    #                    'standard_points', {}).items():
    #                references.append(f"{standard}: {', '.join(versions)}")
    #            new_finding = {
    #                'title': f'{policy.get("description", "No title")}',
    #                'date': finding_date or utc_datetime().date().isoformat(),
    #                'mitigation': policy.get('mitigation'),
    #                'impact': policy.get('impact'),
    #                'severity': policy.get('severity', 'Medium').title(),
    #                'references': "\n".join(references),
    #                'service': policy.get("resourceType"),
    #                'component_name': "",
    #                'tags': ','.join([region]),
    #                'finding_groups': [policy.get('service_section',
    #                                              'No service section')],
    #                'vuln_id_from_tool': policy.get('name')
    #            }
    #            if one_res_per_finding:
    #                for res in resources:
    #                    f_copied = copy.deepcopy(new_finding)
    #                    table_str = self.make_markdown_table([res],
    #                                                         report_fields)
    #                    f_copied.update({
    #                        'description': f'{policy.get("article")}\nRegion: '
    #                                       f'**{region.lower()}**'
    #                                       f'\n\n{table_str}',
    #                    })
    #                    if self.upload_files:
    #                        f_copied.update({
    #                            'files': [{
    #                                'title': f'{policy.get("resourceType")}-'
    #                                         f'{str(uuid.uuid4())[:8]}.json',
    #                                'data': b64encode(
    #                                    json.dumps(res).encode()).decode()}]
    #                        })
    #
    #                    if 'Arn' in res:
    #                        f_copied.update({"component_name": res.get(
    #                            "Arn")})
    #                    else:
    #                        del f_copied["component_name"]
    #
    #                    findings.append(f_copied)
    #            else:
    #                table_str = self.make_markdown_table(resources,
    #                                                     report_fields)
    #
    #                new_finding.update({
    #                    'description': f'{policy.get("article")}\nRegion: '
    #                                   f'**{region.lower()}**'
    #                                   f'\nNumber of resources found: '
    #                                   f'**{len(resources)}**\n\n{table_str}',
    #                })
    #                if self.upload_files:
    #                    new_finding.update({
    #                        'files': [{
    #                            'title': f'{policy.get("resourceType")}-'
    #                                     f'{str(uuid.uuid4())[:8]}.json',
    #                            'data': b64encode(
    #                                json.dumps(res).encode()).decode()
    #                        } for res in resources]
    #                    })
    #                findings.append(new_finding)
    #    return {'findings': findings}

    # def make_markdown_table(self, detailed_resources, report_fields):
    #     display_all_fields = self.display_all_fields
    #     key_report = report_fields[0] if report_fields else None
    #     try:
    #         # TODO make it decent
    #         if key_report:
    #             detailed_resources_sorted = sorted(
    #                 detailed_resources, key=lambda d: d[key_report])
    #             detailed_resources = detailed_resources_sorted
    #     except KeyError:
    #         _LOG.warning(f'Invalid detailed_report.json!!! '
    #                      f'Fields: {report_fields}, '
    #                      f'resources :{detailed_resources}')
    #         display_all_fields = True
    #     resources = []
    #     for detailed_resource in detailed_resources:
    #         resource = {}
    #         for key in detailed_resource:
    #             if report_fields and not display_all_fields:
    #                 if key in report_fields:
    #                     resource[key] = detailed_resource.get(key)
    #                 continue
    #             if isinstance(detailed_resource[key], (str, int, float)):
    #                 resource[key] = detailed_resource[key]
    #         resources.append(resource if resource else detailed_resource)
    #     try:
    #         # the crazy line below helps DefectDojo print table indents
    #         # properly, plus it makes the first letters of headers uppercase :)
    #         resources = [
    #             {f'{key[0].upper() + key[1:]}{NB_SPACE}': f'{value}{NB_SPACE}'
    #              for key, value in resource.items()} for resource in resources
    #         ]
    #         fieldnames = []
    #         for resource in resources:
    #             fieldnames.extend([field for field in resource.keys()
    #                                if field not in fieldnames])
    #         table_writer = MarkdownTableWriter()
    #         with io.StringIO() as buffer:
    #             dict_writer = csv.DictWriter(buffer, sorted(fieldnames))
    #             dict_writer.writeheader()
    #             dict_writer.writerows(resources)
    #             table_writer.from_csv(buffer.getvalue())
    #         table_str = table_writer.dumps()
    #     except Exception as e:
    #         _LOG.warning(
    #             'Something went wrong while making resources table for '
    #             f'DefectDojo\'s finding: {e}. Dumping resources to JSON')
    #         table_str = json.dumps(
    #             resources, sort_keys=True, indent=4).translate(
    #             {ord(c): None for c in '{[]}",'}
    #         )
    #     return table_str
