from collections import defaultdict
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Union

from dacite import from_dict

CLOUD_DATA_TO_EXCLUDE = []


@dataclass
class NameValueDiffData:
    name: Optional[str]
    value: Optional[Union[int, float, dict]]
    diff: Optional[Union[int, float, dict]]

    def __post_init__(self):
        if isinstance(self.value, dict):
            self.value = self.value.get('value')
        if isinstance(self.diff, dict):
            self.diff = self.diff.get('diff')

    def __sub__(self, other):
        if isinstance(other, NameValueDiffData):
            return NameValueDiffData(
                self.name, self.value,
                self.value - other.value if other.value else None)
        return self


@dataclass
class ValueDiffData:
    value: Optional[Union[int, float, dict]]
    diff: Optional[Union[int, float, dict]]

    def __post_init__(self):
        if isinstance(self.value, dict):
            self.value = self.value.get('value')
        if isinstance(self.diff, dict):
            self.diff = self.diff.get('diff')

    def __sub__(self, other):
        if isinstance(other, ValueDiffData):
            return ValueDiffData(
                self.value, self.value - other.value if other.value else None)
        return self


@dataclass
class RegionData:
    total_violated_resources: Union[ValueDiffData, int, None]

    def __sub__(self, other):
        if isinstance(other, RegionData):
            if isinstance(self.total_violated_resources, ValueDiffData):
                self.total_violated_resources = self.total_violated_resources.value
            if isinstance(other.total_violated_resources, int):
                return RegionData(ValueDiffData(
                    self.total_violated_resources,
                    self.total_violated_resources - other.total_violated_resources))
            if isinstance(other.total_violated_resources, ValueDiffData):
                return RegionData(ValueDiffData(
                    self.total_violated_resources,
                    self.total_violated_resources - other.total_violated_resources.value))
        return RegionData(ValueDiffData(
                    self.total_violated_resources, None))


@dataclass
class ServicePolicyData:
    rule: str
    service: str
    category: str
    severity: str
    resource_type: str
    regions_data: Dict[str, RegionData] = field(default_factory=dict)

    def __sub__(self, other):
        if isinstance(other, ServicePolicyData):
            regions_data = {}
            for region, region_data in self.regions_data.items():
                if region in other.regions_data:
                    regions_data[region] = region_data - other.regions_data[region]
                else:
                    regions_data[region] = region_data - RegionData(None)
            return ServicePolicyData(
                rule=self.rule, service=self.service, category=self.category,
                severity=self.severity, resource_type=self.resource_type,
                regions_data=regions_data)
        return self


@dataclass
class ServiceSectionData:
    service_section: str
    rules_data: List[ServicePolicyData] = field(default_factory=list)

    def __sub__(self, other):
        if isinstance(other, ServiceSectionData):
            data = []
            for rule in self.rules_data:
                diff = None
                for other_rule in other.rules_data:
                    if rule.rule == other_rule.rule:
                        diff = rule - other_rule
                        break

                if not diff:
                    diff = rule - ServicePolicyData(None, None, None, None, None, {})
                data.append(diff)

            return ServiceSectionData(self.service_section, data)
        return self


@dataclass
class PolicyData:
    policy: str
    description: str
    severity: str
    resource_type: str
    regions_data: Dict[str, RegionData] = field(default_factory=dict)

    def __sub__(self, other):
        if isinstance(other, PolicyData):
            regions_data = {}
            for region, region_data in self.regions_data.items():
                if region in other.regions_data:
                    regions_data[region] = region_data - other.regions_data[region]
                else:
                    regions_data[region] = region_data - RegionData(None)
            return PolicyData(self.policy, self.description, self.severity,
                              self.resource_type, regions_data)
        return self


@dataclass
class RegionsComplianceData:
    region: str
    standards_data: List[NameValueDiffData] = field(default_factory=list)

    def __sub__(self, other):
        standards_data = []
        for data in self.standards_data:
            diff_standards_data = None
            for other_standard in other.standards_data:
                if data.name == other_standard.name:
                    diff_standards_data = data - other_standard
                    break

            if not diff_standards_data:
                diff_standards_data = data - NameValueDiffData(
                    None, None, None)
            standards_data.append(diff_standards_data)
        return RegionsComplianceData(self.region, standards_data)


@dataclass
class RegionsOverviewData:
    severity_data: Optional[Dict[str, Union[ValueDiffData, int, None]]] = field(default_factory=lambda: defaultdict(dict))
    resource_types_data: Optional[Dict[str, Union[ValueDiffData, int, None]]] = field(default_factory=lambda: defaultdict(dict))

    def __sub__(self, other):
        if isinstance(other, RegionsOverviewData):
            severity_data = {}
            resource_types_data = {}
            for k, v in self.severity_data.items():
                if k in other.severity_data:
                    if isinstance(other.severity_data[k], int):
                        severity_data[k] = ValueDiffData(
                            value=v, diff=v - other.severity_data[k])
                    elif isinstance(v, int) and isinstance(other.severity_data[k], ValueDiffData):
                        severity_data[k] = ValueDiffData(v, v - other.severity_data[k].value)
                    else:
                        severity_data[k] = v - other.severity_data[k]
                else:
                    severity_data[k] = ValueDiffData(value=v, diff=None)

            for k, v in self.resource_types_data.items():
                if k in other.resource_types_data:
                    if isinstance(other.resource_types_data[k], int):
                        resource_types_data[k] = ValueDiffData(
                            value=v, diff=v - other.resource_types_data[k])
                    elif isinstance(v, int) and isinstance(other.resource_types_data[k], ValueDiffData):
                        resource_types_data[k] = ValueDiffData(v, v - other.resource_types_data[k].value)
                    else:
                        resource_types_data[k] = v - other.resource_types_data[k]
                else:
                    resource_types_data[k] = ValueDiffData(value=v, diff=None)

            return RegionsOverviewData(severity_data, resource_types_data)
        return self


@dataclass
class CloudData:
    account_id: str
    tenant_name: str
    last_scan_date: str
    activated_regions: List[str]
    total_scans: Optional[int]
    failed_scans: Optional[int]
    succeeded_scans: Optional[int]
    resources_violated: Optional[int]
    outdated_tenants: Dict[str, Dict[str, str]] = field(default_factory=dict)

    regions_data: Union[Dict[str, RegionsOverviewData], List[RegionsComplianceData]] = field(default_factory=dict)
    data: List[PolicyData] = field(default_factory=list)
    service_data: List[ServiceSectionData] = field(default_factory=list)
    average_data: List[NameValueDiffData] = field(default_factory=list)

    def as_dict(self, to_exclude: list = None):
        exclude = ['activated_regions', 'outdated_tenants']
        exclude = to_exclude + exclude if to_exclude else exclude
        return {k: v for k, v in self.__dict__.items() if k in exclude or
                v not in (None, [], {})}

    def __sub__(self, other):
        if isinstance(other, CloudData):
            data = []
            service_data = []
            regions_data = []
            average_data = []
            for policy in self.data:
                diff_policy = None
                for other_policy in other.data:
                    if policy.policy == other_policy.policy:
                        diff_policy = policy - other_policy
                        break

                if not diff_policy:
                    diff_policy = policy - PolicyData(None, None, None, None, {})
                data.append(diff_policy)

            for service in self.service_data:
                diff_service = None
                for other_service in other.service_data:
                    if service.service_section == other_service.service_section:
                        diff_service = service - other_service
                        break

                if not diff_service:
                    diff_service = service - ServiceSectionData(None, [])
                service_data.append(diff_service)

            if isinstance(self.regions_data, list):
                for r_data in self.regions_data:
                    diff_region_data = None
                    for other_region in other.regions_data:
                        if r_data.region == other_region.region:
                            diff_region_data = r_data - other_region
                            break

                    if not diff_region_data:
                        diff_region_data = r_data - RegionsComplianceData(
                            None, [])
                    regions_data.append(diff_region_data)
            else:
                regions_data = {}
                for region, r_data in self.regions_data.items():
                    if region in other.regions_data:
                        diff = r_data - other.regions_data[region]
                    else:
                        diff = r_data - RegionsOverviewData({}, {})
                    regions_data[region] = diff
            for a_data in self.average_data:
                diff_average_data = None
                for other_average in other.average_data:
                    if a_data.name == other_average.name:
                        diff_average_data = a_data - other_average
                        break

                if not diff_average_data:
                    diff_average_data = a_data - NameValueDiffData(
                        None, None, None)
                average_data.append(diff_average_data)
            return CloudData(self.account_id, self.tenant_name,
                             last_scan_date=self.last_scan_date,
                             activated_regions=self.activated_regions,
                             total_scans=self.total_scans,
                             failed_scans=self.failed_scans,
                             succeeded_scans=self.succeeded_scans,
                             resources_violated=self.resources_violated,
                             outdated_tenants=self.outdated_tenants,
                             regions_data=regions_data if regions_data else None,
                             data=data, service_data=service_data,
                             average_data=average_data).as_dict(
                to_exclude=CLOUD_DATA_TO_EXCLUDE)
        return self


@dataclass
class BaseData:
    aws: List[CloudData] = field(default_factory=list)
    azure: List[CloudData] = field(default_factory=list)
    google: List[CloudData] = field(default_factory=list)

    def __sub__(self, other):
        if isinstance(other, BaseData):
            aws_result = []
            azure_result = []
            google_result = []
            for account in self.aws:
                diff_account = None
                for other_aws in other.aws:
                    if account.account_id == other_aws.account_id:
                        diff_account = account - other_aws
                        break
                if not diff_account:
                    diff_account = account - CloudData(None, None, None, [], None, None, None, None, [], [])
                aws_result.append(diff_account)
            for account in self.azure:
                diff_account = None
                for other_azure in other.azure:
                    if account.account_id == other_azure.account_id:
                        diff_account = account - other_azure
                        break

                if not diff_account:
                    diff_account = account - CloudData(None, None, None, [], None, None, None, None, [], [])
                azure_result.append(diff_account)
            for account in self.google:
                diff_account = None
                for other_google in other.google:
                    if account.account_id == other_google.account_id:
                        diff_account = account - other_google
                        break

                if not diff_account:
                    diff_account = account - CloudData(
                        None, None, None, [], None, None, None, None, [], [])
                google_result.append(diff_account)
            return BaseData(aws_result, azure_result, google_result)


def report_difference(current, prev, report_type):
    global CLOUD_DATA_TO_EXCLUDE

    if report_type == 'finops':
        CLOUD_DATA_TO_EXCLUDE = ['service_data']
    else:
        CLOUD_DATA_TO_EXCLUDE = []
    current_data = from_dict(data_class=BaseData, data=current)
    prev_data = from_dict(data_class=BaseData, data=prev if prev else {})
    diff = current_data-prev_data
    return asdict(diff)


def calculate_dict_diff(current: dict, previous: dict, exclude=None):
    """ Calculates difference between numeric values of the same keys """
    # TODO add some examples to doc because it's difficult to understand
    #  what it does
    result = {}
    for k, v in current.items():
        prev_v = previous.get(k)
        if exclude and k in exclude:
            result[k] = v
        elif k in ('resources', 'compliance', 'overview', 'finops') and \
                isinstance(v, dict):
            result[k] = report_difference(v, prev_v, k)
        elif not prev_v and not isinstance(v, bool) and isinstance(
                    v, (int, float)):
            result[k] = {'value': v, 'diff': None}
        elif not isinstance(v, bool) and isinstance(v, (int, float)):
            result[k] = {'value': v, 'diff': v - prev_v}
        elif isinstance(v, dict):
            result[k] = calculate_dict_diff(
                    v, prev_v if prev_v else {}, exclude=exclude)
        elif isinstance(v, list):
            result_data = []
            for i in v:
                if isinstance(i, str):
                    result_data.append(i)
                    continue
                diff = None
                for j in prev_v if prev_v else {}:
                    if i.get('name') == j.get('name'):
                        diff = asdict(
                            from_dict(data_class=NameValueDiffData, data=i) -
                            from_dict(data_class=NameValueDiffData, data=j))
                        break

                if not diff:
                    diff = asdict(
                        from_dict(data_class=NameValueDiffData, data=i) -
                        NameValueDiffData(None, None, None))
                result_data.append(diff)
            result[k] = result_data
        else:
            result[k] = v
    return result
