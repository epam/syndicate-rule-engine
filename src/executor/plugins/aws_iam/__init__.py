import csv
import datetime
import io
import time

import jmespath
from botocore.exceptions import ClientError
from c7n.exceptions import PolicyExecutionError
from c7n.filters import Filter
from c7n.utils import (
    local_session, type_schema
)


# Used to parse saml provider metadata configuration.

class CreationTimeAWSIAMUserFilter(Filter):
    """
    That filter is used only for comparing time of user creation and access key creation
    with extended resource for iam user. We check detail info about aws users
    """
    schema = type_schema(
        'creation-time-aws-iam-user',
        field_name_1={'$ref': '#/definitions/filters_common/value'},
        field_name_2={'$ref': '#/definitions/filters_common/value'},
        seconds={'$ref': '#/definitions/filters_common/value'})
    permissions = ('iam:ListUserPolicies',
                   'iam:GenerateCredentialReport',
                   'iam:GetCredentialReport',)

    def op(self, access_key_creation_time, user_creation_time):
        user_time_pattern = "%Y-%m-%d %H:%M:%S"
        access_key_pattern = "%Y-%m-%dT%H:%M:%S"
        timezone_pattern_position = -6
        filtered_access_key_creation_time = datetime.datetime.strptime(
            access_key_creation_time[:timezone_pattern_position],
            access_key_pattern)
        filtered_user_creation_time = datetime.datetime.strptime(
            user_creation_time[:timezone_pattern_position], user_time_pattern)
        result_time = filtered_access_key_creation_time - filtered_user_creation_time
        seconds = self.data.get('seconds')
        try:
            seconds = int(seconds)
        except ValueError as e:
            raise PolicyExecutionError(e)
        if result_time.seconds < seconds:
            return True
        return False

    def get_value_or_schema_default(self, k):
        if k in self.data:
            return self.data[k]
        return self.schema['properties']

    def data_validation(self, data):
        if 'N/A' not in data and 'None' not in data:
            return True
        return False

    def configure_extended_resource(self):
        client = local_session(self.manager.session_factory).client('iam')
        try:
            report = client.get_credential_report()['Content']
        except ClientError as e:
            if e.response['Error']['Code'] != 'ReportNotPresent':
                raise
            report = None
        if report is None:
            if not self.get_value_or_schema_default('report_generate'):
                raise ValueError("Credential Report Not Present")
            client.generate_credential_report()
            time.sleep(self.get_value_or_schema_default('report_delay'))
            report = client.get_credential_report()['Content']
        if isinstance(report, bytes):
            reader = csv.reader(io.StringIO(report.decode('utf-8')))
        else:
            reader = csv.reader(io.StringIO(report))
        headers = next(reader)
        results = []
        for line in reader:
            info = dict(zip(headers, line))
            results.append(info)
        return results

    def process(self, resources, event=None):
        extended_resources = self.configure_extended_resource()
        filtered_resources = []
        field_name_1 = None
        for resource in resources:
            field_name_2 = jmespath.search(self.data.get('field_name_2'),
                                           resource)
            report = self._compare_credential_report_and_resources(
                extended_resources, resource)
            if report:
                field_name_1 = jmespath.search(
                    self.data.get('field_name_1'), report)
            if field_name_1 is not None and field_name_2 is not None and \
                self.data_validation(str(field_name_1)) and \
                self.data_validation(str(field_name_2)) and \
                self.op(str(field_name_1), str(field_name_2)):
                filtered_resources.append(resource)
        return filtered_resources

    def _compare_credential_report_and_resources(self, extended_resources,
                                                 resource):
        for extended_resource in extended_resources:
            if resource['UserName'] == extended_resource['user']:
                return extended_resource
        return False


def register() -> None:
    from c7n.resources.iam import User
    User.filter_registry.register('creation-time-aws-iam-user', CreationTimeAWSIAMUserFilter)
