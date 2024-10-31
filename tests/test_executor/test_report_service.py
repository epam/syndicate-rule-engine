# some magic import that fixes c7n.resources.load_resources
from c7n.policy import Policy, PolicyCollection  # noqa

from executor.services.report_service import ReportFieldsLoader


def test_report_fields_loader_not_loaded():
    assert ReportFieldsLoader.get('aws.s3') == {}
    assert ReportFieldsLoader.get('s3') == {}
    assert ReportFieldsLoader.get('azure.vm') == {}


def test_report_fields_loader_loaded_partially():
    ReportFieldsLoader.load(('aws.s3', 'aws.iam-group', 'azure.vm', 'gcp.vpc'))

    assert ReportFieldsLoader.get('s3') == ReportFieldsLoader.get(
        'aws.s3') == {
               'id': 'Name',
               'name': 'Name',
               'arn': None,
               'namespace': 'metadata.namespace',
               'date': 'CreationDate'
           }
    assert ReportFieldsLoader.get('iam-group') == ReportFieldsLoader.get(
        'aws.iam-group') == {
               'id': 'GroupName',
               'name': 'GroupName',
               'arn': 'Arn',
               'namespace': 'metadata.namespace',
               'date': 'CreateDate'
           }

    assert ReportFieldsLoader.get('azure.vm') == {
        'id': 'id',
        'name': 'name',
        'arn': None,
        'namespace': 'metadata.namespace',
        'date': None
    }
    assert ReportFieldsLoader.get('gcp.vpc') == {
        'id': 'name',
        'name': 'name',
        'arn': None,
        'namespace': 'metadata.namespace',
        'date': None
    }

    assert ReportFieldsLoader.get('vm') == {}
    assert ReportFieldsLoader.get(
        'vpc') == {}  # from aws, but now loaded, so empty
