from executor.services.report_service import JobResult, ReportFieldsLoader


def test_report_fields_loader_not_loaded():
    assert ReportFieldsLoader.get('aws.s3') == {}
    assert ReportFieldsLoader.get('s3') == {}
    assert ReportFieldsLoader.get('azure.vm') == {}


def test_report_fields_loader_loaded_partially():
    ReportFieldsLoader.load()
    breakpoint()
    assert ReportFieldsLoader.get('aws.s3') == {

    }

