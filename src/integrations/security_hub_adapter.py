from typing import Optional

import boto3

from helpers import batches
from helpers.log_helper import get_logger
from integrations import AbstractAdapter

SIEM_SH_TYPE = 'security_hub'

_LOG = get_logger(__name__)


class SecurityHubAdapter(AbstractAdapter):
    siem_type = SIEM_SH_TYPE
    request_error = f'An error occurred while uploading ' \
                    f'findings to {SIEM_SH_TYPE}.'

    def __init__(self, aws_region: str, product_arn: str,
                 aws_access_key_id: str, aws_secret_access_key: str,
                 aws_session_token: Optional[str] = None,
                 aws_default_region: Optional[str] = None):
        self.region = aws_region or aws_default_region
        self.product_arn = product_arn
        self.client = boto3.client(
            'securityhub',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=self.region
        )
        super().__init__()

    def add_entity(self, job_id, job_type, findings):
        for finding in findings:
            finding["ProductArn"] = self.product_arn
            # finding["AwsAccountId"] = re.findall(
            #     '/(\d+)/', self.product_arn)[0]
        self._entities.append({
            'job_id': job_id,
            'job_type': job_type,
            'findings': findings
        })

    def upload(self, job_id, job_type, findings: list):
        """
        Upload findings to Security Hub by batches
        """
        _LOG.info(f'Going to upload {len(findings)} findings '
                  f'for {job_type} job: {job_id}')
        max_findings_in_request = 100
        responses = []
        for batch in batches(findings, max_findings_in_request):
            if len(batch) == 0:
                continue
            responses.append(self.client.batch_import_findings(
                Findings=batch
            ))
        _LOG.debug(f'Security hub responses: {responses}')

        _LOG.info('Counting errors from SecurityHub responses')
        total_failed_count, total_succeeded_count, errors = 0, 0, set()
        for response in responses:
            failed = response.get('FailedCount', 0)
            total_failed_count += failed
            total_succeeded_count += response.get('SuccessCount', 0)
            if failed > 0:
                errors.update([finding.get('ErrorMessage') for finding in
                               response.get('FailedFindings')])
        _LOG.warning(f'Errors were counted. Total failed: {total_failed_count}'
                     f', total succeeded: {total_succeeded_count}')
        if errors:
            raise ValueError(
                f'Total failed findings count: {total_failed_count}\n'
                f'Total succeeded findings count: {total_succeeded_count}\n'
                f'Occurred errors:\n' + '\n'.join(errors)
            )
        return responses
