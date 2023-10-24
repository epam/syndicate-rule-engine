import json
import os
import re
from typing import Optional

import boto3

from helpers import batches
from helpers.log_helper import get_logger
from integrations.abstract_adapter import AbstractAdapter

CREDENTIALS = 'Credentials'

_LOG = get_logger(__name__)


class SecurityHubAdapter(AbstractAdapter):
    def __init__(self, aws_region: str, product_arn: str,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
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

    def push_notification(self, **kwargs):
        _LOG.debug(f'Going to push notifications to Security Hub')

        folder_path = kwargs.get('findings_folder')
        file_paths = (
            os.path.join(folder_path, file)
            for file in os.listdir(folder_path) if file.endswith('.json')
        )
        findings = []
        for file_path in file_paths:
            with open(file_path, 'r') as f:
                file_findings = json.load(f)
                findings.extend(file_findings)

        for finding in findings:
            # todo resolve this (add AwsAccountId to configuration or get
            #  from findings)
            finding["ProductArn"] = self.product_arn
            finding["AwsAccountId"] = re.findall(
                '/(\d+)/', self.product_arn)[0]
        _LOG.debug(f'Going to upload {len(findings)} findings')
        responses = self.upload_findings(findings=findings)
        _LOG.debug(f'Security hub responses: {responses}')

    def batch_import_findings(self, findings):
        return self.client.batch_import_findings(Findings=findings)

    def get_findings(self, findings):
        return self.client.get_findings(findings)

    def upload_findings(self, findings: list):
        """
        Upload findings to Security Hub by batches
        """
        _LOG.info(f'Going to upload {len(findings)} findings ')
        max_findings_in_request = 100

        responses = []
        for batch in batches(findings, max_findings_in_request):
            if len(batch) == 0:
                continue
            responses.append(self.client.batch_import_findings(
                Findings=batch
            ))
        _LOG.debug(f'Security hub responses: {responses}')

        return responses
