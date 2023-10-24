import os
from typing import Dict, Set

from pynamodb.attributes import UnicodeAttribute, MapAttribute, ListAttribute
from pynamodb.indexes import AllProjection

from helpers.constants import ENV_VAR_REGION
from models.modular import BaseModel, BaseGSI

BR_ID = 'id'
BR_JOB_ID = 'jid'
BR_RULES_TO_SCAN = 'r'
BR_BUCKET_NAME = 'bn'
BR_BUCKET_PATH = 'bp'
BR_STATUS = 's'
BR_CLOUD_ID = 'cid'
BR_TENANT_NAME = 't'
BR_CUSTOMER_NAME = 'c'
BR_EVENT_REGISTRATION_START = 'ers'
BR_EVENT_REGISTRATION_END = 'ere'
BR_JOB_SUBMITTED_AT = 'jsa'
BR_JOB_STOPPED_AT = 'jsta'
BR_FAILURE_REASON = 'fr'
BR_CREDENTIALS_KEY = 'cr'


class JobIdIndex(BaseGSI):
    class Meta:
        index_name = f'{BR_JOB_ID}-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    job_id = UnicodeAttribute(hash_key=True, attr_name=BR_JOB_ID)


class BatchResults(BaseModel):
    class Meta:
        table_name = "CaaSBatchResults"
        region = os.environ.get(ENV_VAR_REGION)

    id = UnicodeAttribute(hash_key=True, attr_name=BR_ID)  # uuid
    job_id = UnicodeAttribute(attr_name=BR_JOB_ID, null=True)  # Batch job id
    rules = MapAttribute(default=dict, attr_name=BR_RULES_TO_SCAN, null=True)

    credentials_key = UnicodeAttribute(null=True, attr_name=BR_CREDENTIALS_KEY)
    status = UnicodeAttribute(null=True,
                              attr_name=BR_STATUS)  # the same as in CaaSJobs
    reason = UnicodeAttribute(null=True, attr_name=BR_FAILURE_REASON)
    cloud_identifier = UnicodeAttribute(null=True, attr_name=BR_CLOUD_ID)
    tenant_name = UnicodeAttribute(null=True, attr_name=BR_TENANT_NAME)
    customer_name = UnicodeAttribute(null=True, attr_name=BR_CUSTOMER_NAME)
    registration_start = UnicodeAttribute(null=True,
                                          attr_name=BR_EVENT_REGISTRATION_START)
    registration_end = UnicodeAttribute(null=True,
                                        attr_name=BR_EVENT_REGISTRATION_END)
    submitted_at = UnicodeAttribute(null=True, attr_name=BR_JOB_SUBMITTED_AT)
    stopped_at = UnicodeAttribute(null=True, attr_name=BR_JOB_STOPPED_AT)

    job_id_index = JobIdIndex()

    def regions_to_rules(self) -> Dict[str, Set[str]]:
        """
        Retrieves rules attribute from self and transforms in to a mapping:
        {
            'eu-central-1': {'epam-aws-005..', 'epam-aws-006..'},
            'eu-west-1': {'epam-aws-006..', 'epam-aws-007..'}
        }
        """
        ref = {}
        for regions, rules in self.rules.as_dict().items():
            for region in regions.split(','):
                ref.setdefault(region, set()).update(rules)
        return ref

    def rules_to_regions(self) -> Dict[str, Set[str]]:
        """
        Retrieves rules attribute from self and transforms in to a mapping:
        {
            'epam-aws-005..': {'eu-central-1'},
            'epam-aws-006..': {'eu-west-1', 'eu-central-1'},
            'epam-aws-007..': {'eu-west-1'}
        }
        """
        ref = {}
        for regions, rules in self.rules.as_dict().items():
            for rule in rules:
                ref.setdefault(rule, set()).update(regions.split(','))
        return ref
