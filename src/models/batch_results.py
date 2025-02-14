from pynamodb.attributes import UnicodeAttribute, MapAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex

from helpers.constants import CAASEnv, JobState
from helpers.time_helper import utc_iso
from models import BaseModel

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


class CustomerNameSubmittedAtIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = f'{BR_CUSTOMER_NAME}-{BR_JOB_SUBMITTED_AT}-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    customer_name = UnicodeAttribute(hash_key=True, attr_name=BR_CUSTOMER_NAME)
    submitted_at = UnicodeAttribute(range_key=True,
                                    attr_name=BR_JOB_SUBMITTED_AT)


class TenantNameSubmittedAtIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = f'{BR_TENANT_NAME}-{BR_JOB_SUBMITTED_AT}-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    tenant_name = UnicodeAttribute(hash_key=True, attr_name=BR_TENANT_NAME)
    submitted_at = UnicodeAttribute(range_key=True,
                                    attr_name=BR_JOB_SUBMITTED_AT)


class BatchResults(BaseModel):
    class Meta:
        table_name = "CaaSBatchResults"
        region = CAASEnv.AWS_REGION.get()

    id = UnicodeAttribute(hash_key=True, attr_name=BR_ID)  # uuid
    job_id = UnicodeAttribute(attr_name=BR_JOB_ID, null=True)  # Batch job id
    rules = MapAttribute(default=dict, attr_name=BR_RULES_TO_SCAN, null=True)

    credentials_key = UnicodeAttribute(null=True, attr_name=BR_CREDENTIALS_KEY)
    status = UnicodeAttribute(default=JobState.SUBMITTED.value,
                              attr_name=BR_STATUS)  # the same as in CaaSJobs
    reason = UnicodeAttribute(null=True, attr_name=BR_FAILURE_REASON)
    cloud_identifier = UnicodeAttribute(null=True,
                                        attr_name=BR_CLOUD_ID)  # AWS:account_id AZURE:subscription_id
    tenant_name = UnicodeAttribute(attr_name=BR_TENANT_NAME)
    customer_name = UnicodeAttribute(attr_name=BR_CUSTOMER_NAME)
    registration_start = UnicodeAttribute(null=True,
                                          attr_name=BR_EVENT_REGISTRATION_START)
    registration_end = UnicodeAttribute(null=True,
                                        attr_name=BR_EVENT_REGISTRATION_END)
    submitted_at = UnicodeAttribute(attr_name=BR_JOB_SUBMITTED_AT,
                                    default=utc_iso)
    stopped_at = UnicodeAttribute(null=True, attr_name=BR_JOB_STOPPED_AT)

    customer_name_submitted_at_index = CustomerNameSubmittedAtIndex()
    tenant_name_submitted_at_index = TenantNameSubmittedAtIndex()

    def regions_to_rules(self) -> dict[str, set[str]]:
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

    def rules_to_regions(self) -> dict[str, set[str]]:
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
