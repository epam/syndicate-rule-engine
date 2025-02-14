"""
Azure from here https://azuretracks.com/2021/04/current-azure-region-names-reference/
"""

from itertools import chain
from typing import Set

from helpers import Enum
from helpers.constants import Cloud, GLOBAL_REGION

AWS_REGIONS = {
    'ap-northeast-1',
    'af-south-1',
    'eu-west-3',
    'eu-south-2',
    'ap-southeast-4',
    'eu-west-2',
    'eu-north-1',
    'ap-southeast-3',
    'ap-southeast-2',
    'us-east-2',
    'eu-west-1',
    'ap-south-1',
    'us-east-1',
    'ap-east-1',
    'us-gov-east-1',
    'ap-northeast-2',
    'me-south-1',
    'il-central-1',
    'ca-west-1',
    'ap-northeast-3',
    'us-gov-west-1',
    'us-west-1',
    'ca-central-1',
    'eu-south-1',
    'us-west-2',
    'eu-central-2',
    'ap-south-2',
    'me-central-1',
    'ap-southeast-1',
    'eu-central-1',
    'sa-east-1',
}

AZURE_REGIONS = {
    'eastus',
    'eastus2',
    'southcentralus',
    'westus2',
    'westus3',
    'australiaeast',
    'southeastasia',
    'northeurope',
    'swedencentral',
    'uksouth',
    'westeurope',
    'centralus',
    'southafricanorth',
    'centralindia',
    'eastasia',
    'japaneast',
    'koreacentral',
    'canadacentral',
    'francecentral',
    'germanywestcentral',
    'norwayeast',
    'switzerlandnorth',
    'uaenorth',
    'brazilsouth',
    'eastus2euap',
    'qatarcentral',
    'centralusstage',
    'eastusstage',
    'eastus2stage',
    'northcentralusstage',
    'southcentralusstage',
    'westusstage',
    'westus2stage',
    'asia',
    'asiapacific',
    'australia',
    'brazil',
    'canada',
    'europe',
    'france',
    'germany',
    'global',
    'india',
    'japan',
    'korea',
    'norway',
    'singapore',
    'southafrica',
    'switzerland',
    'uae',
    'uk',
    'unitedstates',
    'unitedstateseuap',
    'eastasiastage',
    'southeastasiastage',
    'eastusstg',
    'southcentralusstg',
    'northcentralus',
    'westus',
    'jioindiawest',
    'centraluseuap',
    'westcentralus',
    'southafricawest',
    'australiacentral',
    'australiacentral2',
    'australiasoutheast',
    'japanwest',
    'jioindiacentral',
    'koreasouth',
    'southindia',
    'westindia',
    'canadaeast',
    'francesouth',
    'germanynorth',
    'norwaywest',
    'switzerlandwest',
    'ukwest',
    'uaecentral',
    'brazilsoutheast',
}

GOOGLE_REGIONS = {
    'us-east1',
    'asia-northeast2',
    'us-west2',
    'us-west1',
    'us-west4',
    'us-east4',
    'asia-south1',
    'asia-northeast3',
    'asia-northeast1',
    'europe-west1',
    'europe-west6',
    'asia-east2',
    'asia-east1',
    'europe-north1',
    'europe-west2',
    'europe-west4',
    'southamerica-east1',
    'europe-west3',
    'us-west3',
    'us-central1',
    'asia-southeast1',
    'australia-southeast1',
    'asia-southeast2',
    'northamerica-northeast1',
}

CLOUD_REGIONS = {
    Cloud.AWS.value: AWS_REGIONS,
    Cloud.AZURE.value: AZURE_REGIONS,
    Cloud.GOOGLE.value: GOOGLE_REGIONS,
}

AWSRegion = Enum.build('AWSRegion', sorted(AWS_REGIONS))
AZURERegion = Enum.build('AZURERegion', sorted(AZURE_REGIONS))
GOOGLERegion = Enum.build('GOOGLERegion', sorted(GOOGLE_REGIONS))

AllRegions = Enum.build(
    'AllRegions',
    chain(AWSRegion.iter(), AZURERegion.iter(), GOOGLERegion.iter()),
)

AllRegionsWithGlobal = Enum.build(
    'AllRegionsWithMultiregional',
    chain(AllRegions.iter(), iter((GLOBAL_REGION,))),
)


def get_region_by_cloud(cloud: str) -> Set[str]:
    return CLOUD_REGIONS.get(cloud) or set()
