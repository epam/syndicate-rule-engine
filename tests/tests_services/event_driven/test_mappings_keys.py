from __future__ import annotations

from helpers import Version
from helpers.constants import Cloud
from services.event_driven.mappings.provider import EventMappingBucketKeys


def test_event_mapping_key_with_version_object():
    key = EventMappingBucketKeys.event_mapping_key(
        license_key="lic-1",
        version=Version("2.4.0"),
        cloud=Cloud.AWS,
    )
    assert key == "mappings/lic-1/2.4.0/events/aws.json.gz"


def test_event_mapping_key_with_string_version_and_cloud_str():
    key = EventMappingBucketKeys.event_mapping_key(
        license_key="lic-2",
        version="1.0",
        cloud="AZURE",
    )
    assert key == "mappings/lic-2/1.0/events/azure.json.gz"
