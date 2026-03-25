from __future__ import annotations

from unittest.mock import MagicMock

from helpers import Version
from helpers.constants import Cloud
from services.event_driven.mappings.provider import S3EventMappingProvider


def test_get_from_s3_returns_none_when_object_missing():
    s3 = MagicMock()
    s3.gz_get_json.return_value = None
    env = MagicMock()
    env.get_rulesets_bucket_name.return_value = "rules-bucket"

    provider = S3EventMappingProvider(s3_client=s3, environment_service=env)
    out = provider.get_from_s3("lic", Version("1.0.0"), Cloud.AWS)

    assert out is None
    s3.gz_get_json.assert_called_once()


def test_get_from_s3_caches_second_call():
    s3 = MagicMock()
    s3.gz_get_json.return_value = {"a": {"b": ["r"]}}
    env = MagicMock()
    env.get_rulesets_bucket_name.return_value = "rules-bucket"

    provider = S3EventMappingProvider(s3_client=s3, environment_service=env)
    v = Version("2.0.0")
    first = provider.get_from_s3("lic", v, Cloud.AWS)
    second = provider.get_from_s3("lic", v, Cloud.AWS)

    assert first == second
    assert s3.gz_get_json.call_count == 1
