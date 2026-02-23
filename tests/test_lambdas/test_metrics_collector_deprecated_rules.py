"""
Tests for deprecated rules functionality in MetricsCollector.
"""

import datetime
from unittest.mock import MagicMock

import msgspec
import pytest

from helpers.constants import DEPRECATED_RULE_SUFFIX
from lambdas.metrics_updater.processors.metrics_collector import (
    DeprecatedRule,
    MetricsCollector,
    ReportRulesMetadata,
)
from services.metadata import Deprecation, Metadata, RuleMetadata
from services.sharding import ShardsCollection


class TestDeprecatedRule:
    """Tests for DeprecatedRule struct."""

    def test_deprecated_rule_creation_with_all_fields(self):
        """Test creating DeprecatedRule with all fields."""
        rule = DeprecatedRule(
            id="ecc-aws-123-s3_test_policy-deprecated",
            description="Test deprecated rule description",
            deprecation_date="2025-12-31",
            deprecation_reason="https://example.com/deprecation-info",
        )

        assert rule.id == "ecc-aws-123-s3_test_policy-deprecated"
        assert rule.description == "Test deprecated rule description"
        assert rule.deprecation_date == "2025-12-31"
        assert rule.deprecation_reason == "https://example.com/deprecation-info"

    def test_deprecated_rule_creation_with_minimal_fields(self):
        """Test creating DeprecatedRule with only required id field."""
        rule = DeprecatedRule(id="ecc-aws-123-s3_test_policy-deprecated")

        assert rule.id == "ecc-aws-123-s3_test_policy-deprecated"
        assert rule.description is msgspec.UNSET
        assert rule.deprecation_date is msgspec.UNSET
        assert rule.deprecation_reason is msgspec.UNSET

    def test_deprecated_rule_serialization(self):
        """Test that DeprecatedRule can be serialized to JSON."""
        rule = DeprecatedRule(
            id="ecc-aws-123-s3_test_policy-deprecated",
            description="Test description",
            deprecation_date="2025-12-31",
            deprecation_reason="Test reason",
        )

        # Serialize to JSON
        json_data = msgspec.json.encode(rule)
        assert isinstance(json_data, bytes)

        # Decode back
        decoded = msgspec.json.decode(json_data, type=DeprecatedRule)
        assert decoded.id == rule.id
        assert decoded.description == rule.description
        assert decoded.deprecation_date == rule.deprecation_date
        assert decoded.deprecation_reason == rule.deprecation_reason

    def test_deprecated_rule_with_none_date(self):
        """Test DeprecatedRule with None deprecation_date."""
        rule = DeprecatedRule(
            id="ecc-aws-123-s3_test_policy-deprecated",
            deprecation_date=None,
        )

        assert rule.deprecation_date is None


class TestIterDeprecatedRules:
    """Tests for _iter_deprecated_rules method."""

    @pytest.fixture
    def mock_collection(self):
        """Create a mock ShardsCollection with deprecated rules."""
        collection = MagicMock(spec=ShardsCollection)
        collection.meta = {
            "ecc-aws-123-s3_test_policy-deprecated": {
                "description": "S3 test policy description",
                "resource": "aws.s3",
            },
            "ecc-aws-150-api_gateway_rest_api_encryption_at_rest": {
                "description": "API Gateway encryption",
                "resource": "aws.apigateway",
            },
            "ecc-aws-200-deprecated-rule-deprecated": {
                "description": "Another deprecated rule",
                "resource": "aws.ec2",
            },
        }
        return collection

    @pytest.fixture
    def mock_metadata(self):
        """Create a mock Metadata with deprecation information."""
        metadata = Metadata(
            rules={
                "ecc-aws-123-s3_test_policy-deprecated": RuleMetadata(
                    source="AWS",
                    category="Security",
                    service_section="Storage",
                    service="S3",
                    article="",
                    impact="Test impact",
                    remediation="",
                    cloud="AWS",
                    deprecation=Deprecation(
                        date=datetime.date(2025, 12, 31),
                        link="https://example.com/deprecation-info",
                    ),
                ),
                "ecc-aws-200-deprecated-rule-deprecated": RuleMetadata(
                    source="AWS",
                    category="Security",
                    service_section="Compute",
                    service="EC2",
                    article="",
                    impact="Another impact",
                    remediation="",
                    cloud="AWS",
                    deprecation=Deprecation(
                        date=datetime.date(2026, 1, 15),
                        link="",
                    ),
                ),
            }
        )
        return metadata

    def test_iter_deprecated_rules_finds_deprecated_rules(
        self, mock_collection, mock_metadata
    ):
        """Test that _iter_deprecated_rules finds all deprecated rules."""
        collector = MetricsCollector.build()
        deprecated_rules = list(
            collector._iter_deprecated_rules(mock_collection, mock_metadata)
        )

        # Should find 2 deprecated rules (ending with -deprecated)
        assert len(deprecated_rules) == 2
        rule_ids = {rule.id for rule in deprecated_rules}
        assert "ecc-aws-123-s3_test_policy-deprecated" in rule_ids
        assert "ecc-aws-200-deprecated-rule-deprecated" in rule_ids
        assert "ecc-aws-150-api_gateway_rest_api_encryption_at_rest" not in rule_ids

    def test_iter_deprecated_rules_with_description_from_collection(
        self, mock_collection, mock_metadata
    ):
        """Test that description is taken from collection.meta."""
        collector = MetricsCollector.build()
        deprecated_rules = list(
            collector._iter_deprecated_rules(mock_collection, mock_metadata)
        )

        rule = next(
            r
            for r in deprecated_rules
            if r.id == "ecc-aws-123-s3_test_policy-deprecated"
        )
        assert rule.description == "S3 test policy description"

    def test_iter_deprecated_rules_with_description_fallback(
        self, mock_collection, mock_metadata
    ):
        """Test that description falls back to impact if not in collection.meta."""
        # Remove description from collection.meta
        mock_collection.meta["ecc-aws-123-s3_test_policy-deprecated"] = {
            "resource": "aws.s3",
        }

        collector = MetricsCollector.build()
        deprecated_rules = list(
            collector._iter_deprecated_rules(mock_collection, mock_metadata)
        )

        rule = next(
            r
            for r in deprecated_rules
            if r.id == "ecc-aws-123-s3_test_policy-deprecated"
        )
        # Should fallback to impact
        assert rule.description == "Test impact"

    def test_iter_deprecated_rules_with_deprecation_date(
        self, mock_collection, mock_metadata
    ):
        """Test that deprecation_date is formatted correctly."""
        collector = MetricsCollector.build()
        deprecated_rules = list(
            collector._iter_deprecated_rules(mock_collection, mock_metadata)
        )

        rule = next(
            r
            for r in deprecated_rules
            if r.id == "ecc-aws-123-s3_test_policy-deprecated"
        )
        assert rule.deprecation_date == "2025-12-31"

    def test_iter_deprecated_rules_with_deprecation_reason_from_link(
        self, mock_collection, mock_metadata
    ):
        """Test that deprecation_reason uses link when available."""
        collector = MetricsCollector.build()
        deprecated_rules = list(
            collector._iter_deprecated_rules(mock_collection, mock_metadata)
        )

        rule = next(
            r
            for r in deprecated_rules
            if r.id == "ecc-aws-123-s3_test_policy-deprecated"
        )
        assert rule.deprecation_reason == "https://example.com/deprecation-info"

    def test_iter_deprecated_rules_with_deprecation_reason_fallback(
        self, mock_collection, mock_metadata
    ):
        """Test that deprecation_reason falls back to impact when link is empty."""
        collector = MetricsCollector.build()
        deprecated_rules = list(
            collector._iter_deprecated_rules(mock_collection, mock_metadata)
        )

        rule = next(
            r
            for r in deprecated_rules
            if r.id == "ecc-aws-200-deprecated-rule-deprecated"
        )
        # Should fallback to impact since link is empty
        assert rule.deprecation_reason == "Another impact"

    def test_iter_deprecated_rules_with_no_date(self, mock_collection):
        """Test handling of deprecated rule without deprecation date."""
        metadata = Metadata(
            rules={
                "ecc-aws-123-s3_test_policy-deprecated": RuleMetadata(
                    source="AWS",
                    category="Security",
                    service_section="Storage",
                    service="S3",
                    article="",
                    impact="Test impact",
                    remediation="",
                    cloud="AWS",
                    deprecation=Deprecation(),  # No date
                ),
            }
        )

        collector = MetricsCollector.build()
        deprecated_rules = list(
            collector._iter_deprecated_rules(mock_collection, metadata)
        )

        rule = next(
            r
            for r in deprecated_rules
            if r.id == "ecc-aws-123-s3_test_policy-deprecated"
        )
        assert rule.deprecation_date is msgspec.UNSET

    def test_iter_deprecated_rules_with_empty_collection(self):
        """Test _iter_deprecated_rules with empty collection."""
        collection = MagicMock(spec=ShardsCollection)
        collection.meta = {}
        metadata = Metadata()

        collector = MetricsCollector.build()
        deprecated_rules = list(collector._iter_deprecated_rules(collection, metadata))

        assert len(deprecated_rules) == 0


class TestReportRulesMetadataWithDeprecatedRules:
    """Tests for ReportRulesMetadata with deprecated rules."""

    def test_report_rules_metadata_with_deprecated_rules(self):
        """Test ReportRulesMetadata can store DeprecatedRule objects."""
        deprecated_rule = DeprecatedRule(
            id="ecc-aws-123-s3_test_policy-deprecated",
            description="Test description",
            deprecation_date="2025-12-31",
            deprecation_reason="Test reason",
        )

        metadata = ReportRulesMetadata(
            total=10,
            disabled=(),
            deprecated=(deprecated_rule,),
            passed=(),
            failed=(),
        )

        assert len(metadata.deprecated) == 1
        assert metadata.deprecated[0].id == "ecc-aws-123-s3_test_policy-deprecated"
        assert metadata.deprecated[0].description == "Test description"

    def test_report_rules_metadata_with_multiple_deprecated_rules(self):
        """Test ReportRulesMetadata with multiple deprecated rules."""
        deprecated_rules = (
            DeprecatedRule(
                id="ecc-aws-123-s3_test_policy-deprecated",
                description="First deprecated rule",
                deprecation_date="2025-12-31",
                deprecation_reason="Reason 1",
            ),
            DeprecatedRule(
                id="ecc-aws-200-deprecated-rule-deprecated",
                description="Second deprecated rule",
                deprecation_date="2026-01-15",
                deprecation_reason="Reason 2",
            ),
        )

        metadata = ReportRulesMetadata(
            total=10,
            disabled=(),
            deprecated=deprecated_rules,
            passed=(),
            failed=(),
        )

        assert len(metadata.deprecated) == 2
        assert metadata.deprecated[0].id == "ecc-aws-123-s3_test_policy-deprecated"
        assert metadata.deprecated[1].id == "ecc-aws-200-deprecated-rule-deprecated"

    def test_report_rules_metadata_serialization(self):
        """Test that ReportRulesMetadata with deprecated rules serializes correctly."""
        deprecated_rule = DeprecatedRule(
            id="ecc-aws-123-s3_test_policy-deprecated",
            description="Test description",
            deprecation_date="2025-12-31",
            deprecation_reason="Test reason",
        )

        metadata = ReportRulesMetadata(
            total=10,
            disabled=(),
            deprecated=(deprecated_rule,),
            passed=(),
            failed=(),
        )

        # Serialize to JSON
        json_data = msgspec.json.encode(metadata)
        assert isinstance(json_data, bytes)

        # Decode back
        decoded = msgspec.json.decode(json_data, type=ReportRulesMetadata)
        assert len(decoded.deprecated) == 1
        assert decoded.deprecated[0].id == deprecated_rule.id
        assert decoded.deprecated[0].description == deprecated_rule.description
        assert (
            decoded.deprecated[0].deprecation_date == deprecated_rule.deprecation_date
        )
        assert (
            decoded.deprecated[0].deprecation_reason
            == deprecated_rule.deprecation_reason
        )

    def test_report_rules_metadata_with_empty_deprecated(self):
        """Test ReportRulesMetadata with empty deprecated rules."""
        metadata = ReportRulesMetadata(
            total=10,
            disabled=(),
            deprecated=(),
            passed=(),
            failed=(),
        )

        assert len(metadata.deprecated) == 0
        assert metadata.deprecated == ()


class TestDeprecatedRulesIntegration:
    """Integration tests for deprecated rules in the metrics collection process."""

    @pytest.fixture
    def mock_collection_with_deprecated(self):
        """Create a mock collection with both regular and deprecated rules."""
        collection = MagicMock(spec=ShardsCollection)
        collection.meta = {
            "ecc-aws-123-s3_test_policy-deprecated": {
                "description": "Deprecated S3 policy",
                "resource": "aws.s3",
            },
            "ecc-aws-150-api_gateway_rest_api_encryption_at_rest": {
                "description": "API Gateway encryption",
                "resource": "aws.apigateway",
            },
        }
        return collection

    @pytest.fixture
    def mock_metadata_with_deprecation(self):
        """Create metadata with deprecation info."""
        return Metadata(
            rules={
                "ecc-aws-123-s3_test_policy-deprecated": RuleMetadata(
                    source="AWS",
                    category="Security",
                    service_section="Storage",
                    service="S3",
                    article="",
                    impact="This rule is deprecated",
                    remediation="",
                    cloud="AWS",
                    deprecation=Deprecation(
                        date=datetime.date(2025, 12, 31),
                        link="https://example.com/deprecation",
                    ),
                ),
            }
        )

    def test_deprecated_rules_in_scope_filtering(
        self, mock_collection_with_deprecated, mock_metadata_with_deprecation
    ):
        """Test that deprecated rules are correctly filtered when in scope."""
        collector = MetricsCollector.build()
        deprecated = tuple(
            collector._iter_deprecated_rules(
                mock_collection_with_deprecated, mock_metadata_with_deprecation
            )
        )

        # Extract IDs for scope operations
        deprecated_ids = {rule.id for rule in deprecated}

        # Simulate scope intersection
        scope = {
            "ecc-aws-123-s3_test_policy-deprecated",
            "ecc-aws-150-api_gateway_rest_api_encryption_at_rest",
        }
        deprecated_in_scope = tuple(
            rule for rule in deprecated if rule.id in scope.intersection(deprecated_ids)
        )

        assert len(deprecated_in_scope) == 1
        assert deprecated_in_scope[0].id == "ecc-aws-123-s3_test_policy-deprecated"
        assert deprecated_in_scope[0].description == "Deprecated S3 policy"
        assert deprecated_in_scope[0].deprecation_date == "2025-12-31"
        assert (
            deprecated_in_scope[0].deprecation_reason
            == "https://example.com/deprecation"
        )

    def test_deprecated_rules_json_structure(self):
        """Test that deprecated rules serialize to expected JSON structure."""
        deprecated_rule = DeprecatedRule(
            id="ecc-aws-123-s3_test_policy-deprecated",
            description="Test deprecated rule",
            deprecation_date="2025-12-31",
            deprecation_reason="Rule is being replaced",
        )

        # Serialize to JSON
        json_bytes = msgspec.json.encode(deprecated_rule)
        json_str = json_bytes.decode("utf-8")

        # Verify structure
        assert '"id"' in json_str
        assert '"description"' in json_str
        assert '"deprecation_date"' in json_str
        assert '"deprecation_reason"' in json_str
        assert "ecc-aws-123-s3_test_policy-deprecated" in json_str
        assert "2025-12-31" in json_str

    def test_report_rules_metadata_json_structure(self):
        """Test that ReportRulesMetadata with deprecated rules has correct JSON structure."""
        deprecated_rule = DeprecatedRule(
            id="ecc-aws-123-s3_test_policy-deprecated",
            description="Test description",
            deprecation_date="2025-12-31",
            deprecation_reason="Test reason",
        )

        metadata = ReportRulesMetadata(
            total=15,
            disabled=(),
            deprecated=(deprecated_rule,),
            passed=(),
            failed=(),
        )

        # Serialize to JSON
        json_bytes = msgspec.json.encode(metadata)
        json_str = json_bytes.decode("utf-8")

        # Verify structure matches expected format
        assert '"deprecated"' in json_str
        assert '"id"' in json_str
        assert '"description"' in json_str
        assert '"deprecation_date"' in json_str
        assert '"deprecation_reason"' in json_str
