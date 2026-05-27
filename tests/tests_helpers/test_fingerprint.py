"""
Tests for fingerprint functionality.

Demonstrates how fingerprint computation works and how to use UnifiedRuleIdentity.
"""

from helpers.fingerprint import compute_rule_fingerprint
from services.unified_rule_identity import UnifiedRuleIdentity


class TestFingerprintComputation:
    """Tests for fingerprint computation."""

    def test_same_resource_and_filters_produce_same_fingerprint(self):
        """Rules with the same resource and filters produce the same fingerprint."""
        resource = "aws.s3"
        filters = [{"type": "value", "key": "Encryption", "op": "ne", "value": None}]

        fp1 = compute_rule_fingerprint(resource, filters)
        fp2 = compute_rule_fingerprint(resource, filters)

        assert fp1 == fp2
        assert len(fp1) == 16  # First 16 characters of SHA-256

    def test_different_resources_produce_different_fingerprints(self):
        """Different resources produce different fingerprints."""
        filters = [{"type": "value", "key": "Encryption", "op": "ne", "value": None}]

        fp1 = compute_rule_fingerprint("aws.s3", filters)
        fp2 = compute_rule_fingerprint("aws.ec2", filters)

        assert fp1 != fp2

    def test_different_filters_produce_different_fingerprints(self):
        """Different filters produce different fingerprints."""
        resource = "aws.s3"

        fp1 = compute_rule_fingerprint(
            resource, [{"type": "value", "key": "Encryption"}]
        )
        fp2 = compute_rule_fingerprint(
            resource, [{"type": "value", "key": "Versioning"}]
        )

        assert fp1 != fp2

    def test_resource_normalization(self):
        """Resource is normalized (lowercase, strip)."""
        filters = []

        fp1 = compute_rule_fingerprint("AWS.S3", filters)
        fp2 = compute_rule_fingerprint("aws.s3", filters)
        fp3 = compute_rule_fingerprint("  aws.s3  ", filters)

        assert fp1 == fp2 == fp3

    def test_filter_normalization(self):
        """Filters are normalized (lowercase for strings, sorting for dict)."""
        resource = "aws.s3"

        # Different key order in dict - same fingerprint
        fp1 = compute_rule_fingerprint(
            resource, [{"type": "value", "key": "Encryption", "op": "ne"}]
        )
        fp2 = compute_rule_fingerprint(
            resource, [{"op": "ne", "key": "Encryption", "type": "value"}]
        )

        assert fp1 == fp2

    def test_string_filter_normalization(self):
        """String filters are normalized."""
        resource = "aws.s3"

        fp1 = compute_rule_fingerprint(resource, ["Cross-Account"])
        fp2 = compute_rule_fingerprint(resource, ["cross-account"])
        fp3 = compute_rule_fingerprint(resource, ["  CROSS-ACCOUNT  "])

        assert fp1 == fp2 == fp3

    def test_filter_order_matters(self):
        """Filter order matters (important for Cloud Custodian)."""
        resource = "aws.s3"

        fp1 = compute_rule_fingerprint(
            resource, [{"type": "value", "key": "A"}, {"type": "value", "key": "B"}]
        )
        fp2 = compute_rule_fingerprint(
            resource, [{"type": "value", "key": "B"}, {"type": "value", "key": "A"}]
        )

        assert fp1 != fp2  # Order matters!

    def test_empty_filters(self):
        """Empty filters work correctly."""
        fp = compute_rule_fingerprint("aws.s3", [])
        assert len(fp) == 16
        assert isinstance(fp, str)

    def test_complex_filters(self):
        """Complex nested filters are processed correctly."""
        resource = "aws.ec2"
        filters = [
            {
                "type": "and",
                "value": [
                    {"type": "value", "key": "Tags.Environment", "value": "Production"},
                    {"type": "value", "key": "State.Name", "value": "running"},
                ],
            }
        ]

        fp = compute_rule_fingerprint(resource, filters)
        assert len(fp) == 16
        assert isinstance(fp, str)


class TestUnifiedRuleIdentity:
    """Tests for UnifiedRuleIdentity."""

    def test_build_index(self):
        """Building index from rules."""
        identity = UnifiedRuleIdentity()

        # Create mock rules
        class MockRule:
            def __init__(self, name, fingerprint):
                self.name = name
                self.fingerprint = fingerprint

        rules = [
            MockRule("rule-1", "fp123"),
            MockRule("rule-2", "fp456"),
            MockRule("rule-3", "fp123"),  # Duplicate of rule-1
        ]

        identity.build_index(rules)  # type: ignore[arg-type]

        assert identity.total_rules == 3
        assert identity.total_unique == 2  # 2 unique fingerprints
        assert identity.total_duplicates == 1  # 1 duplicate group

    def test_get_fingerprint(self):
        """Get fingerprint by rule name."""
        identity = UnifiedRuleIdentity()
        identity.add_rule("rule-1", "fp123")
        identity.add_rule("rule-2", "fp456")

        assert identity.get_fingerprint("rule-1") == "fp123"
        assert identity.get_fingerprint("rule-2") == "fp456"
        assert identity.get_fingerprint("unknown") is None

    def test_get_canonical_name(self):
        """Get canonical name."""
        identity = UnifiedRuleIdentity()
        identity.add_rule("rule-1", "fp123")
        identity.add_rule("rule-2", "fp123")  # Same fingerprint

        # First rule becomes canonical
        assert identity.get_canonical_name("rule-1") == "rule-1"
        assert identity.get_canonical_name("rule-2") == "rule-1"  # Returns canonical
        assert identity.get_canonical_name("unknown") == "unknown"  # If not in index

    def test_get_all_aliases(self):
        """Get all aliases (rules with the same fingerprint)."""
        identity = UnifiedRuleIdentity()
        identity.add_rule("rule-1", "fp123")
        identity.add_rule("rule-2", "fp123")
        identity.add_rule("rule-3", "fp123")
        identity.add_rule("rule-4", "fp456")  # Different fingerprint

        aliases = identity.get_all_aliases("rule-1")
        assert aliases == {"rule-1", "rule-2", "rule-3"}

        aliases = identity.get_all_aliases("rule-4")
        assert aliases == {"rule-4"}

        aliases = identity.get_all_aliases("unknown")
        assert aliases == {"unknown"}  # If not in index

    def test_is_same_rule(self):
        """Check if two rules are the same."""
        identity = UnifiedRuleIdentity()
        identity.add_rule("rule-1", "fp123")
        identity.add_rule("rule-2", "fp123")
        identity.add_rule("rule-3", "fp456")

        assert identity.is_same_rule("rule-1", "rule-2") is True
        assert identity.is_same_rule("rule-1", "rule-3") is False
        assert identity.is_same_rule("unknown-1", "unknown-2") is False
        assert identity.is_same_rule("rule-1", "unknown") is False

    def test_iter_duplicate_groups(self):
        """Iterate over duplicate groups."""
        identity = UnifiedRuleIdentity()
        identity.add_rule("rule-1", "fp123")
        identity.add_rule("rule-2", "fp123")
        identity.add_rule("rule-3", "fp456")
        identity.add_rule("rule-4", "fp456")
        identity.add_rule("rule-5", "fp456")
        identity.add_rule("rule-6", "fp789")  # Unique

        duplicates = list(identity.iter_duplicate_groups())

        assert len(duplicates) == 2  # 2 duplicate groups

        # Check first group
        fp, names = duplicates[0]
        assert fp in ("fp123", "fp456")
        if fp == "fp123":
            assert names == {"rule-1", "rule-2"}
        else:
            assert names == {"rule-3", "rule-4", "rule-5"}

    def test_skip_rules_without_fingerprint(self):
        """Rules without fingerprint are ignored."""
        identity = UnifiedRuleIdentity()

        class MockRule:
            def __init__(self, name, fingerprint):
                self.name = name
                self.fingerprint = fingerprint

        rules = [
            MockRule("rule-1", "fp123"),
            MockRule("rule-2", None),  # No fingerprint
            MockRule("rule-3", ""),  # Empty fingerprint
        ]

        identity.build_index(rules)  # type: ignore[arg-type]

        assert identity.total_rules == 1  # Only rule-1
        assert identity.get_fingerprint("rule-1") == "fp123"
        assert identity.get_fingerprint("rule-2") is None
        assert identity.get_fingerprint("rule-3") is None

    def test_incremental_add(self):
        """Adding rules one by one."""
        identity = UnifiedRuleIdentity()

        identity.add_rule("rule-1", "fp123")
        assert identity.total_rules == 1

        identity.add_rule("rule-2", "fp123")
        assert identity.total_rules == 2
        assert identity.get_all_aliases("rule-1") == {"rule-1", "rule-2"}

        identity.add_rule("rule-3", "fp456")
        assert identity.total_rules == 3
        assert identity.total_unique == 2


class TestFingerprintIntegration:
    """Integration tests for fingerprint and UnifiedRuleIdentity."""

    def test_end_to_end_scenario(self):
        """Full scenario: fingerprint computation and alias handling."""
        # Create rules with the same content
        resource = "aws.s3"
        filters = [{"type": "value", "key": "Encryption", "op": "ne", "value": None}]

        fp1 = compute_rule_fingerprint(resource, filters)
        fp2 = compute_rule_fingerprint(resource, filters)

        assert fp1 == fp2

        # Create index
        identity = UnifiedRuleIdentity()
        identity.add_rule("ecc-aws-001-s3-encryption", fp1)
        identity.add_rule(
            "s3-encryption-check", fp2
        )  # Different name, same fingerprint

        # Check that these are the same rule
        assert identity.is_same_rule("ecc-aws-001-s3-encryption", "s3-encryption-check")

        # Get all aliases
        aliases = identity.get_all_aliases("ecc-aws-001-s3-encryption")
        assert aliases == {"ecc-aws-001-s3-encryption", "s3-encryption-check"}

        # Canonical name - first rule
        canonical = identity.get_canonical_name("s3-encryption-check")
        assert canonical == "ecc-aws-001-s3-encryption"
