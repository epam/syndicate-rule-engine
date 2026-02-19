"""
Patch for backfilling rule fingerprints.

Scans every Rule item via the PynamoDB model (MongoDB adapter in docker
mode), computes a content-based fingerprint (SHA-256 of resource +
normalized filters), writes it to the ``fp`` field, and reports duplicate
groups (rules that share the same fingerprint but have different names).

The patch is idempotent — running it more than once is safe.  Rules that
already have the correct fingerprint are skipped.
"""

import sys

from collections import defaultdict

from src.helpers.fingerprint import compute_rule_fingerprint  # TODO: remove src.
from src.helpers.log_helper import get_logger  # TODO: remove src.
from src.models.rule import Rule  # TODO: remove src.


_LOG = get_logger(__name__)


def patch_fingerprints() -> None:
    total = 0
    updated = 0
    skipped = 0
    errors = 0

    # fingerprint -> list of rule names (for duplicate detection)
    fp_groups: dict[str, list[str]] = defaultdict(list)

    _LOG.info("Starting fingerprint backfill for Rule model")

    for rule in Rule.scan():
        total += 1
        name = rule.name

        if not rule.resource:
            _LOG.warning("Rule %s has no resource field — skipping", name)
            errors += 1
            continue

        fp = compute_rule_fingerprint(rule.resource, rule.filters or [])
        fp_groups[fp].append(name)

        if rule.fingerprint == fp:
            skipped += 1
        else:
            rule.update(actions=[Rule.fingerprint.set(fp)])
            updated += 1

        if total % 500 == 0:
            _LOG.info("Processed %d rules so far ...", total)

    _LOG.info(
        "Backfill complete: %d total, %d updated, %d already up-to-date, " "%d errors",
        total,
        updated,
        skipped,
        errors,
    )

    # Report duplicate groups
    dup_count = 0
    for fp, names in sorted(fp_groups.items()):
        if len(names) > 1:
            dup_count += 1
            _LOG.info(
                "  Duplicate group (fp=%s, count=%d): %s",
                fp,
                len(names),
                sorted(names),
            )

    _LOG.info(
        "Summary: %d unique fingerprints, %d duplicate groups",
        len(fp_groups),
        dup_count,
    )


def main() -> int:
    try:
        patch_fingerprints()
        return 0
    except Exception:
        _LOG.exception("Unexpected exception")
        return 1


if __name__ == "__main__":
    sys.exit(main())
