import argparse
import logging
import sys

from .base import BasePatch
from helpers.log_helper import get_logger

__all__ = ("BasePatch", "run_patches", "main")


_LOG = get_logger(__name__)


def get_patch_args() -> tuple[bool, bool]:
    """Get command line arguments for patches.

    Returns:
        tuple[bool, bool]: dry_run and isolated flags.
    """
    parser = argparse.ArgumentParser(
        description="Execute patches for syndicate-rule-engine"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run patches without writing changes (dry-run mode)",
    )
    parser.add_argument(
        "--isolated",
        action="store_true",
        help="Run patches in isolated mode for CI",
    )
    args = parser.parse_args()
    return args.dry_run, args.isolated


def run_patches(
    patches: list[BasePatch],
    dry_run: bool = False,
    isolated: bool = False,
) -> int:
    """Run all patches."""
    status = 0
    for patch in patches:
        patch.set_dry_run(dry_run)
        patch.set_isolated(isolated)
        _LOG.info("Running patch: %s", patch.name)
        status |= patch.run()
        _LOG.info("Patch %s completed with status: %s", patch.name, status)
    _LOG.info("Patch execution completed with status: %s", status)
    return status


def main(patches: list[BasePatch]) -> None:
    """Main entry point for patch execution.

    Args:
        patches: List of patch instances to execute.

    Exits:
        Exits with code 0 on success, non-zero on failure.
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s",
        level=logging.INFO,
    )

    dry_run, isolated = get_patch_args()

    _LOG.info(
        "Starting patch execution: dry_run=%s, isolated=%s",
        dry_run,
        isolated,
    )

    exit_code = run_patches(
        patches,
        dry_run=dry_run,
        isolated=isolated,
    )
    sys.exit(exit_code)
