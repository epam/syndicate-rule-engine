from abc import ABC, abstractmethod


from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


class BasePatch(ABC):
    """Base class for all patches."""

    def __init__(
        self,
        dry_run: bool = False,
        isolated: bool = False,
    ) -> None:
        self.dry_run = dry_run
        self.isolated = isolated

    def set_dry_run(self, dry_run: bool) -> None:
        """Set dry_run flag. When True, patch runs without writing changes."""
        self.dry_run = dry_run

    def set_isolated(self, isolated: bool) -> None:
        """Set isolated flag. When True, patch runs in isolated mode for CI."""
        self.isolated = isolated

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of the patch."""
        ...

    @abstractmethod
    def _execute(self) -> int:
        """Execute the patch logic. Returns 0 on success, 1 on failure."""
        ...

    def run(self) -> int:
        """Run the patch."""
        if self.isolated:
            # For CI, we don't want to run the patch in isolated mode.
            _LOG.info("Running patch in isolated mode for CI, skipping all logic.")
            return 0

        try:
            return self._execute()
        except Exception as e:
            _LOG.exception(f"Fatal error in {self.name}: {e}")
            return 1
