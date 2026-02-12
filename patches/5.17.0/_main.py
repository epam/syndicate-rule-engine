import logging
import sys

from src.helpers.log_helper import get_logger  # TODO: remove src.

from .patch_fingerprints import patch_fingerprints


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s",
    level=logging.INFO,
)
_LOG = get_logger(__name__)


def main() -> int:
    try:
        patch_fingerprints()
        return 0
    except Exception:
        _LOG.exception("Unexpected exception")
        return 1


if __name__ == "__main__":
    sys.exit(main())
