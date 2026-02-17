import logging
import sys

from src.helpers.log_helper import get_logger  # TODO: remove src.

from .patch_fingerprints import main as patch_fingerprints
from .patch_jobs import main as patch_jobs


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s",
    level=logging.INFO,
)
_LOG = get_logger(__name__)


def main() -> int:
    status = 0
    status |= patch_fingerprints()
    status |= patch_jobs()
    return status


if __name__ == "__main__":
    sys.exit(main())
