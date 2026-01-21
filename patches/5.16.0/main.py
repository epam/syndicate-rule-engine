import logging
import sys

from helpers.constants import JobType
from helpers.log_helper import get_logger
from models.job import Job


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s",
    level=logging.INFO,
)
_LOG = get_logger(__name__)
COUNT_STEP = 100


def patch_jobs():
    """
    Patch all existing SREJobs to add job_type field.
    Sets job_type to STANDARD for jobs that don't have it set.
    This patch is idempotent - safe to run multiple times.
    """
    _LOG.info("Starting job_type migration patch")

    updated_count = 0
    skipped_count = 0
    error_count = 0

    try:
        # Scan all jobs
        _LOG.info("Scanning all jobs from SREJobs table")
        total_processed = 0

        for job in Job.scan():
            job: Job

            total_processed += 1
            try:
                # Check if job_type attribute exists in raw attribute_values
                # For old records, JOB_TYPE ('ty') might not be present
                if hasattr(job, 'job_type') and job.job_type is None:
                    _LOG.debug(f"Job {job.id} missing job_type, setting to STANDARD")
                    job.update(actions=[Job.job_type.set(JobType.STANDARD)])
                    updated_count += 1
                    if updated_count % COUNT_STEP == 0:
                        _LOG.info(f"Updated {updated_count} jobs so far...")
                else:
                    # Job already has job_type, skip
                    skipped_count += 1
                    if skipped_count % COUNT_STEP == 0:
                        _LOG.debug(
                            f"Processed {skipped_count} jobs that already have job_type"
                        )

                if total_processed % COUNT_STEP == 0:
                    _LOG.info(
                        f"Processed {total_processed} jobs total. "
                        f"Updated: {updated_count}, Skipped: {skipped_count}, Errors: {error_count}"
                    )
            except Exception as e:
                error_count += 1
                _LOG.error(f"Error processing job {job.id}: {e}", exc_info=True)
                # Continue processing other jobs even if one fails

        _LOG.info(
            f"Migration completed. Updated: {updated_count}, "
            f"Skipped: {skipped_count}, Errors: {error_count}"
        )

        if error_count > 0:
            _LOG.warning(f"Migration completed with {error_count} errors")
            return 1

        return 0
    except Exception as e:
        _LOG.exception(f"Unexpected error during migration: {e}")
        return 1


def main() -> int:
    try:
        return patch_jobs()
    except Exception as e:
        _LOG.exception(f"Unexpected exception: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
