import sys

from helpers.constants import JobType
from helpers.log_helper import get_logger
from models.job import Job
from common.base import BasePatch


_LOG = get_logger(__name__)
COUNT_STEP = 100


class PatchJobs(BasePatch):
    """Patch for adding job_type field to existing jobs."""

    @property
    def name(self) -> str:
        """Return the name of the patch."""
        return "jobs"

    def _execute(self) -> int:
        """
        Patch all existing SREJobs to add job_type field.
        Sets job_type to STANDARD for jobs that don't have it set.
        This patch is idempotent - safe to run multiple times.
        """
        _LOG.info("Starting job_type migration patch")
        if self.dry_run:
            _LOG.info("DRY RUN mode: no changes will be written")

        updated_count = 0
        skipped_count = 0
        error_count = 0
        total_processed = 0

        try:
            _LOG.info("Scanning all jobs from SREJobs table")

            for job in Job.scan():
                job: Job
                job_id = job.id
                total_processed += 1

                try:
                    # Check if job_type attribute exists and is None
                    # For old records, JOB_TYPE ('ty') might not be present
                    if hasattr(job, "job_type") and job.job_type is None:
                        if not self.dry_run:
                            _LOG.info(
                                f"Job {job_id}: missing job_type, setting to STANDARD"
                            )
                            job.update(actions=[Job.job_type.set(JobType.STANDARD)])
                            updated_count += 1
                        else:
                            _LOG.info(
                                f"DRY RUN: Would set job_type=STANDARD for job {job_id}"
                            )
                            updated_count += 1

                        if updated_count % COUNT_STEP == 0:
                            _LOG.info(
                                f"Progress update: Updated {updated_count} jobs "
                                f"(processed {total_processed} total)"
                            )
                    else:
                        # Job already has job_type, skip
                        skipped_count += 1
                        _LOG.info(
                            f"Job {job_id}: already has job_type={job.job_type}, skipping"
                        )

                    # Periodic progress report
                    if total_processed % COUNT_STEP == 0:
                        _LOG.info(
                            f"Progress: Processed {total_processed} jobs "
                            f"(Updated: {updated_count}, Skipped: {skipped_count}, "
                            f"Errors: {error_count})"
                        )

                except Exception as e:
                    error_count += 1
                    _LOG.error(
                        f"Failed to process job {job_id}: {e}", exc_info=True
                    )
                    # Continue processing other jobs even if one fails

            _LOG.info(
                f"Migration completed successfully. "
                f"Total processed: {total_processed}, "
                f"Updated: {updated_count}, Skipped: {skipped_count}, Errors: {error_count}"
            )

            if error_count > 0:
                _LOG.warning(
                    f"Migration completed with {error_count} error(s) out of "
                    f"{total_processed} total jobs processed"
                )
                return 1

            if updated_count == 0:
                _LOG.info(
                    "No jobs required updates - all jobs already have job_type set"
                )

            return 0

        except Exception as e:
            _LOG.exception(f"Unexpected error during migration: {e}")
            _LOG.error(
                f"Migration failed. Processed: {total_processed}, "
                f"Updated: {updated_count}, Skipped: {skipped_count}, Errors: {error_count}"
            )
            return 1


def main() -> int:
    """Main function for standalone execution."""
    patch = PatchJobs()
    return patch.run()

if __name__ == "__main__":
    sys.exit(main())
