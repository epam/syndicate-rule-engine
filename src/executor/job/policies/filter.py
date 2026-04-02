"""Policy filtering and deduplication."""

from pathlib import Path
from typing import Iterable

from helpers.log_helper import get_logger

from executor.job.execution.context import JobExecutionContext

_LOG = get_logger(__name__)


def filter_policies(
    it: Iterable[dict],
    keep: set[str] | None = None,
    exclude: set[str] | None = None,
) -> Iterable[dict]:
    if exclude:
        it = filter(lambda p: p['name'] not in exclude, it)
    if keep:
        it = filter(lambda p: p['name'] in keep, it)
    return it


def skip_duplicated_policies(
    ctx: JobExecutionContext,
    it: Iterable[dict],
    deduplicate_by_fingerprint: bool = True,
) -> Iterable[dict]:
    """
    Skip policies that appear more than once.

    First level: exact name deduplication (original behaviour).
    Second level (``deduplicate_by_fingerprint=True``): if two policies
    have different names but the same ``fingerprint`` field, only the
    first one is executed.  The mapping between fingerprint and all
    skipped aliases is stored in ``ctx.fingerprint_aliases`` so results
    can later be expanded to all aliases via
    ``expand_results_to_aliases``.
    """
    emitted_names: set[str] = set()
    emitted_fps: dict[str, str] = {}
    duplicated_names: list[str] = []
    fp_skipped: list[str] = []

    for p in it:
        name = p['name']
        fp = p.get('fingerprint') if deduplicate_by_fingerprint else None

        if name in emitted_names:
            _LOG.warning(f'Duplicated policy found {name} (fingerprint: {fp}). Skipping')
            duplicated_names.append(name)
            continue
        emitted_names.add(name)

        if fp:
            if fp in emitted_fps:
                primary = emitted_fps[fp]
                _LOG.info(
                    f'Policy {name} shares fingerprint {fp} with '
                    f'{primary}. Skipping execution (will be expanded to aliases later)'
                )
                ctx.fingerprint_aliases.setdefault(fp, [primary]).append(name)
                fp_skipped.append(name)
                continue
            emitted_fps[fp] = name
            ctx.fingerprint_aliases.setdefault(fp, [name])
            _LOG.debug(f'Policy {name} added with fingerprint {fp}')

        yield p

    if duplicated_names:
        ctx.add_warnings(
            *[
                f'multiple policies with name {name}'
                for name in sorted(duplicated_names)
            ]
        )
    if fp_skipped:
        _LOG.info(
            f'Fingerprint dedup: skipped {len(fp_skipped)} policies '
            f'(aliases: {fp_skipped})'
        )


def expand_results_to_aliases(
    ctx: JobExecutionContext,
    work_dir: Path,
) -> None:
    """
    After scanning, duplicate the output files of each "primary" policy
    to all its fingerprint aliases so that reports attribute findings
    correctly to every rule name.

    Cloud Custodian writes its results to ``<work_dir>/<policy_name>/``.
    For every fingerprint group that has aliases, this function copies
    the primary result directory to each alias directory.
    """
    import shutil

    for fp, names in ctx.fingerprint_aliases.items():
        if len(names) <= 1:
            continue
        primary = names[0]
        primary_dir = work_dir / primary
        if not primary_dir.exists():
            continue
        for alias in names[1:]:
            alias_dir = work_dir / alias
            if alias_dir.exists():
                continue
            _LOG.info(
                f'Expanding results from {primary} to alias {alias} '
                f'(fp={fp})'
            )
            shutil.copytree(primary_dir, alias_dir)
    _LOG.info('Finished expanding results to aliases')
