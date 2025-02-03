import itertools
import statistics
from typing import Annotated, Any, Generator

import msgspec
from typing_extensions import Self

PercentFloat = Annotated[float, msgspec.Meta(ge=-1.0, le=1.0)]


class StandardCoverageCalculator:
    """
    As simple as it can be. Calculates coverage for some specific standard and
    version pair.

    Each standard has a concrete number N of so-called "controls".
    Coverage for a standard is a mean value of coverages of all its controls.
    Coverage for a control is ratio between number of rules that successfully
    verified this control and total number of rules that check for this
    control (currently we have no technical ability to take number of
    resources in consideration). Also, coverage for a control can be
    derived based on client's answers regarding some specific aspects of his
    infrastructure. Those controls must be provided from outside.
    User responses have higher priority.
    """

    __slots__ = '_cm', '_tc', '_buf'

    def __init__(
        self,
        coverage_of_missing: float = 0.0,
        total_controls: int | None = None,
    ):
        """
        Initialized a new coverage calculator. Its methods generally return
        the same instance of calculator and can be used in calls chain.

        :param coverage_of_missing: how covered to consider controls without
        provided information
        :param total_controls: total number of controls. If not specified the
        number of already provided controls will be used
        """
        self._cm: PercentFloat = coverage_of_missing
        self._buf: dict[str, PercentFloat] = {}
        # TODO: allow to use Decimal if needed + validate each percent to be
        # either -1 or 0..1

        self._tc: int | None = None
        if total_controls is not None:
            self.set_total(total_controls)

    def reset(self) -> Self:
        """
        Resets ONLY controls buffer.
        """
        self._buf.clear()
        return self

    def set_total(self, n: int) -> Self:
        assert n > 0, 'total must be bigger than 0'
        self._tc = n
        return self

    @property
    def total(self) -> int:
        if self._tc is not None:
            return self._tc
        return len(self._buf)

    def update(
        self,
        one: dict[str, PercentFloat] | str,
        two: PercentFloat | int | None = None,
        three: int | None = None,
        /,
    ) -> Self:
        """
        It's up to you not to pass values that higher than 1 or less 0 here
        (-1 is allowed)
        >>> c = CoverageCalculator()
        >>> c.update({"1.1": 0.5, "1.2": 0.5})  # ok
        >>> c.update('1.1', 0.5)  # ok
        >>> c.update('1.1', 2, 4)  # ok
        """
        if isinstance(one, dict):
            self._buf.update(one)
            return self
        if three is not None:
            assert two is not None and two <= three, 'Invalid usage'
            self._buf[str(one)] = two / three
            return self

        assert two is not None, 'Invalid usage'
        self._buf[str(one)] = two
        return self

    def produce(self) -> float | None:
        """
        Returns None if cannot calculate coverage. It can be in case the
        total number of controls is zero or specified total is lower than
        number of provided controls.
        Controls with -1.0 coverage are discarded from calculation.
        """
        buf_l = len(self._buf)
        total = self.total
        if total == 0 or total < buf_l:
            return
        # by here total >= len(self._buf)

        it = itertools.chain(
            self._buf.values(), itertools.repeat(self._cm, total - buf_l)
        )
        it = filter(lambda x: x != -1.0, it)
        items = tuple(it)
        if len(items) == 0:
            return
        return statistics.mean(items)


def calculate_controls_coverages(
    successful: dict[str, int], total: dict[str, int]
) -> dict[str, float]:
    res = {}
    for control, total_n in total.items():
        successful_n = successful.get(control)
        if successful_n:
            res[control] = successful_n / total_n
        else:
            res[control] = 0.0
    return res


class MappingAverageCalculator:
    __slots__ = ('_buf',)

    def __init__(self):
        self._buf = {}

    def update(self, dct: dict[Any, float]) -> None:
        for k, v in dct.items():
            self._buf.setdefault(k, []).append(v)

    def produce(self) -> Generator[tuple[Any, float], None, None]:
        for k, v in self._buf.items():
            yield k, statistics.mean(v)

    def reset(self) -> None:
        self._buf.clear()
