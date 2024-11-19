from __future__ import annotations

import itertools
import statistics
from enum import Enum
from typing import Annotated, Generator

import msgspec
from typing_extensions import Self

PercentFloat = Annotated[float, msgspec.Meta(ge=-1.0, le=1.0)]
NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]


class QOption(str, Enum):
    """
    Questionnaire options
    """

    percent: PercentFloat

    def __new__(cls, value: str, percent: PercentFloat):
        obj = str.__new__(cls, value)
        obj._value_ = value

        obj.percent = percent
        return obj

    # these percents may be wrong but seems like all coverages have same values
    NOT_APPLICABLE = 'NA', -1.0
    NOT_COMPATIBLE = 'NC', 0.0
    SOMEWHAT_COMPATIBLE = 'SC', 0.25
    PARTIALLY_COMPATIBLE = 'PC', 0.5
    MOSTLY_COMPATIBLE = 'MC', 0.75
    FULLY_COMPATIBLE = 'FC', 1.0


class CoverageNode(msgspec.Struct, frozen=True):
    """
    Coverage Control is a recursive structure that holds some info
    about points and questinaries for a version of some standard
    """

    # NOTE: all commented values currently are just not necessary for
    # the algorithm so i ignore them.

    # percent: PercentFloat = msgspec.field(name='%')
    points: dict[str, CoverageNode] | msgspec.UnsetType = msgspec.field(
        name='P', default=msgspec.UNSET
    )
    # section_title: str | msgspec.UnsetType = msgspec.field(
    #     name='S', default=msgspec.UNSET
    # )
    # control_title: str | msgspec.UnsetType = msgspec.field(
    #     name='C', default=msgspec.UNSET
    # )
    # compliant_rules: NonNegativeInt | msgspec.UnsetType = msgspec.field(
    #     name='compliant_rules', default=msgspec.UNSET
    # )
    total_rules: NonNegativeInt | msgspec.UnsetType = msgspec.field(
        name='total_rules', default=msgspec.UNSET
    )

    # @property
    # def title(self) -> str | None:
    #     if self.control_title is not msgspec.UNSET:
    #         return self.control_title
    #     if self.section_title is not msgspec.UNSET:
    #         return self.section_title

    @property
    def is_control(self) -> bool:
        """
        Each control must have total_rules so we probably can rely on that.
        """
        return self.total_rules is not msgspec.UNSET

    @property
    def is_section(self) -> bool:
        """
        Everything that is not control is probably a section or a subsection.
        Not so important here.
        """
        return not self.is_control

    @classmethod
    def _traverse_points(
        cls, points: dict[str, CoverageNode] | msgspec.UnsetType
    ) -> Generator[tuple[str, CoverageNode], None, None]:
        if points is msgspec.UNSET or not points:
            return
        for name, leaf in points.items():
            yield name, leaf
            yield from cls._traverse_points(leaf.points)

    def traverse(self) -> Generator[tuple[str, CoverageNode], None, None]:
        """
        Iterates over all nodes. Yields name and corresponding node.
        Eield reserverd name for the root node: root
        """
        yield 'root', self
        yield from self._traverse_points(self.points)

    def iter_control_total_rules(
        self,
    ) -> Generator[tuple[str, int], None, None]:
        """
        Iterates over each node but yields only contols: their names and
        total number of rules. Those are values we need
        """
        _unset = msgspec.UNSET
        for name, leaf in self.traverse():
            # we don't need controls with 0 rules. Those are for questions
            if leaf.total_rules is _unset or leaf.total_rules == 0:
                continue
            yield name, leaf.total_rules


class CoverageCalculator:
    """
    As simple as it can be. Calculates coverage for some specific standard and
    version pair.

    Each standard has a concrete number N of so-called "controls".
    Coverage for a standard is a mean value of coverages of all its controls.
    Coverage for a control is ratio between number of rules that succesfully
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
