from typing import Generator, Iterable

from helpers.constants import Severity


class Standard(tuple):
    # TODO: can be broken during sorting if there are two standards with the
    #  same name but the first one has version and another one does not have.
    NULL = 'null'  # null version means no version

    def __new__(cls, name: str, version: str | None = None):
        if isinstance(name, Standard):
            return name
        if isinstance(name, tuple) and len(name) == 2:
            return tuple.__new__(Standard, name)
        if isinstance(name, tuple) and len(name) == 1:
            return tuple.__new__(Standard, (name[0], None))
        if not isinstance(name, str):
            raise TypeError('only string or tuple are allowed')
        version = None if version == cls.NULL else version
        return tuple.__new__(Standard, (name, version))

    def __init__(self, *args, **kwargs):
        super().__init__()
        self._points = None

    def get_points(self) -> frozenset:
        if self._points is None:
            return frozenset()
        return self._points

    def set_points(self, val: Iterable[str]) -> None:
        self._points = frozenset(val)

    @property
    def name(self) -> str:
        return self[0]

    @property
    def version(self) -> str | None:
        return self[1]

    @property
    def version_str(self) -> str:
        return self.version or self.NULL

    def __repr__(self) -> str:
        return f'{self.name} {self.version}'

    @property
    def full_name(self) -> str:
        if not self.version:
            return self.name
        return f'{self.name} {self.version}'

    @classmethod
    def deserialize(
        cls,
        standards: dict[str, list] | dict[str, dict],
        include_points: bool = True,
    ) -> Generator['Standard', None, None]:
        """Currently rules' standards look like it's showed below
        {
            'Standard_1': [
                'v1 (point1,sub-point1,point2)',
                'v2'
            ],
            'Standard_2': [
                '(sub-point2)'
            ],
        }
        The method will transform it to this:
        {('Standard_1', 'v1'), ('Standard_1', 'v2'), ('Standard_2', 'null')}
        Each standard will contain a set of its points inside
        """
        for standard, versions in standards.items():
            for version in versions:
                v = None
                p = []

                version_points = version.rsplit(maxsplit=1)
                if len(version_points) == 2:  # version and points
                    v = version_points[0]
                    if include_points:
                        p = version_points[1].strip('()').split(',')
                elif len(version_points) == 1 and version_points[0].startswith(
                    '('
                ):  # only points
                    if include_points:
                        p = version_points[0].strip('()').split(',')
                elif len(version_points) == 1:  # only version
                    v = version_points[0]
                else:
                    raise ValueError(
                        f'Wrong rule standard format: '
                        f'{standard}, {version}'
                    )
                item = cls(standard, v)
                if include_points:
                    item.set_points(filter(None, p))
                yield item


def keep_highest(*args: set):
    """
    >>> a, b, c = {0,1,2,3}, {2,3,4}, {1, 2, 3, 4, 5}
    >>> keep_highest(a, b, c)
    >>> a, b, c
    ({0}, {}, {1,2,3,4,5})
    """
    _last = len(args) - 1
    for i in range(_last):
        cur: set = args[i]
        for j in range(i + 1, _last + 1):
            ne: set = args[j]
            to_remove = []
            for item in cur:
                if item in ne:
                    to_remove.append(item)
            cur.difference_update(to_remove)


severity_chain = {v: i for i, v in enumerate(Severity.iter())}


class SeverityCmp:
    def __call__(self, one: str, two: str) -> int:
        oi = severity_chain.get(one)
        ti = severity_chain.get(two)
        if not isinstance(oi, int):
            return 1
        if not isinstance(ti, int):
            return -1
        return oi - ti


severity_cmp = SeverityCmp()


def adjust_resource_type(rt: str, /) -> str:
    """
    Removes cloud prefix from resource type
    """
    return rt.split('.', maxsplit=1)[-1]


def service_from_resource_type(rt: str, /) -> str:
    """
    Best try to convert CC resource name to human-readable service name
    """
    return adjust_resource_type(rt).replace('-', ' ').replace('_', ' ').title()
