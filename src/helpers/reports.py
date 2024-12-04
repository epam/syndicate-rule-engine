from helpers.constants import Severity

NONE_VERSION = 'null'


# class Standard(tuple):
#     # todo can be broken during sorting if there are two standards with the
#     #  same name but the first one has version and another one does not have.
#
#     def __new__(cls, name: str, version: str | None = None):
#         if isinstance(name, Standard):
#             return name
#         if isinstance(name, tuple) and len(name) == 2:
#             return tuple.__new__(Standard, name)
#         if isinstance(name, tuple) and len(name) == 1:
#             return tuple.__new__(Standard, (name[0], None))
#         if not isinstance(name, str):
#             raise TypeError('only string or tuple are allowed')
#         version = None if version == 'null' else version
#         return tuple.__new__(Standard, (name, version))
#
#     def __init__(self, *args, **kwargs):
#         super().__init__()
#         self.points = set()
#
#     @property
#     def name(self) -> str:
#         return self[0]
#
#     @property
#     def version(self) -> str | None:
#         return self[1]
#
#     def __repr__(self) -> str:
#         return f'{self.name} {self.version}'
#
#     @property
#     def full_name(self) -> str:
#         if not self.version:
#             return self.name
#         return f'{self.name} {self.version}'
#
#     @classmethod
#     def deserialize(cls, standards: dict[str, list] | dict[str, dict],
#                     include_points: bool = False
#                     ) -> Generator['Standard', None, None]:
#         """Currently rules' standards look like it's showed below
#         {
#             'Standard_1': [
#                 'v1 (point1,sub-point1,point2)',
#                 'v2'
#             ],
#             'Standard_2': [
#                 '(sub-point2)'
#             ],
#         }
#         The method will transform it to this:
#         {('Standard_1', 'v1'), ('Standard_1', 'v2'), ('Standard_2', 'null')}
#         Each standard will contain a set of its points inside
#         """
#         for standard, versions in standards.items():
#             for version in versions:
#                 v = None
#                 p = set()
#
#                 version_points = version.rsplit(maxsplit=1)
#                 if len(version_points) == 2:  # version and points
#                     v = version_points[0]
#                     if include_points:
#                         p = set(version_points[1].strip('()').split(','))
#                 elif len(version_points) == 1 and version_points[0].startswith('('):  # only points
#                     if include_points:
#                         p = set(version_points[0].strip('()').split(','))
#                 elif len(version_points) == 1:  # only version
#                     v = version_points[0]
#                 else:
#                     raise ValueError(f'Wrong rule standard format: '
#                                      f'{standard}, {version}')
#                 item = Standard(standard, v)
#                 if include_points:
#                     item.points.update(p)
#                 yield item

class Standard:
    """
    Basic representation of rule's standard with version
    """
    __slots__ = ('name', 'version', 'points')

    def __init__(self, name: str, version: str = NONE_VERSION,
                 points: set[str] | None = None):
        self.name: str = name
        self.version: str = version
        if points:
            self.points: set[str] = set(filter(None, points))
        else:
            self.points: set[str] = set()

    def __hash__(self):
        return hash((self.name, self.version))

    def __eq__(self, other) -> bool:
        if isinstance(other, Standard):
            return (self.name, self.version) == (other.name, other.version)
        elif isinstance(other, tuple) and len(other) == 2:
            return (self.name, self.version) == (other[0], other[1])
        return False

    def __repr__(self):
        return f'({self.name}, {self.version})'

    @property
    def full_name(self):
        return f'{self.name} {self.version}' \
            if self.version != NONE_VERSION else self.name

    @classmethod
    def deserialize(cls, standards: dict[str, list] | dict[str, dict]
                    ) -> set['Standard']:
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
        result = set()
        for standard, versions in standards.items():
            for version in versions:
                params = dict(name=standard)
                version_points = version.rsplit(maxsplit=1)
                if len(version_points) == 2:  # version and points
                    v, points = version_points
                    params['version'] = v
                    params['points'] = set(points.strip('()').split(','))
                elif len(version_points) == 1 and version_points[0].startswith(
                        '('):  # only points
                    params['points'] = set(version_points[0].strip(
                        '()').split(','))
                elif len(version_points) == 1:  # only version
                    params['version'] = version_points[0]
                else:
                    raise ValueError(f'Wrong rule standard format: '
                                     f'{standard}, {version}')
                result.add(cls(**params))
        return result

    @classmethod
    def deserialize_to_strs(cls, standards: dict[str, list] | dict[str, dict]
                            ) -> set[str]:
        return {item.full_name for item in cls.deserialize(standards)}


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
    return adjust_resource_type(rt).replace('-', ' ').replace('_', ' ').title()


def merge_dictionaries(dict_to_merge: dict, dict_in: dict):
    """
    Merge one dict into another

    :param dict_to_merge: dictionary that we will merge
    :param dict_in: dictionary in which we will merge
    :return:
    """
    for key in dict_to_merge:
        if key in dict_in:
            dict_in[key].update(dict_to_merge[key])
        else:
            dict_in[key] = dict_to_merge[key]
