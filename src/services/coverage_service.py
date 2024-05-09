from enum import Enum
from statistics import mean

from helpers.constants import GLOBAL_REGION, Cloud
from helpers.log_helper import get_logger
from helpers.reports import Standard
from services.mappings_collector import LazyLoadedMappingsCollector
from services.sharding import ShardsCollection


class CoverageKey(str, Enum):
    POINTS = 'P'
    PERCENTAGES = '%'


_LOG = get_logger(__name__)

Points = dict[Standard, set[str]]
RegionPoints = dict[str, Points]
Coverage = dict[Standard, float]


class CoverageCalculator:  # todo test
    def __init__(self, standards_coverage: dict):
        self._standards_coverage = standards_coverage

    @classmethod
    def _calculate(cls, coverage: dict, points: set) -> float:
        """Calculates coverage percents based on coverage data and
        given points. Coverage data dict consists of so-called "Point" dicts.
        One Point dict looks like this:
        point_dict = {
            "%": 0.56,
            "P": {
                "point1": $point_dict,
                "point2": $point_dict
            }
        }
        Coverage data dict on the highest level starts with a list of points:
        coverage = {
            "point1": $point_dict,
            "point2": $point_dict
        }
        "points" variable is a required set of points or/and sub-points to
        calculate coverage for:
        points = {"point1", "point2"}
        """
        if not coverage:
            return 0
        percents: list[float] = []
        for point, data in coverage.items():
            if point in points:
                percents.append(data.get(CoverageKey.PERCENTAGES, 0))
            elif data.get(CoverageKey.PERCENTAGES, 0):
                percents.append(
                    cls._calculate(data.get(CoverageKey.POINTS, {}), points))
            else:
                percents.append(0)
        return mean(percents)

    def get_coverage(self, points: Points) -> Coverage:
        """
        Accepts a dict where keys are Standard instances and values are sets
        of points within this standard. Returns a dict of standard
        instances to calculated coverages
        :param points:
        [0 - 100]
        :return:
        """
        coverages = {}
        for standard, standard_points in points.items():
            standard_params = self._standards_coverage.get(
                standard.name, {}).get(standard.version)
            if not standard_params:
                _LOG.warning(f'Not found standards coverages for '
                             f'{standard}. Skipping the standard')
                continue
            standard_coverage = self._calculate(
                self._coverages_value(standard_params), standard_points)
            coverages[standard] = standard_coverage
        return coverages

    @staticmethod
    def _coverages_value(d: dict):
        """
        Retrieves the value of the first key in coverages dict
        which is not the '%' key
        """
        keys = list(d)
        if len(keys) == 0 or \
                (len(keys) == 1 and keys[0] == CoverageKey.PERCENTAGES):
            return {}
        for key in keys:
            if key != CoverageKey.PERCENTAGES:
                return d[key]


class CoverageService:
    def __init__(self, mappings_collector: LazyLoadedMappingsCollector):
        self._mappings_collector = mappings_collector

    def points_from_collection(self, collection: ShardsCollection
                               ) -> RegionPoints:
        """
        Derives points from shards collection to the following format:
        {'region': {Standard: {'point1', 'point2', 'point3'}}}
        :param collection:
        :return: Points
        """
        remapped = {}  # policy to its parts
        for part in collection.iter_parts():
            remapped.setdefault(part.policy, []).append(part)
        points = {}
        for policy, parts in remapped.items():
            standards = Standard.deserialize(
                self._mappings_collector.standard.get(policy) or {}
            )
            for part in parts:
                points.setdefault(part.location, {})
                for standard in standards:
                    points[part.location].setdefault(standard, set())
                    if not part.resources:
                        points[part.location][standard].update(standard.points)
        return points

    def standards_coverage(self, cloud: Cloud) -> dict:
        match cloud:
            case Cloud.AWS:
                return self._mappings_collector.aws_standards_coverage
            case Cloud.AZURE:
                return self._mappings_collector.azure_standards_coverage
            case Cloud.GOOGLE:
                return self._mappings_collector.google_standards_coverage
            case _:
                return {}

    @staticmethod
    def distribute_global(points: RegionPoints) -> RegionPoints:
        """
        Updates each region's points with global points.
        The method updates the given object and returns it as well,
        (just to keep +- similar interface)
        :param points:
        :return:
        """
        if len(points) == 1:  # only one region in dict
            return points
        multi_region = points.pop(GLOBAL_REGION, None)
        if not multi_region:
            return points
        for region, region_result in points.items():
            for standard, _points in multi_region.items():
                points[region].setdefault(standard, set()).update(_points)
        return points

    @staticmethod
    def congest_to_global(points: RegionPoints) -> RegionPoints:
        """
        Merges all the points to global ones
        :param points:
        :return:
        """
        standards = {}
        for region_data in points.values():
            for standard, points in region_data.items():
                standards.setdefault(standard, set()).update(points)
        return {GLOBAL_REGION: standards}

    @staticmethod
    def format_coverage(coverage: Coverage) -> dict[str, float]:
        """
        Replaces Standard instances with strings, transforms 0-1 to percents
        """
        return {k.full_name: round(v * 100, 2) for k, v in coverage.items()}

    def calculate_region_coverages(self, points: RegionPoints, cloud: Cloud,
                                   ) -> dict[str, Coverage]:
        calc = CoverageCalculator(self.standards_coverage(cloud))
        return {
            region: calc.get_coverage(region_points)
            for region, region_points in points.items()
        }

    def format_region_coverages(self, coverages: dict[str, Coverage]
                                ) -> dict[str, float]:
        return {k: self.format_coverage(v) for k, v in coverages.items()}

    def coverage_from_collection(self, collection: ShardsCollection,
                                 cloud: Cloud) -> dict:
        points = self.points_from_collection(collection)
        if cloud == Cloud.AWS:
            points = self.distribute_global(points)
        if cloud == Cloud.AZURE:
            points = self.congest_to_global(points)
        coverages = self.calculate_region_coverages(points, cloud)
        return self.format_region_coverages(coverages)
