import io
import json
from statistics import mean
from typing import Union, Dict, List, Set, Optional

from helpers import adjust_cloud
from helpers.constants import AWS_CLOUD_ATTR, GCP_CLOUD_ATTR, \
    AZURE_CLOUD_ATTR, MULTIREGION
from helpers.log_helper import get_logger
from helpers.reports import Standard
from services.rule_meta_service import LazyLoadedMappingsCollector

COVERAGE_POINTS_KEY, COVERAGE_PERCENTAGES_KEY = 'P', '%'
_LOG = get_logger(__name__)

# region to standard to set of points

Points = Dict[Standard, Set[str]]  # standard to set of points
RegionPoints = Dict[str, Points]

Coverage = Dict[Union[Standard, str], float]
RegionCoverage = Dict[str, Coverage]


class CoverageCalculator:
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
        percents: List[float] = []
        for point, data in coverage.items():
            if point in points:
                percents.append(data.get(COVERAGE_PERCENTAGES_KEY, 0))
            elif data.get(COVERAGE_PERCENTAGES_KEY, 0):
                percents.append(
                    cls._calculate(data.get(COVERAGE_POINTS_KEY, {}), points))
            else:
                percents.append(0)
        return mean(percents)

    def get_coverage(self, points: Points,
                     to_percents: bool) -> Coverage:
        """
        Accepts a dict where keys are Standard instances and values are sets
        of points within this standard. Returns a dict of standard
        instances to calculated coverages
        :param points:
        :param to_percents: indicates to parse coverage results as percents:
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
            if to_percents:
                standard_coverage = round(standard_coverage * 100, 2)
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
                (len(keys) == 1 and keys[0] == COVERAGE_PERCENTAGES_KEY):
            return {}
        for key in keys:
            if key != COVERAGE_PERCENTAGES_KEY:
                return d[key]


class CoverageService:
    def __init__(self, mappings_collector: LazyLoadedMappingsCollector):
        self._mappings_collector = mappings_collector

    @staticmethod
    def _load_if_stream(obj: Union[io.IOBase, Dict]) -> dict:
        if isinstance(obj, io.IOBase):
            _LOG.debug('The given object is stream. Loading to json')
            return json.load(obj)
        return obj

    def derive_points_from_detailed_report(
            self, detailed_report: Union[io.IOBase, Dict],
            regions: Optional[List[str]] = None) -> RegionPoints:
        """
        Derives points from detailed_report format into a dict with
        the following format:
        {'region': {Standard: {'point1', 'point2', 'point3'}}}
        :param detailed_report: Union[io.IOBase, Dict]
        :param regions: Optional[List[str]], denotes regions to derive for,
         given None - assumes to calculate for any region.
        :return: Points
        """
        _LOG.info('Deriving points from detailed_report')
        detailed_report = self._load_if_stream(detailed_report)
        points = {}
        for region, region_policies in detailed_report.items():
            if region != MULTIREGION and regions and region not in regions:
                continue

            points.setdefault(region, {})
            for policy in region_policies:
                if policy.get('resources'):
                    continue
                name = policy.get('policy', {}).get('name')
                policy_standards = Standard.deserialize(
                    self._mappings_collector.standard.get(name) or {}
                )
                for standard in policy_standards:
                    points[region].setdefault(
                        standard, set()
                    ).update(standard.points)
        return points

    def derive_points_from_findings(self, findings: Dict,
                                    regions: Optional[List[str]] = None
                                    ) -> RegionPoints:
        """
        Derives points from findings format into a dict with
        the following format:
        {'region': {Standard: {'point1', 'point2', 'point3'}}}
        :param findings: Dict
        :param regions: Optional[List[str]], denotes regions to derive for,
         given None - assumes to calculate for any region.
        :return: Points
        """
        _LOG.info('Loading points from findings')
        points = {}
        for name, policy_data in findings.items():
            for region, resources in policy_data.get('resources', {}).items():
                if resources:
                    continue
                if regions and region != MULTIREGION and region not in regions:
                    continue
                points.setdefault(region, {})
                policy_standards = Standard.deserialize(
                    self._mappings_collector.standard.get(name) or {}
                )
                for standard in policy_standards:
                    points[region].setdefault(standard, set()).update(
                        standard.points)
        return points

    def standards_coverage(self, cloud: str) -> dict:
        cloud = adjust_cloud(cloud)
        if cloud == AWS_CLOUD_ATTR:
            return self._mappings_collector.aws_standards_coverage
        if cloud == AZURE_CLOUD_ATTR:
            return self._mappings_collector.azure_standards_coverage
        if cloud == GCP_CLOUD_ATTR:
            return self._mappings_collector.google_standards_coverage
        raise AssertionError(f'Not available cloud: {cloud}')

    @staticmethod
    def distribute_multiregion(points: RegionPoints) -> RegionPoints:
        """
        Updates each region's points with multi-region's points.
        The method updates the given object and returns it as well,
        (just to keep +- similar interface)
        :param points:
        :return:
        """
        if len(points) == 1:  # only one region in dict
            return points
        multi_region = points.pop(MULTIREGION, None)
        if not multi_region:
            return points
        for region, region_result in points.items():
            for standard, _points in multi_region.items():
                points[region].setdefault(standard, set()).update(_points)
        return points

    @staticmethod
    def congest_to_multiregion(points: RegionPoints) -> RegionPoints:
        """
        Merges all the points to multi-region
        :param points:
        :return:
        """
        standards: Dict[Standard, Set] = {}
        for region_data in points.values():
            for standard, points in region_data.items():
                standards.setdefault(standard, set()).update(points)
        return {MULTIREGION: standards}

    def calculate_region_coverages(self, points: RegionPoints, cloud: str,
                                   to_percents: bool = True) -> RegionCoverage:
        calculator = CoverageCalculator(self.standards_coverage(cloud))
        result = {}
        for region, region_points in points.items():
            result[region] = {
                st.full_name: cov
                for st, cov in calculator.get_coverage(
                    points=region_points,
                    to_percents=to_percents).items()
            }
        return result
