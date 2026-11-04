from services.coverage_service import StandardCoverageCalculator as CC
from services.coverage_service import calculate_controls_coverages
from services.metadata import CoverageControl


def test_coverage_calculator():
    assert CC().update({'1.1': 1, '1.2': 0.0}).produce() == 0.5
    assert (
        CC().set_total(10).update('1.1', 1.0).update('1.2', 1.0).produce()
        == 0.2
    )
    assert (
        CC().set_total(11).update('1.1', 1.0).update('1.2', -1).produce()
        == 0.1
    )
    assert (
        CC()
        .set_total(4)
        .update({'1.1': 1, '1.2': 1})
        .update('1.2', 0)
        .update('1.3', 2, 4)
        .produce()
        == 0.375
    )


def test_calculate_controls_coverages_accepts_enriched_controls_including_zero():
    assert calculate_controls_coverages(
        successful={'1.1': 2},
        total={
            '1.1': CoverageControl(total_rules=4, control_title='Access'),
            '1.2': CoverageControl(total_rules=0, control_title='Network'),
        },
    ) == {'1.1': 0.5, '1.2': 0.0}
