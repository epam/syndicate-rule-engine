from services.coverage_service import StandardCoverageCalculator as CC


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
