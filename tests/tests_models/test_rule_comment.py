from helpers.constants import Cloud
from models.rule import RuleIndex


class TestRuleIndex:
    def test_cloud(self):
        assert RuleIndex('010000000000').cloud == Cloud.AWS
        assert RuleIndex('020000000000').cloud == Cloud.AZURE
        assert RuleIndex('030000000000').cloud == Cloud.GOOGLE

        assert RuleIndex('000100000000').cloud == Cloud.KUBERNETES
        assert RuleIndex('000200000000').cloud == Cloud.KUBERNETES
        assert RuleIndex('000300000000').cloud == Cloud.KUBERNETES

    def test_category(self):
        assert RuleIndex('010000000000').category == 'FinOps'
        assert RuleIndex('010030000000').category == 'Network security'
        assert RuleIndex('010050000000').category == 'High availability'

    def test_service_section(self):
        assert RuleIndex('010050220000').service_section == 'General Policies'
        assert (RuleIndex('010050140000').service_section ==
                'Application Integration')
        assert (RuleIndex('010050180000').service_section ==
                'Microsoft Defender for Cloud')

    def test_source(self):
        assert (RuleIndex('010050221700').source ==
                'CIS Oracle Database 19 Benchmark v1.0.0')
        assert (RuleIndex('010050222400').source ==
                'CIS RedHat OpenShift Container Platform Benchmark v1.4.0')

    def test_has_customization(self):
        assert RuleIndex('010050221710').has_customization
        assert not RuleIndex('010050221700').has_customization

    def test_global(self):
        assert not RuleIndex('010050221710').is_global
        assert RuleIndex('010050221701').is_global
        assert RuleIndex('0100502217').is_global
        assert RuleIndex('').is_global
