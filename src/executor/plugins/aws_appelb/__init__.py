import datetime

import jmespath
import tzlocal
from c7n.filters import (
    ValueFilter
)
from c7n.filters.core import OPERATORS
from executor.plugins.aws_elb import CidrEgressPortRangeELBFilter
from c7n.utils import (
    local_session, type_schema)

TZLOCAL_ZONE = tzlocal.get_localzone()


class ElbAcmFilter(ValueFilter):
    """
    This filter allows check certificates expiration day
    In that case we use local_zone function for comparing
    dates in Amazon and locally with one time space.
    """

    schema = type_schema('appelb-acm-filter',
                         key={'type': 'string'},
                         value_type={
                             '$ref': '#/definitions/filters_common/value_types'},
                         op={
                             '$ref': '#/definitions/filters_common/comparison_operators'},
                         value={'$ref': '#/definitions/filters_common/value'})
    permissions = ('elasticloadbalancing:DescribeLoadBalancers',)

    def __call__(self, resource):
        return resource

    def process(self, resources, event=None):
        acm_client = local_session(self.manager.session_factory).client("acm")
        elb_client = local_session(self.manager.session_factory).client(
            'elbv2')
        result = []
        value = self.data.get('value')
        local_time_zone = datetime.datetime.now(TZLOCAL_ZONE)

        for elb_arn in resources:
            for listener in elb_client.describe_listeners(
                LoadBalancerArn=elb_arn['LoadBalancerArn'])['Listeners']:
                listeners = elb_client.describe_listener_certificates(
                    ListenerArn=listener['ListenerArn'])
                if 'Certificates' in listeners:
                    for certificate in listeners['Certificates']:
                        if certificate['CertificateArn'].startswith(
                            'arn:aws:acm'):
                            cert_description = acm_client.describe_certificate(
                                CertificateArn=certificate['CertificateArn'])
                            key = jmespath.search(self.data.get('key'),
                                                  cert_description[
                                                      'Certificate'])
                            op = OPERATORS[self.data.get('op')]
                            if isinstance(value, int):
                                value = local_time_zone + datetime.timedelta(
                                    days=int(value))
                            if op(key, value):
                                result.append(elb_arn)
                                break

        return result


class ElbIamCertFilter(ValueFilter):
    """
    This filter allows check certificates expiration day
    In that case we use local_zone function for comparing
    dates in Amazon and locally with one time space.
    """

    schema = type_schema('appelb-iam-cert-filter',
                         key={'type': 'string'},
                         value_type={
                             '$ref': '#/definitions/filters_common/value_types'},
                         op={
                             '$ref': '#/definitions/filters_common/comparison_operators'},
                         value={'$ref': '#/definitions/filters_common/value'})
    permissions = ('elasticloadbalancing:DescribeLoadBalancers',)

    def __call__(self, resource):
        return resource

    def process(self, resources, event=None):
        iam_client = local_session(self.manager.session_factory).client("iam")
        elb_client = local_session(self.manager.session_factory).client(
            'elbv2')
        result = []
        value = self.data.get('value')
        local_time_zone = datetime.datetime.now(TZLOCAL_ZONE)

        for elb_arn in resources:
            for listener in elb_client.describe_listeners(
                LoadBalancerArn=elb_arn['LoadBalancerArn'])['Listeners']:
                listeners = elb_client.describe_listener_certificates(
                    ListenerArn=listener['ListenerArn'])
                if 'Certificates' in listeners:
                    for certificate in listeners['Certificates']:
                        if certificate['CertificateArn'].startswith(
                            'arn:aws:iam'):
                            certificate_name = \
                            certificate['CertificateArn'].split('/')[-1]
                            cert_description = iam_client.get_server_certificate(
                                ServerCertificateName=certificate_name)
                            key = jmespath.search(self.data.get('key'),
                                                  cert_description[
                                                      'ServerCertificate'])
                            op = OPERATORS[self.data.get('op')]
                            if isinstance(value, int):
                                value = local_time_zone + datetime.timedelta(
                                    days=int(value))
                            if op(key, value):
                                result.append(elb_arn)
                                break

        return result


class CidrIpSecurityGroupAppELBFilter(CidrEgressPortRangeELBFilter):
    schema = type_schema('cidrip-security-group-appelb-filter',
                         **{"required": ['required-ports', 'egress', 'cidr'],
                            "required-ports": {
                                '$ref': '#/definitions/filters_common/value'},
                            "egress": {
                                '$ref': '#/definitions/filters_common/value'},
                            "cidr": {
                                '$ref': '#/definitions/filters_common/value'}})


def register() -> None:
    from c7n.resources.appelb import AppELB

    AppELB.filter_registry.register('appelb-acm-filter', ElbAcmFilter)
    AppELB.filter_registry.register('appelb-iam-cert-filter', ElbIamCertFilter)
    AppELB.filter_registry.register('cidrip-security-group-appelb-filter',
                                    CidrIpSecurityGroupAppELBFilter)
