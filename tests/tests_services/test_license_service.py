from services.license_service import LicenseService, PERMITTED_ATTACHMENT, \
    PROHIBITED_ATTACHMENT


def test_is_subject_applicable():
    class License:
        __slots__ = ('customers',)

        def __init__(self, customers: dict):
            self.customers = customers

    lic = License({
        'customer': {
            'tenant_license_key': 'mock',
            'tenants': ['tenant1', 'tenant2'],
            'attachment_model': PERMITTED_ATTACHMENT
        }
    })
    assert LicenseService.is_subject_applicable(lic, 'customer', 'tenant1')
    assert LicenseService.is_subject_applicable(lic, 'customer', 'tenant2')
    assert LicenseService.is_subject_applicable(lic, 'customer')
    assert not LicenseService.is_subject_applicable(lic, 'customer', 'tenant3')
    assert not LicenseService.is_subject_applicable(lic, 'cst', 'tenant3')

    lic = License({
        'customer': {
            'tenant_license_key': 'mock',
            'tenants': [],
            'attachment_model': PERMITTED_ATTACHMENT
        }
    })
    assert LicenseService.is_subject_applicable(lic, 'customer', 'tenant1')
    assert LicenseService.is_subject_applicable(lic, 'customer', 'tenant2')
    assert LicenseService.is_subject_applicable(lic, 'customer')
    assert LicenseService.is_subject_applicable(lic, 'customer', 'tenant3')
    assert not LicenseService.is_subject_applicable(lic, 'cst', 'tenant3')

    lic = License({
        'customer': {
            'tenant_license_key': 'mock',
            'tenants': ['tenant1', 'tenant2'],
            'attachment_model': PROHIBITED_ATTACHMENT
        }
    })
    assert not LicenseService.is_subject_applicable(lic, 'customer', 'tenant1')
    assert not LicenseService.is_subject_applicable(lic, 'customer', 'tenant2')
    assert LicenseService.is_subject_applicable(lic, 'customer')
    assert LicenseService.is_subject_applicable(lic, 'customer', 'tenant3')
    assert not LicenseService.is_subject_applicable(lic, 'cst', 'tenant3')

    lic = License({
        'customer': {
            'tenant_license_key': 'mock',
            'tenants': [],
            'attachment_model': PROHIBITED_ATTACHMENT
        }
    })
    assert not LicenseService.is_subject_applicable(lic, 'customer', 'tenant1')
    assert not LicenseService.is_subject_applicable(lic, 'customer', 'tenant2')
    assert LicenseService.is_subject_applicable(lic, 'customer')
    assert not LicenseService.is_subject_applicable(lic, 'customer', 'tenant3')
    assert not LicenseService.is_subject_applicable(lic, 'cst', 'tenant3')
