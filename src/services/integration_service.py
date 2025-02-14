from typing import Generator

from modular_sdk.commons.constants import ParentType
from modular_sdk.models.tenant import Tenant
from modular_sdk.services.parent_service import ParentService

from services.ambiguous_job_service import AmbiguousJob
from services.ruleset_service import RulesetName
from services.chronicle_service import (
    ChronicleInstance,
    ChronicleInstanceService,
    ChronicleParentMeta,
)
from services.defect_dojo_service import (
    DefectDojoConfiguration,
    DefectDojoParentMeta,
    DefectDojoService,
)


class IntegrationService:
    def __init__(self, parent_service: ParentService,
                 defect_dojo_service: DefectDojoService,
                 chronicle_instance_service: ChronicleInstanceService):
        self._ps = parent_service
        self._dds = defect_dojo_service
        self._chr = chronicle_instance_service

    def get_chronicle_adapters(self, tenant: Tenant,
                               send_after_job: bool | None = None
                               ) -> Generator[tuple[ChronicleInstance, ChronicleParentMeta], None, None]:
        parent = self._ps.get_linked_parent_by_tenant(
            tenant=tenant, type_=ParentType.GCP_CHRONICLE_INSTANCE
        )
        if not parent:
            return
        configuration = ChronicleParentMeta.from_parent(parent)
        if (isinstance(send_after_job, bool) and
                configuration.send_after_job != send_after_job):
            return
        chronicle = self._chr.get_nullable(parent.application_id)
        if not chronicle or chronicle.is_deleted:
            return
        yield chronicle, configuration

    def get_dojo_adapters(self, tenant: Tenant,
                          send_after_job: bool | None = None
                          ) -> Generator[tuple[DefectDojoConfiguration, DefectDojoParentMeta], None, None]:
        """
        Yields dojo configurations for the given tenant. Currently, can yield
        only one, but maybe in future need multiple
        :param tenant:
        :param send_after_job:
        :return:
        """
        parent = self._ps.get_linked_parent_by_tenant(
            tenant=tenant, type_=ParentType.CUSTODIAN_SIEM_DEFECT_DOJO
        )
        if not parent:
            return
        configuration = DefectDojoParentMeta.from_parent(parent)
        if (isinstance(send_after_job, bool) and
                configuration.send_after_job != send_after_job):
            return

        dojo = self._dds.get_nullable(parent.application_id)
        if not dojo:
            return
        yield dojo, configuration

    @staticmethod
    def job_tags_dojo(job: AmbiguousJob) -> list[str]:
        return list(filter(None, [
            job.owner,
            job.type.value,
            job.scheduled_rule_name,

            *(RulesetName(rs).name for rs in (job.rulesets or ())),
        ]))
