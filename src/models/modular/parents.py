import dataclasses
from typing import Literal, List, Dict

from modular_sdk.commons import DataclassBase
from modular_sdk.models.parent import Parent

Parent = Parent


@dataclasses.dataclass()
class ScopeParentMeta(DataclassBase):
    """
    Common parent meta
    """
    scope: Literal['SPECIFIC_TENANT', 'ALL']
    clouds: List[Literal['AWS', 'AZURE', 'GOOGLE']]


@dataclasses.dataclass()
class ParentMeta(ScopeParentMeta):
    """
    Parent with type CUSTODIAN_LICENSES meta
    """
    rules_to_exclude: List[str]


@dataclasses.dataclass()
class DefectDojoParentMeta(ScopeParentMeta):
    entities_mapping: Dict[str, str] = dataclasses.field(default_factory=dict)
    display_all_fields: bool = False
    upload_files: bool = False
    resource_per_finding: bool = False
