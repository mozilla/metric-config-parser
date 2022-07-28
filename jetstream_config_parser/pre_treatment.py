from typing import TYPE_CHECKING, Any, Dict

import attr

if TYPE_CHECKING:
    from .analysis import AnalysisSpec


@attr.s(auto_attribs=True)
class PreTreatmentReference:
    name: str
    args: Dict[str, Any]

    def resolve(self, spec: "AnalysisSpec") -> "PreTreatmentReference":
        return self
