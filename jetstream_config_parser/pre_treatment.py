from typing import Any, Dict

import attr

from .analysis import AnalysisSpec


@attr.s(auto_attribs=True)
class PreTreatmentReference:
    name: str
    args: Dict[str, Any]

    def resolve(self, spec: "AnalysisSpec") -> "PreTreatmentReference":
        return self
