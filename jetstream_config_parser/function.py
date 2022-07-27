from typing import Any, Callable, Dict, Mapping

import attr


@attr.s(auto_attribs=True)
class Function:
    slug: str
    definition: Callable


@attr.s(auto_attribs=True)
class FunctionsSpec:
    functions: Dict[str, Function]

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "FunctionsSpec":
        return cls(
            {
                slug: Function(
                    slug=slug, definition=lambda select_expr: fun["definition"].format(select_expr)
                )
                for slug, fun in d["functions"].items()
            }
        )
