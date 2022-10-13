from functools import partial
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
                    slug=slug,
                    definition=(
                        partial(
                            lambda select_expr, definition: definition.format(
                                select_expr=select_expr
                            ),
                            definition=fun["definition"],
                        )
                    ),
                )
                for slug, fun in d["functions"].items()
            }
        )
