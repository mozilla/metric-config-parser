from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import attr

from metric_config_parser.errors import DefinitionNotFound

if TYPE_CHECKING:
    from metric_config_parser.config import ConfigCollection
    from .experiment import ExperimentConfiguration
    from .definition import DefinitionSpecSub
    from .project import ProjectConfiguration

from .util import converter


@attr.s(frozen=True, slots=True)
class DataSource:
    """Represents a table or view, from which Metrics may be defined.
    Args:
        name (str): Name for the Data Source. Used in sanity metric
            column names.
        from_expression (str): FROM expression - often just a fully-qualified
            table name. Sometimes a subquery. May contain the string
            ``{dataset}`` which will be replaced with an app-specific
            dataset for Glean apps. If the expression is templated
            on dataset, default_dataset is mandatory.
        experiments_column_type (str or None): Info about the schema
            of the table or view:
            * 'simple': There is an ``experiments`` column, which is an
              (experiment_slug:str -> branch_name:str) map.
            * 'native': There is an ``experiments`` column, which is an
              (experiment_slug:str -> struct) map, where the struct
              contains a ``branch`` field, which is the branch as a
              string.
            * None: There is no ``experiments`` column, so skip the
              sanity checks that rely on it. We'll also be unable to
              filter out pre-enrollment data from day 0 in the
              experiment.
        client_id_column (str, optional): Name of the column that
            contains the ``client_id`` (join key). Defaults to
            'client_id'.
        submission_date_column (str, optional): Name of the column
            that contains the submission date (as a date, not
            timestamp). Defaults to 'submission_date'.
        default_dataset (str, optional): The value to use for
            `{dataset}` in from_expr if a value is not provided
            at runtime. Mandatory if from_expr contains a
            `{dataset}` parameter.
    """

    name = attr.ib(validator=attr.validators.instance_of(str))
    from_expression = attr.ib(validator=attr.validators.instance_of(str))
    experiments_column_type = attr.ib(default="simple", type=str)
    client_id_column = attr.ib(default="client_id", type=str)
    submission_date_column = attr.ib(default="submission_date", type=str)
    default_dataset = attr.ib(default=None, type=Optional[str])
    build_id_column = attr.ib(default="SAFE.SUBSTR(application.build_id, 0, 8)", type=str)
    friendly_name = attr.ib(default=None, type=str)
    description = attr.ib(default=None, type=str)

    EXPERIMENT_COLUMN_TYPES = (None, "simple", "native", "glean")

    @experiments_column_type.validator
    def _check_experiments_column_type(self, attribute, value):
        if value not in self.EXPERIMENT_COLUMN_TYPES:
            raise ValueError(
                f"experiments_column_type {repr(value)} must be one of: "
                f"{repr(self.EXPERIMENT_COLUMN_TYPES)}"
            )

    @default_dataset.validator
    def _check_default_dataset_provided_if_needed(self, attribute, value):
        self.from_expr_for(None)

    def from_expr_for(self, dataset: Optional[str]) -> str:
        """Expands the ``from_expression`` template for the given dataset.
        If ``from_expression`` is not a template, returns ``from_expression``.
        Args:
            dataset (str or None): Dataset name to substitute
                into the from expression.
        """
        effective_dataset = dataset or self.default_dataset
        if effective_dataset is None:
            try:
                return self.from_expression.format()
            except Exception as e:
                raise ValueError(
                    f"{self.name}: from_expression contains a dataset template but no value was provided."  # noqa:E501
                ) from e
        return self.from_expression.format(dataset=effective_dataset)


@attr.s(auto_attribs=True)
class DataSourceReference:
    name: str

    def resolve(
        self,
        spec: "DefinitionSpecSub",
        conf: Union["ExperimentConfiguration", "ProjectConfiguration"],
        configs: "ConfigCollection",
    ) -> DataSource:
        if self.name in spec.data_sources.definitions:
            return spec.data_sources.definitions[self.name].resolve(spec)

        data_source_definition = configs.get_data_source_definition(self.name, conf.app_name)
        if data_source_definition is None:
            raise DefinitionNotFound(f"No default definition for data source '{self.name}' found")
        return data_source_definition.resolve(spec)


converter.register_structure_hook(
    DataSourceReference, lambda obj, _type: DataSourceReference(name=obj)
)


@attr.s(auto_attribs=True)
class DataSourceDefinition:
    """Describes the interface for defining a data source in configuration."""

    name: str  # implicit in configuration
    from_expression: Optional[str] = None
    experiments_column_type: Optional[str] = None
    client_id_column: Optional[str] = None
    submission_date_column: Optional[str] = None
    default_dataset: Optional[str] = None
    build_id_column: Optional[str] = None
    friendly_name: Optional[str] = None
    description: Optional[str] = None

    def resolve(self, spec: "DefinitionSpecSub") -> DataSource:
        params: Dict[str, Any] = {"name": self.name, "from_expression": self.from_expression}
        # Allow mozanalysis to infer defaults for these values:
        for k in (
            "experiments_column_type",
            "client_id_column",
            "submission_date_column",
            "default_dataset",
            "build_id_column",
            "friendly_name",
            "description",
        ):
            v = getattr(self, k)
            if v:
                params[k] = v
        # experiments_column_type is a little special, though!
        # `None` is a valid value, which means there isn't any `experiments` column in the
        # data source, so mozanalysis shouldn't try to use it.
        # But mozanalysis has a different default value for that param ("simple"), and
        # TOML can't represent an explicit null. So we'll look for the string "none" and
        # transform it to the value None.
        if (self.experiments_column_type or "").lower() == "none":
            params["experiments_column_type"] = None
        return DataSource(**params)

    def merge(self, other: "DataSourceDefinition"):
        """Merge with another data source definition."""
        for key in attr.fields_dict(type(self)):
            setattr(self, key, getattr(other, key) or getattr(self, key))


@attr.s(auto_attribs=True)
class DataSourcesSpec:
    """Holds data source definitions.

    This doesn't have a resolve() method to produce a concrete DataSourcesConfiguration
    because it's just a container for the definitions, and we don't need it after the spec phase."""

    definitions: Dict[str, DataSourceDefinition] = attr.Factory(dict)

    @classmethod
    def from_dict(cls, d: dict) -> "DataSourcesSpec":
        definitions = {
            k: converter.structure(
                {"name": k, **dict((kk.lower(), vv) for kk, vv in v.items())}, DataSourceDefinition
            )
            for k, v in d.items()
        }
        return cls(definitions)

    def merge(self, other: "DataSourcesSpec"):
        """
        Merge another datasource spec into the current one.
        The `other` DataSourcesSpec overwrites existing keys.
        """
        seen = []
        for key, _ in self.definitions.items():
            if key in other.definitions:
                self.definitions[key].merge(other.definitions[key])
            seen.append(key)
        for key, definition in other.definitions.items():
            if key not in seen:
                self.definitions[key] = definition


converter.register_structure_hook(
    DataSourcesSpec, lambda obj, _type: DataSourcesSpec.from_dict(obj)
)
