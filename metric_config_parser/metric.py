import copy
from collections import defaultdict
from enum import Enum
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import attr
import jinja2

from metric_config_parser.errors import DefinitionNotFound

if TYPE_CHECKING:
    from .analysis import AnalysisSpec
    from .config import ConfigCollection
    from .experiment import ExperimentConfiguration
    from .definition import DefinitionSpecSub
    from .project import ProjectConfiguration

from .data_source import DataSource, DataSourceReference
from .parameter import ParameterDefinition
from .pre_treatment import PreTreatmentReference
from .statistic import Statistic
from .util import converter


class AnalysisBasis(Enum):
    """Determines what the population used for the analysis will be based on."""

    ENROLLMENTS = "enrollments"
    EXPOSURES = "exposures"


class AnalysisPeriod(Enum):
    DAY = "day"
    WEEK = "week"
    DAYS_28 = "days28"
    OVERALL = "overall"

    @property
    def mozanalysis_label(self) -> str:
        d = {"day": "daily", "week": "weekly", "days28": "28_day", "overall": "overall"}
        return d[self.value]

    @property
    def table_suffix(self) -> str:
        d = {"day": "daily", "week": "weekly", "days28": "days28", "overall": "overall"}
        return d[self.value]


@attr.s(auto_attribs=True)
class Summary:
    """Represents a metric with a statistical treatment."""

    metric: "Metric"
    statistic: "Statistic"
    pre_treatments: List[PreTreatmentReference] = attr.Factory(list)


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Metric:
    """
    Metric representation.

    Metrics are supersets of mozanalysis metrics with additional
    metadata required for analysis.
    """

    name: str
    data_source: DataSource
    select_expression: str
    friendly_name: Optional[str] = None
    description: Optional[str] = None
    bigger_is_better: bool = True
    analysis_bases: List[AnalysisBasis] = [AnalysisBasis.ENROLLMENTS, AnalysisBasis.EXPOSURES]
    type: str = "scalar"
    category: Optional[str] = None


@attr.s(auto_attribs=True)
class MetricReference:
    name: str

    def resolve(
        self,
        spec: "AnalysisSpec",
        conf: Union["ExperimentConfiguration", "ProjectConfiguration"],
        configs: "ConfigCollection",
    ) -> List[Summary]:
        if self.name in spec.metrics.definitions:
            return spec.metrics.definitions[self.name].resolve(spec, conf, configs)

        metric_definition = configs.get_metric_definition(self.name, conf.app_name)
        if metric_definition:
            return metric_definition.resolve(spec, conf, configs=configs)

        raise DefinitionNotFound(f"Could not locate metric {self.name}")


# These are bare strings in the configuration file.
converter.register_structure_hook(MetricReference, lambda obj, _type: MetricReference(name=obj))


@attr.s(auto_attribs=True)
class MetricDefinition:
    """Describes the interface for defining a metric in configuration.

    The `select_expression` of the metric may use Jinja2 template syntax to refer to the
    aggregation helper functions defined in `mozanalysis.metrics`, like
        '{{agg_any("payload.processes.scalars.some_boolean_thing")}}'
    """

    name: str  # implicit in configuration
    statistics: Optional[Dict[str, Dict[str, Any]]] = None
    select_expression: Optional[str] = None
    data_source: Optional[DataSourceReference] = None
    friendly_name: Optional[str] = None
    description: Optional[str] = None
    bigger_is_better: bool = True
    analysis_bases: Optional[List[AnalysisBasis]] = None
    type: Optional[str] = None
    category: Optional[str] = None

    @staticmethod
    def generate_select_expression(
        param_definitions: Dict[str, ParameterDefinition],
        select_expr_template: Union[str, jinja2.nodes.Template],
        configs: "ConfigCollection",
    ) -> str:
        """
        Takes in param configuration and converts it to a select statement string
        """

        if "parameters" not in str(select_expr_template):
            return configs.get_env().from_string(select_expr_template).render()

        formatted_params: Dict[str, Any] = defaultdict()

        for param_name, param_definition in param_definitions.items():
            if param_definition.distinct_by_branch and isinstance(param_definition.value, dict):
                formatted_params.update(
                    {
                        param_name: "CASE e.branch "
                        + " ".join(
                            [
                                f'WHEN "{branch}" THEN "{value}"'
                                for branch, value in param_definition.value.items()
                            ]
                        )
                        + " END"
                    }
                )
            else:
                formatted_params.update({param_name: param_definition.value})

        return (
            configs.get_env().from_string(select_expr_template).render(parameters=formatted_params)
        )

    def resolve(
        self,
        spec: "DefinitionSpecSub",
        conf: Union["ExperimentConfiguration", "ProjectConfiguration"],
        configs: "ConfigCollection",
    ) -> List[Summary]:
        metric_summary = None
        metric = None

        if self.select_expression is None or self.data_source is None:
            # checks if a metric from mozanalysis was referenced
            metric_definition = configs.get_metric_definition(self.name, conf.app_name)

            if metric_definition is None:
                raise DefinitionNotFound(
                    f"No default definition found for referenced metric {self.name}"
                )

            metric_definition.analysis_bases = self.analysis_bases or [
                AnalysisBasis.ENROLLMENTS,
                AnalysisBasis.EXPOSURES,
            ]
            metric_definition.statistics = self.statistics
            metric_summary = metric_definition.resolve(spec, conf, configs)
        else:
            select_expression = self.generate_select_expression(
                spec.parameters.definitions,
                select_expr_template=self.select_expression,
                configs=configs,
            )

            metric = Metric(
                name=self.name,
                data_source=self.data_source.resolve(spec, conf, configs),
                select_expression=select_expression,
                friendly_name=dedent(self.friendly_name)
                if self.friendly_name
                else self.friendly_name,
                description=dedent(self.description) if self.description else self.description,
                bigger_is_better=self.bigger_is_better,
                analysis_bases=self.analysis_bases
                or [AnalysisBasis.ENROLLMENTS, AnalysisBasis.EXPOSURES],
                type=self.type or "scalar",
                category=self.category,
            )

        metrics_with_treatments = []

        if metric_summary:
            if self.statistics:
                for statistic_name, params in self.statistics.items():
                    stats_params = copy.deepcopy(params)
                    pre_treatments = []
                    for pt in stats_params.pop("pre_treatments", []):
                        if isinstance(pt, str):
                            ref = PreTreatmentReference(pt, {})
                        else:
                            name = pt.pop("name")
                            ref = PreTreatmentReference(name, pt)
                        pre_treatments.append(ref.resolve(spec))

                    metrics_with_treatments.append(
                        Summary(
                            metric=metric_summary[0].metric,
                            statistic=Statistic(statistic_name, stats_params),
                            pre_treatments=pre_treatments,
                        )
                    )
            else:
                metrics_with_treatments += metric_summary
        elif metric:
            if self.statistics is None:
                raise ValueError(f"No statistical treatment defined for metric '{self.name}'")

            for statistic_name, params in self.statistics.items():
                stats_params = copy.deepcopy(params)
                pre_treatments = []
                for pt in stats_params.pop("pre_treatments", []):
                    if isinstance(pt, str):
                        ref = PreTreatmentReference(pt, {})
                    else:
                        name = pt.pop("name")
                        ref = PreTreatmentReference(name, pt)
                    pre_treatments.append(ref.resolve(spec))

                metrics_with_treatments.append(
                    Summary(
                        metric=metric,
                        statistic=Statistic(statistic_name, stats_params),
                        pre_treatments=pre_treatments,
                    )
                )

        if len(metrics_with_treatments) == 0:
            raise ValueError(f"Metric {self.name} has no statistical treatment defined.")

        return metrics_with_treatments

    def merge(self, other: "MetricDefinition"):
        """Merge with another metric definition."""
        for key in attr.fields_dict(type(self)):
            setattr(self, key, getattr(other, key) or getattr(self, key))


MetricsConfigurationType = Dict[AnalysisPeriod, List[Summary]]


@attr.s(auto_attribs=True)
class MetricsSpec:
    """Describes the interface for the metrics section in configuration."""

    daily: List[MetricReference] = attr.Factory(list)
    weekly: List[MetricReference] = attr.Factory(list)
    days28: List[MetricReference] = attr.Factory(list)
    overall: List[MetricReference] = attr.Factory(list)
    definitions: Dict[str, MetricDefinition] = attr.Factory(dict)

    @classmethod
    def from_dict(cls, d: dict) -> "MetricsSpec":
        params: Dict[str, Any] = {}
        known_keys = {f.name for f in attr.fields(cls)}
        for k in known_keys:
            if k == "days28":
                v = d.get("28_day", [])
            else:
                v = d.get(k, [])
            if not isinstance(v, list):
                raise ValueError(f"metrics.{k} should be a list of metrics")
            params[k] = [MetricReference(m) for m in v]

        params["definitions"] = {
            k: converter.structure(
                {"name": k, **dict((kk.lower(), vv) for kk, vv in v.items())}, MetricDefinition
            )
            for k, v in d.items()
            if k not in known_keys and k != "28_day"
        }

        return cls(**params)

    def resolve(
        self,
        spec: "AnalysisSpec",
        conf: Union["ExperimentConfiguration", "ProjectConfiguration"],
        configs: "ConfigCollection",
    ) -> MetricsConfigurationType:
        result = {}
        for period in AnalysisPeriod:
            summaries = [
                summary
                for ref in getattr(self, period.table_suffix)
                for summary in ref.resolve(spec, conf, configs)
            ]
            unique_summaries = []
            seen_summaries = set()

            # summaries needs to be reversed to make sure merged configs overwrite existing ones
            summaries.reverse()
            for summary in summaries:
                if (summary.metric.name, summary.statistic.name) not in seen_summaries:
                    seen_summaries.add((summary.metric.name, summary.statistic.name))
                    unique_summaries.append(summary)

            result[period] = unique_summaries

        return result

    def merge(self, other: "MetricsSpec"):
        """
        Merges another metrics spec into the current one.

        The `other` MetricsSpec overwrites existing metrics.
        """
        self.daily += other.daily
        self.weekly += other.weekly
        self.days28 += other.days28
        self.overall += other.overall

        seen = []
        for key, _ in self.definitions.items():
            if key in other.definitions:
                self.definitions[key].merge(other.definitions[key])
            seen.append(key)
        for key, definition in other.definitions.items():
            if key not in seen:
                self.definitions[key] = definition


converter.register_structure_hook(MetricsSpec, lambda obj, _type: MetricsSpec.from_dict(obj))
