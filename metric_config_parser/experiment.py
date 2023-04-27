import datetime as dt
import enum
from typing import TYPE_CHECKING, Any, List, Optional

import attr
import jinja2
from jinja2 import StrictUndefined

if TYPE_CHECKING:
    from .config import ConfigCollection
    from .analysis import AnalysisSpec

from .errors import NoStartDateException
from .exposure_signal import ExposureSignal, ExposureSignalDefinition
from .segment import Segment, SegmentReference
from .util import parse_date


class Channel(enum.Enum):
    """Release channel."""

    NIGHTLY = "nightly"
    BETA = "beta"
    RELEASE = "release"

    @classmethod
    def has_value(cls, value: str) -> bool:
        """Check if a specific value is represented by the enum."""
        return value in cls._value2member_map_  # type: ignore


@attr.s(auto_attribs=True, kw_only=True, slots=True, frozen=True)
class Branch:
    slug: str
    ratio: int


@attr.s(auto_attribs=True, kw_only=True, slots=True, frozen=True)
class Experiment:
    """
    Common experiment representation.
    Attributes:
        experimenter_slug: Slug generated by Experimenter for V1 experiments;
            None for V6 experiments
        normandy_slug: V1 experiment normandy_slug; V6 experiment slug
        type: V1 experiment type; always "v6" for V6 experiments
        status: V1 experiment status; "Live" for active V6 experiments,
            "Complete" for V6 experiments with endDate in the past
        branches: V1 experiment variants converted to branches; V6 experiment branches
        start_date: experiment start_date
        end_date: experiment end_date
        proposed_enrollment: experiment proposed_enrollment
        reference_branch: V1 experiment branch slug where is_control is True;
            V6 experiment reference_branch
        enrollment_end_date: experiment enrollment_end_date
        is_enrollment_paused: True if enrollment has ended;
            needed because enrollment_end_date may be computed/proposed
    """

    experimenter_slug: Optional[str]
    normandy_slug: Optional[str]
    type: str
    status: Optional[str]
    branches: List[Branch]
    start_date: Optional[dt.datetime]
    end_date: Optional[dt.datetime]
    proposed_enrollment: Optional[int]
    reference_branch: Optional[str]
    is_high_population: bool
    app_name: str
    is_enrollment_paused: Optional[bool] = None
    app_id: Optional[str] = None
    outcomes: List[str] = attr.Factory(list)
    enrollment_end_date: Optional[dt.datetime] = None
    boolean_pref: Optional[str] = None
    channel: Optional[Channel] = None
    is_rollout: bool = False


@attr.s(auto_attribs=True)
class ExperimentConfiguration:
    """Represents the configuration of an experiment for analysis."""

    experiment_spec: "ExperimentSpec"
    experiment: "Experiment"
    segments: List[Segment]
    exposure_signal: Optional[ExposureSignal] = None

    def __attrs_post_init__(self):
        # Catch any exceptions at instantiation
        self._enrollment_query = self.enrollment_query

    @property
    def enrollment_query(self) -> Optional[str]:
        if self.experiment_spec.enrollment_query is None:
            return None

        cached = getattr(self, "_enrollment_query", None)
        if cached:
            return cached

        class ExperimentProxy:
            @property
            def enrollment_query(proxy):
                raise ValueError()

            def __getattr__(proxy, name):
                return getattr(self, name)

        env = jinja2.Environment(autoescape=False, undefined=StrictUndefined)
        return env.from_string(self.experiment_spec.enrollment_query).render(
            experiment=ExperimentProxy()
        )

    @property
    def proposed_enrollment(self) -> int:
        return self.experiment_spec.enrollment_period or self.experiment.proposed_enrollment or 0

    @property
    def enrollment_end_date(self) -> Optional[dt.datetime]:
        return self.experiment.enrollment_end_date

    @property
    def is_enrollment_paused(self) -> Optional[bool]:
        return self.experiment.is_enrollment_paused

    @property
    def enrollment_period(self) -> int:
        if self.experiment_spec.enrollment_period is not None:
            return self.experiment_spec.enrollment_period
        elif self.enrollment_end_date is not None and self.start_date is not None:
            return (self.enrollment_end_date - self.start_date).days + 1

        return self.proposed_enrollment or 0

    @property
    def reference_branch(self) -> Optional[str]:
        return self.experiment_spec.reference_branch or self.experiment.reference_branch

    @property
    def start_date(self) -> Optional[dt.datetime]:
        return parse_date(self.experiment_spec.start_date) or self.experiment.start_date

    @property
    def end_date(self) -> Optional[dt.datetime]:
        return parse_date(self.experiment_spec.end_date) or self.experiment.end_date

    @property
    def status(self) -> Optional[str]:
        """Assert the experiment is Complete if an end date is provided.

        Functionally, this lets the Overall metrics run on the specified date.
        """
        return "Complete" if self.experiment_spec.end_date else self.experiment.status

    # Helpers for configuration templates
    @property
    def start_date_str(self) -> str:
        if not self.start_date:
            raise NoStartDateException(self.normandy_slug)
        return self.start_date.strftime("%Y-%m-%d")

    @property
    def last_enrollment_date_str(self) -> str:
        if not self.start_date:
            raise NoStartDateException(self.normandy_slug)
        return (self.start_date + dt.timedelta(days=self.enrollment_period)).strftime("%Y-%m-%d")

    @property
    def skip(self) -> bool:
        return self.experiment_spec.skip

    @property
    def is_private(self) -> bool:
        return self.experiment_spec.is_private

    @property
    def app_name(self) -> str:
        return self.experiment.app_name

    @property
    def dataset_id(self) -> Optional[str]:
        return self.experiment_spec.dataset_id

    def has_external_config_overrides(self) -> bool:
        """Check whether the external config overrides experiment configuration."""
        return (
            self.reference_branch != self.experiment.reference_branch
            or self.start_date != self.experiment.start_date
            or self.end_date != self.experiment.end_date
            or self.proposed_enrollment != self.experiment.proposed_enrollment
            or self.enrollment_end_date != self.experiment.enrollment_end_date
        )

    # see https://stackoverflow.com/questions/50888391/pickle-of-object-with-getattr-method-in-
    # python-returns-typeerror-object-no
    def __getstate__(self):
        return vars(self)

    def __setstate__(self, state):
        vars(self).update(state)

    def __getattr__(self, name: str) -> Any:
        if "experiment" not in vars(self):
            raise AttributeError
        return getattr(self.experiment, name)


def _validate_yyyy_mm_dd(instance: Any, attribute: Any, value: Any) -> None:
    parse_date(value)


def _validate_dataset_id(instance: Any, attribute, value):
    if instance.is_private and value is None:
        raise ValueError("dataset_id must be set to a custom dataset for private experiments")


@attr.s(auto_attribs=True, kw_only=True)
class ExperimentSpec:
    """Describes the interface for overriding experiment details."""

    enrollment_query: Optional[str] = None
    enrollment_period: Optional[int] = None
    reference_branch: Optional[str] = None
    start_date: Optional[str] = attr.ib(default=None, validator=_validate_yyyy_mm_dd)
    end_date: Optional[str] = attr.ib(default=None, validator=_validate_yyyy_mm_dd)
    segments: List[SegmentReference] = attr.Factory(list)
    skip: bool = False
    exposure_signal: Optional[ExposureSignalDefinition] = None
    is_private: bool = False
    dataset_id: Optional[str] = attr.ib(default=None, validator=_validate_dataset_id)

    def resolve(
        self, spec: "AnalysisSpec", experiment: "Experiment", configs: "ConfigCollection"
    ) -> ExperimentConfiguration:
        experiment_config = ExperimentConfiguration(self, experiment, [])
        # Segment data sources may need to know the enrollment dates of the experiment,
        # so we'll forward the Experiment we know about so far.
        experiment_config.segments = [
            ref.resolve(spec, experiment_config, configs) for ref in self.segments
        ]

        if self.exposure_signal:
            experiment_config.exposure_signal = self.exposure_signal.resolve(
                spec, conf=experiment_config, configs=configs
            )

        return experiment_config

    def merge(self, other: "ExperimentSpec") -> None:
        for key in attr.fields_dict(type(self)):
            setattr(self, key, getattr(other, key) or getattr(self, key))
