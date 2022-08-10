import datetime as dt
from pathlib import Path
from typing import List, Optional, Union

import attr
import jinja2
import toml
from git import Repo
from jinja2 import StrictUndefined
from pytz import UTC

from jetstream_config_parser.data_source import DataSourceDefinition
from jetstream_config_parser.function import FunctionsSpec
from jetstream_config_parser.segment import (
    SegmentDataSourceDefinition,
    SegmentDefinition,
)

from .analysis import AnalysisSpec
from .errors import UnexpectedKeyConfigurationException
from .experiment import Experiment
from .metric import MetricDefinition
from .outcome import OutcomeSpec
from .util import TemporaryDirectory

OUTCOMES_DIR = "outcomes"
DEFAULTS_DIR = "defaults"
DEFINITIONS_DIR = "definitions"
FUNCTIONS_FILE = "functions.toml"
JETSTREAM_CONFIG_URL = "https://github.com/mozilla/jetstream-config"


@attr.s(auto_attribs=True)
class Config:
    """Represent an external config file."""

    slug: str
    spec: AnalysisSpec
    last_modified: dt.datetime

    def validate(self, configs: "ConfigCollection", experiment: Experiment) -> None:
        spec = AnalysisSpec.default_for_experiment(experiment, configs)
        spec.merge(self.spec)
        spec.resolve(experiment, configs)


def validate_config_settings(config_file: Path) -> None:
    """
    Implemented to resolve a Github issue:
        - https://github.com/mozilla/jetstream/issues/843

    Loads external config file and runs a number of validation steps on it:
    - Checks that all config keys/settings are lowercase
    - Checks for missing core config keys
    - Checks for unexpected core configuration keys
    - Checks that all segments defined under experiment have configuration in segments section
    - Checks if metric with custom config is defined in metrics.weekly or metrics.overall fields

    Returns None, if issues found with the configuration an Exception is raised
    """

    config = toml.loads(config_file.read_text())

    optional_core_config_keys = (
        "metrics",
        "experiment",
        "segments",
        "data_sources",
        "friendly_name",
        "description",
        "parameters",
    )

    core_config_keys_specified = config.keys()

    # checks for unexpected core configuration keys
    unexpected_config_keys = set(core_config_keys_specified) - set(optional_core_config_keys)
    if unexpected_config_keys:
        err_msg = (
            f"Unexpected config key[s] found: {unexpected_config_keys}. "
            f"config_file: {str(config_file).split('/')[-1]}"
        )
        raise UnexpectedKeyConfigurationException(err_msg)

    return None


@attr.s(auto_attribs=True)
class DefaultConfig(Config):
    """
    Represents an external config files with platform-specific defaults.

    These config files are not associated to a specific experiment, since
    they are applied to all experiments.
    """

    def validate(self, configs: "ConfigCollection", _experiment: Experiment = None) -> None:
        dummy_experiment = Experiment(
            experimenter_slug="dummy-experiment",
            normandy_slug="dummy_experiment",
            type="v6",
            status="Live",
            branches=[],
            end_date=None,
            reference_branch="control",
            is_high_population=False,
            start_date=dt.datetime.now(UTC),
            proposed_enrollment=14,
            app_name=self.slug,
        )
        spec = AnalysisSpec.default_for_experiment(dummy_experiment, configs)
        spec.merge(self.spec)
        spec.resolve(dummy_experiment, configs)


@attr.s(auto_attribs=True)
class Outcome:
    """Represents an external outcome snippet."""

    slug: str
    spec: OutcomeSpec
    platform: str
    commit_hash: Optional[str]

    def validate(self, configs: "ConfigCollection") -> None:
        dummy_experiment = Experiment(
            experimenter_slug="dummy-experiment",
            normandy_slug="dummy_experiment",
            type="v6",
            status="Live",
            branches=[],
            end_date=None,
            reference_branch="control",
            is_high_population=False,
            start_date=dt.datetime.now(UTC),
            proposed_enrollment=14,
            app_name=self.platform,
        )

        spec = AnalysisSpec.default_for_experiment(dummy_experiment, configs)
        spec.merge_outcome(self.spec)
        spec.merge_parameters(self.spec.parameters)
        spec.resolve(dummy_experiment, configs)


@attr.s(auto_attribs=True)
class DefinitionConfig(Config):
    """
    Represents an definition config file with definition that can be referenced in other configs.
    """

    platform: str

    def validate(self, configs: "ConfigCollection", _experiment: Experiment = None) -> None:
        dummy_experiment = Experiment(
            experimenter_slug="dummy-experiment",
            normandy_slug="dummy_experiment",
            type="v6",
            status="Live",
            branches=[],
            end_date=None,
            reference_branch="control",
            is_high_population=False,
            start_date=dt.datetime.now(UTC),
            proposed_enrollment=14,
            app_name=self.platform,
        )

        self.spec.resolve(dummy_experiment, configs)


def entity_from_path(path: Path) -> Union[Config, Outcome, DefaultConfig, DefinitionConfig]:
    is_outcome = path.parent.parent.name == OUTCOMES_DIR
    is_default_config = path.parent.name == DEFAULTS_DIR
    is_definition_config = path.parent.name == DEFINITIONS_DIR
    slug = path.stem

    validate_config_settings(path)

    config_dict = toml.loads(path.read_text())

    if is_outcome:
        platform = path.parent.name
        spec = OutcomeSpec.from_dict(config_dict)
        return Outcome(slug=slug, spec=spec, platform=platform, commit_hash=None)
    elif is_default_config:
        return DefaultConfig(
            slug=slug,
            spec=AnalysisSpec.from_dict(config_dict),
            last_modified=dt.datetime.fromtimestamp(path.stat().st_mtime, UTC),
        )
    elif is_definition_config:
        return DefinitionConfig(
            slug=slug,
            spec=AnalysisSpec.from_dict(config_dict),
            last_modified=dt.datetime.fromtimestamp(path.stat().st_mtime, UTC),
            platform=slug,
        )
    return Config(
        slug=slug,
        spec=AnalysisSpec.from_dict(config_dict),
        last_modified=dt.datetime.fromtimestamp(path.stat().st_mtime, UTC),
    )


@attr.s(auto_attribs=True)
class ConfigCollection:
    """
    Collection of experiment-specific configurations pulled in
    from an external GitHub repository.
    """

    configs: List[Config] = attr.Factory(list)
    outcomes: List[Outcome] = attr.Factory(list)
    defaults: List[DefaultConfig] = attr.Factory(list)
    definitions: List[DefinitionConfig] = attr.Factory(list)
    functions: Optional[FunctionsSpec] = None

    repo_url = "https://github.com/mozilla/jetstream-config"

    @classmethod
    def from_github_repo(cls) -> "ConfigCollection":
        """Pull in external config files."""
        # download files to tmp directory
        with TemporaryDirectory() as tmp_dir:
            repo = Repo.clone_from(cls.repo_url, tmp_dir)

            external_configs = []

            for config_file in tmp_dir.glob("*.toml"):
                last_modified = next(repo.iter_commits("main", paths=config_file)).committed_date

                external_configs.append(
                    Config(
                        config_file.stem,
                        AnalysisSpec.from_dict(toml.load(config_file)),
                        UTC.localize(dt.datetime.utcfromtimestamp(last_modified)),
                    )
                )

            outcomes = []
            for outcome_file in tmp_dir.glob(f"**/{OUTCOMES_DIR}/*/*.toml"):
                commit_hash = next(repo.iter_commits("main", paths=outcome_file)).hexsha

                outcomes.append(
                    Outcome(
                        slug=outcome_file.stem,
                        spec=OutcomeSpec.from_dict(toml.load(outcome_file)),
                        platform=outcome_file.parent.name,
                        commit_hash=commit_hash,
                    )
                )

            default_configs = []
            for default_config_file in tmp_dir.glob(f"**/{DEFAULTS_DIR}/*.toml"):
                last_modified = next(
                    repo.iter_commits("main", paths=default_config_file)
                ).committed_date

                default_configs.append(
                    DefaultConfig(
                        default_config_file.stem,
                        AnalysisSpec.from_dict(toml.load(default_config_file)),
                        UTC.localize(dt.datetime.utcfromtimestamp(last_modified)),
                    )
                )

            definitions = []
            for definitions_config_file in tmp_dir.glob(f"**/{DEFINITIONS_DIR}/*.toml"):
                last_modified = next(
                    repo.iter_commits("main", paths=definitions_config_file)
                ).committed_date

                definitions.append(
                    DefinitionConfig(
                        definitions_config_file.stem,
                        AnalysisSpec.from_dict(toml.load(definitions_config_file)),
                        UTC.localize(dt.datetime.utcfromtimestamp(last_modified)),
                        platform=definitions_config_file.stem,
                    )
                )

            functions_spec = None
            for functions_file in tmp_dir.glob(f"**/{DEFINITIONS_DIR}/{FUNCTIONS_FILE}"):
                functions_spec = FunctionsSpec.from_dict(toml.load(functions_file))

        return cls(external_configs, outcomes, default_configs, definitions, functions_spec)

    def spec_for_outcome(self, slug: str, platform: str) -> Optional[OutcomeSpec]:
        """Return the spec for a specific outcome"""
        for outcome in self.outcomes:
            if outcome.slug == slug and outcome.platform == platform:
                return outcome.spec

        return None

    def spec_for_experiment(self, slug: str) -> Optional[AnalysisSpec]:
        """Return the spec for a specific experiment."""
        for config in self.configs:
            if config.slug == slug:
                return config.spec

        return None

    def get_platform_defaults(self, platform: str) -> Optional[AnalysisSpec]:
        for default in self.defaults:
            if platform == default.slug:
                return default.spec

        return None

    def get_metric_definition(self, slug: str, app_name: str) -> Optional[MetricDefinition]:
        for definition in self.definitions:
            if app_name == definition.platform:
                for metric_slug, metric in definition.spec.metrics.definitions.items():
                    if metric_slug == slug:
                        return metric

        return None

    def get_data_source_definition(
        self, slug: str, app_name: str
    ) -> Optional[DataSourceDefinition]:
        for definition in self.definitions:
            if app_name == definition.platform:
                for (
                    data_source_slug,
                    data_source,
                ) in definition.spec.data_sources.definitions.items():
                    if data_source_slug == slug:
                        return data_source
        return None

    def get_segment_data_source_definition(
        self, slug: str, app_name: str
    ) -> Optional[SegmentDataSourceDefinition]:
        for definition in self.definitions:
            if app_name == definition.platform:
                for (
                    segment_source_slug,
                    segment_source,
                ) in definition.spec.segments.data_sources.items():
                    if segment_source_slug == slug:
                        return segment_source

        return None

    def get_segment_definition(self, slug: str, app_name: str) -> Optional[SegmentDefinition]:
        for definition in self.definitions:
            if app_name == definition.platform:
                for segment_slug, segment in definition.spec.segments.definitions.items():
                    if segment_slug == slug:
                        return segment

        return None

    def get_env(self) -> jinja2.Environment:
        """
        Create a Jinja2 environment that understands the SQL agg_* helpers in mozanalysis.metrics.

        Just a wrapper to avoid leaking temporary variables to the module scope."""
        env = jinja2.Environment(autoescape=False, undefined=StrictUndefined)
        if self.functions is not None:
            for slug, function in self.functions.functions.items():
                env.globals[slug] = function.definition

        return env
