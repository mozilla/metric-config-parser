import datetime as dt
from copy import deepcopy
from pathlib import Path
from typing import List, Optional, Union

import attr
import jinja2
import toml
from git import Repo
from jinja2 import StrictUndefined
from pytz import UTC

from metric_config_parser.data_source import DataSourceDefinition
from metric_config_parser.definition import DefinitionSpec, DefinitionSpecSub
from metric_config_parser.function import FunctionsSpec
from metric_config_parser.monitoring import MonitoringSpec
from metric_config_parser.segment import SegmentDataSourceDefinition, SegmentDefinition

from .analysis import AnalysisSpec
from .errors import UnexpectedKeyConfigurationException
from .experiment import Channel, Experiment
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
    spec: DefinitionSpecSub
    last_modified: dt.datetime
    is_private: bool = False

    def validate(self, configs: "ConfigCollection", experiment: Experiment) -> None:
        if isinstance(self.spec, AnalysisSpec):
            analysis_spec = AnalysisSpec.default_for_experiment(experiment, configs)
            analysis_spec.merge(self.spec)
            resolved = analysis_spec.resolve(experiment, configs)
            # private configs need to override the default dataset
            if self.is_private and resolved.experiment.dataset_id is None:
                raise ValueError("dataset_id needs to be explicitly set for private experiments")
        elif isinstance(self.spec, MonitoringSpec):
            monitoring_spec = MonitoringSpec.default_for_platform_or_type(
                (experiment.app_name if experiment else self.spec.project.platform)
                or "firefox_desktop",
                configs,
            )
            if experiment and experiment.is_rollout:
                rollout_spec = MonitoringSpec.default_for_platform_or_type("rollout", configs)
                monitoring_spec.merge(rollout_spec)
            monitoring_spec.merge(self.spec)
            monitoring_spec.resolve(experiment, configs)


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
        "project",
        "population",
        "metrics",
        "experiment",
        "segments",
        "data_sources",
        "friendly_name",
        "description",
        "parameters",
        "alerts",
        "dimensions",
        "functions",
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
            channel=Channel.NIGHTLY,
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
    is_private: bool = False

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
            channel=Channel.NIGHTLY,
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

    platform: str = "firefox_desktop"

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
            channel=Channel.NIGHTLY,
        )

        if not isinstance(self.spec, DefinitionSpec):
            # this should not happen
            raise ValueError("Incorrect result type when parsing definition config")

        analysis_spec = AnalysisSpec.from_definition_spec(self.spec)
        analysis_spec.resolve(dummy_experiment, configs)


def entity_from_path(
    path: Path, is_private: bool = False
) -> Union[Config, Outcome, DefaultConfig, DefinitionConfig, FunctionsSpec]:
    is_outcome = path.parent.parent.name == OUTCOMES_DIR
    is_default_config = path.parent.name == DEFAULTS_DIR
    is_definition_config = path.parent.name == DEFINITIONS_DIR
    slug = path.stem

    validate_config_settings(path)

    config_dict = toml.loads(path.read_text())

    if is_outcome:
        platform = path.parent.name
        spec = OutcomeSpec.from_dict(config_dict)
        return Outcome(
            slug=slug, spec=spec, platform=platform, commit_hash=None, is_private=is_private
        )
    elif is_default_config:
        if "project" in config_dict:
            # config is from opmon
            return DefaultConfig(
                slug=slug,
                spec=MonitoringSpec.from_dict(config_dict),
                last_modified=dt.datetime.fromtimestamp(path.stat().st_mtime, UTC),
                is_private=is_private,
            )
        else:
            return DefaultConfig(
                slug=slug,
                spec=AnalysisSpec.from_dict(config_dict),
                last_modified=dt.datetime.fromtimestamp(path.stat().st_mtime, UTC),
                is_private=is_private,
            )
    elif is_definition_config:
        if path.name == FUNCTIONS_FILE:
            return FunctionsSpec.from_dict(config_dict)
        else:
            return DefinitionConfig(
                slug=slug,
                spec=DefinitionSpec.from_dict(config_dict),
                last_modified=dt.datetime.fromtimestamp(path.stat().st_mtime, UTC),
                platform=slug,
                is_private=is_private,
            )

    if "project" in config_dict:
        # config is from opmon
        return Config(
            slug=slug,
            spec=MonitoringSpec.from_dict(config_dict),
            last_modified=dt.datetime.fromtimestamp(path.stat().st_mtime, UTC),
            is_private=is_private,
        )
    else:
        return Config(
            slug=slug,
            spec=AnalysisSpec.from_dict(config_dict),
            last_modified=dt.datetime.fromtimestamp(path.stat().st_mtime, UTC),
            is_private=is_private,
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

    repo_url = "https://github.com/mozilla/metric-hub"

    @classmethod
    def from_github_repo(
        cls, repo_url: Optional[str] = None, is_private: bool = False
    ) -> "ConfigCollection":
        """Pull in external config files."""
        # download files to tmp directory
        with TemporaryDirectory() as tmp_dir:
            if repo_url is not None and Path(repo_url).exists() and Path(repo_url).is_dir():
                repo = Repo(repo_url)
                tmp_dir = Path(repo_url)
            else:
                repo = Repo.clone_from(repo_url or cls.repo_url, tmp_dir)

            external_configs = []

            for config_file in tmp_dir.glob("*.toml"):
                last_modified = next(repo.iter_commits("HEAD", paths=config_file)).committed_date
                config_json = toml.load(config_file)

                if "project" in config_json:
                    # opmon spec
                    spec: DefinitionSpecSub = MonitoringSpec.from_dict(config_json)
                else:
                    spec = AnalysisSpec.from_dict(config_json)
                    spec.experiment.is_private = spec.experiment.is_private or is_private

                external_configs.append(
                    Config(
                        config_file.stem,
                        spec,
                        UTC.localize(dt.datetime.utcfromtimestamp(last_modified)),
                        is_private=is_private,
                    )
                )

            outcomes = []
            for outcome_file in tmp_dir.glob(f"**/{OUTCOMES_DIR}/*/*.toml"):
                commit_hash = next(repo.iter_commits("HEAD", paths=outcome_file)).hexsha

                outcomes.append(
                    Outcome(
                        slug=outcome_file.stem,
                        spec=OutcomeSpec.from_dict(toml.load(outcome_file)),
                        platform=outcome_file.parent.name,
                        commit_hash=commit_hash,
                        is_private=is_private,
                    )
                )

            default_configs = []
            for default_config_file in tmp_dir.glob(f"**/{DEFAULTS_DIR}/*.toml"):
                last_modified = next(
                    repo.iter_commits("HEAD", paths=default_config_file)
                ).committed_date

                default_config_json = toml.load(default_config_file)

                if "project" in config_json:
                    # opmon spec
                    spec = MonitoringSpec.from_dict(default_config_json)
                else:
                    spec = AnalysisSpec.from_dict(default_config_json)
                    spec.experiment.is_private = spec.experiment.is_private or is_private

                default_configs.append(
                    DefaultConfig(
                        default_config_file.stem,
                        spec,
                        UTC.localize(dt.datetime.utcfromtimestamp(last_modified)),
                        is_private=is_private,
                    )
                )

            definitions = []
            for definitions_config_file in tmp_dir.glob(f"**/{DEFINITIONS_DIR}/*.toml"):
                last_modified = next(
                    repo.iter_commits("HEAD", paths=definitions_config_file)
                ).committed_date

                definitions.append(
                    DefinitionConfig(
                        definitions_config_file.stem,
                        DefinitionSpec.from_dict(toml.load(definitions_config_file)),
                        UTC.localize(dt.datetime.utcfromtimestamp(last_modified)),
                        platform=definitions_config_file.stem,
                        is_private=is_private,
                    )
                )

            functions_spec = None
            for functions_file in tmp_dir.glob(f"**/{DEFINITIONS_DIR}/{FUNCTIONS_FILE}"):
                functions_spec = FunctionsSpec.from_dict(toml.load(functions_file))

        return cls(external_configs, outcomes, default_configs, definitions, functions_spec)

    @classmethod
    def from_github_repos(
        cls, repo_urls: Optional[List[str]] = None, is_private: bool = False
    ) -> "ConfigCollection":
        """Load configs from the provided repos."""
        if repo_urls is None or len(repo_urls) < 1:
            return ConfigCollection.from_github_repo()

        configs = None

        for repo in repo_urls:
            if configs is None:
                configs = ConfigCollection.from_github_repo(repo, is_private=is_private)
            else:
                collection = ConfigCollection.from_github_repo(repo, is_private=is_private)
                configs.merge(collection)
        return configs or ConfigCollection.from_github_repo()

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
                if isinstance(config.spec, AnalysisSpec):
                    return config.spec

        return None

    def spec_for_project(self, slug: str) -> Optional[MonitoringSpec]:
        """Return the spec for a specific project."""
        for config in self.configs:
            if config.slug == slug:
                if isinstance(config.spec, MonitoringSpec):
                    return config.spec

        return None

    def get_platform_defaults(self, platform: str) -> Optional[DefinitionSpecSub]:
        for default in self.defaults:
            if platform == default.slug:
                return default.spec

        return None

    def get_platform_definitions(self, platform: str) -> Optional[DefinitionSpecSub]:
        for definition in self.definitions:
            if platform == definition.slug:
                return definition.spec

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
                if not isinstance(definition.spec, MonitoringSpec):
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
                if not isinstance(definition.spec, MonitoringSpec):
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

    def merge(self, other: "ConfigCollection"):
        """
        Merge this config collection with another.

        Configs in `other` will take precedence.
        """
        # merge configs
        other_configs = {config.slug: config for config in deepcopy(other.configs)}
        configs = {config.slug: config for config in deepcopy(self.configs)}

        for slug, config in other_configs.items():
            if slug not in configs:
                configs[slug] = config
            else:
                configs[slug].spec.merge(config.spec)

        self.configs = list(configs.values())

        # merge outcomes
        outcomes = deepcopy(other.outcomes)
        slugs = [outcome.slug for outcome in outcomes]
        for outcome in self.outcomes:
            if outcome.slug not in slugs:
                outcomes.append(outcome)

        self.outcomes = outcomes

        # merge definitions
        other_definitions = {
            definition.slug: definition for definition in deepcopy(other.definitions)
        }
        definitions = {definition.slug: definition for definition in deepcopy(self.definitions)}

        for slug, definition in other_definitions.items():
            if slug not in definitions:
                definitions[slug] = definition
            else:
                definitions[slug].spec.merge(definition.spec)

        self.definitions = list(definitions.values())

        # merge defaults
        other_defaults = {default.slug: default for default in deepcopy(other.defaults)}
        defaults = {default.slug: default for default in deepcopy(self.defaults)}

        for slug, default in other_defaults.items():
            if slug not in defaults:
                defaults[slug] = default
            else:
                defaults[slug].spec.merge(default.spec)

        self.defaults = list(defaults.values())

        # merge functions
        functions = deepcopy(other.functions.functions) if other.functions else {}
        slugs = [slug for slug, _ in functions.items()]
        for slug, function in self.functions.functions.items() if self.functions else {}:
            if slug not in slugs:
                functions[slug] = function

        if self.functions is None:
            self.functions = FunctionsSpec(functions=functions)
        else:
            self.functions.functions = functions
