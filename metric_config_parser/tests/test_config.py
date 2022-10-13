import datetime
from pathlib import Path
from textwrap import dedent

import pytest
import pytz
import toml

from metric_config_parser.analysis import AnalysisSpec
from metric_config_parser.config import (
    Config,
    ConfigCollection,
    DefaultConfig,
    DefinitionConfig,
    Outcome,
)
from metric_config_parser.errors import DefinitionNotFound
from metric_config_parser.outcome import OutcomeSpec

TEST_DIR = Path(__file__).parent
DEFAULT_METRICS_CONFIG = TEST_DIR / "data" / "default_metrics.toml"


class TestConfigIntegration:
    config_str = dedent(
        """
        [metrics]
        weekly = ["active_hours"]

        [metrics.active_hours.statistics.bootstrap_mean]
        """
    )
    spec = AnalysisSpec.from_dict(toml.loads(config_str))

    def test_old_config(self):
        config = Config(
            slug="new_table",
            spec=self.spec,
            last_modified=pytz.UTC.localize(
                datetime.datetime.utcnow() - datetime.timedelta(days=1)
            ),
        )

        config_collection = ConfigCollection([config])

        assert config_collection.spec_for_experiment("new_table") is not None
        assert config_collection.spec_for_outcome("test", "foo") is None
        assert config_collection.get_platform_defaults("desktop") is None
        assert config_collection.get_segment_data_source_definition("foo", "test") is None

    def test_definition_config(self):
        config_str = dedent(
            """
            [metrics.retained]
            select_expression = "COALESCE(COUNT(document_id), 0) > 0"
            data_source = "baseline"

            [data_sources.baseline]
            from_expression = "mozdata.search.baseline"
            experiments_column_type = "simple"
            """
        )
        definition = DefinitionConfig(
            slug="firefox_desktop",
            platform="firefox_desktop",
            spec=AnalysisSpec.from_dict(toml.loads(config_str)),
            last_modified=datetime.datetime.now(),
        )

        assert definition

    def test_valid_config_validates(self, experiments):
        config_str = dedent(
            """
            [metrics.active_hours]
            select_expression = "1"
            data_source = "baseline"

            [metrics.active_hours.statistics.bootstrap_mean]

            [data_sources.baseline]
            from_expression = "mozdata.search.baseline"
            experiments_column_type = "simple"
            """
        )
        extern = Config(
            slug="cool_experiment",
            spec=self.spec,
            last_modified=datetime.datetime.now(),
        )
        definition = DefinitionConfig(
            slug="firefox_desktop",
            platform="firefox_desktop",
            spec=AnalysisSpec.from_dict(toml.loads(config_str)),
            last_modified=datetime.datetime.now(),
        )

        config_collection = ConfigCollection(
            configs=[extern], outcomes=[], defaults=[], definitions=[definition]
        )
        extern.validate(config_collection, experiments[0])

    def test_busted_config_fails(self, experiments):
        config = dedent(
            """\
            [metrics]
            weekly = ["bogus_metric"]

            [metrics.bogus_metric]
            select_expression = "SUM(fake_column)"
            data_source = "clients_daily"
            statistics = { bootstrap_mean = {} }
            """
        )
        spec = AnalysisSpec.from_dict(toml.loads(config))
        extern = Config(
            slug="bad_experiment",
            spec=spec,
            last_modified=datetime.datetime.now(),
        )
        config_collection = ConfigCollection([extern])
        with pytest.raises(DefinitionNotFound):
            extern.validate(config_collection, experiments[0])

    def test_valid_outcome_validates(self):
        config = dedent(
            """\
            friendly_name = "Fred"
            description = "Just your average paleolithic dad."

            [metrics.rocks_mined]
            select_expression = "COALESCE(SUM(pings_aggregated_by_this_row), 0)"
            data_source = "clients_daily"
            statistics = { bootstrap_mean = {} }
            friendly_name = "Rocks mined"
            description = "Number of rocks mined at the quarry"

            [data_sources.clients_daily]
            from_expression = "1"
            """
        )
        spec = OutcomeSpec.from_dict(toml.loads(config))
        extern = Outcome(
            slug="good_outcome",
            spec=spec,
            platform="firefox_desktop",
            commit_hash="0000000",
        )
        extern.validate(configs=ConfigCollection())

    def test_busted_outcome_fails(self):
        config = dedent(
            """\
            friendly_name = "Fred"
            description = "Just your average paleolithic dad."

            [metrics.rocks_mined]
            select_expression = "COALESCE(SUM(fake_column_whoop_whoop), 0)"
            data_source = "clients_daily"
            statistics = { bootstrap_mean = {} }
            friendly_name = "Rocks mined"
            description = "Number of rocks mined at the quarry"
            """
        )
        spec = OutcomeSpec.from_dict(toml.loads(config))
        extern = Outcome(
            slug="bogus_outcome",
            spec=spec,
            platform="firefox_desktop",
            commit_hash="0000000",
        )
        with pytest.raises(DefinitionNotFound):
            extern.validate(configs=ConfigCollection())

    def test_valid_default_config_validates(self):
        config_str = dedent(
            """
            [metrics.active_hours]
            select_expression = "1"
            data_source = "baseline"

            [metrics.active_hours.statistics.bootstrap_mean]

            [data_sources.baseline]
            from_expression = "mozdata.search.baseline"
            experiments_column_type = "simple"
            """
        )
        extern = Config(
            slug="cool_experiment",
            spec=self.spec,
            last_modified=datetime.datetime.now(),
        )
        definition = DefinitionConfig(
            slug="firefox_desktop",
            platform="firefox_desktop",
            spec=AnalysisSpec.from_dict(toml.loads(config_str)),
            last_modified=datetime.datetime.now(),
        )
        extern = DefaultConfig(
            slug="firefox_desktop",
            spec=self.spec,
            last_modified=datetime.datetime.now(),
        )
        extern.validate(configs=ConfigCollection(definitions=[definition]))

    def test_busted_default_config_fails(self):
        config = dedent(
            """\
            [metrics]
            weekly = ["bogus_metric"]

            [metrics.bogus_metric]
            select_expression = "SUM(fake_column)"
            data_source = "clients_daily"
            statistics = { bootstrap_mean = {} }
            """
        )
        spec = AnalysisSpec.from_dict(toml.loads(config))
        extern = DefaultConfig(
            slug="firefox_desktop",
            spec=spec,
            last_modified=datetime.datetime.now(),
        )
        with pytest.raises(DefinitionNotFound):
            extern.validate(configs=ConfigCollection())

    def test_merge_config_collection(self):
        config_str = dedent(
            """
            [metrics.active_hours]
            select_expression = "1"
            data_source = "baseline"

            [metrics.active_hours.statistics.bootstrap_mean]

            [data_sources.baseline]
            from_expression = "mozdata.search.baseline"
            experiments_column_type = "simple"
            """
        )
        extern = Config(
            slug="cool_experiment",
            spec=self.spec,
            last_modified=datetime.datetime.now(),
        )
        definition = DefinitionConfig(
            slug="firefox_desktop",
            platform="firefox_desktop",
            spec=AnalysisSpec.from_dict(toml.loads(config_str)),
            last_modified=datetime.datetime.now(),
        )
        config_collection_1 = ConfigCollection(
            configs=[extern], outcomes=[], defaults=[], definitions=[definition]
        )

        config_str = dedent(
            """
            [metrics.active_hours]
            select_expression = "1"
            data_source = "baseline"

            [metrics.active_hours.statistics.bootstrap_mean]

            [data_sources.baseline]
            from_expression = "mozdata.search.baseline"
            experiments_column_type = "simple"
            """
        )
        extern = Config(
            slug="cool_experiment_2",
            spec=self.spec,
            last_modified=datetime.datetime.now(),
        )
        definition = DefinitionConfig(
            slug="firefox_desktop",
            platform="firefox_desktop",
            spec=AnalysisSpec.from_dict(toml.loads(config_str)),
            last_modified=datetime.datetime.now(),
        )
        config_collection_2 = ConfigCollection(
            configs=[extern], outcomes=[], defaults=[], definitions=[definition]
        )

        config_collection_1.merge(config_collection_2)
        assert config_collection_1.configs[0].slug == "cool_experiment"
        assert config_collection_1.configs[1].slug == "cool_experiment_2"

    def test_merge_config_collection_override(self):
        config_str = dedent(
            """
            [metrics.active_hours]
            select_expression = "1"
            data_source = "baseline"

            [metrics.active_hours.statistics.bootstrap_mean]

            [data_sources.baseline]
            from_expression = "mozdata.search.baseline"
            experiments_column_type = "simple"
            """
        )
        extern = Config(
            slug="cool_experiment",
            spec=self.spec,
            last_modified=datetime.datetime.now(),
        )
        config_collection_1 = ConfigCollection(
            configs=[extern], outcomes=[], defaults=[], definitions=[]
        )

        config_str = dedent(
            """
            [metrics.active_hours]
            select_expression = "4"
            data_source = "baseline"

            [metrics.active_hours.statistics.bootstrap_mean]

            [metrics.unenroll]
            select_expression = "3"
            data_source = "baseline"

            [metrics.unenroll.statistics.bootstrap_mean]

            [data_sources.baseline]
            from_expression = "mozdata.search.baseline"
            experiments_column_type = "simple"
            """
        )
        extern = Config(
            slug="cool_experiment",
            spec=self.spec,
            last_modified=datetime.datetime.now(),
        )
        definition = DefinitionConfig(
            slug="firefox_desktop",
            platform="firefox_desktop",
            spec=AnalysisSpec.from_dict(toml.loads(config_str)),
            last_modified=datetime.datetime.now(),
        )
        config_collection_2 = ConfigCollection(
            configs=[extern], outcomes=[], defaults=[], definitions=[definition]
        )

        assert len(config_collection_1.definitions) == 0
        config_collection_1.merge(config_collection_2)

        assert len(config_collection_1.configs) == 1
        assert config_collection_1.configs[0].slug == "cool_experiment"

        assert [
            m
            for slug, m in config_collection_1.definitions[0].spec.metrics.definitions.items()
            if slug == "active_hours"
        ][0].select_expression == "4"
        assert [
            m
            for slug, m in config_collection_1.definitions[0].spec.metrics.definitions.items()
            if slug == "unenroll"
        ][0].select_expression == "3"
