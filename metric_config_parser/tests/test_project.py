from textwrap import dedent

import pytest
import toml

from metric_config_parser.monitoring import MonitoringSpec


class TestProjectSpec:
    def test_group_by_fail(self, config_collection):
        config_str = dedent(
            """
            [project]
            name = "foo"
            xaxis = "build_id"
            metrics = []

            [project.population]
            data_source = "foo"
            group_by_dimension = "os"

            [data_sources]
            [data_sources.foo]
            from_expression = "test"

            [dimensions]
            [dimensions.os]
            select_expression = "os"
            data_source = "foo"
            """
        )

        spec = MonitoringSpec.from_dict(toml.loads(config_str))

        with pytest.raises(ValueError):
            spec.resolve(experiment=None, configs=config_collection)

    def test_bad_project_dates(self):
        config_str = dedent(
            """
            [project]
            start_date = "My birthday"
            """
        )

        with pytest.raises(ValueError):
            MonitoringSpec.from_dict(toml.loads(config_str))

    def test_bad_project_xaxis(self):
        config_str = dedent(
            """
            [project]
            xaxis = "Nothing"
            """
        )

        with pytest.raises(ValueError):
            MonitoringSpec.from_dict(toml.loads(config_str))
