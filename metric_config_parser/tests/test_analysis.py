from pathlib import Path
from textwrap import dedent

import pytest
import toml
from cattrs.errors import ClassValidationError

from metric_config_parser.analysis import AnalysisConfiguration, AnalysisSpec
from metric_config_parser.data_source import DataSource
from metric_config_parser.errors import InvalidConfigurationException
from metric_config_parser.exposure_signal import AnalysisWindow, ExposureSignal
from metric_config_parser.metric import AnalysisBasis, AnalysisPeriod
from metric_config_parser.parameter import ParameterDefinition, ParameterSpec

TEST_DIR = Path(__file__).parent
DEFAULT_METRICS_CONFIG = TEST_DIR / "data" / "default_metrics.toml"


class TestAnalysisSpec:
    def test_trivial_configuration(self, experiments, config_collection):
        spec = AnalysisSpec.from_dict({})
        assert isinstance(spec, AnalysisSpec)
        cfg = spec.resolve(experiments[0], config_collection)
        assert isinstance(cfg, AnalysisConfiguration)
        assert cfg.experiment.segments == []

    def test_scalar_metrics_throws_exception(self):
        config_str = dedent(
            """
            [metrics]
            weekly = "my_cool_metric"
            """
        )
        with pytest.raises(ClassValidationError):
            AnalysisSpec.from_dict(toml.loads(config_str))

    def test_template_expansion(self, experiments, config_collection):
        config_str = dedent(
            """
            [metrics]
            weekly = ["my_cool_metric"]
            [metrics.my_cool_metric]
            data_source = "main"
            select_expression = "{{agg_histogram_mean('payload.content.my_cool_histogram')}}"

            [metrics.my_cool_metric.statistics.bootstrap_mean]
            """
        )
        spec = AnalysisSpec.from_dict(toml.loads(config_str))
        cfg = spec.resolve(experiments[0], config_collection)
        metric = [m for m in cfg.metrics[AnalysisPeriod.WEEK] if m.metric.name == "my_cool_metric"][
            0
        ].metric
        assert "agg_histogram_mean" not in metric.select_expression
        assert "hist.extract" in metric.select_expression

    def test_recognizes_metrics(self, experiments, config_collection):
        config_str = dedent(
            """
            [metrics]
            weekly = ["view_about_logins"]
            """
        )
        spec = AnalysisSpec.from_dict(toml.loads(config_str))
        cfg = spec.resolve(experiments[0], config_collection)
        assert (
            len(
                [
                    m
                    for m in cfg.metrics[AnalysisPeriod.WEEK]
                    if m.metric.name == "view_about_logins"
                ]
            )
            == 1
        )

    def test_duplicate_metrics_are_okay(self, experiments, config_collection):
        config_str = dedent(
            """
            [metrics]
            weekly = ["unenroll", "unenroll", "active_hours"]

            [metrics.unenroll.statistics.binomial]
            [metrics.active_hours.statistics.bootstrap_mean]
            """
        )
        spec = AnalysisSpec.from_dict(toml.loads(config_str))
        cfg = spec.resolve(experiments[0], config_collection)
        assert (
            len([m for m in cfg.metrics[AnalysisPeriod.WEEK] if m.metric.name == "unenroll"]) == 1
        )

    def test_data_source_definition(self, experiments, config_collection):
        config_str = dedent(
            """
            [metrics]
            weekly = ["spam", "taunts"]
            [metrics.spam]
            data_source = "eggs"
            select_expression = "1"
            [metrics.spam.statistics.bootstrap_mean]

            [metrics.taunts]
            data_source = "silly_knight"
            select_expression = "1"
            [metrics.taunts.statistics.bootstrap_mean]

            [data_sources.eggs]
            from_expression = "england.camelot"
            client_id_column = "client_info.client_id"

            [data_sources.silly_knight]
            from_expression = "france"
            experiments_column_type = "none"

            [metrics.forgotten_metric]
            data_souRce = "silly_knight"
            select_expression = "1"
            [metrics.forgotten_metric.statistics.bootstrap_mean]
            """
        )
        spec = AnalysisSpec.from_dict(toml.loads(config_str))
        cfg = spec.resolve(experiments[0], config_collection)
        spam = [m for m in cfg.metrics[AnalysisPeriod.WEEK] if m.metric.name == "spam"][0].metric
        taunts = [m for m in cfg.metrics[AnalysisPeriod.WEEK] if m.metric.name == "taunts"][
            0
        ].metric
        assert spam.data_source.name == "eggs"
        assert "camelot" in spam.data_source.from_expression
        assert "client_info" in spam.data_source.client_id_column
        assert spam.data_source.experiments_column_type == "simple"
        assert taunts.data_source.experiments_column_type is None

    def test_definitions_override_other_metrics(self, experiments, config_collection):
        """Test that config definitions override mozanalysis definitions.
        Users can specify a metric with the same name as a metric built into mozanalysis.
        The user's metric from the config file should win.
        """
        config_str = dedent(
            """
            [metrics]
            weekly = ["active_hours"]
            """
        )
        default_spec = AnalysisSpec.from_dict(toml.load(DEFAULT_METRICS_CONFIG))
        spec = AnalysisSpec.from_dict(toml.loads(config_str))
        spec.merge(default_spec)
        cfg = spec.resolve(experiments[0], config_collection)
        stock = [m for m in cfg.metrics[AnalysisPeriod.WEEK] if m.metric.name == "active_hours"][
            0
        ].metric

        config_str = dedent(
            """
            [metrics]
            weekly = ["active_hours"]
            [metrics.active_hours]
            select_expression = "spam"
            data_source = "main"

            [metrics.active_hours.statistics.bootstrap_mean]
            """
        )
        spec = AnalysisSpec.from_dict(toml.loads(config_str))
        cfg = spec.resolve(experiments[0], config_collection)
        custom = [m for m in cfg.metrics[AnalysisPeriod.WEEK] if m.metric.name == "active_hours"][
            0
        ].metric

        assert stock != custom
        assert custom.select_expression == "spam"
        assert stock.select_expression != custom.select_expression

    def test_overwrite_statistic(self, experiments, config_collection):
        config_str = dedent(
            """
            [metrics]
            weekly = ["spam"]

            [metrics.spam]
            data_source = "main"
            select_expression = "1"

            [metrics.spam.statistics.bootstrap_mean]
            num_samples = 10
            """
        )

        spec = AnalysisSpec.from_dict(toml.loads(config_str))
        cfg = spec.resolve(experiments[0], config_collection)
        bootstrap_mean = [m for m in cfg.metrics[AnalysisPeriod.WEEK] if m.metric.name == "spam"][
            0
        ].statistic
        bootstrap_mean.name = "bootstrap_mean"

        assert bootstrap_mean.params["num_samples"] == 10

    def test_overwrite_default_statistic(self, experiments, config_collection):
        config_str = dedent(
            """
            [metrics]
            weekly = ["active_hours"]

            [metrics.active_hours.statistics.deciles]
            num_samples = 10
            """
        )

        default_spec = AnalysisSpec.from_dict(toml.load(DEFAULT_METRICS_CONFIG))
        spec = AnalysisSpec.from_dict(toml.loads(config_str))
        default_spec.merge(spec)
        cfg = default_spec.resolve(experiments[0], config_collection)
        bootstrap_mean = [
            m for m in cfg.metrics[AnalysisPeriod.WEEK] if m.metric.name == "active_hours"
        ][0].statistic
        bootstrap_mean.name = "deciles"

        assert bootstrap_mean.params["num_samples"] == 10

    def test_pre_treatment(self, experiments, config_collection):
        config_str = dedent(
            """
            [metrics]
            weekly = ["spam"]

            [metrics.spam]
            data_source = "main"
            select_expression = "1"

            [metrics.spam.statistics.bootstrap_mean]
            num_samples = 10
            pre_treatments = ["remove_nulls"]
            """
        )

        spec = AnalysisSpec.from_dict(toml.loads(config_str))
        cfg = spec.resolve(experiments[0], config_collection)
        pre_treatments = [m for m in cfg.metrics[AnalysisPeriod.WEEK] if m.metric.name == "spam"][
            0
        ].pre_treatments

        assert len(pre_treatments) == 1
        assert pre_treatments[0].name == "remove_nulls"

    def test_merge_configs(self, experiments, config_collection):
        orig_conf = dedent(
            """
            [metrics]
            weekly = ["spam"]

            [metrics.spam]
            data_source = "main"
            select_expression = "1"

            [metrics.spam.statistics.bootstrap_mean]
            num_samples = 10
            """
        )

        custom_conf = dedent(
            """
            [metrics]
            weekly = ["foo"]

            [metrics.foo]
            data_source = "main"
            select_expression = "2"

            [metrics.foo.statistics.bootstrap_mean]
            num_samples = 100
            """
        )

        spec = AnalysisSpec.from_dict(toml.loads(orig_conf))
        spec.merge(AnalysisSpec.from_dict(toml.loads(custom_conf)))
        cfg = spec.resolve(experiments[0], config_collection)

        assert len(cfg.metrics[AnalysisPeriod.WEEK]) == 2
        assert len([m for m in cfg.metrics[AnalysisPeriod.WEEK] if m.metric.name == "spam"]) == 1
        assert len([m for m in cfg.metrics[AnalysisPeriod.WEEK] if m.metric.name == "foo"]) == 1

    def test_merge_configs_override_metric(self, experiments, config_collection):
        orig_conf = dedent(
            """
            [metrics]
            weekly = ["spam"]

            [metrics.spam]
            data_source = "main"
            select_expression = "1"

            [metrics.spam.statistics.bootstrap_mean]
            num_samples = 10
            """
        )

        custom_conf = dedent(
            """
            [metrics]
            weekly = ["spam"]

            [metrics.spam]
            data_source = "main"
            select_expression = "2"

            [metrics.spam.statistics.bootstrap_mean]
            num_samples = 100
            """
        )

        spec = AnalysisSpec.from_dict(toml.loads(orig_conf))
        spec.merge(AnalysisSpec.from_dict(toml.loads(custom_conf)))
        cfg = spec.resolve(experiments[0], config_collection)

        spam = [m for m in cfg.metrics[AnalysisPeriod.WEEK] if m.metric.name == "spam"][0]

        assert len(cfg.metrics[AnalysisPeriod.WEEK]) == 1
        assert spam.metric.data_source.name == "main"
        assert spam.metric.select_expression == "2"
        assert spam.metric.analysis_bases == [AnalysisBasis.ENROLLMENTS, AnalysisBasis.EXPOSURES]
        assert spam.statistic.name == "bootstrap_mean"
        assert spam.statistic.params["num_samples"] == 100

    def test_exposure_based_metric(self, experiments, config_collection):
        config_str = dedent(
            """
            [metrics]
            weekly = ["spam"]

            [metrics.spam]
            data_source = "main"
            select_expression = "1"
            analysis_bases = ["exposures"]

            [metrics.spam.statistics.bootstrap_mean]
            """
        )

        spec = AnalysisSpec.from_dict(toml.loads(config_str))
        cfg = spec.resolve(experiments[0], config_collection)
        metric = [m for m in cfg.metrics[AnalysisPeriod.WEEK] if m.metric.name == "spam"][0].metric

        assert AnalysisBasis.EXPOSURES in metric.analysis_bases
        assert AnalysisBasis.ENROLLMENTS not in metric.analysis_bases

    def test_exposure_and_enrollments_based_metric(self, experiments, config_collection):
        config_str = dedent(
            """
            [metrics]
            weekly = ["spam"]

            [metrics.spam]
            data_source = "main"
            select_expression = "1"
            analysis_bases = ["exposures", "enrollments"]

            [metrics.spam.statistics.bootstrap_mean]
            """
        )

        spec = AnalysisSpec.from_dict(toml.loads(config_str))
        cfg = spec.resolve(experiments[0], config_collection)
        metric = [m for m in cfg.metrics[AnalysisPeriod.WEEK] if m.metric.name == "spam"][0].metric

        assert AnalysisBasis.EXPOSURES in metric.analysis_bases
        assert AnalysisBasis.ENROLLMENTS in metric.analysis_bases

    def test_change_metric_to_exposure(self, experiments, config_collection):
        config_str = dedent(
            """
            [metrics]
            weekly = ["active_hours"]

            [metrics.active_hours]
            analysis_bases = ["exposures"]

            [metrics.active_hours.statistics.bootstrap_mean]
            num_samples = 10
            """
        )

        spec = AnalysisSpec.from_dict(toml.loads(config_str))
        cfg = spec.resolve(experiments[0], config_collection)
        metric = [m for m in cfg.metrics[AnalysisPeriod.WEEK] if m.metric.name == "active_hours"][
            0
        ].metric

        assert AnalysisBasis.EXPOSURES in metric.analysis_bases
        assert cfg.experiment.exposure_signal is None

    def test_exposure_signal(self, experiments, config_collection):
        config_str = dedent(
            """
            [experiment.exposure_signal]
            name = "ad_exposure"
            data_source = "main"
            select_expression = "ad_click > 0"
            friendly_name = "Ad exposure"
            description = "Clients have clicked on ad"

            [metrics]
            weekly = ["active_hours"]

            [metrics.active_hours.statistics.bootstrap_mean]
            num_samples = 10
            """
        )

        spec = AnalysisSpec.from_dict(toml.loads(config_str))
        assert spec.experiment.exposure_signal.window_start is None
        assert spec.experiment.exposure_signal.window_end is None

        cfg = spec.resolve(experiments[0], config_collection)

        assert cfg.experiment.exposure_signal == ExposureSignal(
            name="ad_exposure",
            data_source=DataSource(name="main", from_expression="SELECT 1"),
            select_expression="ad_click > 0",
            friendly_name="Ad exposure",
            description="Clients have clicked on ad",
        )

    def test_exposure_signal_windows(self):
        config_str = dedent(
            """
            [experiment.exposure_signal]
            name = "ad_exposure"
            data_source = "search_clients_daily"
            select_expression = "ad_click > 0"
            friendly_name = "Ad exposure"
            description = "Clients have clicked on ad"
            window_start = "enrollment_start"
            window_end = "analysis_window_end"

            [metrics]
            weekly = ["ad_clicks"]

            [metrics.ad_clicks.statistics.bootstrap_mean]
            num_samples = 10
            """
        )

        spec = AnalysisSpec.from_dict(toml.loads(config_str))
        assert spec.experiment.exposure_signal.window_start is AnalysisWindow.ENROLLMENT_START
        assert spec.experiment.exposure_signal.window_end == AnalysisWindow.ANALYSIS_WINDOW_END

    def test_exposure_signal_invalid_windows(self, experiments):
        config_str = dedent(
            """
            [experiment.exposure_signal]
            name = "ad_exposure"
            data_source = "search_clients_daily"
            select_expression = "ad_click > 0"
            friendly_name = "Ad exposure"
            description = "Clients have clicked on ad"
            window_start = 1
            window_end = "invalid"

            [metrics]
            weekly = ["ad_clicks"]

            [metrics.ad_clicks.statistics.bootstrap_mean]
            num_samples = 10
            """
        )

        with pytest.raises(Exception):
            AnalysisSpec.from_dict(toml.loads(config_str))

    def test_merge_parameters(self):
        config_str = dedent(
            """
            description = "Clients have clicked on ad"
            """
        )

        spec = AnalysisSpec.from_dict(toml.loads(config_str))
        default_outcome_param_spec = ParameterSpec.from_dict(
            {
                "param": {
                    "default": "default_overwrite",
                    "friendly_name": "friendly_overwrite",
                    "description": "description_overwrite",
                    "distinct_by_branch": False,
                }
            }
        )

        assert "param" not in spec.parameters.definitions

        spec.merge_parameters(default_outcome_param_spec)

        assert "param" in spec.parameters.definitions
        assert spec.parameters.definitions["param"].name == "param"
        assert spec.parameters.definitions["param"].default == "default_overwrite"
        assert spec.parameters.definitions["param"].friendly_name == "friendly_overwrite"
        assert spec.parameters.definitions["param"].value == "default_overwrite"
        assert spec.parameters.definitions["param"].description == "description_overwrite"
        assert not spec.parameters.definitions["param"].distinct_by_branch

    @pytest.mark.parametrize(
        "input,expected",
        (
            (
                [
                    ParameterDefinition(name="test", value="1"),
                    ParameterDefinition(name="test"),
                ],
                ParameterDefinition(name="test", value="1"),
            ),
            (
                [
                    ParameterDefinition(name="test"),
                    ParameterDefinition(name="test", default="1"),
                ],
                ParameterDefinition(name="test", value="1", default="1"),
            ),
            (
                [
                    ParameterDefinition(name="test", default=None, value="2"),
                    ParameterDefinition(name="test", default="10"),
                ],
                ParameterDefinition(name="test", value="2", default="10"),
            ),
            (
                [
                    ParameterDefinition(name="test", default=None, value="2"),
                    ParameterDefinition(name="test", default="10", friendly_name="friendly"),
                ],
                ParameterDefinition(name="test", friendly_name="friendly", value="2", default="10"),
            ),
        ),
    )
    def test__merge_param(self, input, expected):
        param_definition_1, param_definition_2 = input

        assert expected == AnalysisSpec._merge_param(param_definition_1, param_definition_2)

    @pytest.mark.parametrize(
        "input",
        (
            (
                [
                    ParameterDefinition(name="test"),
                    ParameterDefinition(name="test"),
                ]
            ),
            (
                [
                    ParameterDefinition(
                        name="test", default=None, value="2", distinct_by_branch=True
                    ),
                    ParameterDefinition(name="test"),
                ]
            ),
            (
                [
                    ParameterDefinition(
                        name="test", default=None, value={"branch_1": "1"}, distinct_by_branch=False
                    ),
                    ParameterDefinition(name="test"),
                ]
            ),
        ),
    )
    def test__merge_param_raises(self, input):
        param_definition_1, param_definition_2 = input

        with pytest.raises(InvalidConfigurationException):
            AnalysisSpec._merge_param(param_definition_1, param_definition_2)
