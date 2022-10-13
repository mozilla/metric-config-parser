import datetime as dt
from textwrap import dedent

import pytest
import pytz
import toml

from metric_config_parser.analysis import AnalysisSpec
from metric_config_parser.config import ConfigCollection, DefinitionConfig, Outcome
from metric_config_parser.experiment import Branch, Experiment
from metric_config_parser.function import FunctionsSpec
from metric_config_parser.outcome import OutcomeSpec


@pytest.fixture
def config_collection():

    config_str = dedent(
        """
        [metrics]

        [metrics.view_about_logins]
        data_source = "main"
        select_expression = "1"

        [metrics.view_about_logins.statistics.bootstrap_mean]

        [metrics.unenroll]
        data_source = "main"
        select_expression = "1"

        [metrics.unenroll.statistics.bootstrap_mean]

        [metrics.active_hours]
        data_source = "main"
        select_expression = "1"

        [metrics.active_hours.statistics.bootstrap_mean]

        [data_sources]

        [data_sources.main]
        from_expression = "SELECT 1"

        [data_sources.clients_daily]
        from_expression = "SELECT 1"

        [segments]

        [segments.regular_users_v3]
        data_source = "my_cool_data_source"
        select_expression = "{{agg_any('1')}}"

        [segments.data_sources.my_cool_data_source]
        from_expression = '''
            (SELECT 1 WHERE submission_date BETWEEN {{experiment.start_date_str}}
            AND {{experiment.last_enrollment_date_str}})
        '''
        """
    )

    performance_config = dedent(
        """
        friendly_name = "Performance outcomes"
        description = "Outcomes related to performance"
        default_metrics = ["speed"]

        [metrics.speed]
        data_source = "main"
        select_expression = "1"

        [metrics.speed.statistics.bootstrap_mean]
        """
    )

    tastiness_config = dedent(
        """
        friendly_name = "Tastiness outcomes"
        description = "Outcomes related to tastiness 😋"

        [metrics.meals_eaten]
        data_source = "meals"
        select_expression = "1"
        friendly_name = "Meals eaten"
        description = "Number of consumed meals"

        [metrics.meals_eaten.statistics.bootstrap_mean]
        num_samples = 10
        pre_treatments = ["remove_nulls"]

        [data_sources.meals]
        from_expression = "meals"
        client_id_column = "client_info.client_id"
        """
    )

    parameterized_config = dedent(
        """
        friendly_name = "Outcome with parameter same across all branches"
        description = "Outcome that has a parameter that is the same across all branches"
        default_metrics = ["sample_id_count"]

        [metrics.sample_id_count]
        data_source = "main"
        select_expression = "COUNTIF(sample_id = {{ parameters.id }})"

        [metrics.sample_id_count.statistics.bootstrap_mean]

        [parameters.id]
        friendly_name = "Some random ID"
        description = "A random ID used to count samples"
        default = "700"
        distinct_by_branch = false
        """
    )

    parameterized_distinct_by_branch_config = dedent(
        """
        friendly_name = "Outcome with parameter same across all branches"
        description = "Outcome that has a parameter that is the same across all branches"
        default_metrics = ["sample_id_count"]

        [metrics.sample_id_count]
        data_source = "main"
        select_expression = "COUNTIF(sample_id = {{ parameters.id }})"

        [metrics.sample_id_count.statistics.bootstrap_mean]

        [parameters.id]
        friendly_name = "Some random ID"
        description = "A random ID used to count samples"
        distinct_by_branch = true

        default.branch_1 = 1
        """
    )

    return ConfigCollection(
        configs=[],
        outcomes=[
            Outcome(
                slug="performance",
                spec=OutcomeSpec.from_dict(toml.loads(performance_config)),
                platform="firefox_desktop",
                commit_hash="000000",
            ),
            Outcome(
                slug="tastiness",
                spec=OutcomeSpec.from_dict(toml.loads(tastiness_config)),
                platform="firefox_desktop",
                commit_hash="000000",
            ),
            Outcome(
                slug="parameterized",
                spec=OutcomeSpec.from_dict(toml.loads(parameterized_config)),
                platform="firefox_desktop",
                commit_hash="000000",
            ),
            Outcome(
                slug="parameterized_distinct_by_branch_config",
                spec=OutcomeSpec.from_dict(toml.loads(parameterized_distinct_by_branch_config)),
                platform="firefox_desktop",
                commit_hash="000000",
            ),
        ],
        defaults=[],
        definitions=[
            DefinitionConfig(
                slug="firefox_desktop",
                spec=AnalysisSpec.from_dict(toml.loads(config_str)),
                last_modified=dt.date.today(),
                platform="firefox_desktop",
            )
        ],
        functions=FunctionsSpec.from_dict(
            {
                "functions": {
                    "agg_histogram_mean": {
                        "definition": """SAFE_DIVIDE(
                        SUM(CAST(JSON_EXTRACT_SCALAR({select_expr}, "$.sum") AS int64)),
                        SUM((SELECT SUM(value)
                        FROM UNNEST(mozfun.hist.extract({select_expr}).values)))
                    )""",
                    },
                    "agg_any": {"definition": "COALESCE(LOGICAL_OR({select_expr}), FALSE)"},
                }
            }
        ),
    )


@pytest.fixture
def experiments():
    return [
        Experiment(
            experimenter_slug="test_slug",
            type="pref",
            status="Complete",
            start_date=dt.datetime(2019, 12, 1, tzinfo=pytz.utc),
            end_date=dt.datetime(2020, 3, 1, tzinfo=pytz.utc),
            proposed_enrollment=7,
            branches=[Branch(slug="a", ratio=1), Branch(slug="b", ratio=1)],
            normandy_slug="normandy-test-slug",
            reference_branch="b",
            is_high_population=False,
            app_name="firefox_desktop",
        ),
        Experiment(
            experimenter_slug="test_slug",
            type="addon",
            status="Complete",
            start_date=dt.datetime(2019, 12, 1, tzinfo=pytz.utc),
            end_date=dt.datetime(2020, 3, 1, tzinfo=pytz.utc),
            proposed_enrollment=0,
            branches=[],
            normandy_slug=None,
            reference_branch=None,
            is_high_population=False,
            app_name="firefox_desktop",
        ),
        Experiment(
            experimenter_slug="test_slug",
            type="pref",
            status="Live",
            start_date=dt.datetime(2019, 12, 1, tzinfo=pytz.utc),
            end_date=dt.datetime(2020, 3, 1, tzinfo=pytz.utc),
            proposed_enrollment=7,
            branches=[],
            normandy_slug="normandy-test-slug",
            reference_branch=None,
            is_high_population=False,
            app_name="firefox_desktop",
        ),
        Experiment(
            experimenter_slug="test_slug",
            type="pref",
            status="Live",
            start_date=dt.datetime(2019, 12, 1, tzinfo=pytz.utc),
            end_date=dt.datetime(2020, 3, 1, tzinfo=pytz.utc),
            proposed_enrollment=7,
            branches=[],
            normandy_slug="normandy-test-slug",
            reference_branch=None,
            is_high_population=True,
            app_name="firefox_desktop",
        ),
        Experiment(
            experimenter_slug="test_slug",
            type="pref",
            status="Complete",
            start_date=dt.datetime(2019, 12, 1, tzinfo=pytz.utc),
            end_date=dt.datetime(2020, 3, 1, tzinfo=pytz.utc),
            proposed_enrollment=7,
            branches=[Branch(slug="a", ratio=1), Branch(slug="b", ratio=1)],
            normandy_slug="normandy-test-slug",
            reference_branch="b",
            is_high_population=False,
            app_name="firefox_desktop",
        ),
        Experiment(
            experimenter_slug="test_slug",
            type="pref",
            status="Live",
            start_date=dt.datetime(2019, 12, 1, tzinfo=pytz.utc),
            end_date=dt.datetime(2020, 3, 1, tzinfo=pytz.utc),
            proposed_enrollment=7,
            branches=[],
            normandy_slug="normandy-test-slug",
            reference_branch=None,
            is_high_population=True,
            outcomes=["performance", "tastiness"],
            app_name="firefox_desktop",
        ),
        Experiment(
            experimenter_slug="test_slug",
            type="pref",
            status="Live",
            start_date=dt.datetime(2019, 12, 1, tzinfo=pytz.utc),
            end_date=dt.datetime(2020, 3, 1, tzinfo=pytz.utc),
            proposed_enrollment=7,
            branches=[],
            normandy_slug="normandy-test-slug",
            reference_branch=None,
            is_high_population=True,
            outcomes=["parameterized"],
            app_name="firefox_desktop",
        ),
        Experiment(
            experimenter_slug="test_slug",
            type="pref",
            status="Live",
            start_date=dt.datetime(2019, 12, 1, tzinfo=pytz.utc),
            end_date=dt.datetime(2020, 3, 1, tzinfo=pytz.utc),
            proposed_enrollment=7,
            branches=[],
            normandy_slug="normandy-test-slug",
            reference_branch=None,
            is_high_population=True,
            outcomes=["parameterized_distinct_by_branch_config"],
            app_name="firefox_desktop",
            enrollment_end_date=dt.datetime(2019, 12, 3, tzinfo=pytz.utc),
        ),
    ]
