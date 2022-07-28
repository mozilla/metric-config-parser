import datetime as dt
from textwrap import dedent

import pytest
import pytz
import toml

from jetstream_config_parser.analysis import AnalysisSpec
from jetstream_config_parser.config import ConfigCollection, DefinitionConfig
from jetstream_config_parser.experiment import Branch, Experiment
from jetstream_config_parser.function import FunctionsSpec


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

    return ConfigCollection(
        configs=[],
        outcomes=[],
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
            outcomes=["parameterised_distinct_by_branch_config"],
            app_name="firefox_desktop",
        ),
    ]
