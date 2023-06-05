from datetime import datetime
from pathlib import Path

from pytz import UTC

from metric_config_parser.config import ConfigCollection


class TestConfigIntegration:
    def test_configs_from_repo(self):
        config_collection = ConfigCollection.from_github_repos(
            ["https://github.com/mozilla/metric-hub"]
        )
        assert config_collection is not None
        assert config_collection.get_platform_defaults("firefox_desktop") is None
        assert config_collection.spec_for_outcome("test", "firefox_desktop") is None
        assert (
            config_collection.get_data_source_definition("clients_daily", "firefox_desktop")
            is not None
        )
        assert config_collection.get_metric_definition("baseline_ping_count", "fenix") is not None
        assert config_collection.get_metric_definition("not_exist", "firefox_desktop") is None
        assert (
            config_collection.get_segment_definition("regular_users_v3", "firefox_desktop")
            is not None
        )

    def test_configs_from_multiple_repos(self):
        config_collection = ConfigCollection.from_github_repos(
            repo_urls=[ConfigCollection.repo_url, ConfigCollection.repo_url]
        )
        assert config_collection is not None
        assert config_collection.functions is not None

        default_collection = ConfigCollection.from_github_repo()
        assert len(config_collection.configs) == len(default_collection.configs)
        assert config_collection.outcomes == default_collection.outcomes
        assert len(config_collection.defaults) == len(default_collection.defaults)
        assert len(config_collection.definitions) == len(default_collection.definitions)

    def test_config_from_repo_tree(self):
        config_collection = ConfigCollection.from_github_repo(
            "https://github.com/mozilla/metric-hub/tree/main/jetstream"
        )
        assert config_collection.configs is not None

    def test_config_from_repo_tree_multiple(self):
        config_collection = ConfigCollection.from_github_repos(
            repo_urls=[
                ConfigCollection.repo_url,
                "https://github.com/mozilla/metric-hub/tree/main/jetstream",
            ]
        )

        assert config_collection.configs is not None
        assert len(config_collection.definitions) > 0

    def test_config_as_of(self):
        config_collection = ConfigCollection.from_github_repo(
            "https://github.com/mozilla/metric-hub/tree/main/jetstream"
        ).as_of(
            UTC.localize(datetime(2023, 5, 15))
        )  # 6a052aea23e7e2332a20c992b2e6f07468c3d161

        assert config_collection is not None
        assert config_collection.spec_for_outcome("networking", "firefox_desktop") is None

        config_collection = config_collection.as_of(UTC.localize(datetime(2023, 5, 30)))  # 0f92ef5
        assert config_collection.spec_for_outcome("networking", "firefox_desktop") is not None

    def test_config_as_of_multiple_repos(self):
        config_collection = ConfigCollection.from_github_repos(
            repo_urls=[
                ConfigCollection.repo_url,
                "https://github.com/mozilla/metric-hub/tree/main/jetstream",
            ]
        )

        assert config_collection is not None
        assert config_collection.spec_for_outcome("networking", "firefox_desktop") is not None
        assert config_collection.get_metric_definition("daily_active_users_v2", "fenix") is not None

        config_collection = config_collection.as_of(
            UTC.localize(datetime(2023, 5, 15))
        )  # 6a052aea23e7e2332a20c992b2e6f07468c3d161

        assert config_collection.spec_for_outcome("networking", "firefox_desktop") is None
        assert config_collection.get_metric_definition("daily_active_users_v2", "fenix") is None

        config_collection = config_collection.as_of(
            UTC.localize(datetime(2023, 5, 25, 20, 0, 0))  # 2fa5433
        )

        assert config_collection.spec_for_outcome("networking", "firefox_desktop") is None
        assert config_collection.get_metric_definition("daily_active_users_v2", "fenix") is not None

    def test_remove_tmp_dir_on_destruct(self):
        config_collection = ConfigCollection.from_github_repos(
            repo_urls=[
                ConfigCollection.repo_url,
                "https://github.com/mozilla/metric-hub/tree/main/jetstream",
            ]
        )

        tmp_dirs = [Path(r.repo.git_dir).parent for r in config_collection.repos]

        for tmp_dir in tmp_dirs:
            assert tmp_dir.exists()

        del config_collection

        for tmp_dir in tmp_dirs:
            assert tmp_dir.exists() is False
