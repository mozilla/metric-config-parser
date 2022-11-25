from metric_config_parser.config import ConfigCollection


class TestConfigIntegration:
    def test_configs_from_repo(self):
        config_collection = ConfigCollection.from_github_repos(
            ["https://github.com/mozilla/metric-hub", "https://github.com/mozilla/jetstream-config"]
        )
        assert config_collection is not None
        assert config_collection.get_platform_defaults("firefox_desktop") is not None
        assert (
            config_collection.spec_for_outcome(
                config_collection.outcomes[0].slug, config_collection.outcomes[0].platform
            )
            is not None
        )
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

    def test_configs_from_private_repo(self):
        config_collection = ConfigCollection.from_github_repo(
            "https://github.com/mozilla/jetstream-config", is_private=True
        )
        assert config_collection is not None
        assert config_collection.configs[0].spec.experiment.is_private

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
